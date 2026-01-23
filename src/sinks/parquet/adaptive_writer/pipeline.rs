//! Pipeline tasks for the adaptive Parquet writer.
//!
//! ## Pipeline Architecture
//!
//! ```text
//! ┌────────┐ ingest_queue  ┌───────────┐ encoding_queue  ┌───────────┐ write_queue ┌──────────┐
//! │ Source │──────────────▶│ Ingestion ├────────────────▶│ Encoding  │────────────▶│  Writer  │
//! │        │ (RecordBatch) │ (1 task)  │ (RowGroupWork)  │ (N tasks) │ (EncodedRG) │ (1 task) │
//! └────────┘               └───────────┘                 └───────────┘             └──────────┘
//! ```
//!
//! ## Stages
//!
//! 1. **Source** (`ingest_queue`): RecordBatches from DataSources/queries flow into the writer.
//!
//! 2. **Queue** (`ingest_queue`): RecordBatch items are sent to the ingestion task.
//!
//! 3. **Ingestion** (1 task): Collects batches, analyzes column cardinality, assembles row
//!    groups. Outputs `RowGroupWork` containing the batch and per-row-group writer properties.
//!
//! 4. **Queue** (`encoding_queue`): RowGroupWork items are sent to the encoding tasks.
//!
//! 5. **Encoding** (N parallel tasks): Encodes each row group's columns using the rayon
//!    thread pool (`--parquet-column-encoding-threads`). Outputs `EncodedRowGroup`.
//!
//! 6. **Queue** (`write_queue`): EncodedRowGroup items are sent to the writer task.
//!
//! 7. **Writing** (1 task): Writes encoded row groups to disk in order using the I/O rayon
//!    pool (`--parquet-io-threads`).
//!
//! ## Queue Sizing
//!
//! - `ingest_queue_size` (`--parquet-ingest-queue-size`): How many record batches can wait
//!   to be assembled into row groups. Larger values let the source stay ahead of row group
//!   assembly.
//!
//! - `encoding_queue_size` (`--parquet-encoding-queue-size`): How many row groups can wait
//!   to be encoded. Larger values let ingestion stay ahead of encoding.
//!
//! - `write_queue_size` (`--parquet-write-queue-size`): How many encoded row groups can
//!   wait to be written. Larger values let encoding stay ahead of I/O.

use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::ArrowWriter;
#[allow(deprecated)]
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowWriterOptions, compute_leaves, get_column_writers,
};
use parquet::file::properties::{WriterProperties, WriterPropertiesPtr};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::SchemaDescPtr;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::analysis::{ColumnAnalysis, RowGroupAnalysisState, SimpleAccumulator};
use super::config::{AdaptiveWriterConfig, ResolvedColumnConfigs};
use super::encoding::build_row_group_properties;
use crate::sinks::parquet::ParquetThreadPools;
use crate::utils::cancellable_channel::{
    CancellableReceiver, CancellableSender, cancellable_channel_bounded,
};
use crate::utils::first_error::FirstError;
use crate::utils::ordered_channel::{OrderedReceiver, OrderedSender, ordered_channel};
use crate::utils::rayon_bridge::spawn_on_pool;

pub(crate) struct RowGroupWork {
    pub index: usize,
    pub batch: RecordBatch,
    pub props: WriterPropertiesPtr,
    pub parquet_schema: SchemaDescPtr,
    pub schema: SchemaRef,
}

pub(crate) struct EncodedRowGroup {
    pub chunks: Vec<ArrowColumnChunk>,
    pub num_rows: usize,
}

pub(crate) async fn create_file(
    path: PathBuf,
    buffer_size: usize,
    pools: Arc<ParquetThreadPools>,
) -> Result<BufWriter<File>> {
    spawn_on_pool(&pools.io, {
        let path = path.clone();
        move || -> Result<_> { Ok(BufWriter::with_capacity(buffer_size, File::create(&path)?)) }
    })
    .await
    .context("file create task failed")?
}

pub(crate) fn create_arrow_writer(
    file: BufWriter<File>,
    schema: &SchemaRef,
    base_props: WriterProperties,
    skip_arrow_metadata: bool,
) -> Result<(
    SerializedFileWriter<BufWriter<File>>,
    SchemaDescPtr,
    WriterPropertiesPtr,
)> {
    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(true)
        .convert(schema.as_ref())?;

    let arrow_writer = ArrowWriter::try_new_with_options(
        file,
        Arc::clone(schema),
        ArrowWriterOptions::new()
            .with_properties(base_props.clone())
            .with_skip_arrow_metadata(skip_arrow_metadata)
            .with_parquet_schema(parquet_schema.clone()),
    )?;

    let (file_writer, _) = arrow_writer.into_serialized_writer()?;
    let parquet_schema: SchemaDescPtr = Arc::new(parquet_schema);
    let base_props: WriterPropertiesPtr = Arc::new(base_props);

    Ok((file_writer, parquet_schema, base_props))
}

pub(crate) async fn run_pipeline(
    path: PathBuf,
    schema: SchemaRef,
    base_props: WriterProperties,
    pools: Arc<ParquetThreadPools>,
    config: AdaptiveWriterConfig,
    batch_rx: CancellableReceiver<RecordBatch>,
    first_error: Arc<FirstError>,
) -> Result<u64> {
    let file = create_file(path, config.buffer_size, Arc::clone(&pools)).await?;

    let (encoder_tx, encoder_rx) = cancellable_channel_bounded::<RowGroupWork>(
        config.encoding_queue_size,
        first_error.cancel_token(),
        "encoder",
    );

    let (writer_tx, writer_rx) =
        ordered_channel::<EncodedRowGroup>(config.write_queue_size, first_error.cancel_token());

    let (file_writer, parquet_schema, base_props) =
        create_arrow_writer(file, &schema, base_props, config.skip_arrow_metadata)?;

    let total_rows = Arc::new(AtomicU64::new(0));

    let mut tasks: JoinSet<()> = JoinSet::new();

    tasks.spawn(ingestion_task(
        batch_rx,
        encoder_tx,
        Arc::clone(&schema),
        Arc::clone(&parquet_schema),
        Arc::clone(&base_props),
        config.clone(),
        Arc::clone(&first_error),
    ));

    for _ in 0..config.max_row_group_concurrency {
        tasks.spawn(encoder_task(
            encoder_rx.clone(),
            writer_tx.clone(),
            Arc::clone(&pools),
            Arc::clone(&first_error),
        ));
    }
    drop(encoder_rx);
    drop(writer_tx);

    tasks.spawn(writer_task(
        writer_rx,
        file_writer,
        Arc::clone(&pools),
        Arc::clone(&first_error),
        Arc::clone(&total_rows),
    ));

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            first_error.set(anyhow!("task panicked: {e}"));
        }
    }

    if let Some(err) = first_error.take() {
        return Err(err);
    }

    Ok(total_rows.load(Ordering::SeqCst))
}

async fn ingestion_task(
    batch_rx: CancellableReceiver<RecordBatch>,
    encoder_tx: CancellableSender<RowGroupWork>,
    schema: SchemaRef,
    parquet_schema: SchemaDescPtr,
    base_props: WriterPropertiesPtr,
    config: AdaptiveWriterConfig,
    first_error: Arc<FirstError>,
) {
    let resolved = ResolvedColumnConfigs::resolve(&schema, &config);
    let columns_to_analyze = resolved.columns_needing_analysis();

    let result = if columns_to_analyze.is_empty() {
        ingestion_task_simple(
            batch_rx,
            encoder_tx,
            schema,
            parquet_schema,
            base_props,
            config,
            resolved,
        )
        .await
    } else {
        ingestion_task_with_analysis(
            batch_rx,
            encoder_tx,
            schema,
            parquet_schema,
            base_props,
            config,
            resolved,
            first_error.cancel_token(),
        )
        .await
    };

    if let Err(e) = result {
        first_error.set(e);
    }
}

#[allow(clippy::too_many_arguments)]
async fn ingestion_task_simple(
    mut batch_rx: CancellableReceiver<RecordBatch>,
    encoder_tx: CancellableSender<RowGroupWork>,
    schema: SchemaRef,
    parquet_schema: SchemaDescPtr,
    base_props: WriterPropertiesPtr,
    config: AdaptiveWriterConfig,
    resolved: ResolvedColumnConfigs,
) -> Result<()> {
    let mut row_group_index = 0usize;
    let max_rows = config.max_row_group_size;
    let mut acc = SimpleAccumulator::new(Arc::clone(&schema), max_rows);
    let empty_analysis: HashMap<String, ColumnAnalysis> = HashMap::new();

    while let Ok(batch) = batch_rx.recv().await {
        let mut remaining = Some(batch);

        while let Some(batch) = remaining.take() {
            let batch_rows = batch.num_rows();
            let mut space_left = max_rows.saturating_sub(acc.rows());

            if batch_rows <= space_left {
                acc.push(batch)?;
                space_left -= batch_rows;
            } else {
                let for_current = batch.slice(0, space_left);
                let for_next = batch.slice(space_left, batch_rows - space_left);
                acc.push(for_current)?;
                space_left = 0;
                remaining = Some(for_next);
            }

            if space_left == 0 {
                let (batch, num_rows) = acc.take()?;
                let props = build_row_group_properties(
                    &schema,
                    &base_props,
                    &config,
                    &resolved,
                    &empty_analysis,
                    num_rows,
                )?;
                let work = RowGroupWork {
                    index: row_group_index,
                    batch,
                    props: Arc::new(props),
                    parquet_schema: Arc::clone(&parquet_schema),
                    schema: Arc::clone(&schema),
                };
                if encoder_tx.send(work).await.is_err() {
                    return Ok(());
                }
                row_group_index += 1;
                acc = SimpleAccumulator::new(Arc::clone(&schema), max_rows);
            }
        }
    }

    if acc.rows() > 0 {
        let (batch, num_rows) = acc.take()?;
        let props = build_row_group_properties(
            &schema,
            &base_props,
            &config,
            &resolved,
            &empty_analysis,
            num_rows,
        )?;
        let work = RowGroupWork {
            index: row_group_index,
            batch,
            props: Arc::new(props),
            parquet_schema: Arc::clone(&parquet_schema),
            schema: Arc::clone(&schema),
        };
        encoder_tx
            .send(work)
            .await
            .context("failed to send final batch")?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn ingestion_task_with_analysis(
    mut batch_rx: CancellableReceiver<RecordBatch>,
    encoder_tx: CancellableSender<RowGroupWork>,
    schema: SchemaRef,
    parquet_schema: SchemaDescPtr,
    base_props: WriterPropertiesPtr,
    config: AdaptiveWriterConfig,
    resolved: ResolvedColumnConfigs,
    cancel_signal: CancellationToken,
) -> Result<()> {
    let columns_to_analyze = resolved.columns_needing_analysis();
    let mut row_group_index = 0usize;
    let max_rows = config.max_row_group_size;
    let mut state =
        RowGroupAnalysisState::new(&schema, &columns_to_analyze, &cancel_signal, max_rows);

    while let Ok(batch) = batch_rx.recv().await {
        let mut remaining = Some(batch);

        while let Some(batch) = remaining.take() {
            let batch_rows = batch.num_rows();
            let mut space_left = max_rows.saturating_sub(state.rows());

            if batch_rows <= space_left {
                state.push(batch).await?;
                space_left -= batch_rows;
            } else {
                let for_current = batch.slice(0, space_left);
                let for_next = batch.slice(space_left, batch_rows - space_left);
                state.push(for_current).await?;
                space_left = 0;
                remaining = Some(for_next);
            }

            if space_left == 0 {
                let (batch, num_rows, analysis) = state.finalize().await?;
                let props = build_row_group_properties(
                    &schema,
                    &base_props,
                    &config,
                    &resolved,
                    &analysis,
                    num_rows,
                )?;

                let work = RowGroupWork {
                    index: row_group_index,
                    batch,
                    props: Arc::new(props),
                    parquet_schema: Arc::clone(&parquet_schema),
                    schema: Arc::clone(&schema),
                };

                if encoder_tx.send(work).await.is_err() {
                    return Ok(());
                }
                row_group_index += 1;
                state = RowGroupAnalysisState::new(
                    &schema,
                    &columns_to_analyze,
                    &cancel_signal,
                    max_rows,
                );
            }
        }
    }

    if state.rows() > 0 {
        let (batch, num_rows, analysis) = state.finalize().await?;
        let props = build_row_group_properties(
            &schema,
            &base_props,
            &config,
            &resolved,
            &analysis,
            num_rows,
        )?;

        let work = RowGroupWork {
            index: row_group_index,
            batch,
            props: Arc::new(props),
            parquet_schema: Arc::clone(&parquet_schema),
            schema: Arc::clone(&schema),
        };
        encoder_tx
            .send(work)
            .await
            .context("failed to send final batch")?;
    }

    Ok(())
}

async fn encoder_task(
    encoder_rx: CancellableReceiver<RowGroupWork>,
    writer_tx: OrderedSender<EncodedRowGroup>,
    pools: Arc<ParquetThreadPools>,
    first_error: Arc<FirstError>,
) {
    let mut rx = encoder_rx.with_task_name("encoder");

    let result = async {
        while let Ok(work) = rx.recv().await {
            #[allow(deprecated)]
            let column_writers =
                get_column_writers(&work.parquet_schema, &work.props, &work.schema)?;
            let encoded =
                encode_row_group(work.batch, column_writers, &work.schema, &pools).await?;

            let _ = writer_tx.send(work.index, encoded).await;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;

    if let Err(e) = result {
        first_error.set(e);
    }
}

async fn writer_task(
    mut encoded_rx: OrderedReceiver<EncodedRowGroup>,
    file_writer: SerializedFileWriter<BufWriter<File>>,
    pools: Arc<ParquetThreadPools>,
    first_error: Arc<FirstError>,
    total_rows: Arc<AtomicU64>,
) {
    let result = async {
        let mut fw = file_writer;

        while let Ok(rg) = encoded_rx.recv().await {
            total_rows.fetch_add(rg.num_rows as u64, Ordering::SeqCst);
            fw = write_row_group(&pools, fw, rg).await?;
        }

        spawn_on_pool(&pools.io, move || fw.close().map_err(|e| anyhow!(e)))
            .await
            .context("file close task failed")??;

        Ok::<_, anyhow::Error>(())
    }
    .await;

    if let Err(e) = result {
        first_error.set(e);
    }
}

async fn write_row_group(
    pools: &ParquetThreadPools,
    fw: SerializedFileWriter<BufWriter<File>>,
    rg: EncodedRowGroup,
) -> Result<SerializedFileWriter<BufWriter<File>>> {
    spawn_on_pool(&pools.io, move || -> Result<_> {
        let mut fw = fw;
        let mut row_group = fw.next_row_group()?;
        for chunk in rg.chunks {
            chunk.append_to_row_group(&mut row_group)?;
        }
        row_group.close()?;
        Ok(fw)
    })
    .await
    .context("row group write task failed")?
}

async fn encode_row_group(
    batch: RecordBatch,
    column_writers: Vec<ArrowColumnWriter>,
    schema: &SchemaRef,
    pools: &ParquetThreadPools,
) -> Result<EncodedRowGroup> {
    let num_rows = batch.num_rows();
    let num_columns = column_writers.len();

    let mut leaves_per_column: Vec<Vec<_>> = (0..num_columns).map(|_| Vec::new()).collect();
    let mut col_idx = 0;
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let leaves = compute_leaves(field.as_ref(), column)?;
        for leaf in leaves {
            leaves_per_column[col_idx].push(leaf);
            col_idx += 1;
        }
    }

    let mut handles = Vec::with_capacity(num_columns);
    for (writer, leaves) in column_writers.into_iter().zip(leaves_per_column) {
        handles.push(spawn_on_pool(&pools.encoding, move || -> Result<_> {
            let mut writer = writer;
            for leaf in leaves {
                writer.write(&leaf)?;
            }
            writer.close().map_err(|e| anyhow!(e))
        }));
    }

    let mut chunks = Vec::with_capacity(num_columns);
    for handle in handles {
        chunks.push(handle.await.context("column encode task failed")??);
    }

    Ok(EncodedRowGroup { chunks, num_rows })
}
