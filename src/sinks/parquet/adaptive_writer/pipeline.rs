//! Pipeline tasks for the adaptive Parquet writer.
//!
//! ## Pipeline Architecture
//!
//! ```text
//! Main Runtime              CpuRuntime              IoRuntime
//!      │                        │                       │
//!      ├─► ingestion_task       │                       │
//!      │        │               │                       │
//!      │   tokio::mpsc          │                       │
//!      │        │               │                       │
//!      ├─► encoder_coordinator ─┼─► encode tasks        │
//!      │   (FuturesOrdered)     │   (CPU work inline)   │
//!      │        │               │        │              │
//!      │   tokio::mpsc          │        │              │
//!      │        ◄───────────────┼────────┘              │
//!      │        │                                       │
//!      └─► writer_task ─────────────────────────────────┼─► I/O tasks
//!               │                                       │   (file ops)
//!               └───────────────────────────────────────┘
//! ```
//!
//! ## Stages
//!
//! 1. **Source**: RecordBatches flow into the ingestion channel.
//!
//! 2. **Ingestion** (1 task): Accumulates batches into row groups. Two modes:
//!    - **Pass-through**: Simple accumulation with `BatchCoalescer`.
//!    - **Analyze**: Streams to background analysis for `dictionary: auto` columns.
//!
//! 3. **Encoder Coordinator** (1 task): Uses `FuturesOrdered` to spawn encoding tasks
//!    on `CpuRuntime` (up to `max_row_group_concurrency`). Results come out in order.
//!
//! 4. **Writing** (1 task): Receives encoded row groups in order, writes to disk
//!    via `IoRuntime`.

use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use arrow::array::RecordBatch;
use arrow::compute::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::ArrowWriter;
#[allow(deprecated)]
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowWriterOptions, compute_leaves, get_column_writers,
};
use parquet::file::properties::{WriterProperties, WriterPropertiesPtr};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::SchemaDescPtr;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use super::analysis::{ColumnAnalysis, RowGroupAnalysisState};
use super::config::{AdaptiveWriterConfig, ResolvedColumnConfigs};
use super::encoding::build_row_group_properties;
use crate::sinks::parquet::pools::ParquetRuntimes;

pub(crate) struct RowGroupWork {
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
    runtimes: &ParquetRuntimes,
) -> Result<BufWriter<File>> {
    let mut tasks = JoinSet::new();
    tasks.spawn_on(
        async move { Ok(BufWriter::with_capacity(buffer_size, File::create(&path)?)) },
        runtimes.io.handle(),
    );
    tasks
        .join_next()
        .await
        .ok_or_else(|| anyhow!("file create task set unexpectedly empty"))?
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
    runtimes: Arc<ParquetRuntimes>,
    config: AdaptiveWriterConfig,
    ingestion_rx: mpsc::Receiver<RecordBatch>,
) -> Result<u64> {
    let file = create_file(path, config.buffer_size, &runtimes).await?;

    let (encoding_tx, encoding_rx) = mpsc::channel::<RowGroupWork>(config.encoding_queue_size);
    let (writing_tx, writing_rx) = mpsc::channel::<EncodedRowGroup>(config.writing_queue_size);

    let (file_writer, parquet_schema, base_props) =
        create_arrow_writer(file, &schema, base_props, config.skip_arrow_metadata)?;

    let total_rows = Arc::new(AtomicU64::new(0));
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();

    tasks.spawn(ingestion_task(
        ingestion_rx,
        encoding_tx,
        Arc::clone(&schema),
        Arc::clone(&parquet_schema),
        Arc::clone(&base_props),
        config.clone(),
    ));

    // encoder coordinator on main runtime (spawns to CpuRuntime)
    let cpu_handle = runtimes.cpu.handle().clone();
    tasks.spawn(encoder_coordinator(
        encoding_rx,
        writing_tx,
        cpu_handle,
        config.max_row_group_concurrency,
    ));

    // writer task on main runtime (spawns I/O to IoRuntime)
    let io_handle = runtimes.io.handle().clone();
    tasks.spawn(writer_task(
        writing_rx,
        file_writer,
        io_handle,
        Arc::clone(&total_rows),
    ));

    let mut errors: Vec<anyhow::Error> = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Err(e) if e.is_panic() => {
                errors.push(anyhow!("task panicked: {e}"));
                tasks.abort_all();
            }
            Err(e) if e.is_cancelled() => continue,
            Err(e) => {
                // unexpected join error
                errors.push(anyhow!("task join error: {e}"));
                tasks.abort_all();
            }
            Ok(Err(e)) => {
                if errors.is_empty() {
                    tasks.abort_all();
                }
                errors.push(e);
            }
            Ok(Ok(())) => continue,
        }
    }

    match errors.len() {
        0 => Ok(total_rows.load(Ordering::SeqCst)),
        1 => Err(errors.pop().unwrap()),
        _ => Err(anyhow!(
            "pipeline failed with {} errors:\n{}",
            errors.len(),
            errors
                .iter()
                .map(|e| format!("  - {e}"))
                .collect::<Vec<_>>()
                .join("\n")
        )),
    }
}

async fn ingestion_task(
    ingestion_rx: mpsc::Receiver<RecordBatch>,
    encoding_tx: mpsc::Sender<RowGroupWork>,
    schema: SchemaRef,
    parquet_schema: SchemaDescPtr,
    base_props: WriterPropertiesPtr,
    config: AdaptiveWriterConfig,
) -> Result<()> {
    let resolved = ResolvedColumnConfigs::resolve(&schema, &config);
    let columns_to_analyze = resolved.columns_needing_analysis();

    if columns_to_analyze.is_empty() {
        ingestion_task_passthrough(
            ingestion_rx,
            encoding_tx,
            schema,
            parquet_schema,
            base_props,
            config,
            resolved,
        )
        .await
    } else {
        ingestion_task_analyze(
            ingestion_rx,
            encoding_tx,
            schema,
            parquet_schema,
            base_props,
            config,
            resolved,
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn ingestion_task_passthrough(
    mut ingestion_rx: mpsc::Receiver<RecordBatch>,
    encoding_tx: mpsc::Sender<RowGroupWork>,
    schema: SchemaRef,
    parquet_schema: SchemaDescPtr,
    base_props: WriterPropertiesPtr,
    config: AdaptiveWriterConfig,
    resolved: ResolvedColumnConfigs,
) -> Result<()> {
    let mut coalescer = BatchCoalescer::new(Arc::clone(&schema), config.max_row_group_size);
    let empty_analysis: HashMap<String, ColumnAnalysis> = HashMap::new();

    while let Some(batch) = ingestion_rx.recv().await {
        coalescer.push_batch(batch)?;

        while let Some(batch) = coalescer.next_completed_batch() {
            let num_rows = batch.num_rows();
            let props = build_row_group_properties(
                &schema,
                &base_props,
                &config,
                &resolved,
                &empty_analysis,
                num_rows,
            )?;
            let work = RowGroupWork {
                batch,
                props: Arc::new(props),
                parquet_schema: Arc::clone(&parquet_schema),
                schema: Arc::clone(&schema),
            };
            if encoding_tx.send(work).await.is_err() {
                return Ok(());
            }
        }
    }

    coalescer.finish_buffered_batch()?;
    if let Some(batch) = coalescer.next_completed_batch() {
        let num_rows = batch.num_rows();
        let props = build_row_group_properties(
            &schema,
            &base_props,
            &config,
            &resolved,
            &empty_analysis,
            num_rows,
        )?;
        let work = RowGroupWork {
            batch,
            props: Arc::new(props),
            parquet_schema: Arc::clone(&parquet_schema),
            schema: Arc::clone(&schema),
        };
        // channel close is graceful shutdown
        let _ = encoding_tx.send(work).await;
    }

    Ok(())
}

/// Wraps BatchCoalescer with hard row limit splitting.
/// Returns the pushed portion so callers can forward it elsewhere (e.g., analysis).
struct HardLimitBatchCoalescer {
    coalescer: BatchCoalescer,
    max_rows: usize,
    rows: usize,
}

impl HardLimitBatchCoalescer {
    fn new(schema: &SchemaRef, max_rows: usize) -> Self {
        Self {
            coalescer: BatchCoalescer::new(Arc::clone(schema), max_rows),
            max_rows,
            rows: 0,
        }
    }

    /// Push a batch, splitting at boundary if needed.
    /// Returns (pushed_portion, remaining_portion).
    fn push_batch(&mut self, batch: RecordBatch) -> Result<(RecordBatch, Option<RecordBatch>)> {
        let space_left = self.max_rows - self.rows;

        if space_left == 0 {
            return Ok((RecordBatch::new_empty(batch.schema()), Some(batch)));
        }

        let (to_push, remaining) = if batch.num_rows() <= space_left {
            (batch, None)
        } else {
            let for_current = batch.slice(0, space_left);
            let for_next = batch.slice(space_left, batch.num_rows() - space_left);
            (for_current, Some(for_next))
        };

        self.rows += to_push.num_rows();
        self.coalescer.push_batch(to_push.clone())?;
        Ok((to_push, remaining))
    }

    fn is_full(&self) -> bool {
        self.rows >= self.max_rows
    }

    fn finish(&mut self) -> Result<Option<RecordBatch>> {
        self.coalescer.finish_buffered_batch()?;
        self.rows = 0;
        Ok(self.coalescer.next_completed_batch())
    }
}

#[allow(clippy::too_many_arguments)]
async fn ingestion_task_analyze(
    mut ingestion_rx: mpsc::Receiver<RecordBatch>,
    encoding_tx: mpsc::Sender<RowGroupWork>,
    schema: SchemaRef,
    parquet_schema: SchemaDescPtr,
    base_props: WriterPropertiesPtr,
    config: AdaptiveWriterConfig,
    resolved: ResolvedColumnConfigs,
) -> Result<()> {
    let columns_to_analyze = resolved.columns_needing_analysis();
    let max_rows = config.max_row_group_size;
    let mut coalescer = HardLimitBatchCoalescer::new(&schema, max_rows);
    let mut analysis = RowGroupAnalysisState::try_new(&schema, &columns_to_analyze)?;

    while let Some(batch) = ingestion_rx.recv().await {
        let mut remaining = Some(batch);

        while let Some(batch) = remaining.take() {
            let (pushed, rest) = coalescer.push_batch(batch)?;
            remaining = rest;
            if pushed.num_rows() > 0 {
                analysis.push(pushed).await?;
            }

            if coalescer.is_full() {
                let batch = coalescer.finish()?.expect("coalescer was full");
                let num_rows = batch.num_rows();
                let analysis_result = analysis.finalize().await?;
                let props = build_row_group_properties(
                    &schema,
                    &base_props,
                    &config,
                    &resolved,
                    &analysis_result,
                    num_rows,
                )?;
                let work = RowGroupWork {
                    batch,
                    props: Arc::new(props),
                    parquet_schema: Arc::clone(&parquet_schema),
                    schema: Arc::clone(&schema),
                };
                if encoding_tx.send(work).await.is_err() {
                    return Ok(());
                }
                coalescer = HardLimitBatchCoalescer::new(&schema, max_rows);
                analysis = RowGroupAnalysisState::try_new(&schema, &columns_to_analyze)?;
            }
        }
    }

    if let Some(batch) = coalescer.finish()? {
        let num_rows = batch.num_rows();
        let analysis_result = analysis.finalize().await?;
        let props = build_row_group_properties(
            &schema,
            &base_props,
            &config,
            &resolved,
            &analysis_result,
            num_rows,
        )?;
        let work = RowGroupWork {
            batch,
            props: Arc::new(props),
            parquet_schema: Arc::clone(&parquet_schema),
            schema: Arc::clone(&schema),
        };
        // channel close is graceful shutdown
        let _ = encoding_tx.send(work).await;
    }

    Ok(())
}

async fn encoder_coordinator(
    mut work_rx: mpsc::Receiver<RowGroupWork>,
    writing_tx: mpsc::Sender<EncodedRowGroup>,
    cpu_handle: Handle,
    max_concurrent: usize,
) -> Result<()> {
    let mut pending: FuturesOrdered<_> = FuturesOrdered::new();
    let mut input_done = false;

    loop {
        tokio::select! {
            // receive work if under limit and input not exhausted
            work = work_rx.recv(), if !input_done && pending.len() < max_concurrent => {
                match work {
                    Some(work) => {
                        let handle = cpu_handle.clone();
                        pending.push_back(async move {
                            encode_row_group(work, &handle).await
                        });
                    }
                    None => {
                        input_done = true;
                    }
                }
            }

            // yield completed tasks (in submission order)
            Some(result) = pending.next(), if !pending.is_empty() => {
                let encoded = result.context("encoding failed")?;
                writing_tx
                    .send(encoded)
                    .await
                    .map_err(|_| anyhow!("writing channel closed"))?;
            }

            else => break,
        }
    }

    Ok(())
}

async fn writer_task(
    mut encoded_rx: mpsc::Receiver<EncodedRowGroup>,
    file_writer: SerializedFileWriter<BufWriter<File>>,
    io_handle: Handle,
    total_rows: Arc<AtomicU64>,
) -> Result<()> {
    let mut fw = file_writer;

    while let Some(rg) = encoded_rx.recv().await {
        total_rows.fetch_add(rg.num_rows as u64, Ordering::Relaxed);
        fw = write_row_group(&io_handle, fw, rg).await?;
    }

    io_handle
        .spawn(async move { fw.close().map_err(|e| anyhow!(e)) })
        .await
        .context("file close task failed")??;

    Ok(())
}

async fn write_row_group(
    io_handle: &Handle,
    fw: SerializedFileWriter<BufWriter<File>>,
    rg: EncodedRowGroup,
) -> Result<SerializedFileWriter<BufWriter<File>>> {
    io_handle
        .spawn(async move {
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

async fn encode_row_group(work: RowGroupWork, cpu_handle: &Handle) -> Result<EncodedRowGroup> {
    let num_rows = work.batch.num_rows();

    #[allow(deprecated)]
    let column_writers = get_column_writers(&work.parquet_schema, &work.props, &work.schema)?;
    let num_columns = column_writers.len();

    let mut leaves_per_column: Vec<Vec<_>> = (0..num_columns).map(|_| Vec::new()).collect();
    let mut col_idx = 0;
    for (field, column) in work.schema.fields().iter().zip(work.batch.columns()) {
        let leaves = compute_leaves(field.as_ref(), column)?;
        for leaf in leaves {
            leaves_per_column[col_idx].push(leaf);
            col_idx += 1;
        }
    }

    // JoinSet aborts all tasks on drop (proper cancellation)
    let mut col_tasks = JoinSet::new();
    for (col_idx, (writer, leaves)) in column_writers
        .into_iter()
        .zip(leaves_per_column)
        .enumerate()
    {
        col_tasks.spawn_on(
            async move {
                let mut writer = writer;
                for leaf in leaves {
                    writer.write(&leaf)?;
                }
                let chunk = writer.close().map_err(|e| anyhow!(e))?;
                Ok::<_, anyhow::Error>((col_idx, chunk))
            },
            cpu_handle,
        );
    }

    let mut indexed_chunks = Vec::with_capacity(num_columns);
    while let Some(result) = col_tasks.join_next().await {
        indexed_chunks.push(result.context("column encode task failed")??);
    }

    indexed_chunks.sort_by_key(|(idx, _)| *idx);
    let chunks = indexed_chunks.into_iter().map(|(_, chunk)| chunk).collect();

    Ok(EncodedRowGroup { chunks, num_rows })
}
