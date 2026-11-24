//! Pipeline tasks for Parquet output.
//!
//! ## Pipeline Architecture
//!
//! ```text
//! Pipeline scope         Encoding runtime       Writing runtime
//!      │                        │                       │
//!      ├─► ingestion task      │                       │
//!      │        │               │                       │
//!      ├─► encoder coordinator ├─► encoding tasks       │
//!      │        │               │                       │
//!      └─► writer task ────────────────────────────────►│
//!                                                       └─► upload bridge
//! ```
//!
//! ## Stages
//!
//! 1. **Source**: RecordBatches flow into the ingestion channel.
//!
//! 2. **Ingestion** (1 task): Gathers batches into row groups. A row group
//!    is the batches it was gathered from, cut where it ends, and not a copy
//!    of them: one task copying every row was all that the encoders behind
//!    it could be fed by. Copying 100M rows of 13 columns from one Parquet
//!    file to another, on 24 threads, took 8.9 s with that copy and takes
//!    7.5 s without it. Only batches under [`MIN_PIECE_ROWS`] rows are still
//!    copied, together, since each piece of a row group costs a call per
//!    column to encode.
//!
//! 3. **Encoder Coordinator** (1 task): Uses `FuturesOrdered` to spawn tasks on
//!    the encoding runtime, up to `max_row_group_concurrency`. Results remain
//!    ordered. A row group's task first analyzes the columns whose encoding
//!    depends on their cardinality, a task per column, then encodes it, also
//!    a task per column.
//!
//! 4. **Writing** (1 task): Receives encoded row groups in order and feeds the
//!    object-upload bridge.
//!
//! Every stage and nested task is spawned through one pipeline scope. The scope
//! applies the operation's cancellation token to stages, tracks work on the
//! dedicated runtimes, and waits for that work before the pipeline task releases
//! those runtimes.

use std::collections::HashMap;
use std::io::BufWriter;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use arrow::array::{Array, RecordBatch, RecordBatchOptions};
use arrow::compute::concat;
use arrow::datatypes::{Field, Schema, SchemaRef};
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
use silk_chiffon_storage::BlockingObjectUploadWriter;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use super::analysis::RowGroupAnalyzer;
use super::config::{ColumnPolicies, PipelineConfig};
use super::encoding::build_row_group_properties;
use super::{PipelineCancelled, PipelineTaskScope};
use crate::output::OutputRuntimes;

pub(super) struct PipelineSetup {
    pub(super) writer: BlockingObjectUploadWriter,
    pub(super) schema: SchemaRef,
    pub(super) base_props: WriterProperties,
    pub(super) runtimes: Arc<OutputRuntimes>,
    pub(super) config: PipelineConfig,
    pub(super) ingestion_rx: mpsc::Receiver<RecordBatch>,
    pub(super) scope: PipelineTaskScope,
}

pub(crate) struct RowGroupWork {
    /// The row group's rows, in order, as the batches they arrived in.
    pub batches: Vec<RecordBatch>,
    pub num_rows: usize,
    pub encoding: Arc<RowGroupEncoding>,
}

/// What every row group of a file is encoded with.
pub(crate) struct RowGroupEncoding {
    schema: SchemaRef,
    parquet_schema: SchemaDescPtr,
    base_props: WriterPropertiesPtr,
    config: PipelineConfig,
    policies: ColumnPolicies,
    /// `None` when no column's encoding depends on its cardinality.
    analyzer: Option<RowGroupAnalyzer>,
}

impl RowGroupEncoding {
    fn new(
        schema: SchemaRef,
        parquet_schema: SchemaDescPtr,
        base_props: WriterPropertiesPtr,
        config: PipelineConfig,
    ) -> Self {
        let policies = ColumnPolicies::resolve(&schema, &config);
        let columns = policies.columns_needing_analysis();
        Self {
            analyzer: (!columns.is_empty()).then(|| RowGroupAnalyzer::new(columns)),
            schema,
            parquet_schema,
            base_props,
            config,
            policies,
        }
    }
}

pub(crate) struct EncodedRowGroup {
    pub chunks: Vec<ArrowColumnChunk>,
    pub num_rows: usize,
}

pub(crate) fn create_arrow_writer(
    file: BufWriter<BlockingObjectUploadWriter>,
    schema: &SchemaRef,
    base_props: WriterProperties,
    skip_arrow_metadata: bool,
) -> Result<(
    SerializedFileWriter<BufWriter<BlockingObjectUploadWriter>>,
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

pub(super) async fn run_pipeline(setup: PipelineSetup) -> Result<u64> {
    let PipelineSetup {
        writer,
        schema,
        base_props,
        runtimes,
        config,
        ingestion_rx,
        scope,
    } = setup;
    let file = BufWriter::with_capacity(config.buffer_size, writer);

    let (encoding_tx, encoding_rx) = mpsc::channel::<RowGroupWork>(config.encoding_queue_size);
    let (writing_tx, writing_rx) = mpsc::channel::<EncodedRowGroup>(config.writing_queue_size);

    let (file_writer, parquet_schema, base_props) =
        create_arrow_writer(file, &schema, base_props, config.skip_arrow_metadata)?;

    let total_rows = Arc::new(AtomicU64::new(0));
    let mut pipeline_tasks: JoinSet<Result<()>> = JoinSet::new();
    let max_row_group_concurrency = config.max_row_group_concurrency;
    let encoding = Arc::new(RowGroupEncoding::new(
        schema,
        parquet_schema,
        base_props,
        config,
    ));

    scope.spawn_stage(
        &mut pipeline_tasks,
        ingestion_task(ingestion_rx, encoding_tx, encoding),
    );

    let encoding_handle = runtimes.encoding().clone();
    scope.spawn_stage(
        &mut pipeline_tasks,
        encoder_coordinator(
            encoding_rx,
            writing_tx,
            encoding_handle,
            max_row_group_concurrency,
            scope.clone(),
        ),
    );

    let writing_handle = runtimes.writing().clone();
    scope.spawn_stage(
        &mut pipeline_tasks,
        writer_task(
            writing_rx,
            file_writer,
            writing_handle,
            Arc::clone(&total_rows),
            scope.clone(),
        ),
    );

    settle_pipeline_tasks(&scope, &mut pipeline_tasks).await?;
    Ok(total_rows.load(Ordering::SeqCst))
}

pub(super) async fn settle_pipeline_tasks(
    scope: &PipelineTaskScope,
    pipeline_tasks: &mut JoinSet<Result<()>>,
) -> Result<()> {
    let mut errors = Vec::new();
    while let Some(result) = pipeline_tasks.join_next().await {
        match result {
            Err(e) if e.is_panic() => {
                errors.push(anyhow!("task panicked: {e}"));
                scope.cancel();
                pipeline_tasks.abort_all();
            }
            Err(e) if e.is_cancelled() => continue,
            Err(e) => {
                // unexpected join error
                errors.push(anyhow!("task join error: {e}"));
                scope.cancel();
                pipeline_tasks.abort_all();
            }
            Ok(Err(e)) if e.is::<PipelineCancelled>() => continue,
            Ok(Err(e)) => {
                if errors.is_empty() {
                    scope.cancel();
                    pipeline_tasks.abort_all();
                }
                errors.push(e);
            }
            Ok(Ok(())) => continue,
        }
    }

    let result = match errors.len() {
        0 if scope.is_cancelled() => Err(PipelineCancelled.into()),
        0 => Ok(()),
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
    };
    scope.wait().await;
    result
}

/// Batches with fewer rows than this are copied together before they join a
/// row group. Every piece of a row group costs the encoders a call per column,
/// and holds an array per column, and partitioned output can hand a file
/// batches of a row or two.
const MIN_PIECE_ROWS: usize = 1024;

/// Copies small batches into one piece. Not `concat_batches` under the sink's
/// schema: the sink accepts batches whose nested fields are non-nullable where
/// its own are nullable, and `RecordBatch::try_new` rejects those columns under
/// its schema. The encoders read a piece's columns, never its schema.
fn copy_together(schema: &SchemaRef, small: &[RecordBatch]) -> Result<RecordBatch> {
    let columns = (0..schema.fields().len())
        .map(|index| {
            let arrays: Vec<&dyn Array> = small
                .iter()
                .map(|batch| batch.column(index).as_ref())
                .collect();
            concat(&arrays)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(&columns)
        .map(|(field, column)| Field::new(field.name(), column.data_type().clone(), true))
        .collect();
    let options = RecordBatchOptions::new()
        .with_row_count(Some(small.iter().map(RecordBatch::num_rows).sum()));
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        columns,
        &options,
    )?)
}

async fn ingestion_task(
    mut ingestion_rx: mpsc::Receiver<RecordBatch>,
    encoding_tx: mpsc::Sender<RowGroupWork>,
    encoding: Arc<RowGroupEncoding>,
) -> Result<()> {
    let row_group_rows = encoding.config.max_row_group_size.max(1);
    let mut batches = Vec::new();
    let mut num_rows = 0;
    let mut small = Vec::new();
    let mut small_rows = 0;

    loop {
        let received = ingestion_rx.recv().await;
        let mut pieces = Vec::with_capacity(2);
        match received {
            // holding an empty batch until the next copy would keep its
            // arrays alive for nothing
            Some(batch) if batch.num_rows() == 0 => continue,
            Some(batch) if batch.num_rows() < MIN_PIECE_ROWS => {
                small_rows += batch.num_rows();
                small.push(batch);
                // never past the row group's end, so that a row group goes to
                // the encoders as soon as it is full
                if small_rows >= MIN_PIECE_ROWS.min(row_group_rows - num_rows) {
                    pieces.push(copy_together(&encoding.schema, &small)?);
                    (small, small_rows) = (Vec::new(), 0);
                }
            }
            received => {
                // the small batches before it go first, to keep the rows in order
                if !small.is_empty() {
                    pieces.push(copy_together(&encoding.schema, &small)?);
                    (small, small_rows) = (Vec::new(), 0);
                }
                pieces.extend(received);
            }
        }
        let finished = pieces.is_empty() && small.is_empty();
        for mut batch in pieces {
            // a batch that reaches past the row group's end is cut there,
            // which copies nothing
            while num_rows + batch.num_rows() >= row_group_rows {
                let fitting = row_group_rows - num_rows;
                batches.push(batch.slice(0, fitting));
                batch = batch.slice(fitting, batch.num_rows() - fitting);
                let work = RowGroupWork {
                    batches: std::mem::take(&mut batches),
                    num_rows: row_group_rows,
                    encoding: Arc::clone(&encoding),
                };
                num_rows = 0;
                if encoding_tx.send(work).await.is_err() {
                    return Ok(());
                }
            }
            if batch.num_rows() > 0 {
                num_rows += batch.num_rows();
                batches.push(batch);
            }
        }
        if finished {
            break;
        }
    }

    if num_rows > 0 {
        let work = RowGroupWork {
            batches,
            num_rows,
            encoding,
        };
        // channel close is graceful shutdown
        let _ = encoding_tx.send(work).await;
    }

    Ok(())
}

async fn encoder_coordinator(
    mut work_rx: mpsc::Receiver<RowGroupWork>,
    writing_tx: mpsc::Sender<EncodedRowGroup>,
    encoding_handle: Handle,
    max_concurrent: usize,
    scope: PipelineTaskScope,
) -> Result<()> {
    let mut pending: FuturesOrdered<_> = FuturesOrdered::new();
    let mut input_done = false;

    loop {
        tokio::select! {
            // receive work if under limit and input not exhausted
            work = work_rx.recv(), if !input_done && pending.len() < max_concurrent => {
                match work {
                    Some(work) => {
                        let handle = encoding_handle.clone();
                        let nested_scope = scope.clone();
                        let task = scope.spawn_on(
                            async move {
                                encode_row_group(work, handle, nested_scope).await
                            },
                            &encoding_handle,
                        );
                        pending.push_back(task);
                    }
                    None => {
                        input_done = true;
                    }
                }
            }

            // yield completed tasks (in submission order)
            Some(result) = pending.next(), if !pending.is_empty() => {
                let encoded = result.context("encoding failed")??;
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
    file_writer: SerializedFileWriter<BufWriter<BlockingObjectUploadWriter>>,
    writing_handle: Handle,
    total_rows: Arc<AtomicU64>,
    scope: PipelineTaskScope,
) -> Result<()> {
    let mut fw = file_writer;

    while let Some(rg) = encoded_rx.recv().await {
        total_rows.fetch_add(rg.num_rows as u64, Ordering::Relaxed);
        fw = write_row_group(&writing_handle, fw, rg, &scope).await?;
    }

    scope
        .spawn_on(
            async move { fw.close().map_err(|e| anyhow!(e)) },
            &writing_handle,
        )
        .await
        .context("file close task failed")??;

    Ok(())
}

async fn write_row_group(
    writing_handle: &Handle,
    fw: SerializedFileWriter<BufWriter<BlockingObjectUploadWriter>>,
    rg: EncodedRowGroup,
    scope: &PipelineTaskScope,
) -> Result<SerializedFileWriter<BufWriter<BlockingObjectUploadWriter>>> {
    scope
        .spawn_on(
            async move {
                let mut fw = fw;
                let mut row_group = fw.next_row_group()?;
                for chunk in rg.chunks {
                    chunk.append_to_row_group(&mut row_group)?;
                }
                row_group.close()?;
                Ok(fw)
            },
            writing_handle,
        )
        .await
        .context("row group write task failed")?
}

async fn encode_row_group(
    work: RowGroupWork,
    encoding_handle: Handle,
    scope: PipelineTaskScope,
) -> Result<EncodedRowGroup> {
    let RowGroupWork {
        batches,
        num_rows,
        encoding,
    } = work;

    let analysis = match &encoding.analyzer {
        Some(analyzer) => analyzer.analyze(&batches, &encoding_handle, &scope).await?,
        None => HashMap::new(),
    };
    let props = Arc::new(build_row_group_properties(
        &encoding.schema,
        &encoding.base_props,
        &encoding.config,
        &encoding.policies,
        &analysis,
        num_rows,
    )?);

    #[allow(deprecated)]
    let column_writers = get_column_writers(&encoding.parquet_schema, &props, &encoding.schema)?;
    let num_columns = column_writers.len();

    let mut leaves_per_column: Vec<Vec<_>> = (0..num_columns).map(|_| Vec::new()).collect();
    for batch in &batches {
        let mut col_idx = 0;
        for (field, column) in encoding.schema.fields().iter().zip(batch.columns()) {
            let leaves = compute_leaves(field.as_ref(), column)?;
            for leaf in leaves {
                leaves_per_column[col_idx].push(leaf);
                col_idx += 1;
            }
        }
    }
    // the leaves hold what they need of the batches
    drop(batches);

    let mut col_tasks = JoinSet::new();
    for (col_idx, (writer, leaves)) in column_writers
        .into_iter()
        .zip(leaves_per_column)
        .enumerate()
    {
        scope.spawn_in_on(
            &mut col_tasks,
            async move {
                let mut writer = writer;
                for leaf in leaves {
                    writer.write(&leaf)?;
                }
                let chunk = writer.close().map_err(|e| anyhow!(e))?;
                Ok::<_, anyhow::Error>((col_idx, chunk))
            },
            &encoding_handle,
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

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use futures::future::pending;
    use tokio::sync::{Barrier, oneshot};
    use tokio_util::sync::CancellationToken;

    use super::*;

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct NotifyOnDrop(Option<oneshot::Sender<()>>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn external_cancellation_is_reported_as_an_error() {
        let cancellation = CancellationToken::new();
        let scope = PipelineTaskScope::new(cancellation.clone());
        let mut tasks = JoinSet::new();
        scope.spawn_stage(&mut tasks, pending::<Result<()>>());

        cancellation.cancel();
        let error = settle_pipeline_tasks(&scope, &mut tasks).await.unwrap_err();

        assert_eq!(error.to_string(), "Parquet pipeline was cancelled");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stage_failure_cancels_siblings_and_awaits_nested_tasks() {
        let scope = PipelineTaskScope::new(CancellationToken::new());
        let ready = Arc::new(Barrier::new(3));
        let sibling_dropped = Arc::new(AtomicBool::new(false));
        let nested_dropped = Arc::new(AtomicBool::new(false));
        let mut tasks = JoinSet::new();

        scope.spawn_stage(&mut tasks, {
            let ready = Arc::clone(&ready);
            async move {
                ready.wait().await;
                anyhow::bail!("controlled stage failure")
            }
        });
        scope.spawn_stage(&mut tasks, {
            let nested_scope = scope.clone();
            let ready = Arc::clone(&ready);
            let sibling_dropped = Arc::clone(&sibling_dropped);
            let nested_dropped = Arc::clone(&nested_dropped);
            async move {
                let _sibling = DropSignal(sibling_dropped);
                let _nested = nested_scope.spawn(async move {
                    let _nested = DropSignal(nested_dropped);
                    pending::<()>().await;
                });
                ready.wait().await;
                pending::<Result<()>>().await
            }
        });

        ready.wait().await;

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            settle_pipeline_tasks(&scope, &mut tasks),
        )
        .await
        .expect("pipeline settlement timed out")
        .unwrap_err();

        assert!(error.to_string().contains("controlled stage failure"));
        assert!(sibling_dropped.load(Ordering::SeqCst));
        assert!(nested_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stage_panic_cancels_and_awaits_its_sibling() {
        let scope = PipelineTaskScope::new(CancellationToken::new());
        let ready = Arc::new(Barrier::new(3));
        let sibling_dropped = Arc::new(AtomicBool::new(false));
        let (panic_finished_tx, panic_finished_rx) = oneshot::channel();
        let mut tasks = JoinSet::new();

        scope.spawn_stage(&mut tasks, {
            let ready = Arc::clone(&ready);
            async move {
                let _panic_finished = NotifyOnDrop(Some(panic_finished_tx));
                ready.wait().await;
                panic!("controlled stage panic")
            }
        });
        scope.spawn_stage(&mut tasks, {
            let ready = Arc::clone(&ready);
            let sibling_dropped = Arc::clone(&sibling_dropped);
            async move {
                let _sibling = DropSignal(sibling_dropped);
                ready.wait().await;
                pending::<Result<()>>().await
            }
        });

        ready.wait().await;
        panic_finished_rx
            .await
            .expect("panic stage dropped without notification");

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            settle_pipeline_tasks(&scope, &mut tasks),
        )
        .await
        .expect("pipeline settlement timed out")
        .unwrap_err();

        assert!(error.to_string().contains("task panicked"));
        assert!(sibling_dropped.load(Ordering::SeqCst));
    }

    /// Gathers batches of `sizes` rows, numbered in order, into row groups of
    /// `row_group_rows`, and returns each row group's pieces' row counts and
    /// its values.
    async fn gather(sizes: &[usize], row_group_rows: usize) -> Vec<(Vec<usize>, Vec<i32>)> {
        use arrow::array::{AsArray, Int32Array};
        use arrow::datatypes::{DataType, Field, Int32Type, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let parquet_schema = Arc::new(ArrowSchemaConverter::new().convert(&schema).unwrap());
        let config = PipelineConfig {
            max_row_group_size: row_group_rows,
            ..PipelineConfig::default()
        };
        let encoding = Arc::new(RowGroupEncoding::new(
            Arc::clone(&schema),
            parquet_schema,
            Arc::new(WriterProperties::builder().build()),
            config,
        ));
        let (batches_tx, batches_rx) = mpsc::channel(sizes.len().max(1));
        let (groups_tx, mut groups_rx) = mpsc::channel(sizes.len().max(1) + 1);
        let mut next = 0;
        for rows in sizes {
            let rows = i32::try_from(*rows).unwrap();
            let values: Int32Array = (next..next + rows).collect();
            next += rows;
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).unwrap();
            batches_tx.send(batch).await.unwrap();
        }
        drop(batches_tx);
        ingestion_task(batches_rx, groups_tx, encoding)
            .await
            .unwrap();
        let mut groups = Vec::new();
        while let Some(work) = groups_rx.recv().await {
            let pieces = work.batches.iter().map(RecordBatch::num_rows).collect();
            let values: Vec<i32> = work
                .batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_primitive::<Int32Type>()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(values.len(), work.num_rows);
            groups.push((pieces, values));
        }
        groups
    }

    #[tokio::test]
    async fn small_batches_are_copied_together_and_keep_their_order() {
        let mut sizes = vec![1; 3000];
        // a large batch between small ones goes after the small ones before it
        sizes.insert(1500, 2000);
        let groups = gather(&sizes, 4096).await;
        let rows: Vec<usize> = groups.iter().map(|(_, values)| values.len()).collect();
        assert_eq!(rows, [4096, 904]);
        let values: Vec<i32> = groups
            .into_iter()
            .flat_map(|(pieces, values)| {
                assert!(pieces.len() <= 5, "{pieces:?}");
                values
            })
            .collect();
        assert_eq!(values, (0..5000).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn small_batches_may_narrow_nested_nullability() {
        use arrow::array::{AsArray, Int32Builder, ListBuilder};
        use arrow::datatypes::{DataType, Field, Schema};

        let list = |nullable| DataType::new_list(DataType::Int32, nullable);
        let schema = Arc::new(Schema::new(vec![Field::new("l", list(true), true)]));
        let parquet_schema = Arc::new(ArrowSchemaConverter::new().convert(&schema).unwrap());
        let encoding = Arc::new(RowGroupEncoding::new(
            Arc::clone(&schema),
            parquet_schema,
            Arc::new(WriterProperties::builder().build()),
            PipelineConfig::default(),
        ));
        let narrow = Arc::new(Schema::new(vec![Field::new("l", list(false), true)]));
        let (batches_tx, batches_rx) = mpsc::channel(2);
        let (groups_tx, mut groups_rx) = mpsc::channel(2);
        for rows in [2, 3] {
            let mut builder = ListBuilder::new(Int32Builder::new())
                .with_field(Field::new_list_field(DataType::Int32, false));
            for n in 0..rows {
                builder.append_value([Some(n)]);
            }
            let batch = RecordBatch::try_new(Arc::clone(&narrow), vec![Arc::new(builder.finish())])
                .unwrap();
            batches_tx.send(batch).await.unwrap();
        }
        drop(batches_tx);
        ingestion_task(batches_rx, groups_tx, encoding)
            .await
            .unwrap();

        let work = groups_rx.recv().await.unwrap();
        assert!(groups_rx.recv().await.is_none());
        let rows: usize = work.batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!((rows, work.num_rows), (5, 5));
        assert_eq!(work.batches[0].column(0).as_list::<i32>().len(), 5);
    }

    #[tokio::test]
    async fn a_row_group_is_its_batches_cut_where_it_ends() {
        use arrow::array::{AsArray, Int32Array};
        use arrow::datatypes::{DataType, Field, Int32Type, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let parquet_schema = Arc::new(ArrowSchemaConverter::new().convert(&schema).unwrap());
        let config = PipelineConfig {
            max_row_group_size: 4,
            ..PipelineConfig::default()
        };
        let encoding = Arc::new(RowGroupEncoding::new(
            Arc::clone(&schema),
            parquet_schema,
            Arc::new(WriterProperties::builder().build()),
            config,
        ));
        let (batches_tx, batches_rx) = mpsc::channel(8);
        let (groups_tx, mut groups_rx) = mpsc::channel(8);
        let mut next = 0;
        // the second batch ends one row group and fills the next exactly
        for rows in [3, 5, 4, 1] {
            let values: Int32Array = (next..next + rows).collect();
            next += rows;
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).unwrap();
            batches_tx.send(batch).await.unwrap();
        }
        drop(batches_tx);
        ingestion_task(batches_rx, groups_tx, encoding)
            .await
            .unwrap();

        let mut groups = Vec::new();
        while let Some(work) = groups_rx.recv().await {
            let values: Vec<i32> = work
                .batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_primitive::<Int32Type>()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(values.len(), work.num_rows);
            groups.push(values);
        }
        assert_eq!(
            groups,
            [
                vec![0, 1, 2, 3],
                vec![4, 5, 6, 7],
                vec![8, 9, 10, 11],
                vec![12]
            ]
        );
    }
}
