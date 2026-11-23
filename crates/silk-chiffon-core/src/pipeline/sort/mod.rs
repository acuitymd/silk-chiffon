//! A sort that runs in parallel end to end and decides for itself when to
//! spill.
//!
//! DataFusion sorts each input partition on its own and then merges the
//! partitions on one thread. Its sorters also depend on the memory pool
//! granting more memory at the moment they spill, and fail the query when it
//! does not. This operator replaces both parts.
//!
//! - **Key ranges.** What is buffered is sampled and split into key ranges,
//!   and the ranges are sorted independently: see [`ranges`]. Their
//!   concatenation is the output, so nothing is merged in memory.
//! - **Keys as integers.** A row's sort key is its Arrow row format, stored as
//!   native 64-bit words, so a comparison is a few integer comparisons. A
//!   record holds its key whole when it can: keys of variable width set how
//!   wide records are when the first batch arrives.
//! - **Rows beside their keys.** Rows of a few narrow fixed-width columns
//!   are held whole in the records that are sorted. Other rows are kept in
//!   row format beside the records, one run of bytes each, and decoded when
//!   they are sent.
//! - **One reservation.** The sort asks the pool for memory only to buffer
//!   input. A refusal means "write what is buffered to disk", never an error.
//!   Sorting, spilling, and merging use memory the operator already holds or
//!   sized up front.
//! - **Spilling.** Rows held in row format are written to disk as they are,
//!   in blocks, and a flush keeps the first key of each block it wrote. The
//!   merge of the flushes is cut into slices from those, and the slices merge
//!   in parallel however the rows arrived.
//!
//! # Rows the row format cannot hold
//!
//! Two of those points need every column of a row in Arrow's row format:
//! keeping rows beside their keys, and spilling them as they are. Some
//! dictionary columns cannot be kept that way, because the row format cannot
//! encode them or because Arrow cannot rebuild them from it. `Payload::try_new`
//! in [`keys`] decides, and its docs list the types.
//!
//! A sort with such a column still sorts key ranges in parallel, and still
//! spills when the pool refuses memory. Three things change, and each makes
//! the sort slower:
//!
//! - **Rows are kept as the batches they arrived in.** A record, the
//!   fixed-size entry that the sort compares and moves, names its row by
//!   batch and row number. Output is gathered from the batches, which reads
//!   a buffer per column for every row, where a row in row format is one run
//!   of bytes.
//! - **A record holds 16 bytes of a key of variable width.** A comparison
//!   that those bytes do not decide reads both whole keys from row format.
//!   When rows are in row format, records are made wide enough to hold such
//!   keys whole, up to 247 bytes.
//! - **A flush is written as Arrow batches.** It is a file per group of the
//!   key range, and DataFusion merges each group's files. A group's merge is
//!   mostly one thread's work, and a small budget merges one group at a
//!   time: see [`spill`].
//!
//! A plan says how its sort holds rows, after `parallelism`: `rows=records`
//! for whole rows in records, `rows=row-format`, or `rows=batches`.
//!
//! A sort key that the row format cannot encode is a different case, and
//! never reaches this operator: the planner gives that sort to DataFusion's.
//! See [`ParallelSortExec::supports`].

mod keys;
mod ranges;
mod record;
mod run;
mod spill;
mod workers;

use std::{
    collections::VecDeque,
    fmt,
    ops::Range,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use arrow::{array::RecordBatch, datatypes::SchemaRef, row::Rows};
use datafusion::{
    common::{runtime::JoinSet, tree_node::TreeNodeRecursion},
    config::SpillCompression,
    error::{DataFusionError, Result},
    execution::{
        SendableRecordBatchStream, TaskContext,
        memory_pool::{MemoryConsumer, MemoryLimit, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
    physical_expr::{LexOrdering, PhysicalExpr},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
        PlanProperties,
        execution_plan::{
            Boundedness, ChildrenPropertiesMode, EmissionType, ReplaceChildrenOptions,
        },
        metrics::{
            Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet, SpillMetrics, Time,
        },
        stream::RecordBatchReceiverStream,
    },
};
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::mpsc::{self, Receiver, Sender};

use keys::{Contents, RowEncoder};
use ranges::Ranges;
use record::Words;
use run::{Buffered, Grant, Lane, Splitters};
use spill::{BatchFlush, BlockFlush, Segment, Slices, Spill};
use workers::Workers;

/// The operator's name in plans.
const OPERATOR: &str = "ParallelSortExec";

/// The rows that one sort thread's scratch memory is sized for, at two
/// records a row. A thread sorts a range of buffered rows in that memory, so
/// this bounds what the sort holds beyond the rows it has buffered.
const MAX_SCRATCH_ROWS: usize = 122_880;
const MIN_SCRATCH_ROWS: usize = 1024;

/// Ranges per sort thread. More ranges than threads keeps the threads evenly
/// loaded when the splitters divide the rows unevenly.
const RANGES_PER_THREAD: usize = 16;

/// The share of the pool held back for sorting what is buffered: one part in
/// this many. A flush is what frees memory, so its sort cannot ask for
/// memory when it starts.
const SCRATCH_POOL_SHARE: usize = 32;

/// Rows a spill file is worth having for.
const MIN_ROWS_PER_FILE: usize = 16 * 1024;

/// Rows in one slice of the merge of flushes of records, for each row that a
/// thread's scratch memory is sized for. A slice of that merge reads into a
/// block of each flush that the slice before it also read, so larger slices
/// read fewer blocks twice: with sixteen, a merge of 25 flushes reads a tenth
/// of the blocks twice.
const MERGE_SLICE_SCRATCH_ROWS: usize = 16;

/// Blocks of rows in row format that make one slice of the merge of flushes,
/// which is as many blocks as a slice of the merge of records has.
const MERGE_SLICE_ROW_BLOCKS: usize = 256;

/// The share of one thread's scratch memory that a block of rows in row
/// format fills: one part in this many. A merge holds a block of every flush,
/// so sixteen flushes merge in the memory a thread sorts a range in. Blocks
/// are no larger than a read is worth.
const ROW_BLOCK_SCRATCH_SHARE: usize = 32;
const MIN_ROW_BLOCK_BYTES: usize = 1024;
const MAX_ROW_BLOCK_BYTES: usize = 256 * 1024;

/// Spill files kept at once where the process's open-file limit cannot be
/// read: a quarter of 1024, the common default. Each spill file is an open
/// file descriptor for as long as it exists.
#[cfg(not(unix))]
const MIN_OPEN_FILES: usize = 256;

/// The most spill files kept at once, however many the process may open.
const MAX_OPEN_FILES: usize = 4096;

/// The spill files to keep at once: a quarter of the files this process may
/// have open, which leaves the rest to its inputs and outputs.
///
/// Past this many, the flushes on disk are merged into one, which reads and
/// writes everything spilled so far once more. Sorting 100M rows of 13
/// columns in 1 GiB made 390 files, and merging them early to stay under 256
/// was a quarter of the run.
fn open_file_limit() -> usize {
    #[cfg(unix)]
    let allowed = rustix::process::getrlimit(rustix::process::Resource::Nofile)
        .current
        .map_or(MAX_OPEN_FILES, |files| {
            usize::try_from(files / 4).unwrap_or(MAX_OPEN_FILES)
        });
    #[cfg(not(unix))]
    let allowed = MIN_OPEN_FILES;
    // a merge reads two at least; a lower limit than that cannot be helped
    allowed.clamp(2, MAX_OPEN_FILES)
}

/// Settings for [`ParallelSortExec`].
#[derive(Clone, Copy, Debug)]
pub struct ParallelSortOptions {
    /// Threads that sort and merge.
    pub parallelism: usize,
    /// The least buffered before a flush, capped at half the pool. A pool that
    /// refuses memory below this is overdrawn instead, because flushing a few
    /// rows at a time makes no progress.
    pub min_run_bytes: usize,
    /// Spill files kept at once, and read at once across every merge in
    /// flight.
    pub max_open_files: usize,
}

impl ParallelSortOptions {
    /// Options for `parallelism` threads with the default minimum flush.
    pub fn new(parallelism: usize) -> Self {
        Self {
            parallelism: parallelism.max(1),
            min_run_bytes: 8 * 1024 * 1024,
            max_open_files: open_file_limit(),
        }
    }
}

/// Sorts every input partition into one sorted output partition.
pub struct ParallelSortExec {
    input: Arc<dyn ExecutionPlan>,
    ordering: LexOrdering,
    encoder: Arc<RowEncoder>,
    options: ParallelSortOptions,
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
}

impl ParallelSortExec {
    /// Whether this operator can sort `input` by `ordering`. It needs every sort
    /// column to have a row-format encoding.
    pub fn supports(schema: &SchemaRef, ordering: &LexOrdering) -> bool {
        keys::supports(ordering, schema)
    }

    /// Creates the sort. Check [`Self::supports`] first.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        ordering: LexOrdering,
        options: ParallelSortOptions,
    ) -> Result<Self> {
        let mut equivalence = input.equivalence_properties().clone();
        // the input's partitions become one, where a value constant within
        // each partition need not be constant
        equivalence.clear_per_partition_constants();
        equivalence.reorder(ordering.clone())?;
        let boundedness = match input.boundedness() {
            Boundedness::Unbounded { .. } => Boundedness::Unbounded {
                requires_infinite_memory: true,
            },
            bounded => bounded,
        };
        let properties = PlanProperties::new(
            equivalence,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            boundedness,
        );
        Ok(Self {
            encoder: Arc::new(RowEncoder::try_new(&ordering, &input.schema())?),
            input,
            ordering,
            options,
            metrics: ExecutionPlanMetricsSet::new(),
            properties: Arc::new(properties),
        })
    }
}

impl fmt::Debug for ParallelSortExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct(OPERATOR)
            .field("ordering", &self.ordering)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ParallelSortExec {
    fn fmt_as(&self, _format: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        // which of the three ways the sort holds rows decides how fast it
        // is, and only the schema says which
        let rows = match (self.encoder.contents(), self.encoder.keeps_rows()) {
            (Contents::Row, _) => "records",
            (Contents::Source, true) => "row-format",
            (Contents::Source, false) => "batches",
        };
        write!(
            formatter,
            "{OPERATOR}: expr=[{}], parallelism={}, rows={rows}",
            self.ordering, self.options.parallelism
        )
    }
}

impl ExecutionPlan for ParallelSortExec {
    fn name(&self) -> &'static str {
        OPERATOR
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .pop()
            .filter(|_| children.is_empty())
            .ok_or_else(|| DataFusionError::Internal("the sort has one child".into()))?;
        Ok(Arc::new(Self::try_new(
            input,
            self.ordering.clone(),
            self.options,
        )?))
    }

    fn apply_expressions(
        &self,
        apply: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for sort in &self.ordering {
            if apply(&sort.expr)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "the sort has one output partition, asked for {partition}"
            )));
        }
        let sorter = Arc::new(Sorter::try_new(self, &context)?);
        let input = Arc::clone(&self.input);
        let mut builder = RecordBatchReceiverStream::builder(self.schema(), 2);
        let output = builder.tx();
        builder.spawn(async move { sorter.run(input, context, output).await });
        Ok(builder.build())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Where one sort spent its time, shown with the plan's other metrics.
struct SortMetrics {
    /// Per input partition: time waiting for the input's next batch. When this
    /// is most of the read phase, the input is what limits the sort.
    input_wait_time: Vec<Time>,
    /// Per input partition: time encoding and buffering batches, including
    /// waiting while a flush is written to disk.
    buffer_time: Vec<Time>,
    input_rows: Vec<Count>,
    /// Time spent sorting and writing flushes while input was held.
    flush_time: Time,
    /// Time putting buffered rows in order before they are written or sent:
    /// splitting them into ranges and sorting those.
    sort_time: Time,
    /// Time the sorted output waited for its consumer to take a batch.
    output_wait_time: Time,
    output_rows: Count,
    /// The most memory the sort held for rows buffered between flushes.
    peak_run_bytes: Gauge,
    /// The most ranges that buffered rows were split into.
    ranges: Gauge,
    /// Rows in the largest range. Far above rows / ranges means the splitters
    /// divided the rows unevenly.
    largest_range_rows: Gauge,
    /// Rows in ranges that had too many records to sort within the scratch
    /// memory, and were sorted in more.
    oversized_range_rows: Count,
    flushes: Count,
    /// Times the flushes on disk were merged into one to bound the open files.
    compactions: Count,
    /// Files that one flush is written as.
    flush_files: Gauge,
    /// Merges of what was spilled that ran at once for the output: slices of
    /// flushes in blocks, or groups of flushes of Arrow batches. At one, the
    /// whole output came through a single merge.
    merge_concurrency: Gauge,
    /// Flushes that one of those merges read at once.
    merge_fan_in: Gauge,
    /// Bytes in one of the records that are sorted.
    record_bytes: Gauge,
    /// Rows whose keys were too long for their records. Their ties are
    /// broken through the row format.
    cut_keys: Count,
}

impl SortMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, lanes: usize) -> Self {
        let time = |name: &'static str, partition| {
            MetricBuilder::new(metrics).subset_time(name, partition)
        };
        Self {
            input_wait_time: (0..lanes)
                .map(|lane| time("input_wait_time", lane))
                .collect(),
            buffer_time: (0..lanes).map(|lane| time("buffer_time", lane)).collect(),
            input_rows: (0..lanes)
                .map(|lane| MetricBuilder::new(metrics).counter("input_rows", lane))
                .collect(),
            flush_time: time("flush_time", 0),
            sort_time: time("sort_time", 0),
            output_wait_time: time("output_wait_time", 0),
            output_rows: MetricBuilder::new(metrics).output_rows(0),
            peak_run_bytes: MetricBuilder::new(metrics).gauge("peak_run_bytes", 0),
            ranges: MetricBuilder::new(metrics).gauge("ranges", 0),
            largest_range_rows: MetricBuilder::new(metrics).gauge("largest_range_rows", 0),
            oversized_range_rows: MetricBuilder::new(metrics).counter("oversized_range_rows", 0),
            flushes: MetricBuilder::new(metrics).counter("spilled_runs", 0),
            compactions: MetricBuilder::new(metrics).counter("spill_compactions", 0),
            flush_files: MetricBuilder::new(metrics).gauge("flush_files", 0),
            merge_concurrency: MetricBuilder::new(metrics).gauge("merge_concurrency", 0),
            merge_fan_in: MetricBuilder::new(metrics).gauge("merge_fan_in", 0),
            record_bytes: MetricBuilder::new(metrics).gauge("record_bytes", 0),
            cut_keys: MetricBuilder::new(metrics).counter("cut_keys", 0),
        }
    }

    fn record_buffered(&self, buffered: &Buffered) {
        self.peak_run_bytes.set_max(buffered.held());
        self.cut_keys.add(buffered.cut_keys);
    }
}

/// The pool reservation every part of one sort draws from.
struct Budget {
    reservation: MemoryReservation,
    /// The part of the reservation held for sorting ranges, from the first
    /// batch to the end.
    scratch: OnceLock<usize>,
    /// The part held for the indexes of flushes on disk, which are not rows
    /// buffered for the next flush.
    index: AtomicUsize,
}

impl Grant for Budget {
    fn grant(&self, bytes: usize) -> bool {
        self.reservation.try_grow(bytes).is_ok()
    }

    fn force(&self, bytes: usize) {
        self.reservation.grow(bytes);
    }
}

impl Budget {
    fn new(reservation: MemoryReservation) -> Self {
        Self {
            reservation,
            scratch: OnceLock::new(),
            index: AtomicUsize::new(0),
        }
    }

    /// Takes memory for the indexes of flushes, which cannot wait for the
    /// pool: the flushes are already written.
    fn hold_index(&self, bytes: usize) {
        self.reservation.grow(bytes);
        self.index.fetch_add(bytes, Ordering::AcqRel);
    }

    fn release_index(&self, bytes: usize) {
        self.index.fetch_sub(bytes, Ordering::AcqRel);
        self.reservation.shrink(bytes);
    }

    /// Takes the scratch memory, once.
    fn hold_scratch(&self, bytes: usize) {
        if self.scratch.set(bytes).is_ok() {
            self.reservation.grow(bytes);
        }
    }

    fn scratch(&self) -> usize {
        self.scratch.get().copied().unwrap_or(0)
    }

    /// Bytes held for rows buffered since the last flush, and for work on
    /// them: beyond the scratch memory and the indexes.
    fn held(&self) -> usize {
        self.reservation
            .size()
            .saturating_sub(self.scratch() + self.index.load(Ordering::Acquire))
    }

    fn release(&self, bytes: usize) {
        self.reservation.shrink(bytes);
    }

    /// Takes as much of `wanted` as the pool grants, halving the request while
    /// it is refused, and never less than `floor`.
    fn take_up_to(&self, wanted: usize, floor: usize) -> usize {
        let mut bytes = wanted.max(floor);
        while bytes > floor {
            if self.grant(bytes) {
                return bytes;
            }
            bytes = (bytes / 2).max(floor);
        }
        self.force(floor);
        floor
    }
}

/// Bytes that the indexes of `flushes` hold in memory.
fn index_bytes(flushes: &[BlockFlush]) -> usize {
    flushes.iter().map(BlockFlush::index_bytes).sum()
}

/// Divides `ranges` key ranges into at most `files` stretches of consecutive
/// ranges, one for each file of a flush.
fn range_groups(ranges: usize, files: usize) -> Vec<Range<usize>> {
    let files = files.clamp(1, ranges.max(1));
    (0..files)
        .map(|file| file * ranges / files..(file + 1) * ranges / files)
        .collect()
}

/// What a sort has written to disk: flushes in blocks, of records that hold
/// whole rows or of rows kept in row format, or flushes of Arrow batches,
/// where records name rows of kept batches.
enum Flushes {
    Blocks(Vec<BlockFlush>),
    Batches(Vec<BatchFlush>),
}

impl Flushes {
    fn is_empty(&self) -> bool {
        match self {
            Self::Blocks(flushes) => flushes.is_empty(),
            Self::Batches(flushes) => flushes.is_empty(),
        }
    }

    /// Spill files there will be once another flush of up to `next` files is
    /// written.
    fn files_after(&self, next: usize) -> usize {
        match self {
            Self::Blocks(flushes) => flushes.iter().map(BlockFlush::files).sum::<usize>() + next,
            Self::Batches(flushes) => {
                (flushes.len() + 1) * flushes.iter().map(Vec::len).max().unwrap_or(0)
            }
        }
    }
}

/// What a sort sizes from the width of its records.
///
/// Records that hold keys of variable width are made as wide as the keys of
/// the first batch to arrive, so none of this is known before then.
struct Shape {
    words: Words,
    /// The size of the buffers that rows are buffered in, in words: see
    /// [`ranges::chunk_words`].
    chunk: usize,
    /// Rows that one sort thread's scratch memory is sized for.
    scratch_rows: usize,
    /// Scratch memory one sort thread may use at a time.
    scratch_bytes: usize,
    spill: Arc<Spill>,
}

/// One execution of the sort.
struct Sorter {
    encoder: Arc<RowEncoder>,
    schema: SchemaRef,
    ordering: LexOrdering,
    batch_size: usize,
    options: ParallelSortOptions,
    min_run_bytes: usize,
    pool_bytes: usize,
    /// Where spill files go, how they are compressed, and where they are
    /// counted.
    runtime: Arc<RuntimeEnv>,
    spill_compression: SpillCompression,
    spill_metrics: SpillMetrics,
    /// Set by the first batch to arrive: see [`Self::shape_from`].
    shape: OnceLock<Shape>,
    /// Inserts hold this shared. Flushing and finishing hold it exclusively,
    /// which stops every lane between batches.
    gate: tokio::sync::RwLock<()>,
    lanes: Vec<Mutex<Lane>>,
    /// The next batch id of the rows being buffered.
    next_batch: AtomicU32,
    /// Counts flushes, so a lane that finds the pool full can tell whether
    /// another lane flushed while it waited for the gate.
    flush_count: AtomicU64,
    /// The groups that flushes of Arrow batches are written in, chosen at the
    /// first flush from what it sorted.
    splitters: OnceLock<Splitters>,
    flushed: Mutex<Flushes>,
    budget: Budget,
    workers: Arc<Workers>,
    metrics: SortMetrics,
}

impl Sorter {
    fn try_new(plan: &ParallelSortExec, context: &Arc<TaskContext>) -> Result<Self> {
        let schema = plan.input.schema();
        let encoder = Arc::clone(&plan.encoder);
        let runtime = context.runtime_env();
        let pool_bytes = match runtime.memory_pool.memory_limit() {
            MemoryLimit::Finite(bytes) => bytes,
            _ => usize::MAX,
        };
        let options = plan.options;
        let workers = Arc::new(Workers::try_new(options.parallelism)?);
        let lane_count = plan.input.output_partitioning().partition_count();
        Ok(Self {
            schema,
            ordering: plan.ordering.clone(),
            batch_size: context.session_config().batch_size(),
            options,
            // more than half the pool would leave no room for the consumers
            // that cannot spill, which the pool serves by overdrawing. At
            // least a byte: with nothing buffered, a pool too small for any
            // batch then takes one by force, where it would flush nothing
            // forever
            min_run_bytes: options.min_run_bytes.min(pool_bytes / 2).max(1),
            pool_bytes,
            spill_compression: context.session_config().spill_compression(),
            spill_metrics: SpillMetrics::new(&plan.metrics, 0),
            shape: OnceLock::new(),
            gate: tokio::sync::RwLock::new(()),
            lanes: (0..lane_count)
                .map(|_| Mutex::new(Lane::default()))
                .collect(),
            next_batch: AtomicU32::new(0),
            flush_count: AtomicU64::new(0),
            splitters: OnceLock::new(),
            flushed: Mutex::new(
                if encoder.contents() == Contents::Row || encoder.keeps_rows() {
                    Flushes::Blocks(Vec::new())
                } else {
                    Flushes::Batches(Vec::new())
                },
            ),
            encoder,
            budget: Budget::new(
                MemoryConsumer::new("ParallelSort")
                    .with_can_spill(true)
                    .register(&runtime.memory_pool),
            ),
            runtime,
            workers,
            metrics: SortMetrics::new(&plan.metrics, lane_count),
        })
    }

    /// The sort's shape, which the first batch to arrive decides. `keys` are
    /// the keys of a batch.
    ///
    /// Must be called inside the runtime that the sort's input runs on.
    fn shape_from(&self, keys: &Rows) -> &Shape {
        self.shape.get_or_init(|| {
            let longest = keys.iter().map(|key| key.data().len()).max().unwrap_or(0);
            let words = Words::new(self.encoder.layout_for(longest));
            let (options, pool_bytes) = (self.options, self.pool_bytes);
            let row_bytes = 2 * words.record_bytes();
            let scratch_rows = (pool_bytes / SCRATCH_POOL_SHARE / options.parallelism / row_bytes)
                .clamp(MIN_SCRATCH_ROWS, MAX_SCRATCH_ROWS);
            let scratch_bytes = scratch_rows * row_bytes;
            // every sort thread may be sorting a range at once
            self.budget
                .hold_scratch(options.parallelism * scratch_bytes);
            self.metrics.record_bytes.set(words.record_bytes());
            Shape {
                words,
                chunk: ranges::chunk_words(words, scratch_bytes, options.parallelism),
                scratch_rows,
                scratch_bytes,
                spill: Arc::new(Spill::new(
                    Arc::clone(&self.encoder),
                    words,
                    Arc::clone(&self.schema),
                    self.ordering.clone(),
                    self.batch_size,
                    // large enough that a block is worth a read, and small
                    // against a slice of the merge of flushes, which reads
                    // one block of every flush twice
                    scratch_rows / 16,
                    (scratch_bytes / ROW_BLOCK_SCRATCH_SHARE)
                        .clamp(MIN_ROW_BLOCK_BYTES, MAX_ROW_BLOCK_BYTES),
                    &self.runtime,
                    self.spill_compression,
                    self.spill_metrics.clone(),
                    Arc::clone(&self.workers),
                )),
            }
        })
    }

    fn shape(&self) -> &Shape {
        self.shape
            .get()
            .expect("the first batch gave the sort its shape")
    }

    async fn run(
        self: Arc<Self>,
        input: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        output: Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        let mut readers = JoinSet::new();
        for lane in 0..self.lanes.len() {
            let sorter = Arc::clone(&self);
            let input = Arc::clone(&input);
            let context = Arc::clone(&context);
            readers.spawn(async move {
                let mut batches = input.execute(lane, context)?;
                loop {
                    let waiting = Instant::now();
                    let Some(batch) = batches.next().await else {
                        break;
                    };
                    let buffering = Instant::now();
                    sorter.metrics.input_wait_time[lane].add_duration(buffering - waiting);
                    let batch = batch?;
                    sorter.metrics.input_rows[lane].add(batch.num_rows());
                    sorter.insert(lane, batch).await?;
                    sorter.metrics.buffer_time[lane].add_elapsed(buffering);
                }
                Ok::<_, DataFusionError>(())
            });
        }
        while let Some(reader) = readers.join_next().await {
            reader.map_err(|error| DataFusionError::ExecutionJoin(Box::new(error)))??;
        }
        let finished = self.finish(output).await;
        self.budget.release(self.budget.scratch());
        finished
    }

    async fn insert(self: &Arc<Self>, lane: usize, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if u32::try_from(batch.num_rows()).is_err() {
            // records name a row within its batch in 32 bits
            return Err(DataFusionError::NotImplemented(format!(
                "sorting a batch of {} rows",
                batch.num_rows()
            )));
        }
        let mut rows = Some(self.encoder.encode(&batch)?);
        self.shape_from(rows.as_ref().expect("encoded above"));
        let mut payload = self.encoder.encode_payload(&batch)?;
        let mut force = false;
        loop {
            let flushes = self.flush_count.load(Ordering::Acquire);
            let inserted = {
                let _shared = self.gate.read().await;
                self.try_insert(lane, &batch, &mut rows, &mut payload, force)
            };
            if inserted {
                return Ok(());
            }
            // the pool refused the memory
            let _exclusive = self.gate.write().await;
            if self.flush_count.load(Ordering::Acquire) != flushes {
                // another lane flushed while this one waited
                continue;
            }
            if self.budget.held() < self.min_run_bytes {
                force = true;
                continue;
            }
            // with nothing buffered to write, the pool is full of other
            // things, and waiting for it would wait forever
            force = !self.flush().await?;
        }
    }

    /// Buffers a batch in its lane. Returns `false` when the pool refused the
    /// memory.
    fn try_insert(
        &self,
        lane: usize,
        batch: &RecordBatch,
        rows: &mut Option<Rows>,
        payload: &mut Option<Rows>,
        force: bool,
    ) -> bool {
        self.lanes[lane].lock().insert(
            &self.next_batch,
            batch,
            rows,
            payload,
            self.shape().words,
            self.shape().chunk,
            &self.budget,
            force,
        )
    }

    /// Takes everything buffered out of the lanes. Needs the gate held
    /// exclusively.
    fn take_buffered(&self) -> Buffered {
        let mut lanes: Vec<_> = self.lanes.iter().map(|lane| lane.lock()).collect();
        let mut lanes: Vec<&mut Lane> = lanes.iter_mut().map(|lane| &mut **lane).collect();
        let buffered = Buffered::take(&mut lanes, Arc::clone(&self.encoder), self.shape().words);
        self.next_batch.store(0, Ordering::Release);
        self.metrics.record_buffered(&buffered);
        buffered
    }

    /// Puts buffered rows in order on the sort's threads: splits them into
    /// ranges and sorts those.
    async fn prepare(self: &Arc<Self>, buffered: Buffered) -> Result<Ranges> {
        let sorter = Arc::clone(self);
        self.on_pool(move || {
            let _timer = sorter.metrics.sort_time.timer();
            let ranges = Ranges::split(
                buffered,
                sorter.options.parallelism * RANGES_PER_THREAD,
                sorter.shape().chunk,
                sorter.shape().scratch_bytes,
                &sorter.metrics.oversized_range_rows,
                &sorter.workers,
            );
            sorter.metrics.ranges.set_max(ranges.range_count());
            sorter
                .metrics
                .largest_range_rows
                .set_max(ranges.largest_range_rows());
            ranges
        })
        .await
    }

    /// Sorts everything buffered and writes it to disk. Needs the gate held
    /// exclusively.
    async fn flush(self: &Arc<Self>) -> Result<bool> {
        let buffered = self.take_buffered();
        if buffered.num_rows() == 0 {
            self.budget.release(buffered.held());
            return Ok(false);
        }
        let _timer = self.metrics.flush_time.timer();
        self.metrics.flushes.add(1);
        let files = (buffered.num_rows() / MIN_ROWS_PER_FILE).clamp(1, self.options.parallelism);
        let ready = self.prepare(buffered).await?;
        let this = Arc::clone(self);
        let (written, held) = self
            .on_pool(move || (this.write(&ready, files), ready.held()))
            .await?;
        self.budget.release(held);
        written?;
        if self.flushed.lock().files_after(self.options.parallelism) > self.options.max_open_files {
            self.compact().await?;
        }
        self.flush_count.fetch_add(1, Ordering::AcqRel);
        Ok(true)
    }

    /// Writes sorted ranges to disk as one flush of up to `files` files. Runs
    /// on the sort's threads.
    fn write(&self, ranges: &Ranges, files: usize) -> Result<()> {
        let spill = &self.shape().spill;
        if matches!(&*self.flushed.lock(), Flushes::Blocks(_)) {
            let groups = range_groups(ranges.range_count(), files);
            let flush = spill.write_parts(groups.len(), ranges.kept(), |file, push| {
                groups[file]
                    .clone()
                    .try_for_each(|range| ranges.visit_sorted(range, &mut *push))
            })?;
            self.keep(Flushes::Blocks(vec![flush]));
        } else {
            let bounds = self.splitters.get_or_init(|| ranges.group_bounds(files));
            let flush = spill.write_batch_groups(bounds.groups(), |group, push| {
                let mut batcher = ranges.batcher(self.batch_size, |batch| push(&batch));
                ranges.visit_group(bounds, group, |records| batcher.push(records))?;
                batcher.finish()
            })?;
            self.keep(Flushes::Batches(vec![flush]));
        }
        Ok(())
    }

    /// Adds a flush to the ones on disk.
    fn keep(&self, flush: Flushes) {
        match (&mut *self.flushed.lock(), flush) {
            (Flushes::Blocks(all), Flushes::Blocks(new)) => {
                self.metrics
                    .flush_files
                    .set_max(new.iter().map(BlockFlush::files).max().unwrap_or(0));
                // the index stays in memory until the flush is merged
                self.budget.hold_index(index_bytes(&new));
                all.extend(new);
            }
            (Flushes::Batches(all), Flushes::Batches(new)) => {
                self.metrics
                    .flush_files
                    .set_max(new.iter().map(Vec::len).max().unwrap_or(0));
                all.extend(new);
            }
            _ => unreachable!("a sort writes one kind of flush"),
        }
    }

    /// Merges every flush on disk into one, which bounds how many spill files
    /// exist however many flushes the input needs.
    async fn compact(self: &Arc<Self>) -> Result<()> {
        self.metrics.compactions.add(1);
        let on_disk = match &mut *self.flushed.lock() {
            Flushes::Blocks(flushes) => Flushes::Blocks(std::mem::take(flushes)),
            Flushes::Batches(flushes) => Flushes::Batches(std::mem::take(flushes)),
        };
        match on_disk {
            Flushes::Blocks(flushes) => {
                let index = index_bytes(&flushes);
                let merged = self
                    .merge_block_flushes(flushes, self.options.parallelism)
                    .await?;
                self.budget.release_index(index);
                self.keep(Flushes::Blocks(vec![merged]));
            }
            Flushes::Batches(flushes) => match self.compact_batches(&flushes).await? {
                Some(merged) => self.keep(Flushes::Batches(vec![merged])),
                // nothing was merged, so the flushes are still what holds the rows
                None => self.keep(Flushes::Batches(flushes)),
            },
        }
        Ok(())
    }

    /// Merges flushes in blocks into one, `concurrent` files at a time at
    /// most.
    async fn merge_block_flushes(
        self: &Arc<Self>,
        flushes: Vec<BlockFlush>,
        concurrent: usize,
    ) -> Result<BlockFlush> {
        let concurrent = concurrent.clamp(
            1,
            (self.options.max_open_files / flushes.len().max(1)).max(1),
        );
        // merging is what makes room, so its memory cannot wait for the pool
        let bytes = concurrent * flushes.len() * self.shape().spill.input_bytes(&flushes);
        self.budget.force(bytes);
        let (spill, files) = (Arc::clone(&self.shape().spill), self.options.parallelism);
        let merged = self
            .on_pool(move || spill.merge_flushes(flushes, files, concurrent))
            .await;
        self.budget.release(bytes);
        merged?
    }

    /// Merges flushes of Arrow batches into one, group by group.
    async fn compact_batches(
        self: &Arc<Self>,
        flushes: &[BatchFlush],
    ) -> Result<Option<BatchFlush>> {
        let grouped = GroupedFlushes::new(flushes);
        let Some(plan) = MergePlan::new(
            &self.budget,
            &grouped,
            self.options.parallelism,
            self.options.max_open_files,
            0,
        ) else {
            return Ok(None);
        };
        let (spill, workers) = (Arc::clone(&self.shape().spill), Arc::clone(&self.workers));
        let compacted = self
            .on_pool(move || {
                let groups = grouped.by_group.len();
                let pending = Mutex::new(grouped.by_group.into_iter().enumerate());
                let compacted = Mutex::new(vec![None; groups]);
                let failure = Mutex::new(None);
                // each of these takes groups until none are left
                workers.each(plan.concurrent, &|_| {
                    while failure.lock().is_none() {
                        let next = pending.lock().next();
                        let Some((group, segments)) = next else {
                            return;
                        };
                        match spill.compact(segments, plan.fan_in) {
                            Ok(segment) => compacted.lock()[group] = segment,
                            Err(error) => *failure.lock() = Some(error),
                        }
                    }
                });
                failure.into_inner().map_or(Ok(compacted.into_inner()), Err)
            })
            .await;
        self.budget.release(plan.bytes);
        compacted?.map(Some)
    }

    async fn finish(self: &Arc<Self>, output: Sender<Result<RecordBatch>>) -> Result<()> {
        let _exclusive = self.gate.write().await;
        if self.shape.get().is_none() {
            // no batch arrived
            return Ok(());
        }
        if self.flushed.lock().is_empty() {
            let buffered = self.take_buffered();
            if buffered.num_rows() == 0 {
                self.budget.release(buffered.held());
                return Ok(());
            }
            let ranges = self.prepare(buffered).await?;
            self.emit_ranges(ranges, output).await
        } else {
            self.flush().await?;
            let on_disk = std::mem::replace(&mut *self.flushed.lock(), Flushes::Blocks(Vec::new()));
            match on_disk {
                Flushes::Blocks(flushes) => self.emit_slices(flushes, output).await,
                Flushes::Batches(flushes) => self.emit_groups(flushes, output).await,
            }
        }
    }

    /// Sends sorted ranges that fit in memory to the output in order.
    async fn emit_ranges(
        self: &Arc<Self>,
        ranges: Ranges,
        output: Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        let ranges = Arc::new(ranges);
        let count = ranges.range_count();
        let range_batches = ranges.num_rows() / count / self.batch_size + 2;
        let batch_bytes = ranges.bytes_per_row() * self.batch_size;
        let window = Window::plan(
            &self.budget,
            self.options.parallelism,
            range_batches,
            batch_bytes,
        );
        let batch_size = self.batch_size;
        let result = emit_in_order(count, window.ranges, &output, &self.metrics, |range| {
            let (sender, receiver) = mpsc::channel(window.batches);
            let ranges = Arc::clone(&ranges);
            self.workers.spawn(move || {
                if sender.is_closed() {
                    // the output was dropped before this range's turn
                    return;
                }
                let mut batcher = ranges
                    .batcher(batch_size, |batch| send(&sender, Ok(batch)))
                    .compacting();
                let sent = ranges
                    .visit_sorted(range, |records| batcher.push(records))
                    .and_then(|()| batcher.finish());
                if let Err(error) = sent {
                    // the receiver is gone when the output was dropped
                    let _ = send(&sender, Err(error));
                }
            });
            receiver
        })
        .await;
        self.budget.release(window.bytes + ranges.held());
        result
    }

    /// Merges flushes in blocks and sends them to the output, a slice at a
    /// time.
    async fn emit_slices(
        self: &Arc<Self>,
        mut flushes: Vec<BlockFlush>,
        output: Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        let shape = self.shape();
        let input_bytes = shape.spill.input_bytes(&flushes);
        let (slice_rows, row_bytes) = match shape.words.layout.contents {
            Contents::Row => (
                MERGE_SLICE_SCRATCH_ROWS * shape.scratch_rows,
                shape.words.layout.inline,
            ),
            Contents::Source => {
                let total =
                    |of: fn(&BlockFlush) -> usize| flushes.iter().map(of).sum::<usize>().max(1);
                let rows = total(BlockFlush::rows);
                (
                    (MERGE_SLICE_ROW_BLOCKS * rows / total(BlockFlush::blocks)).max(1),
                    total(BlockFlush::bytes) / rows,
                )
            }
        };
        let batch_bytes = (row_bytes * self.batch_size).max(1);
        let slice_batches = slice_rows / self.batch_size + 2;
        // a slice reads a block of every flush and buffers its own output
        let per_slice = flushes.len() * input_bytes + slice_batches * batch_bytes;
        let at_most =
            (self.options.max_open_files / flushes.len()).clamp(1, self.options.parallelism);
        let floor = 2 * (input_bytes + batch_bytes);
        let bytes = self.budget.take_up_to(at_most * per_slice, floor);
        // what the grant does not cover in one pass is merged in several
        let fan_in = (bytes.saturating_sub(2 * batch_bytes) / input_bytes)
            .clamp(2, self.options.max_open_files);
        while flushes.len() > fan_in {
            // leave exactly `fan_in` when one pass can
            let count = fan_in.min(flushes.len() - fan_in + 1);
            let merged: Vec<BlockFlush> = flushes.drain(..count).collect();
            let merged_index = index_bytes(&merged);
            let merged = self.on_pool({
                let (spill, files) = (Arc::clone(&self.shape().spill), self.options.parallelism);
                move || spill.merge_flushes(merged, files, 1)
            });
            match merged.await.and_then(|merged| merged) {
                Ok(merged) => {
                    self.budget.hold_index(merged.index_bytes());
                    self.budget.release_index(merged_index);
                    flushes.push(merged);
                }
                Err(error) => {
                    self.budget.release(bytes);
                    return Err(error);
                }
            }
        }
        // a merged flush can have larger blocks than the ones it replaced
        let input_bytes = shape.spill.input_bytes(&flushes);
        let inputs = flushes.len() * input_bytes;
        let concurrent = (bytes / (inputs + slice_batches * batch_bytes)).clamp(1, at_most);
        // one slice alone buffers what is left of the grant after its inputs
        let batches = if concurrent > 1 {
            slice_batches
        } else {
            (bytes.saturating_sub(inputs) / batch_bytes).clamp(2, slice_batches)
        };
        self.metrics.merge_concurrency.set_max(concurrent);
        self.metrics.merge_fan_in.set_max(flushes.len());
        let index = index_bytes(&flushes);
        let slices = Arc::new(Slices::new(flushes, slice_rows));
        let result = emit_in_order(slices.len(), concurrent, &output, &self.metrics, |slice| {
            let (sender, receiver) = mpsc::channel(batches);
            let (spill, slices) = (Arc::clone(&self.shape().spill), Arc::clone(&slices));
            self.workers.spawn(move || {
                if sender.is_closed() {
                    // the output was dropped before this slice's turn
                    return;
                }
                let sent = spill.merge_slice(&slices, slice, |batch| send(&sender, Ok(batch)));
                if let Err(error) = sent {
                    // the receiver is gone when the output was dropped
                    let _ = send(&sender, Err(error));
                }
            });
            receiver
        })
        .await;
        self.budget.release(bytes);
        self.budget.release_index(index);
        result
    }

    /// Merges flushes of Arrow batches group by group and sends them to the
    /// output.
    async fn emit_groups(
        self: &Arc<Self>,
        flushed: Vec<BatchFlush>,
        output: Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        let mut grouped = GroupedFlushes::new(&flushed);
        drop(flushed);
        let groups = grouped.by_group.len();
        let group_batches = grouped.rows / groups.max(1) / self.batch_size + 2;
        let buffered = group_batches * grouped.batch_bytes;
        let Some(plan) = MergePlan::new(
            &self.budget,
            &grouped,
            self.options.parallelism,
            self.options.max_open_files,
            buffered,
        ) else {
            return Ok(());
        };
        self.metrics.merge_concurrency.set_max(plan.concurrent);
        self.metrics.merge_fan_in.set_max(plan.fan_in);
        if let Err(error) = self.reduce_groups(&mut grouped, plan).await {
            self.budget.release(plan.bytes);
            return Err(error);
        }
        // one merge alone buffers what is left of the grant after its inputs
        let batches = if plan.concurrent > 1 {
            group_batches
        } else {
            let spare = plan.bytes.saturating_sub(plan.fan_in * grouped.input_bytes);
            (spare / grouped.batch_bytes.max(1)).clamp(2, group_batches)
        };
        let result = emit_in_order(groups, plan.concurrent, &output, &self.metrics, |group| {
            let (sender, receiver) = mpsc::channel(batches);
            let segments = std::mem::take(&mut grouped.by_group[group]);
            let spill = Arc::clone(&self.shape().spill);
            self.workers.spawn(move || {
                if sender.is_closed() {
                    // the output was dropped before this group's turn
                    return;
                }
                let sent = spill.merge(&segments, |batch| send(&sender, Ok(batch)));
                if let Err(error) = sent {
                    // the receiver is gone when the output was dropped
                    let _ = send(&sender, Err(error));
                }
            });
            receiver
        })
        .await;
        self.budget.release(plan.bytes);
        result
    }

    /// Merges every group's segments down to what one merge may read, where
    /// the pool granted less than a group has. This is done for several
    /// groups at once and before any output: a group that did it in its
    /// own turn kept the output waiting on one thread while the rest idled,
    /// which was 4 to 6 s of a 42 s sort of 100M rows in 1 GiB.
    async fn reduce_groups(
        self: &Arc<Self>,
        grouped: &mut GroupedFlushes,
        plan: MergePlan,
    ) -> Result<()> {
        // the first pass of a group reads the most inputs of its passes
        let Some(widest_pass) = grouped
            .by_group
            .iter()
            .map(Vec::len)
            .filter(|segments| *segments > plan.fan_in)
            .map(|segments| plan.fan_in.min(segments - plan.fan_in + 1))
            .max()
        else {
            return Ok(());
        };
        let at_once = (plan.bytes / (widest_pass * grouped.input_bytes).max(1))
            .clamp(1, self.options.parallelism);
        let groups = std::mem::take(&mut grouped.by_group);
        let (spill, workers) = (Arc::clone(&self.shape().spill), Arc::clone(&self.workers));
        grouped.by_group = self
            .on_pool(move || {
                let mut reduced = Vec::with_capacity(groups.len());
                for together in groups.chunks(at_once) {
                    let passes = workers.map(together.len(), &|group| {
                        spill.reduce(together[group].clone(), plan.fan_in)
                    });
                    for segments in passes {
                        reduced.push(segments?);
                    }
                }
                Ok::<_, DataFusionError>(reduced)
            })
            .await??;
        Ok(())
    }

    /// Runs CPU-bound work on the sort's workers without blocking the async
    /// task that waits for it.
    async fn on_pool<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.workers.spawn(move || {
            let _ = sender.send(work());
        });
        receiver
            .await
            .map_err(|_| DataFusionError::Internal("the sort's worker stopped early".into()))
    }
}

fn cancelled() -> DataFusionError {
    DataFusionError::Execution("the sort's output was dropped".into())
}

/// Sends a batch, or what went wrong, from a sort job to the task that puts
/// the output in order, waiting while that task is behind.
fn send(sender: &mpsc::Sender<Result<RecordBatch>>, batch: Result<RecordBatch>) -> Result<()> {
    sender.blocking_send(batch).map_err(|_| cancelled())
}

/// The flushes on disk regrouped for merging: each group's segment from every
/// flush.
struct GroupedFlushes {
    by_group: Vec<Vec<Segment>>,
    /// Memory the largest segment needs as a merge input.
    input_bytes: usize,
    /// The largest batch in any segment.
    batch_bytes: usize,
    rows: usize,
}

impl GroupedFlushes {
    fn new(flushed: &[BatchFlush]) -> Self {
        let groups = flushed.iter().map(Vec::len).max().unwrap_or(0);
        let by_group: Vec<Vec<Segment>> = (0..groups)
            .map(|group| {
                flushed
                    .iter()
                    .filter_map(|flush| flush.get(group).cloned().flatten())
                    .collect()
            })
            .collect();
        let segments = || by_group.iter().flatten();
        Self {
            // at least a byte, which the merges are sized by dividing by
            input_bytes: segments()
                .map(Segment::input_bytes)
                .max()
                .unwrap_or(0)
                .max(1),
            batch_bytes: segments().map(Segment::batch_bytes).max().unwrap_or(0),
            rows: segments().map(Segment::rows).sum(),
            by_group,
        }
    }
}

/// How many groups merge at once, and how many inputs each merge reads.
#[derive(Clone, Copy)]
struct MergePlan {
    concurrent: usize,
    fan_in: usize,
    /// Bytes taken from the budget, for the caller to release.
    bytes: usize,
}

impl MergePlan {
    /// Sizes the merges to what the pool grants. The goal is `parallelism`
    /// groups at once, each reading all of its segments in one pass and
    /// holding `buffered` bytes of output. The floor is one merge of two
    /// inputs, which the pool is overdrawn for if it has to be.
    ///
    /// `None` when there is nothing to merge.
    fn new(
        budget: &Budget,
        grouped: &GroupedFlushes,
        parallelism: usize,
        max_open_files: usize,
        buffered: usize,
    ) -> Option<Self> {
        let widest = grouped.by_group.iter().map(Vec::len).max()?;
        let groups = grouped.by_group.len().min(parallelism).max(1);
        let floor = 2 * grouped.input_bytes;
        let fan_in = widest.clamp(2, (max_open_files / groups).max(2));
        let per_merge = fan_in * grouped.input_bytes + buffered;
        let bytes = budget.take_up_to(groups * per_merge, floor);
        if bytes >= per_merge {
            return Some(Self {
                concurrent: (bytes / per_merge).clamp(1, groups),
                fan_in,
                bytes,
            });
        }
        Some(Self {
            concurrent: 1,
            fan_in: (bytes / grouped.input_bytes).clamp(2, fan_in),
            bytes,
        })
    }
}

/// How far ahead of the output sorted ranges are made into batches.
struct Window {
    /// Ranges in flight at once.
    ranges: usize,
    /// Output batches buffered per range.
    batches: usize,
    /// Bytes taken from the budget, for the caller to release.
    bytes: usize,
}

impl Window {
    /// Sizes the window to what the pool grants. The goal is `parallelism`
    /// ranges in flight, each buffering all of its output so it never waits
    /// for the ranges before it. The floor is one range that buffers two
    /// batches.
    fn plan(budget: &Budget, parallelism: usize, range_batches: usize, batch_bytes: usize) -> Self {
        let batch_bytes = batch_bytes.max(1);
        let range_bytes = range_batches * batch_bytes;
        let bytes = budget.take_up_to(parallelism * range_bytes, 2 * batch_bytes);
        let ranges = (bytes / range_bytes).clamp(1, parallelism);
        let batches = if ranges > 1 {
            range_batches
        } else {
            (bytes / batch_bytes).clamp(2, range_batches.max(2))
        };
        Self {
            ranges,
            batches,
            bytes,
        }
    }
}

/// Sends every part's batches to `output` in order, keeping up to `window`
/// parts started ahead of the one being sent.
async fn emit_in_order(
    parts: usize,
    window: usize,
    output: &Sender<Result<RecordBatch>>,
    metrics: &SortMetrics,
    mut start: impl FnMut(usize) -> Receiver<Result<RecordBatch>>,
) -> Result<()> {
    let mut started: VecDeque<Receiver<Result<RecordBatch>>> = VecDeque::new();
    let mut next = 0;
    loop {
        while started.len() < window && next < parts {
            started.push_back(start(next));
            next += 1;
        }
        let Some(mut part) = started.pop_front() else {
            return Ok(());
        };
        while let Some(batch) = part.recv().await {
            let batch = batch?;
            metrics.output_rows.add(batch.num_rows());
            let _timer = metrics.output_wait_time.timer();
            if output.send(Ok(batch)).await.is_err() {
                // the consumer stopped reading
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests;
