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
//! - **One reservation, within the pool.** The sort takes a floor of memory
//!   when its first batch arrives: enough to sort ranges, write a flush, and
//!   merge two flushes. Beyond it, the sort asks the pool for memory to buffer
//!   input, to split and merge more at once, and to hold the index of what it
//!   spilled, and uses only what the pool grants. A refusal to buffer means
//!   "write what is buffered to disk". The sort fails only when the pool will
//!   not give it its floor, room for one batch with nothing else buffered, or
//!   the least a merge needs beside the index of what it spilled, and then
//!   says how much it needed: see `Budget`.
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
        atomic::{AtomicU32, AtomicU64, Ordering},
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
        memory_pool::{MemoryConsumer, MemoryLimit, MemoryReservation, human_readable_size},
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
use ranges::{Ranges, SPLIT_HEADROOM_SHARE};
use record::Words;
use run::{Buffered, Grant, Lane, Splitters};
use spill::{BatchFlush, BlockFlush, MergeMemory, Segment, Slices, Spill};
use workers::Workers;

/// The operator's name in plans.
const OPERATOR: &str = "ParallelSortExec";

/// The rows that one sort thread's scratch memory is sized for, at two
/// records a row. A thread sorts a range of buffered rows in that memory, so
/// this bounds what the sort holds beyond the rows it has buffered.
const MAX_SCRATCH_ROWS: usize = 122_880;
const MIN_SCRATCH_ROWS: usize = 256;

/// The fewest rows in a batch of output, when memory is too short for the
/// session's batch size.
const MIN_OUTPUT_ROWS: usize = 128;

/// Ranges per sort thread. More ranges than threads keeps the threads evenly
/// loaded when the splitters divide the rows unevenly.
const RANGES_PER_THREAD: usize = 16;

/// The share of the pool held back for sorting what is buffered: one part in
/// this many. A flush is what frees memory, so its sort cannot ask for
/// memory when it starts.
const SCRATCH_POOL_SHARE: usize = 32;

/// Rows a spill file is worth having for.
const MIN_ROWS_PER_FILE: usize = 16 * 1024;

/// Blocks that make one slice of the merge of flushes. A slice of that merge
/// reads into a block of each flush that the slice before it also read, so
/// larger slices read fewer blocks twice: with 256, a merge of 25 flushes
/// reads a tenth of the blocks twice.
const MERGE_SLICE_BLOCKS: usize = 256;

/// The share of the pool that a block of a flush fills: one part in this
/// many. The index of a flush keeps the first key of every block, and 40 bytes
/// more, in memory for as long as the flush exists, so small blocks make an
/// index that can outgrow the pool: blocks of 8 KiB with 50-byte keys make one
/// a hundredth the size of what is spilled. A merge holds a block of every
/// flush it reads, so large ones leave room for fewer flushes at once, and
/// blocks are no larger than a read is worth.
const BLOCK_POOL_SHARE: usize = 1024;
const MIN_BLOCK_BYTES: usize = 4 * 1024;
const MAX_BLOCK_BYTES: usize = 256 * 1024;

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
    /// The least buffered before a flush, capped at a quarter of the pool. The
    /// sort holds this from its first batch, so however full other consumers
    /// keep the pool, it buffers this much, less the index of what it has
    /// spilled, before it writes a run to disk.
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
    /// Rows whose keys were too long for their records when they were sorted.
    /// Their ties are broken through the row format.
    cut_keys: Count,
    /// Times the records were made wider, because the keys that came after
    /// the first batch were longer than its keys.
    widenings: Count,
    /// What the sort held from its first batch to its end: see [`Budget`].
    floor_bytes: Gauge,
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
            widenings: MetricBuilder::new(metrics).counter("record_widenings", 0),
            floor_bytes: MetricBuilder::new(metrics).gauge("floor_bytes", 0),
        }
    }

    fn record_buffered(&self, buffered: &Buffered) {
        self.peak_run_bytes.set_max(buffered.held());
    }
}

/// The pool reservation every part of one sort draws from.
///
/// The reservation never drops below the sort's floor, which it takes when the
/// first batch arrives: memory to sort ranges in, to write a flush with, and a
/// credit that is room to buffer rows until the first flush and, once rows are
/// flushed, room to merge them. Memory used within the floor is never asked of
/// the pool, so a flush always has what it needs to start, and a merge does
/// until the index of what was spilled takes the credit. Memory beyond the
/// floor is asked of the pool, and only what it grants is used.
///
/// Jobs on the sort's threads hold the budget too, so a sort whose output is
/// dropped gives its memory back when its last job stops, not before.
struct Budget {
    reservation: MemoryReservation,
    state: Mutex<BudgetState>,
}

#[derive(Default)]
struct BudgetState {
    /// Bytes in use, within the reservation.
    used: usize,
    /// The least the reservation holds once the first batch has arrived.
    floor: usize,
}

impl Grant for Budget {
    fn grant(&self, bytes: usize) -> bool {
        let mut state = self.state.lock();
        let wanted = state.used + bytes;
        let size = self.reservation.size();
        if wanted > size && self.reservation.try_grow(wanted - size).is_err() {
            return false;
        }
        state.used = wanted;
        true
    }
}

impl Budget {
    fn new(reservation: MemoryReservation) -> Self {
        Self {
            reservation,
            state: Mutex::new(BudgetState::default()),
        }
    }

    /// Takes the floor, of which `fixed` bytes are in use from then on.
    /// `false` when the pool refuses.
    fn take_floor(&self, floor: usize, fixed: usize) -> bool {
        let mut state = self.state.lock();
        let size = self.reservation.size();
        if floor > size && self.reservation.try_grow(floor - size).is_err() {
            return false;
        }
        state.floor = floor;
        state.used += fixed;
        true
    }

    fn release(&self, bytes: usize) {
        self.replace(bytes, 0)
            .expect("releasing memory asks nothing of the pool");
    }

    /// Stops using `release` bytes and starts using `take`, asking the pool
    /// only for what the reservation does not already hold.
    fn replace(&self, release: usize, take: usize) -> Result<()> {
        let mut state = self.state.lock();
        let used = state
            .used
            .checked_sub(release)
            .expect("a sort releases only memory it took")
            + take;
        let size = self.reservation.size();
        if used > size {
            self.reservation
                .try_grow(used - size)
                .map_err(|error| too_small(used - size, state.used, state.floor, &error))?;
        } else {
            let keep = used.max(state.floor);
            if size > keep {
                self.reservation.shrink(size - keep);
            }
        }
        state.used = used;
        Ok(())
    }

    /// Takes as much of `wanted` as the pool grants, halving the request while
    /// it is refused, and never less than `floor`: work that cannot be done in
    /// less than `floor` fails when the pool will not give that.
    fn take_up_to(&self, wanted: usize, floor: usize) -> Result<usize> {
        let mut bytes = wanted.max(floor);
        while bytes > floor {
            if self.grant(bytes) {
                return Ok(bytes);
            }
            bytes = (bytes / 2).max(floor);
        }
        self.replace(0, floor)?;
        Ok(floor)
    }

    /// Takes `bytes` without asking the pool, for DataFusion code that grows a
    /// reservation where it cannot take a refusal. The sort itself never
    /// does.
    fn take_unchecked(&self, bytes: usize) {
        let mut state = self.state.lock();
        state.used += bytes;
        let size = self.reservation.size();
        if state.used > size {
            self.reservation.grow(state.used - size);
        }
    }

    /// Gives everything back, at the sort's end.
    fn close(&self) {
        let mut state = self.state.lock();
        *state = BudgetState::default();
        self.reservation.free();
    }
}

/// The error for work that needs `bytes` more of the pool than it grants, while
/// the sort uses `used` bytes and holds a floor of `floor`.
fn too_small(
    bytes: usize,
    used: usize,
    floor: usize,
    refusal: &DataFusionError,
) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!(
        "the memory pool is too small for this sort, which uses {} of the {} it holds from the \
         start and needs {} more: {}",
        human_readable_size(used),
        human_readable_size(floor),
        human_readable_size(bytes),
        refusal.message()
    ))
}

/// What a sort holds from its first batch to its end: see [`Budget`].
struct Floor {
    /// Memory in use the whole time: scratch, writers, and buffers being
    /// filled.
    fixed: usize,
    /// Room to buffer rows until a flush, and to merge them after. The index
    /// of what is spilled takes from it too.
    credit: usize,
}

impl Floor {
    fn total(&self) -> usize {
        self.fixed + self.credit
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
    /// Scratch memory one sort thread may use at a time.
    scratch_bytes: usize,
    /// Rows in a batch of a flush of Arrow batches.
    spill_batch_rows: usize,
    spill: Arc<Spill>,
}

impl Shape {
    fn chunk_bytes(&self) -> usize {
        self.chunk * size_of::<u64>()
    }
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
    /// Set by the first batch to arrive: see [`Self::shape_for`].
    shape: parking_lot::RwLock<Option<Arc<Shape>>>,
    /// Wider records that a flush found its rows needed and the pool had no
    /// room for, to buffer later rows in once the flush is written.
    wider: Mutex<Option<Words>>,
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
    budget: Arc<Budget>,
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
            // the sort's floor holds this, which should leave most of the pool
            // to grow into
            min_run_bytes: options.min_run_bytes.min(pool_bytes / 4),
            pool_bytes,
            spill_compression: context.session_config().spill_compression(),
            spill_metrics: SpillMetrics::new(&plan.metrics, 0),
            shape: parking_lot::RwLock::new(None),
            wider: Mutex::new(None),
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
            budget: Arc::new(Budget::new(
                MemoryConsumer::new("ParallelSort")
                    .with_can_spill(true)
                    .register(&runtime.memory_pool),
            )),
            runtime,
            workers,
            metrics: SortMetrics::new(&plan.metrics, lane_count),
        })
    }

    /// The sort's shape, which the first batch to arrive decides, and with it
    /// the sort's floor. `keys` are the keys of a batch, and `row_bytes` about
    /// what one of its rows takes besides its record.
    ///
    /// The shape is sized from the pool first, and smaller while the floor that
    /// goes with it is more than half the pool or the pool refuses it. A pool
    /// that refuses the floor of the smallest shape is too small for the sort.
    ///
    /// Must be called inside the runtime that the sort's input runs on.
    fn shape_for(&self, keys: &Rows, row_bytes: usize) -> Result<Arc<Shape>> {
        if let Some(shape) = self.shape.read().as_ref() {
            return Ok(Arc::clone(shape));
        }
        let mut slot = self.shape.write();
        if let Some(shape) = slot.as_ref() {
            return Ok(Arc::clone(shape));
        }
        let longest = keys.iter().map(|key| key.data().len()).max().unwrap_or(0);
        let words = Words::new(self.encoder.layout_for(longest));
        let parallelism = self.options.parallelism;
        let sort_row_bytes = 2 * words.record_bytes();
        let mut scratch_rows =
            (self.pool_bytes / SCRATCH_POOL_SHARE / parallelism / sort_row_bytes)
                .clamp(MIN_SCRATCH_ROWS, MAX_SCRATCH_ROWS);
        let mut spill_batch_rows = self.batch_size.max(MIN_OUTPUT_ROWS);
        let shape = loop {
            let shape = self.shape_of(words, scratch_rows, spill_batch_rows);
            let floor = self.floor_of(&shape, row_bytes, longest);
            let smallest = scratch_rows == MIN_SCRATCH_ROWS && spill_batch_rows == MIN_OUTPUT_ROWS;
            // the rest of the pool is for the rows to grow into, and for the
            // index of what is spilled
            if (smallest || floor.total() <= self.pool_bytes / 2)
                && self.budget.take_floor(floor.total(), floor.fixed)
            {
                self.metrics.floor_bytes.set(floor.total());
                break shape;
            }
            if smallest {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "the memory pool is too small for this sort, which needs {} to start",
                    human_readable_size(floor.total())
                )));
            }
            scratch_rows = (scratch_rows / 2).max(MIN_SCRATCH_ROWS);
            spill_batch_rows = (spill_batch_rows / 2).max(MIN_OUTPUT_ROWS);
        };
        self.metrics.record_bytes.set(words.record_bytes());
        let shape = Arc::new(shape);
        *slot = Some(Arc::clone(&shape));
        Ok(shape)
    }

    /// The shape of a sort whose threads sort ranges of `scratch_rows` and
    /// whose flushes of Arrow batches are batches of `spill_batch_rows`.
    fn shape_of(&self, words: Words, scratch_rows: usize, spill_batch_rows: usize) -> Shape {
        let parallelism = self.options.parallelism;
        let scratch_bytes = scratch_rows * 2 * words.record_bytes();
        let block_bytes =
            (self.pool_bytes / BLOCK_POOL_SHARE).clamp(MIN_BLOCK_BYTES, MAX_BLOCK_BYTES);
        Shape {
            words,
            chunk: ranges::chunk_words(words, scratch_bytes, parallelism),
            scratch_bytes,
            spill_batch_rows,
            spill: Arc::new(Spill::new(
                Arc::clone(&self.encoder),
                words,
                Arc::clone(&self.schema),
                self.ordering.clone(),
                spill_batch_rows,
                block_bytes / words.record_bytes(),
                block_bytes,
                &self.runtime,
                self.spill_compression,
                self.spill_metrics.clone(),
                Arc::clone(&self.workers),
            )),
        }
    }

    /// What a sort of `shape` holds from its first batch to its end. `row_bytes`
    /// is about what a row takes besides its record, and `key_bytes` its key.
    fn floor_of(&self, shape: &Shape, row_bytes: usize, key_bytes: usize) -> Floor {
        let parallelism = self.options.parallelism;
        let spill = &shape.spill;
        let output_row_bytes = match shape.words.layout.contents {
            Contents::Row => shape.words.layout.inline,
            Contents::Source => shape.words.record_bytes() + row_bytes,
        };
        let (writer, input) = if matches!(&*self.flushed.lock(), Flushes::Batches(_)) {
            let batch_bytes = shape.spill_batch_rows * output_row_bytes;
            (
                spill.batch_writer_bytes(batch_bytes),
                Spill::batch_input_bytes(batch_bytes, shape.spill_batch_rows * key_bytes),
            )
        } else {
            (
                spill.writer_bytes(),
                spill.planned_input_bytes(key_bytes + row_bytes, key_bytes),
            )
        };
        // the buffer each lane is filling, and the chunk each thread fills for
        // each range of a split into one range per thread
        let chunks = (self.lanes.len() + parallelism * (parallelism + 1)) * shape.chunk_bytes();
        // the least that merges two inputs, through a writer when a pass
        // writes them to disk, or into two small output batches
        let merge = 2 * input + writer.max(2 * MIN_OUTPUT_ROWS * output_row_bytes);
        Floor {
            // a flush is written by up to a writer per sort thread
            fixed: parallelism * (shape.scratch_bytes + writer) + chunks,
            credit: self.min_run_bytes.max(merge),
        }
    }

    /// Makes records wider, for these rows and those buffered after them,
    /// when the first batch's keys made them too narrow for many of the keys
    /// that came after: see [`RowEncoder::wider_layout`]. Needs the gate held
    /// exclusively.
    ///
    /// The rows of a flush on disk carry their whole keys, so flushes of
    /// different widths merge as they are. A flush is usually written because
    /// the pool refused more memory, so it seldom has room for wider records
    /// of its own rows: those are then written as they are, and the rows
    /// buffered after the flush are the first to be wider.
    ///
    /// Returns the wider layout, the chunk size of its buffers, and the bytes
    /// it takes, when the pool granted them: the caller rebuilds the records
    /// with [`Buffered::widen`] on the sort's threads.
    fn widening_for(&self, buffered: &Buffered) -> Option<(Words, usize, usize)> {
        let layout = self
            .encoder
            .wider_layout(buffered.words.layout, &buffered.key_widths)?;
        let words = Words::new(layout);
        let wider = buffered.num_rows() * (words.record_bytes() - buffered.words.record_bytes());
        let extra = wider + wider / SPLIT_HEADROOM_SHARE;
        if !self.budget.grant(extra) {
            *self.wider.lock() = Some(words);
            return None;
        }
        let shape = self.reshape(words);
        Some((words, shape.chunk, extra))
    }

    /// Lays out every batch buffered from now on as `words`, in the scratch
    /// memory the sort already holds.
    fn reshape(&self, words: Words) -> Arc<Shape> {
        self.metrics.widenings.add(1);
        let mut slot = self.shape.write();
        let old = slot
            .as_ref()
            .expect("the first batch gave the sort its shape");
        let scratch_rows = (old.scratch_bytes / (2 * words.record_bytes())).max(1);
        let shape = Arc::new(self.shape_of(words, scratch_rows, old.spill_batch_rows));
        self.metrics.record_bytes.set(words.record_bytes());
        *slot = Some(Arc::clone(&shape));
        shape
    }

    fn shape(&self) -> Arc<Shape> {
        Arc::clone(
            self.shape
                .read()
                .as_ref()
                .expect("the first batch gave the sort its shape"),
        )
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
        self.budget.close();
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
        let mut payload = self.encoder.encode_payload(&batch)?;
        let row_bytes = payload
            .as_ref()
            .map_or(batch.get_array_memory_size(), Rows::size)
            .div_ceil(batch.num_rows());
        self.shape_for(rows.as_ref().expect("encoded above"), row_bytes)?;
        loop {
            let flushes = self.flush_count.load(Ordering::Acquire);
            let inserted = {
                let _shared = self.gate.read().await;
                self.try_insert(lane, &batch, &mut rows, &mut payload)
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
            if !self.flush().await? {
                // with nothing buffered to write, the floor's credit was too
                // small for this batch and the pool is full of other things
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "the memory pool is too small for this sort to buffer a batch of {} rows",
                    batch.num_rows()
                )));
            }
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
    ) -> bool {
        let shape = self.shape();
        self.lanes[lane].lock().insert(
            &self.next_batch,
            batch,
            rows,
            payload,
            shape.words,
            shape.chunk,
            &*self.budget,
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
    ///
    /// A split fills a chunk for each range on each thread at once, beyond
    /// the chunks the records were in. The floor holds that for one range per
    /// thread, the rows were buffered with room for it for as many ranges as
    /// fit in the scratch memory, and more ranges are made only as far as the
    /// pool grants the chunks for them. Ranges too large for the scratch
    /// memory are sorted in pieces of it.
    async fn prepare(self: &Arc<Self>, mut buffered: Buffered) -> Result<Ranges> {
        let widening = self.widening_for(&buffered);
        let shape = self.shape();
        let parallelism = self.options.parallelism;
        let wanted = ranges::wanted(
            buffered.num_rows(),
            parallelism * RANGES_PER_THREAD,
            shape.words,
            shape.scratch_bytes,
        );
        let per_range = parallelism * shape.chunk_bytes();
        // the rows were counted with room for a split into as many ranges as
        // fit in the scratch memory, and more ranges than that ask the pool
        let headroom = buffered.num_rows() * shape.words.record_bytes() / SPLIT_HEADROOM_SHARE;
        let split_bytes = self.budget.take_up_to(
            (wanted.saturating_sub(parallelism) * per_range).saturating_sub(headroom),
            0,
        )?;
        let at_most = parallelism + (headroom + split_bytes) / per_range;
        let sorter = Arc::clone(self);
        self.on_pool(move || {
            let _timer = sorter.metrics.sort_time.timer();
            if let Some((words, chunk, extra)) = widening {
                let freed = buffered.widen(words, chunk, extra, &sorter.workers);
                sorter.budget.release(freed);
            }
            sorter.metrics.cut_keys.add(buffered.cut_keys);
            let ranges = Ranges::split(
                buffered,
                wanted.min(at_most),
                shape.chunk,
                shape.scratch_bytes,
                split_bytes,
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
        self.write_ranges(ready, files).await?;
        if let Some(words) = self.wider.lock().take() {
            self.reshape(words);
        }
        if self.flushed.lock().files_after(self.options.parallelism) > self.options.max_open_files {
            self.compact().await?;
        }
        self.flush_count.fetch_add(1, Ordering::AcqRel);
        Ok(true)
    }

    /// Writes sorted ranges to disk as one flush of up to `files` files, and
    /// gives back what they held but the index of what was written, which
    /// stays in memory until the flush is merged.
    async fn write_ranges(self: &Arc<Self>, ranges: Ranges, files: usize) -> Result<()> {
        let this = Arc::clone(self);
        let (written, held) = self
            .on_pool(move || (this.write(&ranges, files), ranges.held()))
            .await?;
        match written {
            Ok(index) => self.budget.replace(held, index),
            Err(error) => {
                self.budget.release(held);
                Err(error)
            }
        }
    }

    /// Writes sorted ranges to disk as one flush of up to `files` files. Runs
    /// on the sort's threads. Returns the bytes of the flush's index.
    fn write(&self, ranges: &Ranges, files: usize) -> Result<usize> {
        let spill = &self.shape().spill;
        if matches!(&*self.flushed.lock(), Flushes::Blocks(_)) {
            let groups = range_groups(ranges.range_count(), files);
            let flush = spill.write_parts(groups.len(), ranges.kept(), |file, push| {
                groups[file]
                    .clone()
                    .try_for_each(|range| ranges.visit_sorted(range, &mut *push))
            })?;
            let index = flush.index_bytes();
            self.keep(Flushes::Blocks(vec![flush]));
            Ok(index)
        } else {
            let bounds = self.splitters.get_or_init(|| ranges.group_bounds(files));
            let batch_rows = self.shape().spill_batch_rows;
            let flush = spill.write_batch_groups(bounds.groups(), |group, push| {
                let mut batcher = ranges.batcher(batch_rows, |batch| push(&batch));
                ranges.visit_group(bounds, group, |records| batcher.push(records))?;
                batcher.finish()
            })?;
            self.keep(Flushes::Batches(vec![flush]));
            Ok(0)
        }
    }

    /// Adds a flush to the ones on disk.
    fn keep(&self, flush: Flushes) {
        match (&mut *self.flushed.lock(), flush) {
            (Flushes::Blocks(all), Flushes::Blocks(new)) => {
                self.metrics
                    .flush_files
                    .set_max(new.iter().map(BlockFlush::files).max().unwrap_or(0));
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
                let merged = self.merge_block_flushes(flushes).await?;
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

    /// Merges flushes in blocks into one. As many files are written at once as
    /// the pool grants memory for, up to one per sort thread, and the flushes
    /// are first merged down in passes where it grants too little for one
    /// merge to read them all.
    async fn merge_block_flushes(self: &Arc<Self>, flushes: Vec<BlockFlush>) -> Result<BlockFlush> {
        let spill = Arc::clone(&self.shape().spill);
        let (input, writer) = (spill.input_bytes(&flushes), spill.writer_bytes());
        let at_most = self.options.parallelism.clamp(
            1,
            (self.options.max_open_files / flushes.len().max(1)).max(1),
        );
        let bytes = self.budget.take_up_to(
            at_most * (flushes.len() * input + writer),
            2 * input + writer,
        )?;
        let fan_in =
            (bytes.saturating_sub(writer) / input.max(1)).clamp(2, self.options.max_open_files);
        let flushes = match self.reduce_flushes(flushes, fan_in).await {
            Ok(flushes) => flushes,
            Err(error) => {
                self.budget.release(bytes);
                return Err(error);
            }
        };
        // a merged flush can have larger blocks than the ones it replaced
        let per_file = flushes.len() * spill.input_bytes(&flushes) + writer;
        let concurrent = (bytes / per_file.max(1)).clamp(1, at_most);
        let index = index_bytes(&flushes);
        let files = self.options.parallelism;
        let merged = self
            .on_pool(move || spill.merge_flushes(flushes, files, concurrent))
            .await
            .and_then(|merged| merged);
        match merged {
            Ok(merged) => {
                self.budget.replace(bytes + index, merged.index_bytes())?;
                Ok(merged)
            }
            Err(error) => {
                self.budget.release(bytes);
                Err(error)
            }
        }
    }

    /// Merges flushes in blocks a pass at a time, `fan_in` at once, until at
    /// most `fan_in` are left. Each pass writes up to a file per sort thread,
    /// one at a time on one thread, from memory the caller holds.
    async fn reduce_flushes(
        self: &Arc<Self>,
        mut flushes: Vec<BlockFlush>,
        fan_in: usize,
    ) -> Result<Vec<BlockFlush>> {
        while flushes.len() > fan_in {
            // leave exactly `fan_in` when one pass can
            let count = fan_in.min(flushes.len() - fan_in + 1);
            let merging: Vec<BlockFlush> = flushes.drain(..count).collect();
            let index = index_bytes(&merging);
            let spill = Arc::clone(&self.shape().spill);
            let files = self.options.parallelism;
            let merged = self
                .on_pool(move || spill.merge_flushes(merging, files, 1))
                .await??;
            self.budget.replace(index, merged.index_bytes())?;
            flushes.push(merged);
        }
        Ok(flushes)
    }

    /// Merges flushes of Arrow batches into one, group by group.
    async fn compact_batches(
        self: &Arc<Self>,
        flushes: &[BatchFlush],
    ) -> Result<Option<BatchFlush>> {
        let grouped = GroupedFlushes::new(flushes);
        let spill = Arc::clone(&self.shape().spill);
        let writer = spill.batch_writer_bytes(grouped.batch_bytes);
        let Some(plan) = MergePlan::new(
            &self.budget,
            &grouped,
            self.options.parallelism,
            self.options.max_open_files,
            (writer, writer),
        )?
        else {
            return Ok(None);
        };
        let workers = Arc::clone(&self.workers);
        let memory = MergeMemory {
            budget: Arc::clone(&self.budget),
            planned: plan.merge_memory(&grouped),
        };
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
                        match spill.compact(segments, plan.fan_in, &memory) {
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
        if self.shape.read().is_none() {
            // no batch arrived
            return Ok(());
        }
        if self.flushed.lock().is_empty() {
            let buffered = self.take_buffered();
            if buffered.num_rows() == 0 {
                self.budget.release(buffered.held());
                return Ok(());
            }
            let files =
                (buffered.num_rows() / MIN_ROWS_PER_FILE).clamp(1, self.options.parallelism);
            let ranges = self.prepare(buffered).await?;
            match self.output_window(&ranges)? {
                Some(window) => return self.emit_ranges(ranges, window, output).await,
                // the pool cannot hold the output beside the rows, so the rows
                // go to disk and come back as the output is taken
                None => self.write_ranges(ranges, files).await?,
            }
        } else {
            self.flush().await?;
        }
        let on_disk = std::mem::replace(&mut *self.flushed.lock(), Flushes::Blocks(Vec::new()));
        match on_disk {
            Flushes::Blocks(flushes) => self.emit_slices(flushes, output).await,
            Flushes::Batches(flushes) => self.emit_groups(flushes, output).await,
        }
    }

    /// How far ahead of the output sorted ranges are made into batches, sized
    /// to what the pool grants. `None` when it grants too little for even two
    /// small batches.
    fn output_window(&self, ranges: &Ranges) -> Result<Option<Window>> {
        let count = ranges.range_count();
        let row_bytes = ranges.bytes_per_row();
        let range_batches = ranges.num_rows() / count / self.batch_size + 2;
        let wanted = self.options.parallelism * range_batches * self.batch_size * row_bytes;
        let floor = 2 * MIN_OUTPUT_ROWS * row_bytes;
        // the window takes what the floor's credit has left beside the rows,
        // then the pool
        let mut bytes = wanted.max(floor);
        while !self.budget.grant(bytes) {
            if bytes == floor {
                return Ok(None);
            }
            bytes = (bytes / 2).max(floor);
        }
        Ok(Some(Window::plan(
            bytes,
            self.options.parallelism,
            range_batches,
            self.batch_size,
            row_bytes,
        )))
    }

    /// Sends sorted ranges that fit in memory to the output in order.
    async fn emit_ranges(
        self: &Arc<Self>,
        ranges: Ranges,
        window: Window,
        output: Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        let ranges = Arc::new(ranges);
        let count = ranges.range_count();
        let batch_rows = window.batch_rows;
        let result = emit_in_order(count, window.ranges, &output, &self.metrics, |range| {
            let (sender, receiver) = mpsc::channel(window.batches);
            let ranges = Arc::clone(&ranges);
            let budget = Arc::clone(&self.budget);
            self.workers.spawn(move || {
                let _budget = budget;
                if sender.is_closed() {
                    // the output was dropped before this range's turn
                    return;
                }
                let mut batcher = ranges
                    .batcher(batch_rows, |batch| send(&sender, Ok(batch)))
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
        flushes: Vec<BlockFlush>,
        output: Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        let shape = self.shape();
        let input_bytes = shape.spill.input_bytes(&flushes);
        let (slice_rows, row_bytes) = match shape.words.layout.contents {
            Contents::Row => (
                MERGE_SLICE_BLOCKS * shape.spill.block_records(),
                shape.words.layout.inline,
            ),
            Contents::Source => {
                let total =
                    |of: fn(&BlockFlush) -> usize| flushes.iter().map(of).sum::<usize>().max(1);
                let rows = total(BlockFlush::rows);
                (
                    (MERGE_SLICE_BLOCKS * rows / total(BlockFlush::blocks)).max(1),
                    (total(BlockFlush::bytes) / rows).max(1),
                )
            }
        };
        let batch_bytes = row_bytes * self.batch_size;
        let slice_batches = slice_rows / self.batch_size + 2;
        // a slice reads a block of every flush and buffers its own output
        let per_slice = flushes.len() * input_bytes + slice_batches * batch_bytes;
        let at_most =
            (self.options.max_open_files / flushes.len()).clamp(1, self.options.parallelism);
        // a pass writes what it merges, and the output needs two batches at
        // least, however small
        let beside_inputs = shape
            .spill
            .writer_bytes()
            .max(2 * MIN_OUTPUT_ROWS * row_bytes);
        let bytes = self
            .budget
            .take_up_to(at_most * per_slice, 2 * input_bytes + beside_inputs)?;
        // what the grant does not cover in one pass is merged in several
        let fan_in = (bytes.saturating_sub(beside_inputs) / input_bytes.max(1))
            .clamp(2, self.options.max_open_files);
        let flushes = match self.reduce_flushes(flushes, fan_in).await {
            Ok(flushes) => flushes,
            Err(error) => {
                self.budget.release(bytes);
                return Err(error);
            }
        };
        // a merged flush can have larger blocks than the ones it replaced
        let inputs = flushes.len() * shape.spill.input_bytes(&flushes);
        let concurrent = (bytes / (inputs + slice_batches * batch_bytes)).clamp(1, at_most);
        // one slice alone buffers what is left of the grant after its inputs,
        // in smaller batches if two of the usual size do not fit
        let (batches, batch_rows) = if concurrent > 1 {
            (slice_batches, self.batch_size)
        } else {
            let spare = bytes.saturating_sub(inputs);
            let batch_rows = (spare / 2 / row_bytes)
                .clamp(MIN_OUTPUT_ROWS.min(self.batch_size), self.batch_size);
            (
                (spare / (batch_rows * row_bytes)).clamp(2, slice_batches),
                batch_rows,
            )
        };
        self.metrics.merge_concurrency.set_max(concurrent);
        self.metrics.merge_fan_in.set_max(flushes.len());
        let index = index_bytes(&flushes);
        let slices = Arc::new(Slices::new(flushes, slice_rows));
        let result = emit_in_order(slices.len(), concurrent, &output, &self.metrics, |slice| {
            let (sender, receiver) = mpsc::channel(batches);
            let (spill, slices) = (Arc::clone(&shape.spill), Arc::clone(&slices));
            let budget = Arc::clone(&self.budget);
            self.workers.spawn(move || {
                let _budget = budget;
                if sender.is_closed() {
                    // the output was dropped before this slice's turn
                    return;
                }
                let sent =
                    spill.merge_slice(&slices, slice, batch_rows, |batch| send(&sender, Ok(batch)));
                if let Err(error) = sent {
                    // the receiver is gone when the output was dropped
                    let _ = send(&sender, Err(error));
                }
            });
            receiver
        })
        .await;
        self.budget.release(bytes + index);
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
        let shape = self.shape();
        let group_batches = grouped.rows / groups.max(1) / shape.spill_batch_rows + 2;
        let buffered = group_batches * grouped.batch_bytes;
        let spill = Arc::clone(&shape.spill);
        // a pass writes what it merges, and the output needs two batches
        let least = spill
            .batch_writer_bytes(grouped.batch_bytes)
            .max(2 * grouped.batch_bytes);
        let Some(plan) = MergePlan::new(
            &self.budget,
            &grouped,
            self.options.parallelism,
            self.options.max_open_files,
            (buffered.max(least), least),
        )?
        else {
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
        let memory = MergeMemory {
            budget: Arc::clone(&self.budget),
            planned: plan.merge_memory(&grouped),
        };
        let result = emit_in_order(groups, plan.concurrent, &output, &self.metrics, |group| {
            let (sender, receiver) = mpsc::channel(batches);
            let segments = std::mem::take(&mut grouped.by_group[group]);
            let spill = Arc::clone(&spill);
            let memory = memory.clone();
            self.workers.spawn(move || {
                if sender.is_closed() {
                    // the output was dropped before this group's turn
                    return;
                }
                let sent = spill.merge(&segments, &memory, |batch| send(&sender, Ok(batch)));
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
        let spill = Arc::clone(&self.shape().spill);
        let writer = spill.batch_writer_bytes(grouped.batch_bytes);
        let at_once = (plan.bytes / (widest_pass * grouped.input_bytes + writer).max(1))
            .clamp(1, self.options.parallelism);
        let memory = MergeMemory {
            budget: Arc::clone(&self.budget),
            planned: plan.merge_memory(grouped),
        };
        let groups = std::mem::take(&mut grouped.by_group);
        let workers = Arc::clone(&self.workers);
        grouped.by_group = self
            .on_pool(move || {
                let mut reduced = Vec::with_capacity(groups.len());
                for together in groups.chunks(at_once) {
                    let passes = workers.map(together.len(), &|group| {
                        spill.reduce(together[group].clone(), plan.fan_in, &memory)
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
        // the job keeps the sort's memory taken until it stops, should the
        // sort be dropped while it runs
        let budget = Arc::clone(&self.budget);
        self.workers.spawn(move || {
            let _budget = budget;
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
    /// holding the first of `beside` besides its inputs: its output, or the
    /// writer of a pass. The floor is one merge of two inputs with the second
    /// of `beside`.
    ///
    /// `None` when there is nothing to merge.
    fn new(
        budget: &Budget,
        grouped: &GroupedFlushes,
        parallelism: usize,
        max_open_files: usize,
        (beside, least): (usize, usize),
    ) -> Result<Option<Self>> {
        let Some(widest) = grouped.by_group.iter().map(Vec::len).max() else {
            return Ok(None);
        };
        let groups = grouped.by_group.len().min(parallelism).max(1);
        let floor = 2 * grouped.input_bytes + least;
        let fan_in = widest.clamp(2, (max_open_files / groups).max(2));
        let per_merge = fan_in * grouped.input_bytes + beside;
        let bytes = budget.take_up_to(groups * per_merge, floor)?;
        if bytes >= per_merge {
            return Ok(Some(Self {
                concurrent: (bytes / per_merge).clamp(1, groups),
                fan_in,
                bytes,
            }));
        }
        Ok(Some(Self {
            concurrent: 1,
            fan_in: (bytes.saturating_sub(least) / grouped.input_bytes).clamp(2, fan_in),
            bytes,
        }))
    }

    /// What DataFusion's merge of one group may reserve: its inputs.
    fn merge_memory(&self, grouped: &GroupedFlushes) -> usize {
        self.fan_in * grouped.input_bytes
    }
}

/// How far ahead of the output sorted ranges are made into batches.
struct Window {
    /// Ranges in flight at once.
    ranges: usize,
    /// Output batches buffered per range.
    batches: usize,
    /// Rows in an output batch.
    batch_rows: usize,
    /// Bytes taken from the budget, for the caller to release.
    bytes: usize,
}

impl Window {
    /// Divides `bytes` among the ranges in flight. The goal is `parallelism`
    /// ranges in flight, each buffering all of its output so it never waits
    /// for the ranges before it. Less buffers two batches of one range, of
    /// fewer than `batch_size` rows if two of those do not fit.
    fn plan(
        bytes: usize,
        parallelism: usize,
        range_batches: usize,
        batch_size: usize,
        row_bytes: usize,
    ) -> Self {
        let batch_bytes = (batch_size * row_bytes).max(1);
        let range_bytes = range_batches * batch_bytes;
        let ranges = (bytes / range_bytes).clamp(1, parallelism);
        let batch_rows =
            (bytes / 2 / row_bytes.max(1)).clamp(MIN_OUTPUT_ROWS.min(batch_size), batch_size);
        let batches = if ranges > 1 {
            range_batches
        } else {
            (bytes / (batch_rows * row_bytes).max(1)).clamp(2, range_batches.max(2))
        };
        Self {
            ranges,
            batches,
            batch_rows,
            bytes,
        }
    }
}

/// Sends every part's batches to `output` in order, keeping up to `window`
/// parts started ahead of the one being sent.
///
/// Returns once every part it started has stopped, also when the output was
/// dropped or a part failed: the parts hold memory that the caller gives back
/// when this returns.
async fn emit_in_order(
    parts: usize,
    window: usize,
    output: &Sender<Result<RecordBatch>>,
    metrics: &SortMetrics,
    mut start: impl FnMut(usize) -> Receiver<Result<RecordBatch>>,
) -> Result<()> {
    let mut started: VecDeque<Receiver<Result<RecordBatch>>> = VecDeque::new();
    let mut next = 0;
    let result = 'parts: loop {
        while started.len() < window && next < parts {
            started.push_back(start(next));
            next += 1;
        }
        let Some(part) = started.front_mut() else {
            break Ok(());
        };
        while let Some(batch) = part.recv().await {
            let batch = match batch {
                Ok(batch) => batch,
                Err(error) => break 'parts Err(error),
            };
            metrics.output_rows.add(batch.num_rows());
            let _timer = metrics.output_wait_time.timer();
            if output.send(Ok(batch)).await.is_err() {
                // the consumer stopped reading
                break 'parts Ok(());
            }
        }
        started.pop_front();
    };
    for mut part in started {
        // a part's next send fails once its receiver is closed, and it drops
        // its sender when it stops
        part.close();
        while part.recv().await.is_some() {}
    }
    result
}

#[cfg(test)]
mod tests;
