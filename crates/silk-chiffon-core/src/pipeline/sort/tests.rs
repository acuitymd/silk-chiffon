use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
        Float64Array, Int16Array, Int32Array, LargeStringArray, ListArray, RecordBatch,
        StringArray, StructArray, TimestampMicrosecondArray, UInt32Array,
    },
    compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch},
    datatypes::{DataType, Field, Int32Type, Schema, SchemaRef},
};
use datafusion::{
    common::Result,
    config::SpillCompression,
    execution::{
        TaskContext,
        memory_pool::{FairSpillPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation},
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    physical_plan::{ExecutionPlan, collect},
    prelude::SessionConfig,
};
use datafusion_datasource::memory::MemorySourceConfig;
use futures::StreamExt;

use super::{ParallelSortExec, ParallelSortOptions};

const ASCENDING: SortOptions = SortOptions {
    descending: false,
    nulls_first: true,
};
const DESCENDING_NULLS_LAST: SortOptions = SortOptions {
    descending: true,
    nulls_first: false,
};

/// A small deterministic generator, so failures reproduce.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int16, true),
        Field::new("code", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("row", DataType::UInt32, false),
    ]))
}

/// Batches of `rows_per_batch` rows. `row` is unique across all of them, so
/// every row is distinguishable and a lost or repeated row shows.
fn batches(
    random: &mut Random,
    count: u32,
    rows_per_batch: u32,
    first_row: u32,
    code: impl Fn(&mut Random, u32) -> Option<i32>,
) -> Vec<RecordBatch> {
    let schema = schema();
    (0..count)
        .map(|batch| {
            let base = first_row + batch * rows_per_batch;
            let rows = base..base + rows_per_batch;
            let year: Int16Array = rows
                .clone()
                .map(|_| (random.below(20) != 0).then(|| 2020 + small::<i16>(random.below(6))))
                .collect();
            let codes: Int32Array = rows.clone().map(|row| code(random, row)).collect();
            let names: StringArray = rows
                .clone()
                .map(|_| {
                    (random.below(10) != 0).then(|| {
                        // most names agree on more bytes than a key prefix holds
                        format!("a common prefix of names {:04}", random.below(500))
                    })
                })
                .collect();
            let row: UInt32Array = rows.collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(year) as ArrayRef,
                    Arc::new(codes),
                    Arc::new(names),
                    Arc::new(row),
                ],
            )
            .unwrap()
        })
        .collect()
}

/// A small random value as the integer type a column needs.
fn small<T: TryFrom<u64>>(value: u64) -> T {
    T::try_from(value).ok().expect("the value fits the column")
}

fn random_code(random: &mut Random, _row: u32) -> Option<i32> {
    (random.below(15) != 0).then(|| small::<i32>(random.below(2000)) - 1000)
}

fn ordering(schema: &SchemaRef, columns: &[(&str, SortOptions)]) -> LexOrdering {
    LexOrdering::new(columns.iter().map(|(name, options)| {
        PhysicalSortExpr::new(
            Arc::new(Column::new_with_schema(name, schema).unwrap()),
            *options,
        )
    }))
    .unwrap()
}

fn runtime(memory_limit: usize) -> Arc<RuntimeEnv> {
    Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(PeakPool {
                inner: FairSpillPool::new(memory_limit),
                peak: AtomicUsize::new(0),
            }))
            .build()
            .unwrap(),
    )
}

/// A pool that remembers the most it ever had reserved, so a test can check
/// that the sort kept within it.
#[derive(Debug)]
struct PeakPool {
    inner: FairSpillPool,
    peak: AtomicUsize,
}

impl PeakPool {
    fn note(&self) {
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
    }
}

impl fmt::Display for PeakPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "peak({})", self.inner)
    }
}

impl MemoryPool for PeakPool {
    fn name(&self) -> &str {
        "peak"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.note();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.note();
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

struct Sorted {
    batches: Vec<RecordBatch>,
    spill_files: usize,
    compactions: usize,
    /// The most ranges that buffered rows were split into.
    ranges: usize,
    flushes: usize,
    /// Flushes that one merge for the output read at once.
    merge_fan_in: usize,
    /// Bytes in one of the records that were sorted.
    record_bytes: usize,
    /// Rows whose keys were too long for their records.
    cut_keys: usize,
    /// Times the records were made wider.
    widenings: usize,
}

/// The schema of the input, which is [`schema`] or [`narrow`]'s part of it.
fn schema_of(partitions: &[Vec<RecordBatch>]) -> SchemaRef {
    partitions
        .iter()
        .flatten()
        .next()
        .map_or_else(schema, RecordBatch::schema)
}

/// Drops the string column, which leaves only fixed-width columns. The sort
/// then keeps whole rows in its records instead of the input batches.
fn narrow(partitions: Vec<Vec<RecordBatch>>) -> Vec<Vec<RecordBatch>> {
    partitions
        .into_iter()
        .map(|batches| {
            batches
                .into_iter()
                .map(|batch| batch.project(&[0, 1, 3]).unwrap())
                .collect()
        })
        .collect()
}

/// Adds a column that the row format cannot encode, a dictionary of lists.
/// The sort then keeps the input's batches, gathers its output from them, and
/// spills them as Arrow batches.
fn with_unencodable_column(partitions: Vec<Vec<RecordBatch>>) -> Vec<Vec<RecordBatch>> {
    let values = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), None]),
        Some(vec![]),
        Some(vec![Some(7), Some(8), Some(9)]),
    ]));
    partitions
        .into_iter()
        .map(|batches| {
            batches
                .into_iter()
                .map(|batch| {
                    let keys: Int32Array = (0..batch.num_rows())
                        .map(|row| i32::try_from(row % 3).ok().filter(|_| row % 7 != 0))
                        .collect();
                    let lists =
                        DictionaryArray::try_new(keys, Arc::clone(&values) as ArrayRef).unwrap();
                    let mut fields = batch.schema().fields().to_vec();
                    fields.push(Arc::new(Field::new(
                        "lists",
                        lists.data_type().clone(),
                        true,
                    )));
                    let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(lists));
                    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
                })
                .collect()
        })
        .collect()
}

/// Puts `start` before every name. A start of a hundred bytes makes a key
/// that no record holds whole.
fn with_longer_names(batch: &RecordBatch, start: &str) -> RecordBatch {
    let names: StringArray = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|name| name.map(|name| format!("{start}{name}")))
        .collect();
    let mut columns = batch.columns().to_vec();
    columns[2] = Arc::new(names);
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

async fn sort(
    partitions: Vec<Vec<RecordBatch>>,
    columns: &[(&str, SortOptions)],
    options: ParallelSortOptions,
    memory_limit: usize,
) -> Sorted {
    sort_compressed(
        partitions,
        columns,
        options,
        memory_limit,
        SpillCompression::Uncompressed,
    )
    .await
}

async fn sort_compressed(
    partitions: Vec<Vec<RecordBatch>>,
    columns: &[(&str, SortOptions)],
    options: ParallelSortOptions,
    memory_limit: usize,
    compression: SpillCompression,
) -> Sorted {
    let schema = schema_of(&partitions);
    let input = MemorySourceConfig::try_new_exec(&partitions, Arc::clone(&schema), None).unwrap();
    let plan: Arc<dyn ExecutionPlan> =
        Arc::new(ParallelSortExec::try_new(input, ordering(&schema, columns), options).unwrap());
    let mut config = SessionConfig::new().with_batch_size(512);
    config.options_mut().execution.spill_compression = compression;
    let context = Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime(memory_limit)),
    );
    let batches = collect(Arc::clone(&plan), Arc::clone(&context))
        .await
        .unwrap();
    let pool = &context.runtime_env().memory_pool;
    assert_eq!(
        pool.reserved(),
        0,
        "the sort returns every byte it reserved"
    );
    let peak = pool
        .downcast_ref::<PeakPool>()
        .expect("the tests' pool")
        .peak
        .load(Ordering::Relaxed);
    assert!(
        peak <= memory_limit,
        "the sort reserved {peak} bytes of a {memory_limit}-byte pool"
    );
    let metrics = plan.metrics().unwrap();
    Sorted {
        batches,
        spill_files: metrics.spill_count().unwrap_or(0),
        compactions: metrics
            .sum_by_name("spill_compactions")
            .map_or(0, |value| value.as_usize()),
        ranges: metrics
            .sum_by_name("ranges")
            .map_or(0, |value| value.as_usize()),
        flushes: metrics
            .sum_by_name("spilled_runs")
            .map_or(0, |value| value.as_usize()),
        merge_fan_in: metrics
            .sum_by_name("merge_fan_in")
            .map_or(0, |value| value.as_usize()),
        record_bytes: metrics
            .sum_by_name("record_bytes")
            .map_or(0, |value| value.as_usize()),
        cut_keys: metrics
            .sum_by_name("cut_keys")
            .map_or(0, |value| value.as_usize()),
        widenings: metrics
            .sum_by_name("record_widenings")
            .map_or(0, |value| value.as_usize()),
    }
}

fn sort_columns(batch: &RecordBatch, columns: &[(&str, SortOptions)]) -> Vec<SortColumn> {
    columns
        .iter()
        .map(|(name, options)| SortColumn {
            values: Arc::clone(batch.column_by_name(name).unwrap()),
            options: Some(*options),
        })
        .collect()
}

/// Checks `sorted` against Arrow's own sort of the same rows: the key columns
/// must match exactly, and the rows must be the same set.
fn assert_sorted(
    input: &[Vec<RecordBatch>],
    sorted: &[RecordBatch],
    columns: &[(&str, SortOptions)],
) {
    let schema = schema_of(input);
    let input = concat_batches(&schema, input.iter().flatten()).unwrap();
    let sorted = concat_batches(&schema, sorted).unwrap();
    assert_eq!(sorted.schema(), schema);
    assert_eq!(sorted.num_rows(), input.num_rows());

    let expected = lexsort_to_indices(&sort_columns(&input, columns), None).unwrap();
    let expected = take_record_batch(&input, &expected).unwrap();
    for (name, _) in columns {
        assert_eq!(
            sorted.column_by_name(name).unwrap(),
            expected.column_by_name(name).unwrap(),
            "column {name} is out of order"
        );
    }

    // rows with equal keys may come out in any order, so compare the rows in
    // an order that depends on every column
    // `row` is unique, so it alone fixes the order
    let canonical = |batch: &RecordBatch| {
        let indices =
            lexsort_to_indices(&sort_columns(batch, &[("row", ASCENDING)]), None).unwrap();
        take_record_batch(batch, &indices).unwrap()
    };
    assert_eq!(canonical(&sorted), canonical(&input));
}

const EXPORT_KEY: [(&str, SortOptions); 3] = [
    ("year", ASCENDING),
    ("code", DESCENDING_NULLS_LAST),
    ("row", ASCENDING),
];

/// Enough memory to hold every test's input.
const AMPLE: usize = 64 * 1024 * 1024;

fn options(parallelism: usize, min_run_bytes: usize) -> ParallelSortOptions {
    ParallelSortOptions {
        parallelism,
        min_run_bytes,
        // what a process that may open 1024 files gets, whatever this one may
        max_open_files: 256,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorts_fixed_width_keys_in_memory() {
    let mut random = Random(7);
    let input: Vec<_> = (0..4)
        .map(|partition| batches(&mut random, 20, 700, partition * 100_000, random_code))
        .collect();
    let sorted = sort(
        input.clone(),
        &EXPORT_KEY,
        ParallelSortOptions::new(4),
        AMPLE,
    )
    .await;
    assert_eq!(sorted.spill_files, 0);
    assert_sorted(&input, &sorted.batches, &EXPORT_KEY);
    assert!(sorted.batches.iter().all(|batch| batch.num_rows() <= 512));
}

/// With only fixed-width columns the records hold whole rows, and the output
/// is decoded from them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorts_whole_row_records_in_memory_and_spilled() {
    let mut random = Random(41);
    let input: Vec<_> = (0..4)
        .map(|partition| batches(&mut random, 30, 1000, partition * 1_000_000, random_code))
        .collect();
    let input = narrow(input);
    // `code` is the only key, so `year` and `row` ride along as payload
    let key = [("code", DESCENDING_NULLS_LAST)];
    for (memory_limit, spills) in [(AMPLE, false), (1024 * 1024, true)] {
        let sorted = sort(input.clone(), &key, options(4, 128 * 1024), memory_limit).await;
        assert_eq!(sorted.spill_files > 0, spills);
        assert_sorted(&input, &sorted.batches, &key);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorts_variable_width_keys_in_memory() {
    let mut random = Random(11);
    let key = [("name", DESCENDING_NULLS_LAST), ("code", ASCENDING)];
    let input: Vec<_> = (0..3)
        .map(|partition| batches(&mut random, 15, 900, partition * 100_000, random_code))
        .collect();
    let sorted = sort(input.clone(), &key, ParallelSortOptions::new(4), AMPLE).await;
    assert_sorted(&input, &sorted.batches, &key);
}

/// Rows enough for many ranges are split into many, however few input
/// partitions they came from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn splits_many_rows_into_many_ranges() {
    let mut random = Random(53);
    let input: Vec<_> = (0..2)
        .map(|partition| batches(&mut random, 25, 8000, partition * 1_000_000, random_code))
        .collect();
    let input = narrow(input);
    let sorted = sort(
        input.clone(),
        &EXPORT_KEY,
        ParallelSortOptions::new(4),
        AMPLE,
    )
    .await;
    assert_eq!(sorted.spill_files, 0);
    // 400,000 rows are worth far more than one range
    assert!(sorted.ranges > 8, "{} ranges", sorted.ranges);
    assert_sorted(&input, &sorted.batches, &EXPORT_KEY);
}

/// A pool too small for the input makes the sort flush to disk and merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spills_and_merges_when_the_pool_is_small() {
    let mut random = Random(13);
    let input: Vec<_> = (0..4)
        .map(|partition| batches(&mut random, 40, 1000, partition * 1_000_000, random_code))
        .collect();
    let sorted = sort(
        input.clone(),
        &EXPORT_KEY,
        options(4, 256 * 1024),
        2 * 1024 * 1024,
    )
    .await;
    assert!(sorted.spill_files > 1, "{} spill files", sorted.spill_files);
    assert_sorted(&input, &sorted.batches, &EXPORT_KEY);
}

/// Flushes in blocks are compressed a block at a time, whether the blocks
/// hold whole-row records or rows in row format.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spills_compressed_blocks() {
    let mut random = Random(61);
    let input: Vec<_> = (0..4)
        .map(|partition| batches(&mut random, 40, 1000, partition * 1_000_000, random_code))
        .collect();
    for input in [narrow(input.clone()), input] {
        for compression in [SpillCompression::Lz4Frame, SpillCompression::Zstd] {
            let sorted = sort_compressed(
                input.clone(),
                &EXPORT_KEY,
                options(4, 256 * 1024),
                2 * 1024 * 1024,
                compression,
            )
            .await;
            assert!(sorted.spill_files > 1, "{} spill files", sorted.spill_files);
            assert_sorted(&input, &sorted.batches, &EXPORT_KEY);
        }
    }
}

/// A record holds a key of variable width whole when the first batch's keys
/// say it can, so sorting such keys never reads them from anywhere else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn holds_variable_width_keys_whole_in_records() {
    let mut random = Random(71);
    let key = [("name", ASCENDING), ("row", DESCENDING_NULLS_LAST)];
    let input: Vec<_> = (0..3)
        .map(|partition| batches(&mut random, 20, 1000, partition * 1_000_000, random_code))
        .collect();
    for memory_limit in [AMPLE, 2 * 1024 * 1024] {
        let sorted = sort(input.clone(), &key, options(3, 256 * 1024), memory_limit).await;
        // a name of 28 bytes is 37 in row format, and `row` is 5: with a
        // byte for the key's length and the row a record names, 56
        assert_eq!(sorted.record_bytes, 56);
        assert_eq!(sorted.cut_keys, 0);
        assert_sorted(&input, &sorted.batches, &key);
    }
}

/// Keys longer than the first batch's make the records wider for the runs
/// after the first, so that the records alone order those.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_longer_than_the_first_batch_had_widen_later_runs() {
    let mut random = Random(73);
    let key = [("name", DESCENDING_NULLS_LAST), ("row", ASCENDING)];
    // one partition, so that the first batch is the one that arrives first
    let mut input = batches(&mut random, 30, 1000, 0, random_code);
    for batch in input.iter_mut().skip(1).step_by(3) {
        // long names that agree for longer than the records hold
        *batch = with_longer_names(batch, "a name that outgrew the first batch: ");
    }
    let input = vec![input];
    // in memory the one run is sorted as it was buffered, with the long names
    // cut. A spilled sort widens the records from the second run on, and cuts
    // only the long names in its first run
    let in_memory = sort(input.clone(), &key, options(3, 256 * 1024), AMPLE).await;
    assert_eq!(in_memory.widenings, 0);
    assert!(in_memory.cut_keys > 5000, "{} cut keys", in_memory.cut_keys);
    assert_sorted(&input, &in_memory.batches, &key);
    let spilled = sort(input.clone(), &key, options(3, 256 * 1024), 2 * 1024 * 1024).await;
    assert!(spilled.widenings > 0);
    assert!(
        spilled.cut_keys < in_memory.cut_keys,
        "{} cut keys",
        spilled.cut_keys
    );
    assert_sorted(&input, &spilled.batches, &key);
}

/// A block of rows has room for 64 times its first key, so keys of a
/// kilobyte after a first batch of short ones make blocks far larger than the
/// floor planned writers for, and merge passes write blocks larger than they
/// read. The sort asks the pool for both, and still sorts in a small one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_far_longer_than_the_first_batch_had_grow_blocks_within_the_pool() {
    let mut random = Random(109);
    let key = [("name", ASCENDING), ("row", DESCENDING_NULLS_LAST)];
    let start = "a start far longer than any key of the first batch, ".repeat(20);
    // one partition, so that the first batch is the one that arrives first
    let mut input = batches(&mut random, 30, 1000, 0, random_code);
    for batch in input.iter_mut().skip(1).step_by(2) {
        *batch = with_longer_names(batch, &start);
    }
    let input = vec![input];
    let sorted = sort(input.clone(), &key, options(3, 16 * 1024), 2 * 1024 * 1024).await;
    assert!(
        sorted.merge_fan_in < sorted.flushes,
        "{} flushes at once of {}",
        sorted.merge_fan_in,
        sorted.flushes
    );
    assert_sorted(&input, &sorted.batches, &key);
}

/// Keys too long for any record are sorted by a prefix that is respelled for
/// each run, with ties broken through the row format.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorts_keys_too_long_for_a_record() {
    let mut random = Random(79);
    let key = [("name", ASCENDING), ("code", DESCENDING_NULLS_LAST)];
    let start = "a start that every name shares, and that is far longer than a record: ".repeat(4);
    let input: Vec<_> = (0..3)
        .map(|partition| {
            batches(&mut random, 20, 1000, partition * 1_000_000, random_code)
                .iter()
                .map(|batch| with_longer_names(batch, &start))
                .collect::<Vec<_>>()
        })
        .collect();
    for memory_limit in [AMPLE, 2 * 1024 * 1024] {
        let sorted = sort(input.clone(), &key, options(3, 256 * 1024), memory_limit).await;
        // sixteen bytes of key and the row the record names
        assert_eq!(sorted.record_bytes, 24);
        assert_sorted(&input, &sorted.batches, &key);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spills_variable_width_keys() {
    let mut random = Random(17);
    let key = [("name", ASCENDING), ("row", DESCENDING_NULLS_LAST)];
    let input: Vec<_> = (0..4)
        .map(|partition| batches(&mut random, 30, 1000, partition * 1_000_000, random_code))
        .collect();
    let sorted = sort(input.clone(), &key, options(3, 256 * 1024), 2 * 1024 * 1024).await;
    assert!(sorted.spill_files > 1);
    assert_sorted(&input, &sorted.batches, &key);
}

/// A pool barely larger than the sort's floor still sorts: it flushes many
/// small runs and merges them a few at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pool_little_larger_than_the_floor_still_sorts() {
    let mut random = Random(19);
    let input: Vec<_> = (0..3)
        .map(|partition| batches(&mut random, 30, 1000, partition * 1_000_000, random_code))
        .collect();
    let sorted = sort(
        input.clone(),
        &EXPORT_KEY,
        options(2, 16 * 1024),
        256 * 1024,
    )
    .await;
    assert!(sorted.spill_files > 8, "{} spill files", sorted.spill_files);
    assert_sorted(&input, &sorted.batches, &EXPORT_KEY);
}

/// Where the pool lets one merge read fewer flushes than there are, they are
/// merged down to that before the output starts, whether they are rows in
/// blocks or Arrow batches in groups. A merge of zstd blocks holds a
/// decompressor for every flush it reads, and one of Arrow batches several
/// batches of each, which is what keeps these merges narrow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merges_flushes_in_passes_when_the_pool_is_tight() {
    let mut random = Random(23);
    let key = [("name", ASCENDING), ("row", DESCENDING_NULLS_LAST)];
    let input: Vec<_> = (0..3)
        .map(|partition| batches(&mut random, 30, 1000, partition * 1_000_000, random_code))
        .collect();
    let cases = [
        (input.clone(), 1024 * 1024, SpillCompression::Zstd),
        (
            with_unencodable_column(input),
            768 * 1024,
            SpillCompression::Uncompressed,
        ),
    ];
    for (input, memory_limit, compression) in cases {
        let sorted = sort_compressed(
            input.clone(),
            &key,
            options(3, 16 * 1024),
            memory_limit,
            compression,
        )
        .await;
        assert!(
            sorted.merge_fan_in < sorted.flushes,
            "{compression:?}: {} flushes at once of {}",
            sorted.merge_fan_in,
            sorted.flushes
        );
        assert_sorted(&input, &sorted.batches, &key);
    }
}

/// A dictionary of booleans is a column the row format encodes and Arrow
/// cannot rebuild from what it decodes to. Its rows are kept as batches too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorts_a_dictionary_column_arrow_cannot_rebuild() {
    let mut random = Random(83);
    let values: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
    let input: Vec<Vec<RecordBatch>> = (0..3)
        .map(|partition| {
            batches(&mut random, 20, 1000, partition * 1_000_000, random_code)
                .into_iter()
                .map(|batch| {
                    let keys: Int32Array = (0..batch.num_rows())
                        .map(|row| i32::try_from(row % 2).ok().filter(|_| row % 5 != 0))
                        .collect();
                    let flags = DictionaryArray::try_new(keys, Arc::clone(&values)).unwrap();
                    let mut fields = batch.schema().fields().to_vec();
                    fields.push(Arc::new(Field::new(
                        "flags",
                        flags.data_type().clone(),
                        true,
                    )));
                    let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(flags));
                    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
                })
                .collect()
        })
        .collect();
    let key = [("name", ASCENDING), ("row", DESCENDING_NULLS_LAST)];
    for (memory_limit, spills) in [(AMPLE, false), (2 * 1024 * 1024, true)] {
        let sorted = sort(input.clone(), &key, options(3, 256 * 1024), memory_limit).await;
        assert_eq!(sorted.spill_files > 0, spills);
        assert_sorted(&input, &sorted.batches, &key);
    }
}

/// A dictionary in a field that may not be null, with keys that name a null
/// value. Its rows are kept as batches, which keep the keys as they are.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorts_a_required_dictionary_that_names_a_null_value() {
    let mut random = Random(103);
    let values: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None]));
    let input: Vec<Vec<RecordBatch>> = (0..2)
        .map(|partition| {
            batches(&mut random, 6, 500, partition * 1_000_000, random_code)
                .into_iter()
                .map(|batch| {
                    let keys: Int32Array = (0..batch.num_rows())
                        .map(|row| Some(i32::try_from(row % 2).unwrap()))
                        .collect();
                    let tags = DictionaryArray::try_new(keys, Arc::clone(&values)).unwrap();
                    let mut fields = batch.schema().fields().to_vec();
                    fields.push(Arc::new(Field::new(
                        "tags",
                        tags.data_type().clone(),
                        false,
                    )));
                    let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(tags));
                    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
                })
                .collect()
        })
        .collect();
    for memory_limit in [AMPLE, 512 * 1024] {
        let sorted = sort(
            input.clone(),
            &EXPORT_KEY,
            options(2, 64 * 1024),
            memory_limit,
        )
        .await;
        assert_sorted(&input, &sorted.batches, &EXPORT_KEY);
    }
}

/// A session batch size below the fewest rows the sort would otherwise put
/// in a batch of output still sorts, in memory and spilled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorts_with_batches_smaller_than_the_fewest_output_rows() {
    let mut random = Random(103);
    let input: Vec<_> = (0..2)
        .map(|partition| batches(&mut random, 4, 300, partition * 1_000_000, random_code))
        .collect();
    let schema = schema_of(&input);
    for (memory_limit, spills) in [(AMPLE, false), (160 * 1024, true)] {
        let source = MemorySourceConfig::try_new_exec(&input, Arc::clone(&schema), None).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            ParallelSortExec::try_new(
                source,
                ordering(&schema, &EXPORT_KEY),
                options(2, 16 * 1024),
            )
            .unwrap(),
        );
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(64))
                .with_runtime(runtime(memory_limit)),
        );
        let sorted = collect(Arc::clone(&plan), Arc::clone(&context))
            .await
            .unwrap();
        assert_sorted(&input, &sorted, &EXPORT_KEY);
        let spill_files = plan.metrics().unwrap().spill_count().unwrap_or(0);
        assert_eq!(spill_files > 0, spills, "{spill_files} spill files");
        let pool = &context.runtime_env().memory_pool;
        assert_eq!(pool.reserved(), 0);
        let peak = pool
            .downcast_ref::<PeakPool>()
            .unwrap()
            .peak
            .load(Ordering::Relaxed);
        assert!(peak <= memory_limit, "{peak} of {memory_limit}");
    }
}

/// A pool too small for the sort's floor fails the sort, saying how much it
/// needs, and keeps nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pool_too_small_for_the_floor_fails_and_says_what_it_needs() {
    let mut random = Random(101);
    let input: Vec<_> = (0..2)
        .map(|partition| batches(&mut random, 3, 200, partition * 1_000_000, random_code))
        .collect();
    let schema = schema_of(&input);
    let source = MemorySourceConfig::try_new_exec(&input, Arc::clone(&schema), None).unwrap();
    let plan: Arc<dyn ExecutionPlan> = Arc::new(
        ParallelSortExec::try_new(
            source,
            ordering(&schema, &EXPORT_KEY),
            ParallelSortOptions::new(2),
        )
        .unwrap(),
    );
    let context = Arc::new(TaskContext::default().with_runtime(runtime(1024)));
    let error = collect(plan, Arc::clone(&context)).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("the memory pool is too small for this sort, which needs"),
        "{error}"
    );
    assert_eq!(context.runtime_env().memory_pool.reserved(), 0);
}

/// A key whose dictionary values are long is a copy of a long value for every
/// row in row format, so the merge of what such a sort spilled as Arrow
/// batches holds far more for each batch's keys than the batch itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merges_spilled_batches_whose_keys_are_long_dictionary_values() {
    let mut random = Random(107);
    let values = Arc::new(StringArray::from_iter_values((0..4).map(|value| {
        format!(
            "{value}{}",
            "a long value that every row repeats ".repeat(30)
        )
    })));
    let input: Vec<_> = (0..3)
        .map(|partition| {
            batches(&mut random, 20, 500, partition * 1_000_000, random_code)
                .into_iter()
                .map(|batch| {
                    let keys: Int32Array = (0..batch.num_rows())
                        .map(|row| Some(i32::try_from((row * 7) % 4).unwrap()))
                        .collect();
                    let tags =
                        DictionaryArray::try_new(keys, Arc::clone(&values) as ArrayRef).unwrap();
                    let mut fields = batch.schema().fields().to_vec();
                    fields.push(Arc::new(Field::new("tag", tags.data_type().clone(), true)));
                    let mut columns = batch.columns().to_vec();
                    columns.push(Arc::new(tags));
                    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
                })
                .collect()
        })
        .collect();
    let input = with_unencodable_column(input);
    let key = [("tag", ASCENDING), ("row", ASCENDING)];
    let sorted = sort(input.clone(), &key, options(3, 64 * 1024), 8 * 1024 * 1024).await;
    assert!(sorted.spill_files > 1, "{} spill files", sorted.spill_files);
    assert_sorted(&input, &sorted.batches, &key);
}

/// A plan says which of the three ways its sort holds rows, which is how a
/// sort that fell back to keeping batches is told from the others.
#[test]
fn a_plan_says_how_its_sort_holds_rows() {
    use datafusion::physical_plan::displayable;

    let mut random = Random(89);
    let input = vec![batches(&mut random, 1, 10, 0, random_code)];
    let shown = |input: Vec<Vec<RecordBatch>>| {
        let schema = schema_of(&input);
        let source = MemorySourceConfig::try_new_exec(&input, Arc::clone(&schema), None).unwrap();
        let plan = ParallelSortExec::try_new(
            source,
            ordering(&schema, &[("code", ASCENDING)]),
            ParallelSortOptions::new(2),
        )
        .unwrap();
        displayable(&plan).one_line().to_string()
    };
    assert!(shown(narrow(input.clone())).contains("rows=records"));
    assert!(shown(input.clone()).contains("rows=row-format"));
    assert!(shown(with_unencodable_column(input)).contains("rows=batches"));
}

/// A column that the row format cannot encode rides along in the batches it
/// arrived in, in memory and spilled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorts_rows_the_row_format_cannot_hold() {
    let mut random = Random(67);
    let input: Vec<_> = (0..3)
        .map(|partition| batches(&mut random, 30, 1000, partition * 1_000_000, random_code))
        .collect();
    let input = with_unencodable_column(input);
    let keys: [&[(&str, SortOptions)]; 2] = [
        &EXPORT_KEY,
        &[("name", DESCENDING_NULLS_LAST), ("code", ASCENDING)],
    ];
    for key in keys {
        for (memory_limit, spills) in [(AMPLE, false), (2 * 1024 * 1024, true)] {
            let sorted = sort(input.clone(), key, options(3, 256 * 1024), memory_limit).await;
            assert_eq!(sorted.spill_files > 0, spills);
            assert_sorted(&input, &sorted.batches, key);
        }
    }
}

/// Every spill file is an open file descriptor. A sort that needs more flushes
/// than fit under the limit merges the ones it has into one and carries on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_flushes_are_compacted_to_bound_open_files() {
    let mut random = Random(43);
    let input: Vec<_> = (0..3)
        .map(|partition| batches(&mut random, 250, 40, partition * 1_000_000, random_code))
        .collect();
    // the 750 batches become dozens of flushes of one file each, past the
    // limit of eight files
    let options = ParallelSortOptions {
        max_open_files: 8,
        ..options(4, 16 * 1024)
    };
    let sorted = sort(input.clone(), &EXPORT_KEY, options, 512 * 1024).await;
    assert!(sorted.compactions > 0, "{} spill files", sorted.spill_files);
    assert_sorted(&input, &sorted.batches, &EXPORT_KEY);
}

/// Input that arrives already sorted. When it spills, the groups come from
/// the first flush, which holds only the smallest keys, so every later row
/// lands in the last group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorts_input_that_arrives_in_order() {
    let mut random = Random(23);
    let key = [("code", ASCENDING)];
    let input = vec![batches(&mut random, 60, 1000, 0, |_, row| {
        i32::try_from(row).ok()
    })];
    for memory_limit in [AMPLE, 2 * 1024 * 1024] {
        let sorted = sort(input.clone(), &key, options(4, 256 * 1024), memory_limit).await;
        assert_sorted(&input, &sorted.batches, &key);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorts_rows_that_all_share_one_key() {
    let mut random = Random(29);
    let key = [("code", ASCENDING)];
    let input: Vec<_> = (0..2)
        .map(|partition| batches(&mut random, 20, 1000, partition * 1_000_000, |_, _| Some(7)))
        .collect();
    for memory_limit in [AMPLE, 1024 * 1024] {
        let sorted = sort(input.clone(), &key, options(4, 128 * 1024), memory_limit).await;
        assert_sorted(&input, &sorted.batches, &key);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sorts_empty_and_single_row_input() {
    // a key of variable width, which waits for a batch to size its records
    for key in [&EXPORT_KEY[..], &[("name", ASCENDING)]] {
        let sorted = sort(
            vec![vec![], vec![]],
            key,
            ParallelSortOptions::new(2),
            AMPLE,
        )
        .await;
        assert!(sorted.batches.is_empty());
    }

    let mut random = Random(31);
    let input = vec![vec![], batches(&mut random, 1, 1, 0, random_code)];
    let sorted = sort(
        input.clone(),
        &EXPORT_KEY,
        ParallelSortOptions::new(2),
        AMPLE,
    )
    .await;
    assert_sorted(&input, &sorted.batches, &EXPORT_KEY);
}

/// Rows of many kinds of columns: keys the row format encodes with a fixed
/// width and with a variable one, and columns that only ride along, some of
/// them nested.
fn assorted(
    random: &mut Random,
    count: u32,
    rows_per_batch: u32,
    first_row: u32,
) -> Vec<RecordBatch> {
    let label_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let point = vec![
        Arc::new(Field::new("x", DataType::Int32, true)),
        Arc::new(Field::new("tag", DataType::Utf8, true)),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("score", DataType::Float64, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("title", DataType::LargeUtf8, true),
        Field::new("bytes", DataType::Binary, true),
        Field::new("day", DataType::Date32, true),
        Field::new(
            "at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("price", DataType::Decimal128(20, 4), true),
        Field::new("label", label_type, true),
        Field::new("point", DataType::Struct(point.clone().into()), true),
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            true,
        ),
        Field::new("row", DataType::UInt32, false),
    ]));
    (0..count)
        .map(|batch| {
            let base = first_row + batch * rows_per_batch;
            let rows = base..base + rows_per_batch;
            let maybe = |random: &mut Random| random.below(8) != 0;
            let score: Float64Array = rows
                .clone()
                .map(|_| {
                    maybe(random).then(|| match random.below(12) {
                        0 => f64::NAN,
                        1 => -0.0,
                        2 => 0.0,
                        3 => f64::NEG_INFINITY,
                        _ => f64::from(small::<i32>(random.below(2000))) / 8.0 - 100.0,
                    })
                })
                .collect();
            let flag: BooleanArray = rows
                .clone()
                .map(|_| maybe(random).then(|| random.below(2) == 0))
                .collect();
            let title: LargeStringArray = rows
                .clone()
                .map(|_| {
                    maybe(random).then(|| match random.below(4) {
                        0 => String::new(),
                        // longer than a key prefix, and mostly shared
                        _ => format!(
                            "a title shared by many rows, number {:03}",
                            random.below(40)
                        ),
                    })
                })
                .collect();
            let bytes: BinaryArray = rows
                .clone()
                .map(|_| {
                    maybe(random).then(|| {
                        vec![small::<u8>(random.below(3)); small::<usize>(random.below(5))]
                    })
                })
                .collect();
            let day: Date32Array = rows
                .clone()
                .map(|_| maybe(random).then(|| small::<i32>(random.below(40_000)) - 20_000))
                .collect();
            let at = rows
                .clone()
                .map(|_| maybe(random).then(|| small::<i64>(random.below(1 << 40))))
                .collect::<TimestampMicrosecondArray>()
                .with_timezone("UTC");
            let price = rows
                .clone()
                .map(|_| {
                    maybe(random)
                        .then(|| i128::from(small::<i64>(random.below(1 << 50))) - (1 << 49))
                })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(20, 4)
                .unwrap();
            let label: DictionaryArray<Int32Type> = rows
                .clone()
                .map(|_| {
                    maybe(random).then(|| {
                        ["north", "south", "east", "west"][small::<usize>(random.below(4))]
                    })
                })
                .collect();
            let xs: Int32Array = rows
                .clone()
                .map(|_| maybe(random).then(|| small::<i32>(random.below(9))))
                .collect();
            let tags: StringArray = rows
                .clone()
                .map(|_| maybe(random).then(|| format!("t{}", random.below(5))))
                .collect();
            let point = StructArray::new(
                point.clone().into(),
                vec![Arc::new(xs) as ArrayRef, Arc::new(tags)],
                Some(rows.clone().map(|_| maybe(random)).collect()),
            );
            let values =
                ListArray::from_iter_primitive::<Int32Type, _, _>(rows.clone().map(|_| {
                    maybe(random).then(|| {
                        (0..random.below(4))
                            .map(|_| maybe(random).then(|| small::<i32>(random.below(100))))
                            .collect::<Vec<_>>()
                    })
                }));
            let row: UInt32Array = rows.collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(score) as ArrayRef,
                    Arc::new(flag),
                    Arc::new(title),
                    Arc::new(bytes),
                    Arc::new(day),
                    Arc::new(at),
                    Arc::new(price),
                    Arc::new(label),
                    Arc::new(point),
                    Arc::new(values),
                    Arc::new(row),
                ],
            )
            .unwrap()
        })
        .collect()
}

/// The sort takes any column the row format encodes as a key, in either
/// direction, and carries every other column along unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorts_many_kinds_of_columns() {
    let mut random = Random(59);
    let input: Vec<_> = (0..3)
        .map(|partition| assorted(&mut random, 12, 700, partition * 1_000_000))
        .collect();
    let keys: [&[(&str, SortOptions)]; 5] = [
        &[("score", ASCENDING), ("flag", DESCENDING_NULLS_LAST)],
        &[("title", DESCENDING_NULLS_LAST), ("bytes", ASCENDING)],
        &[("price", DESCENDING_NULLS_LAST), ("day", ASCENDING)],
        &[("at", ASCENDING)],
        &[
            ("flag", ASCENDING),
            ("day", DESCENDING_NULLS_LAST),
            ("title", ASCENDING),
        ],
    ];
    for key in keys {
        for memory_limit in [AMPLE, 2 * 1024 * 1024] {
            let sorted = sort(input.clone(), key, options(3, 256 * 1024), memory_limit).await;
            assert_sorted(&input, &sorted.batches, key);
        }
    }
}

/// Dropping the output mid-stream stops the sort instead of leaving its
/// threads blocked on a full channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_the_output_releases_the_sort() {
    let mut random = Random(37);
    let schema = schema();
    let input: Vec<_> = (0..4)
        .map(|partition| batches(&mut random, 40, 1000, partition * 1_000_000, random_code))
        .collect();
    for memory_limit in [AMPLE, 2 * 1024 * 1024] {
        let source = MemorySourceConfig::try_new_exec(&input, Arc::clone(&schema), None).unwrap();
        let plan = ParallelSortExec::try_new(
            source,
            ordering(&schema, &EXPORT_KEY),
            options(4, 256 * 1024),
        )
        .unwrap();
        let runtime = runtime(memory_limit);
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(512))
                .with_runtime(Arc::clone(&runtime)),
        );
        let mut output = plan.execute(0, context).unwrap();
        output.next().await.unwrap().unwrap();
        drop(output);

        // the sort's tasks notice the closed channel and return their memory
        for _ in 0..200 {
            if runtime.memory_pool.reserved() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(runtime.memory_pool.reserved(), 0);
    }
}
