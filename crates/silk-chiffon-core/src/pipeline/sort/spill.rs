//! Sorted rows on disk and the merges that read them back.
//!
//! # Flushes in blocks
//!
//! A flush is written in blocks whenever the sort holds its rows in row
//! format. Records that hold whole rows are written as they are. Records that
//! name rows kept in row format are written as those rows, each with its
//! whole key: see [`rows`]. Either way a flush is cut into a few files of
//! equal size, written in parallel, and keeps the first key of every block in
//! memory. With that index the merge of every flush can be cut into slices of
//! about equal size wherever the keys fall, and a slice can start reading
//! each flush at the right block. The slices merge in parallel however the
//! rows arrived. Nothing is decoded until merged rows become output batches,
//! and a merge holds one block per flush, so its memory is known before it
//! starts.
//!
//! # Flushes of Arrow batches
//!
//! A sort that keeps rows as the batches they arrived in has no rows in row
//! format to write in blocks. It keeps batches when the row format cannot
//! hold one of the columns: see the module docs of [`super`]. Its flushes are
//! written as Arrow batches and merged by DataFusion.
//!
//! - **Groups.** The first flush divides the key range into groups of about
//!   as many rows each, one per sort thread at most. Every flush writes a
//!   file for each group it has rows in, which is a segment. Later flushes
//!   keep the first one's bounds, because a group has to be the same stretch
//!   of keys in every flush.
//! - **Merging.** A group of the output is the merge of that group's segment
//!   from every flush, which DataFusion's streaming merge does. As many
//!   groups merge at once as memory allows, up to the number of sort threads,
//!   and they are sent in key order.
//! - **Memory.** DataFusion's merge reserves memory for the batches it holds,
//!   and fails when the pool refuses. So the sort sizes the merges from its
//!   own budget before they start, and gives them a pool that never refuses.
//!   Where the budget does not cover reading a group's segments at once, the
//!   group is first merged down in passes.
//!
//! This is slower than merging blocks in slices, because the groups are few,
//! large, and fixed:
//!
//! - **A group merged ahead of its turn holds its whole output.** A group's
//!   merge is mostly one thread's work, so merging in parallel means several
//!   groups at once, and each buffers every batch it produces until the
//!   groups before it are sent. A group is about a sort thread's share of the
//!   rows, so a budget too small for two groups' output merges one group at a
//!   time. A slice of blocks is small, and many fit.
//! - **The bounds suit later flushes only as far as the first flush resembled
//!   them.** Input that arrives in key order leaves most rows in the last
//!   group, and that group is then one merge.

mod rows;

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::Arc,
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    config::SpillCompression,
    error::{DataFusionError, Result},
    execution::{
        SendableRecordBatchStream, SpillFile, SpillWriter,
        disk_manager::DiskManager,
        memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool},
        runtime_env::RuntimeEnv,
    },
    physical_expr::LexOrdering,
    physical_plan::{
        SpillManager,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, SpillMetrics},
        sorts::streaming_merge::StreamingMergeBuilder,
    },
};
use futures::StreamExt;

use super::keys::{Contents, RowEncoder};
use super::{
    record::{Order, Words},
    run::{Batcher, Input, Kept, merge_rounds},
    workers::Workers,
};
use rows::RowBatcher;

/// Batches one input of a merge of Arrow files holds at once: the reader's
/// two buffered batches, the one it is decoding, the one the merge is reading,
/// and that batch's keys in row format, which can be larger than the batch.
const MERGE_INPUT_BATCHES: usize = 6;

/// Bytes before each block: the block's length, then its length as stored,
/// each a little-endian `u32`.
const BLOCK_HEADER: usize = 8;

/// What a zstd decompressor holds besides the block it fills.
const ZSTD_CONTEXT_BYTES: usize = 256 * 1024;

/// One group of one flush of Arrow batches, on disk.
#[derive(Clone)]
pub(super) struct Segment {
    file: Arc<dyn SpillFile>,
    rows: usize,
    input_bytes: usize,
    batch_bytes: usize,
}

impl Segment {
    pub(super) fn rows(&self) -> usize {
        self.rows
    }

    /// Memory a merge needs to read this segment.
    pub(super) fn input_bytes(&self) -> usize {
        self.input_bytes
    }

    /// Roughly the largest batch this segment's rows come back as.
    pub(super) fn batch_bytes(&self) -> usize {
        self.batch_bytes
    }
}

/// A flush of Arrow batches: its segments by group. Empty groups have none.
pub(super) type BatchFlush = Vec<Option<Segment>>;

/// What the blocks of a flush hold.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BlockKind {
    /// Records that are whole rows.
    Records,
    /// Rows in row format, each with its whole key.
    Rows,
}

/// One file of a flush in blocks: a stretch of the flush's sorted rows.
struct Part {
    file: Arc<dyn SpillFile>,
    /// Where each block starts in the file, and where the last one ends.
    offsets: Vec<u64>,
}

/// A flush on disk in blocks: its files in key order, and the first key of
/// every block, which says where in the flush any key is.
pub(super) struct BlockFlush {
    parts: Vec<Part>,
    /// The file and the block within it of every block, in key order.
    blocks: Vec<(usize, usize)>,
    /// Rows in each block.
    block_rows: Vec<usize>,
    /// The first key of each block, back to back, as bytes that order the
    /// way the keys do.
    firsts: Vec<u8>,
    /// Where each block's first key ends in `firsts`.
    first_ends: Vec<usize>,
    rows: usize,
    /// Bytes of every block together, as they are read back.
    bytes: usize,
    /// Bytes of the largest block, as it is read back.
    largest_block: usize,
}

impl BlockFlush {
    pub(super) fn files(&self) -> usize {
        self.parts.len()
    }

    pub(super) fn rows(&self) -> usize {
        self.rows
    }

    pub(super) fn blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Bytes of every block together, as they are read back.
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Bytes this flush's index of its blocks holds in memory, where a block
    /// of rows with long keys keeps a long first key.
    pub(super) fn index_bytes(&self) -> usize {
        self.firsts.capacity()
            + size_of::<(usize, usize)>() * self.blocks.capacity()
            + size_of::<usize>() * (self.block_rows.capacity() + self.first_ends.capacity())
            + self
                .parts
                .iter()
                .map(|part| size_of::<u64>() * part.offsets.capacity())
                .sum::<usize>()
    }

    /// Rows in the largest block.
    fn largest_block_rows(&self) -> usize {
        self.block_rows.iter().copied().max().unwrap_or(0)
    }

    fn first(&self, block: usize) -> &[u8] {
        let start = block
            .checked_sub(1)
            .map_or(0, |before| self.first_ends[before]);
        &self.firsts[start..self.first_ends[block]]
    }

    /// The block that a reader of the keys at or after `lower` starts at: the
    /// one before the first that starts at or after `lower`, which may end
    /// with keys that are at or after it.
    fn start(&self, lower: Option<&[u8]>) -> usize {
        lower.map_or(0, |lower| {
            let mut from = (0, self.blocks.len());
            while from.0 < from.1 {
                let middle = (from.0 + from.1) / 2;
                if self.first(middle) < lower {
                    from.0 = middle + 1;
                } else {
                    from.1 = middle;
                }
            }
            from.0.saturating_sub(1)
        })
    }

    /// Puts the files written for consecutive stretches of the keys together.
    fn assemble(written: Vec<Option<(Part, WrittenIndex)>>) -> Self {
        let mut flush = Self {
            parts: Vec::new(),
            blocks: Vec::new(),
            block_rows: Vec::new(),
            firsts: Vec::new(),
            first_ends: Vec::new(),
            rows: 0,
            bytes: 0,
            largest_block: 0,
        };
        for (part, index) in written.into_iter().flatten() {
            flush
                .blocks
                .extend((0..index.block_rows.len()).map(|block| (flush.parts.len(), block)));
            flush.rows += index.block_rows.iter().sum::<usize>();
            flush.block_rows.extend(index.block_rows);
            let before = flush.firsts.len();
            flush
                .first_ends
                .extend(index.first_ends.iter().map(|end| before + end));
            flush.firsts.extend(index.firsts);
            flush.bytes += index.bytes;
            flush.largest_block = flush.largest_block.max(index.largest_block);
            flush.parts.push(part);
        }
        flush
    }
}

/// What a writer learned about the blocks of the file it wrote.
#[derive(Default)]
struct WrittenIndex {
    block_rows: Vec<usize>,
    /// The first key of each block. A writer adds a block's when it starts
    /// the block.
    firsts: Vec<u8>,
    first_ends: Vec<usize>,
    bytes: usize,
    largest_block: usize,
}

impl WrittenIndex {
    /// Notes a block that was written, whose first key is already here.
    fn wrote(&mut self, rows: usize, bytes: usize) {
        self.block_rows.push(rows);
        self.first_ends.push(self.firsts.len());
        self.bytes += bytes;
        self.largest_block = self.largest_block.max(bytes);
    }
}

/// The slices that the merge of some flushes in blocks is cut into, in key
/// order.
pub(super) struct Slices {
    flushes: Vec<BlockFlush>,
    /// The key each slice after the first starts at. Rows with that key are
    /// in the slice it starts.
    bounds: Vec<Vec<u8>>,
}

impl Slices {
    /// Cuts the merge of `flushes` into slices of about `slice_rows` rows.
    pub(super) fn new(flushes: Vec<BlockFlush>, slice_rows: usize) -> Self {
        let mut blocks: Vec<(&[u8], usize)> = flushes
            .iter()
            .flat_map(|flush| {
                (0..flush.blocks.len()).map(|block| (flush.first(block), flush.block_rows[block]))
            })
            .collect();
        blocks.sort_unstable_by(|left, right| left.0.cmp(right.0));
        let mut bounds: Vec<Vec<u8>> = Vec::new();
        let mut rows = 0;
        for (first, block_rows) in blocks {
            // a slice starts at a block's first key, once the blocks that
            // start before it hold a slice's worth of rows
            if rows >= slice_rows && bounds.last().is_none_or(|last| last.as_slice() < first) {
                bounds.push(first.to_vec());
                rows = 0;
            }
            rows += block_rows;
        }
        Self { flushes, bounds }
    }

    pub(super) fn len(&self) -> usize {
        self.bounds.len() + 1
    }

    /// The key a slice starts at and the key the next one starts at. The
    /// first slice has no start and the last has no end.
    fn bounds_of(&self, slice: usize) -> (Option<&[u8]>, Option<&[u8]>) {
        (
            slice
                .checked_sub(1)
                .map(|before| self.bounds[before].as_slice()),
            self.bounds.get(slice).map(Vec::as_slice),
        )
    }
}

/// Writes flushes to disk and merges them back.
///
/// Everything here blocks on file reads and writes, so it runs on the sort's
/// own threads.
pub(super) struct Spill {
    encoder: Arc<RowEncoder>,
    /// How the sort's records are laid out.
    words: Words,
    schema: SchemaRef,
    ordering: LexOrdering,
    batch_size: usize,
    kind: BlockKind,
    /// Records in one block of a flush of records.
    block_records: usize,
    /// Bytes that fill one block of a flush of rows.
    row_block_bytes: usize,
    compression: SpillCompression,
    disk: Arc<DiskManager>,
    metrics: SpillMetrics,
    /// Writes and reads segments of Arrow batches.
    batches: SpillManager,
    /// Drives DataFusion's merge of Arrow segments, which is asynchronous.
    runtime: tokio::runtime::Handle,
    /// The threads that the jobs calling into this run on.
    workers: Arc<Workers>,
    merge_metrics: ExecutionPlanMetricsSet,
    /// DataFusion's merge reserves memory for each batch it holds and fails if
    /// refused. The sort decides the fan-in up front from its own budget
    /// instead, so the merge reserves from a pool that never refuses.
    unlimited: Arc<dyn MemoryPool>,
}

impl Spill {
    /// Must be created inside the runtime that the sort's input runs on.
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new(
        encoder: Arc<RowEncoder>,
        words: Words,
        schema: SchemaRef,
        ordering: LexOrdering,
        batch_size: usize,
        block_records: usize,
        row_block_bytes: usize,
        environment: &Arc<RuntimeEnv>,
        compression: SpillCompression,
        metrics: SpillMetrics,
        workers: Arc<Workers>,
    ) -> Self {
        Self {
            workers,
            kind: match words.layout.contents {
                Contents::Row => BlockKind::Records,
                Contents::Source => BlockKind::Rows,
            },
            encoder,
            words,
            ordering,
            batch_size,
            block_records: block_records.max(1),
            row_block_bytes: row_block_bytes.max(1),
            compression,
            disk: Arc::clone(&environment.disk_manager),
            batches: SpillManager::new(
                Arc::clone(environment),
                metrics.clone(),
                Arc::clone(&schema),
            )
            .with_compression_type(compression),
            schema,
            metrics,
            runtime: tokio::runtime::Handle::current(),
            // the merges' own row and time counts would be added to the
            // sort's, which already counts what it reads and emits
            merge_metrics: ExecutionPlanMetricsSet::new(),
            unlimited: Arc::new(UnboundedMemoryPool::default()),
        }
    }

    fn words(&self) -> Words {
        self.words
    }

    fn block_bytes(&self) -> usize {
        self.block_records * self.words().record_bytes()
    }

    /// Memory a merge needs for one of `flushes`: the block being read, and
    /// the block as it is stored when that is different. A block of records
    /// also needs its records restored from it, and the records it adds to a
    /// round of the merge, which are copied and then sorted with as much again
    /// in the worst case. A block of rows also needs where each row starts.
    pub(super) fn input_bytes(&self, flushes: &[BlockFlush]) -> usize {
        let largest = |of: fn(&BlockFlush) -> usize| flushes.iter().map(of).max().unwrap_or(0);
        let block = match self.kind {
            BlockKind::Records => self.block_bytes(),
            BlockKind::Rows => largest(|flush| flush.largest_block),
        };
        let stored = match self.compression {
            SpillCompression::Uncompressed => 0,
            SpillCompression::Lz4Frame => block,
            SpillCompression::Zstd => block + ZSTD_CONTEXT_BYTES,
        };
        match self.kind {
            // the block as read, its records restored from it, their copy in
            // a round, and what sorting the round moves aside
            BlockKind::Records => 4 * block + stored,
            BlockKind::Rows => {
                block + size_of::<usize>() * (largest(BlockFlush::largest_block_rows) + 1) + stored
            }
        }
    }

    /// Writes one flush as `parts` files, in parallel. `fill` pushes a file's
    /// records in sorted order, and every record of a file sorts before those
    /// of the next. Records that name rows of `kept` are written as those
    /// rows.
    pub(super) fn write_parts(
        &self,
        parts: usize,
        kept: Kept<'_>,
        fill: impl Fn(usize, &mut dyn FnMut(&[u64]) -> Result<()>) -> Result<()> + Sync,
    ) -> Result<BlockFlush> {
        let written = self
            .workers
            .map(parts, &|part| match self.kind {
                BlockKind::Records => {
                    let mut writer = self.record_writer("sort run")?;
                    fill(part, &mut |records| writer.push(records))?;
                    writer.finish()
                }
                BlockKind::Rows => {
                    let mut writer = self.row_writer("sort run")?;
                    fill(part, &mut |records| writer.push_kept(kept, records))?;
                    writer.finish()
                }
            })
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        Ok(BlockFlush::assemble(written))
    }

    /// Writes one flush of Arrow batches as a segment per group, in
    /// parallel. `fill` pushes a group's batches in sorted order. A group
    /// that pushes none has no segment.
    pub(super) fn write_batch_groups(
        &self,
        groups: usize,
        fill: impl Fn(usize, &mut dyn FnMut(&RecordBatch) -> Result<()>) -> Result<()> + Sync,
    ) -> Result<BatchFlush> {
        self.workers
            .map(groups, &|group| {
                self.write_batches("sort run", |push| fill(group, push))
            })
            .into_iter()
            .collect()
    }

    /// Calls `emit` with one slice's rows as sorted batches.
    pub(super) fn merge_slice(
        &self,
        slices: &Slices,
        slice: usize,
        emit: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        match self.kind {
            BlockKind::Records => {
                let mut batcher = Batcher::new(
                    &self.encoder,
                    self.words,
                    &[],
                    &[],
                    &[],
                    self.batch_size,
                    emit,
                );
                self.visit_slice(slices, slice, |records| batcher.push(records))?;
                batcher.finish()
            }
            BlockKind::Rows => {
                let mut batcher = RowBatcher::new(&self.encoder, self.batch_size, emit);
                self.visit_row_slice(slices, slice, |row| batcher.push(row))?;
                batcher.finish()
            }
        }
    }

    /// Merges flushes into one flush of up to `parts` files, at most
    /// `concurrent` files at a time, each of which reads every flush. The
    /// flushes are deleted when this returns.
    pub(super) fn merge_flushes(
        &self,
        flushes: Vec<BlockFlush>,
        parts: usize,
        concurrent: usize,
    ) -> Result<BlockFlush> {
        let rows: usize = flushes.iter().map(BlockFlush::rows).sum();
        let at_once = concurrent.max(1);
        let slices = Slices::new(flushes, rows.div_ceil(parts.max(1)).max(1));
        let order: Vec<usize> = (0..slices.len()).collect();
        let mut written = Vec::new();
        for chunk in order.chunks(at_once) {
            let parts = self
                .workers
                .map(chunk.len(), &|at| match self.kind {
                    BlockKind::Records => {
                        let mut writer = self.record_writer("sort merge pass")?;
                        self.visit_slice(&slices, chunk[at], |records| writer.push(records))?;
                        writer.finish()
                    }
                    BlockKind::Rows => {
                        let mut writer = self.row_writer("sort merge pass")?;
                        self.visit_row_slice(&slices, chunk[at], |row| writer.push_row(row))?;
                        writer.finish()
                    }
                })
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            written.extend(parts);
        }
        Ok(BlockFlush::assemble(written))
    }

    /// Calls `visit` with one slice's records in sorted order.
    fn visit_slice(
        &self,
        slices: &Slices,
        slice: usize,
        visit: impl FnMut(&[u64]) -> Result<()>,
    ) -> Result<()> {
        let words = self.words();
        let (lower, upper) = slices.bounds_of(slice);
        let readers = slices
            .flushes
            .iter()
            .map(|flush| RecordReader::open(self, flush, lower, upper))
            .collect::<Result<Vec<_>>>()?;
        // whole-row records carry their whole key
        merge_rounds(readers, Order::new(words, &[]), &mut Vec::new(), visit)
    }

    /// Calls `emit` with the rows of segments of Arrow batches, merged.
    pub(super) fn merge(
        &self,
        segments: &[Segment],
        emit: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        if segments.is_empty() {
            return Ok(());
        }
        self.merge_batches(segments, emit)
    }

    /// Merges segments into fewer, larger ones until at most `fan_in` remain.
    pub(super) fn reduce(&self, mut segments: Vec<Segment>, fan_in: usize) -> Result<Vec<Segment>> {
        let fan_in = fan_in.max(2);
        while segments.len() > fan_in {
            // leave exactly `fan_in` when one pass can
            let group = fan_in.min(segments.len() - fan_in + 1);
            let merged: Vec<Segment> = segments.drain(..group).collect();
            segments.extend(self.merge_to_file(&merged)?);
        }
        Ok(segments)
    }

    /// Merges segments into one, a few at a time when there are more than
    /// `fan_in`.
    pub(super) fn compact(&self, segments: Vec<Segment>, fan_in: usize) -> Result<Option<Segment>> {
        let mut segments = self.reduce(segments, fan_in)?;
        if segments.len() > 1 {
            return self.merge_to_file(&segments);
        }
        Ok(segments.pop())
    }

    /// Merges segments into one new spill file.
    fn merge_to_file(&self, segments: &[Segment]) -> Result<Option<Segment>> {
        self.write_batches("sort merge pass", |push| {
            self.merge_batches(segments, |batch| push(&batch))
        })
    }

    fn record_writer(&self, what: &str) -> Result<RecordWriter<'_>> {
        Ok(RecordWriter {
            blocks: self.block_writer(what)?,
            block: Vec::with_capacity(self.block_bytes()),
            block_rows: 0,
            index: WrittenIndex::default(),
            transposed: Vec::new(),
            rows: 0,
        })
    }

    fn block_writer(&self, what: &str) -> Result<BlockWriter<'_>> {
        let file = self.disk.create_tmp_file(what)?;
        let out = file.open_writer()?;
        Ok(BlockWriter {
            spill: self,
            file,
            out,
            written: 0,
            offsets: Vec::new(),
            stored: Vec::new(),
            compressor: match self.compression {
                // the fastest level: flushes are written while the input waits
                SpillCompression::Zstd => Some(zstd::bulk::Compressor::new(1)?),
                SpillCompression::Lz4Frame | SpillCompression::Uncompressed => None,
            },
        })
    }

    /// Writes the batches that `fill` pushes to one spill file of Arrow
    /// batches. `None` when it pushes none.
    fn write_batches(
        &self,
        what: &str,
        fill: impl FnOnce(&mut dyn FnMut(&RecordBatch) -> Result<()>) -> Result<()>,
    ) -> Result<Option<Segment>> {
        let mut writer = self.batches.create_in_progress_file(what)?;
        let (mut max_batch_bytes, mut rows) = (0, 0);
        fill(&mut |batch| {
            rows += batch.num_rows();
            max_batch_bytes = max_batch_bytes.max(writer.append_batch(batch)?);
            Ok(())
        })?;
        Ok(writer.finish()?.map(|file| Segment {
            file,
            rows,
            input_bytes: max_batch_bytes * MERGE_INPUT_BATCHES,
            batch_bytes: max_batch_bytes,
        }))
    }

    /// Calls `emit` with the merged batches of segments of Arrow batches.
    fn merge_batches(
        &self,
        segments: &[Segment],
        mut emit: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        let mut merged = {
            // reading a spill file starts a task
            let _runtime = self.runtime.enter();
            self.batch_stream(segments)?
        };
        // one batch at a time, so that `emit` may block: it is outside the
        // runtime between batches
        while let Some(batch) = self.runtime.block_on(merged.next()) {
            emit(batch?)?;
        }
        Ok(())
    }

    fn batch_stream(&self, segments: &[Segment]) -> Result<SendableRecordBatchStream> {
        let mut inputs = segments
            .iter()
            .map(|segment| {
                self.batches
                    .read_spill_as_stream(Arc::clone(&segment.file), Some(segment.batch_bytes))
            })
            .collect::<Result<Vec<_>>>()?;
        if inputs.len() == 1 {
            return Ok(inputs.pop().expect("one input"));
        }
        StreamingMergeBuilder::new()
            .with_streams(inputs)
            .with_schema(Arc::clone(&self.schema))
            .with_expressions(&self.ordering)
            .with_metrics(BaselineMetrics::new(&self.merge_metrics, 0))
            .with_batch_size(self.batch_size)
            .with_fetch(None)
            .with_reservation(MemoryConsumer::new("ParallelSortMerge").register(&self.unlimited))
            .build()
    }
}

/// Writes blocks of bytes to one spill file.
struct BlockWriter<'a> {
    spill: &'a Spill,
    file: Arc<dyn SpillFile>,
    out: Box<dyn SpillWriter>,
    /// Bytes written to the file so far.
    written: u64,
    /// Where each block written so far starts.
    offsets: Vec<u64>,
    /// A block as it is stored, after its header.
    stored: Vec<u8>,
    compressor: Option<zstd::bulk::Compressor<'static>>,
}

impl BlockWriter<'_> {
    /// Writes one block: its header and its bytes, in one write.
    fn write(&mut self, block: &[u8]) -> Result<()> {
        let compression = self.spill.compression;
        let bound = match compression {
            SpillCompression::Uncompressed => block.len(),
            SpillCompression::Lz4Frame => lz4_flex::block::get_maximum_output_size(block.len()),
            SpillCompression::Zstd => zstd::zstd_safe::compress_bound(block.len()),
        };
        self.stored.resize(BLOCK_HEADER + bound, 0);
        let (header, body) = self.stored.split_at_mut(BLOCK_HEADER);
        let length = match compression {
            SpillCompression::Uncompressed => {
                body.copy_from_slice(block);
                bound
            }
            SpillCompression::Lz4Frame => lz4_flex::block::compress_into(block, body)
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            SpillCompression::Zstd => self
                .compressor
                .as_mut()
                .expect("made with the writer")
                .compress_to_buffer(block, body)?,
        };
        header[..4].copy_from_slice(&block_length(block.len())?.to_le_bytes());
        header[4..].copy_from_slice(&block_length(length)?.to_le_bytes());
        let bytes = BLOCK_HEADER + length;
        // the writer refuses bytes past the spill limit
        self.out.write_all(&self.stored[..bytes])?;
        self.spill.metrics.spilled_bytes.add(bytes);
        self.offsets.push(self.written);
        self.written += bytes as u64;
        Ok(())
    }

    /// The file, which holds `rows` rows.
    fn finish(mut self, rows: usize) -> Result<Part> {
        self.out.flush()?;
        self.out.finish()?;
        // one more than the blocks, so that every block has an end
        self.offsets.push(self.written);
        self.spill.metrics.spill_file_count.add(1);
        self.spill.metrics.spilled_rows.add(rows);
        Ok(Part {
            file: self.file,
            offsets: self.offsets,
        })
    }
}

fn block_length(bytes: usize) -> Result<u32> {
    u32::try_from(bytes)
        .map_err(|_| DataFusionError::Internal(format!("a spill block of {bytes} bytes")))
}

/// Reads the blocks of a flush back, in the order they were written.
struct BlockReader {
    compression: SpillCompression,
    /// The open file, and which of the flush's files it is.
    file: Option<(usize, File)>,
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
    /// The block last read as it is stored, after its header.
    stored: Vec<u8>,
    /// That block as it was written, when it is stored compressed.
    bytes: Vec<u8>,
}

impl BlockReader {
    fn new(compression: SpillCompression) -> Result<Self> {
        Ok(Self {
            compression,
            file: None,
            decompressor: match compression {
                SpillCompression::Zstd => Some(zstd::bulk::Decompressor::new()?),
                SpillCompression::Lz4Frame | SpillCompression::Uncompressed => None,
            },
            stored: Vec::new(),
            bytes: Vec::new(),
        })
    }

    /// The block last read, as it was written.
    fn block(&self) -> &[u8] {
        match self.compression {
            SpillCompression::Uncompressed => self.stored.get(BLOCK_HEADER..).unwrap_or_default(),
            SpillCompression::Lz4Frame | SpillCompression::Zstd => &self.bytes,
        }
    }

    /// Reads one block of `flush`, which replaces the block last read.
    fn read(&mut self, flush: &BlockFlush, block: usize) -> Result<()> {
        let (part, block_in_part) = flush.blocks[block];
        if self.file.as_ref().is_none_or(|(open, _)| *open != part) {
            let part_file = &flush.parts[part];
            let path = part_file.file.path().ok_or_else(|| {
                DataFusionError::NotImplemented(
                    "sorting with spill files that are not local".into(),
                )
            })?;
            let mut file = File::open(path)?;
            file.seek(SeekFrom::Start(part_file.offsets[block_in_part]))?;
            self.file = Some((part, file));
        }
        let (_, file) = self.file.as_mut().expect("opened above");
        let offsets = &flush.parts[part].offsets;
        let stored = usize::try_from(offsets[block_in_part + 1] - offsets[block_in_part])
            .map_err(|_| DataFusionError::Internal("a spill block larger than memory".into()))?;
        // blocks are read in the order they were written, so the file is
        // already at this one: its header and its bytes come in one read
        resize_exact(&mut self.stored, stored);
        file.read_exact(&mut self.stored)?;
        let Some((header, body)) = self.stored.split_at_checked(BLOCK_HEADER) else {
            return Err(DataFusionError::Internal(format!(
                "a sort run on disk has a block stored in {stored} bytes"
            )));
        };
        let length = |bytes: &[u8]| u32::from_le_bytes(bytes.try_into().expect("four bytes"));
        let bytes = length(&header[..4]) as usize;
        if length(&header[4..]) as usize != body.len() {
            return Err(DataFusionError::Internal(format!(
                "a sort run on disk has a block stored in {} bytes where {} were written",
                body.len(),
                length(&header[4..])
            )));
        }
        let filled = match self.compression {
            SpillCompression::Uncompressed => body.len(),
            SpillCompression::Lz4Frame => {
                resize_exact(&mut self.bytes, bytes);
                lz4_flex::block::decompress_into(body, &mut self.bytes)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?
            }
            SpillCompression::Zstd => {
                resize_exact(&mut self.bytes, bytes);
                let decompressor = self.decompressor.as_mut().expect("made with the reader");
                decompressor.decompress_to_buffer(body, &mut self.bytes[..])?
            }
        };
        if filled != bytes {
            return Err(DataFusionError::Internal(format!(
                "a sort run on disk has a block of {filled} bytes, not {bytes}"
            )));
        }
        Ok(())
    }
}

/// Resizes a buffer that is reused for blocks of about one size. Growing a
/// `Vec` a little doubles its capacity, which a merge that holds a block of
/// every flush has not planned for.
fn resize_exact(buffer: &mut Vec<u8>, bytes: usize) {
    buffer.reserve_exact(bytes.saturating_sub(buffer.len()));
    buffer.resize(bytes, 0);
}

/// Writes records to a spill file in blocks.
struct RecordWriter<'a> {
    blocks: BlockWriter<'a>,
    /// The block being filled: records as little-endian words.
    block: Vec<u8>,
    block_rows: usize,
    index: WrittenIndex,
    /// The block with its bytes transposed, which is what is compressed.
    transposed: Vec<u8>,
    rows: usize,
}

impl RecordWriter<'_> {
    fn push(&mut self, records: &[u64]) -> Result<()> {
        let spill = self.blocks.spill;
        let words = spill.words().record;
        let block_bytes = spill.block_bytes();
        for record in records.chunks_exact(words) {
            if self.block.is_empty() {
                // big-endian, so that the bytes order the way the words do
                for word in record {
                    self.index.firsts.extend_from_slice(&word.to_be_bytes());
                }
            }
            for word in record {
                self.block.extend_from_slice(&word.to_le_bytes());
            }
            self.block_rows += 1;
            if self.block.len() >= block_bytes {
                self.write_block()?;
            }
        }
        self.rows += records.len() / words;
        Ok(())
    }

    /// Writes the block being filled.
    fn write_block(&mut self) -> Result<()> {
        let spill = self.blocks.spill;
        if spill.compression == SpillCompression::Uncompressed {
            self.blocks.write(&self.block)?;
        } else {
            transpose(
                &self.block,
                spill.words().record_bytes(),
                &mut self.transposed,
            );
            self.blocks.write(&self.transposed)?;
        }
        self.index.wrote(self.block_rows, self.block.len());
        self.block.clear();
        self.block_rows = 0;
        Ok(())
    }

    /// The file and what is in it. `None` when nothing was written.
    fn finish(mut self) -> Result<Option<(Part, WrittenIndex)>> {
        if !self.block.is_empty() {
            self.write_block()?;
        }
        if self.rows == 0 {
            return Ok(None);
        }
        Ok(Some((self.blocks.finish(self.rows)?, self.index)))
    }
}

/// A bound of a reader of records: a key as the index of a flush holds it,
/// and as the words that records are.
struct Bound<'a> {
    key: &'a [u8],
    record: Vec<u64>,
}

impl<'a> Bound<'a> {
    fn new(key: &'a [u8]) -> Self {
        let (words, _) = key.as_chunks::<8>();
        Self {
            key,
            record: words.iter().map(|word| u64::from_be_bytes(*word)).collect(),
        }
    }
}

/// Reads the records of one flush that are at or after `lower` and before
/// `upper`, a block at a time.
struct RecordReader<'a> {
    spill: &'a Spill,
    flush: &'a BlockFlush,
    blocks: BlockReader,
    /// The next block of the flush to read.
    next: usize,
    /// Records before this are not this reader's. Cleared once passed.
    lower: Option<Bound<'a>>,
    /// Records at or after this are not this reader's.
    upper: Option<Bound<'a>>,
    block: Vec<u64>,
    /// Where the unread part of `block` starts.
    position: usize,
}

impl<'a> RecordReader<'a> {
    fn open(
        spill: &'a Spill,
        flush: &'a BlockFlush,
        lower: Option<&'a [u8]>,
        upper: Option<&'a [u8]>,
    ) -> Result<Self> {
        let mut reader = Self {
            spill,
            flush,
            blocks: BlockReader::new(spill.compression)?,
            next: flush.start(lower),
            lower: lower.map(Bound::new),
            upper: upper.map(Bound::new),
            block: Vec::new(),
            position: 0,
        };
        reader.read_block()?;
        Ok(reader)
    }

    /// Loads the next block that has records for this reader. Leaves nothing
    /// loaded at the end.
    fn read_block(&mut self) -> Result<()> {
        let words = self.spill.words().record;
        loop {
            self.block.clear();
            self.position = 0;
            if self.next == self.flush.blocks.len() {
                return Ok(());
            }
            if self
                .upper
                .as_ref()
                .is_some_and(|upper| self.flush.first(self.next) >= upper.key)
            {
                // this block and every later one belong to later readers
                self.next = self.flush.blocks.len();
                return Ok(());
            }
            self.load(self.next)?;
            self.next += 1;
            if let Some(upper) = &self.upper {
                let keep = count_before(&self.block, words, &upper.record);
                self.block.truncate(keep * words);
            }
            if let Some(lower) = self.lower.take() {
                self.position = count_before(&self.block, words, &lower.record) * words;
            }
            if self.position < self.block.len() {
                return Ok(());
            }
        }
    }

    /// Reads one block of the flush into `self.block`.
    fn load(&mut self, block: usize) -> Result<()> {
        self.blocks.read(self.flush, block)?;
        let bytes = self.blocks.block();
        let expected = self.flush.block_rows[block] * self.spill.words().record_bytes();
        if bytes.len() != expected {
            return Err(DataFusionError::Internal(format!(
                "a sort run on disk has a block of {} bytes where {expected} were written",
                bytes.len()
            )));
        }
        if self.spill.compression == SpillCompression::Uncompressed {
            let (words, _) = bytes.as_chunks::<8>();
            self.block
                .extend(words.iter().map(|word| u64::from_le_bytes(*word)));
        } else {
            restore(bytes, self.spill.words().record, &mut self.block);
        }
        Ok(())
    }
}

/// Rearranges a block of records so that the first byte of every record comes
/// first, then the second byte of every record, and so on.
///
/// Sorted records agree with their neighbors on most bytes, and a compressor
/// finds that far more easily when each byte position is stored together.
/// On blocks of sorted export records, lz4 wrote 22.6 bytes a record as they
/// were and 13.2 transposed, and took less time to do it: 13.8 ns a record
/// with the transposing against 19.0.
fn transpose(block: &[u8], record_bytes: usize, transposed: &mut Vec<u8>) {
    let rows = block.len() / record_bytes;
    transposed.clear();
    transposed.resize(block.len(), 0);
    for (row, record) in block.chunks_exact(record_bytes).enumerate() {
        for (position, byte) in record.iter().enumerate() {
            transposed[position * rows + row] = *byte;
        }
    }
}

/// Appends the records of a transposed block, as words. A record's bytes in
/// the block are its words in little-endian order.
fn restore(transposed: &[u8], record_words: usize, block: &mut Vec<u64>) {
    let rows = transposed.len() / (8 * record_words);
    let start = block.len();
    block.resize(start + rows * record_words, 0);
    let records = &mut block[start..];
    for (position, column) in transposed.chunks_exact(rows.max(1)).enumerate() {
        let (word, shift) = (position / 8, 8 * (position % 8));
        for (record, byte) in records.chunks_exact_mut(record_words).zip(column) {
            record[word] |= u64::from(*byte) << shift;
        }
    }
}

/// How many of the sorted `records` come before `bound`.
fn count_before(records: &[u64], words: usize, bound: &[u64]) -> usize {
    let (mut low, mut high) = (0, records.len() / words);
    while low < high {
        let middle = (low + high) / 2;
        if &records[middle * words..][..words] < bound {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}

impl Input for RecordReader<'_> {
    fn loaded(&self) -> &[u64] {
        &self.block[self.position..]
    }

    fn consume(&mut self, words: usize) -> Result<()> {
        self.position += words;
        if self.position == self.block.len() {
            self.read_block()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_transposed_block_restores_to_the_same_records() {
        let records: Vec<u64> = (0..7 * 3)
            .map(|word: u64| word.wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .collect();
        let block: Vec<u8> = records.iter().flat_map(|word| word.to_le_bytes()).collect();
        let mut transposed = Vec::new();
        transpose(&block, 24, &mut transposed);
        // the first byte of each of the seven records comes first
        assert_eq!(
            transposed[..7],
            [0, 3, 6, 9, 12, 15, 18].map(|word| block[8 * word])
        );

        let mut restored = vec![42];
        restore(&transposed, 3, &mut restored);
        assert_eq!(restored[0], 42);
        assert_eq!(restored[1..], records);
    }
}
