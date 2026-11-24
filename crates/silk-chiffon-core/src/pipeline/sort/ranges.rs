//! Sorting buffered rows by splitting them into key ranges.
//!
//! Everything buffered is sampled, and the sample picks the records that
//! divide the key space into ranges of about as many rows each. Every row in
//! range `n` sorts before every row in range `n + 1`, so the ranges sort
//! independently and their concatenation is the sorted whole: nothing has to
//! be merged.
//!
//! The ranges are chosen again for everything buffered between two flushes,
//! so input that arrives in key order still splits evenly.

use std::{collections::VecDeque, sync::Arc};

use arrow::{array::RecordBatch, row::Rows};
use datafusion::{error::Result, physical_plan::metrics::Count};
use parking_lot::Mutex;

use super::{
    keys::{Contents, Keys, RowEncoder},
    record::{MAX_WORDS, Order, Words},
    run::{Batcher, Buffered, Kept, Splitters},
    workers::Workers,
};

/// Sample records taken per range when choosing splitters.
const SAMPLES_PER_RANGE: usize = 128;

/// Rows a range is worth having for.
const MIN_ROWS_PER_RANGE: usize = 16 * 1024;

/// The most ranges made. Each one costs every splitting thread a buffer.
const MAX_RANGES: usize = 16 * 1024;

/// Chunks that one thread's share of a full range takes. Records move between
/// buffers of one size, the chunk: rows are buffered in chunks, copied into
/// chunks of their range, and copied out in sorted order into chunks again.
/// Each thread reuses the chunks it has emptied, so the only memory beyond
/// the records is the chunk each thread is filling for each range.
const CHUNKS_PER_SHARE: usize = 16;

/// Buffered records are counted with one part in this many more, which is
/// what a split fills beyond them when it makes as many ranges as fit in the
/// scratch memory: a chunk of each range on each thread, each a sixteenth of
/// that thread's share of the range. The split then has its chunks however
/// full the pool is when the run is flushed.
pub(super) const SPLIT_HEADROOM_SHARE: usize = CHUNKS_PER_SHARE;
const MIN_CHUNK_WORDS: usize = 256;
const MAX_CHUNK_WORDS: usize = 64 * 1024;

/// How far under a power of two a chunk is, in words.
const UNDER_POWER_OF_TWO_WORDS: usize = 8;

/// Bytes one thread holds for each record of a range while it sorts the
/// range: its entry, and its copy in sorted order.
fn sort_bytes(words: Words) -> usize {
    size_of::<Entry>() + words.record_bytes()
}

/// The size, in words, of the buffers that rows are buffered in and that
/// ranges are made of. A whole number of records.
///
/// It is a little under a power of two bytes, which is the size the allocator
/// packs best. 2 GiB of 16,384-byte buffers took 24% more memory than they
/// hold, and a 200M-row sort held 7.6 GiB of them for 6.0 GiB of records. The
/// same 2 GiB in 16,320-byte buffers took 0.8% more.
pub(super) fn chunk_words(words: Words, scratch_bytes: usize, threads: usize) -> usize {
    // a range is sized to sort in half of a thread's scratch memory
    let range_words = scratch_bytes / sort_bytes(words) / 2 * words.record;
    let wanted =
        (range_words / threads.max(1) / CHUNKS_PER_SHARE).clamp(MIN_CHUNK_WORDS, MAX_CHUNK_WORDS);
    let power_of_two = 1 << wanted.ilog2();
    ((power_of_two - UNDER_POWER_OF_TWO_WORDS) / words.record).max(1) * words.record
}

/// How many ranges `rows` buffered rows are best split into, aiming for
/// `ranges` when there are rows enough: more when that is what keeps each
/// range within `scratch_bytes`, with room for the sample being off by half.
pub(super) fn wanted(rows: usize, ranges: usize, words: Words, scratch_bytes: usize) -> usize {
    let fitting = (2 * rows * sort_bytes(words)).div_ceil(scratch_bytes.max(1));
    (rows / MIN_ROWS_PER_RANGE)
        .clamp(1, ranges)
        .max(fitting)
        .min(MAX_RANGES)
}

/// Everything buffered between two flushes, split into key ranges and sorted.
pub(super) struct Ranges {
    encoder: Arc<RowEncoder>,
    words: Words,
    /// Source batches by id. Empty when records hold whole rows, or output is
    /// decoded from `payloads`.
    batches: Vec<RecordBatch>,
    /// Whole rows in row format by batch id, when output is decoded from them.
    payloads: Vec<Rows>,
    /// Full keys by batch id. Empty when the key words are the whole key.
    rows: Vec<Option<Rows>>,
    /// Whether the records' key words decide the order without the full keys.
    settled: bool,
    splitters: RangeSplitters,
    /// Each range's records in sorted order, in chunks.
    ranges: Vec<Vec<Vec<u64>>>,
    range_rows: Vec<usize>,
    /// Budget bytes held, for the owner to release.
    held: usize,
    /// The part of `held` that is the kept batches or rows.
    source_bytes: usize,
    rows_total: usize,
}

impl Ranges {
    /// Splits buffered rows into `ranges` ranges and sorts every range, in
    /// parallel on `workers`. `chunk` is the capacity of the buffers that the
    /// rows were buffered in, from [`chunk_words`], and `split_bytes` what the
    /// caller took for the chunks a split fills at once, which these ranges
    /// hold until they are released.
    ///
    /// A range is sorted through a 16-byte entry per record and copied out in
    /// order, so one thread needs a range's records and entries again while it
    /// sorts it, which [`wanted`] fits in `scratch_bytes`. Rows in ranges that
    /// still do not fit, because the sample was off or there were fewer ranges
    /// than wanted, are counted in `oversized`: those ranges are sorted in
    /// pieces that fit, and the pieces merged.
    ///
    /// All of the sorting is done here, before any row is written or sent.
    /// The threads that encode the output then have the machine to
    /// themselves: sorting each range as it was read took 6.2 s on a 200M-row
    /// export that takes 5.0 s when the sorting is done first.
    pub(super) fn split(
        mut buffered: Buffered,
        ranges: usize,
        chunk: usize,
        scratch_bytes: usize,
        split_bytes: usize,
        oversized: &Count,
        workers: &Workers,
    ) -> Self {
        if buffered.words.layout.keys == Keys::Prefix {
            buffered.respell_keys(workers);
        }
        let words = buffered.words;
        let rows_total = buffered.rows_total;
        let ranges = ranges.max(1);
        let buffers = std::mem::take(&mut buffered.buffers);
        let (splitters, ranges) = {
            let order = Order::new(words, &buffered.rows).settled(buffered.settled);
            let stride = (rows_total / (ranges * SAMPLES_PER_RANGE)).max(1);
            let mut samples = sample(&buffers, words.record, stride);
            samples.sort_unstable_by(|left, right| order.compare(left, right));
            let splitters = RangeSplitters::from_samples(order, &samples, ranges);
            drop(samples);
            let mut unsorted: Vec<Vec<Vec<u64>>> = Vec::new();
            unsorted.resize_with(splitters.count + 1, Vec::new);
            for thread in split_buffers(buffers, &splitters, order, chunk, workers) {
                for (range, chunks) in thread.into_iter().enumerate() {
                    unsorted[range].extend(chunks);
                }
            }
            let unsorted: Vec<Mutex<Vec<Vec<u64>>>> =
                unsorted.into_iter().map(Mutex::new).collect();
            // emptied chunks, kept from one range to the next by whichever
            // worker sorts it
            let spares: Mutex<Vec<Vec<Vec<u64>>>> = Mutex::new(Vec::new());
            let ranges: Vec<Vec<Vec<u64>>> = workers.map(unsorted.len(), &|range| {
                let chunks = std::mem::take(&mut *unsorted[range].lock());
                let rows = chunks.iter().map(Vec::len).sum::<usize>() / words.record;
                let mut spare = spares.lock().pop().unwrap_or_default();
                let fitting_rows = scratch_bytes / sort_bytes(words);
                let sorted = if rows > fitting_rows {
                    oversized.add(rows);
                    sort_in_pieces(chunks, order, chunk, fitting_rows.max(1), &mut spare)
                } else {
                    sort_range(chunks, order, chunk, &mut spare)
                };
                spares.lock().push(spare);
                sorted
            });
            (splitters, ranges)
        };
        Self {
            encoder: buffered.encoder,
            words,
            batches: buffered.batches,
            payloads: buffered.payloads,
            rows: buffered.rows,
            settled: buffered.settled,
            splitters,
            range_rows: ranges
                .iter()
                .map(|chunks| chunks.iter().map(Vec::len).sum::<usize>() / words.record)
                .collect(),
            ranges,
            held: buffered.held + split_bytes,
            source_bytes: buffered.source_bytes,
            rows_total,
        }
    }

    pub(super) fn held(&self) -> usize {
        self.held
    }

    pub(super) fn num_rows(&self) -> usize {
        self.rows_total
    }

    pub(super) fn range_count(&self) -> usize {
        self.ranges.len()
    }

    pub(super) fn largest_range_rows(&self) -> usize {
        self.range_rows.iter().copied().max().unwrap_or(0)
    }

    /// Roughly how many bytes a row of output takes.
    pub(super) fn bytes_per_row(&self) -> usize {
        match self.words.layout.contents {
            Contents::Row => self.words.layout.inline,
            Contents::Source => {
                let source = self.source_bytes.checked_div(self.rows_total).unwrap_or(0);
                // the payload leaves out the key columns that output decodes
                // from the kept full keys, or else from the records' key words
                let keys = if self.encoder.payload_needs_keys() {
                    let kept = self.rows.iter().flatten().map(Rows::size).sum::<usize>();
                    kept.checked_div(self.rows_total)
                        .unwrap_or(0)
                        .max(self.words.key_bytes())
                } else {
                    0
                };
                (source + keys).max(1)
            }
        }
    }

    fn order(&self) -> Order<'_> {
        Order::new(self.words, &self.rows).settled(self.settled)
    }

    /// The rows that these ranges' records name.
    pub(super) fn kept(&self) -> Kept<'_> {
        Kept {
            payloads: &self.payloads,
            key_rows: &self.rows,
        }
    }

    /// A batcher for these ranges' records.
    pub(super) fn batcher<Emit: FnMut(RecordBatch) -> Result<()>>(
        &self,
        batch_size: usize,
        emit: Emit,
    ) -> Batcher<'_, Emit> {
        Batcher::new(
            &self.encoder,
            self.words,
            &self.batches,
            &self.payloads,
            &self.rows,
            batch_size,
            emit,
        )
    }

    /// Bounds that divide the key space into `groups` groups of about as many
    /// of these rows each, for writing flushes of Arrow batches in groups.
    pub(super) fn group_bounds(&self, groups: usize) -> Splitters {
        let count = self.splitters.count;
        let groups = groups.clamp(1, count + 1);
        Splitters::from_records(
            self.order(),
            (1..groups).map(|group| self.splitters.record(group * (count + 1) / groups - 1)),
        )
    }

    /// Calls `visit` with the records of one range, in sorted order.
    pub(super) fn visit_sorted(
        &self,
        range: usize,
        visit: impl FnMut(&[u64]) -> Result<()>,
    ) -> Result<()> {
        self.ranges[range]
            .iter()
            .map(Vec::as_slice)
            .try_for_each(visit)
    }

    /// Calls `visit` with the records of one group, in sorted order: those at
    /// or after the group's lower bound and before its upper bound.
    pub(super) fn visit_group(
        &self,
        bounds: &Splitters,
        group: usize,
        mut visit: impl FnMut(&[u64]) -> Result<()>,
    ) -> Result<()> {
        let order = self.order();
        let lower = group.checked_sub(1);
        let upper = (group + 1 < bounds.groups()).then_some(group);
        for range in 0..self.range_count() {
            // a range holds the records from the splitter before it up to,
            // and not including, the splitter after it
            let first = range.checked_sub(1).map(|at| self.splitters.record(at));
            let end = (range < self.splitters.count).then(|| self.splitters.record(range));
            if let (Some(upper), Some(first)) = (upper, first)
                && bounds.precedes(upper, first, order)
            {
                // this range and every later one are past the group
                break;
            }
            if let (Some(lower), Some(end)) = (lower, end)
                && !bounds.precedes(lower, end, order)
            {
                continue;
            }
            let starts_inside = lower.is_none_or(|lower| {
                first.is_some_and(|first| bounds.precedes(lower, first, order))
            });
            let ends_inside = upper
                .is_none_or(|upper| end.is_some_and(|end| !bounds.precedes(upper, end, order)));
            if starts_inside && ends_inside {
                self.visit_sorted(range, &mut visit)?;
                continue;
            }
            // the range straddles a bound of the group
            self.visit_sorted(range, |records| {
                for record in records.chunks_exact(self.words.record) {
                    let before = lower.is_some_and(|lower| !bounds.precedes(lower, record, order));
                    let after = upper.is_some_and(|upper| bounds.precedes(upper, record, order));
                    if !before && !after {
                        visit(record)?;
                    }
                }
                Ok(())
            })?;
        }
        Ok(())
    }
}

/// Every `stride`-th record of the buffers taken as one sequence. A buffer can
/// hold fewer records than the stride, so the count runs on across buffers.
fn sample(buffers: &[Vec<u64>], width: usize, stride: usize) -> Vec<&[u64]> {
    let mut samples = Vec::new();
    let mut next = stride / 2;
    let mut passed = 0;
    for buffer in buffers {
        let records = buffer.len() / width;
        while next < passed + records {
            samples.push(&buffer[(next - passed) * width..][..width]);
            next += stride;
        }
        passed += records;
    }
    samples
}

/// Copies every record into chunks of its range. Each thread takes some of the
/// buffers. A buffer it has copied becomes a chunk for a range that needs one,
/// so the copy allocates only the chunks that are being filled at once: one
/// per range for each thread.
///
/// Making one allocation per range up front and freeing buffers behind the
/// copy needed far more: the freed memory was another thread's to reuse, and
/// a 200M-row sort peaked at 12.4 GiB where 8.6 GiB is what it needs.
fn split_buffers(
    buffers: Vec<Vec<u64>>,
    splitters: &RangeSplitters,
    order: Order<'_>,
    chunk: usize,
    workers: &Workers,
) -> Vec<Vec<Vec<Vec<u64>>>> {
    let width = order.words().record;
    let threads = workers.parallelism();
    let mut shares: Vec<Vec<Vec<u64>>> = (0..threads).map(|_| Vec::new()).collect();
    for (index, buffer) in buffers.into_iter().enumerate() {
        shares[index % threads].push(buffer);
    }
    let shares: Vec<Mutex<Vec<Vec<u64>>>> = shares.into_iter().map(Mutex::new).collect();
    workers.map(shares.len(), &|share| {
        let buffers = std::mem::take(&mut *shares[share].lock());
        let mut ranges: Vec<Vec<Vec<u64>>> = Vec::new();
        ranges.resize_with(splitters.count + 1, Vec::new);
        let mut spare: Vec<Vec<u64>> = Vec::new();
        for mut buffer in buffers {
            for record in buffer.chunks_exact(width) {
                let range = &mut ranges[splitters.range_of(record, order)];
                with_room(range, &mut spare, chunk).extend_from_slice(record);
            }
            buffer.clear();
            spare.push(buffer);
        }
        ranges
    })
}

/// The last chunk of `chunks` if it has room for a record, or else a chunk
/// added from `spare` or newly made.
#[inline]
fn with_room<'a>(
    chunks: &'a mut Vec<Vec<u64>>,
    spare: &mut Vec<Vec<u64>>,
    chunk: usize,
) -> &'a mut Vec<u64> {
    let full = chunks
        .last()
        .is_none_or(|last| last.len() == last.capacity());
    if full {
        chunks.push(spare.pop().unwrap_or_else(|| Vec::with_capacity(chunk)));
    }
    chunks.last_mut().expect("there is a chunk with room")
}

/// Puts one range's records in order: returns chunks that hold them in sorted
/// order, and leaves the chunks they were in, emptied, in `spare` for the
/// next range.
fn sort_range(
    chunks: Vec<Vec<u64>>,
    order: Order<'_>,
    chunk: usize,
    spare: &mut Vec<Vec<u64>>,
) -> Vec<Vec<u64>> {
    let width = order.words().record;
    let varying = Varying::among(&chunks, order.words());
    if varying.lead.is_empty() && order.is_settled() {
        // every record has the same key, so they are in order as they are
        return chunks;
    }
    let entries = if order.words().layout.keys == Keys::Prefix && !order.is_settled() {
        sorted_entries_of_full_keys(&chunks, order)
    } else {
        // the records hold their keys. Any that were cut tie on their words,
        // and the full comparison settles those
        sorted_entries(&chunks, &varying, order)
    };
    let mut sorted = Vec::with_capacity(chunks.len());
    for entry in &entries {
        with_room(&mut sorted, spare, chunk).extend_from_slice(entry.record(&chunks, width));
    }
    for mut emptied in chunks {
        emptied.clear();
        spare.push(emptied);
    }
    // more than the next range can use is not worth keeping
    spare.truncate(sorted.len() + 1);
    sorted
}

/// Puts the records of a range too large for the scratch memory in order:
/// sorts pieces of up to `piece_rows` records one at a time, which fits, and
/// merges the sorted pieces. The merge fills chunks from the ones it has
/// emptied, so beyond the records it holds only a chunk of each piece and the
/// one it is filling.
fn sort_in_pieces(
    chunks: Vec<Vec<u64>>,
    order: Order<'_>,
    chunk: usize,
    piece_rows: usize,
    spare: &mut Vec<Vec<u64>>,
) -> Vec<Vec<u64>> {
    let width = order.words().record;
    let mut pieces: Vec<VecDeque<Vec<u64>>> = Vec::new();
    let mut piece = Vec::new();
    let mut rows = 0;
    for chunk_records in chunks {
        let chunk_rows = chunk_records.len() / width;
        if rows + chunk_rows > piece_rows && !piece.is_empty() {
            pieces.push(sort_range(std::mem::take(&mut piece), order, chunk, spare).into());
            rows = 0;
        }
        rows += chunk_rows;
        piece.push(chunk_records);
    }
    if !piece.is_empty() {
        pieces.push(sort_range(piece, order, chunk, spare).into());
    }
    pieces.retain(|piece| !piece.is_empty());
    if pieces.len() <= 1 {
        return pieces.pop().map(Vec::from).unwrap_or_default();
    }
    // where each piece is in its first chunk
    let mut at = vec![0_usize; pieces.len()];
    let before = |pieces: &[VecDeque<Vec<u64>>], at: &[usize], left: usize, right: usize| {
        let next = |piece: usize| &pieces[piece][0][at[piece]..][..width];
        order.compare(next(left), next(right)).is_lt()
    };
    // a binary heap of piece indexes, ordered by each piece's next record
    let mut heap: Vec<usize> = (0..pieces.len()).collect();
    for start in (0..heap.len() / 2).rev() {
        sift_down(&mut heap, start, |left, right| {
            before(&pieces, &at, left, right)
        });
    }
    let mut sorted: Vec<Vec<u64>> = Vec::new();
    while let Some(&first) = heap.first() {
        let record = &pieces[first][0][at[first]..][..width];
        with_room(&mut sorted, spare, chunk).extend_from_slice(record);
        at[first] += width;
        if at[first] == pieces[first][0].len() {
            let mut emptied = pieces[first].pop_front().expect("the piece had a chunk");
            emptied.clear();
            spare.push(emptied);
            at[first] = 0;
            if pieces[first].is_empty() {
                let last = heap.pop().expect("the heap holds this piece");
                if heap.is_empty() {
                    break;
                }
                heap[0] = last;
            }
        }
        sift_down(&mut heap, 0, |left, right| {
            before(&pieces, &at, left, right)
        });
    }
    spare.truncate(sorted.len() + 1);
    sorted
}

/// Moves the entry at `start` down a binary min-heap ordered by `before`.
fn sift_down(heap: &mut [usize], mut start: usize, before: impl Fn(usize, usize) -> bool) {
    loop {
        let (left, right) = (2 * start + 1, 2 * start + 2);
        let mut least = start;
        if left < heap.len() && before(heap[left], heap[least]) {
            least = left;
        }
        if right < heap.len() && before(heap[right], heap[least]) {
            least = right;
        }
        if least == start {
            return;
        }
        heap.swap(start, least);
        start = least;
    }
}

/// The records that divide the key space into ranges.
struct RangeSplitters {
    words: Words,
    /// Whole sampled records in ascending order, back to back.
    records: Vec<u64>,
    count: usize,
    /// The first eight byte positions of the key where splitters differ.
    lead: Vec<usize>,
    /// For each key word up to the last lead position: the bits where every
    /// splitter is the same and a record has to agree to be placed by `lead`.
    shared: Vec<u64>,
    /// Each splitter's lead bytes as one integer, in ascending order.
    summaries: Vec<u64>,
}

impl RangeSplitters {
    /// Picks up to `ranges - 1` evenly spaced splitters from sorted samples.
    fn from_samples(order: Order<'_>, samples: &[&[u64]], ranges: usize) -> Self {
        let words = order.words();
        let mut records = Vec::new();
        let mut count = 0;
        let mut previous: Option<&[u64]> = None;
        for range in 1..ranges {
            let Some(&sample) = samples.get(range * samples.len() / ranges) else {
                break;
            };
            // equal splitters would leave an empty range between them
            if previous.is_none_or(|previous| order.compare(previous, sample).is_ne()) {
                records.extend_from_slice(sample);
                count += 1;
                previous = Some(sample);
            }
        }
        let splitters = || records.chunks_exact(words.record);
        let mut differs = [0_u64; MAX_WORDS];
        for splitter in splitters() {
            for ((differ, word), first) in
                differs[..words.key].iter_mut().zip(splitter).zip(&records)
            {
                *differ |= word ^ first;
            }
        }
        let lead: Vec<usize> = varying_bytes(&differs[..words.key]).take(8).collect();
        let shared = lead.last().map_or(Vec::new(), |last| {
            (0..=last / 8)
                .map(|word| {
                    (0..8)
                        .filter(|byte| {
                            8 * word + byte <= *last && !lead.contains(&(8 * word + byte))
                        })
                        .fold(0, |mask, byte| mask | (0xFF << (56 - 8 * byte)))
                })
                .collect()
        });
        let summaries = splitters()
            .map(|splitter| gather(&lead, splitter))
            .collect();
        Self {
            words,
            records,
            count,
            lead,
            shared,
            summaries,
        }
    }

    fn record(&self, splitter: usize) -> &[u64] {
        &self.records[splitter * self.words.record..][..self.words.record]
    }

    /// The range of a record: how many splitters are at or before it.
    ///
    /// Splitters differ from each other at only a few byte positions, so a
    /// record that agrees with them everywhere else is placed by comparing
    /// those positions as one integer. Searching the splitters by comparing
    /// whole keys was a tenth of an export's CPU time.
    #[inline]
    fn range_of(&self, record: &[u64], order: Order<'_>) -> usize {
        let strays = record
            .iter()
            .zip(&self.records)
            .zip(&self.shared)
            .fold(0, |strays, ((word, first), shared)| {
                strays | ((word ^ first) & shared)
            });
        if strays != 0 {
            // the record differs from the splitters where they all agree, so
            // the lead bytes alone do not order it against them
            return self.search(0, self.count, record, order);
        }
        let summary = gather(&self.lead, record);
        let below = self
            .summaries
            .partition_point(|splitter| *splitter < summary);
        let tied = self.summaries[below..]
            .iter()
            .take_while(|splitter| **splitter == summary)
            .count();
        // splitters with the record's summary agree with it up to the last
        // lead byte, and what follows decides
        self.search(below, below + tied, record, order)
    }

    /// How many splitters are at or before `record`, given that those before
    /// `low` are and those from `high` on are not.
    fn search(&self, mut low: usize, mut high: usize, record: &[u64], order: Order<'_>) -> usize {
        while low < high {
            let middle = (low + high) / 2;
            if order.compare(self.record(middle), record).is_le() {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }
}

/// The byte positions of a key, in the order they compare, at which the
/// given words say some keys differ.
fn varying_bytes(differs: &[u64]) -> impl Iterator<Item = usize> + '_ {
    (0..8 * differs.len()).filter(|at| (differs[at / 8] >> (56 - 8 * (at % 8))) & 0xFF != 0)
}

/// The bytes of a record's key at the `lead` positions, as one integer. It
/// orders records that agree everywhere else up to the last of the positions.
#[inline]
fn gather(lead: &[usize], record: &[u64]) -> u64 {
    lead.iter().fold(0, |gathered, at| {
        (gathered << 8) | ((record[at / 8] >> (56 - 8 * (at % 8))) & 0xFF)
    })
}

/// Which bytes of the keys of some records differ from record to record.
struct Varying {
    /// The first eight such positions.
    lead: Vec<usize>,
    /// Whether there are more than those.
    more: bool,
}

impl Varying {
    fn among(pieces: &[Vec<u64>], words: Words) -> Self {
        let mut differs = [0_u64; MAX_WORDS];
        if let Some(first) = pieces.iter().find(|piece| !piece.is_empty()) {
            for piece in pieces {
                for record in piece.chunks_exact(words.record) {
                    for ((differ, word), first) in
                        differs[..words.key].iter_mut().zip(record).zip(first)
                    {
                        *differ |= word ^ first;
                    }
                }
            }
        }
        let mut varying = varying_bytes(&differs[..words.key]);
        Self {
            lead: varying.by_ref().take(8).collect(),
            more: varying.next().is_some(),
        }
    }
}

/// Where one record of a range is, with the leading key bytes that tell it
/// apart from the range's other records.
#[derive(Clone, Copy)]
struct Entry {
    prefix: u64,
    piece: u32,
    record: u32,
}

impl Entry {
    #[inline]
    fn record<'a>(&self, pieces: &'a [Vec<u64>], width: usize) -> &'a [u64] {
        &pieces[self.piece as usize][self.record as usize * width..][..width]
    }
}

/// The records of a range in sorted order, as entries that say where each is.
///
/// Within one range most key bytes are the same in every record: validity
/// bytes, the high bytes of integers, and whole leading columns. So each
/// record is summarized by the first eight key bytes that differ anywhere in
/// the range, read as one integer, and the summaries are sorted. Records with
/// equal summaries are then settled by the full comparison.
///
/// On ranges of export records, sorting this way and copying the records out
/// in order measured 19 ns a record. Sorting a range's pieces by comparison
/// and merging them with a heap measured 27 ns and 31 to 79 ns.
fn sorted_entries(pieces: &[Vec<u64>], varying: &Varying, order: Order<'_>) -> Vec<Entry> {
    let width = order.words().record;
    let records = pieces.iter().map(Vec::len).sum::<usize>() / width;
    let mut entries = Vec::with_capacity(records);
    // a range has far fewer than `u32::MAX` chunks, and a chunk far fewer
    // records
    for (piece, records) in (0_u32..).zip(pieces) {
        for (record, words) in (0_u32..).zip(records.chunks_exact(width)) {
            entries.push(Entry {
                prefix: gather(&varying.lead, words),
                piece,
                record,
            });
        }
    }
    entries.sort_unstable_by_key(|entry| entry.prefix);
    if varying.more || !order.is_settled() {
        // the summaries do not hold everything that can decide the order
        for tied in entries.chunk_by_mut(|left, right| left.prefix == right.prefix) {
            if tied.len() > 1 {
                tied.sort_unstable_by(|left, right| {
                    order.compare(left.record(pieces, width), right.record(pieces, width))
                });
            }
        }
    }
    entries
}

/// The leading bytes of full keys that are examined for where a range's keys
/// differ. Keys that first differ after this many bytes are compared in full.
const FULL_KEY_SPAN: usize = 256;

/// The records of a range in sorted order, for records whose key words are
/// only a prefix of their keys.
///
/// The key words are chosen for the whole input, so they are spent on the
/// leading columns, and within one range those are the same in every record:
/// sorting by the words left almost every comparison to the full keys, each
/// one a read of two keys from wherever they are. So here the summaries are
/// read from the full keys: the first eight bytes at which the range's keys
/// differ, which are bytes of the columns that tell its records apart. That
/// costs two reads of each key. Sorting the ranges of 50M rows by six
/// columns, the first a string with twelve values, took 4.4 s by the key
/// words and takes 2.1 s this way.
fn sorted_entries_of_full_keys(pieces: &[Vec<u64>], order: Order<'_>) -> Vec<Entry> {
    let width = order.words().record;
    let keys = || {
        pieces
            .iter()
            .flat_map(|piece| piece.chunks_exact(width))
            .map(|record| order.full_key(record))
    };
    let Some(first) = keys().next() else {
        return Vec::new();
    };
    // Keys are compared as if zero bytes followed them. Two different keys
    // differ before either ends, so that changes no comparison.
    let padded = |key: &[u8], at: usize| key.get(at).copied().unwrap_or(0);
    let mut differs = [0_u8; FULL_KEY_SPAN];
    for key in keys() {
        let shared = key.len().min(first.len()).min(FULL_KEY_SPAN);
        for ((differ, byte), first) in differs.iter_mut().zip(key).zip(first) {
            *differ |= byte ^ first;
        }
        for (at, differ) in differs
            .iter_mut()
            .enumerate()
            .take(key.len().max(first.len()))
            .skip(shared)
        {
            *differ |= padded(key, at) ^ padded(first, at);
        }
    }
    let lead: Vec<usize> = (0..FULL_KEY_SPAN)
        .filter(|at| differs[*at] != 0)
        .take(8)
        .collect();
    let records = pieces.iter().map(Vec::len).sum::<usize>() / width;
    let mut entries = Vec::with_capacity(records);
    for (piece, records) in (0_u32..).zip(pieces) {
        for (record, words) in (0_u32..).zip(records.chunks_exact(width)) {
            let key = order.full_key(words);
            entries.push(Entry {
                prefix: lead.iter().fold(0, |gathered, at| {
                    (gathered << 8) | u64::from(padded(key, *at))
                }),
                piece,
                record,
            });
        }
    }
    entries.sort_unstable_by_key(|entry| entry.prefix);
    for tied in entries.chunk_by_mut(|left, right| left.prefix == right.prefix) {
        if tied.len() > 1 {
            tied.sort_unstable_by(|left, right| {
                order
                    .full_key(left.record(pieces, width))
                    .cmp(order.full_key(right.record(pieces, width)))
            });
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use super::*;
    use crate::pipeline::sort::{
        keys::RecordLayout,
        run::{Grant, Lane},
    };

    /// Words in a test record. A function, so that the tests cut records the
    /// way the sort does, by a width it learns at run time.
    fn width() -> usize {
        words().record
    }

    fn words() -> Words {
        Words::new(RecordLayout {
            inline: 24,
            record: 24,
            keys: Keys::Whole,
            contents: Contents::Row,
        })
    }

    struct Random(u64);

    impl Random {
        fn below(&mut self, bound: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % bound
        }
    }

    /// Records of three words whose keys differ at nine byte positions,
    /// spread over all three words.
    fn records(random: &mut Random, count: usize) -> Vec<u64> {
        (0..count)
            .flat_map(|_| {
                [
                    0x0100_0000_0000_0000 | (random.below(3) << 8),
                    0x0101_0000_0000_0000 | (random.below(500) << 32) | random.below(4),
                    (random.below(7) << 56) | random.below(1 << 30),
                ]
            })
            .collect()
    }

    struct Always;

    impl Grant for Always {
        fn grant(&self, _bytes: usize) -> bool {
            true
        }
    }

    /// Sorts strings, one input partition per list, and returns them in the
    /// sorted order with whether the key words alone decided it. Records are
    /// made to hold keys of up to `fitted` bytes whole, or prefixes of them
    /// when there is none.
    fn sort_strings(partitions: &[Vec<&str>], fitted: Option<usize>) -> (Vec<String>, bool) {
        use arrow::{
            array::StringArray,
            datatypes::{DataType, Field, Schema},
        };

        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let ordering = datafusion::physical_expr::LexOrdering::new([
            datafusion::physical_expr::PhysicalSortExpr::new_default(Arc::new(
                datafusion::physical_expr::expressions::Column::new("s", 0),
            )),
        ])
        .unwrap();
        let encoder = Arc::new(RowEncoder::try_new(&ordering, &schema).unwrap());
        // no record holds a key of a thousand bytes whole
        let words = Words::new(encoder.layout_for(fitted.unwrap_or(1000)));
        let scratch_bytes = 1024 * 1024;
        let chunk = chunk_words(words, scratch_bytes, 2);
        let next_id = AtomicU32::new(0);
        let mut lanes: Vec<Lane> = partitions.iter().map(|_| Lane::default()).collect();
        let mut values = Vec::new();
        for (lane, strings) in lanes.iter_mut().zip(partitions) {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(strings.clone()))],
            )
            .unwrap();
            let mut rows = Some(encoder.encode(&batch).unwrap());
            let mut payload = encoder.encode_payload(&batch).unwrap();
            assert!(lane.insert(
                &next_id,
                &batch,
                &mut rows,
                &mut payload,
                words,
                chunk,
                &Always,
            ));
            values.push(strings.clone());
        }
        let mut lanes: Vec<&mut Lane> = lanes.iter_mut().collect();
        let buffered = Buffered::take(&mut lanes, encoder, words);
        let ranges = Ranges::split(
            buffered,
            4,
            chunk,
            scratch_bytes,
            0,
            &Count::new(),
            &Workers::try_new(2).unwrap(),
        );
        let mut sorted = Vec::new();
        for range in 0..ranges.range_count() {
            ranges
                .visit_sorted(range, |records| {
                    for record in records.chunks_exact(words.record) {
                        let (batch, row) = Words::source(record);
                        sorted.push(values[batch as usize][row as usize].to_owned());
                    }
                    Ok(())
                })
                .unwrap();
        }
        (sorted, ranges.settled)
    }

    fn in_order(partitions: &[Vec<&str>]) -> Vec<String> {
        let mut all: Vec<String> = partitions
            .iter()
            .flatten()
            .map(|s| (*s).to_owned())
            .collect();
        all.sort_unstable();
        all
    }

    #[test]
    fn keys_that_share_a_long_start_are_ordered_by_the_bytes_that_differ() {
        // every key agrees on more bytes than a record holds
        let partitions = vec![
            vec![
                "a shared start of more than sixteen bytes: 0412",
                "a shared start of more than sixteen bytes: 0007",
                "a shared start of more than sixteen bytes: 9310",
            ],
            vec![
                "a shared start of more than sixteen bytes: 0411",
                "a shared start of more than sixteen bytes: 0412",
            ],
        ];
        let (sorted, settled) = sort_strings(&partitions, None);
        assert_eq!(sorted, in_order(&partitions));
        // four positions differ and every key is one length, so the records
        // hold all that tells the keys apart
        assert!(settled);
    }

    #[test]
    fn keys_that_fit_their_records_are_ordered_by_the_records_alone() {
        let partitions = vec![
            vec!["pear", "pear tree", "", "plum"],
            vec!["pea", "a pear", "pear"],
        ];
        // the longest, "pear tree", is 19 bytes in row format
        let (sorted, settled) = sort_strings(&partitions, Some(19));
        assert_eq!(sorted, in_order(&partitions));
        assert!(settled);
    }

    #[test]
    fn keys_too_long_for_their_records_are_ordered_through_the_row_format() {
        let partitions = vec![
            vec![
                "pear",
                "a key longer than the records were made for, ending in 2",
                "pea",
                "",
            ],
            vec![
                "a key longer than the records were made for, ending in 1",
                "pear",
                "a key",
            ],
        ];
        // records made for "pear", 10 bytes in row format, hold keys of up to
        // 15 bytes, so the long keys are cut where they still agree
        let (sorted, settled) = sort_strings(&partitions, Some(10));
        assert_eq!(sorted, in_order(&partitions));
        assert!(!settled);
    }

    #[test]
    fn keys_of_different_lengths_still_break_ties_on_the_whole_key() {
        let partitions = vec![
            vec![
                "pear",
                "pear tree",
                "",
                "a far longer key than a record can hold, ending in 1",
            ],
            vec![
                "pea",
                "a far longer key than a record can hold, ending in 0",
                "pear",
            ],
        ];
        let (sorted, settled) = sort_strings(&partitions, None);
        assert_eq!(sorted, in_order(&partitions));
        assert!(!settled);
    }

    /// Rows kept in row format leave out the key columns, which output
    /// decodes from the keys. The window that output is buffered in is sized
    /// from [`Ranges::bytes_per_row`], so that has to count them: here keys
    /// of a kilobyte, held as a prefix, and of a hundred bytes, held whole,
    /// beside a four-byte payload.
    #[test]
    fn output_batches_stay_within_the_smallest_window() {
        use arrow::{
            array::{Int32Array, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};

        use crate::pipeline::sort::{MIN_OUTPUT_ROWS, Window};

        const ROWS: usize = 2000;
        const BATCH_SIZE: usize = 512;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0)))])
                .unwrap();
        let encoder = Arc::new(RowEncoder::try_new(&ordering, &schema).unwrap());
        for (key_length, keys) in [(1000, Keys::Prefix), (100, Keys::Fitted)] {
            let pad = "x".repeat(key_length - 7);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from_iter_values(
                        (0..ROWS).rev().map(|row| format!("{row:07}{pad}")),
                    )),
                    Arc::new(Int32Array::from_iter_values(0..2000)),
                ],
            )
            .unwrap();
            let mut rows = Some(encoder.encode(&batch).unwrap());
            let longest = rows.as_ref().unwrap().row(0).data().len();
            let words = Words::new(encoder.layout_for(longest));
            assert_eq!(words.layout.keys, keys);
            let scratch_bytes = 4 * 1024 * 1024;
            let chunk = chunk_words(words, scratch_bytes, 2);
            let mut lane = Lane::default();
            let mut payload = encoder.encode_payload(&batch).unwrap();
            assert!(lane.insert(
                &AtomicU32::new(0),
                &batch,
                &mut rows,
                &mut payload,
                words,
                chunk,
                &Always,
            ));
            let buffered = Buffered::take(&mut [&mut lane], Arc::clone(&encoder), words);
            let ranges = Ranges::split(
                buffered,
                1,
                chunk,
                scratch_bytes,
                0,
                &Count::new(),
                &Workers::try_new(2).unwrap(),
            );
            let row_bytes = ranges.bytes_per_row();
            // the least that the sort grants output: two small batches
            let window = Window::plan(
                2 * MIN_OUTPUT_ROWS * row_bytes,
                2,
                ROWS / BATCH_SIZE + 2,
                BATCH_SIZE,
                row_bytes,
            );
            let mut sent = Vec::new();
            let mut batcher = ranges
                .batcher(window.batch_rows, |batch| {
                    sent.push(batch.get_array_memory_size());
                    Ok(())
                })
                .compacting();
            ranges
                .visit_sorted(0, |records| batcher.push(records))
                .unwrap();
            batcher.finish().unwrap();
            // a part buffers this many batches before it waits
            let buffered: usize = sent.iter().take(window.batches).sum();
            assert!(
                buffered <= window.bytes,
                "keys of {key_length} bytes: {buffered} bytes buffered in a window of {}",
                window.bytes
            );
        }
    }

    #[test]
    fn splitters_place_records_as_whole_comparisons_would() {
        let mut random = Random(0x9E37_79B9_7F4A_7C15);
        let words = words();
        let order = Order::new(words, &[]);
        let drawn = records(&mut random, 4000);
        let mut samples: Vec<&[u64]> = drawn.chunks_exact(width()).collect();
        samples.sort_unstable();
        let splitters = RangeSplitters::from_samples(order, &samples, 64);
        assert!(splitters.count > 8);

        let mut probes = records(&mut random, 2000);
        // records that differ from every splitter where the splitters agree
        probes.extend([
            0,
            0,
            0,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            0x0100_0000_0000_0000,
            7,
            7,
        ]);
        for record in probes.chunks_exact(width()) {
            let expected = (0..splitters.count)
                .filter(|splitter| splitters.record(*splitter) <= record)
                .count();
            assert_eq!(splitters.range_of(record, order), expected, "{record:x?}");
        }
    }

    #[test]
    fn a_range_larger_than_the_scratch_is_sorted_in_pieces_and_merged() {
        let mut random = Random(0x9E37_79B9_7F4A_7C15);
        let words = words();
        let order = Order::new(words, &[]);
        let chunk = 50 * width();
        let records = records(&mut random, 1000);
        let mut expected: Vec<&[u64]> = records.chunks_exact(width()).collect();
        expected.sort_unstable();

        let chunks: Vec<Vec<u64>> = records.chunks(chunk).map(<[u64]>::to_vec).collect();
        let mut spare = Vec::new();
        let sorted = sort_in_pieces(chunks, order, chunk, 120, &mut spare);
        let sorted: Vec<&[u64]> = sorted
            .iter()
            .flat_map(|chunk| chunk.chunks_exact(width()))
            .collect();
        assert_eq!(sorted, expected);
    }

    #[test]
    fn sorted_entries_order_records_across_pieces() {
        let mut random = Random(0xD1B5_4A32_D192_ED03);
        let words = words();
        let order = Order::new(words, &[]);
        // the middle piece is empty
        let pieces = vec![
            records(&mut random, 700),
            Vec::new(),
            records(&mut random, 1300),
        ];
        let mut expected: Vec<&[u64]> = pieces
            .iter()
            .flat_map(|piece| piece.chunks_exact(width()))
            .collect();
        expected.sort_unstable();

        let varying = Varying::among(&pieces, words);
        // more key bytes differ than a summary holds
        assert!(varying.more);
        let sorted: Vec<&[u64]> = sorted_entries(&pieces, &varying, order)
            .iter()
            .map(|entry| entry.record(&pieces, width()))
            .collect();
        assert_eq!(sorted, expected);
    }

    #[test]
    fn full_key_summaries_order_keys_that_share_their_first_bytes() {
        use arrow::{
            array::StringArray,
            compute::SortOptions,
            datatypes::{DataType, Field, Schema},
        };
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};

        // keys of several lengths that agree for longer than a record holds
        let mut random = Random(0xA24B_AED4_963E_E407);
        let values: Vec<String> = (0..3000)
            .map(|_| {
                let digits = 1 + usize::try_from(random.below(40)).unwrap();
                format!("shared-by-every-key/{:0>digits$}", random.below(400))
            })
            .collect();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(values))],
        )
        .unwrap();
        let ordering = LexOrdering::new([PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions::default(),
        )])
        .unwrap();
        let encoder = RowEncoder::try_new(&ordering, &schema).unwrap();
        // as for keys too long for a record to hold whole
        let words = Words::new(encoder.layout_for(1000));
        assert_eq!(words.layout.keys, Keys::Prefix);
        let rows = encoder.encode(&batch).unwrap();
        // the middle piece is empty
        let mut pieces = vec![Vec::new(), Vec::new(), Vec::new()];
        for (row, key) in (0_u32..).zip(rows.iter()) {
            let piece = if row % 3 == 0 { 0 } else { 2 };
            words.push(&mut pieces[piece], key.data(), 0, row);
        }
        let rows = [Some(rows)];
        let order = Order::new(words, &rows);

        let mut expected: Vec<&[u8]> = pieces
            .iter()
            .flat_map(|piece| piece.chunks_exact(words.record))
            .map(|record| order.full_key(record))
            .collect();
        expected.sort_unstable();
        let sorted: Vec<&[u8]> = sorted_entries_of_full_keys(&pieces, order)
            .iter()
            .map(|entry| order.full_key(entry.record(&pieces, words.record)))
            .collect();
        assert_eq!(sorted, expected);
    }

    #[test]
    fn samples_are_taken_across_buffers_smaller_than_the_stride() {
        // ten buffers of three records each, and a stride of eight records
        let buffers: Vec<Vec<u64>> = (0..10_u64)
            .map(|buffer| (0..3).flat_map(|record| [buffer, record, 0]).collect())
            .collect();
        let sampled: Vec<(u64, u64)> = sample(&buffers, 3, 8)
            .iter()
            .map(|record| (record[0], record[1]))
            .collect();
        // records 4, 12, 20, and 28 of the thirty
        assert_eq!(sampled, [(1, 1), (4, 0), (6, 2), (9, 1)]);
    }

    #[test]
    fn records_with_one_key_have_no_varying_bytes() {
        let piece: Vec<u64> = [5, 6, 7].repeat(100);
        let varying = Varying::among(&[piece.clone(), piece], words());
        assert!(varying.lead.is_empty() && !varying.more);
    }
}
