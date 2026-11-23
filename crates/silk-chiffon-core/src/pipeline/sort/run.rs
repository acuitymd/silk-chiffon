//! Rows buffered in memory as records, the merge of sorted records, and the
//! batches that sorted records become.
//!
//! Each input partition buffers its rows as records, in buffers of one size.
//! [`super::ranges`] takes the buffers as they are and splits them by key.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering as AtomicOrdering},
    },
};

use arrow::{
    array::{Array, ArrayData, ArrayRef, AsArray, RecordBatch},
    buffer::Buffer,
    compute::interleave_record_batch,
    datatypes::DataType,
    row::Rows,
};
use datafusion::error::Result;
use parking_lot::Mutex;

use super::keys::{Contents, KeyWidths, Keys, RowEncoder, cut_buckets, key_width_bucket};
use super::record::{MAX_WORDS, Order, Words};
use super::workers::Workers;

/// The leading bytes of whole keys that are examined when keys are held as a
/// prefix. Keys that first differ after this many bytes are compared in full.
const KEY_SPAN: usize = 256;

/// Grants memory to a lane. `false` means what is buffered should be flushed
/// first.
pub(super) trait Grant {
    fn grant(&self, bytes: usize) -> bool;
}

/// What one input partition has buffered since the last flush.
#[derive(Default)]
pub(super) struct Lane {
    /// Batches with their ids, which are unique within the flush. Kept when
    /// output is gathered from them.
    batches: Vec<(u32, RecordBatch)>,
    /// Whole rows in row format by batch id, kept in place of the batch when
    /// output is decoded from them.
    payloads: Vec<(u32, Rows)>,
    /// Full keys by batch id, kept when tie-breaks need them: for every
    /// batch of prefix keys, and for a batch of fitted keys with a key that
    /// was cut.
    rows: Vec<(u32, Rows)>,
    /// Records, in buffers of one size. A full buffer is left as it is and
    /// another is started, so buffered records are never copied to grow.
    buffers: Vec<Vec<u64>>,
    /// Bytes this lane holds from the budget.
    held: usize,
    /// The part of `held` that is the kept batches or rows.
    source_bytes: usize,
    buffered_rows: usize,
    /// Rows whose keys were too long for their records.
    cut_keys: usize,
    /// The widths of the keys buffered, when records fit their keys.
    key_widths: KeyWidths,
    /// The allocations behind the batches kept so far. Batches read from one
    /// Parquet page share its buffers, and each is counted once.
    kept_buffers: HashSet<usize>,
}

impl Lane {
    /// Buffers a batch as records, in buffers of `chunk` words. Returns
    /// `false` when the budget refused the memory.
    #[expect(clippy::too_many_arguments)]
    pub(super) fn insert(
        &mut self,
        next_id: &AtomicU32,
        batch: &RecordBatch,
        rows: &mut Option<Rows>,
        payload: &mut Option<Rows>,
        words: Words,
        chunk: usize,
        grant: &impl Grant,
    ) -> bool {
        let encoded = rows.as_ref().expect("rows are encoded before inserting");
        let layout = words.layout;
        let mut widths = KeyWidths::default();
        let cut_keys = match layout.keys {
            Keys::Fitted => {
                for key in encoded.iter() {
                    widths[key_width_bucket(key.data().len())] += 1;
                }
                widths[cut_buckets(layout.inline)].iter().sum()
            }
            Keys::Whole | Keys::Prefix => 0,
        };
        let keeps_keys = layout.keys == Keys::Prefix || cut_keys > 0;
        let key_bytes = if keeps_keys { encoded.size() } else { 0 };
        let (source_bytes, new_buffers) = match (layout.contents, payload.as_ref()) {
            (Contents::Row, _) => (0, Vec::new()),
            (Contents::Source, Some(payload)) => (payload.size(), Vec::new()),
            (Contents::Source, None) => new_buffers(batch, &self.kept_buffers),
        };
        let bytes = encoded.num_rows() * words.record_bytes() + source_bytes + key_bytes;
        if !grant.grant(bytes) {
            return false;
        }
        let id = match layout.contents {
            Contents::Row => 0,
            Contents::Source => {
                // ids are taken only for batches that stay, so they are dense
                let id = next_id.fetch_add(1, AtomicOrdering::AcqRel);
                match payload.take() {
                    Some(payload) => self.payloads.push((id, payload)),
                    None => {
                        self.batches.push((id, batch.clone()));
                        self.kept_buffers.extend(new_buffers);
                    }
                }
                id
            }
        };
        self.held += bytes;
        self.source_bytes += source_bytes;
        self.buffered_rows += batch.num_rows();
        self.cut_keys += cut_keys;
        for (total, width) in self.key_widths.iter_mut().zip(widths) {
            *total += width;
        }
        // the sort rejects batches with more rows than fit in 32 bits
        for (row, key) in (0_u32..).zip(encoded.iter()) {
            words.push(self.buffer_with_room(chunk), key.data(), id, row);
        }
        let encoded = rows.take().expect("checked above");
        if keeps_keys {
            self.rows.push((id, encoded));
        }
        true
    }

    /// The buffer to write the next record to. Buffers are filled to their
    /// capacity, which is a whole number of records.
    #[inline]
    fn buffer_with_room(&mut self, chunk: usize) -> &mut Vec<u64> {
        let full = self
            .buffers
            .last()
            .is_none_or(|buffer| buffer.len() == buffer.capacity());
        if full {
            self.buffers.push(Vec::with_capacity(chunk));
        }
        self.buffers.last_mut().expect("there is a buffer by now")
    }
}

/// The allocations behind `batch` that are not in `kept`, and their total
/// size. Keeping the batch keeps each of them alive in full, however little of
/// one the batch uses.
fn new_buffers(batch: &RecordBatch, kept: &HashSet<usize>) -> (usize, Vec<usize>) {
    let mut bytes = 0;
    let mut new = Vec::new();
    let mut count = |buffer: &Buffer| {
        let allocation = buffer.data_ptr().as_ptr() as usize;
        if !kept.contains(&allocation) && !new.contains(&allocation) {
            new.push(allocation);
            bytes += buffer.capacity();
        }
    };
    let mut pending: Vec<ArrayData> = batch
        .columns()
        .iter()
        .map(|column| column.to_data())
        .collect();
    while let Some(data) = pending.pop() {
        data.buffers().iter().for_each(&mut count);
        if let Some(nulls) = data.nulls() {
            count(nulls.buffer());
        }
        pending.extend(data.child_data().iter().cloned());
    }
    (bytes, new)
}

/// The rows that records name, where they are kept in row format.
#[derive(Clone, Copy)]
pub(super) struct Kept<'a> {
    /// What [`RowEncoder::encode_payload`] encoded of each row, by batch id.
    pub(super) payloads: &'a [Rows],
    /// Full keys by batch id, when records hold only a prefix.
    pub(super) key_rows: &'a [Option<Rows>],
}

/// Everything buffered between two flushes, gathered from the lanes.
pub(super) struct Buffered {
    pub(super) encoder: Arc<RowEncoder>,
    pub(super) words: Words,
    /// Source batches by id. Empty when records hold whole rows, or output is
    /// decoded from `payloads`.
    pub(super) batches: Vec<RecordBatch>,
    /// Whole rows in row format by batch id, when output is decoded from them.
    pub(super) payloads: Vec<Rows>,
    /// Full keys by batch id, where they were kept. Empty when every key is
    /// whole in its record.
    pub(super) rows: Vec<Option<Rows>>,
    /// The lanes' buffers of records, in the order the rows arrived.
    pub(super) buffers: Vec<Vec<u64>>,
    /// Whether the records' key words decide the order without the full keys.
    pub(super) settled: bool,
    /// Budget bytes held, for the owner to release.
    pub(super) held: usize,
    /// The part of `held` that is the kept batches or rows.
    pub(super) source_bytes: usize,
    pub(super) rows_total: usize,
    /// Rows whose keys were too long for their records.
    pub(super) cut_keys: usize,
    /// The widths of the keys buffered, when records fit their keys.
    pub(super) key_widths: KeyWidths,
}

impl Buffered {
    /// Takes the buffered state out of every lane.
    pub(super) fn take(lanes: &mut [&mut Lane], encoder: Arc<RowEncoder>, words: Words) -> Self {
        let lanes: Vec<Lane> = lanes
            .iter_mut()
            .map(|lane| std::mem::take(&mut **lane))
            .collect();
        let batches_kept: usize = lanes.iter().map(|lane| lane.batches.len()).sum();
        let rows_kept: usize = lanes.iter().map(|lane| lane.payloads.len()).sum();
        // a sort keeps batches or their rows, not both
        let batch_count = batches_kept + rows_kept;
        let mut batches: Vec<Option<RecordBatch>> = vec![None; batches_kept];
        let mut payloads: Vec<Option<Rows>> = Vec::new();
        payloads.resize_with(rows_kept, || None);
        let mut rows: Vec<Option<Rows>> = Vec::new();
        if words.layout.keys != Keys::Whole {
            rows.resize_with(batch_count, || None);
        }
        let mut buffers = Vec::new();
        let (mut held, mut source_bytes, mut rows_total, mut cut_keys) = (0, 0, 0, 0);
        let mut key_widths = KeyWidths::default();
        for lane in lanes {
            held += lane.held;
            source_bytes += lane.source_bytes;
            rows_total += lane.buffered_rows;
            cut_keys += lane.cut_keys;
            for (total, width) in key_widths.iter_mut().zip(lane.key_widths) {
                *total += width;
            }
            for (id, batch) in lane.batches {
                batches[id as usize] = Some(batch);
            }
            for (id, payload) in lane.payloads {
                payloads[id as usize] = Some(payload);
            }
            for (id, lane_rows) in lane.rows {
                rows[id as usize] = Some(lane_rows);
            }
            buffers.extend(lane.buffers.into_iter().filter(|buffer| !buffer.is_empty()));
        }
        Self {
            encoder,
            words,
            batches: batches
                .into_iter()
                .map(|batch| batch.expect("batch ids are dense within a flush"))
                .collect(),
            payloads: payloads
                .into_iter()
                .map(|payload| payload.expect("batch ids are dense within a flush"))
                .collect(),
            rows,
            buffers,
            settled: match words.layout.keys {
                Keys::Whole => true,
                // records of keys that all fit compare as their keys do
                Keys::Fitted => cut_keys == 0,
                Keys::Prefix => false,
            },
            held,
            source_bytes,
            rows_total,
            cut_keys,
            key_widths,
        }
    }

    /// Lays the records out as `words`, a wider layout of fitted keys, in
    /// buffers of `chunk` words. `extra` is the budget the wider records take
    /// beyond the ones they replace, which the caller has granted. Returns the
    /// bytes of whole keys no longer kept, for the caller to release: once no
    /// key is cut, the records alone order the rows.
    pub(super) fn widen(&mut self, words: Words, chunk: usize, extra: usize) -> usize {
        let old = self.words;
        let order = Order::new(old, &self.rows);
        let mut key_bytes = [0_u8; 8 * MAX_WORDS];
        let mut cut_keys = 0;
        let mut widened: Vec<Vec<u64>> = Vec::new();
        for buffer in std::mem::take(&mut self.buffers) {
            for record in buffer.chunks_exact(old.record) {
                let (batch, row) = Words::source(record);
                let key = if old.is_cut(record) {
                    order.full_key(record)
                } else {
                    for (bytes, word) in key_bytes
                        .as_chunks_mut::<8>()
                        .0
                        .iter_mut()
                        .zip(&record[..old.key])
                    {
                        bytes.copy_from_slice(&word.to_be_bytes());
                    }
                    old.fitted_key(&key_bytes).expect("a key that was not cut")
                };
                if key.len() >= words.layout.inline {
                    cut_keys += 1;
                }
                let full = widened
                    .last()
                    .is_none_or(|last| last.len() + words.record > last.capacity());
                if full {
                    widened.push(Vec::with_capacity(chunk.max(words.record)));
                }
                words.push(widened.last_mut().expect("pushed above"), key, batch, row);
            }
        }
        self.buffers = widened;
        self.words = words;
        self.cut_keys = cut_keys;
        self.held += extra;
        if cut_keys > 0 {
            return 0;
        }
        self.settled = true;
        let freed: usize = self
            .rows
            .iter_mut()
            .filter_map(Option::take)
            .map(|rows| rows.size())
            .sum();
        self.held -= freed;
        freed
    }

    pub(super) fn held(&self) -> usize {
        self.held
    }

    pub(super) fn num_rows(&self) -> usize {
        self.rows_total
    }

    /// Rewrites the key words of records whose keys are held as a prefix, from
    /// the bytes of the whole keys that tell these rows apart.
    ///
    /// A record of such a key holds its first 16 bytes. Keys often share more
    /// than that, and then every comparison fetches two whole keys. Sorting
    /// 30M rows by an 18-character name with a shared 9-character start took
    /// 4.95 s that way, with 45% of the transform's CPU time in comparing
    /// whole keys, and 2.22 s this way. DuckDB skips the bytes that a run's
    /// keys share. This does the same once for everything buffered: it finds
    /// the byte positions where any two keys differ and takes each record's
    /// key words from the first of those positions.
    ///
    /// Two keys first differ at a position where both have a byte, and that
    /// position is one of those. So the words order two keys the same way
    /// their bytes do, or call them equal. When the words hold every position
    /// that differs and the keys are all one length, equal words mean equal
    /// keys, and the full keys are not needed at all.
    pub(super) fn respell_keys(&mut self, workers: &Workers) {
        let (record, key) = (self.words.record, self.words.key);
        let order = Order::new(self.words, &self.rows);
        let buffers: Vec<Mutex<Vec<u64>>> = std::mem::take(&mut self.buffers)
            .into_iter()
            .map(Mutex::new)
            .collect();
        let seen = workers
            .map(buffers.len(), &|buffer| {
                let mut seen = KeyBytes::default();
                for record in buffers[buffer].lock().chunks_exact(record) {
                    seen.add(order.full_key(record));
                }
                seen
            })
            .into_iter()
            .fold(KeyBytes::default(), |left, right| left.merge(&right));
        let mut telling = (0..KEY_SPAN).filter(|at| seen.differs[*at]);
        let lead: Vec<usize> = telling.by_ref().take(8 * key).collect();
        self.settled =
            telling.next().is_none() && seen.shortest == seen.longest && seen.longest <= KEY_SPAN;
        workers.each(buffers.len(), &|buffer| {
            for record in buffers[buffer].lock().chunks_exact_mut(record) {
                let full_key = order.full_key(record);
                let mut bytes = [0_u8; 8 * MAX_WORDS];
                for (byte, at) in bytes.iter_mut().zip(&lead) {
                    // a key that ends before this position differs from every
                    // longer key before it ends
                    *byte = full_key.get(*at).copied().unwrap_or(0);
                }
                let (words, _) = bytes.as_chunks::<8>();
                for (word, bytes) in record[..key].iter_mut().zip(words) {
                    *word = u64::from_be_bytes(*bytes);
                }
            }
        });
        self.buffers = buffers.into_iter().map(Mutex::into_inner).collect();
    }
}

/// The records that divide the key space into groups. Every flush writes the
/// same groups, so a group of the output is the merge of that group from
/// every flush, and the groups merge independently.
pub(super) struct Splitters {
    words: Words,
    count: usize,
    /// The key words of each splitter, back to back and ascending, when they
    /// are the whole key.
    keys: Vec<u64>,
    /// Each splitter's whole key, when records hold a prefix. Their key words
    /// are spelled differently in every flush, and the rows that the
    /// splitters came from are gone after their own.
    full_keys: Vec<Vec<u8>>,
}

impl Splitters {
    /// Makes splitters of records that are in ascending order.
    pub(super) fn from_records<'a>(
        order: Order<'_>,
        records: impl Iterator<Item = &'a [u64]>,
    ) -> Self {
        let words = order.words();
        let mut splitters = Self {
            words,
            count: 0,
            keys: Vec::new(),
            full_keys: Vec::new(),
        };
        let mut previous: Option<&[u64]> = None;
        for record in records {
            // equal splitters would leave an empty group between them
            let same = previous.is_some_and(|previous| {
                if words.layout.keys == Keys::Whole {
                    previous[..words.key] == record[..words.key]
                } else {
                    order.full_key(previous) == order.full_key(record)
                }
            });
            if !same {
                if words.layout.keys == Keys::Whole {
                    splitters.keys.extend_from_slice(&record[..words.key]);
                } else {
                    splitters.full_keys.push(order.full_key(record).to_vec());
                }
                splitters.count += 1;
                previous = Some(record);
            }
        }
        splitters
    }

    pub(super) fn groups(&self) -> usize {
        self.count + 1
    }

    /// Whether a splitter sorts at or before `record`, which puts the record
    /// in the splitter's group or a later one.
    pub(super) fn precedes(&self, splitter: usize, record: &[u64], order: Order<'_>) -> bool {
        match self.full_keys.get(splitter) {
            Some(full_key) => full_key.as_slice() <= order.full_key(record),
            None => {
                let key = self.words.key;
                self.keys[splitter * key..][..key] <= record[..key]
            }
        }
    }
}

/// Which bytes of some keys differ from key to key.
struct KeyBytes {
    /// The first byte seen at each position below `known`.
    first: [u8; KEY_SPAN],
    known: usize,
    differs: [bool; KEY_SPAN],
    shortest: usize,
    longest: usize,
}

impl Default for KeyBytes {
    fn default() -> Self {
        Self {
            first: [0; KEY_SPAN],
            known: 0,
            differs: [false; KEY_SPAN],
            shortest: usize::MAX,
            longest: 0,
        }
    }
}

impl KeyBytes {
    fn add(&mut self, key: &[u8]) {
        let span = key.len().min(KEY_SPAN);
        let known = span.min(self.known);
        for ((differs, first), byte) in self.differs[..known]
            .iter_mut()
            .zip(&self.first[..known])
            .zip(key)
        {
            *differs |= first != byte;
        }
        if span > self.known {
            self.first[self.known..span].copy_from_slice(&key[self.known..span]);
            self.known = span;
        }
        self.shortest = self.shortest.min(key.len());
        self.longest = self.longest.max(key.len());
    }

    fn merge(mut self, other: &Self) -> Self {
        let known = self.known.min(other.known);
        for at in 0..KEY_SPAN {
            self.differs[at] |=
                other.differs[at] || (at < known && self.first[at] != other.first[at]);
        }
        if other.known > self.known {
            self.first[self.known..other.known]
                .copy_from_slice(&other.first[self.known..other.known]);
            self.known = other.known;
        }
        self.shortest = self.shortest.min(other.shortest);
        self.longest = self.longest.max(other.longest);
        self
    }
}

/// A sorted sequence of records that a merge reads from the front.
pub(super) trait Input {
    /// The records in memory that the merge has not taken, in order. Empty
    /// only at the end of the input.
    fn loaded(&self) -> &[u64];

    /// Drops the first `words` words of them, and loads more when none are
    /// left.
    fn consume(&mut self, words: usize) -> Result<()>;
}

/// Merges sorted inputs, calling `visit` with their records in order, a round
/// at a time.
///
/// A round takes from every input the records at or below the smallest of the
/// inputs' last loaded records. Nothing an input has yet to load can come
/// before those, so they can go out. They are copied into `round` one input
/// after another and sorted, which merges them. The input whose last record
/// was the smallest is used up by the round, so every round makes progress.
pub(super) fn merge_rounds<I: Input>(
    mut inputs: Vec<I>,
    order: Order<'_>,
    round: &mut Vec<u64>,
    mut visit: impl FnMut(&[u64]) -> Result<()>,
) -> Result<()> {
    let record = order.words().record;
    loop {
        inputs.retain(|input| !input.loaded().is_empty());
        if let [input] = inputs.as_mut_slice() {
            // one input left is already in order
            let loaded = input.loaded();
            visit(loaded)?;
            let words = loaded.len();
            input.consume(words)?;
            continue;
        }
        let Some(last) = inputs
            .iter()
            .map(|input| {
                let loaded = input.loaded();
                &loaded[loaded.len() - record..]
            })
            .min_by(|left, right| order.compare(left, right))
        else {
            return Ok(());
        };
        let mut bound = [0_u64; MAX_WORDS];
        let bound = &mut bound[..record];
        bound.copy_from_slice(last);

        round.clear();
        for input in &mut inputs {
            let loaded = input.loaded();
            let (mut low, mut high) = (0, loaded.len() / record);
            while low < high {
                let middle = (low + high) / 2;
                if order
                    .compare(&loaded[middle * record..][..record], bound)
                    .is_le()
                {
                    low = middle + 1;
                } else {
                    high = middle;
                }
            }
            round.extend_from_slice(&loaded[..low * record]);
            input.consume(low * record)?;
        }
        order.sort_pieces(round);
        visit(round)?;
    }
}

/// Turns records that arrive in sorted order into batches.
pub(super) struct Batcher<'a, Emit> {
    encoder: &'a RowEncoder,
    words: Words,
    /// Present when records name rows of kept batches.
    gatherer: Option<Gatherer<'a>>,
    /// Rows by batch id, when records name rows kept in row format.
    payloads: &'a [Rows],
    /// Full keys by batch id, for output that takes its key columns from them.
    key_rows: &'a [Option<Rows>],
    /// Whether output takes its key columns from the keys in the records.
    inline_keys: bool,
    /// The keys of the batch being built, when output is decoded from them.
    keys: Vec<u8>,
    /// The rows of the batch being built, when records name them.
    sources: Vec<(u32, u32)>,
    rows: usize,
    batch_size: usize,
    /// Whether gathered batches get buffers of their own for their strings.
    compact: bool,
    emit: Emit,
}

impl<'a, Emit: FnMut(RecordBatch) -> Result<()>> Batcher<'a, Emit> {
    /// Records that name their source name rows of `batches`, or of
    /// `payloads` when the rows were kept in row format, with their full keys
    /// in `key_rows` where records do not hold them whole. All three are
    /// unused for whole-row records.
    pub(super) fn new(
        encoder: &'a RowEncoder,
        words: Words,
        batches: &'a [RecordBatch],
        payloads: &'a [Rows],
        key_rows: &'a [Option<Rows>],
        batch_size: usize,
        emit: Emit,
    ) -> Self {
        let decodes = words.layout.contents == Contents::Source && !payloads.is_empty();
        Self {
            encoder,
            words,
            gatherer: (words.layout.contents == Contents::Source && payloads.is_empty())
                .then(|| Gatherer::new(batches)),
            payloads,
            key_rows,
            inline_keys: decodes
                && encoder.payload_needs_keys()
                && words.layout.keys != Keys::Prefix,
            keys: Vec::new(),
            sources: Vec::new(),
            rows: 0,
            batch_size: batch_size.max(1),
            compact: false,
            emit,
        }
    }

    /// Copies the strings of gathered view columns into buffers of each
    /// batch's own. For batches that go on to another operator: the spill
    /// writer makes that copy itself.
    pub(super) fn compacting(mut self) -> Self {
        self.compact = true;
        self
    }

    pub(super) fn push(&mut self, mut records: &[u64]) -> Result<()> {
        while !records.is_empty() {
            let rows = (records.len() / self.words.record).min(self.batch_size - self.rows);
            let (now, later) = records.split_at(rows * self.words.record);
            if self.words.layout.contents == Contents::Source {
                self.sources
                    .extend(now.chunks_exact(self.words.record).map(Words::source));
            }
            if self.words.layout.contents == Contents::Row || self.inline_keys {
                self.words.write_keys(now, &mut self.keys);
            }
            self.rows += rows;
            if self.rows == self.batch_size {
                self.flush()?;
            }
            records = later;
        }
        Ok(())
    }

    /// Emits the rows that did not fill a batch.
    pub(super) fn finish(mut self) -> Result<()> {
        self.flush()
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let batch = match (&mut self.gatherer, self.words.layout.contents) {
            (Some(gatherer), _) => {
                let batch = gatherer.gather(&self.sources)?;
                if self.compact {
                    compact_views(&batch)?
                } else {
                    batch
                }
            }
            (None, Contents::Source) => {
                let (payloads, key_rows) = (self.payloads, self.key_rows);
                let rows = self
                    .sources
                    .iter()
                    .map(|&(batch, row)| payloads[batch as usize].row(row as usize));
                let kept_key = |&(batch, row): &(u32, u32)| {
                    key_rows[batch as usize]
                        .as_ref()
                        .expect("cut and prefix keys keep their rows while the run is in memory")
                        .row(row as usize)
                };
                if self.inline_keys {
                    let parser = self.encoder.key_parser();
                    let words = self.words;
                    // key words are padded to whole words
                    let keys = self
                        .keys
                        .chunks_exact(words.key_bytes())
                        .zip(&self.sources)
                        .map(|(key, source)| match words.layout.keys {
                            Keys::Fitted => words
                                .fitted_key(key)
                                .map_or_else(|| kept_key(source), |key| parser.parse(key)),
                            Keys::Whole | Keys::Prefix => parser.parse(&key[..words.layout.inline]),
                        });
                    self.encoder.decode_payload(keys, rows)?
                } else if self.encoder.payload_needs_keys() {
                    let keys = self.sources.iter().map(kept_key);
                    self.encoder.decode_payload(keys, rows)?
                } else {
                    self.encoder.decode_payload(std::iter::empty(), rows)?
                }
            }
            (None, Contents::Row) => self
                .encoder
                .decode(self.keys.chunks_exact(self.words.key_bytes()))?,
        };
        self.keys.clear();
        self.sources.clear();
        self.rows = 0;
        (self.emit)(batch)
    }
}

/// Builds output batches from rows scattered across the kept batches.
struct Gatherer<'a> {
    batches: &'a [RecordBatch],
    /// For each source batch, the generation that last used it and its
    /// position in `used`. `interleave` scans every batch it is given, so each
    /// call gets only the batches its rows come from.
    slots: Vec<(u32, usize)>,
    generation: u32,
    used: Vec<&'a RecordBatch>,
    indices: Vec<(usize, usize)>,
}

impl<'a> Gatherer<'a> {
    fn new(batches: &'a [RecordBatch]) -> Self {
        Self {
            batches,
            slots: vec![(0, 0); batches.len()],
            generation: 0,
            used: Vec::new(),
            indices: Vec::new(),
        }
    }

    fn gather(&mut self, sources: &[(u32, u32)]) -> Result<RecordBatch> {
        self.generation += 1;
        self.used.clear();
        self.indices.clear();
        for &(batch, row) in sources {
            let slot = &mut self.slots[batch as usize];
            if slot.0 != self.generation {
                *slot = (self.generation, self.used.len());
                self.used.push(&self.batches[batch as usize]);
            }
            self.indices.push((slot.1, row as usize));
        }
        Ok(interleave_record_batch(&self.used, &self.indices)?)
    }
}

/// Copies the strings of view columns into buffers of the batch's own.
///
/// Gathering a view column moves only its 16-byte views, which still point
/// into the buffers of every batch a row came from. Whoever reads the strings
/// next then reads them from all over memory, and that reader is one task
/// downstream of the sort. Copying here spreads those reads over the sort's
/// threads: sorting 100M rows of 13 columns by five integer columns in memory
/// took 40 s without it and 25 s with it.
fn compact_views(batch: &RecordBatch) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| match column.data_type() {
            DataType::Utf8View => Arc::new(column.as_string_view().gc()) as ArrayRef,
            DataType::BinaryView => Arc::new(column.as_binary_view().gc()) as ArrayRef,
            _ => Arc::clone(column),
        })
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::sort::keys::RecordLayout;

    struct Random(u64);

    impl Random {
        fn below(&mut self, bound: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % bound
        }
    }

    /// Sorted records in memory, shown to the merge a window at a time.
    struct Windowed<'a> {
        rest: &'a [u64],
        window: usize,
    }

    impl Input for Windowed<'_> {
        fn loaded(&self) -> &[u64] {
            &self.rest[..self.rest.len().min(self.window)]
        }

        fn consume(&mut self, words: usize) -> Result<()> {
            self.rest = &self.rest[words..];
            Ok(())
        }
    }

    #[test]
    fn compacting_batches_hold_only_their_own_strings() {
        use arrow::{
            array::StringViewArray,
            compute::SortOptions,
            datatypes::{Field, Schema},
        };
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};

        // strings too long for a view, so each one lives in its batch's buffers
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Utf8View,
            false,
        )]));
        let kept: Vec<RecordBatch> = (0..3)
            .map(|batch| {
                let strings =
                    (0..500).map(|row| format!("batch {batch} row {row:04}, too long for a view"));
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(StringViewArray::from_iter_values(strings))],
                )
                .unwrap()
            })
            .collect();
        let ordering = LexOrdering::new([PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            SortOptions::default(),
        )])
        .unwrap();
        let encoder = RowEncoder::try_new(&ordering, &schema).unwrap();
        let words = Words::new(encoder.layout_for(1000));
        // one row of each batch
        let mut records = Vec::new();
        for (id, batch) in (0_u32..).zip(&kept) {
            let rows = encoder.encode(batch).unwrap();
            words.push(&mut records, rows.row(7).data(), id, 7);
        }
        let string_bytes = |compact: bool| {
            let mut bytes = 0;
            let emit = |batch: RecordBatch| {
                bytes = batch
                    .column(0)
                    .as_string_view()
                    .data_buffers()
                    .iter()
                    .map(Buffer::len)
                    .sum::<usize>();
                Ok(())
            };
            let mut batcher = Batcher::new(&encoder, words, &kept, &[], &[], 16, emit);
            if compact {
                batcher = batcher.compacting();
            }
            batcher.push(&records).unwrap();
            batcher.finish().unwrap();
            bytes
        };
        let row = "batch 0 row 0007, too long for a view".len();
        assert_eq!(string_bytes(true), 3 * row);
        // gathered as they are, the views keep the buffers they came from
        assert!(string_bytes(false) > 100 * row);
    }

    #[test]
    fn rounds_merge_inputs_whatever_they_have_loaded() {
        let mut random = Random(0xD1B5_4A32_D192_ED03);
        // one-word records with many repeated values
        let words = Words::new(RecordLayout {
            inline: 8,
            record: 8,
            keys: Keys::Whole,
            contents: Contents::Row,
        });
        let sorted: Vec<Vec<u64>> = [300, 1, 0, 700, 64]
            .into_iter()
            .map(|length| {
                let mut records: Vec<u64> = (0..length).map(|_| random.below(50)).collect();
                records.sort_unstable();
                records
            })
            .collect();
        let mut expected = sorted.concat();
        expected.sort_unstable();
        for window in [1, 7, 64, 10_000] {
            let inputs = sorted
                .iter()
                .map(|records| Windowed {
                    rest: records,
                    window,
                })
                .collect();
            let mut merged = Vec::new();
            merge_rounds(inputs, Order::new(words, &[]), &mut Vec::new(), |records| {
                merged.extend_from_slice(records);
                Ok(())
            })
            .unwrap();
            assert_eq!(merged, expected, "window {window}");
        }
    }
}
