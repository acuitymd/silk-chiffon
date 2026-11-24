//! Flushes of rows in row format: how their blocks are written, read back,
//! and merged.
//!
//! A row in a block is the lengths of its key and of the rest of it, then the
//! key, then the rest, which is what [`RowEncoder::encode_payload`] kept of
//! the row. The key is whole, whatever part of it the records in memory held,
//! so the rows of different flushes are ordered by comparing their key bytes.
//!
//! Writing a flush this way copies bytes the sort already holds, and reading
//! it back decodes them once, when merged rows become output batches. Written
//! as Arrow batches, every row was decoded while the input waited, encoded
//! for the file, and had its keys encoded again to be merged, by one thread
//! per group. Sorting 100M rows of 13 columns by six of them took 34 to 35 s
//! in 16 GiB and 39.5 s in 1 GiB that way, and takes 31 s and 30.5 s this
//! way.

use std::ops::Range;

use arrow::array::RecordBatch;
use datafusion::error::{DataFusionError, Result};

use super::{BlockFlush, BlockReader, BlockWriter, Part, Slices, Spill, WrittenIndex};
use crate::pipeline::sort::{
    keys::{Keys, RowEncoder},
    record::{MAX_WORDS, Words},
    run::{Kept, emit_split_on_overflow},
};

/// Bytes before each row in a block: the length of its key, then of the rest
/// of it, each a little-endian `u32`.
const ROW_HEADER: usize = 8;

/// The room a block has, as a multiple of its first key. The index keeps
/// the first key of every block in memory for as long as the flush exists, so
/// this keeps the index to a small share of what is spilled however long the
/// keys are. A block of 1 KiB holds about four rows with keys of 300 bytes,
/// so the index of such a flush would be about a quarter of its size.
pub(super) const BLOCK_KEY_MULTIPLE: usize = 64;

fn length(bytes: &[u8]) -> usize {
    u32::from_le_bytes(bytes.try_into().expect("four bytes")) as usize
}

/// The key and the rest of a row as it is in a block.
fn parts(row: &[u8]) -> (&[u8], &[u8]) {
    row[ROW_HEADER..].split_at(length(&row[..4]))
}

impl Spill {
    pub(super) fn row_writer(&self, what: &str) -> Result<RowWriter<'_>> {
        Ok(RowWriter {
            blocks: self.block_writer(what)?,
            block: Vec::with_capacity(self.row_block_bytes),
            block_rows: 0,
            block_end: self.row_block_bytes,
            index: WrittenIndex::default(),
            rows: 0,
        })
    }

    /// Calls `visit` with one slice's rows in sorted order, each as it is in
    /// a block.
    pub(super) fn visit_row_slice(
        &self,
        slices: &Slices,
        slice: usize,
        visit: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let (lower, upper) = slices.bounds_of(slice);
        // a key from one bound up to the other starts with what the bounds
        // share
        let shared = match (lower, upper) {
            (Some(lower), Some(upper)) => lower
                .iter()
                .zip(upper)
                .take_while(|(lower, upper)| lower == upper)
                .count(),
            _ => 0,
        };
        let readers = slices
            .flushes
            .iter()
            .map(|flush| RowReader::open(self, flush, lower, upper))
            .collect::<Result<Vec<_>>>()?;
        merge(readers, shared, visit)
    }
}

/// Writes rows to a spill file in blocks.
pub(super) struct RowWriter<'a> {
    blocks: BlockWriter<'a>,
    /// The block being filled.
    block: Vec<u8>,
    block_rows: usize,
    /// The most the block being filled holds: see [`BLOCK_KEY_MULTIPLE`].
    block_end: usize,
    index: WrittenIndex,
    rows: usize,
}

impl RowWriter<'_> {
    /// Writes the rows of `kept` that sorted records name.
    pub(super) fn push_kept(&mut self, kept: Kept<'_>, records: &[u64]) -> Result<()> {
        let words = self.blocks.spill.words();
        let mut inline = [[0_u8; 8]; MAX_WORDS];
        for record in records.chunks_exact(words.record) {
            let (batch, row) = Words::source(record);
            let rest = kept.payloads[batch as usize].row(row as usize).data();
            let kept_key = || {
                kept.key_rows[batch as usize]
                    .as_ref()
                    .expect("cut and prefix keys keep their rows while the run is in memory")
                    .row(row as usize)
                    .data()
            };
            if words.layout.keys != Keys::Prefix {
                // the record holds its key, padded to whole words
                for (bytes, word) in inline.iter_mut().zip(&record[..words.key]) {
                    *bytes = word.to_be_bytes();
                }
            }
            let key = match words.layout.keys {
                Keys::Whole => &inline.as_flattened()[..words.layout.inline],
                Keys::Fitted => words
                    .fitted_key(inline.as_flattened())
                    .unwrap_or_else(kept_key),
                Keys::Prefix => kept_key(),
            };
            let header = |bytes: &[u8]| {
                u32::try_from(bytes.len()).map_err(|_| {
                    DataFusionError::NotImplemented(format!(
                        "spilling a row of {} bytes from a sort",
                        bytes.len()
                    ))
                })
            };
            let (key_length, rest_length) = (header(key)?, header(rest)?);
            self.start_row(key, ROW_HEADER + key.len() + rest.len())?;
            self.block.extend_from_slice(&key_length.to_le_bytes());
            self.block.extend_from_slice(&rest_length.to_le_bytes());
            self.block.extend_from_slice(key);
            self.block.extend_from_slice(rest);
            self.end_row();
        }
        Ok(())
    }

    /// Writes a row as it was read back from a block.
    pub(super) fn push_row(&mut self, row: &[u8]) -> Result<()> {
        self.start_row(parts(row).0, row.len())?;
        self.block.extend_from_slice(row);
        self.end_row();
        Ok(())
    }

    /// Makes room for a row of `bytes` whose key is `key`. A block is written
    /// before a row that would take it past its end, so it holds no more than
    /// [`Spill::writer_bytes`] planned, unless one row alone is larger.
    fn start_row(&mut self, key: &[u8], bytes: usize) -> Result<()> {
        if !self.block.is_empty() && self.block.len() + bytes > self.block_end {
            self.write_block()?;
        }
        if self.block.is_empty() {
            self.index.firsts.extend_from_slice(key);
            self.block_end = self.blocks.spill.row_block_end(key.len());
            // a `Vec` that grows doubles, which no plan counts
            self.block.reserve_exact(self.block_end.max(bytes));
        }
        Ok(())
    }

    fn end_row(&mut self) {
        self.block_rows += 1;
        self.rows += 1;
    }

    /// Writes the block being filled.
    fn write_block(&mut self) -> Result<()> {
        self.blocks.write(&self.block)?;
        self.index.wrote(self.block_rows, self.block.len());
        self.block.clear();
        self.block_rows = 0;
        Ok(())
    }

    /// The file and what is in it. `None` when nothing was written.
    pub(super) fn finish(mut self) -> Result<Option<(Part, WrittenIndex)>> {
        if !self.block.is_empty() {
            self.write_block()?;
        }
        if self.rows == 0 {
            return Ok(None);
        }
        Ok(Some((self.blocks.finish(self.rows)?, self.index)))
    }
}

/// Reads the rows of one flush whose keys are at or after `lower` and before
/// `upper`, a block at a time.
struct RowReader<'a> {
    flush: &'a BlockFlush,
    blocks: BlockReader,
    /// The next block of the flush to read.
    next: usize,
    /// Rows with keys before this are not this reader's. Cleared once passed.
    lower: Option<&'a [u8]>,
    /// Rows with keys at or after this are not this reader's.
    upper: Option<&'a [u8]>,
    /// Where each row of the loaded block starts, and where the last one
    /// ends.
    starts: Vec<usize>,
    /// The row of the loaded block that the reader is at.
    at: usize,
    /// The first row of the loaded block that is not this reader's.
    end: usize,
}

impl<'a> RowReader<'a> {
    fn open(
        spill: &Spill,
        flush: &'a BlockFlush,
        lower: Option<&'a [u8]>,
        upper: Option<&'a [u8]>,
    ) -> Result<Self> {
        let mut reader = Self {
            flush,
            blocks: BlockReader::new(spill.compression)?,
            next: flush.start(lower),
            lower,
            upper,
            starts: Vec::new(),
            at: 0,
            end: 0,
        };
        reader.read_block()?;
        Ok(reader)
    }

    /// Whether every row has been taken.
    fn is_done(&self) -> bool {
        self.at == self.end
    }

    /// The row the reader is at, as it is in its block.
    fn row(&self) -> &[u8] {
        self.nth(self.at)
    }

    fn key(&self) -> &[u8] {
        parts(self.row()).0
    }

    fn nth(&self, row: usize) -> &[u8] {
        &self.blocks.block()[self.starts[row]..self.starts[row + 1]]
    }

    /// Moves to the next row, which is in the next block when the loaded one
    /// is used up.
    fn advance(&mut self) -> Result<()> {
        self.at += 1;
        if self.at == self.end {
            self.read_block()?;
        }
        Ok(())
    }

    /// Loads the next block that has rows for this reader. Leaves nothing
    /// loaded at the end.
    fn read_block(&mut self) -> Result<()> {
        loop {
            (self.at, self.end) = (0, 0);
            if self.next == self.flush.blocks.len() {
                return Ok(());
            }
            if self
                .upper
                .is_some_and(|upper| self.flush.first(self.next) >= upper)
            {
                // this block and every later one belong to later readers
                self.next = self.flush.blocks.len();
                return Ok(());
            }
            self.load(self.next)?;
            self.next += 1;
            let rows = self.starts.len() - 1;
            self.end = self
                .upper
                .map_or(rows, |upper| self.count_before(rows, upper));
            if let Some(lower) = self.lower.take() {
                self.at = self.count_before(self.end, lower);
            }
            if self.at < self.end {
                return Ok(());
            }
        }
    }

    /// How many of the first `rows` rows of the loaded block have keys before
    /// `bound`.
    fn count_before(&self, rows: usize, bound: &[u8]) -> usize {
        let (mut low, mut high) = (0, rows);
        while low < high {
            let middle = (low + high) / 2;
            if parts(self.nth(middle)).0 < bound {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }

    /// Reads one block of the flush and finds where its rows start.
    fn load(&mut self, block: usize) -> Result<()> {
        self.blocks.read(self.flush, block)?;
        let bytes = self.blocks.block();
        self.starts.clear();
        let mut at = 0;
        while at < bytes.len() {
            self.starts.push(at);
            let Some(header) = bytes.get(at..at + ROW_HEADER) else {
                break;
            };
            at += ROW_HEADER + length(&header[..4]) + length(&header[4..]);
        }
        if at != bytes.len() || self.starts.len() != self.flush.block_rows[block] {
            return Err(DataFusionError::Internal(format!(
                "a sort run on disk has a block that is not the {} rows written",
                self.flush.block_rows[block]
            )));
        }
        self.starts.push(at);
        Ok(())
    }
}

/// One input of a merge of rows, by the row it is at.
#[derive(Clone, Copy)]
struct Head {
    /// The start of that row's key: see [`lead`].
    lead: u64,
    input: usize,
}

/// The eight bytes of a key that follow its first `shared` bytes, as one
/// integer. Keys that agree on their first `shared` bytes are ordered the way
/// these are, where these differ: a key that ends early is padded with zeros,
/// and is then a prefix of any key it ties with up to its end.
fn lead(key: &[u8], shared: usize) -> u64 {
    let rest = key.get(shared..).unwrap_or_default();
    let mut bytes = [0_u8; 8];
    let held = rest.len().min(8);
    bytes[..held].copy_from_slice(&rest[..held]);
    u64::from_be_bytes(bytes)
}

/// Calls `visit` with the rows of sorted inputs in order, each as it is in a
/// block. Rows with equal keys come in the order of their inputs. Every key
/// starts with the same `shared` bytes.
///
/// The inputs are kept in a heap by the row each is at. Most comparisons are
/// settled by one integer: the bytes of the two keys that follow what every
/// key shares, which are the bytes that tell neighboring rows apart.
fn merge(
    mut readers: Vec<RowReader<'_>>,
    shared: usize,
    mut visit: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let mut heads: Vec<Head> = readers
        .iter()
        .enumerate()
        .filter(|(_, reader)| !reader.is_done())
        .map(|(input, reader)| Head {
            lead: lead(reader.key(), shared),
            input,
        })
        .collect();
    for at in (0..heads.len() / 2).rev() {
        sink(&mut heads, at, &readers);
    }
    while let Some(least) = heads.first().copied() {
        let reader = &mut readers[least.input];
        visit(reader.row())?;
        reader.advance()?;
        if reader.is_done() {
            heads.swap_remove(0);
        } else {
            heads[0].lead = lead(reader.key(), shared);
        }
        sink(&mut heads, 0, &readers);
    }
    Ok(())
}

/// Moves the head at `at` down the heap to where it belongs.
fn sink(heads: &mut [Head], mut at: usize, readers: &[RowReader<'_>]) {
    let before = |left: Head, right: Head| {
        left.lead
            .cmp(&right.lead)
            .then_with(|| readers[left.input].key().cmp(readers[right.input].key()))
            .then(left.input.cmp(&right.input))
            .is_lt()
    };
    loop {
        let mut least = at;
        for below in [2 * at + 1, 2 * at + 2] {
            if below < heads.len() && before(heads[below], heads[least]) {
                least = below;
            }
        }
        if least == at {
            return;
        }
        heads.swap(at, least);
        at = least;
    }
}

/// Turns rows read back from blocks, in sorted order, into batches.
pub(super) struct RowBatcher<'a, Emit> {
    encoder: &'a RowEncoder,
    /// The rows of the batch being built, back to back as they are in a
    /// block.
    rows: Vec<u8>,
    /// Where each of them ends.
    ends: Vec<usize>,
    batch_size: usize,
    emit: Emit,
}

impl<'a, Emit: FnMut(RecordBatch) -> Result<()>> RowBatcher<'a, Emit> {
    pub(super) fn new(encoder: &'a RowEncoder, batch_size: usize, emit: Emit) -> Self {
        Self {
            encoder,
            rows: Vec::new(),
            ends: Vec::new(),
            batch_size: batch_size.max(1),
            emit,
        }
    }

    pub(super) fn push(&mut self, row: &[u8]) -> Result<()> {
        self.rows.extend_from_slice(row);
        self.ends.push(self.rows.len());
        if self.ends.len() == self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// Emits the rows that did not fill a batch.
    pub(super) fn finish(mut self) -> Result<()> {
        self.flush()
    }

    fn flush(&mut self) -> Result<()> {
        if self.ends.is_empty() {
            return Ok(());
        }
        let (bytes, ends) = (&self.rows, &self.ends);
        let build = |rows: Range<usize>| {
            let first = rows.start.checked_sub(1).map_or(0, |before| ends[before]);
            let stored = || {
                ends[rows.clone()].iter().scan(first, |start, end| {
                    let row = &bytes[*start..*end];
                    *start = *end;
                    Some(parts(row))
                })
            };
            self.encoder
                .decode_stored(stored().map(|(key, _)| key), stored().map(|(_, rest)| rest))
        };
        emit_split_on_overflow(self.ends.len(), build, &mut self.emit)?;
        self.rows.clear();
        self.ends.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// A block of rows has room for a multiple of its first key, so keys
    /// longer than the first ones make larger blocks. Each still ends before
    /// the row that would take it past that, in a buffer of that size.
    #[tokio::test]
    async fn blocks_stay_within_the_room_their_first_key_gives() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::{
            config::SpillCompression,
            execution::runtime_env::RuntimeEnv,
            physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
            physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics},
        };

        use crate::pipeline::sort::workers::Workers;

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(Column::new("k", 0)))])
                .unwrap();
        let encoder = Arc::new(RowEncoder::try_new(&ordering, &schema).unwrap());
        let spill = Spill::new(
            Arc::clone(&encoder),
            Words::new(encoder.layout_for(24)),
            schema,
            ordering,
            512,
            64,
            4096,
            &Arc::new(RuntimeEnv::default()),
            SpillCompression::Uncompressed,
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            Arc::new(Workers::try_new(1).unwrap()),
        );
        let row = |key_bytes: usize, at: usize| {
            let key = format!("{at:0key_bytes$}");
            let rest = [7_u8; 100];
            let mut row = Vec::new();
            row.extend_from_slice(&u32::try_from(key.len()).unwrap().to_le_bytes());
            row.extend_from_slice(&u32::try_from(rest.len()).unwrap().to_le_bytes());
            row.extend_from_slice(key.as_bytes());
            row.extend_from_slice(&rest);
            row
        };
        let mut writer = spill.row_writer("test").unwrap();
        for at in 0..200 {
            writer.push_row(&row(20, at)).unwrap();
        }
        for at in 200..400 {
            writer.push_row(&row(2000, at)).unwrap();
        }
        let room = spill.row_block_end(2000);
        assert!(
            writer.block.capacity() <= room,
            "{}",
            writer.block.capacity()
        );
        let (_, index) = writer.finish().unwrap().unwrap();
        assert!(index.largest_block <= room, "{}", index.largest_block);
    }

    #[test]
    fn leads_order_keys_as_their_bytes_do_or_tie() {
        // every key starts with the same two bytes
        let keys: [&[u8]; 7] = [
            b"ab",
            b"ab\0",
            b"ab\0\0\0\0\0\0\0\0\x01",
            b"abc",
            b"abcdefghij1",
            b"abcdefghij2",
            b"abd",
        ];
        for left in keys {
            for right in keys {
                let leads = lead(left, 2).cmp(&lead(right, 2));
                assert!(
                    leads.is_eq() || leads == left.cmp(right),
                    "{left:?} against {right:?}"
                );
            }
        }
        // the eight bytes past the shared ones are what is compared
        assert!(lead(b"abcdefghij1", 2) < lead(b"abd", 2));
        assert_eq!(lead(b"abcdefghij1", 2), lead(b"abcdefghij2", 2));
    }
}
