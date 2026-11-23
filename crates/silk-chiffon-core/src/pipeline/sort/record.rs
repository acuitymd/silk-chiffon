//! Records as native integers.
//!
//! A record is a row's sort key in Arrow's row format, cut into 8-byte words
//! that are stored as native `u64`s. Two records then compare as a few
//! integers, which is how DuckDB lays out a sort key. On 8M export records,
//! sorting in place measured 30 ns a record this way against 39 ns comparing
//! the same bytes with `memcmp`. Merging sorted pieces by sorting them again
//! measured 7 to 14 ns against 19 to 36.

use std::cmp::Ordering;

use arrow::row::Rows;

use super::keys::{Contents, Keys, RecordLayout};

/// The most words in a record: the widest layout is 256 bytes.
pub(super) const MAX_WORDS: usize = 32;

/// How a row is laid out in a record of words.
#[derive(Clone, Copy, Debug)]
pub(super) struct Words {
    pub(super) layout: RecordLayout,
    /// Words in a record.
    pub(super) record: usize,
    /// The leading words, which hold key bytes. A record that names a kept row
    /// has one more: the batch in its high half and the row in its low half.
    pub(super) key: usize,
}

impl Words {
    pub(super) fn new(layout: RecordLayout) -> Self {
        let record = layout.record / 8;
        Self {
            layout,
            record,
            key: match layout.contents {
                Contents::Row => record,
                Contents::Source => record - 1,
            },
        }
    }

    pub(super) fn record_bytes(&self) -> usize {
        8 * self.record
    }

    /// Bytes of each key that [`Self::write_keys`] writes.
    pub(super) fn key_bytes(&self) -> usize {
        8 * self.key
    }

    /// Appends the record of one row. `batch` and `row` are used only by
    /// records that name a kept row.
    #[inline]
    pub(super) fn push(&self, target: &mut Vec<u64>, key: &[u8], batch: u32, row: u32) {
        let start = target.len();
        let fitted = self.layout.keys == Keys::Fitted;
        // the last inline byte of a fitted key is its length
        let held = self.layout.inline - usize::from(fitted);
        let inline = &key[..key.len().min(held)];
        let (words, rest) = inline.as_chunks::<8>();
        target.extend(words.iter().map(|word| u64::from_be_bytes(*word)));
        if !rest.is_empty() {
            let mut word = [0_u8; 8];
            word[..rest.len()].copy_from_slice(rest);
            target.push(u64::from_be_bytes(word));
        }
        // a short key is padded with zeros, which never orders it after a
        // longer key that it prefixes; the tie-break settles the rest
        target.resize(start + self.key, 0);
        if fitted {
            let length = if key.len() <= held {
                key.len()
            } else {
                self.layout.inline
            };
            target[start + self.key - 1] |= length as u64;
        }
        if self.layout.contents == Contents::Source {
            target.push((u64::from(batch) << 32) | u64::from(row));
        }
    }

    /// Whether a record of fitted keys holds only the start of its key.
    #[inline]
    pub(super) fn is_cut(&self, record: &[u64]) -> bool {
        record[self.key - 1] & 0xFF == self.layout.inline as u64
    }

    /// The key in the key bytes of one record of fitted keys, as
    /// [`Self::write_keys`] writes them. `None` when the key was cut.
    #[inline]
    pub(super) fn fitted_key<'a>(&self, bytes: &'a [u8]) -> Option<&'a [u8]> {
        let length = usize::from(bytes[self.layout.inline - 1]);
        (length < self.layout.inline).then(|| &bytes[..length])
    }

    /// The batch and row that a record names.
    #[inline]
    pub(super) fn source(record: &[u64]) -> (u32, u32) {
        let tail = record[record.len() - 1].to_be_bytes();
        (
            u32::from_be_bytes(tail[..4].try_into().expect("four bytes")),
            u32::from_be_bytes(tail[4..].try_into().expect("four bytes")),
        )
    }

    /// Appends the key bytes of each record, as the row format wrote them and
    /// zero padded to [`Self::key_bytes`].
    pub(super) fn write_keys(&self, records: &[u64], bytes: &mut Vec<u8>) {
        for record in records.chunks_exact(self.record) {
            for word in &record[..self.key] {
                bytes.extend_from_slice(&word.to_be_bytes());
            }
        }
    }
}

/// Orders records.
#[derive(Clone, Copy)]
pub(super) struct Order<'a> {
    words: Words,
    /// Full keys by batch, present when the key words are only a prefix.
    rows: &'a [Option<Rows>],
    /// Whether the key words decide the order without the full keys.
    settled: bool,
}

impl<'a> Order<'a> {
    pub(super) fn new(words: Words, rows: &'a [Option<Rows>]) -> Self {
        Self {
            words,
            rows,
            settled: words.layout.keys == Keys::Whole,
        }
    }

    /// Says whether the key words of the records at hand decide their order,
    /// for keys that are not held in full.
    pub(super) fn settled(mut self, settled: bool) -> Self {
        self.settled = settled;
        self
    }

    pub(super) fn words(&self) -> Words {
        self.words
    }

    /// Whether the key words decide the order without the full keys.
    pub(super) fn is_settled(&self) -> bool {
        self.settled
    }

    #[inline]
    pub(super) fn compare(&self, left: &[u64], right: &[u64]) -> Ordering {
        if self.settled {
            // the word after the key, if there is one, orders equal keys by
            // where they came from, which is as good an order as any
            return left.cmp(right);
        }
        let key = self.words.key;
        left[..key].cmp(&right[..key]).then_with(|| {
            if self.words.layout.keys == Keys::Fitted && !self.words.is_cut(left) {
                // fitted keys with the same words are the same key, unless
                // both were cut
                Ordering::Equal
            } else {
                self.full_key(left).cmp(self.full_key(right))
            }
        })
    }

    /// The whole key of a record whose key words are only the start of it.
    #[inline]
    pub(super) fn full_key(&self, record: &[u64]) -> &'a [u8] {
        let (batch, row) = Words::source(record);
        self.rows[batch as usize]
            .as_ref()
            .expect("cut and prefix keys keep their rows while the run is in memory")
            .row(row as usize)
            .data()
    }

    /// Puts sorted pieces that were copied one after another in order. This
    /// is how DuckDB merges: a sort that notices the pieces are sorted merges
    /// them, and does it faster than a heap over the pieces would.
    pub(super) fn sort_pieces(&self, records: &mut [u64]) {
        macro_rules! sort_sized {
            ($($words:literal),+) => {
                match self.words.record {
                    $($words => self.sort_sized::<$words>(records),)+
                    other => unreachable!("no record layout is {other} words"),
                }
            };
        }
        sort_sized!(
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32
        );
    }

    fn sort_sized<const WORDS: usize>(&self, records: &mut [u64]) {
        let (records, rest) = records.as_chunks_mut::<WORDS>();
        debug_assert!(rest.is_empty(), "a buffer holds whole records");
        // the standard stable sort is the one that merges existing runs
        if self.settled {
            records.sort();
        } else {
            records.sort_by(|left, right| self.compare(left, right));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row_layout(inline: usize) -> RecordLayout {
        RecordLayout {
            inline,
            record: inline.next_multiple_of(8),
            keys: Keys::Whole,
            contents: Contents::Row,
        }
    }

    #[test]
    fn words_keep_the_order_of_the_bytes() {
        let words = Words::new(row_layout(11));
        let keys: [&[u8]; 4] = [
            &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
            &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 9],
            &[0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0],
            &[9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ];
        let mut records = Vec::new();
        for key in keys.iter().rev() {
            words.push(&mut records, key, 0, 0);
        }
        assert_eq!(records.len(), 8);
        Order::new(words, &[]).sort_pieces(&mut records);

        let mut bytes = Vec::new();
        words.write_keys(&records, &mut bytes);
        let (padded, _) = bytes.as_chunks::<16>();
        let sorted: Vec<&[u8]> = padded.iter().map(|key| &key[..11]).collect();
        assert_eq!(sorted, keys);
    }

    #[test]
    fn a_record_names_its_batch_and_row() {
        let words = Words::new(RecordLayout {
            inline: 4,
            record: 16,
            keys: Keys::Whole,
            contents: Contents::Source,
        });
        let mut records = Vec::new();
        words.push(&mut records, &[1, 2, 3, 4], 7, u32::MAX);
        assert_eq!(records.len(), 2);
        assert_eq!(Words::source(&records), (7, u32::MAX));
    }

    #[test]
    fn sorting_pieces_merges_them() {
        let words = Words::new(row_layout(8));
        let mut records: Vec<u64> = [1, 4, 9, 2, 3, 10, 0, 5].to_vec();
        Order::new(words, &[]).sort_pieces(&mut records);
        assert_eq!(records, [0, 1, 2, 3, 4, 5, 9, 10]);
    }
}
