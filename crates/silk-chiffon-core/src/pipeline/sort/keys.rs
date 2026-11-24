//! Rows as byte strings that compare with `memcmp`.
//!
//! Arrow's row format encodes columns into bytes whose lexicographic order is
//! the requested sort order, including direction and null placement. The sort
//! works on fixed-size records that hold those bytes inline, so comparing two
//! rows rarely follows a pointer. This module decides what a record holds and
//! encodes rows; [`super::record`] lays records out and orders them.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, new_null_array},
    compute::cast,
    datatypes::{DataType, Field, SchemaRef},
    row::{Row, RowConverter, RowParser, Rows, SortField},
};
use datafusion::{
    error::{DataFusionError, Result},
    physical_expr::{LexOrdering, PhysicalExpr, expressions::Column},
};

/// Bytes naming the source row at the end of a record: batch then row, each a
/// little-endian `u32`.
const RECORD_TAIL: usize = 8;

/// Key bytes held inline when the full key cannot be.
const KEY_PREFIX: usize = 16;

/// The widest key or row held inline in full. Wider keys sort by prefix and
/// break ties through the row format.
const MAX_INLINE: usize = 120;

/// The widest that keys of variable width are held inline, with the byte for
/// their length. It is more than fixed-width keys get: a key this wide that
/// is not in its record is fetched from row format for most comparisons,
/// which costs more than moving a record of 256 bytes. Sorting 100M rows by
/// a five-column key of up to 154 bytes took 9% less time in memory with the
/// keys whole in 168-byte records, 5% less in 16 GiB, and as long as before
/// in 4 GiB and 1 GiB.
const MAX_FITTED: usize = 248;

/// Buckets of [`KeyWidths`]: one for each multiple of eight bytes that a
/// fitted key and its length byte can take, and one for keys too long for any.
pub(super) const KEY_WIDTH_BUCKETS: usize = MAX_FITTED / 8 + 1;

/// How many keys took each multiple of eight bytes, with the byte for their
/// length: bucket `n` counts the keys that a fitted record holds whole when
/// its key is `8 * (n + 1)` bytes wide, and the last bucket the keys no record
/// holds whole.
pub(super) type KeyWidths = [usize; KEY_WIDTH_BUCKETS];

/// The bucket of [`KeyWidths`] a key of `bytes` falls in.
pub(super) fn key_width_bucket(bytes: usize) -> usize {
    ((bytes + 1).div_ceil(8) - 1).min(KEY_WIDTH_BUCKETS - 1)
}

/// The buckets of [`KeyWidths`] whose keys are cut in a fitted record of
/// `inline` bytes.
pub(super) fn cut_buckets(inline: usize) -> std::ops::RangeFrom<usize> {
    inline / 8..
}

/// Records are made wider when more than one key in this many was cut, as
/// wide as leaves no more than that cut.
const CUT_KEY_SHARE: usize = 16;

/// How much of its key a record holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::pipeline) enum Keys {
    /// Every key is as wide as the next, and a record holds all of it.
    Whole,
    /// Keys vary in width, and a record holds a key whole when it fits: the
    /// key, zeros up to the last inline byte, and in that byte the key's
    /// length. Such records compare the way their keys' bytes do. A key
    /// too long for that is cut: the record holds its first bytes, and the
    /// inline width where the length would be. Ties between cut keys are
    /// broken through the row format.
    Fitted,
    /// A record holds the first bytes of its key, and ties are broken through
    /// the row format.
    Prefix,
}

/// What a record holds besides its key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::pipeline) enum Contents {
    /// The record is the whole row: the sort key, then every other column.
    /// The input batch is not kept, and output is decoded from the records.
    Row,
    /// The record ends with the batch and row it came from. What is kept of
    /// the input to build output from is the encoder's choice: see
    /// [`RowEncoder::encode_payload`].
    Source,
}

/// How a row is laid out in a record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::pipeline) struct RecordLayout {
    /// Bytes compared inline.
    pub(in crate::pipeline) inline: usize,
    /// Total record size, including zero padding and any tail.
    pub(in crate::pipeline) record: usize,
    pub(in crate::pipeline) keys: Keys,
    pub(in crate::pipeline) contents: Contents,
}

impl RecordLayout {
    fn row(width: usize) -> Self {
        Self {
            inline: width,
            record: width.next_multiple_of(8).max(8),
            keys: Keys::Whole,
            contents: Contents::Row,
        }
    }

    fn source(fixed_key_width: Option<usize>) -> Self {
        let (inline, keys) = match fixed_key_width {
            Some(width) if width <= MAX_INLINE => (width, Keys::Whole),
            _ => (KEY_PREFIX, Keys::Prefix),
        };
        Self {
            inline,
            record: (inline + RECORD_TAIL).next_multiple_of(8),
            keys,
            contents: Contents::Source,
        }
    }

    /// The layout that holds keys of up to `longest` bytes whole, when a
    /// record can be that wide.
    fn fitted(longest: usize) -> Option<Self> {
        // whole words, the last byte of which is the key's length
        let inline = (longest + 1).next_multiple_of(8);
        (inline <= MAX_FITTED).then_some(Self {
            inline,
            record: inline + RECORD_TAIL,
            keys: Keys::Fitted,
            contents: Contents::Source,
        })
    }
}

/// Encodes batches into row format, and decodes whole-row records back.
pub(in crate::pipeline) struct RowEncoder {
    schema: SchemaRef,
    converter: RowConverter,
    /// What each row encodes, in order: the sort keys, then for whole-row
    /// records every other column.
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    /// How a row is laid out in a record, as far as the columns' types say:
    /// see [`Self::layout_for`].
    layout: RecordLayout,
    /// For whole-row records, each schema column's position in the row.
    row_positions: Vec<usize>,
    /// Encodes every column of a row for records that name their source,
    /// when the row format can hold them all.
    payload: Option<Payload>,
}

/// Rows kept in row format, to build output from.
///
/// A row of a kept batch is spread over a buffer per column, more for strings,
/// so gathering sorted output from kept batches reads that many places per
/// row, all over what is kept. In row format a row is one run of bytes.
/// Sorting 100M rows of 13 columns in memory took 25 to 36 s gathering from
/// batches and 20 to 31 s decoding from rows, in up to 30% less memory.
///
/// The sort keys are already kept in row format, so when they are plain
/// columns only the other columns are encoded here, and output takes the key
/// columns back from the keys. Encoding them again made a sort whose key is
/// most of the row take 48 s where it now takes 29 s.
struct Payload {
    converter: RowConverter,
    /// The schema column that each encoded column is.
    columns: Vec<usize>,
    /// The schema column that each sort key is, when output takes the key
    /// columns from the keys. `None` when the rows hold every column.
    key_columns: Option<Vec<usize>>,
}

impl Payload {
    /// `None` when rows cannot be kept in row format. The sort then keeps the
    /// batches they arrived in, which is slower: see the module docs of
    /// [`super`]. Three kinds of column cause it, and all are dictionaries.
    ///
    /// - **The row format cannot encode it:** a dictionary whose values are
    ///   nested, which is a list of any kind, a struct, a map, or a union.
    ///   [`RowConverter::supports_fields`] is asked.
    /// - **Arrow cannot rebuild it:** the row format encodes a dictionary
    ///   column as its values, so the column decodes to its values and is
    ///   cast back to a dictionary. Arrow has no such cast for values that
    ///   are booleans, durations, intervals, dictionaries, or run-end
    ///   encoded. The cast is tried here, on a batch of no rows, which shows
    ///   only that Arrow has it. A batch of output whose values are more
    ///   than its key type indexes fails the cast, and goes out as smaller
    ///   batches instead: see [`super::run::emit_split_on_overflow`].
    /// - **The cast back may not be exact:** a dictionary in a field that may
    ///   not be null: see `loses_dictionary`.
    ///
    /// Any of them counts wherever it is in a column's type: a list of
    /// dictionaries of booleans is one. Sort keys count too, as output takes
    /// the key columns back from the keys. This holds no list of types of its
    /// own for the first two, so which types those are follows the Arrow
    /// version in use, and those above are Arrow 59's.
    fn try_new(
        ordering: &LexOrdering,
        schema: &SchemaRef,
        keys: &RowConverter,
    ) -> Result<Option<Self>> {
        let every_column = || (0..schema.fields().len()).collect::<Vec<_>>();
        let key_columns: Option<Vec<usize>> = ordering
            .iter()
            .map(|sort| Some(sort.expr.downcast_ref::<Column>()?.index()))
            .collect();
        let (columns, key_columns) = match key_columns {
            Some(key_columns) => {
                let rest: Vec<usize> = every_column()
                    .into_iter()
                    .filter(|index| !key_columns.contains(index))
                    .collect();
                if rest.is_empty() {
                    // every column is a key: the rows hold them all, as the
                    // converter needs at least one
                    (every_column(), None)
                } else {
                    (rest, Some(key_columns))
                }
            }
            None => (every_column(), None),
        };
        let fields: Vec<SortField> = columns
            .iter()
            .map(|index| SortField::new(schema.field(*index).data_type().clone()))
            .collect();
        if fields.is_empty()
            || !RowConverter::supports_fields(&fields)
            || schema.fields().iter().any(|field| loses_dictionary(field))
        {
            return Ok(None);
        }
        let converter = RowConverter::new(fields)?;
        let payload = Self {
            converter,
            columns,
            key_columns,
        };
        // decoding no rows shows the types that decoding produces
        let empty = RecordBatch::new_empty(Arc::clone(schema));
        let encoded = payload.encode(&empty)?;
        let decoded = payload.converter.convert_rows(&encoded)?;
        let mut restored = decoded.iter().zip(&payload.columns).collect::<Vec<_>>();
        let key_rows;
        let decoded_keys;
        if let Some(key_columns) = &payload.key_columns {
            let key_arrays: Vec<ArrayRef> = key_columns
                .iter()
                .map(|index| Arc::clone(empty.column(*index)))
                .collect();
            key_rows = keys.convert_columns(&key_arrays)?;
            decoded_keys = keys.convert_rows(&key_rows)?;
            restored.extend(decoded_keys.iter().zip(key_columns));
        }
        let restorable = restored.iter().all(|(column, index)| {
            let wanted = schema.field(**index).data_type();
            // the cast is tried, not asked about: Arrow says it can cast
            // booleans, durations and intervals to dictionaries of them, and
            // then cannot
            column.data_type() == wanted || cast(column, wanted).is_ok()
        });
        Ok(restorable.then_some(payload))
    }

    fn encode(&self, batch: &RecordBatch) -> Result<Rows> {
        let columns: Vec<ArrayRef> = self
            .columns
            .iter()
            .map(|index| Arc::clone(batch.column(*index)))
            .collect();
        Ok(self.converter.convert_columns(&columns)?)
    }
}

impl RowEncoder {
    pub(in crate::pipeline) fn try_new(ordering: &LexOrdering, schema: &SchemaRef) -> Result<Self> {
        let key_types = key_types(ordering, schema)?;
        let key_fields = sort_fields(ordering, &key_types);
        let key_expressions: Vec<_> = ordering.iter().map(|sort| Arc::clone(&sort.expr)).collect();
        if let Some(whole_row) = Self::whole_row(ordering, schema, &key_types, &key_fields)? {
            return Ok(whole_row);
        }
        let key_width = fixed_width(&key_types, &key_fields)?;
        let converter = RowConverter::new(key_fields)?;
        let payload = Payload::try_new(ordering, schema, &converter)?;
        Ok(Self {
            schema: Arc::clone(schema),
            converter,
            expressions: key_expressions,
            layout: RecordLayout::source(key_width),
            row_positions: Vec::new(),
            payload,
        })
    }

    /// The encoder for whole-row records, when every column has a fixed width,
    /// the keys are plain columns, and the row fits inline.
    fn whole_row(
        ordering: &LexOrdering,
        schema: &SchemaRef,
        key_types: &[DataType],
        key_fields: &[SortField],
    ) -> Result<Option<Self>> {
        let key_columns: Option<Vec<usize>> = ordering
            .iter()
            .map(|sort| Some(sort.expr.downcast_ref::<Column>()?.index()))
            .collect();
        let Some(key_columns) = key_columns else {
            return Ok(None);
        };
        let rest: Vec<usize> = (0..schema.fields().len())
            .filter(|index| !key_columns.contains(index))
            .collect();
        let mut types = key_types.to_vec();
        let mut fields = key_fields.to_vec();
        let mut expressions: Vec<Arc<dyn PhysicalExpr>> =
            ordering.iter().map(|sort| Arc::clone(&sort.expr)).collect();
        for &index in &rest {
            let field = schema.field(index);
            types.push(field.data_type().clone());
            fields.push(SortField::new(field.data_type().clone()));
            expressions.push(Arc::new(Column::new(field.name(), index)));
        }
        if !RowConverter::supports_fields(&fields) {
            return Ok(None);
        }
        let Some(width) = fixed_width(&types, &fields)?.filter(|width| *width <= MAX_INLINE) else {
            return Ok(None);
        };
        let row_positions = (0..schema.fields().len())
            .map(|index| {
                key_columns
                    .iter()
                    .position(|key| *key == index)
                    .or_else(|| {
                        rest.iter()
                            .position(|other| *other == index)
                            .map(|position| key_columns.len() + position)
                    })
                    .expect("every column is a key or one of the rest")
            })
            .collect();
        Ok(Some(Self {
            schema: Arc::clone(schema),
            converter: RowConverter::new(fields)?,
            expressions,
            layout: RecordLayout::row(width),
            row_positions,
            payload: None,
        }))
    }

    /// How rows are laid out in the records of a sort whose first batch has
    /// keys of up to `longest` bytes.
    ///
    /// Only keys of variable width depend on the rows. Their width is not
    /// known until rows arrive, and a record that holds such a key whole is
    /// worth waiting for: comparing two records never leaves them, where a
    /// record that holds 16 bytes of a key sends most comparisons to the
    /// keys in row format, each wherever it is in memory. Sorting 100M rows
    /// by a six-column key with two strings in it spent 10 to 11 s putting
    /// them in order with 16 bytes of each key in its record, and 3.3 to
    /// 3.8 s with the keys whole, in 7% less memory.
    ///
    /// Rows kept as the batches they arrived in keep prefixes: they are
    /// spilled in groups whose bounds are whole keys.
    pub(in crate::pipeline) fn layout_for(&self, longest: usize) -> RecordLayout {
        if self.layout.keys == Keys::Prefix && self.payload.is_some() {
            RecordLayout::fitted(longest).unwrap_or(self.layout)
        } else {
            self.layout
        }
    }

    /// A wider layout than `current` for keys whose widths are `widths`, when
    /// more than one in [`CUT_KEY_SHARE`] of them were cut: the narrowest that
    /// cuts no more than that, or the widest there is.
    ///
    /// The first batch to arrive decides how wide records are, and can be one
    /// of short or null keys before long ones. Most comparisons of cut keys
    /// read both whole keys from row format, which is far slower than
    /// comparing records, and cut keys keep those whole keys in memory besides.
    pub(super) fn wider_layout(
        &self,
        current: RecordLayout,
        widths: &KeyWidths,
    ) -> Option<RecordLayout> {
        if current.keys != Keys::Fitted {
            return None;
        }
        let keys: usize = widths.iter().sum();
        let cut: usize = widths[cut_buckets(current.inline)].iter().sum();
        if cut * CUT_KEY_SHARE <= keys {
            return None;
        }
        let mut uncut = 0;
        let bucket = (0..KEY_WIDTH_BUCKETS - 1)
            .find(|bucket| {
                uncut += widths[*bucket];
                (keys - uncut) * CUT_KEY_SHARE <= keys
            })
            .unwrap_or(KEY_WIDTH_BUCKETS - 2);
        let inline = 8 * (bucket + 1);
        (inline > current.inline)
            .then(|| RecordLayout::fitted(inline - 1))
            .flatten()
    }

    /// What records hold besides their keys.
    pub(in crate::pipeline) fn contents(&self) -> Contents {
        self.layout.contents
    }

    pub(in crate::pipeline) fn encode(&self, batch: &RecordBatch) -> Result<Rows> {
        let columns = self
            .expressions
            .iter()
            .map(|expression| expression.evaluate(batch)?.into_array(batch.num_rows()))
            .collect::<Result<Vec<ArrayRef>>>()?;
        let rows = self.converter.convert_columns(&columns)?;
        if self.layout.keys == Keys::Whole
            && rows.num_rows() > 0
            && rows.row(0).data().len() != self.layout.inline
        {
            return Err(DataFusionError::Internal(format!(
                "a row encoded to {} bytes, expected {}",
                rows.row(0).data().len(),
                self.layout.inline
            )));
        }
        Ok(rows)
    }

    /// Every column of `batch` in row format, for records that name their
    /// source. `None` when there is nothing to encode: records hold whole
    /// rows, or rows cannot be kept in row format and the batch is kept
    /// instead, as `Payload::try_new` decided.
    pub(in crate::pipeline) fn encode_payload(&self, batch: &RecordBatch) -> Result<Option<Rows>> {
        self.payload
            .as_ref()
            .map(|payload| payload.encode(batch))
            .transpose()
    }

    /// Whether rows are kept in row format, for records that name their
    /// source: [`Self::encode_payload`] encodes them.
    pub(in crate::pipeline) fn keeps_rows(&self) -> bool {
        self.payload.is_some()
    }

    /// Whether [`Self::decode_payload`] takes the key columns from the keys.
    pub(in crate::pipeline) fn payload_needs_keys(&self) -> bool {
        self.payload
            .as_ref()
            .is_some_and(|payload| payload.key_columns.is_some())
    }

    /// Parses keys held inline in records, for [`Self::decode_payload`].
    pub(in crate::pipeline) fn key_parser(&self) -> RowParser {
        self.converter.parser()
    }

    /// Rebuilds a batch from rows that [`Self::encode_payload`] encoded, and
    /// from the keys of the same rows when [`Self::payload_needs_keys`].
    ///
    /// Fails with Arrow's dictionary key overflow when the rows hold more
    /// values of a dictionary column than its key type indexes: see
    /// [`super::run::emit_split_on_overflow`].
    pub(in crate::pipeline) fn decode_payload<'a>(
        &self,
        keys: impl IntoIterator<Item = Row<'a>>,
        rows: impl IntoIterator<Item = Row<'a>>,
    ) -> Result<RecordBatch> {
        let payload = self
            .payload
            .as_ref()
            .expect("rows were encoded, so there is a payload");
        let mut columns: Vec<Option<ArrayRef>> = vec![None; self.schema.fields().len()];
        if let Some(key_columns) = &payload.key_columns {
            for (column, index) in self
                .converter
                .convert_rows(keys)?
                .into_iter()
                .zip(key_columns)
            {
                // a column that is a key twice is taken from the first
                columns[*index].get_or_insert(column);
            }
        }
        for (column, index) in payload
            .converter
            .convert_rows(rows)?
            .into_iter()
            .zip(&payload.columns)
        {
            columns[*index] = Some(column);
        }
        let columns = columns
            .into_iter()
            .zip(self.schema.fields())
            .map(|(column, field)| {
                let column = column.expect("every column is a key or is encoded");
                // dictionaries decode to their values
                if column.data_type() == field.data_type() {
                    Ok(column)
                } else {
                    Ok(cast(&column, field.data_type())?)
                }
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }

    /// [`Self::decode_payload`] for rows that were written to disk and read
    /// back as bytes: each row's whole key, and what
    /// [`Self::encode_payload`] encoded of it.
    pub(in crate::pipeline) fn decode_stored<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a [u8]>,
        rows: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<RecordBatch> {
        let payload = self
            .payload
            .as_ref()
            .expect("rows were encoded, so there is a payload");
        let (key_parser, row_parser) = (self.converter.parser(), payload.converter.parser());
        let rows = rows.into_iter().map(|row| row_parser.parse(row));
        if payload.key_columns.is_some() {
            self.decode_payload(keys.into_iter().map(|key| key_parser.parse(key)), rows)
        } else {
            self.decode_payload(std::iter::empty(), rows)
        }
    }

    /// Rebuilds a batch from whole-row records.
    pub(in crate::pipeline) fn decode<'a>(
        &self,
        records: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<RecordBatch> {
        debug_assert_eq!(self.layout.contents, Contents::Row);
        let parser = self.converter.parser();
        let width = self.layout.inline;
        let columns = self.converter.convert_rows(
            records
                .into_iter()
                .map(|record| parser.parse(&record[..width])),
        )?;
        let columns = self
            .row_positions
            .iter()
            .map(|position| Arc::clone(&columns[*position]))
            .collect();
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

/// Whether a column is, or holds, a dictionary that its values in row format
/// may not rebuild exactly. The row format holds a dictionary as its values,
/// and output casts them back. A field that may not be null can still have a
/// key that names a null value, which comes back as a null key.
fn loses_dictionary(field: &Field) -> bool {
    // a stack, not recursion: a schema's nesting is input
    let mut pending = vec![field];
    while let Some(field) = pending.pop() {
        match field.data_type() {
            DataType::Dictionary(..) if !field.is_nullable() => return true,
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::ListView(child)
            | DataType::LargeListView(child)
            | DataType::FixedSizeList(child, _)
            | DataType::Map(child, _) => pending.push(child),
            DataType::Struct(children) => pending.extend(children.iter().map(AsRef::as_ref)),
            DataType::Union(children, _) => {
                pending.extend(children.iter().map(|(_, child)| child.as_ref()));
            }
            DataType::RunEndEncoded(_, values) => pending.push(values),
            _ => {}
        }
    }
    false
}

/// Whether the row format can encode this ordering. Planning falls back to
/// DataFusion's sort when it cannot, which in Arrow 59 is when a sort key is,
/// or holds, a dictionary of nested values.
pub(in crate::pipeline) fn supports(ordering: &LexOrdering, schema: &SchemaRef) -> bool {
    key_types(ordering, schema)
        .is_ok_and(|types| RowConverter::supports_fields(&sort_fields(ordering, &types)))
}

fn key_types(ordering: &LexOrdering, schema: &SchemaRef) -> Result<Vec<DataType>> {
    ordering
        .iter()
        .map(|sort| sort.expr.data_type(schema))
        .collect()
}

fn sort_fields(ordering: &LexOrdering, types: &[DataType]) -> Vec<SortField> {
    ordering
        .iter()
        .zip(types)
        .map(|(sort, data_type)| SortField::new_with_options(data_type.clone(), sort.options))
        .collect()
}

/// The encoded width of a row whose columns all have a fixed width.
///
/// Measured by encoding one null row, so it tracks the row format instead of
/// restating its per-type widths.
fn fixed_width(types: &[DataType], fields: &[SortField]) -> Result<Option<usize>> {
    if !types.iter().all(is_fixed_width) {
        return Ok(None);
    }
    let converter = RowConverter::new(fields.to_vec())?;
    let nulls: Vec<ArrayRef> = types
        .iter()
        .map(|data_type| new_null_array(data_type, 1))
        .collect();
    let rows = converter.convert_columns(&nulls)?;
    Ok(Some(rows.row(0).data().len()))
}

fn is_fixed_width(data_type: &DataType) -> bool {
    data_type.primitive_width().is_some()
        || matches!(data_type, DataType::Boolean | DataType::FixedSizeBinary(_))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, StringArray},
        compute::SortOptions,
        datatypes::{Field, Schema},
    };
    use datafusion::physical_expr::PhysicalSortExpr;

    use super::{
        super::record::{Order, Words},
        *,
    };

    fn ordering(schema: &SchemaRef, columns: &[(&str, SortOptions)]) -> LexOrdering {
        LexOrdering::new(columns.iter().map(|(name, options)| {
            PhysicalSortExpr::new(
                Arc::new(Column::new_with_schema(name, schema).unwrap()),
                *options,
            )
        }))
        .unwrap()
    }

    #[test]
    fn payload_rebuilds_rows_of_many_types() {
        use arrow::{
            array::{
                Array, BooleanArray, Decimal128Array, DictionaryArray, ListArray, StringViewArray,
                UInt32Array,
            },
            compute::take_record_batch,
            datatypes::Int32Type,
        };

        let tags: DictionaryArray<Int32Type> =
            vec![Some("x"), None, Some("y"), Some("x"), Some("z")]
                .into_iter()
                .collect();
        let parts = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
            Some(vec![Some(7)]),
            Some(vec![Some(2), Some(3), Some(4)]),
        ]);
        let amounts = Decimal128Array::from(vec![Some(125), Some(-4), None, Some(0), Some(99)])
            .with_precision_and_scale(18, 2)
            .unwrap();
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8View, true),
            Field::new("tag", tags.data_type().clone(), true),
            Field::new("parts", parts.data_type().clone(), true),
            Field::new("amount", amounts.data_type().clone(), true),
            Field::new("flag", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringViewArray::from(vec![
                    Some("a string too long to be held in a view"),
                    Some("short"),
                    None,
                    Some(""),
                    Some("another string that needs a buffer"),
                ])),
                Arc::new(tags),
                Arc::new(parts),
                Arc::new(amounts),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(false),
                    Some(true),
                    None,
                ])),
            ],
        )
        .unwrap();
        let encoder = RowEncoder::try_new(
            &ordering(&schema, &[("name", SortOptions::default())]),
            &schema,
        )
        .unwrap();
        let payload = encoder
            .encode_payload(&batch)
            .unwrap()
            .expect("the row format holds every one of these columns");
        // the key column is rebuilt from the keys
        assert!(encoder.payload_needs_keys());
        let keys = encoder.encode(&batch).unwrap();
        let picked = [4_u32, 0, 2, 2, 1];
        let rebuilt = encoder
            .decode_payload(
                picked.iter().map(|row| keys.row(*row as usize)),
                picked.iter().map(|row| payload.row(*row as usize)),
            )
            .unwrap();
        let expected = take_record_batch(&batch, &UInt32Array::from(picked.to_vec())).unwrap();
        assert_eq!(rebuilt, expected);
    }

    #[test]
    fn a_key_that_is_not_a_column_leaves_every_column_in_the_payload() {
        use arrow::array::UInt32Array;
        use arrow::compute::take_record_batch;
        use datafusion::{
            logical_expr::Operator,
            physical_expr::expressions::{BinaryExpr, lit},
        };

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let doubled: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Multiply,
            lit(2_i32),
        ));
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new(doubled, SortOptions::default())]).unwrap();
        let encoder = RowEncoder::try_new(&ordering, &schema).unwrap();
        assert!(!encoder.payload_needs_keys());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(3), None, Some(-1)])),
                Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
            ],
        )
        .unwrap();
        let payload = encoder.encode_payload(&batch).unwrap().unwrap();
        let picked = [2_u32, 0, 1];
        let rebuilt = encoder
            .decode_payload(
                std::iter::empty(),
                picked.iter().map(|row| payload.row(*row as usize)),
            )
            .unwrap();
        let expected = take_record_batch(&batch, &UInt32Array::from(picked.to_vec())).unwrap();
        assert_eq!(rebuilt, expected);
    }

    /// The row format decodes a dictionary column to its values, and the
    /// sort casts them back. Arrow cannot build a dictionary of some types,
    /// and rows with such a column are kept as the batches they arrived in.
    #[test]
    fn a_dictionary_arrow_cannot_rebuild_is_not_kept_in_row_format() {
        use arrow::datatypes::{IntervalUnit, TimeUnit};

        let dictionary =
            |values: DataType| DataType::Dictionary(Box::new(DataType::Int32), Box::new(values));
        let keeps_rows = |column: DataType| {
            let schema: SchemaRef = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, true),
                Field::new("c", column, true),
            ]));
            RowEncoder::try_new(
                &ordering(&schema, &[("k", SortOptions::default())]),
                &schema,
            )
            .unwrap()
            .keeps_rows()
        };
        assert!(keeps_rows(dictionary(DataType::Utf8)));
        assert!(keeps_rows(dictionary(DataType::Int64)));
        // a field that may not be null can still name null values
        let required: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("c", dictionary(DataType::Utf8), false),
        ]));
        assert!(
            !RowEncoder::try_new(
                &ordering(&required, &[("k", SortOptions::default())]),
                &required
            )
            .unwrap()
            .keeps_rows()
        );
        for values in [
            DataType::Boolean,
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Interval(IntervalUnit::DayTime),
            dictionary(DataType::Utf8),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", DataType::Utf8, true)),
            ),
        ] {
            assert!(!keeps_rows(dictionary(values.clone())), "{values}");
            // as the sort key, whose column output takes back from the keys
            let keyed: SchemaRef = Arc::new(Schema::new(vec![
                Field::new("k", dictionary(values.clone()), true),
                Field::new("c", DataType::Utf8, true),
            ]));
            let ordered = ordering(&keyed, &[("k", SortOptions::default())]);
            assert!(supports(&ordered, &keyed), "{values}");
            assert!(
                !RowEncoder::try_new(&ordered, &keyed).unwrap().keeps_rows(),
                "a key of {values}"
            );
            // inside another column as well
            let list = DataType::List(Arc::new(Field::new_list_field(
                dictionary(values.clone()),
                true,
            )));
            assert!(!keeps_rows(list), "a list of {values}");
        }
    }

    #[test]
    fn whole_row_records_keep_no_payload() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int16, true),
        ]));
        let encoder = RowEncoder::try_new(
            &ordering(&schema, &[("a", SortOptions::default())]),
            &schema,
        )
        .unwrap();
        assert_eq!(encoder.contents(), Contents::Row);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(arrow::array::Int16Array::from(vec![3, 4])),
            ],
        )
        .unwrap();
        assert!(encoder.encode_payload(&batch).unwrap().is_none());
    }

    #[test]
    fn fixed_width_keys_are_inline_in_full() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int16, true),
        ]));
        let encoder = RowEncoder::try_new(
            &ordering(
                &schema,
                &[("a", SortOptions::default()), ("b", SortOptions::default())],
            ),
            &schema,
        )
        .unwrap();
        // one validity byte per column, then the value; both columns are
        // keys, so the record is the row and needs no tail
        assert_eq!(
            encoder.layout_for(0),
            RecordLayout {
                inline: 8,
                record: 8,
                keys: Keys::Whole,
                contents: Contents::Row,
            }
        );
    }

    #[test]
    fn a_variable_width_column_makes_records_name_their_source() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let encoder = RowEncoder::try_new(
            &ordering(&schema, &[("a", SortOptions::default())]),
            &schema,
        )
        .unwrap();
        assert_eq!(
            encoder.layout_for(0),
            RecordLayout {
                inline: 5,
                record: 16,
                keys: Keys::Whole,
                contents: Contents::Source,
            }
        );
    }

    #[test]
    fn whole_row_records_decode_to_the_original_columns() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Float64, true),
            Field::new("b", DataType::Int16, true),
            Field::new("flag", DataType::Boolean, false),
            Field::new("a", DataType::Int32, true),
        ]));
        let descending = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let encoder = RowEncoder::try_new(
            &ordering(&schema, &[("a", descending), ("b", SortOptions::default())]),
            &schema,
        )
        .unwrap();
        let layout = encoder.layout_for(0);
        assert_eq!(layout.contents, Contents::Row);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Float64Array::from(vec![
                    Some(0.5),
                    None,
                    Some(-0.0),
                    Some(f64::NAN),
                ])),
                Arc::new(arrow::array::Int16Array::from(vec![
                    Some(2),
                    Some(1),
                    None,
                    Some(1),
                ])),
                Arc::new(arrow::array::BooleanArray::from(vec![
                    true, false, true, false,
                ])),
                Arc::new(Int32Array::from(vec![Some(7), Some(7), Some(9), None])),
            ],
        )
        .unwrap();
        let rows = encoder.encode(&batch).unwrap();
        let words = Words::new(layout);
        let mut records = Vec::new();
        for (row, key) in (0_u32..).zip(&rows) {
            words.push(&mut records, key.data(), 0, row);
        }
        let decode = |records: &[u64]| {
            let mut keys = Vec::new();
            words.write_keys(records, &mut keys);
            encoder
                .decode(keys.chunks_exact(words.key_bytes()))
                .unwrap()
        };
        // unsorted records decode to the batch they came from, bit for bit
        let decoded = decode(&records);
        assert_eq!(decoded.schema(), schema);
        assert_eq!(format!("{decoded:?}"), format!("{batch:?}"));

        Order::new(words, &[]).sort_pieces(&mut records);
        let sorted = decode(&records);
        let a: Vec<Option<i32>> = sorted
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(a, vec![Some(9), Some(7), Some(7), None]);
    }

    #[test]
    fn variable_width_keys_sort_by_prefix_then_full_key() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let encoder = RowEncoder::try_new(
            &ordering(&schema, &[("s", SortOptions::default())]),
            &schema,
        )
        .unwrap();
        // as for keys too long for a record to hold whole
        let layout = encoder.layout_for(1000);
        assert_eq!(layout.keys, Keys::Prefix);

        // these agree on more bytes than the prefix holds
        let values = vec![
            "a shared prefix longer than sixteen bytes: zebra",
            "a shared prefix longer than sixteen bytes: apple",
            "a",
            "",
        ];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(values.clone()))],
        )
        .unwrap();
        let rows = encoder.encode(&batch).unwrap();
        let words = Words::new(layout);
        let mut records = Vec::new();
        for (row, key) in (0_u32..).zip(&rows) {
            words.push(&mut records, key.data(), 0, row);
        }
        let rows = [Some(rows)];
        Order::new(words, &rows).sort_pieces(&mut records);

        let sorted: Vec<&str> = records
            .chunks_exact(words.record)
            .map(|record| values[Words::source(record).1 as usize])
            .collect();
        assert_eq!(
            sorted,
            vec![
                "",
                "a",
                "a shared prefix longer than sixteen bytes: apple",
                "a shared prefix longer than sixteen bytes: zebra",
            ]
        );
    }

    #[test]
    fn descending_nulls_last_order_is_in_the_bytes() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let options = SortOptions {
            descending: true,
            nulls_first: false,
        };
        let encoder = RowEncoder::try_new(&ordering(&schema, &[("a", options)]), &schema).unwrap();
        let layout = encoder.layout_for(0);
        let values = vec![Some(1), None, Some(3), Some(-2)];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values.clone()))],
        )
        .unwrap();
        let rows = encoder.encode(&batch).unwrap();
        let words = Words::new(layout);
        let mut records = Vec::new();
        for (row, key) in (0_u32..).zip(&rows) {
            words.push(&mut records, key.data(), 0, row);
        }
        Order::new(words, &[]).sort_pieces(&mut records);
        // the only column is the key, so the records are whole rows
        let mut keys = Vec::new();
        words.write_keys(&records, &mut keys);
        let sorted = encoder
            .decode(keys.chunks_exact(words.key_bytes()))
            .unwrap();
        let sorted: Vec<Option<i32>> = sorted
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(sorted, vec![Some(3), Some(1), Some(-2), None]);
    }
}
