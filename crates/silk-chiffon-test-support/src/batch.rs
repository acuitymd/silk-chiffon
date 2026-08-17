//! Record-batch fixtures shared by workspace tests.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
        ListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
    },
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use futures::stream::{self, BoxStream};
#[derive(Default)]
pub struct TestBatchBuilder {
    columns: Vec<(String, ArrayRef, bool)>, // (name, array, nullable)
}

impl TestBatchBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn column_i32(mut self, name: &str, values: &[i32]) -> Self {
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        self.columns.push((name.to_string(), array, false));
        self
    }

    pub fn column_i32_nullable(mut self, name: &str, values: &[Option<i32>]) -> Self {
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        self.columns.push((name.to_string(), array, true));
        self
    }

    pub fn column_i64(mut self, name: &str, values: &[i64]) -> Self {
        let array: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        self.columns.push((name.to_string(), array, false));
        self
    }

    pub fn column_i64_nullable(mut self, name: &str, values: &[Option<i64>]) -> Self {
        let array: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        self.columns.push((name.to_string(), array, true));
        self
    }

    pub fn column_f32(mut self, name: &str, values: &[f32]) -> Self {
        let array: ArrayRef = Arc::new(Float32Array::from(values.to_vec()));
        self.columns.push((name.to_string(), array, false));
        self
    }

    pub fn column_f64(mut self, name: &str, values: &[f64]) -> Self {
        let array: ArrayRef = Arc::new(Float64Array::from(values.to_vec()));
        self.columns.push((name.to_string(), array, false));
        self
    }

    pub fn column_string(mut self, name: &str, values: &[&str]) -> Self {
        let array: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
        self.columns.push((name.to_string(), array, false));
        self
    }

    pub fn column_string_nullable(mut self, name: &str, values: &[Option<&str>]) -> Self {
        let array: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
        self.columns.push((name.to_string(), array, true));
        self
    }

    pub fn column_bool(mut self, name: &str, values: &[bool]) -> Self {
        let array: ArrayRef = Arc::new(BooleanArray::from(values.to_vec()));
        self.columns.push((name.to_string(), array, false));
        self
    }

    /// values are days since epoch
    pub fn column_date32(mut self, name: &str, values: &[i32]) -> Self {
        let array: ArrayRef = Arc::new(Date32Array::from(values.to_vec()));
        self.columns.push((name.to_string(), array, false));
        self
    }

    /// values are microseconds since epoch
    pub fn column_timestamp_micros(mut self, name: &str, values: &[i64]) -> Self {
        let array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(values.to_vec()));
        self.columns.push((name.to_string(), array, false));
        self
    }

    pub fn column_struct<F>(mut self, name: &str, builder_fn: F) -> Self
    where
        F: FnOnce(StructColumnBuilder) -> StructColumnBuilder,
    {
        let struct_builder = builder_fn(StructColumnBuilder::new());
        let (array, fields) = struct_builder.build();
        let struct_array = StructArray::from(
            fields
                .into_iter()
                .zip(array)
                .map(|(f, a)| (Arc::new(f), a))
                .collect::<Vec<_>>(),
        );
        self.columns
            .push((name.to_string(), Arc::new(struct_array), false));
        self
    }

    pub fn column_list_string(mut self, name: &str, values: &[Vec<&str>]) -> Self {
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let flat: Vec<&str> = values.iter().flat_map(|v| v.iter().copied()).collect();
        let values_array: ArrayRef = Arc::new(StringArray::from(flat));

        let mut offsets = vec![0i32];
        for v in values {
            offsets.push(offsets.last().unwrap() + i32::try_from(v.len()).unwrap());
        }
        let offset_buffer = OffsetBuffer::new(offsets.into());

        let list_array = ListArray::new(field, offset_buffer, values_array, None);
        self.columns
            .push((name.to_string(), Arc::new(list_array), false));
        self
    }

    pub fn column_list_i32(mut self, name: &str, values: &[Vec<i32>]) -> Self {
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));

        let flat: Vec<i32> = values.iter().flat_map(|v| v.iter().copied()).collect();
        let values_array: ArrayRef = Arc::new(Int32Array::from(flat));

        let mut offsets = vec![0i32];
        for v in values {
            offsets.push(offsets.last().unwrap() + i32::try_from(v.len()).unwrap());
        }
        let offset_buffer = OffsetBuffer::new(offsets.into());

        let list_array = ListArray::new(field, offset_buffer, values_array, None);
        self.columns
            .push((name.to_string(), Arc::new(list_array), false));
        self
    }

    pub fn build(self) -> RecordBatch {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|(name, array, nullable)| Field::new(name, array.data_type().clone(), *nullable))
            .collect();

        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<ArrayRef> = self
            .columns
            .into_iter()
            .map(|(_, array, _)| array)
            .collect();

        RecordBatch::try_new(schema, arrays).expect("failed to create RecordBatch")
    }

    pub fn build_with_schema(self) -> (RecordBatch, SchemaRef) {
        let batch = self.build();
        let schema = batch.schema();
        (batch, schema)
    }
}

pub struct StructColumnBuilder {
    fields: Vec<Field>,
    arrays: Vec<ArrayRef>,
}

impl StructColumnBuilder {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            arrays: Vec::new(),
        }
    }

    pub fn field_i32(mut self, name: &str, values: &[i32]) -> Self {
        self.fields.push(Field::new(name, DataType::Int32, false));
        self.arrays
            .push(Arc::new(Int32Array::from(values.to_vec())));
        self
    }

    pub fn field_string(mut self, name: &str, values: &[&str]) -> Self {
        self.fields.push(Field::new(name, DataType::Utf8, false));
        self.arrays
            .push(Arc::new(StringArray::from(values.to_vec())));
        self
    }

    fn build(self) -> (Vec<ArrayRef>, Vec<Field>) {
        (self.arrays, self.fields)
    }
}

impl Default for StructColumnBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TestBatch;

impl TestBatch {
    pub fn builder() -> TestBatchBuilder {
        TestBatchBuilder::new()
    }

    /// id (i32), name (string) - 3 rows
    pub fn simple() -> RecordBatch {
        TestBatchBuilder::new()
            .column_i32("id", &[1, 2, 3])
            .column_string("name", &["a", "b", "c"])
            .build()
    }

    pub fn simple_with(ids: &[i32], names: &[&str]) -> RecordBatch {
        TestBatchBuilder::new()
            .column_i32("id", ids)
            .column_string("name", names)
            .build()
    }

    /// id: i32, name: string
    pub fn simple_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    pub fn with_nullable_id(ids: &[Option<i32>], names: &[&str]) -> RecordBatch {
        TestBatchBuilder::new()
            .column_i32_nullable("id", ids)
            .column_string("name", names)
            .build()
    }

    /// id: i32 (nullable), name: string
    pub fn nullable_id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    /// id, name, created_at - 3 rows
    pub fn with_dates() -> RecordBatch {
        TestBatchBuilder::new()
            .column_i32("id", &[1, 2, 3])
            .column_string("name", &["a", "b", "c"])
            .column_date32("created_at", &[19000, 19001, 19002])
            .build()
    }

    /// id, name, ts - 3 rows
    pub fn with_timestamps() -> RecordBatch {
        TestBatchBuilder::new()
            .column_i32("id", &[1, 2, 3])
            .column_string("name", &["a", "b", "c"])
            .column_timestamp_micros(
                "ts",
                &[
                    1_640_000_000_000_000,
                    1_640_000_001_000_000,
                    1_640_000_002_000_000,
                ],
            )
            .build()
    }

    /// id, person { name, age } - 3 rows
    pub fn with_structs() -> RecordBatch {
        TestBatchBuilder::new()
            .column_i32("id", &[1, 2, 3])
            .column_struct("person", |s| {
                s.field_string("name", &["alice", "bob", "charlie"])
                    .field_i32("age", &[30, 25, 35])
            })
            .build()
    }

    /// region, year, value - 4 rows
    pub fn for_partitioning() -> RecordBatch {
        TestBatchBuilder::new()
            .column_string("region", &["us", "us", "eu", "eu"])
            .column_i32("year", &[2023, 2024, 2023, 2024])
            .column_i64("value", &[100, 200, 150, 250])
            .build()
    }

    /// group, value - 4 rows (unsorted)
    pub fn for_sorting() -> RecordBatch {
        TestBatchBuilder::new()
            .column_i32("group", &[2, 1, 2, 1])
            .column_i32("value", &[20, 10, 21, 11])
            .build()
    }

    /// group: i32, value: i32
    pub fn for_sorting_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("group", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    /// id, tags (`list<string>`) - 3 rows
    pub fn with_lists() -> RecordBatch {
        TestBatchBuilder::new()
            .column_i32("id", &[1, 2, 3])
            .column_list_string("tags", &[vec!["a", "b"], vec!["c"], vec!["d", "e", "f"]])
            .build()
    }

    pub fn into_stream(
        batch: RecordBatch,
    ) -> BoxStream<'static, Result<RecordBatch, arrow::error::ArrowError>> {
        Self::batches_into_stream(vec![batch])
    }

    pub fn batches_into_stream(
        batches: Vec<RecordBatch>,
    ) -> BoxStream<'static, Result<RecordBatch, arrow::error::ArrowError>> {
        Box::pin(stream::iter(batches.into_iter().map(Ok)))
    }
}
