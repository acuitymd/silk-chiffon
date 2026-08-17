//! Test data factory for creating Arrow `RecordBatch`es in tests.
//!
//! # Usage
//!
//! ```rust
//! use silk_chiffon::utils::test_data::TestBatch;
//!
//! let batch = TestBatch::builder()
//!     .column_i32("id", &[1, 2, 3])
//!     .column_string("name", &["a", "b", "c"])
//!     .build();
//!
//! // or using a preset batch
//! let batch = TestBatch::simple();  // id: i32, name: string (3 rows)
//! ```

use std::sync::Arc;

use arrow::ipc::{
    reader::{FileReader as ArrowFileReader, StreamReader as ArrowStreamReader},
    writer::{FileWriter as ArrowFileWriter, StreamWriter as ArrowStreamWriter},
};
use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array,
        Int64Array, ListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
    },
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use futures::stream::{self, BoxStream};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
use std::{fs::File, path::Path};

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

    /// id, tags (list<string>) - 3 rows
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

pub struct TestFile;

impl TestFile {
    pub fn write_arrow(path: &Path, batches: &[RecordBatch]) {
        assert!(!batches.is_empty(), "need at least one batch");
        let schema = batches[0].schema();
        let file = File::create(path).expect("failed to create file");
        let mut writer = ArrowFileWriter::try_new(file, &schema).expect("failed to create writer");
        for batch in batches {
            writer.write(batch).expect("failed to write batch");
        }
        writer.finish().expect("failed to finish writing");
    }

    pub fn write_arrow_batch(path: &Path, batch: &RecordBatch) {
        Self::write_arrow(path, std::slice::from_ref(batch));
    }

    /// schema only, no data
    pub fn write_arrow_empty(path: &Path, schema: &SchemaRef) {
        let file = File::create(path).expect("failed to create file");
        let mut writer = ArrowFileWriter::try_new(file, schema).expect("failed to create writer");
        writer.finish().expect("failed to finish writing");
    }

    pub fn write_arrow_stream(path: &Path, batches: &[RecordBatch]) {
        assert!(!batches.is_empty(), "need at least one batch");
        let schema = batches[0].schema();
        let file = File::create(path).expect("failed to create file");
        let mut writer =
            ArrowStreamWriter::try_new(file, &schema).expect("failed to create writer");
        for batch in batches {
            writer.write(batch).expect("failed to write batch");
        }
        writer.finish().expect("failed to finish writing");
    }

    pub fn write_parquet(path: &Path, batches: &[RecordBatch]) {
        assert!(!batches.is_empty(), "need at least one batch");
        let schema = batches[0].schema();
        let file = File::create(path).expect("failed to create file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("failed to create writer");
        for batch in batches {
            writer.write(batch).expect("failed to write batch");
        }
        writer.close().expect("failed to close writer");
    }

    pub fn write_parquet_batch(path: &Path, batch: &RecordBatch) {
        Self::write_parquet(path, std::slice::from_ref(batch));
    }

    pub fn read_arrow(path: &Path) -> Vec<RecordBatch> {
        let file = File::open(path).expect("failed to open file");
        let reader = ArrowFileReader::try_new(file, None).expect("failed to create reader");
        reader
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read batches")
    }

    pub fn read_arrow_stream(path: &Path) -> Vec<RecordBatch> {
        let file = File::open(path).expect("failed to open file");
        let reader = ArrowStreamReader::try_new(file, None).expect("failed to create reader");
        reader
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read batches")
    }

    pub fn read_parquet(path: &Path) -> Vec<RecordBatch> {
        let file = File::open(path).expect("failed to open file");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("failed to create reader builder")
            .build()
            .expect("failed to build reader");
        reader
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read batches")
    }

    /// tries Arrow file format first, falls back to stream
    pub fn read_arrow_auto(path: &Path) -> Vec<RecordBatch> {
        let file = File::open(path).expect("failed to open file");
        if let Ok(reader) = ArrowFileReader::try_new(file.try_clone().unwrap(), None) {
            reader
                .collect::<Result<Vec<_>, _>>()
                .expect("failed to read batches")
        } else {
            let reader = ArrowStreamReader::try_new(file, None).expect("failed to create reader");
            reader
                .collect::<Result<Vec<_>, _>>()
                .expect("failed to read batches")
        }
    }
}

pub struct TestExtract;

impl TestExtract {
    /// panics on null
    pub fn i32(batch: &RecordBatch, column: &str) -> Vec<i32> {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column '{column}' not found"));
        let arr = col
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("column '{column}' is not Int32"));
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    pub fn i32_nullable(batch: &RecordBatch, column: &str) -> Vec<Option<i32>> {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column '{column}' not found"));
        let arr = col
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("column '{column}' is not Int32"));
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            })
            .collect()
    }

    /// panics on null
    pub fn i64(batch: &RecordBatch, column: &str) -> Vec<i64> {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column '{column}' not found"));
        let arr = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column '{column}' is not Int64"));
        (0..arr.len()).map(|i| arr.value(i)).collect()
    }

    /// panics on null
    pub fn string(batch: &RecordBatch, column: &str) -> Vec<String> {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column '{column}' not found"));
        let arr = col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column '{column}' is not String"));
        (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
    }

    pub fn string_nullable(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column '{column}' not found"));
        let arr = col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column '{column}' is not String"));
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i).to_string())
                }
            })
            .collect()
    }

    pub fn i32_all(batches: &[RecordBatch], column: &str) -> Vec<i32> {
        batches.iter().flat_map(|b| Self::i32(b, column)).collect()
    }

    pub fn string_all(batches: &[RecordBatch], column: &str) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| Self::string(b, column))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;

    #[test]
    fn test_simple_preset() {
        let batch = TestBatch::simple();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema(), TestBatch::simple_schema());
    }

    #[test]
    fn test_simple_with_custom_data() {
        let batch = TestBatch::simple_with(&[10, 20], &["x", "y"]);
        assert_eq!(batch.num_rows(), 2);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 10);
        assert_eq!(ids.value(1), 20);
    }

    #[test]
    fn test_builder_with_multiple_types() {
        let batch = TestBatch::builder()
            .column_i32("a", &[1, 2])
            .column_i64("b", &[100, 200])
            .column_f64("c", &[1.5, 2.5])
            .column_string("d", &["x", "y"])
            .column_bool("e", &[true, false])
            .build();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn test_nullable_columns() {
        let batch = TestBatch::with_nullable_id(&[Some(1), None, Some(3)], &["a", "b", "c"]);
        assert_eq!(batch.num_rows(), 3);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(!ids.is_null(0));
        assert!(ids.is_null(1));
        assert!(!ids.is_null(2));
    }

    #[test]
    fn test_with_dates() {
        let batch = TestBatch::with_dates();
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Date32);
    }

    #[test]
    fn test_with_timestamps() {
        let batch = TestBatch::with_timestamps();
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(
            batch.schema().field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_with_structs() {
        let batch = TestBatch::with_structs();
        assert_eq!(batch.num_columns(), 2);

        let schema = batch.schema();
        let person_field = schema.field(1);
        assert_eq!(person_field.name(), "person");

        if let DataType::Struct(fields) = person_field.data_type() {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "name");
            assert_eq!(fields[1].name(), "age");
        } else {
            panic!("expected struct type");
        }
    }

    #[test]
    fn test_with_lists() {
        let batch = TestBatch::with_lists();
        assert_eq!(batch.num_columns(), 2);

        let schema = batch.schema();
        let tags_field = schema.field(1);
        assert!(matches!(tags_field.data_type(), DataType::List(_)));
    }

    #[test]
    fn test_for_partitioning() {
        let batch = TestBatch::for_partitioning();
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_for_sorting() {
        let batch = TestBatch::for_sorting();
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.schema(), TestBatch::for_sorting_schema());
    }

    #[test]
    fn test_extract_i32() {
        let batch = TestBatch::simple_with(&[10, 20, 30], &["a", "b", "c"]);
        let ids = TestExtract::i32(&batch, "id");
        assert_eq!(ids, vec![10, 20, 30]);
    }

    #[test]
    fn test_extract_string() {
        let batch = TestBatch::simple_with(&[1, 2], &["hello", "world"]);
        let names = TestExtract::string(&batch, "name");
        assert_eq!(names, vec!["hello", "world"]);
    }

    #[test]
    fn test_extract_nullable() {
        let batch = TestBatch::with_nullable_id(&[Some(1), None, Some(3)], &["a", "b", "c"]);
        let ids = TestExtract::i32_nullable(&batch, "id");
        assert_eq!(ids, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_file_round_trip_arrow() {
        let batch = TestBatch::simple();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.arrow");

        TestFile::write_arrow_batch(&path, &batch);
        let read_batches = TestFile::read_arrow(&path);

        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 3);
        assert_eq!(TestExtract::i32(&read_batches[0], "id"), vec![1, 2, 3]);
    }

    #[test]
    fn test_file_round_trip_parquet() {
        let batch = TestBatch::simple();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        TestFile::write_parquet_batch(&path, &batch);
        let read_batches = TestFile::read_parquet(&path);

        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 3);
        assert_eq!(TestExtract::i32(&read_batches[0], "id"), vec![1, 2, 3]);
    }

    #[test]
    fn test_extract_all_batches() {
        let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
        let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);
        let batches = vec![batch1, batch2];

        let ids = TestExtract::i32_all(&batches, "id");
        assert_eq!(ids, vec![1, 2, 3, 4]);

        let names = TestExtract::string_all(&batches, "name");
        assert_eq!(names, vec!["a", "b", "c", "d"]);
    }
}
