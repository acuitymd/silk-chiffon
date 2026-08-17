use anyhow::Result;
use arrow::{
    array::{Array, Int32Array, RecordBatch, StringArray},
    datatypes::Schema,
    ipc::reader::{FileReader, StreamReader},
};
use std::{collections::HashMap, fs::File, path::Path};

use crate::batch as test_data;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub fn read_output_file(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader = FileReader::try_new_buffered(file, None)?;
    reader.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

pub fn read_parquet_file(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

pub fn read_output_stream(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let reader = StreamReader::try_new(file, None)?;
    reader.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

pub fn read_file_metadata(path: &Path) -> Result<HashMap<String, String>> {
    let file = File::open(path)?;
    let reader = FileReader::try_new_buffered(file, None)?;
    let metadata = reader.custom_metadata().clone();
    Ok(metadata)
}

pub fn assert_schema_matches(actual: &Schema, expected: &Schema) {
    assert_eq!(actual.fields().len(), expected.fields().len());
    for (i, field) in expected.fields().iter().enumerate() {
        let actual_field = actual.field(i);

        assert_eq!(actual_field.name(), field.name());
        assert_eq!(actual_field.data_type(), field.data_type());
        assert_eq!(actual_field.is_nullable(), field.is_nullable());
    }
}

pub fn assert_id_name_batch_data_matches(
    batch: &RecordBatch,
    expected_ids: &[i32],
    expected_names: &[&str],
) {
    assert_schema_matches(&batch.schema(), &test_data::simple_schema());

    let id_column = batch.column_by_name("id").unwrap();
    let name_column = batch.column_by_name("name").unwrap();

    let ids = id_column.as_any().downcast_ref::<Int32Array>().unwrap();
    let names = name_column.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(ids.len(), expected_ids.len());
    assert_eq!(names.len(), expected_names.len());

    for (i, expected_id) in expected_ids.iter().enumerate() {
        assert_eq!(ids.value(i), *expected_id);
    }
    for (i, expected_name) in expected_names.iter().enumerate() {
        assert_eq!(names.value(i), *expected_name);
    }
}

pub fn extract_column_as_i32_vec(batch: &RecordBatch, column_name: &str) -> Vec<i32> {
    let column = batch.column_by_name(column_name).unwrap();
    let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
    (0..array.len()).map(|i| array.value(i)).collect()
}
