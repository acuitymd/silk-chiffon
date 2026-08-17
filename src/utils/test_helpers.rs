pub mod test_data {
    use std::{fs::File, sync::Arc};

    use arrow::{
        array::{Float64Array, Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
        ipc::writer::FileWriter,
    };

    pub fn simple_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    pub fn nullable_id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    pub fn multi_column_for_sorting_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("group", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    pub fn create_batch_with_ids_and_names(
        schema: &SchemaRef,
        ids: &[i32],
        names: &[&str],
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    pub fn create_batch_with_nullable_ids_and_non_nullable_names(
        schema: &SchemaRef,
        ids: &[Option<i32>],
        names: &[&str],
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    pub fn create_multi_column_for_sorting_batch(
        schema: &Arc<Schema>,
        groups: &[i32],
        values: &[i32],
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(groups.to_vec())),
                Arc::new(Int32Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    pub fn create_arrow_file_with_range_of_ids(path: &std::path::Path, start_id: i32, count: i32) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let ids: Vec<i32> = (start_id..start_id + count).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("Person_{id}")).collect();
        let values: Vec<f64> = ids.iter().map(|id| f64::from(*id) * 1.5).collect();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap();

        let file = File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
}

pub mod file_helpers {
    use anyhow::Result;
    use arrow::{
        array::RecordBatch,
        datatypes::SchemaRef,
        ipc::writer::{FileWriter, StreamWriter},
    };
    use std::{fs::File, path::Path};

    pub fn write_arrow_file(
        path: &Path,
        schema: &SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = FileWriter::try_new(file, schema)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
        Ok(())
    }

    pub fn write_arrow_stream(
        path: &Path,
        schema: &SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = StreamWriter::try_new(file, schema)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
        Ok(())
    }

    pub fn write_invalid_file(path: &std::path::Path) -> Result<()> {
        std::fs::write(path, b"not an arrow file")?;
        Ok(())
    }
}

pub mod verify {
    use anyhow::Result;
    use arrow::{
        array::{Array, Int32Array, RecordBatch, StringArray},
        datatypes::Schema,
        ipc::reader::{FileReader, StreamReader},
    };
    use std::{collections::HashMap, fs::File, path::Path};

    use crate::utils::test_helpers::test_data;
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
}
