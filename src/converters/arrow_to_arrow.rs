use std::{
    fs::File,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use arrow::{array::RecordBatch, compute::BatchCoalescer};
use arrow::{
    datatypes::SchemaRef,
    ipc::{
        CompressionType,
        writer::{FileWriter, IpcWriteOptions, StreamWriter},
    },
};
use async_trait::async_trait;
use datafusion::{
    execution::{RecordBatchStream, options::ArrowReadOptions, runtime_env::RuntimeEnvBuilder},
    prelude::{SessionConfig, SessionContext, col},
};

use crate::{
    ArrowCompression, QueryDialect, SortDirection, SortSpec,
    utils::{
        arrow_io::{
            ArrowFileSource, ArrowIPCFormat, ArrowIPCReader, ArrowRecordBatchWriter,
            RecordBatchIterator,
        },
        query_executor::{QueryExecutor, build_query_with_sort},
    },
};
use anyhow::Result;

pub struct ArrowToArrowConverter {
    input: Box<dyn RecordBatchIterator>,
    output_path: PathBuf,
    write_options: IpcWriteOptions,
    sort_spec: SortSpec,
    record_batch_size: usize,
    datafusion_batch_size: usize,
    output_ipc_format: ArrowIPCFormat,
    query: Option<String>,
    dialect: QueryDialect,
}

#[async_trait]
trait UniversalRecordBatchIterator {
    async fn next(&mut self) -> Result<Option<RecordBatch>>;
}

#[async_trait]
impl UniversalRecordBatchIterator for Box<dyn RecordBatchIterator> {
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.next_batch()
    }
}

#[async_trait]
impl UniversalRecordBatchIterator for Pin<Box<dyn RecordBatchStream + Send>> {
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        futures::StreamExt::next(&mut *self)
            .await
            .transpose()
            .map_err(Into::into)
    }
}

impl ArrowToArrowConverter {
    pub fn new(input_path: &str, output_path: &Path) -> Result<Self> {
        let reader = ArrowIPCReader::from_path(input_path)?;
        let input = reader.into_batch_iterator()?;

        Ok(Self::from_iterator(input, output_path))
    }

    pub fn from_iterator(input: Box<dyn RecordBatchIterator>, output_path: &Path) -> Self {
        let output_path = output_path.to_path_buf();
        let write_options = IpcWriteOptions::default();
        Self {
            input,
            output_path,
            write_options,
            sort_spec: SortSpec::default(),
            record_batch_size: 122_880,
            datafusion_batch_size: 8192,
            output_ipc_format: ArrowIPCFormat::default(),
            query: None,
            dialect: QueryDialect::default(),
        }
    }

    pub fn with_compression(mut self, compression: ArrowCompression) -> Self {
        let compression_type =
            <ArrowCompression as Into<Option<CompressionType>>>::into(compression);
        self.write_options = self
            .write_options
            .try_with_compression(compression_type)
            .unwrap_or_else(|_| {
                panic!(
                    "Failed to set compression to {compression_type:?} (compression: {compression:?}) -- feature not enabled"
                );
            });
        self
    }

    pub fn with_sorting(mut self, sort_spec: SortSpec) -> Self {
        self.sort_spec = sort_spec;
        self
    }

    pub fn with_record_batch_size(mut self, record_batch_size: usize) -> Self {
        self.record_batch_size = record_batch_size;
        self
    }

    pub fn with_output_ipc_format(mut self, output_ipc_format: ArrowIPCFormat) -> Self {
        self.output_ipc_format = output_ipc_format;
        self
    }

    pub fn with_query(mut self, query: Option<String>) -> Self {
        self.query = query;
        self
    }

    pub fn with_dialect(mut self, dialect: QueryDialect) -> Self {
        self.dialect = dialect;
        self
    }

    pub async fn convert(mut self) -> Result<()> {
        if self.query.is_some() || !self.sort_spec.columns.is_empty() {
            self.convert_with_query_or_sorting().await
        } else {
            self.convert_direct().await
        }
    }

    fn new_writer(
        &self,
        output_file: File,
        schema: &SchemaRef,
    ) -> Result<Box<dyn ArrowRecordBatchWriter>> {
        let writer: Box<dyn ArrowRecordBatchWriter> = match self.output_ipc_format {
            ArrowIPCFormat::File => Box::new(FileWriter::try_new_with_options(
                output_file,
                schema,
                self.write_options.clone(),
            )?),
            ArrowIPCFormat::Stream => Box::new(StreamWriter::try_new_with_options(
                output_file,
                schema,
                self.write_options.clone(),
            )?),
        };

        Ok(writer)
    }

    async fn write_to_output<T: UniversalRecordBatchIterator>(
        input: &mut T,
        schema: &SchemaRef,
        writer: &mut dyn ArrowRecordBatchWriter,
        record_batch_size: usize,
    ) -> Result<()> {
        let mut coalescer = BatchCoalescer::new(Arc::clone(schema), record_batch_size);

        while let Some(batch) = input.next().await? {
            coalescer.push_batch(batch)?;

            while let Some(completed_batch) = coalescer.next_completed_batch() {
                writer.write(&completed_batch)?;
            }
        }

        coalescer.finish_buffered_batch()?;

        if let Some(final_batch) = coalescer.next_completed_batch() {
            writer.write(&final_batch)?;
        }

        writer.finish()?;

        Ok(())
    }

    async fn convert_with_query_or_sorting(&mut self) -> Result<()> {
        let mut config = SessionConfig::new();
        let options = config.options_mut();
        options.execution.batch_size = self.datafusion_batch_size;

        let runtime_config = RuntimeEnvBuilder::new();
        let runtime_env = runtime_config.build()?;
        let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

        let file_format = self.as_file_format().await?;

        let mut stream = if let Some(query) = &self.query {
            let final_query = if !self.sort_spec.columns.is_empty() {
                let sort_columns: Vec<(String, bool)> = self
                    .sort_spec
                    .columns
                    .iter()
                    .map(|col| {
                        (
                            col.name.clone(),
                            matches!(col.direction, SortDirection::Ascending),
                        )
                    })
                    .collect();
                build_query_with_sort(query, &sort_columns)
            } else {
                query.clone()
            };

            let executor = QueryExecutor::new(final_query, self.dialect);
            executor
                .execute_on_file(file_format.path_str(), "data")
                .await?
        } else {
            ctx.register_arrow(
                "input_table",
                file_format.path_str(),
                ArrowReadOptions::default(),
            )
            .await?;
            let df = ctx.table("input_table").await?;

            let sort_exprs = self
                .sort_spec
                .columns
                .iter()
                .map(|sort_column| {
                    col(&sort_column.name).sort(
                        matches!(sort_column.direction, SortDirection::Ascending),
                        // match the behavior of postgres here, where nulls are first for descending
                        // and last for ascending
                        // https://www.postgresql.org/docs/current/queries-order.html#:~:text=The%20NULLS%20FIRST%20and%20NULLS%20LAST%20options%20can%20be%20used%20to%20determine%20whether%20nulls%20appear%20before%20or%20after%20non%2Dnull%20values%20in%20the%20sort%20ordering.%20By%20default%2C%20null%20values%20sort%20as%20if%20larger%20than%20any%20non%2Dnull%20value%3B%20that%20is%2C%20NULLS%20FIRST%20is%20the%20default%20for%20DESC%20order%2C%20and%20NULLS%20LAST%20otherwise
                        matches!(sort_column.direction, SortDirection::Descending),
                    )
                })
                .collect();

            let df = df.sort(sort_exprs)?;
            df.execute_stream().await?
        };
        let schema = stream.schema();
        let output_file = File::create(&self.output_path)?;

        let mut writer = self.new_writer(output_file, &schema)?;

        Self::write_to_output(
            &mut stream,
            &schema,
            writer.as_mut(),
            self.record_batch_size,
        )
        .await?;

        Ok(())
    }

    async fn convert_direct(&mut self) -> Result<()> {
        let output_file = File::create(&self.output_path)?;
        let schema = self.input.schema();
        let mut writer = self.new_writer(output_file, &schema)?;

        Self::write_to_output(
            &mut self.input,
            &schema,
            writer.as_mut(),
            self.record_batch_size,
        )
        .await?;

        Ok(())
    }

    pub async fn as_file_format(&mut self) -> Result<ArrowFileSource> {
        self.input.as_file_format()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ArrowCompression, SortDirection, SortSpec,
        converters::arrow_to_arrow::ArrowToArrowConverter,
        utils::test_helpers::{file_helpers, test_data, verify},
    };
    use arrow::array::{Array, Int32Array};
    use tempfile::tempdir;

    mod arrow_converter_tests {
        use super::*;

        #[tokio::test]
        async fn test_converter_basic_file_to_file() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_ids = vec![1, 2, 3, 4, 5];
            let test_names = vec!["Alice", "Bob", "Charlie", "David", "Eve"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let converter =
                ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path).unwrap();
            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            verify::assert_id_name_batch_data_matches(&batches[0], &test_ids, &test_names);
        }

        #[tokio::test]
        async fn test_converter_file_to_stream_format() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrows");

            let test_ids = vec![1, 2, 3, 4, 5];
            let test_names = vec!["Alice", "Bob", "Charlie", "David", "Eve"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_output_ipc_format(crate::utils::arrow_io::ArrowIPCFormat::Stream);
            converter.convert().await.unwrap();

            let batches = verify::read_output_stream(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            verify::assert_id_name_batch_data_matches(&batches[0], &test_ids, &test_names);
        }

        #[tokio::test]
        async fn test_converter_stream_to_stream_format() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrows");
            let output_path = temp_dir.path().join("output.arrows");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_stream(&input_path, &schema, vec![batch]).unwrap();

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_output_ipc_format(crate::utils::arrow_io::ArrowIPCFormat::Stream);
            converter.convert().await.unwrap();

            let batches = verify::read_output_stream(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            verify::assert_id_name_batch_data_matches(&batches[0], &test_ids, &test_names);
        }

        #[tokio::test]
        async fn test_converter_basic_stream_to_file() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_stream(&input_path, &schema, vec![batch]).unwrap();

            let converter =
                ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path).unwrap();
            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            verify::assert_id_name_batch_data_matches(&batches[0], &test_ids, &test_names);
        }

        #[tokio::test]
        async fn test_converter_with_zstd_compression() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_compression(ArrowCompression::Zstd);
            converter.convert().await.unwrap();

            assert!(output_path.exists());
            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
        }

        #[tokio::test]
        async fn test_converter_with_lz4_compression() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_compression(ArrowCompression::Lz4);
            converter.convert().await.unwrap();

            assert!(output_path.exists());
            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
        }

        #[tokio::test]
        async fn test_converter_multiple_batches() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_first_batch_ids = vec![1, 2, 3];
            let test_first_batch_names = vec!["A", "B", "C"];
            let test_second_batch_ids = vec![4, 5];
            let test_second_batch_names = vec!["D", "E"];

            let schema = test_data::simple_schema();
            let batch1 = test_data::create_batch_with_ids_and_names(
                &schema,
                &test_first_batch_ids,
                &test_first_batch_names,
            );
            let batch2 = test_data::create_batch_with_ids_and_names(
                &schema,
                &test_second_batch_ids,
                &test_second_batch_names,
            );
            file_helpers::write_arrow_stream(&input_path, &schema, vec![batch1, batch2]).unwrap();

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_record_batch_size(3);
            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 2);
            assert_eq!(batches[0].num_rows(), 3);
            assert_eq!(batches[1].num_rows(), 2);
        }

        #[tokio::test]
        async fn test_converter_empty_file() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            file_helpers::write_arrow_stream(&input_path, &schema, vec![]).unwrap();

            let converter =
                ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path).unwrap();
            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 0);
        }

        #[tokio::test]
        async fn test_converter_invalid_input_path() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let result = ArrowToArrowConverter::new("/nonexistent/file.arrow", &output_path);
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_converter_corrupted_file() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("corrupted.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            file_helpers::write_invalid_file(&input_path).unwrap();

            let result = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path);
            assert!(result.is_err());
        }
    }

    mod sorting_tests {
        use super::*;

        #[tokio::test]
        async fn test_single_column_sort_ascending() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_ids = vec![5, 2, 4, 1, 3];
            let test_names = vec!["Eve", "Bob", "David", "Alice", "Charlie"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let sort_spec = SortSpec {
                columns: vec![crate::SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Ascending,
                }],
            };

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_sorting(sort_spec);
            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);

            let ids = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(ids.value(0), 1);
            assert_eq!(ids.value(1), 2);
            assert_eq!(ids.value(2), 3);
            assert_eq!(ids.value(3), 4);
            assert_eq!(ids.value(4), 5);
        }

        #[tokio::test]
        async fn test_single_column_sort_descending() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_ids = vec![30, 10, 20];
            let test_names = vec!["C", "A", "B"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_stream(&input_path, &schema, vec![batch]).unwrap();

            let sort_spec = SortSpec {
                columns: vec![crate::SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Descending,
                }],
            };

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_sorting(sort_spec);
            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            let ids = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(ids.value(0), 30);
            assert_eq!(ids.value(1), 20);
            assert_eq!(ids.value(2), 10);
        }

        #[tokio::test]
        async fn test_multi_column_sort() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_groups = vec![1, 2, 1, 2, 1];
            let test_values = vec![30, 20, 10, 40, 20];

            let schema = test_data::multi_column_for_sorting_schema();
            let batch = test_data::create_multi_column_for_sorting_batch(
                &schema,
                &test_groups,
                &test_values,
            );
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let sort_spec = SortSpec {
                columns: vec![
                    crate::SortColumn {
                        name: "group".to_string(),
                        direction: SortDirection::Ascending,
                    },
                    crate::SortColumn {
                        name: "value".to_string(),
                        direction: SortDirection::Descending,
                    },
                ],
            };

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_sorting(sort_spec);
            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            let batch = &batches[0];
            let groups = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            assert_eq!(groups.value(0), 1);
            assert_eq!(values.value(0), 30);
            assert_eq!(groups.value(1), 1);
            assert_eq!(values.value(1), 20);
            assert_eq!(groups.value(2), 1);
            assert_eq!(values.value(2), 10);

            assert_eq!(groups.value(3), 2);
            assert_eq!(values.value(3), 40);
            assert_eq!(groups.value(4), 2);
            assert_eq!(values.value(4), 20);
        }

        #[tokio::test]
        async fn test_sort_with_nulls_ascending() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_ids = vec![Some(3), None, Some(1), None, Some(2)];
            let test_names = vec!["C", "null1", "A", "null2", "B"];

            let schema = test_data::nullable_id_schema();
            let batch = test_data::create_batch_with_nullable_ids_and_non_nullable_names(
                &schema,
                &test_ids,
                &test_names,
            );
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let sort_spec = SortSpec {
                columns: vec![crate::SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Ascending,
                }],
            };

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_sorting(sort_spec);
            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            let ids = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            assert_eq!(ids.value(0), 1);
            assert_eq!(ids.value(1), 2);
            assert_eq!(ids.value(2), 3);
            assert!(ids.is_null(3));
            assert!(ids.is_null(4));
        }

        #[tokio::test]
        async fn test_sort_with_nulls_descending() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_ids = vec![Some(3), None, Some(1), None, Some(2)];
            let test_names = vec!["C", "null1", "A", "null2", "B"];

            let schema = test_data::nullable_id_schema();
            let batch = test_data::create_batch_with_nullable_ids_and_non_nullable_names(
                &schema,
                &test_ids,
                &test_names,
            );
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let sort_spec = SortSpec {
                columns: vec![crate::SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Descending,
                }],
            };

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_sorting(sort_spec);
            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            let ids = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            assert!(ids.is_null(0));
            assert!(ids.is_null(1));
            assert_eq!(ids.value(2), 3);
            assert_eq!(ids.value(3), 2);
            assert_eq!(ids.value(4), 1);
        }

        #[tokio::test]
        async fn test_sort_with_compression() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_ids = vec![3, 1, 2];
            let test_names = vec!["C", "A", "B"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_stream(&input_path, &schema, vec![batch]).unwrap();

            let sort_spec = SortSpec {
                columns: vec![crate::SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Ascending,
                }],
            };

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_sorting(sort_spec)
                .with_compression(ArrowCompression::Zstd);
            converter.convert().await.unwrap();

            assert!(output_path.exists());
            let batches = verify::read_output_file(&output_path).unwrap();
            let ids = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(ids.value(0), 1);
            assert_eq!(ids.value(1), 2);
            assert_eq!(ids.value(2), 3);
        }

        #[tokio::test]
        async fn test_sort_invalid_column() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let sort_spec = SortSpec {
                columns: vec![crate::SortColumn {
                    name: "nonexistent_column".to_string(),
                    direction: SortDirection::Ascending,
                }],
            };

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_sorting(sort_spec);
            let result = converter.convert().await;

            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_sort_with_stream_output_format() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrows");

            let test_ids = vec![3, 1, 2];
            let test_names = vec!["C", "A", "B"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let sort_spec = SortSpec {
                columns: vec![crate::SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Ascending,
                }],
            };

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_sorting(sort_spec)
                .with_output_ipc_format(crate::utils::arrow_io::ArrowIPCFormat::Stream);
            converter.convert().await.unwrap();

            let batches = verify::read_output_stream(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            let ids = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(ids.value(0), 1);
            assert_eq!(ids.value(1), 2);
            assert_eq!(ids.value(2), 3);
        }

        #[tokio::test]
        async fn test_converter_with_query() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(
                &schema,
                &[1, 2, 3, 4, 5],
                &["Alice", "Bob", "Charlie", "David", "Eve"],
            );
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_query(Some("SELECT * FROM data WHERE id > 3".to_string()));

            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);

            let batch = &batches[0];
            assert_eq!(batch.num_rows(), 2);

            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            assert_eq!(ids.value(0), 4);
            assert_eq!(ids.value(1), 5);
        }

        #[tokio::test]
        async fn test_converter_with_query_and_sort() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(
                &schema,
                &[5, 3, 1, 4, 2],
                &["Eve", "Charlie", "Alice", "David", "Bob"],
            );
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let converter = ArrowToArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .unwrap()
                .with_query(Some("SELECT id, name FROM data WHERE id <= 3".to_string()))
                .with_sorting(crate::SortSpec {
                    columns: vec![crate::SortColumn {
                        name: "id".to_string(),
                        direction: crate::SortDirection::Ascending,
                    }],
                });

            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);

            let batch = &batches[0];
            assert_eq!(batch.num_rows(), 3);

            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            assert_eq!(ids.value(0), 1);
            assert_eq!(ids.value(1), 2);
            assert_eq!(ids.value(2), 3);
        }
    }
}
