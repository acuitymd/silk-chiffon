use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::compute::BatchCoalescer;
use arrow::{
    array::RecordBatch,
    error::ArrowError,
    ipc::{
        CompressionType,
        writer::{FileWriter, IpcWriteOptions},
    },
};
use datafusion::{
    execution::{options::ArrowReadOptions, runtime_env::RuntimeEnvBuilder},
    prelude::{SessionConfig, SessionContext, col},
};
use futures::StreamExt;

use crate::{
    ArrowCompression, SortDirection, SortSpec,
    utils::arrow_io::{ArrowFileSource, ArrowIPCFormat, ArrowIPCReader, HasSchema},
};
use anyhow::Result;

pub struct ArrowConverter {
    input_path: String,
    output_path: PathBuf,
    write_options: IpcWriteOptions,
    sort_spec: SortSpec,
    record_batch_size: usize,
    datafusion_batch_size: usize,
}

impl ArrowConverter {
    pub fn new(input_path: &str, output_path: &Path) -> Self {
        let output_path = output_path.to_path_buf();
        let write_options = IpcWriteOptions::default();
        Self {
            input_path: input_path.to_string(),
            output_path,
            write_options,
            sort_spec: SortSpec::default(),
            record_batch_size: 122_880,
            datafusion_batch_size: 8192,
        }
    }

    pub fn with_compression(mut self, compression: ArrowCompression) -> Self {
        let compression_type = match compression {
            ArrowCompression::Zstd => Some(CompressionType::ZSTD),
            ArrowCompression::Lz4 => Some(CompressionType::LZ4_FRAME),
            ArrowCompression::None => None,
        };
        self.write_options = self
            .write_options
            .try_with_compression(compression_type)
            .unwrap_or_else(|_| {
                panic!(
                    "Failed to set compression to {:?} (compression: {:?}) -- feature not enabled",
                    compression_type, compression
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

    pub async fn convert(&self) -> Result<()> {
        if self.sort_spec.columns.is_empty() {
            self.convert_direct().await
        } else {
            self.convert_with_sorting().await
        }
    }

    async fn convert_with_sorting(&self) -> Result<()> {
        let mut config = SessionConfig::new();
        let options = config.options_mut();
        options.execution.batch_size = self.datafusion_batch_size;

        let runtime_config = RuntimeEnvBuilder::new();
        let runtime_env = runtime_config.build()?;
        let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime_env));

        let output_path = self.output_path.clone();
        let write_options = self.write_options.clone();
        let sort_spec = self.sort_spec.clone();
        let record_batch_size = self.record_batch_size;

        let file_format = self.as_file_format().await?;

        ctx.register_arrow(
            "input_table",
            file_format.path_str(),
            ArrowReadOptions::default(),
        )
        .await?;
        let df = ctx.table("input_table").await?;

        let sort_exprs = sort_spec
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

        let mut stream = df.execute_stream().await?;
        let schema = stream.schema();
        let output_file = File::create(output_path)?;
        let mut writer = FileWriter::try_new_with_options(output_file, &schema, write_options)?;

        let mut coalescer = BatchCoalescer::new(schema.clone(), record_batch_size);

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;

            coalescer.push_batch(batch)?;

            while let Some(completed_batch) = coalescer.next_completed_batch() {
                writer.write(&completed_batch)?;
            }
        }

        if let Ok(()) = coalescer.finish_buffered_batch() {
            if let Some(final_batch) = coalescer.next_completed_batch() {
                writer.write(&final_batch)?;
            }
        }

        writer.finish()?;

        Ok(())
    }

    async fn convert_direct(&self) -> Result<()> {
        let input = ArrowIPCReader::from_path(&self.input_path)?;
        match input.format() {
            ArrowIPCFormat::File => {
                let file_reader = input.file_reader()?;
                ArrowConverter::convert_arrow_reader_to_file_format(
                    file_reader,
                    &self.output_path,
                    self.write_options.clone(),
                    self.record_batch_size,
                )
                .await
            }
            ArrowIPCFormat::Stream => {
                let stream_reader = input.stream_reader()?;
                ArrowConverter::convert_arrow_reader_to_file_format(
                    stream_reader,
                    &self.output_path,
                    self.write_options.clone(),
                    self.record_batch_size,
                )
                .await
            }
        }
    }

    async fn convert_arrow_reader_to_file_format<I>(
        reader: I,
        output_path: &Path,
        write_options: IpcWriteOptions,
        record_batch_size: usize,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch, ArrowError>> + HasSchema,
    {
        let schema = reader.schema();
        let output_file = File::create(output_path)?;
        let mut writer = FileWriter::try_new_with_options(output_file, &schema, write_options)?;

        let mut coalescer = BatchCoalescer::new(schema.clone(), record_batch_size);

        for batch_result in reader {
            let batch = batch_result?;

            coalescer.push_batch(batch)?;

            while let Some(completed_batch) = coalescer.next_completed_batch() {
                writer.write(&completed_batch)?;
            }
        }

        if let Ok(()) = coalescer.finish_buffered_batch() {
            if let Some(final_batch) = coalescer.next_completed_batch() {
                writer.write(&final_batch)?;
            }
        }

        writer.finish()?;

        Ok(())
    }

    pub async fn as_file_format(&self) -> Result<ArrowFileSource> {
        let input = ArrowIPCReader::from_path(&self.input_path)?;
        match input.format() {
            ArrowIPCFormat::File => Ok(ArrowFileSource::Original(input.path().to_path_buf())),
            ArrowIPCFormat::Stream => {
                let temp_file = tempfile::Builder::new().suffix(".arrow").tempfile()?;
                ArrowConverter::convert_arrow_reader_to_file_format(
                    input.stream_reader()?,
                    temp_file.path(),
                    IpcWriteOptions::default(),
                    self.record_batch_size,
                )
                .await?;

                Ok(ArrowFileSource::Temp {
                    original_path: input.path().to_path_buf(),
                    temp_file,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ArrowCompression, SortDirection, SortSpec,
        converters::arrow::ArrowConverter,
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path);
            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path);
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path)
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path)
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path)
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path);
            converter.convert().await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 0);
        }

        #[tokio::test]
        async fn test_converter_invalid_input_path() {
            let temp_dir = tempdir().unwrap();
            let output_path = temp_dir.path().join("output.arrow");

            let converter = ArrowConverter::new("/nonexistent/file.arrow", &output_path);
            let result = converter.convert().await;

            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_converter_corrupted_file() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("corrupted.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            file_helpers::write_invalid_file(&input_path).unwrap();

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path);
            let result = converter.convert().await;

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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path)
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path)
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path)
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path)
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path)
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path)
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

            let converter = ArrowConverter::new(input_path.to_str().unwrap(), &output_path)
                .with_sorting(sort_spec);
            let result = converter.convert().await;

            assert!(result.is_err());
        }
    }
}
