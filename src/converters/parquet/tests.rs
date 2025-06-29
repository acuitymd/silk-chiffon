#[cfg(test)]
mod parquet_converter_tests {
    use crate::{
        ParquetCompression, ParquetStatistics, ParquetWriterVersion, SortColumn, SortDirection,
        SortSpec,
        utils::test_helpers::{file_helpers, test_data, verify},
    };
    use arrow::array::{Array, Int32Array};
    use parquet::file::reader::FileReader;
    use tempfile::tempdir;

    use crate::converters::parquet::ParquetConverter;

    #[tokio::test]
    async fn test_converter_basic_file_to_parquet() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3, 4, 5];
        let test_names = vec!["Alice", "Bob", "Charlie", "David", "Eve"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        );
        converter.convert().await.unwrap();

        assert!(output_path.exists());
        let file = std::fs::File::open(&output_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        verify::assert_id_name_batch_data_matches(&batches[0], &test_ids, &test_names);
    }

    #[tokio::test]
    async fn test_converter_basic_stream_to_parquet() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_stream(&input_path, &schema, vec![batch]).unwrap();

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        );
        converter.convert().await.unwrap();

        assert!(output_path.exists());
        let file = std::fs::File::open(&output_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        verify::assert_id_name_batch_data_matches(&batches[0], &test_ids, &test_names);
    }

    #[tokio::test]
    async fn test_converter_with_compression() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        for compression in [
            ParquetCompression::Zstd,
            ParquetCompression::Snappy,
            ParquetCompression::Gzip,
            ParquetCompression::Lz4,
        ] {
            let output = temp_dir
                .path()
                .join(format!("output_{:?}.parquet", compression));
            let converter =
                ParquetConverter::new(input_path.to_str().unwrap().to_string(), output.clone())
                    .with_compression(compression);
            converter.convert().await.unwrap();
            assert!(output.exists());
        }
    }

    #[tokio::test]
    async fn test_converter_with_sorting() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![5, 2, 4, 1, 3];
        let test_names = vec!["Eve", "Bob", "David", "Alice", "Charlie"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_sort_spec(Some(sort_spec));
        converter.convert().await.unwrap();

        let file = std::fs::File::open(&output_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
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
    async fn test_converter_with_statistics() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        for stats in [
            ParquetStatistics::None,
            ParquetStatistics::Chunk,
            ParquetStatistics::Page,
        ] {
            let output = temp_dir.path().join(format!("output_{:?}.parquet", stats));
            let converter =
                ParquetConverter::new(input_path.to_str().unwrap().to_string(), output.clone())
                    .with_statistics(stats);
            converter.convert().await.unwrap();
            assert!(output.exists());
        }
    }

    #[tokio::test]
    async fn test_converter_with_writer_version() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        for version in [ParquetWriterVersion::V1, ParquetWriterVersion::V2] {
            let output = temp_dir
                .path()
                .join(format!("output_{:?}.parquet", version));
            let converter =
                ParquetConverter::new(input_path.to_str().unwrap().to_string(), output.clone())
                    .with_writer_version(version);
            converter.convert().await.unwrap();
            assert!(output.exists());
        }
    }

    #[tokio::test]
    async fn test_converter_with_dictionary_encoding() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_no_dictionary(true);
        converter.convert().await.unwrap();

        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_converter_with_row_group_size() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_data::simple_schema();
        let mut batches = Vec::new();
        for i in 0..10 {
            let test_ids = vec![i * 3 + 1, i * 3 + 2, i * 3 + 3];
            let test_names = vec!["A", "B", "C"];
            batches.push(test_data::create_batch_with_ids_and_names(
                &schema,
                &test_ids,
                &test_names,
            ));
        }
        file_helpers::write_arrow_file(&input_path, &schema, batches).unwrap();

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_parquet_row_group_size(10);
        converter.convert().await.unwrap();

        let file = std::fs::File::open(&output_path).unwrap();
        let parquet_reader = parquet::file::reader::SerializedFileReader::new(file).unwrap();
        let metadata = parquet_reader.metadata();
        assert!(metadata.num_row_groups() > 1);
    }

    #[tokio::test]
    async fn test_converter_empty_file() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_data::simple_schema();
        file_helpers::write_arrow_stream(&input_path, &schema, vec![]).unwrap();

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        );
        converter.convert().await.unwrap();

        let file = std::fs::File::open(&output_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[tokio::test]
    async fn test_converter_invalid_input_path() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let converter = ParquetConverter::new("/nonexistent/file.arrow".to_string(), output_path);
        let result = converter.convert().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_converter_corrupted_file() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("corrupted.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        file_helpers::write_invalid_file(&input_path).unwrap();

        let converter =
            ParquetConverter::new(input_path.to_str().unwrap().to_string(), output_path);
        let result = converter.convert().await;

        assert!(result.is_err());
    }
}

#[cfg(test)]
mod bloom_filter_tests {
    use crate::converters::parquet::ParquetConverter;
    use crate::{
        AllColumnsBloomFilterConfig, BloomFilterConfig, ColumnBloomFilterConfig,
        ColumnSpecificBloomFilterConfig,
        utils::test_helpers::{file_helpers, test_data},
    };
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_converter_with_bloom_filter_all_columns() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3, 1, 2];
        let test_names = vec!["A", "B", "C", "A", "B"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let bloom_config = BloomFilterConfig::All(AllColumnsBloomFilterConfig { fpp: Some(0.001) });

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_bloom_filters(bloom_config);
        converter.convert().await.unwrap();

        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_converter_with_bloom_filter_specific_columns() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3, 1, 2];
        let test_names = vec!["A", "B", "C", "A", "B"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let bloom_config = BloomFilterConfig::Columns(vec![
            ColumnSpecificBloomFilterConfig {
                name: "id".to_string(),
                config: ColumnBloomFilterConfig { fpp: Some(0.01) },
            },
            ColumnSpecificBloomFilterConfig {
                name: "name".to_string(),
                config: ColumnBloomFilterConfig { fpp: None },
            },
        ]);

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_bloom_filters(bloom_config);
        converter.convert().await.unwrap();

        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_converter_bloom_filter_invalid_column() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let bloom_config = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
            name: "nonexistent_column".to_string(),
            config: ColumnBloomFilterConfig { fpp: Some(0.01) },
        }]);

        let converter =
            ParquetConverter::new(input_path.to_str().unwrap().to_string(), output_path)
                .with_bloom_filters(bloom_config);
        let result = converter.convert().await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not found in schema")
        );
    }
}

#[cfg(test)]
mod sorting_metadata_tests {
    use crate::SortColumn;
    use crate::converters::parquet::ParquetConverter;
    use crate::{
        SortDirection, SortSpec,
        utils::test_helpers::{file_helpers, test_data},
    };
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_converter_with_sorted_metadata() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![5, 2, 4, 1, 3];
        let test_names = vec!["Eve", "Bob", "David", "Alice", "Charlie"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_sort_spec(Some(sort_spec))
        .with_write_sorted_metadata(true);
        converter.convert().await.unwrap();

        // TODO: Once parquet-rs exposes sort metadata reading, verify it here
        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_converter_multi_column_sort_metadata() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_groups = vec![1, 2, 1, 2, 1];
        let test_values = vec![30, 20, 10, 40, 20];

        let schema = test_data::multi_column_for_sorting_schema();
        let batch =
            test_data::create_multi_column_for_sorting_batch(&schema, &test_groups, &test_values);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let sort_spec = SortSpec {
            columns: vec![
                SortColumn {
                    name: "group".to_string(),
                    direction: SortDirection::Ascending,
                },
                SortColumn {
                    name: "value".to_string(),
                    direction: SortDirection::Descending,
                },
            ],
        };

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_sort_spec(Some(sort_spec))
        .with_write_sorted_metadata(true);
        converter.convert().await.unwrap();

        assert!(output_path.exists());
    }

    #[tokio::test]
    async fn test_converter_sort_invalid_column() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![1, 2, 3];
        let test_names = vec!["A", "B", "C"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "nonexistent_column".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        let converter =
            ParquetConverter::new(input_path.to_str().unwrap().to_string(), output_path)
                .with_sort_spec(Some(sort_spec))
                .with_write_sorted_metadata(true);
        let result = converter.convert().await;

        assert!(result.is_err());
    }
}

#[cfg(test)]
mod integration_tests {
    use crate::converters::parquet::ParquetConverter;
    use crate::{
        BloomFilterConfig, ColumnBloomFilterConfig, ColumnSpecificBloomFilterConfig,
        ParquetCompression, ParquetStatistics, ParquetWriterVersion, SortColumn, SortDirection,
        SortSpec,
        utils::test_helpers::{file_helpers, test_data},
    };
    use arrow::array::{Array, Int32Array};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_converter_full_configuration() {
        let temp_dir = tempdir().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        let test_ids = vec![5, 2, 4, 1, 3, 5, 2];
        let test_names = vec!["Eve", "Bob", "David", "Alice", "Charlie", "Eve", "Bob"];

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let sort_spec = SortSpec {
            columns: vec![SortColumn {
                name: "id".to_string(),
                direction: SortDirection::Ascending,
            }],
        };

        let bloom_config = BloomFilterConfig::Columns(vec![ColumnSpecificBloomFilterConfig {
            name: "id".to_string(),
            config: ColumnBloomFilterConfig { fpp: Some(0.001) },
        }]);

        let converter = ParquetConverter::new(
            input_path.to_str().unwrap().to_string(),
            output_path.clone(),
        )
        .with_sort_spec(Some(sort_spec))
        .with_compression(ParquetCompression::Zstd)
        .with_bloom_filters(bloom_config)
        .with_statistics(ParquetStatistics::Page)
        .with_parquet_row_group_size(100_000)
        .with_no_dictionary(false)
        .with_writer_version(ParquetWriterVersion::V2)
        .with_write_sorted_metadata(true)
        .with_record_batch_size(1000);

        converter.convert().await.unwrap();

        assert!(output_path.exists());
        let file = std::fs::File::open(&output_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 2);
        assert_eq!(ids.value(3), 3);
        assert_eq!(ids.value(4), 4);
        assert_eq!(ids.value(5), 5);
        assert_eq!(ids.value(6), 5);
    }
}
