//! Adaptive parquet writer with per-row-group encoding optimization.
//!
//! - **Ingestion task**: receives record batches, coalesces them into row-group-sized chunks,
//!   and optionally runs cardinality analysis via DataFusion streaming aggregation.
//! - **Encoder tasks**: encodes row groups in parallel using the parquet-rs encoder.
//!   Multiple encoder tasks allow CPU-bound encoding to proceed concurrently.
//! - **Writer task**: writes encoded row groups to disk in order. Uses an ordered channel
//!   to ensure row groups are written sequentially even when encoded out of order.
//!
//! # Dictionary and Bloom Filter Configuration
//!
//! Columns can be configured for dictionary encoding in three modes:
//! - `Always`: force dictionary encoding (parquet-rs handles overflow)
//! - `Analyze`: use streaming cardinality analysis to decide per row group
//! - `Disabled`: never use dictionary encoding

mod analysis;
mod config;
pub(crate) mod encoding;
mod pipeline;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use parquet::file::properties::WriterProperties;
use tokio::sync::mpsc;

pub use config::AdaptiveWriterConfig;

use crate::sinks::parquet::{ParquetRuntimes, ParquetWriter};
use pipeline::run_pipeline;

pub struct AdaptiveParquetWriter {
    ingestion_sender: Option<mpsc::Sender<RecordBatch>>,
    monitor_handle: Option<tokio::task::JoinHandle<Result<u64>>>,
}

impl AdaptiveParquetWriter {
    pub fn new(
        path: &Path,
        schema: &SchemaRef,
        base_props: WriterProperties,
        runtimes: Arc<ParquetRuntimes>,
        config: AdaptiveWriterConfig,
    ) -> Self {
        let (ingestion_tx, ingestion_rx) = mpsc::channel(config.ingestion_queue_size);

        let monitor_handle = tokio::spawn({
            let path = path.to_path_buf();
            let schema = Arc::clone(schema);
            async move { run_pipeline(path, schema, base_props, runtimes, config, ingestion_rx).await }
        });

        Self {
            ingestion_sender: Some(ingestion_tx),
            monitor_handle: Some(monitor_handle),
        }
    }

    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let sender = self
            .ingestion_sender
            .as_ref()
            .ok_or_else(|| anyhow!("writer already closed"))?;

        sender
            .send(batch)
            .await
            .map_err(|_| anyhow!("batch send failed: writer pipeline closed"))
    }

    pub fn blocking_write(&mut self, batch: RecordBatch) -> Result<()> {
        let sender = self
            .ingestion_sender
            .as_ref()
            .ok_or_else(|| anyhow!("writer already closed"))?;

        sender
            .blocking_send(batch)
            .map_err(|_| anyhow!("batch send failed: writer pipeline closed"))
    }

    pub async fn close(mut self) -> Result<u64> {
        // drop sender to signal end of input
        self.ingestion_sender.take();

        let handle = self
            .monitor_handle
            .take()
            .ok_or_else(|| anyhow!("already closed"))?;

        match handle.await {
            Ok(result) => result,
            Err(e) => Err(anyhow!("writer task panicked: {e}")),
        }
    }

    pub fn blocking_close(self) -> Result<u64> {
        crate::utils::blocking::block_on(self.close())
    }

    pub async fn cancel(mut self) -> Result<()> {
        self.ingestion_sender.take();
        if let Some(handle) = self.monitor_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        Ok(())
    }

    pub fn blocking_cancel(self) -> Result<()> {
        crate::utils::blocking::block_on(self.cancel())
    }
}

impl Drop for AdaptiveParquetWriter {
    fn drop(&mut self) {
        if let Some(handle) = self.monitor_handle.take() {
            handle.abort();
        }
    }
}

#[async_trait]
impl ParquetWriter for AdaptiveParquetWriter {
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        AdaptiveParquetWriter::write(self, batch).await
    }

    fn blocking_write(&mut self, batch: RecordBatch) -> Result<()> {
        AdaptiveParquetWriter::blocking_write(self, batch)
    }

    async fn close(self: Box<Self>) -> Result<u64> {
        AdaptiveParquetWriter::close(*self).await
    }

    fn blocking_close(self: Box<Self>) -> Result<u64> {
        AdaptiveParquetWriter::blocking_close(*self)
    }

    async fn cancel(self: Box<Self>) -> Result<()> {
        AdaptiveParquetWriter::cancel(*self).await
    }

    fn blocking_cancel(self: Box<Self>) -> Result<()> {
        AdaptiveParquetWriter::blocking_cancel(*self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, Int64Array, ListArray, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File as StdFile;
    use tempfile::tempdir;

    use crate::{
        AllColumnsBloomFilterConfig, BloomFilterConfigBuilder, ColumnBloomFilterConfig,
        ColumnSpecificBloomFilterConfig,
    };

    fn test_runtimes() -> Arc<ParquetRuntimes> {
        Arc::new(ParquetRuntimes::try_new(2, 1).unwrap())
    }

    fn test_schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_batch(schema: &SchemaRef, ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    fn read_column_metadata(path: &Path) -> Vec<(String, Option<i64>, Option<i64>)> {
        let file = StdFile::open(path).unwrap();
        let reader: SerializedFileReader<StdFile> = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        let mut results = Vec::new();
        for rg in metadata.row_groups() {
            for col in rg.columns() {
                let name = col.column_path().string();
                results.push((
                    name,
                    col.dictionary_page_offset(),
                    col.bloom_filter_offset(),
                ));
            }
        }
        results
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_adaptive_writer_basic() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();
        let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig::default();

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        let rows = Box::new(writer).close().await.unwrap();

        assert_eq!(rows, 3);
        assert!(output_path.exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_adaptive_writer_multiple_row_groups() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            max_row_group_size: 10,
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);

        for i in 0..5 {
            let ids: Vec<i32> = (i * 10..(i + 1) * 10).collect();
            let names: Vec<String> = ids.iter().map(|id| format!("name_{id}")).collect();
            let names_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let batch = create_batch(&schema, &ids, &names_refs);
            writer.write(batch).await.unwrap();
        }

        let rows = Box::new(writer).close().await.unwrap();
        assert_eq!(rows, 50);

        let file = StdFile::open(&output_path).unwrap();
        let reader: SerializedFileReader<StdFile> = SerializedFileReader::new(file).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dictionary_encoding_forced_on() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();
        let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            user_enabled_dictionary_always: vec!["name".to_string()],
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        let name_col = columns.iter().find(|(n, _, _)| n == "name").unwrap();
        assert!(
            name_col.1.is_some(),
            "name column should have dictionary encoding"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dictionary_encoding_forced_off() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();
        let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            user_disabled_dictionary: vec!["name".to_string()],
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        let name_col = columns.iter().find(|(n, _, _)| n == "name").unwrap();
        assert!(
            name_col.1.is_none(),
            "name column should NOT have dictionary encoding"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dictionary_per_column_override() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();
        let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            dictionary_enabled_default: false,
            user_enabled_dictionary_always: vec!["name".to_string()],
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        let id_col = columns.iter().find(|(n, _, _)| n == "id").unwrap();
        let name_col = columns.iter().find(|(n, _, _)| n == "name").unwrap();

        assert!(
            id_col.1.is_none(),
            "id column should NOT have dictionary (default off)"
        );
        assert!(
            name_col.1.is_some(),
            "name column SHOULD have dictionary (override)"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dictionary_analyze_mode_low_cardinality() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();

        let ids: Vec<i32> = (0..100).map(|i| i % 2).collect();
        let names: Vec<String> = (0..100).map(|i| format!("name_{}", i % 2)).collect();
        let names_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let batch = create_batch(&schema, &ids, &names_refs);

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            dictionary_enabled_default: true,
            user_enabled_dictionary_analyze: vec!["id".to_string(), "name".to_string()],
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        for (name, dict_offset, _bloom_offset) in &columns {
            assert!(
                dict_offset.is_some(),
                "column '{name}' should have dictionary (low cardinality)"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dictionary_analyze_mode_high_cardinality() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();

        let ids: Vec<i32> = (0..100).collect();
        let names: Vec<String> = (0..100).map(|i| format!("name_{i}")).collect();
        let names_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let batch = create_batch(&schema, &ids, &names_refs);

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            dictionary_enabled_default: true,
            user_enabled_dictionary_analyze: vec!["id".to_string(), "name".to_string()],
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        for (name, dict_offset, _bloom_offset) in &columns {
            assert!(
                dict_offset.is_none(),
                "column '{name}' should NOT have dictionary (high cardinality)"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bloom_filter_enabled() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();
        let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);

        let bloom_config = BloomFilterConfigBuilder::default()
            .all_enabled(AllColumnsBloomFilterConfig {
                fpp: 0.01,
                ndv: Some(100),
            })
            .build()
            .unwrap();

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            bloom_filter_config: Some(bloom_config),
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        for (name, _dict_offset, bloom_offset) in &columns {
            assert!(
                bloom_offset.is_some(),
                "column '{name}' should have bloom filter"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bloom_filter_disabled_by_default() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();
        let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig::default();

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        for (name, _dict_offset, bloom_offset) in &columns {
            assert!(
                bloom_offset.is_none(),
                "column '{name}' should NOT have bloom filter (disabled by default)"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bloom_filter_per_column_override() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();
        let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);

        let bloom_config = BloomFilterConfigBuilder::default()
            .enable_column(ColumnSpecificBloomFilterConfig {
                name: "name".to_string(),
                config: ColumnBloomFilterConfig {
                    fpp: 0.05,
                    ndv: Some(50),
                },
            })
            .build()
            .unwrap();

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            bloom_filter_config: Some(bloom_config),
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        let id_col = columns.iter().find(|(n, _, _)| n == "id").unwrap();
        let name_col = columns.iter().find(|(n, _, _)| n == "name").unwrap();

        assert!(id_col.2.is_none(), "id column should NOT have bloom filter");
        assert!(name_col.2.is_some(), "name column SHOULD have bloom filter");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bloom_filter_column_specific_enable() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();
        let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);

        let bloom_config = BloomFilterConfigBuilder::default()
            .enable_column(ColumnSpecificBloomFilterConfig {
                name: "id".to_string(),
                config: ColumnBloomFilterConfig {
                    fpp: 0.01,
                    ndv: Some(100),
                },
            })
            .build()
            .unwrap();

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            bloom_filter_config: Some(bloom_config),
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        let id_col = columns.iter().find(|(n, _, _)| n == "id").unwrap();
        let name_col = columns.iter().find(|(n, _, _)| n == "name").unwrap();

        assert!(id_col.2.is_some(), "id column SHOULD have bloom filter");
        assert!(
            name_col.2.is_none(),
            "name column should NOT have bloom filter"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_combined_dictionary_and_bloom_filter() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let schema = test_schema();
        let batch = create_batch(&schema, &[1, 2, 3], &["a", "b", "c"]);

        let bloom_config = BloomFilterConfigBuilder::default()
            .all_enabled(AllColumnsBloomFilterConfig {
                fpp: 0.01,
                ndv: Some(100),
            })
            .build()
            .unwrap();

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            user_enabled_dictionary_always: vec!["name".to_string()],
            bloom_filter_config: Some(bloom_config),
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        let name_col = columns.iter().find(|(n, _, _)| n == "name").unwrap();
        assert!(
            name_col.1.is_some(),
            "name column should have dictionary encoding"
        );
        assert!(name_col.2.is_some(), "name column should have bloom filter");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compression_propagated_to_nested_columns() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let inner_field = Field::new("item", DataType::Int64, true);
        let list_field = Field::new("values", DataType::List(Arc::new(inner_field)), true);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![list_field]));

        let values = Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let offsets = OffsetBuffer::new(vec![0, 2, 5].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            offsets,
            Arc::new(values),
            None,
        );
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(list_array) as ArrayRef])
                .unwrap();

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .build();
        let config = AdaptiveWriterConfig::default();

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let file = StdFile::open(&output_path).unwrap();
        let reader: SerializedFileReader<StdFile> = SerializedFileReader::new(file).unwrap();
        let row_group = reader.metadata().row_group(0);
        for col in row_group.columns() {
            assert_eq!(
                col.compression(),
                parquet::basic::Compression::ZSTD(Default::default()),
                "column {} should have ZSTD compression",
                col.column_path().string()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bloom_filter_on_nested_columns() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let struct_field = Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("b", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![struct_field]));

        let a_values = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
        let b_values = StringArray::from(vec![Some("x"), Some("y"), Some("z")]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int64, true)),
                Arc::new(a_values) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(b_values) as ArrayRef,
            ),
        ]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(struct_array) as ArrayRef],
        )
        .unwrap();

        let bloom_config = BloomFilterConfigBuilder::default()
            .enable_column(ColumnSpecificBloomFilterConfig {
                name: "data.a".to_string(),
                config: ColumnBloomFilterConfig {
                    fpp: 0.01,
                    ndv: Some(100),
                },
            })
            .enable_column(ColumnSpecificBloomFilterConfig {
                name: "data.b".to_string(),
                config: ColumnBloomFilterConfig {
                    fpp: 0.01,
                    ndv: Some(100),
                },
            })
            .build()
            .unwrap();

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            bloom_filter_config: Some(bloom_config),
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        let a_col = columns.iter().find(|(n, _, _)| n == "data.a").unwrap();
        let b_col = columns.iter().find(|(n, _, _)| n == "data.b").unwrap();

        assert!(a_col.2.is_some(), "data.a should have bloom filter");
        assert!(b_col.2.is_some(), "data.b should have bloom filter");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_bloom_filter_on_nested_columns_respects_dict_decision() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.parquet");

        let struct_field = Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("b", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![struct_field]));

        let a_values = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
        let b_values = StringArray::from(vec![Some("x"), Some("y"), Some("z")]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int64, true)),
                Arc::new(a_values) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(b_values) as ArrayRef,
            ),
        ]);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(struct_array) as ArrayRef],
        )
        .unwrap();

        let bloom_config = BloomFilterConfigBuilder::default()
            .all_enabled(AllColumnsBloomFilterConfig {
                fpp: 0.01,
                ndv: Some(100),
            })
            .build()
            .unwrap();

        let props = WriterProperties::builder().build();
        let config = AdaptiveWriterConfig {
            user_disabled_dictionary: vec!["data.a".to_string()],
            bloom_filter_config: Some(bloom_config),
            ..Default::default()
        };

        let mut writer =
            AdaptiveParquetWriter::new(&output_path, &schema, props, test_runtimes(), config);
        writer.write(batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let columns = read_column_metadata(&output_path);
        let a_col = columns.iter().find(|(n, _, _)| n == "data.a").unwrap();
        let b_col = columns.iter().find(|(n, _, _)| n == "data.b").unwrap();

        assert!(a_col.1.is_none(), "data.a should NOT have dictionary");
        assert!(a_col.2.is_some(), "data.a should have bloom filter");

        assert!(b_col.1.is_some(), "data.b should have dictionary");
        assert!(b_col.2.is_some(), "data.b should have bloom filter");
    }
}
