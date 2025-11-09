use crate::utils::arrow_io::ArrowIPCFormat;
use crate::{
    ArrowCompression, BloomFilterConfig, ParquetCompression, ParquetStatistics,
    ParquetWriterVersion, SortDirection, SortSpec,
};
use anyhow::{Result, anyhow};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};
use async_trait::async_trait;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::format::SortingColumn;
use parquet::schema::types::ColumnPath;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

const IO_BUFFER_SIZE: usize = 1024 * 1024;

pub trait PartitionWriter: Send {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn finish(&mut self) -> Result<PathBuf>;
}

#[async_trait]
pub trait WriterBuilder: Send + Sync {
    async fn build_writer(
        &self,
        path: &Path,
        schema: &SchemaRef,
    ) -> Result<Box<dyn PartitionWriter>>;
}

pub struct ArrowFileWriter {
    writer: FileWriter<BufWriter<File>>,
    path: PathBuf,
}

impl PartitionWriter for ArrowFileWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer
            .write(batch)
            .map_err(|e| anyhow!("Failed to write batch: {}", e))
    }

    fn finish(&mut self) -> Result<PathBuf> {
        self.writer
            .finish()
            .map_err(|e| anyhow!("Failed to finish writer: {}", e))?;
        Ok(self.path.clone())
    }
}

pub struct ArrowStreamWriter {
    writer: StreamWriter<BufWriter<File>>,
    path: PathBuf,
}

impl PartitionWriter for ArrowStreamWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer
            .write(batch)
            .map_err(|e| anyhow!("Failed to write batch: {}", e))
    }

    fn finish(&mut self) -> Result<PathBuf> {
        self.writer
            .finish()
            .map_err(|e| anyhow!("Failed to finish writer: {}", e))?;
        Ok(self.path.clone())
    }
}

pub struct ParquetWriter {
    writer: ArrowWriter<BufWriter<File>>,
    path: PathBuf,
}

impl PartitionWriter for ParquetWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer
            .write(batch)
            .map_err(|e| anyhow!("Failed to write batch: {}", e))
    }

    fn finish(&mut self) -> Result<PathBuf> {
        self.writer
            .finish()
            .map_err(|e| anyhow!("Failed to finish writer: {}", e))?;
        Ok(self.path.clone())
    }
}

pub struct ArrowWriterBuilder {
    compression: Option<ArrowCompression>,
    ipc_format: ArrowIPCFormat,
}

impl ArrowWriterBuilder {
    pub fn new() -> Self {
        Self {
            compression: None,
            ipc_format: ArrowIPCFormat::default(),
        }
    }

    pub fn with_compression(mut self, compression: Option<ArrowCompression>) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_ipc_format(mut self, ipc_format: ArrowIPCFormat) -> Self {
        self.ipc_format = ipc_format;
        self
    }
}

#[async_trait]
impl WriterBuilder for ArrowWriterBuilder {
    async fn build_writer(
        &self,
        path: &std::path::Path,
        schema: &SchemaRef,
    ) -> Result<Box<dyn PartitionWriter>> {
        let file = File::create(path)?;
        let buf_writer = BufWriter::with_capacity(IO_BUFFER_SIZE, file);

        let options = if let Some(compression) = &self.compression {
            match compression {
                ArrowCompression::Zstd => IpcWriteOptions::default()
                    .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))?,
                ArrowCompression::Lz4 => IpcWriteOptions::default()
                    .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))?,
                ArrowCompression::None => IpcWriteOptions::default(),
            }
        } else {
            IpcWriteOptions::default()
        };

        match self.ipc_format {
            ArrowIPCFormat::File => {
                let writer = FileWriter::try_new_with_options(buf_writer, schema, options)?;
                Ok(Box::new(ArrowFileWriter {
                    writer,
                    path: path.to_path_buf(),
                }))
            }
            ArrowIPCFormat::Stream => {
                let writer = StreamWriter::try_new_with_options(buf_writer, schema, options)?;
                Ok(Box::new(ArrowStreamWriter {
                    writer,
                    path: path.to_path_buf(),
                }))
            }
        }
    }
}

pub struct ParquetWriterBuilder {
    compression: Option<ParquetCompression>,
    statistics: ParquetStatistics,
    max_row_group_size: usize,
    writer_version: ParquetWriterVersion,
    no_dictionary: bool,
    bloom_filters: BloomFilterConfig,
    write_sorted_metadata: bool,
    sort_spec: Option<SortSpec>,
}

impl ParquetWriterBuilder {
    pub fn new() -> Self {
        Self {
            compression: None,
            statistics: ParquetStatistics::Page,
            max_row_group_size: 1_048_576,
            writer_version: ParquetWriterVersion::V2,
            no_dictionary: false,
            bloom_filters: BloomFilterConfig::None,
            write_sorted_metadata: false,
            sort_spec: None,
        }
    }

    pub fn with_compression(mut self, compression: Option<ParquetCompression>) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_statistics(mut self, statistics: ParquetStatistics) -> Self {
        self.statistics = statistics;
        self
    }

    pub fn with_max_row_group_size(mut self, size: usize) -> Self {
        self.max_row_group_size = size;
        self
    }

    pub fn with_writer_version(mut self, version: ParquetWriterVersion) -> Self {
        self.writer_version = version;
        self
    }

    pub fn with_no_dictionary(mut self, no_dict: bool) -> Self {
        self.no_dictionary = no_dict;
        self
    }

    pub fn with_bloom_filters(mut self, bloom: BloomFilterConfig) -> Self {
        self.bloom_filters = bloom;
        self
    }

    pub fn with_write_sorted_metadata(mut self, write: bool) -> Self {
        self.write_sorted_metadata = write;
        self
    }

    pub fn with_sort_spec(mut self, sort: Option<SortSpec>) -> Self {
        self.sort_spec = sort;
        self
    }

    async fn build_writer_properties(&self, schema: &SchemaRef) -> Result<WriterProperties> {
        let mut builder = WriterProperties::builder()
            .set_max_row_group_size(self.max_row_group_size)
            .set_dictionary_enabled(!self.no_dictionary);

        if let Some(compression) = &self.compression {
            builder = builder.set_compression((*compression).into());
        }

        builder = builder.set_statistics_enabled(self.statistics.into());
        builder = builder.set_writer_version(self.writer_version.into());

        if self.bloom_filters.is_configured() {
            builder = self.apply_bloom_filters(builder)?;
        }

        if self.write_sorted_metadata && self.sort_spec.is_some() {
            builder = self.apply_sort_metadata(builder, schema)?;
        }

        Ok(builder.build())
    }

    fn apply_bloom_filters(
        &self,
        builder: WriterPropertiesBuilder,
    ) -> Result<WriterPropertiesBuilder> {
        match &self.bloom_filters {
            BloomFilterConfig::None => Ok(builder),
            BloomFilterConfig::All(bloom_all) => Ok(builder
                .set_bloom_filter_enabled(true)
                .set_bloom_filter_fpp(bloom_all.fpp)),
            BloomFilterConfig::Columns(columns) => {
                Ok(columns.iter().fold(builder, |acc_builder, bloom_col| {
                    let col_path = ColumnPath::from(bloom_col.name.as_str());
                    acc_builder
                        .set_column_bloom_filter_enabled(col_path.clone(), true)
                        .set_column_bloom_filter_fpp(col_path.clone(), bloom_col.config.fpp)
                }))
            }
        }
    }

    fn apply_sort_metadata(
        &self,
        builder: WriterPropertiesBuilder,
        schema: &SchemaRef,
    ) -> Result<WriterPropertiesBuilder> {
        if let Some(sort_spec) = &self.sort_spec {
            if sort_spec.is_empty() {
                return Ok(builder);
            }

            let mut sorting_columns = Vec::new();

            for sort_col in &sort_spec.columns {
                let column_idx = schema
                    .index_of(&sort_col.name)
                    .map_err(|_| anyhow!("Sort column '{}' not found in schema", sort_col.name))?;

                let descending = sort_col.direction == SortDirection::Descending;

                sorting_columns.push(SortingColumn {
                    column_idx: i32::try_from(column_idx)
                        .map_err(|_| anyhow!("Column index out of range"))?,
                    descending,
                    nulls_first: descending,
                });
            }

            Ok(builder.set_sorting_columns(Some(sorting_columns)))
        } else {
            Ok(builder)
        }
    }
}

#[async_trait]
impl WriterBuilder for ParquetWriterBuilder {
    async fn build_writer(
        &self,
        path: &std::path::Path,
        schema: &SchemaRef,
    ) -> Result<Box<dyn PartitionWriter>> {
        let file = File::create(path)?;
        let buf_writer = BufWriter::with_capacity(IO_BUFFER_SIZE, file);

        let writer_properties = self.build_writer_properties(schema).await?;
        let writer = ArrowWriter::try_new(buf_writer, schema.clone(), Some(writer_properties))?;

        Ok(Box::new(ParquetWriter {
            writer,
            path: path.to_path_buf(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_test_batch(schema: SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_arrow_writer_no_compression() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.arrow");
        let schema = create_test_schema();

        let builder = ArrowWriterBuilder::new();
        let mut writer = builder.build_writer(&path, &schema).await.unwrap();

        let batch = create_test_batch(schema);
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        assert!(path.exists());
        assert!(path.metadata().unwrap().len() > 0);
    }

    #[tokio::test]
    async fn test_arrow_writer_with_compression() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let batch = create_test_batch(schema.clone());

        for compression in [ArrowCompression::Lz4, ArrowCompression::Zstd] {
            let path = temp_dir.path().join(format!("test_{compression:?}.arrow"));

            let builder = ArrowWriterBuilder::new().with_compression(Some(compression));
            let mut writer = builder.build_writer(&path, &schema).await.unwrap();

            writer.write_batch(&batch).unwrap();
            writer.finish().unwrap();

            assert!(path.exists());
        }
    }

    #[tokio::test]
    async fn test_parquet_writer_basic() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");
        let schema = create_test_schema();

        let builder = ParquetWriterBuilder::new();
        let mut writer = builder.build_writer(&path, &schema).await.unwrap();

        let batch = create_test_batch(schema);
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        assert!(path.exists());

        let file = File::open(&path).unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 1);
    }

    #[tokio::test]
    async fn test_parquet_writer_with_options() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_options.parquet");
        let schema = create_test_schema();

        let builder = ParquetWriterBuilder::new()
            .with_compression(Some(ParquetCompression::Snappy))
            .with_statistics(ParquetStatistics::Page)
            .with_max_row_group_size(100)
            .with_writer_version(ParquetWriterVersion::V2)
            .with_no_dictionary(true);

        let mut writer = builder.build_writer(&path, &schema).await.unwrap();

        for _ in 0..50 {
            let batch = create_test_batch(schema.clone());
            writer.write_batch(&batch).unwrap();
        }
        writer.finish().unwrap();

        assert!(path.exists());

        let file = File::open(&path).unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = reader.metadata();

        assert!(metadata.num_row_groups() > 1);
    }

    #[tokio::test]
    async fn test_parquet_writer_with_bloom_filters() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_bloom.parquet");
        let schema = create_test_schema();

        let bloom_config = BloomFilterConfig::All(crate::AllColumnsBloomFilterConfig { fpp: 0.01 });

        let builder = ParquetWriterBuilder::new().with_bloom_filters(bloom_config);

        let mut writer = builder.build_writer(&path, &schema).await.unwrap();
        let batch = create_test_batch(schema);
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_parquet_writer_with_sorted_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test_sorted.parquet");
        let schema = create_test_schema();

        let sort_spec = Some(SortSpec {
            columns: vec![crate::SortColumn {
                name: "id".to_string(),
                direction: crate::SortDirection::Ascending,
            }],
        });

        let builder = ParquetWriterBuilder::new()
            .with_sort_spec(sort_spec)
            .with_write_sorted_metadata(true);

        let mut writer = builder.build_writer(&path, &schema).await.unwrap();
        let batch = create_test_batch(schema);
        writer.write_batch(&batch).unwrap();
        writer.finish().unwrap();

        assert!(path.exists());
    }
}
