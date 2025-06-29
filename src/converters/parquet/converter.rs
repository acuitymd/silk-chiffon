use anyhow::{Result, anyhow};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{
    fs::File,
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;

use crate::{
    BloomFilterConfig, ParquetCompression, ParquetStatistics, ParquetWriterVersion, SortSpec,
    converters::arrow::ArrowConverter,
    utils::arrow_io::{ArrowIPCFormat, ArrowIPCReader},
};

use super::{
    config::ParquetConfig, metadata::SortMetadataBuilder, ndv_calculator::NdvCalculator,
    writer_builder::ParquetWriterBuilder,
};

/// Main converter for Arrow to Parquet format
pub struct ParquetConverter {
    config: ParquetConfig,
}

impl ParquetConverter {
    pub fn new(input_path: String, output_path: PathBuf) -> Self {
        Self {
            config: ParquetConfig::new(input_path, output_path),
        }
    }

    // Builder methods
    pub fn with_sort_spec(mut self, sort_spec: Option<SortSpec>) -> Self {
        self.config.sort_spec = sort_spec;
        self
    }

    pub fn with_record_batch_size(mut self, size: usize) -> Self {
        self.config.record_batch_size = size;
        self
    }

    pub fn with_compression(mut self, compression: ParquetCompression) -> Self {
        self.config.compression = compression;
        self
    }

    pub fn with_bloom_filters(mut self, bloom_filters: BloomFilterConfig) -> Self {
        self.config.bloom_filters = bloom_filters;
        self
    }

    pub fn with_statistics(mut self, statistics: ParquetStatistics) -> Self {
        self.config.statistics = statistics;
        self
    }

    pub fn with_parquet_row_group_size(mut self, size: usize) -> Self {
        self.config.parquet_row_group_size = size;
        self
    }

    pub fn with_no_dictionary(mut self, no_dictionary: bool) -> Self {
        self.config.no_dictionary = no_dictionary;
        self
    }

    pub fn with_writer_version(mut self, version: ParquetWriterVersion) -> Self {
        self.config.writer_version = version;
        self
    }

    pub fn with_write_sorted_metadata(mut self, write_sorted_metadata: bool) -> Self {
        self.config.write_sorted_metadata = write_sorted_metadata;
        self
    }

    pub async fn convert(&self) -> Result<()> {
        let (arrow_file_path, _temp_keep_alive) = self.prepare_arrow_file().await?;

        let ndv_calculator = NdvCalculator::new(self.config.bloom_filters.clone());
        let ndv_map = ndv_calculator.calculate(&arrow_file_path).await?;

        let writer_properties = self.create_writer_properties(&arrow_file_path, &ndv_map)?;

        self.write_parquet(&arrow_file_path, writer_properties)
            .await?;

        Ok(())
    }

    async fn prepare_arrow_file(&self) -> Result<(PathBuf, Option<NamedTempFile>)> {
        if self.needs_intermediate_arrow_file() {
            let temp_file = NamedTempFile::new()?;
            let temp_path = temp_file.path().with_extension("arrow").to_path_buf();

            let mut arrow_converter = ArrowConverter::new(&self.config.input_path, &temp_path);

            if let Some(ref sort_spec) = self.config.sort_spec {
                arrow_converter = arrow_converter.with_sorting(sort_spec.clone());
            }

            arrow_converter = arrow_converter.with_record_batch_size(self.config.record_batch_size);

            arrow_converter.convert().await?;

            Ok((temp_path, Some(temp_file)))
        } else {
            Ok((PathBuf::from(&self.config.input_path), None))
        }
    }

    fn needs_intermediate_arrow_file(&self) -> bool {
        self.input_is_arrow_stream()
            || self.config.sort_spec.is_some()
            || NdvCalculator::new(self.config.bloom_filters.clone()).needs_calculation()
    }

    fn input_is_arrow_stream(&self) -> bool {
        ArrowIPCReader::from_path(&self.config.input_path)
            .is_ok_and(|r| r.format() == ArrowIPCFormat::Stream)
    }

    fn create_writer_properties(
        &self,
        input_path: &Path,
        ndv_map: &std::collections::HashMap<String, u64>,
    ) -> Result<WriterProperties> {
        let sort_metadata_builder = if self.config.write_sorted_metadata {
            self.config.sort_spec.as_ref().map(SortMetadataBuilder::new)
        } else {
            None
        };

        let writer_builder = ParquetWriterBuilder::new(
            self.config.compression,
            self.config.statistics,
            self.config.writer_version,
            self.config.parquet_row_group_size,
            self.config.no_dictionary,
            &self.config.bloom_filters,
            sort_metadata_builder,
        );

        writer_builder.build(input_path, ndv_map)
    }

    async fn write_parquet(
        &self,
        input_path: &Path,
        writer_properties: WriterProperties,
    ) -> Result<()> {
        let reader = ArrowIPCReader::from_path(
            input_path
                .to_str()
                .ok_or_else(|| anyhow!("Invalid path: {:?}", input_path))?,
        )?;
        let schema = reader
            .schema()
            .map_err(|e| anyhow!("Schema not found: {}", e))?;

        let file = File::create(&self.config.output_path)?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(writer_properties))
            .map_err(|e| anyhow!("Failed to create Parquet writer: {}", e))?;

        match reader.format() {
            ArrowIPCFormat::File => {
                let file_reader = reader.file_reader()?;
                for batch in file_reader {
                    writer.write(&batch?)?;
                }
            }
            ArrowIPCFormat::Stream => {
                let stream_reader = reader.stream_reader()?;
                for batch in stream_reader {
                    writer.write(&batch?)?;
                }
            }
        }

        writer.close()?;
        Ok(())
    }
}
