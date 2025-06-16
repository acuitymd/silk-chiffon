use crate::{
    ParquetArgs, ParquetCompression, ParquetWriterVersion,
    utils::filesystem::ensure_parent_dir_exists,
};
use anyhow::{Result, anyhow};
use parquet::{
    basic::{Compression, GzipLevel, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties},
};

pub async fn run(args: ParquetArgs) -> Result<()> {
    let _input_path = args.input.path().to_str().ok_or_else(|| {
        anyhow!(
            "Input path contains invalid UTF-8 characters: {:?}",
            args.input.path()
        )
    })?;
    let output_path = args.output.path();

    ensure_parent_dir_exists(output_path).await?;

    Ok(())
}

fn _create_writer_properties(args: &ParquetArgs) -> Result<WriterProperties> {
    let mut builder = WriterProperties::builder().set_max_row_group_size(args.max_row_group_size);

    builder = match args.compression {
        ParquetCompression::Zstd => {
            builder.set_compression(Compression::ZSTD(ZstdLevel::default()))
        }
        ParquetCompression::Snappy => builder.set_compression(Compression::SNAPPY),
        ParquetCompression::Gzip => {
            builder.set_compression(Compression::GZIP(GzipLevel::default()))
        }
        ParquetCompression::Lz4 => builder.set_compression(Compression::LZ4_RAW),
        ParquetCompression::None => builder.set_compression(Compression::UNCOMPRESSED),
    };

    builder = match args.writer_version {
        ParquetWriterVersion::V1 => {
            builder.set_writer_version(parquet::file::properties::WriterVersion::PARQUET_1_0)
        }
        ParquetWriterVersion::V2 => {
            builder.set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        }
    };

    if args.no_stats {
        builder = builder.set_statistics_enabled(EnabledStatistics::None);
    }

    if args.no_dictionary {
        builder = builder.set_dictionary_enabled(false);
    }

    // if let Some(bloom_all) = &args.bloom_all {
    //     builder = configure_bloom_all(builder, bloom_all)?;
    // }

    // for bloom_col in &args.bloom_column {
    //     builder = configure_bloom_column(builder, bloom_col)?;
    // }

    // builder = add_metadata_to_properties(builder, args);

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ParquetCompression, ParquetWriterVersion};
    use clio::{Input, OutputPath};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_run_returns_ok() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        std::fs::write(&input_path, b"dummy").unwrap();

        let args = ParquetArgs {
            input: Input::new(&input_path).unwrap(),
            output: OutputPath::new(&output_path).unwrap(),
            sort_by: None,
            compression: ParquetCompression::None,
            write_sorted_metadata: false,
            bloom_all: None,
            bloom_column: vec![],
            max_row_group_size: 122_880,
            no_stats: false,
            no_dictionary: false,
            writer_version: ParquetWriterVersion::V2,
        };

        assert!(run(args).await.is_ok());
    }

    #[tokio::test]
    async fn test_run_with_compression() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.parquet");

        std::fs::write(&input_path, b"dummy").unwrap();

        let args = ParquetArgs {
            input: Input::new(&input_path).unwrap(),
            output: OutputPath::new(&output_path).unwrap(),
            sort_by: None,
            compression: ParquetCompression::Zstd,
            write_sorted_metadata: false,
            bloom_all: None,
            bloom_column: vec![],
            max_row_group_size: 122_880,
            no_stats: false,
            no_dictionary: false,
            writer_version: ParquetWriterVersion::V2,
        };

        assert!(run(args).await.is_ok());
    }
}
