use anyhow::Result;
use arrow::ipc::CompressionType;
use clap::{Args, ValueEnum};

use crate::variant::IpcVariant;

#[derive(Args, Clone, Debug)]
#[group(id = "arrow_transform")]
pub(crate) struct TransformArgs {
    /// Arrow IPC compression codec.
    #[arg(long, value_enum, default_value_t = Compression::default(), help_heading = "Arrow Options")]
    pub(crate) arrow_compression: Compression,

    /// Arrow IPC format (file or stream).
    #[arg(long, value_enum, default_value_t = IpcVariant::default(), help_heading = "Arrow Options")]
    pub(crate) arrow_format: IpcVariant,

    /// Arrow record batch size.
    #[arg(long, default_value_t = 122_880, help_heading = "Arrow Options")]
    pub(crate) arrow_record_batch_size: usize,

    /// Arrow writer queue size (number of batches buffered before backpressure).
    #[arg(long, default_value = "16", value_parser = parse_positive, help_heading = "Arrow Options")]
    pub(crate) arrow_writing_queue_size: usize,
}

#[derive(Args, Clone, Debug)]
#[group(id = "arrow_inspection")]
pub(crate) struct InspectionArgs {
    /// Show per-record-batch details.
    #[arg(long)]
    pub(crate) batches: bool,

    /// Count total rows by reading every record batch.
    #[arg(long)]
    pub(crate) row_count: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lowercase")]
pub(crate) enum Compression {
    Zstd,
    Lz4,
    #[default]
    None,
}

impl From<Compression> for Option<CompressionType> {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::Zstd => Some(CompressionType::ZSTD),
            Compression::Lz4 => Some(CompressionType::LZ4_FRAME),
            Compression::None => None,
        }
    }
}

fn parse_positive(value: &str) -> Result<usize> {
    let value: usize = value.parse()?;
    if value == 0 {
        anyhow::bail!("value must be at least 1");
    }
    Ok(value)
}
