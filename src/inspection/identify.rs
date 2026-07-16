//! Format detection for data files.

use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    inspection::readers::{read_arrow_file_metadata, read_stream_metadata},
    inspection::{
        magic::read_magic_edges,
        style::{dim, value},
    },
    storage::InputObject,
};

const PARQUET_MAGIC: &[u8] = b"PAR1";
const ARROW_MAGIC: &[u8] = b"ARROW1";
const VORTEX_MAGIC: &[u8] = b"VTXF";

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(tag = "format", rename_all = "lowercase")]
pub enum DetectedFormat {
    Parquet,
    #[serde(rename = "arrow")]
    Arrow {
        variant: String,
    },
    Vortex,
    Unknown,
}

impl std::fmt::Display for DetectedFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DetectedFormat::Parquet => write!(f, "{}", value("Parquet")),
            DetectedFormat::Arrow { variant } => write!(
                f,
                "{} {}",
                value("Arrow IPC"),
                dim(format!("({})", variant))
            ),
            DetectedFormat::Vortex => write!(f, "{} {}", value("Vortex"), dim("(file)")),
            DetectedFormat::Unknown => write!(f, "{}", dim("Unknown")),
        }
    }
}

impl DetectedFormat {
    pub fn to_json(&self) -> Value {
        json!(self)
    }
}

/// Detect the format of an object from small ranges.
///
/// Tries each format in order:
/// 1. Parquet (PAR1 magic at start and end)
/// 2. Arrow IPC file
/// 3. Vortex (VTXF magic at start and end)
/// 4. Arrow IPC stream
pub async fn detect_format(input: &InputObject) -> Result<DetectedFormat> {
    let edges = read_magic_edges(input).await?;
    if edges.matches(PARQUET_MAGIC) {
        return Ok(DetectedFormat::Parquet);
    }

    if edges.matches(ARROW_MAGIC)
        && read_arrow_file_metadata(input, false)
            .await
            .is_ok_and(|metadata| metadata.is_some())
    {
        return Ok(DetectedFormat::Arrow {
            variant: "file".to_string(),
        });
    }

    if edges.matches(VORTEX_MAGIC) {
        return Ok(DetectedFormat::Vortex);
    }

    if read_stream_metadata(input, false).await.is_ok() {
        return Ok(DetectedFormat::Arrow {
            variant: "stream".to_string(),
        });
    }

    Ok(DetectedFormat::Unknown)
}
