//! Format detection for data files.

use std::path::Path;

use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};

use super::{
    arrow::ArrowInspector,
    inspectable::Inspectable,
    parquet::ParquetInspector,
    style::{dim, value},
    vortex::VortexInspector,
};

#[derive(Debug, Clone, Serialize)]
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

/// Detect the format of a data file.
///
/// Tries each format in order of most specific magic bytes to least:
/// 1. Parquet (PAR1 magic)
/// 2. Arrow file (ARROW1 magic)
/// 3. Vortex file (VTXF magic)
/// 4. Arrow stream (try opening)
pub fn detect_format(path: &Path) -> Result<DetectedFormat> {
    if ParquetInspector::try_open(path)?.is_some() {
        return Ok(DetectedFormat::Parquet);
    }

    if let Some(inspector) = ArrowInspector::try_open(path)? {
        let variant = inspector.variant().to_string();
        return Ok(DetectedFormat::Arrow { variant });
    }

    if VortexInspector::try_open(path)?.is_some() {
        return Ok(DetectedFormat::Vortex);
    }

    Ok(DetectedFormat::Unknown)
}
