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
/// Tries each format in order:
/// 1. Parquet (PAR1 magic at start and end)
/// 2. Arrow IPC (tries opening as file then stream)
/// 3. Vortex (VTXF magic at start)
pub fn detect_format(path: &Path) -> Result<DetectedFormat> {
    if ParquetInspector::is_format(path)? {
        return Ok(DetectedFormat::Parquet);
    }

    if let Ok(variant) = ArrowInspector::detect_variant(path) {
        return Ok(DetectedFormat::Arrow {
            variant: variant.to_string(),
        });
    }

    if VortexInspector::is_format(path)? {
        return Ok(DetectedFormat::Vortex);
    }

    Ok(DetectedFormat::Unknown)
}
