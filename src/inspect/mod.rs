pub mod arrow_ipc;
pub mod output;

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use std::path::Path;

/// Trait for inspecting file metadata without reading actual data.
pub trait FileInspector: Send + Sync {
    fn inspect(&self, path: &Path) -> Result<FileInfo>;
}

/// Format-agnostic file information.
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: std::path::PathBuf,
    pub format: FileFormat,
    pub schema: SchemaRef,
    pub num_rows: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    ArrowFile,
    ArrowStream,
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormat::ArrowFile => write!(f, "Arrow IPC (file)"),
            FileFormat::ArrowStream => write!(f, "Arrow IPC (stream)"),
        }
    }
}
