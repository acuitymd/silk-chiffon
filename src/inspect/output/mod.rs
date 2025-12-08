pub mod text;

use crate::inspect::FileInfo;
use anyhow::Result;

pub trait OutputFormatter {
    fn format(&self, info: &FileInfo) -> Result<String>;
}
