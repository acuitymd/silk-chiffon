//! File inspection utilities for various columnar data formats.

pub mod arrow;
pub mod identify;
pub mod inspectable;
pub mod parquet;
pub mod style;
pub mod vortex;

pub use identify::{DetectedFormat, detect_format};
pub use inspectable::Inspectable;
