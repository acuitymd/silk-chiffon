//! Columnar format inspection over object-store ranges and streams.
//!
//! Inspectors resolve local or GCS objects through the storage context. Arrow
//! and Parquet files read metadata and exact ranges. Arrow streams decode
//! sequential chunks, and Vortex uses its object-store range reader.

pub mod arrow;
pub mod identify;
pub mod inspectable;
pub mod parquet;
pub mod style;
pub mod vortex;

pub mod magic;
pub use identify::{DetectedFormat, detect_format};
pub use inspectable::Inspectable;
