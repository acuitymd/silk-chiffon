//! File inspection utilities for various columnar data formats.

pub mod inspectable;
pub mod parquet;
pub mod style;
pub mod vortex;

pub mod magic;
pub use inspectable::Inspectable;
