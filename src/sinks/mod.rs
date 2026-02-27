//! Output sinks for writing transformed data to Arrow, Parquet, and Vortex formats.

pub mod arrow;
pub mod data_sink;
pub mod parquet;
pub mod vortex;

/// Default record batch size for sinks. 122,880 = 120 × 1024, chosen to align
/// with parquet row group boundaries while keeping batches large enough for
/// efficient columnar processing.
pub const DEFAULT_RECORD_BATCH_SIZE: usize = 122_880;
