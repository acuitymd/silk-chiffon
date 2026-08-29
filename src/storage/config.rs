//! Validated CLI tuning for object-store requests and multipart uploads.

use clap::Args;

use crate::{parse_at_least_one, parse_nonzero_byte_size};

const DEFAULT_MAX_REQUESTS: usize = 64;
const DEFAULT_UPLOAD_PART_SIZE: usize = 10 * 1024 * 1024;
const DEFAULT_UPLOAD_CONCURRENCY: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageConfig {
    pub max_requests: usize,
    pub upload_part_size: usize,
    pub upload_concurrency: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_requests: DEFAULT_MAX_REQUESTS,
            upload_part_size: DEFAULT_UPLOAD_PART_SIZE,
            upload_concurrency: DEFAULT_UPLOAD_CONCURRENCY,
        }
    }
}

#[derive(Args, Clone, Debug)]
pub struct StorageArgs {
    /// Maximum concurrent requests to each remote object store.
    #[arg(
        long = "object-store-max-requests",
        global = true,
        default_value_t = DEFAULT_MAX_REQUESTS,
        value_parser = parse_at_least_one,
        help_heading = "Object Store"
    )]
    max_requests: usize,

    /// Part size for multipart object uploads.
    #[arg(
        long = "object-store-upload-part-size",
        global = true,
        default_value = "10MiB",
        value_parser = parse_nonzero_byte_size,
        help_heading = "Object Store"
    )]
    upload_part_size: usize,

    /// Maximum concurrent upload requests for each output.
    #[arg(
        long = "object-store-upload-concurrency",
        global = true,
        default_value_t = DEFAULT_UPLOAD_CONCURRENCY,
        value_parser = parse_at_least_one,
        help_heading = "Object Store"
    )]
    upload_concurrency: usize,
}

impl StorageArgs {
    #[must_use]
    pub fn resolve(&self) -> StorageConfig {
        StorageConfig {
            max_requests: self.max_requests,
            upload_part_size: self.upload_part_size,
            upload_concurrency: self.upload_concurrency,
        }
    }
}
