pub mod arrow;
pub mod data_sink;
pub mod parquet;
pub mod vortex;

use std::fmt;

use anyhow::Error;

pub(crate) fn with_cleanup_error(primary: Error, cleanup: Option<Error>) -> Error {
    match cleanup {
        Some(cleanup) => Error::new(PrimaryWithCleanup { primary, cleanup }),
        None => primary,
    }
}

#[derive(Debug)]
struct PrimaryWithCleanup {
    primary: Error,
    cleanup: Error,
}

impl fmt::Display for PrimaryWithCleanup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; cleanup also failed: {:#}",
            self.primary, self.cleanup
        )
    }
}

impl std::error::Error for PrimaryWithCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.primary.source()
    }
}
