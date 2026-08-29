//! Object-store copy-commit compatibility for object_store 0.13.2.
//!
//! The Google Cloud Storage client and its Application Default Credentials
//! adapter arrive with the remote backend in a later change. This module
//! carries only the store-agnostic copy helper that the output commit uses, so
//! the commit machinery can be exercised against injected stores before the
//! remote client exists.

use object_store::{CopyMode, CopyOptions, ObjectStore, path::Path};

/// Copies an object with the intended conditional mode on object_store 0.13.2.
///
/// GCS 0.13.2 passes the opposite `if_not_exists` value to its client:
/// <https://github.com/apache/arrow-rs-object-store/blob/v0.13.2/src/gcp/mod.rs#L218-L228>.
/// Version 0.14 passes `CopyMode` directly and fixes the inversion:
/// <https://github.com/apache/arrow-rs-object-store/blob/v0.14.0/src/gcp/mod.rs#L221-L230>.
pub(crate) async fn commit_gcs_013(
    store: &dyn ObjectStore,
    from: &Path,
    to: &Path,
    intended_mode: CopyMode,
) -> object_store::Result<()> {
    store
        .copy_opts(
            from,
            to,
            CopyOptions::new().with_mode(gcs_copy_mode_013(intended_mode)),
        )
        .await
}

pub(crate) const fn gcs_copy_mode_013(intended_mode: CopyMode) -> CopyMode {
    match intended_mode {
        CopyMode::Create => CopyMode::Overwrite,
        CopyMode::Overwrite => CopyMode::Create,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_store_013_gcs_copy_mode_is_inverted_at_compatibility_boundary() {
        assert_eq!(gcs_copy_mode_013(CopyMode::Create), CopyMode::Overwrite);
        assert_eq!(gcs_copy_mode_013(CopyMode::Overwrite), CopyMode::Create);
    }
}
