//! Bounded Vortex detection for resolved input objects.
//!
//! Both format markers are required. Once the leading marker identifies a
//! Vortex object, a missing trailer is malformed input rather than a mismatch
//! that another detector may claim.

use anyhow::anyhow;
use object_store::ObjectStoreExt;
use silk_chiffon_core::{FormatFuture, FormatInputVariant, InputDetection};
use silk_chiffon_storage::InputObject;
use vortex::file::MAGIC_BYTES;

pub(crate) fn detect(object: &InputObject) -> FormatFuture<'_, InputDetection> {
    Box::pin(async move {
        let size = object.metadata().size;
        if size < MAGIC_BYTES.len() as u64 {
            return Ok(InputDetection::Mismatch);
        }

        let handle = object.input_handle();
        let leading = handle
            .object_store()
            .get_range(handle.object_path(), 0..MAGIC_BYTES.len() as u64)
            .await?;
        if leading.as_ref() != MAGIC_BYTES {
            return Ok(InputDetection::Mismatch);
        }
        if size < 2 * MAGIC_BYTES.len() as u64 {
            return Ok(InputDetection::Malformed(anyhow!(
                "Vortex input is too short to contain its trailing magic marker"
            )));
        }

        let trailing = handle
            .object_store()
            .get_range(handle.object_path(), size - MAGIC_BYTES.len() as u64..size)
            .await?;
        Ok(if trailing.as_ref() == MAGIC_BYTES {
            InputDetection::Match(FormatInputVariant::named("file", "file"))
        } else {
            InputDetection::Malformed(anyhow!("Vortex input is missing its trailing magic marker"))
        })
    })
}

#[cfg(test)]
mod tests {
    use object_store::GetRange;

    use super::*;
    use crate::test_support::{guard, object_with, store, vortex_bytes};

    #[tokio::test]
    async fn complete_file_matches_with_two_bounded_marker_reads() {
        let _guard = guard().await;
        let bytes = vortex_bytes(Vec::new()).await;
        let size = bytes.len() as u64;
        let object = object_with(bytes).await;
        store().reset_observation();

        let detection = detect(&object).await.unwrap();

        let InputDetection::Match(variant) = detection else {
            panic!("expected a Vortex match");
        };
        assert_eq!(variant.name(), Some("file"));
        assert_eq!(variant.display_name(), Some("file"));
        let ranges = store().ranges();
        assert_eq!(
            ranges,
            [
                GetRange::Bounded(0..MAGIC_BYTES.len() as u64),
                GetRange::Bounded(size - MAGIC_BYTES.len() as u64..size),
            ]
        );
    }

    #[tokio::test]
    async fn leading_marker_distinguishes_malformed_from_mismatch() {
        let _guard = guard().await;
        for (bytes, expected_message) in [
            (b"VTXF".as_slice(), "too short"),
            (b"VTXFbroken".as_slice(), "trailing magic marker"),
        ] {
            let object = object_with(bytes).await;
            let InputDetection::Malformed(error) = detect(&object).await.unwrap() else {
                panic!("leading marker should make truncated input malformed");
            };
            assert!(error.to_string().contains(expected_message), "{error:#}");
        }

        for bytes in [b"VTX".as_slice(), b"PAR1garbagePAR1".as_slice()] {
            let object = object_with(bytes).await;
            assert!(matches!(
                detect(&object).await.unwrap(),
                InputDetection::Mismatch
            ));
        }
    }

    #[tokio::test]
    async fn storage_failures_are_not_reclassified_as_format_results() {
        let _guard = guard().await;
        let object = object_with(vortex_bytes(Vec::new()).await).await;
        store().reset_observation();
        store().set_fail_reads(true);

        let error = detect(&object).await.unwrap_err();

        assert!(
            format!("{error:#}").contains("controlled object-store read failure"),
            "{error:#}"
        );
        store().reset_observation();
    }
}
