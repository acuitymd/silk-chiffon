//! Bounded Parquet detection for resolved input objects.
//!
//! Both magic markers are required. A leading marker without the trailing
//! marker is diagnosed as a malformed Parquet object instead of allowing a
//! later format detector to claim it.

use anyhow::anyhow;
use silk_chiffon_core::{FormatFuture, FormatInputVariant, InputDetection};
use silk_chiffon_storage::InputObject;

pub(crate) fn detect(object: &InputObject) -> FormatFuture<'_, InputDetection> {
    Box::pin(async move {
        const MAGIC: &[u8] = b"PAR1";
        if object.metadata().size < 8 {
            return Ok(InputDetection::Mismatch);
        }
        let handle = object.input_handle();
        let size = object.metadata().size;
        let magic = handle
            .object_store()
            .get_ranges(handle.object_path(), &[0..4, size - 4..size])
            .await?;
        let starts = magic[0].as_ref() == MAGIC;
        let ends = magic[1].as_ref() == MAGIC;
        Ok(match (starts, ends) {
            (true, true) => InputDetection::Match(FormatInputVariant::new()),
            (true, false) => InputDetection::Malformed(anyhow!(
                "Parquet input is missing its trailing magic marker"
            )),
            _ => InputDetection::Mismatch,
        })
    })
}
