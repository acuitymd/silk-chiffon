use anyhow::anyhow;
use arrow::{buffer::Buffer, ipc::reader::StreamDecoder};
use object_store::ObjectStoreExt;
use silk_chiffon_core::{FormatFuture, InputDetection};
use silk_chiffon_storage::InputObject;

use crate::variant::IpcVariant;

const ARROW_MAGIC: &[u8] = b"ARROW1";
const MAX_DETECTION_READ: u64 = 1024 * 1024;

pub(crate) fn detect(object: &InputObject) -> FormatFuture<'_, InputDetection> {
    Box::pin(async move {
        let handle = object.input_handle();
        let size = object.metadata().size;
        if size >= 12 {
            let ranges = [0..6, size - 6..size];
            let magic = handle
                .object_store()
                .get_ranges(handle.object_path(), &ranges)
                .await?;
            let starts = magic[0].as_ref() == ARROW_MAGIC;
            let ends = magic[1].as_ref() == ARROW_MAGIC;
            if starts && ends {
                return Ok(InputDetection::Match(
                    IpcVariant::File.format_input_variant(),
                ));
            }
            if starts {
                return Ok(InputDetection::Malformed(anyhow!(
                    "Arrow IPC file is missing its trailing magic marker"
                )));
            }
        }

        let prefix_len = size.min(8);
        if prefix_len < 4 {
            return Ok(InputDetection::Mismatch);
        }
        let prefix = handle
            .object_store()
            .get_range(handle.object_path(), 0..prefix_len)
            .await?;
        let first = u32::from_le_bytes(prefix[..4].try_into().expect("four bytes were read"));
        let (header_len, message_len, recognized) = if first == u32::MAX {
            if prefix.len() < 8 {
                return Ok(InputDetection::Malformed(anyhow!(
                    "Arrow IPC continuation marker is missing its message length"
                )));
            }
            (
                8_u64,
                u64::from(u32::from_le_bytes(prefix[4..8].try_into().unwrap())),
                true,
            )
        } else {
            (4_u64, u64::from(first), false)
        };
        if message_len == 0 || header_len + message_len > size {
            return Ok(if recognized {
                InputDetection::Malformed(anyhow!("Arrow IPC schema message is truncated"))
            } else {
                InputDetection::Mismatch
            });
        }
        if message_len > MAX_DETECTION_READ && recognized {
            return Ok(InputDetection::Match(
                IpcVariant::Stream.format_input_variant(),
            ));
        }
        if message_len > MAX_DETECTION_READ {
            return Ok(InputDetection::Mismatch);
        }
        let bytes = handle
            .object_store()
            .get_range(
                handle.object_path(),
                0..(header_len + message_len + 1).min(size),
            )
            .await?;
        let mut decoder = StreamDecoder::new();
        let mut buffer = Buffer::from(bytes);
        match decoder.decode(&mut buffer) {
            Ok(_) if decoder.schema().is_some() => Ok(InputDetection::Match(
                IpcVariant::Stream.format_input_variant(),
            )),
            Ok(_) if recognized => Ok(InputDetection::Malformed(anyhow!(
                "Arrow IPC stream did not begin with a schema message"
            ))),
            Err(error) if recognized => Ok(InputDetection::Malformed(error.into())),
            Ok(_) | Err(_) => Ok(InputDetection::Mismatch),
        }
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::{
        datatypes::{DataType, Field, Schema},
        ipc::writer::StreamWriter,
    };
    use silk_chiffon_core::InputDetection;
    use silk_chiffon_storage::{LocationInput, local};
    use tempfile::tempdir;
    use url::Url;

    use super::*;

    async fn detect_bytes(bytes: &[u8]) -> InputDetection {
        let directory = tempdir().unwrap();
        let path = directory.path().join("input");
        std::fs::write(&path, bytes).unwrap();
        let url = Url::from_file_path(path).unwrap();
        let location = LocationInput::parse(url.as_str()).unwrap();
        let object = local::session()
            .unwrap()
            .lookup_input(&location)
            .await
            .unwrap();
        detect(&object).await.unwrap()
    }

    #[tokio::test]
    async fn trailing_file_magic_does_not_claim_unrecognized_input() {
        let mut bytes = b"not an Arrow input".to_vec();
        bytes.extend_from_slice(ARROW_MAGIC);

        assert!(matches!(
            detect_bytes(&bytes).await,
            InputDetection::Mismatch
        ));
    }

    #[tokio::test]
    async fn stream_schema_takes_precedence_over_incidental_trailing_file_magic() {
        let schema = Schema::new(vec![Field::new("value", DataType::Utf8, true)]);
        let mut bytes = Cursor::new(Vec::new());
        StreamWriter::try_new(&mut bytes, &schema)
            .unwrap()
            .finish()
            .unwrap();
        let mut bytes = bytes.into_inner();
        bytes.extend_from_slice(ARROW_MAGIC);

        let InputDetection::Match(variant) = detect_bytes(&bytes).await else {
            panic!("Arrow stream schema was not detected");
        };
        assert_eq!(variant.name(), Some("stream"));
    }

    #[tokio::test]
    async fn leading_file_magic_without_a_trailer_remains_malformed() {
        let mut bytes = ARROW_MAGIC.to_vec();
        bytes.extend_from_slice(b"not a complete Arrow file");

        assert!(matches!(
            detect_bytes(&bytes).await,
            InputDetection::Malformed(_)
        ));
    }
}
