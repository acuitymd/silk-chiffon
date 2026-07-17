//! Format detection for data files.

use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    inspection::{
        magic::read_magic_edges,
        style::{dim, value},
    },
    sources::arrow::{read_arrow_file_metadata, read_stream_metadata},
    storage::InputObject,
};

const PARQUET_MAGIC: &[u8] = b"PAR1";
const ARROW_MAGIC: &[u8] = b"ARROW1";
const VORTEX_MAGIC: &[u8] = b"VTXF";

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(tag = "format", rename_all = "lowercase")]
pub enum DetectedFormat {
    Parquet,
    #[serde(rename = "arrow")]
    Arrow {
        variant: String,
    },
    Vortex,
    Unknown,
}

impl std::fmt::Display for DetectedFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DetectedFormat::Parquet => write!(f, "{}", value("Parquet")),
            DetectedFormat::Arrow { variant } => write!(
                f,
                "{} {}",
                value("Arrow IPC"),
                dim(format!("({})", variant))
            ),
            DetectedFormat::Vortex => write!(f, "{} {}", value("Vortex"), dim("(file)")),
            DetectedFormat::Unknown => write!(f, "{}", dim("Unknown")),
        }
    }
}

impl DetectedFormat {
    pub fn to_json(&self) -> Value {
        json!(self)
    }
}

/// Detect the format of an object from small ranges.
///
/// Tries each format in order:
/// 1. Parquet (PAR1 magic at start and end)
/// 2. Arrow IPC file
/// 3. Vortex (VTXF magic at start and end)
/// 4. Arrow IPC stream
pub async fn detect_format(input: &InputObject) -> Result<DetectedFormat> {
    let edges = read_magic_edges(input).await?;
    if edges.matches(PARQUET_MAGIC) {
        return Ok(DetectedFormat::Parquet);
    }

    if edges.matches(ARROW_MAGIC)
        && read_arrow_file_metadata(input, false)
            .await
            .is_ok_and(|metadata| metadata.is_some())
    {
        return Ok(DetectedFormat::Arrow {
            variant: "file".to_string(),
        });
    }

    if edges.matches(VORTEX_MAGIC) {
        return Ok(DetectedFormat::Vortex);
    }

    if read_stream_metadata(input, false).await.is_ok() {
        return Ok(DetectedFormat::Arrow {
            variant: "stream".to_string(),
        });
    }

    Ok(DetectedFormat::Unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, atomic::Ordering};

    use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

    use crate::{
        storage::{StorageConfig, StorageContext},
        utils::test_helpers::object_store::CountingStore,
    };

    async fn input(
        bytes: Vec<u8>,
        key: &str,
    ) -> (
        crate::storage::InputObject,
        Arc<crate::utils::test_helpers::object_store::RequestLog>,
    ) {
        let inner = InMemory::new();
        inner
            .put(&Path::from(key), PutPayload::from(bytes))
            .await
            .unwrap();
        let (store, requests) = CountingStore::new(inner);
        let store: Arc<dyn ObjectStore> = Arc::new(store);
        let storage = StorageContext::with_gcs_store(StorageConfig::default(), store);
        let input = storage
            .resolve_input(&format!("gs://inspect-tests/{key}"))
            .await
            .unwrap();
        (input, requests)
    }

    #[tokio::test]
    async fn identifies_object_store_formats_with_bounded_ranges() {
        let fixtures = [
            (
                std::fs::read("tests/files/people.parquet").unwrap(),
                "people.parquet",
                DetectedFormat::Parquet,
                2,
            ),
            (
                std::fs::read("tests/files/people.file.arrow").unwrap(),
                "people.file.arrow",
                DetectedFormat::Arrow {
                    variant: "file".to_string(),
                },
                4,
            ),
            (
                std::fs::read("tests/files/people.stream.arrow").unwrap(),
                "people.stream.arrow",
                DetectedFormat::Arrow {
                    variant: "stream".to_string(),
                },
                5,
            ),
            (
                b"VTXFpayloadVTXF".to_vec(),
                "people.vortex",
                DetectedFormat::Vortex,
                2,
            ),
        ];

        for (bytes, key, expected, expected_ranges) in fixtures {
            let size = bytes.len() as u64;
            let (input, requests) = input(bytes, key).await;

            assert_eq!(detect_format(&input).await.unwrap(), expected);
            assert_eq!(requests.heads.load(Ordering::SeqCst), 1);
            assert_eq!(requests.full_gets.load(Ordering::SeqCst), 0);
            let ranges = requests.ranges.lock().unwrap();
            assert_eq!(ranges.len(), expected_ranges);
            assert!(ranges.iter().all(|range| {
                range
                    .as_range(size)
                    .is_ok_and(|range| range.end - range.start < size)
            }));
        }
    }

    #[tokio::test]
    async fn truncated_objects_are_unknown() {
        for bytes in [Vec::new(), b"PAR".to_vec(), b"VTX".to_vec()] {
            let (input, _) = input(bytes, "truncated.data").await;
            assert_eq!(
                detect_format(&input).await.unwrap(),
                DetectedFormat::Unknown
            );
        }
    }
}
