//! Small object-store ranges used for format detection.

use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::ObjectStoreExt;

use crate::storage::InputObject;

const PREFIX_LENGTH: u64 = 8;
const SUFFIX_LENGTH: u64 = 10;

pub(crate) struct MagicEdges {
    pub(crate) prefix: Bytes,
    pub(crate) suffix: Bytes,
    pub(crate) size: u64,
}

impl MagicEdges {
    pub(crate) fn matches(&self, magic: &[u8]) -> bool {
        self.size >= (magic.len() * 2) as u64
            && self.prefix.starts_with(magic)
            && self.suffix.ends_with(magic)
    }
}

pub(crate) async fn read_magic_edges(input: &InputObject) -> Result<MagicEdges> {
    let size = input.metadata().size;
    if size == 0 {
        return Ok(MagicEdges {
            prefix: Bytes::new(),
            suffix: Bytes::new(),
            size,
        });
    }

    let prefix_end = size.min(PREFIX_LENGTH);
    let suffix_start = size.saturating_sub(SUFFIX_LENGTH);
    let store = input.location().store().object_store();
    let path = input.location().path();
    let (prefix, suffix) = tokio::try_join!(
        store.get_range(path, 0..prefix_end),
        store.get_range(path, suffix_start..size)
    )
    .with_context(|| {
        format!(
            "could not read format markers from '{}'",
            input.location().display()
        )
    })?;

    Ok(MagicEdges {
        prefix,
        suffix,
        size,
    })
}
