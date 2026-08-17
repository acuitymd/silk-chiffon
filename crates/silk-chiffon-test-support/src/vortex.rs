//! Vortex fixtures written directly through the upstream codec.

use std::sync::Arc;

use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use futures::stream;
use vortex::{
    VortexSessionDefault,
    array::{ArrayRef, stream::ArrayStreamAdapter},
    arrow::{FromArrowArray, FromArrowType},
    dtype::DType,
    file::WriteOptionsSessionExt,
    session::VortexSession,
};

/// Encodes batches as one Vortex file without using Silk Chiffon's sink.
pub async fn write_batches(schema: &SchemaRef, batches: Vec<RecordBatch>) -> Result<Vec<u8>> {
    let arrays = batches
        .into_iter()
        .map(|batch| ArrayRef::from_arrow(batch, false))
        .collect::<Result<Vec<_>, _>>()?;
    let stream = ArrayStreamAdapter::new(
        DType::from_arrow(Arc::clone(schema)),
        stream::iter(arrays.into_iter().map(Ok)),
    );
    let mut bytes = Vec::new();
    VortexSession::default()
        .write_options()
        .write(&mut bytes, stream)
        .await?;
    Ok(bytes)
}
