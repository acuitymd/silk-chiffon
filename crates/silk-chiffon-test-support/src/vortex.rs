//! Vortex fixtures written directly through the upstream codec.

use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use futures::stream;
use vortex::{
    VortexSessionDefault, array::stream::ArrayStreamAdapter, arrow::ArrowSessionExt,
    file::WriteOptionsSessionExt, session::VortexSession,
};

/// Encodes batches as one Vortex file without using Silk Chiffon's sink.
pub async fn encode_batches(schema: &SchemaRef, batches: Vec<RecordBatch>) -> Result<Vec<u8>> {
    let session = VortexSession::default();
    let (dtype, arrays) = {
        let arrow = session.arrow();
        let arrays = batches
            .into_iter()
            .map(|batch| arrow.from_arrow_record_batch(batch, schema))
            .collect::<Result<Vec<_>, _>>()?;
        (arrow.from_arrow_schema(schema)?, arrays)
    };
    let stream = ArrayStreamAdapter::new(dtype, stream::iter(arrays.into_iter().map(Ok)));
    let mut bytes = Vec::new();
    session.write_options().write(&mut bytes, stream).await?;
    Ok(bytes)
}
