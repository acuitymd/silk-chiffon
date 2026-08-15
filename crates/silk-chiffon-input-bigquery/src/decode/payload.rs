use std::{collections::HashMap, fmt, sync::Arc, time::Duration};

use arrow::{
    array::RecordBatch,
    buffer::Buffer,
    ipc::{MessageHeader, reader::read_record_batch},
};
use arrow_buffer::TrackingMemoryPool;

use crate::args::ResponseCompression;
use crate::proto::bigquery_storage::{ReadRowsResponse, read_rows_response};

use super::{
    DecodeError, DecodeErrorKind, DecodeLimit,
    compression::{decompress_raw_lz4, validate_native_buffers},
    schema::SessionSchema,
    validation::parse_encapsulated,
};

/// Compression applied by the Storage Read response envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RowPayloadCodec {
    None,
    RawLz4,
}

impl From<ResponseCompression> for RowPayloadCodec {
    fn from(value: ResponseCompression) -> Self {
        match value {
            ResponseCompression::None => Self::None,
            ResponseCompression::Lz4 => Self::RawLz4,
        }
    }
}

/// Owned Arrow row bytes plus validated protobuf row metadata.
pub(crate) struct SerializedRows {
    payload: Vec<u8>,
    response_row_count: usize,
    declared_uncompressed: Option<usize>,
    codec: RowPayloadCodec,
}

impl SerializedRows {
    pub fn from_response(
        response: ReadRowsResponse,
        canonical: &SessionSchema,
        codec: RowPayloadCodec,
        limit: DecodeLimit,
    ) -> Result<Option<Self>, DecodeError> {
        let response_row_count = usize::try_from(response.row_count)
            .map_err(|_| DecodeError::new(DecodeErrorKind::NegativeResponseRowCount))?;
        validate_repeated_schema(response.schema, canonical, limit)?;

        let declared_uncompressed = validate_compression_state(
            response.uncompressed_byte_size,
            response.rows.is_some(),
            codec,
            limit,
        )?;
        let batch = match response.rows {
            Some(read_rows_response::Rows::ArrowRecordBatch(batch)) => batch,
            Some(read_rows_response::Rows::AvroRows(_)) => {
                return Err(DecodeError::new(DecodeErrorKind::AvroRows));
            }
            None if response_row_count == 0 => return Ok(None),
            None => return Err(DecodeError::new(DecodeErrorKind::MissingRows)),
        };
        #[allow(deprecated)]
        if batch.row_count != 0 && batch.row_count != response.row_count {
            return Err(DecodeError::new(
                DecodeErrorKind::DeprecatedRowCountMismatch,
            ));
        }
        if batch.serialized_record_batch.len() > limit.get() {
            return Err(DecodeError::new(DecodeErrorKind::SerializedPayloadLimit));
        }
        Ok(Some(Self {
            payload: batch.serialized_record_batch,
            response_row_count,
            declared_uncompressed,
            codec,
        }))
    }

    pub fn decode(
        self,
        canonical: &SessionSchema,
        limit: DecodeLimit,
    ) -> Result<DecodedBatch, DecodeError> {
        self.decode_profiled(canonical, limit, |_, _| {})
    }

    pub(crate) fn memory_upper_bound(&self, limit: DecodeLimit) -> Result<usize, DecodeError> {
        let additional = if let Some(declared) = self.declared_uncompressed {
            declared
        } else {
            let ipc = Buffer::from(self.payload.as_slice());
            let envelope = parse_encapsulated(&ipc, limit)?;
            let metadata = envelope
                .message
                .header_as_record_batch()
                .ok_or_else(|| DecodeError::new(DecodeErrorKind::WrongRecordBatchMessageHeader))?;
            if metadata.compression().is_some() {
                validate_native_buffers(metadata, envelope.body, limit)?
            } else {
                0
            }
        };
        self.payload
            .len()
            .checked_add(additional)
            .ok_or_else(|| DecodeError::new(DecodeErrorKind::DecodedPayloadLimit))
    }

    pub(crate) fn decode_profiled(
        self,
        canonical: &SessionSchema,
        limit: DecodeLimit,
        mut observe: impl FnMut(DecodeStage, Duration),
    ) -> Result<DecodedBatch, DecodeError> {
        let serialized_payload = self.payload.len();
        let started = std::time::Instant::now();
        let decompressed = match self.declared_uncompressed {
            Some(length) => {
                decompress_raw_lz4(&self.payload, length, limit).map(|payload| (payload, length))
            }
            None => Ok((self.payload, 0)),
        };
        observe(DecodeStage::PayloadDecompression, started.elapsed());
        let (ipc, payload_decompressed) = decompressed?;
        let started = std::time::Instant::now();
        let decoded = decode_ipc(
            ipc,
            serialized_payload,
            payload_decompressed,
            self.response_row_count,
            canonical,
            limit,
        );
        observe(DecodeStage::ArrowIpc, started.elapsed());
        decoded
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecodeStage {
    PayloadDecompression,
    ArrowIpc,
}

fn decode_ipc(
    ipc: Vec<u8>,
    _serialized_payload: usize,
    _payload_decompressed: usize,
    response_row_count: usize,
    canonical: &SessionSchema,
    limit: DecodeLimit,
) -> Result<DecodedBatch, DecodeError> {
    let ipc = Buffer::from_vec(ipc);
    let envelope = parse_encapsulated(&ipc, limit)?;
    match envelope.message.header_type() {
        MessageHeader::DictionaryBatch => {
            return Err(DecodeError::new(
                DecodeErrorKind::DictionaryBatchUnsupported,
            ));
        }
        MessageHeader::RecordBatch => {}
        _ => {
            return Err(DecodeError::new(
                DecodeErrorKind::WrongRecordBatchMessageHeader,
            ));
        }
    }
    let batch_metadata = envelope
        .message
        .header_as_record_batch()
        .ok_or_else(|| DecodeError::new(DecodeErrorKind::InvalidFlatbuffer))?;
    validate_native_buffers(batch_metadata, envelope.body, limit)?;

    let metadata_end = ipc.len() - envelope.body.len();
    let body = ipc.slice(metadata_end);
    let batch = read_record_batch(
        &body,
        batch_metadata,
        Arc::clone(&canonical.schema),
        &HashMap::new(),
        None,
        &envelope.message.version(),
    )
    .map_err(|_| DecodeError::new(DecodeErrorKind::InvalidRecordBatch))?;
    if batch.schema().as_ref() != canonical.schema.as_ref() {
        return Err(DecodeError::new(DecodeErrorKind::BatchSchemaMismatch));
    }
    if batch.num_rows() != response_row_count {
        return Err(DecodeError::new(DecodeErrorKind::DecodedRowCountMismatch));
    }

    let pool = TrackingMemoryPool::default();
    batch.claim(&pool);
    let arrow_buffer_memory = pool.allocated();
    if arrow_buffer_memory > limit.get() {
        return Err(DecodeError::new(DecodeErrorKind::DecodedPayloadLimit));
    }
    Ok(DecodedBatch {
        batch,
        row_count: response_row_count,
        bytes: DecodedBytes {
            #[cfg(test)]
            serialized_payload: _serialized_payload,
            #[cfg(test)]
            payload_decompressed: _payload_decompressed,
            arrow_buffer_memory,
        },
    })
}

impl fmt::Debug for SerializedRows {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SerializedRows")
            .field("serialized_payload", &self.payload.len())
            .field("response_row_count", &self.response_row_count)
            .field("declared_uncompressed", &self.declared_uncompressed)
            .field("codec", &self.codec)
            .finish()
    }
}

fn validate_repeated_schema(
    repeated: Option<read_rows_response::Schema>,
    canonical: &SessionSchema,
    limit: DecodeLimit,
) -> Result<(), DecodeError> {
    match repeated {
        None => Ok(()),
        Some(read_rows_response::Schema::AvroSchema(_)) => {
            Err(DecodeError::new(DecodeErrorKind::AvroSchema))
        }
        Some(read_rows_response::Schema::ArrowSchema(schema)) => {
            let repeated = SessionSchema::from_serialized(&schema.serialized_schema, limit)
                .map_err(|_| DecodeError::new(DecodeErrorKind::RepeatedSchemaMismatch))?;
            if repeated.schema == canonical.schema {
                Ok(())
            } else {
                Err(DecodeError::new(DecodeErrorKind::RepeatedSchemaMismatch))
            }
        }
    }
}

fn validate_compression_state(
    declared: Option<i64>,
    has_rows: bool,
    codec: RowPayloadCodec,
    limit: DecodeLimit,
) -> Result<Option<usize>, DecodeError> {
    match (codec, declared) {
        (RowPayloadCodec::None, None) => Ok(None),
        (RowPayloadCodec::None, Some(_)) => {
            Err(DecodeError::new(DecodeErrorKind::CompressionNotRequested))
        }
        (RowPayloadCodec::RawLz4, None | Some(0 | -1)) => Ok(None),
        (RowPayloadCodec::RawLz4, Some(value)) if value < -1 => {
            Err(DecodeError::new(DecodeErrorKind::UnknownCompressionState))
        }
        (RowPayloadCodec::RawLz4, Some(value)) => {
            if !has_rows {
                return Err(DecodeError::new(DecodeErrorKind::PositiveSizeWithoutRows));
            }
            let value = usize::try_from(value)
                .map_err(|_| DecodeError::new(DecodeErrorKind::DecodedPayloadLimit))?;
            if value > limit.get() {
                return Err(DecodeError::new(DecodeErrorKind::DecodedPayloadLimit));
            }
            Ok(Some(value))
        }
    }
}

/// Checked byte counts used for DataFusion memory admission and plan metrics.
///
/// `arrow_buffer_memory` counts each shared Arrow buffer allocation once through
/// Arrow's memory-pool accounting. It excludes schema, array-wrapper, and
/// allocator overhead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecodedBytes {
    #[cfg(test)]
    serialized_payload: usize,
    #[cfg(test)]
    payload_decompressed: usize,
    arrow_buffer_memory: usize,
}

impl DecodedBytes {
    #[cfg(test)]
    pub const fn serialized_payload(self) -> usize {
        self.serialized_payload
    }

    #[cfg(test)]
    pub const fn payload_decompressed(self) -> usize {
        self.payload_decompressed
    }

    pub const fn arrow_buffer_memory(self) -> usize {
        self.arrow_buffer_memory
    }
}

/// One decoded record batch and its checked row and byte accounting.
pub(crate) struct DecodedBatch {
    batch: RecordBatch,
    row_count: usize,
    bytes: DecodedBytes,
}

impl DecodedBatch {
    pub const fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    pub const fn bytes(&self) -> DecodedBytes {
        self.bytes
    }
}

impl fmt::Debug for DecodedBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedBatch")
            .field("row_count", &self.row_count)
            .field("bytes", &self.bytes)
            .finish()
    }
}
