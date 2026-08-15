//! Checked decoding for BigQuery Storage Read Arrow responses.

mod compression;
mod payload;
mod schema;
mod validation;

use std::{error::Error, fmt, num::NonZeroUsize};

pub(crate) use payload::{DecodedBatch, RowPayloadCodec, SerializedRows};
pub(crate) use schema::SessionSchema;

/// A hard bound applied before allocating decoded row payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DecodeLimit(NonZeroUsize);

impl DecodeLimit {
    pub const fn new(bytes: usize) -> Option<Self> {
        match NonZeroUsize::new(bytes) {
            Some(bytes) => Some(Self(bytes)),
            None => None,
        }
    }

    pub const fn get(self) -> usize {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DecodeErrorKind {
    SerializedPayloadLimit,
    DecodedPayloadLimit,
    InvalidIpcFraming,
    InvalidFlatbuffer,
    WrongSchemaMessageHeader,
    WrongRecordBatchMessageHeader,
    DictionaryBatchUnsupported,
    InvalidArrowSchema,
    RepeatedSchemaMismatch,
    AvroSchema,
    MissingRows,
    AvroRows,
    NegativeResponseRowCount,
    DeprecatedRowCountMismatch,
    DecodedRowCountMismatch,
    PositiveSizeWithoutRows,
    CompressionNotRequested,
    UnknownCompressionState,
    RawLz4Decompression,
    NativeCompression,
    InvalidRecordBatch,
    BatchSchemaMismatch,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct DecodeError {
    kind: DecodeErrorKind,
}

#[cfg(test)]
mod tests;

impl DecodeError {
    pub(crate) const fn new(kind: DecodeErrorKind) -> Self {
        Self { kind }
    }

    #[cfg(test)]
    pub const fn kind(&self) -> DecodeErrorKind {
        self.kind
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            DecodeErrorKind::SerializedPayloadLimit => {
                "serialized Arrow payload exceeds the decode limit"
            }
            DecodeErrorKind::DecodedPayloadLimit => {
                "decoded Arrow payload exceeds the decode limit"
            }
            DecodeErrorKind::InvalidIpcFraming => "invalid standalone Arrow IPC framing",
            DecodeErrorKind::InvalidFlatbuffer => "invalid Arrow IPC metadata",
            DecodeErrorKind::WrongSchemaMessageHeader => {
                "session schema payload is not an Arrow schema message"
            }
            DecodeErrorKind::WrongRecordBatchMessageHeader => {
                "row payload is not an Arrow record batch message"
            }
            DecodeErrorKind::DictionaryBatchUnsupported => {
                "Arrow dictionary batches are unsupported"
            }
            DecodeErrorKind::InvalidArrowSchema => "invalid or unsupported Arrow session schema",
            DecodeErrorKind::RepeatedSchemaMismatch => {
                "response Arrow schema differs from the session schema"
            }
            DecodeErrorKind::AvroSchema => "Avro response schema is unsupported",
            DecodeErrorKind::MissingRows => "response contains no Arrow rows",
            DecodeErrorKind::AvroRows => "Avro response rows are unsupported",
            DecodeErrorKind::NegativeResponseRowCount => "response contains a negative row count",
            DecodeErrorKind::DeprecatedRowCountMismatch => {
                "deprecated batch row count differs from the response row count"
            }
            DecodeErrorKind::DecodedRowCountMismatch => {
                "decoded Arrow row count differs from the response row count"
            }
            DecodeErrorKind::PositiveSizeWithoutRows => {
                "compressed response size is set without row data"
            }
            DecodeErrorKind::CompressionNotRequested => {
                "compressed row payload was returned without raw-LZ4 being requested"
            }
            DecodeErrorKind::UnknownCompressionState => {
                "response contains an unknown compression state"
            }
            DecodeErrorKind::RawLz4Decompression => "raw-LZ4 row payload decompression failed",
            DecodeErrorKind::NativeCompression => {
                "invalid or oversized native Arrow compression metadata"
            }
            DecodeErrorKind::InvalidRecordBatch => "Arrow record batch decoding failed",
            DecodeErrorKind::BatchSchemaMismatch => {
                "decoded Arrow batch differs from the session schema"
            }
        })
    }
}

impl fmt::Debug for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodeError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl Error for DecodeError {}
