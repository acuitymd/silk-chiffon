use std::io::Read;

use arrow::ipc::{BodyCompressionMethod, CompressionType, RecordBatch};

use super::{DecodeError, DecodeErrorKind, DecodeLimit};

pub(crate) fn decompress_raw_lz4(
    input: &[u8],
    output_length: usize,
    limit: DecodeLimit,
) -> Result<Vec<u8>, DecodeError> {
    if output_length > limit.get() {
        return Err(DecodeError::new(DecodeErrorKind::DecodedPayloadLimit));
    }
    let mut output = vec![0_u8; output_length];
    let written = lz4_flex::block::decompress_into(input, &mut output)
        .map_err(|_| DecodeError::new(DecodeErrorKind::RawLz4Decompression))?;
    if written != output_length {
        return Err(DecodeError::new(DecodeErrorKind::RawLz4Decompression));
    }
    Ok(output)
}

pub(crate) fn validate_native_buffers(
    batch: RecordBatch<'_>,
    body: &[u8],
    limit: DecodeLimit,
) -> Result<usize, DecodeError> {
    let compression = batch.compression();
    let mut decoded_bytes = 0_usize;
    let buffers = batch
        .buffers()
        .ok_or_else(|| DecodeError::new(DecodeErrorKind::InvalidRecordBatch))?;
    for buffer in buffers {
        let offset = usize::try_from(buffer.offset())
            .map_err(|_| DecodeError::new(DecodeErrorKind::InvalidRecordBatch))?;
        let length = usize::try_from(buffer.length())
            .map_err(|_| DecodeError::new(DecodeErrorKind::InvalidRecordBatch))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| DecodeError::new(DecodeErrorKind::InvalidRecordBatch))?;
        let encoded = body
            .get(offset..end)
            .ok_or_else(|| DecodeError::new(DecodeErrorKind::InvalidRecordBatch))?;

        let decoded_length = match compression {
            None => length,
            Some(compression) => {
                if compression.method() != BodyCompressionMethod::BUFFER {
                    return Err(DecodeError::new(DecodeErrorKind::NativeCompression));
                }
                validate_native_buffer(compression.codec(), encoded, limit)?
            }
        };
        decoded_bytes = decoded_bytes
            .checked_add(decoded_length)
            .ok_or_else(|| DecodeError::new(DecodeErrorKind::DecodedPayloadLimit))?;
        if decoded_bytes > limit.get() {
            return Err(DecodeError::new(DecodeErrorKind::DecodedPayloadLimit));
        }
    }
    Ok(decoded_bytes)
}

fn validate_native_buffer(
    codec: CompressionType,
    encoded: &[u8],
    limit: DecodeLimit,
) -> Result<usize, DecodeError> {
    if encoded.is_empty() {
        return Ok(0);
    }
    let prefix = encoded
        .get(..8)
        .ok_or_else(|| DecodeError::new(DecodeErrorKind::NativeCompression))?;
    let declared = i64::from_le_bytes(
        prefix
            .try_into()
            .map_err(|_| DecodeError::new(DecodeErrorKind::NativeCompression))?,
    );
    if declared == -1 {
        return Ok(encoded.len() - 8);
    }
    let declared = usize::try_from(declared)
        .map_err(|_| DecodeError::new(DecodeErrorKind::NativeCompression))?;
    if declared > limit.get() {
        return Err(DecodeError::new(DecodeErrorKind::DecodedPayloadLimit));
    }
    let compressed = &encoded[8..];
    match codec {
        CompressionType::LZ4_FRAME => validate_lz4_frame(compressed, declared)?,
        CompressionType::ZSTD => {}
        _ => return Err(DecodeError::new(DecodeErrorKind::NativeCompression)),
    }
    Ok(declared)
}

fn validate_lz4_frame(input: &[u8], declared: usize) -> Result<(), DecodeError> {
    let cap = declared
        .checked_add(1)
        .ok_or_else(|| DecodeError::new(DecodeErrorKind::DecodedPayloadLimit))?;
    // Arrow 58.4 grows its output Vec when an LZ4 frame exceeds the prefix. This
    // bounded pass rejects a lying frame before Arrow performs the real decode.
    let mut decoder = lz4_flex::frame::FrameDecoder::new(input);
    let mut scratch = [0_u8; 64 * 1024];
    let mut output_bytes = 0_usize;
    while output_bytes < cap {
        let remaining = cap - output_bytes;
        let read_length = remaining.min(scratch.len());
        let read = decoder
            .read(&mut scratch[..read_length])
            .map_err(|_| DecodeError::new(DecodeErrorKind::NativeCompression))?;
        if read == 0 {
            break;
        }
        output_bytes += read;
    }
    if output_bytes != declared {
        return Err(DecodeError::new(DecodeErrorKind::NativeCompression));
    }
    Ok(())
}
