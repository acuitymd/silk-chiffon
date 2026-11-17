use arrow::{
    buffer::Buffer,
    ipc::{Message, MetadataVersion, root_as_message},
};

use super::{DecodeError, DecodeErrorKind, DecodeLimit};

const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

pub(crate) struct EncapsulatedMessage<'a> {
    pub(crate) message: Message<'a>,
    pub(crate) body: &'a [u8],
}

pub(crate) fn parse_encapsulated(
    buffer: &Buffer,
    limit: DecodeLimit,
) -> Result<EncapsulatedMessage<'_>, DecodeError> {
    let bytes = buffer.as_slice();
    if bytes.len() > limit.get() {
        return Err(DecodeError::new(DecodeErrorKind::SerializedPayloadLimit));
    }
    if bytes.len() < 4 {
        return Err(DecodeError::new(DecodeErrorKind::InvalidIpcFraming));
    }

    let (prefix_length, length_bytes) = if bytes[..4] == CONTINUATION_MARKER {
        if bytes.len() < 8 {
            return Err(DecodeError::new(DecodeErrorKind::InvalidIpcFraming));
        }
        (8_usize, &bytes[4..8])
    } else {
        (4_usize, &bytes[..4])
    };
    let metadata_length = i32::from_le_bytes(
        length_bytes
            .try_into()
            .map_err(|_| DecodeError::new(DecodeErrorKind::InvalidIpcFraming))?,
    );
    let metadata_length = usize::try_from(metadata_length)
        .map_err(|_| DecodeError::new(DecodeErrorKind::InvalidIpcFraming))?;
    if metadata_length == 0 {
        return Err(DecodeError::new(DecodeErrorKind::InvalidIpcFraming));
    }
    let metadata_end = prefix_length
        .checked_add(metadata_length)
        .ok_or_else(|| DecodeError::new(DecodeErrorKind::InvalidIpcFraming))?;
    if metadata_end > bytes.len() || metadata_end % 8 != 0 {
        return Err(DecodeError::new(DecodeErrorKind::InvalidIpcFraming));
    }

    let message = root_as_message(&bytes[prefix_length..metadata_end])
        .map_err(|_| DecodeError::new(DecodeErrorKind::InvalidFlatbuffer))?;
    if !matches!(message.version(), MetadataVersion::V4 | MetadataVersion::V5) {
        return Err(DecodeError::new(DecodeErrorKind::InvalidFlatbuffer));
    }
    let body_length = usize::try_from(message.bodyLength())
        .map_err(|_| DecodeError::new(DecodeErrorKind::InvalidIpcFraming))?;
    if body_length % 8 != 0 {
        return Err(DecodeError::new(DecodeErrorKind::InvalidIpcFraming));
    }
    let message_end = metadata_end
        .checked_add(body_length)
        .ok_or_else(|| DecodeError::new(DecodeErrorKind::InvalidIpcFraming))?;
    if message_end != bytes.len() {
        return Err(DecodeError::new(DecodeErrorKind::InvalidIpcFraming));
    }

    Ok(EncapsulatedMessage {
        message,
        body: &bytes[metadata_end..message_end],
    })
}
