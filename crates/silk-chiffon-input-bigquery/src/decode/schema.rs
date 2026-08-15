use std::{fmt, sync::Arc};

use arrow::{
    buffer::Buffer,
    datatypes::Schema,
    ipc::{self, Endianness, MessageHeader, Type},
};

use super::{DecodeError, DecodeErrorKind, DecodeLimit, validation::parse_encapsulated};

#[derive(Clone)]
pub(crate) struct SessionSchema {
    pub(crate) schema: Arc<Schema>,
}

impl SessionSchema {
    pub fn from_serialized(bytes: &[u8], limit: DecodeLimit) -> Result<Self, DecodeError> {
        if bytes.len() > limit.get() {
            return Err(DecodeError::new(DecodeErrorKind::SerializedPayloadLimit));
        }
        let buffer = Buffer::from(bytes);
        let envelope = parse_encapsulated(&buffer, limit)?;
        if envelope.message.header_type() != MessageHeader::Schema {
            return Err(DecodeError::new(DecodeErrorKind::WrongSchemaMessageHeader));
        }
        if !envelope.body.is_empty() {
            return Err(DecodeError::new(DecodeErrorKind::InvalidIpcFraming));
        }
        let ipc_schema = envelope
            .message
            .header_as_schema()
            .ok_or_else(|| DecodeError::new(DecodeErrorKind::InvalidFlatbuffer))?;
        validate_schema(ipc_schema)?;
        let schema = ipc::convert::fb_to_schema(ipc_schema);
        Ok(Self {
            schema: Arc::new(schema),
        })
    }

    pub fn as_arrow(&self) -> &Arc<Schema> {
        &self.schema
    }
}

impl fmt::Debug for SessionSchema {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionSchema")
            .field("field_count", &self.schema.fields().len())
            .field("schema_metadata_entries", &self.schema.metadata().len())
            .finish()
    }
}

fn validate_schema(schema: ipc::Schema<'_>) -> Result<(), DecodeError> {
    if schema.endianness() != Endianness::Little {
        return Err(DecodeError::new(DecodeErrorKind::InvalidArrowSchema));
    }
    let fields = schema
        .fields()
        .ok_or_else(|| DecodeError::new(DecodeErrorKind::InvalidArrowSchema))?;
    for field in fields {
        validate_field(field)?;
    }
    Ok(())
}

fn validate_field(field: ipc::Field<'_>) -> Result<(), DecodeError> {
    if field.dictionary().is_some() {
        return Err(DecodeError::new(
            DecodeErrorKind::DictionaryBatchUnsupported,
        ));
    }
    let valid = match field.type_type() {
        Type::Bool => field.type_as_bool().is_some(),
        Type::Int => field
            .type_as_int()
            .is_some_and(|value| value.bitWidth() == 64 && value.is_signed()),
        Type::FloatingPoint => field
            .type_as_floating_point()
            .is_some_and(|value| value.precision() == ipc::Precision::DOUBLE),
        Type::Binary => field.type_as_binary().is_some(),
        Type::Utf8 => field.type_as_utf_8().is_some(),
        Type::Date => field
            .type_as_date()
            .is_some_and(|value| value.unit() == ipc::DateUnit::DAY),
        Type::Time => field.type_as_time().is_some_and(|value| {
            value.bitWidth() == 64 && value.unit() == ipc::TimeUnit::MICROSECOND
        }),
        Type::Timestamp => field.type_as_timestamp().is_some_and(|value| {
            matches!(
                value.unit(),
                ipc::TimeUnit::MICROSECOND | ipc::TimeUnit::NANOSECOND
            ) && value.timezone().is_none_or(|timezone| timezone == "UTC")
        }),
        Type::Decimal => field.type_as_decimal().is_some_and(|value| {
            let precision = value.precision();
            let scale = value.scale();
            (match value.bitWidth() {
                128 => (1..=38).contains(&precision) && (0..=9).contains(&scale),
                256 => (1..=76).contains(&precision) && (0..=38).contains(&scale),
                _ => false,
            }) && scale <= precision
        }),
        Type::List => {
            field.type_as_list().is_some()
                && field.children().is_some_and(|children| {
                    children.len() == 1 && validate_field(children.get(0)).is_ok()
                })
        }
        Type::Struct_ => {
            field.type_as_struct_().is_some()
                && field.children().is_none_or(|children| {
                    children.iter().all(|child| validate_field(child).is_ok())
                })
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(DecodeError::new(DecodeErrorKind::InvalidArrowSchema))
    }
}
