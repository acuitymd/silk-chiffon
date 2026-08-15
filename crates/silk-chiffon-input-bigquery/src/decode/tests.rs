use std::sync::Arc;

use crate::{
    decode::{DecodeErrorKind, DecodeLimit, RowPayloadCodec, SerializedRows, SessionSchema},
    proto::bigquery_storage::{
        ArrowRecordBatch, ArrowSchema, ReadRowsResponse, read_rows_response,
    },
};
use arrow::{
    array::{Int64Array, RecordBatch, RecordBatchOptions, StringArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    ipc::CompressionType,
};
use silk_chiffon_test_support::bigquery_arrow::{
    documented_mapping_fixture, encode_batch, encode_dictionary_message, encode_schema,
    encode_schema_with_options, legacy_schema_options, negative_body_length_message,
};

const LIMIT: usize = 4 * 1024 * 1024;

fn response(
    payload: Vec<u8>,
    rows: i64,
    deprecated_rows: i64,
    declared_size: Option<i64>,
    repeated_schema: Option<Vec<u8>>,
) -> ReadRowsResponse {
    ReadRowsResponse {
        row_count: rows,
        rows: Some(read_rows_response::Rows::ArrowRecordBatch(
            #[allow(deprecated)]
            ArrowRecordBatch {
                serialized_record_batch: payload,
                row_count: deprecated_rows,
            },
        )),
        schema: repeated_schema.map(|serialized_schema| {
            read_rows_response::Schema::ArrowSchema(ArrowSchema { serialized_schema })
        }),
        uncompressed_byte_size: declared_size,
        ..Default::default()
    }
}

fn decode(
    schema: &SessionSchema,
    response: ReadRowsResponse,
    codec: RowPayloadCodec,
) -> Result<crate::decode::DecodedBatch, crate::decode::DecodeError> {
    SerializedRows::from_response(response, schema, codec, DecodeLimit::new(LIMIT).unwrap())?
        .expect("test response has rows")
        .decode(schema, DecodeLimit::new(LIMIT).unwrap())
}

#[test]
fn arrow_fixture_decode_documented_mappings_and_metadata() {
    let fixture = documented_mapping_fixture();
    let schema_bytes = encode_schema(&fixture.schema);
    let schema =
        SessionSchema::from_serialized(&schema_bytes, DecodeLimit::new(LIMIT).unwrap()).unwrap();
    let payload = encode_batch(&fixture.batch, None);

    let decoded = decode(
        &schema,
        response(payload.clone(), 2, 2, None, None),
        RowPayloadCodec::None,
    )
    .unwrap();

    assert_eq!(decoded.record_batch(), &fixture.batch);
    assert_eq!(decoded.record_batch().schema(), fixture.schema);
    assert_eq!(decoded.row_count(), 2);
    assert_eq!(decoded.bytes().serialized_payload(), payload.len());
    assert!(decoded.bytes().arrow_buffer_memory() > 0);
    assert_eq!(
        decoded.record_batch().schema().field(3).data_type(),
        &DataType::Decimal128(38, 9)
    );
    assert_eq!(
        decoded.record_batch().schema().field(4).data_type(),
        &DataType::Decimal256(76, 38)
    );
    assert_eq!(
        decoded.record_batch().schema().field(8).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        decoded.record_batch().schema().field(12).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        decoded.record_batch().schema().field(13).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        decoded.record_batch().schema().field(14).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
    assert_eq!(
        decoded.record_batch().schema().field(15).data_type(),
        &DataType::Utf8
    );
}

#[test]
fn compression_decode_native_lz4_and_zstd() {
    let fixture = documented_mapping_fixture();
    let schema = SessionSchema::from_serialized(
        &encode_schema(&fixture.schema),
        DecodeLimit::new(LIMIT).unwrap(),
    )
    .unwrap();

    for codec in [CompressionType::LZ4_FRAME, CompressionType::ZSTD] {
        let payload = encode_batch(&fixture.batch, Some(codec));
        let decoded = decode(
            &schema,
            response(payload, 2, 0, None, None),
            RowPayloadCodec::None,
        )
        .unwrap();
        assert_eq!(decoded.record_batch(), &fixture.batch);
    }
}

#[test]
fn compression_decode_raw_lz4_and_skip_nonpositive_sizes() {
    let fixture = documented_mapping_fixture();
    let schema = SessionSchema::from_serialized(
        &encode_schema(&fixture.schema),
        DecodeLimit::new(LIMIT).unwrap(),
    )
    .unwrap();
    let ipc = encode_batch(&fixture.batch, None);
    let compressed = lz4_flex::block::compress(&ipc);

    let decoded = decode(
        &schema,
        response(
            compressed.clone(),
            2,
            0,
            Some(i64::try_from(ipc.len()).unwrap()),
            None,
        ),
        RowPayloadCodec::RawLz4,
    )
    .unwrap();
    assert_eq!(decoded.record_batch(), &fixture.batch);
    assert_eq!(decoded.bytes().serialized_payload(), compressed.len());
    assert_eq!(decoded.bytes().payload_decompressed(), ipc.len());

    for declared in [None, Some(0), Some(-1)] {
        let decoded = decode(
            &schema,
            response(ipc.clone(), 2, 0, declared, None),
            RowPayloadCodec::RawLz4,
        )
        .unwrap();
        assert_eq!(decoded.bytes().payload_decompressed(), 0);
    }
    assert_eq!(
        decode(
            &schema,
            response(ipc, 2, 0, Some(-2), None),
            RowPayloadCodec::RawLz4,
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::UnknownCompressionState
    );
}

#[test]
fn arrow_fixture_accepts_empty_and_zero_column_batches() {
    let fixture = documented_mapping_fixture();
    let schema = SessionSchema::from_serialized(
        &encode_schema(&fixture.schema),
        DecodeLimit::new(LIMIT).unwrap(),
    )
    .unwrap();
    let empty = fixture.batch.slice(0, 0);
    assert_eq!(
        decode(
            &schema,
            response(encode_batch(&empty, None), 0, 0, None, None),
            RowPayloadCodec::None,
        )
        .unwrap()
        .row_count(),
        0
    );

    let zero_schema = Arc::new(Schema::empty());
    let zero_columns = RecordBatch::try_new_with_options(
        Arc::clone(&zero_schema),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(3)),
    )
    .unwrap();
    let schema = SessionSchema::from_serialized(
        &encode_schema(&zero_schema),
        DecodeLimit::new(LIMIT).unwrap(),
    )
    .unwrap();
    assert_eq!(
        decode(
            &schema,
            response(encode_batch(&zero_columns, None), 3, 3, None, None),
            RowPayloadCodec::None,
        )
        .unwrap()
        .row_count(),
        3
    );
}

#[test]
fn arrow_fixture_accepts_semantically_equivalent_repeated_schema() {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("value", DataType::Int64, true)],
        [("schema-key".to_owned(), "schema-value".to_owned())].into(),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![Some(1)]))],
    )
    .unwrap();
    let canonical =
        SessionSchema::from_serialized(&encode_schema(&schema), DecodeLimit::new(LIMIT).unwrap())
            .unwrap();
    let legacy = encode_schema_with_options(&schema, &legacy_schema_options());

    let decoded = decode(
        &canonical,
        response(encode_batch(&batch, None), 1, 1, None, Some(legacy)),
        RowPayloadCodec::None,
    )
    .unwrap();
    assert_eq!(decoded.record_batch(), &batch);

    let metadata_mismatch = Arc::new(Schema::new_with_metadata(
        vec![Field::new("value", DataType::Int64, true)],
        [("schema-key".to_owned(), "different-value".to_owned())].into(),
    ));
    assert_eq!(
        SerializedRows::from_response(
            response(
                encode_batch(&batch, None),
                1,
                1,
                None,
                Some(encode_schema(&metadata_mismatch)),
            ),
            &canonical,
            RowPayloadCodec::None,
            DecodeLimit::new(LIMIT).unwrap(),
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::RepeatedSchemaMismatch
    );
}

#[test]
fn arrow_fixture_rejects_mismatches_corruption_and_avro() {
    let fixture = documented_mapping_fixture();
    let schema = SessionSchema::from_serialized(
        &encode_schema(&fixture.schema),
        DecodeLimit::new(LIMIT).unwrap(),
    )
    .unwrap();
    let payload = encode_batch(&fixture.batch, None);

    let cases = [
        (
            response(payload.clone(), -1, 0, None, None),
            DecodeErrorKind::NegativeResponseRowCount,
        ),
        (
            response(payload.clone(), 2, 1, None, None),
            DecodeErrorKind::DeprecatedRowCountMismatch,
        ),
        (
            response(payload.clone(), 1, 0, None, None),
            DecodeErrorKind::DecodedRowCountMismatch,
        ),
    ];
    for (response, kind) in cases {
        assert_eq!(
            decode(&schema, response, RowPayloadCodec::None)
                .unwrap_err()
                .kind(),
            kind
        );
    }

    for declared in [0, -1, -2, 10] {
        assert_eq!(
            decode(
                &schema,
                response(payload.clone(), 2, 0, Some(declared), None),
                RowPayloadCodec::None,
            )
            .unwrap_err()
            .kind(),
            DecodeErrorKind::CompressionNotRequested
        );
    }

    assert_eq!(
        SerializedRows::from_response(
            response(payload.clone(), 2, 0, None, None),
            &schema,
            RowPayloadCodec::None,
            DecodeLimit::new(payload.len() - 1).unwrap(),
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::SerializedPayloadLimit
    );

    let mut truncated = payload.clone();
    truncated.pop();
    assert_eq!(
        decode(
            &schema,
            response(truncated, 2, 0, None, None),
            RowPayloadCodec::None,
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::InvalidIpcFraming
    );

    let mut trailing = payload;
    trailing.push(0);
    assert_eq!(
        decode(
            &schema,
            response(trailing, 2, 0, None, None),
            RowPayloadCodec::None,
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::InvalidIpcFraming
    );

    let mut invalid_schema = encode_schema(&fixture.schema);
    invalid_schema[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        SessionSchema::from_serialized(&invalid_schema, DecodeLimit::new(LIMIT).unwrap())
            .unwrap_err()
            .kind(),
        DecodeErrorKind::InvalidFlatbuffer
    );
    assert_eq!(
        SessionSchema::from_serialized(
            &negative_body_length_message(),
            DecodeLimit::new(LIMIT).unwrap(),
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::InvalidIpcFraming
    );

    let avro = ReadRowsResponse {
        row_count: 1,
        rows: Some(read_rows_response::Rows::AvroRows(Default::default())),
        ..Default::default()
    };
    assert_eq!(
        SerializedRows::from_response(
            avro,
            &schema,
            RowPayloadCodec::None,
            DecodeLimit::new(LIMIT).unwrap(),
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::AvroRows
    );

    let avro_schema = ReadRowsResponse {
        row_count: 0,
        schema: Some(read_rows_response::Schema::AvroSchema(Default::default())),
        ..Default::default()
    };
    assert_eq!(
        SerializedRows::from_response(
            avro_schema,
            &schema,
            RowPayloadCodec::None,
            DecodeLimit::new(LIMIT).unwrap(),
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::AvroSchema
    );
}

#[test]
fn decode_accepts_progress_only_response_and_rejects_missing_positive_rows() {
    let fixture = documented_mapping_fixture();
    let schema = SessionSchema::from_serialized(
        &encode_schema(&fixture.schema),
        DecodeLimit::new(LIMIT).unwrap(),
    )
    .unwrap();
    let progress_only = ReadRowsResponse {
        row_count: 0,
        ..Default::default()
    };
    assert!(
        SerializedRows::from_response(
            progress_only,
            &schema,
            RowPayloadCodec::None,
            DecodeLimit::new(LIMIT).unwrap(),
        )
        .unwrap()
        .is_none()
    );

    let compressed_without_rows = ReadRowsResponse {
        row_count: 0,
        uncompressed_byte_size: Some(1),
        ..Default::default()
    };
    assert_eq!(
        SerializedRows::from_response(
            compressed_without_rows,
            &schema,
            RowPayloadCodec::RawLz4,
            DecodeLimit::new(LIMIT).unwrap(),
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::PositiveSizeWithoutRows
    );
}

#[test]
fn compression_rejects_size_mismatch_limit_bomb_and_corruption() {
    let fixture = documented_mapping_fixture();
    let schema = SessionSchema::from_serialized(
        &encode_schema(&fixture.schema),
        DecodeLimit::new(LIMIT).unwrap(),
    )
    .unwrap();
    let ipc = encode_batch(&fixture.batch, None);
    let compressed = lz4_flex::block::compress(&ipc);

    for wrong_size in [ipc.len() - 1, ipc.len() + 1] {
        assert_eq!(
            decode(
                &schema,
                response(
                    compressed.clone(),
                    2,
                    0,
                    Some(i64::try_from(wrong_size).unwrap()),
                    None,
                ),
                RowPayloadCodec::RawLz4,
            )
            .unwrap_err()
            .kind(),
            DecodeErrorKind::RawLz4Decompression
        );
    }
    assert_eq!(
        SerializedRows::from_response(
            response(
                compressed.clone(),
                2,
                0,
                Some(i64::try_from(LIMIT + 1).unwrap()),
                None,
            ),
            &schema,
            RowPayloadCodec::RawLz4,
            DecodeLimit::new(LIMIT).unwrap(),
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::DecodedPayloadLimit
    );

    let bomb = lz4_flex::block::compress(&vec![0_u8; 128 * 1024]);
    assert_eq!(
        decode(
            &schema,
            response(bomb, 2, 0, Some(1), None),
            RowPayloadCodec::RawLz4,
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::RawLz4Decompression
    );

    let mut corrupt = compressed;
    corrupt[0] ^= 0xff;
    assert_eq!(
        decode(
            &schema,
            response(corrupt, 2, 0, Some(i64::try_from(ipc.len()).unwrap()), None,),
            RowPayloadCodec::RawLz4,
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::RawLz4Decompression
    );
}

#[test]
fn arrow_fixture_rejects_wrong_headers_dictionary_and_schema_mismatch() {
    let fixture = documented_mapping_fixture();
    let schema = SessionSchema::from_serialized(
        &encode_schema(&fixture.schema),
        DecodeLimit::new(LIMIT).unwrap(),
    )
    .unwrap();

    assert_eq!(
        decode(
            &schema,
            response(encode_schema(&fixture.schema), 2, 0, None, None),
            RowPayloadCodec::None,
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::WrongRecordBatchMessageHeader
    );
    assert_eq!(
        decode(
            &schema,
            response(encode_dictionary_message(), 2, 0, None, None),
            RowPayloadCodec::None,
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::DictionaryBatchUnsupported
    );

    let different = Arc::new(Schema::new(vec![Field::new(
        "different-contract-field",
        DataType::Int64,
        true,
    )]));
    assert_eq!(
        SerializedRows::from_response(
            response(
                encode_batch(&fixture.batch, None),
                2,
                0,
                None,
                Some(encode_schema(&different)),
            ),
            &schema,
            RowPayloadCodec::None,
            DecodeLimit::new(LIMIT).unwrap(),
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::RepeatedSchemaMismatch
    );
}

#[test]
fn compression_rejects_corrupt_native_lz4_and_zstd() {
    let values = vec![Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); 4096];
    let array = Arc::new(StringArray::from(values));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "compressible",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
    let canonical =
        SessionSchema::from_serialized(&encode_schema(&schema), DecodeLimit::new(LIMIT).unwrap())
            .unwrap();

    for (codec, magic) in [
        (CompressionType::LZ4_FRAME, [0x04, 0x22, 0x4d, 0x18]),
        (CompressionType::ZSTD, [0x28, 0xb5, 0x2f, 0xfd]),
    ] {
        let mut payload = encode_batch(&batch, Some(codec));
        let position = payload
            .windows(magic.len())
            .position(|window| window == magic)
            .expect("compressed buffer contains codec frame");
        payload[position + magic.len()] ^= 0xff;
        assert!(
            decode(
                &canonical,
                response(payload, 4096, 0, None, None),
                RowPayloadCodec::None,
            )
            .is_err()
        );
    }

    let mut oversized = encode_batch(&batch, Some(CompressionType::LZ4_FRAME));
    let position = oversized
        .windows(4)
        .position(|window| window == [0x04, 0x22, 0x4d, 0x18])
        .unwrap();
    oversized[position - 8..position]
        .copy_from_slice(&i64::try_from(LIMIT + 1).unwrap().to_le_bytes());
    assert_eq!(
        decode(
            &canonical,
            response(oversized, 4096, 0, None, None),
            RowPayloadCodec::None,
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::DecodedPayloadLimit
    );

    let mut lying = encode_batch(&batch, Some(CompressionType::LZ4_FRAME));
    let position = lying
        .windows(4)
        .position(|window| window == [0x04, 0x22, 0x4d, 0x18])
        .unwrap();
    lying[position - 8..position].copy_from_slice(&1_i64.to_le_bytes());
    assert_eq!(
        decode(
            &canonical,
            response(lying, 4096, 0, None, None),
            RowPayloadCodec::None,
        )
        .unwrap_err()
        .kind(),
        DecodeErrorKind::NativeCompression
    );
}

#[test]
fn decode_memory_accounting_deduplicates_shared_ipc_buffers() {
    let fixture = documented_mapping_fixture();
    let schema = SessionSchema::from_serialized(
        &encode_schema(&fixture.schema),
        DecodeLimit::new(LIMIT).unwrap(),
    )
    .unwrap();
    let decoded = decode(
        &schema,
        response(encode_batch(&fixture.batch, None), 2, 0, None, None),
        RowPayloadCodec::None,
    )
    .unwrap();

    assert!(decoded.bytes().arrow_buffer_memory() < decoded.record_batch().get_array_memory_size());
}

#[test]
fn decode_debug_and_errors_redact_schema_and_payload_content() {
    let secret = "secret-field-and-value-sentinel";
    let schema = Arc::new(Schema::new(vec![Field::new(secret, DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![Some(secret)]))],
    )
    .unwrap();
    let session =
        SessionSchema::from_serialized(&encode_schema(&schema), DecodeLimit::new(LIMIT).unwrap())
            .unwrap();
    let serialized = SerializedRows::from_response(
        response(encode_batch(&batch, None), 2, 0, None, None),
        &session,
        RowPayloadCodec::None,
        DecodeLimit::new(LIMIT).unwrap(),
    )
    .unwrap()
    .unwrap();
    assert!(!format!("{session:?}{serialized:?}").contains(secret));

    let error = serialized
        .decode(&session, DecodeLimit::new(LIMIT).unwrap())
        .unwrap_err();
    assert!(!format!("{error:?} {error}").contains(secret));
}
