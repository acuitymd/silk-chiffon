use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
        DictionaryArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
        StructArray, Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
        types::{Int32Type, Int64Type},
    },
    datatypes::{DataType, Field, Schema},
    ipc::{
        CompressionType, MetadataVersion,
        writer::{
            CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions, write_message,
        },
    },
};
use arrow_buffer::i256;
use flatbuffers::FlatBufferBuilder;

pub struct ArrowFixture {
    pub schema: Arc<Schema>,
    pub batch: RecordBatch,
}

pub fn documented_mapping_fixture() -> ArrowFixture {
    let boolean: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None]));
    let int64: ArrayRef = Arc::new(Int64Array::from(vec![Some(42), Some(-7)]));
    let float64: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None]));
    let numeric: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(123_456_789_i128), None])
            .with_precision_and_scale(38, 9)
            .unwrap(),
    );
    let bignumeric: ArrayRef = Arc::new(
        Decimal256Array::from(vec![Some(i256::from_i128(123_456_789)), None])
            .with_precision_and_scale(76, 38)
            .unwrap(),
    );
    let string: ArrayRef = Arc::new(StringArray::from(vec![Some("alpha"), None]));
    let bytes: ArrayRef = Arc::new(BinaryArray::from(vec![Some(&b"bytes"[..]), None]));
    let date: ArrayRef = Arc::new(Date32Array::from(vec![Some(20_000), None]));
    let datetime: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(10), None]));
    let geography: ArrayRef = Arc::new(StringArray::from(vec![Some("POINT(1 2)"), None]));
    let json: ArrayRef = Arc::new(StringArray::from(vec![Some("{\"a\":1}"), None]));
    let time: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![Some(12_345), None]));
    let timestamp: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_000), None])
            .with_timezone("UTC"),
    );
    let picos_micros: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(1_700_000_000_000_001), None])
            .with_timezone("UTC"),
    );
    let picos_nanos: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(vec![Some(1_700_000_000_000_000_001), None])
            .with_timezone("UTC"),
    );
    let picos_string: ArrayRef = Arc::new(StringArray::from(vec![
        Some("2026-07-21T12:34:56.123456789012Z"),
        None,
    ]));
    let repeated: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1), None, Some(3)]),
        None,
    ]));

    let nested_name: ArrayRef = Arc::new(StringArray::from(vec![Some("nested"), None]));
    let nested_values: ArrayRef =
        Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(5), Some(8)]),
            Some(vec![]),
        ]));
    let nested: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            nested_name,
        ),
        (
            Arc::new(Field::new(
                "values",
                nested_values.data_type().clone(),
                false,
            )),
            nested_values,
        ),
    ]));

    let range_start: ArrayRef = Arc::new(Date32Array::from(vec![Some(20_000), None]));
    let range_end: ArrayRef = Arc::new(Date32Array::from(vec![Some(20_010), None]));
    let range: ArrayRef = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("start", DataType::Date32, true)),
            range_start,
        ),
        (
            Arc::new(Field::new("end", DataType::Date32, true)),
            range_end,
        ),
    ]));

    let columns = vec![
        boolean,
        int64,
        float64,
        numeric,
        bignumeric,
        string,
        bytes,
        date,
        datetime,
        geography,
        json,
        time,
        timestamp,
        picos_micros,
        picos_nanos,
        picos_string,
        repeated,
        nested,
        range,
    ];
    let names = [
        "boolean",
        "int64",
        "float64",
        "numeric",
        "bignumeric",
        "string",
        "bytes",
        "date",
        "datetime",
        "geography",
        "json",
        "time",
        "timestamp",
        "timestamp_picos_micros",
        "timestamp_picos_nanos",
        "timestamp_picos_string",
        "repeated",
        "nested",
        "range",
    ];
    let mut fields = Vec::with_capacity(columns.len());
    for (name, column) in names.into_iter().zip(&columns) {
        let mut metadata = HashMap::new();
        metadata.insert("contract.fixture.logical_type".to_owned(), name.to_owned());
        fields.push(Field::new(name, column.data_type().clone(), true).with_metadata(metadata));
    }
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(
            "contract.fixture.origin".to_owned(),
            "official-bigquery-mapping-docs".to_owned(),
        )]),
    ));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();

    ArrowFixture { schema, batch }
}

pub fn encode_schema(schema: &Schema) -> Vec<u8> {
    encode_schema_with_options(schema, &IpcWriteOptions::default())
}

pub fn encode_schema_with_options(schema: &Schema, options: &IpcWriteOptions) -> Vec<u8> {
    let mut tracker = DictionaryTracker::new(false);
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut tracker,
        options,
    );
    let mut output = Vec::new();
    write_message(&mut output, encoded, options).unwrap();
    output
}

pub fn encode_batch(batch: &RecordBatch, compression: Option<CompressionType>) -> Vec<u8> {
    let options = IpcWriteOptions::default()
        .try_with_compression(compression)
        .unwrap();
    let mut tracker = DictionaryTracker::new(false);
    let (dictionaries, encoded) = IpcDataGenerator::default()
        .encode(
            batch,
            &mut tracker,
            &options,
            &mut CompressionContext::default(),
        )
        .unwrap();
    assert!(dictionaries.is_empty());
    let mut output = Vec::new();
    write_message(&mut output, encoded, &options).unwrap();
    output
}

pub fn encode_dictionary_message() -> Vec<u8> {
    let dictionary: ArrayRef = Arc::new(
        [Some("dictionary"), Some("dictionary")]
            .into_iter()
            .collect::<DictionaryArray<Int32Type>>(),
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dictionary",
        dictionary.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![dictionary]).unwrap();
    let options = IpcWriteOptions::default();
    let mut tracker = DictionaryTracker::new(false);
    IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        &schema,
        &mut tracker,
        &options,
    );
    let (dictionaries, _) = IpcDataGenerator::default()
        .encode(
            &batch,
            &mut tracker,
            &options,
            &mut CompressionContext::default(),
        )
        .unwrap();
    let mut output = Vec::new();
    write_message(
        &mut output,
        dictionaries.into_iter().next().unwrap(),
        &options,
    )
    .unwrap();
    output
}

pub fn legacy_schema_options() -> IpcWriteOptions {
    IpcWriteOptions::try_new(8, true, MetadataVersion::V4).unwrap()
}

pub fn negative_body_length_message() -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let mut message = arrow::ipc::MessageBuilder::new(&mut builder);
    message.add_version(MetadataVersion::V5);
    message.add_bodyLength(-1);
    let message = message.finish();
    builder.finish(message, None);
    let metadata = builder.finished_data();
    let padded_length = metadata.len().next_multiple_of(8);
    let mut output = Vec::with_capacity(8 + padded_length);
    output.extend_from_slice(&[0xff; 4]);
    output.extend_from_slice(&i32::try_from(padded_length).unwrap().to_le_bytes());
    output.extend_from_slice(metadata);
    output.resize(8 + padded_length, 0);
    output
}
