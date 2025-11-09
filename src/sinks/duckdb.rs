use std::{path::PathBuf, sync::Mutex};

use anyhow::{Result, anyhow, bail};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use duckdb::{
    Connection,
    core::{LogicalTypeHandle, LogicalTypeId},
    vtab::to_duckdb_logical_type,
};
use pg_escape::quote_identifier;

use crate::sinks::data_sink::{DataSink, SinkResult};

pub struct DuckDBSinkInner {
    path: PathBuf,
    table_name: String,
    schema: SchemaRef,
    rows_written: u64,
    conn: Option<Connection>,
}

pub struct DuckDBSink {
    inner: Mutex<DuckDBSinkInner>,
}

impl DuckDBSink {
    pub fn create(path: PathBuf, table_name: String, schema: SchemaRef) -> Result<Self> {
        let conn = Connection::open(&path)?;

        let mut sink = Self {
            inner: Mutex::new(DuckDBSinkInner {
                path,
                table_name,
                schema,
                rows_written: 0,
                conn: Some(conn),
            }),
        };

        sink.create_table()?;

        Ok(sink)
    }

    fn create_table(&mut self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let columns = inner
            .schema
            .fields()
            .iter()
            .map(|field| {
                let col = field.name();
                let logical_type = to_duckdb_logical_type(field.data_type())
                    .map_err(|e| anyhow!("Failed to convert logical type: {:?}", e))?;
                let col_type = Self::logical_type_to_sql(&logical_type)?;
                let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
                Ok(format!(
                    "{} {}{}",
                    quote_identifier(col),
                    col_type,
                    nullable
                ))
            })
            .collect::<Result<Vec<String>>>()?;

        let sql = format!(
            "CREATE TABLE {} ({})",
            quote_identifier(&inner.table_name),
            columns.join(", ")
        );

        inner
            .conn
            .as_mut()
            .ok_or_else(|| anyhow!("Connection not found"))?
            .execute(&sql, [])?;

        Ok(())
    }

    fn logical_type_to_sql(logical_type: &LogicalTypeHandle) -> Result<String> {
        let sql = match logical_type.id() {
            LogicalTypeId::Boolean => "BOOLEAN".to_string(),
            LogicalTypeId::Tinyint => "TINYINT".to_string(),
            LogicalTypeId::Smallint => "SMALLINT".to_string(),
            LogicalTypeId::Integer => "INTEGER".to_string(),
            LogicalTypeId::Bigint => "BIGINT".to_string(),
            LogicalTypeId::UTinyint => "UTINYINT".to_string(),
            LogicalTypeId::USmallint => "USMALLINT".to_string(),
            LogicalTypeId::UInteger => "UINTEGER".to_string(),
            LogicalTypeId::UBigint => "UBIGINT".to_string(),
            LogicalTypeId::Float => "FLOAT".to_string(),
            LogicalTypeId::Double => "DOUBLE".to_string(),
            LogicalTypeId::Timestamp => "TIMESTAMP".to_string(),
            LogicalTypeId::Date => "DATE".to_string(),
            LogicalTypeId::Time => "TIME".to_string(),
            LogicalTypeId::Interval => "INTERVAL".to_string(),
            LogicalTypeId::Varchar => "VARCHAR".to_string(),
            LogicalTypeId::Blob => "BLOB".to_string(),
            LogicalTypeId::Decimal => {
                format!(
                    "DECIMAL({}, {})",
                    logical_type.decimal_width(),
                    logical_type.decimal_scale()
                )
            }
            LogicalTypeId::TimestampS => "TIMESTAMP_S".to_string(),
            LogicalTypeId::TimestampMs => "TIMESTAMP_MS".to_string(),
            LogicalTypeId::TimestampNs => "TIMESTAMP_NS".to_string(),
            LogicalTypeId::TimestampTZ => "TIMESTAMPTZ".to_string(),
            LogicalTypeId::Uuid => "UUID".to_string(),
            // Unsupported types:
            // - Enum
            // - Union
            // - Hugeint
            // - Map
            // - List
            // - Struct
            _ => bail!("Unsupported logical type: {:?}", logical_type.id()),
        };
        Ok(sql)
    }
}

#[async_trait]
impl DataSink for DuckDBSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let batch_rows = batch.num_rows();
        let table_name = inner.table_name.clone();
        let conn = inner
            .conn
            .as_mut()
            .ok_or_else(|| anyhow!("Connection not found"))?;
        conn.appender(&table_name)?.append_record_batch(batch)?;
        inner.rows_written += batch_rows as u64;
        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        let mut inner = self.inner.lock().unwrap();
        let path = inner.path.clone();
        let rows_written = inner.rows_written;
        inner
            .conn
            .take()
            .ok_or_else(|| anyhow!("Connection already closed"))?
            .close()
            .map_err(|e| anyhow!("Failed to close connection: {:?}", e.1))?;
        Ok(SinkResult {
            files_written: vec![path],
            rows_written,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{
            BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
            Int8Array, Int16Array, Int32Array, Int64Array, StringArray, Time64NanosecondArray,
            TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        },
        datatypes::{DataType, Field, Schema, TimeUnit},
    };
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::{sources::data_source::DataSource, utils::test_helpers::test_data};

    mod duckdb_sink_tests {
        use super::*;

        #[tokio::test]
        async fn test_sink_writes_single_batch() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = test_data::simple_schema();
            let batch =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

            let mut sink =
                DuckDBSink::create(db_path.clone(), "test_table".to_string(), schema).unwrap();

            sink.write_batch(batch).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 3);
            assert_eq!(result.files_written.len(), 1);
            assert_eq!(result.files_written[0], db_path);

            let conn = Connection::open(&db_path).unwrap();
            let count: i32 = conn
                .query_row("SELECT COUNT(*) FROM test_table", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 3);
        }

        #[tokio::test]
        async fn test_sink_writes_multiple_batches() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = test_data::simple_schema();
            let batch1 = test_data::create_batch_with_ids_and_names(&schema, &[1, 2], &["a", "b"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[3, 4], &["c", "d"]);

            let mut sink =
                DuckDBSink::create(db_path.clone(), "test_table".to_string(), schema).unwrap();

            sink.write_batch(batch1).await.unwrap();
            sink.write_batch(batch2).await.unwrap();
            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 4);

            let conn = Connection::open(&db_path).unwrap();
            let count: i32 = conn
                .query_row("SELECT COUNT(*) FROM test_table", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 4);

            let mut stmt = conn.prepare("SELECT id, name FROM test_table").unwrap();
            let rows: Vec<(i32, String)> = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(rows.len(), 4);
        }

        #[tokio::test]
        async fn test_sink_with_nullable_columns() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = test_data::nullable_id_schema();
            let batch = test_data::create_batch_with_nullable_ids_and_non_nullable_names(
                &schema,
                &[Some(1), None, Some(3)],
                &["a", "b", "c"],
            );

            let mut sink =
                DuckDBSink::create(db_path.clone(), "nullable_test".to_string(), schema).unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            let mut stmt = conn.prepare("SELECT id, name FROM nullable_test").unwrap();
            let rows: Vec<(Option<i32>, String)> = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0], (Some(1), "a".to_string()));
            assert_eq!(rows[1], (None, "b".to_string()));
            assert_eq!(rows[2], (Some(3), "c".to_string()));
        }

        #[tokio::test]
        async fn test_sink_empty_table() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = test_data::simple_schema();
            let mut sink =
                DuckDBSink::create(db_path.clone(), "empty_table".to_string(), schema).unwrap();

            let result = sink.finish().await.unwrap();

            assert_eq!(result.rows_written, 0);

            let conn = Connection::open(&db_path).unwrap();
            let count: i32 = conn
                .query_row("SELECT COUNT(*) FROM empty_table", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 0);
        }

        #[tokio::test]
        async fn test_sink_special_characters_in_table_name() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &[1], &["a"]);

            let mut sink =
                DuckDBSink::create(db_path.clone(), "my-test.table".to_string(), schema.clone())
                    .unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            let count: i32 = conn
                .query_row(r#"SELECT COUNT(*) FROM "my-test.table""#, [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 1);
        }

        #[tokio::test]
        async fn test_sink_special_characters_in_column_names() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = Arc::new(Schema::new(vec![
                Field::new("weird-column", DataType::Int32, false),
                Field::new("another.col", DataType::Utf8, false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["x", "y"])),
                ],
            )
            .unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "special_cols".to_string(), schema).unwrap();

            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            let val: i32 = conn
                .query_row(
                    r#"SELECT "weird-column" FROM special_cols WHERE "another.col" = 'x'"#,
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(val, 1);
        }

        #[tokio::test]
        async fn test_sink_write_stream() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let db_path = temp_dir.path().join("test.db");

            let schema = test_data::simple_schema();
            let batch1 =
                test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);
            let batch2 = test_data::create_batch_with_ids_and_names(&schema, &[4, 5], &["d", "e"]);

            crate::utils::test_helpers::file_helpers::write_arrow_file(
                &input_path,
                &schema,
                vec![batch1, batch2],
            )
            .unwrap();

            let source = crate::sources::arrow_file::ArrowFileDataSource::new(
                input_path.to_str().unwrap().to_string(),
            );
            let stream = source.as_stream().await.unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "stream_test".to_string(), schema).unwrap();

            let result = sink.write_stream(stream).await.unwrap();

            assert_eq!(result.rows_written, 5);

            let conn = Connection::open(&db_path).unwrap();
            let count: i32 = conn
                .query_row("SELECT COUNT(*) FROM stream_test", [], |row| row.get(0))
                .unwrap();
            assert_eq!(count, 5);
        }

        #[tokio::test]
        async fn test_finish_twice_fails() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = test_data::simple_schema();
            let mut sink = DuckDBSink::create(db_path, "test_table".to_string(), schema).unwrap();

            sink.finish().await.unwrap();
            let result = sink.finish().await;

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Connection already closed")
            );
        }
    }

    mod type_conversion_tests {
        use chrono::{Duration, TimeDelta};

        use super::*;

        #[tokio::test]
        async fn test_numeric_types() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = Arc::new(Schema::new(vec![
                Field::new("i8_col", DataType::Int8, false),
                Field::new("i16_col", DataType::Int16, false),
                Field::new("i32_col", DataType::Int32, false),
                Field::new("i64_col", DataType::Int64, false),
                Field::new("u8_col", DataType::UInt8, false),
                Field::new("u16_col", DataType::UInt16, false),
                Field::new("u32_col", DataType::UInt32, false),
                Field::new("u64_col", DataType::UInt64, false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int8Array::from(vec![1_i8, 2, 3])),
                    Arc::new(Int16Array::from(vec![1_i16, 2, 3])),
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
                    Arc::new(UInt8Array::from(vec![1_u8, 2, 3])),
                    Arc::new(UInt16Array::from(vec![1_u16, 2, 3])),
                    Arc::new(UInt32Array::from(vec![1_u32, 2, 3])),
                    Arc::new(UInt64Array::from(vec![1_u64, 2, 3])),
                ],
            )
            .unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "numeric_test".to_string(), schema).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();

            let mut stmt = conn
                .prepare("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'numeric_test' ORDER BY ordinal_position")
                .unwrap();
            let columns: Vec<(String, String)> = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(columns.len(), 8);
            assert_eq!(columns[0], ("i8_col".to_string(), "TINYINT".to_string()));
            assert_eq!(columns[1], ("i16_col".to_string(), "SMALLINT".to_string()));
            assert_eq!(columns[2], ("i32_col".to_string(), "INTEGER".to_string()));
            assert_eq!(columns[3], ("i64_col".to_string(), "BIGINT".to_string()));
            assert_eq!(columns[4], ("u8_col".to_string(), "UTINYINT".to_string()));
            assert_eq!(columns[5], ("u16_col".to_string(), "USMALLINT".to_string()));
            assert_eq!(columns[6], ("u32_col".to_string(), "UINTEGER".to_string()));
            assert_eq!(columns[7], ("u64_col".to_string(), "UBIGINT".to_string()));

            let row: (i8, i16, i32, i64, u8, u16, u32, u64) = conn
                .query_row(
                    "SELECT i8_col, i16_col, i32_col, i64_col, u8_col, u16_col, u32_col, u64_col FROM numeric_test ORDER BY i32_col LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?)),
                )
                .unwrap();
            assert_eq!(row, (1_i8, 1_i16, 1, 1_i64, 1_u8, 1_u16, 1_u32, 1_u64));
        }

        #[tokio::test]
        async fn test_float_types() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = Arc::new(Schema::new(vec![
                Field::new("f32_col", DataType::Float32, false),
                Field::new("f64_col", DataType::Float64, false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Float32Array::from(vec![1.5_f32, 2.5, 3.5])),
                    Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
                ],
            )
            .unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "float_test".to_string(), schema).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            let (val1, val2): (f32, f64) = conn
                .query_row(
                    "SELECT f32_col, f64_col FROM float_test ORDER BY f32_col LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert!((val1 - 1.5).abs() < 0.001);
            assert!((val2 - 1.5).abs() < 0.001);
        }

        #[tokio::test]
        async fn test_boolean_type() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("flag", DataType::Boolean, false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(BooleanArray::from(vec![true, false, true])),
                ],
            )
            .unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "bool_test".to_string(), schema).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            let flag: bool = conn
                .query_row(
                    "SELECT flag FROM bool_test WHERE id = 1 ORDER BY id",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(flag);
        }

        #[tokio::test]
        async fn test_string_and_binary_types() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = Arc::new(Schema::new(vec![
                Field::new("str_col", DataType::Utf8, false),
                Field::new("bin_col", DataType::Binary, false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["hello", "world"])),
                    Arc::new(BinaryArray::from(vec![
                        b"bytes1".as_slice(),
                        b"bytes2".as_slice(),
                    ])),
                ],
            )
            .unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "string_test".to_string(), schema).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            let (s1, b1): (String, Vec<u8>) = conn
                .query_row(
                    "SELECT str_col, bin_col FROM string_test ORDER BY str_col LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(s1, "hello");
            assert_eq!(b1, b"bytes1");
        }

        #[tokio::test]
        async fn test_temporal_types() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = Arc::new(Schema::new(vec![
                Field::new("date_col", DataType::Date32, false),
                Field::new(
                    "timestamp_col",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("time_col", DataType::Time64(TimeUnit::Nanosecond), false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Date32Array::from(vec![18628, 18629])),
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        1609459200000000_i64,
                        1609545600000000,
                    ])),
                    Arc::new(Time64NanosecondArray::from(vec![
                        3600000000000_i64,
                        7200000000000,
                    ])),
                ],
            )
            .unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "temporal_test".to_string(), schema).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            let (date1, timestamp1, time1): (i32, i64, i64) = conn
                .query_row(
                    "SELECT date_col, timestamp_col, time_col FROM temporal_test ORDER BY date_col LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(date1, 18628);
            assert_eq!(timestamp1, 1609459200000000);
            assert_eq!(time1, 3600000000);
        }

        #[tokio::test]
        async fn test_decimal_type() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("amount", DataType::Decimal128(10, 2), false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(
                        Decimal128Array::from(vec![12345_i128, 67890])
                            .with_precision_and_scale(10, 2)
                            .unwrap(),
                    ),
                ],
            )
            .unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "decimal_test".to_string(), schema).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            let (id1, amount1): (i32, f64) = conn
                .query_row(
                    "SELECT id, CAST(amount AS DOUBLE) FROM decimal_test ORDER BY id LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(id1, 1);
            assert!((amount1 - 123.45).abs() < 0.001);
        }

        #[tokio::test]
        async fn test_timestamp_variants() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = Arc::new(Schema::new(vec![
                Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), false),
                Field::new(
                    "ts_ms",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new(
                    "ts_us",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new(
                    "ts_ns",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(arrow::array::TimestampSecondArray::from(vec![
                        1609459200_i64,
                    ])),
                    Arc::new(arrow::array::TimestampMillisecondArray::from(vec![
                        1609459200000_i64,
                    ])),
                    Arc::new(TimestampMicrosecondArray::from(vec![1609459200000000_i64])),
                    Arc::new(arrow::array::TimestampNanosecondArray::from(vec![
                        1609459200000000000_i64,
                    ])),
                ],
            )
            .unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "timestamp_variants".to_string(), schema)
                    .unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            // all timestamp variants should be readable as i64
            let (ts_s, ts_ms, ts_us, ts_ns): (i64, i64, i64, i64) = conn
                .query_row(
                    "SELECT ts_s, ts_ms, ts_us, ts_ns FROM timestamp_variants LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap();

            assert_eq!(ts_s, 1609459200);
            assert_eq!(ts_ms, 1609459200000);
            assert_eq!(ts_us, 1609459200000000);
            assert_eq!(ts_ns, 1609459200000000000);
        }

        #[tokio::test]
        async fn test_interval_type() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "duration",
                    DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
                    false,
                ),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1])),
                    Arc::new(arrow::array::IntervalMonthDayNanoArray::from(vec![
                        arrow::datatypes::IntervalMonthDayNano {
                            months: 0,
                            days: 1,
                            nanoseconds: 1000000000,
                        },
                    ])),
                ],
            )
            .unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "interval_test".to_string(), schema).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            let (id1, duration1): (i32, Duration) = conn
                .query_row(
                    "SELECT id, duration FROM interval_test ORDER BY id LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(id1, 1);
            assert_eq!(
                duration1,
                TimeDelta::nanoseconds(1000000000) + TimeDelta::days(1)
            );
        }

        #[tokio::test]
        async fn test_table_schema_created_correctly() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &[1], &["a"]);

            let mut sink =
                DuckDBSink::create(db_path.clone(), "test_table".to_string(), schema).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            let mut stmt = conn
                .prepare("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'test_table' ORDER BY ordinal_position")
                .unwrap();

            let columns: Vec<(String, String, String)> = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(columns.len(), 2);
            assert_eq!(
                columns[0],
                ("id".to_string(), "INTEGER".to_string(), "NO".to_string())
            );
            assert_eq!(
                columns[1],
                ("name".to_string(), "VARCHAR".to_string(), "NO".to_string())
            );
        }

        #[tokio::test]
        async fn test_nullable_schema_created_correctly() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = test_data::nullable_id_schema();
            let batch = test_data::create_batch_with_nullable_ids_and_non_nullable_names(
                &schema,
                &[Some(1)],
                &["a"],
            );

            let mut sink =
                DuckDBSink::create(db_path.clone(), "nullable_test".to_string(), schema).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();
            let mut stmt = conn
                .prepare("SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'nullable_test' ORDER BY ordinal_position")
                .unwrap();

            let columns: Vec<(String, String, String)> = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(columns.len(), 2);
            assert_eq!(
                columns[0],
                ("id".to_string(), "INTEGER".to_string(), "YES".to_string())
            );
            assert_eq!(
                columns[1],
                ("name".to_string(), "VARCHAR".to_string(), "NO".to_string())
            );
        }

        #[tokio::test]
        async fn test_uuid_type() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            // arrow represents UUID as FixedSizeBinary(16)
            // but duckdb's to_duckdb_logical_type converts it to BLOB, not UUID
            // the LogicalTypeId::Uuid mapping is for actual UUID logical types
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("uuid_col", DataType::FixedSizeBinary(16), false),
            ]));

            let uuid_bytes: [u8; 16] = [
                0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4,
                0x30, 0xc8,
            ];

            let uuid_array = arrow::array::FixedSizeBinaryBuilder::with_capacity(2, 16);
            let mut uuid_array = uuid_array;
            uuid_array.append_value(uuid_bytes).unwrap();
            uuid_array.append_value(uuid_bytes).unwrap();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(uuid_array.finish()),
                ],
            )
            .unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "uuid_test".to_string(), schema).unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();

            let mut stmt = conn
                .prepare("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'uuid_test' ORDER BY ordinal_position")
                .unwrap();
            let columns: Vec<(String, String)> = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(columns[1], ("uuid_col".to_string(), "BLOB".to_string()));

            let (id1, uuid1): (i32, Vec<u8>) = conn
                .query_row(
                    "SELECT id, uuid_col FROM uuid_test ORDER BY id LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(id1, 1);
            assert_eq!(uuid1, uuid_bytes);
        }

        #[tokio::test]
        async fn test_timestamp_with_timezone() {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("test.db");

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "ts_tz",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![
                            1609459200000000_i64,
                            1609545600000000,
                        ])
                        .with_timezone("UTC"),
                    ),
                ],
            )
            .unwrap();

            let mut sink =
                DuckDBSink::create(db_path.clone(), "timestamptz_test".to_string(), schema)
                    .unwrap();
            sink.write_batch(batch).await.unwrap();
            sink.finish().await.unwrap();

            let conn = Connection::open(&db_path).unwrap();

            let mut stmt = conn
                .prepare("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'timestamptz_test' ORDER BY ordinal_position")
                .unwrap();
            let columns: Vec<(String, String)> = stmt
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(
                columns[1],
                ("ts_tz".to_string(), "TIMESTAMP WITH TIME ZONE".to_string())
            );

            let ts_val: i64 = conn
                .query_row(
                    "SELECT ts_tz FROM timestamptz_test WHERE id = 1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(ts_val, 1609459200000000);
        }
    }
}
