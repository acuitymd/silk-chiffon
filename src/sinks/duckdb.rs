use std::path::PathBuf;

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

pub struct DuckDBSink {
    path: PathBuf,
    table_name: String,
    schema: SchemaRef,
    rows_written: u64,
    conn: Option<Connection>,
}

impl DuckDBSink {
    pub fn create(path: PathBuf, table_name: String, schema: SchemaRef) -> Result<Self> {
        let conn = Connection::open(&path)?;

        let mut sink = Self {
            path,
            table_name,
            schema,
            rows_written: 0,
            conn: Some(conn),
        };

        sink.create_table()?;

        Ok(sink)
    }

    fn create_table(&mut self) -> Result<()> {
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|field| {
                let col = field.name();
                let logical_type = to_duckdb_logical_type(field.data_type())
                    .map_err(|e| anyhow!("Failed to convert logical type: {:?}", e))?;
                let col_type = Self::logical_type_to_sql(logical_type)?;
                let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
                Ok(format!(
                    "{} {}{}",
                    quote_identifier(&col),
                    col_type,
                    nullable
                ))
            })
            .collect::<Result<Vec<String>>>()?;

        let sql = format!(
            "CREATE TABLE {} ({})",
            quote_identifier(&self.table_name),
            columns.join(", ")
        );

        self.conn
            .as_mut()
            .ok_or_else(|| anyhow!("Connection not found"))?
            .execute(&sql, [])?;

        Ok(())
    }

    fn logical_type_to_sql(logical_type: LogicalTypeHandle) -> Result<String> {
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
            LogicalTypeId::Hugeint => "HUGEINT".to_string(),
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
            LogicalTypeId::List => {
                format!("{}[]", Self::logical_type_to_sql(logical_type.child(0))?)
            }
            LogicalTypeId::Struct => {
                let mut fields = Vec::new();
                for i in 0..logical_type.num_children() {
                    let name = logical_type.child_name(i);
                    let child_sql = Self::logical_type_to_sql(logical_type.child(i))?;
                    fields.push(format!("{} {}", name, child_sql));
                }
                format!("STRUCT({})", fields.join(", "))
            }
            LogicalTypeId::Map => {
                format!(
                    "MAP({}, {})",
                    Self::logical_type_to_sql(logical_type.child(0))?,
                    Self::logical_type_to_sql(logical_type.child(1))?
                )
            }
            // I don't know how to handle Enum or Union and we're unlikely to be using them
            _ => bail!("Unsupported logical type: {:?}", logical_type.id()),
        };
        Ok(sql)
    }
}

#[async_trait]
impl DataSink for DuckDBSink {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let batch_rows = batch.num_rows();
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| anyhow!("Connection not found"))?;
        conn.appender(&self.table_name)?
            .append_record_batch(batch)?;
        self.rows_written += batch_rows as u64;
        Ok(())
    }

    async fn finish(&mut self) -> Result<SinkResult> {
        self.conn
            .take()
            .ok_or_else(|| anyhow!("Connection already closed"))?
            .close()
            .map_err(|e| anyhow!("Failed to close connection: {:?}", e.1))?;
        Ok(SinkResult {
            files_written: vec![self.path.clone()],
            rows_written: self.rows_written,
        })
    }
}
