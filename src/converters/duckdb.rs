use crate::{
    QueryDialect, SortSpec,
    converters::parquet::ParquetConverter,
    utils::arrow_io::{ArrowIPCReader, RecordBatchIterator},
};
use anyhow::{Context, Result, anyhow};
use duckdb::Connection;
use pg_escape::{quote_identifier, quote_literal};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;
use tokio::fs::remove_file;

pub struct DuckDbConverter {
    input: Box<dyn RecordBatchIterator>,
    output_path: PathBuf,
    table_name: String,
    sort_spec: SortSpec,
    truncate: bool,
    drop_table: bool,
    query: Option<String>,
    dialect: QueryDialect,
}

impl DuckDbConverter {
    pub fn new(input_path: String, output_path: PathBuf, table_name: String) -> Result<Self> {
        let reader = ArrowIPCReader::from_path(input_path)?;
        let input = reader.into_batch_iterator()?;
        Ok(Self::from_iterator(input, output_path, table_name))
    }

    pub fn from_iterator(
        input: Box<dyn RecordBatchIterator>,
        output_path: PathBuf,
        table_name: String,
    ) -> Self {
        Self {
            input,
            output_path,
            table_name,
            sort_spec: SortSpec::default(),
            truncate: false,
            drop_table: false,
            query: None,
            dialect: QueryDialect::default(),
        }
    }

    pub fn with_sort_spec(mut self, sort_spec: SortSpec) -> Self {
        self.sort_spec = sort_spec;
        self
    }

    pub fn with_truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }

    pub fn with_drop_table(mut self, drop_table: bool) -> Self {
        self.drop_table = drop_table;
        self
    }

    pub fn with_query(mut self, query: Option<String>) -> Self {
        self.query = query;
        self
    }

    pub fn with_dialect(mut self, dialect: QueryDialect) -> Self {
        self.dialect = dialect;
        self
    }

    pub async fn convert(self) -> Result<()> {
        let temp_file = NamedTempFile::with_suffix(".parquet")?;
        let parquet_path = temp_file.path().to_path_buf();

        ParquetConverter::from_iterator(self.input.clone()?, parquet_path.clone())
            .with_sort_spec(self.sort_spec.clone())
            .with_query(self.query.clone())
            .with_dialect(self.dialect)
            .convert()
            .await?;

        self.load_into_duckdb(&parquet_path).await?;

        Ok(())
    }

    async fn load_into_duckdb(&self, parquet_path: &Path) -> Result<()> {
        if self.truncate {
            remove_file(self.output_path.clone()).await.ok();
        }

        let conn = Connection::open(&self.output_path).with_context(|| {
            format!(
                "Failed to open DuckDB database at {}",
                self.output_path.display()
            )
        })?;

        if !self.truncate {
            let table_exists = {
                let query = "SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_name) = LOWER(?)";
                let count: i32 = conn.query_row(query, [&self.table_name], |row| row.get(0))?;
                count > 0
            };

            if table_exists {
                if self.drop_table {
                    let drop_query = format!(
                        "DROP TABLE IF EXISTS {}",
                        quote_identifier(&self.table_name)
                    );
                    conn.execute(&drop_query, [])?;
                } else {
                    return Err(anyhow!(
                        "Table '{}' already exists. Use --drop-table to replace it.",
                        self.table_name
                    ));
                }
            }
        }

        let query = format!(
            "CREATE TABLE {} AS SELECT * FROM {}",
            quote_identifier(&self.table_name),
            quote_literal(&parquet_path.display().to_string())
        );
        conn.execute(&query, [])?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_helpers::{file_helpers, test_data};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_converter_basic() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.db");

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(
            &schema,
            &[1, 2, 3],
            &["Alice", "Bob", "Charlie"],
        );
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let converter = DuckDbConverter::new(
            input_path.to_string_lossy().to_string(),
            output_path.clone(),
            "test_table".to_string(),
        )
        .unwrap();

        assert!(converter.convert().await.is_ok());

        let conn = Connection::open(&output_path).unwrap();
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM test_table", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_converter_with_sorting() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.db");

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(
            &schema,
            &[3, 1, 2],
            &["Charlie", "Alice", "Bob"],
        );
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let converter = DuckDbConverter::new(
            input_path.to_string_lossy().to_string(),
            output_path.clone(),
            "test_table".to_string(),
        )
        .unwrap()
        .with_sort_spec("id:asc".parse().unwrap());

        assert!(converter.convert().await.is_ok());

        let conn = Connection::open(&output_path).unwrap();
        let mut stmt = conn
            .prepare("SELECT id, name FROM test_table ORDER BY rowid")
            .unwrap();
        let rows: Vec<(i64, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(rows[0], (1, "Alice".to_string()));
        assert_eq!(rows[1], (2, "Bob".to_string()));
        assert_eq!(rows[2], (3, "Charlie".to_string()));
    }

    #[tokio::test]
    async fn test_converter_table_exists_error() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.db");

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &[1], &["Alice"]);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let converter = DuckDbConverter::new(
            input_path.to_string_lossy().to_string(),
            output_path.clone(),
            "test_table".to_string(),
        )
        .unwrap();
        converter.convert().await.unwrap();

        let converter = DuckDbConverter::new(
            input_path.to_string_lossy().to_string(),
            output_path.clone(),
            "test_table".to_string(),
        )
        .unwrap();
        let result = converter.convert().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_converter_with_drop_table() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.db");

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &[1], &["Alice"]);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch.clone()]).unwrap();

        let converter = DuckDbConverter::new(
            input_path.to_string_lossy().to_string(),
            output_path.clone(),
            "test_table".to_string(),
        )
        .unwrap();
        converter.convert().await.unwrap();

        let batch2 =
            test_data::create_batch_with_ids_and_names(&schema, &[2, 3], &["Bob", "Charlie"]);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch2]).unwrap();

        let converter = DuckDbConverter::new(
            input_path.to_string_lossy().to_string(),
            output_path.clone(),
            "test_table".to_string(),
        )
        .unwrap()
        .with_drop_table(true);
        converter.convert().await.unwrap();

        let conn = Connection::open(&output_path).unwrap();
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM test_table", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_converter_with_truncate() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().join("input.arrow");
        let output_path = temp_dir.path().join("output.db");

        let schema = test_data::simple_schema();
        let batch = test_data::create_batch_with_ids_and_names(&schema, &[1], &["Alice"]);
        file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

        let converter = DuckDbConverter::new(
            input_path.to_string_lossy().to_string(),
            output_path.clone(),
            "old_table".to_string(),
        )
        .unwrap();
        converter.convert().await.unwrap();

        let converter = DuckDbConverter::new(
            input_path.to_string_lossy().to_string(),
            output_path.clone(),
            "new_table".to_string(),
        )
        .unwrap()
        .with_truncate(true);
        converter.convert().await.unwrap();

        let conn = Connection::open(&output_path).unwrap();
        let old_exists: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'old_table'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(old_exists, 0);

        let new_exists: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'new_table'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(new_exists, 1);
    }
}
