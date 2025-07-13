use crate::{
    DuckDbArgs, converters::duckdb::DuckDbConverter, utils::filesystem::ensure_parent_dir_exists,
};
use anyhow::Result;

pub async fn run(args: DuckDbArgs) -> Result<()> {
    ensure_parent_dir_exists(args.output.path()).await?;

    let input_path = args.input.path().to_string_lossy().to_string();
    let output_path = args.output.path().to_path_buf();

    let converter = DuckDbConverter::new(input_path, output_path, args.table_name)
        .with_sort_spec(args.sort_by.clone())
        .with_truncate(args.truncate)
        .with_drop_table(args.drop_table)
        .with_query(args.query);

    converter.convert().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SortSpec,
        utils::test_helpers::{file_helpers, test_data},
    };
    use clio::{Input, OutputPath};
    use duckdb::Connection;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_run_basic() {
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

        let args = DuckDbArgs {
            input: Input::new(&input_path).unwrap(),
            output: OutputPath::new(&output_path).unwrap(),
            table_name: "test_table".to_string(),
            query: None,
            sort_by: SortSpec::default(),
            truncate: false,
            drop_table: false,
        };

        assert!(run(args).await.is_ok());

        let conn = Connection::open(&output_path).unwrap();
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM test_table", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_run_with_sort() {
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

        let args = DuckDbArgs {
            input: Input::new(&input_path).unwrap(),
            output: OutputPath::new(&output_path).unwrap(),
            table_name: "test_table".to_string(),
            query: None,
            sort_by: "id:asc".parse().unwrap(),
            truncate: false,
            drop_table: false,
        };

        assert!(run(args).await.is_ok());
    }
}
