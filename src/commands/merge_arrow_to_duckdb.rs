use crate::{
    MergeArrowToDuckdbArgs,
    converters::arrow_to_duckdb::ArrowToDuckDbConverter,
    utils::{
        arrow_io::{ArrowIPCReader, MultiFileIterator},
        filesystem::ensure_parent_dir_exists,
    },
};
use anyhow::{Result, bail};
use glob::glob;

pub async fn run(args: MergeArrowToDuckdbArgs) -> Result<()> {
    ensure_parent_dir_exists(args.output.path()).await?;

    let mut expanded_paths = Vec::new();

    for pattern in &args.inputs {
        for entry in glob(pattern)? {
            expanded_paths.push(entry?.to_string_lossy().to_string());
        }
    }

    expanded_paths.sort();
    expanded_paths.dedup();

    if expanded_paths.is_empty() {
        bail!("No input files found");
    }

    let iterator = if expanded_paths.len() == 1 {
        ArrowIPCReader::from_path(&expanded_paths[0])?.into_batch_iterator()?
    } else {
        Box::new(MultiFileIterator::new(
            expanded_paths,
            args.record_batch_size,
        )?)
    };

    let duckdb_converter = ArrowToDuckDbConverter::from_iterator(
        iterator,
        args.output.path().to_path_buf(),
        args.table_name,
    )
    .with_sort_spec(args.sort_by.unwrap_or_default())
    .with_query(args.query.clone())
    .with_dialect(args.dialect)
    .with_truncate(args.truncate)
    .with_drop_table(args.drop_table);

    duckdb_converter.convert().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        QueryDialect,
        utils::test_helpers::{file_helpers, test_data},
    };
    use clio::OutputPath;
    use duckdb::Connection;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_merge_arrow_to_duckdb_two_files() {
        let temp_dir = tempdir().unwrap();
        let input1_path = temp_dir.path().join("input1.arrow");
        let input2_path = temp_dir.path().join("input2.arrow");
        let output_path = temp_dir.path().join("output.db");

        let schema = test_data::simple_schema();
        let batch1 = test_data::create_batch_with_ids_and_names(
            &schema,
            &[1, 2, 3],
            &["Alice", "Bob", "Charlie"],
        );
        let batch2 = test_data::create_batch_with_ids_and_names(
            &schema,
            &[4, 5, 6],
            &["David", "Eve", "Frank"],
        );

        file_helpers::write_arrow_file(&input1_path, &schema, vec![batch1]).unwrap();
        file_helpers::write_arrow_file(&input2_path, &schema, vec![batch2]).unwrap();

        let args = MergeArrowToDuckdbArgs {
            inputs: vec![
                input1_path.to_string_lossy().to_string(),
                input2_path.to_string_lossy().to_string(),
            ],
            output: OutputPath::new(&output_path).unwrap(),
            table_name: "merged_data".to_string(),
            query: None,
            dialect: QueryDialect::default(),
            sort_by: None,
            truncate: false,
            drop_table: false,
            record_batch_size: 122_880,
        };

        run(args).await.unwrap();

        let conn = Connection::open(&output_path).unwrap();
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM merged_data", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 6);
    }

    #[tokio::test]
    async fn test_merge_arrow_to_duckdb_with_query() {
        let temp_dir = tempdir().unwrap();
        let input1_path = temp_dir.path().join("input1.arrow");
        let input2_path = temp_dir.path().join("input2.arrow");
        let output_path = temp_dir.path().join("output.db");

        let schema = test_data::simple_schema();
        let batch1 = test_data::create_batch_with_ids_and_names(
            &schema,
            &[1, 2, 3],
            &["Alice", "Bob", "Charlie"],
        );
        let batch2 = test_data::create_batch_with_ids_and_names(
            &schema,
            &[4, 5, 6],
            &["David", "Eve", "Frank"],
        );

        file_helpers::write_arrow_file(&input1_path, &schema, vec![batch1]).unwrap();
        file_helpers::write_arrow_file(&input2_path, &schema, vec![batch2]).unwrap();

        let args = MergeArrowToDuckdbArgs {
            inputs: vec![
                input1_path.to_string_lossy().to_string(),
                input2_path.to_string_lossy().to_string(),
            ],
            output: OutputPath::new(&output_path).unwrap(),
            table_name: "filtered_data".to_string(),
            query: Some("SELECT * FROM data WHERE id > 3".to_string()),
            dialect: QueryDialect::default(),
            sort_by: None,
            truncate: false,
            drop_table: false,
            record_batch_size: 122_880,
        };

        run(args).await.unwrap();

        let conn = Connection::open(&output_path).unwrap();
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM filtered_data", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3); // only ids 4, 5, 6

        let min_id: i32 = conn
            .query_row("SELECT MIN(id) FROM filtered_data", [], |row| row.get(0))
            .unwrap();
        assert_eq!(min_id, 4);
    }
}
