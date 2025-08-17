use crate::{
    MergeToArrowArgs,
    converters::arrow::ArrowConverter,
    utils::{
        arrow_io::{ArrowIPCReader, MultiFileIterator},
        filesystem::ensure_parent_dir_exists,
    },
};
use anyhow::{Result, bail};
use glob::glob;

pub async fn run(args: MergeToArrowArgs) -> Result<()> {
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

    let converter = ArrowConverter::from_iterator(iterator, args.output.path())
        .with_compression(args.compression)
        .with_sorting(args.sort_by.unwrap_or_default())
        .with_output_ipc_format(args.output_ipc_format)
        .with_query(args.query)
        .with_dialect(args.dialect);

    converter.convert().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ArrowCompression, QueryDialect,
        utils::{
            arrow_io::ArrowIPCFormat,
            test_helpers::{file_helpers, test_data, verify},
        },
    };
    use clio::OutputPath;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_merge_two_files() {
        let temp_dir = tempdir().unwrap();
        let input1_path = temp_dir.path().join("input1.arrow");
        let input2_path = temp_dir.path().join("input2.arrow");
        let output_path = temp_dir.path().join("output.arrow");

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

        let args = MergeToArrowArgs {
            inputs: vec![
                input1_path.to_string_lossy().to_string(),
                input2_path.to_string_lossy().to_string(),
            ],
            output: OutputPath::new(&output_path).unwrap(),
            query: None,
            dialect: QueryDialect::default(),
            sort_by: None,
            compression: ArrowCompression::None,
            record_batch_size: 122_880,
            output_ipc_format: ArrowIPCFormat::File,
        };

        run(args).await.unwrap();

        let batches = verify::read_output_file(&output_path).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 6);
    }

    #[tokio::test]
    async fn test_merge_with_glob_pattern() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("output.arrow");

        let schema = test_data::simple_schema();
        for i in 0..3 {
            let path = temp_dir.path().join(format!("data_{i}.arrow"));
            let batch = test_data::create_batch_with_ids_and_names(
                &schema,
                &[i * 2 + 1, i * 2 + 2],
                &[&format!("User{}", i * 2 + 1), &format!("User{}", i * 2 + 2)],
            );
            file_helpers::write_arrow_file(&path, &schema, vec![batch]).unwrap();
        }

        let glob_pattern = format!("{}/*.arrow", temp_dir.path().display());
        let args = MergeToArrowArgs {
            inputs: vec![glob_pattern],
            output: OutputPath::new(&output_path).unwrap(),
            query: None,
            dialect: QueryDialect::default(),
            sort_by: None,
            compression: ArrowCompression::None,
            record_batch_size: 122_880,
            output_ipc_format: ArrowIPCFormat::File,
        };

        run(args).await.unwrap();

        let batches = verify::read_output_file(&output_path).unwrap();
        assert_eq!(batches[0].num_rows(), 6);
    }

    #[tokio::test]
    async fn test_merge_schema_mismatch() {
        let temp_dir = tempdir().unwrap();
        let input1_path = temp_dir.path().join("input1.arrow");
        let input2_path = temp_dir.path().join("input2.arrow");
        let output_path = temp_dir.path().join("output.arrow");

        let schema1 = test_data::simple_schema();
        let schema2 = test_data::multi_column_for_sorting_schema();

        let batch1 =
            test_data::create_batch_with_ids_and_names(&schema1, &[1, 2], &["Alice", "Bob"]);
        let batch2 =
            test_data::create_multi_column_for_sorting_batch(&schema2, &[1, 2], &[100, 200]);

        file_helpers::write_arrow_file(&input1_path, &schema1, vec![batch1]).unwrap();
        file_helpers::write_arrow_file(&input2_path, &schema2, vec![batch2]).unwrap();

        let args = MergeToArrowArgs {
            inputs: vec![
                input1_path.to_string_lossy().to_string(),
                input2_path.to_string_lossy().to_string(),
            ],
            output: OutputPath::new(&output_path).unwrap(),
            query: None,
            dialect: QueryDialect::default(),
            sort_by: None,
            compression: ArrowCompression::None,
            record_batch_size: 122_880,
            output_ipc_format: ArrowIPCFormat::File,
        };

        let result = run(args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Schema mismatch"));
    }
}
