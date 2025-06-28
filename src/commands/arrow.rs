use crate::{
    ArrowArgs, converters::arrow::ArrowConverter, utils::filesystem::ensure_parent_dir_exists,
};
use anyhow::Result;

pub async fn run(args: ArrowArgs) -> Result<()> {
    ensure_parent_dir_exists(args.output.path()).await?;

    let input_path = args.input.path().to_str().unwrap();
    let output_path = args.output.path();

    let converter = ArrowConverter::new(input_path, output_path)
        .with_compression(args.compression)
        .with_sorting(args.sort_by.unwrap_or_default())
        .with_record_batch_size(args.record_batch_size);

    converter.convert().await
}

#[cfg(test)]
mod tests {
    use crate::{
        ArrowArgs, ArrowCompression, SortDirection, SortSpec,
        commands::arrow::run,
        utils::{
            filesystem::ensure_parent_dir_exists,
            test_helpers::{file_helpers, test_data, verify},
        },
    };
    use arrow::array::{Array, Int32Array};
    use nix::unistd::Uid;
    use std::path::Path;
    use tempfile::tempdir;

    mod integration_tests {
        use super::*;

        #[tokio::test]
        async fn test_run_creates_parent_directory() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_dir = temp_dir.path().join("nested/subdir");
            let output_path = output_dir.join("output.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            // clio::OutputPath requires parent directory to exist first
            std::fs::create_dir_all(&output_dir).unwrap();

            let args = ArrowArgs {
                input: clio::Input::new(&input_path).unwrap(),
                output: clio::OutputPath::new(&output_path).unwrap(),
                sort_by: None,
                compression: ArrowCompression::None,
                record_batch_size: 122_880,
            };

            // remove the directory to test that run() creates it
            std::fs::remove_dir(&output_dir).unwrap();

            run(args).await.unwrap();

            assert!(output_dir.exists());
            assert!(output_path.exists());
        }

        #[tokio::test]
        async fn test_run_with_all_options() {
            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");
            let output_path = temp_dir.path().join("output.arrow");

            let test_ids = vec![3, 1, 2];
            let test_names = vec!["C", "A", "B"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_stream(&input_path, &schema, vec![batch]).unwrap();

            let sort_spec = SortSpec {
                columns: vec![crate::SortColumn {
                    name: "id".to_string(),
                    direction: SortDirection::Ascending,
                }],
            };

            let args = ArrowArgs {
                input: clio::Input::new(&input_path).unwrap(),
                output: clio::OutputPath::new(&output_path).unwrap(),
                sort_by: Some(sort_spec),
                compression: ArrowCompression::Zstd,
                record_batch_size: 122_880,
            };

            run(args).await.unwrap();

            let batches = verify::read_output_file(&output_path).unwrap();
            assert_eq!(batches.len(), 1);
            let ids = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(ids.value(0), 1);
            assert_eq!(ids.value(1), 2);
            assert_eq!(ids.value(2), 3);
        }

        #[tokio::test]
        async fn test_output_directory_creation_failure() {
            if Uid::effective().is_root() {
                // skip test if running as root
                return;
            }

            let temp_dir = tempdir().unwrap();
            let input_path = temp_dir.path().join("input.arrow");

            let test_ids = vec![1, 2, 3];
            let test_names = vec!["A", "B", "C"];

            let schema = test_data::simple_schema();
            let batch = test_data::create_batch_with_ids_and_names(&schema, &test_ids, &test_names);
            file_helpers::write_arrow_file(&input_path, &schema, vec![batch]).unwrap();

            let result = ensure_parent_dir_exists(Path::new("/root/no_permission")).await;
            assert!(result.is_err());
        }
    }
}
