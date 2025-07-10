use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use silk_chiffon::utils::arrow_io::ArrowIPCFormat;
use silk_chiffon::{ArrowCompression, ListOutputsFormat, SplitToArrowArgs};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

fn create_test_data() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(vec!["A", "B", "A", "B", "C"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef,
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![6, 7, 8, 9, 10])) as ArrayRef,
            Arc::new(StringArray::from(vec!["A", "C", "B", "C", "A"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![60, 70, 80, 90, 100])) as ArrayRef,
        ],
    )
    .unwrap();

    (schema, vec![batch1, batch2])
}

fn write_test_arrow_file(path: &Path, schema: &Schema, batches: Vec<RecordBatch>) {
    let file = File::create(path).unwrap();
    let mut writer = FileWriter::try_new(file, schema).unwrap();
    for batch in batches {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
}

#[tokio::test]
async fn test_split_to_arrow_basic() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_dir = temp_dir.path().join("output");

    let (schema, batches) = create_test_data();
    write_test_arrow_file(&input_path, &schema, batches);

    let args = SplitToArrowArgs {
        input: clio::Input::new(&input_path).unwrap(),
        by: "category".to_string(),
        output_template: format!("{}/{{value}}.arrow", output_dir.display()),
        record_batch_size: 122_880,
        sort_by: None,
        create_dirs: true,
        overwrite: false,
        compression: ArrowCompression::None,
        list_outputs: ListOutputsFormat::None,
        output_ipc_format: ArrowIPCFormat::File,
    };

    silk_chiffon::commands::split_to_arrow::run(args)
        .await
        .unwrap();

    assert!(output_dir.join("A.arrow").exists());
    assert!(output_dir.join("B.arrow").exists());
    assert!(output_dir.join("C.arrow").exists());

    let reader_a = arrow::ipc::reader::FileReader::try_new(
        File::open(output_dir.join("A.arrow")).unwrap(),
        None,
    )
    .unwrap();
    let batches_a: Vec<_> = reader_a.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches_a.len(), 1);
    assert_eq!(batches_a[0].num_rows(), 4);

    let reader_b = arrow::ipc::reader::FileReader::try_new(
        File::open(output_dir.join("B.arrow")).unwrap(),
        None,
    )
    .unwrap();
    let batches_b: Vec<_> = reader_b.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches_b.len(), 1);
    assert_eq!(batches_b[0].num_rows(), 3);

    let reader_c = arrow::ipc::reader::FileReader::try_new(
        File::open(output_dir.join("C.arrow")).unwrap(),
        None,
    )
    .unwrap();
    let batches_c: Vec<_> = reader_c.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches_c.len(), 1);
    assert_eq!(batches_c[0].num_rows(), 3);
}

#[tokio::test]
async fn test_split_to_arrow_with_template_placeholders() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_dir = temp_dir.path().join("output");

    let (schema, batches) = create_test_data();
    write_test_arrow_file(&input_path, &schema, batches);

    let args = SplitToArrowArgs {
        input: clio::Input::new(&input_path).unwrap(),
        by: "category".to_string(),
        output_template: format!("{}/{{column}}_{{value}}_data.arrow", output_dir.display()),
        record_batch_size: 122_880,
        sort_by: None,
        create_dirs: true,
        overwrite: false,
        compression: ArrowCompression::None,
        list_outputs: ListOutputsFormat::None,
        output_ipc_format: ArrowIPCFormat::File,
    };

    silk_chiffon::commands::split_to_arrow::run(args)
        .await
        .unwrap();

    assert!(output_dir.join("category_A_data.arrow").exists());
    assert!(output_dir.join("category_B_data.arrow").exists());
    assert!(output_dir.join("category_C_data.arrow").exists());
}

#[tokio::test]
async fn test_split_to_arrow_with_sorting() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_dir = temp_dir.path().join("output");

    let (schema, batches) = create_test_data();
    write_test_arrow_file(&input_path, &schema, batches);

    let args = SplitToArrowArgs {
        input: clio::Input::new(&input_path).unwrap(),
        by: "category".to_string(),
        output_template: format!("{}/{{value}}.arrow", output_dir.display()),
        record_batch_size: 122_880,
        sort_by: Some("value:desc".parse().unwrap()),
        create_dirs: true,
        overwrite: false,
        compression: ArrowCompression::None,
        list_outputs: ListOutputsFormat::None,
        output_ipc_format: ArrowIPCFormat::File,
    };

    silk_chiffon::commands::split_to_arrow::run(args)
        .await
        .unwrap();

    let reader_a = arrow::ipc::reader::FileReader::try_new(
        File::open(output_dir.join("A.arrow")).unwrap(),
        None,
    )
    .unwrap();
    let batches_a: Vec<_> = reader_a.collect::<Result<Vec<_>, _>>().unwrap();
    let batch_a = &batches_a[0];

    let values = batch_a
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let values_vec: Vec<_> = values.iter().flatten().collect();
    assert_eq!(values_vec, vec![100, 60, 30, 10]);
}

#[tokio::test]
async fn test_split_to_arrow_with_int_column() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_dir = temp_dir.path().join("output");

    let (schema, batches) = create_test_data();
    write_test_arrow_file(&input_path, &schema, batches);

    let args = SplitToArrowArgs {
        input: clio::Input::new(&input_path).unwrap(),
        by: "id".to_string(),
        output_template: format!("{}/item_{{value}}.arrow", output_dir.display()),
        record_batch_size: 122_880,
        sort_by: None,
        create_dirs: true,
        overwrite: false,
        compression: ArrowCompression::None,
        list_outputs: ListOutputsFormat::None,
        output_ipc_format: ArrowIPCFormat::File,
    };

    silk_chiffon::commands::split_to_arrow::run(args)
        .await
        .unwrap();

    for i in 1..=10 {
        assert!(output_dir.join(format!("item_{i}.arrow")).exists());
    }
}

#[tokio::test]
async fn test_split_to_arrow_error_nonexistent_column() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");

    let (schema, batches) = create_test_data();
    write_test_arrow_file(&input_path, &schema, batches);

    let args = SplitToArrowArgs {
        input: clio::Input::new(&input_path).unwrap(),
        by: "nonexistent".to_string(),
        output_template: "{value}.arrow".to_string(),
        record_batch_size: 122_880,
        sort_by: None,
        create_dirs: true,
        overwrite: false,
        compression: ArrowCompression::None,
        list_outputs: ListOutputsFormat::None,
        output_ipc_format: ArrowIPCFormat::File,
    };

    let result = silk_chiffon::commands::split_to_arrow::run(args).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("No field named nonexistent")
    );
}

#[tokio::test]
async fn test_split_to_arrow_safe_value_placeholder() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_dir = temp_dir.path().join("output");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("path", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "file/path",
                "another:test",
                "normal",
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    write_test_arrow_file(&input_path, &schema, vec![batch]);

    let args = SplitToArrowArgs {
        input: clio::Input::new(&input_path).unwrap(),
        by: "path".to_string(),
        output_template: format!("{}/{{safe_value}}.arrow", output_dir.display()),
        record_batch_size: 122_880,
        sort_by: None,
        create_dirs: true,
        overwrite: false,
        compression: ArrowCompression::None,
        list_outputs: ListOutputsFormat::None,
        output_ipc_format: ArrowIPCFormat::File,
    };

    silk_chiffon::commands::split_to_arrow::run(args)
        .await
        .unwrap();

    assert!(output_dir.join("file_path.arrow").exists());
    assert!(output_dir.join("another_test.arrow").exists());
    assert!(output_dir.join("normal.arrow").exists());
}

#[tokio::test]
async fn test_split_to_arrow_stream_format() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_dir = temp_dir.path().join("output");

    let (schema, batches) = create_test_data();
    write_test_arrow_file(&input_path, &schema, batches);

    let args = SplitToArrowArgs {
        input: clio::Input::new(&input_path).unwrap(),
        by: "category".to_string(),
        output_template: format!("{}/{{value}}.arrows", output_dir.display()),
        record_batch_size: 122_880,
        sort_by: None,
        create_dirs: true,
        overwrite: false,
        compression: ArrowCompression::None,
        list_outputs: ListOutputsFormat::None,
        output_ipc_format: ArrowIPCFormat::Stream,
    };

    silk_chiffon::commands::split_to_arrow::run(args)
        .await
        .unwrap();

    assert!(output_dir.join("A.arrows").exists());
    assert!(output_dir.join("B.arrows").exists());
    assert!(output_dir.join("C.arrows").exists());

    let reader_a = arrow::ipc::reader::StreamReader::try_new(
        File::open(output_dir.join("A.arrows")).unwrap(),
        None,
    )
    .unwrap();
    let batches_a: Vec<_> = reader_a.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches_a.len(), 1);
    assert_eq!(batches_a[0].num_rows(), 4);

    let reader_b = arrow::ipc::reader::StreamReader::try_new(
        File::open(output_dir.join("B.arrows")).unwrap(),
        None,
    )
    .unwrap();
    let batches_b: Vec<_> = reader_b.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches_b.len(), 1);
    assert_eq!(batches_b[0].num_rows(), 3);

    let reader_c = arrow::ipc::reader::StreamReader::try_new(
        File::open(output_dir.join("C.arrows")).unwrap(),
        None,
    )
    .unwrap();
    let batches_c: Vec<_> = reader_c.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches_c.len(), 1);
    assert_eq!(batches_c[0].num_rows(), 3);
}

#[tokio::test]
async fn test_split_to_arrow_stream_format_with_compression() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.arrow");
    let output_dir = temp_dir.path().join("output");

    let (schema, batches) = create_test_data();
    write_test_arrow_file(&input_path, &schema, batches);

    let args = SplitToArrowArgs {
        input: clio::Input::new(&input_path).unwrap(),
        by: "category".to_string(),
        output_template: format!("{}/{{value}}.arrows", output_dir.display()),
        record_batch_size: 122_880,
        sort_by: Some("value:asc".parse().unwrap()),
        create_dirs: true,
        overwrite: false,
        compression: ArrowCompression::Lz4,
        list_outputs: ListOutputsFormat::None,
        output_ipc_format: ArrowIPCFormat::Stream,
    };

    silk_chiffon::commands::split_to_arrow::run(args)
        .await
        .unwrap();

    assert!(output_dir.join("A.arrows").exists());
    assert!(output_dir.join("B.arrows").exists());
    assert!(output_dir.join("C.arrows").exists());

    let reader_a = arrow::ipc::reader::StreamReader::try_new(
        File::open(output_dir.join("A.arrows")).unwrap(),
        None,
    )
    .unwrap();
    let batches_a: Vec<_> = reader_a.collect::<Result<Vec<_>, _>>().unwrap();
    let batch_a = &batches_a[0];

    let values = batch_a
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let values_vec: Vec<_> = values.iter().flatten().collect();
    assert_eq!(values_vec, vec![10, 30, 60, 100]);
}
