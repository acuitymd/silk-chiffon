//! Internal fixtures shared by Silk Chiffon's tests and benchmarks.
//!
//! This crate is development-only. It deliberately knows concrete Arrow,
//! Parquet, and Vortex encodings so runtime crates do not carry fixture
//! behavior.

mod batch;
pub mod bigquery_arrow;
pub mod controlled_upload;
pub mod extract;
pub mod fault_injecting_store;
pub mod file;
pub mod output;
pub mod parquet;
pub mod read_probe_store;
pub mod vortex;

pub use batch::{StructColumnBuilder, TestBatch, TestBatchBuilder};
pub use extract::TestExtract;
pub use fault_injecting_store::{FaultInjectingStore, ObjectStoreOperation};
pub use file::TestFile;
pub use output::{local_storage_session, prepared_local_output_target};
pub use read_probe_store::ReadProbeStore;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, Int32Array},
        datatypes::{DataType, TimeUnit},
    };

    #[test]
    fn test_simple_preset() {
        let batch = TestBatch::simple();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema(), TestBatch::simple_schema());
    }

    #[test]
    fn test_simple_with_custom_data() {
        let batch = TestBatch::simple_with(&[10, 20], &["x", "y"]);
        assert_eq!(batch.num_rows(), 2);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 10);
        assert_eq!(ids.value(1), 20);
    }

    #[test]
    fn test_builder_with_multiple_types() {
        let batch = TestBatch::builder()
            .column_i32("a", &[1, 2])
            .column_i64("b", &[100, 200])
            .column_f64("c", &[1.5, 2.5])
            .column_string("d", &["x", "y"])
            .column_bool("e", &[true, false])
            .build();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn test_nullable_columns() {
        let batch = TestBatch::with_nullable_id(&[Some(1), None, Some(3)], &["a", "b", "c"]);
        assert_eq!(batch.num_rows(), 3);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(!ids.is_null(0));
        assert!(ids.is_null(1));
        assert!(!ids.is_null(2));
    }

    #[test]
    fn test_with_dates() {
        let batch = TestBatch::with_dates();
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Date32);
    }

    #[test]
    fn test_with_timestamps() {
        let batch = TestBatch::with_timestamps();
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(
            batch.schema().field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_with_structs() {
        let batch = TestBatch::with_structs();
        assert_eq!(batch.num_columns(), 2);

        let schema = batch.schema();
        let person_field = schema.field(1);
        assert_eq!(person_field.name(), "person");

        if let DataType::Struct(fields) = person_field.data_type() {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "name");
            assert_eq!(fields[1].name(), "age");
        } else {
            panic!("expected struct type");
        }
    }

    #[test]
    fn test_with_lists() {
        let batch = TestBatch::with_lists();
        assert_eq!(batch.num_columns(), 2);

        let schema = batch.schema();
        let tags_field = schema.field(1);
        assert!(matches!(tags_field.data_type(), DataType::List(_)));
    }

    #[test]
    fn test_for_partitioning() {
        let batch = TestBatch::for_partitioning();
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_for_sorting() {
        let batch = TestBatch::for_sorting();
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.schema(), TestBatch::for_sorting_schema());
    }

    #[test]
    fn test_extract_i32() {
        let batch = TestBatch::simple_with(&[10, 20, 30], &["a", "b", "c"]);
        let ids = TestExtract::i32(&batch, "id");
        assert_eq!(ids, vec![10, 20, 30]);
    }

    #[test]
    fn test_extract_string() {
        let batch = TestBatch::simple_with(&[1, 2], &["hello", "world"]);
        let names = TestExtract::string(&batch, "name");
        assert_eq!(names, vec!["hello", "world"]);
    }

    #[test]
    fn test_extract_nullable() {
        let batch = TestBatch::with_nullable_id(&[Some(1), None, Some(3)], &["a", "b", "c"]);
        let ids = TestExtract::i32_nullable(&batch, "id");
        assert_eq!(ids, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn test_file_round_trip_arrow() {
        let batch = TestBatch::simple();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.arrow");

        TestFile::write_arrow_batch(&path, &batch);
        let read_batches = TestFile::read_arrow(&path);

        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 3);
        assert_eq!(TestExtract::i32(&read_batches[0], "id"), vec![1, 2, 3]);
    }

    #[test]
    fn test_file_round_trip_parquet() {
        let batch = TestBatch::simple();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        TestFile::write_parquet_batch(&path, &batch);
        let read_batches = TestFile::read_parquet(&path);

        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 3);
        assert_eq!(TestExtract::i32(&read_batches[0], "id"), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_vortex_fixture_uses_the_upstream_writer() {
        let batch = TestBatch::simple();
        let bytes = vortex::encode_batches(&batch.schema(), vec![batch])
            .await
            .unwrap();

        assert_eq!(&bytes[..4], b"VTXF");
        assert_eq!(&bytes[bytes.len() - 4..], b"VTXF");
    }

    #[test]
    fn test_extract_all_batches() {
        let batch1 = TestBatch::simple_with(&[1, 2], &["a", "b"]);
        let batch2 = TestBatch::simple_with(&[3, 4], &["c", "d"]);
        let batches = vec![batch1, batch2];

        let ids = TestExtract::i32_all(&batches, "id");
        assert_eq!(ids, vec![1, 2, 3, 4]);

        let names = TestExtract::string_all(&batches, "name");
        assert_eq!(names, vec!["a", "b", "c", "d"]);
    }
}
