pub mod test_data {
    use std::{fs::File, sync::Arc};

    use arrow::{
        array::{Float64Array, Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
        ipc::writer::FileWriter,
    };

    pub fn simple_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    pub fn nullable_id_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    pub fn multi_column_for_sorting_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("group", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    pub fn create_batch_with_ids_and_names(
        schema: &SchemaRef,
        ids: &[i32],
        names: &[&str],
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    pub fn create_batch_with_nullable_ids_and_non_nullable_names(
        schema: &SchemaRef,
        ids: &[Option<i32>],
        names: &[&str],
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    pub fn create_multi_column_for_sorting_batch(
        schema: &Arc<Schema>,
        groups: &[i32],
        values: &[i32],
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(groups.to_vec())),
                Arc::new(Int32Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    pub fn create_arrow_file_with_range_of_ids(path: &std::path::Path, start_id: i32, count: i32) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let ids: Vec<i32> = (start_id..start_id + count).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("Person_{id}")).collect();
        let values: Vec<f64> = ids.iter().map(|id| f64::from(*id) * 1.5).collect();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap();

        let file = File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
}

pub mod file_helpers {
    use anyhow::Result;
    use arrow::{
        array::RecordBatch,
        datatypes::SchemaRef,
        ipc::writer::{FileWriter, StreamWriter},
    };
    use std::{fs::File, path::Path};

    pub fn write_arrow_file(
        path: &Path,
        schema: &SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = FileWriter::try_new(file, schema)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
        Ok(())
    }

    pub fn write_arrow_stream(
        path: &Path,
        schema: &SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let file = File::create(path)?;
        let mut writer = StreamWriter::try_new(file, schema)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
        Ok(())
    }

    pub fn write_invalid_file(path: &std::path::Path) -> Result<()> {
        std::fs::write(path, b"not an arrow file")?;
        Ok(())
    }
}

pub mod verify {
    use anyhow::Result;
    use arrow::{
        array::{Array, Int32Array, RecordBatch, StringArray},
        datatypes::Schema,
        ipc::reader::{FileReader, StreamReader},
    };
    use std::{collections::HashMap, fs::File, path::Path};

    use crate::utils::test_helpers::test_data;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    pub fn read_output_file(path: &Path) -> Result<Vec<RecordBatch>> {
        let file = File::open(path)?;
        let reader = FileReader::try_new_buffered(file, None)?;
        reader.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn read_parquet_file(path: &Path) -> Result<Vec<RecordBatch>> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }
        Ok(batches)
    }

    pub fn read_output_stream(path: &Path) -> Result<Vec<RecordBatch>> {
        let file = File::open(path)?;
        let reader = StreamReader::try_new(file, None)?;
        reader.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn read_file_metadata(path: &Path) -> Result<HashMap<String, String>> {
        let file = File::open(path)?;
        let reader = FileReader::try_new_buffered(file, None)?;
        let metadata = reader.custom_metadata().clone();
        Ok(metadata)
    }

    pub fn assert_schema_matches(actual: &Schema, expected: &Schema) {
        assert_eq!(actual.fields().len(), expected.fields().len());
        for (i, field) in expected.fields().iter().enumerate() {
            let actual_field = actual.field(i);

            assert_eq!(actual_field.name(), field.name());
            assert_eq!(actual_field.data_type(), field.data_type());
            assert_eq!(actual_field.is_nullable(), field.is_nullable());
        }
    }

    pub fn assert_id_name_batch_data_matches(
        batch: &RecordBatch,
        expected_ids: &[i32],
        expected_names: &[&str],
    ) {
        assert_schema_matches(&batch.schema(), &test_data::simple_schema());

        let id_column = batch.column_by_name("id").unwrap();
        let name_column = batch.column_by_name("name").unwrap();

        let ids = id_column.as_any().downcast_ref::<Int32Array>().unwrap();
        let names = name_column.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(ids.len(), expected_ids.len());
        assert_eq!(names.len(), expected_names.len());

        for (i, expected_id) in expected_ids.iter().enumerate() {
            assert_eq!(ids.value(i), *expected_id);
        }
        for (i, expected_name) in expected_names.iter().enumerate() {
            assert_eq!(names.value(i), *expected_name);
        }
    }

    pub fn extract_column_as_i32_vec(batch: &RecordBatch, column_name: &str) -> Vec<i32> {
        let column = batch.column_by_name(column_name).unwrap();
        let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
        (0..array.len()).map(|i| array.value(i)).collect()
    }
}

#[cfg(test)]
pub mod object_store {
    //! Counting in-memory object store for request-shape assertions.

    use std::{
        fmt,
        ops::Range,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use object_store::{
        CopyMode, CopyOptions, GetOptions, GetRange, GetResult, ListResult, MultipartUpload,
        ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        RenameOptions, Result as StoreResult, UploadPart, memory::InMemory, path::Path,
    };

    #[derive(Debug, Default)]
    pub struct RequestLog {
        pub ranges: Mutex<Vec<GetRange>>,
        pub full_gets: AtomicUsize,
        pub heads: AtomicUsize,
        pub multiparts: AtomicUsize,
        pub parts: AtomicUsize,
        pub aborts: AtomicUsize,
        pub fail_parts: AtomicBool,
        pub active_ranges: AtomicUsize,
        pub max_active_ranges: AtomicUsize,
    }

    #[derive(Debug)]
    pub struct CountingStore {
        inner: InMemory,
        requests: Arc<RequestLog>,
        emulate_gcs_013: bool,
        range_delay: Option<Duration>,
    }

    impl CountingStore {
        pub fn new(inner: InMemory) -> (Self, Arc<RequestLog>) {
            let requests = Arc::new(RequestLog::default());
            (
                Self {
                    inner,
                    requests: Arc::clone(&requests),
                    emulate_gcs_013: false,
                    range_delay: None,
                },
                requests,
            )
        }

        pub fn new_gcs_013(inner: InMemory) -> (Self, Arc<RequestLog>) {
            let (mut store, requests) = Self::new(inner);
            store.emulate_gcs_013 = true;
            (store, requests)
        }

        pub fn new_with_range_delay(
            inner: InMemory,
            range_delay: Duration,
        ) -> (Self, Arc<RequestLog>) {
            let (mut store, requests) = Self::new(inner);
            store.range_delay = Some(range_delay);
            (store, requests)
        }
    }

    pub async fn wait_for_count(counter: &AtomicUsize, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while counter.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for object-store request");
    }

    impl fmt::Display for CountingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("CountingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> StoreResult<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> StoreResult<Box<dyn MultipartUpload>> {
            self.requests.multiparts.fetch_add(1, Ordering::SeqCst);
            let inner = self.inner.put_multipart_opts(location, options).await?;
            Ok(Box::new(CountingMultipart {
                inner,
                requests: Arc::clone(&self.requests),
            }))
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
            if options.head {
                self.requests.heads.fetch_add(1, Ordering::SeqCst);
            } else if let Some(range) = &options.range {
                self.requests.ranges.lock().unwrap().push(range.clone());
                let active = self.requests.active_ranges.fetch_add(1, Ordering::SeqCst) + 1;
                self.requests
                    .max_active_ranges
                    .fetch_max(active, Ordering::SeqCst);
                if let Some(delay) = self.range_delay {
                    tokio::time::sleep(delay).await;
                }
                let result = self.inner.get_opts(location, options).await;
                self.requests.active_ranges.fetch_sub(1, Ordering::SeqCst);
                return result;
            } else {
                self.requests.full_gets.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> StoreResult<Vec<Bytes>> {
            self.requests
                .ranges
                .lock()
                .unwrap()
                .extend(ranges.iter().cloned().map(GetRange::Bounded));
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, StoreResult<Path>>,
        ) -> BoxStream<'static, StoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
            let mode = if self.emulate_gcs_013 {
                match options.mode {
                    CopyMode::Create => CopyMode::Overwrite,
                    CopyMode::Overwrite => CopyMode::Create,
                }
            } else {
                options.mode
            };
            self.inner
                .copy_opts(from, to, CopyOptions::new().with_mode(mode))
                .await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> StoreResult<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    #[derive(Debug)]
    struct CountingMultipart {
        inner: Box<dyn MultipartUpload>,
        requests: Arc<RequestLog>,
    }

    #[async_trait]
    impl MultipartUpload for CountingMultipart {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let future = self.inner.put_part(data);
            let requests = Arc::clone(&self.requests);
            Box::pin(async move {
                requests.parts.fetch_add(1, Ordering::SeqCst);
                if requests.fail_parts.load(Ordering::SeqCst) {
                    return Err(object_store::Error::Generic {
                        store: "counting test store",
                        source: "injected upload failure".into(),
                    });
                }
                future.await
            })
        }

        async fn complete(&mut self) -> StoreResult<PutResult> {
            self.inner.complete().await
        }

        async fn abort(&mut self) -> StoreResult<()> {
            self.requests.aborts.fetch_add(1, Ordering::SeqCst);
            self.inner.abort().await
        }
    }
}
