//! Parquet metadata and scans backed by `ObjectStore`.

use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider, datasource::file_format::parquet::ParquetFormat,
    prelude::SessionContext,
};
use parquet::arrow::{
    async_reader::{AsyncFileReader, ParquetObjectReader},
    parquet_to_arrow_schema,
};
use parquet::file::metadata::ParquetMetaData;
use tokio::sync::OnceCell;

use crate::{
    sources::data_source::{DataSource, SourceMetadata, object_store_table_provider},
    storage::InputObject,
};

pub struct ParquetDataSource {
    input: InputObject,
    metadata: OnceCell<SourceMetadata>,
}

impl ParquetDataSource {
    pub fn new(input: InputObject) -> Self {
        Self {
            input,
            metadata: OnceCell::new(),
        }
    }

    async fn metadata(&self) -> Result<&SourceMetadata> {
        self.metadata
            .get_or_try_init(|| async {
                let metadata = read_parquet_metadata(&self.input).await?;
                let file_metadata = metadata.file_metadata();
                let schema = parquet_to_arrow_schema(
                    file_metadata.schema_descr(),
                    file_metadata.key_value_metadata(),
                )?;
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let row_count = file_metadata.num_rows() as usize;
                Ok(SourceMetadata {
                    schema: Arc::new(schema),
                    row_count,
                })
            })
            .await
    }
}

pub(crate) async fn read_parquet_metadata(input: &InputObject) -> Result<Arc<ParquetMetaData>> {
    let location = input.location();
    let mut reader = ParquetObjectReader::new(
        Arc::clone(location.store().object_store()),
        location.path().clone(),
    )
    .with_file_size(input.metadata().size);
    Ok(reader.get_metadata(None).await?)
}

#[async_trait]
impl DataSource for ParquetDataSource {
    fn name(&self) -> &str {
        "parquet"
    }

    fn input(&self) -> &InputObject {
        &self.input
    }

    async fn schema(&self) -> Result<SchemaRef> {
        Ok(Arc::clone(&self.metadata().await?.schema))
    }

    async fn row_count(&self) -> Result<usize> {
        Ok(self.metadata().await?.row_count)
    }

    async fn as_table_provider(&self, _ctx: &SessionContext) -> Result<Arc<dyn TableProvider>> {
        Ok(object_store_table_provider(
            &self.input,
            self.metadata().await?,
            Arc::new(ParquetFormat::default()),
        ))
    }

    fn supports_table_provider(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::Ordering;

    use futures::StreamExt;
    use object_store::{
        ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path,
    };

    use crate::{
        storage::{ObjectLocation, StorageConfig, StorageContext, StoreHandle, StoreKind},
        utils::test_helpers::object_store::{CountingStore, RequestLog},
    };

    const TEST_PARQUET_PATH: &str = "tests/files/people.parquet";

    async fn source() -> ParquetDataSource {
        let storage = StorageContext::new(StorageConfig::default()).unwrap();
        let input = storage.resolve_input(TEST_PARQUET_PATH).await.unwrap();
        ParquetDataSource::new(input)
    }

    async fn memory_source() -> (ParquetDataSource, Arc<RequestLog>) {
        let bytes = std::fs::read(TEST_PARQUET_PATH).unwrap();
        let size = u64::try_from(bytes.len()).unwrap();
        let path = Path::from("people.data");
        let inner = InMemory::new();
        inner.put(&path, PutPayload::from(bytes)).await.unwrap();
        let (store, requests) = CountingStore::new(inner);
        let store: Arc<dyn ObjectStore> = Arc::new(store);
        let handle = Arc::new(StoreHandle::new(
            StoreKind::Gcs {
                bucket: "test".to_string(),
            },
            datafusion::execution::object_store::ObjectStoreUrl::parse("memory://test").unwrap(),
            store,
        ));
        let location = ObjectLocation::new(
            "memory://test/people.data".to_string(),
            "memory://test/people.data".to_string(),
            path.clone(),
            handle,
        );
        let meta = ObjectMeta {
            location: path,
            last_modified: chrono::Utc::now(),
            size,
            e_tag: None,
            version: None,
        };
        (
            ParquetDataSource::new(InputObject::new(location, meta)),
            requests,
        )
    }

    #[tokio::test]
    async fn test_name() {
        assert_eq!(source().await.name(), "parquet");
    }

    #[tokio::test]
    async fn test_as_table_provider() {
        let source = source().await;
        let ctx = SessionContext::new();
        crate::io_strategies::input_strategy::register_object_stores(
            &ctx,
            std::iter::once(&source as &dyn DataSource),
        );
        let table_provider = source.as_table_provider(&ctx).await.unwrap();
        assert!(!table_provider.schema().fields().is_empty());
    }

    #[tokio::test]
    async fn test_as_table_provider_can_be_queried() {
        let source = source().await;
        let ctx = SessionContext::new();
        crate::io_strategies::input_strategy::register_object_stores(
            &ctx,
            std::iter::once(&source as &dyn DataSource),
        );
        let table_provider = source.as_table_provider(&ctx).await.unwrap();
        ctx.register_table("test_table", table_provider).unwrap();

        let batches = ctx
            .sql("SELECT name FROM test_table WHERE id > 0")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[tokio::test]
    async fn reads_metadata_through_resolved_object_store_input() {
        let source = source().await;
        let count = source.row_count().await.unwrap();
        assert!(!source.schema().await.unwrap().fields().is_empty());

        let mut stream = source.as_stream().await.unwrap();
        let mut streamed = 0;
        while let Some(batch) = stream.next().await {
            streamed += batch.unwrap().num_rows();
        }
        assert_eq!(count, streamed);
    }

    #[tokio::test]
    async fn metadata_uses_ranges_without_another_head_or_full_get() {
        let (source, requests) = memory_source().await;

        assert!(source.row_count().await.unwrap() > 0);
        assert!(!requests.ranges.lock().unwrap().is_empty());
        assert_eq!(requests.full_gets.load(Ordering::SeqCst), 0);
        assert_eq!(requests.heads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn object_store_scan_applies_projection_and_predicate() {
        let (source, _) = memory_source().await;
        let strategy =
            crate::io_strategies::input_strategy::InputStrategy::Single(Box::new(source));
        let ctx = SessionContext::new();
        let provider = strategy.as_table_provider(&ctx, None).await.unwrap();
        ctx.register_table("people", provider).unwrap();

        let dataframe = ctx
            .sql("SELECT name FROM people WHERE id > 0")
            .await
            .unwrap();
        let plan = dataframe.create_physical_plan().await.unwrap();
        let plan = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        let batches = dataframe.collect().await.unwrap();

        assert_eq!(batches[0].num_columns(), 1);
        assert!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>() > 0);
        assert!(plan.contains("predicate=id@0 > 0"), "{plan}");
    }

    #[tokio::test]
    async fn test_row_count_written_file() {
        use crate::utils::test_data::{TestBatch, TestFile};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let batch = TestBatch::builder()
            .column_i32("id", &[1, 2, 3, 4, 5])
            .column_string("name", &["a", "b", "c", "d", "e"])
            .build();
        TestFile::write_parquet_batch(&path, &batch);

        let storage = StorageContext::new(StorageConfig::default()).unwrap();
        let input = storage
            .resolve_input(path.to_string_lossy().as_ref())
            .await
            .unwrap();
        let source = ParquetDataSource::new(input);
        assert_eq!(source.row_count().await.unwrap(), 5);
    }
}
