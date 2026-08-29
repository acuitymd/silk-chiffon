//! Input source registration, metadata validation, and deterministic unions.
//!
//! Each unique object store is registered before providers are built. Provider
//! futures run concurrently under a fixed bound, while `buffered` retains input
//! order for the resulting DataFusion union.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    catalog::TableProvider,
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream},
    prelude::{DataFrame, SessionContext},
};
use futures::{
    StreamExt, TryStreamExt,
    stream::{SelectAll, Stream, select_all},
};

use crate::sources::data_source::DataSource;

const PROVIDER_BUILD_CONCURRENCY: usize = 16;

struct MergedSendableRecordBatchStreams {
    schema: SchemaRef,
    inner: SelectAll<SendableRecordBatchStream>,
}

impl Stream for MergedSendableRecordBatchStreams {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl RecordBatchStream for MergedSendableRecordBatchStreams {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub enum InputStrategy {
    Single(Box<dyn DataSource>),
    Multiple(Vec<Box<dyn DataSource>>),
}

impl InputStrategy {
    pub async fn as_table_provider(
        &self,
        ctx: &SessionContext,
        _working_directory: Option<String>,
    ) -> Result<Arc<dyn TableProvider>> {
        match self {
            InputStrategy::Single(source) => {
                register_object_stores(ctx, std::iter::once(source.as_ref()));
                source.as_table_provider(ctx).await
            }
            InputStrategy::Multiple(sources) => {
                if sources.is_empty() {
                    anyhow::bail!("No sources provided");
                }

                register_object_stores(ctx, sources.iter().map(Box::as_ref));
                let providers: Vec<Arc<dyn TableProvider>> = futures::stream::iter(
                    sources.iter().map(|source| source.as_table_provider(ctx)),
                )
                .buffered(PROVIDER_BUILD_CONCURRENCY)
                .try_collect()
                .await?;

                let mut df: DataFrame = ctx.read_table(Arc::clone(&providers[0]))?;

                for provider in &providers[1..] {
                    df = df.union(ctx.read_table(Arc::clone(provider))?)?;
                }

                Ok(df.into_view())
            }
        }
    }

    pub async fn row_count(&self) -> Result<usize> {
        match self {
            InputStrategy::Single(source) => source.row_count().await,
            InputStrategy::Multiple(sources) => {
                let counts: Vec<usize> =
                    futures::stream::iter(sources.iter().map(|source| source.row_count()))
                        .buffered(PROVIDER_BUILD_CONCURRENCY)
                        .try_collect()
                        .await?;
                Ok(counts.into_iter().sum())
            }
        }
    }

    pub async fn validate_schemas(&self) -> Result<SchemaRef> {
        let sources = match self {
            InputStrategy::Single(source) => return source.schema().await,
            InputStrategy::Multiple(sources) if sources.is_empty() => {
                anyhow::bail!("No sources provided");
            }
            InputStrategy::Multiple(sources) => sources,
        };
        let schemas: Vec<SchemaRef> =
            futures::stream::iter(sources.iter().map(|source| source.schema()))
                .buffered(PROVIDER_BUILD_CONCURRENCY)
                .try_collect()
                .await?;
        let schema = Arc::clone(&schemas[0]);
        if let Some((index, _)) = schemas
            .iter()
            .enumerate()
            .skip(1)
            .find(|(_, candidate)| candidate.as_ref() != schema.as_ref())
        {
            anyhow::bail!(
                "Schema mismatch for input file {} (does not match other file(s))",
                sources[index].input().location().display()
            );
        }
        Ok(schema)
    }

    pub async fn as_stream(&self, ctx: &SessionContext) -> Result<SendableRecordBatchStream> {
        match self {
            InputStrategy::Single(source) => {
                register_object_stores(ctx, std::iter::once(source.as_ref()));
                source.as_stream_with_session_context(ctx).await
            }
            InputStrategy::Multiple(sources) => {
                if sources.is_empty() {
                    anyhow::bail!("No sources provided");
                }

                register_object_stores(ctx, sources.iter().map(Box::as_ref));
                let mut streams: Vec<SendableRecordBatchStream> = futures::stream::iter(
                    sources
                        .iter()
                        .map(|source| source.as_stream_with_session_context(ctx)),
                )
                .buffered(PROVIDER_BUILD_CONCURRENCY)
                .try_collect()
                .await?;
                let first_stream = streams.remove(0);
                let schema = first_stream.schema();

                for stream in &streams {
                    if stream.schema() != schema {
                        anyhow::bail!("Schemas do not match");
                    }
                }
                streams.insert(0, first_stream);

                let merged = MergedSendableRecordBatchStreams {
                    schema,
                    inner: select_all(streams),
                };

                Ok(Box::pin(merged))
            }
        }
    }
}

pub(crate) fn register_object_stores<'a>(
    ctx: &SessionContext,
    sources: impl IntoIterator<Item = &'a dyn DataSource>,
) -> usize {
    let mut registered = HashSet::new();
    for source in sources {
        let store = source.input().location().store();
        if registered.insert(store.store_url().as_str().to_string()) {
            ctx.register_object_store(store.store_url().as_ref(), Arc::clone(store.object_store()));
        }
    }
    registered.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use async_trait::async_trait;
    use datafusion::datasource::MemTable;
    use object_store::{
        ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path,
    };

    use crate::{
        sources::arrow::ArrowDataSource,
        storage::{InputObject, ObjectLocation, StoreHandle, StoreKind},
        utils::test_data::{TestBatch, TestFile},
    };

    fn input(handle: Arc<StoreHandle>, key: &str, size: u64) -> InputObject {
        let path = Path::from(key);
        InputObject::new(
            ObjectLocation::new(
                format!("memory://test/{key}"),
                format!("memory://test/{key}"),
                path.clone(),
                handle,
            ),
            ObjectMeta {
                location: path,
                last_modified: chrono::Utc::now(),
                size,
                e_tag: None,
                version: None,
            },
        )
    }

    fn memory_handle(store: Arc<dyn ObjectStore>) -> Arc<StoreHandle> {
        Arc::new(StoreHandle::new(
            StoreKind::Gcs {
                bucket: "test".to_string(),
            },
            datafusion::execution::object_store::ObjectStoreUrl::parse("memory://test").unwrap(),
            store,
        ))
    }

    async fn remote_arrow_source(path: &std::path::Path, key: &str) -> ArrowDataSource {
        let bytes = std::fs::read(path).unwrap();
        let size = u64::try_from(bytes.len()).unwrap();
        let store = InMemory::new();
        store
            .put(&Path::from(key), PutPayload::from(bytes))
            .await
            .unwrap();
        let handle = memory_handle(Arc::new(store));
        ArrowDataSource::new(input(handle, key, size))
    }

    #[tokio::test]
    async fn one_registered_store_is_shared_by_multiple_sources() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let handle = memory_handle(store);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let sources: Vec<Box<dyn DataSource>> = (0..2)
            .map(|value| {
                Box::new(DelayedSource::new(
                    input(Arc::clone(&handle), &format!("{value}.arrow"), 1),
                    Arc::clone(&schema),
                    value,
                    Duration::ZERO,
                    Arc::clone(&active),
                    Arc::clone(&max_active),
                )) as Box<dyn DataSource>
            })
            .collect();
        let ctx = SessionContext::new();

        assert_eq!(
            register_object_stores(&ctx, sources.iter().map(Box::as_ref)),
            1
        );
    }

    #[tokio::test]
    async fn mixed_local_and_memory_sources_scan_together() {
        let dir = tempfile::tempdir().unwrap();
        let local_path = dir.path().join("local.arrow");
        let remote_path = dir.path().join("remote.arrow");
        TestFile::write_arrow_batch(&local_path, &TestBatch::simple_with(&[1, 2], &["a", "b"]));
        TestFile::write_arrow_batch(&remote_path, &TestBatch::simple_with(&[3, 4], &["c", "d"]));
        let storage = crate::storage::StorageContext::new(Default::default()).unwrap();
        let local = ArrowDataSource::new(
            storage
                .resolve_input(local_path.to_string_lossy().as_ref())
                .await
                .unwrap(),
        );
        let remote = remote_arrow_source(&remote_path, "remote.data").await;
        let strategy = InputStrategy::Multiple(vec![Box::new(local), Box::new(remote)]);
        let ctx = SessionContext::new();

        let provider = strategy.as_table_provider(&ctx, None).await.unwrap();
        let batches = ctx.read_table(provider).unwrap().collect().await.unwrap();

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    }

    #[tokio::test]
    async fn schema_mismatch_names_the_later_input() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let handle = memory_handle(store);
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let first = DelayedSource::new(
            input(Arc::clone(&handle), "first.arrow", 1),
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            1,
            Duration::ZERO,
            Arc::clone(&active),
            Arc::clone(&max_active),
        );
        let second = DelayedSource::new(
            input(handle, "second.arrow", 1),
            Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)])),
            2,
            Duration::ZERO,
            active,
            max_active,
        );
        let strategy = InputStrategy::Multiple(vec![Box::new(first), Box::new(second)]);

        let error = strategy.validate_schemas().await.unwrap_err();

        assert!(error.to_string().contains("memory://test/second.arrow"));
    }

    #[tokio::test]
    async fn provider_builds_are_bounded_and_union_order_is_stable() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let handle = memory_handle(store);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let sources: Vec<Box<dyn DataSource>> = (0..20)
            .map(|value| {
                Box::new(DelayedSource::new(
                    input(Arc::clone(&handle), &format!("{value}.arrow"), 1),
                    Arc::clone(&schema),
                    value,
                    Duration::from_millis(u64::try_from(20 - value).unwrap()),
                    Arc::clone(&active),
                    Arc::clone(&max_active),
                )) as Box<dyn DataSource>
            })
            .collect();
        let strategy = InputStrategy::Multiple(sources);
        let ctx = SessionContext::new();

        let provider = strategy.as_table_provider(&ctx, None).await.unwrap();
        let batches = ctx.read_table(provider).unwrap().collect().await.unwrap();
        let values: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        assert_eq!(values, (0..20).collect::<Vec<_>>());
        assert!(max_active.load(Ordering::SeqCst) > 1);
        assert!(max_active.load(Ordering::SeqCst) <= PROVIDER_BUILD_CONCURRENCY);
    }

    struct DelayedSource {
        input: InputObject,
        schema: SchemaRef,
        value: i32,
        delay: Duration,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    impl DelayedSource {
        fn new(
            input: InputObject,
            schema: SchemaRef,
            value: i32,
            delay: Duration,
            active: Arc<AtomicUsize>,
            max_active: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                input,
                schema,
                value,
                delay,
                active,
                max_active,
            }
        }
    }

    #[async_trait]
    impl DataSource for DelayedSource {
        fn name(&self) -> &str {
            "delayed"
        }

        fn input(&self) -> &InputObject {
            &self.input
        }

        async fn schema(&self) -> Result<SchemaRef> {
            Ok(Arc::clone(&self.schema))
        }

        async fn row_count(&self) -> Result<usize> {
            Ok(1)
        }

        async fn as_table_provider(&self, _ctx: &SessionContext) -> Result<Arc<dyn TableProvider>> {
            let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            let batch = RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![Arc::new(Int32Array::from(vec![self.value]))],
            )?;
            Ok(Arc::new(MemTable::try_new(
                Arc::clone(&self.schema),
                vec![vec![batch]],
            )?))
        }
    }
}
