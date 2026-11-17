use std::{fmt, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{ColumnStatistics, Result, Statistics, stats::Precision},
    config::ConfigOptions,
    datasource::{
        file_format::{FileFormat, FileMeta, file_compression_type::FileCompressionType},
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSinkConfig, FileSource},
        source::DataSourceExec,
        table_schema::TableSchema,
    },
    execution::object_store::ObjectStoreUrl,
    logical_expr::{TableProviderFilterPushDown, col, lit},
    physical_expr::{
        LexOrdering, LexRequirement, PhysicalExpr, PhysicalSortExpr, expressions::Column,
    },
    physical_expr_adapter::DefaultPhysicalExprAdapterFactory,
    physical_plan::{
        ExecutionPlan,
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
        projection::ProjectionExprs,
    },
    prelude::{SessionConfig, SessionContext},
};
use datafusion_datasource::projection::SplitProjection;
use object_store::{ObjectMeta, ObjectStore, memory::InMemory};
use parking_lot::Mutex;
use silk_chiffon_core::ExactFileTableProviderBuilder;

struct CapturedScan {
    target_partitions: usize,
    config: FileScanConfig,
    store_registered: bool,
}

struct CapturingFormat {
    captured: Arc<Mutex<Option<CapturedScan>>>,
    filter_pushdown_calls: Arc<Mutex<Vec<Vec<String>>>>,
}

impl fmt::Debug for CapturingFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CapturingFormat")
    }
}

#[async_trait]
impl FileFormat for CapturingFormat {
    fn get_ext(&self) -> String {
        "capture".to_owned()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        Ok(self.get_ext())
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<Arc<Schema>> {
        unreachable!()
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: Arc<Schema>,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        unreachable!()
    }

    async fn infer_stats_and_ordering(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: Arc<Schema>,
        _object: &ObjectMeta,
    ) -> Result<FileMeta> {
        unreachable!()
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        config: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let store_registered = state
            .runtime_env()
            .object_store(&config.object_store_url)
            .is_ok();
        *self.captured.lock() = Some(CapturedScan {
            target_partitions: state.config_options().execution.target_partitions,
            config: config.clone(),
            store_registered,
        });
        Ok(DataSourceExec::from_data_source(config))
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unreachable!()
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(CapturingSource::new(
            table_schema,
            Arc::clone(&self.filter_pushdown_calls),
        ))
    }
}

#[derive(Clone)]
struct CapturingSource {
    table_schema: TableSchema,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
    filter_pushdown_calls: Arc<Mutex<Vec<Vec<String>>>>,
}

impl CapturingSource {
    fn new(table_schema: TableSchema, filter_pushdown_calls: Arc<Mutex<Vec<Vec<String>>>>) -> Self {
        let projection = SplitProjection::unprojected(&table_schema);
        Self {
            table_schema,
            projection,
            metrics: ExecutionPlanMetricsSet::new(),
            filter_pushdown_calls,
        }
    }
}

impl FileSource for CapturingSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        struct UnusedOpener;
        impl FileOpener for UnusedOpener {
            fn open(&self, _file: PartitionedFile) -> Result<FileOpenFuture> {
                unreachable!()
            }
        }
        Ok(Arc::new(UnusedOpener))
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        source.projection = SplitProjection::new(
            self.table_schema.file_schema(),
            &self.projection.source.try_merge(projection)?,
        );
        Ok(Some(Arc::new(source)))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "capture"
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        self.filter_pushdown_calls
            .lock()
            .push(filters.iter().map(ToString::to_string).collect());
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(
            vec![PushedDown::No; filters.len()],
        ))
    }
}

#[test]
fn provider_passes_retained_file_primitives_to_the_format_unchanged() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let statistics = Statistics {
        num_rows: Precision::Exact(12),
        total_byte_size: Precision::Inexact(1_024),
        column_statistics: vec![ColumnStatistics::new_unknown(); 2],
    };
    let file_statistics = Statistics {
        num_rows: Precision::Exact(12),
        total_byte_size: Precision::Exact(512),
        column_statistics: vec![ColumnStatistics::new_unknown(); 2],
    };
    let metadata = ObjectMeta {
        location: "exact/object.data".into(),
        last_modified: std::time::SystemTime::now().into(),
        size: 512,
        e_tag: Some("etag".to_owned()),
        version: Some("version".to_owned()),
    };
    let ordering: LexOrdering = [PhysicalSortExpr::new_default(Arc::new(Column::new(
        "value", 1,
    )))]
    .into();
    let captured = Arc::new(Mutex::new(None));
    let filter_pushdown_calls = Arc::new(Mutex::new(Vec::new()));
    let format = Arc::new(CapturingFormat {
        captured: Arc::clone(&captured),
        filter_pushdown_calls: Arc::clone(&filter_pushdown_calls),
    });
    let store_url = ObjectStoreUrl::parse("memory://root").unwrap();
    let provider = ExactFileTableProviderBuilder::new()
        .object_store_url(store_url.clone())
        .schema(Arc::clone(&schema))
        .files(vec![
            PartitionedFile::new_from_meta(metadata.clone())
                .with_statistics(Arc::new(file_statistics.clone())),
        ])
        .statistics(statistics.clone())
        .output_ordering(vec![ordering])
        .format(format)
        .expression_adapter_factory(Arc::new(DefaultPhysicalExprAdapterFactory))
        .build()
        .unwrap();
    let session = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(7));
    session
        .runtime_env()
        .register_object_store(store_url.as_ref(), Arc::new(InMemory::new()));

    let projection = vec![1];
    let plan = futures::executor::block_on(provider.scan(
        &session.state(),
        Some(&projection),
        &[],
        Some(5),
    ))
    .unwrap();
    let captured_guard = captured.lock();
    let captured_scan = captured_guard.as_ref().unwrap();

    assert_eq!(
        provider.table_type(),
        datafusion::logical_expr::TableType::Base
    );
    assert_eq!(
        provider.supports_filters_pushdown(&[&col("id")]).unwrap(),
        [TableProviderFilterPushDown::Inexact]
    );
    let debug = format!("{provider:?}");
    assert!(debug.contains("ExactFileTableProvider"));
    assert!(debug.contains("files: 1"));
    assert!(debug.contains("has_expression_adapter_factory: true"));
    assert_eq!(provider.schema(), schema);
    assert_eq!(provider.statistics(), Some(statistics));
    assert_eq!(plan.schema().fields().len(), 1);
    assert_eq!(captured_scan.target_partitions, 7);
    assert!(captured_scan.store_registered);
    assert_eq!(captured_scan.config.object_store_url, store_url);
    assert_eq!(captured_scan.config.limit, Some(5));
    assert_eq!(captured_scan.config.file_groups.len(), 1);
    assert_eq!(captured_scan.config.file_groups[0].len(), 1);
    let retained = &captured_scan.config.file_groups[0].files()[0];
    assert_eq!(retained.object_meta, metadata);
    assert_eq!(retained.statistics.as_deref(), Some(&file_statistics));
    assert_eq!(
        captured_scan.config.statistics().num_rows,
        Precision::Exact(12)
    );
    assert!(captured_scan.config.expr_adapter_factory.is_some());
    assert_eq!(captured_scan.config.output_ordering.len(), 1);
    drop(captured_guard);

    let frame = session
        .read_table(provider)
        .unwrap()
        .filter(col("id").gt(lit(5_i64)))
        .unwrap();
    futures::executor::block_on(frame.create_physical_plan()).unwrap();
    assert!(
        filter_pushdown_calls
            .lock()
            .iter()
            .any(|filters| filters.as_slice() == ["id@0 > 5"])
    );
}

#[test]
fn provider_rejects_an_empty_exact_file_set() {
    let schema = Arc::new(Schema::empty());
    let format = Arc::new(CapturingFormat {
        captured: Arc::new(Mutex::new(None)),
        filter_pushdown_calls: Arc::new(Mutex::new(Vec::new())),
    });

    let error = ExactFileTableProviderBuilder::new()
        .object_store_url(ObjectStoreUrl::local_filesystem())
        .schema(Arc::clone(&schema))
        .files(Vec::new())
        .statistics(Statistics::new_unknown(&schema))
        .output_ordering(Vec::new())
        .format(format)
        .build()
        .expect_err("an empty exact-file provider must be rejected");

    assert!(error.to_string().contains("requires at least one file"));
}
