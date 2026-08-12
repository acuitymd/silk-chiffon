use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Weak},
};

use anyhow::{Context, Result};
use arrow::{
    buffer::Buffer,
    datatypes::SchemaRef,
    ipc::{
        Block, MetadataVersion,
        convert::fb_to_schema,
        reader::{FileDecoder, StreamDecoder},
    },
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{ColumnStatistics, Statistics, internal_datafusion_err, stats::Precision},
    datasource::{
        file_format::{FileFormat, FileMeta, file_compression_type::FileCompressionType},
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener, FileScanConfig, FileSinkConfig, FileSource},
        source::DataSourceExec,
        table_schema::TableSchema,
    },
    execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
    physical_expr::LexRequirement,
    physical_plan::{ExecutionPlan, metrics::ExecutionPlanMetricsSet, projection::ProjectionExprs},
    prelude::SessionContext,
};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use parking_lot::Mutex;
use silk_chiffon_core::{CanonicalInput, InputVariant, file_table_provider, register_input_store};
use silk_chiffon_storage::InputObject;
use tokio::sync::OnceCell;

use crate::sources::file::structurally_equal;

const SAMPLE_ROWS: usize = 100_000;
const MAX_IPC_MESSAGE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArrowIpcVariant {
    File,
    Stream,
}

impl ArrowIpcVariant {
    fn parse(variant: &InputVariant) -> Result<Self> {
        match variant.name() {
            Some("file") => Ok(Self::File),
            Some("stream") => Ok(Self::Stream),
            other => anyhow::bail!("unknown Arrow IPC input variant {other:?}"),
        }
    }
}

pub(crate) async fn create_provider(
    objects: &[InputObject],
    variant: &InputVariant,
    session: &SessionContext,
) -> Result<Arc<dyn TableProvider>> {
    let variant = ArrowIpcVariant::parse(variant)?;
    let representative = objects
        .iter()
        .max_by(|left, right| {
            left.metadata()
                .size
                .cmp(&right.metadata().size)
                .then_with(|| {
                    right
                        .handle()
                        .url()
                        .as_str()
                        .cmp(left.handle().url().as_str())
                })
        })
        .context("cannot build an empty Arrow input leaf")?;
    let representative_index = objects
        .iter()
        .position(|object| std::ptr::eq(object, representative))
        .expect("the representative came from the object slice");
    let (store_url, files) = register_input_store(session, objects)?;
    let store = session.runtime_env().object_store(&store_url)?;
    let active_files = Arc::new(ActiveFiles::default());
    let memory_pool = Arc::clone(&session.runtime_env().memory_pool);
    let format = Arc::new(ArrowIpcFormat {
        variant,
        active_files,
        memory_pool: Arc::clone(&memory_pool),
    });
    let representative_meta = &files[representative_index].object_meta;
    let representative_url = representative.handle().url().as_str();
    let schema = match variant {
        ArrowIpcVariant::File => {
            let lease = format.active_files.lease(representative_meta);
            Ok::<_, datafusion::common::DataFusionError>(Arc::clone(
                &lease
                    .get_or_try_init(|| {
                        read_file_layout(
                            &store,
                            representative_meta,
                            Arc::clone(&memory_pool),
                            representative_url,
                        )
                    })
                    .await?
                    .schema,
            ))
        }
        ArrowIpcVariant::Stream => {
            infer_stream_schema(&store, representative_meta, representative_url).await
        }
    }
    .with_context(|| {
        format!("while inferring Arrow schema from representative {representative_url}")
    })?;
    let statistics = match sample_statistics(
        variant,
        &store,
        &files[representative_index],
        &schema,
        files.iter().try_fold(0_u64, |total, file| {
            total
                .checked_add(file.object_meta.size)
                .context("Arrow input size overflow")
        })?,
        objects.len() == 1,
        memory_pool,
    )
    .await
    .with_context(|| format!("while sampling Arrow representative {representative_url}"))?
    {
        SampleStatistics::Available(statistics) => statistics,
        SampleStatistics::Unavailable => Statistics::new_unknown(&schema),
    };
    file_table_provider(
        store_url,
        schema,
        files,
        statistics,
        Vec::new(),
        format,
        None,
    )
    .map_err(Into::into)
}

#[derive(Debug)]
struct ArrowIpcFormat {
    variant: ArrowIpcVariant,
    active_files: Arc<ActiveFiles>,
    memory_pool: Arc<dyn MemoryPool>,
}

#[async_trait]
impl FileFormat for ArrowIpcFormat {
    fn get_ext(&self) -> String {
        "arrow".to_owned()
    }

    fn get_ext_with_compression(
        &self,
        compression: &FileCompressionType,
    ) -> datafusion::common::Result<String> {
        if compression.is_compressed() {
            return Err(internal_datafusion_err!(
                "Arrow IPC does not support file-level compression"
            ));
        }
        Ok(self.get_ext())
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::common::Result<SchemaRef> {
        let object = objects.first().ok_or_else(|| {
            internal_datafusion_err!("Arrow schema inference requires one object")
        })?;
        match self.variant {
            ArrowIpcVariant::File => {
                let lease = self.active_files.lease(object);
                let memory_pool = Arc::clone(&self.memory_pool);
                let identity = object.location.to_string();
                Ok(Arc::clone(
                    &lease
                        .get_or_try_init(|| read_file_layout(store, object, memory_pool, &identity))
                        .await?
                        .schema,
                ))
            }
            ArrowIpcVariant::Stream => {
                let identity = object.location.to_string();
                infer_stream_schema(store, object, &identity).await
            }
        }
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::common::Result<Statistics> {
        Ok(Statistics::new_unknown(&schema))
    }

    async fn infer_stats_and_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::common::Result<FileMeta> {
        Ok(FileMeta::new(
            self.infer_stats(state, store, schema, object).await?,
        ))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        config: FileScanConfig,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(DataSourceExec::from_data_source(config))
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _config: FileSinkConfig,
        _ordering: Option<LexRequirement>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Err(datafusion::common::DataFusionError::NotImplemented(
            "ArrowIpcFormat is input-only".to_owned(),
        ))
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(ArrowIpcSource {
            variant: self.variant,
            table_schema: table_schema.clone(),
            projection: SplitProjection::unprojected(&table_schema),
            metrics: ExecutionPlanMetricsSet::new(),
            active_files: Arc::clone(&self.active_files),
            memory_pool: Arc::clone(&self.memory_pool),
        })
    }
}

#[derive(Clone)]
struct ArrowIpcSource {
    variant: ArrowIpcVariant,
    table_schema: TableSchema,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
    active_files: Arc<ActiveFiles>,
    memory_pool: Arc<dyn MemoryPool>,
}

impl FileSource for ArrowIpcSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _config: &FileScanConfig,
        _partition: usize,
    ) -> datafusion::common::Result<Arc<dyn FileOpener>> {
        let projection = Some(self.projection.file_indices.clone());
        let opener: Arc<dyn FileOpener> = Arc::new(ArrowIpcOpener {
            variant: self.variant,
            object_store,
            projection,
            expected_schema: Arc::clone(self.table_schema.file_schema()),
            active_files: Arc::clone(&self.active_files),
            memory_pool: Arc::clone(&self.memory_pool),
        });
        ProjectionOpener::try_new(
            self.projection.clone(),
            opener,
            self.table_schema.file_schema(),
        )
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
    ) -> datafusion::common::Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        source.projection = SplitProjection::new(
            self.table_schema.file_schema(),
            &source.projection.source.try_merge(projection)?,
        );
        Ok(Some(Arc::new(source)))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        match self.variant {
            ArrowIpcVariant::File => "arrow",
            ArrowIpcVariant::Stream => "arrow_stream",
        }
    }

    fn supports_repartitioning(&self) -> bool {
        self.variant == ArrowIpcVariant::File
    }
}

struct ArrowIpcOpener {
    variant: ArrowIpcVariant,
    object_store: Arc<dyn ObjectStore>,
    projection: Option<Vec<usize>>,
    expected_schema: SchemaRef,
    active_files: Arc<ActiveFiles>,
    memory_pool: Arc<dyn MemoryPool>,
}

impl FileOpener for ArrowIpcOpener {
    fn open(&self, file: PartitionedFile) -> datafusion::common::Result<FileOpenFuture> {
        let canonical_url = file
            .extension::<CanonicalInput>()
            .expect("registered input files retain their canonical URL")
            .url()
            .to_string();
        let store = Arc::clone(&self.object_store);
        let projection = self.projection.clone();
        let expected_schema = Arc::clone(&self.expected_schema);
        let active_files = Arc::clone(&self.active_files);
        let memory_pool = Arc::clone(&self.memory_pool);
        let variant = self.variant;
        Ok(Box::pin(async move {
            let read_url = canonical_url.clone();
            let stream = match variant {
                ArrowIpcVariant::File => {
                    open_file(
                        store,
                        file,
                        read_url,
                        projection,
                        expected_schema,
                        active_files,
                        memory_pool,
                    )
                    .await
                }
                ArrowIpcVariant::Stream => {
                    open_stream(
                        store,
                        file,
                        read_url,
                        projection,
                        expected_schema,
                        memory_pool,
                    )
                    .await
                }
            }
            .map_err(|source| canonical_arrow_error(&canonical_url, &source))?;
            let stream_url = canonical_url.clone();
            Ok(
                Box::pin(stream.map_err(move |source| canonical_arrow_error(&stream_url, &source)))
                    as futures::stream::BoxStream<'static, _>,
            )
        }))
    }
}

fn canonical_arrow_error(
    canonical_url: &str,
    source: &datafusion::common::DataFusionError,
) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Execution(format!(
        "while reading input {canonical_url}: {source}"
    ))
}

async fn open_file(
    store: Arc<dyn ObjectStore>,
    file: PartitionedFile,
    canonical_url: String,
    projection: Option<Vec<usize>>,
    expected_schema: SchemaRef,
    active_files: Arc<ActiveFiles>,
    memory_pool: Arc<dyn MemoryPool>,
) -> datafusion::common::Result<
    futures::stream::BoxStream<'static, datafusion::common::Result<RecordBatch>>,
> {
    let lease = active_files.lease(&file.object_meta);
    let layout_memory_pool = Arc::clone(&memory_pool);
    let layout = Arc::clone(
        lease
            .get_or_try_init(|| {
                read_file_layout(
                    &store,
                    &file.object_meta,
                    layout_memory_pool,
                    &canonical_url,
                )
            })
            .await?,
    );
    if !structurally_equal(&expected_schema, &layout.schema) {
        return Err(datafusion::common::DataFusionError::Execution(format!(
            "Arrow input schema mismatch for {}: expected {expected_schema:?}, got {:?}",
            canonical_url, layout.schema
        )));
    }
    let blocks = layout
        .record_batches
        .iter()
        .copied()
        .filter(|block| {
            file.range
                .as_ref()
                .is_none_or(|range| block.offset() >= range.start && block.offset() < range.end)
        })
        .collect::<Vec<_>>();
    let stream = async_stream::try_stream! {
        let _lease = lease;
        let reservation = MemoryConsumer::new("Arrow IPC file reader").register(&memory_pool);
        let mut decoder = FileDecoder::new(Arc::clone(&layout.schema), layout.version);
        if let Some(projection) = projection {
            decoder = decoder.with_projection(projection);
        }
        let mut dictionary_bytes = 0usize;
        for block in &layout.dictionaries {
            dictionary_bytes = dictionary_bytes
                .checked_add(block_size(block)?)
                .ok_or_else(|| internal_datafusion_err!("Arrow IPC dictionary size overflows"))?;
            reservation.try_resize(dictionary_bytes)?;
            let data = read_block(&store, &file.object_meta.location, block).await?;
            decoder.read_dictionary(block, &Buffer::from(data))?;
        }
        for block in blocks {
            let block_bytes = block_size(&block)?;
            reservation.try_resize(
                dictionary_bytes
                    .checked_add(block_bytes)
                    .ok_or_else(|| internal_datafusion_err!("Arrow IPC reader size overflows"))?,
            )?;
            let bytes = read_block(&store, &file.object_meta.location, &block).await?;
            if let Some(batch) = decoder.read_record_batch(&block, &Buffer::from(bytes))? {
                reservation.try_resize(
                    dictionary_bytes
                        .checked_add(batch.get_array_memory_size())
                        .ok_or_else(|| internal_datafusion_err!("Arrow IPC batch size overflows"))?,
                )?;
                yield batch;
            }
            reservation.try_resize(dictionary_bytes)?;
        }
    };
    Ok(Box::pin(stream))
}

async fn open_stream(
    store: Arc<dyn ObjectStore>,
    file: PartitionedFile,
    canonical_url: String,
    projection: Option<Vec<usize>>,
    expected_schema: SchemaRef,
    memory_pool: Arc<dyn MemoryPool>,
) -> datafusion::common::Result<
    futures::stream::BoxStream<'static, datafusion::common::Result<RecordBatch>>,
> {
    if file.range.is_some() {
        return Err(internal_datafusion_err!(
            "Arrow IPC streams do not support byte-range partitions"
        ));
    }
    let input = store.get(&file.object_meta.location).await?.into_stream();
    let stream = async_stream::try_stream! {
        let reservation = MemoryConsumer::new("Arrow IPC stream reader").register(&memory_pool);
        let mut input = input;
        let mut decoder = StreamDecoder::new();
        let mut schema_checked = false;
        while let Some(chunk) = input.try_next().await? {
            reservation.try_resize(chunk.len())?;
            let mut buffer = Buffer::from(chunk);
            while !buffer.is_empty() {
                let batch = decoder.decode(&mut buffer)?;
                if !schema_checked && let Some(schema) = decoder.schema() {
                    if !structurally_equal(&expected_schema, &schema) {
                        Err(datafusion::common::DataFusionError::Execution(format!(
                            "Arrow input schema mismatch for {}: expected {expected_schema:?}, got {schema:?}",
                            canonical_url
                        )))?;
                    }
                    schema_checked = true;
                }
                if let Some(batch) = batch {
                    let batch = if let Some(projection) = &projection {
                        batch.project(projection)?
                    } else {
                        batch
                    };
                    reservation.try_resize(
                        reservation
                            .size()
                            .checked_add(batch.get_array_memory_size())
                            .ok_or_else(|| internal_datafusion_err!("Arrow IPC stream batch size overflows"))?,
                    )?;
                    yield batch;
                    reservation.try_resize(buffer.len())?;
                }
            }
            reservation.free();
        }
        decoder.finish()?;
    };
    Ok(Box::pin(stream))
}

#[derive(Debug)]
struct FileLayout {
    schema: SchemaRef,
    version: MetadataVersion,
    dictionaries: Vec<Block>,
    record_batches: Vec<Block>,
    _reservation: MemoryReservation,
}

async fn read_file_layout(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    memory_pool: Arc<dyn MemoryPool>,
    identity: &str,
) -> datafusion::common::Result<Arc<FileLayout>> {
    if object.size < 10 {
        return Err(datafusion::common::DataFusionError::Execution(format!(
            "Arrow IPC file {} is shorter than its trailer",
            identity
        )));
    }
    let trailer = store
        .get_range(&object.location, object.size - 10..object.size)
        .await?;
    let footer_len = arrow::ipc::reader::read_footer_length(
        trailer
            .as_ref()
            .try_into()
            .map_err(|_| internal_datafusion_err!("Arrow IPC trailer has the wrong length"))?,
    )?;
    let footer_len = u64::try_from(footer_len)
        .map_err(|_| internal_datafusion_err!("Arrow IPC footer length is invalid"))?;
    if footer_len + 10 > object.size {
        return Err(datafusion::common::DataFusionError::Execution(format!(
            "Arrow IPC footer length {footer_len} is invalid for {}",
            identity
        )));
    }
    let reservation = MemoryConsumer::new("Arrow IPC file layout").register(&memory_pool);
    reservation.try_resize(
        usize::try_from(footer_len)
            .map_err(|_| internal_datafusion_err!("Arrow IPC footer length exceeds usize"))?,
    )?;
    let footer_bytes = store
        .get_range(
            &object.location,
            object.size - 10 - footer_len..object.size - 10,
        )
        .await?;
    let footer = arrow::ipc::root_as_footer(&footer_bytes)
        .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))?;
    let schema =
        Arc::new(fb_to_schema(footer.schema().ok_or_else(|| {
            internal_datafusion_err!("Arrow IPC footer has no schema")
        })?));
    let version = footer.version();
    let dictionary_blocks = footer
        .dictionaries()
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    let record_batches = footer
        .recordBatches()
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    validate_blocks(
        object,
        identity,
        dictionary_blocks.iter().chain(&record_batches),
    )?;
    Ok(Arc::new(FileLayout {
        schema,
        version,
        dictionaries: dictionary_blocks,
        record_batches,
        _reservation: reservation,
    }))
}

fn validate_blocks<'a>(
    object: &ObjectMeta,
    identity: &str,
    blocks: impl Iterator<Item = &'a Block>,
) -> datafusion::common::Result<()> {
    for block in blocks {
        let offset = u64::try_from(block.offset())
            .map_err(|_| internal_datafusion_err!("Arrow IPC block offset is negative"))?;
        let metadata = u64::try_from(block.metaDataLength())
            .map_err(|_| internal_datafusion_err!("Arrow IPC metadata length is negative"))?;
        let body = u64::try_from(block.bodyLength())
            .map_err(|_| internal_datafusion_err!("Arrow IPC body length is negative"))?;
        let end = offset
            .checked_add(metadata)
            .and_then(|end| end.checked_add(body))
            .ok_or_else(|| internal_datafusion_err!("Arrow IPC block range overflows"))?;
        if end > object.size {
            return Err(internal_datafusion_err!(
                "Arrow IPC block range {offset}..{end} exceeds object size {} for {identity}",
                object.size
            ));
        }
    }
    Ok(())
}

async fn read_block(
    store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    block: &Block,
) -> datafusion::common::Result<Bytes> {
    let start = u64::try_from(block.offset())
        .map_err(|_| internal_datafusion_err!("Arrow IPC block offset is negative"))?;
    let length = u64::try_from(block_size(block)?)
        .map_err(|_| internal_datafusion_err!("Arrow IPC block length exceeds u64"))?;
    Ok(store.get_range(location, start..start + length).await?)
}

fn block_size(block: &Block) -> datafusion::common::Result<usize> {
    let metadata = usize::try_from(block.metaDataLength())
        .map_err(|_| internal_datafusion_err!("Arrow IPC metadata length is negative"))?;
    let body = usize::try_from(block.bodyLength())
        .map_err(|_| internal_datafusion_err!("Arrow IPC body length is negative"))?;
    metadata
        .checked_add(body)
        .ok_or_else(|| internal_datafusion_err!("Arrow IPC block length overflows"))
}

fn reserve_sample_block(
    reservation: &MemoryReservation,
    block: &Block,
) -> Result<SampleReservation> {
    let metadata =
        u64::try_from(block.metaDataLength()).context("Arrow IPC metadata length is negative")?;
    let body = u64::try_from(block.bodyLength()).context("Arrow IPC body length is negative")?;
    if metadata > MAX_IPC_MESSAGE_BYTES || body > MAX_IPC_MESSAGE_BYTES {
        return Ok(SampleReservation::SafetyBoundExceeded);
    }
    let size = metadata
        .checked_add(body)
        .context("Arrow IPC sample block size overflow")?;
    reservation.try_resize(usize::try_from(size)?)?;
    Ok(SampleReservation::Reserved)
}

async fn infer_stream_schema(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    identity: &str,
) -> datafusion::common::Result<SchemaRef> {
    let mut decoder = StreamDecoder::new();
    let mut offset = 0;
    loop {
        match read_stream_message(store, object, offset, identity, false).await? {
            StreamMessageRead::Message(message) => {
                offset = message.end;
                let mut buffer = Buffer::from(message.bytes);
                let _ = decoder.decode(&mut buffer)?;
                if let Some(schema) = decoder.schema() {
                    return Ok(schema);
                }
            }
            StreamMessageRead::End => break,
            StreamMessageRead::SafetyBoundExceeded => {
                unreachable!("schema inference does not request the sampling bound")
            }
        }
    }
    Err(datafusion::common::DataFusionError::Execution(format!(
        "Arrow IPC stream {} ended before its schema",
        identity
    )))
}

async fn sample_statistics(
    variant: ArrowIpcVariant,
    store: &Arc<dyn ObjectStore>,
    representative_file: &PartitionedFile,
    schema: &SchemaRef,
    selected_encoded_bytes: u64,
    single_object: bool,
    memory_pool: Arc<dyn MemoryPool>,
) -> Result<SampleStatistics> {
    let representative = &representative_file.object_meta;
    let identity = representative_file
        .extension::<CanonicalInput>()
        .map_or_else(
            || representative.location.to_string(),
            |input| input.url().to_string(),
        );
    let reservation =
        MemoryConsumer::new("Arrow IPC representative sampling").register(&memory_pool);
    let mut rows = 0usize;
    let mut decoded_bytes = 0usize;
    let mut column_bytes = vec![0usize; schema.fields().len()];
    let mut represented_encoded_bytes = 0u64;
    let mut reached_eof = true;
    match variant {
        ArrowIpcVariant::File => {
            let layout =
                read_file_layout(store, representative, Arc::clone(&memory_pool), &identity)
                    .await?;
            let mut decoder = FileDecoder::new(Arc::clone(&layout.schema), layout.version);
            for block in &layout.dictionaries {
                if reserve_sample_block(&reservation, block)?
                    == SampleReservation::SafetyBoundExceeded
                {
                    return Ok(SampleStatistics::Unavailable);
                }
                let data = read_block(store, &representative.location, block).await?;
                represented_encoded_bytes = represented_encoded_bytes
                    .checked_add(u64::try_from(data.len())?)
                    .context("Arrow sample byte count overflow")?;
                decoder.read_dictionary(block, &Buffer::from(data.clone()))?;
                reservation.free();
            }
            for (index, block) in layout.record_batches.iter().enumerate() {
                if reserve_sample_block(&reservation, block)?
                    == SampleReservation::SafetyBoundExceeded
                {
                    return Ok(SampleStatistics::Unavailable);
                }
                let data = read_block(store, &representative.location, block).await?;
                represented_encoded_bytes = represented_encoded_bytes
                    .checked_add(u64::try_from(data.len())?)
                    .context("Arrow sample byte count overflow")?;
                if let Some(batch) = decoder.read_record_batch(block, &Buffer::from(data))? {
                    reservation.try_resize(
                        reservation
                            .size()
                            .checked_add(batch.get_array_memory_size())
                            .context("Arrow sample reservation size overflow")?,
                    )?;
                    let target_reached =
                        record_sample(&batch, &mut rows, &mut decoded_bytes, &mut column_bytes)?;
                    if target_reached {
                        reached_eof = index + 1 == layout.record_batches.len();
                    }
                }
                reservation.free();
                if rows >= SAMPLE_ROWS {
                    break;
                }
            }
        }
        ArrowIpcVariant::Stream => {
            let mut decoder = StreamDecoder::new();
            let mut offset = 0;
            loop {
                let message =
                    match read_stream_message(store, representative, offset, &identity, true)
                        .await?
                    {
                        StreamMessageRead::Message(message) => message,
                        StreamMessageRead::End => break,
                        StreamMessageRead::SafetyBoundExceeded => {
                            return Ok(SampleStatistics::Unavailable);
                        }
                    };
                offset = message.end;
                reservation.try_resize(message.bytes.len())?;
                represented_encoded_bytes = represented_encoded_bytes
                    .checked_add(u64::try_from(message.bytes.len())?)
                    .context("Arrow sample byte count overflow")?;
                let mut buffer = Buffer::from(message.bytes);
                if let Some(batch) = decoder.decode(&mut buffer)? {
                    reservation.try_resize(
                        reservation
                            .size()
                            .checked_add(batch.get_array_memory_size())
                            .context("Arrow sample reservation size overflow")?,
                    )?;
                    record_sample(&batch, &mut rows, &mut decoded_bytes, &mut column_bytes)?;
                }
                reservation.free();
                if rows >= SAMPLE_ROWS {
                    reached_eof = offset >= representative.size;
                    break;
                }
            }
            if reached_eof {
                decoder.finish()?;
            }
        }
    }
    if rows == 0 || represented_encoded_bytes == 0 {
        return Ok(SampleStatistics::Unavailable);
    }
    let exact = single_object && reached_eof;
    let estimate = |sample| {
        sample_estimate(
            sample,
            selected_encoded_bytes,
            represented_encoded_bytes,
            exact,
        )
    };
    let precision = |value| {
        if exact {
            Precision::Exact(value)
        } else {
            Precision::Inexact(value)
        }
    };
    let column_statistics = column_bytes
        .into_iter()
        .map(|bytes| {
            Ok(ColumnStatistics {
                byte_size: precision(estimate(bytes)?),
                ..ColumnStatistics::new_unknown()
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SampleStatistics::Available(Statistics {
        num_rows: precision(estimate(rows)?),
        total_byte_size: precision(estimate(decoded_bytes)?),
        column_statistics,
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SampleReservation {
    Reserved,
    SafetyBoundExceeded,
}

enum SampleStatistics {
    Available(Statistics),
    Unavailable,
}

struct StreamMessage {
    bytes: Bytes,
    end: u64,
}

enum StreamMessageRead {
    End,
    Message(StreamMessage),
    SafetyBoundExceeded,
}

async fn read_stream_message(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    offset: u64,
    identity: &str,
    bounded: bool,
) -> datafusion::common::Result<StreamMessageRead> {
    if offset == object.size {
        return Ok(StreamMessageRead::End);
    }
    if object.size.saturating_sub(offset) < 4 {
        return Err(internal_datafusion_err!(
            "Arrow IPC stream {} ends inside a message header",
            identity
        ));
    }
    let first = store
        .get_range(&object.location, offset..offset + 4)
        .await?;
    let first = u32::from_le_bytes(first.as_ref().try_into().map_err(|_| {
        internal_datafusion_err!("Arrow IPC stream message header has the wrong length")
    })?);
    let (header_len, metadata_len) = if first == u32::MAX {
        if object.size.saturating_sub(offset) < 8 {
            return Err(internal_datafusion_err!(
                "Arrow IPC stream {} ends after a continuation marker",
                identity
            ));
        }
        let length = store
            .get_range(&object.location, offset + 4..offset + 8)
            .await?;
        (
            8_u64,
            u64::from(u32::from_le_bytes(length.as_ref().try_into().map_err(
                |_| internal_datafusion_err!("Arrow IPC stream message length is malformed"),
            )?)),
        )
    } else {
        (4_u64, u64::from(first))
    };
    if metadata_len == 0 {
        let end = offset + header_len;
        let bytes = store.get_range(&object.location, offset..end).await?;
        return Ok(StreamMessageRead::Message(StreamMessage { bytes, end }));
    }
    let metadata_start = offset
        .checked_add(header_len)
        .ok_or_else(|| internal_datafusion_err!("Arrow IPC stream range overflows"))?;
    let metadata_end = metadata_start
        .checked_add(metadata_len)
        .ok_or_else(|| internal_datafusion_err!("Arrow IPC stream range overflows"))?;
    if metadata_end > object.size {
        return Err(internal_datafusion_err!(
            "Arrow IPC stream {} ends inside message metadata",
            identity
        ));
    }
    if bounded && metadata_len > MAX_IPC_MESSAGE_BYTES {
        return Ok(StreamMessageRead::SafetyBoundExceeded);
    }
    let metadata = store
        .get_range(&object.location, metadata_start..metadata_end)
        .await?;
    let message = arrow::ipc::root_as_message(&metadata)
        .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))?;
    let body_len = u64::try_from(message.bodyLength())
        .map_err(|_| internal_datafusion_err!("Arrow IPC stream body length is negative"))?;
    let end = metadata_end
        .checked_add(body_len)
        .ok_or_else(|| internal_datafusion_err!("Arrow IPC stream range overflows"))?;
    if end > object.size {
        return Err(internal_datafusion_err!(
            "Arrow IPC stream {} ends inside a message body",
            identity
        ));
    }
    if bounded && body_len > MAX_IPC_MESSAGE_BYTES {
        return Ok(StreamMessageRead::SafetyBoundExceeded);
    }
    let bytes = store.get_range(&object.location, offset..end).await?;
    Ok(StreamMessageRead::Message(StreamMessage { bytes, end }))
}

fn record_sample(
    batch: &RecordBatch,
    rows: &mut usize,
    decoded_bytes: &mut usize,
    column_bytes: &mut [usize],
) -> Result<bool> {
    *rows = rows
        .checked_add(batch.num_rows())
        .context("Arrow sample row count overflow")?;
    *decoded_bytes = decoded_bytes
        .checked_add(batch.get_array_memory_size())
        .context("Arrow sample decoded byte count overflow")?;
    for (total, column) in column_bytes.iter_mut().zip(batch.columns()) {
        *total = total
            .checked_add(column.get_array_memory_size())
            .context("Arrow sample column byte count overflow")?;
    }
    Ok(*rows >= SAMPLE_ROWS)
}

fn scale(sample: usize, total: u64, represented: u64) -> Result<usize> {
    let numerator = u128::try_from(sample)?
        .checked_mul(u128::from(total))
        .context("Arrow statistics scaling overflow")?;
    let estimate = numerator.div_ceil(u128::from(represented));
    usize::try_from(estimate).context("Arrow statistics estimate exceeds usize")
}

fn sample_estimate(sample: usize, total: u64, represented: u64, exact: bool) -> Result<usize> {
    if exact {
        Ok(sample)
    } else {
        scale(sample, total, represented)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ObjectIdentity {
    location: String,
    size: u64,
    last_modified: i64,
    e_tag: Option<String>,
    version: Option<String>,
}

impl From<&ObjectMeta> for ObjectIdentity {
    fn from(meta: &ObjectMeta) -> Self {
        Self {
            location: meta.location.to_string(),
            size: meta.size,
            last_modified: meta.last_modified.timestamp_nanos_opt().unwrap_or(i64::MAX),
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        }
    }
}

type LayoutCell = OnceCell<Arc<FileLayout>>;

#[derive(Default)]
struct ActiveFiles {
    entries: Mutex<HashMap<ObjectIdentity, Weak<LayoutCell>>>,
}

impl fmt::Debug for ActiveFiles {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveFiles")
            .field("entries", &self.entries.lock().len())
            .finish()
    }
}

impl ActiveFiles {
    fn lease(&self, meta: &ObjectMeta) -> Arc<LayoutCell> {
        let identity = ObjectIdentity::from(meta);
        let mut entries = self.entries.lock();
        entries.retain(|_, entry| entry.strong_count() > 0);
        if let Some(lease) = entries.get(&identity).and_then(Weak::upgrade) {
            return lease;
        }
        let lease = Arc::new(OnceCell::new());
        entries.insert(identity, Arc::downgrade(&lease));
        lease
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::NullArray,
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use object_store::{ObjectStoreExt, memory::InMemory};

    use super::*;

    fn batch(rows: usize) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("value", DataType::Null, true)])),
            vec![Arc::new(NullArray::new(rows))],
        )
        .unwrap()
    }

    fn sample_batch(batch: &RecordBatch, rows: &mut usize) -> bool {
        record_sample(batch, rows, &mut 0, &mut [0]).unwrap()
    }

    fn object(location: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: location.into(),
            last_modified: chrono::Utc::now(),
            size,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn sample_scaling_rounds_up() {
        assert_eq!(scale(3, 5, 2).unwrap(), 8);
    }

    #[test]
    fn complete_single_file_samples_are_not_scaled_by_container_overhead() {
        assert_eq!(sample_estimate(3, 1_000, 500, true).unwrap(), 3);
        assert_eq!(sample_estimate(3, 1_000, 500, false).unwrap(), 6);
    }

    #[test]
    fn sampling_stops_at_the_row_target_after_recording_the_complete_batch() {
        let mut rows = 0;
        assert!(sample_batch(&batch(SAMPLE_ROWS), &mut rows));
        assert_eq!(rows, SAMPLE_ROWS);

        let mut rows = 0;
        assert!(!sample_batch(&batch(SAMPLE_ROWS - 1), &mut rows));
        assert!(sample_batch(&batch(2), &mut rows));
        assert_eq!(rows, SAMPLE_ROWS + 1);

        let mut rows = 0;
        assert!(sample_batch(&batch(SAMPLE_ROWS * 2), &mut rows));
        assert_eq!(rows, SAMPLE_ROWS * 2);
    }

    #[test]
    fn oversized_message_makes_sampling_unavailable_without_reserving_it() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(usize::MAX));
        let reservation = MemoryConsumer::new("test").register(&pool);
        let block = Block::new(0, 0, i64::try_from(MAX_IPC_MESSAGE_BYTES + 1).unwrap());
        let outcome = reserve_sample_block(&reservation, &block).unwrap();

        assert_eq!(outcome, SampleReservation::SafetyBoundExceeded);
        assert_eq!(reservation.size(), 0);
    }

    #[test]
    fn active_file_registry_prunes_dead_leases() {
        let registry = ActiveFiles::default();
        let meta = object("one.arrow", 1);
        drop(registry.lease(&meta));
        let other = object("two.arrow", 1);
        let _lease = registry.lease(&other);
        assert_eq!(registry.entries.lock().len(), 1);
    }

    #[test]
    fn active_file_registry_does_not_grow_with_completed_files() {
        let registry = ActiveFiles::default();
        for index in 0..10_000 {
            drop(registry.lease(&object(&format!("{index}.arrow"), 1)));
        }
        let _live = registry.lease(&object("live.arrow", 1));

        assert_eq!(registry.entries.lock().len(), 1);
    }

    #[tokio::test]
    async fn malformed_representative_samples_are_not_downgraded_to_unknown_statistics() {
        let batch = batch(1);
        let mut bytes = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut bytes, batch.schema().as_ref())
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        bytes.pop();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location: object_store::path::Path = "malformed.arrow".into();
        store
            .put(&location, Bytes::from(bytes.clone()).into())
            .await
            .unwrap();
        let object = object("malformed.arrow", u64::try_from(bytes.len()).unwrap());
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(usize::MAX));

        let error = sample_statistics(
            ArrowIpcVariant::Stream,
            &store,
            &PartitionedFile::new_from_meta(object.clone()),
            &batch.schema(),
            object.size,
            true,
            pool,
        )
        .await
        .err()
        .expect("malformed input must fail sampling");

        let message = format!("{error:#}");
        assert!(
            message.contains("Arrow IPC stream malformed.arrow ends"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn cancelled_or_failed_layout_initialization_is_retryable() {
        use std::future::pending;

        fn layout() -> Arc<FileLayout> {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(usize::MAX));
            Arc::new(FileLayout {
                schema: Arc::new(Schema::empty()),
                version: MetadataVersion::V5,
                dictionaries: Vec::new(),
                record_batches: Vec::new(),
                _reservation: MemoryConsumer::new("test layout").register(&pool),
            })
        }

        let cell = Arc::new(LayoutCell::new());
        let failed = cell
            .get_or_try_init(|| async {
                Err::<Arc<FileLayout>, _>(internal_datafusion_err!("failed initialization"))
            })
            .await;
        assert!(failed.is_err());
        assert!(cell.get().is_none());
        assert!(
            cell.get_or_try_init(|| async {
                Ok::<_, datafusion::common::DataFusionError>(layout())
            })
            .await
            .is_ok()
        );

        let cell = Arc::new(LayoutCell::new());
        let initializing = Arc::clone(&cell);
        let task = tokio::spawn(async move {
            initializing
                .get_or_try_init(pending::<datafusion::common::Result<Arc<FileLayout>>>)
                .await
                .map(|_| ())
        });
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(cell.get().is_none());
        assert!(
            cell.get_or_try_init(|| async {
                Ok::<_, datafusion::common::DataFusionError>(layout())
            })
            .await
            .is_ok()
        );
    }
}
