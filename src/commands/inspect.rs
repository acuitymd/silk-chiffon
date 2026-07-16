//! Inspect command for examining file metadata and structure.

use std::{
    io::{self, Write},
    sync::Arc,
};

use anyhow::{Result, anyhow};

use crate::{
    InspectArrowArgs, InspectIdentifyArgs, InspectParquetArgs, InspectSubcommand,
    InspectVortexArgs, StorageConfig,
    inspection::{
        arrow::ArrowInspector, detect_format, inspectable::Inspectable, parquet::ParquetInspector,
        vortex::VortexInspector,
    },
    storage::StorageContext,
};

pub async fn run(command: InspectSubcommand) -> Result<()> {
    run_with_storage(command, &StorageConfig::default()).await
}

pub async fn run_with_storage(
    command: InspectSubcommand,
    storage_config: &StorageConfig,
) -> Result<()> {
    let storage = Arc::new(StorageContext::new(*storage_config)?);
    run_with_storage_context(command, storage).await
}

pub(crate) async fn run_with_storage_context(
    command: InspectSubcommand,
    storage: Arc<StorageContext>,
) -> Result<()> {
    match &command {
        InspectSubcommand::Identify(args) => run_identify(args, &storage).await,
        InspectSubcommand::Parquet(args) => run_parquet(args, &storage).await,
        InspectSubcommand::Arrow(args) => run_arrow(args, &storage).await,
        InspectSubcommand::Vortex(args) => run_vortex(args, &storage).await,
    }
}

async fn run_identify(args: &InspectIdentifyArgs, storage: &StorageContext) -> Result<()> {
    let input = storage.resolve_input(&args.file).await?;
    let format = detect_format(&input).await?;

    if args.format.resolves_to_json() {
        println!("{}", serde_json::to_string(&format.to_json())?);
    } else {
        println!("{}", format);
    }

    Ok(())
}

async fn run_parquet(args: &InspectParquetArgs, storage: &StorageContext) -> Result<()> {
    let input = storage.resolve_input(&args.file).await?;
    let mut inspector = ParquetInspector::open(&input)
        .await
        .map_err(|e| anyhow!("Failed to open Parquet file: {}", e))?;

    let mut out = io::stdout();

    let columns_filter: Option<Vec<&str>> = args.pages.as_ref().and_then(|cols| {
        if cols.is_empty() {
            None
        } else {
            Some(cols.split(',').map(|s| s.trim()).collect())
        }
    });

    if args.format.resolves_to_json() {
        if let Some(columns) = columns_filter.as_deref() {
            inspector.validate_columns(None, columns)?;
        }
        inspector.load_pages(None, None).await?;
    } else if args.pages.is_some() {
        inspector
            .load_pages(Some(args.row_group), columns_filter.as_deref())
            .await?;
    }

    if args.format.resolves_to_json() {
        if args.pages.is_some() {
            let json = inspector.to_json_with_pages(columns_filter.as_deref())?;
            writeln!(out, "{}", serde_json::to_string(&json)?)?;
        } else {
            inspector.render_to_json(&mut out)?;
        }
    } else {
        inspector.render_with_row_group(&mut out, args.row_group)?;

        if args.pages.is_some() {
            inspector.render_pages(&mut out, args.row_group, columns_filter.as_deref())?;
        }
    }

    out.flush()?;
    Ok(())
}

async fn run_arrow(args: &InspectArrowArgs, storage: &StorageContext) -> Result<()> {
    let input = storage.resolve_input(&args.file).await?;
    let inspector = ArrowInspector::open(&input, args.row_count || args.batches)
        .await
        .map_err(|e| anyhow!("Failed to open Arrow file: {}", e))?;

    let mut out = io::stdout();

    if args.format.resolves_to_json() {
        inspector.render_to_json(&mut out)?;
        return Ok(());
    }

    inspector.render_default(&mut out)?;

    if args.batches {
        inspector.render_batches(&mut out)?;
    }

    out.flush()?;
    Ok(())
}

async fn run_vortex(args: &InspectVortexArgs, storage: &StorageContext) -> Result<()> {
    let input = storage.resolve_input(&args.file).await?;
    let inspector = VortexInspector::open(&input)
        .await
        .map_err(|e| anyhow!("Failed to open Vortex file: {}", e))?;

    let mut out = io::stdout();

    if args.format.resolves_to_json() {
        inspector.render_to_json(&mut out)?;
        return Ok(());
    }

    inspector.render_default(&mut out)?;

    if args.schema {
        inspector.render_schema(&mut out)?;
    }

    if args.stats {
        inspector.render_stats(&mut out)?;
    }

    if args.layout {
        inspector.render_layout(&mut out)?;
    }

    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

    use crate::{OutputFormat, utils::test_helpers::object_store::CountingStore};

    #[tokio::test]
    async fn arrow_errors_name_remote_location() {
        let store = InMemory::new();
        store
            .put(
                &Path::from("truncated.arrow"),
                PutPayload::from_static(b"ARROW1"),
            )
            .await
            .unwrap();
        let storage = Arc::new(StorageContext::with_gcs_store(
            StorageConfig::default(),
            Arc::new(store),
        ));
        let command = InspectSubcommand::Arrow(InspectArrowArgs {
            file: "gs://inspect-tests/truncated.arrow".to_string(),
            batches: false,
            format: OutputFormat::Json,
            row_count: false,
        });

        let error = run_with_storage_context(command, storage)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("gs://inspect-tests/truncated.arrow"),
            "{error}"
        );
        assert!(error.contains("trailer"), "{error}");
    }

    #[tokio::test]
    async fn parquet_json_validates_remote_columns_before_page_fetch() {
        let inner = InMemory::new();
        inner
            .put(
                &Path::from("people.parquet"),
                PutPayload::from(std::fs::read("tests/files/people.parquet").unwrap()),
            )
            .await
            .unwrap();
        let (store, requests) = CountingStore::new(inner);
        let store: Arc<dyn ObjectStore> = Arc::new(store);
        let storage = Arc::new(StorageContext::with_gcs_store(
            StorageConfig::default(),
            store,
        ));
        let input = storage
            .resolve_input("gs://inspect-tests/people.parquet")
            .await
            .unwrap();
        ParquetInspector::open(&input).await.unwrap();
        let footer_ranges = requests.ranges.lock().unwrap().clone();
        requests.ranges.lock().unwrap().clear();
        let command = InspectSubcommand::Parquet(InspectParquetArgs {
            file: "gs://inspect-tests/people.parquet".to_string(),
            format: OutputFormat::Json,
            row_group: 0,
            pages: Some("missing".to_string()),
        });

        let error = run_with_storage_context(command, storage)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("column missing"));
        assert!(error.contains("gs://inspect-tests/people.parquet"));
        assert_eq!(*requests.ranges.lock().unwrap(), footer_ranges);
    }

    #[tokio::test]
    async fn vortex_errors_name_remote_location() {
        let store = InMemory::new();
        store
            .put(
                &Path::from("truncated.vortex"),
                PutPayload::from_static(b"VTXFbroken"),
            )
            .await
            .unwrap();
        let storage = Arc::new(StorageContext::with_gcs_store(
            StorageConfig::default(),
            Arc::new(store),
        ));
        let command = InspectSubcommand::Vortex(InspectVortexArgs {
            file: "gs://inspect-tests/truncated.vortex".to_string(),
            schema: false,
            stats: false,
            layout: false,
            format: OutputFormat::Text,
        });

        let error = run_with_storage_context(command, storage)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("gs://inspect-tests/truncated.vortex"));
    }
}
