use anyhow::{Context, Result, anyhow};
use datafusion::prelude::SessionContext;
use silk_chiffon_core::{DataSource, TransformBinding, TransformBindings};
use silk_chiffon_storage::{LocationInput, LocationPattern, StorageHandle, StorageSession};

/// Command-scoped file input behavior over bound storage and format settings.
pub(super) struct FileInputRoute<'a> {
    storage: &'a StorageSession,
    formats: &'a TransformBindings,
    explicit_format: Option<&'a str>,
    session: &'a SessionContext,
}

impl<'a> FileInputRoute<'a> {
    pub(super) fn new(
        storage: &'a StorageSession,
        formats: &'a TransformBindings,
        explicit_format: Option<&'a str>,
        session: &'a SessionContext,
    ) -> Self {
        Self {
            storage,
            formats,
            explicit_format,
            session,
        }
    }

    pub(super) async fn create_exact_source(&self, reference: &str) -> Result<Box<dyn DataSource>> {
        let location = LocationInput::parse(reference)
            .with_context(|| format!("while parsing exact file input {reference:?}"))?;
        let handle = self
            .storage
            .input_handle(&location)
            .with_context(|| format!("while resolving exact file input {reference:?}"))?;
        self.create_source_for_handle(&handle, reference).await
    }

    async fn create_source_for_handle(
        &self,
        handle: &StorageHandle,
        reference: &str,
    ) -> Result<Box<dyn DataSource>> {
        self.register_object_store(handle);
        let format = self.format_for_handle(handle, reference)?;
        format
            .create_source(handle, self.session)
            .await
            .with_context(|| format!("while creating file input source for {reference:?}"))
    }

    pub(super) async fn create_pattern_sources(
        &self,
        patterns: &[String],
        allow_unmatched: bool,
    ) -> Result<Vec<Box<dyn DataSource>>> {
        let mut handles = Vec::new();
        for pattern in patterns {
            let location_pattern = LocationPattern::parse(pattern)
                .with_context(|| format!("while parsing file input pattern {pattern:?}"))?;
            let mut matched = self
                .storage
                .expand_input_pattern(&location_pattern)
                .await
                .with_context(|| format!("while expanding file input pattern {pattern:?}"))?;
            if matched.is_empty() && !allow_unmatched {
                anyhow::bail!("file input pattern {pattern:?} matched no locations");
            }
            handles.append(&mut matched);
        }
        handles.sort_by(|left, right| left.url().as_str().cmp(right.url().as_str()));
        handles.dedup_by(|left, right| left.url() == right.url());

        let mut sources = Vec::with_capacity(handles.len());
        for handle in handles {
            let reference = handle.url().as_str().to_owned();
            sources.push(self.create_source_for_handle(&handle, &reference).await?);
        }
        Ok(sources)
    }

    fn register_object_store(&self, handle: &StorageHandle) {
        self.session
            .runtime_env()
            .register_object_store(handle.store_url(), handle.object_store());
    }

    fn format_for_handle<'b>(
        &'b self,
        handle: &StorageHandle,
        reference: &str,
    ) -> Result<&'b TransformBinding> {
        if let Some(format) = self.explicit_format {
            return self
                .formats
                .get(format)
                .ok_or_else(|| anyhow!("format is not registered: {format}"));
        }
        let extension = handle.object_path().extension();
        extension
            .and_then(|extension| self.formats.by_extension(extension))
            .ok_or_else(|| {
                anyhow!(
                    "Could not detect format from path {reference:?}. Use \
                     --input-format to specify explicitly."
                )
            })
    }
}
