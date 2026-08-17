use anyhow::{Context, Result, anyhow};
use datafusion::prelude::SessionContext;
use glob::glob;
use silk_chiffon_core::{DataSource, TransformBinding, TransformBindings};
use silk_chiffon_storage::{LocationInput, StorageHandle, StorageSession};

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
        self.register_object_store(&handle);
        let format = self.format_for_handle(&handle, reference)?;
        format
            .create_source(&handle, self.session)
            .await
            .with_context(|| format!("while creating file input source for {reference:?}"))
    }

    pub(super) async fn create_pattern_sources<'b>(
        &self,
        patterns: impl Iterator<Item = &'b String>,
    ) -> Result<Vec<Box<dyn DataSource>>> {
        let mut references = Vec::new();
        for pattern in patterns {
            let mut matched = false;
            for entry in glob(pattern)
                .with_context(|| format!("while expanding file input pattern {pattern:?}"))?
            {
                matched = true;
                references.push(
                    entry
                        .with_context(|| {
                            format!("while decoding a match for file input pattern {pattern:?}")
                        })?
                        .to_string_lossy()
                        .into_owned(),
                );
            }
            if !matched {
                anyhow::bail!("file input pattern {pattern:?} matched no locations");
            }
        }
        references.sort();
        references.dedup();

        let mut sources = Vec::with_capacity(references.len());
        for reference in references {
            sources.push(self.create_exact_source(&reference).await?);
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
        let extension = std::path::Path::new(handle.url().path())
            .extension()
            .and_then(std::ffi::OsStr::to_str);
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
