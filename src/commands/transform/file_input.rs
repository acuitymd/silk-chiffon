use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use silk_chiffon_core::{FormatInputVariant, TransformBinding, TransformBindings};
use silk_chiffon_storage::{InputObject, LocationInput, LocationPattern, StorageSession};

/// Command-scoped file input behavior over bound storage and format state.
pub(super) struct FileInputPreparer<'a> {
    storage: &'a StorageSession,
    formats: &'a TransformBindings,
    explicit_format: Option<&'a str>,
    session: &'a SessionContext,
}

impl<'a> FileInputPreparer<'a> {
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

    pub(super) async fn prepare_exact(&self, reference: &str) -> Result<Arc<dyn TableProvider>> {
        let location = LocationInput::parse(reference)
            .with_context(|| format!("while parsing exact file input {reference:?}"))?;
        let object = self
            .storage
            .lookup_input(&location)
            .await
            .with_context(|| format!("while resolving exact file input {reference:?}"))?;
        let (format, variant) = self.identify(&object).await?;
        format
            .create_input_provider(std::slice::from_ref(&object), variant, self.session)
            .await
            .with_context(|| format!("while creating file input provider for {reference:?}"))
    }

    pub(super) async fn prepare_patterns(
        &self,
        patterns: &[String],
        allow_unmatched: bool,
    ) -> Result<Vec<Arc<dyn TableProvider>>> {
        let mut providers = Vec::new();
        for pattern in patterns {
            let location_pattern = LocationPattern::parse(pattern)
                .with_context(|| format!("while parsing file input pattern {pattern:?}"))?;
            let mut objects = self
                .storage
                .expand_input_pattern(&location_pattern)
                .await
                .with_context(|| format!("while expanding file input pattern {pattern:?}"))?;
            if objects.is_empty() && !allow_unmatched {
                anyhow::bail!("file input pattern {pattern:?} matched no locations");
            }
            objects.sort_by(|left, right| {
                left.input_handle()
                    .url()
                    .as_str()
                    .cmp(right.input_handle().url().as_str())
            });
            objects.dedup_by(|left, right| left.input_handle().url() == right.input_handle().url());

            let mut groups: Vec<DetectedFileGroup<'_>> = Vec::new();
            for object in objects {
                let (format, variant) = self.identify(&object).await?;
                let store_url = object.input_handle().store_url().as_str();
                if let Some(group) = groups.iter_mut().find(|group| {
                    group.format.format() == format.format()
                        && group.variant == variant
                        && group.store_url == store_url
                }) {
                    group.objects.push(object);
                } else {
                    groups.push(DetectedFileGroup {
                        format,
                        variant,
                        store_url: store_url.to_owned(),
                        objects: vec![object],
                    });
                }
            }
            for group in groups {
                providers.push(
                    group
                        .format
                        .create_input_provider(&group.objects, group.variant, self.session)
                        .await
                        .with_context(|| {
                            format!("while creating file input provider for pattern {pattern:?}")
                        })?,
                );
            }
        }
        Ok(providers)
    }

    async fn identify(
        &'a self,
        object: &InputObject,
    ) -> Result<(&'a TransformBinding, FormatInputVariant)> {
        if let Some(name) = self.explicit_format {
            let format = self
                .formats
                .get(name)
                .ok_or_else(|| anyhow!("format is not registered: {name}"))?;
            let variant = if format.has_detector() {
                format.detect(object).await?.ok_or_else(|| {
                    anyhow!(
                        "input {} is not recognized as {}",
                        object.input_handle().url(),
                        format.format(),
                    )
                })?
            } else {
                FormatInputVariant::new()
            };
            return Ok((format, variant));
        }
        self.formats.detect(object).await?.ok_or_else(|| {
            anyhow!(
                "could not detect the format of input {}; use --input-format to select it explicitly",
                object.input_handle().url()
            )
        })
    }
}

struct DetectedFileGroup<'a> {
    format: &'a TransformBinding,
    variant: FormatInputVariant,
    store_url: String,
    objects: Vec<InputObject>,
}
