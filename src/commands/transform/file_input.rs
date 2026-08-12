use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use silk_chiffon_core::{InputLeaf, InputVariant, TransformBinding, TransformBindings};
use silk_chiffon_storage::{InputObject, LocationInput, LocationPattern, StorageSession};

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

    pub(super) async fn create_exact_provider(
        &self,
        reference: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let location = LocationInput::parse(reference)
            .with_context(|| format!("while parsing exact file input {reference:?}"))?;
        let object = self
            .storage
            .lookup_input(&location)
            .await
            .with_context(|| format!("while resolving exact file input {reference:?}"))?;
        let (format, variant) = self.identify(&object).await?;
        let leaf = InputLeaf::try_new(self.session, std::slice::from_ref(&object), variant)
            .with_context(|| format!("while preparing exact file input {reference:?}"))?;
        format
            .create_input_provider(&leaf, self.session)
            .await
            .with_context(|| format!("while creating file input provider for {reference:?}"))
    }

    pub(super) async fn create_pattern_providers(
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
                left.handle()
                    .url()
                    .as_str()
                    .cmp(right.handle().url().as_str())
            });
            objects.dedup_by(|left, right| left.handle().url() == right.handle().url());

            let mut groups: Vec<InputGroup<'_>> = Vec::new();
            for object in objects {
                let (format, variant) = self.identify(&object).await?;
                let store_url = object.handle().store_url().as_str();
                if let Some(group) = groups.iter_mut().find(|group| {
                    group.format.format() == format.format()
                        && group.variant == variant
                        && group.store_url == store_url
                }) {
                    group.objects.push(object);
                } else {
                    groups.push(InputGroup {
                        format,
                        variant,
                        store_url: store_url.to_owned(),
                        objects: vec![object],
                    });
                }
            }
            for group in groups {
                let leaf = InputLeaf::try_new(self.session, &group.objects, group.variant)
                    .with_context(|| format!("while preparing file input pattern {pattern:?}"))?;
                providers.push(
                    group
                        .format
                        .create_input_provider(&leaf, self.session)
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
    ) -> Result<(&'a TransformBinding, InputVariant)> {
        if let Some(name) = self.explicit_format {
            let format = self
                .formats
                .get(name)
                .ok_or_else(|| anyhow!("format is not registered: {name}"))?;
            let variant = if format.has_detector() {
                format.detect(object).await?.ok_or_else(|| {
                    anyhow!(
                        "input {} is not recognized as {}",
                        object.handle().url(),
                        format.format(),
                    )
                })?
            } else {
                InputVariant::new()
            };
            return Ok((format, variant));
        }
        self.formats.detect(object).await?.ok_or_else(|| {
            anyhow!(
                "could not detect the format of input {}; use --input-format to select it explicitly",
                object.handle().url()
            )
        })
    }
}

struct InputGroup<'a> {
    format: &'a TransformBinding,
    variant: InputVariant,
    store_url: String,
    objects: Vec<InputObject>,
}
