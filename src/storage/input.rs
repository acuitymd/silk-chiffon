//! Exact input lookup and deterministic object-store glob expansion.
//!
//! Exact locations issue one metadata request. Glob patterns issue one recursive
//! listing beneath their literal prefix, then match full keys in memory. Results
//! from every store are sorted by display location and deduplicated by store and
//! object key.

use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use futures::TryStreamExt;
use object_store::{ObjectMeta, ObjectStoreExt, path::Path};

use super::{ObjectLocation, StorageContext, glob::ParsedInput};

#[derive(Clone, Debug)]
pub struct InputObject {
    location: ObjectLocation,
    meta: ObjectMeta,
}

impl InputObject {
    #[cfg(test)]
    pub(crate) fn new(location: ObjectLocation, meta: ObjectMeta) -> Self {
        Self { location, meta }
    }

    #[must_use]
    pub fn location(&self) -> &ObjectLocation {
        &self.location
    }

    #[must_use]
    pub fn metadata(&self) -> &ObjectMeta {
        &self.meta
    }

    #[must_use]
    pub fn extension(&self) -> Option<&str> {
        self.meta.location.extension()
    }
}

impl StorageContext {
    pub async fn resolve_input(&self, value: &str) -> Result<InputObject> {
        let location = self.resolve(value)?;
        head_input(location).await
    }

    pub async fn resolve_inputs(&self, values: &[String]) -> Result<Vec<InputObject>> {
        let mut inputs = Vec::new();
        for value in values {
            match ParsedInput::parse(self, value)? {
                ParsedInput::Exact(location) => inputs.push(head_input(location).await?),
                ParsedInput::Glob(pattern) => {
                    let prefix = (!pattern.list_prefix().is_root()).then(|| pattern.list_prefix());
                    let objects = pattern
                        .location()
                        .store()
                        .object_store()
                        .list(prefix)
                        .try_collect::<Vec<_>>()
                        .await
                        .with_context(|| {
                            format!(
                                "could not list objects for input pattern '{}'",
                                pattern.original()
                            )
                        })?;
                    for meta in objects {
                        if pattern.matches(&meta.location) {
                            inputs.push(InputObject {
                                location: pattern.concrete_location(meta.location.clone())?,
                                meta,
                            });
                        }
                    }
                }
            }
        }

        inputs.sort_by(|left, right| left.location.display().cmp(right.location.display()));
        let mut seen = HashSet::<(String, Path)>::new();
        inputs.retain(|input| {
            seen.insert((
                input.location.store().store_url().as_str().to_string(),
                input.meta.location.clone(),
            ))
        });

        if inputs.is_empty() {
            bail!("No input files found matching patterns: {values:?}");
        }
        Ok(inputs)
    }
}

async fn head_input(location: ObjectLocation) -> Result<InputObject> {
    let meta = location
        .store()
        .object_store()
        .head(location.path())
        .await
        .with_context(|| {
            format!(
                "could not read input metadata for '{}'",
                location.original()
            )
        })?;
    Ok(InputObject { location, meta })
}
