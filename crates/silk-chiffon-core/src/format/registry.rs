//! Validation and lookup indexes for a collection of format definitions.

use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use clap::{ArgMatches, Command};
use silk_chiffon_storage::InputObject;
use thiserror::Error;

use super::definition::{
    DetectedFormat, FormatDefinition, FormatOperationError, TransformBinding, TransformBindings,
};

/// A conflict between independently contributed format definitions.
///
/// Each variant reports every format that claimed the conflicting value, including registries
/// with more than two claimants.
#[derive(Debug, Error)]
pub enum FormatRegistryError {
    #[error("duplicate format name {name}: {formats:?}")]
    DuplicateName {
        name: String,
        formats: Vec<&'static str>,
    },
    #[error("duplicate format extension {extension}: {formats:?}")]
    DuplicateExtension {
        extension: String,
        formats: Vec<&'static str>,
    },
    #[error("duplicate format CLI argument {argument}: {formats:?}")]
    DuplicateCliArgument {
        argument: String,
        formats: Vec<&'static str>,
    },
}

/// Collects format definitions before validating them as one registry.
///
/// Registration order is retained for iteration and as the tie-breaker between equal detection
/// priorities.
pub struct FormatRegistryBuilder {
    formats: Vec<FormatDefinition>,
}

impl FormatRegistryBuilder {
    /// Adds one format definition to the set that [`Self::build`] will validate.
    pub fn register(mut self, format: FormatDefinition) -> Self {
        self.formats.push(format);
        self
    }

    /// Validates cross-format claims and constructs the lookup indexes.
    pub fn build(self) -> Result<FormatRegistry, FormatRegistryError> {
        FormatRegistry::from_formats(self.formats)
    }
}

/// A validated and indexed collection of immutable format definitions.
///
/// The registry exists at application-definition time. It owns no parsed command settings.
/// Hosts use it to compose Clap commands, detect inputs, find definitions, and create
/// command-scoped [`TransformBindings`].
pub struct FormatRegistry {
    formats: Vec<FormatDefinition>,
    names: HashMap<String, usize>,
    extensions: HashMap<String, usize>,
    detection_order: Vec<usize>,
}

impl FormatRegistry {
    /// Starts an empty registry builder.
    pub fn builder() -> FormatRegistryBuilder {
        FormatRegistryBuilder {
            formats: Vec::new(),
        }
    }

    fn from_formats(formats: Vec<FormatDefinition>) -> Result<Self, FormatRegistryError> {
        let mut name_claims = BTreeMap::<String, Vec<&'static str>>::new();
        let mut extension_claims = BTreeMap::<String, Vec<&'static str>>::new();
        let mut argument_claims = BTreeMap::<String, (String, Vec<&'static str>)>::new();

        for format in &formats {
            name_claims
                .entry(format.name.to_ascii_lowercase())
                .or_default()
                .push(format.name);
            for alias in &format.aliases {
                name_claims
                    .entry(alias.to_ascii_lowercase())
                    .or_default()
                    .push(format.name);
            }
            for extension in &format.extensions {
                extension_claims
                    .entry(extension.trim_start_matches('.').to_ascii_lowercase())
                    .or_default()
                    .push(format.name);
            }
            if let Some(transform) = &format.transform {
                for (key, argument) in transform.definition.argument_keys() {
                    argument_claims
                        .entry(key)
                        .or_insert_with(|| (argument, Vec::new()))
                        .1
                        .push(format.name);
                }
            }
        }

        if let Some((name, formats)) = name_claims.iter().find(|(_, formats)| formats.len() > 1) {
            return Err(FormatRegistryError::DuplicateName {
                name: name.clone(),
                formats: formats.clone(),
            });
        }
        if let Some((extension, formats)) = extension_claims
            .iter()
            .find(|(_, formats)| formats.len() > 1)
        {
            return Err(FormatRegistryError::DuplicateExtension {
                extension: extension.clone(),
                formats: formats.clone(),
            });
        }
        if let Some((_, (argument, formats))) = argument_claims
            .iter()
            .find(|(_, (_, formats))| formats.len() > 1)
        {
            return Err(FormatRegistryError::DuplicateCliArgument {
                argument: argument.clone(),
                formats: formats.clone(),
            });
        }

        let mut names = HashMap::new();
        let mut extensions = HashMap::new();
        for (index, format) in formats.iter().enumerate() {
            names.insert(format.name.to_ascii_lowercase(), index);
            for alias in &format.aliases {
                names.insert(alias.to_ascii_lowercase(), index);
            }
            for extension in &format.extensions {
                extensions.insert(
                    extension.trim_start_matches('.').to_ascii_lowercase(),
                    index,
                );
            }
        }

        let mut detection_order = formats
            .iter()
            .enumerate()
            .filter_map(|(index, format)| format.detector.map(|_| index))
            .collect::<Vec<_>>();
        detection_order.sort_by_key(|index| (formats[*index].detection_priority, *index));

        Ok(Self {
            formats,
            names,
            extensions,
            detection_order,
        })
    }

    /// Iterates over definitions in registration order.
    pub fn formats(&self) -> impl Iterator<Item = &FormatDefinition> {
        self.formats.iter()
    }

    /// Runs the input's extension owner first, then detectors in priority order.
    ///
    /// Detector errors stop detection so an I/O or parser failure is not reported as an unknown
    /// format.
    pub async fn detect(
        &self,
        object: &InputObject,
    ) -> Result<Option<DetectedFormat>, FormatOperationError> {
        let preferred = object
            .handle()
            .object_path()
            .extension()
            .and_then(|extension| {
                self.extensions
                    .get(&extension.to_ascii_lowercase())
                    .copied()
            });
        if let Some(index) = preferred
            && self.formats[index].detector.is_some()
            && let Some(detected) = self.formats[index].detect(object).await?
        {
            return Ok(Some(detected));
        }
        for index in &self.detection_order {
            if Some(*index) == preferred {
                continue;
            }
            if let Some(detected) = self.formats[*index].detect(object).await? {
                return Ok(Some(detected));
            }
        }
        Ok(None)
    }

    /// Looks up a definition by canonical name or alias, ignoring ASCII case.
    pub fn get(&self, name_or_alias: &str) -> Option<&FormatDefinition> {
        self.names
            .get(&name_or_alias.to_ascii_lowercase())
            .map(|index| &self.formats[*index])
    }

    /// Looks up a definition by filename extension, with or without a leading dot.
    pub fn by_extension(&self, extension: &str) -> Option<&FormatDefinition> {
        self.extensions
            .get(&extension.trim_start_matches('.').to_ascii_lowercase())
            .map(|index| &self.formats[*index])
    }

    /// Adds every format's transform arguments to a host-owned Clap command.
    ///
    /// This method composes arguments only. The host remains responsible for parsing the complete
    /// command and passes its resulting matches to [`Self::bind_transform`].
    pub fn augment_transform_args(&self, mut command: Command) -> Command {
        for format in &self.formats {
            if let Some(transform) = &format.transform {
                command = transform.definition.augment(command);
            }
        }
        command
    }

    /// Parses each format's transform arguments into command-scoped bindings.
    pub fn bind_transform(&self, matches: &ArgMatches) -> Result<TransformBindings, clap::Error> {
        let mut bindings = Vec::new();
        let mut names = HashMap::new();
        let mut extensions = HashMap::new();
        let mut detection_priorities = Vec::new();
        for format in &self.formats {
            let Some(transform) = &format.transform else {
                continue;
            };
            let index = bindings.len();
            bindings.push(TransformBinding {
                format: format.name,
                detector: format.detector,
                binding: transform.definition.bind(matches)?,
            });
            detection_priorities.push((format.detection_priority, index));
            names.insert(format.name.to_ascii_lowercase(), index);
            for alias in &format.aliases {
                names.insert(alias.to_ascii_lowercase(), index);
            }
            for extension in &format.extensions {
                extensions.insert(
                    extension.trim_start_matches('.').to_ascii_lowercase(),
                    index,
                );
            }
        }
        detection_priorities.sort_by_key(|&(priority, index)| (priority, index));
        let detection_order = detection_priorities
            .into_iter()
            .filter_map(|(_, index)| {
                (bindings[index].detector.is_some() && bindings[index].has_input_provider())
                    .then_some(index)
            })
            .collect();
        Ok(TransformBindings {
            bindings,
            names,
            extensions,
            detection_order,
        })
    }
}
