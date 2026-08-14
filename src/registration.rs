use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    ffi::OsString,
    fmt,
};

use anyhow::Result;
use clap::{
    Args, Command as ClapCommand, CommandFactory, FromArgMatches,
    builder::{PossibleValue, PossibleValuesParser},
};
use silk_chiffon_core::{
    FormatRegistry, InspectionMode, ServiceInputBinding, ServiceInputDefinition,
    ServiceOutputBinding, ServiceOutputDefinition,
};
use silk_chiffon_storage::{StorageDirection, StorageRegistry};
use thiserror::Error;

#[cfg(feature = "gcs")]
use silk_chiffon_storage::gcs;
#[cfg(feature = "local")]
use silk_chiffon_storage::local;
#[cfg(feature = "s3")]
use silk_chiffon_storage::s3;

use crate::{
    Cli, Command as RuntimeCommand, DetectArgs, DetectCommand, InspectCommand, InspectionArgs,
    OutputFormat, TransformArgs, TransformCommand,
};

/// Builds the executable's set of available data formats.
pub fn format_registry() -> FormatRegistry {
    FormatRegistry::builder()
        .register(silk_chiffon_format_arrow::definition())
        .register(silk_chiffon_format_parquet::definition())
        .register(silk_chiffon_format_vortex::definition())
        .build()
        .expect("built-in format registrations must not conflict")
}

/// Builds the executable's feature-selected storage backends.
pub fn storage_registry() -> StorageRegistry {
    let builder = StorageRegistry::builder();
    #[cfg(feature = "gcs")]
    let builder = builder.register(gcs::backend().expect("built-in GCS backend must be valid"));
    #[cfg(feature = "local")]
    let builder = builder.register(local::backend().expect("built-in local backend must be valid"));
    #[cfg(feature = "s3")]
    let builder = builder.register(s3::backend().expect("built-in S3 backend must be valid"));
    builder
        .build()
        .expect("built-in storage backends must not conflict")
}

pub(crate) enum InputSchemeOwner {
    FileInput,
    ServiceInput(usize),
}

pub(crate) enum OutputSchemeOwner {
    FileOutput,
    ServiceOutput(usize),
}

pub(crate) struct InputSchemeIndex(HashMap<&'static str, InputSchemeOwner>);

impl InputSchemeIndex {
    fn new(
        storage: &StorageRegistry,
        services: &[ServiceInputDefinition],
    ) -> Result<Self, ApplicationAssemblyError> {
        let mut claims = BTreeMap::<&'static str, Vec<String>>::new();
        for backend in storage
            .backends()
            .iter()
            .filter(|backend| backend.supports(StorageDirection::Input))
        {
            for &scheme in backend.schemes() {
                claims
                    .entry(scheme)
                    .or_default()
                    .push(format!("file input storage {:?}", backend.name()));
            }
        }
        for (position, service) in services.iter().enumerate() {
            for &scheme in service.schemes() {
                claims.entry(scheme).or_default().push(format!(
                    "service input {:?} at position {}",
                    service.name(),
                    position + 1
                ));
            }
        }
        if let Some((&scheme, claimants)) = claims.iter().find(|(_, claimants)| claimants.len() > 1)
        {
            return Err(ApplicationAssemblyError::InputSchemeClaimConflict {
                scheme,
                claimants: ConflictingClaimants::from_vec(claimants.clone()),
            });
        }

        let mut index = HashMap::new();
        for backend in storage
            .backends()
            .iter()
            .filter(|backend| backend.supports(StorageDirection::Input))
        {
            for &scheme in backend.schemes() {
                index.insert(scheme, InputSchemeOwner::FileInput);
            }
        }
        for (position, service) in services.iter().enumerate() {
            for &scheme in service.schemes() {
                index.insert(scheme, InputSchemeOwner::ServiceInput(position));
            }
        }
        Ok(Self(index))
    }

    pub(crate) fn owner(&self, scheme: &str) -> Option<&InputSchemeOwner> {
        self.0.get(scheme)
    }
}

pub(crate) struct OutputSchemeIndex(HashMap<&'static str, OutputSchemeOwner>);

impl OutputSchemeIndex {
    fn new(
        storage: &StorageRegistry,
        services: &[ServiceOutputDefinition],
    ) -> Result<Self, ApplicationAssemblyError> {
        let mut claims = BTreeMap::<&'static str, Vec<String>>::new();
        for backend in storage
            .backends()
            .iter()
            .filter(|backend| backend.supports(StorageDirection::Output))
        {
            for &scheme in backend.schemes() {
                claims
                    .entry(scheme)
                    .or_default()
                    .push(format!("file output storage {:?}", backend.name()));
            }
        }
        for (position, service) in services.iter().enumerate() {
            for &scheme in service.schemes() {
                claims.entry(scheme).or_default().push(format!(
                    "service output {:?} at position {}",
                    service.name(),
                    position + 1
                ));
            }
        }
        if let Some((&scheme, claimants)) = claims.iter().find(|(_, claimants)| claimants.len() > 1)
        {
            return Err(ApplicationAssemblyError::OutputSchemeClaimConflict {
                scheme,
                claimants: ConflictingClaimants::from_vec(claimants.clone()),
            });
        }

        let mut index = HashMap::new();
        for backend in storage
            .backends()
            .iter()
            .filter(|backend| backend.supports(StorageDirection::Output))
        {
            for &scheme in backend.schemes() {
                index.insert(scheme, OutputSchemeOwner::FileOutput);
            }
        }
        for (position, service) in services.iter().enumerate() {
            for &scheme in service.schemes() {
                index.insert(scheme, OutputSchemeOwner::ServiceOutput(position));
            }
        }
        Ok(Self(index))
    }

    pub(crate) fn owner(&self, scheme: &str) -> Option<&OutputSchemeOwner> {
        self.0.get(scheme)
    }
}

pub(crate) struct ServiceInputBindings(Box<[ServiceInputBinding]>);

impl ServiceInputBindings {
    pub(crate) fn get(&self, index: usize) -> &ServiceInputBinding {
        &self.0[index]
    }
}

pub(crate) struct ServiceOutputBindings(Box<[ServiceOutputBinding]>);

impl ServiceOutputBindings {
    pub(crate) fn get(&self, index: usize) -> &ServiceOutputBinding {
        &self.0[index]
    }
}

#[derive(Debug)]
struct DefinitionSnapshot {
    position: usize,
    name: &'static str,
    schemes: Box<[&'static str]>,
}

impl fmt::Display for DefinitionSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "definition #{} {:?} schemes {:?}",
            self.position, self.name, self.schemes
        )
    }
}

#[derive(Debug)]
struct ConflictingClaimants {
    first: String,
    second: String,
    rest: Box<[String]>,
}

impl ConflictingClaimants {
    fn from_vec(claimants: Vec<String>) -> Self {
        let mut claimants = claimants.into_iter();
        Self {
            first: claimants.next().expect("a conflict has a first claimant"),
            second: claimants.next().expect("a conflict has a second claimant"),
            rest: claimants.collect(),
        }
    }
}

impl fmt::Display for ConflictingClaimants {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}, {}", self.first, self.second)?;
        for claimant in &self.rest {
            write!(formatter, ", {claimant}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
enum ApplicationAssemblyError {
    #[error(
        "duplicate service input name {name:?}: {}",
        format_snapshots(.definitions)
    )]
    DuplicateServiceInputName {
        name: &'static str,
        definitions: Box<[DefinitionSnapshot]>,
    },
    #[error(
        "duplicate service output name {name:?}: {}",
        format_snapshots(.definitions)
    )]
    DuplicateServiceOutputName {
        name: &'static str,
        definitions: Box<[DefinitionSnapshot]>,
    },
    #[error("input scheme {scheme:?} is claimed by multiple routes: {claimants}")]
    InputSchemeClaimConflict {
        scheme: &'static str,
        claimants: ConflictingClaimants,
    },
    #[error("output scheme {scheme:?} is claimed by multiple routes: {claimants}")]
    OutputSchemeClaimConflict {
        scheme: &'static str,
        claimants: ConflictingClaimants,
    },
    #[error("CLI key {key:?} is claimed by multiple definitions: {claimants}")]
    CliKeyClaimConflict {
        key: String,
        claimants: ConflictingClaimants,
    },
}

fn format_snapshots(snapshots: &[DefinitionSnapshot]) -> String {
    snapshots
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn validate_service_input_names(
    definitions: &[ServiceInputDefinition],
) -> Result<(), ApplicationAssemblyError> {
    validate_service_names(
        definitions
            .iter()
            .map(|definition| (definition.name(), definition.schemes())),
    )
    .map_err(
        |(name, definitions)| ApplicationAssemblyError::DuplicateServiceInputName {
            name,
            definitions,
        },
    )
}

fn validate_service_output_names(
    definitions: &[ServiceOutputDefinition],
) -> Result<(), ApplicationAssemblyError> {
    validate_service_names(
        definitions
            .iter()
            .map(|definition| (definition.name(), definition.schemes())),
    )
    .map_err(
        |(name, definitions)| ApplicationAssemblyError::DuplicateServiceOutputName {
            name,
            definitions,
        },
    )
}

fn validate_service_names<'a>(
    definitions: impl Iterator<Item = (&'static str, &'a [&'static str])>,
) -> std::result::Result<(), (&'static str, Box<[DefinitionSnapshot]>)> {
    let definitions = definitions.collect::<Vec<_>>();
    for &(name, _) in &definitions {
        let snapshots = definitions
            .iter()
            .enumerate()
            .filter(|(_, (candidate, _))| *candidate == name)
            .map(|(position, &(name, schemes))| DefinitionSnapshot {
                position: position + 1,
                name,
                schemes: schemes.into(),
            })
            .collect::<Vec<_>>();
        if snapshots.len() > 1 {
            return Err((name, snapshots.into_boxed_slice()));
        }
    }
    Ok(())
}

fn validate_cli_key_claims(
    formats: &FormatRegistry,
    storage: &StorageRegistry,
    service_inputs: &[ServiceInputDefinition],
    service_outputs: &[ServiceOutputDefinition],
) -> Result<(), ApplicationAssemblyError> {
    let mut claims = BTreeMap::<CliKey, Vec<String>>::new();
    let command_name = "fake-convenience-command-that-is-never-used";
    add_cli_key_claims(
        &mut claims,
        "application transform arguments",
        &TransformArgs::augment_args(ClapCommand::new(command_name)),
    );
    add_cli_key_claims(
        &mut claims,
        "format definitions",
        &formats.augment_transform_args(ClapCommand::new(command_name)),
    );
    add_cli_key_claims(
        &mut claims,
        "storage definitions",
        &storage.augment_args(ClapCommand::new(command_name)),
    );
    for (position, definition) in service_inputs.iter().enumerate() {
        add_cli_key_claims(
            &mut claims,
            &format!(
                "service input {:?} at position {}",
                definition.name(),
                position + 1
            ),
            &definition.augment_args(ClapCommand::new(command_name)),
        );
    }
    for (position, definition) in service_outputs.iter().enumerate() {
        add_cli_key_claims(
            &mut claims,
            &format!(
                "service output {:?} at position {}",
                definition.name(),
                position + 1
            ),
            &definition.augment_args(ClapCommand::new(command_name)),
        );
    }

    if let Some((key, claimants)) = claims.iter().find(|(_, claimants)| claimants.len() > 1) {
        return Err(ApplicationAssemblyError::CliKeyClaimConflict {
            key: key.to_string(),
            claimants: ConflictingClaimants::from_vec(claimants.clone()),
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CliKey {
    Id(String),
    Long(String),
    Short(char),
}

impl fmt::Display for CliKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Id(id) => write!(formatter, "Clap ID {id:?}"),
            Self::Long(long) => write!(formatter, "--{long}"),
            Self::Short(short) => write!(formatter, "-{short}"),
        }
    }
}

fn add_cli_key_claims(
    claims: &mut BTreeMap<CliKey, Vec<String>>,
    claimant: &str,
    command: &ClapCommand,
) {
    let mut keys = BTreeSet::new();
    for argument in command.get_arguments() {
        keys.insert(CliKey::Id(argument.get_id().as_str().to_owned()));
        if let Some(long) = argument.get_long() {
            keys.insert(CliKey::Long(long.to_owned()));
        }
        if let Some(aliases) = argument.get_all_aliases() {
            keys.extend(
                aliases
                    .into_iter()
                    .map(|alias| CliKey::Long(alias.to_owned())),
            );
        }
        if let Some(short) = argument.get_short() {
            keys.insert(CliKey::Short(short));
        }
        if let Some(aliases) = argument.get_all_short_aliases() {
            keys.extend(aliases.into_iter().map(CliKey::Short));
        }
    }
    for group in command.get_groups() {
        keys.insert(CliKey::Id(group.get_id().as_str().to_owned()));
    }
    for key in keys {
        claims.entry(key).or_default().push(claimant.to_owned());
    }
}

pub(crate) struct ApplicationDefinition {
    formats: FormatRegistry,
    storage: StorageRegistry,
    service_inputs: Box<[ServiceInputDefinition]>,
    service_outputs: Box<[ServiceOutputDefinition]>,
    input_schemes: InputSchemeIndex,
    output_schemes: OutputSchemeIndex,
}

impl ApplicationDefinition {
    pub(crate) fn new() -> Self {
        Self::from_parts(
            format_registry(),
            storage_registry(),
            Vec::new(),
            Vec::new(),
        )
        .expect("built-in application definitions must not conflict")
    }

    fn from_parts(
        formats: FormatRegistry,
        storage: StorageRegistry,
        service_inputs: Vec<ServiceInputDefinition>,
        service_outputs: Vec<ServiceOutputDefinition>,
    ) -> Result<Self, ApplicationAssemblyError> {
        validate_service_input_names(&service_inputs)?;
        validate_service_output_names(&service_outputs)?;
        let input_schemes = InputSchemeIndex::new(&storage, &service_inputs)?;
        let output_schemes = OutputSchemeIndex::new(&storage, &service_outputs)?;
        validate_cli_key_claims(&formats, &storage, &service_inputs, &service_outputs)?;
        Ok(Self {
            formats,
            storage,
            service_inputs: service_inputs.into_boxed_slice(),
            service_outputs: service_outputs.into_boxed_slice(),
            input_schemes,
            output_schemes,
        })
    }

    pub(crate) fn command(&self, command: ClapCommand) -> ClapCommand {
        command.mut_subcommands(|command| match command.get_name() {
            "transform" => self.augment_transform_command(command),
            "inspect" => self.augment_inspect_command(command),
            "detect" => self.storage.augment_args(command),
            _ => command,
        })
    }

    fn augment_transform_command(&self, command: ClapCommand) -> ClapCommand {
        let input_formats = self
            .formats
            .formats()
            .filter(|format| format.has_input_provider())
            .map(|format| PossibleValue::new(format.name()).aliases(format.aliases()))
            .collect::<Vec<_>>();
        let output_formats = self
            .formats
            .formats()
            .filter(|format| format.has_sink())
            .map(|format| PossibleValue::new(format.name()).aliases(format.aliases()))
            .collect::<Vec<_>>();
        let command = command.mut_args(|argument| match argument.get_id().as_str() {
            "input_format" => {
                argument.value_parser(PossibleValuesParser::new(input_formats.clone()))
            }
            "output_format" => {
                argument.value_parser(PossibleValuesParser::new(output_formats.clone()))
            }
            _ => argument,
        });
        let mut command = self
            .formats
            .augment_transform_args(self.storage.augment_args(command));
        for definition in &self.service_inputs {
            command = definition.augment_args(command);
        }
        for definition in &self.service_outputs {
            command = definition.augment_args(command);
        }
        command
    }

    fn augment_inspect_command(&self, mut command: ClapCommand) -> ClapCommand {
        for format in self
            .formats
            .formats()
            .filter(|format| format.has_inspector())
        {
            let format_command = ClapCommand::new(format.name())
                .about(format!(
                    "Inspect {} file metadata and structure",
                    format.name()
                ))
                .visible_aliases(format.aliases().iter().copied());
            let format_command = InspectionArgs::augment_args(format_command);
            let format_command = format.augment_inspection_args(format_command);
            command = command.subcommand(self.storage.augment_args(format_command));
        }
        command
    }

    fn bind(self, matches: &clap::ArgMatches) -> Result<Cli, clap::Error> {
        let (name, matches) = matches.subcommand().ok_or_else(|| {
            clap::Error::raw(
                clap::error::ErrorKind::MissingSubcommand,
                "a command is required",
            )
        })?;
        let command = match name {
            "transform" => {
                let args = TransformArgs::from_arg_matches(matches)?;
                let formats = self.formats.bind_transform(matches)?;
                let storage = self.storage.create_session(matches).map_err(clap_error)?;
                let service_inputs = ServiceInputBindings(
                    self.service_inputs
                        .iter()
                        .map(|definition| definition.bind(matches))
                        .collect::<Result<Vec<_>, _>>()?
                        .into_boxed_slice(),
                );
                let service_outputs = ServiceOutputBindings(
                    self.service_outputs
                        .iter()
                        .map(|definition| definition.bind(matches))
                        .collect::<Result<Vec<_>, _>>()?
                        .into_boxed_slice(),
                );
                RuntimeCommand::Transform(TransformCommand::from_parsed(
                    args,
                    formats,
                    storage,
                    service_inputs,
                    service_outputs,
                    self.input_schemes,
                    self.output_schemes,
                ))
            }
            "detect" => {
                let args = DetectArgs::from_arg_matches(matches)?;
                let storage = self.storage.create_session(matches).map_err(clap_error)?;
                RuntimeCommand::Detect(DetectCommand::from_parsed(args, storage, self.formats))
            }
            "inspect" => {
                RuntimeCommand::Inspect(parse_inspect(matches, &self.formats, &self.storage)?)
            }
            "completions" => RuntimeCommand::Completions {
                shell: *matches
                    .get_one("shell")
                    .expect("Clap requires the completion shell"),
            },
            _ => unreachable!("Clap accepted an unknown command"),
        };
        Ok(Cli { command })
    }
}

pub(crate) fn try_parse_from<I, T>(arguments: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let definition = ApplicationDefinition::new();
    let matches = definition
        .command(crate::CliSchema::command())
        .try_get_matches_from(arguments)?;
    definition.bind(&matches)
}

fn parse_inspect(
    matches: &clap::ArgMatches,
    formats: &FormatRegistry,
    storage_registry: &StorageRegistry,
) -> Result<InspectCommand, clap::Error> {
    let (name, matches) = matches.subcommand().ok_or_else(|| {
        clap::Error::raw(
            clap::error::ErrorKind::MissingSubcommand,
            "an inspect command is required",
        )
    })?;
    let storage = storage_registry
        .create_session(matches)
        .map_err(clap_error)?;
    let args = InspectionArgs::from_arg_matches(matches)?;
    let inspection = bind_inspection(formats, name, matches)?;
    Ok(InspectCommand::from_parsed(
        args.file,
        inspection_mode(args.format),
        inspection,
        storage,
    ))
}

fn inspection_mode(format: OutputFormat) -> InspectionMode {
    if format.resolves_to_json() {
        InspectionMode::Json
    } else {
        InspectionMode::Text
    }
}

fn bind_inspection(
    formats: &FormatRegistry,
    format: &str,
    matches: &clap::ArgMatches,
) -> Result<silk_chiffon_core::InspectionBinding, clap::Error> {
    formats
        .get(format)
        .expect("the CLI contains only registered formats")
        .bind_inspection(matches)
}

fn clap_error(error: impl std::fmt::Display) -> clap::Error {
    clap::Error::raw(clap::error::ErrorKind::ValueValidation, error.to_string())
}

#[cfg(all(test, feature = "local-bare-paths"))]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc, LazyLock, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use anyhow::Result;
    use arrow::array::RecordBatch;
    use bytes::Bytes;
    use clap::{Args, CommandFactory};
    use datafusion::{
        catalog::{TableProvider, streaming::StreamingTable},
        datasource::MemTable,
        execution::TaskContext,
        physical_plan::{
            SendableRecordBatchStream, stream::RecordBatchReceiverStreamBuilder,
            streaming::PartitionStream,
        },
        prelude::SessionContext,
    };
    use futures::{StreamExt, future::BoxFuture};
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path as ObjectPath};
    use silk_chiffon_core::{
        DataSink, FormatDefinition, FormatFuture, FormatRegistry, InputLeaf,
        ServiceInputDefinition, ServiceOutputDefinition, SinkBinding, TransformDefinition,
    };
    use silk_chiffon_storage::{
        OutputPreparation, StorageAccess, StorageBackend, StorageHandle, StorageRegistry,
    };
    use url::Url;

    use super::{ApplicationAssemblyError, ApplicationDefinition, storage_registry};
    use crate::{CliSchema, Command};
    use silk_chiffon_test_support::{TestBatch, TestExtract, TestFile, parquet::read_entire_file};
    static SINK_BINDINGS: AtomicUsize = AtomicUsize::new(0);
    static SERVICE_INPUT_REFERENCES: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static SERVICE_OUTPUT_RESULT: Mutex<Option<(String, usize)>> = Mutex::new(None);
    static TYPED_SERVICE_OUTPUT_RESULT: Mutex<Option<TypedServiceOutputResult>> = Mutex::new(None);
    static LARGE_LEAF_FILES: AtomicUsize = AtomicUsize::new(0);
    static SERVICE_SOURCE_STATE: LazyLock<Arc<ServiceSourceState>> =
        LazyLock::new(|| Arc::new(ServiceSourceState::new()));
    static REMOTE_STORES: LazyLock<Mutex<HashMap<String, Arc<InMemory>>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    #[derive(Debug, Eq, PartialEq)]
    struct TypedServiceOutputResult {
        target: String,
        marker: usize,
        fields: Vec<String>,
        ids: Vec<i32>,
    }

    #[derive(Debug)]
    struct ServiceSourceState {
        started: AtomicBool,
        stopped: AtomicBool,
        cancelled: AtomicBool,
        state_changed: tokio::sync::Notify,
    }

    impl ServiceSourceState {
        fn new() -> Self {
            Self {
                started: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                state_changed: tokio::sync::Notify::new(),
            }
        }

        fn reset(&self) {
            self.started.store(false, Ordering::SeqCst);
            self.stopped.store(false, Ordering::SeqCst);
            self.cancelled.store(false, Ordering::SeqCst);
        }

        async fn wait_until_stopped(&self) {
            loop {
                let state_changed = self.state_changed.notified();
                if self.stopped.load(Ordering::SeqCst) {
                    return;
                }
                state_changed.await;
            }
        }
    }

    struct ServiceSourceLifetime {
        state: Arc<ServiceSourceState>,
    }

    impl Drop for ServiceSourceLifetime {
        fn drop(&mut self) {
            self.state.cancelled.store(true, Ordering::SeqCst);
            self.state.stopped.store(true, Ordering::SeqCst);
            self.state.state_changed.notify_waiters();
        }
    }

    #[derive(Debug)]
    struct StructuredServicePartition {
        batch: RecordBatch,
    }

    impl PartitionStream for StructuredServicePartition {
        fn schema(&self) -> &arrow::datatypes::SchemaRef {
            self.batch.schema_ref()
        }

        fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
            let mut stream = RecordBatchReceiverStreamBuilder::new(self.batch.schema(), 1);
            let sender = stream.tx();
            let batch = self.batch.clone();
            let state = Arc::clone(&SERVICE_SOURCE_STATE);
            stream.spawn(async move {
                let _lifetime = ServiceSourceLifetime {
                    state: Arc::clone(&state),
                };
                state.started.store(true, Ordering::SeqCst);
                state.state_changed.notify_waiters();
                loop {
                    if sender.send(Ok(batch.clone())).await.is_err() {
                        return Ok(());
                    }
                }
            });
            stream.build()
        }
    }

    fn remote_store(root: &str) -> Arc<InMemory> {
        Arc::clone(
            REMOTE_STORES
                .lock()
                .unwrap()
                .entry(root.to_owned())
                .or_insert_with(|| Arc::new(InMemory::new())),
        )
    }

    fn create_remote_store(
        store_url: &Url,
        _: &(),
        _: Option<&object_store::RetryConfig>,
    ) -> Result<Arc<dyn ObjectStore>> {
        Ok(remote_store(store_url.as_str()))
    }

    fn prepare_remote_output<'a>(
        _: &'a StorageHandle,
        _: &'a OutputPreparation,
        _: &'a (),
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn remote_backend() -> StorageBackend {
        StorageBackend::without_args()
            .name("test-remote")
            .schemes(["test-remote"])
            .access(StorageAccess::ReadWrite)
            .allow_any_location()
            .object_store_creator(create_remote_store)
            .prepare_output_target(prepare_remote_output)
            .build()
            .unwrap()
    }

    fn remote_storage_registry() -> StorageRegistry {
        StorageRegistry::builder()
            .register(super::local::backend().unwrap())
            .register(remote_backend())
            .build()
            .unwrap()
    }

    async fn put_remote_file(root: &str, path: &str, bytes: Vec<u8>) {
        remote_store(root)
            .put(&ObjectPath::from(path), Bytes::from(bytes).into())
            .await
            .unwrap();
    }

    fn file_bytes(extension: &str, batch: &RecordBatch) -> Vec<u8> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(format!("input.{extension}"));
        match extension {
            "arrow" => TestFile::write_arrow_batch(&path, batch),
            "parquet" => TestFile::write_parquet_batch(&path, batch),
            _ => panic!("unsupported test format {extension}"),
        }
        std::fs::read(path).unwrap()
    }

    fn arrow_stream_bytes(batches: &[RecordBatch]) -> Vec<u8> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("input.arrow");
        TestFile::write_arrow_stream(&path, batches);
        std::fs::read(path).unwrap()
    }

    async fn vortex_bytes(batch: RecordBatch) -> Vec<u8> {
        silk_chiffon_test_support::vortex::encode_batches(&batch.schema(), vec![batch])
            .await
            .unwrap()
    }

    fn test_provider(batch: RecordBatch) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(MemTable::try_new(
            batch.schema(),
            vec![vec![batch]],
        )?))
    }

    fn create_test_service_input<'a>(
        reference: &'a str,
        _: &'a SessionContext,
        _: &'a (),
    ) -> BoxFuture<'a, Result<Arc<dyn TableProvider>>> {
        SERVICE_INPUT_REFERENCES
            .lock()
            .unwrap()
            .push(reference.to_owned());
        Box::pin(async { test_provider(TestBatch::simple_with(&[4, 5, 6], &["d", "e", "f"])) })
    }

    fn create_structured_service_input<'a>(
        _: &'a str,
        _: &'a SessionContext,
        _: &'a (),
    ) -> BoxFuture<'a, Result<Arc<dyn TableProvider>>> {
        Box::pin(async {
            let batch = TestBatch::simple_with(&[4, 5, 6], &["d", "e", "f"]);
            Ok(Arc::new(StreamingTable::try_new(
                batch.schema(),
                vec![Arc::new(StructuredServicePartition { batch })],
            )?) as Arc<dyn TableProvider>)
        })
    }

    fn write_test_service_output<'a>(
        target: &'a str,
        mut stream: SendableRecordBatchStream,
        _: &'a (),
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                rows += batch?.num_rows();
            }
            *SERVICE_OUTPUT_RESULT.lock().unwrap() = Some((target.to_owned(), rows));
            Ok(())
        })
    }

    fn fail_after_one_service_batch<'a>(
        _: &'a str,
        mut stream: SendableRecordBatchStream,
        _: &'a (),
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            stream
                .next()
                .await
                .ok_or_else(|| anyhow::anyhow!("service source ended before its first batch"))??;
            anyhow::bail!("controlled service output failure")
        })
    }

    fn test_service_input(name: &'static str, scheme: &'static str) -> ServiceInputDefinition {
        ServiceInputDefinition::without_args(create_test_service_input)
            .name(name)
            .schemes([scheme])
            .build()
            .unwrap()
    }

    fn test_service_output(name: &'static str, scheme: &'static str) -> ServiceOutputDefinition {
        ServiceOutputDefinition::without_args(write_test_service_output)
            .name(name)
            .schemes([scheme])
            .build()
            .unwrap()
    }

    #[derive(Args)]
    struct TypedServiceInputArgs {
        #[arg(long)]
        test_service_input_start: i32,
    }

    #[derive(Args)]
    struct TypedServiceOutputArgs {
        #[arg(long)]
        test_service_output_marker: usize,
    }

    fn create_typed_service_input<'a>(
        reference: &'a str,
        _: &'a SessionContext,
        settings: &'a TypedServiceInputArgs,
    ) -> BoxFuture<'a, Result<Arc<dyn TableProvider>>> {
        Box::pin(async move {
            anyhow::ensure!(reference == "typed-input://dataset");
            let start = settings.test_service_input_start;
            test_provider(TestBatch::simple_with(
                &[start, start + 1, start + 2],
                &["a", "b", "c"],
            ))
        })
    }

    fn write_typed_service_output<'a>(
        target: &'a str,
        mut stream: SendableRecordBatchStream,
        settings: &'a TypedServiceOutputArgs,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let fields = stream
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }
            *TYPED_SERVICE_OUTPUT_RESULT.lock().unwrap() = Some(TypedServiceOutputResult {
                target: target.to_owned(),
                marker: settings.test_service_output_marker,
                fields,
                ids: TestExtract::i32_all(&batches, "id"),
            });
            Ok(())
        })
    }

    #[derive(Args)]
    struct ConflictingInputArgs {
        #[arg(long)]
        query: Option<String>,
    }

    #[derive(Args)]
    struct ConflictingOutputArgs {
        #[arg(long)]
        query: Option<String>,
    }

    fn create_conflicting_service_input<'a>(
        reference: &'a str,
        _: &'a SessionContext,
        _: &'a ConflictingInputArgs,
    ) -> BoxFuture<'a, Result<Arc<dyn TableProvider>>> {
        SERVICE_INPUT_REFERENCES
            .lock()
            .unwrap()
            .push(reference.to_owned());
        Box::pin(async { test_provider(TestBatch::simple_with(&[4, 5, 6], &["d", "e", "f"])) })
    }

    fn write_conflicting_service_output<'a>(
        target: &'a str,
        mut stream: SendableRecordBatchStream,
        _: &'a ConflictingOutputArgs,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                rows += batch?.num_rows();
            }
            *SERVICE_OUTPUT_RESULT.lock().unwrap() = Some((target.to_owned(), rows));
            Ok(())
        })
    }

    fn input_only_provider<'a>(
        _: &'a InputLeaf,
        _: &'a SessionContext,
        _: &'a (),
    ) -> FormatFuture<'a, Arc<dyn TableProvider>> {
        Box::pin(async { test_provider(TestBatch::simple_with(&[3, 1, 2], &["c", "a", "b"])) })
    }

    fn large_leaf_provider<'a>(
        leaf: &'a InputLeaf,
        _: &'a SessionContext,
        _: &'a (),
    ) -> FormatFuture<'a, Arc<dyn TableProvider>> {
        LARGE_LEAF_FILES.store(leaf.files().len(), Ordering::SeqCst);
        Box::pin(async { test_provider(TestBatch::simple_with(&[1], &["one"])) })
    }

    fn input_only_format() -> FormatDefinition {
        FormatDefinition::builder("input-only-test", "Input only test")
            .extensions(["input-only-test"])
            .transform(
                TransformDefinition::without_args()
                    .input_provider(input_only_provider)
                    .build(),
            )
            .build()
    }

    fn large_leaf_format() -> FormatDefinition {
        FormatDefinition::builder("large-leaf-test", "Large leaf test")
            .extensions(["large-leaf-test"])
            .transform(
                TransformDefinition::without_args()
                    .input_provider(large_leaf_provider)
                    .build(),
            )
            .build()
    }

    fn count_sink_binding<'a>(
        _: &'a silk_chiffon_core::SinkBindingConfig,
        _: &'a (),
    ) -> FormatFuture<'a, Box<dyn SinkBinding>> {
        SINK_BINDINGS.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(Box::new(UnavailableSinkBinding) as Box<dyn SinkBinding>) })
    }

    struct UnavailableSinkBinding;

    #[async_trait::async_trait]
    impl SinkBinding for UnavailableSinkBinding {
        async fn open_sink(
            &self,
            _: StorageHandle,
            _: arrow::datatypes::SchemaRef,
        ) -> Result<Box<dyn DataSink>> {
            anyhow::bail!("test sink is not opened")
        }
    }

    fn counted_sink_format() -> FormatDefinition {
        FormatDefinition::builder("counted-sink-test", "Counted sink test")
            .extensions(["counted-sink-test"])
            .transform(
                TransformDefinition::without_args()
                    .sink(count_sink_binding)
                    .build(),
            )
            .build()
    }

    fn test_cli(definition: ApplicationDefinition, arguments: &[&str]) -> crate::Cli {
        let matches = definition
            .command(CliSchema::command())
            .try_get_matches_from(arguments)
            .unwrap();
        definition.bind(&matches).unwrap()
    }

    fn application_definition(formats: FormatRegistry) -> ApplicationDefinition {
        ApplicationDefinition::from_parts(formats, storage_registry(), Vec::new(), Vec::new())
            .unwrap()
    }

    fn application_definition_with_services(
        formats: FormatRegistry,
        service_inputs: Vec<ServiceInputDefinition>,
        service_outputs: Vec<ServiceOutputDefinition>,
    ) -> ApplicationDefinition {
        ApplicationDefinition::from_parts(
            formats,
            storage_registry(),
            service_inputs,
            service_outputs,
        )
        .unwrap()
    }

    fn remote_application_definition() -> ApplicationDefinition {
        ApplicationDefinition::from_parts(
            FormatRegistry::builder()
                .register(silk_chiffon_format_arrow::definition())
                .register(silk_chiffon_format_parquet::definition())
                .register(silk_chiffon_format_vortex::definition())
                .build()
                .unwrap(),
            remote_storage_registry(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap()
    }

    #[test]
    fn assembly_reports_every_duplicate_service_name_snapshot_in_order() {
        let error = ApplicationDefinition::from_parts(
            FormatRegistry::builder().build().unwrap(),
            storage_registry(),
            vec![
                test_service_input("duplicate", "test-a"),
                test_service_input("other", "test-b"),
                test_service_input("duplicate", "test-c"),
            ],
            Vec::new(),
        )
        .err()
        .expect("duplicate names must fail assembly");

        let ApplicationAssemblyError::DuplicateServiceInputName { name, definitions } = error
        else {
            panic!("expected duplicate service input names");
        };
        assert_eq!(name, "duplicate");
        assert_eq!(definitions.len(), 2);
        assert_eq!(definitions[0].position, 1);
        assert_eq!(definitions[0].schemes.as_ref(), ["test-a"]);
        assert_eq!(definitions[1].position, 3);
        assert_eq!(definitions[1].schemes.as_ref(), ["test-c"]);
    }

    #[test]
    fn assembly_allows_the_same_service_name_once_per_direction() {
        ApplicationDefinition::from_parts(
            FormatRegistry::builder().build().unwrap(),
            storage_registry(),
            vec![test_service_input("shared", "test-input")],
            vec![test_service_output("shared", "test-output")],
        )
        .unwrap();
    }

    #[test]
    fn assembly_reports_duplicate_service_output_names_separately() {
        let error = ApplicationDefinition::from_parts(
            FormatRegistry::builder().build().unwrap(),
            storage_registry(),
            Vec::new(),
            vec![
                test_service_output("duplicate", "test-a"),
                test_service_output("duplicate", "test-b"),
            ],
        )
        .err()
        .expect("duplicate names must fail assembly");

        let ApplicationAssemblyError::DuplicateServiceOutputName { name, definitions } = error
        else {
            panic!("expected duplicate service output names");
        };
        assert_eq!(name, "duplicate");
        assert_eq!(definitions.len(), 2);
        assert_eq!(definitions[0].position, 1);
        assert_eq!(definitions[1].position, 2);
    }

    #[test]
    fn assembly_reports_file_and_service_scheme_claimants_by_direction() {
        let input_error = ApplicationDefinition::from_parts(
            FormatRegistry::builder().build().unwrap(),
            storage_registry(),
            vec![test_service_input("conflict", "file")],
            Vec::new(),
        )
        .err()
        .expect("the local file input route already claims file");
        let ApplicationAssemblyError::InputSchemeClaimConflict { scheme, claimants } = input_error
        else {
            panic!("expected an input scheme conflict");
        };
        assert_eq!(scheme, "file");
        assert!(claimants.first.contains("file input storage"));
        assert!(claimants.second.contains("service input"));

        let output_error = ApplicationDefinition::from_parts(
            FormatRegistry::builder().build().unwrap(),
            storage_registry(),
            Vec::new(),
            vec![test_service_output("conflict", "file")],
        )
        .err()
        .expect("the local file output route already claims file");
        assert!(matches!(
            output_error,
            ApplicationAssemblyError::OutputSchemeClaimConflict { scheme: "file", .. }
        ));
    }

    #[test]
    fn assembly_reports_all_claimants_for_the_first_cli_key_conflict() {
        let input = ServiceInputDefinition::with_args(create_conflicting_service_input)
            .name("conflicting-input")
            .schemes(["test-input"])
            .build()
            .unwrap();
        let output = ServiceOutputDefinition::with_args(write_conflicting_service_output)
            .name("conflicting-output")
            .schemes(["test-output"])
            .build()
            .unwrap();
        let error = ApplicationDefinition::from_parts(
            FormatRegistry::builder().build().unwrap(),
            storage_registry(),
            vec![input],
            vec![output],
        )
        .err()
        .expect("CLI key conflicts must fail assembly");

        let ApplicationAssemblyError::CliKeyClaimConflict { key, claimants } = error else {
            panic!("expected a CLI key conflict");
        };
        assert_eq!(key, "Clap ID \"query\"");
        assert_eq!(claimants.first, "application transform arguments");
        assert!(claimants.second.contains("service input"));
        assert_eq!(claimants.rest.len(), 1);
        assert!(claimants.rest[0].contains("service output"));
    }

    #[tokio::test]
    async fn exact_file_and_service_inputs_share_one_service_output() {
        SERVICE_INPUT_REFERENCES.lock().unwrap().clear();
        SERVICE_OUTPUT_RESULT.lock().unwrap().take();
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input.arrow");
        TestFile::write_arrow_batch(
            &input,
            &TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]),
        );
        let definition = application_definition_with_services(
            FormatRegistry::builder()
                .register(silk_chiffon_format_arrow::definition())
                .build()
                .unwrap(),
            vec![test_service_input("test-input", "test-input")],
            vec![test_service_output("test-output", "test-output")],
        );
        let cli = test_cli(
            definition,
            &[
                "silk-chiffon",
                "transform",
                "--from",
                input.to_str().unwrap(),
                "--from",
                "test-input://same",
                "--from",
                "test-input://same",
                "--to",
                "test-output://result",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        assert_eq!(
            SERVICE_INPUT_REFERENCES.lock().unwrap().as_slice(),
            ["test-input://same", "test-input://same"]
        );
        assert_eq!(
            SERVICE_OUTPUT_RESULT.lock().unwrap().as_ref(),
            Some(&("test-output://result".to_owned(), 9))
        );
    }

    #[tokio::test]
    async fn remote_exact_input_runs_through_query_projection_and_limit() {
        let root = "test-remote://coverage-exact/";
        let batch = TestBatch::simple_with(&[1, 2, 3, 4], &["a", "b", "c", "d"]);
        put_remote_file(root, "nested/input.arrow", file_bytes("arrow", &batch)).await;
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.arrow");
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "test-remote://coverage-exact/nested/input.arrow",
                "--to",
                output.to_str().unwrap(),
                "--query",
                "SELECT name FROM data WHERE id >= 2 ORDER BY id LIMIT 2",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        let batches = TestFile::read_arrow(&output);
        assert_eq!(batches[0].schema().fields().len(), 1);
        assert_eq!(TestExtract::string_all(&batches, "name"), ["b", "c"]);
    }

    #[tokio::test]
    async fn exact_remote_output_runs_through_the_complete_application_route() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input.arrow");
        TestFile::write_arrow_batch(
            &input,
            &TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]),
        );
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                input.to_str().unwrap(),
                "--to",
                "test-remote://coverage-output/exact/output.arrow",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        let bytes = remote_store("test-remote://coverage-output/")
            .get(&ObjectPath::from("exact/output.arrow"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let downloaded = directory.path().join("downloaded.arrow");
        std::fs::write(&downloaded, bytes).unwrap();
        assert_eq!(
            TestExtract::i32_all(&TestFile::read_arrow(&downloaded), "id"),
            [1, 2, 3]
        );
    }

    #[tokio::test]
    async fn remote_parquet_input_and_output_exercise_the_registered_format_end_to_end() {
        let input_root = "test-remote://coverage-parquet-input/";
        let batch = TestBatch::simple_with(&[1, 2, 3, 4], &["a", "b", "b", "c"]);
        put_remote_file(input_root, "input.parquet", file_bytes("parquet", &batch)).await;
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "test-remote://coverage-parquet-input/input.parquet",
                "--to",
                "test-remote://coverage-parquet-output/output.parquet",
                "--query",
                "SELECT id, name FROM data WHERE id >= 2",
                "--sort-by",
                "id:desc",
                "--parquet-row-group-size",
                "2",
                "--parquet-row-group-concurrency",
                "2",
                "--parquet-ingestion-queue-size",
                "1",
                "--parquet-encoding-queue-size",
                "1",
                "--parquet-writing-queue-size",
                "1",
                "--parquet-buffer-size",
                "1B",
                "--parquet-compression",
                "zstd",
                "--parquet-writer-version",
                "v2",
                "--parquet-dictionary-column",
                "name:analyze",
                "--parquet-bloom-column",
                "id:ndv=4",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        let bytes = remote_store("test-remote://coverage-parquet-output/")
            .get(&ObjectPath::from("output.parquet"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let downloaded = directory.path().join("output.parquet");
        std::fs::write(&downloaded, bytes).unwrap();
        let batches = TestFile::read_parquet(&downloaded);
        assert_eq!(TestExtract::i32_all(&batches, "id"), [4, 3, 2]);
        assert_eq!(TestExtract::string_all(&batches, "name"), ["c", "b", "b"]);
        let contents = read_entire_file(&downloaded).unwrap();
        assert_eq!(
            contents
                .row_groups
                .iter()
                .map(|row_group| row_group.num_rows)
                .collect::<Vec<_>>(),
            [2, 1]
        );
        assert_eq!(
            contents.compression_used,
            ["ZSTD(ZstdLevel(1))".to_owned()].into()
        );
        assert!(!contents.column("name").unwrap().has_dictionary);
        assert!(contents.column("id").unwrap().has_bloom_filter);
    }

    #[tokio::test]
    async fn partitioned_remote_output_prepares_and_completes_each_object_lazily() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input.arrow");
        TestFile::write_arrow_batch(
            &input,
            &TestBatch::simple_with(&[1, 2, 3], &["a", "b", "a"]),
        );
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                input.to_str().unwrap(),
                "--to-many",
                "test-remote://coverage-partitioned/{{name}}.parquet",
                "--by",
                "name",
                "--partition-strategy",
                "nosort-multi",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        let store = remote_store("test-remote://coverage-partitioned/");
        let mut ids = Vec::new();
        for name in ["a", "b"] {
            let bytes = store
                .get(&ObjectPath::from(format!("{name}.parquet")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let downloaded = directory.path().join(format!("{name}.parquet"));
            std::fs::write(&downloaded, bytes).unwrap();
            ids.extend(TestExtract::i32_all(
                &TestFile::read_parquet(&downloaded),
                "id",
            ));
        }
        ids.sort_unstable();
        assert_eq!(ids, [1, 2, 3]);
    }

    #[tokio::test]
    async fn remote_arrow_stream_executes_through_the_scoped_store() {
        let root = "test-remote://coverage-stream/";
        put_remote_file(
            root,
            "input.arrow",
            arrow_stream_bytes(&[
                TestBatch::simple_with(&[1, 2], &["a", "b"]),
                TestBatch::simple_with(&[3, 4], &["c", "d"]),
            ]),
        )
        .await;
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.arrow");
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "test-remote://coverage-stream/input.arrow?versionId=pinned",
                "--to",
                output.to_str().unwrap(),
                "--query",
                "SELECT name FROM data WHERE id > 1 ORDER BY id DESC LIMIT 2",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        let batches = TestFile::read_arrow(&output);
        assert_eq!(batches[0].schema().fields().len(), 1);
        assert_eq!(TestExtract::string_all(&batches, "name"), ["d", "c"]);
    }

    #[tokio::test]
    async fn missing_remote_exact_input_fails_before_output_construction() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.arrow");
        let input = "test-remote://coverage-missing/missing.arrow?versionId=absent";
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                input,
                "--to",
                output.to_str().unwrap(),
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        let error = crate::commands::transform::run(command).await.unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains(input), "{message}");
        assert!(
            message.to_ascii_lowercase().contains("not found"),
            "{message}"
        );
        assert!(!message.contains("__silk_input"), "{message}");
        assert!(!output.exists());
    }

    #[tokio::test]
    async fn remote_vortex_input_uses_its_native_provider_end_to_end() {
        let root = "test-remote://coverage-vortex/";
        let batch = TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]);
        put_remote_file(root, "input.vortex", vortex_bytes(batch).await).await;
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.arrow");
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "test-remote://coverage-vortex/input.vortex",
                "--to",
                output.to_str().unwrap(),
                "--query",
                "SELECT id FROM data WHERE id >= 2",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        assert_eq!(
            TestExtract::i32_all(&TestFile::read_arrow(&output), "id"),
            [2, 3]
        );
    }

    #[tokio::test]
    async fn recognized_malformed_remote_input_stops_format_fallback() {
        let root = "test-remote://coverage-malformed/";
        let mut bytes = b"ARROW1".to_vec();
        bytes.extend_from_slice(&[0; 16]);
        put_remote_file(root, "input.arrow", bytes).await;
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.arrow");
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "test-remote://coverage-malformed/input.arrow",
                "--to",
                output.to_str().unwrap(),
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        let error = crate::commands::transform::run(command).await.unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("malformed arrow input"), "{message}");
        assert!(
            message.contains("coverage-malformed/input.arrow"),
            "{message}"
        );
        assert!(
            message.contains("missing its trailing magic marker"),
            "{message}"
        );
        assert!(!output.exists());
    }

    #[tokio::test]
    async fn leading_parquet_magic_without_a_trailer_stops_format_fallback() {
        let root = "test-remote://coverage-malformed-parquet/";
        let mut bytes = b"PAR1".to_vec();
        bytes.extend_from_slice(b"not a complete Parquet file");
        put_remote_file(root, "input.parquet", bytes).await;
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.arrow");
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "test-remote://coverage-malformed-parquet/input.parquet",
                "--to",
                output.to_str().unwrap(),
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        let error = crate::commands::transform::run(command).await.unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("malformed parquet input"), "{message}");
        assert!(
            message.contains("missing its trailing magic marker"),
            "{message}"
        );
        assert!(!output.exists());
    }

    #[tokio::test]
    async fn trailing_parquet_magic_does_not_claim_unknown_remote_input() {
        let root = "test-remote://coverage-trailing-parquet/";
        let mut bytes = b"not a known format".to_vec();
        bytes.extend_from_slice(b"PAR1");
        put_remote_file(root, "input.parquet", bytes).await;
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.arrow");
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "test-remote://coverage-trailing-parquet/input.parquet",
                "--to",
                output.to_str().unwrap(),
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        let error = crate::commands::transform::run(command).await.unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("could not detect the format"), "{message}");
        assert!(!message.contains("malformed parquet input"), "{message}");
        assert!(!output.exists());
    }

    #[tokio::test]
    async fn unknown_remote_bytes_report_the_canonical_input() {
        let root = "test-remote://coverage-unknown/";
        put_remote_file(root, "input.unknown", b"not a known format".to_vec()).await;
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.arrow");
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "test-remote://coverage-unknown/input.unknown",
                "--to",
                output.to_str().unwrap(),
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        let error = crate::commands::transform::run(command).await.unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("could not detect the format"), "{message}");
        assert!(
            message.contains("coverage-unknown/input.unknown"),
            "{message}"
        );
        assert!(!output.exists());
    }

    #[tokio::test]
    async fn identical_paths_in_different_remote_roots_cannot_cross_read() {
        let first_root = "test-remote://coverage-root-a/";
        let second_root = "test-remote://coverage-root-b/";
        put_remote_file(
            first_root,
            "shared.arrow",
            file_bytes("arrow", &TestBatch::simple_with(&[1, 2], &["a", "b"])),
        )
        .await;
        put_remote_file(
            second_root,
            "shared.arrow",
            file_bytes("arrow", &TestBatch::simple_with(&[90], &["z"])),
        )
        .await;
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.arrow");
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "test-remote://coverage-root-a/shared.arrow",
                "--from",
                "test-remote://coverage-root-b/shared.arrow",
                "--to",
                output.to_str().unwrap(),
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        let mut ids = TestExtract::i32_all(&TestFile::read_arrow(&output), "id");
        ids.sort_unstable();
        assert_eq!(ids, [1, 2, 90]);
    }

    #[tokio::test]
    async fn remote_pattern_groups_mixed_formats_without_losing_rows() {
        let root = "test-remote://coverage-pattern/";
        put_remote_file(
            root,
            "dataset/a.arrow",
            file_bytes("arrow", &TestBatch::simple_with(&[1, 2], &["a", "b"])),
        )
        .await;
        put_remote_file(
            root,
            "dataset/b.parquet",
            file_bytes("parquet", &TestBatch::simple_with(&[3, 4], &["c", "d"])),
        )
        .await;
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.arrow");
        let cli = test_cli(
            remote_application_definition(),
            &[
                "silk-chiffon",
                "transform",
                "--from-pattern",
                "test-remote://coverage-pattern/dataset/*",
                "--to",
                output.to_str().unwrap(),
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        let mut ids = TestExtract::i32_all(&TestFile::read_arrow(&output), "id");
        ids.sort_unstable();
        assert_eq!(ids, [1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn six_figure_remote_pattern_builds_one_leaf_and_executes() {
        const FILES: usize = 100_000;
        LARGE_LEAF_FILES.store(0, Ordering::SeqCst);
        SERVICE_OUTPUT_RESULT.lock().unwrap().take();
        let root = "test-remote://coverage-large/";
        let store = remote_store(root);
        for index in 0..FILES {
            store
                .put(
                    &ObjectPath::from(format!("dataset/{index:06}.large-leaf-test")),
                    Bytes::new().into(),
                )
                .await
                .unwrap();
        }
        let definition = ApplicationDefinition::from_parts(
            FormatRegistry::builder()
                .register(large_leaf_format())
                .build()
                .unwrap(),
            remote_storage_registry(),
            Vec::new(),
            vec![test_service_output("test-output", "test-output")],
        )
        .unwrap();
        let cli = test_cli(
            definition,
            &[
                "silk-chiffon",
                "transform",
                "--from-pattern",
                "test-remote://coverage-large/dataset/*.large-leaf-test",
                "--input-format",
                "large-leaf-test",
                "--to",
                "test-output://large",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        assert_eq!(LARGE_LEAF_FILES.load(Ordering::SeqCst), FILES);
        assert_eq!(
            SERVICE_OUTPUT_RESULT.lock().unwrap().as_ref(),
            Some(&("test-output://large".to_owned(), 1))
        );
    }

    #[tokio::test]
    async fn typed_service_only_transform_projects_and_drains_the_output() {
        TYPED_SERVICE_OUTPUT_RESULT.lock().unwrap().take();
        let input = ServiceInputDefinition::with_args(create_typed_service_input)
            .name("typed-input")
            .schemes(["typed-input"])
            .build()
            .unwrap();
        let output = ServiceOutputDefinition::with_args(write_typed_service_output)
            .name("typed-output")
            .schemes(["typed-output"])
            .build()
            .unwrap();
        let definition = application_definition_with_services(
            FormatRegistry::builder().build().unwrap(),
            vec![input],
            vec![output],
        );
        let cli = test_cli(
            definition,
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "typed-input://dataset",
                "--to",
                "typed-output://result",
                "--test-service-input-start",
                "40",
                "--test-service-output-marker",
                "23",
                "--exclude-columns",
                "name",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        assert_eq!(
            TYPED_SERVICE_OUTPUT_RESULT.lock().unwrap().as_ref(),
            Some(&TypedServiceOutputResult {
                target: "typed-output://result".to_owned(),
                marker: 23,
                fields: vec!["id".to_owned()],
                ids: vec![40, 41, 42],
            })
        );
    }

    #[tokio::test]
    async fn service_output_failure_cancels_the_service_input_execution() {
        SERVICE_SOURCE_STATE.reset();
        let input = ServiceInputDefinition::without_args(create_structured_service_input)
            .name("structured-input")
            .schemes(["structured-input"])
            .build()
            .unwrap();
        let output = ServiceOutputDefinition::without_args(fail_after_one_service_batch)
            .name("failing-output")
            .schemes(["failing-output"])
            .build()
            .unwrap();
        let definition = application_definition_with_services(
            FormatRegistry::builder().build().unwrap(),
            vec![input],
            vec![output],
        );
        let cli = test_cli(
            definition,
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "structured-input://dataset",
                "--to",
                "failing-output://result",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        let error = crate::commands::transform::run(command).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("controlled service output failure"),
            "{error:#}"
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            SERVICE_SOURCE_STATE.wait_until_stopped().await;
        })
        .await
        .expect("service input task survived its execution stream");
        assert!(SERVICE_SOURCE_STATE.started.load(Ordering::SeqCst));
        assert!(SERVICE_SOURCE_STATE.cancelled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn service_input_patterns_are_rejected_before_file_expansion() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.arrow");
        let definition = application_definition_with_services(
            FormatRegistry::builder()
                .register(silk_chiffon_format_arrow::definition())
                .build()
                .unwrap(),
            vec![test_service_input("test-input", "test-input")],
            Vec::new(),
        );
        let cli = test_cli(
            definition,
            &[
                "silk-chiffon",
                "transform",
                "--from-pattern",
                "test-input://*",
                "--to",
                output.to_str().unwrap(),
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        let error = crate::commands::transform::run(command).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not support --from-pattern")
        );
    }

    #[tokio::test]
    async fn service_input_satisfies_nonempty_when_an_allowed_file_pattern_is_unmatched() {
        SERVICE_OUTPUT_RESULT.lock().unwrap().take();
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing-*.arrow");
        let definition = application_definition_with_services(
            FormatRegistry::builder().build().unwrap(),
            vec![test_service_input("test-input", "test-input")],
            vec![test_service_output("test-output", "test-output")],
        );
        let cli = test_cli(
            definition,
            &[
                "silk-chiffon",
                "transform",
                "--from",
                "test-input://source",
                "--from-pattern",
                missing.to_str().unwrap(),
                "--allow-unmatched-patterns",
                "--to",
                "test-output://result",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        crate::commands::transform::run(command).await.unwrap();

        assert_eq!(
            SERVICE_OUTPUT_RESULT.lock().unwrap().as_ref(),
            Some(&("test-output://result".to_owned(), 3))
        );
    }

    #[tokio::test]
    async fn service_outputs_reject_partition_templates() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input.arrow");
        TestFile::write_arrow_batch(
            &input,
            &TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]),
        );
        let definition = application_definition_with_services(
            FormatRegistry::builder()
                .register(silk_chiffon_format_arrow::definition())
                .build()
                .unwrap(),
            Vec::new(),
            vec![test_service_output("test-output", "test-output")],
        );
        let cli = test_cli(
            definition,
            &[
                "silk-chiffon",
                "transform",
                "--from",
                input.to_str().unwrap(),
                "--to-many",
                "test-output://{{name}}",
                "--by",
                "name",
            ],
        );
        let Command::Transform(command) = cli.command else {
            panic!("expected transform command");
        };

        let error = crate::commands::transform::run(command).await.unwrap_err();
        assert!(error.to_string().contains("does not support --to-many"));
    }

    #[tokio::test]
    async fn service_routes_reject_file_only_options_and_unknown_schemes() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("input.arrow");
        let output = directory.path().join("output.arrow");
        TestFile::write_arrow_batch(
            &input,
            &TestBatch::simple_with(&[1, 2, 3], &["a", "b", "c"]),
        );
        let input = input.to_str().unwrap();
        let output = output.to_str().unwrap();
        let cases = [
            (
                vec![
                    "silk-chiffon",
                    "transform",
                    "--from",
                    "test-input://source",
                    "--to",
                    output,
                    "--input-format",
                    "arrow",
                ],
                "--input-format applies only to file inputs",
            ),
            (
                vec![
                    "silk-chiffon",
                    "transform",
                    "--from",
                    input,
                    "--to",
                    "test-output://target",
                    "--output-format",
                    "arrow",
                ],
                "--output-format applies only to file outputs",
            ),
            (
                vec![
                    "silk-chiffon",
                    "transform",
                    "--from",
                    input,
                    "--to",
                    "test-output://target",
                    "--list-outputs",
                    "text",
                ],
                "--list-outputs applies only to file outputs",
            ),
            (
                vec![
                    "silk-chiffon",
                    "transform",
                    "--from",
                    "unknown-input://source",
                    "--to",
                    output,
                ],
                "unsupported input scheme \"unknown-input\"",
            ),
            (
                vec![
                    "silk-chiffon",
                    "transform",
                    "--from",
                    input,
                    "--to",
                    "unknown-output://target",
                ],
                "unsupported output scheme \"unknown-output\"",
            ),
        ];

        for (arguments, expected) in cases {
            let definition = application_definition_with_services(
                FormatRegistry::builder()
                    .register(silk_chiffon_format_arrow::definition())
                    .build()
                    .unwrap(),
                vec![test_service_input("test-input", "test-input")],
                vec![test_service_output("test-output", "test-output")],
            );
            let cli = test_cli(definition, &arguments);
            let Command::Transform(command) = cli.command else {
                panic!("expected transform command");
            };

            let error = crate::commands::transform::run(command).await.unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error:#}"
            );
        }
    }

    fn directional_format_definition() -> ApplicationDefinition {
        application_definition(
            FormatRegistry::builder()
                .register(input_only_format())
                .register(counted_sink_format())
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn transform_format_values_follow_input_and_output_capabilities() {
        directional_format_definition()
            .command(CliSchema::command())
            .try_get_matches_from([
                "silk-chiffon",
                "transform",
                "--from",
                "input.input-only-test",
                "--input-format",
                "input-only-test",
                "--to",
                "output.counted-sink-test",
                "--output-format",
                "counted-sink-test",
            ])
            .unwrap();

        let input_error = directional_format_definition()
            .command(CliSchema::command())
            .try_get_matches_from([
                "silk-chiffon",
                "transform",
                "--from",
                "input.counted-sink-test",
                "--input-format",
                "counted-sink-test",
                "--to",
                "output.counted-sink-test",
            ])
            .unwrap_err();
        assert_eq!(input_error.kind(), clap::error::ErrorKind::InvalidValue);

        let output_error = directional_format_definition()
            .command(CliSchema::command())
            .try_get_matches_from([
                "silk-chiffon",
                "transform",
                "--from",
                "input.input-only-test",
                "--to",
                "output.input-only-test",
                "--output-format",
                "input-only-test",
            ])
            .unwrap_err();
        assert_eq!(output_error.kind(), clap::error::ErrorKind::InvalidValue);
    }
}
