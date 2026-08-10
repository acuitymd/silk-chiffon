use std::{
    path::Path as FilePath,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use clap::{Args, Command};
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjectPath};
use silk_chiffon_storage::{
    Location, LocationInput, RetryConfig, RetryConfigurationError, StorageAccess, StorageBackend,
    StorageBackendBuildError, StorageDirection, StorageError, StorageRegistry,
    StorageRegistryError, StorageSession,
};
use url::Url;

static LAST_LABEL: Mutex<Option<String>> = Mutex::new(None);
static LAST_BARE_LOCATION: Mutex<Option<String>> = Mutex::new(None);
static LAST_RETRY_COUNT: Mutex<Option<usize>> = Mutex::new(None);
static READ_ONLY_PATH_MAPPINGS: AtomicUsize = AtomicUsize::new(0);
static OBJECT_PATH_MAPPINGS: AtomicUsize = AtomicUsize::new(0);
static OBJECT_STORE_CREATIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Args, Clone)]
struct MemoryArgs {
    #[arg(long = "memory-label", default_value = "default")]
    label: String,
}

#[derive(Args, Clone)]
struct RequiredCloudArgs {
    #[arg(long = "cloud-account")]
    account: String,
}

#[derive(Args, Clone)]
struct SharedIdArgs {
    #[arg(long = "first-option")]
    shared: bool,
}

#[derive(Args, Clone)]
struct DuplicateIdArgs {
    #[arg(long = "second-option")]
    shared: bool,
}

#[derive(Args, Clone)]
struct SharedLongArgs {
    #[arg(id = "first_long", long = "shared-long")]
    first_long: bool,
}

#[derive(Args, Clone)]
struct DuplicateLongArgs {
    #[arg(id = "second_long", long = "shared-long")]
    second_long: bool,
}

#[derive(Args, Clone)]
struct ThirdLongArgs {
    #[arg(id = "third_long", long = "shared-long")]
    third_long: bool,
}

#[derive(Args, Clone)]
struct DuplicateLongAliasArgs {
    #[arg(long = "first-alias", alias = "shared-alias")]
    first_alias: bool,
    #[arg(long = "second-alias", alias = "shared-alias")]
    second_alias: bool,
}

#[derive(Args, Clone)]
struct DuplicateShortAliasArgs {
    #[arg(long = "first-short-alias", short = 'a', short_alias = 'x')]
    first_short_alias: bool,
    #[arg(long = "second-short-alias", short = 'b', short_alias = 'x')]
    second_short_alias: bool,
}

#[derive(Args, Clone)]
struct DuplicateLocalIdArgs {
    #[arg(id = "shared_id", long = "first-id")]
    first_id: bool,
    #[arg(id = "shared_id", long = "second-id")]
    second_id: bool,
}

#[derive(Args, Clone)]
struct DuplicateLocalLongArgs {
    #[arg(id = "first_long", long = "shared-long")]
    first_long: bool,
    #[arg(id = "second_long", long = "shared-long")]
    second_long: bool,
}

#[derive(Args, Clone)]
struct DuplicateLocalShortArgs {
    #[arg(long = "first-short", short = 'x')]
    first_short: bool,
    #[arg(long = "second-short", short = 'x')]
    second_short: bool,
}

#[derive(Args, Clone)]
struct LongAliasContributorArgs {
    #[arg(long = "first-option", alias = "shared-alias")]
    first_option: bool,
}

#[derive(Args, Clone)]
struct LongAliasClaimantArgs {
    #[arg(long = "shared-alias")]
    shared_alias: bool,
}

#[derive(Args, Clone)]
struct ShortAliasContributorArgs {
    #[arg(long = "first-short-option", short = 'a', short_alias = 'x')]
    first_short_option: bool,
}

#[derive(Args, Clone)]
struct ShortAliasClaimantArgs {
    #[arg(long = "second-short-option", short = 'x')]
    second_short_option: bool,
}

#[derive(Args, Clone)]
struct SharedShortArgs {
    #[arg(long = "first-short", short = 'x')]
    first_short: bool,
}

#[derive(Args, Clone)]
struct DuplicateShortArgs {
    #[arg(long = "second-short", short = 'x')]
    second_short: bool,
}

#[derive(Args, Clone)]
#[group(id = "shared_group")]
struct SharedGroupArgs {
    #[arg(long = "first-group-option")]
    first_group_option: bool,
}

#[derive(Args, Clone)]
#[group(id = "shared_group")]
struct DuplicateGroupArgs {
    #[arg(long = "second-group-option")]
    second_group_option: bool,
}

#[derive(Args, Clone)]
struct GroupIdAsArgumentArgs {
    #[arg(id = "shared_group", long = "group-id-as-argument")]
    group_id_as_argument: bool,
}

#[derive(Args, Clone)]
struct SharedRetryCollisionArgs {
    #[arg(long = "storage-max-retries")]
    storage_max_retries: bool,
}

fn memory_object_path(location: &Location, settings: &MemoryArgs) -> anyhow::Result<ObjectPath> {
    *LAST_LABEL.lock().unwrap() = Some(settings.label.clone());
    Ok(ObjectPath::from_url_path(location.url().path())?)
}

fn map_memory_bare_location(input: &str, settings: &MemoryArgs) -> anyhow::Result<Location> {
    *LAST_BARE_LOCATION.lock().unwrap() = Some(input.to_owned());
    Ok(Location::parse_url(format!(
        "mem://{}/mapped-object",
        settings.label
    ))?)
}

fn map_first_bare_location(_input: &str, _settings: &()) -> anyhow::Result<Location> {
    Ok(Location::parse_url("first://bucket/object")?)
}

fn map_second_bare_location(_input: &str, _settings: &()) -> anyhow::Result<Location> {
    Ok(Location::parse_url("second://bucket/object")?)
}

fn map_mismatched_bare_location(_input: &str, _settings: &()) -> anyhow::Result<Location> {
    Ok(Location::parse_url("other://bucket/object")?)
}

fn object_path<T>(location: &Location, _settings: &T) -> anyhow::Result<ObjectPath> {
    Ok(ObjectPath::from_url_path(location.url().path())?)
}

fn counted_object_path(location: &Location, settings: &()) -> anyhow::Result<ObjectPath> {
    OBJECT_PATH_MAPPINGS.fetch_add(1, Ordering::SeqCst);
    object_path(location, settings)
}

fn retry_object_store(
    _store_url: &Url,
    _settings: &(),
    retry: Option<&RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    *LAST_RETRY_COUNT.lock().unwrap() = retry.map(|configuration| configuration.max_retries);
    Ok(Arc::new(InMemory::new()))
}

fn counted_object_store(
    _store_url: &Url,
    _settings: &(),
    _retry: Option<&RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    OBJECT_STORE_CREATIONS.fetch_add(1, Ordering::SeqCst);
    Ok(Arc::new(InMemory::new()))
}

fn read_only_object_path(location: &Location, _settings: &()) -> anyhow::Result<ObjectPath> {
    READ_ONLY_PATH_MAPPINGS.fetch_add(1, Ordering::SeqCst);
    object_path(location, &())
}

fn failing_object_path(_location: &Location, _settings: &()) -> anyhow::Result<ObjectPath> {
    anyhow::bail!("backend-specific object-path failure")
}

fn failing_bare_location(_input: &str, _settings: &()) -> anyhow::Result<Location> {
    anyhow::bail!("backend-specific bare-location failure")
}

fn failing_object_store(
    _store_url: &Url,
    _settings: &(),
    _retry: Option<&RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    anyhow::bail!("backend-specific object-store failure")
}

fn in_memory_object_store<T>(
    _store_url: &Url,
    _settings: &T,
    _retry: Option<&RetryConfig>,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(InMemory::new()))
}

fn unit_backend(name: &'static str, scheme: &'static str) -> StorageBackend {
    StorageBackend::without_args()
        .name(name)
        .schemes([scheme])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build()
        .unwrap()
}

fn command_and_registry(
    backends: impl IntoIterator<Item = StorageBackend>,
) -> (Command, StorageRegistry) {
    let mut builder = StorageRegistry::builder();
    for backend in backends {
        builder = builder.register(backend);
    }
    let registry = builder.build().unwrap();
    let command = registry.augment_args(Command::new("storage-test"));
    (command, registry)
}

fn create_default_session(registry: &StorageRegistry) -> StorageSession {
    let command = registry.augment_args(Command::new("storage-test"));
    let matches = command
        .try_get_matches_from(["storage-test"])
        .expect("default arguments should parse");
    registry
        .create_session(&matches)
        .expect("default arguments should create a session")
}

fn location_input(input: &str) -> LocationInput {
    LocationInput::parse(input).unwrap()
}

#[test]
fn backend_build_validates_the_complete_definition() {
    let missing_name = StorageBackend::without_args()
        .schemes(["mem"])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build();
    assert!(matches!(
        missing_name,
        Err(StorageBackendBuildError::MissingName)
    ));

    for name in ["", "Memory", "memory_store"] {
        let result = StorageBackend::without_args()
            .name(name)
            .schemes(["mem"])
            .access(StorageAccess::ReadWrite)
            .object_path_mapper(object_path)
            .object_store_creator(in_memory_object_store)
            .build();
        assert!(matches!(
            result,
            Err(StorageBackendBuildError::InvalidName { name: invalid }) if invalid == name
        ));
    }

    let missing_schemes = StorageBackend::without_args()
        .name("memory")
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build();
    assert!(matches!(
        missing_schemes,
        Err(StorageBackendBuildError::MissingSchemes)
    ));

    for scheme in ["", "MEM", "mem_store"] {
        let result = StorageBackend::without_args()
            .name("memory")
            .schemes([scheme])
            .access(StorageAccess::ReadWrite)
            .object_path_mapper(object_path)
            .object_store_creator(in_memory_object_store)
            .build();
        assert!(matches!(
            result,
            Err(StorageBackendBuildError::InvalidScheme { scheme: invalid }) if invalid == scheme
        ));
    }

    let duplicate_scheme = StorageBackend::without_args()
        .name("memory")
        .schemes(["mem", "mem"])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build();
    assert!(matches!(
        duplicate_scheme,
        Err(StorageBackendBuildError::DuplicateScheme { scheme: "mem" })
    ));

    let missing_access = StorageBackend::without_args()
        .name("memory")
        .schemes(["mem"])
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build();
    assert!(matches!(
        missing_access,
        Err(StorageBackendBuildError::MissingAccess)
    ));

    let missing_path_mapper = StorageBackend::without_args()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadWrite)
        .object_store_creator(in_memory_object_store)
        .build();
    assert!(matches!(
        missing_path_mapper,
        Err(StorageBackendBuildError::MissingObjectPathMapper)
    ));

    let missing_store_creator = StorageBackend::without_args()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(object_path)
        .build();
    assert!(matches!(
        missing_store_creator,
        Err(StorageBackendBuildError::MissingObjectStoreCreator)
    ));
}

#[test]
fn backend_builder_setters_replace_earlier_values() {
    let backend = StorageBackend::without_args()
        .name("first")
        .name("second")
        .schemes(["first"])
        .schemes(["second"])
        .access(StorageAccess::ReadOnly)
        .access(StorageAccess::WriteOnly)
        .bare_location_mapper(map_first_bare_location)
        .bare_location_mapper(map_second_bare_location)
        .object_path_mapper(failing_object_path)
        .object_path_mapper(object_path)
        .object_store_creator(failing_object_store)
        .object_store_creator(in_memory_object_store)
        .shared_retries()
        .shared_retries()
        .build()
        .unwrap();

    assert_eq!(backend.name(), "second");
    assert_eq!(backend.schemes(), ["second"]);
    assert!(!backend.supports(StorageDirection::Input));
    assert!(backend.supports(StorageDirection::Output));
    assert!(backend.claims_bare_locations());
    assert!(backend.uses_shared_retries());

    let (_, registry) = command_and_registry([backend]);
    let storage = create_default_session(&registry);
    let handle = storage
        .output_handle(&location_input("bare-object"))
        .unwrap();
    assert_eq!(handle.url().as_str(), "second://bucket/object");
}

#[test]
fn registered_arguments_bind_typed_settings_and_create_handles_for_claimed_schemes() {
    let backend = StorageBackend::with_args::<MemoryArgs>()
        .name("memory")
        .schemes(["mem", "memory"])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(memory_object_path)
        .object_store_creator(in_memory_object_store)
        .bare_location_mapper(map_memory_bare_location)
        .build()
        .unwrap();
    let (mut command, registry) = command_and_registry([backend]);

    let help = command.render_long_help().to_string();
    assert!(help.contains("--memory-label"));
    assert_eq!(registry.by_scheme("mem").unwrap().name(), "memory");
    assert_eq!(registry.by_scheme("memory").unwrap().name(), "memory");
    assert!(registry.by_scheme("MEM").is_none());

    let matches = command
        .try_get_matches_from(["storage-test", "--memory-label", "bound"])
        .unwrap();
    let storage = registry.create_session(&matches).unwrap();
    let location = location_input("mem://bucket/object");
    let handle = storage.input_handle(&location).unwrap();

    assert_eq!(handle.url().as_str(), "mem://bucket/object");
    assert_eq!(handle.object_path().as_ref(), "object");
    assert_eq!(LAST_LABEL.lock().unwrap().as_deref(), Some("bound"));

    let encoded_location = location_input("mem://bucket/data%20set");
    let encoded = storage.input_handle(&encoded_location).unwrap();
    assert_eq!(encoded.url().as_str(), "mem://bucket/data%20set");
    assert_eq!(encoded.object_path().as_ref(), "data set");

    let unicode_location = location_input("mem://bucket/r%C3%A9sum%C3%A9");
    let unicode = storage.input_handle(&unicode_location).unwrap();
    assert_eq!(unicode.url().as_str(), "mem://bucket/r%C3%A9sum%C3%A9");
    assert_eq!(unicode.object_path().as_ref(), "résumé");

    let bare = location_input("literal ?#% résumé");
    let mapped = storage.input_handle(&bare).unwrap();
    assert_eq!(mapped.url().as_str(), "mem://bound/mapped-object");
    assert_eq!(
        LAST_BARE_LOCATION.lock().unwrap().as_deref(),
        Some("literal ?#% résumé")
    );

    let unsupported = location_input("other://bucket/object");
    assert!(matches!(
        storage.input_handle(&unsupported),
        Err(StorageError::UnsupportedScheme(scheme)) if scheme == "other"
    ));
}

#[test]
fn local_path_rejects_non_file_schemes_that_have_path_shaped_urls() {
    let registry = StorageRegistry::builder()
        .register(unit_backend("memory", "mem"))
        .build()
        .unwrap();
    let storage = create_default_session(&registry);
    let location = location_input("mem:///tmp/object");
    let handle = storage.input_handle(&location).unwrap();

    assert!(matches!(
        handle.local_path(),
        Err(StorageError::InvalidFilePath(path)) if path == FilePath::new("mem:///tmp/object")
    ));
}

#[test]
fn registry_rejects_duplicate_backend_names_and_schemes() {
    let duplicate_name = StorageRegistry::builder()
        .register(unit_backend("memory", "mem"))
        .register(unit_backend("memory", "other"))
        .register(unit_backend("memory", "third"))
        .build();
    assert!(matches!(
        duplicate_name,
        Err(StorageRegistryError::DuplicateBackendName {
            name: "memory",
            occurrences: 3,
        })
    ));

    let duplicate_scheme = StorageRegistry::builder()
        .register(unit_backend("first", "mem"))
        .register(unit_backend("second", "mem"))
        .register(unit_backend("third", "mem"))
        .build();
    assert!(matches!(
        duplicate_scheme,
        Err(StorageRegistryError::DuplicateScheme {
            scheme: "mem",
            backends,
        }) if backends.as_ref() == ["first", "second", "third"]
    ));
}

#[test]
fn registry_rejects_duplicate_cli_ids_long_options_and_short_options() {
    let duplicate_id = StorageRegistry::builder()
        .register(
            StorageBackend::with_args::<SharedIdArgs>()
                .name("first")
                .schemes(["first"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .register(
            StorageBackend::with_args::<DuplicateIdArgs>()
                .name("second")
                .schemes(["second"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .build();
    assert!(matches!(
        duplicate_id,
        Err(StorageRegistryError::DuplicateCliArgument {
            argument,
            contributors,
        }) if argument == "Clap ID \"shared\""
            && contributors.as_ref() == ["first", "second"]
    ));

    let duplicate_long = StorageRegistry::builder()
        .register(
            StorageBackend::with_args::<SharedLongArgs>()
                .name("first")
                .schemes(["first"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .register(
            StorageBackend::with_args::<DuplicateLongArgs>()
                .name("second")
                .schemes(["second"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .register(
            StorageBackend::with_args::<ThirdLongArgs>()
                .name("third")
                .schemes(["third"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .build();
    assert!(matches!(
        duplicate_long,
        Err(StorageRegistryError::DuplicateCliArgument {
            argument,
            contributors,
        }) if argument == "--shared-long"
            && contributors.as_ref() == ["first", "second", "third"]
    ));

    let duplicate_short = StorageRegistry::builder()
        .register(
            StorageBackend::with_args::<SharedShortArgs>()
                .name("first")
                .schemes(["first"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .register(
            StorageBackend::with_args::<DuplicateShortArgs>()
                .name("second")
                .schemes(["second"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .build();
    assert!(matches!(
        duplicate_short,
        Err(StorageRegistryError::DuplicateCliArgument {
            argument,
            contributors,
        }) if argument == "-x"
            && contributors.as_ref() == ["first", "second"]
    ));

    let shared_retry_collision = StorageRegistry::builder()
        .register(
            StorageBackend::with_args::<SharedRetryCollisionArgs>()
                .name("memory")
                .schemes(["mem"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .shared_retries()
                .build()
                .unwrap(),
        )
        .build();
    assert!(matches!(
        shared_retry_collision,
        Err(StorageRegistryError::DuplicateCliArgument {
            argument,
            contributors,
        }) if argument == "--storage-max-retries"
            && contributors.as_ref() == ["shared storage retries", "memory"]
    ));
}

#[test]
fn backend_build_rejects_duplicate_cli_aliases() {
    let duplicate_id = StorageBackend::with_args::<DuplicateLocalIdArgs>()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadOnly)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build();
    assert!(matches!(
        duplicate_id,
        Err(StorageBackendBuildError::DuplicateCliArgument { argument })
            if argument == "Clap ID \"shared_id\""
    ));

    let duplicate_long = StorageBackend::with_args::<DuplicateLocalLongArgs>()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadOnly)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build();
    assert!(matches!(
        duplicate_long,
        Err(StorageBackendBuildError::DuplicateCliArgument { argument })
            if argument == "--shared-long"
    ));

    let duplicate_short = StorageBackend::with_args::<DuplicateLocalShortArgs>()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadOnly)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build();
    assert!(matches!(
        duplicate_short,
        Err(StorageBackendBuildError::DuplicateCliArgument { argument }) if argument == "-x"
    ));

    let duplicate_long_alias = StorageBackend::with_args::<DuplicateLongAliasArgs>()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadOnly)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build();
    assert!(matches!(
        duplicate_long_alias,
        Err(StorageBackendBuildError::DuplicateCliArgument { argument })
            if argument == "--shared-alias"
    ));

    let duplicate_short_alias = StorageBackend::with_args::<DuplicateShortAliasArgs>()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadOnly)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build();
    assert!(matches!(
        duplicate_short_alias,
        Err(StorageBackendBuildError::DuplicateCliArgument { argument }) if argument == "-x"
    ));
}

#[test]
fn registry_rejects_cli_alias_collisions_across_backends() {
    let long_alias_collision = StorageRegistry::builder()
        .register(
            StorageBackend::with_args::<LongAliasContributorArgs>()
                .name("first")
                .schemes(["first"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .register(
            StorageBackend::with_args::<LongAliasClaimantArgs>()
                .name("second")
                .schemes(["second"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .build();
    assert!(matches!(
        long_alias_collision,
        Err(StorageRegistryError::DuplicateCliArgument {
            argument,
            contributors,
        }) if argument == "--shared-alias"
            && contributors.as_ref() == ["first", "second"]
    ));

    let short_alias_collision = StorageRegistry::builder()
        .register(
            StorageBackend::with_args::<ShortAliasContributorArgs>()
                .name("first")
                .schemes(["first"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .register(
            StorageBackend::with_args::<ShortAliasClaimantArgs>()
                .name("second")
                .schemes(["second"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .build();
    assert!(matches!(
        short_alias_collision,
        Err(StorageRegistryError::DuplicateCliArgument {
            argument,
            contributors,
        }) if argument == "-x" && contributors.as_ref() == ["first", "second"]
    ));
}

#[test]
fn registry_rejects_duplicate_group_ids_and_argument_group_id_collisions() {
    let duplicate_group = StorageRegistry::builder()
        .register(
            StorageBackend::with_args::<SharedGroupArgs>()
                .name("first")
                .schemes(["first"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .register(
            StorageBackend::with_args::<DuplicateGroupArgs>()
                .name("second")
                .schemes(["second"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .build();
    assert!(matches!(
        duplicate_group,
        Err(StorageRegistryError::DuplicateCliArgument {
            argument,
            contributors,
        }) if argument == "Clap ID \"shared_group\""
            && contributors.as_ref() == ["first", "second"]
    ));

    let argument_group_collision = StorageRegistry::builder()
        .register(
            StorageBackend::with_args::<SharedGroupArgs>()
                .name("first")
                .schemes(["first"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .register(
            StorageBackend::with_args::<GroupIdAsArgumentArgs>()
                .name("second")
                .schemes(["second"])
                .access(StorageAccess::ReadOnly)
                .object_path_mapper(object_path)
                .object_store_creator(in_memory_object_store)
                .build()
                .unwrap(),
        )
        .build();
    assert!(matches!(
        argument_group_collision,
        Err(StorageRegistryError::DuplicateCliArgument {
            argument,
            contributors,
        }) if argument == "Clap ID \"shared_group\""
            && contributors.as_ref() == ["first", "second"]
    ));
}

#[test]
fn registry_rejects_multiple_bare_location_backends() {
    let first = StorageBackend::without_args()
        .name("first")
        .schemes(["first"])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .bare_location_mapper(map_first_bare_location)
        .build()
        .unwrap();
    let second = StorageBackend::without_args()
        .name("second")
        .schemes(["second"])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .bare_location_mapper(map_second_bare_location)
        .build()
        .unwrap();
    let third = StorageBackend::without_args()
        .name("third")
        .schemes(["third"])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .bare_location_mapper(map_second_bare_location)
        .build()
        .unwrap();

    let registry = StorageRegistry::builder()
        .register(first)
        .register(second)
        .register(third)
        .build();

    assert!(matches!(
        registry,
        Err(StorageRegistryError::MultipleBareLocationBackends { backends })
            if backends.as_ref() == ["first", "second", "third"]
    ));
}

#[test]
fn bare_location_mapper_must_return_a_scheme_claimed_by_its_backend() {
    let backend = StorageBackend::without_args()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .bare_location_mapper(map_mismatched_bare_location)
        .build()
        .unwrap();
    let (_, registry) = command_and_registry([backend]);
    let storage = create_default_session(&registry);
    let location = location_input("raw-object");

    assert!(matches!(
        storage.input_handle(&location),
        Err(StorageError::BareLocationSchemeMismatch {
            backend: "memory",
            scheme,
        }) if scheme == "other"
    ));
}

#[test]
fn bare_locations_are_unsupported_without_a_claiming_backend() {
    let (_, registry) = command_and_registry([unit_backend("memory", "mem")]);
    assert!(registry.bare_location_backend().is_none());
    let storage = create_default_session(&registry);
    let location = location_input("raw-object");

    assert!(matches!(
        storage.input_handle(&location),
        Err(StorageError::UnsupportedBareLocation(input)) if input == "raw-object"
    ));
}

#[test]
fn read_only_backend_rejects_output_before_invoking_its_mapper() {
    READ_ONLY_PATH_MAPPINGS.store(0, Ordering::SeqCst);
    let backend = StorageBackend::without_args()
        .name("read-only")
        .schemes(["readonly"])
        .access(StorageAccess::ReadOnly)
        .object_path_mapper(read_only_object_path)
        .object_store_creator(in_memory_object_store)
        .build()
        .unwrap();
    assert!(backend.supports(StorageDirection::Input));
    assert!(!backend.supports(StorageDirection::Output));
    let (_, registry) = command_and_registry([backend]);
    let storage = create_default_session(&registry);
    let location = location_input("readonly://source/table");

    storage.input_handle(&location).unwrap();
    assert_eq!(READ_ONLY_PATH_MAPPINGS.load(Ordering::SeqCst), 1);

    let error = storage.output_handle(&location).unwrap_err();
    assert!(matches!(
        error,
        StorageError::DirectionUnsupported {
            backend: "read-only",
            direction: StorageDirection::Output,
        }
    ));
    assert_eq!(READ_ONLY_PATH_MAPPINGS.load(Ordering::SeqCst), 1);
}

#[test]
fn unregistered_backends_are_absent_with_no_required_arguments_or_schemes() {
    let omitted = StorageBackend::with_args::<RequiredCloudArgs>()
        .name("cloud")
        .schemes(["cloud"])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build()
        .unwrap();
    let (command, registry) = command_and_registry([]);
    command
        .try_get_matches_from(["storage-test"])
        .expect("an omitted backend must not enforce its required arguments");
    assert!(registry.backends().is_empty());
    assert!(registry.by_scheme("cloud").is_none());
    let storage = create_default_session(&registry);
    let location = location_input("cloud://bucket/object");

    assert!(matches!(
        storage.input_handle(&location),
        Err(StorageError::UnsupportedScheme(scheme)) if scheme == "cloud"
    ));

    let (registered_command, _) = command_and_registry([omitted]);
    assert!(
        registered_command
            .try_get_matches_from(["storage-test"])
            .is_err()
    );
}

#[test]
fn retry_capable_backends_share_one_argument_group_and_receive_defaults() {
    let first = StorageBackend::without_args()
        .name("first")
        .schemes(["first"])
        .access(StorageAccess::ReadOnly)
        .object_path_mapper(object_path)
        .object_store_creator(retry_object_store)
        .shared_retries()
        .build()
        .unwrap();
    let second = StorageBackend::without_args()
        .name("second")
        .schemes(["second"])
        .access(StorageAccess::ReadOnly)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .shared_retries()
        .build()
        .unwrap();
    let (command, registry) = command_and_registry([first, second]);

    assert_eq!(
        command
            .get_arguments()
            .filter(|argument| argument.get_long() == Some("storage-max-retries"))
            .count(),
        1
    );
    let mut help_command = command.clone();
    assert!(
        help_command
            .render_long_help()
            .to_string()
            .contains("Maximum retries for one backend request")
    );

    let storage = create_default_session(&registry);
    let retry = storage.retry_configuration().unwrap();
    assert_eq!(retry.max_retries, 10);
    assert_eq!(retry.retry_timeout, std::time::Duration::from_secs(180));
    assert_eq!(
        retry.backoff.init_backoff,
        std::time::Duration::from_millis(100)
    );
    assert_eq!(
        retry.backoff.max_backoff,
        std::time::Duration::from_secs(15)
    );
    assert_eq!(retry.backoff.base, 2.0);

    *LAST_RETRY_COUNT.lock().unwrap() = None;
    let location = location_input("first://bucket/object");
    storage.input_handle(&location).unwrap();
    assert_eq!(*LAST_RETRY_COUNT.lock().unwrap(), Some(10));
}

#[test]
#[cfg(feature = "local")]
fn local_only_registry_omits_shared_retry_arguments() {
    let registry = StorageRegistry::builder()
        .register(silk_chiffon_storage::local::backend().unwrap())
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("storage-test"));

    assert!(
        command
            .get_arguments()
            .all(|argument| argument.get_long() != Some("storage-max-retries"))
    );
    let storage = create_default_session(&registry);
    assert!(storage.retry_configuration().is_none());
    assert_eq!(
        registry.bare_location_backend().map(StorageBackend::name),
        if cfg!(feature = "local-bare-paths") {
            Some("local")
        } else {
            None
        }
    );

    let explicit = location_input("file:///tmp/local-object");
    let input = storage.input_handle(&explicit).unwrap();
    let output = storage.output_handle(&explicit).unwrap();
    assert!(Arc::ptr_eq(&input.object_store(), &output.object_store(),));

    let bare = location_input("/tmp/local-object");
    #[cfg(feature = "local-bare-paths")]
    {
        let mapped = storage.input_handle(&bare).unwrap();
        assert_eq!(mapped.url().as_str(), "file:///tmp/local-object");
    }
    #[cfg(not(feature = "local-bare-paths"))]
    {
        assert!(matches!(storage.input_handle(&bare), Err(
            StorageError::UnsupportedBareLocation(input)
        ) if input == "/tmp/local-object"));
    }
}

#[test]
fn enabled_retries_validate_backoff_while_zero_retries_disable_validation() {
    let backend = || {
        StorageBackend::without_args()
            .name("memory")
            .schemes(["mem"])
            .access(StorageAccess::ReadOnly)
            .object_path_mapper(object_path)
            .object_store_creator(in_memory_object_store)
            .shared_retries()
            .build()
            .unwrap()
    };
    let (command, registry) = command_and_registry([backend()]);
    let invalid_matches = command
        .clone()
        .try_get_matches_from([
            "storage-test",
            "--storage-backoff-base",
            "0.5",
            "--storage-initial-backoff",
            "20s",
            "--storage-max-backoff",
            "10s",
        ])
        .unwrap();

    assert!(matches!(
        registry.create_session(&invalid_matches),
        Err(silk_chiffon_storage::StorageSessionCreationError::Retry(
            RetryConfigurationError::BackoffBaseNotGreaterThanOne(_)
                | RetryConfigurationError::InitialBackoffExceedsMaximum { .. }
        ))
    ));

    let (_, zero_registry) = command_and_registry([backend()]);
    let zero_command = zero_registry.augment_args(Command::new("storage-test"));
    let zero_matches = zero_command
        .try_get_matches_from([
            "storage-test",
            "--storage-max-retries",
            "0",
            "--storage-retry-timeout",
            "0s",
            "--storage-initial-backoff",
            "0s",
            "--storage-max-backoff",
            "0s",
            "--storage-backoff-base",
            "0.5",
        ])
        .unwrap();
    let storage = zero_registry.create_session(&zero_matches).unwrap();

    assert_eq!(storage.retry_configuration().unwrap().max_retries, 0);
}

#[test]
fn enabled_retries_reject_each_invalid_retry_dimension() {
    let backend = StorageBackend::without_args()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadOnly)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .shared_retries()
        .build()
        .unwrap();
    let (_, registry) = command_and_registry([backend]);

    let cases = [
        (
            vec!["--storage-retry-timeout", "0s"],
            "storage retry timeout must be greater than zero",
        ),
        (
            vec!["--storage-initial-backoff", "0s"],
            "storage retry initial backoff must be greater than zero",
        ),
        (
            vec!["--storage-max-backoff", "0s"],
            "storage retry maximum backoff must be greater than zero",
        ),
        (
            vec!["--storage-backoff-base", "NaN"],
            "storage retry backoff base must be finite",
        ),
        (
            vec!["--storage-backoff-base", "0.5"],
            "storage retry backoff base must be greater than 1.0",
        ),
        (
            vec!["--storage-backoff-base", "1.0"],
            "storage retry backoff base must be greater than 1.0",
        ),
        (
            vec![
                "--storage-initial-backoff",
                "20s",
                "--storage-max-backoff",
                "10s",
            ],
            "exceeds maximum backoff",
        ),
    ];

    for (arguments, expected) in cases {
        let command = registry.augment_args(Command::new("storage-test"));
        let matches = command
            .try_get_matches_from(std::iter::once("storage-test").chain(arguments.iter().copied()))
            .unwrap();
        let error = match registry.create_session(&matches) {
            Ok(_) => panic!("invalid retry configuration should not create a session"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains(expected),
            "{error:?} should contain {expected:?}"
        );
    }

    let finite_base_with_infinite_range = f64::MAX.to_string();
    let command = registry.augment_args(Command::new("storage-test"));
    let matches = command
        .try_get_matches_from([
            "storage-test",
            "--storage-backoff-base",
            finite_base_with_infinite_range.as_str(),
        ])
        .unwrap();
    let error = match registry.create_session(&matches) {
        Ok(_) => panic!("an overflowing retry multiplier should not create a session"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        silk_chiffon_storage::StorageSessionCreationError::Retry(
            RetryConfigurationError::BackoffRangeOverflow { base, .. }
        ) if base == f64::MAX
    ));
}

#[test]
fn cache_reuses_one_store_per_origin_within_each_session() {
    OBJECT_PATH_MAPPINGS.store(0, Ordering::SeqCst);
    OBJECT_STORE_CREATIONS.store(0, Ordering::SeqCst);
    let backend = StorageBackend::without_args()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadOnly)
        .object_path_mapper(counted_object_path)
        .object_store_creator(counted_object_store)
        .shared_retries()
        .build()
        .unwrap();
    let (command, registry) = command_and_registry([backend]);
    let matches = command
        .try_get_matches_from(["storage-test", "--storage-max-retries", "1"])
        .unwrap();
    let storage = registry.create_session(&matches).unwrap();
    let storage_clone = storage.clone();

    let first_location = location_input("mem://one/blue/object-a");
    let equivalent_location = location_input("mem://one/blue/object-b?version=1");
    let other_authority = location_input("mem://two/blue/object");
    let other_path = location_input("mem://one/red/object");

    let first = storage.input_handle(&first_location).unwrap();
    let equivalent = storage_clone.input_handle(&equivalent_location).unwrap();
    let authority = storage.input_handle(&other_authority).unwrap();
    let path = storage.input_handle(&other_path).unwrap();

    assert!(Arc::ptr_eq(
        &first.object_store(),
        &equivalent.object_store(),
    ));
    assert!(!Arc::ptr_eq(
        &first.object_store(),
        &authority.object_store(),
    ));
    assert!(Arc::ptr_eq(&first.object_store(), &path.object_store(),));
    assert_eq!(first.store_url().as_str(), "mem://one/");
    assert_eq!(equivalent.store_url(), first.store_url());
    assert_eq!(OBJECT_PATH_MAPPINGS.load(Ordering::SeqCst), 4);
    assert_eq!(OBJECT_STORE_CREATIONS.load(Ordering::SeqCst), 2);

    let retry_command = registry.augment_args(Command::new("storage-test"));
    let retry_matches = retry_command
        .try_get_matches_from(["storage-test", "--storage-max-retries", "2"])
        .unwrap();
    let retry_storage = registry.create_session(&retry_matches).unwrap();
    let different_retry = retry_storage.input_handle(&first_location).unwrap();
    assert!(!Arc::ptr_eq(
        &first.object_store(),
        &different_retry.object_store(),
    ));
    assert_eq!(OBJECT_PATH_MAPPINGS.load(Ordering::SeqCst), 5);
    assert_eq!(OBJECT_STORE_CREATIONS.load(Ordering::SeqCst), 3);
}

#[test]
fn backend_errors_retain_stage_specific_context() {
    let bare_backend = StorageBackend::without_args()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadWrite)
        .bare_location_mapper(failing_bare_location)
        .object_path_mapper(object_path)
        .object_store_creator(in_memory_object_store)
        .build()
        .unwrap();
    let (_, registry) = command_and_registry([bare_backend]);
    let storage = create_default_session(&registry);
    match storage
        .output_handle(&location_input("bare-object"))
        .unwrap_err()
    {
        StorageError::BareLocationMapping {
            backend,
            bare_location,
            source,
        } => {
            assert_eq!(backend, "memory");
            assert_eq!(bare_location, "bare-object");
            assert_eq!(source.to_string(), "backend-specific bare-location failure");
        }
        other => panic!("expected bare-location mapping error, got {other:?}"),
    }

    let path_backend = StorageBackend::without_args()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(failing_object_path)
        .object_store_creator(in_memory_object_store)
        .build()
        .unwrap();
    let (_, registry) = command_and_registry([path_backend]);
    let storage = create_default_session(&registry);
    let location = location_input("mem://bucket/object");
    match storage.output_handle(&location).unwrap_err() {
        StorageError::ObjectPathMapping {
            backend,
            location,
            source,
        } => {
            assert_eq!(backend, "memory");
            assert_eq!(location.as_str(), "mem://bucket/object");
            assert_eq!(source.to_string(), "backend-specific object-path failure");
        }
        other => panic!("expected object-path mapping error, got {other:?}"),
    }

    let store_backend = StorageBackend::without_args()
        .name("memory")
        .schemes(["mem"])
        .access(StorageAccess::ReadWrite)
        .object_path_mapper(object_path)
        .object_store_creator(failing_object_store)
        .build()
        .unwrap();
    let (_, registry) = command_and_registry([store_backend]);
    let storage = create_default_session(&registry);
    match storage.output_handle(&location).unwrap_err() {
        StorageError::ObjectStoreCreation {
            backend,
            store_url,
            source,
        } => {
            assert_eq!(backend, "memory");
            assert_eq!(store_url.as_str(), "mem://bucket/");
            assert_eq!(source.to_string(), "backend-specific object-store failure");
        }
        other => panic!("expected object-store creation error, got {other:?}"),
    }
}
