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
    Location, ProviderResolution, RetryConfigurationError, StorageDirection, StorageError,
    StorageProviderRegistration, StorageRegistry, StorageRegistryError,
};
use url::Url;

static LAST_LABEL: Mutex<Option<String>> = Mutex::new(None);
static LAST_RETRY_COUNT: Mutex<Option<usize>> = Mutex::new(None);
static READ_ONLY_CALLS: AtomicUsize = AtomicUsize::new(0);
static STORE_CONSTRUCTIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Args, Clone)]
struct MemoryArgs {
    #[arg(long = "memory-label", default_value = "default")]
    label: String,
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
struct SharedRetryCollisionArgs {
    #[arg(long = "storage-max-retries")]
    storage_max_retries: bool,
}

fn memory_resolution(
    location: &Location,
    settings: &MemoryArgs,
    _retry: Option<&silk_chiffon_storage::RetryConfiguration>,
) -> Result<ProviderResolution, StorageError> {
    *LAST_LABEL.lock().unwrap() = Some(settings.label.clone());
    Ok(provider_resolution(location).with_configuration("label", &settings.label))
}

fn unconfigured_resolution(
    location: &Location,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfiguration>,
) -> Result<ProviderResolution, StorageError> {
    Ok(provider_resolution(location))
}

fn retry_resolution(
    location: &Location,
    _settings: &(),
    retry: Option<&silk_chiffon_storage::RetryConfiguration>,
) -> Result<ProviderResolution, StorageError> {
    *LAST_RETRY_COUNT.lock().unwrap() = retry.map(|configuration| configuration.max_retries());
    Ok(provider_resolution(location))
}

fn path_configured_resolution(
    location: &Location,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfiguration>,
) -> Result<ProviderResolution, StorageError> {
    let path = ObjectPath::from_url_path(location.url().path())?;
    let configuration = path
        .parts()
        .next()
        .map(|part| part.as_ref().to_owned())
        .unwrap_or_default();
    let authority = location.url().host_str().unwrap_or_default();
    let store_url = Url::parse(&format!("{}://{authority}", location.url().scheme())).unwrap();
    Ok(ProviderResolution::from_factory(store_url, path, || {
        STORE_CONSTRUCTIONS.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>)
    })
    .with_configuration("path-configuration", configuration))
}

fn read_only_resolution(
    location: &Location,
    _settings: &(),
    _retry: Option<&silk_chiffon_storage::RetryConfiguration>,
) -> Result<ProviderResolution, StorageError> {
    READ_ONLY_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(provider_resolution(location))
}

fn provider_resolution(location: &Location) -> ProviderResolution {
    let authority = location.url().host_str().unwrap_or_default();
    let store_url = Url::parse(&format!("{}://{authority}", location.url().scheme())).unwrap();
    let path = ObjectPath::from_url_path(location.url().path()).unwrap();
    ProviderResolution::from_factory(store_url, path, || {
        Ok(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>)
    })
}

fn unit_registration(name: &'static str, scheme: &'static str) -> StorageProviderRegistration {
    StorageProviderRegistration::without_args(name)
        .schemes([scheme])
        .input(unconfigured_resolution)
        .output(unconfigured_resolution)
        .build()
}

fn command_and_registry(
    registrations: impl IntoIterator<Item = StorageProviderRegistration>,
) -> (Command, StorageRegistry) {
    let mut builder = StorageRegistry::builder();
    for registration in registrations {
        builder = builder.register(registration);
    }
    let registry = builder.build().unwrap();
    let command = registry.augment_args(Command::new("storage-test"));
    (command, registry)
}

fn bind_defaults(registry: &StorageRegistry) -> silk_chiffon_storage::StorageResolver {
    let command = registry.augment_args(Command::new("storage-test"));
    let matches = command
        .try_get_matches_from(["storage-test"])
        .expect("default arguments should parse");
    registry
        .bind_args(&matches)
        .expect("default arguments should bind")
}

#[test]
fn registered_arguments_contribute_help_bind_typed_settings_and_resolve_declared_schemes() {
    let registration = StorageProviderRegistration::with_args::<MemoryArgs>("memory")
        .aliases(["ram"])
        .schemes(["mem"])
        .input(memory_resolution)
        .output(memory_resolution)
        .build();
    let (mut command, registry) = command_and_registry([registration]);

    let help = command.render_long_help().to_string();
    assert!(help.contains("--memory-label"));
    assert_eq!(registry.get("RAM").unwrap().name(), "memory");
    assert_eq!(registry.by_scheme("MEM").unwrap().name(), "memory");

    let matches = command
        .try_get_matches_from(["storage-test", "--memory-label", "bound"])
        .unwrap();
    let resolver = registry.bind_args(&matches).unwrap();
    let location = registry
        .parse_location("mem://bucket/object", FilePath::new("/work"))
        .unwrap();
    let object = resolver.resolve_input(&location).unwrap();

    assert_eq!(object.url.as_str(), "mem://bucket/object");
    assert_eq!(object.path.as_ref(), "object");
    assert_eq!(LAST_LABEL.lock().unwrap().as_deref(), Some("bound"));

    let encoded_location = registry
        .parse_location("mem://bucket/data%20set", FilePath::new("/work"))
        .unwrap();
    let encoded = resolver.resolve_input(&encoded_location).unwrap();
    assert_eq!(encoded.url.as_str(), "mem://bucket/data%20set");
    assert_eq!(encoded.path.as_ref(), "data set");

    assert!(matches!(
        registry.parse_location("other://bucket/object", FilePath::new("/work")),
        Err(StorageError::UnsupportedScheme(scheme)) if scheme == "other"
    ));
    for invalid in [
        "mem:/bucket/object",
        "MEM://bucket/object",
        "mem://bucket/object?version=1",
        "mem://bucket/object#fragment",
        "mem://user:password@bucket/object",
        "mem://bucket/a/../object",
        "mem://bucket/a/./object",
        "mem://bucket/a/%2E%2E/object",
        "mem://bucket/%ZZ",
    ] {
        assert!(
            registry
                .parse_location(invalid, FilePath::new("/work"))
                .is_err(),
            "{invalid:?} should be rejected"
        );
    }
}

#[test]
fn registry_rejects_duplicate_provider_names_schemes_and_aliases() {
    let duplicate_name = StorageRegistry::builder()
        .register(unit_registration("memory", "mem"))
        .register(unit_registration("MEMORY", "other"))
        .build();
    assert!(matches!(
        duplicate_name,
        Err(StorageRegistryError::DuplicateName(name)) if name == "memory"
    ));

    let duplicate_scheme = StorageRegistry::builder()
        .register(unit_registration("first", "mem"))
        .register(unit_registration("second", "MEM"))
        .build();
    assert!(matches!(
        duplicate_scheme,
        Err(StorageRegistryError::DuplicateScheme(scheme)) if scheme == "mem"
    ));

    let duplicate_alias = StorageRegistry::builder()
        .register(
            StorageProviderRegistration::without_args("first")
                .aliases(["memory"])
                .schemes(["first"])
                .build(),
        )
        .register(
            StorageProviderRegistration::without_args("second")
                .aliases(["MEMORY"])
                .schemes(["second"])
                .build(),
        )
        .build();
    assert!(matches!(
        duplicate_alias,
        Err(StorageRegistryError::DuplicateAlias(alias)) if alias == "memory"
    ));
}

#[test]
fn registry_rejects_duplicate_cli_ids_long_options_and_short_options() {
    let duplicate_id = StorageRegistry::builder()
        .register(
            StorageProviderRegistration::with_args::<SharedIdArgs>("first")
                .schemes(["first"])
                .build(),
        )
        .register(
            StorageProviderRegistration::with_args::<DuplicateIdArgs>("second")
                .schemes(["second"])
                .build(),
        )
        .build();
    assert!(matches!(
        duplicate_id,
        Err(StorageRegistryError::DuplicateCliArgument(argument)) if argument == "shared"
    ));

    let duplicate_long = StorageRegistry::builder()
        .register(
            StorageProviderRegistration::with_args::<SharedLongArgs>("first")
                .schemes(["first"])
                .build(),
        )
        .register(
            StorageProviderRegistration::with_args::<DuplicateLongArgs>("second")
                .schemes(["second"])
                .build(),
        )
        .build();
    assert!(matches!(
        duplicate_long,
        Err(StorageRegistryError::DuplicateCliArgument(argument)) if argument == "second_long"
    ));

    let duplicate_short = StorageRegistry::builder()
        .register(
            StorageProviderRegistration::with_args::<SharedShortArgs>("first")
                .schemes(["first"])
                .build(),
        )
        .register(
            StorageProviderRegistration::with_args::<DuplicateShortArgs>("second")
                .schemes(["second"])
                .build(),
        )
        .build();
    assert!(matches!(
        duplicate_short,
        Err(StorageRegistryError::DuplicateCliArgument(argument)) if argument == "second_short"
    ));

    let shared_retry_collision = StorageRegistry::builder()
        .register(
            StorageProviderRegistration::with_args::<SharedRetryCollisionArgs>("memory")
                .schemes(["mem"])
                .shared_retries()
                .build(),
        )
        .build();
    assert!(matches!(
        shared_retry_collision,
        Err(StorageRegistryError::DuplicateCliArgument(argument))
            if argument == "storage_max_retries"
    ));
}

#[test]
fn read_only_provider_rejects_output_before_invoking_its_callback() {
    READ_ONLY_CALLS.store(0, Ordering::SeqCst);
    let registration = StorageProviderRegistration::without_args("read-only")
        .schemes(["readonly"])
        .input(read_only_resolution)
        .build();
    let (_, registry) = command_and_registry([registration]);
    let resolver = bind_defaults(&registry);
    let location = registry
        .parse_location("readonly://source/table", FilePath::new("/work"))
        .unwrap();

    resolver.resolve_input(&location).unwrap();
    assert_eq!(READ_ONLY_CALLS.load(Ordering::SeqCst), 1);

    let error = resolver.resolve_output(&location).unwrap_err();
    assert!(matches!(
        error,
        StorageError::DirectionUnsupported {
            provider: "read-only",
            direction: StorageDirection::Output,
        }
    ));
    assert_eq!(READ_ONLY_CALLS.load(Ordering::SeqCst), 1);
}

#[test]
fn disabled_provider_reports_its_registration_diagnostic() {
    let registration = StorageProviderRegistration::without_args("cloud")
        .schemes(["cloud"])
        .feature_disabled_diagnostic("rebuild with the cloud feature")
        .build();
    let (_, registry) = command_and_registry([registration]);
    let resolver = bind_defaults(&registry);
    let location = registry
        .parse_location("cloud://bucket/object", FilePath::new("/work"))
        .unwrap();

    assert!(matches!(
        resolver.resolve_input(&location),
        Err(StorageError::ProviderDisabled {
            provider: "cloud",
            diagnostic: "rebuild with the cloud feature",
        })
    ));
}

#[test]
fn retry_capable_providers_share_one_argument_group_and_receive_defaults() {
    let first = StorageProviderRegistration::without_args("first")
        .schemes(["first"])
        .input(retry_resolution)
        .shared_retries()
        .build();
    let second = StorageProviderRegistration::without_args("second")
        .schemes(["second"])
        .input(unconfigured_resolution)
        .shared_retries()
        .build();
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
            .contains("Maximum retries for one provider request")
    );

    let resolver = bind_defaults(&registry);
    let retry = resolver.retry_configuration().unwrap();
    assert_eq!(retry.max_retries(), 10);
    assert_eq!(retry.retry_timeout(), std::time::Duration::from_secs(180));
    assert_eq!(
        retry.initial_backoff(),
        std::time::Duration::from_millis(100)
    );
    assert_eq!(retry.max_backoff(), std::time::Duration::from_secs(15));
    assert_eq!(retry.backoff_base(), 2.0);

    *LAST_RETRY_COUNT.lock().unwrap() = None;
    let location = registry
        .parse_location("first://bucket/object", FilePath::new("/work"))
        .unwrap();
    resolver.resolve_input(&location).unwrap();
    assert_eq!(*LAST_RETRY_COUNT.lock().unwrap(), Some(10));
}

#[test]
fn local_only_registry_omits_shared_retry_arguments() {
    let registry = StorageRegistry::builder()
        .register(silk_chiffon_storage::local::registration())
        .build()
        .unwrap();
    let command = registry.augment_args(Command::new("storage-test"));

    assert!(
        command
            .get_arguments()
            .all(|argument| argument.get_long() != Some("storage-max-retries"))
    );
    let resolver = bind_defaults(&registry);
    assert!(resolver.retry_configuration().is_none());

    #[cfg(feature = "local")]
    {
        let location = registry
            .parse_location("local-object", FilePath::new("/work"))
            .unwrap();
        let input = resolver.resolve_input(&location).unwrap();
        let output = resolver.resolve_output(&location).unwrap();
        assert!(Arc::ptr_eq(&input.store, &output.store));
    }

    #[cfg(not(feature = "local"))]
    {
        let location = registry
            .parse_location("local-object", FilePath::new("/work"))
            .unwrap();
        assert!(matches!(
            resolver.resolve_input(&location),
            Err(StorageError::ProviderDisabled {
                provider: "local",
                ..
            })
        ));
    }
}

#[test]
fn enabled_retries_validate_backoff_while_zero_retries_disable_validation() {
    let registration = StorageProviderRegistration::without_args("memory")
        .schemes(["mem"])
        .input(unconfigured_resolution)
        .shared_retries()
        .build();
    let (command, registry) = command_and_registry([registration.clone()]);
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
        registry.bind_args(&invalid_matches),
        Err(silk_chiffon_storage::StorageResolverBuildError::Retry(
            RetryConfigurationError::BackoffBaseBelowOne(_)
                | RetryConfigurationError::InitialBackoffExceedsMaximum { .. }
        ))
    ));

    let (_, zero_registry) = command_and_registry([registration]);
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
    let resolver = zero_registry.bind_args(&zero_matches).unwrap();

    assert_eq!(resolver.retry_configuration().unwrap().max_retries(), 0);
}

#[test]
fn enabled_retries_reject_each_invalid_retry_dimension() {
    let registration = StorageProviderRegistration::without_args("memory")
        .schemes(["mem"])
        .input(unconfigured_resolution)
        .shared_retries()
        .build();
    let (_, registry) = command_and_registry([registration]);

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
            "storage retry backoff base must be at least 1.0",
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
        let error = match registry.bind_args(&matches) {
            Ok(_) => panic!("invalid retry configuration should not bind"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains(expected),
            "{error:?} should contain {expected:?}"
        );
    }
}

#[test]
fn cache_reuses_equivalent_stores_and_separates_authority_configuration_and_retries() {
    STORE_CONSTRUCTIONS.store(0, Ordering::SeqCst);
    let registration = StorageProviderRegistration::without_args("memory")
        .schemes(["mem"])
        .input(path_configured_resolution)
        .shared_retries()
        .build();
    let (command, registry) = command_and_registry([registration]);
    let matches = command
        .try_get_matches_from(["storage-test", "--storage-max-retries", "1"])
        .unwrap();
    let resolver = registry.bind_args(&matches).unwrap();

    let first_location = registry
        .parse_location("mem://one/blue/object-a", FilePath::new("/work"))
        .unwrap();
    let equivalent_location = registry
        .parse_location("mem://one/blue/object-b", FilePath::new("/work"))
        .unwrap();
    let other_authority = registry
        .parse_location("mem://two/blue/object", FilePath::new("/work"))
        .unwrap();
    let other_configuration = registry
        .parse_location("mem://one/red/object", FilePath::new("/work"))
        .unwrap();

    let first = resolver.resolve_input(&first_location).unwrap();
    let equivalent = resolver.resolve_input(&equivalent_location).unwrap();
    let authority = resolver.resolve_input(&other_authority).unwrap();
    let configuration = resolver.resolve_input(&other_configuration).unwrap();

    assert!(Arc::ptr_eq(&first.store, &equivalent.store));
    assert!(!Arc::ptr_eq(&first.store, &authority.store));
    assert!(!Arc::ptr_eq(&first.store, &configuration.store));
    assert_ne!(first.cache_key(), authority.cache_key());
    assert_ne!(first.cache_key(), configuration.cache_key());
    assert_eq!(STORE_CONSTRUCTIONS.load(Ordering::SeqCst), 3);

    let retry_command = registry.augment_args(Command::new("storage-test"));
    let retry_matches = retry_command
        .try_get_matches_from(["storage-test", "--storage-max-retries", "2"])
        .unwrap();
    let retry_resolver = registry.bind_args(&retry_matches).unwrap();
    let different_retry = retry_resolver.resolve_input(&first_location).unwrap();
    assert_ne!(first.cache_key(), different_retry.cache_key());
    assert_eq!(STORE_CONSTRUCTIONS.load(Ordering::SeqCst), 4);
}

#[test]
fn storage_binding_source_has_no_dynamic_settings_bag_or_downcast() {
    fn collect_rust_source(path: &FilePath, source: &mut String) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                collect_rust_source(&path, source);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                source.push_str(&std::fs::read_to_string(path).unwrap());
            }
        }
    }

    let mut source = String::new();
    collect_rust_source(
        &FilePath::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut source,
    );

    for forbidden in [
        "std::any::Any",
        "dyn Any",
        ".downcast_ref",
        ".downcast_mut",
        "StorageRuntimeSettings",
        "provider_settings::<",
        "SettingsMismatch",
    ] {
        assert!(
            !source.contains(forbidden),
            "storage binding source contains forbidden dynamic-settings pattern {forbidden:?}"
        );
    }
}
