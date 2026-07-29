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
    Location, ProviderResolution, RetryConfig, RetryConfigurationError, StorageAccess,
    StorageDirection, StorageError, StorageProviderRegistration, StorageRegistry,
    StorageRegistryError,
};

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
    _retry: Option<&RetryConfig>,
) -> anyhow::Result<ProviderResolution> {
    *LAST_LABEL.lock().unwrap() = Some(settings.label.clone());
    Ok(provider_resolution(location))
}

fn unconfigured_resolution(
    location: &Location,
    _settings: &(),
    _retry: Option<&RetryConfig>,
) -> anyhow::Result<ProviderResolution> {
    Ok(provider_resolution(location))
}

fn retry_resolution(
    location: &Location,
    _settings: &(),
    retry: Option<&RetryConfig>,
) -> anyhow::Result<ProviderResolution> {
    *LAST_RETRY_COUNT.lock().unwrap() = retry.map(|configuration| configuration.max_retries);
    Ok(provider_resolution(location))
}

fn counted_resolution(
    location: &Location,
    _settings: &(),
    _retry: Option<&RetryConfig>,
) -> anyhow::Result<ProviderResolution> {
    let path = ObjectPath::from_url_path(location.url().path())?;
    Ok(ProviderResolution::from_factory(path, || {
        STORE_CONSTRUCTIONS.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>)
    }))
}

fn read_only_resolution(
    location: &Location,
    _settings: &(),
    _retry: Option<&RetryConfig>,
) -> anyhow::Result<ProviderResolution> {
    READ_ONLY_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(provider_resolution(location))
}

fn failing_resolution(
    _location: &Location,
    _settings: &(),
    _retry: Option<&RetryConfig>,
) -> anyhow::Result<ProviderResolution> {
    anyhow::bail!("provider-specific resolution failure")
}

fn failing_factory_resolution(
    location: &Location,
    _settings: &(),
    _retry: Option<&RetryConfig>,
) -> anyhow::Result<ProviderResolution> {
    let path = ObjectPath::from_url_path(location.url().path())?;
    Ok(ProviderResolution::from_factory(path, || {
        anyhow::bail!("provider-specific factory failure")
    }))
}

fn provider_resolution(location: &Location) -> ProviderResolution {
    let path = ObjectPath::from_url_path(location.url().path()).unwrap();
    ProviderResolution::from_factory(path, || {
        Ok(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>)
    })
}

fn unit_registration(name: &'static str, scheme: &'static str) -> StorageProviderRegistration {
    StorageProviderRegistration::without_args(name)
        .schemes([scheme])
        .enabled(StorageAccess::ReadWrite, unconfigured_resolution)
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
        .schemes(["mem"])
        .enabled(StorageAccess::ReadWrite, memory_resolution);
    let (mut command, registry) = command_and_registry([registration]);

    let help = command.render_long_help().to_string();
    assert!(help.contains("--memory-label"));
    assert_eq!(registry.by_scheme("MEM").unwrap().name(), "memory");

    let matches = command
        .try_get_matches_from(["storage-test", "--memory-label", "bound"])
        .unwrap();
    let resolver = registry.bind_args(&matches).unwrap();
    let location = Location::parse("mem://bucket/object", FilePath::new("/work")).unwrap();
    let object = resolver.resolve_input(&location).unwrap();

    assert_eq!(object.url.as_str(), "mem://bucket/object");
    assert_eq!(object.path.as_ref(), "object");
    assert_eq!(LAST_LABEL.lock().unwrap().as_deref(), Some("bound"));

    let encoded_location =
        Location::parse("mem://bucket/data%20set", FilePath::new("/work")).unwrap();
    let encoded = resolver.resolve_input(&encoded_location).unwrap();
    assert_eq!(encoded.url.as_str(), "mem://bucket/data%20set");
    assert_eq!(encoded.path.as_ref(), "data set");

    let unicode_location =
        Location::parse("mem://bucket/r%C3%A9sum%C3%A9", FilePath::new("/work")).unwrap();
    let unicode = resolver.resolve_input(&unicode_location).unwrap();
    assert_eq!(unicode.url.as_str(), "mem://bucket/r%C3%A9sum%C3%A9");
    assert_eq!(unicode.path.as_ref(), "résumé");

    let unsupported = Location::parse("other://bucket/object", FilePath::new("/work")).unwrap();
    assert!(matches!(
        resolver.resolve_input(&unsupported),
        Err(StorageError::UnsupportedScheme(scheme)) if scheme == "other"
    ));
}

#[test]
fn registry_rejects_duplicate_provider_names_and_schemes() {
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
}

#[test]
fn registry_rejects_duplicate_cli_ids_long_options_and_short_options() {
    let duplicate_id = StorageRegistry::builder()
        .register(
            StorageProviderRegistration::with_args::<SharedIdArgs>("first")
                .schemes(["first"])
                .disabled("unused"),
        )
        .register(
            StorageProviderRegistration::with_args::<DuplicateIdArgs>("second")
                .schemes(["second"])
                .disabled("unused"),
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
                .disabled("unused"),
        )
        .register(
            StorageProviderRegistration::with_args::<DuplicateLongArgs>("second")
                .schemes(["second"])
                .disabled("unused"),
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
                .disabled("unused"),
        )
        .register(
            StorageProviderRegistration::with_args::<DuplicateShortArgs>("second")
                .schemes(["second"])
                .disabled("unused"),
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
                .disabled("unused"),
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
        .enabled(StorageAccess::ReadOnly, read_only_resolution);
    assert!(registration.has_input());
    assert!(!registration.has_output());
    let (_, registry) = command_and_registry([registration]);
    let resolver = bind_defaults(&registry);
    let location = Location::parse("readonly://source/table", FilePath::new("/work")).unwrap();

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
        .disabled("rebuild with the cloud feature");
    assert!(!registration.has_input());
    assert!(!registration.has_output());
    let (_, registry) = command_and_registry([registration]);
    let resolver = bind_defaults(&registry);
    let location = Location::parse("cloud://bucket/object", FilePath::new("/work")).unwrap();

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
        .shared_retries()
        .enabled(StorageAccess::ReadOnly, retry_resolution);
    let second = StorageProviderRegistration::without_args("second")
        .schemes(["second"])
        .shared_retries()
        .enabled(StorageAccess::ReadOnly, unconfigured_resolution);
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
    let location = Location::parse("first://bucket/object", FilePath::new("/work")).unwrap();
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
        let location = Location::parse("local-object", FilePath::new("/work")).unwrap();
        let input = resolver.resolve_input(&location).unwrap();
        let output = resolver.resolve_output(&location).unwrap();
        assert!(Arc::ptr_eq(&input.store, &output.store));
    }

    #[cfg(not(feature = "local"))]
    {
        let location = Location::parse("local-object", FilePath::new("/work")).unwrap();
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
        .shared_retries()
        .enabled(StorageAccess::ReadOnly, unconfigured_resolution);
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
            RetryConfigurationError::BackoffBaseNotGreaterThanOne(_)
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

    assert_eq!(resolver.retry_configuration().unwrap().max_retries, 0);
}

#[test]
fn enabled_retries_reject_each_invalid_retry_dimension() {
    let registration = StorageProviderRegistration::without_args("memory")
        .schemes(["mem"])
        .shared_retries()
        .enabled(StorageAccess::ReadOnly, unconfigured_resolution);
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
fn cache_reuses_one_store_per_origin_within_each_bound_command() {
    STORE_CONSTRUCTIONS.store(0, Ordering::SeqCst);
    let registration = StorageProviderRegistration::without_args("memory")
        .schemes(["mem"])
        .shared_retries()
        .enabled(StorageAccess::ReadOnly, counted_resolution);
    let (command, registry) = command_and_registry([registration]);
    let matches = command
        .try_get_matches_from(["storage-test", "--storage-max-retries", "1"])
        .unwrap();
    let resolver = registry.bind_args(&matches).unwrap();

    let first_location =
        Location::parse("mem://one/blue/object-a", FilePath::new("/work")).unwrap();
    let equivalent_location =
        Location::parse("mem://one/blue/object-b?version=1", FilePath::new("/work")).unwrap();
    let other_authority = Location::parse("mem://two/blue/object", FilePath::new("/work")).unwrap();
    let other_path = Location::parse("mem://one/red/object", FilePath::new("/work")).unwrap();

    let first = resolver.resolve_input(&first_location).unwrap();
    let equivalent = resolver.resolve_input(&equivalent_location).unwrap();
    let authority = resolver.resolve_input(&other_authority).unwrap();
    let path = resolver.resolve_input(&other_path).unwrap();

    assert!(Arc::ptr_eq(&first.store, &equivalent.store));
    assert!(!Arc::ptr_eq(&first.store, &authority.store));
    assert!(Arc::ptr_eq(&first.store, &path.store));
    assert_eq!(first.store_url().as_str(), "mem://one/");
    assert_eq!(equivalent.store_url(), first.store_url());
    assert_eq!(STORE_CONSTRUCTIONS.load(Ordering::SeqCst), 2);

    let retry_command = registry.augment_args(Command::new("storage-test"));
    let retry_matches = retry_command
        .try_get_matches_from(["storage-test", "--storage-max-retries", "2"])
        .unwrap();
    let retry_resolver = registry.bind_args(&retry_matches).unwrap();
    let different_retry = retry_resolver.resolve_input(&first_location).unwrap();
    assert!(!Arc::ptr_eq(&first.store, &different_retry.store));
    assert_eq!(STORE_CONSTRUCTIONS.load(Ordering::SeqCst), 3);
}

#[test]
fn provider_errors_retain_provider_direction_and_source_context() {
    for (resolver, expected) in [
        (
            failing_resolution
                as fn(&Location, &(), Option<&RetryConfig>) -> anyhow::Result<ProviderResolution>,
            "provider-specific resolution failure",
        ),
        (
            failing_factory_resolution
                as fn(&Location, &(), Option<&RetryConfig>) -> anyhow::Result<ProviderResolution>,
            "provider-specific factory failure",
        ),
    ] {
        let registration = StorageProviderRegistration::without_args("memory")
            .schemes(["mem"])
            .enabled(StorageAccess::ReadWrite, resolver);
        let (_, registry) = command_and_registry([registration]);
        let bound = bind_defaults(&registry);
        let location = Location::parse("mem://bucket/object", FilePath::new("/work")).unwrap();
        let error = bound.resolve_output(&location).unwrap_err();

        match error {
            StorageError::ProviderResolution {
                provider,
                direction,
                source,
            } => {
                assert_eq!(provider, "memory");
                assert_eq!(direction, StorageDirection::Output);
                assert_eq!(source.to_string(), expected);
            }
            other => panic!("expected provider resolution error, got {other:?}"),
        }
    }
}
