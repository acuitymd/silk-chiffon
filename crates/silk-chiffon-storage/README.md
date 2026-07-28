# Silk Chiffon storage

`silk-chiffon-storage` maps exact paths and registered URLs to upstream `object_store` clients and object paths. It owns strict location parsing, typed provider registration, shared retry settings, and command-scoped client caching.

## Resolve a local location

An exact location identifies one object. Parsing establishes its URL without checking whether the object exists.

```rust
use silk_chiffon_storage::{Location, StorageResolver};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let working_directory = std::env::current_dir()?;
    let location = Location::parse("data/input.parquet", working_directory)?;
    let resolver = StorageResolver::new()?;
    let resolved = resolver.resolve_input(&location)?;

    assert_eq!(resolved.url.scheme(), "file");
    Ok(())
}
```

`ResolvedLocation` contains the absolute `url`, the upstream `Arc<dyn ObjectStore>`, and the upstream `object_store::path::Path`. `local_path` adapts the resolved URL for code that requires a filesystem path.

## Compose and bind providers

A `StorageProviderRegistration` declares one provider's names, URL schemes, ordinary Clap argument type, and optional input and output callbacks. A provider can register either direction or both. Asking a read-only provider to resolve an output returns `StorageError::DirectionUnsupported` before its callback runs.

The executable builds a registry explicitly, adds its arguments to a Clap command, and binds one command's matches:

```rust
use clap::Command;
use silk_chiffon_storage::{StorageRegistry, local};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = StorageRegistry::builder()
        .register(local::registration())
        .build()?;
    let command = registry.augment_args(Command::new("storage-example"));
    let matches = command.try_get_matches_from(["storage-example"])?;
    let resolver = registry.bind_args(&matches)?;

    let location = registry.parse_location("data/input.parquet", std::env::current_dir()?)?;
    let input = resolver.resolve_input(&location)?;
    let output = resolver.resolve_output(&location)?;

    assert!(std::sync::Arc::ptr_eq(&input.store, &output.store));
    Ok(())
}
```

Binding keeps each provider's parsed argument value with callbacks that accept that concrete type. Private behavior objects hide the provider implementation while retaining that binding.

Provider-owned long options follow the `--{provider}-*` convention, such as `--gcs-endpoint` or `--s3-region`. Shared options may use a global name. Registry construction rejects collisions across provider names, aliases, schemes, and every Clap identifier.

## Shared retries

A provider opts into shared retry settings with `StorageProviderRegistrationBuilder::shared_retries`. If at least one registration opts in, the registry contributes this argument group once:

| Argument                    | Default | Meaning                                      |
| --------------------------- | ------- | -------------------------------------------- |
| `--storage-max-retries`     | `10`    | Maximum retries for one provider request     |
| `--storage-retry-timeout`   | `3m`    | Total retry window for one provider request  |
| `--storage-initial-backoff` | `100ms` | First delay before a retry                   |
| `--storage-max-backoff`     | `15s`   | Maximum delay between retries                |
| `--storage-backoff-base`    | `2`     | Multiplier used by the provider retry policy |

Durations use `humantime` syntax. With retries enabled, time values must be nonzero, the initial backoff cannot exceed the maximum, and the base must be finite and at least `1.0`. Setting `--storage-max-retries=0` disables those semantic checks. Clap still rejects values it cannot parse.

A local-only registry omits the group. Providers that do not opt in receive no retry configuration.

## Accepted location grammar

`Location::parse` accepts the local forms below. `StorageRegistry::parse_location` also accepts the exact `scheme://authority/path` syntax for schemes in that registry.

| Input                        | Meaning                                                                                   |
| ---------------------------- | ----------------------------------------------------------------------------------------- |
| `data/input.parquet`         | A bare relative filesystem path, resolved against the explicit absolute working directory |
| `/data/input.parquet`        | A bare absolute filesystem path                                                           |
| `file:///data/input.parquet` | A canonical local file URL                                                                |

Local file URL input must use lowercase `file:` followed by exactly three slashes. Alternate spellings such as `file:/data`, `file://localhost/data`, `FILE:///data`, and `file:////data` are rejected rather than normalized.

Both parsers reject:

- empty input
- noncanonical local file URLs
- unsupported, malformed, ambiguous, or noncanonical scheme-like input
- query strings and fragments
- embedded user information
- invalid percent encoding
- paths that cannot become upstream object paths

A registered URL scheme must be lowercase and followed by `://`. A filename that resembles a scheme can be made unambiguous with an explicit relative prefix such as `./name:value.parquet`.

Path handling is lexical and does not call `canonicalize`, resolve symlinks, or require the target to exist. Converting a bare path to a URL performs only the encoding and URL path processing needed for an absolute local file URL. Once a canonical `file:///` input passes the spelling check, URL parsing may decode valid percent encoding.

Glob patterns have a separate contract because `?` has a different meaning in a URL. They do not pass through `Location::parse`.

## Store identity and DataFusion

Each call to `StorageRegistry::bind_args` creates a command-scoped `StorageResolver`. The resolver caches stores by scheme, authority, effective provider configuration, and shared retry configuration. It invokes a provider's client factory only after a cache miss, so equivalent locations construct one client. Different bound commands do not share clients.

`ResolvedLocation::register_with_datafusion` registers that same `Arc` with a DataFusion `RuntimeEnv`. Direct object-store calls and DataFusion scans therefore use the same client.

## Existence and output policy

Resolution does not check storage:

- `validate_input` requires `head` to find an existing object.
- `preflight_output` allows an absent object, rejects an existing object when overwrite is disabled, and skips `head` when overwrite is enabled.

Keeping these checks separate lets callers resolve a destination before writing a new object.

## Cargo feature

The default `local` feature enables `object_store/fs` and the narrow DataFusion execution dependency used for registration. Without default features, parsing and provider registration remain available. Resolving a local location returns the diagnostic attached to the disabled local registration.
