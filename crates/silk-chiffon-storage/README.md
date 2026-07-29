# Silk Chiffon storage

`silk-chiffon-storage` parses filesystem paths and storage URLs into exact locations, then resolves registered schemes to upstream `object_store` clients and object paths. It owns strict location parsing, typed provider registration, shared retry settings, and command-scoped client caching.

## Resolve a local location

A `Location` identifies one storage location and exposes its canonical URL representation. Parsing a bare filesystem path creates the equivalent absolute `file:///` URL without checking scheme support or touching storage.

```rust
use silk_chiffon_storage::{Location, StorageResolver};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let working_directory = std::env::current_dir()?;
    let location = Location::parse("data/input.parquet", working_directory)?;
    let resolver = StorageResolver::local()?;
    let resolved = resolver.resolve_input(&location)?;

    assert_eq!(resolved.url.scheme(), "file");
    Ok(())
}
```

`ResolvedLocation` contains the absolute `url`, the upstream `Arc<dyn ObjectStore>`, and the upstream `object_store::path::Path`. `local_path` adapts the resolved URL for code that requires a filesystem path.

## Compose and bind providers

A `StorageProviderRegistration` declares one provider's name, URL schemes, ordinary Clap argument type, access, and resolver. `StorageAccess` declares read-only, write-only, or read-write support separately from URL resolution. Asking a read-only provider to resolve an output returns `StorageError::DirectionUnsupported` before its resolver runs.

Registration ends with either `enabled(access, resolver)` or `disabled(diagnostic)`, so a registration cannot omit both a resolver and an explanation.

The executable builds a registry explicitly, adds its arguments to a Clap command, and binds one command's matches:

```rust
use clap::Command;
use silk_chiffon_storage::{Location, StorageRegistry, local};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = StorageRegistry::builder()
        .register(local::registration())
        .build()?;
    let command = registry.augment_args(Command::new("storage-example"));
    let matches = command.try_get_matches_from(["storage-example"])?;
    let resolver = registry.bind_args(&matches)?;

    let location = Location::parse("data/input.parquet", std::env::current_dir()?)?;
    let input = resolver.resolve_input(&location)?;
    let output = resolver.resolve_output(&location)?;

    assert!(std::sync::Arc::ptr_eq(&input.store, &output.store));
    Ok(())
}
```

Binding parses each provider's concrete argument type once and stores the value with the resolver that accepts that type. The command-scoped resolver holds that pair behind a private behavior trait so providers with different argument types can share one registry. Settings never pass through `Any` or downcasting.

Provider-owned long options follow the `--{provider}-*` convention, such as `--gcs-endpoint` or `--s3-region`. Shared options may use a global name. Registry construction rejects collisions across provider names, schemes, and every Clap identifier.

## Shared retries

A provider opts into shared retry settings with `StorageProviderRegistrationBuilder::shared_retries`. If at least one registration opts in, the registry contributes this argument group once:

| Argument                    | Default | Meaning                                      |
| --------------------------- | ------- | -------------------------------------------- |
| `--storage-max-retries`     | `10`    | Maximum retries for one provider request     |
| `--storage-retry-timeout`   | `3m`    | Total retry window for one provider request  |
| `--storage-initial-backoff` | `100ms` | First delay before a retry                   |
| `--storage-max-backoff`     | `15s`   | Maximum delay between retries                |
| `--storage-backoff-base`    | `2`     | Multiplier used by the provider retry policy |

Durations use `humantime` syntax. With retries enabled, time values must be nonzero, the initial backoff cannot exceed the maximum, and the base must be finite and greater than `1.0`. Setting `--storage-max-retries=0` disables those semantic checks. Clap still rejects values it cannot parse.

A local-only registry omits the group. Providers that do not opt in receive no retry configuration. Participating resolvers receive `object_store::RetryConfig`, including its `BackoffConfig`, so they can pass the validated settings directly to an upstream store builder.

## Accepted location grammar

`Location::parse` accepts bare filesystem paths and canonical storage URLs. It normalizes bare paths to absolute file URLs. Scheme registration is checked later when a `StorageResolver` resolves the location.

| Input                        | Meaning                                                                                   |
| ---------------------------- | ----------------------------------------------------------------------------------------- |
| `data/input.parquet`         | A bare relative filesystem path, resolved against the explicit absolute working directory |
| `/data/input.parquet`        | A bare absolute filesystem path                                                           |
| `file:///data/input.parquet` | A canonical local file URL                                                                |
| `s3://bucket/input.parquet`  | A canonical storage URL whose scheme is checked during resolution                         |

### Bare paths

Bare input is a filesystem path, not URL source text. It may contain spaces and Unicode. Literal `%`, `?`, and `#` characters are also accepted. Converting the path to the internal absolute `file:///` URL applies the necessary percent encoding without changing the filesystem path. For example, the `%20` sequence in the bare filename `literal%20name.parquet` remains literal and does not name `literal name.parquet`.

### URLs

Input with a scheme is URL source text. Its path must percent-encode characters that URL syntax does not allow literally. For example, a URL uses `data%20set.parquet` rather than `data set.parquet`, while a bare path accepts either filename as written.

Local file URLs must use lowercase `file:` followed by exactly three slashes. Other storage URLs must use a lowercase scheme followed by `://`. Alternate file spellings such as `file:/data`, `file://localhost/data`, `FILE:///data`, and `file:////data` are rejected rather than normalized. URL input preserves query strings for the provider or downstream consumer. It rejects fragments, embedded user information, malformed percent encoding, and paths that require implicit encoding or normalization.

A query is separate from the object path. `s3://bucket/data?version=1` resolves the path `data` and preserves `version=1` on the URL. A provider may interpret, pass through, ignore, or reject that query. Bare paths remain filesystem paths, so `data?version=1` names a file containing `?`.

### Validation layers

Validation is split by layer:

- `Location::parse` rejects empty, malformed, ambiguous, or noncanonical input. It does not require a registered scheme or impose `object_store` path rules.
- `StorageResolver` rejects unregistered schemes and unsupported directions.
- Provider resolvers own authority and path validation.

Path handling is lexical and does not call `canonicalize`, resolve symlinks, or require the target to exist. Converting a bare path to a URL performs only the encoding and URL path processing needed for an absolute local file URL. Once a canonical `file:///` input passes the spelling check, URL parsing may decode valid percent encoding.

Provider resolvers accept `Location` rather than raw input. A filename that resembles a scheme can be made unambiguous with an explicit relative prefix such as `./name:value.parquet`.

Glob patterns have a separate contract because `?` has a different meaning in a URL. They do not pass through `Location::parse`.

## Store identity and DataFusion

Each call to `StorageRegistry::bind_args` creates a command-scoped `StorageResolver`. The resolver caches one store for each URL origin: scheme, host, and port. Paths and queries do not create another store because DataFusion uses the same origin identity for object-store registration. Provider settings and retry settings are fixed for the lifetime of a bound resolver. Different bound commands do not share clients.

`ResolvedLocation::store_url` exposes the origin URL. The pipeline registers the resolved `Arc<dyn ObjectStore>` with DataFusion, while this crate remains independent of DataFusion.

## Existence and output policy

Resolution does not check storage:

- `validate_input` requires `head` to find an existing object.
- `preflight_output` allows an absent object, rejects an existing object when overwrite is disabled, and skips `head` when overwrite is enabled.

Keeping these checks separate lets callers resolve a destination before writing a new object.

## Cargo feature

The default `local` feature enables `object_store/fs`. The crate enables `object_store/cloud` for shared retry types but does not enable a concrete cloud backend. Without default features, parsing and provider registration remain available. Resolving a local location returns the diagnostic attached to the disabled local registration.
