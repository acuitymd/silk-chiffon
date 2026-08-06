# Silk Chiffon storage

`silk-chiffon-storage` classifies exact location input, routes it through explicitly registered storage providers, and resolves it to upstream `object_store` clients and object paths. It owns strict URL parsing, typed provider registration, shared retry settings, and command-scoped client caching without assigning filesystem semantics to generic schemeless input.

## Resolve a local location

`LocationInput` preserves the distinction between an explicit URL and schemeless text. With the default feature set, the built-in local registration claims schemeless input and maps a bare filesystem path to an absolute `file:///` `Location` during resolution.

```rust
use silk_chiffon_storage::{LocationInput, StorageResolver};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let location = LocationInput::parse("data/input.parquet")?;
    let resolver = StorageResolver::local()?;
    let resolved = resolver.resolve_input(&location)?;

    assert_eq!(resolved.url.scheme(), "file");
    Ok(())
}
```

`ResolvedLocation` contains the absolute `url`, the upstream `Arc<dyn ObjectStore>`, and the upstream `object_store::path::Path`. `local_path` adapts the resolved URL for code that requires a filesystem path.

## Compose and bind providers

A `StorageProviderRegistration` declares one available provider's name, URL schemes, ordinary Clap argument type, access, and resolver. `StorageAccess` declares read-only, write-only, or read-write support separately from URL resolution. Asking a read-only provider to resolve an output returns `StorageError::DirectionUnsupported` before its resolver runs.

The host executable controls availability by choosing which registrations to include. An omitted provider contributes no schemes or CLI arguments, does not appear in registry introspection, and cannot collide with a registered provider. Resolving one of its URLs returns `StorageError::UnsupportedScheme`.

One registration may call `bare_locations(mapper)` to claim all schemeless input. The mapper receives the original text and the provider's parsed settings, then returns a canonical `Location` using one of that registration's schemes. Registry construction rejects a second claimant, and resolution rejects a mapper result whose scheme belongs to another provider or to no provider.

The executable builds a registry explicitly, adds its arguments to a Clap command, and binds one command's matches:

```rust
use clap::Command;
use silk_chiffon_storage::{LocationInput, StorageRegistry, local};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = StorageRegistry::builder()
        .register(local::registration())
        .build()?;
    let command = registry.augment_args(Command::new("storage-example"));
    let matches = command.try_get_matches_from(["storage-example"])?;
    let resolver = registry.bind_args(&matches)?;

    let location = LocationInput::parse("data/input.parquet")?;
    let input = resolver.resolve_input(&location)?;
    let output = resolver.resolve_output(&location)?;

    assert!(std::sync::Arc::ptr_eq(&input.store, &output.store));
    Ok(())
}
```

Binding parses each provider's concrete argument type once and stores the value with the resolver and optional bare-location mapper that accept that type. The command-scoped resolver holds each typed group behind a private behavior trait so providers with different argument types can share one registry. Settings never pass through `Any` or downcasting.

Provider-owned long options follow the `--{provider}-*` convention, such as `--gcs-endpoint` or `--s3-region`. Shared options may use a global name. Registry construction rejects collisions across provider names, schemes, the bare-location claim, and every Clap identifier.

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

`LocationInput::parse` classifies nonempty input without consulting the registry. Canonical explicit URLs become `LocationInput::Url(Location)`. Input without URL syntax becomes `LocationInput::Bare(String)` and is preserved exactly until the registered bare-location provider interprets it.

| Input                        | Meaning                                                                           |
| ---------------------------- | --------------------------------------------------------------------------------- |
| `data/input.parquet`         | Schemless text; the registered bare-location provider decides what it means       |
| `/data/input.parquet`        | Schemless text; the default local mapper treats it as an absolute filesystem path |
| `file:///data/input.parquet` | A canonical local file URL                                                        |
| `s3://bucket/input.parquet`  | A canonical storage URL whose scheme is checked during resolution                 |

### Bare locations

Core does not assume that schemeless input names a file. It preserves spaces, Unicode, and literal `%`, `?`, and `#` characters in the `String` passed to the claiming provider. A provider may interpret that text using its own typed command-line settings before mapping it to one of its explicit URL schemes.

With `local-bare-paths`, the local provider treats bare input as a filesystem path. Relative paths use the process working directory, and absolute paths stay absolute. Mapping applies the encoding needed for an absolute `file:///` URL without changing the filesystem path. For example, the `%20` sequence in the bare filename `literal%20name.parquet` remains literal and does not name `literal name.parquet`.

### URLs

Input with a scheme is URL source text. Its path must percent-encode characters that URL syntax does not allow literally. For example, a URL uses `data%20set.parquet` rather than `data set.parquet`, while a bare location preserves either spelling for its provider to interpret.

Local file URLs must use lowercase `file:` followed by exactly three slashes. Other storage URLs must use a lowercase scheme followed by `://`. Alternate file spellings such as `file:/data`, `file://localhost/data`, `FILE:///data`, and `file:////data` are rejected rather than normalized. URL input preserves query strings for the provider or downstream consumer. It rejects fragments, embedded user information, malformed percent encoding, and paths that require implicit encoding or normalization.

A query is separate from the object path. `s3://bucket/data?version=1` resolves the path `data` and preserves `version=1` on the URL. A provider may interpret, pass through, ignore, or reject that query. The local bare-location mapper treats `data?version=1` as a filename containing `?` because bare input never passes through URL parsing.

### Validation layers

Validation is split by layer:

- `LocationInput::parse` rejects empty, malformed, ambiguous, or noncanonical explicit URL input. It preserves other input without requiring a registered provider or imposing `object_store` path rules.
- `StorageResolver` rejects unregistered schemes, bare input without a claimant, unsupported directions, and bare mappers that return a scheme the selected provider does not own.
- Bare-location mappers own the meaning assigned to schemeless text. Provider resolvers own authority and object-path validation after every input has a canonical `Location`.

Local path handling is lexical and does not call `canonicalize`, resolve symlinks, or require the target to exist. Mapping a bare local path to a URL performs only the encoding and URL path processing needed for an absolute local file URL. Once a canonical `file:///` input passes the spelling check, URL parsing may decode valid percent encoding.

Provider resolvers accept `Location` rather than raw input; only a provider's optional `BareLocationMapper<T>` sees schemeless source text. A bare location that resembles a scheme can be made syntactically unambiguous with a separator before its colon, such as `./name:value.parquet`.

Glob patterns have a separate contract because `?` has a different meaning in a URL. They do not pass through `LocationInput::parse`.

## Store identity and DataFusion

Each call to `StorageRegistry::bind_args` creates a command-scoped `StorageResolver`. The resolver caches one store for each URL origin: scheme, host, and port. Paths and queries do not create another store because DataFusion uses the same origin identity for object-store registration. Provider settings and retry settings are fixed for the lifetime of a bound resolver. Different bound commands do not share clients.

`ResolvedLocation::store_url` exposes the origin URL. The pipeline registers the resolved `Arc<dyn ObjectStore>` with DataFusion, while this crate remains independent of DataFusion.

## Existence and output policy

Resolution does not check storage:

- `validate_input` requires `head` to find an existing object.
- `preflight_output` allows an absent object, rejects an existing object when overwrite is disabled, and skips `head` when overwrite is enabled.

Keeping these checks separate lets callers resolve a destination before writing a new object.

## Cargo features

The `local` feature enables `object_store/fs` and exposes the built-in registration for explicit `file:///` URLs. `local-bare-paths` depends on `local` and makes that provider claim schemeless input. It is the default feature so existing bare local paths continue to work.

Use `default-features = false, features = ["local"]` to keep explicit `file:///` support while leaving the bare-location route available for another provider. With neither local feature, the crate contributes no `file:` registration, so that scheme is unsupported unless the host registers another owner. Location parsing and provider registration remain available. The crate enables `object_store/cloud` for shared retry types but does not enable a concrete cloud backend.
