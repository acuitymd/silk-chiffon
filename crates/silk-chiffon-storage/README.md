# Silk Chiffon storage

`silk-chiffon-storage` turns exact storage locations and object-path patterns into object-store handles. It routes locations through typed backend settings and caches object-store clients within each command session. It does not assume that schemeless input names a local file.

## Create a local handle

`LocationInput` preserves the distinction between an explicit URL and a bare string, meaning input with no URL scheme. With the default feature set, the built-in local backend claims bare strings and interprets them as filesystem paths.

```rust
use silk_chiffon_storage::{LocationInput, local};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let location = LocationInput::parse("data/input.parquet")?;
    let storage = local::session()?;
    let handle = storage.input_handle(&location)?;

    assert_eq!(handle.url().scheme(), "file");
    Ok(())
}
```

A `StorageHandle` keeps the canonical location URL, an `Arc<dyn ObjectStore>`, the object path decoded from that URL, and the root URL that identifies the cached client. Its fields are private so values from different handle requests cannot be mixed accidentally. `StorageHandle::local_path` adapts a `file:` handle for code that still requires a filesystem path.

Handle creation selects and invokes a backend. It does not check whether an input exists or whether an output may be overwritten.

## Understand the lifecycle

The public types separate configuration that lasts for the executable from state that lasts for one command invocation.

| Type              | Lifetime and responsibility                                                                                                              |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `LocationPattern` | One parsed exact location or object-path glob. It contains syntax only and owns no store or command policy.                              |
| `StorageBackend`  | One immutable backend definition: registry name, schemes, access, Clap behavior, and typed callbacks.                                    |
| `StorageRegistry` | One validated and indexed collection of the backends available in this build. It contains no parsed command settings.                    |
| `StorageSession`  | One command invocation's parsed backend settings, retry configuration, routing indexes, and object-store cache. Clones share this state. |
| `StorageHandle`   | One canonical exact location paired with the object store and object path needed to access it.                                           |

The host executable chooses which backends exist, lets the registry augment its Clap command, parses the complete host command, and gives those matches back to the registry:

```rust
use clap::Command;
use silk_chiffon_storage::{LocationInput, StorageRegistry, local};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = StorageRegistry::builder()
        .register(local::backend()?)
        .build()?;

    let command = registry.augment_args(Command::new("storage-example"));
    let matches = command.try_get_matches_from(["storage-example"])?;
    let storage = registry.create_session(&matches)?;

    let location = LocationInput::parse("data/input.parquet")?;
    let input = storage.input_handle(&location)?;
    let output = storage.output_handle(&location)?;

    assert!(std::sync::Arc::ptr_eq(
        &input.object_store(),
        &output.object_store(),
    ));
    Ok(())
}
```

`StorageRegistry::augment_args` only adds storage arguments to the command the host owns. The registry never parses process arguments. `StorageRegistry::create_session` receives the host-parsed `ArgMatches`, parses one settings value per backend, parses shared retry settings when needed, and starts a fresh object-store cache.

Calling `create_session` again produces independently parsed settings and a fresh cache. Cloning one session shares its settings and cache.

## Define a backend

A backend crate starts with `StorageBackend::with_args::<T>()` when it contributes a Clap `Args` type, or `StorageBackend::without_args()` when it has no settings. Setters may be called in any order. The final `build` validates that all required pieces are present and that the definition is internally unambiguous.

```rust,ignore
let backend = StorageBackend::with_args::<CloudArgs>()
    .name("example-cloud")
    .schemes(["example"])
    .access(StorageAccess::ReadWrite)
    .bare_location_mapper(map_bare_location)
    .bare_pattern_mapper(map_bare_pattern)
    .location_validator(validate_location)
    .object_store_creator(create_object_store)
    .shared_retries()
    .build()?;
```

The settings type `T` stays coupled to the parser and every callback that accepts `&T`. The registry can therefore store backends from unrelated crates without putting settings into `Any` or asking callers to downcast them. The backend definition retains functions typed over `T`. Creating a session produces a backend binding: that backend's parsed `T` paired with its typed callbacks. Private behavior traits let `StorageRegistry` store definitions and `StorageSession` invoke bindings without naming each concrete settings type.

The callbacks divide handle creation and pattern routing into four backend-owned decisions:

- `BareLocationMapper<T>` is an optional callback. When configured, it maps the original schemeless text to a canonical `Location` and claims the registry's single bare-location route.
- `BarePatternMapper<T>` is an optional callback for schemeless patterns. It requires the same backend to claim exact bare locations and must return a `LocationPattern` under one of that backend's schemes.
- Every backend must choose a location-validation policy. `LocationValidator<T>` checks authority, query, and other backend-specific URL rules after routing; `allow_any_location()` explicitly accepts every location that passed core syntax validation. Storage derives the `ObjectPath` generically from the decoded URL path.
- `ObjectStoreCreatorFn<T>` creates a client for one store-root URL. It runs only on a session cache miss and receives shared retry configuration only when the backend opted in.

`StorageAccess` declares read-only, write-only, or read-write support independently of those callbacks. A session rejects an unsupported direction before location validation or store creation.

## Registry invariants

Registration means availability. A backend omitted by a Cargo feature or by the host claims no schemes or CLI arguments, appears in no registry introspection, and cannot participate in a collision. A URL using a scheme claimed only by an omitted backend returns `StorageError::UnsupportedScheme`.

Backend construction validates its own name, schemes, required callbacks, access declaration, and contributed Clap keys. Registry construction then rejects conflicts across the complete available set:

- backend names must be unique;
- every claimed URL scheme has exactly one registered owner;
- every Clap ID, long option, long alias, short option, and short alias has exactly one storage contributor; and
- at most one backend may claim bare locations.

`StorageRegistry::backends` preserves registration order. `by_scheme` performs exact lowercase lookup, and `bare_location_backend` exposes the optional bare-route owner.

Backend-specific long options should follow the `--{backend}-*` convention, such as `--gcs-endpoint` or `--s3-region`. Shared arguments may use global names.

## Bare locations

A bare location is source text with no explicit URL scheme. `LocationInput::parse` preserves that text exactly, and a session gives it only to the backend that claimed the bare route. That backend may interpret it using its own parsed settings before returning a canonical `Location` under one of its registered schemes.

This route is not inherently local. A future backend could interpret a bare string relative to a configured bucket, namespace, working root, or another command option. The registry rejects a second claimant, and the session rejects a mapper result whose scheme is not owned by the selected backend.

With `local-bare-paths`, the local mapper treats bare input as a filesystem path. Relative paths use the process working directory, and absolute paths stay absolute. It converts the absolute path with `Url::from_file_path`; it does not call `canonicalize`, resolve symlinks, or require the target to exist.

Bare text preserves spaces, Unicode, and literal `%`, `?`, and `#` characters. For example, `literal%20name.parquet` remains a filename containing those three literal characters rather than naming `literal name.parquet`.

## Accepted URL syntax

`LocationInput::parse` classifies nonempty input without consulting the registry. Canonical explicit URLs become `LocationInput::Url(Location)`. Input without a colon before its first path separator becomes `LocationInput::Bare(String)`. A colon in that position starts URL-like syntax: a valid scheme prefix is parsed as an explicit URL, while an invalid prefix is rejected as ambiguous. Silk Chiffon is Unix-only; the storage crate rejects non-Unix builds at compile time.

| Input                        | Meaning                                                                       |
| ---------------------------- | ----------------------------------------------------------------------------- |
| `data/input.parquet`         | Bare text whose meaning belongs to the registered bare backend.               |
| `/data/input.parquet`        | Bare text; the default local mapper treats it as an absolute filesystem path. |
| `file:///data/input.parquet` | A canonical local file URL.                                                   |
| `s3://bucket/input.parquet`  | A canonical storage URL routed by its scheme.                                 |

Local file URLs must use lowercase `file:` followed by exactly three slashes. Other storage URLs must use a lowercase scheme followed by `://`. Rejected variants are not silently normalized. Explicit URLs reject fragments, embedded user information, malformed percent encoding, and paths that require implicit encoding or normalization.

A query remains on the canonical URL and is syntactically separate from the URL path. The location validator receives the full `Location`, so a backend may use, ignore, or reject the query. In exact bare input, `?` remains an ordinary character for the selected backend to interpret.

## Location patterns

`LocationPattern::parse` keeps pattern syntax separate from exact `LocationInput` parsing. An exact pattern still follows normal routing, performs one `head` request, and returns either one handle or no handles. An active glob lists through the selected `ObjectStore`, starting at the longest prefix made entirely of complete literal path segments, then matches the complete canonical object path.

Matching is case-sensitive. `*` and `?` do not cross `/`; `**` crosses path segments and must occupy a complete segment. Leading dots have no special treatment. Character classes use `glob::Pattern` syntax. Glob syntax is valid only in the object path, never in the scheme or authority.

Explicit pattern URLs use one raw `?` as the one-character wildcard. Percent-encode a literal question mark as `%3F`. Because ordinary URL syntax also uses `?`, spell the query delimiter as `??` in a pattern operand:

```text
s3://bucket/part-?.parquet??versionId=one
```

Each matched exact URL uses the ordinary single `?query` spelling. Matched object names percent-encode `*`, `?`, `[`, and `]`, so those characters remain literal when the URL is parsed through exact-only `LocationInput`. Pass a generated URL with a query as an exact input, or change its query delimiter back to `??` before using it as a new pattern operand. `StorageSession::expand_input_pattern` returns zero or more handles in unspecified order; the calling application owns no-match policy, ordering, and deduplication.

## Shared retries

A backend opts into shared retry settings with `StorageBackendBuilder::shared_retries`. If at least one registered backend opts in, the registry contributes this argument group once:

| Argument                    | Default | Meaning                                      |
| --------------------------- | ------- | -------------------------------------------- |
| `--storage-max-retries`     | `10`    | Maximum retries for one backend request.     |
| `--storage-retry-timeout`   | `3m`    | Elapsed-time limit checked after a failure.  |
| `--storage-initial-backoff` | `100ms` | First delay before a retry.                  |
| `--storage-max-backoff`     | `15s`   | Maximum delay between retries.               |
| `--storage-backoff-base`    | `2`     | Multiplier used by the backend retry policy. |

Durations use `humantime` syntax. With retries enabled, time values must be nonzero, the initial backoff cannot exceed the maximum, and the base must be finite and greater than `1.0`. Setting `--storage-max-retries=0` disables those semantic checks, though Clap still rejects values it cannot parse.

Backends that do not opt in receive no retry configuration. Participating object-store factories receive the validated upstream `object_store::RetryConfig` and may pass it directly to an upstream store builder.

## Store identity and DataFusion

A session caches one object-store client per store-root URL: scheme, host, and port, with the path reset to `/` and the query and fragment removed. Location validation and generic object-path derivation still run on cache hits. The object-store creator runs only on a cache miss while the cache lock is held, so concurrent requests cannot create duplicate clients for the same root.

`StorageHandle::store_url` exposes the cache key, and `StorageHandle::object_store` returns a cheap clone of the shared client pointer. The pipeline registers that pair with DataFusion. This crate itself remains independent of DataFusion.

## Existence and output policy

Ordinary handle creation performs neither an object-existence check nor an overwrite check. Those policies remain explicit:

- `validate_input` calls `head` and requires the input object to exist.
- `ensure_output_absent` permits an absent object and rejects an existing object. Callers skip it when overwrite is enabled.

Keeping these checks separate lets callers create an input handle without forcing an eager existence check and create an output handle before its object exists.

Pattern expansion is the exception: an exact `LocationPattern` calls `head` once so absence can contribute zero matches without listing.

## Cargo features

The `local` feature enables `object_store/fs` and exposes `local::backend` and `local::session` for explicit `file:///` locations. `local-bare-paths` depends on `local` and also makes that backend claim bare input. It is the default feature.

Use `default-features = false, features = ["local"]` to keep explicit local URLs while leaving the bare route available for another backend. With neither feature, the crate exposes no built-in local backend functions. A host may still define and register other backends.

The crate enables `object_store/cloud` for shared retry types but does not register a concrete cloud backend.
