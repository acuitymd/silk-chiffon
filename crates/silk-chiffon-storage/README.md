# Silk Chiffon storage

`silk-chiffon-storage` turns exact storage locations and object-path patterns into object-store handles. It routes locations through typed backend settings and caches object-store clients within each command session. It does not assume that schemeless input names a local file.

## Create a local handle

`LocationInput` preserves the distinction between an explicit URL and a bare string, meaning input with no URL scheme. With the default feature set, the built-in local backend claims bare strings and interprets them as filesystem paths.

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
    let handle = storage.input_handle(&location)?;

    assert_eq!(handle.url().scheme(), "file");
    Ok(())
}
```

The public handle types record what the caller may do. `InputHandle` exposes a read-only object-store view. `OutputTarget` exists only while the backend applies its output policy. `PreparedOutputTarget` proves that the session claim, existence policy, and backend preparation have succeeded, so a sink can begin writing. All three retain the same canonical URL, decoded object path, store-root URL, and cached client identity without allowing an input capability to be passed to an output API.

Input handle creation selects and invokes a backend without checking existence. `StorageSession::prepare_output_target` resolves and claims an output, optionally observes external existence, then invokes backend preparation such as local parent-directory handling.

## Understand the lifecycle

The public types separate configuration that lasts for the executable from state that lasts for one command invocation.

| Type                   | Lifetime and responsibility                                                                                                              |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `LocationPattern`      | One parsed exact location or object-path glob. It contains syntax only and owns no store or command policy.                              |
| `StorageBackend`       | One immutable backend definition: registry name, schemes, access, Clap behavior, and typed callbacks.                                    |
| `StorageRegistry`      | One validated and indexed collection of the backends available in this build. It contains no parsed command settings.                    |
| `StorageSession`       | One command invocation's parsed backend settings, retry configuration, routing indexes, and object-store cache. Clones share this state. |
| `InputHandle`          | One canonical exact input with a read-only object-store view.                                                                            |
| `OutputTarget`         | One selected and claimed output visible to backend preparation code.                                                                     |
| `PreparedOutputTarget` | One output whose claim, external-existence policy, and backend preparation succeeded.                                                    |

The host executable chooses which backends exist, lets the registry augment its Clap command, parses the complete host command, and gives those matches back to the registry:

```rust
use clap::Command;
use silk_chiffon_storage::{ExistingOutput, LocationInput, OutputPreparation, StorageRegistry, local};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = StorageRegistry::builder()
        .register(local::backend()?)
        .build()?;

    let command = registry.augment_args(Command::new("storage-example"));
    let matches = command.try_get_matches_from(["storage-example"])?;
    let storage = registry.create_session(&matches)?;

    let location = LocationInput::parse("data/input.parquet")?;
    let input = storage.input_handle(&location)?;
    let output = storage
        .prepare_output_target(
            &location,
            &OutputPreparation::new(ExistingOutput::Allow, false),
        )
        .await?;

    assert_eq!(input.store_url(), output.store_url());
    Ok(())
}
```

`StorageRegistry::augment_args` only adds storage arguments to the command the host owns. The registry never parses process arguments. `StorageRegistry::create_session` receives the host-parsed `ArgMatches`, parses one settings value per backend, shared retry settings when needed, and object-upload settings, then starts a fresh object-store cache and upload limiter.

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
- `PrepareOutputTargetFn<T>` performs backend-specific target preparation after the session has claimed the normalized target and applied the advisory external-existence policy. The local backend validates or creates parent directories here.

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

`LocationPattern::parse` keeps pattern syntax separate from exact `LocationInput` parsing. An exact pattern still follows normal routing, performs one `head` request, and returns either one input object or no objects. An active glob lists through the selected `ObjectStore`, starting at the longest prefix made entirely of complete literal path segments, then matches the complete canonical object path.

Matching is case-sensitive. `*` and `?` do not cross `/`; `**` crosses path segments and must occupy a complete segment. Leading dots have no special treatment. Character classes use `glob::Pattern` syntax. Glob syntax is valid only in the object path, never in the scheme or authority.

Explicit pattern URLs use one raw `?` as the one-character wildcard. Percent-encode a literal question mark as `%3F`. Because ordinary URL syntax also uses `?`, spell the query delimiter as `??` in a pattern operand:

```text
s3://bucket/part-?.parquet??versionId=one
```

Each matched exact URL uses the ordinary single `?query` spelling. Matched object names percent-encode `*`, `?`, `[`, and `]`, so those characters remain literal when the URL is parsed through exact-only `LocationInput`. Pass a generated URL with a query as an exact input, or change its query delimiter back to `??` before using it as a new pattern operand. `StorageSession::expand_input_pattern` returns zero or more `InputObject` values in unspecified order. Each value retains the metadata returned by the exact `head` or active-glob listing. The calling application owns no-match policy, ordering, and deduplication.

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

Each directional handle exposes the cache key through `store_url` and returns cheap shared ownership of its permitted object-store interface through `object_store`. A host may register an input pair directly or place a root-scoped view in front of it for DataFusion-specific cache identity and diagnostics. This crate itself remains independent of DataFusion.

## Existence and output policy

Input lookup and output policy remain explicit:

- `StorageSession::lookup_input` calls `head` and returns an `InputObject` containing the observed metadata.
- `StorageSession::prepare_output_target` atomically retains a command-session claim on normalized `(store_url, object_path)` identity, then applies `ExistingOutput::Allow` or `ExistingOutput::RejectIfObserved` and the backend callback. A second same-session claim fails even when overwrite is allowed.
- `ObjectUpload` owns the one-object put or multipart lifecycle. `complete` is the durability boundary, while `abort` cancels in-flight work and awaits multipart cleanup.

The observed input or output metadata is neither a snapshot nor an external reservation. Callers require selected inputs to remain stable for the command lifetime, and another process may race an advisory output check.

Every session uses a 10 MiB adaptive single-put threshold and multipart part size by default, with at most eight part requests in flight across all of its uploads. The registry contributes `--object-store-upload-part-size` and `--object-store-max-in-flight-parts` to the host command so each command invocation can override those defaults.

Pattern expansion is the exception: an exact `LocationPattern` calls `head` once so absence can contribute zero matches without listing.

## Cargo features

The `local` feature enables `object_store/fs` and exposes `local::backend` for explicit `file:///` locations. `local-bare-paths` depends on `local` and also makes that backend claim bare input. It is the default feature.

Use `default-features = false, features = ["local"]` to keep explicit local URLs while leaving the bare route available for another backend. With neither feature, the crate exposes no built-in local backend functions. A host may still define and register other backends.

The `gcs` and `s3` features expose `gcs::backend` and `s3::backend`. A host still decides whether to register either backend. The pinned `object_store` release gates `RetryConfig` behind its provider-neutral `cloud` base feature, so the storage crate enables that base without selecting GCP or AWS.

| Feature | Registered by the Silk Chiffon host | Typed non-secret command settings                                                    | Credential discovery                                                      |
| ------- | ----------------------------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------- |
| `gcs`   | `gs://`                             | endpoint, anonymous mode, request timeout                                            | Upstream Application Default Credentials and environment discovery        |
| `s3`    | `s3://`                             | region, endpoint, path or virtual-hosted addressing, anonymous mode, request timeout | Upstream AWS environment, web-identity, container, and instance discovery |

Credential values are not part of either backend's Clap settings. The builders are created with `GoogleCloudStorageBuilder::from_env` and `AmazonS3Builder::from_env`, then receive only the typed command overrides and the session's shared `RetryConfig`. Proxy and client-transport tuning remain in upstream configuration or are unsupported by the command. The command also leaves S3 encryption, checksums, conditional writes, requester-pays behavior, S3 Express, and metadata options unsupported.

Cloud locations require a bucket host and reject ports and query parameters. Core location parsing rejects fragments and embedded user information before backend validation. `s3a://` is not registered. The pinned upstream S3 builder can parse that spelling, but Silk Chiffon would need cache and output-claim canonicalization before two schemes could identify one remote object safely.

`--gcs-anonymous` and `--s3-anonymous` disable credential discovery and request signing. The pinned `object_store` GCS mutation path still emits the exact empty marker `Authorization: Bearer`. It carries no credential material. Anonymous access does not make an object writable. The S3 builder omits the header for anonymous reads and writes, and the GCS builder omits it for anonymous reads.

An explicit S3 HTTP endpoint enables upstream HTTP support for that endpoint. Path-style addressing appends the bucket to the endpoint. With virtual-hosted addressing, the endpoint must already include the bucket name, matching the pinned upstream builder's endpoint contract.

## Opt-in live tests

Normal storage and command tests use in-memory stores or loopback HTTP servers. The ignored live targets compile without credentials and run only when explicitly selected. Set an explicit bucket and a prefix with at least two non-root path segments:

| Provider | Bucket variable                | Prefix variable                |
| -------- | ------------------------------ | ------------------------------ |
| GCS      | `SILK_CHIFFON_LIVE_GCS_BUCKET` | `SILK_CHIFFON_LIVE_GCS_PREFIX` |
| S3       | `SILK_CHIFFON_LIVE_S3_BUCKET`  | `SILK_CHIFFON_LIVE_S3_PREFIX`  |

Each run appends a unique child to the configured prefix. The test rejects a bucket value that could alter URL authority or path structure, cleans only that child prefix, and reports any leftover objects. Run a provider target only after reviewing the bucket and prefix:

```bash
cargo test -p silk-chiffon-storage --test cloud_live --features gcs live_gcs_exact_patterns_ranges_outputs_multipart_claims_and_cleanup -- --ignored --exact
cargo test --test cloud_live_e2e live_gcs_composed_cli_detects_inspects_transforms_verifies_and_cleans_up -- --ignored --exact

cargo test -p silk-chiffon-storage --test cloud_live --features s3 live_s3_exact_patterns_ranges_outputs_multipart_claims_and_cleanup -- --ignored --exact
cargo test --test cloud_live_e2e live_s3_composed_cli_detects_inspects_transforms_verifies_and_cleans_up -- --ignored --exact
```

The storage target covers exact and pattern inputs, metadata, ranges, uploads, overwrite observation, session claims, multipart behavior, and cleanup. The root target seeds a formatted object and exercises the composed `detect`, `inspect`, and `transform` paths. It verifies the output and cleans its run prefix.
