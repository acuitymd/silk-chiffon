# Silk Chiffon storage

`silk-chiffon-storage` resolves exact local locations into upstream `object_store` clients and object paths for Silk Chiffon contributors.

## Resolve an exact location

An exact location identifies one object. Parsing establishes the URL and object path without checking whether the object exists.

```rust
use silk_chiffon_storage::{Location, StorageResolver};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let working_directory = std::env::current_dir()?;
    let location = Location::parse("data/input.parquet", working_directory)?;
    let resolved = StorageResolver::new().resolve(&location)?;

    assert_eq!(resolved.url.scheme(), "file");
    Ok(())
}
```

`ResolvedLocation` contains the absolute `url`, the upstream `Arc<dyn ObjectStore>`, and the upstream `object_store::path::Path`. `local_path` is a transitional adapter for code that still requires a filesystem path.

## Accepted location grammar

The crate accepts a small exact-location grammar:

| Input                        | Meaning                                                                                   |
| ---------------------------- | ----------------------------------------------------------------------------------------- |
| `data/input.parquet`         | A bare relative filesystem path, resolved against the explicit absolute working directory |
| `/data/input.parquet`        | A bare absolute filesystem path                                                           |
| `file:///data/input.parquet` | A canonical local file URL                                                                |

Local file URL input must use lowercase `file:` followed by exactly three slashes. Alternate spellings such as `file:/data`, `file://localhost/data`, `FILE:///data`, and `file:////data` are rejected rather than normalized.

The parser rejects:

- empty input
- noncanonical local file URLs
- unsupported, malformed, or ambiguous scheme-like input
- query strings and fragments
- invalid percent encoding
- paths that cannot become upstream object paths

A filename that resembles a scheme can be made unambiguous with an explicit relative prefix such as `./name:value.parquet`.

Path handling is lexical and does not call `canonicalize`, resolve symlinks, or require the target to exist. Converting a bare path to a URL performs only the encoding and URL path processing needed for an absolute local file URL. Once a canonical `file:///` input passes the spelling check, URL parsing may decode valid percent encoding.

Glob patterns have a separate contract because `?` has a different meaning in a URL. They do not pass through `Location::parse`.

## Store identity and DataFusion

`StorageResolver` caches stores by scheme, authority, and effective provider configuration. The local key has the `file` scheme, an empty authority, and no provider settings, so local locations resolved by the same resolver share one `Arc<dyn ObjectStore>`.

`ResolvedLocation::register_with_datafusion` registers that same `Arc` with a DataFusion `RuntimeEnv`. Direct object-store calls and DataFusion scans therefore use the same client.

## Existence and output policy

Resolution does not check storage:

- `validate_input` requires `head` to find an existing object.
- `preflight_output` allows an absent object, rejects an existing object when overwrite is disabled, and skips `head` when overwrite is enabled.

Keeping these checks separate lets callers resolve a destination before writing a new object.

## Cargo feature

The default `local` feature enables `object_store/fs` and the narrow DataFusion execution dependency used for registration. Without default features, location parsing remains available and local resolution returns a feature-disabled error.
