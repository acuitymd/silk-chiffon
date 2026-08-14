# 🎀 silk-chiffon

_Converting columnar data, silky-smooth._

silk-chiffon is a command-line tool for moving data between the three columnar formats we reach for most: [Apache Arrow](https://arrow.apache.org/) IPC (both the file and streaming variants), [Apache Parquet](https://parquet.apache.org/), and [Vortex](https://docs.vortex.dev/) (a newer columnar format). It reads any of the three and writes any of the three, and it can sort, filter, merge, re-encode, and partition the data on the way through.

It runs on [DataFusion](https://datafusion.apache.org/), so any reshaping you would express in SQL is a `--query` away. It also streams data in batches against a memory budget rather than reading a whole file at once, so large inputs convert without exhausting memory.

## Install

Silk Chiffon supports Unix platforms, including Linux and macOS. Prebuilt binaries for each release are on the [releases page](https://github.com/acuitymd/silk-chiffon/releases). Or build it yourself with a recent Rust toolchain:

```bash
# from a local checkout
cargo install --path .

# straight from GitHub
cargo install --git https://github.com/acuitymd/silk-chiffon
```

> [!NOTE]
> A downloaded macOS binary is unsigned, so Gatekeeper offers to move it to the trash. Clear the quarantine flag to keep it: `xattr -d com.apple.quarantine /path/to/silk-chiffon`.

## Quick start

Convert an Arrow file to Parquet, compressed and sorted:

```bash
silk-chiffon transform --from data.arrow --to data.parquet \
  --parquet-compression zstd --sort-by amount:desc
```

silk-chiffon reads the format from each file's extension (`.arrow`, `.parquet`, `.vortex`), so one command reads Arrow and writes Parquet without being told which is which. (Pass `--input-format` or `--output-format` when the extension can't say.) Then look at what you wrote:

```bash
silk-chiffon inspect parquet data.parquet
```

## Cloud storage

Google Cloud Storage and Amazon S3 are optional build features. Enable `gcs`, `s3`, or both when installing from source. Release binaries built with the default features accept local paths only.

```bash
cargo install --git https://github.com/acuitymd/silk-chiffon --features gcs,s3
```

Cloud URLs work for exact input, pattern input, inspection, and output arguments:

```bash
silk-chiffon transform \
  --from-pattern 'gs://source-bucket/shards/*.arrow' \
  --to s3://result-bucket/combined.parquet

silk-chiffon detect gs://source-bucket/mystery.bin
silk-chiffon inspect parquet s3://result-bucket/combined.parquet
```

Credentials come from the cloud provider's standard discovery chain. GCS uses Application Default Credentials and its supported environment settings. S3 uses its supported AWS environment, web-identity, container, and instance sources. Silk Chiffon does not accept access keys, tokens, service-account JSON, or private keys as command arguments.

GCS adds `--gcs-endpoint`, `--gcs-anonymous`, and `--gcs-request-timeout`. S3 adds `--s3-region`, `--s3-endpoint`, `--s3-addressing-style`, `--s3-anonymous`, and `--s3-request-timeout`. Both providers use the shared `--storage-*` retry settings and `--object-store-*` upload settings in the [command reference](docs/CLI.md). Anonymous mode disables credential discovery and signing. It does not grant write permission.

For an S3-compatible endpoint, path-style addressing appends the bucket to the endpoint. With virtual-hosted addressing, the endpoint must already include the bucket name:

```bash
silk-chiffon detect s3://example-bucket/data.parquet \
  --s3-endpoint https://storage.example.com \
  --s3-addressing-style path \
  --s3-region example-region
```

Only canonical `gs://` and `s3://` URLs are registered. `s3a://` is intentionally unsupported because treating it as a second spelling for the same S3 object would split cache and output-claim identity.

## Recipes

### Merge many files into one

Repeat `--from` for exact references and `--from-pattern` for file globs. The two flags may be combined:

```bash
silk-chiffon transform --from shard-1.arrow --from shard-2.arrow --to combined.parquet

# or select file inputs with a pattern
silk-chiffon transform --from-pattern 'shards/*.arrow' --to combined.parquet
```

Each pattern must match at least one file by default. Add `--allow-unmatched-patterns` when optional shards may be absent; the command still requires another exact or matched input. Exact inputs retain their CLI occurrence order and duplicates. Pattern operands are processed in CLI order after all exact inputs. Within one pattern, matches are sorted by canonical URL and deduplicated before they are collected into homogeneous groups by storage root, format, and container variant. Groups follow the first URL that belongs to each group, and URLs remain sorted within a group, but grouping can move a later URL ahead of an earlier URL from another group. Repeated or overlapping operands intentionally contribute rows again.

This ordering makes input selection deterministic; it does not guarantee output row order. DataFusion may read providers, files, and partitions concurrently and interleave their rows. `--preserve-input-order` is available only for one exact `--from` file written to one output without a query or sort, and it cannot be combined with `--from-pattern`. Use `--sort-by` when the final row order must be defined by data columns.

Files grouped from one pattern must have the same structural schema. Separate exact inputs and pattern groups are combined by column name, so columns missing from one group become null there.

Patterns use case-sensitive Unix glob rules. In an explicit URL path, `?` matches one character, `%3F` names a literal question mark, and `??` starts the query copied to each matched exact URL.

### Partition one input into many files

`--to-many` is a path template, and `--by` names the columns whose values fill it:

```bash
silk-chiffon transform --from events.arrow \
  --to-many 'by-date/{{year}}/{{month}}.parquet' --by year,month
```

The `sort-single` and `nosort-multi` strategies render that template as the complete target and write one file per logical partition. `nosort-evict` can reopen a partition after eviction, so it requires a direct unconditional `{{ file_number }}` interpolation. The value is a zero-based integer and can appear anywhere in the object path; Silk Chiffon never inserts or scans suffixes on its own:

```bash
silk-chiffon transform --from events.arrow \
  --to-many 'by-region/{{region}}_{{file_number}}.parquet' --by region \
  --partition-strategy nosort-evict --max-open-partitions 100
```

Object uploads use a 10 MiB adaptive single-put threshold and multipart part size by default, with at most eight multipart part requests in flight across the command. Tune these with `--object-store-upload-part-size` and `--object-store-max-in-flight-parts`.

### Reshape with SQL

`--query` runs a DataFusion query over the input, which is registered as a table named `data`. Filter it, or re-cast a column's type and keep the rest:

```bash
# keep only the active rows
silk-chiffon transform --from data.arrow --to active.parquet \
  --query "SELECT * FROM data WHERE status = 'active'"

# narrow a timestamp down to a Date32, leaving every other column untouched
silk-chiffon transform --from data.arrow --to compact.parquet \
  --query "SELECT * EXCEPT (created_at), arrow_cast(created_at, 'Date32') AS created_at FROM data"
```

### Tune the Parquet output

Compression, row-group size, statistics, and bloom filters are all yours to set. Bloom filters turn on automatically for the low-cardinality columns that keep dictionary encoding. Override a column by hand when you already know its cardinality:

```bash
silk-chiffon transform --from logs.arrow --to logs.parquet \
  --parquet-compression zstd \
  --parquet-bloom-column "user_id:fpp=0.001,ndv=1000000"
```

### Do it all in one pass

Merge, filter, sort, partition, and encode together:

```bash
silk-chiffon transform \
  --from-pattern 'raw/*.arrow' \
  --to-many 'out/{{region}}/data.parquet' --by region \
  --query "SELECT * FROM data WHERE amount > 0" \
  --sort-by date:desc \
  --parquet-compression zstd \
  --list-outputs text
```

A large sort spills to disk instead of holding everything in the memory budget, so this works on inputs bigger than memory. `--memory-budget` and `--spill-path` control it. The [full reference](docs/CLI.md) has the details.

## Inspecting files

The `inspect` command reads a file's structure without converting it:

```bash
silk-chiffon detect mystery.bin                 # which of the three formats is this?
silk-chiffon inspect parquet data.parquet       # schema, row groups, and statistics
silk-chiffon inspect arrow data.arrow --batches  # schema and record-batch layout
silk-chiffon inspect vortex data.vortex --stats  # schema and column statistics
```

`detect` and each format-specific inspector can emit JSON with `--format json` for piping into other tools.

Inspectors resolve inputs through the same storage registry as transforms, so an exact URL works whenever its storage backend is registered. Parquet inspection reads footer metadata by default. `--pages[=<columns>]` additionally reads the selected row group's page data one column chunk at a time; unknown columns and row groups are errors, and a column chunk larger than 512 MiB is rejected as a safety bound.

## Command reference

`transform`, `detect`, and `inspect` carry more options than these examples show: compression levels, writer versions, dictionary control, partition strategies, and thread and queue tuning among them. The complete reference is in **[docs/CLI.md](docs/CLI.md)**, generated from the code. At the terminal, `silk-chiffon <command> --help` prints the same content.

Shell completions are available for zsh, bash, and fish:

```bash
eval "$(silk-chiffon completions zsh)"                          # this session only
echo 'eval "$(silk-chiffon completions bash)"' >> ~/.bashrc     # persistent
```

## Building from source

silk-chiffon uses [`just`](https://github.com/casey/just) as its task runner. Run `just` on its own to list every task.

```bash
just build      # release build
just test       # run the test suite (via cargo nextest)
just verify     # type-check, format, lint, and regenerate docs/CLI.md
```

`just verify` is the pre-push gate. Because it regenerates `docs/CLI.md`, the CLI reference tracks the code without anyone having to remember to update it.

## License

MIT. See [LICENSE](./LICENSE).
