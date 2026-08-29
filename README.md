# silk-chiffon

silk-chiffon converts, queries, sorts, and partitions Arrow, Parquet, and Vortex data on local disk or Google Cloud Storage (GCS).

It reads and writes [Apache Arrow](https://arrow.apache.org/) IPC files and streams, [Apache Parquet](https://parquet.apache.org/), and [Vortex](https://docs.vortex.dev/). The CLI is for engineers who need one-off conversions and repeatable data jobs without loading a whole dataset into memory.

It runs on [DataFusion](https://datafusion.apache.org/), so any reshaping you would express in SQL is a `--query` away. It streams data in batches and applies a best-effort memory budget instead of reading a whole file at once. The budget is not a hard process-memory limit.

## Install

Prebuilt binaries for each release are on the [releases page](https://github.com/acuitymd/silk-chiffon/releases). Or build it yourself with the current stable Rust toolchain:

```bash
# from a local checkout
cargo install --path .

# straight from GitHub
cargo install --git https://github.com/acuitymd/silk-chiffon
```

> [!NOTE]
> A downloaded macOS binary is unsigned, so Gatekeeper offers to move it to the trash. Clear the quarantine flag to keep it: `xattr -d com.apple.quarantine /path/to/silk-chiffon`.

## Quick start

Given an Arrow IPC file named `data.arrow` with an `amount` column, convert it to compressed, sorted Parquet:

```bash
silk-chiffon transform --from data.arrow --to data.parquet \
  --parquet-compression zstd --sort-by amount:desc
```

silk-chiffon reads the format from each file's extension (`.arrow`, `.parquet`, `.vortex`), so one command reads Arrow and writes Parquet without being told which is which. (Pass `--input-format` or `--output-format` when the extension can't say.) Then look at what you wrote:

```bash
silk-chiffon inspect parquet data.parquet
```

## Use Google Cloud Storage

Every input, output, and inspect location accepts a bare local path, a `file://` URL, or a `gs://bucket/key` URI. A single transform can mix local and GCS inputs. GCS is the only remote backend.

```bash
# local to GCS
silk-chiffon transform --from data.arrow --to gs://my-bucket/exports/data.parquet

# GCS to local
silk-chiffon transform --from gs://my-bucket/exports/data.parquet --to data.vortex

# GCS to GCS
silk-chiffon transform \
  --from gs://source-bucket/events/data.parquet \
  --to gs://result-bucket/events/data.arrow
```

Quote globs and templates so the shell passes them unchanged:

```bash
silk-chiffon transform \
  --from-many 'gs://my-bucket/raw/**/*.parquet' \
  --to-many 'gs://my-bucket/by-region/{{region}}/data.parquet' \
  --by region \
  --list-outputs json \
  --list-outputs-file gs://my-bucket/manifests/by-region.json
```

`*` does not cross `/`. Use `**` for recursive matches. A GCS template must use a literal bucket. Template values can change only the object key.

Anonymous access can read public objects. A glob also needs permission to list the matching bucket prefix.

### Authenticate with ADC

silk-chiffon uses [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials). It checks these sources when the first GCS URI is resolved:

1. The credential configuration named by `GOOGLE_APPLICATION_CREDENTIALS`.
2. The well-known local ADC file.
3. The metadata server for an attached service account.

For local development, create the well-known ADC file with the Google Cloud CLI:

```bash
gcloud auth application-default login
silk-chiffon inspect identify gs://my-bucket/data/input.parquet
```

Set `GOOGLE_APPLICATION_CREDENTIALS` for service-account, authorized-user, [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation), external-account executable, or [service-account impersonation](https://cloud.google.com/docs/authentication/use-service-account-impersonation) configuration:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/adc.json
silk-chiffon transform --from data.arrow --to gs://my-bucket/data.parquet
```

The Google Cloud CLI can also write impersonated ADC:

```bash
gcloud auth application-default login \
  --impersonate-service-account=worker@my-project.iam.gserviceaccount.com
```

On Google Cloud, attach a user-managed service account to the workload and let ADC use the metadata server. Google documents this as the preferred setup for production workloads running on Google Cloud. No Silk Chiffon credential flag is needed. See [attached service account setup](https://cloud.google.com/docs/authentication/set-up-adc-attached-service-account).

For an anonymously readable bucket or an emulator that does not sign requests, set `GOOGLE_SKIP_SIGNATURE=true`. `GOOGLE_BASE_URL` changes the GCS API endpoint. Both settings come from the [object_store GCS configuration](https://docs.rs/object_store/0.13.2/object_store/gcp/enum.GoogleConfigKey.html). Emulator behavior varies for XML multipart uploads and conditional copy, so run the real-GCS tests before relying on those paths.

```bash
GOOGLE_SKIP_SIGNATURE=true \
GOOGLE_BASE_URL=http://127.0.0.1:4443 \
silk-chiffon inspect identify gs://public-or-emulated-bucket/data.parquet
```

### Tune remote I/O

Three global options bound remote work:

- `--object-store-max-requests 64` caps concurrent requests to each GCS bucket.
- `--object-store-upload-part-size 10MiB` sets the multipart part size.
- `--object-store-upload-concurrency 8` caps concurrent part uploads for each output.

The values shown are the defaults. They may appear before or after a subcommand. DataFusion partitions Arrow IPC files and Parquet scans. Vortex owns its scan concurrency. In deterministic tests with Vortex 0.78, multiple Vortex input files read concurrently, while one 12,951,180-byte fixture issued two serial ranges. Silk Chiffon does not split every download into fixed-size chunks. Arrow IPC streams remain sequential because their messages must be decoded in order.

### Output and cleanup rules

An output manifest is the text or JSON list produced by `--list-outputs`. Use `--list-outputs-file` to commit that list to a local path or GCS object.

- GCS prefixes are object-name prefixes, so `--create-dirs` creates nothing in GCS. Local parent directories are created by default. `--spill-path` accepts only a local directory.
- Outputs use create-only commit unless `--overwrite` is present. Create-only commit rejects a destination written by another process after the preflight check. `--overwrite` applies to data objects and `--list-outputs-file` manifests.
- Each output is uploaded as `__silk_chiffon_tmp__/<destination-key-prefix>-<uuid>` and copied to its final key after its writer finishes. When an operation returns an error, Silk Chiffon waits for multipart and temporary-object cleanup. The returned error includes any cleanup failure. In library use, canceling an output future while the async runtime remains alive schedules the same cleanup.
- The manifest destination is checked before data processing. Silk Chiffon commits the manifest once after all data objects commit. Another writer can win the manifest's create-only commit after the data succeeds. Partitioned data and its manifest are separate commits. An empty text listing creates an empty manifest object.

Ctrl-C, SIGTERM, a process crash, or host termination can stop the runtime before cleanup finishes. This can leave an incomplete multipart upload or temporary object. Configure [GCS Object Lifecycle Management](https://cloud.google.com/storage/docs/lifecycle) to delete stale objects under `__silk_chiffon_tmp__/` and abort old incomplete multipart uploads.

## Recipes

### Merge many files into one

`--from-many` takes repeated paths or a glob, as long as the inputs share a schema:

```bash
silk-chiffon transform --from-many 'shards/*.arrow' --to combined.parquet
```

### Partition one input into many files

`--to-many` is a path template, and `--by` names the columns whose values fill it:

```bash
silk-chiffon transform --from events.arrow \
  --to-many 'by-date/{{year}}/{{month}}.parquet' --by year,month
```

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

You can set compression, row-group size, statistics, and bloom filters. Bloom filters turn on automatically for the low-cardinality columns that keep dictionary encoding. The `fpp` value configures the false-positive probability (FPP), and `ndv` supplies the number of distinct values (NDV) expected for a column:

```bash
silk-chiffon transform --from logs.arrow --to logs.parquet \
  --parquet-compression zstd \
  --parquet-bloom-column "user_id:fpp=0.001,ndv=1000000"
```

### Do it all in one pass

Merge, filter, sort, partition, and encode together:

```bash
silk-chiffon transform \
  --from-many 'raw/*.arrow' \
  --to-many 'out/{{region}}/data.parquet' --by region \
  --query "SELECT * FROM data WHERE amount > 0" \
  --sort-by date:desc \
  --parquet-compression zstd \
  --list-outputs text
```

Sorts can spill intermediate data to disk when they exceed the memory budget. `--memory-budget` and `--spill-path` control that behavior. The [full reference](docs/CLI.md) has the details.

## Inspecting files

The `inspect` command reads a file's structure without converting it:

```bash
silk-chiffon inspect identify mystery.bin       # which of the three formats is this?
silk-chiffon inspect parquet data.parquet       # schema, row groups, and statistics
silk-chiffon inspect arrow data.arrow --batches  # schema and record-batch layout
silk-chiffon inspect vortex data.vortex --stats  # schema and column statistics
```

Each inspector can emit JSON with `--format json` for piping into other tools.

Inspect works directly against GCS without staging a local copy:

```bash
silk-chiffon inspect identify gs://my-bucket/data/unknown.bin
silk-chiffon inspect arrow gs://my-bucket/data/events.arrow --batches --row-count
silk-chiffon inspect parquet gs://my-bucket/data/events.parquet --pages=event.id
silk-chiffon inspect vortex gs://my-bucket/data/events.vortex --schema --stats --layout
```

Parquet text output reads footer metadata unless `--pages` is present. JSON output retains page-encoding details and can fetch most column chunks. Page reads fetch exact column-chunk ranges with up to 16 inspector requests, still bounded by `--object-store-max-requests`. Vortex inspection uses its object-store range reader.

## Command reference

`transform` and `inspect` include compression levels, writer versions, dictionary control, partition strategies, and thread and queue tuning. The complete reference is in [docs/CLI.md](docs/CLI.md). It is generated from the code, and `silk-chiffon <command> --help` prints the same content.

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

### Run the live GCS scenarios

Normal tests are network-independent. The ignored GCS suite uses ADC and creates one UUID-owned prefix. It cleans only that prefix and its matching reserved temporary namespace on success, error, or panic.

```bash
export SILK_CHIFFON_GCS_TEST_BUCKET=my-test-bucket
# Optional base up to 256 UTF-8 bytes. A UUID is always appended.
export SILK_CHIFFON_GCS_TEST_PREFIX=silk-chiffon-tests
cargo test --test gcs_integration -- --ignored --nocapture --test-threads=1
```

The suite covers exact paths, globs, every local and GCS transfer direction, and all three formats. It also covers Arrow file and stream output, multipart uploads, partitioned output, remote manifests, overwrite policy, inspect modes, and failure cleanup. Without `SILK_CHIFFON_GCS_TEST_BUCKET`, it prints a skip message and makes no network request.

### Run the GCS performance contract

The performance harness runs chargeable GCS work and requires a release binary plus an explicit opt-in. It creates one UUID-owned prefix and applies the same bounded cleanup as the live scenario suite.

```bash
cargo build --release

export SILK_CHIFFON_GCS_TEST_BUCKET=my-test-bucket
export SILK_CHIFFON_GCS_PERF=1

cargo test --test gcs_integration gcs_object_store_performance -- \
  --ignored --exact --nocapture --test-threads=1
```

The default binary is `target/release/silk-chiffon`. Set `SILK_CHIFFON_GCS_PERF_BINARY` to another release binary. The optional numeric settings are:

- `SILK_CHIFFON_GCS_PERF_ROWS`, default 250000 rows.
- `SILK_CHIFFON_GCS_PERF_TARGET_PARTITIONS`, default 8 DataFusion partitions.
- `SILK_CHIFFON_GCS_PERF_MAX_REQUESTS`, default 64 requests per GCS store.
- `SILK_CHIFFON_GCS_PERF_UPLOAD_PART_SIZE`, default 10485760 bytes.
- `SILK_CHIFFON_GCS_PERF_UPLOAD_CONCURRENCY`, default 8 requests per output.
- `SILK_CHIFFON_GCS_PERF_PARTITIONS`, default 64 open partition outputs.

The harness covers Parquet projection and filtering, Arrow file and stream reads, Vortex projection, large uploads for every output variant, four parallel inputs, 64 open partition sinks, and four Parquet inspection modes. Each JSONL record contains input and output sizes, rows, selected columns, execution and output partition counts, storage settings, elapsed time, and peak resident memory. The external CLI does not expose provider request counts, request concurrency, or transferred bytes, so those fields are `null` with a reason instead of estimates. Normal verification compiles this contract and exercises its no-configuration skip. It does not run the chargeable workloads.

## License

MIT. See [LICENSE](./LICENSE).
