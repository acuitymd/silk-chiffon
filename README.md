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

## Cloud inputs and storage

Default builds include local paths, Google Cloud Storage, Amazon S3, and BigQuery Storage Read. Source builds can use `--no-default-features` with only the provider features they need: `local-bare-paths`, `gcs`, `s3`, and `bigquery` are independently composable.

```bash
cargo install --git https://github.com/acuitymd/silk-chiffon

# a smaller BigQuery-only build
cargo install --git https://github.com/acuitymd/silk-chiffon \
  --no-default-features --features bigquery
```

Object-storage URLs work for exact input, pattern input, inspection, and output arguments:

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

### BigQuery Storage Read

Use a canonical `bqs` table reference anywhere `transform --from` accepts an exact service input:

```bash
silk-chiffon transform \
  --from 'bqs:///projects/table-project/datasets/analytics/tables/events?location=us' \
  --to events.parquet \
  --query "SELECT event_id, occurred_at FROM data WHERE occurred_at >= TIMESTAMP '2026-08-01'"
```

The only URL query parameters are `snapshot=RFC3339` and `location=REGION`. `snapshot` requests BigQuery time travel at that exact instant. When it is absent, Silk pins the Google server's current time before schema discovery and reuses that exact snapshot for execution. `location` asserts the location returned by BigQuery; it does not choose an endpoint. Duplicate or unknown parameters, credentials, fragments, and noncanonical path forms are rejected.

BigQuery authentication uses Application Default Credentials discovery. This includes `gcloud auth application-default` user credentials, service-account and impersonated-service-account credentials, external-account or workload-identity credentials, and Google metadata-service credentials. `GOOGLE_APPLICATION_CREDENTIALS` can override the discovered credential document. The caller needs `roles/bigquery.readSessionUser` on the project that owns read sessions and `roles/bigquery.dataViewer` on the source table. `--bqs-session-project` changes the session-owning project, which otherwise defaults to the table project, and `--bqs-quota-project` changes the quota project. A distinct quota project may also require `serviceusage.services.use`. No token, key, or credential document is accepted in a URL or command argument.

Provider creation performs a control-plane CreateReadSession call to obtain the authoritative Arrow schema. Planning a scan performs another CreateReadSession call with the projected fields and any exactly translatable DataFusion filters. Discovery calls no ReadRows RPC and has no billed read bytes, but both session calls consume API quota and latency. The source is bounded, but BigQuery's row and byte values are estimates; a DataFusion `LIMIT` is local and is not a billing guarantee.

Each BigQuery stream is one demand-driven DataFusion partition. `--bqs-max-stream-count` overrides the target-partition request, although BigQuery may return fewer streams. Responses are decoded one at a time with DataFusion memory-pool accounting and a command-shared decode limit. `--bqs-max-response-bytes` changes the 256 MiB response safety limit. Native Arrow `lz4` or `zstd` and whole-response `lz4` modes are available but mutually exclusive; `--bqs-picos-timestamp-precision` controls Pico timestamp representation.

ReadRows reconnects only to the same stream at the last batch offset accepted after strict decoding and memory admission. Lost or expired sessions, exhausted retries, schema drift, and invalid responses are terminal; Silk never replaces an execution session after output may have escaped. The defaults are a 60-second active-network idle timeout, a 24-hour stream retry window, 100-millisecond initial backoff, and 60-second maximum backoff. Downstream backpressure does not count as network idleness. See the generated [command reference](docs/CLI.md) for every `--bqs-*` setting.

`--bqs-endpoint` is intended for controlled testing and drives both the REST server-clock probe and the Storage Read gRPC client. Plaintext overrides must be loopback addresses. Production defaults use Google's distinct REST and gRPC endpoints; `--bqs-universe-domain` derives both defaults for another Google Cloud universe and conflicts with an explicit endpoint.

The normal test suite is offline and credential-free. Maintainers can run the ignored read-only live proof with `just test-bigquery-live` only after setting `SILK_CHIFFON_BQS_LIVE_ACKNOWLEDGE_COST=1`, `SILK_CHIFFON_BQS_LIVE_SESSION_PROJECT`, `SILK_CHIFFON_BQS_LIVE_TABLE_PROJECT`, `SILK_CHIFFON_BQS_LIVE_DATASET`, `SILK_CHIFFON_BQS_LIVE_TABLE`, `SILK_CHIFFON_BQS_LIVE_EXPECTED_LOCATION`, and a positive `SILK_CHIFFON_BQS_LIVE_MAX_ESTIMATED_BYTES`; `SILK_CHIFFON_BQS_LIVE_QUOTA_PROJECT` is optional. The fixture must be a pre-existing immutable, non-sensitive, small table. The test creates no dataset, table, job, or cloud object, requests one stream with one projected field and an exact non-null filter, applies a local `LIMIT 100`, refuses an absent, nonpositive, or excessive scanned-byte estimate before ReadRows, writes Arrow and Parquet only in a local temporary directory, and enforces a 120-second bound. The estimate, filter, and local limit are safety guards, not billing guarantees.

### Seeded live cloud soak

The ignored `just test-cloud-live-soak` target repeatedly runs the real composed CLI against the selected input services, formats, and object stores. By default, each case combines BQS rows with a generated local Arrow IPC file, Arrow IPC stream, Parquet file, or Vortex file; writes Arrow IPC file or stream, Parquet, or Vortex output to local storage, GCS, or S3; and varies DataFusion target partitions, requested BQS streams, direct output, `sort-single`, `nosort-multi`, and `nosort-evict`. The independent `SILK_CHIFFON_LIVE_SOAK_INPUT_FORMATS`, `SILK_CHIFFON_LIVE_SOAK_OUTPUT_FORMATS`, `SILK_CHIFFON_LIVE_SOAK_INPUT_OBJECT_STORES`, `SILK_CHIFFON_LIVE_SOAK_OUTPUT_OBJECT_STORES`, and `SILK_CHIFFON_LIVE_SOAK_INPUT_SERVICES` selectors narrow those dimensions without changing the test code. A selected GCS or S3 input fixture is uploaded under the run prefix and is removed by the final cleanup sweep along with selected output objects. The source table has an unused column so every BQS case requests a strict projection, projection order varies, and exact predicates range from a non-null check to prefix and middle ranges across the table's integer partitions. Eviction scenarios apply their 1,000-row range both as a pushed SQL predicate and an explicit BQS restriction, keep BQS rows in one output partition, and use 17 alternating local rows to visit every logical partition; this exercises bounded eviction and numbered objects without creating millions of tiny cloud requests. The other strategies can read all five million rows. It reads each result back through the registered format implementation into a local Arrow IPC stream. A native streaming oracle requires every selected fixture and local ID exactly once, rejects every unexpected or duplicate ID, and checks every row's complete `name` value. The runner does not assume row order and retains only a compact fixture-ID bitset instead of the dataset.

The fixture contract is three nullable columns: `id INT64` contains every integer from 1 through `SILK_CHIFFON_BQS_LIVE_EXPECTED_ROWS` exactly once, `name STRING` contains `row-{id}` for each row, and `payload STRING` exists only to prove projection pushdown excludes an unused field. The example partitions `id` into 100,000-row integer ranges so range predicates exercise BigQuery partition pruning. A temporary five-million-row table can be created with:

```bash
export SILK_CHIFFON_BQS_LIVE_TABLE="silk_chiffon_soak_$(date -u +%Y%m%d_%H%M%S)"
export SILK_CHIFFON_BQS_LIVE_EXPECTED_ROWS=5000000

bq --location="$SILK_CHIFFON_BQS_LIVE_EXPECTED_LOCATION" query --use_legacy_sql=false "
CREATE TABLE \`$SILK_CHIFFON_BQS_LIVE_TABLE_PROJECT.$SILK_CHIFFON_BQS_LIVE_DATASET.$SILK_CHIFFON_BQS_LIVE_TABLE\`
PARTITION BY RANGE_BUCKET(id, GENERATE_ARRAY(0, 5100000, 100000))
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
AS
SELECT major * 1000 + minor + 1 AS id,
       CONCAT('row-', CAST(major * 1000 + minor + 1 AS STRING)) AS name,
       CONCAT('unused-', CAST(major * 1000 + minor + 1 AS STRING)) AS payload
FROM UNNEST(GENERATE_ARRAY(0, 4999)) AS major
CROSS JOIN UNNEST(GENERATE_ARRAY(0, 999)) AS minor
"
```

Set the GCS and S3 bucket and prefix variables only for the selected cloud stores, using dedicated non-root test locations; the prefixes must contain at least two path segments. Set the BQS variables required by `just test-bigquery-live`, plus `SILK_CHIFFON_BQS_LIVE_EXPECTED_ROWS`, only when `bqs` is selected. The root package enables local, GCS, S3, BQS, and every format by default. If the S3 implementation should use an AWS CLI profile, export that profile's temporary environment credentials before starting the process:

The soak defaults to the full matrix, but each dimension can be narrowed without changing the test binary. Set `SILK_CHIFFON_LIVE_SOAK_INPUT_FORMATS` and `SILK_CHIFFON_LIVE_SOAK_OUTPUT_FORMATS` independently to comma-separated subsets of `arrow-file`, `arrow-stream`, `parquet`, and `vortex`. Set `SILK_CHIFFON_LIVE_SOAK_INPUT_OBJECT_STORES` and `SILK_CHIFFON_LIVE_SOAK_OUTPUT_OBJECT_STORES` independently to subsets of `local`, `gcs`, and `s3`; an input fixture selected for GCS or S3 is uploaded before the transform and removed by the final cleanup sweep. Unselected cloud stores do not require credentials or bucket variables. Set `SILK_CHIFFON_LIVE_SOAK_INPUT_SERVICES` to `bqs`, `local`, or both; omitting `bqs` avoids BigQuery credentials and table variables, while omitting `local` tests the service input alone. Each selector must contain at least one value.

```bash
eval "$(aws configure export-credentials --profile default --format env)"
export AWS_DEFAULT_REGION="$(aws configure get region --profile default)"
```

Run `just test-bigquery-live` first so its server-provided scanned-byte estimate is checked against `SILK_CHIFFON_BQS_LIVE_MAX_ESTIMATED_BYTES`. Then choose a duration and optional reproducible seed:

```bash
export SILK_CHIFFON_LIVE_SOAK_DURATION=3h
export SILK_CHIFFON_LIVE_SOAK_SEED=0x5eed
just test-cloud-live-soak
```

The default duration is five minutes. The deterministic prelude includes every selected input/output object-store pair and every selected output-format/partitioned-strategy pair; with the defaults, it contains 36 local, GCS, and S3 cases. Later cases are seeded random combinations that also include selected local and direct output. `SILK_CHIFFON_LIVE_SOAK_MAX_CASES` adds a case limit. Every failure prints its seed, case number, and complete scenario. Set `SILK_CHIFFON_LIVE_SOAK_CASE` with the same seed to replay only that case. Each case uses a unique cloud child prefix, and the harness performs one final sweep of each selected cloud prefix after the case run, including after a transform or verification failure. The temporary BigQuery table is not owned by the test; remove it when the run ends, although the one-day table expiration is a fallback:

```bash
bq rm -f -t "$SILK_CHIFFON_BQS_LIVE_TABLE_PROJECT:$SILK_CHIFFON_BQS_LIVE_DATASET.$SILK_CHIFFON_BQS_LIVE_TABLE"
```

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
