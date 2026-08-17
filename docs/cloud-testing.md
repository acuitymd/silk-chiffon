# Cloud testing

The normal test suite is offline and credential-free. Live tests are ignored, require explicit acknowledgement variables, and must use dedicated non-production resources. They create unique child prefixes and local temporary files, but credentials and bucket/table selection remain the operator's responsibility.

## Authentication

GCS and BigQuery use Google Application Default Credentials. For a workstation login:

```bash
gcloud auth application-default login
gcloud auth application-default print-access-token >/dev/null
```

S3 uses the AWS SDK credential chain through `object_store`. To use an AWS CLI profile without writing credentials into Silk Chiffon arguments:

```bash
eval "$(aws configure export-credentials --profile default --format env)"
export AWS_DEFAULT_REGION="$(aws configure get region --profile default)"
aws sts get-caller-identity
```

The live tests never need secret values in repository files or command arguments.

## Focused storage tests

The storage and composed-CLI tests exercise exact reads, range reads, patterns, outputs, multipart abort, output claims, inspection, transform, data verification, and cleanup. Configure only the provider you intend to run, then use `just test-gcs-live` or `just test-s3-live`.

| Provider | Bucket variable                | Prefix variable                |
| -------- | ------------------------------ | ------------------------------ |
| GCS      | `SILK_CHIFFON_LIVE_GCS_BUCKET` | `SILK_CHIFFON_LIVE_GCS_PREFIX` |
| S3       | `SILK_CHIFFON_LIVE_S3_BUCKET`  | `SILK_CHIFFON_LIVE_S3_PREFIX`  |

The bucket must be one canonical host. The prefix must be a dedicated non-root path containing at least two nonempty segments and only ASCII letters, digits, `-`, `_`, and `/`.

These tests delete only objects below the unique run prefix they created. They do not create or delete buckets. If the process is killed before cleanup, locate the printed run prefix and inspect it before deleting it manually.

## Focused BigQuery test

Run `just test-bigquery-live` only after setting:

- `SILK_CHIFFON_BQS_LIVE_ACKNOWLEDGE_COST=1`
- `SILK_CHIFFON_BQS_LIVE_SESSION_PROJECT`
- `SILK_CHIFFON_BQS_LIVE_TABLE_PROJECT`
- `SILK_CHIFFON_BQS_LIVE_DATASET`
- `SILK_CHIFFON_BQS_LIVE_TABLE`
- `SILK_CHIFFON_BQS_LIVE_EXPECTED_LOCATION`
- a positive `SILK_CHIFFON_BQS_LIVE_MAX_ESTIMATED_BYTES`

`SILK_CHIFFON_BQS_LIVE_QUOTA_PROJECT` is optional. The fixture must be an existing immutable, non-sensitive table. The test creates no dataset, table, query job, or cloud object. It requests one stream and one projected field with an exact non-null filter, applies a local `LIMIT 100`, rejects an absent, nonpositive, or excessive server-provided scan estimate before ReadRows, writes Arrow and Parquet only in a local temporary directory, and enforces a 120-second bound. The estimate, filter, and local limit reduce risk but are not billing guarantees.

## Seeded cross-provider soak

`just test-cloud-live-soak` repeatedly invokes the real composed CLI. Its independent selectors are comma-separated lists:

| Variable                                      | Values                                            |
| --------------------------------------------- | ------------------------------------------------- |
| `SILK_CHIFFON_LIVE_SOAK_INPUT_FORMATS`        | `arrow-file`, `arrow-stream`, `parquet`, `vortex` |
| `SILK_CHIFFON_LIVE_SOAK_OUTPUT_FORMATS`       | `arrow-file`, `arrow-stream`, `parquet`, `vortex` |
| `SILK_CHIFFON_LIVE_SOAK_INPUT_OBJECT_STORES`  | `local`, `gcs`, `s3`                              |
| `SILK_CHIFFON_LIVE_SOAK_OUTPUT_OBJECT_STORES` | `local`, `gcs`, `s3`                              |
| `SILK_CHIFFON_LIVE_SOAK_INPUT_SERVICES`       | `local`, `bqs`                                    |

Omitting a selector enables every value in that dimension. Unselected cloud providers require no credentials or bucket variables. Selecting `local` as an input service adds generated files in the selected input formats; selecting `bqs` adds the configured table. GCS and S3 input fixtures are uploaded under the run prefix and removed during the final cleanup sweep.

The matrix varies target partitions, requested BQS streams, direct output, `sort-single`, `nosort-multi`, and `nosort-evict`. Every BQS case projects away an unused field and applies an exactly translatable predicate. Eviction scenarios use a 1,000-row range and alternating local rows so numbered output and open-partition eviction are exercised without millions of tiny cloud requests. Other strategies may read the full fixture.

Each result is read back through its registered format into a local Arrow IPC stream. The oracle requires every selected fixture and local ID exactly once, rejects unexpected and duplicate IDs, checks every complete `name`, and does not assume row order. Verification retains a compact bitset rather than all rows.

## BigQuery soak fixture

The BQS fixture has three nullable columns: `id INT64` contains each integer from 1 through `SILK_CHIFFON_BQS_LIVE_EXPECTED_ROWS` exactly once, `name STRING` is `row-{id}`, and `payload STRING` exists only to prove projection pushdown. This command creates a five-million-row table partitioned into 100,000-row ranges and gives it a one-day expiration:

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

Run the focused BQS test first so its scan estimate is checked. Then choose a duration and optional reproducible seed:

```bash
export SILK_CHIFFON_LIVE_SOAK_DURATION=3h
export SILK_CHIFFON_LIVE_SOAK_SEED=0x5eed
just test-cloud-live-soak
```

The default duration is five minutes. `SILK_CHIFFON_LIVE_SOAK_MAX_CASES` caps the case count. Every failure prints the seed, case number, and scenario. Set `SILK_CHIFFON_LIVE_SOAK_CASE` with the same seed to replay only that case.

Each case gets a unique cloud child prefix. The harness performs one final sweep of each selected run prefix after all cases, including after transform or verification failure. It does not own the BigQuery fixture table. Remove that table when the run ends; expiration is only a fallback:

```bash
bq rm -f -t "$SILK_CHIFFON_BQS_LIVE_TABLE_PROJECT:$SILK_CHIFFON_BQS_LIVE_DATASET.$SILK_CHIFFON_BQS_LIVE_TABLE"
```

If the process is forcibly killed, inspect the printed cloud prefixes and table name. Clean only those exact resources.
