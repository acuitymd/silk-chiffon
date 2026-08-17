<!-- Generated from the CLI by `just docs`; edit the clap definitions, not this file. -->

# Command-Line Help for `silk-chiffon`

This document contains the help content for the `silk-chiffon` command-line program.

**Command Overview:**

- [`silk-chiffon`↴](#silk-chiffon)
- [`silk-chiffon transform`↴](#silk-chiffon-transform)
- [`silk-chiffon inspect`↴](#silk-chiffon-inspect)
- [`silk-chiffon inspect identify`↴](#silk-chiffon-inspect-identify)
- [`silk-chiffon inspect parquet`↴](#silk-chiffon-inspect-parquet)
- [`silk-chiffon inspect arrow`↴](#silk-chiffon-inspect-arrow)
- [`silk-chiffon inspect vortex`↴](#silk-chiffon-inspect-vortex)
- [`silk-chiffon completions`↴](#silk-chiffon-completions)

## `silk-chiffon`

Silky smooth conversion between columnar data formats 💝

**Usage:** `silk-chiffon <COMMAND>`

###### **Subcommands:**

- `transform` — Transform data between formats with optional filtering, sorting, merging, and partitioning.
- `inspect` — Inspect file metadata and structure.
- `completions` — Generate shell completions for your shell.

## `silk-chiffon transform`

Transform data between formats with optional filtering, sorting, merging, and partitioning.

Examples:

    # Simple conversion
    silk-chiffon transform --from input.arrow --to output.parquet

    # Merge multiple files
    silk-chiffon transform --from-many file1.arrow --from-many file2.arrow --to merged.parquet

    # Partition into multiple files
    silk-chiffon transform --from input.arrow --to-many "{{region}}.parquet" --by region

    # Merge and partition with glob
    silk-chiffon transform --from-many '*.arrow' --to-many "{{year}}/{{month}}.parquet" --by year,month

**Usage:** `silk-chiffon transform [OPTIONS]`

###### **Options:**

- `--from <FROM>` — Single input file path
- `--from-many <FROM_MANY>` — Multiple input file paths (supports glob patterns). Can be specified multiple times
- `--input-format <INPUT_FORMAT>` — Override input format detection

  Possible values: `arrow`, `parquet`, `vortex`

- `--output-format <OUTPUT_FORMAT>` — Override output format detection

  Possible values: `arrow`, `parquet`, `vortex`

- `--to <TO>` — Single output file path
- `--to-many <TO_MANY>` — Output path template for partitioning (e.g., "{{region}}.parquet"). Requires --by
- `-d`, `--dialect <DIALECT>` — The query dialect to use

  Default value: `duckdb`

  Possible values: `duckdb`, `generic`, `mysql`, `postgresql`, `hive`, `sqlite`, `snowflake`, `redshift`, `mssql`, `clickhouse`, `bigquery`, `ansi`, `databricks`

- `-e`, `--exclude-columns <EXCLUDE_COLUMNS>` — Names of columns to exclude from the output
- `-q`, `--query <QUERY>` — SQL query to apply to the data. The input data is available as table 'data'.

  Examples:

      --query "SELECT * FROM data WHERE status = 'active'"
      --query "SELECT id, name, amount FROM data"
      --query "SELECT region, SUM(amount) FROM data GROUP BY region"
      --query "SELECT *, amount * 1.1 as adjusted FROM data"
- `-s`, `--sort-by <SORT_BY>` — Sort the data by one or more columns before writing.

  Format: A comma-separated list like `col_a,col_b:desc,col_c`.
- `--memory-budget <MEMORY_BUDGET>` — Target memory budget. Best-effort, not a hard limit.

  Accepts a byte size (e.g. "8GB"), "total[:pct]" for a percentage of total RAM, "available[:pct]" for a percentage of free RAM, or "reserve:<size>" to use total RAM minus a reserved amount. All keyword modes (total, available, reserve) support an optional minimum: "total:80:min:4GB". Examples: "total:90", "available:60%", "reserve:2GB:min:1GB", "4GB".

  Setting this too low may cause out-of-memory errors, since some internal buffers cannot spill to disk. The minimum depends on schema width, batch size, and parallelism — there is no fixed floor.

  Default: 80% of total memory, container-aware on Linux.

  Default value: `total:80%`
- `--non-spillable-reserve <NON_SPILLABLE_RESERVE>` — Reserve memory for non-spillable operations (sort merge phases, etc.).

  Enables an alternative memory pool that prevents spillable operators (sorts, aggregations) from consuming the entire pool, guaranteeing headroom for non-spillable consumers that would otherwise fail.

  Accepts a percentage (e.g. "10%", "10") or a fixed byte size (e.g. "200MB"). Percentages are resolved against the memory pool size.

  When not set, the default DataFusion FairSpillPool is used instead.
- `--memory-pool-top-consumers <MEMORY_POOL_TOP_CONSUMERS>` — Number of top memory consumers to report in out-of-memory error messages.

  When a memory allocation fails, the error message includes the N largest memory consumers to help diagnose what is using the pool. Set to 0 to report all consumers.

  Default: 10.

  Default value: `10`
- `-t`, `--thread-budget <THREAD_BUDGET>` — Target thread budget for parallel work. Best-effort, not a hard limit.

  Accepts a number (e.g. "8"), "reserve:N" to use all CPUs minus N (minimum 1), or "reserve:N:min:M" for a custom minimum. Examples: "8", "reserve:2", "reserve:2:min:4".

  Split between encoding and query execution based on workload. Thread pools intentionally overcommit since not all threads are active simultaneously.

  Default: all CPU cores.
- `--target-partitions <TARGET_PARTITIONS>` — Number of partitions for query execution parallelism.

  Controls how DataFusion partitions data during queries (aggregations, joins, sorts). Higher values increase parallelism but use more memory.

  Default: auto-detected based on workload. With sorting (--sort-by or --by): 75% of available CPU cores. Without sorting: DataFusion default.
- `--spill-path <SPILL_PATH>` — Directory for spilling intermediate data when memory limit is exceeded.

  When DataFusion operators (sort, group by, aggregation) exceed the memory limit, they spill to this path. Default: system temp directory.
- `--spill-compression <SPILL_COMPRESSION>` — Compression for spilled intermediate data.

  Controls compression when DataFusion spills to disk. Lz4 is faster but produces larger files; zstd achieves better compression but is slower.

  Default value: `lz4`

  Possible values:
  - `none`:
    No compression (fastest, largest files)
  - `lz4`:
    LZ4 frame compression (fast, good compression)
  - `zstd`:
    Zstd compression (slower, best compression)

- `--preserve-input-order` — Preserve the row order from the input file in the output.

  By default, DataFusion reads files using multiple partitions for parallelism, which can interleave rows. This flag forces single-partition reading to maintain the original row order. Only valid for single-file-to-single-file transforms without queries or sorting.

  Default value: `false`
- `-b`, `--by <BY>` — Column(s) to partition by (comma-separated for multi-column partitioning). Partition output by column values. Only primitive types (integers, floats, strings, dates, etc.) are supported. Complex types (arrays, structs, maps) will error
- `--partition-strategy <PARTITION_STRATEGY>` — Partitioning strategy for writing output files

  Default value: `sort-single`

  Possible values:
  - `sort-single`:
    Sort by partition columns first, then write one file at a time. Uses minimal file handles but requires sorting the entire dataset. Best for high-cardinality partition columns, or when partition columns are highly fragmented
  - `nosort-multi`:
    Keep a file handle open per partition, write rows directly. No sorting required, preserves input order within each partition. Best for low-cardinality partition columns with low fragmentation
  - `nosort-evict`:
    Like nosort-multi but caps the number of simultaneously open partition writers. When the cap is hit, the least-recently-written partition is finalized. If that partition reappears, a new numbered file is created. Best for high-cardinality partitions where sorting is too expensive. Per-writer concurrency is minimized (sequential encoding) since parallelism comes from having many partition writers active simultaneously

- `--max-open-partitions <MAX_OPEN_PARTITIONS>` — Maximum number of partition file handles to keep open simultaneously. When this limit is reached, the least-recently-written partition is finalized. Only used with --partition-strategy=nosort-evict. Defaults to 100
- `-l`, `--list-outputs <LIST_OUTPUTS>` — List the output files after creation (only with --to-many)

  Possible values: `none`, `text`, `json`

- `--list-outputs-file <LIST_OUTPUTS_FILE>` — Write output file listing to a file instead of stdout
- `--create-dirs` — Create directories as needed

  Default value: `true`
- `--overwrite` — Overwrite existing files
- `--arrow-compression <ARROW_COMPRESSION>` — Arrow IPC compression codec

  Default value: `none`

  Possible values: `zstd`, `lz4`, `none`

- `--arrow-format <ARROW_FORMAT>` — Arrow IPC format (file or stream)

  Default value: `file`

  Possible values: `file`, `stream`

- `--arrow-record-batch-size <ARROW_RECORD_BATCH_SIZE>` — Arrow record batch size

  Default value: `122880`
- `--arrow-writing-queue-size <ARROW_WRITING_QUEUE_SIZE>` — Arrow writer queue size (number of batches buffered before backpressure)

  Default value: `16`
- `--parquet-bloom-all <[fpp=VALUE][,ndv=VALUE]>` — Enable bloom filters for columns (default behavior).

  DICTIONARY/BLOOM INTERACTION:
  Bloom filters are coupled to dictionary encoding decisions:
  - Columns that KEEP dictionary encoding -> bloom filter enabled (using analyzed NDV)
  - Columns where dictionary is DISABLED (high cardinality) -> bloom filter disabled
  - This coupling exists because both features degrade for high-cardinality data

  To force bloom filters ON regardless of dictionary decisions, specify explicit NDV:

      --parquet-bloom-column "high_card_col:ndv=100000"

  NESTED TYPES (structs, lists, maps):
  Bloom filters apply to leaf columns within nested structures. The column path uses
  dot notation (e.g., "struct_col.field" or "list_col.element").

  Formats:

      --parquet-bloom-all                       # Use defaults (fpp=0.01, auto NDV)
      --parquet-bloom-all "fpp=VALUE"           # Custom false positive probability
      --parquet-bloom-all "ndv=VALUE"           # Force bloom on with explicit NDV
      --parquet-bloom-all "fpp=VALUE,ndv=VALUE" # Both custom

  CONFLICTS: Cannot be used with --parquet-bloom-all-off.

  Examples:

      --parquet-bloom-all                                     # Bloom for low-cardinality columns
      --parquet-bloom-all "fpp=0.001"                         # Tighter false positive rate
      --parquet-bloom-all "ndv=10000"                         # Force bloom on ALL columns
      --parquet-bloom-all --parquet-bloom-column-off user_id  # Exclude user_id
- `--parquet-bloom-all-off` — Disable bloom filters for all columns.

  Use with --parquet-bloom-column to enable bloom filters for specific columns only.

  CONFLICTS: Cannot be used with --parquet-bloom-all.

  Examples:

      --parquet-bloom-all-off                                 # No bloom filters
      --parquet-bloom-all-off --parquet-bloom-column user_id  # Only user_id
- `--parquet-bloom-column <COLUMN[:fpp=VALUE][,ndv=VALUE]>` — Enable or customize bloom filters for specific columns.

  Overrides --parquet-bloom-all-off for the specified columns.

  WITHOUT explicit NDV: bloom filter depends on dictionary decision (see --parquet-bloom-all).
  WITH explicit NDV: bloom filter is FORCED ON regardless of dictionary encoding.

  Use explicit NDV to enable bloom filters on high-cardinality columns that won't
  use dictionary encoding (e.g., UUIDs, timestamps).

  NESTED TYPES: Use dot notation for nested paths (e.g., "user.address"). This matches
  all leaf columns under that path, so you don't need to know Parquet internal naming.

  Formats:

      COLUMN                     # Depends on dictionary decision
      COLUMN:fpp=VALUE           # Custom false positive probability
      COLUMN:ndv=VALUE           # Force bloom ON with explicit NDV
      COLUMN:fpp=VALUE,ndv=VALUE # Both custom

  CONFLICTS: Cannot specify same column in both --parquet-bloom-column and
  --parquet-bloom-column-off.

  Examples:

      --parquet-bloom-column "region"                    # If region keeps dictionary
      --parquet-bloom-column "user.address"              # All leaves under user.address
      --parquet-bloom-column "user_id:ndv=1000000"       # Force bloom on high-card column
      --parquet-bloom-column "user_id:fpp=0.001"         # Tighter FPP
- `--parquet-bloom-column-off <COLUMN>` — Disable bloom filter for specific columns (repeatable).

  Overrides --parquet-bloom-all for the specified columns.

  NESTED TYPES: Use dot notation for nested paths (e.g., "user.address"). This disables
  bloom filters for all leaf columns under that path.

  CONFLICTS: Cannot specify same column in both --parquet-bloom-column and
  --parquet-bloom-column-off.

  Examples:

      --parquet-bloom-all --parquet-bloom-column-off user_id  # All except user_id
      --parquet-bloom-column-off "user.address"               # Disable for all user.address leaves
      --parquet-bloom-column-off col1 --parquet-bloom-column-off col2  # Disable multiple
- `--parquet-buffer-size <PARQUET_BUFFER_SIZE>` — I/O buffer size for Parquet writing (e.g., "32MB", "64MB", "1GB").

  Controls the size of the buffer used when writing encoded data to disk. Supports suffixes: B, KB, MB, GB, TB (or KiB, MiB, GiB, TiB for binary). Default: 32MB.
- `--parquet-dictionary-all-off` — Disable dictionary encoding globally for all Parquet columns.

  Dictionary encoding builds a dictionary of unique values and stores references to it,
  which is effective for low-cardinality columns (few unique values). When disabled,
  columns use their data page encoding directly (see --parquet-encoding).

  DEFAULT BEHAVIOR (without this flag):
  - Most primitive columns use "analyze" mode: cardinality analysis decides per-row-group
    whether to use dictionary (disabled if >20% distinct values)
  - Floats use "always" mode: high cardinality makes analysis unhelpful
  - Nested columns (structs, lists, maps) use "always" mode: dictionary encoding is
    always attempted (parquet-rs handles overflow gracefully)

  BLOOM FILTER IMPACT: Disabling dictionary also disables bloom filters for affected
  columns (unless explicit NDV is provided via --parquet-bloom-column).

  Use --parquet-dictionary-column to re-enable for specific columns.
- `--parquet-dictionary-column <COLUMN:MODE>` — Enable dictionary encoding for specific columns. Can be specified multiple times.

  Overrides --parquet-dictionary-all-off for the named columns.

  Format: COLUMN:MODE where MODE is:
  - analyze: Cardinality analysis decides per-row-group (disabled if >20% distinct)
  - always: Always attempt dictionary; parquet-rs handles overflow gracefully

  NON-ANALYZABLE TYPES:
  Cardinality analysis only works on certain types. Non-analyzable types (nested types
  like structs/lists/maps, and floats due to high cardinality) automatically use "always"
  mode even if you specify "analyze". Use dot notation for nested paths (e.g., "user.address").
  This enables dictionary for all leaf columns under that path.

  BLOOM FILTER INTERACTION:
  The cardinality analysis from "analyze" mode also provides NDV for bloom filter sizing.
  When dictionary is disabled (high cardinality), bloom filters are also disabled unless
  you provide explicit NDV via --parquet-bloom-column.

  CONFLICTS: Cannot specify same column in both --parquet-dictionary-column and
  --parquet-dictionary-column-off.

  Examples:

      --parquet-dictionary-column region:analyze       # Let analysis decide
      --parquet-dictionary-column region:always        # Force dictionary on
      --parquet-dictionary-column "user.address:always"   # All user.address leaves
- `--parquet-column-encoding <COLUMN=ENCODING>` — Set data page encoding for specific columns. Can be specified multiple times.

  Overrides --parquet-encoding and automatic encoding selection for the named column.

  DICTIONARY INTERACTION:
  - Dictionary ENABLED: this encoding is used when dictionary overflows
  - Dictionary DISABLED: this encoding is used for all data

  NESTED TYPES: Use dot notation for nested paths (e.g., "user.address"). This sets
  encoding for all leaf columns under that path.

  Format: COLUMN=ENCODING

  Options: plain, rle, delta-binary-packed, delta-length-byte-array, delta-byte-array,
  byte-stream-split

  Examples:

      --parquet-column-encoding id=delta-binary-packed      # Efficient for sorted integers
      --parquet-column-encoding name=delta-byte-array       # Efficient for strings
      --parquet-column-encoding price=byte-stream-split     # Efficient for floats
- `--parquet-column-encoding-threads <PARQUET_COLUMN_ENCODING_THREADS>` — Number of threads for CPU-bound parquet column encoding.

  Controls the rayon thread pool size for encoding columns within row groups. Column encoding is CPU-intensive and benefits from parallelism.

  Default: auto-detected based on workload. With sorting (--sort-by or --by): 25% of available CPU cores. Without sorting: 75% of available CPU cores.
- `--parquet-ingestion-queue-size <PARQUET_INGESTION_QUEUE_SIZE>` — Queue size for record batches waiting to be assembled into row groups.

  Controls backpressure between the data source and ingestion stage. Higher values allow the source to stay ahead of row group assembly.

  Default value: `1`
- `--parquet-encoding-queue-size <PARQUET_ENCODING_QUEUE_SIZE>` — Queue size for row groups waiting to be encoded.

  Controls backpressure between ingestion and encoding stages. Higher values allow more row groups to be assembled while encoders are busy.

  Default value: `4`
- `--parquet-writing-queue-size <PARQUET_WRITING_QUEUE_SIZE>` — Queue size for encoded row groups waiting to be written to disk.

  Controls backpressure between encoding and I/O stages. Higher values allow more encoding to proceed while I/O is in progress.

  Default value: `4`
- `--parquet-dictionary-column-off <COLUMN>` — Disable dictionary encoding for specific columns. Can be specified multiple times.

  Overrides the default (dictionary enabled with analysis) for the named columns.

  BLOOM FILTER IMPACT: Disabling dictionary also disables bloom filters for the column
  (unless you provide explicit NDV via --parquet-bloom-column).

  NESTED TYPES: Use dot notation for nested paths (e.g., "user.address"). This disables
  dictionary for all leaf columns under that path.

  CONFLICTS: Cannot specify same column in both --parquet-dictionary-column and
  --parquet-dictionary-column-off.

  Useful for high-cardinality columns like UUIDs or timestamps where dictionary
  encoding adds overhead without compression benefit.
- `--parquet-compression <PARQUET_COMPRESSION>` — Parquet compression codec

  Default value: `none`

  Possible values: `zstd`, `snappy`, `gzip`, `brotli`, `lz4`, `none`

- `--parquet-compression-level <PARQUET_COMPRESSION_LEVEL>` — Compression level for the chosen codec.

  Only applies to gzip (0-9, default 6), brotli (0-11, default 1), and zstd (1-22, default 1). Not supported for snappy, lz4, or none.
- `--parquet-encoding <PARQUET_ENCODING>` — Data page encoding for Parquet columns.

  This encoding is used for column data pages. Its role depends on dictionary encoding:
  - Dictionary ENABLED: this is the fallback encoding when dictionary overflows
  - Dictionary DISABLED: this is the primary encoding for all data

  AUTOMATIC ENCODING (when not specified):
  The writer automatically selects encodings based on writer version:

  V1 (--parquet-writer-version=v1):
  All columns use PLAIN encoding for maximum compatibility with older readers.

  V2 (default):
  Optimized encodings are selected based on column type:
  - Integers -> delta-binary-packed (good for sorted/sequential data)
  - Floats -> byte-stream-split (better compression for floats)
  - Strings/Binary -> delta-length-byte-array
  - Booleans -> plain

  Use this flag to override automatic selection globally, or --parquet-column-encoding
  for specific columns.

  Options: plain, rle, delta-binary-packed, delta-length-byte-array, delta-byte-array,
  byte-stream-split

  Possible values: `plain`, `rle`, `delta-binary-packed`, `delta-length-byte-array`, `delta-byte-array`, `byte-stream-split`

- `--parquet-io-threads <PARQUET_IO_THREADS>` — Number of threads for blocking parquet I/O operations.

  Controls the rayon thread pool size for file writes during parquet output. Typically needs fewer threads than encoding since I/O is less CPU-intensive. Defaults to 1.
- `--parquet-row-group-concurrency <PARQUET_ROW_GROUP_CONCURRENCY>` — Maximum number of row groups that can be encoding concurrently.

  Controls how many row groups can be actively encoding at once. Higher values increase parallelism but use more memory. Each row group encodes its columns in parallel using --parquet-column-encoding-threads. Defaults to 4.
- `--parquet-row-group-size <PARQUET_ROW_GROUP_SIZE>` — Maximum number of rows per Parquet row group
- `--parquet-sorted-metadata` — Embed metadata indicating that the file's data is sorted.

  Requires --sort-by to be set.

  Default value: `false`
- `--parquet-statistics <PARQUET_STATISTICS>` — Parquet column statistics level

  Default value: `chunk`

  Possible values: `none`, `chunk`, `page`

- `--parquet-writer-version <PARQUET_WRITER_VERSION>` — Parquet writer version

  Default value: `v2`

  Possible values: `v1`, `v2`

- `--parquet-data-page-size <PARQUET_DATA_PAGE_SIZE>` — Maximum data page size in bytes.

  Controls the maximum size of each data page within a column chunk. Larger pages reduce overhead but increase granularity of reads. Default: 100MB (DuckDB MAX_UNCOMPRESSED_PAGE_SIZE).
- `--parquet-data-page-row-limit <PARQUET_DATA_PAGE_ROW_LIMIT>` — Maximum rows per data page.

  Controls the maximum number of rows in each data page within a column chunk. Default: unlimited (one page per row group for optimal DuckDB compatibility).
- `--parquet-dictionary-page-size <PARQUET_DICTIONARY_PAGE_SIZE>` — Maximum dictionary page size in bytes.

  Controls the maximum size of dictionary pages. When a dictionary exceeds this size, the writer falls back to the data page encoding for remaining values. Default: 1GB (DuckDB MAX_UNCOMPRESSED_DICT_PAGE_SIZE).
- `--parquet-write-batch-size <PARQUET_WRITE_BATCH_SIZE>` — Internal write batch size.

  Controls how many rows are processed at once when writing data pages. Larger values improve throughput but use more memory. Default: 8192.
- `--parquet-offset-index` — Enable offset index writing.

  Offset indexes store the position of each data page within column chunks, enabling faster page-level seeks. Only useful when there are multiple data pages per column chunk. Default: disabled (not needed with one page per row group).
- `--parquet-page-header-statistics` — Write statistics to page headers.

  Embeds min/max statistics in each data page header. This is redundant with column index statistics and can increase file size. Plus it's generally not used by query engines.

  Only use if you know what you're doing. Default: disabled.
- `--parquet-arrow-metadata` — Embed Arrow schema in file metadata.

  Stores the original Arrow schema in the Parquet file's key-value metadata. This enables exact schema round-tripping but adds overhead. Default: disabled (not needed for most use cases).
- `--vortex-record-batch-size <VORTEX_RECORD_BATCH_SIZE>` — Vortex record batch size

## `silk-chiffon inspect`

Inspect file metadata and structure.

Examples:

    # Identify format
    silk-chiffon inspect identify data.parquet

    # Inspect Parquet file
    silk-chiffon inspect parquet data.parquet --pages

    # Inspect Arrow file
    silk-chiffon inspect arrow data.arrow --batches

**Usage:** `silk-chiffon inspect <COMMAND>`

###### **Subcommands:**

- `identify` — Detect file format
- `parquet` — Inspect a Parquet file
- `arrow` — Inspect an Arrow IPC file
- `vortex` — Inspect a Vortex file

## `silk-chiffon inspect identify`

Detect file format

**Usage:** `silk-chiffon inspect identify [OPTIONS] <FILE>`

###### **Arguments:**

- `<FILE>` — Path to the file to identify

###### **Options:**

- `-f`, `--format <FORMAT>` — Output format (auto-detects based on TTY if not specified)

  Default value: `auto`

  Possible values:
  - `auto`:
    Auto-detect: JSON if stdout is not a TTY, otherwise text
  - `text`:
    Human-readable text output
  - `json`:
    JSON output

## `silk-chiffon inspect parquet`

Inspect a Parquet file

**Usage:** `silk-chiffon inspect parquet [OPTIONS] <FILE>`

###### **Arguments:**

- `<FILE>` — Path to the Parquet file

###### **Options:**

- `-f`, `--format <FORMAT>` — Output format (auto-detects based on TTY if not specified)

  Default value: `auto`

  Possible values:
  - `auto`:
    Auto-detect: JSON if stdout is not a TTY, otherwise text
  - `text`:
    Human-readable text output
  - `json`:
    JSON output

- `-g`, `--row-group <ROW_GROUP>` — Row group to display details for (default: 0)

  Default value: `0`
- `-p`, `--pages <PAGES>` — Show page details for columns (comma-separated, or omit value for all columns)

## `silk-chiffon inspect arrow`

Inspect an Arrow IPC file

**Usage:** `silk-chiffon inspect arrow [OPTIONS] <FILE>`

###### **Arguments:**

- `<FILE>` — Path to the Arrow IPC file

###### **Options:**

- `--batches` — Show per-record-batch details
- `-f`, `--format <FORMAT>` — Output format (auto-detects based on TTY if not specified)

  Default value: `auto`

  Possible values:
  - `auto`:
    Auto-detect: JSON if stdout is not a TTY, otherwise text
  - `text`:
    Human-readable text output
  - `json`:
    JSON output

- `--row-count` — Count total rows (requires reading entire file)

## `silk-chiffon inspect vortex`

Inspect a Vortex file

**Usage:** `silk-chiffon inspect vortex [OPTIONS] <FILE>`

###### **Arguments:**

- `<FILE>` — Path to the Vortex file

###### **Options:**

- `--schema` — Show full schema details
- `--stats` — Show per-column statistics
- `--layout` — Show layout structure
- `-f`, `--format <FORMAT>` — Output format (auto-detects based on TTY if not specified)

  Default value: `auto`

  Possible values:
  - `auto`:
    Auto-detect: JSON if stdout is not a TTY, otherwise text
  - `text`:
    Human-readable text output
  - `json`:
    JSON output

## `silk-chiffon completions`

Generate shell completions for your shell.

To add completions for your current shell session only:

    zsh:  eval "$(silk-chiffon completions zsh)"
    bash: eval "$(silk-chiffon completions bash)"
    fish: silk-chiffon completions fish | source

To persist completions across sessions:

    zsh:  echo 'eval "$(silk-chiffon completions zsh)"' >> ~/.zshrc
    bash: echo 'eval "$(silk-chiffon completions bash)"' >> ~/.bashrc
    fish: silk-chiffon completions fish > ~/.config/fish/completions/silk-chiffon.fish

**Usage:** `silk-chiffon completions <SHELL>`

###### **Arguments:**

- `<SHELL>` — Shell to generate completions for

  Possible values: `bash`, `elvish`, `fish`, `powershell`, `zsh`

<hr/>

<small><i>
This document was generated automatically by
<a href="https://crates.io/crates/clap-markdown"><code>clap-markdown</code></a>.
</i></small>
