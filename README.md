# 🎀 Silk Chiffon

> _Converting Arrow files has never been silkier..._

## ✨ What is Silk Chiffon?

Silk Chiffon is a blazingly fast, memory-efficient CLI tool for converting from/to the Apache Arrow IPC, Parquet, and Vortex columnar data formats. Written in Rust for maximum performance.

Like its namesake fabric -- light, flowing, and effortlessly elegant -- this tool makes data transformations silky smooth.

### ️🎯 Core Features

- **⚡ Lightning Fast**: Built with Rust for native performance.
- **🤹🏻‍♀ Multi-Format Support**: Convert to/from Arrow IPC (file/stream), Parquet, and Vortex.
- **🪓 Partitioning**: Partition data into multiple files based on column values.
- **🔗 Merging**: Merge data from multiple files into a single file.
- **🧠 Smart Processing**: Sort, compress, filter with SQL, and optimize your data on-the-fly.
- **🦋 Re-cast column types**: Use SQL to convert input columns to right-sized output columns and even add new ones.
- **🤏🏻 Memory Efficient**: Configurable batch processing for huge datasets.
- **⚙️ Rich Configuration**: Fine-tune many aspects of your conversions.

## 📦 Installation

### From Source locally

```bash
cargo install --path .
```

### From GitHub

```bash
cargo install --git https://github.com/acuitymd/silk-chiffon
```

### Releases

You can download prebuilt binaries from [each of our releases](https://github.com/acuitymd/silk-chiffon/releases).

> [!IMPORTANT]
> macOS will correctly detect that the downloaded binary is unsigned and will graciously offer to yeet the entire binary. To remove this roadblock you can unquarantine the binary using: `xattr -d com.apple.quarantine /path/to/silk-chiffon`.

## 🚀 Quick Start

### Basic Conversions

Convert Arrow to Parquet with compression and sorting:

```bash
silk-chiffon transform --from input.arrow --to output.parquet --parquet-compression zstd --sort-by amount
```

Convert between Arrow formats:

```bash
silk-chiffon transform --from stream.arrows --to file.arrow --arrow-compression lz4
```

### Merging Multiple Files

Merge Arrow files into a single Parquet file:

```bash
silk-chiffon transform --from-many file1.arrow --from-many file2.arrow --to merged.parquet
```

Using glob patterns:

```bash
silk-chiffon transform --from-many '*.arrow' --to merged.parquet
```

### Partitioning Data

Partition data by column values:

```bash
silk-chiffon transform --from data.arrow --to-many '{{region}}/data.parquet' --by region
```

Multi-column partitioning:

```bash
silk-chiffon transform --from data.arrow --to-many '{{year}}/{{month}}/data.parquet' --by year,month
```

### SQL Filtering

Filter data with SQL queries:

```bash
silk-chiffon transform --from data.arrow --to filtered.parquet \
  --query "SELECT * FROM data WHERE amount > 1000 AND status = 'active'"
```

### SQL Casting

Convert input columns to different types, and even add new ones:

```bash
silk-chiffon transform --from data.arrow --to date_casted.parquet \
  --query "SELECT * EXCEPT (created_at), arrow_cast(created_at, 'Date32') AS created_at FROM data"

silk-chiffon transform --from data.arrow --to id_casted.parquet \
  --query "SELECT * EXCEPT (id), arrow_cast(id, 'Int32') AS id FROM data"

silk-chiffon transform --from data.arrow --to added_creation_year.parquet \
  --query "SELECT *, extract(year FROM created_at) AS creation_year FROM data"
```

### Combining Operations

Merge, filter, sort, and partition in one command:

```bash
silk-chiffon transform \
  --from-many 'source/*.arrow' \
  --to-many 'output/{{region}}/{{year}}.parquet' \
  --by region,year \
  --query "SELECT * FROM data WHERE status = 'active'" \
  --sort-by date:desc \
  --parquet-compression zstd
```

## 🗒️ Command Reference

### Transform Command

The `transform` command is your one-stop shop for all data transformations:

```bash
silk-chiffon transform [OPTIONS]
```

### Input/Output Options

- `--from <PATH>` - Single input file path
- `--from-many <PATH>` - Multiple input files (supports glob patterns, can be specified multiple times)
- `--to <PATH>` - Single output file path
- `--to-many <TEMPLATE>` - Output path template for partitioning (e.g., `{{column}}.parquet`)
- `--by <COLUMNS>` - Column(s) to partition by (comma-separated, requires `--to-many`)

### Processing Options

- `--query <SQL>` - SQL query to filter/transform data
- `--dialect <DIALECT>` - SQL dialect (duckdb, postgres, mysql, sqlite, etc.)
- `--sort-by <SPEC>` - Sort by columns (e.g., `date,amount:desc`)
- `--exclude-columns <COLS>` - Columns to exclude from output

### Format Detection

- `--input-format <FORMAT>` - Override input format detection (arrow, parquet)
- `--output-format <FORMAT>` - Override output format detection (arrow, parquet)

### Arrow Options

- `--arrow-compression <CODEC>` - Compression codec (zstd, lz4, none)
- `--arrow-format <FORMAT>` - IPC format (file, stream)
- `--arrow-record-batch-size <SIZE>` - Record batch size

### Parquet Options

- `--parquet-compression <CODEC>` - Compression codec (zstd, snappy, gzip, lz4, none)
- `--parquet-row-group-size <SIZE>` - Maximum rows per row group
- `--parquet-statistics <LEVEL>` - Statistics level (none, chunk, page)
- `--parquet-writer-version <VERSION>` - Writer version (v1, v2)
- `--parquet-dictionary-all-off` - Disable dictionary encoding
- `--parquet-sorted-metadata` - Embed sorted metadata (requires `--sort-by`)

### Vortex Options

- `--vortex-record-batch-size <VORTEX_RECORD_BATCH_SIZE>` - Vortex record batch size

### Bloom Filter Options

Bloom filters are automatically enabled for columns that keep dictionary encoding
(low cardinality columns). The NDV is determined by cardinality analysis. Use
`--parquet-bloom-all-off` to disable globally, or `--parquet-bloom-column-off`
to exclude specific columns.

Specifying NDV explicitly forces bloom filters ON regardless of dictionary state:

```bash
--parquet-bloom-all "ndv=10000"           # Force bloom on all columns with NDV=10000
--parquet-bloom-all "fpp=0.001,ndv=10000" # Custom FPP and NDV
```

Just specifying FPP uses the automatic dictionary-based decision:

```bash
--parquet-bloom-all "fpp=0.001" # Custom FPP, auto NDV from analysis
```

Per-column configuration (overrides defaults):

```bash
--parquet-bloom-column "user_id:ndv=50000"           # Force bloom on with NDV
--parquet-bloom-column "user_id:fpp=0.001,ndv=50000" # Custom FPP and NDV
--parquet-bloom-column-off "high_cardinality_col"    # Disable for specific column
```

### Other Options

- `--create-dirs` - Create output directories as needed (default: true)
- `--overwrite` - Overwrite existing files
- `--list-outputs <FORMAT>` - List output files after creation (text, json)

### Shell Completions

Generate shell completions for your shell:

```bash
# To add completions for your current shell session only

## zsh
eval "$(silk-chiffon completions zsh)"

## bash
eval "$(silk-chiffon completions bash)"

## fish
silk-chiffon completions fish | source

# To persist completions across sessions

## zsh
echo 'eval "$(silk-chiffon completions zsh)"' >> ~/.zshrc

## bash
echo 'eval "$(silk-chiffon completions bash)"' >> ~/.bashrc

## fish
silk-chiffon completions fish > ~/.config/fish/completions/silk-chiffon.fish
```

## 📚 Examples

### Convert with Sorting

```bash
silk-chiffon transform --from data.arrow --to sorted.parquet --sort-by timestamp:desc,user_id
```

### Partition by Multiple Columns

```bash
silk-chiffon transform \
  --from events.arrow \
  --to-many 'partitioned/year={{year}}/month={{month}}/data.parquet' \
  --by year,month \
  --list-outputs text
```

### Filter and Aggregate with SQL

```bash
silk-chiffon transform \
  --from transactions.arrow \
  --to summary.parquet \
  --query "SELECT region, DATE_TRUNC('month', date) as month, SUM(amount) as total FROM data GROUP BY region, month"
```

### Merge with Bloom Filters

```bash
# force bloom filters on high-cardinality ID columns with explicit NDV
silk-chiffon transform \
  --from-many 'logs/*.arrow' \
  --to optimized.parquet \
  --parquet-compression zstd \
  --parquet-bloom-column "user_id:fpp=0.001,ndv=1000000" \
  --parquet-bloom-column "session_id:fpp=0.001,ndv=5000000"
```

### Complex Pipeline

```bash
silk-chiffon transform \
  --from-many 'raw_data_*.arrow' \
  --to-many 'processed/{{status}}/data_{{hash}}.parquet' \
  --by status \
  --query "SELECT *, CASE WHEN amount > 1000 THEN 'high' ELSE 'low' END as tier FROM data WHERE timestamp > '2024-01-01'" \
  --sort-by timestamp:desc \
  --parquet-compression zstd \
  --parquet-sorted-metadata \
  --list-outputs json
```

## 🔧 Development

### Building

```bash
just build
```

### Testing

```bash
just test
```

### Linting

```bash
just lint
```

### Formatting

```bash
just fmt
```

## 📝 License

Silk Chiffon is open source software, licensed under [LICENSE](./LICENSE).

---

_Made with 🦀 and ❤️ by AcuityMD for the data community_
