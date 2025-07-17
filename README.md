# 🎀 Silk Chiffon

> _Converting Arrow files has never been silkier..._

A blazingly fast, memory-efficient CLI tool and Python library for converting between the Apache Arrow IPC data format and a handful of other formats. Written in Rust for maximum performance, wrapped in Python for maximum convenience.

## ✨ What is Silk Chiffon?

Silk Chiffon is versatile tool for Arrow-to-X data format conversions. Like its namesake fabric&mdash;light, flowing, and effortlessly elegant&mdash;this tool makes data transformations silky smooth.

### ️🎯 Core Features

- **⚡ Lightning Fast**: Built with Rust for native performance.
- **🤹🏻‍♀ Multi-Format Support**: Convert from Arrow IPC to Arrow IPC, Parquet, and DuckDB.
- **🪓 Data Splitting**: Split data into multiple files based on column values.
- **🔗 Data Merging**: Merge data from multiple files into a single file.
- **🧠 Smart Processing**: Sort, compress, and optimize your data on-the-fly.
- **🐍 Python-Friendly**: Native Python bindings for seamless integration.
- **🤏🏻 Memory Efficient**: Configurable batch processing for huge datasets.
- **⚙️ Rich Configuration**: Fine-tune many aspects of your conversions.

## 📦 Installation

### From Source

```bash
cargo install --path .
```

### Python Package

> [!NOTE]
> Soooooon....

```bash
pip install silk-chiffon
```

### Binary

> [!NOTE]
> Soooooon....

```bash
cargo binstall silk-chiffon
```

## 🚀 Quick Start

### Command Line

Convert Arrow to Parquet with compression and sorting:

```bash
silk-chiffon parquet input.arrow output.parquet --compression zstd --sort-by "amount:asc"
```

Convert to DuckDB with sorting:

```bash
silk-chiffon duckdb input.arrow output.db --table-name sales --sort-by "date,amount:desc"
```

Transform Arrow formats with sorting and compression:

```bash
silk-chiffon arrow stream.arrows file.arrow --compression lz4 --sort-by "date:asc"
```

### Python API

```python
import silk_chiffon as sc

# Convert Arrow to Parquet with bloom filters, with NDV automatically optimized
sc.arrow_to_parquet(
    "data.arrow",
    "data.parquet",
    compression="zstd",
    bloom_columns=["user_id", "product_id"]
)

# Load Arrow data into DuckDB
sc.arrow_to_duckdb(
    "data.arrow",
    "analytics.db",
    table_name="events",
    sort_by="timestamp"
)

# Split Arrow data by category into multiple Arrow files
sc.split_to_arrow(
    "data.arrow",
    "output/{value}.arrow",
    split_by="category",
    sort_by=[("timestamp", "desc")],
    compression="lz4"
)

# Split Arrow data into partitioned Parquet files
sc.split_to_parquet(
    "transactions.arrow",
    "by_customer/{value}/data.parquet",
    split_by="customer_id",
    compression="zstd",
    bloom_filter_columns=["transaction_id"],
    create_dirs=True
)
```

## 🗒️ Command Reference

### Available Commands

- **[`parquet`](#-arrow--parquet)** - Convert Arrow to Parquet format
- **[`duckdb`](#-arrow--duckdb)** - Convert Arrow to DuckDB database
- **[`arrow`](#-arrow--arrow)** - Convert between Arrow formats (file ↔ stream)
- **[`split-to-arrow`](#-split-arrow--multiple-arrow-files)** - Split Arrow data into multiple Arrow files
- **[`split-to-parquet`](#-split-arrow--multiple-parquet-files)** - Split Arrow data into multiple Parquet files
- **[`merge-to-arrow`](#-merge-arrow--arrow)** - Merge multiple Arrow files into single Arrow file
- **[`merge-to-parquet`](#-merge-arrow--parquet)** - Merge multiple Arrow files into single Parquet file
- **[`merge-to-duckdb`](#-merge-arrow--duckdb)** - Merge multiple Arrow files into DuckDB table

### 🪶 Arrow → Parquet

Transform your Arrow data into optimized Parquet files:

```bash
silk-chiffon parquet [OPTIONS] <INPUT> <OUTPUT>
```

**Key Options:**

- `--compression`: Choose from `zstd`, `snappy`, `gzip`, `lz4`, or `none`
- `--sort-by`: Sort by columns (e.g., `"date,amount:desc"`)
- `--bloom-all`: Enable bloom filters for all columns, with optimal NDV calculated automatically
- `--bloom-column`: Target specific columns for bloom filters, also with automatic NDV values
- `--max-row-group-size`: Control Parquet row group sizing
- `--statistics`: Set statistics level (`none`, `chunk`, `page`)
- `--writer-version`: Choose Parquet format version (`v1`, `v2`)

**Advanced Bloom Filter Configuration:**

```bash
# Default false positive probability (1%)
silk-chiffon parquet data.arrow data.parquet --bloom-all

# Custom FPP for all columns
silk-chiffon parquet data.arrow data.parquet --bloom-all "fpp=0.001"

# Per-column bloom filters with custom settings
silk-chiffon parquet data.arrow data.parquet \
  --bloom-column "user_id:fpp=0.001" \
  --bloom-column "session_id"
```

### 🦆 Arrow → DuckDB

Load your Arrow data directly into the DuckDB format:

```bash
silk-chiffon duckdb [OPTIONS] --table-name <TABLE_NAME> <INPUT> <OUTPUT>
```

> [!NOTE]
> By default this will add new tables to existing DuckDB files, assuming the table doesn't already exist. Use `--truncate` if you want a fresh file.

**Key Options:**

- `--table-name`: Required table name for your data
- `--sort-by`: Pre-sort data before insertion
- `--drop-table`: Replace existing table of the same name
- `--truncate`: Start fresh with an empty database

### 🏹 Arrow → Arrow

Transform between Arrow formats or apply optimizations to an Arrow file:

```bash
silk-chiffon arrow [OPTIONS] <INPUT> <OUTPUT>
```

**Key Options:**

- `--compression`: Apply `zstd`, `lz4`, or keep uncompressed
- `--sort-by`: Reorder your data
- `--record-batch-size`: Control memory usage
- `--output-ipc-format`: Choose output format (`file` or `stream`)

### 📂 Split Arrow → Multiple Arrow Files

Split your Arrow data into multiple files based on unique values in a column:

```bash
silk-chiffon split-to-arrow [OPTIONS] --by <COLUMN> <INPUT>
```

**Key Options:**

- `--by`: Column to split by (required)
- `--output-template`: File naming template with placeholders
- `--sort-by`: Sort data within each partition
- `--compression`: Apply compression to output files
- `--create-dirs`: Create output directories as needed
- `--overwrite`: Replace existing files
- `--output-ipc-format`: Output format for split files (`file` or `stream`)
- `--list-outputs`: List created files after splitting (`json`, `text`, or none)

**Example:**

```bash
# Split by region, creating one file per unique region value
silk-chiffon split-to-arrow data.arrow --by region \
  --output-template "output/{column}/{value}.arrow"

# Split with sorting and compression
silk-chiffon split-to-arrow events.arrow --by date \
  --output-template "events_{value}.arrow" \
  --sort-by "timestamp:desc" \
  --compression lz4 \
  --create-dirs

# Split to Arrow IPC streaming format files with JSON output listing
silk-chiffon split-to-arrow data.arrow --by region \
  --output-template "streams/{value}.arrows" \
  --output-ipc-format stream \
  --list-outputs json
```

### 📁 Split Arrow → Multiple Parquet Files

Split your Arrow data into multiple Parquet files based on unique values in a column:

```bash
silk-chiffon split-to-parquet [OPTIONS] --by <COLUMN> <INPUT>
```

**Key Options:**

- `--by`: Column to split by (required)
- `--output-template`: File naming template with placeholders
- `--sort-by`: Sort data within each partition
- `--compression`: Choose compression algorithm
- `--bloom-all` / `--bloom-column`: Configure bloom filters
- `--write-sorted-metadata`: Embed sort metadata
- `--create-dirs`: Create output directories as needed
- `--overwrite`: Replace existing files

**Example:**

```bash
# Split by customer_id with Parquet optimizations
silk-chiffon split-to-parquet transactions.arrow --by customer_id \
  --output-template "customers/{value}/transactions.parquet" \
  --compression zstd \
  --bloom-column "transaction_id" \
  --create-dirs

# Split with sorting and metadata
silk-chiffon split-to-parquet logs.arrow --by log_level \
  --output-template "logs/level_{value}.parquet" \
  --sort-by "timestamp" \
  --write-sorted-metadata \
  --statistics page
```

**Template Placeholders:**

- `{value}`: The raw column value
- `{column}`: The column name
- `{safe_value}`: Sanitized value safe for filenames
- `{hash}`: First 8 characters of SHA256 hash of the value

### 🔗 Merge Arrow → Arrow

Merge multiple Arrow files into a single Arrow file:

```bash
silk-chiffon merge-to-arrow [OPTIONS] <INPUTS...> <OUTPUT>
```

**Key Options:**

- `--sort-by`: Sort the merged data
- `--compression`: Apply `zstd`, `lz4`, or keep uncompressed
- `--record-batch-size`: Control memory usage
- `--output-ipc-format`: Choose output format (`file` or `stream`)

**Example:**

```bash
# Merge all Arrow files in a directory
silk-chiffon merge-to-arrow data/*.arrow merged.arrow

# Merge with sorting and compression
silk-chiffon merge-to-arrow events_*.arrow merged_events.arrow \
  --sort-by "timestamp:asc" \
  --compression lz4

# Merge to streaming format
silk-chiffon merge-to-arrow region_*.arrow all_regions.arrows \
  --output-ipc-format stream
```

### 🔗 Merge Arrow → Parquet

Merge multiple Arrow files into a single Parquet file:

```bash
silk-chiffon merge-to-parquet [OPTIONS] <INPUTS...> <OUTPUT>
```

**Key Options:**

- `--compression`: Choose from `zstd`, `snappy`, `gzip`, `lz4`, or `none`
- `--sort-by`: Sort the merged data
- `--bloom-all` / `--bloom-column`: Configure bloom filters
- `--max-row-group-size`: Control Parquet row group sizing
- `--statistics`: Set statistics level (`none`, `chunk`, `page`)
- `--writer-version`: Choose Parquet format version (`v1`, `v2`)

**Example:**

```bash
# Merge partitioned data back into single file
silk-chiffon merge-to-parquet customers/*.arrow all_customers.parquet \
  --compression zstd \
  --bloom-column "customer_id"

# Merge with sorting and optimizations
silk-chiffon merge-to-parquet daily_*.arrow yearly.parquet \
  --sort-by "date,amount:desc" \
  --statistics page \
  --write-sorted-metadata
```

### 🔗 Merge Arrow → DuckDB

Merge multiple Arrow files into a single DuckDB table:

```bash
silk-chiffon merge-to-duckdb [OPTIONS] --table-name <TABLE_NAME> <INPUTS...> <OUTPUT>
```

**Key Options:**

- `--table-name`: Required table name for merged data
- `--sort-by`: Pre-sort data before insertion
- `--drop-table`: Replace existing table of the same name
- `--truncate`: Start fresh with an empty database

**Example:**

```bash
# Merge regional data into unified table
silk-chiffon merge-to-duckdb regions/*.arrow analytics.db \
  --table-name sales_data \
  --sort-by "date,region"

# Merge with table replacement
silk-chiffon merge-to-duckdb monthly_*.arrow yearly.db \
  --table-name transactions \
  --drop-table \
  --sort-by "timestamp"
```

## 🎯 Use Cases

### 📊 Data Pipeline Integration

Convert streaming Arrow data to Parquet for long-term storage:

```bash
# Optimize for analytics with compression and bloom filters
silk-chiffon parquet stream.arrow warehouse/data.parquet \
  --compression zstd \
  --bloom-column "customer_id" \
  --sort-by "timestamp"
```

### 🔍 Analytics Workflows

Load Arrow data into DuckDB for analysis:

```bash
# Create an analytics-ready database
silk-chiffon duckdb events.arrow analytics.db \
  --table-name events \
  --sort-by "event_time,user_id"
```

### 🗄️ Format Optimization

Transform and optimize existing Arrow files:

```bash
# Apply compression and sorting to Arrow files
silk-chiffon arrow large.arrow optimized.arrow \
  --compression lz4 \
  --sort-by "date" \
  --record-batch-size 50000

# Convert Arrow IPC file format to streaming format
silk-chiffon arrow data.arrow stream.arrow --output-ipc-format stream
```

### 🐍 Python Data Science

Integrate with pandas and PyArrow workflows:

```python
import silk_chiffon as sc
import pandas as pd

# Your existing Arrow data from pandas/PyArrow
df = pd.read_csv("data.csv")
df.to_arrow("temp.arrow")

# Convert to optimized Parquet
sc.arrow_to_parquet(
    "temp.arrow",
    "optimized.parquet",
    compression="zstd",
    sort_by="date,value:desc",
    statistics="page"
)
```

### 🗂️ Data Partitioning

Split large datasets into manageable partitions:

```bash
# Partition by date for time-series data
silk-chiffon split-to-parquet sensor_data.arrow \
  --by date \
  --output-template "data/{column}/{value}/sensors.parquet" \
  --sort-by "timestamp,sensor_id" \
  --compression zstd \
  --create-dirs

# Split customer data by region for parallel processing
silk-chiffon split-to-arrow customers.arrow \
  --by region \
  --output-template "regions/{safe_value}.arrow" \
  --sort-by "customer_id"
```

## 🔧 Advanced Features

### Bloom Filters

Silk Chiffon offers sophisticated bloom filter configuration for Parquet files:

- Apply to all columns or specific ones
- Customize false positive probability (FPP) per column
- Automatically determines optimal number of distinct values (NDV) per column

### Smart Sorting

- Multi-column sorting with direction control
- Efficient memory usage during sort operations
- Metadata embedding for sorted Parquet files

### Compression Options

- Multiple algorithms: ZSTD (best ratio), Snappy (fastest), GZIP, LZ4

## 🏗️ Architecture

Silk Chiffon is built on a foundation of high-performance Rust libraries:

- **Apache Arrow**: Columnar memory format
- **Apache Parquet**: Columnar disk format
- **DataFusion**: Query engine for sorting operations
- **DuckDB**: Embedded analytical database
- **PyO3**: Python bindings

The tool follows a composable architecture with dedicated converters for each format, each building upon the others, to ensure adequate performance and maximal maintainability.

## 🤝 Contributing

We welcome contributions! Please check out our [GitHub repository](https://github.com/acuitymd/silk-chiffon) for:

- Issue tracking
- Feature requests
- Development guidelines

## 📄 License

Silk Chiffon is open source software, licensed under [LICENSE](./LICENSE).

---

_Made with 🦀 and ❤️ by AcuityMD for the data community_
