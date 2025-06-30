# 🎀 Silk Chiffon

> *Converting Arrow files has never been silkier...*

A blazingly fast, memory-efficient CLI tool and Python library for converting between Apache Arrow data format and a handful of others. Written in Rust for maximum performance, wrapped in Python for maximum convenience.

## ✨ What is Silk Chiffon?

Silk Chiffon is your Swiss Army knife for Arrow data format conversions. Like its namesake fabric&mdash;light, flowing, and effortlessly elegant&mdash;this tool makes data transformations smooth and painless.

### 🎯 Core Features

- **🚀 Lightning Fast**: Built with Rust for native performance.
- **🔄 Multi-Format Support**: Convert between Arrow IPC, Parquet, and DuckDB.
- **🧩 Smart Processing**: Sort, compress, and optimize your data on-the-fly.
- **🐍 Python-Friendly**: Native Python bindings for seamless integration.
- **💾 Memory Efficient**: Configurable batch processing for huge datasets.
- **🎨 Rich Configuration**: Fine-tune every aspect of your conversions.

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

# Convert Arrow to Parquet with bloom filters
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
```

## 📋 Command Reference

### 🪶 Arrow → Parquet

Transform your Arrow data into analytics-optimized Parquet files:

```bash
silk-chiffon parquet [OPTIONS] <INPUT> <OUTPUT>
```

**Key Options:**

- `--compression`: Choose from `zstd`, `snappy`, `gzip`, `lz4`, or `none`
- `--sort-by`: Sort by columns (e.g., `"date,amount:desc"`)
- `--bloom-all`: Enable bloom filters for all columns
- `--bloom-column`: Target specific columns for bloom filters
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

**Key Options:**

- `--table-name`: Required table name for your data
- `--sort-by`: Pre-sort data before insertion
- `--drop-table`: Replace existing table of the same name
- `--truncate`: Start fresh with an empty database

### 🔄 Arrow → Arrow

Transform between Arrow formats or apply optimizations:

```bash
silk-chiffon arrow [OPTIONS] <INPUT> <OUTPUT>
```

**Key Options:**

- `--compression`: Apply `zstd`, `lz4`, or keep uncompressed
- `--sort-by`: Reorder your data
- `--record-batch-size`: Control memory usage

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

Silk Chiffon is open source software, licensed under [LICENSE].

---

*Made with 🦀 and ❤️ for the data community*