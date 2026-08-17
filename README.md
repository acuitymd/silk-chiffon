# 🎀 silk-chiffon

_Converting columnar data, silky-smooth._

silk-chiffon is a command-line tool for moving data between the three columnar formats we reach for most: [Apache Arrow](https://arrow.apache.org/) IPC (both the file and streaming variants), [Apache Parquet](https://parquet.apache.org/), and [Vortex](https://docs.vortex.dev/) (a newer columnar format). It reads any of the three and writes any of the three, and it can sort, filter, merge, re-encode, and partition the data on the way through.

It runs on [DataFusion](https://datafusion.apache.org/), so any reshaping you would express in SQL is a `--query` away. It also streams data in batches against a memory budget rather than reading a whole file at once, so large inputs convert without exhausting memory.

## Install

Prebuilt binaries for each release are on the [releases page](https://github.com/acuitymd/silk-chiffon/releases). Or build it yourself with a recent Rust toolchain:

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
  --from-many 'raw/*.arrow' \
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
silk-chiffon inspect identify mystery.bin       # which of the three formats is this?
silk-chiffon inspect parquet data.parquet       # schema, row groups, and statistics
silk-chiffon inspect arrow data.arrow --batches  # schema and record-batch layout
silk-chiffon inspect vortex data.vortex --stats  # schema and column statistics
```

Each inspector can emit JSON with `--format json` for piping into other tools.

## Command reference

`transform` and `inspect` carry many more options than these examples show: compression levels, writer versions, dictionary control, partition strategies, and thread and queue tuning among them. The complete reference is in **[docs/CLI.md](docs/CLI.md)**, generated from the code so it can't drift. At the terminal, `silk-chiffon <command> --help` prints the same content.

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
