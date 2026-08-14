pub mod commands;
pub mod inspection;
pub mod operations;
pub mod registration;
pub mod sinks;
pub mod sources;
pub mod utils;

use crate::utils::collections::{uniq, uniq_by};
use anyhow::{Result, anyhow};
use camino::Utf8PathBuf;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum, builder::ValueHint};
use clap_complete::Shell;
use silk_chiffon_core::{QueryDialect, SpillCompression};
use std::{
    ffi::OsString,
    fmt::{self, Formatter},
    io::{self, IsTerminal},
    str::FromStr,
};
use strum_macros::Display;

/// Parse a usize that must be at least 1.
pub fn parse_at_least_one(s: &str) -> Result<usize> {
    let n: usize = s.parse().map_err(anyhow::Error::new)?;

    if n == 0 {
        anyhow::bail!("value must be at least 1");
    }
    Ok(n)
}

/// Parse a human-readable byte size (e.g., "512MB", "2GB") that must be greater than 0.
#[allow(clippy::cast_possible_truncation)]
pub fn parse_nonzero_byte_size(s: &str) -> Result<usize> {
    let bytes = s
        .parse::<bytesize::ByteSize>()
        .map_err(|_| {
            anyhow!("invalid byte size '{s}': expected format like '512MB', '2GB', or '1GiB'")
        })?
        .as_u64() as usize;
    if bytes == 0 {
        anyhow::bail!("value must be greater than 0");
    }
    Ok(bytes)
}

/// Default thread budget: all available CPUs.
pub fn default_thread_budget() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
}

/// Specifies how to determine the thread budget.
#[derive(Debug, Clone)]
pub enum ThreadBudgetSpec {
    /// Use a fixed thread count.
    Fixed(usize),
    /// Use all CPUs minus a reserved count, with an optional minimum.
    Reserve { reserve: usize, min: usize },
}

impl ThreadBudgetSpec {
    pub fn resolve(&self) -> usize {
        match self {
            ThreadBudgetSpec::Fixed(n) => *n,
            ThreadBudgetSpec::Reserve { reserve, min } => {
                default_thread_budget().saturating_sub(*reserve).max(*min)
            }
        }
    }
}

impl FromStr for ThreadBudgetSpec {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // plain number
        if let Ok(n) = s.parse::<usize>() {
            if n == 0 {
                anyhow::bail!("thread budget must be at least 1");
            }
            return Ok(ThreadBudgetSpec::Fixed(n));
        }

        let parts: Vec<&str> = s.split(':').collect();
        match parts.as_slice() {
            // reserve:N
            ["reserve", n] => {
                let reserve: usize = n
                    .parse()
                    .map_err(|_| anyhow!("invalid reserve count '{n}'"))?;
                Ok(ThreadBudgetSpec::Reserve { reserve, min: 1 })
            }
            // reserve:N:min:M
            ["reserve", n, "min", m] => {
                let reserve: usize = n
                    .parse()
                    .map_err(|_| anyhow!("invalid reserve count '{n}'"))?;
                let min: usize = m.parse().map_err(|_| anyhow!("invalid minimum '{m}'"))?;
                if min == 0 {
                    anyhow::bail!("minimum must be at least 1");
                }
                Ok(ThreadBudgetSpec::Reserve { reserve, min })
            }
            _ => anyhow::bail!(
                "invalid thread budget '{s}': expected a number, 'reserve:N', or 'reserve:N:min:M'"
            ),
        }
    }
}

/// Specifies how to determine the memory budget.
#[derive(Debug, Clone)]
pub enum MemoryBudgetSpec {
    /// Use a percentage of total system memory, with optional minimum bytes.
    Total { pct: u8, min: Option<usize> },
    /// Use a percentage of currently available (free) memory, with optional minimum bytes.
    Available { pct: u8, min: Option<usize> },
    /// Use a fixed byte amount.
    Fixed(usize),
    /// Use total memory minus a reserved byte amount, with optional minimum bytes.
    Reserve { reserve: usize, min: Option<usize> },
}

impl MemoryBudgetSpec {
    pub fn resolve(&self) -> usize {
        match self {
            MemoryBudgetSpec::Total { pct, min } => {
                let budget = utils::memory::total_memory() * usize::from(*pct) / 100;
                budget.max(min.unwrap_or(0))
            }
            MemoryBudgetSpec::Available { pct, min } => {
                let budget = utils::memory::available_memory() * usize::from(*pct) / 100;
                budget.max(min.unwrap_or(0))
            }
            MemoryBudgetSpec::Fixed(n) => *n,
            MemoryBudgetSpec::Reserve { reserve, min } => {
                let budget = utils::memory::total_memory().saturating_sub(*reserve);
                budget.max(min.unwrap_or(0))
            }
        }
    }
}

impl FromStr for MemoryBudgetSpec {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();

        let keyword = parts[0].to_ascii_lowercase();
        match keyword.as_str() {
            "total" | "available" => {
                let pct_str = parts.get(1).copied();
                let pct = parse_percent(pct_str, 80)?;
                let min = parse_optional_min(&parts, 2)?;

                if keyword == "total" {
                    Ok(MemoryBudgetSpec::Total { pct, min })
                } else {
                    Ok(MemoryBudgetSpec::Available { pct, min })
                }
            }
            "reserve" => {
                let val = parts
                    .get(1)
                    .ok_or_else(|| anyhow!("reserve requires a byte size, e.g. 'reserve:2GB'"))?;
                let reserve = parse_nonzero_byte_size(val)?;
                let min = parse_optional_min(&parts, 2)?;
                Ok(MemoryBudgetSpec::Reserve { reserve, min })
            }
            _ => {
                if parts.len() > 1 {
                    anyhow::bail!(
                        "unknown keyword '{keyword}': expected 'total', 'available', 'reserve', or a byte size"
                    );
                }
                parse_nonzero_byte_size(s).map(MemoryBudgetSpec::Fixed)
            }
        }
    }
}

/// Parses an optional `:min:<size>` suffix from a split parts array starting at `offset`.
fn parse_optional_min(parts: &[&str], offset: usize) -> Result<Option<usize>> {
    match parts.get(offset) {
        None => Ok(None),
        Some(&"min") => {
            let val = parts
                .get(offset + 1)
                .ok_or_else(|| anyhow!("min requires a byte size, e.g. 'min:4GB'"))?;
            Ok(Some(parse_nonzero_byte_size(val)?))
        }
        Some(other) => anyhow::bail!("unexpected segment '{other}': expected 'min'"),
    }
}

fn parse_percent(s: Option<&str>, default: u8) -> Result<u8> {
    let Some(s) = s else { return Ok(default) };

    let s = s.strip_suffix('%').unwrap_or(s);

    let pct: u8 = s
        .parse()
        .map_err(|_| anyhow!("invalid percentage '{s}': expected 1-100"))?;

    if pct == 0 || pct > 100 {
        anyhow::bail!("percentage must be between 1 and 100, got {pct}");
    }

    Ok(pct)
}

/// Specifies a memory reserve as either a percentage or fixed byte size.
///
/// Used for pool sub-budgets where the reference point is the pool size.
/// - `"10"` or `"10%"` - percentage of pool
/// - `"200MB"` or `"1GiB"` - fixed byte size (unit required)
#[derive(Debug, Clone, Copy)]
pub enum PoolReserveSpec {
    /// Percentage of pool size (1-99).
    Percent(u8),
    /// Fixed byte amount.
    Fixed(usize),
}

impl PoolReserveSpec {
    /// Resolve the reserve against the given pool size.
    ///
    /// Returns an error if pool_size is 0 or if the reserve exceeds `pool_size - 1`.
    pub fn resolve(&self, pool_size: usize) -> Result<usize> {
        if pool_size == 0 {
            anyhow::bail!("cannot resolve non-spillable reserve against a zero-byte pool");
        }

        let reserve = match self {
            Self::Percent(pct) => {
                let r = pool_size * usize::from(*pct) / 100;
                if r == 0 {
                    anyhow::bail!(
                        "non-spillable reserve of {pct}% resolves to 0 bytes for pool size {}; \
                         use a larger pool or a fixed byte reserve instead",
                        bytesize::ByteSize::b(pool_size as u64),
                    );
                }
                r
            }
            Self::Fixed(n) => *n,
        };

        let max_reserve = pool_size - 1;
        if reserve > max_reserve {
            anyhow::bail!(
                "non-spillable reserve ({}) exceeds pool size ({}); \
                 the reserve must be less than the pool size",
                bytesize::ByteSize::b(reserve as u64),
                bytesize::ByteSize::b(pool_size as u64),
            );
        }

        Ok(reserve)
    }
}

impl FromStr for PoolReserveSpec {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s_stripped = s.strip_suffix('%').unwrap_or(s);

        if !s_stripped.is_empty() && s_stripped.chars().all(|c| c.is_ascii_digit()) {
            let pct: u8 = s_stripped
                .parse()
                .map_err(|_| anyhow!("invalid percentage '{s}': expected 1-99"))?;
            if pct == 0 || pct >= 100 {
                anyhow::bail!("reserve percentage must be between 1 and 99, got {pct}");
            }
            return Ok(Self::Percent(pct));
        }

        parse_nonzero_byte_size(s).map(Self::Fixed)
    }
}

#[derive(Parser)]
#[command(
    name = "silk-chiffon",
    version = env!("SILK_CHIFFON_VERSION"),
    about,
    long_about = None
)]
struct CliSchema {
    #[command(subcommand)]
    command: CommandSchema,
}

/// A parsed command whose registered format, storage, and service settings are bound.
pub struct Cli {
    pub command: Command,
}

impl Cli {
    /// Parses the process arguments with the composed application definition.
    pub fn parse() -> Self {
        Self::try_parse_from(std::env::args_os()).unwrap_or_else(|error| error.exit())
    }

    /// Parses an explicit argument sequence with the composed application definition.
    pub fn try_parse_from<I, T>(arguments: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        crate::registration::try_parse_from(arguments)
    }
}

impl fmt::Debug for Cli {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Cli").finish_non_exhaustive()
    }
}

/// Render the full CLI reference as Markdown, used by `just docs` to regenerate
/// `docs/CLI.md`. Behind the `docs` feature so `clap-markdown` stays out of the
/// release binary.
#[cfg(feature = "docs")]
pub fn cli_markdown() -> String {
    format!(
        "<!-- Generated from the CLI by `just docs`; edit the clap definitions, not this file. -->\n\n{}",
        clap_markdown::help_markdown::<Cli>()
    )
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum CommandSchema {
    /// Transform data between formats with optional filtering, sorting, merging, and partitioning.
    ///
    /// Examples:
    ///
    ///     # Simple conversion
    ///     silk-chiffon transform --from input.arrow --to output.parquet
    ///
    ///     # Merge multiple files
    ///     silk-chiffon transform --from file1.arrow --from file2.arrow --to merged.parquet
    ///
    ///     # Partition into multiple files
    ///     silk-chiffon transform --from input.arrow --to-many "{{region}}.parquet" --by region
    ///
    ///     # Merge and partition with glob
    ///     silk-chiffon transform --from-pattern '*.arrow' \
    ///       --to-many "{{year}}/{{month}}.parquet" --by year,month
    #[command(verbatim_doc_comment)]
    Transform(TransformArgs),

    /// Detect the format of an input.
    Detect(DetectArgs),

    /// Inspect file metadata and structure.
    ///
    /// Examples:
    ///
    ///     # Inspect Parquet file
    ///     silk-chiffon inspect parquet data.parquet --pages
    ///
    ///     # Inspect Arrow file
    ///     silk-chiffon inspect arrow data.arrow --batches
    #[command(verbatim_doc_comment)]
    Inspect(InspectSchema),

    /// Generate shell completions for your shell.
    ///
    /// To add completions for your current shell session only:
    ///
    ///     zsh:  eval "$(silk-chiffon completions zsh)"
    ///     bash: eval "$(silk-chiffon completions bash)"
    ///     fish: silk-chiffon completions fish | source
    ///
    /// To persist completions across sessions:
    ///
    ///     zsh:  echo 'eval "$(silk-chiffon completions zsh)"' >> ~/.zshrc
    ///     bash: echo 'eval "$(silk-chiffon completions bash)"' >> ~/.bashrc
    ///     fish: silk-chiffon completions fish > ~/.config/fish/completions/silk-chiffon.fish
    #[command(verbatim_doc_comment)]
    Completions {
        /// Shell to generate completions for
        shell: Shell,
    },
}

#[allow(clippy::large_enum_variant)]
/// Runtime state for one parsed top-level command.
///
/// Unlike the private Clap schema, these variants contain command-scoped
/// extension state. Command implementations therefore receive validated
/// bindings instead of consulting global registries.
pub enum Command {
    Transform(TransformCommand),
    Detect(DetectCommand),
    Inspect(InspectCommand),
    Completions { shell: Shell },
}

impl clap::CommandFactory for Cli {
    fn command() -> clap::Command {
        crate::registration::ApplicationDefinition::new().command(CliSchema::command())
    }

    fn command_for_update() -> clap::Command {
        crate::registration::ApplicationDefinition::new().command(CliSchema::command_for_update())
    }
}

impl Command {
    /// Resolves the Tokio runtime worker count for this command.
    pub fn runtime_worker_threads(&self) -> usize {
        match self {
            Self::Transform(command) => command
                .thread_budget
                .as_ref()
                .map(ThreadBudgetSpec::resolve)
                .unwrap_or_else(default_thread_budget),
            _ => default_thread_budget(),
        }
    }

    /// Writes shell completions for the fully composed CLI.
    pub fn generate_completions(shell: Shell) {
        clap_complete::generate(
            shell,
            &mut Cli::command(),
            "silk-chiffon",
            &mut std::io::stdout(),
        );
    }
}

#[derive(ValueEnum, Clone, Copy, Debug, Default, Display)]
#[value(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum ListOutputsFormat {
    #[default]
    None,
    Text,
    Json,
}

/// Strategy for writing partitioned output files.
#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq, Display)]
#[value(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum PartitionStrategy {
    /// Sort by partition columns first, then write one file at a time.
    /// Keeps at most one output sink open but requires sorting the entire dataset.
    /// Best for high-cardinality partition columns, or when partition columns
    /// are highly fragmented.
    #[default]
    SortSingle,
    /// Keep one output sink open per partition and write rows directly.
    /// No sorting required, preserves input order within each partition.
    /// Best for low-cardinality partition columns with low fragmentation.
    NosortMulti,
    /// Like nosort-multi but caps the number of simultaneously open partition writers.
    /// When the cap is hit, the least-recently-written partition is finalized.
    /// If that partition reappears, `file_number` advances and the complete template is rendered again.
    /// Requires a direct unconditional `{{ file_number }}` interpolation in `--to-many`.
    /// Best for high-cardinality partitions where sorting is too expensive.
    /// Per-writer concurrency is minimized (sequential encoding) since parallelism
    /// comes from having many partition writers active simultaneously.
    NosortEvict,
}

#[derive(Debug, Clone)]
pub struct SortColumn {
    pub name: String,
    pub direction: SortDirection,
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
#[value(rename_all = "lowercase")]
pub enum SortDirection {
    #[value(name = "asc")]
    Ascending,
    #[value(name = "desc")]
    Descending,
}

#[derive(Debug, Clone, Default)]
pub struct SortSpec {
    pub columns: Vec<SortColumn>,
}

impl From<Vec<String>> for SortSpec {
    fn from(names: Vec<String>) -> Self {
        Self {
            columns: uniq(&names)
                .iter()
                .map(|name| SortColumn {
                    name: name.clone(),
                    direction: SortDirection::Ascending,
                })
                .collect(),
        }
    }
}

impl SortSpec {
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn is_configured(&self) -> bool {
        !self.is_empty()
    }

    pub fn contains(&self, column_name: &str) -> bool {
        self.columns.iter().any(|c| c.name == column_name)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    pub fn without_columns_named(&self, column_names: &[String]) -> Self {
        Self {
            columns: self
                .columns
                .iter()
                .filter(|c| !column_names.contains(&c.name))
                .cloned()
                .collect(),
        }
    }

    pub fn extend(&mut self, other: &Self) {
        self.columns.extend(other.columns.iter().cloned());
        self.columns = uniq_by(&self.columns, |c| &c.name);
    }
}

impl FromStr for SortSpec {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut columns = Vec::new();

        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let (name, descending) = if let Some((col, direction)) = part.split_once(':') {
                let direction = direction.trim().to_lowercase();
                match direction.as_str() {
                    "desc" | "descending" => (col.trim(), true),
                    "asc" | "ascending" => (col.trim(), false),
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Invalid sort direction '{}'. Use 'asc' or 'desc'",
                            direction
                        ));
                    }
                }
            } else {
                (part, false) // default to ascending
            };

            columns.push(SortColumn {
                name: name.to_string(),
                direction: if descending {
                    SortDirection::Descending
                } else {
                    SortDirection::Ascending
                },
            });
        }

        Ok(SortSpec {
            columns: uniq_by(&columns, |c| &c.name),
        })
    }
}

impl fmt::Display for SortSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<String> = self
            .columns
            .iter()
            .map(|col| match col.direction {
                SortDirection::Descending => format!("{}:desc", col.name),
                SortDirection::Ascending => col.name.clone(),
            })
            .collect();
        write!(f, "{}", parts.join(","))
    }
}

#[derive(Args, Clone, Debug)]
struct TransformArgs {
    /// Exact input reference. May be specified multiple times.
    #[arg(
        long,
        required_unless_present = "from_pattern",
        help_heading = "Input/Output"
    )]
    pub from: Vec<String>,

    /// File location pattern. May be specified multiple times.
    #[arg(
        long = "from-pattern",
        required_unless_present = "from",
        help_heading = "Input/Output"
    )]
    pub from_pattern: Vec<String>,

    /// Allow an individual file location pattern to match no files.
    #[arg(long, requires = "from_pattern", help_heading = "Input/Output")]
    pub allow_unmatched_patterns: bool,

    /// Override file input format detection.
    #[arg(long, help_heading = "Input/Output")]
    pub input_format: Option<String>,

    /// Override file output format detection.
    #[arg(long, help_heading = "Input/Output")]
    pub output_format: Option<String>,

    /// Exact file or service output target.
    #[arg(
        long,
        conflicts_with_all = ["to_many", "by"],
        required_unless_present = "to_many",
        help_heading = "Input/Output"
    )]
    pub to: Option<String>,

    /// File output template for partitioning (e.g., "{{region}}.parquet"). Requires --by.
    #[arg(
        long,
        conflicts_with = "to",
        requires = "by",
        required_unless_present = "to",
        help_heading = "Input/Output"
    )]
    pub to_many: Option<String>,

    /// The query dialect to use.
    #[arg(
        short,
        long,
        default_value_t,
        value_enum,
        help_heading = "Transformations"
    )]
    pub dialect: QueryDialect,

    /// Names of columns to exclude from the output.
    #[arg(long, short = 'e', help_heading = "Transformations")]
    pub exclude_columns: Vec<String>,

    /// SQL query to apply to the data. The input data is available as table 'data'.
    ///
    /// Examples:
    ///
    ///     --query "SELECT * FROM data WHERE status = 'active'"
    ///     --query "SELECT id, name, amount FROM data"
    ///     --query "SELECT region, SUM(amount) FROM data GROUP BY region"
    ///     --query "SELECT *, amount * 1.1 as adjusted FROM data"
    #[arg(short, long, verbatim_doc_comment, help_heading = "Transformations")]
    pub query: Option<String>,

    /// Sort the data by one or more columns before writing.
    ///
    /// Format: A comma-separated list like `col_a,col_b:desc,col_c`.
    #[arg(short, long, help_heading = "Transformations")]
    pub sort_by: Option<SortSpec>,

    /// Target memory budget. Best-effort, not a hard limit.
    ///
    /// Accepts a byte size (e.g. "8GB"), "total[:pct]" for a percentage of total RAM,
    /// "available[:pct]" for a percentage of free RAM, or "reserve:<size>" to use
    /// total RAM minus a reserved amount. All keyword modes (total, available, reserve)
    /// support an optional minimum: "total:80:min:4GB". Examples: "total:90", "available:60%",
    /// "reserve:2GB:min:1GB", "4GB".
    ///
    /// Setting this too low may cause out-of-memory errors, since some
    /// internal buffers cannot spill to disk. The minimum depends on schema
    /// width, batch size, and parallelism — there is no fixed floor.
    ///
    /// Default: 80% of total memory, container-aware on Linux.
    #[arg(long, help_heading = "Execution", value_parser = MemoryBudgetSpec::from_str, default_value = "total:80%")]
    pub memory_budget: MemoryBudgetSpec,

    /// Reserve memory for non-spillable operations (sort merge phases, etc.).
    ///
    /// Enables an alternative memory pool that prevents spillable operators
    /// (sorts, aggregations) from consuming the entire pool, guaranteeing
    /// headroom for non-spillable consumers that would otherwise fail.
    ///
    /// Accepts a percentage (e.g. "10%", "10") or a fixed byte size (e.g. "200MB").
    /// Percentages are resolved against the memory pool size.
    ///
    /// When not set, the default DataFusion FairSpillPool is used instead.
    #[arg(long, help_heading = "Execution", value_parser = PoolReserveSpec::from_str)]
    pub non_spillable_reserve: Option<PoolReserveSpec>,

    /// Number of top memory consumers to report in out-of-memory error messages.
    ///
    /// When a memory allocation fails, the error message includes the N largest
    /// memory consumers to help diagnose what is using the pool. Set to 0 to
    /// report all consumers.
    ///
    /// Default: 10.
    #[arg(long, help_heading = "Execution", default_value = "10")]
    pub memory_pool_top_consumers: usize,

    /// Target thread budget for parallel work. Best-effort, not a hard limit.
    ///
    /// Accepts a number (e.g. "8"), "reserve:N" to use all CPUs minus N (minimum 1),
    /// or "reserve:N:min:M" for a custom minimum. Examples: "8", "reserve:2",
    /// "reserve:2:min:4".
    ///
    /// Split between encoding and query execution based on workload. Thread pools
    /// intentionally overcommit since not all threads are active simultaneously.
    ///
    /// Default: all CPU cores.
    #[arg(long, short = 't', help_heading = "Execution", value_parser = ThreadBudgetSpec::from_str)]
    pub thread_budget: Option<ThreadBudgetSpec>,

    /// Number of partitions for query execution parallelism.
    ///
    /// Controls how DataFusion partitions data during queries (aggregations, joins, sorts).
    /// Higher values increase parallelism but use more memory.
    ///
    /// Default: auto-detected based on workload. With sorting (--sort-by or --by): 75%
    /// of available CPU cores. Without sorting: DataFusion default.
    #[arg(long, help_heading = "Execution", value_parser = parse_at_least_one)]
    pub target_partitions: Option<usize>,

    /// Directory for spilling intermediate data when memory limit is exceeded.
    ///
    /// When DataFusion operators (sort, group by, aggregation) exceed the memory
    /// limit, they spill to this path. Default: system temp directory.
    #[arg(long, help_heading = "Execution")]
    pub spill_path: Option<Utf8PathBuf>,

    /// Compression for spilled intermediate data.
    ///
    /// Controls compression when DataFusion spills to disk. Lz4 is faster but
    /// produces larger files; zstd achieves better compression but is slower.
    #[arg(long, value_enum, default_value_t = SpillCompression::default(), help_heading = "Execution")]
    pub spill_compression: SpillCompression,

    /// Preserve the row order from the input file in the output.
    ///
    /// By default, DataFusion reads files using multiple partitions for parallelism,
    /// which can interleave rows. This flag forces single-partition reading to maintain
    /// the original row order. Only valid for single-file-to-single-file transforms
    /// without queries or sorting.
    #[arg(
        long,
        default_value_t = false,
        conflicts_with_all = ["query", "sort_by", "to_many", "from_pattern"],
        help_heading = "Execution"
    )]
    pub preserve_input_order: bool,

    /// Column(s) to partition by (comma-separated for multi-column partitioning).
    /// Partition output by column values. Only primitive types (integers, floats,
    /// strings, dates, etc.) are supported. Complex types (arrays, structs, maps)
    /// will error.
    #[arg(long, short, requires = "to_many", help_heading = "Partitioning")]
    pub by: Option<String>,

    /// Partitioning strategy for writing output files.
    #[arg(
        long,
        value_enum,
        default_value_t,
        requires = "by",
        help_heading = "Partitioning"
    )]
    pub partition_strategy: PartitionStrategy,

    /// Maximum number of partition output sinks to keep open simultaneously.
    /// When this limit is reached, the least-recently-written partition is finalized.
    /// Only used with --partition-strategy=nosort-evict. Defaults to 100.
    #[arg(long, requires = "by", help_heading = "Partitioning")]
    pub max_open_partitions: Option<usize>,

    /// List the output files after creation (only with --to-many).
    #[arg(
        long,
        short = 'l',
        value_enum,
        requires = "to_many",
        help_heading = "Partitioning"
    )]
    pub list_outputs: Option<ListOutputsFormat>,

    /// Write output file listing to a file instead of stdout.
    #[arg(long, requires = "list_outputs", help_heading = "Partitioning")]
    pub list_outputs_file: Option<Utf8PathBuf>,

    /// Create file-output directories as needed.
    #[arg(long, default_value_t = true, help_heading = "Output Behavior")]
    pub create_dirs: bool,

    /// Overwrite existing file outputs.
    #[arg(long, help_heading = "Output Behavior")]
    pub overwrite: bool,
}

#[derive(Args, Clone, Debug)]
pub struct VortexArgs {
    /// Vortex record batch size.
    #[arg(long, help_heading = "Vortex Options")]
    pub vortex_record_batch_size: Option<usize>,
}

/// Parsed transform arguments with command-scoped format bindings and storage state.
pub struct TransformCommand {
    inputs: InputRequest,
    allow_unmatched_patterns: bool,
    input_format: Option<String>,
    output_format: Option<String>,
    to: Option<String>,
    to_many: Option<String>,
    dialect: QueryDialect,
    exclude_columns: Vec<String>,
    query: Option<String>,
    sort_by: Option<SortSpec>,
    memory_budget: MemoryBudgetSpec,
    non_spillable_reserve: Option<PoolReserveSpec>,
    memory_pool_top_consumers: usize,
    thread_budget: Option<ThreadBudgetSpec>,
    target_partitions: Option<usize>,
    spill_path: Option<Utf8PathBuf>,
    spill_compression: SpillCompression,
    preserve_input_order: bool,
    by: Option<String>,
    partition_strategy: PartitionStrategy,
    max_open_partitions: Option<usize>,
    list_outputs: Option<ListOutputsFormat>,
    list_outputs_file: Option<Utf8PathBuf>,
    create_dirs: bool,
    overwrite: bool,
    formats: silk_chiffon_core::TransformBindings,
    storage: silk_chiffon_storage::StorageSession,
    service_inputs: crate::registration::ServiceInputBindings,
    service_outputs: crate::registration::ServiceOutputBindings,
    input_schemes: crate::registration::InputSchemeIndex,
    output_schemes: crate::registration::OutputSchemeIndex,
}

pub(crate) struct InputRequest {
    pub(crate) exact_references: Vec<String>,
    pub(crate) file_patterns: Vec<String>,
}

impl TransformCommand {
    fn from_parsed(
        args: TransformArgs,
        formats: silk_chiffon_core::TransformBindings,
        storage: silk_chiffon_storage::StorageSession,
        service_inputs: crate::registration::ServiceInputBindings,
        service_outputs: crate::registration::ServiceOutputBindings,
        input_schemes: crate::registration::InputSchemeIndex,
        output_schemes: crate::registration::OutputSchemeIndex,
    ) -> Self {
        let TransformArgs {
            from,
            from_pattern,
            allow_unmatched_patterns,
            input_format,
            output_format,
            to,
            to_many,
            dialect,
            exclude_columns,
            query,
            sort_by,
            memory_budget,
            non_spillable_reserve,
            memory_pool_top_consumers,
            thread_budget,
            target_partitions,
            spill_path,
            spill_compression,
            preserve_input_order,
            by,
            partition_strategy,
            max_open_partitions,
            list_outputs,
            list_outputs_file,
            create_dirs,
            overwrite,
        } = args;

        let inputs = InputRequest {
            exact_references: from,
            file_patterns: from_pattern,
        };

        Self {
            inputs,
            allow_unmatched_patterns,
            input_format,
            output_format,
            to,
            to_many,
            dialect,
            exclude_columns,
            query,
            sort_by,
            memory_budget,
            non_spillable_reserve,
            memory_pool_top_consumers,
            thread_budget,
            target_partitions,
            spill_path,
            spill_compression,
            preserve_input_order,
            by,
            partition_strategy,
            max_open_partitions,
            list_outputs,
            list_outputs_file,
            create_dirs,
            overwrite,
            formats,
            storage,
            service_inputs,
            service_outputs,
            input_schemes,
            output_schemes,
        }
    }
}

#[derive(Args)]
struct InspectSchema {}

/// One format-specific inspection with its bound arguments and storage session.
pub struct InspectCommand {
    file: Utf8PathBuf,
    mode: silk_chiffon_core::InspectionMode,
    inspection: silk_chiffon_core::InspectionBinding,
    storage: silk_chiffon_storage::StorageSession,
}

impl InspectCommand {
    fn from_parsed(
        file: Utf8PathBuf,
        mode: silk_chiffon_core::InspectionMode,
        inspection: silk_chiffon_core::InspectionBinding,
        storage: silk_chiffon_storage::StorageSession,
    ) -> Self {
        Self {
            file,
            mode,
            inspection,
            storage,
        }
    }

    /// Returns the storage session created for this command invocation.
    pub fn storage(&self) -> &silk_chiffon_storage::StorageSession {
        &self.storage
    }

    /// Returns the selected format's inspection function and parsed settings.
    pub fn inspection(&self) -> &silk_chiffon_core::InspectionBinding {
        &self.inspection
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Utf8PathBuf,
        silk_chiffon_core::InspectionMode,
        silk_chiffon_core::InspectionBinding,
        silk_chiffon_storage::StorageSession,
    ) {
        (self.file, self.mode, self.inspection, self.storage)
    }
}

#[derive(Args, Clone, Debug)]
struct InspectionArgs {
    /// Path to the file to inspect
    #[arg(value_hint = ValueHint::FilePath)]
    file: Utf8PathBuf,
    /// Output format (auto-detects based on TTY if not specified)
    #[arg(long, short = 'f', value_enum, default_value = "auto")]
    format: OutputFormat,
}

#[derive(Args, Clone, Debug)]
/// Arguments for content-based format detection.
pub struct DetectArgs {
    /// Path to the input whose format should be detected
    #[arg(value_hint = ValueHint::FilePath)]
    pub file: Utf8PathBuf,
    /// Output format (auto-detects based on TTY if not specified)
    #[arg(long, short = 'f', value_enum, default_value = "auto")]
    pub format: OutputFormat,
}

/// A detection request with the immutable format registry and command storage session.
pub struct DetectCommand {
    args: DetectArgs,
    storage: silk_chiffon_storage::StorageSession,
    formats: silk_chiffon_core::FormatRegistry,
}

impl DetectCommand {
    fn from_parsed(
        args: DetectArgs,
        storage: silk_chiffon_storage::StorageSession,
        formats: silk_chiffon_core::FormatRegistry,
    ) -> Self {
        Self {
            args,
            storage,
            formats,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        DetectArgs,
        silk_chiffon_storage::StorageSession,
        silk_chiffon_core::FormatRegistry,
    ) {
        (self.args, self.storage, self.formats)
    }
}

#[derive(Args, Clone, Debug)]
pub struct InspectVortexArgs {
    /// Show full schema details
    #[arg(long)]
    pub schema: bool,
    /// Show per-column statistics
    #[arg(long)]
    pub stats: bool,
    /// Show layout structure
    #[arg(long)]
    pub layout: bool,
}

/// Output format for inspect commands
#[derive(ValueEnum, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[value(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Auto-detect: JSON if stdout is not a TTY, otherwise text
    #[default]
    Auto,
    /// Human-readable text output
    Text,
    /// JSON output
    Json,
}

impl OutputFormat {
    pub fn resolves_to_json(&self) -> bool {
        match self {
            OutputFormat::Auto => !io::stdout().is_terminal(),
            OutputFormat::Text => false,
            OutputFormat::Json => true,
        }
    }

    pub fn resolves_to_text(&self) -> bool {
        match self {
            OutputFormat::Auto => io::stdout().is_terminal(),
            OutputFormat::Text => true,
            OutputFormat::Json => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_enum_from_str() {
        assert_eq!(
            QueryDialect::from_str("duckdb", true),
            Ok(QueryDialect::DuckDb)
        );
        assert_eq!(
            QueryDialect::from_str("generic", true),
            Ok(QueryDialect::Generic)
        );
        assert_eq!(
            QueryDialect::from_str("mysql", true),
            Ok(QueryDialect::MySQL)
        );
        assert_eq!(QueryDialect::from_str("hive", true), Ok(QueryDialect::Hive));
        assert_eq!(
            QueryDialect::from_str("sqlite", true),
            Ok(QueryDialect::SQLite)
        );
    }

    mod thread_budget_spec_tests {
        use super::*;

        #[test]
        fn test_fixed() {
            let spec = ThreadBudgetSpec::from_str("8").unwrap();
            assert!(matches!(spec, ThreadBudgetSpec::Fixed(8)));
            assert_eq!(spec.resolve(), 8);
        }

        #[test]
        fn test_reserve() {
            let spec = ThreadBudgetSpec::from_str("reserve:2").unwrap();
            assert!(matches!(
                spec,
                ThreadBudgetSpec::Reserve { reserve: 2, min: 1 }
            ));
        }

        #[test]
        fn test_reserve_with_min() {
            let spec = ThreadBudgetSpec::from_str("reserve:2:min:4").unwrap();
            assert!(matches!(
                spec,
                ThreadBudgetSpec::Reserve { reserve: 2, min: 4 }
            ));
        }

        #[test]
        fn test_reserve_resolve_respects_min() {
            let spec = ThreadBudgetSpec::Reserve {
                reserve: 1000,
                min: 2,
            };
            assert_eq!(spec.resolve(), 2);
        }

        #[test]
        fn test_zero_rejected() {
            assert!(ThreadBudgetSpec::from_str("0").is_err());
        }

        #[test]
        fn test_min_zero_rejected() {
            assert!(ThreadBudgetSpec::from_str("reserve:2:min:0").is_err());
        }

        #[test]
        fn test_invalid_format_rejected() {
            assert!(ThreadBudgetSpec::from_str("reserve").is_err());
            assert!(ThreadBudgetSpec::from_str("garbage:2").is_err());
            assert!(ThreadBudgetSpec::from_str("reserve:abc").is_err());
        }
    }

    mod pool_reserve_spec_tests {
        use super::*;

        #[test]
        fn parse_bare_number_as_percentage() {
            let spec: PoolReserveSpec = "10".parse().unwrap();
            assert!(matches!(spec, PoolReserveSpec::Percent(10)));
        }

        #[test]
        fn parse_percentage_with_suffix() {
            let spec: PoolReserveSpec = "25%".parse().unwrap();
            assert!(matches!(spec, PoolReserveSpec::Percent(25)));
        }

        #[test]
        fn parse_fixed_byte_size() {
            let spec: PoolReserveSpec = "200MB".parse().unwrap();
            assert!(matches!(spec, PoolReserveSpec::Fixed(200_000_000)));
        }

        #[test]
        fn parse_fixed_byte_size_binary() {
            let spec: PoolReserveSpec = "1GiB".parse().unwrap();
            assert!(matches!(spec, PoolReserveSpec::Fixed(1_073_741_824)));
        }

        #[test]
        fn reject_zero_percentage() {
            assert!("0".parse::<PoolReserveSpec>().is_err());
            assert!("0%".parse::<PoolReserveSpec>().is_err());
        }

        #[test]
        fn reject_100_percent() {
            assert!("100".parse::<PoolReserveSpec>().is_err());
            assert!("100%".parse::<PoolReserveSpec>().is_err());
        }

        #[test]
        fn reject_empty_string() {
            assert!("".parse::<PoolReserveSpec>().is_err());
        }

        #[test]
        fn resolve_percentage() {
            let spec = PoolReserveSpec::Percent(10);
            assert_eq!(spec.resolve(1000).unwrap(), 100);
        }

        #[test]
        fn resolve_fixed_errors_when_exceeds_pool() {
            let spec = PoolReserveSpec::Fixed(8_000_000_000);
            let err = spec.resolve(4_000_000_000).unwrap_err();
            assert!(err.to_string().contains("exceeds pool size"));
        }

        #[test]
        fn resolve_fixed_within_pool() {
            let spec = PoolReserveSpec::Fixed(200_000_000);
            assert_eq!(spec.resolve(1_000_000_000).unwrap(), 200_000_000);
        }

        #[test]
        fn resolve_percentage_errors_when_rounds_to_zero() {
            let spec = PoolReserveSpec::Percent(1);
            let err = spec.resolve(50).unwrap_err();
            assert!(err.to_string().contains("resolves to 0 bytes"));
        }

        #[test]
        fn resolve_errors_on_zero_pool() {
            let spec = PoolReserveSpec::Percent(10);
            assert!(spec.resolve(0).is_err());
        }
    }

    mod memory_budget_spec_tests {
        use super::*;
        use std::str::FromStr;

        #[test]
        fn test_total_default_percent() {
            assert!(matches!(
                "total".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Total { pct: 80, min: None }
            ));
        }

        #[test]
        fn test_total_explicit_percent() {
            assert!(matches!(
                "total:90".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Total { pct: 90, min: None }
            ));
        }

        #[test]
        fn test_total_percent_with_symbol() {
            assert!(matches!(
                "total:90%".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Total { pct: 90, min: None }
            ));
        }

        #[test]
        fn test_total_with_min() {
            assert!(matches!(
                "total:80:min:4GB".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Total { pct: 80, min: Some(n) } if n == 4_000_000_000
            ));
        }

        #[test]
        fn test_available_default_percent() {
            assert!(matches!(
                "available".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Available { pct: 80, min: None }
            ));
        }

        #[test]
        fn test_available_explicit_percent() {
            assert!(matches!(
                "available:60".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Available { pct: 60, min: None }
            ));
        }

        #[test]
        fn test_available_percent_with_symbol() {
            assert!(matches!(
                "available:60%".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Available { pct: 60, min: None }
            ));
        }

        #[test]
        fn test_available_with_min() {
            assert!(matches!(
                "available:60:min:2GB".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Available { pct: 60, min: Some(n) } if n == 2_000_000_000
            ));
        }

        #[test]
        fn test_fixed_byte_size() {
            assert!(
                matches!("8GB".parse::<MemoryBudgetSpec>().unwrap(), MemoryBudgetSpec::Fixed(n) if n == 8_000_000_000)
            );
        }

        #[test]
        fn test_reserve_byte_size() {
            assert!(matches!(
                "reserve:2GB".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Reserve { reserve, min: None } if reserve == 2_000_000_000
            ));
        }

        #[test]
        fn test_reserve_with_min() {
            assert!(matches!(
                "reserve:2GB:min:1GB".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Reserve { reserve, min: Some(m) } if reserve == 2_000_000_000 && m == 1_000_000_000
            ));
        }

        #[test]
        fn test_reserve_requires_value() {
            assert!(MemoryBudgetSpec::from_str("reserve").is_err());
        }

        #[test]
        fn test_reserve_rejects_zero() {
            assert!(MemoryBudgetSpec::from_str("reserve:0").is_err());
        }

        #[test]
        fn test_min_rejects_zero() {
            assert!(MemoryBudgetSpec::from_str("total:80:min:0").is_err());
        }

        #[test]
        fn test_min_requires_value() {
            assert!(MemoryBudgetSpec::from_str("total:80:min").is_err());
        }

        #[test]
        fn test_case_insensitive() {
            assert!(matches!(
                "Total:50".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Total { pct: 50, min: None }
            ));
            assert!(matches!(
                "AVAILABLE".parse::<MemoryBudgetSpec>().unwrap(),
                MemoryBudgetSpec::Available { pct: 80, min: None }
            ));
        }

        #[test]
        fn test_percent_zero_rejected() {
            assert!(MemoryBudgetSpec::from_str("total:0").is_err());
        }

        #[test]
        fn test_percent_over_100_rejected() {
            assert!(MemoryBudgetSpec::from_str("total:101").is_err());
        }

        #[test]
        fn test_unknown_keyword_with_colon_rejected() {
            assert!(MemoryBudgetSpec::from_str("garbage:80").is_err());
        }

        #[test]
        fn test_invalid_percent_rejected() {
            assert!(MemoryBudgetSpec::from_str("total:abc").is_err());
        }

        #[test]
        fn test_zero_bytes_rejected() {
            assert!(MemoryBudgetSpec::from_str("0").is_err());
        }

        #[test]
        fn test_unexpected_segment_rejected() {
            assert!(MemoryBudgetSpec::from_str("total:80:foo:bar").is_err());
        }
    }

    mod cli_validation_tests {
        use super::*;

        #[test]
        fn test_preserve_input_order_conflicts_with_query() {
            let result = Cli::try_parse_from([
                "silk-chiffon",
                "transform",
                "--from",
                "input.parquet",
                "--to",
                "output.parquet",
                "--preserve-input-order",
                "--query",
                "SELECT * FROM data",
            ]);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("preserve-input-order") || err.contains("query"));
        }

        #[test]
        fn test_preserve_input_order_conflicts_with_sort_by() {
            let result = Cli::try_parse_from([
                "silk-chiffon",
                "transform",
                "--from",
                "input.parquet",
                "--to",
                "output.parquet",
                "--preserve-input-order",
                "--sort-by",
                "id",
            ]);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("preserve-input-order") || err.contains("sort-by"));
        }

        #[test]
        fn test_preserve_input_order_conflicts_with_to_many() {
            let result = Cli::try_parse_from([
                "silk-chiffon",
                "transform",
                "--from",
                "input.parquet",
                "--to-many",
                "output_{id}.parquet",
                "--preserve-input-order",
                "--by",
                "id",
            ]);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("preserve-input-order") || err.contains("to-many"));
        }

        #[test]
        fn test_preserve_input_order_conflicts_with_from_pattern() {
            let result = Cli::try_parse_from([
                "silk-chiffon",
                "transform",
                "--from-pattern",
                "*.parquet",
                "--to",
                "output.parquet",
                "--preserve-input-order",
            ]);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("preserve-input-order") || err.contains("from-pattern"));
        }
    }
}
