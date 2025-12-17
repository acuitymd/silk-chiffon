//! Parquet file inspection.

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use parquet::{
    arrow::parquet_to_arrow_schema,
    basic::Compression,
    column::page::Page,
    file::{
        metadata::SortingColumn,
        reader::{FileReader, RowGroupReader, SerializedFileReader},
        statistics::Statistics as ParquetStatistics,
    },
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::inspection::magic::{magic_bytes_match_end, magic_bytes_match_start};

use super::{
    inspectable::{
        Inspectable, format_bytes, format_number, render_metadata_map, render_schema_fields,
        schema_to_json,
    },
    style::{dim, header, label, value},
};

const PARQUET_MAGIC: &[u8] = b"PAR1";

pub struct ParquetInspector {
    schema: SchemaRef,
    row_groups: Vec<RowGroupInfo>,
    num_rows: u64,
    total_compressed_size: u64,
    total_uncompressed_size: u64,
    compression_codecs: HashSet<String>,
    has_dictionary: bool,
    has_bloom_filters: bool,
    custom_metadata: HashMap<String, String>,
    file_path: PathBuf,
    /// aggregated per-column stats across all row groups
    file_column_stats: Vec<FileColumnStats>,
}

#[derive(Debug, Serialize)]
pub struct RowGroupInfo {
    pub index: usize,
    pub num_rows: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub sorting_columns: Option<Vec<SortingColumnInfo>>,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Serialize, Clone)]
pub struct SortingColumnInfo {
    pub column_idx: i32,
    pub descending: bool,
    pub nulls_first: bool,
}

impl From<&SortingColumn> for SortingColumnInfo {
    fn from(sc: &SortingColumn) -> Self {
        Self {
            column_idx: sc.column_idx,
            descending: sc.descending,
            nulls_first: sc.nulls_first,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub compression: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub has_bloom_filter: bool,
    /// high-level encoding list from column chunk metadata (free)
    pub encodings: Vec<String>,
    pub statistics: Option<ColumnStatistics>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PageEncodings {
    /// encoding used for dictionary page values (if present)
    pub dictionary: Option<String>,
    /// encodings used for data page values
    pub data: Vec<String>,
    /// encoding used for definition levels (v1 pages only)
    pub def_levels: Option<String>,
    /// encoding used for repetition levels (v1 pages only)
    pub rep_levels: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ColumnStatistics {
    pub min: Option<String>,
    pub max: Option<String>,
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
}

/// File-level aggregated statistics for a column (across all row groups).
#[derive(Debug, Serialize, Clone)]
pub struct FileColumnStats {
    pub name: String,
    pub total_null_count: Option<u64>,
    pub total_compressed_size: u64,
    pub total_uncompressed_size: u64,
}

impl ColumnStatistics {
    fn from_parquet(
        stats: &ParquetStatistics,
        logical_type: Option<&parquet::basic::LogicalType>,
    ) -> Self {
        Self {
            min: format_stat_value(stats, logical_type, true),
            max: format_stat_value(stats, logical_type, false),
            null_count: stats.null_count_opt(),
            distinct_count: stats.distinct_count_opt(),
        }
    }
}

fn format_stat_value(
    stats: &ParquetStatistics,
    logical_type: Option<&parquet::basic::LogicalType>,
    is_min: bool,
) -> Option<String> {
    use chrono::NaiveDate;
    use parquet::basic::LogicalType;
    use parquet::file::statistics::Statistics;

    match stats {
        Statistics::Int32(s) => {
            let val = if is_min { *s.min_opt()? } else { *s.max_opt()? };
            if let Some(LogicalType::Date) = logical_type {
                let date = NaiveDate::from_num_days_from_ce_opt(val + 719163)?;
                return Some(date.to_string());
            }
            Some(val.to_string())
        }
        Statistics::Int64(s) => {
            let val = if is_min { *s.min_opt()? } else { *s.max_opt()? };
            if let Some(LogicalType::Timestamp { unit, .. }) = logical_type {
                return Some(format_timestamp_chrono(val, unit));
            }
            Some(val.to_string())
        }
        Statistics::Float(s) => {
            let val = if is_min { s.min_opt()? } else { s.max_opt()? };
            Some(val.to_string())
        }
        Statistics::Double(s) => {
            let val = if is_min { s.min_opt()? } else { s.max_opt()? };
            Some(val.to_string())
        }
        Statistics::ByteArray(s) => {
            let val = if is_min { s.min_opt()? } else { s.max_opt()? };
            Some(format_bytes_as_string_or_hex(val.data()))
        }
        Statistics::FixedLenByteArray(s) => {
            let val = if is_min { s.min_opt()? } else { s.max_opt()? };
            Some(format_bytes_as_string_or_hex(val.data()))
        }
        _ => None,
    }
}

fn format_bytes_as_string_or_hex(bytes: &[u8]) -> String {
    if let Ok(s) = std::str::from_utf8(bytes)
        && s.chars().all(|c| !c.is_control())
    {
        return format!("\"{}\"", s);
    }
    let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    format!("0x{}", hex)
}

fn format_timestamp_chrono(val: i64, unit: &parquet::basic::TimeUnit) -> String {
    use chrono::{DateTime, Utc};
    use parquet::basic::TimeUnit;

    let datetime: Option<DateTime<Utc>> = match unit {
        TimeUnit::MILLIS => DateTime::from_timestamp_millis(val),
        TimeUnit::MICROS => DateTime::from_timestamp_micros(val),
        TimeUnit::NANOS => {
            let secs = val.div_euclid(1_000_000_000);
            // rem_euclid guarantees result in [0, 999_999_999]
            #[allow(clippy::cast_possible_truncation)]
            let nsecs = val.rem_euclid(1_000_000_000) as u32;
            DateTime::from_timestamp(secs, nsecs)
        }
    };
    datetime
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| val.to_string())
}

/// read pages from a column and extract encoding info by page type
fn read_page_encodings(rg_reader: &dyn RowGroupReader, col_idx: usize) -> Option<PageEncodings> {
    let mut page_reader = rg_reader.get_column_page_reader(col_idx).ok()?;
    let mut encodings = PageEncodings::default();
    let mut data_encodings_seen = HashSet::new();

    while let Ok(Some(page)) = page_reader.get_next_page() {
        match page {
            Page::DictionaryPage { encoding, .. } => {
                encodings.dictionary = Some(format!("{encoding:?}"));
            }
            Page::DataPage {
                encoding,
                def_level_encoding,
                rep_level_encoding,
                ..
            } => {
                let enc_str = format!("{encoding:?}");
                if data_encodings_seen.insert(enc_str.clone()) {
                    encodings.data.push(enc_str);
                }
                if encodings.def_levels.is_none() {
                    encodings.def_levels = Some(format!("{def_level_encoding:?}"));
                }
                if encodings.rep_levels.is_none() {
                    encodings.rep_levels = Some(format!("{rep_level_encoding:?}"));
                }
            }
            Page::DataPageV2 { encoding, .. } => {
                let enc_str = format!("{encoding:?}");
                if data_encodings_seen.insert(enc_str.clone()) {
                    encodings.data.push(enc_str);
                }
                // v2 pages always use RLE for def/rep levels
            }
        }
    }

    Some(encodings)
}

impl ParquetInspector {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();
        let file_metadata = metadata.file_metadata();

        let schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )?;

        // parquet metadata uses i64 for sizes/counts; clamp negatives to 0 for safety
        let num_rows = u64::try_from(file_metadata.num_rows()).unwrap_or(0);

        let mut inspector = Self {
            schema: schema.into(),
            row_groups: Vec::new(),
            num_rows,
            total_compressed_size: 0,
            total_uncompressed_size: 0,
            compression_codecs: HashSet::new(),
            has_dictionary: false,
            has_bloom_filters: false,
            custom_metadata: HashMap::new(),
            file_path: path.to_path_buf(),
            file_column_stats: Vec::new(),
        };

        if let Some(kv_meta) = file_metadata.key_value_metadata() {
            for kv in kv_meta {
                if let Some(v) = &kv.value {
                    inspector.custom_metadata.insert(kv.key.clone(), v.clone());
                }
            }
        }

        // track per-column aggregates across row groups
        let num_columns = if metadata.num_row_groups() > 0 {
            metadata.row_group(0).num_columns()
        } else {
            0
        };
        // start with Some(0) - becomes None if ANY row group lacks null count stats
        let mut col_null_counts: Vec<Option<u64>> = vec![Some(0); num_columns];
        let mut col_compressed: Vec<u64> = vec![0; num_columns];
        let mut col_uncompressed: Vec<u64> = vec![0; num_columns];
        let mut col_names: Vec<String> = vec![String::new(); num_columns];

        for rg_idx in 0..metadata.num_row_groups() {
            let rg_meta = metadata.row_group(rg_idx);
            let mut columns = Vec::new();

            for col_idx in 0..rg_meta.num_columns() {
                let col_meta = rg_meta.column(col_idx);
                let has_bloom = col_meta.bloom_filter_offset().is_some();
                let compression = col_meta.compression();

                inspector.has_bloom_filters |= has_bloom;
                let compression_str = format_compression(compression);
                inspector
                    .compression_codecs
                    .insert(compression_str.to_string());

                let compressed_size = u64::try_from(col_meta.compressed_size()).unwrap_or(0);
                let uncompressed_size = u64::try_from(col_meta.uncompressed_size()).unwrap_or(0);
                inspector.total_compressed_size += compressed_size;
                inspector.total_uncompressed_size += uncompressed_size;

                col_compressed[col_idx] += compressed_size;
                col_uncompressed[col_idx] += uncompressed_size;

                // get encodings from column chunk metadata (free, no page reading)
                let encodings: Vec<String> =
                    col_meta.encodings().map(|e| format!("{e:?}")).collect();

                // detect dictionary usage from encodings
                if encodings
                    .iter()
                    .any(|e| e.contains("DICTIONARY") || e == "PLAIN_DICTIONARY")
                {
                    inspector.has_dictionary = true;
                }

                let logical_type = col_meta.column_descr().logical_type_ref();

                // column_path().to_string() wraps names in quotes, use parts instead
                let name = col_meta.column_path().parts().join(".");

                if rg_idx == 0 {
                    col_names[col_idx] = name.clone();
                }

                let stats = col_meta
                    .statistics()
                    .map(|s| ColumnStatistics::from_parquet(s, logical_type));

                // only produce a total if ALL row groups have null count stats
                match (
                    col_null_counts[col_idx],
                    stats.as_ref().and_then(|s| s.null_count),
                ) {
                    (Some(sum), Some(nc)) => col_null_counts[col_idx] = Some(sum + nc),
                    (_, None) => col_null_counts[col_idx] = None,
                    (None, _) => {} // already invalidated
                }

                columns.push(ColumnInfo {
                    name,
                    compression: compression_str.to_string(),
                    compressed_size,
                    uncompressed_size,
                    has_bloom_filter: has_bloom,
                    encodings,
                    statistics: stats,
                });
            }

            inspector.row_groups.push(RowGroupInfo {
                index: rg_idx,
                num_rows: u64::try_from(rg_meta.num_rows()).unwrap_or(0),
                compressed_size: u64::try_from(rg_meta.compressed_size()).unwrap_or(0),
                uncompressed_size: u64::try_from(rg_meta.total_byte_size()).unwrap_or(0),
                sorting_columns: rg_meta
                    .sorting_columns()
                    .map(|cols| cols.iter().map(SortingColumnInfo::from).collect()),
                columns,
            });
        }

        inspector.file_column_stats = (0..num_columns)
            .map(|i| FileColumnStats {
                name: col_names[i].clone(),
                total_null_count: col_null_counts[i],
                total_compressed_size: col_compressed[i],
                total_uncompressed_size: col_uncompressed[i],
            })
            .collect();

        Ok(inspector)
    }

    pub fn render_stats(&self, out: &mut dyn Write) -> Result<()> {
        writeln!(
            out,
            "\n{} {}:",
            header("Column Statistics"),
            dim("(file totals)")
        )?;
        writeln!(out)?;

        for fcs in &self.file_column_stats {
            writeln!(out, "  {}", header(&fcs.name))?;
            writeln!(
                out,
                "    {}: {}",
                label("Compressed"),
                value(format_bytes(fcs.total_compressed_size))
            )?;
            writeln!(
                out,
                "    {}: {}",
                label("Uncompressed"),
                value(format_bytes(fcs.total_uncompressed_size))
            )?;
            if let Some(nc) = fcs.total_null_count {
                writeln!(
                    out,
                    "    {}: {}",
                    label("Null count"),
                    value(format_number(nc))
                )?;
            }
            writeln!(out)?;
        }

        // detailed stats from row group 0 (min/max/distinct are per-row-group)
        if let Some(rg) = self.row_groups.first() {
            let rg_label = if self.row_groups.len() > 1 {
                "(from row group 0 only)"
            } else {
                ""
            };
            writeln!(out, "{} {}:", header("Detailed Statistics"), dim(rg_label))?;
            writeln!(out)?;

            for col in &rg.columns {
                writeln!(out, "  {}", header(&col.name))?;
                writeln!(
                    out,
                    "    {}: {}",
                    label("Compression"),
                    value(&col.compression)
                )?;

                if !col.encodings.is_empty() {
                    writeln!(
                        out,
                        "    {}: {}",
                        label("Encodings"),
                        value(col.encodings.join(", "))
                    )?;
                }

                if col.has_bloom_filter {
                    writeln!(out, "    {}: {}", label("Bloom filter"), value("yes"))?;
                }

                if let Some(stats) = &col.statistics {
                    if let Some(min) = &stats.min {
                        writeln!(out, "    {}: {}", label("Min"), value(min))?;
                    }
                    if let Some(max) = &stats.max {
                        writeln!(out, "    {}: {}", label("Max"), value(max))?;
                    }
                    if let Some(distinct) = stats.distinct_count {
                        writeln!(
                            out,
                            "    {}: {}",
                            label("Distinct"),
                            value(format_number(distinct))
                        )?;
                    }
                }
                writeln!(out)?;
            }
        }

        Ok(())
    }

    pub fn render_row_groups(
        &self,
        out: &mut dyn Write,
        include_stats: bool,
        detailed_encodings: bool,
    ) -> Result<()> {
        writeln!(
            out,
            "\n{} ({}):",
            header("Row Groups"),
            value(self.row_groups.len())
        )?;
        writeln!(out)?;

        let reader = if detailed_encodings {
            let file = File::open(&self.file_path)?;
            Some(SerializedFileReader::new(file)?)
        } else {
            None
        };

        for rg in &self.row_groups {
            writeln!(out, "  {} {}", header("Row Group"), value(rg.index))?;
            writeln!(
                out,
                "    {}: {}",
                label("Rows"),
                value(format_number(rg.num_rows))
            )?;
            writeln!(
                out,
                "    {}: {}",
                label("Compressed"),
                value(format_bytes(rg.compressed_size))
            )?;
            writeln!(
                out,
                "    {}: {}",
                label("Uncompressed"),
                value(format_bytes(rg.uncompressed_size))
            )?;

            if let Some(sorting) = &rg.sorting_columns {
                let sort_strs: Vec<String> = sorting
                    .iter()
                    .map(|s| {
                        let dir = if s.descending { "desc" } else { "asc" };
                        let nulls = if s.nulls_first {
                            "nulls first"
                        } else {
                            "nulls last"
                        };
                        format!("col_{} {} {}", s.column_idx, dir, nulls)
                    })
                    .collect();
                writeln!(
                    out,
                    "    {}: {}",
                    label("Sorting"),
                    value(sort_strs.join(", "))
                )?;
            }

            if include_stats || detailed_encodings {
                let rg_reader = if let Some(ref r) = reader {
                    r.get_row_group(rg.index).ok()
                } else {
                    None
                };

                writeln!(out)?;
                for (col_idx, col) in rg.columns.iter().enumerate() {
                    writeln!(
                        out,
                        "      {} {}",
                        header(&col.name),
                        dim(format!("({})", col.compression))
                    )?;

                    if detailed_encodings
                        && let Some(ref rg_r) = rg_reader
                        && let Some(pe) = read_page_encodings(rg_r.as_ref(), col_idx)
                    {
                        if let Some(dict) = &pe.dictionary {
                            writeln!(
                                out,
                                "        {}: {} {}",
                                label("Dictionary"),
                                value(dict),
                                dim("(values)")
                            )?;
                        }
                        if !pe.data.is_empty() {
                            writeln!(
                                out,
                                "        {}: {}",
                                label("Data pages"),
                                value(pe.data.join(", "))
                            )?;
                        }
                    } else if !col.encodings.is_empty() {
                        writeln!(
                            out,
                            "        {}: {}",
                            label("Encodings"),
                            value(col.encodings.join(", "))
                        )?;
                    }

                    if include_stats && let Some(stats) = &col.statistics {
                        if let Some(min) = &stats.min {
                            writeln!(out, "        {}: {}", label("Min"), value(min))?;
                        }
                        if let Some(max) = &stats.max {
                            writeln!(out, "        {}: {}", label("Max"), value(max))?;
                        }
                        if let Some(null_count) = stats.null_count {
                            writeln!(
                                out,
                                "        {}: {}",
                                label("Nulls"),
                                value(format_number(null_count))
                            )?;
                        }
                    }
                }
            }
            writeln!(out)?;
        }

        Ok(())
    }

    pub fn render_metadata(&self, out: &mut dyn Write) -> Result<()> {
        render_metadata_map(out, "File Metadata", &self.custom_metadata)
    }

    fn compression_summary(&self) -> String {
        let mut codecs: Vec<&str> = self.compression_codecs.iter().map(|s| s.as_str()).collect();
        codecs.sort();
        if codecs.is_empty() {
            "UNCOMPRESSED".to_string()
        } else if codecs.len() == 1 {
            codecs[0].to_string()
        } else {
            codecs.join(", ")
        }
    }
}

fn format_compression(c: Compression) -> &'static str {
    match c {
        Compression::UNCOMPRESSED => "UNCOMPRESSED",
        Compression::SNAPPY => "SNAPPY",
        Compression::GZIP(_) => "GZIP",
        Compression::LZO => "LZO",
        Compression::BROTLI(_) => "BROTLI",
        Compression::LZ4 => "LZ4",
        Compression::ZSTD(_) => "ZSTD",
        Compression::LZ4_RAW => "LZ4_RAW",
    }
}

impl Inspectable for ParquetInspector {
    fn is_format(path: &Path) -> Result<bool> {
        let mut file = File::open(path)?;

        if !magic_bytes_match_start(&mut file, PARQUET_MAGIC)? {
            return Ok(false);
        }

        if !magic_bytes_match_end(&mut file, PARQUET_MAGIC)? {
            return Ok(false);
        }

        Ok(true)
    }

    fn format_name(&self) -> &str {
        "Parquet"
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn row_count(&self) -> Option<u64> {
        Some(self.num_rows)
    }

    fn custom_metadata(&self) -> Option<&HashMap<String, String>> {
        if self.custom_metadata.is_empty() {
            None
        } else {
            Some(&self.custom_metadata)
        }
    }

    fn render_default(&self, out: &mut dyn Write) -> Result<()> {
        writeln!(
            out,
            "{} {}",
            header(self.file_path.display()),
            dim("(Parquet)")
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "{:<14} {}",
            label("Rows:"),
            value(format_number(self.num_rows))
        )?;
        writeln!(
            out,
            "{:<14} {}",
            label("Row groups:"),
            value(self.row_groups.len())
        )?;
        writeln!(
            out,
            "{:<14} {}",
            label("Compressed:"),
            value(format_bytes(self.total_compressed_size))
        )?;
        writeln!(
            out,
            "{:<14} {}",
            label("Uncompressed:"),
            value(format_bytes(self.total_uncompressed_size))
        )?;

        if self.total_compressed_size > 0 {
            #[allow(clippy::cast_precision_loss)]
            let ratio = self.total_uncompressed_size as f64 / self.total_compressed_size as f64;
            writeln!(
                out,
                "{:<14} {}",
                label("Ratio:"),
                value(format!("{:.2}x", ratio))
            )?;
        }

        writeln!(
            out,
            "{:<14} {}",
            label("Compression:"),
            value(self.compression_summary())
        )?;
        writeln!(out)?;
        writeln!(
            out,
            "{} ({}):",
            header("Columns"),
            value(self.schema.fields().len())
        )?;
        render_schema_fields(&self.schema, out)?;

        Ok(())
    }

    fn to_json(&self) -> Value {
        // file_column_stats are Parquet leaf columns which don't map 1:1 to Arrow
        // schema fields for nested types, so we just output the stats without
        // trying to correlate with schema field metadata
        let file_columns: Vec<Value> = self
            .file_column_stats
            .iter()
            .map(|fcs| {
                json!({
                    "name": fcs.name,
                    "total_null_count": fcs.total_null_count,
                    "total_compressed_size": fcs.total_compressed_size,
                    "total_uncompressed_size": fcs.total_uncompressed_size,
                })
            })
            .collect();

        let row_groups_json: Vec<Value> = self
            .row_groups
            .iter()
            .map(|rg| {
                let cols: Vec<Value> = rg
                    .columns
                    .iter()
                    .map(|col| {
                        let stats_json = col.statistics.as_ref().map(|s| {
                            let mut stats = serde_json::Map::new();
                            if let Some(min) = &s.min {
                                stats.insert("min".to_string(), json!(min));
                            }
                            if let Some(max) = &s.max {
                                stats.insert("max".to_string(), json!(max));
                            }
                            if let Some(null_count) = s.null_count {
                                stats.insert("null_count".to_string(), json!(null_count));
                            }
                            if let Some(distinct_count) = s.distinct_count {
                                stats.insert("distinct_count".to_string(), json!(distinct_count));
                            }
                            Value::Object(stats)
                        });

                        json!({
                            "name": col.name,
                            "compression": col.compression,
                            "compressed_size": col.compressed_size,
                            "uncompressed_size": col.uncompressed_size,
                            "has_bloom_filter": col.has_bloom_filter,
                            "encodings": col.encodings,
                            "statistics": stats_json,
                        })
                    })
                    .collect();
                json!({
                    "index": rg.index,
                    "num_rows": rg.num_rows,
                    "compressed_size": rg.compressed_size,
                    "uncompressed_size": rg.uncompressed_size,
                    "sorting_columns": rg.sorting_columns,
                    "columns": cols,
                })
            })
            .collect();

        json!({
            "format": "parquet",
            "file": self.file_path.display().to_string(),
            "rows": self.num_rows,
            "num_row_groups": self.row_groups.len(),
            "compressed_size": self.total_compressed_size,
            "uncompressed_size": self.total_uncompressed_size,
            "compression": self.compression_summary(),
            "has_dictionary": self.has_dictionary,
            "has_bloom_filters": self.has_bloom_filters,
            "schema": schema_to_json(&self.schema),
            "columns": file_columns,
            "row_groups": row_groups_json,
            "metadata": self.custom_metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    fn simple_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_is_format_parquet_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let batch = create_batch(&schema);

        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        assert!(ParquetInspector::is_format(&path).unwrap());
    }

    #[test]
    fn test_open_parquet_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");

        let schema = simple_schema();
        let batch = create_batch(&schema);

        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let inspector = ParquetInspector::open(&path).unwrap();
        assert_eq!(inspector.row_count(), Some(3));
        assert_eq!(inspector.format_name(), "Parquet");
    }

    #[test]
    fn test_is_format_non_parquet_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.txt");
        std::fs::write(&path, "not a parquet file").unwrap();

        assert!(!ParquetInspector::is_format(&path).unwrap());
    }

    #[test]
    fn test_is_format_partial_magic_bytes() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.parquet");
        // only start magic, no end magic
        std::fs::write(&path, b"PAR1garbage").unwrap();

        assert!(!ParquetInspector::is_format(&path).unwrap());
    }

    #[test]
    fn test_is_format_nonexistent_file() {
        let path = Path::new("/nonexistent/path/file.parquet");
        let result = ParquetInspector::is_format(path);
        assert!(result.is_err());
    }
}
