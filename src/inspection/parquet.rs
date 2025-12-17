//! Parquet file inspection.

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::Write,
    path::Path,
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

use super::{
    inspectable::{
        Inspectable, format_bytes, format_number, render_metadata_map, render_schema_fields,
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
    file_path: String,
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
    pub statistics: Option<ColumnStatistics>,
    pub page_encodings: Option<PageEncodings>,
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
    // hex format
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
            file_path: path.display().to_string(),
            file_column_stats: Vec::new(),
        };

        if let Some(kv_meta) = file_metadata.key_value_metadata() {
            for kv in kv_meta {
                if let Some(v) = &kv.value {
                    inspector.custom_metadata.insert(kv.key.clone(), v.clone());
                }
            }
        }

        // get page reader for first row group to extract page-level encoding info
        let first_rg_reader = if metadata.num_row_groups() > 0 {
            reader.get_row_group(0).ok()
        } else {
            None
        };

        // track per-column aggregates across row groups
        let num_columns = if metadata.num_row_groups() > 0 {
            metadata.row_group(0).num_columns()
        } else {
            0
        };
        let mut col_null_counts: Vec<Option<u64>> = vec![None; num_columns];
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

                // aggregate per-column stats
                col_compressed[col_idx] += compressed_size;
                col_uncompressed[col_idx] += uncompressed_size;

                // only read page encodings for first row group (expensive for large files)
                let page_encodings = if rg_idx == 0 {
                    first_rg_reader
                        .as_ref()
                        .and_then(|rg| read_page_encodings(rg.as_ref(), col_idx))
                } else {
                    None
                };

                if let Some(ref pe) = page_encodings
                    && pe.dictionary.is_some()
                {
                    inspector.has_dictionary = true;
                }

                let logical_type = col_meta.column_descr().logical_type_ref();

                // column_path().to_string() wraps names in quotes, use parts instead
                let name = col_meta.column_path().parts().join(".");

                // store name on first row group
                if rg_idx == 0 {
                    col_names[col_idx] = name.clone();
                }

                let stats = col_meta
                    .statistics()
                    .map(|s| ColumnStatistics::from_parquet(s, logical_type));

                // aggregate null counts
                if let Some(ref s) = stats
                    && let Some(nc) = s.null_count
                {
                    col_null_counts[col_idx] = Some(col_null_counts[col_idx].unwrap_or(0) + nc);
                }

                columns.push(ColumnInfo {
                    name,
                    compression: compression_str.to_string(),
                    compressed_size,
                    uncompressed_size,
                    has_bloom_filter: has_bloom,
                    statistics: stats,
                    page_encodings,
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

        // build file-level column stats
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
        // file-level summary (aggregated across all row groups)
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

                if let Some(pe) = &col.page_encodings {
                    if let Some(dict) = &pe.dictionary {
                        writeln!(
                            out,
                            "    {}: {} {}",
                            label("Dictionary"),
                            value(dict),
                            dim("(values)")
                        )?;
                    }
                    if !pe.data.is_empty() {
                        writeln!(
                            out,
                            "    {}: {}",
                            label("Data pages"),
                            value(pe.data.join(", "))
                        )?;
                    }
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

    pub fn render_row_groups(&self, out: &mut dyn Write, include_stats: bool) -> Result<()> {
        writeln!(
            out,
            "\n{} ({}):",
            header("Row Groups"),
            value(self.row_groups.len())
        )?;
        writeln!(out)?;

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

            if include_stats {
                writeln!(out)?;
                for col in &rg.columns {
                    writeln!(
                        out,
                        "      {} {}",
                        header(&col.name),
                        dim(format!("({})", col.compression))
                    )?;
                    if let Some(stats) = &col.statistics {
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
    fn try_open(path: &Path) -> Result<Option<Self>> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Ok(None),
        };

        use std::io::{Read, Seek, SeekFrom};

        let mut magic = [0u8; 4];
        if file.read_exact(&mut magic).is_err() {
            return Ok(None);
        }
        if magic != PARQUET_MAGIC {
            return Ok(None);
        }

        if file.seek(SeekFrom::End(-4)).is_err() {
            return Ok(None);
        }
        let mut footer_magic = [0u8; 4];
        if file.read_exact(&mut footer_magic).is_err() {
            return Ok(None);
        }
        if footer_magic != PARQUET_MAGIC {
            return Ok(None);
        }

        Self::open(path).map(Some)
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
        writeln!(out, "{} {}", header(&self.file_path), dim("(Parquet)"))?;
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
        // file-level aggregated stats per column
        let file_columns: Vec<Value> = self
            .file_column_stats
            .iter()
            .enumerate()
            .map(|(i, fcs)| {
                let field = self.schema.field(i);
                json!({
                    "name": fcs.name,
                    "data_type": format!("{}", field.data_type()),
                    "nullable": field.is_nullable(),
                    "total_null_count": fcs.total_null_count,
                    "total_compressed_size": fcs.total_compressed_size,
                    "total_uncompressed_size": fcs.total_uncompressed_size,
                })
            })
            .collect();

        // detailed per-row-group info
        let row_groups_json: Vec<Value> = self
            .row_groups
            .iter()
            .map(|rg| {
                let cols: Vec<Value> = rg
                    .columns
                    .iter()
                    .map(|col| {
                        json!({
                            "name": col.name,
                            "compression": col.compression,
                            "compressed_size": col.compressed_size,
                            "uncompressed_size": col.uncompressed_size,
                            "has_bloom_filter": col.has_bloom_filter,
                            "statistics": col.statistics.as_ref().map(|s| json!({
                                "min": s.min,
                                "max": s.max,
                                "null_count": s.null_count,
                                "distinct_count": s.distinct_count,
                            })),
                            "encodings": col.page_encodings.as_ref().map(|pe| json!({
                                "dictionary": pe.dictionary,
                                "data": pe.data,
                            })),
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
            "file": self.file_path,
            "rows": self.num_rows,
            "num_row_groups": self.row_groups.len(),
            "compressed_size": self.total_compressed_size,
            "uncompressed_size": self.total_uncompressed_size,
            "compression": self.compression_summary(),
            "has_dictionary": self.has_dictionary,
            "has_bloom_filters": self.has_bloom_filters,
            "columns": file_columns,
            "row_groups": row_groups_json,
            "metadata": self.custom_metadata,
        })
    }
}
