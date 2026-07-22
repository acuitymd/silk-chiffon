//! Parquet file inspection.

use std::{
    collections::{HashMap, HashSet},
    io::Write,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use arrow::datatypes::SchemaRef;
use bytes::{Buf, Bytes};
use chrono::{DateTime, NaiveDate, Utc};
use futures::{StreamExt, stream};
use num_format::{Locale, ToFormattedString};
use object_store::ObjectStoreExt;
use owo_colors::OwoColorize;
use parquet::{
    arrow::parquet_to_arrow_schema,
    basic::{Compression, ConvertedType, LogicalType, TimeUnit},
    column::page::{Page, PageReader},
    errors::ParquetError,
    file::{
        metadata::{ColumnChunkMetaData, ParquetMetaData, SortingColumn},
        reader::{ChunkReader, Length},
        serialized_reader::SerializedPageReader,
        statistics::Statistics,
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use tabled::{
    Table, Tabled,
    settings::{
        Alignment, Color, Modify, Remove, Style,
        object::{Columns, Rows},
    },
};

use crate::{
    inspection::readers::read_parquet_metadata,
    inspection::{
        inspectable::{
            Inspectable, format_bytes, format_number, render_metadata_map, render_schema_fields,
            schema_to_json, truncate_chars,
        },
        style::{
            apply_theme, boolean_display, column_name, compression, dim, encoding, header, label,
            missing_value, true_or_missing_display, value,
        },
    },
    storage::InputObject,
};

const PAGE_FETCH_CONCURRENCY: usize = 16;

pub struct ParquetInspector {
    input: InputObject,
    metadata: Arc<ParquetMetaData>,
    schema: SchemaRef,
    row_groups: Vec<RowGroupInfo>,
    num_rows: u64,
    num_columns: usize,
    file_size: u64,
    total_compressed_size: u64,
    total_uncompressed_size: u64,
    total_bloom_filter_size: u64,
    compression_codecs: HashSet<String>,
    has_dictionary: bool,
    has_bloom_filters: bool,
    has_page_index: bool,
    custom_metadata: HashMap<String, String>,
    file_path: String,
    file_column_stats: Vec<FileColumnStats>,
    format_version: String,
    created_by: Option<String>,
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
    pub has_dictionary: bool,
    pub has_bloom_filter: bool,
    pub has_page_index: bool,
    pub has_statistics: bool,
    /// high-level encoding list from column chunk metadata (free)
    pub encodings: Vec<String>,
    /// detailed page-level encodings (requires reading pages)
    pub page_encodings: Option<PageEncodings>,
    pub statistics: Option<ColumnStatistics>,
    #[serde(skip)]
    pages: Option<Vec<PageInfo>>,
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

#[derive(Debug, Clone)]
struct PageInfo {
    index: usize,
    page_type: &'static str,
    encoding: String,
    num_values: u64,
    size: u64,
    rows: Option<u64>,
    nulls: Option<u64>,
    def_encoding: Option<String>,
    rep_encoding: Option<String>,
    def_levels_byte_len: Option<u64>,
    rep_levels_byte_len: Option<u64>,
    is_sorted: Option<bool>,
    is_compressed: Option<bool>,
    has_statistics: bool,
}

impl PageInfo {
    fn to_json(&self) -> Value {
        match self.page_type {
            "Dict" => json!({
                "index": self.index,
                "type": self.page_type,
                "encoding": self.encoding,
                "num_values": self.num_values,
                "size": self.size,
                "is_sorted": self.is_sorted,
            }),
            "Data" => json!({
                "index": self.index,
                "type": self.page_type,
                "encoding": self.encoding,
                "num_values": self.num_values,
                "size": self.size,
                "def_level_encoding": self.def_encoding,
                "rep_level_encoding": self.rep_encoding,
                "has_statistics": self.has_statistics,
            }),
            "DataV2" => json!({
                "index": self.index,
                "type": self.page_type,
                "encoding": self.encoding,
                "num_values": self.num_values,
                "size": self.size,
                "num_rows": self.rows,
                "num_nulls": self.nulls,
                "def_levels_byte_len": self.def_levels_byte_len,
                "rep_levels_byte_len": self.rep_levels_byte_len,
                "is_compressed": self.is_compressed,
                "has_statistics": self.has_statistics,
            }),
            _ => unreachable!("page type is constructed internally"),
        }
    }
}

#[derive(Clone)]
struct OffsetChunkReader {
    bytes: Bytes,
    base: u64,
    file_len: u64,
}

impl Length for OffsetChunkReader {
    fn len(&self) -> u64 {
        self.file_len
    }
}

impl OffsetChunkReader {
    fn relative_offset(&self, start: u64) -> parquet::errors::Result<usize> {
        let relative = start.checked_sub(self.base).ok_or_else(|| {
            ParquetError::EOF(format!(
                "requested offset {start} precedes fetched range starting at {}",
                self.base
            ))
        })?;
        let relative = usize::try_from(relative)
            .map_err(|_| ParquetError::EOF(format!("requested offset {start} is too large")))?;
        if relative > self.bytes.len() {
            return Err(ParquetError::EOF(format!(
                "requested offset {start} exceeds fetched range ending at {}",
                self.base + self.bytes.len() as u64
            )));
        }
        Ok(relative)
    }
}

impl ChunkReader for OffsetChunkReader {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let offset = self.relative_offset(start)?;
        Ok(self.bytes.slice(offset..).reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let offset = self.relative_offset(start)?;
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| {
                ParquetError::EOF(format!(
                    "requested {length} bytes at offset {start} exceeds the fetched column chunk"
                ))
            })?;
        Ok(self.bytes.slice(offset..end))
    }
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
        stats: &Statistics,
        logical_type: Option<&LogicalType>,
        converted_type: ConvertedType,
    ) -> Self {
        Self {
            min: format_stat_value(stats, logical_type, converted_type, true),
            max: format_stat_value(stats, logical_type, converted_type, false),
            null_count: stats.null_count_opt(),
            distinct_count: stats.distinct_count_opt(),
        }
    }
}

fn format_stat_value(
    stats: &Statistics,
    logical_type: Option<&LogicalType>,
    converted_type: ConvertedType,
    is_min: bool,
) -> Option<String> {
    match stats {
        Statistics::Int32(s) => {
            let val = if is_min { *s.min_opt()? } else { *s.max_opt()? };
            if matches!(logical_type, Some(LogicalType::Date))
                || converted_type == ConvertedType::DATE
            {
                // 719163 = days from 0001-01-01 CE to 1970-01-01 (parquet stores days since CE)
                let date = NaiveDate::from_num_days_from_ce_opt(val + 719163)?;
                return Some(date.to_string());
            }
            Some(val.to_formatted_string(&Locale::en))
        }
        Statistics::Int64(s) => {
            let val = if is_min { *s.min_opt()? } else { *s.max_opt()? };
            if let Some(LogicalType::Timestamp { unit, .. }) = logical_type {
                return Some(format_timestamp_chrono(val, unit));
            }
            // fallback to legacy converted types for timestamps
            match converted_type {
                ConvertedType::TIMESTAMP_MILLIS => {
                    return Some(format_timestamp_chrono(val, &TimeUnit::MILLIS));
                }
                ConvertedType::TIMESTAMP_MICROS => {
                    return Some(format_timestamp_chrono(val, &TimeUnit::MICROS));
                }
                _ => {}
            }
            Some(val.to_formatted_string(&Locale::en))
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

fn format_timestamp_chrono(val: i64, unit: &TimeUnit) -> String {
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

struct ColumnPageInspection {
    encodings: PageEncodings,
    pages: Vec<PageInfo>,
}

fn inspect_column_pages(
    bytes: Bytes,
    base: u64,
    file_len: u64,
    column: &ColumnChunkMetaData,
    row_count: usize,
) -> Result<ColumnPageInspection> {
    let chunk = OffsetChunkReader {
        bytes,
        base,
        file_len,
    };
    let mut page_reader = SerializedPageReader::new(Arc::new(chunk), column, row_count, None)?;
    let mut encodings = PageEncodings::default();
    let mut data_encodings_seen = HashSet::new();
    let mut pages = Vec::new();

    while let Some(page) = page_reader.get_next_page()? {
        let index = pages.len();
        let page = match page {
            Page::DictionaryPage {
                buf,
                num_values,
                encoding,
                is_sorted,
            } => {
                encodings.dictionary = Some(format!("{encoding:?}"));
                PageInfo {
                    index,
                    page_type: "Dict",
                    encoding: format!("{encoding:?}"),
                    num_values: u64::from(num_values),
                    size: buf.len() as u64,
                    rows: None,
                    nulls: None,
                    def_encoding: None,
                    rep_encoding: None,
                    def_levels_byte_len: None,
                    rep_levels_byte_len: None,
                    is_sorted: Some(is_sorted),
                    is_compressed: None,
                    has_statistics: false,
                }
            }
            Page::DataPage {
                buf,
                num_values,
                encoding,
                def_level_encoding,
                rep_level_encoding,
                statistics,
            } => {
                let enc_str = format!("{encoding:?}");
                if data_encodings_seen.insert(enc_str.clone()) {
                    encodings.data.push(enc_str.clone());
                }
                if encodings.def_levels.is_none() {
                    encodings.def_levels = Some(format!("{def_level_encoding:?}"));
                }
                if encodings.rep_levels.is_none() {
                    encodings.rep_levels = Some(format!("{rep_level_encoding:?}"));
                }
                PageInfo {
                    index,
                    page_type: "Data",
                    encoding: enc_str,
                    num_values: u64::from(num_values),
                    size: buf.len() as u64,
                    rows: None,
                    nulls: None,
                    def_encoding: Some(format!("{def_level_encoding:?}")),
                    rep_encoding: Some(format!("{rep_level_encoding:?}")),
                    def_levels_byte_len: None,
                    rep_levels_byte_len: None,
                    is_sorted: None,
                    is_compressed: None,
                    has_statistics: statistics.is_some(),
                }
            }
            Page::DataPageV2 {
                buf,
                num_values,
                encoding,
                num_nulls,
                num_rows,
                def_levels_byte_len,
                rep_levels_byte_len,
                is_compressed,
                statistics,
            } => {
                let enc_str = format!("{encoding:?}");
                if data_encodings_seen.insert(enc_str.clone()) {
                    encodings.data.push(enc_str.clone());
                }
                PageInfo {
                    index,
                    page_type: "DataV2",
                    encoding: enc_str,
                    num_values: u64::from(num_values),
                    size: buf.len() as u64,
                    rows: Some(u64::from(num_rows)),
                    nulls: Some(u64::from(num_nulls)),
                    def_encoding: None,
                    rep_encoding: None,
                    def_levels_byte_len: Some(u64::from(def_levels_byte_len)),
                    rep_levels_byte_len: Some(u64::from(rep_levels_byte_len)),
                    is_sorted: None,
                    is_compressed: Some(is_compressed),
                    has_statistics: statistics.is_some(),
                }
            }
        };
        pages.push(page);
    }

    Ok(ColumnPageInspection { encodings, pages })
}

impl ParquetInspector {
    pub async fn open(input: &InputObject) -> Result<Self> {
        let metadata = read_parquet_metadata(input).await.with_context(|| {
            format!(
                "could not read Parquet metadata from '{}'",
                input.location().display()
            )
        })?;
        let file_metadata = metadata.file_metadata();

        let schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )?;

        // parquet metadata uses i64 for sizes/counts; clamp negatives to 0 for safety
        let num_rows = u64::try_from(file_metadata.num_rows()).unwrap_or(0);

        let format_version = format!("{:?}", file_metadata.version());
        let created_by = file_metadata.created_by().map(String::from);

        let mut inspector = Self {
            input: input.clone(),
            metadata: Arc::clone(&metadata),
            schema: schema.into(),
            row_groups: Vec::new(),
            num_rows,
            num_columns: 0,
            file_size: input.metadata().size,
            total_compressed_size: 0,
            total_uncompressed_size: 0,
            total_bloom_filter_size: 0,
            compression_codecs: HashSet::new(),
            has_dictionary: false,
            has_bloom_filters: false,
            has_page_index: false,
            custom_metadata: HashMap::new(),
            file_path: input.location().display().to_string(),
            file_column_stats: Vec::new(),
            format_version,
            created_by,
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
                let bloom_filter_size =
                    u64::from(col_meta.bloom_filter_length().unwrap_or(0).cast_unsigned());
                let compression = col_meta.compression();

                inspector.has_bloom_filters |= has_bloom;
                inspector.total_bloom_filter_size += bloom_filter_size;
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
                let has_dict = encodings
                    .iter()
                    .any(|e| e.contains("DICTIONARY") || e == "PLAIN_DICTIONARY");
                if has_dict {
                    inspector.has_dictionary = true;
                }

                let has_page_idx = col_meta.column_index_offset().is_some()
                    || col_meta.offset_index_offset().is_some();
                if has_page_idx {
                    inspector.has_page_index = true;
                }

                let col_descr = col_meta.column_descr();
                let logical_type = col_descr.logical_type_ref();
                let converted_type = col_descr.converted_type();

                // column_path().to_string() wraps names in quotes, use parts instead
                let name = col_meta.column_path().parts().join(".");

                if rg_idx == 0 {
                    col_names[col_idx] = name.clone();
                }

                let stats = col_meta
                    .statistics()
                    .map(|s| ColumnStatistics::from_parquet(s, logical_type, converted_type));

                let has_stats = stats.is_some();

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
                    has_dictionary: has_dict,
                    has_bloom_filter: has_bloom,
                    has_page_index: has_page_idx,
                    has_statistics: has_stats,
                    encodings,
                    page_encodings: None,
                    statistics: stats,
                    pages: None,
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

        inspector.num_columns = num_columns;

        Ok(inspector)
    }

    pub async fn load_pages(
        &mut self,
        row_group: Option<usize>,
        columns: Option<&[&str]>,
    ) -> Result<()> {
        let requested = self.resolve_columns(row_group, columns)?;
        let row_groups = self.selected_row_groups(row_group)?;
        let mut plans = Vec::new();
        for row_group_index in row_groups {
            let metadata = self.metadata.row_group(row_group_index);
            let row_count = usize::try_from(metadata.num_rows()).map_err(|_| {
                anyhow!(
                    "row count for row group {row_group_index} in '{}' exceeds this platform's limit",
                    self.file_path
                )
            })?;
            for (column_index, column) in metadata.columns().iter().enumerate() {
                let name = column.column_path().parts().join(".");
                if requested
                    .as_ref()
                    .is_some_and(|names| !names.contains(&name))
                {
                    continue;
                }
                let (start, length) = column.byte_range();
                let end = start.checked_add(length).ok_or_else(|| {
                    anyhow!(
                        "column chunk range overflows for row group {row_group_index}, column {column_index} in '{}'",
                        self.file_path
                    )
                })?;
                plans.push((row_group_index, column_index, row_count, start..end));
            }
        }

        let store = Arc::clone(self.input.location().store().object_store());
        let path = self.input.location().path().clone();
        let metadata = Arc::clone(&self.metadata);
        let file_len = self.file_size;
        let display = self.file_path.clone();
        let mut fetched = stream::iter(plans)
            .map(move |(row_group_index, column_index, row_count, range)| {
                let store = Arc::clone(&store);
                let path = path.clone();
                let metadata = Arc::clone(&metadata);
                let display = display.clone();
                async move {
                    let base = range.start;
                    let bytes = store.get_range(&path, range).await.with_context(|| {
                        format!(
                            "could not read row group {row_group_index}, column {column_index} from '{display}'"
                        )
                    })?;
                    let column = metadata.row_group(row_group_index).column(column_index);
                    let pages = inspect_column_pages(bytes, base, file_len, column, row_count)
                        .with_context(|| {
                            format!(
                                "could not parse pages for row group {row_group_index}, column {column_index} in '{display}'"
                            )
                        })?;
                    Ok::<_, anyhow::Error>((row_group_index, column_index, pages))
                }
            })
            .buffer_unordered(PAGE_FETCH_CONCURRENCY);

        while let Some(result) = fetched.next().await {
            let (row_group_index, column_index, pages) = result?;
            let column = &mut self.row_groups[row_group_index].columns[column_index];
            column.page_encodings = Some(pages.encodings);
            column.pages = Some(pages.pages);
        }

        Ok(())
    }

    pub(crate) fn validate_columns(
        &self,
        row_group: Option<usize>,
        columns: &[&str],
    ) -> Result<()> {
        self.resolve_columns(row_group, Some(columns))?;
        Ok(())
    }

    fn selected_row_groups(&self, row_group: Option<usize>) -> Result<Vec<usize>> {
        match row_group {
            Some(index) if index >= self.row_groups.len() => bail!(
                "row group {index} does not exist in '{}' (file has {} row groups)",
                self.file_path,
                self.row_groups.len()
            ),
            Some(index) => Ok(vec![index]),
            None => Ok((0..self.row_groups.len()).collect()),
        }
    }

    fn resolve_columns(
        &self,
        row_group: Option<usize>,
        columns: Option<&[&str]>,
    ) -> Result<Option<HashSet<String>>> {
        let Some(columns) = columns else {
            self.selected_row_groups(row_group)?;
            return Ok(None);
        };
        let row_groups = self.selected_row_groups(row_group)?;
        let available = row_groups
            .iter()
            .flat_map(|index| self.row_groups[*index].columns.iter())
            .map(|column| column.name.clone())
            .collect::<HashSet<_>>();
        let mut resolved = HashSet::new();
        for requested in columns {
            if available.contains(*requested) {
                resolved.insert((*requested).to_string());
                continue;
            }
            let mut leaf_matches = available
                .iter()
                .filter(|name| name.rsplit('.').next() == Some(*requested))
                .cloned()
                .collect::<Vec<_>>();
            leaf_matches.sort_unstable();
            match leaf_matches.as_slice() {
                [name] => {
                    resolved.insert(name.clone());
                }
                [] => bail!("column {requested} not found in '{}'", self.file_path),
                names => bail!(
                    "column name {requested} is ambiguous in '{}'; use one of {}",
                    self.file_path,
                    names.join(", ")
                ),
            }
        }
        Ok(Some(resolved))
    }

    pub fn row_groups(&self) -> &[RowGroupInfo] {
        &self.row_groups
    }

    pub fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.row_groups
            .first()?
            .columns
            .iter()
            .find(|c| c.name == name)
    }

    pub fn column_in_row_group(&self, row_group: usize, name: &str) -> Option<&ColumnInfo> {
        self.row_groups
            .get(row_group)?
            .columns
            .iter()
            .find(|c| c.name == name)
    }

    /// Calculate the metadata size (file_size - data - bloom filters).
    fn metadata_size(&self) -> u64 {
        self.file_size
            .saturating_sub(self.total_compressed_size)
            .saturating_sub(self.total_bloom_filter_size)
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
                writeln!(out)?;
                for col in &rg.columns {
                    writeln!(
                        out,
                        "      {} {}",
                        header(&col.name),
                        dim(format!("({})", col.compression))
                    )?;

                    if detailed_encodings && let Some(pe) = &col.page_encodings {
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

    pub fn render_with_row_group(&self, out: &mut dyn Write, row_group_idx: usize) -> Result<()> {
        fn format_encodings(col: &ColumnInfo) -> String {
            if let Some(ref pe) = col.page_encodings {
                let mut parts = Vec::new();
                if let Some(ref dict) = pe.dictionary {
                    parts.push(format!("{} {}", dim("Dict:"), encoding(dict)));
                }
                if !pe.data.is_empty() {
                    let encoded: Vec<_> = pe.data.iter().map(|e| encoding(e)).collect();
                    parts.push(format!("{} {}", dim("Data:"), encoded.join(", ")));
                }
                if !parts.is_empty() {
                    return parts.join(", ");
                }
            }
            col.encodings
                .iter()
                .map(|e| encoding(e))
                .collect::<Vec<_>>()
                .join(", ")
        }

        writeln!(out, "{}", header(&self.file_path))?;
        writeln!(out)?;

        let is_uncompressed = self.compression_codecs.iter().all(|c| c == "UNCOMPRESSED");

        #[derive(Tabled)]
        struct InfoRow {
            #[tabled(rename = "")]
            label: String,
            #[tabled(rename = "")]
            value: String,
        }

        let version_display = format!("{}.0", self.format_version);
        let metadata_size = self.metadata_size();

        let mut info_rows = vec![
            InfoRow {
                label: "Format".to_string(),
                value: format!("Parquet {}", version_display),
            },
            InfoRow {
                label: "Row groups".to_string(),
                value: self.row_groups.len().to_string(),
            },
            InfoRow {
                label: "Rows".to_string(),
                value: format_number(self.num_rows),
            },
            InfoRow {
                label: "Columns".to_string(),
                value: self.num_columns.to_string(),
            },
            InfoRow {
                label: "Uncompressed".to_string(),
                value: format_bytes(self.total_uncompressed_size),
            },
            InfoRow {
                label: "Compressed".to_string(),
                value: if is_uncompressed {
                    missing_value()
                } else {
                    format_bytes(self.total_compressed_size)
                },
            },
            InfoRow {
                label: "File size".to_string(),
                value: format_bytes(self.file_size),
            },
        ];

        // only show bloom filter size if there are bloom filters
        if self.total_bloom_filter_size > 0 {
            info_rows.push(InfoRow {
                label: "Bloom filters".to_string(),
                value: format_bytes(self.total_bloom_filter_size),
            });
        }

        if metadata_size > 1024 {
            info_rows.push(InfoRow {
                label: "Metadata".to_string(),
                value: format_bytes(metadata_size),
            });
        }

        if let Some(ref created_by) = self.created_by {
            info_rows.insert(
                1,
                InfoRow {
                    label: "Created by".to_string(),
                    value: created_by.clone(),
                },
            );
        }

        let info_table = Table::new(&info_rows)
            .with(Remove::row(Rows::first()))
            .with(Style::rounded().remove_horizontals())
            .with(Modify::new(Columns::new(0..1)).with(Alignment::right()))
            .with(
                Modify::new(Columns::new(1..))
                    .with(Alignment::left())
                    .with(Color::BOLD),
            )
            .to_string();
        writeln!(out, "{info_table}")?;

        writeln!(out)?;
        writeln!(out, "{}", header("Schema"))?;
        render_schema_fields(&self.schema, out)?;

        writeln!(out)?;
        writeln!(out, "{}", header("Row Groups"))?;
        #[derive(Tabled)]
        struct RowGroupRow {
            #[tabled(rename = "RG")]
            index: usize,
            #[tabled(rename = "Rows")]
            rows: String,
            #[tabled(rename = "Uncompressed")]
            uncompressed: String,
            #[tabled(rename = "Compressed")]
            compressed: String,
        }
        let rg_rows: Vec<RowGroupRow> = self
            .row_groups
            .iter()
            .map(|rg| RowGroupRow {
                index: rg.index,
                rows: format_number(rg.num_rows),
                uncompressed: format_bytes(rg.uncompressed_size),
                compressed: format_bytes(rg.compressed_size),
            })
            .collect();
        let mut rg_table = Table::new(&rg_rows);
        apply_theme(&mut rg_table);
        let rg_table = rg_table
            .with(Modify::new(Columns::new(1..)).with(Alignment::right()))
            .to_string();
        writeln!(out, "{rg_table}")?;

        if let Some(rg) = self.row_groups.get(row_group_idx) {
            writeln!(out)?;
            writeln!(
                out,
                "{} {}",
                header("Column Chunks"),
                dim(format!("(row group {})", row_group_idx))
            )?;
            #[derive(Tabled)]
            struct ColChunkRow {
                #[tabled(rename = "Column")]
                name: String,
                #[tabled(rename = "Encoding(s)")]
                encodings: String,
                #[tabled(rename = "Compression")]
                compression: String,
                #[tabled(rename = "Uncompressed")]
                uncompressed: String,
                #[tabled(rename = "Compressed")]
                compressed: String,
                #[tabled(rename = "Dict")]
                dict: String,
                #[tabled(rename = "Stats")]
                stats: String,
                #[tabled(rename = "PageIdx")]
                page_idx: String,
                #[tabled(rename = "Bloom")]
                bloom: String,
            }
            let col_rows: Vec<ColChunkRow> = rg
                .columns
                .iter()
                .map(|col| ColChunkRow {
                    name: column_name(&col.name),
                    encodings: format_encodings(col),
                    compression: compression(&col.compression),
                    uncompressed: format_bytes(col.uncompressed_size),
                    compressed: if col.compression == "UNCOMPRESSED" {
                        missing_value()
                    } else {
                        format_bytes(col.compressed_size)
                    },
                    dict: boolean_display(col.has_dictionary),
                    stats: boolean_display(col.has_statistics),
                    page_idx: boolean_display(col.has_page_index),
                    bloom: boolean_display(col.has_bloom_filter),
                })
                .collect();
            let mut col_table = Table::new(&col_rows);
            apply_theme(&mut col_table);
            let col_table = col_table
                .with(Modify::new(Columns::new(3..=4)).with(Alignment::right()))
                .with(Modify::new(Columns::new(5..)).with(Alignment::center()))
                .to_string();
            writeln!(out, "{col_table}")?;

            let has_any_stats = rg.columns.iter().any(|c| c.statistics.is_some());
            if has_any_stats {
                writeln!(out)?;
                writeln!(
                    out,
                    "{} {}",
                    header("Column Statistics"),
                    dim(format!("(row group {})", row_group_idx))
                )?;
                #[derive(Tabled)]
                struct StatsRow {
                    #[tabled(rename = "Column")]
                    name: String,
                    #[tabled(rename = "Nulls")]
                    nulls: String,
                    #[tabled(rename = "Distinct")]
                    distinct: String,
                    #[tabled(rename = "Min")]
                    min: String,
                    #[tabled(rename = "Max")]
                    max: String,
                }
                let stats_rows: Vec<StatsRow> = rg
                    .columns
                    .iter()
                    .map(|col| {
                        let (nulls, distinct, min, max) = if let Some(s) = &col.statistics {
                            (
                                s.null_count.map_or_else(missing_value, format_number),
                                s.distinct_count.map_or_else(missing_value, format_number),
                                s.min.clone().unwrap_or_else(missing_value),
                                s.max.clone().unwrap_or_else(missing_value),
                            )
                        } else {
                            (
                                missing_value(),
                                missing_value(),
                                missing_value(),
                                missing_value(),
                            )
                        };
                        StatsRow {
                            name: column_name(&col.name),
                            nulls,
                            distinct,
                            min,
                            max,
                        }
                    })
                    .collect();
                let mut stats_table = Table::new(&stats_rows);
                apply_theme(&mut stats_table);
                let stats_table = stats_table
                    .with(Modify::new(Columns::new(1..)).with(Alignment::right()))
                    .to_string();
                writeln!(out, "{stats_table}")?;
            }
        } else {
            writeln!(out)?;
            let msg = format!(
                "Row group {} does not exist (file has {} row group{})",
                row_group_idx,
                self.row_groups.len(),
                if self.row_groups.len() == 1 { "" } else { "s" }
            );
            #[derive(Tabled)]
            struct MsgRow {
                #[tabled(rename = "")]
                msg: String,
            }
            let styled_msg = msg.style(owo_colors::Style::new().red()).to_string();
            let mut msg_table = Table::new([MsgRow { msg: styled_msg }]);
            msg_table
                .with(Remove::row(Rows::first()))
                .with(tabled::settings::Style::rounded().remove_horizontals());
            writeln!(out, "{msg_table}")?;
        }

        if !self.custom_metadata.is_empty() {
            writeln!(out)?;
            writeln!(out, "{}", header("Metadata"))?;
            #[derive(Tabled)]
            struct MetaRow {
                #[tabled(rename = "Key")]
                key: String,
                #[tabled(rename = "Value")]
                value: String,
            }
            let meta_rows: Vec<MetaRow> = self
                .custom_metadata
                .iter()
                .map(|(k, v)| {
                    let truncated = if v.len() > 60 {
                        format!("{}...", truncate_chars(v, 57))
                    } else {
                        v.clone()
                    };
                    MetaRow {
                        key: k.clone(),
                        value: truncated,
                    }
                })
                .collect();
            let mut meta_table = Table::new(&meta_rows);
            apply_theme(&mut meta_table);
            let meta_table = meta_table.to_string();
            writeln!(out, "{meta_table}")?;
        }

        Ok(())
    }

    /// Render page-level details for specified columns in a row group.
    pub fn render_pages(
        &self,
        out: &mut dyn Write,
        row_group_idx: usize,
        columns: Option<&[&str]>,
    ) -> Result<()> {
        let Some(row_group) = self.row_groups.get(row_group_idx) else {
            writeln!(
                out,
                "Error: row group {} does not exist (file has {} row groups)",
                row_group_idx,
                self.row_groups.len()
            )?;
            return Ok(());
        };

        let column_names = row_group
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        let selected = self.resolve_columns(Some(row_group_idx), columns)?;
        let columns_to_show = column_names
            .iter()
            .enumerate()
            .filter_map(|(index, name)| {
                selected
                    .as_ref()
                    .is_none_or(|selected| selected.contains(*name))
                    .then_some(index)
            })
            .collect::<Vec<_>>();

        for col_idx in columns_to_show {
            let col_name = &column_names[col_idx];
            writeln!(out)?;
            writeln!(
                out,
                "{} {} {}",
                header("Pages"),
                label(col_name),
                dim(format!("(row group {})", row_group_idx))
            )?;

            #[derive(Tabled)]
            struct PageRow {
                #[tabled(rename = "#")]
                index: usize,
                #[tabled(rename = "Type")]
                page_type: String,
                #[tabled(rename = "Encoding")]
                encoding: String,
                #[tabled(rename = "Values")]
                num_values: String,
                #[tabled(rename = "Size")]
                size: String,
                #[tabled(rename = "Rows")]
                rows: String,
                #[tabled(rename = "Nulls")]
                nulls: String,
                #[tabled(rename = "Def")]
                def_info: String,
                #[tabled(rename = "Rep")]
                rep_info: String,
                #[tabled(rename = "Extra")]
                extra: String,
            }

            let page_rows = row_group.columns[col_idx]
                .pages
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|page| PageRow {
                    index: page.index,
                    page_type: page.page_type.to_string(),
                    encoding: encoding(&page.encoding),
                    num_values: format_number(page.num_values),
                    size: format_bytes(page.size),
                    rows: page.rows.map_or_else(missing_value, format_number),
                    nulls: page.nulls.map_or_else(missing_value, format_number),
                    def_info: page.def_encoding.as_ref().map_or_else(
                        || {
                            page.def_levels_byte_len
                                .map_or_else(missing_value, format_bytes)
                        },
                        |value| encoding(value),
                    ),
                    rep_info: page.rep_encoding.as_ref().map_or_else(
                        || {
                            page.rep_levels_byte_len
                                .map_or_else(missing_value, format_bytes)
                        },
                        |value| encoding(value),
                    ),
                    extra: if page.page_type == "Dict" {
                        true_or_missing_display(page.is_sorted.unwrap_or(false))
                    } else if page.is_compressed == Some(true) {
                        dim("comp")
                    } else {
                        missing_value()
                    },
                })
                .collect::<Vec<_>>();

            if page_rows.is_empty() {
                writeln!(out, "  {}", dim("(no pages)"))?;
            } else {
                let mut table = Table::new(&page_rows);
                apply_theme(&mut table);
                table.with(Modify::new(Columns::new(3..)).with(Alignment::right()));
                writeln!(out, "{table}")?;
            }
        }

        Ok(())
    }

    /// Get JSON output including page-level details for specified columns.
    pub fn to_json_with_pages(&self, columns: Option<&[&str]>) -> Result<Value> {
        let columns = self.resolve_columns(None, columns)?;
        Ok(self.to_json_impl(true, columns.as_ref()))
    }

    fn to_json_impl(&self, include_pages: bool, columns_filter: Option<&HashSet<String>>) -> Value {
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

                        let page_encodings_json = col.page_encodings.as_ref().map(|pe| {
                            json!({
                                "dictionary": pe.dictionary,
                                "data": pe.data,
                                "def_levels": pe.def_levels,
                                "rep_levels": pe.rep_levels,
                            })
                        });

                        // read pages if requested and column matches filter
                        let pages_json = if include_pages
                            && columns_filter.is_none_or(|columns| columns.contains(&col.name))
                        {
                            col.pages.as_ref().map(|pages| {
                                pages.iter().map(PageInfo::to_json).collect::<Vec<_>>()
                            })
                        } else {
                            None
                        };

                        json!({
                            "name": col.name,
                            "compression": col.compression,
                            "compressed_size": col.compressed_size,
                            "uncompressed_size": col.uncompressed_size,
                            "has_dictionary": col.has_dictionary,
                            "has_bloom_filter": col.has_bloom_filter,
                            "has_page_index": col.has_page_index,
                            "has_statistics": col.has_statistics,
                            "encodings": col.encodings,
                            "page_encodings": page_encodings_json,
                            "statistics": stats_json,
                            "pages": pages_json,
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

        let metadata_size = self.metadata_size();

        json!({
            "format": "parquet",
            "format_version": self.format_version,
            "created_by": self.created_by,
            "file": &self.file_path,
            "rows": self.num_rows,
            "num_columns": self.num_columns,
            "num_row_groups": self.row_groups.len(),
            "file_size": self.file_size,
            "compressed_size": self.total_compressed_size,
            "uncompressed_size": self.total_uncompressed_size,
            "bloom_filter_size": self.total_bloom_filter_size,
            "metadata_size": metadata_size,
            "compression": self.compression_summary(),
            "has_dictionary": self.has_dictionary,
            "has_bloom_filters": self.has_bloom_filters,
            "has_page_index": self.has_page_index,
            "schema": schema_to_json(&self.schema),
            "columns": file_columns,
            "row_groups": row_groups_json,
            "metadata": self.custom_metadata,
        })
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
        self.render_with_row_group(out, 0)
    }

    fn to_json(&self) -> Value {
        self.to_json_impl(false, None)
    }
}
