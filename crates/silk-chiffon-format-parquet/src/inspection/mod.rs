//! Object-store-native Parquet inspection.
//!
//! Routine output is derived from footer metadata. Page details are opt-in
//! because they require column-chunk reads; those reads are selected strictly,
//! performed one chunk at a time, and bounded by a per-chunk safety limit.

#[cfg(test)]
use std::fs::File;
use std::{
    collections::{HashMap, HashSet},
    io::{Cursor, Write},
    sync::Arc,
};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
#[cfg(test)]
use camino::Utf8Path;
use chrono::{DateTime, NaiveDate, Utc};
use num_format::{Locale, ToFormattedString};
use object_store::ObjectStoreExt;
#[cfg(test)]
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::{
    arrow::{
        async_reader::{AsyncFileReader, ParquetObjectReader},
        parquet_to_arrow_schema,
    },
    basic::{Compression, ConvertedType, LogicalType, TimeUnit},
    column::page::{Page, PageReader},
    errors::{ParquetError, Result as ParquetResult},
    file::{
        metadata::{ParquetMetaData, SortingColumn},
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

use silk_chiffon_core::{FormatFuture, InspectionOutput, PresentationMode};
use silk_chiffon_inspection_output::{
    apply_theme, boolean_display, compression, dim, encoding, format_bytes, format_number, header,
    label, missing_value, render_schema_fields, schema_json as schema_to_json,
    true_or_missing_display, truncate_chars,
};
use silk_chiffon_storage::InputObject;

fn column_name(value: &impl ToString) -> String {
    value.to_string()
}

pub(crate) struct Inspector {
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
    location: String,
    file_column_stats: Vec<FileColumnStats>,
    format_version: String,
    created_by: Option<String>,
    metadata: Arc<ParquetMetaData>,
    object: Option<InputObject>,
}

const MAX_COLUMN_CHUNK_SIZE: u64 = 512 * 1024 * 1024;

fn checked_column_chunk_range(start: u64, length: u64) -> Result<std::ops::Range<u64>> {
    if length > MAX_COLUMN_CHUNK_SIZE {
        anyhow::bail!(
            "column chunk is {} bytes, exceeding the 512 MiB inspection safety limit",
            length
        );
    }
    let end = start
        .checked_add(length)
        .ok_or_else(|| anyhow::anyhow!("column chunk range overflowed"))?;
    Ok(start..end)
}

struct ObjectChunk {
    base: u64,
    bytes: Bytes,
}

impl Length for ObjectChunk {
    fn len(&self) -> u64 {
        self.base + self.bytes.len() as u64
    }
}

impl ChunkReader for ObjectChunk {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        let offset = self.offset(start)?;
        Ok(Cursor::new(self.bytes.slice(offset..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let offset = self.offset(start)?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| ParquetError::General("column chunk range overflowed".to_owned()))?;
        if end > self.bytes.len() {
            return Err(ParquetError::EOF(format!(
                "column chunk read ended at {end}, but only {} bytes were fetched",
                self.bytes.len()
            )));
        }
        Ok(self.bytes.slice(offset..end))
    }
}

impl ObjectChunk {
    fn offset(&self, start: u64) -> ParquetResult<usize> {
        let offset = start.checked_sub(self.base).ok_or_else(|| {
            ParquetError::General(format!(
                "column chunk read started at {start} before {}",
                self.base
            ))
        })?;
        usize::try_from(offset)
            .map_err(|_| ParquetError::General("column chunk offset is too large".to_owned()))
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct RowGroupInfo {
    pub index: usize,
    pub num_rows: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub sorting_columns: Option<Vec<SortingColumnInfo>>,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct SortingColumnInfo {
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
pub(crate) struct ColumnInfo {
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
}

#[derive(Debug, Clone, Serialize, Default)]
pub(crate) struct PageEncodings {
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
pub(crate) struct ColumnStatistics {
    pub min: Option<String>,
    pub max: Option<String>,
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
}

/// File-level aggregated statistics for a column (across all row groups).
#[derive(Debug, Serialize, Clone)]
pub(crate) struct FileColumnStats {
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

fn read_pages_json(page_reader: &mut dyn PageReader) -> Result<Option<Vec<Value>>> {
    let mut pages = Vec::new();
    let mut page_idx = 0;

    while let Some(page) = page_reader.get_next_page()? {
        let page_json = match &page {
            Page::DictionaryPage {
                buf,
                num_values,
                encoding,
                is_sorted,
            } => {
                json!({
                    "index": page_idx,
                    "type": "Dict",
                    "encoding": format!("{encoding:?}"),
                    "num_values": num_values,
                    "size": buf.len(),
                    "is_sorted": is_sorted,
                })
            }
            Page::DataPage {
                buf,
                num_values,
                encoding,
                def_level_encoding,
                rep_level_encoding,
                statistics,
            } => {
                let stats_json = statistics.as_ref().map(|_s| {
                    // statistics in DataPage are already decoded by parquet-rs
                    // but we don't have access to logical type here, so just note presence
                    json!(true)
                });
                json!({
                    "index": page_idx,
                    "type": "Data",
                    "encoding": format!("{encoding:?}"),
                    "num_values": num_values,
                    "size": buf.len(),
                    "def_level_encoding": format!("{def_level_encoding:?}"),
                    "rep_level_encoding": format!("{rep_level_encoding:?}"),
                    "has_statistics": stats_json.is_some(),
                })
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
                let stats_json = statistics.as_ref().map(|_s| json!(true));
                json!({
                    "index": page_idx,
                    "type": "DataV2",
                    "encoding": format!("{encoding:?}"),
                    "num_values": num_values,
                    "size": buf.len(),
                    "num_rows": num_rows,
                    "num_nulls": num_nulls,
                    "def_levels_byte_len": def_levels_byte_len,
                    "rep_levels_byte_len": rep_levels_byte_len,
                    "is_compressed": is_compressed,
                    "has_statistics": stats_json.is_some(),
                })
            }
        };
        pages.push(page_json);
        page_idx += 1;
    }

    Ok(if pages.is_empty() { None } else { Some(pages) })
}

impl Inspector {
    fn validate_row_group(&self, row_group: usize) -> Result<()> {
        if row_group >= self.row_groups.len() {
            anyhow::bail!(
                "row group {row_group} does not exist (file has {} row groups)",
                self.row_groups.len()
            );
        }
        Ok(())
    }

    fn selected_row_group(&self, requested: Option<usize>) -> Result<Option<usize>> {
        if let Some(row_group) = requested {
            self.validate_row_group(row_group)?;
            return Ok(Some(row_group));
        }
        Ok((!self.row_groups.is_empty()).then_some(0))
    }

    #[cfg(test)]
    fn is_format(path: &Utf8Path) -> Result<bool> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = File::open(path)?;
        if file.metadata()?.len() < 8 {
            return Ok(false);
        }
        let mut start = [0; 4];
        let mut end = [0; 4];
        file.read_exact(&mut start)?;
        file.seek(SeekFrom::End(-4))?;
        file.read_exact(&mut end)?;
        Ok(&start == b"PAR1" && &end == b"PAR1")
    }

    #[cfg(test)]
    fn format_name(&self) -> &'static str {
        "Parquet"
    }

    #[cfg(test)]
    fn row_count(&self) -> Option<u64> {
        Some(self.num_rows)
    }

    #[cfg(test)]
    pub(crate) fn open(path: &Utf8Path) -> Result<Self> {
        let file_size = std::fs::metadata(path)?.len();
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = Arc::new(reader.metadata().clone());
        Self::from_metadata(&metadata, file_size, path.to_string(), None)
    }

    async fn load(object: &InputObject) -> Result<Self> {
        let handle = object.input_handle();
        let mut reader =
            ParquetObjectReader::new(handle.object_store(), handle.object_path().clone())
                .with_file_size(object.metadata().size);
        let metadata = reader.get_metadata(None).await?;
        let location = silk_chiffon_inspection_output::display_location(object)?;
        Self::from_metadata(
            &metadata,
            object.metadata().size,
            location,
            Some(object.clone()),
        )
    }

    async fn page_reader(
        &self,
        row_group: usize,
        column: usize,
    ) -> Result<SerializedPageReader<ObjectChunk>> {
        let object = self
            .object
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("page inspection requires a resolved input object"))?;
        let row_group_metadata = self
            .metadata
            .row_groups()
            .get(row_group)
            .ok_or_else(|| anyhow::anyhow!("row group {row_group} does not exist"))?;
        let column_metadata = row_group_metadata.columns().get(column).ok_or_else(|| {
            anyhow::anyhow!("column {column} does not exist in row group {row_group}")
        })?;
        let (start, length) = column_metadata.byte_range();
        let range = checked_column_chunk_range(start, length)?;
        let bytes = object
            .input_handle()
            .object_store()
            .get_range(object.input_handle().object_path(), range)
            .await?;
        let rows = usize::try_from(row_group_metadata.num_rows())?;
        Ok(SerializedPageReader::new(
            Arc::new(ObjectChunk { base: start, bytes }),
            column_metadata,
            rows,
            None,
        )?)
    }

    fn from_metadata(
        metadata: &Arc<ParquetMetaData>,
        file_size: u64,
        location: String,
        object: Option<InputObject>,
    ) -> Result<Self> {
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
            schema: schema.into(),
            row_groups: Vec::new(),
            num_rows,
            num_columns: 0,
            file_size,
            total_compressed_size: 0,
            total_uncompressed_size: 0,
            total_bloom_filter_size: 0,
            compression_codecs: HashSet::new(),
            has_dictionary: false,
            has_bloom_filters: false,
            has_page_index: false,
            custom_metadata: HashMap::new(),
            location,
            file_column_stats: Vec::new(),
            format_version,
            created_by,
            metadata: Arc::clone(metadata),
            object,
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

    #[cfg(test)]
    pub(crate) fn row_groups(&self) -> &[RowGroupInfo] {
        &self.row_groups
    }

    #[cfg(test)]
    pub(crate) fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.row_groups
            .first()?
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

    pub fn render_with_row_group(
        &self,
        out: &mut dyn Write,
        row_group_idx: Option<usize>,
    ) -> Result<()> {
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

        writeln!(out, "{}", header(&self.location))?;
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

        if let Some(row_group_idx) = row_group_idx {
            let rg = &self.row_groups[row_group_idx];
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
    pub async fn render_pages(
        &self,
        out: &mut (dyn Write + Send),
        row_group_idx: usize,
        columns: Option<&[&str]>,
    ) -> Result<()> {
        if row_group_idx >= self.row_groups.len() {
            writeln!(
                out,
                "Error: row group {} does not exist (file has {} row groups)",
                row_group_idx,
                self.row_groups.len()
            )?;
            return Ok(());
        }

        let row_group = &self.row_groups[row_group_idx];
        let column_names: Vec<String> = row_group
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();

        let columns_to_show: Vec<usize> = match columns {
            Some(names) => {
                let mut indices = Vec::new();
                for name in names {
                    let idx = column_names
                        .iter()
                        .position(|column| column == name)
                        .ok_or_else(|| anyhow::anyhow!("column '{name}' does not exist"))?;
                    indices.push(idx);
                }
                indices
            }
            None => (0..row_group.columns.len()).collect(),
        };

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

            let mut page_rows = Vec::new();
            let mut page_reader = self.page_reader(row_group_idx, col_idx).await?;
            let mut page_idx = 0;
            while let Some(page) = page_reader.get_next_page()? {
                let row = match &page {
                    Page::DictionaryPage {
                        buf,
                        num_values,
                        encoding: enc,
                        is_sorted,
                    } => PageRow {
                        index: page_idx,
                        page_type: "Dict".to_string(),
                        encoding: encoding(&format!("{enc:?}")),
                        num_values: format_number(u64::from(*num_values)),
                        size: format_bytes(buf.len() as u64),
                        rows: missing_value(),
                        nulls: missing_value(),
                        def_info: missing_value(),
                        rep_info: missing_value(),
                        extra: true_or_missing_display(*is_sorted),
                    },
                    Page::DataPage {
                        buf,
                        num_values,
                        encoding: enc,
                        def_level_encoding,
                        rep_level_encoding,
                        ..
                    } => PageRow {
                        index: page_idx,
                        page_type: "Data".to_string(),
                        encoding: encoding(&format!("{enc:?}")),
                        num_values: format_number(u64::from(*num_values)),
                        size: format_bytes(buf.len() as u64),
                        rows: missing_value(),
                        nulls: missing_value(),
                        def_info: encoding(&format!("{def_level_encoding:?}")),
                        rep_info: encoding(&format!("{rep_level_encoding:?}")),
                        extra: missing_value(),
                    },
                    Page::DataPageV2 {
                        buf,
                        num_values,
                        encoding: enc,
                        num_nulls,
                        num_rows,
                        def_levels_byte_len,
                        rep_levels_byte_len,
                        is_compressed,
                        ..
                    } => PageRow {
                        index: page_idx,
                        page_type: "DataV2".to_string(),
                        encoding: encoding(&format!("{enc:?}")),
                        num_values: format_number(u64::from(*num_values)),
                        size: format_bytes(buf.len() as u64),
                        rows: format_number(u64::from(*num_rows)),
                        nulls: format_number(u64::from(*num_nulls)),
                        def_info: format_bytes(u64::from(*def_levels_byte_len)),
                        rep_info: format_bytes(u64::from(*rep_levels_byte_len)),
                        extra: if *is_compressed {
                            dim("comp")
                        } else {
                            missing_value()
                        },
                    },
                };
                page_rows.push(row);
                page_idx += 1;
            }

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
    async fn to_json_with_pages(
        &self,
        row_group: usize,
        columns: Option<&[&str]>,
    ) -> Result<Value> {
        let column_names = self
            .row_groups
            .first()
            .map(|row_group| {
                row_group
                    .columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let selected = match columns {
            Some(names) => names
                .iter()
                .map(|name| {
                    column_names
                        .iter()
                        .position(|column| column == name)
                        .ok_or_else(|| anyhow::anyhow!("column '{name}' does not exist"))
                })
                .collect::<Result<Vec<_>>>()?,
            None => (0..column_names.len()).collect(),
        };
        let mut output = self.to_json_impl();
        for &column in &selected {
            let mut page_reader = self.page_reader(row_group, column).await?;
            output["row_groups"][row_group]["columns"][column]["pages"] =
                serde_json::to_value(read_pages_json(&mut page_reader)?)?;
        }
        Ok(output)
    }

    fn to_json_impl(&self) -> Value {
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
                            "pages": null,
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
            "file": self.location,
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

impl Inspector {
    fn to_json(&self) -> Value {
        self.to_json_impl()
    }
}

pub(crate) fn inspect<'a>(
    object: &'a InputObject,
    mode: PresentationMode,
    args: &'a crate::InspectionArgs,
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async move {
        let inspector = Inspector::load(object).await?;
        let selected_row_group = inspector.selected_row_group(args.row_group)?;
        let page_row_group = selected_row_group.unwrap_or(0);
        if args.pages.is_some() {
            inspector.validate_row_group(page_row_group)?;
        }
        let columns = args.pages.as_ref().and_then(|columns| {
            (!columns.is_empty()).then(|| columns.split(',').map(str::trim).collect::<Vec<_>>())
        });
        if mode == PresentationMode::Json {
            let value = if args.pages.is_some() {
                inspector
                    .to_json_with_pages(page_row_group, columns.as_deref())
                    .await?
            } else {
                inspector.to_json()
            };
            return Ok(InspectionOutput::Json(value));
        }
        let mut output = Vec::new();
        inspector.render_with_row_group(&mut output, selected_row_group)?;
        if args.pages.is_some() {
            inspector
                .render_pages(&mut output, page_row_group, columns.as_deref())
                .await?;
        }
        Ok(InspectionOutput::Text(String::from_utf8(output)?))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    };
    use tempfile::TempDir;

    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use clap::Command;
    use object_store::{GetRange, ObjectStore};
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
    use silk_chiffon_core::{InspectionOutput, PresentationMode};
    use silk_chiffon_storage::{
        ExistingOutput, LocationInput, OutputPreparation, StorageAccess, StorageBackend,
        StorageRegistry, StorageSession,
    };
    use silk_chiffon_test_support::ReadProbeStore;

    static STORE: OnceLock<Arc<ReadProbeStore>> = OnceLock::new();
    static TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    static OBJECT_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

    struct FailingPageReader;

    impl Iterator for FailingPageReader {
        type Item = ParquetResult<Page>;

        fn next(&mut self) -> Option<Self::Item> {
            Some(Err(ParquetError::General(
                "controlled page failure".to_owned(),
            )))
        }
    }

    impl PageReader for FailingPageReader {
        fn get_next_page(&mut self) -> ParquetResult<Option<Page>> {
            Err(ParquetError::General("controlled page failure".to_owned()))
        }

        fn peek_next_page(&mut self) -> ParquetResult<Option<parquet::column::page::PageMetadata>> {
            unreachable!("read_pages_json does not peek")
        }

        fn skip_next_page(&mut self) -> ParquetResult<()> {
            unreachable!("read_pages_json does not skip")
        }
    }

    #[test]
    fn page_inspection_reports_decoder_failures() {
        let error = read_pages_json(&mut FailingPageReader).unwrap_err();
        assert!(error.to_string().contains("controlled page failure"));
    }

    fn store() -> Arc<ReadProbeStore> {
        Arc::clone(STORE.get_or_init(|| Arc::new(ReadProbeStore::new())))
    }

    async fn test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        TEST_LOCK
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await
    }

    fn create_store(
        _: &url::Url,
        _: &(),
        _: Option<&silk_chiffon_storage::RetryConfig>,
    ) -> Result<Arc<dyn ObjectStore>> {
        Ok(store())
    }

    fn session() -> StorageSession {
        let registry = StorageRegistry::builder()
            .register(
                StorageBackend::without_args()
                    .name("memory")
                    .schemes(["memory"])
                    .access(StorageAccess::ReadWrite)
                    .allow_any_location()
                    .object_store_creator(create_store)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let matches = registry
            .augment_args(Command::new("test"))
            .try_get_matches_from(["test"])
            .unwrap();
        registry.create_session(&matches).unwrap()
    }

    fn parquet_bytes_with_row_group_size(row_group_size: usize) -> Bytes {
        let schema = simple_schema();
        let batch = create_batch(&schema);
        let mut bytes = Cursor::new(Vec::new());
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(row_group_size))
            .build();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(bytes.into_inner())
    }

    fn parquet_bytes() -> Bytes {
        parquet_bytes_with_row_group_size(1024)
    }

    fn empty_parquet_bytes() -> Bytes {
        let mut bytes = Cursor::new(Vec::new());
        let writer = ArrowWriter::try_new(&mut bytes, simple_schema(), None).unwrap();
        writer.close().unwrap();
        Bytes::from(bytes.into_inner())
    }

    async fn remote_object_with(bytes: Bytes) -> InputObject {
        let session = session();
        let sequence = OBJECT_SEQUENCE.fetch_add(1, Ordering::SeqCst);
        let location =
            LocationInput::parse(format!("memory://bucket/inspection-{sequence}.parquet")).unwrap();
        let target = session
            .prepare_output_target(
                &location,
                &OutputPreparation::new(ExistingOutput::Allow, false),
            )
            .await
            .unwrap();
        target
            .object_store()
            .put(target.object_path(), bytes.into())
            .await
            .unwrap();
        session.lookup_input(&location).await.unwrap()
    }

    async fn remote_object() -> InputObject {
        remote_object_with(parquet_bytes()).await
    }

    fn inspection_binding(arguments: &[&str]) -> silk_chiffon_core::InspectionBinding {
        let definition = crate::definition();
        let matches = definition
            .augment_inspection_args(Command::new("inspect"))
            .try_get_matches_from(std::iter::once("inspect").chain(arguments.iter().copied()))
            .unwrap();
        definition.bind_inspection(&matches).unwrap()
    }

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
        let std_path = temp_dir.path().join("test.parquet");
        let path = Utf8Path::from_path(&std_path).unwrap();

        let schema = simple_schema();
        let batch = create_batch(&schema);

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        assert!(Inspector::is_format(path).unwrap());
    }

    #[test]
    fn test_open_parquet_file() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.parquet");
        let path = Utf8Path::from_path(&std_path).unwrap();

        let schema = simple_schema();
        let batch = create_batch(&schema);

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let inspector = Inspector::open(path).unwrap();
        assert_eq!(inspector.row_count(), Some(3));
        assert_eq!(inspector.format_name(), "Parquet");
    }

    #[test]
    fn test_is_format_non_parquet_file() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.txt");
        let path = Utf8Path::from_path(&std_path).unwrap();
        std::fs::write(path, "not a parquet file").unwrap();

        assert!(!Inspector::is_format(path).unwrap());
    }

    #[test]
    fn test_is_format_partial_magic_bytes() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.parquet");
        let path = Utf8Path::from_path(&std_path).unwrap();
        // only start magic, no end magic
        std::fs::write(path, b"PAR1garbage").unwrap();

        assert!(!Inspector::is_format(path).unwrap());
    }

    #[test]
    fn test_is_format_nonexistent_file() {
        let path = Utf8Path::new("/nonexistent/path/file.parquet");
        let result = Inspector::is_format(path);
        assert!(result.is_err());
    }

    #[test]
    fn column_chunk_safety_limit_rejects_oversized_and_overflowing_ranges() {
        let oversized = checked_column_chunk_range(0, MAX_COLUMN_CHUNK_SIZE + 1).unwrap_err();
        assert!(oversized.to_string().contains("512 MiB"));

        let overflow = checked_column_chunk_range(u64::MAX, 1).unwrap_err();
        assert!(overflow.to_string().contains("range overflowed"));
    }

    #[tokio::test]
    async fn registered_summary_inspection_uses_only_bounded_object_store_reads() {
        let _guard = test_guard().await;
        let object = remote_object().await;
        store().reset_observation();

        let output = inspection_binding(&[])
            .inspect(&object, PresentationMode::Json)
            .await
            .unwrap();
        let InspectionOutput::Json(output) = output else {
            panic!("expected JSON output");
        };

        assert_eq!(output["format"], "parquet");
        assert_eq!(output["rows"], 3);
        assert_eq!(output["file"], object.input_handle().url().as_str());
        let ranges = store().ranges();
        assert!(!ranges.is_empty());
        assert!(
            ranges
                .iter()
                .all(|range| matches!(range, GetRange::Bounded(_)))
        );
    }

    #[tokio::test]
    async fn empty_file_summary_does_not_require_a_row_group() {
        let _guard = test_guard().await;
        let object = remote_object_with(empty_parquet_bytes()).await;

        let output = inspection_binding(&[])
            .inspect(&object, PresentationMode::Text)
            .await
            .unwrap();
        let InspectionOutput::Text(output) = output else {
            panic!("expected text output");
        };
        assert!(output.contains("Row groups"));
        assert!(output.contains("Schema"));
        assert!(!output.contains("does not exist"));

        let output = inspection_binding(&[])
            .inspect(&object, PresentationMode::Json)
            .await
            .unwrap();
        let InspectionOutput::Json(output) = output else {
            panic!("expected JSON output");
        };
        assert_eq!(output["num_row_groups"], 0);
        assert!(output["row_groups"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn empty_file_rejects_requested_row_group_details() {
        let _guard = test_guard().await;
        let object = remote_object_with(empty_parquet_bytes()).await;

        for mode in [PresentationMode::Text, PresentationMode::Json] {
            for arguments in [["--row-group=0"].as_slice(), ["--pages"].as_slice()] {
                let error = inspection_binding(arguments)
                    .inspect(&object, mode)
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("row group 0 does not exist"));
            }
        }
    }

    #[tokio::test]
    async fn page_inspection_reads_only_selected_columns_and_rejects_unknown_ones() {
        let _guard = test_guard().await;
        let object = remote_object().await;

        store().reset_observation();
        let output = inspection_binding(&["--pages=id"])
            .inspect(&object, PresentationMode::Json)
            .await
            .unwrap();
        let InspectionOutput::Json(output) = output else {
            panic!("expected JSON output");
        };
        assert!(!output["row_groups"][0]["columns"][0]["pages"].is_null());
        assert!(output["row_groups"][0]["columns"][1]["pages"].is_null());
        let selected_reads = store().ranges().len();

        store().reset_observation();
        let error = inspection_binding(&["--pages=missing"])
            .inspect(&object, PresentationMode::Json)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("column 'missing' does not exist")
        );
        assert!(store().ranges().len() < selected_reads);
    }

    #[tokio::test]
    async fn page_inspection_keeps_object_reads_sequential() {
        let _guard = test_guard().await;
        let object = remote_object().await;
        store().reset_observation();

        inspection_binding(&["--pages"])
            .inspect(&object, PresentationMode::Json)
            .await
            .unwrap();

        assert_eq!(store().max_active_reads(), 1);
        assert!(store().ranges().len() >= 3);
    }

    #[tokio::test]
    async fn page_inspection_attaches_details_only_to_the_selected_row_group() {
        let _guard = test_guard().await;
        let object = remote_object_with(parquet_bytes_with_row_group_size(2)).await;
        store().reset_observation();

        let output = inspection_binding(&["--row-group=1", "--pages=id"])
            .inspect(&object, PresentationMode::Json)
            .await
            .unwrap();
        let InspectionOutput::Json(output) = output else {
            panic!("expected JSON output");
        };

        assert!(output["row_groups"][0]["columns"][0]["pages"].is_null());
        assert!(!output["row_groups"][1]["columns"][0]["pages"].is_null());
    }

    #[tokio::test]
    async fn both_output_modes_reject_an_unknown_row_group() {
        let _guard = test_guard().await;
        let object = remote_object().await;

        for mode in [PresentationMode::Text, PresentationMode::Json] {
            let error = inspection_binding(&["--row-group=1"])
                .inspect(&object, mode)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("row group 1 does not exist"));
        }
    }

    #[tokio::test]
    async fn inspection_surfaces_object_store_failures() {
        let _guard = test_guard().await;
        let object = remote_object().await;
        store().reset_observation();
        store().set_fail_reads(true);

        let error = inspection_binding(&[])
            .inspect(&object, PresentationMode::Text)
            .await
            .unwrap_err();

        assert!(
            format!("{error:#}").contains("controlled object-store read failure"),
            "{error:#}"
        );
        store().reset_observation();
    }
}
