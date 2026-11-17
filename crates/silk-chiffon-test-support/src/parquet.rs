//! Parquet assertions shared by integration tests and benchmarks.

use std::{collections::HashSet, fs::File, path::Path};

use anyhow::Result;
use parquet::{
    basic::{Compression, Encoding},
    file::{
        metadata::{FileMetaData, SortingColumn},
        reader::{FileReader, SerializedFileReader},
        statistics::Statistics as ParquetStatistics,
    },
    record::Row,
};

pub struct ParquetContents {
    pub row_groups: Vec<RowGroup>,
    pub has_any_dictionary: bool,
    pub has_any_bloom_filters: bool,
    pub compression_used: HashSet<String>,
    pub num_rows: i64,
    pub num_row_groups: usize,
    pub total_compressed_size_bytes: i64,
    pub total_uncompressed_size_bytes: i64,
    pub metadata: FileMetaData,
}

impl ParquetContents {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.row_groups
            .first()?
            .columns
            .iter()
            .find(|column| column.name == name)
    }
}

#[derive(Debug)]
pub struct RowGroup {
    pub index: usize,
    pub num_rows: i64,
    pub total_byte_size: i64,
    pub compressed_size: i64,
    pub sorting_columns: Option<Vec<SortingColumn>>,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub compression: Compression,
    pub encodings: Vec<Encoding>,
    pub num_values: i64,
    pub compressed_size: i64,
    pub uncompressed_size: i64,
    pub has_dictionary: bool,
    pub has_bloom_filter: bool,
    pub statistics: Option<Statistics>,
}

#[derive(Debug)]
pub struct Statistics {
    pub min: Option<String>,
    pub max: Option<String>,
    pub null_count: Option<u64>,
    pub distinct_count: Option<u64>,
}

impl From<&ParquetStatistics> for Statistics {
    fn from(statistics: &ParquetStatistics) -> Self {
        Self {
            min: statistics.min_bytes_opt().map(|bytes| format!("{bytes:?}")),
            max: statistics.max_bytes_opt().map(|bytes| format!("{bytes:?}")),
            null_count: statistics.null_count_opt(),
            distinct_count: statistics.distinct_count_opt(),
        }
    }
}

pub fn read_entire_file(path: &Path) -> Result<ParquetContents> {
    let reader = SerializedFileReader::new(File::open(path)?)?;
    let metadata = reader.metadata();
    let mut result = ParquetContents {
        row_groups: Vec::new(),
        has_any_dictionary: false,
        has_any_bloom_filters: false,
        num_rows: metadata.file_metadata().num_rows(),
        num_row_groups: reader.num_row_groups(),
        total_compressed_size_bytes: 0,
        total_uncompressed_size_bytes: 0,
        compression_used: HashSet::new(),
        metadata: metadata.file_metadata().clone(),
    };

    for row_group_index in 0..reader.num_row_groups() {
        let row_group_metadata = metadata.row_group(row_group_index);
        let mut columns = Vec::new();
        for column_index in 0..row_group_metadata.num_columns() {
            let column_metadata = row_group_metadata.column(column_index);
            let has_dictionary = column_metadata.dictionary_page_offset().is_some();
            let has_bloom_filter = column_metadata.bloom_filter_offset().is_some();
            let compression = column_metadata.compression();
            result.has_any_dictionary |= has_dictionary;
            result.has_any_bloom_filters |= has_bloom_filter;
            result.compression_used.insert(compression.to_string());
            result.total_compressed_size_bytes += column_metadata.compressed_size();
            result.total_uncompressed_size_bytes += column_metadata.uncompressed_size();
            columns.push(Column {
                name: column_metadata.column_path().parts().join("."),
                compression,
                encodings: column_metadata.encodings().collect(),
                num_values: column_metadata.num_values(),
                compressed_size: column_metadata.compressed_size(),
                uncompressed_size: column_metadata.uncompressed_size(),
                has_dictionary,
                has_bloom_filter,
                statistics: column_metadata.statistics().map(Statistics::from),
            });
        }

        let row_group_reader = reader.get_row_group(row_group_index)?;
        let rows = row_group_reader
            .get_row_iter(None)?
            .collect::<Result<Vec<_>, _>>()?;
        result.row_groups.push(RowGroup {
            index: row_group_index,
            num_rows: row_group_metadata.num_rows(),
            total_byte_size: row_group_metadata.total_byte_size(),
            compressed_size: row_group_metadata.compressed_size(),
            sorting_columns: row_group_metadata.sorting_columns().cloned(),
            columns,
            rows,
        });
    }
    Ok(result)
}
