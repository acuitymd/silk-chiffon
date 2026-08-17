use std::{collections::HashSet, fs::File, path::Path};

use anyhow::Result;
use parquet::{
    basic::{Compression, Encoding},
    file::{
        metadata::{FileMetaData, SortingColumn},
        reader::{FileReader as ParquetFileReader, SerializedFileReader},
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
    fn from(stats: &ParquetStatistics) -> Self {
        Statistics {
            min: stats.min_bytes_opt().map(|b| format!("{:?}", b)),
            max: stats.max_bytes_opt().map(|b| format!("{:?}", b)),
            null_count: stats.null_count_opt(),
            distinct_count: stats.distinct_count_opt(),
        }
    }
}

pub fn read_entire_parquet_file(path: &Path) -> Result<ParquetContents> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
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

    for row_group_idx in 0..reader.num_row_groups() {
        let rg_metadata = metadata.row_group(row_group_idx);

        let mut columns = Vec::new();
        for col_idx in 0..rg_metadata.num_columns() {
            let col_meta = rg_metadata.column(col_idx);
            let has_dictionary = col_meta.dictionary_page_offset().is_some();
            let has_bloom_filter = col_meta.bloom_filter_offset().is_some();
            let compression = col_meta.compression();

            result.has_any_dictionary |= has_dictionary;
            result.has_any_bloom_filters |= has_bloom_filter;
            result.compression_used.insert(compression.to_string());
            result.total_compressed_size_bytes += col_meta.compressed_size();
            result.total_uncompressed_size_bytes += col_meta.uncompressed_size();

            columns.push(Column {
                name: col_meta.column_path().parts().join("."),
                compression,
                encodings: col_meta.encodings().collect::<Vec<_>>(),
                num_values: col_meta.num_values(),
                compressed_size: col_meta.compressed_size(),
                uncompressed_size: col_meta.uncompressed_size(),
                has_dictionary,
                has_bloom_filter,
                statistics: col_meta.statistics().map(Statistics::from),
            });
        }

        let row_group_reader = reader.get_row_group(row_group_idx)?;
        let rows: Vec<Row> = row_group_reader
            .get_row_iter(None)?
            .collect::<Result<Vec<_>, _>>()?;

        result.row_groups.push(RowGroup {
            index: row_group_idx,
            num_rows: rg_metadata.num_rows(),
            total_byte_size: rg_metadata.total_byte_size(),
            compressed_size: rg_metadata.compressed_size(),
            sorting_columns: rg_metadata.sorting_columns().cloned(),
            columns,
            rows,
        });
    }

    Ok(result)
}
