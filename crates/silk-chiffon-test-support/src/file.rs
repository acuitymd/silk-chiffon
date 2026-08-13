//! Arrow and Parquet file fixtures.

use std::{fs::File, path::Path};

use arrow::{
    array::RecordBatch,
    datatypes::SchemaRef,
    ipc::{
        reader::{FileReader as ArrowFileReader, StreamReader as ArrowStreamReader},
        writer::{FileWriter as ArrowFileWriter, StreamWriter as ArrowStreamWriter},
    },
};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

pub struct TestFile;

impl TestFile {
    pub fn write_arrow(path: &Path, batches: &[RecordBatch]) {
        assert!(!batches.is_empty(), "need at least one batch");
        let schema = batches[0].schema();
        let file = File::create(path).expect("failed to create file");
        let mut writer = ArrowFileWriter::try_new(file, &schema).expect("failed to create writer");
        for batch in batches {
            writer.write(batch).expect("failed to write batch");
        }
        writer.finish().expect("failed to finish writing");
    }

    pub fn write_arrow_batch(path: &Path, batch: &RecordBatch) {
        Self::write_arrow(path, std::slice::from_ref(batch));
    }

    /// schema only, no data
    pub fn write_arrow_empty(path: &Path, schema: &SchemaRef) {
        let file = File::create(path).expect("failed to create file");
        let mut writer = ArrowFileWriter::try_new(file, schema).expect("failed to create writer");
        writer.finish().expect("failed to finish writing");
    }

    pub fn write_arrow_stream(path: &Path, batches: &[RecordBatch]) {
        assert!(!batches.is_empty(), "need at least one batch");
        let schema = batches[0].schema();
        let file = File::create(path).expect("failed to create file");
        let mut writer =
            ArrowStreamWriter::try_new(file, &schema).expect("failed to create writer");
        for batch in batches {
            writer.write(batch).expect("failed to write batch");
        }
        writer.finish().expect("failed to finish writing");
    }

    pub fn write_parquet(path: &Path, batches: &[RecordBatch]) {
        assert!(!batches.is_empty(), "need at least one batch");
        let schema = batches[0].schema();
        let file = File::create(path).expect("failed to create file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("failed to create writer");
        for batch in batches {
            writer.write(batch).expect("failed to write batch");
        }
        writer.close().expect("failed to close writer");
    }

    pub fn write_parquet_batch(path: &Path, batch: &RecordBatch) {
        Self::write_parquet(path, std::slice::from_ref(batch));
    }

    pub fn read_arrow(path: &Path) -> Vec<RecordBatch> {
        let file = File::open(path).expect("failed to open file");
        let reader = ArrowFileReader::try_new(file, None).expect("failed to create reader");
        reader
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read batches")
    }

    pub fn read_arrow_stream(path: &Path) -> Vec<RecordBatch> {
        let file = File::open(path).expect("failed to open file");
        let reader = ArrowStreamReader::try_new(file, None).expect("failed to create reader");
        reader
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read batches")
    }

    pub fn read_parquet(path: &Path) -> Vec<RecordBatch> {
        let file = File::open(path).expect("failed to open file");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("failed to create reader builder")
            .build()
            .expect("failed to build reader");
        reader
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read batches")
    }

    /// tries Arrow file format first, falls back to stream
    pub fn read_arrow_auto(path: &Path) -> Vec<RecordBatch> {
        let file = File::open(path).expect("failed to open file");
        if let Ok(reader) = ArrowFileReader::try_new(file.try_clone().unwrap(), None) {
            reader
                .collect::<Result<Vec<_>, _>>()
                .expect("failed to read batches")
        } else {
            let reader = ArrowStreamReader::try_new(file, None).expect("failed to create reader");
            reader
                .collect::<Result<Vec<_>, _>>()
                .expect("failed to read batches")
        }
    }
}
