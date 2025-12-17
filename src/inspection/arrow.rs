//! Arrow IPC file inspection.

use std::{collections::HashMap, fs::File, io::Write};

use camino::{Utf8Path, Utf8PathBuf};

use anyhow::{Result, bail};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::{FileReader, StreamReader};
use serde::Serialize;
use serde_json::{Value, json};

use super::{
    inspectable::{
        Inspectable, format_number, render_metadata_map, render_schema_fields, schema_to_json,
    },
    style::{dim, header, label, value},
};

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ArrowVariant {
    File,
    Stream,
}

impl std::fmt::Display for ArrowVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowVariant::File => write!(f, "file"),
            ArrowVariant::Stream => write!(f, "stream"),
        }
    }
}

pub struct ArrowInspector {
    schema: SchemaRef,
    variant: ArrowVariant,
    num_rows: Option<u64>,
    num_batches: Option<usize>,
    batch_info: Option<Vec<BatchInfo>>,
    custom_metadata: HashMap<String, String>,
    file_path: Utf8PathBuf,
}

#[derive(Debug, Serialize)]
pub struct BatchInfo {
    pub index: usize,
    pub num_rows: usize,
}

impl ArrowInspector {
    pub fn open(path: &Utf8Path, count_rows: bool) -> Result<Self> {
        if Self::is_file_format_at(path)? {
            Self::open_file(path, count_rows)
        } else if Self::is_stream_format_at(path)? {
            Self::open_stream(path, count_rows)
        } else {
            bail!("not an Arrow IPC file");
        }
    }

    pub fn open_file(path: &Utf8Path, count_rows: bool) -> Result<Self> {
        let file = File::open(path)?;
        let reader = FileReader::try_new(file, None)?;
        let schema = reader.schema();

        let custom_metadata = schema
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        if count_rows {
            let mut batch_info = Vec::new();
            let mut total_rows = 0u64;

            for (idx, batch_result) in reader.enumerate() {
                let batch = batch_result?;
                let num_rows = batch.num_rows();
                total_rows = total_rows.saturating_add(num_rows as u64);
                batch_info.push(BatchInfo {
                    index: idx,
                    num_rows,
                });
            }

            Ok(Self {
                schema,
                variant: ArrowVariant::File,
                num_rows: Some(total_rows),
                num_batches: Some(batch_info.len()),
                batch_info: Some(batch_info),
                custom_metadata,
                file_path: path.to_owned(),
            })
        } else {
            Ok(Self {
                schema,
                variant: ArrowVariant::File,
                num_rows: None,
                num_batches: None,
                batch_info: None,
                custom_metadata,
                file_path: path.to_owned(),
            })
        }
    }

    pub fn open_stream(path: &Utf8Path, count_rows: bool) -> Result<Self> {
        let file = File::open(path)?;
        let reader = StreamReader::try_new(file, None)?;
        let schema = reader.schema();

        let custom_metadata = schema
            .metadata()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        if count_rows {
            let mut batch_info = Vec::new();
            let mut total_rows = 0u64;

            for (idx, batch_result) in reader.enumerate() {
                let batch = batch_result?;
                let num_rows = batch.num_rows();
                total_rows = total_rows.saturating_add(num_rows as u64);
                batch_info.push(BatchInfo {
                    index: idx,
                    num_rows,
                });
            }

            Ok(Self {
                schema,
                variant: ArrowVariant::Stream,
                num_rows: Some(total_rows),
                num_batches: Some(batch_info.len()),
                batch_info: Some(batch_info),
                custom_metadata,
                file_path: path.to_owned(),
            })
        } else {
            Ok(Self {
                schema,
                variant: ArrowVariant::Stream,
                num_rows: None,
                num_batches: None,
                batch_info: None,
                custom_metadata,
                file_path: path.to_owned(),
            })
        }
    }

    pub fn variant(&self) -> ArrowVariant {
        self.variant
    }

    pub fn render_batches(&self, out: &mut dyn Write) -> Result<()> {
        match &self.batch_info {
            Some(batches) => {
                writeln!(
                    out,
                    "\n{} ({}):",
                    header("Record Batches"),
                    value(batches.len())
                )?;
                writeln!(out)?;

                // collapse identical consecutive batch sizes
                let mut i = 0;
                while i < batches.len() {
                    let size = batches[i].num_rows;
                    let start = i;
                    while i < batches.len() && batches[i].num_rows == size {
                        i += 1;
                    }
                    let end = i - 1;

                    if start == end {
                        writeln!(
                            out,
                            "  {} {}: {} rows",
                            label("Batch"),
                            value(start),
                            value(format_number(size as u64))
                        )?;
                    } else {
                        writeln!(
                            out,
                            "  {} {}-{}: {} rows each",
                            label("Batches"),
                            value(start),
                            value(end),
                            value(format_number(size as u64))
                        )?;
                    }
                }
            }
            None => {
                writeln!(
                    out,
                    "\n{}: {}",
                    label("Record Batches"),
                    dim("(use --row-count to read)")
                )?;
            }
        }

        Ok(())
    }

    pub fn render_metadata(&self, out: &mut dyn Write) -> Result<()> {
        render_metadata_map(out, "Custom Metadata", &self.custom_metadata)
    }

    /// Detect which Arrow IPC variant this file is.
    /// Errors if not an Arrow file or on I/O issues.
    pub fn detect_variant(path: &Utf8Path) -> Result<ArrowVariant> {
        if Self::is_file_format_at(path)? {
            return Ok(ArrowVariant::File);
        }

        if Self::is_stream_format_at(path)? {
            return Ok(ArrowVariant::Stream);
        }

        bail!("not an Arrow IPC file")
    }

    fn is_file_format_at(path: &Utf8Path) -> Result<bool> {
        Ok(Self::is_file_format(&File::open(path)?))
    }

    fn is_file_format(file: &File) -> bool {
        FileReader::try_new(file, None).is_ok()
    }

    fn is_stream_format_at(path: &Utf8Path) -> Result<bool> {
        Ok(Self::is_stream_format(&File::open(path)?))
    }

    fn is_stream_format(file: &File) -> bool {
        StreamReader::try_new(file, None).is_ok()
    }
}

impl Inspectable for ArrowInspector {
    fn is_format(path: &Utf8Path) -> Result<bool> {
        Ok(Self::is_file_format_at(path)? || Self::is_stream_format_at(path)?)
    }

    fn format_name(&self) -> &str {
        match self.variant {
            ArrowVariant::File => "Arrow IPC (file)",
            ArrowVariant::Stream => "Arrow IPC (stream)",
        }
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn row_count(&self) -> Option<u64> {
        self.num_rows
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
            header(&self.file_path),
            dim(format!("({})", self.format_name()))
        )?;
        writeln!(out)?;

        match self.num_rows {
            Some(n) => writeln!(out, "{:<15} {}", label("Rows:"), value(format_number(n)))?,
            None => writeln!(
                out,
                "{:<15} {}",
                label("Rows:"),
                dim("(use --row-count to read)")
            )?,
        }
        match self.num_batches {
            Some(n) => writeln!(out, "{:<15} {}", label("Record batches:"), value(n))?,
            None => writeln!(
                out,
                "{:<15} {}",
                label("Record batches:"),
                dim("(use --row-count to read)")
            )?,
        }

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
        json!({
            "format": "arrow",
            "variant": self.variant,
            "file": self.file_path,
            "rows": self.num_rows,
            "record_batches": self.num_batches,
            "schema": schema_to_json(&self.schema),
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
    use arrow::ipc::writer::{FileWriter, StreamWriter};

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
    fn test_is_format_arrow_file() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.arrow");
        let path = Utf8Path::from_path(&std_path).unwrap();

        let schema = simple_schema();
        let batch = create_batch(&schema);

        let file = File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        assert!(ArrowInspector::is_format(path).unwrap());
    }

    #[test]
    fn test_is_format_arrow_stream() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.arrows");
        let path = Utf8Path::from_path(&std_path).unwrap();

        let schema = simple_schema();
        let batch = create_batch(&schema);

        let file = File::create(path).unwrap();
        let mut writer = StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        assert!(ArrowInspector::is_format(path).unwrap());
    }

    #[test]
    fn test_detect_variant_file() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.arrow");
        let path = Utf8Path::from_path(&std_path).unwrap();

        let schema = simple_schema();
        let batch = create_batch(&schema);

        let file = File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let variant = ArrowInspector::detect_variant(path).unwrap();
        assert_eq!(variant, ArrowVariant::File);
    }

    #[test]
    fn test_detect_variant_stream() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.arrows");
        let path = Utf8Path::from_path(&std_path).unwrap();

        let schema = simple_schema();
        let batch = create_batch(&schema);

        let file = File::create(path).unwrap();
        let mut writer = StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let variant = ArrowInspector::detect_variant(path).unwrap();
        assert_eq!(variant, ArrowVariant::Stream);
    }

    #[test]
    fn test_open_file_with_row_count() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.arrow");
        let path = Utf8Path::from_path(&std_path).unwrap();

        let schema = simple_schema();
        let batch = create_batch(&schema);

        let file = File::create(path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let inspector = ArrowInspector::open_file(path, true).unwrap();
        assert_eq!(inspector.variant(), ArrowVariant::File);
        assert_eq!(inspector.row_count(), Some(3));
    }

    #[test]
    fn test_open_stream_with_row_count() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.arrows");
        let path = Utf8Path::from_path(&std_path).unwrap();

        let schema = simple_schema();
        let batch = create_batch(&schema);

        let file = File::create(path).unwrap();
        let mut writer = StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        let inspector = ArrowInspector::open_stream(path, true).unwrap();
        assert_eq!(inspector.variant(), ArrowVariant::Stream);
        assert_eq!(inspector.row_count(), Some(3));
    }

    #[test]
    fn test_is_format_non_arrow_file() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.txt");
        let path = Utf8Path::from_path(&std_path).unwrap();
        std::fs::write(path, "not an arrow file").unwrap();

        assert!(!ArrowInspector::is_format(path).unwrap());
    }

    #[test]
    fn test_detect_variant_non_arrow_errors() {
        let temp_dir = TempDir::new().unwrap();
        let std_path = temp_dir.path().join("test.txt");
        let path = Utf8Path::from_path(&std_path).unwrap();
        std::fs::write(path, "not an arrow file").unwrap();

        let result = ArrowInspector::detect_variant(path);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_format_nonexistent_file() {
        let path = Utf8Path::new("/nonexistent/path/file.arrow");
        let result = ArrowInspector::is_format(path);
        assert!(result.is_err());
    }
}
