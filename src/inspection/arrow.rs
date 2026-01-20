//! Arrow IPC file inspection.

use std::{collections::HashMap, fs::File, io::Write};

use camino::{Utf8Path, Utf8PathBuf};

use anyhow::{Result, bail};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::{FileReader, StreamReader};
use serde::Serialize;
use serde_json::{Value, json};
use tabled::{
    Table, Tabled,
    settings::{Alignment, Modify, Remove, Style, object::Columns, object::Rows},
};

use super::{
    inspectable::{Inspectable, format_bytes, format_number, render_schema_fields, schema_to_json},
    style::{apply_theme, dim, header},
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
    file_size: u64,
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
        let file_size = file.metadata()?.len();
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
                file_size,
                num_rows: Some(total_rows),
                num_batches: Some(batch_info.len()),
                batch_info: Some(batch_info),
                custom_metadata,
                file_path: path.to_owned(),
            })
        } else {
            let num_batches = reader.num_batches();
            Ok(Self {
                schema,
                variant: ArrowVariant::File,
                file_size,
                num_rows: None,
                num_batches: Some(num_batches),
                batch_info: None,
                custom_metadata,
                file_path: path.to_owned(),
            })
        }
    }

    pub fn open_stream(path: &Utf8Path, count_rows: bool) -> Result<Self> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
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
                file_size,
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
                file_size,
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
        writeln!(out)?;
        writeln!(out, "{}", header("Record Batches"))?;

        match &self.batch_info {
            Some(batches) => {
                #[derive(Tabled)]
                struct BatchRow {
                    #[tabled(rename = "Batch")]
                    batch: String,
                    #[tabled(rename = "Rows")]
                    rows: String,
                }

                let rows: Vec<BatchRow> = batches
                    .iter()
                    .map(|b| BatchRow {
                        batch: b.index.to_string(),
                        rows: format_number(b.num_rows as u64),
                    })
                    .collect();

                let mut table = Table::new(&rows);
                apply_theme(&mut table);
                table.with(Modify::new(Columns::new(1..)).with(Alignment::right()));
                writeln!(out, "{table}")?;
            }
            None => {
                writeln!(out, "  {}", dim("(use --row-count to read)"))?;
            }
        }

        Ok(())
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
        writeln!(out, "{}", header(&self.file_path))?;
        writeln!(out)?;

        #[derive(Tabled)]
        struct InfoRow {
            #[tabled(rename = "")]
            label: String,
            #[tabled(rename = "")]
            value: String,
        }

        let format_display = match self.variant {
            ArrowVariant::File => "Arrow IPC (file)",
            ArrowVariant::Stream => "Arrow IPC (stream)",
        };

        let rows_display = self
            .num_rows
            .map_or_else(|| dim("(use --row-count)"), format_number);

        let batches_display = self
            .num_batches
            .map_or_else(|| dim("(use --row-count)"), |n| n.to_string());

        let info_rows = vec![
            InfoRow {
                label: "Format".to_string(),
                value: format_display.to_string(),
            },
            InfoRow {
                label: "Rows".to_string(),
                value: rows_display,
            },
            InfoRow {
                label: "Record batches".to_string(),
                value: batches_display,
            },
            InfoRow {
                label: "Columns".to_string(),
                value: self.schema.fields().len().to_string(),
            },
            InfoRow {
                label: "Size".to_string(),
                value: format_bytes(self.file_size),
            },
        ];

        let info_table = Table::new(&info_rows)
            .with(Remove::row(Rows::first()))
            .with(Style::rounded().remove_horizontals())
            .with(Modify::new(Columns::new(1..)).with(Alignment::right()))
            .to_string();
        writeln!(out, "{info_table}")?;

        writeln!(out)?;
        writeln!(out, "{}", header("Schema"))?;
        render_schema_fields(&self.schema, out)?;

        if !self.custom_metadata.is_empty() {
            writeln!(out)?;
            writeln!(out, "{}", header("File Metadata"))?;
            for (k, v) in &self.custom_metadata {
                let truncated = if v.len() > 60 {
                    format!("{}...", &v[..57])
                } else {
                    v.clone()
                };
                writeln!(out, "  {}: {}", k, truncated)?;
            }
        }

        let has_column_meta = self
            .schema
            .fields()
            .iter()
            .any(|f| !f.metadata().is_empty());
        if has_column_meta {
            writeln!(out)?;
            writeln!(out, "{}", header("Column Metadata"))?;
            for field in self.schema.fields() {
                let field_meta = field.metadata();
                if !field_meta.is_empty() {
                    writeln!(out, "  {}:", field.name())?;
                    for (k, v) in field_meta {
                        writeln!(out, "    {}: {}", k, v)?;
                    }
                }
            }
        }

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
