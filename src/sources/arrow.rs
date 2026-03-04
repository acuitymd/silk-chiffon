use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use anyhow::{Result, bail};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::{FileReader, StreamReader, read_footer_length};
use arrow::ipc::{MessageHeader, root_as_footer, root_as_message};
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider, execution::options::ArrowReadOptions, prelude::SessionContext,
};
use uuid::Uuid;

use crate::sources::data_source::DataSource;

const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

#[derive(Debug)]
pub struct ArrowDataSource {
    path: String,
}

impl ArrowDataSource {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

#[async_trait]
impl DataSource for ArrowDataSource {
    fn name(&self) -> &str {
        "arrow"
    }

    fn schema(&self) -> Result<SchemaRef> {
        if let Ok(reader) = FileReader::try_new(File::open(&self.path)?, None) {
            return Ok(reader.schema());
        }

        if let Ok(reader) = StreamReader::try_new(File::open(&self.path)?, None) {
            return Ok(reader.schema());
        }

        anyhow::bail!("Could not read Arrow file: {}", &self.path)
    }

    fn row_count(&self) -> Result<usize> {
        let mut file = File::open(&self.path)?;

        if let Some(count) = file_format_row_count(&mut file)? {
            return Ok(count);
        }

        // rewind and try stream format
        file.seek(SeekFrom::Start(0))?;
        if let Some(count) = stream_format_row_count(&mut file)? {
            return Ok(count);
        }

        bail!("Could not read Arrow file: {}", self.path)
    }

    async fn as_table_provider(&self, ctx: &mut SessionContext) -> Result<Arc<dyn TableProvider>> {
        let table_name = format!("arrow_{}", Uuid::new_v4().as_simple());
        ctx.register_arrow(&table_name, &self.path, ArrowReadOptions::default())
            .await?;
        let table = ctx.table(&table_name).await?;
        Ok(table.into_view())
    }

    fn supports_table_provider(&self) -> bool {
        true
    }
}

/// Get exact row count from an Arrow IPC file by parsing the footer and
/// reading each block's message header without decoding any record batch data.
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]
fn file_format_row_count(file: &mut File) -> Result<Option<usize>> {
    let file_len = file.metadata()?.len();
    if file_len < 10 {
        return Ok(None);
    }

    // read the 10-byte trailer: 4-byte footer length + 6-byte magic
    let mut trailer = [0u8; 10];
    file.seek(SeekFrom::End(-10))?;
    file.read_exact(&mut trailer)?;

    let footer_len = match read_footer_length(trailer) {
        Ok(len) => len,
        Err(_) => return Ok(None),
    };

    if footer_len + 10 > file_len as usize {
        return Ok(None);
    }
    let mut footer_data = vec![0u8; footer_len];
    file.seek(SeekFrom::End(-10 - footer_len as i64))?;
    file.read_exact(&mut footer_data)?;

    let footer = match root_as_footer(&footer_data) {
        Ok(f) => f,
        Err(_) => return Ok(None),
    };

    let blocks = match footer.recordBatches() {
        Some(b) => b,
        None => return Ok(Some(0)),
    };

    let mut total_rows: usize = 0;
    let mut meta_buf = Vec::new();

    for block in blocks.iter() {
        let offset = block.offset() as u64;
        let raw_meta_len = block.metaDataLength();
        if raw_meta_len < 0 {
            continue;
        }
        let meta_len = raw_meta_len as usize;
        if !(8..=64 * 1024 * 1024).contains(&meta_len) {
            continue;
        }

        if file.seek(SeekFrom::Start(offset)).is_err() {
            continue;
        }
        meta_buf.resize(meta_len, 0);
        if file.read_exact(&mut meta_buf).is_err() {
            continue;
        }

        // skip continuation marker if present
        let msg_bytes = if meta_buf.len() >= 4 && meta_buf[..4] == CONTINUATION_MARKER {
            &meta_buf[8..]
        } else {
            &meta_buf[4..]
        };

        if let Ok(message) = root_as_message(msg_bytes)
            && message.header_type() == MessageHeader::RecordBatch
            && let Some(rb) = message.header_as_record_batch()
            && rb.length() >= 0
        {
            total_rows = total_rows.saturating_add(rb.length() as usize);
        }
    }

    Ok(Some(total_rows))
}

/// Get exact row count from an Arrow IPC stream by parsing message headers
/// and seeking past record batch bodies without decoding them.
#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]
fn stream_format_row_count(file: &mut File) -> Result<Option<usize>> {
    let mut total_rows: usize = 0;
    let mut meta_len_buf = [0u8; 4];
    let mut meta_buf = Vec::new();
    let mut saw_schema = false;

    loop {
        match file.read_exact(&mut meta_len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(_) if !saw_schema => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        // handle continuation marker
        if meta_len_buf == CONTINUATION_MARKER {
            match file.read_exact(&mut meta_len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        }

        let meta_len = i32::from_le_bytes(meta_len_buf);
        if meta_len == 0 {
            break;
        }
        if meta_len < 0 {
            if !saw_schema {
                return Ok(None);
            }
            bail!("invalid metadata length in Arrow IPC stream");
        }
        let meta_len = meta_len as usize;
        if meta_len > 64 * 1024 * 1024 {
            bail!("metadata length {meta_len} exceeds 64MB limit");
        }

        meta_buf.resize(meta_len, 0);
        file.read_exact(&mut meta_buf)?;

        let message = match root_as_message(&meta_buf) {
            Ok(m) => m,
            Err(_) if !saw_schema => return Ok(None),
            Err(e) => bail!("failed to parse IPC message: {e}"),
        };

        let raw_body_len = message.bodyLength();
        if raw_body_len < 0 {
            bail!("invalid body length in Arrow IPC stream");
        }
        let body_len = raw_body_len as u64;

        match message.header_type() {
            MessageHeader::Schema => {
                saw_schema = true;
            }
            MessageHeader::RecordBatch => {
                if let Some(rb) = message.header_as_record_batch()
                    && rb.length() >= 0
                {
                    total_rows = total_rows.saturating_add(rb.length() as usize);
                }
            }
            _ => {}
        }

        // skip past the body without reading it
        file.seek(SeekFrom::Current(body_len as i64))?;
    }

    if saw_schema {
        Ok(Some(total_rows))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;

    use super::*;

    const TEST_ARROW_FILE_PATH: &str = "tests/files/people.file.arrow";
    const TEST_ARROW_STREAM_PATH: &str = "tests/files/people.stream.arrow";

    #[test]
    fn test_new() {
        let source = ArrowDataSource::new(TEST_ARROW_FILE_PATH.to_string());
        assert_eq!(source.path, TEST_ARROW_FILE_PATH);
    }

    #[test]
    fn test_name() {
        let source = ArrowDataSource::new(TEST_ARROW_FILE_PATH.to_string());
        assert_eq!(source.name(), "arrow");
    }

    #[tokio::test]
    async fn test_as_table_provider_file_format() {
        let source = ArrowDataSource::new(TEST_ARROW_FILE_PATH.to_string());
        let mut ctx = SessionContext::new();
        let table_provider = source.as_table_provider(&mut ctx).await.unwrap();
        assert!(!table_provider.schema().fields().is_empty());
    }

    #[tokio::test]
    async fn test_as_table_provider_stream_format() {
        let source = ArrowDataSource::new(TEST_ARROW_STREAM_PATH.to_string());
        let mut ctx = SessionContext::new();
        let table_provider = source.as_table_provider(&mut ctx).await.unwrap();
        assert!(!table_provider.schema().fields().is_empty());
    }

    #[tokio::test]
    async fn test_as_table_provider_can_be_queried() {
        let source = ArrowDataSource::new(TEST_ARROW_FILE_PATH.to_string());
        let mut ctx = SessionContext::new();
        let table_provider = source.as_table_provider(&mut ctx).await.unwrap();

        let ctx = SessionContext::new();
        ctx.register_table("test_table", table_provider).unwrap();

        let df = ctx.sql("SELECT * FROM test_table LIMIT 1").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        let batch = batches[0].clone();
        assert!(batch.num_rows() > 0);
    }

    #[tokio::test]
    async fn test_as_stream_file_format() {
        let source = ArrowDataSource::new(TEST_ARROW_FILE_PATH.to_string());
        let mut stream = source.as_stream().await.unwrap();

        assert!(!stream.schema().fields().is_empty());
        let batch = stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.is_none());
        assert!(batch.num_rows() > 0);
    }

    #[tokio::test]
    async fn test_as_stream_stream_format() {
        let source = ArrowDataSource::new(TEST_ARROW_STREAM_PATH.to_string());
        let mut stream = source.as_stream().await.unwrap();

        assert!(!stream.schema().fields().is_empty());
        let batch = stream.next().await.unwrap().unwrap();
        assert!(stream.next().await.is_none());
        assert!(batch.num_rows() > 0);
    }

    #[tokio::test]
    async fn test_row_count_file_format() {
        let source = ArrowDataSource::new(TEST_ARROW_FILE_PATH.to_string());
        let count = source.row_count().unwrap();

        // verify against actually streaming all rows
        let mut stream = source.as_stream().await.unwrap();
        let mut streamed = 0;
        while let Some(batch) = stream.next().await {
            streamed += batch.unwrap().num_rows();
        }
        assert_eq!(count, streamed);
    }

    #[tokio::test]
    async fn test_row_count_stream_format() {
        let source = ArrowDataSource::new(TEST_ARROW_STREAM_PATH.to_string());
        let count = source.row_count().unwrap();

        let mut stream = source.as_stream().await.unwrap();
        let mut streamed = 0;
        while let Some(batch) = stream.next().await {
            streamed += batch.unwrap().num_rows();
        }
        assert_eq!(count, streamed);
    }

    #[test]
    fn test_row_count_written_file_format() {
        use arrow::ipc::writer::FileWriter;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.arrow");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec!["d", "e"])),
            ],
        )
        .unwrap();

        let file = File::create(&path).unwrap();
        let mut writer = FileWriter::try_new(file, &schema).unwrap();
        writer.write(&batch1).unwrap();
        writer.write(&batch2).unwrap();
        writer.finish().unwrap();

        let source = ArrowDataSource::new(path.to_string_lossy().to_string());
        assert_eq!(source.row_count().unwrap(), 5);
    }

    #[test]
    fn test_row_count_written_stream_format() {
        use arrow::ipc::writer::StreamWriter;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.arrows");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6, 7])),
                Arc::new(StringArray::from(vec!["d", "e", "f", "g"])),
            ],
        )
        .unwrap();

        let file = File::create(&path).unwrap();
        let mut writer = StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch1).unwrap();
        writer.write(&batch2).unwrap();
        writer.finish().unwrap();

        let source = ArrowDataSource::new(path.to_string_lossy().to_string());
        assert_eq!(source.row_count().unwrap(), 7);
    }
}
