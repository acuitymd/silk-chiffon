use super::{FileFormat, FileInfo, FileInspector};
use anyhow::{Context, Result, bail};
use arrow::ipc::reader::{FileReader as ArrowFileReader, StreamReader as ArrowStreamReader};
use arrow_ipc::reader::read_footer_length;
use arrow_ipc::{root_as_footer, root_as_message};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

// max sizes to prevent unbounded allocations from malformed files
const MAX_FOOTER_SIZE: usize = 64 * 1024 * 1024;
const MAX_METADATA_SIZE: usize = 16 * 1024 * 1024;
const MAX_BATCHES: usize = 10_000_000;
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

pub struct ArrowIpcInspector;

impl FileInspector for ArrowIpcInspector {
    fn inspect(&self, path: &Path) -> Result<FileInfo> {
        match try_inspect_file(path) {
            Ok(info) => Ok(info),
            Err(_) => try_inspect_stream(path)
                .context("failed to read as arrow file or stream - file may be corrupted"),
        }
    }
}

fn try_inspect_file(path: &Path) -> Result<FileInfo> {
    let file = File::open(path).context("failed to open arrow file")?;
    let reader = ArrowFileReader::try_new(file, None).context("not a valid arrow file format")?;

    let schema = reader.schema();
    let num_batches = reader.num_batches();

    let mut file = File::open(path).context("failed to reopen arrow file")?;
    let total_rows = read_file_batch_metadata(&mut file, num_batches)?;

    Ok(FileInfo {
        path: path.to_path_buf(),
        format: FileFormat::ArrowFile,
        schema,
        num_rows: Some(total_rows),
    })
}

/// reads just the message metadata from each block to get row counts without reading data
fn read_file_batch_metadata<R: Read + Seek>(reader: &mut R, num_batches: usize) -> Result<i64> {
    // read footer length from last 10 bytes
    reader.seek(SeekFrom::End(-10))?;
    let mut buf = [0u8; 10];
    reader.read_exact(&mut buf)?;
    let footer_len = read_footer_length(buf).context("invalid footer")?;

    if footer_len > MAX_FOOTER_SIZE {
        bail!(
            "footer size {} exceeds maximum {}",
            footer_len,
            MAX_FOOTER_SIZE
        );
    }

    // read footer - footer_len is already usize from read_footer_length
    let footer_len_i64 = i64::try_from(footer_len).context("footer length overflow")?;
    reader.seek(SeekFrom::End(-10 - footer_len_i64))?;
    let mut footer_buf = vec![0u8; footer_len];
    reader.read_exact(&mut footer_buf)?;

    let footer =
        root_as_footer(&footer_buf).map_err(|e| anyhow::anyhow!("invalid footer: {:?}", e))?;

    let blocks = footer
        .recordBatches()
        .ok_or_else(|| anyhow::anyhow!("no record batches in footer"))?;

    let _ = num_batches;
    let mut total_rows = 0i64;
    let mut metadata_buf = Vec::with_capacity(4096);

    if blocks.len() > MAX_BATCHES {
        bail!("too many batches in file (> {})", MAX_BATCHES);
    }

    for block in blocks.iter() {
        let metadata_len =
            usize::try_from(block.metaDataLength()).context("metadata length overflow")?;

        if metadata_len > MAX_METADATA_SIZE {
            bail!(
                "metadata size {} exceeds maximum {}",
                metadata_len,
                MAX_METADATA_SIZE
            );
        }

        let block_offset = u64::try_from(block.offset()).context("block offset overflow")?;

        reader.seek(SeekFrom::Start(block_offset))?;
        metadata_buf.clear();
        metadata_buf.resize(metadata_len, 0);
        reader.read_exact(&mut metadata_buf)?;

        if metadata_buf.len() < 4 {
            continue;
        }

        // handle optional continuation marker (IPC v1 vs v2+)
        let msg_buf = if metadata_buf[..4] == CONTINUATION_MARKER {
            if metadata_buf.len() < 8 {
                continue;
            }
            &metadata_buf[8..]
        } else {
            &metadata_buf[4..]
        };

        if let Ok(message) = root_as_message(msg_buf)
            && let Some(header) = message.header_as_record_batch()
        {
            let num_rows = header.length(); // i64 from Arrow
            if num_rows >= 0 {
                total_rows = total_rows.saturating_add(num_rows);
            }
        }
    }

    Ok(total_rows)
}

fn try_inspect_stream(path: &Path) -> Result<FileInfo> {
    let file = File::open(path).context("failed to open arrow stream")?;
    let buf_reader = BufReader::new(file);
    let reader =
        ArrowStreamReader::try_new(buf_reader, None).context("not a valid arrow stream format")?;

    let schema = reader.schema();

    let file = File::open(path).context("failed to reopen arrow stream")?;
    let total_rows = read_stream_batch_metadata(file)?;

    Ok(FileInfo {
        path: path.to_path_buf(),
        format: FileFormat::ArrowStream,
        schema,
        num_rows: Some(total_rows),
    })
}

/// reads stream message headers to get batch info without reading body data
fn read_stream_batch_metadata<R: Read + Seek>(mut reader: R) -> Result<i64> {
    let mut total_rows = 0i64;
    let mut batch_count = 0usize;
    let mut metadata_buf = Vec::with_capacity(4096);

    loop {
        let mut len_buf = [0u8; 4];
        if reader.read_exact(&mut len_buf).is_err() {
            break;
        }

        let metadata_len_i32 = if len_buf == CONTINUATION_MARKER {
            reader.read_exact(&mut len_buf)?;
            i32::from_le_bytes(len_buf)
        } else {
            i32::from_le_bytes(len_buf)
        };

        if metadata_len_i32 <= 0 {
            break;
        }

        let metadata_len = usize::try_from(metadata_len_i32).context("metadata length overflow")?;

        if metadata_len > MAX_METADATA_SIZE {
            bail!(
                "metadata size {} exceeds maximum {}",
                metadata_len,
                MAX_METADATA_SIZE
            );
        }

        metadata_buf.clear();
        metadata_buf.resize(metadata_len, 0);
        reader.read_exact(&mut metadata_buf)?;

        if let Ok(message) = root_as_message(&metadata_buf) {
            let body_len = message.bodyLength();

            if let Some(header) = message.header_as_record_batch() {
                batch_count += 1;
                if batch_count > MAX_BATCHES {
                    bail!("too many batches in stream (> {})", MAX_BATCHES);
                }

                let num_rows = header.length();
                if num_rows >= 0 {
                    total_rows = total_rows.saturating_add(num_rows);
                }
            }

            // skip past the body data (8-byte aligned)
            // guard against overflow: body_len near i64::MAX would wrap on +7
            if body_len > 0 && body_len <= i64::MAX - 7 {
                let aligned_body_len = (body_len + 7) & !7;
                reader.seek(SeekFrom::Current(aligned_body_len))?;
            }
        } else {
            break;
        }
    }

    Ok(total_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test_helpers::{file_helpers, test_data};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_inspect_arrow_file() {
        let temp_dir = tempdir().unwrap();
        let arrow_path = temp_dir.path().join("test.arrow");

        let schema = test_data::simple_schema();
        let batch =
            test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

        file_helpers::write_arrow_file(&arrow_path, &schema, vec![batch]).unwrap();

        let inspector = ArrowIpcInspector;
        let info = inspector.inspect(&arrow_path).unwrap();

        assert_eq!(info.format, FileFormat::ArrowFile);
        assert_eq!(info.num_rows, Some(3));
        assert_eq!(info.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_inspect_arrow_stream() {
        let temp_dir = tempdir().unwrap();
        let arrow_path = temp_dir.path().join("test.arrows");

        let schema = test_data::simple_schema();
        let batch =
            test_data::create_batch_with_ids_and_names(&schema, &[1, 2, 3], &["a", "b", "c"]);

        file_helpers::write_arrow_stream(&arrow_path, &schema, vec![batch]).unwrap();

        let inspector = ArrowIpcInspector;
        let info = inspector.inspect(&arrow_path).unwrap();

        assert_eq!(info.format, FileFormat::ArrowStream);
        assert_eq!(info.num_rows, Some(3));
    }
}
