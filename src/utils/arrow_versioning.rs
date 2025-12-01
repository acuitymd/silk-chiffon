use std::io::Cursor;

use anyhow::Result;
use arrow::array::RecordBatch as RecordBatchv57;
use arrow::ipc::reader::StreamReader as StreamReaderv57;
use arrow::ipc::writer::StreamWriter as StreamWriterv57;
use arrow_array::RecordBatch as RecordBatchv56;
use arrow_ipc::reader::StreamReader as StreamReaderv56;
use arrow_ipc::writer::StreamWriter as StreamWriterv56;

// The following functions are necessary because vortex only supports arrow 56, not 57.
// We need to convert the arrow 57 batch to arrow 56 so we can use the vortex dtype.
// We write to a buffer in memory for performance but then immediately read it back into
// the correct format for vortex. This temporarily doubles the memory usage for a given batch.
// We do the reverse conversion when reading the batch from the vortex file, as well.

pub fn convert_arrow_56_to_57(batch_v56: &RecordBatchv56) -> Result<RecordBatchv57> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriterv56::try_new(&mut buffer, batch_v56.schema().as_ref())?;
        writer.write(batch_v56)?;
        writer.finish()?;
    }

    let cursor = Cursor::new(buffer);
    let mut reader = StreamReaderv57::try_new(cursor, None)?;
    let batch_v57 = reader
        .next()
        .ok_or_else(|| anyhow::anyhow!("No batch in IPC stream"))??;

    Ok(batch_v57)
}

pub fn convert_arrow_57_to_56(batch_v57: &RecordBatchv57) -> Result<RecordBatchv56> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriterv57::try_new(&mut buffer, batch_v57.schema().as_ref())?;
        writer.write(batch_v57)?;
        writer.finish()?;
    }

    let cursor = Cursor::new(buffer);
    let mut reader = StreamReaderv56::try_new(cursor, None)?;
    let batch_v56 = reader
        .next()
        .ok_or_else(|| anyhow::anyhow!("No batch in IPC stream"))??;

    Ok(batch_v56)
}
