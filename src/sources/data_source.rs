//! Data source trait and helpers for reading input files.
//!
//! Defines the [`DataSource`] trait that all input formats (Arrow, Parquet, Vortex)
//! implement, plus utilities for estimating row sizes and column sizes via sampling.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::TableProvider,
    execution::{SendableRecordBatchStream, SessionStateBuilder},
    prelude::SessionContext,
};
use futures::StreamExt;

use crate::utils::memory::estimate_fixed_type_bytes;

/// Target number of rows to sample for row_size estimates.
/// Fewer is fine if the file is smaller.
pub const TARGET_SAMPLE_ROWS: usize = 100_000;

#[async_trait]
pub trait DataSource: Send + Sync {
    fn name(&self) -> &str;

    fn schema(&self) -> Result<SchemaRef>;

    /// Total number of rows in this data source.
    fn row_count(&self) -> Result<usize>;

    /// Approximate in-memory bytes per row.
    ///
    /// Uses `estimate_column_sizes` under the hood: fixed-width columns come from
    /// the schema, variable-width from metadata (parquet/vortex) or sampled data.
    ///
    /// # Panics
    ///
    /// Panics if called from a single-threaded (`current_thread`) tokio runtime.
    fn row_size(&self) -> Result<usize> {
        let schema = self.schema()?;
        let all_cols: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let sizes = self.estimate_column_sizes(&all_cols, TARGET_SAMPLE_ROWS)?;
        let total: usize = sizes.values().sum();
        if total == 0 {
            // shouldn't happen, but don't return 0 — it's used as a divisor downstream
            return Ok(1);
        }
        Ok(total)
    }

    async fn as_table_provider(&self, _ctx: &mut SessionContext) -> Result<Arc<dyn TableProvider>> {
        Err(anyhow!("as_table_provider is not implemented"))
    }

    fn supports_table_provider(&self) -> bool {
        false
    }

    /// Cache for variable-width column size estimates. Override to return a
    /// reference to a `Mutex<HashMap>` field on the concrete source. When
    /// provided, variable-width columns are sampled once and cached.
    fn variable_column_size_cache(&self) -> Option<&Mutex<HashMap<String, usize>>> {
        None
    }

    /// Estimate average value sizes in bytes for the given columns.
    ///
    /// Fixed-width columns return exact sizes from the schema (no data access).
    /// Variable-width columns are sampled once and cached — subsequent calls
    /// return from cache with no I/O.
    ///
    /// # Panics
    ///
    /// Panics if called from a single-threaded (`current_thread`) tokio runtime.
    fn estimate_column_sizes(
        &self,
        columns: &[String],
        max_sample_rows: usize,
    ) -> Result<HashMap<String, usize>> {
        let schema = self.schema()?;
        let mut result = HashMap::with_capacity(columns.len());
        let mut variable_cols: Vec<(String, usize)> = Vec::new();

        for col_name in columns {
            match schema.column_with_name(col_name) {
                Some((idx, field)) => {
                    if let Some(size) = estimate_fixed_type_bytes(field.data_type()) {
                        result.insert(col_name.clone(), size);
                    } else {
                        variable_cols.push((col_name.clone(), idx));
                    }
                }
                None => continue,
            }
        }

        if variable_cols.is_empty() {
            return Ok(result);
        }

        if let Some(cache) = self.variable_column_size_cache() {
            let cached = cache
                .lock()
                .map_err(|e| anyhow::anyhow!("column size cache lock poisoned: {e}"))?;
            if !cached.is_empty() {
                for (col_name, _) in &variable_cols {
                    if let Some(&size) = cached.get(col_name) {
                        result.insert(col_name.clone(), size);
                    } else {
                        debug_assert!(
                            false,
                            "variable column {col_name} missing from non-empty cache"
                        );
                    }
                }
                return Ok(result);
            }
        }

        // sample ALL variable-width columns from the schema so the cache
        // covers everything, not just the columns requested this call
        let all_variable_cols: Vec<(String, usize)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| estimate_fixed_type_bytes(f.data_type()).is_none())
            .map(|(idx, f)| (f.name().clone(), idx))
            .collect();

        let mut all_sizes = HashMap::with_capacity(all_variable_cols.len());
        self.estimate_variable_column_sizes(
            &all_variable_cols,
            max_sample_rows,
            &mut all_sizes,
        )?;

        for (col_name, _) in &variable_cols {
            if let Some(&size) = all_sizes.get(col_name) {
                result.insert(col_name.clone(), size);
            }
        }

        if let Some(cache) = self.variable_column_size_cache() {
            *cache
                .lock()
                .map_err(|e| anyhow::anyhow!("column size cache lock poisoned: {e}"))? = all_sizes;
        }

        Ok(result)
    }

    /// Estimate variable-width column sizes by reading actual data. Override for
    /// format-specific optimizations (e.g. parquet column metadata).
    ///
    /// Default reads up to `max_sample_rows` rows via `as_stream` and computes
    /// average per-column byte sizes from Arrow memory.
    ///
    /// # Panics
    ///
    /// Panics if called from a single-threaded (`current_thread`) tokio runtime.
    fn estimate_variable_column_sizes(
        &self,
        variable_cols: &[(String, usize)],
        max_sample_rows: usize,
        result: &mut HashMap<String, usize>,
    ) -> Result<()> {
        sample_variable_column_sizes_from_stream(self, variable_cols, max_sample_rows, result)
    }

    async fn as_stream(&self) -> Result<SendableRecordBatchStream> {
        let mut ctx = SessionContext::new();
        self.as_stream_with_session_context(&mut ctx).await
    }

    async fn as_stream_with_session_context(
        &self,
        ctx: &mut SessionContext,
    ) -> Result<SendableRecordBatchStream> {
        let table = self.as_table_provider(ctx).await?;
        let df = ctx.read_table(table)?;
        df.execute_stream().await.map_err(|e| anyhow!(e))
    }
}

/// Compute the actual data footprint of an Arrow array from its buffer lengths.
///
/// Returns the sum of buffer *lengths* (not capacities), recursing into nested
/// types (List, Struct, etc.). Note: when an array has been sliced (e.g. by
/// DataFusion's batch splitting), `buffer.len()` still reports the full
/// pre-slice length. Callers that sum across sliced sub-batches will
/// double-count shared data. To avoid this, ensure the stream delivers
/// unsplit batches (e.g. by setting a large `batch_size` on SessionConfig).
pub fn array_data_size(array: &dyn arrow::array::Array) -> usize {
    array_data_size_inner(&array.to_data())
}

fn array_data_size_inner(data: &arrow::array::ArrayData) -> usize {
    let mut size: usize = data.buffers().iter().map(|b| b.len()).sum();
    if let Some(nulls) = data.nulls() {
        size += nulls.buffer().len();
    }
    for child in data.child_data() {
        size += array_data_size_inner(child);
    }
    size
}

/// Sample variable-width column sizes from a data source's stream.
///
/// Thin wrapper around [`sample_column_sizes_from_batch_stream`] that opens
/// a stream from the source with a large batch_size to avoid sub-batch splitting.
fn sample_variable_column_sizes_from_stream(
    source: &(impl DataSource + ?Sized),
    variable_cols: &[(String, usize)],
    max_sample_rows: usize,
    result: &mut HashMap<String, usize>,
) -> Result<()> {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            let max_total_rows = max_sample_rows.saturating_mul(10);

            // set batch_size to max_total_rows so DataFusion doesn't split IPC
            // record batches into sub-batches. sub-batches share the same
            // underlying data buffers, so array_data_size double-counts them.
            let config = datafusion::prelude::SessionConfig::new().with_batch_size(max_total_rows);
            let state = SessionStateBuilder::new().with_config(config).build();
            let mut ctx = SessionContext::new_with_state(state);
            let stream = source.as_stream_with_session_context(&mut ctx).await?;
            sample_column_sizes_from_batch_stream(stream, variable_cols, max_sample_rows, result)
                .await
        })
    })
}

/// Sample variable-width column sizes from a record batch stream.
///
/// Reads batches until we have at least `max_sample_rows` non-null values
/// for every variable-width column. This ensures we get a robust estimate
/// even when columns have high null rates. Caps total rows read at
/// `10 * max_sample_rows` to avoid reading the entire file.
///
/// The per-row estimate inherently accounts for null rate: dividing total
/// buffer bytes by total rows gives a per-row average where nulls contribute
/// zero data bytes (only offset/bitmap overhead).
pub(crate) async fn sample_column_sizes_from_batch_stream(
    mut stream: SendableRecordBatchStream,
    variable_cols: &[(String, usize)],
    max_sample_rows: usize,
    result: &mut HashMap<String, usize>,
) -> Result<()> {
    let max_total_rows = max_sample_rows.saturating_mul(10);
    let mut col_bytes: Vec<usize> = vec![0; variable_cols.len()];
    let mut col_non_nulls: Vec<usize> = vec![0; variable_cols.len()];
    let mut total_rows: usize = 0;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }

        for (i, (_col_name, col_idx)) in variable_cols.iter().enumerate() {
            let col = batch.column(*col_idx);
            col_bytes[i] += array_data_size(col.as_ref());
            col_non_nulls[i] += n - col.null_count();
        }
        total_rows += n;

        // a column is satisfied when we have enough non-null samples, or
        // when we've read enough rows to be confident it's truly all-null
        // (not just null-prefixed from sorted/partitioned data)
        let all_satisfied = col_non_nulls
            .iter()
            .all(|&nn| nn >= max_sample_rows || (total_rows >= max_sample_rows && nn == 0));
        if all_satisfied || total_rows >= max_total_rows {
            break;
        }
    }

    if total_rows == 0 {
        for (col_name, _) in variable_cols {
            result.insert(col_name.clone(), 32);
        }
        return Ok(());
    }

    for (i, (col_name, _)) in variable_cols.iter().enumerate() {
        let estimate = if col_non_nulls[i] == 0 {
            0
        } else {
            col_bytes[i] / total_rows
        };
        result.insert(col_name.clone(), estimate);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    fn avg_size(array: &dyn arrow::array::Array) -> usize {
        array_data_size(array) / array.len()
    }

    #[test]
    fn test_array_data_size_utf8() {
        // 4 strings of 10 chars each = 40 bytes data + 20 bytes offsets (5 × i32)
        let values: Vec<&str> = vec!["abcdefghij"; 4];
        let array = StringArray::from(values);
        let size = array_data_size(&array);
        // offsets: 5 * 4 = 20, data: 40, total = 60
        assert_eq!(size, 60);
        assert_eq!(avg_size(&array), 15);
    }

    #[test]
    fn test_array_data_size_large_utf8() {
        let values: Vec<&str> = vec!["abcdefghij"; 4];
        let array = LargeStringArray::from(values);
        let size = array_data_size(&array);
        // offsets: 5 * 8 = 40 (i64), data: 40, total = 80
        assert_eq!(size, 80);
        assert_eq!(avg_size(&array), 20);
    }

    #[test]
    fn test_array_data_size_binary() {
        let values: Vec<&[u8]> = vec![&[0u8; 16]; 4]; // 16-byte blobs
        let array = BinaryArray::from(values);
        let size = array_data_size(&array);
        // offsets: 5 * 4 = 20, data: 64, total = 84
        assert_eq!(size, 84);
        assert_eq!(avg_size(&array), 21);
    }

    #[test]
    fn test_array_data_size_large_binary() {
        let values: Vec<&[u8]> = vec![&[0u8; 16]; 4];
        let array = LargeBinaryArray::from(values);
        let size = array_data_size(&array);
        // offsets: 5 * 8 = 40, data: 64, total = 104
        assert_eq!(size, 104);
        assert_eq!(avg_size(&array), 26);
    }

    #[test]
    fn test_array_data_size_list_of_i32() {
        // 4 lists, each with 3 i32s
        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let field = Arc::new(Field::new_list_field(
            arrow::datatypes::DataType::Int32,
            false,
        ));
        let offsets = OffsetBuffer::new(vec![0i32, 3, 6, 9, 12].into());
        let array = ListArray::new(field, offsets, Arc::new(values), None);
        let size = array_data_size(&array);
        // list offsets: 5 * 4 = 20, child i32 data: 12 * 4 = 48, total = 68
        assert_eq!(size, 68);
    }

    #[test]
    fn test_array_data_size_list_of_utf8() {
        // 3 lists of strings: ["ab", "cd"], ["efgh"], ["i", "jk", "lmn"]
        let string_array = StringArray::from(vec!["ab", "cd", "efgh", "i", "jk", "lmn"]);
        let field = Arc::new(Field::new_list_field(
            arrow::datatypes::DataType::Utf8,
            false,
        ));
        let offsets = OffsetBuffer::new(vec![0i32, 2, 3, 6].into());
        let array = ListArray::new(field, offsets, Arc::new(string_array), None);
        let size = array_data_size(&array);
        // list offsets: 4 * 4 = 16
        // string offsets: 7 * 4 = 28, string data: 2+2+4+1+2+3 = 14
        // total = 16 + 28 + 14 = 58
        assert_eq!(size, 58);
    }

    #[test]
    fn test_array_data_size_struct_with_variable_children() {
        // struct { name: Utf8, value: Int32 } — 3 rows
        let names = StringArray::from(vec!["alice", "bob", "charlie"]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("name", arrow::datatypes::DataType::Utf8, false)),
                Arc::new(names) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "value",
                    arrow::datatypes::DataType::Int32,
                    false,
                )),
                Arc::new(values) as ArrayRef,
            ),
        ]);
        let size = array_data_size(&struct_array);
        // struct: no direct buffers
        // name (Utf8): offsets 4*4=16, data 5+3+7=15 → 31
        // value (Int32): 3*4=12
        // total = 43
        assert_eq!(size, 43);
    }

    #[test]
    fn test_array_data_size_nested_list_of_struct() {
        // List<Struct<name: Utf8, id: Int32>> — verifies recursive traversal
        let names = StringArray::from(vec!["a", "bb", "ccc", "dddd"]);
        let ids = Int32Array::from(vec![1, 2, 3, 4]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("name", arrow::datatypes::DataType::Utf8, false)),
                Arc::new(names) as ArrayRef,
            ),
            (
                Arc::new(Field::new("id", arrow::datatypes::DataType::Int32, false)),
                Arc::new(ids) as ArrayRef,
            ),
        ]);
        let struct_field = Field::new(
            "item",
            arrow::datatypes::DataType::Struct(
                vec![
                    Field::new("name", arrow::datatypes::DataType::Utf8, false),
                    Field::new("id", arrow::datatypes::DataType::Int32, false),
                ]
                .into(),
            ),
            false,
        );
        let offsets = OffsetBuffer::new(vec![0i32, 2, 4].into());
        let list_array = ListArray::new(
            Arc::new(struct_field),
            offsets,
            Arc::new(struct_array),
            None,
        );
        let size = array_data_size(&list_array);
        // list offsets: 3 * 4 = 12
        // struct: no direct buffers
        //   name (Utf8): offsets 5*4=20, data 1+2+3+4=10 → 30
        //   id (Int32): 4*4=16
        // total = 12 + 30 + 16 = 58
        assert_eq!(size, 58);
    }

    #[test]
    fn test_array_data_size_dictionary() {
        // Dictionary<Int32, Utf8> — common for categorical strings
        let keys = Int32Array::from(vec![0, 1, 0, 2, 1]);
        let values = StringArray::from(vec!["cat", "dog", "bird"]);
        let dict_array = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();
        let size = array_data_size(&dict_array);
        // keys: 5 * 4 = 20
        // values (Utf8): offsets 4*4=16, data 3+3+4=10 → 26
        // total = 46
        assert_eq!(size, 46);
    }

    #[test]
    fn test_array_data_size_with_nulls() {
        let array = StringArray::from(vec![Some("hello"), None, Some("world"), None]);
        let size = array_data_size(&array);
        // offsets: 5 * 4 = 20, data: 5+0+5+0 = 10, null bitmap: 1 byte
        // total = 31
        assert_eq!(size, 31);
    }

    #[test]
    fn test_array_data_size_large_list() {
        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        let field = Arc::new(Field::new_list_field(
            arrow::datatypes::DataType::Int32,
            false,
        ));
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i64, 3, 6, 9, 12].into());
        let array = arrow::array::LargeListArray::new(field, offsets, Arc::new(values), None);
        let size = array_data_size(&array);
        // list offsets: 5 * 8 = 40 (i64), child i32 data: 12 * 4 = 48, total = 88
        assert_eq!(size, 88);
    }

    #[test]
    fn test_array_data_size_list_view() {
        use arrow::buffer::ScalarBuffer;
        let values = Int32Array::from(vec![10, 20, 30, 40, 50, 60]);
        let field = Arc::new(Field::new_list_field(
            arrow::datatypes::DataType::Int32,
            false,
        ));
        let offsets = ScalarBuffer::from(vec![0i32, 2, 5]);
        let sizes = ScalarBuffer::from(vec![2i32, 3, 1]);
        let array =
            arrow::array::ListViewArray::try_new(field, offsets, sizes, Arc::new(values), None)
                .unwrap();
        let size = array_data_size(&array);
        // offsets: 3 * 4 = 12, sizes: 3 * 4 = 12, child i32 data: 6 * 4 = 24, total = 48
        assert_eq!(size, 48);
    }

    #[test]
    fn test_array_data_size_map() {
        let keys = StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);
        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let entries = arrow::array::StructArray::from(vec![
            (
                Arc::new(Field::new("key", arrow::datatypes::DataType::Utf8, false)),
                Arc::new(keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "value",
                    arrow::datatypes::DataType::Int32,
                    false,
                )),
                Arc::new(values) as ArrayRef,
            ),
        ]);
        let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
        // 3 map entries: {"a":1,"b":2}, {"c":3}, {"d":4,"e":5,"f":6}
        let offsets = OffsetBuffer::new(vec![0i32, 2, 3, 6].into());
        let array = arrow::array::MapArray::new(field, offsets, entries, None, false);
        let size = array_data_size(&array);
        // map offsets: 4 * 4 = 16
        // struct: no direct buffers
        //   key (Utf8): offsets 7*4=28, data 1+1+1+1+1+1=6 → 34
        //   value (Int32): 6*4=24
        // total = 16 + 34 + 24 = 74
        assert_eq!(size, 74);
    }

    #[test]
    fn test_array_data_size_utf8_view() {
        // 16-byte views + out-of-line data buffers for strings >12 bytes
        let array = arrow::array::StringViewArray::from(vec![
            "short",                                   // 5 bytes, inlined
            "hello world!",                            // 12 bytes, inlined
            "this string is longer than twelve bytes", // 39 bytes, out-of-line
            "tiny",                                    // 4 bytes, inlined
        ]);
        let size = array_data_size(&array);
        // views: 4 * 16 = 64
        // out-of-line data buffer: 39 bytes
        // total = 64 + 39 = 103
        assert_eq!(size, 103);
    }

    #[test]
    fn test_array_data_size_binary_view() {
        let array = arrow::array::BinaryViewArray::from(vec![
            &[0u8; 4][..],  // inlined
            &[1u8; 16][..], // out-of-line (>12 bytes)
            &[2u8; 8][..],  // inlined
        ]);
        let size = array_data_size(&array);
        // views: 3 * 16 = 48
        // out-of-line data buffer: 16 bytes
        // total = 64
        assert_eq!(size, 64);
    }

    #[test]
    fn test_array_data_size_dense_union() {
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::UnionFields;
        let string_child = StringArray::from(vec!["hello", "world"]);
        let int_child = Int32Array::from(vec![42]);
        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("str", arrow::datatypes::DataType::Utf8, false),
                Field::new("int", arrow::datatypes::DataType::Int32, false),
            ],
        )
        .unwrap();
        // 3 values: str("hello"), int(42), str("world")
        let type_ids = ScalarBuffer::from(vec![0i8, 1, 0]);
        let offsets = ScalarBuffer::from(vec![0i32, 0, 1]);
        let array = arrow::array::UnionArray::try_new(
            fields,
            type_ids,
            Some(offsets),
            vec![Arc::new(string_child) as ArrayRef, Arc::new(int_child)],
        )
        .unwrap();
        let size = array_data_size(&array);
        // type_ids: 3 * 1 = 3
        // offsets: 3 * 4 = 12
        // string child: offsets 3*4=12, data 5+5=10 → 22
        // int child: 1*4=4
        // total = 3 + 12 + 22 + 4 = 41
        assert_eq!(size, 41);
    }

    #[test]
    fn test_array_data_size_sparse_union() {
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::UnionFields;
        let string_child = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let int_child = Int32Array::from(vec![None, Some(42), None]);
        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("str", arrow::datatypes::DataType::Utf8, true),
                Field::new("int", arrow::datatypes::DataType::Int32, true),
            ],
        )
        .unwrap();
        let type_ids = ScalarBuffer::from(vec![0i8, 1, 0]);
        let array = arrow::array::UnionArray::try_new(
            fields,
            type_ids,
            None,
            vec![Arc::new(string_child) as ArrayRef, Arc::new(int_child)],
        )
        .unwrap();
        let size = array_data_size(&array);
        // type_ids: 3 * 1 = 3
        // string child: offsets 4*4=16, data 5+0+5=10, null bitmap 1 → 27
        // int child: 3*4=12, null bitmap 1 → 13
        // total = 3 + 27 + 13 = 43
        assert_eq!(size, 43);
    }

    #[test]
    fn test_array_data_size_run_end_encoded() {
        let run_ends = Int32Array::from(vec![2, 5, 6]); // 3 runs
        let values = StringArray::from(vec!["aaa", "bbbbb", "c"]);
        let array = arrow::array::RunArray::try_new(&run_ends, &values).unwrap();
        let size = array_data_size(&array);
        // run_ends child: 3 * 4 = 12
        // values child: offsets 4*4=16, data 3+5+1=9 → 25
        // total = 12 + 25 = 37
        assert_eq!(size, 37);
    }

    #[test]
    fn test_array_data_size_null() {
        let array = arrow::array::NullArray::new(100);
        let size = array_data_size(&array);
        assert_eq!(size, 0);
    }
}
