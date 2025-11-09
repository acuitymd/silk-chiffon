use crate::converters::arrow_to_arrow::ArrowToArrowConverter;
use crate::converters::partition_arrow::output_template::OutputTemplate;
use crate::converters::partition_arrow::partition_writer::{
    ArrowWriterBuilder, ParquetWriterBuilder, PartitionWriter, WriterBuilder,
};
use crate::utils::arrow_io::ArrowIPCFormat;
use crate::{
    ArrowCompression, BloomFilterConfig, ParquetCompression, ParquetStatistics,
    ParquetWriterVersion, QueryDialect, SortColumn, SortDirection, SortSpec,
};
use anyhow::{Context, Result, anyhow};
use arrow::array::{Array, AsArray, GenericByteArray, PrimitiveArray, RecordBatch};
use arrow::compute::BatchCoalescer;
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, GenericStringType, Int8Type, Int16Type, Int32Type,
    Int64Type, SchemaBuilder, SchemaRef, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::ipc::reader::FileReader;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;

pub const NULL_PLACEHOLDER: &str = "__NULL__";

#[derive(Debug, Clone)]
pub enum OutputFormat {
    Arrow {
        compression: Option<ArrowCompression>,
        ipc_format: ArrowIPCFormat,
    },
    Parquet {
        compression: Option<ParquetCompression>,
        statistics: ParquetStatistics,
        max_row_group_size: usize,
        writer_version: ParquetWriterVersion,
        no_dictionary: bool,
        bloom_filters: BloomFilterConfig,
        write_sorted_metadata: bool,
    },
}

#[derive(Debug, Serialize, Clone)]
pub struct PartitioningResult {
    pub path: PathBuf,
    pub row_count: usize,
}

struct PartitioningState {
    current_value: Value,
    writer: Option<Box<dyn PartitionWriter>>,
    coalescer: BatchCoalescer,
    result_map: HashMap<String, PartitioningResult>,
    current_row_count: usize,
}

impl PartitioningState {
    fn new(schema: SchemaRef, record_batch_size: usize) -> Self {
        Self {
            current_value: Value::None,
            writer: None,
            coalescer: BatchCoalescer::new(schema, record_batch_size),
            result_map: HashMap::new(),
            current_row_count: 0,
        }
    }
}

pub struct PartitionArrowConverter {
    input_path: String,
    partition_column: String,
    output_template: OutputTemplate,
    output_format: OutputFormat,
    record_batch_size: usize,
    sort_spec: Option<SortSpec>,
    create_dirs: bool,
    overwrite: bool,
    query: Option<String>,
    dialect: QueryDialect,
    exclude_columns: Vec<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub(super) enum Value {
    String(Option<String>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    None, // a missing/undefined value, not a null
}

impl Value {
    /// Returns true only if this is the None variant (missing value),
    /// not if it's a Some(None) null value
    pub(super) fn is_none(&self) -> bool {
        matches!(self, Value::None)
    }
}

#[derive(Debug)]
pub(super) enum ArrayValues<'a> {
    String(&'a GenericByteArray<GenericStringType<i32>>),
    Int8(&'a PrimitiveArray<Int8Type>),
    Int16(&'a PrimitiveArray<Int16Type>),
    Int32(&'a PrimitiveArray<Int32Type>),
    Int64(&'a PrimitiveArray<Int64Type>),
    UInt8(&'a PrimitiveArray<UInt8Type>),
    UInt16(&'a PrimitiveArray<UInt16Type>),
    UInt32(&'a PrimitiveArray<UInt32Type>),
    UInt64(&'a PrimitiveArray<UInt64Type>),
    Float32(&'a PrimitiveArray<Float32Type>),
    Float64(&'a PrimitiveArray<Float64Type>),
}

macro_rules! impl_primitive_ops {
    ($array:expr, $i:expr, $j:expr, $variant:ident) => {{
        let null_i = $array.is_null($i);
        let null_j = $array.is_null($j);
        if null_i != null_j {
            false
        } else if null_i {
            true
        } else {
            $array.value($i) == $array.value($j)
        }
    }};
}

macro_rules! impl_get_value {
    ($array:expr, $index:expr, $variant:ident) => {{
        if $array.is_null($index) {
            Value::$variant(None)
        } else {
            Value::$variant(Some($array.value($index)))
        }
    }};
}

impl<'a> ArrayValues<'a> {
    /// Compare values at two indices without allocation
    pub(super) fn values_equal(&self, i: usize, j: usize) -> bool {
        match self {
            ArrayValues::String(array) => {
                let null_i = array.is_null(i);
                let null_j = array.is_null(j);
                if null_i != null_j {
                    return false;
                }
                if null_i {
                    return true;
                }
                array.value(i) == array.value(j)
            }
            ArrayValues::Int8(array) => impl_primitive_ops!(array, i, j, Int8),
            ArrayValues::Int16(array) => impl_primitive_ops!(array, i, j, Int16),
            ArrayValues::Int32(array) => impl_primitive_ops!(array, i, j, Int32),
            ArrayValues::Int64(array) => impl_primitive_ops!(array, i, j, Int64),
            ArrayValues::UInt8(array) => impl_primitive_ops!(array, i, j, UInt8),
            ArrayValues::UInt16(array) => impl_primitive_ops!(array, i, j, UInt16),
            ArrayValues::UInt32(array) => impl_primitive_ops!(array, i, j, UInt32),
            ArrayValues::UInt64(array) => impl_primitive_ops!(array, i, j, UInt64),
            ArrayValues::Float32(array) => impl_primitive_ops!(array, i, j, Float32),
            ArrayValues::Float64(array) => impl_primitive_ops!(array, i, j, Float64),
        }
    }

    pub(super) fn get_value(&self, index: usize) -> Value {
        match self {
            ArrayValues::String(array) => {
                if array.is_null(index) {
                    Value::String(None)
                } else {
                    Value::String(Some(array.value(index).to_string()))
                }
            }
            ArrayValues::Int8(array) => impl_get_value!(array, index, Int8),
            ArrayValues::Int16(array) => impl_get_value!(array, index, Int16),
            ArrayValues::Int32(array) => impl_get_value!(array, index, Int32),
            ArrayValues::Int64(array) => impl_get_value!(array, index, Int64),
            ArrayValues::UInt8(array) => impl_get_value!(array, index, UInt8),
            ArrayValues::UInt16(array) => impl_get_value!(array, index, UInt16),
            ArrayValues::UInt32(array) => impl_get_value!(array, index, UInt32),
            ArrayValues::UInt64(array) => impl_get_value!(array, index, UInt64),
            ArrayValues::Float32(array) => impl_get_value!(array, index, Float32),
            ArrayValues::Float64(array) => impl_get_value!(array, index, Float64),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SplitConversionResult {
    pub output_files: HashMap<String, PartitioningResult>,
}

impl PartitionArrowConverter {
    pub fn new(input_path: String, partition_column: String, output_template: String) -> Self {
        Self {
            input_path,
            partition_column,
            output_template: OutputTemplate::new(output_template),
            output_format: OutputFormat::Arrow {
                compression: None,
                ipc_format: ArrowIPCFormat::default(),
            },
            record_batch_size: 122_880,
            sort_spec: None,
            create_dirs: true,
            overwrite: false,
            query: None,
            dialect: QueryDialect::default(),
            exclude_columns: vec![],
        }
    }

    fn format_value(&self, value: &Value) -> String {
        macro_rules! format_option {
            ($opt:expr) => {
                match $opt {
                    Some(v) => v.to_string(),
                    None => NULL_PLACEHOLDER.to_string(),
                }
            };
        }

        match value {
            Value::String(opt) => format_option!(opt),
            Value::Int8(opt) => format_option!(opt),
            Value::Int16(opt) => format_option!(opt),
            Value::Int32(opt) => format_option!(opt),
            Value::Int64(opt) => format_option!(opt),
            Value::UInt8(opt) => format_option!(opt),
            Value::UInt16(opt) => format_option!(opt),
            Value::UInt32(opt) => format_option!(opt),
            Value::UInt64(opt) => format_option!(opt),
            Value::Float32(opt) => format_option!(opt),
            Value::Float64(opt) => format_option!(opt),
            Value::None => NULL_PLACEHOLDER.to_string(),
        }
    }

    pub fn with_output_format(mut self, format: OutputFormat) -> Self {
        self.output_format = format;
        self
    }

    pub fn with_record_batch_size(mut self, size: usize) -> Self {
        self.record_batch_size = size;
        self
    }

    pub fn with_sort_spec(mut self, sort: Option<SortSpec>) -> Self {
        self.sort_spec = sort;
        self
    }

    pub fn with_create_dirs(mut self, create: bool) -> Self {
        self.create_dirs = create;
        self
    }

    pub fn with_overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    pub fn with_query(mut self, query: Option<String>) -> Self {
        self.query = query;
        self
    }

    pub fn with_dialect(mut self, dialect: QueryDialect) -> Self {
        self.dialect = dialect;
        self
    }

    pub fn with_exclude_columns(mut self, exclude_columns: Vec<String>) -> Self {
        self.exclude_columns = exclude_columns;
        self
    }

    async fn create_writer(
        &self,
        path: &Path,
        schema: &SchemaRef,
    ) -> Result<Box<dyn PartitionWriter>> {
        let parent = path.parent();

        if self.create_dirs && parent.is_some() {
            fs::create_dir_all(parent.unwrap())?;
        }

        if path.exists() && !self.overwrite {
            return Err(anyhow!(
                "Output file {} already exists. Use --overwrite to replace.",
                path.display()
            ));
        }

        match &self.output_format {
            OutputFormat::Arrow {
                compression,
                ipc_format,
            } => {
                ArrowWriterBuilder::new()
                    .with_compression(*compression)
                    .with_ipc_format(ipc_format.clone())
                    .build_writer(path, schema)
                    .await
            }
            OutputFormat::Parquet {
                compression,
                statistics,
                max_row_group_size,
                writer_version,
                no_dictionary,
                bloom_filters,
                write_sorted_metadata,
            } => {
                ParquetWriterBuilder::new()
                    .with_compression(*compression)
                    .with_statistics(*statistics)
                    .with_max_row_group_size(*max_row_group_size)
                    .with_writer_version(*writer_version)
                    .with_no_dictionary(*no_dictionary)
                    .with_bloom_filters(bloom_filters.clone())
                    .with_write_sorted_metadata(*write_sorted_metadata)
                    .with_sort_spec(self.sort_spec.clone())
                    .build_writer(path, schema)
                    .await
            }
        }
    }

    async fn prepare_sorted_input(&self) -> Result<NamedTempFile> {
        let sorted_path = NamedTempFile::new()?;
        let mut arrow_converter = ArrowToArrowConverter::new(&self.input_path, sorted_path.path())?;

        let mut sort_columns = vec![SortColumn {
            name: self.partition_column.clone(),
            direction: SortDirection::Ascending,
        }];

        if let Some(user_sort) = &self.sort_spec {
            sort_columns.extend(user_sort.columns.clone());
        }

        arrow_converter = arrow_converter.with_sorting(SortSpec {
            columns: sort_columns,
        });

        if let Some(query) = &self.query {
            arrow_converter = arrow_converter
                .with_query(Some(query.clone()))
                .with_dialect(self.dialect);
        }

        arrow_converter.convert().await?;

        Ok(sorted_path)
    }

    fn create_array_values<'a>(&self, column: &'a dyn Array) -> Result<ArrayValues<'a>> {
        match column.data_type() {
            DataType::Utf8 => Ok(ArrayValues::String(column.as_string())),
            DataType::Int8 => Ok(ArrayValues::Int8(column.as_primitive::<Int8Type>())),
            DataType::Int16 => Ok(ArrayValues::Int16(column.as_primitive::<Int16Type>())),
            DataType::Int32 => Ok(ArrayValues::Int32(column.as_primitive::<Int32Type>())),
            DataType::Int64 => Ok(ArrayValues::Int64(column.as_primitive::<Int64Type>())),
            DataType::UInt8 => Ok(ArrayValues::UInt8(column.as_primitive::<UInt8Type>())),
            DataType::UInt16 => Ok(ArrayValues::UInt16(column.as_primitive::<UInt16Type>())),
            DataType::UInt32 => Ok(ArrayValues::UInt32(column.as_primitive::<UInt32Type>())),
            DataType::UInt64 => Ok(ArrayValues::UInt64(column.as_primitive::<UInt64Type>())),
            DataType::Float32 => Ok(ArrayValues::Float32(column.as_primitive::<Float32Type>())),
            DataType::Float64 => Ok(ArrayValues::Float64(column.as_primitive::<Float64Type>())),
            _ => Err(anyhow!(
                "Unsupported column type for partitionting: {:?}. Supported types: Utf8, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64",
                column.data_type()
            )),
        }
    }

    async fn process_batch(
        &self,
        schema: SchemaRef,
        batch: RecordBatch,
        partitioning_state: &mut PartitioningState,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let column = batch
            .column_by_name(&self.partition_column)
            .ok_or_else(|| {
                anyhow!(
                    "Split column '{}' not found in schema",
                    self.partition_column
                )
            })?;

        let array = self.create_array_values(column)?;

        if partitioning_state.current_value.is_none() {
            partitioning_state.current_value = array.get_value(0);
            let initial_path = self.output_template.resolve(
                &self.partition_column,
                &self.format_value(&partitioning_state.current_value),
            );

            partitioning_state.result_map.insert(
                self.format_value(&partitioning_state.current_value),
                PartitioningResult {
                    path: initial_path.clone(),
                    row_count: 0,
                },
            );
            partitioning_state.writer = Some(self.create_writer(&initial_path, &schema).await?);
        }

        let mut from_row_index = 0;
        while from_row_index < batch.num_rows() {
            let mut to_row_index = from_row_index + 1;
            while to_row_index < batch.num_rows()
                && array.values_equal(from_row_index, to_row_index)
            {
                to_row_index += 1;
            }

            let value = array.get_value(from_row_index);

            if !partitioning_state.current_value.is_none()
                && partitioning_state.current_value != value
            {
                self.finish_current_partition(partitioning_state)?;

                let new_path = self
                    .output_template
                    .resolve(&self.partition_column, &self.format_value(&value));

                partitioning_state.result_map.insert(
                    self.format_value(&value),
                    PartitioningResult {
                        path: new_path.clone(),
                        row_count: 0,
                    },
                );
                partitioning_state.writer = Some(self.create_writer(&new_path, &schema).await?);
                partitioning_state.coalescer =
                    BatchCoalescer::new(schema.clone(), self.record_batch_size);
            }

            partitioning_state.current_value = value;

            let slice = self.slice_batch(
                &batch,
                schema.clone(),
                from_row_index,
                to_row_index - from_row_index,
            )?;
            partitioning_state.current_row_count += slice.num_rows();
            partitioning_state.coalescer.push_batch(slice)?;

            self.write_pending_batches(partitioning_state)?;

            from_row_index = to_row_index;
        }

        Ok(())
    }

    fn slice_batch(
        &self,
        batch: &RecordBatch,
        schema: SchemaRef,
        offset: usize,
        length: usize,
    ) -> Result<RecordBatch> {
        assert!((offset + length) <= batch.num_rows());

        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let column = batch
                    .column_by_name(field.name())
                    .ok_or_else(|| anyhow!("Column not found in batch: {}", field.name()))?;
                Ok(column.slice(offset, length))
            })
            .collect::<Result<Vec<_>>>()?;

        RecordBatch::try_new(schema, columns).context("Failed to slice batch")
    }

    fn exclude_columns(&self, schema: &SchemaRef) -> Result<SchemaRef> {
        let mut builder = SchemaBuilder::new();

        for column in schema.fields() {
            if !self.exclude_columns.contains(column.name()) {
                builder.push(column.clone());
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn write_pending_batches(&self, partitioning_state: &mut PartitioningState) -> Result<()> {
        while let Some(completed_batch) = partitioning_state.coalescer.next_completed_batch() {
            partitioning_state
                .writer
                .as_mut()
                .unwrap()
                .write_batch(&completed_batch)?;
        }

        Ok(())
    }

    fn finish_current_partition(&self, partitioning_state: &mut PartitioningState) -> Result<()> {
        partitioning_state.coalescer.finish_buffered_batch()?;

        self.write_pending_batches(partitioning_state)?;

        if let Some(mut w) = partitioning_state.writer.take() {
            w.finish()?;

            let value_key = self.format_value(&partitioning_state.current_value);
            if let Some(result) = partitioning_state.result_map.get_mut(&value_key) {
                result.row_count = partitioning_state.current_row_count;
            }
        }

        partitioning_state.current_row_count = 0;
        Ok(())
    }

    pub async fn convert(&self) -> Result<SplitConversionResult> {
        let sorted_path = self.prepare_sorted_input().await?;

        let file_reader = FileReader::try_new_buffered(File::open(sorted_path.path())?, None)?;
        let schema = self.exclude_columns(&file_reader.schema())?;

        let mut partitioning_state = PartitioningState::new(schema.clone(), self.record_batch_size);

        for batch in file_reader {
            let batch = batch?;
            self.process_batch(schema.clone(), batch, &mut partitioning_state)
                .await?;
        }

        self.finish_current_partition(&mut partitioning_state)?;

        Ok(SplitConversionResult {
            output_files: partitioning_state.result_map,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_batch(num_rows: usize, partition_values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int64Array::from((0..i64::try_from(num_rows).unwrap()).collect::<Vec<_>>());
        let category_array = Int32Array::from(partition_values);
        #[expect(clippy::cast_precision_loss)]
        let value_array =
            Float64Array::from((0..num_rows).map(|i| i as f64 * 1.5).collect::<Vec<_>>());
        let name_array = StringArray::from(
            (0..num_rows)
                .map(|i| format!("item_{i}"))
                .collect::<Vec<_>>(),
        );

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(category_array),
                Arc::new(value_array),
                Arc::new(name_array),
            ],
        )
        .unwrap()
    }

    fn create_test_file(temp_dir: &TempDir, batches: Vec<RecordBatch>) -> PathBuf {
        let path = temp_dir.path().join("input.arrow");
        let file = File::create(&path).unwrap();
        let mut writer = StreamWriter::try_new(file, &batches[0].schema()).unwrap();
        for batch in batches {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
        path
    }

    #[tokio::test]
    async fn test_basic_partition() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");

        let batch = create_test_batch(9, vec![1, 1, 1, 2, 2, 2, 3, 3, 3]);
        let input_path = create_test_file(&temp_dir, vec![batch]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "category".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        )
        .with_create_dirs(true);

        let created_files = converter.convert().await.unwrap();

        assert_eq!(created_files.output_files.len(), 3);
        assert!(output_dir.join("1.arrow").exists());
        assert!(output_dir.join("2.arrow").exists());
        assert!(output_dir.join("3.arrow").exists());
    }

    #[tokio::test]
    async fn test_partition_with_nulls() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Int32, true),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    None,
                    None,
                    Some(2),
                    Some(2),
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
            ],
        )
        .unwrap();

        let input_path = create_test_file(&temp_dir, vec![batch]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "category".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        )
        .with_create_dirs(true);

        let created_files = converter.convert().await.unwrap();

        assert_eq!(created_files.output_files.len(), 3);
        assert!(output_dir.join("1.arrow").exists());
        assert!(output_dir.join("2.arrow").exists());
        assert!(output_dir.join("__NULL__.arrow").exists());
    }

    #[tokio::test]
    async fn test_partition_with_sorting() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");

        let batch = create_test_batch(9, vec![2, 1, 3, 1, 2, 3, 3, 2, 1]);
        let input_path = create_test_file(&temp_dir, vec![batch]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "category".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        )
        .with_create_dirs(true)
        .with_sort_spec(Some("value:desc".parse().unwrap()));

        let created_files = converter.convert().await.unwrap();

        assert_eq!(created_files.output_files.len(), 3);

        let file = File::open(output_dir.join("1.arrow")).unwrap();
        let reader = FileReader::try_new(file, None).unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        let value_col = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 1..value_col.len() {
            assert!(
                value_col.value(i - 1) >= value_col.value(i),
                "Values should be sorted descending"
            );
        }
    }

    #[tokio::test]
    async fn test_overwrite_protection() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");
        fs::create_dir_all(&output_dir).unwrap();

        let existing_file = output_dir.join("1.arrow");
        File::create(&existing_file).unwrap();

        let batch = create_test_batch(3, vec![1, 1, 1]);
        let input_path = create_test_file(&temp_dir, vec![batch]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "category".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        );

        let result = converter.convert().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_overwrite_enabled() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");
        fs::create_dir_all(&output_dir).unwrap();

        let existing_file = output_dir.join("1.arrow");
        File::create(&existing_file).unwrap();

        let batch = create_test_batch(3, vec![1, 1, 1]);
        let input_path = create_test_file(&temp_dir, vec![batch]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "category".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        )
        .with_overwrite(true);

        let result = converter.convert().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_string_partition_column() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "north", "north", "south", "south", "east", "east",
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])),
            ],
        )
        .unwrap();

        let input_path = create_test_file(&temp_dir, vec![batch]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "region".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        )
        .with_create_dirs(true);

        let created_files = converter.convert().await.unwrap();

        assert_eq!(created_files.output_files.len(), 3);
        assert!(output_dir.join("north.arrow").exists());
        assert!(output_dir.join("south.arrow").exists());
        assert!(output_dir.join("east.arrow").exists());
    }

    #[tokio::test]
    async fn test_parquet_output() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");

        let batch = create_test_batch(6, vec![1, 1, 2, 2, 3, 3]);
        let input_path = create_test_file(&temp_dir, vec![batch]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "category".to_string(),
            format!("{}/{{value}}.parquet", output_dir.display()),
        )
        .with_output_format(OutputFormat::Parquet {
            compression: Some(ParquetCompression::Snappy),
            statistics: ParquetStatistics::Page,
            max_row_group_size: 1024,
            writer_version: ParquetWriterVersion::V2,
            no_dictionary: false,
            bloom_filters: BloomFilterConfig::None,
            write_sorted_metadata: false,
        })
        .with_create_dirs(true);

        let created_files = converter.convert().await.unwrap();

        assert_eq!(created_files.output_files.len(), 3);
        assert!(output_dir.join("1.parquet").exists());
        assert!(output_dir.join("2.parquet").exists());
        assert!(output_dir.join("3.parquet").exists());
    }

    #[tokio::test]
    async fn test_large_batch_handling() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");

        let partition_values: Vec<i32> = (0..10000).map(|i| i / 100).collect();
        let batch = create_test_batch(10000, partition_values);
        let input_path = create_test_file(&temp_dir, vec![batch]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "category".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        )
        .with_record_batch_size(1000)
        .with_create_dirs(true);

        let created_files = converter.convert().await.unwrap();

        assert_eq!(created_files.output_files.len(), 100);
    }

    #[tokio::test]
    async fn test_multi_batch_input() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");

        let batch1 = create_test_batch(3, vec![1, 1, 1]);
        let batch2 = create_test_batch(3, vec![2, 2, 2]);
        let batch3 = create_test_batch(3, vec![3, 3, 3]);
        let input_path = create_test_file(&temp_dir, vec![batch1, batch2, batch3]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "category".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        )
        .with_create_dirs(true);

        let created_files = converter.convert().await.unwrap();

        assert_eq!(created_files.output_files.len(), 3);

        for i in 1..=3 {
            let file = File::open(output_dir.join(format!("{i}.arrow"))).unwrap();
            let reader = FileReader::try_new(file, None).unwrap();
            let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3);
        }
    }

    #[tokio::test]
    async fn test_missing_column_error() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");

        let batch = create_test_batch(3, vec![1, 2, 3]);
        let input_path = create_test_file(&temp_dir, vec![batch]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "nonexistent".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        );

        let result = converter.convert().await;
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("nonexistent"),
            "Error message: {error_msg}"
        );
    }

    #[test]
    fn test_value_enum() {
        assert!(Value::None.is_none());
        assert!(!Value::String(Some("test".to_string())).is_none());
        assert!(!Value::Int32(Some(42)).is_none());
        assert!(!Value::Int32(None).is_none());

        assert_eq!(Value::Int32(Some(42)), Value::Int32(Some(42)));
        assert_ne!(Value::Int32(Some(42)), Value::Int32(Some(43)));
        assert_ne!(
            Value::Int32(Some(42)),
            Value::String(Some("42".to_string()))
        );
        assert_ne!(Value::Int32(Some(42)), Value::Int32(None));
    }

    #[test]
    fn test_array_values() {
        let int_array = Int32Array::from(vec![Some(1), Some(1), Some(2), None, None]);
        let array_values = ArrayValues::Int32(&int_array);

        assert!(array_values.values_equal(0, 1));
        assert!(!array_values.values_equal(0, 2));
        assert!(array_values.values_equal(3, 4));
        assert!(!array_values.values_equal(0, 3));

        assert_eq!(array_values.get_value(0), Value::Int32(Some(1)));
        assert_eq!(array_values.get_value(3), Value::Int32(None));
    }

    #[tokio::test]
    async fn test_empty_batch_handling() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let empty_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![] as Vec<i64>)),
                Arc::new(Int32Array::from(vec![] as Vec<i32>)),
                Arc::new(Float64Array::from(vec![] as Vec<f64>)),
            ],
        )
        .unwrap();

        let normal_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 2])),
                Arc::new(Int32Array::from(vec![1, 1, 1])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();

        let input_path = create_test_file(&temp_dir, vec![empty_batch, normal_batch]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "category".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        )
        .with_create_dirs(true);

        let result = converter.convert().await;
        assert!(result.is_ok());

        let created_files = result.unwrap();
        assert_eq!(created_files.output_files.len(), 1);
        assert!(output_dir.join("1.arrow").exists());
    }

    #[tokio::test]
    async fn test_partition_with_query() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");

        // create test data with mixed categories
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Int32, false),
            Field::new("status", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(Int32Array::from(vec![1, 2, 1, 2, 1, 2])),
                Arc::new(StringArray::from(vec![
                    "active", "inactive", "active", "active", "inactive", "active",
                ])),
            ],
        )
        .unwrap();

        let input_path = create_test_file(&temp_dir, vec![batch]);

        // filter to only active records before partitionting
        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "category".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        )
        .with_query(Some(
            "SELECT * FROM data WHERE status = 'active'".to_string(),
        ))
        .with_create_dirs(true);

        let created_files = converter.convert().await.unwrap();

        // should have 2 categories but only active records
        assert_eq!(created_files.output_files.len(), 2);
        assert!(output_dir.join("1.arrow").exists());
        assert!(output_dir.join("2.arrow").exists());

        // check category 1 file
        let file = File::open(output_dir.join("1.arrow")).unwrap();
        let reader = FileReader::try_new(file, None).unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // only active records with category 1

        // check category 2 file
        let file = File::open(output_dir.join("2.arrow")).unwrap();
        let reader = FileReader::try_new(file, None).unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // only active records with category 2
    }

    #[tokio::test]
    async fn test_exclude_columns() {
        let temp_dir = TempDir::new().unwrap();
        let output_dir = temp_dir.path().join("output");

        let batch = create_test_batch(3, vec![1, 1, 1]);
        let input_path = create_test_file(&temp_dir, vec![batch.clone()]);

        let converter = PartitionArrowConverter::new(
            input_path.to_str().unwrap().to_string(),
            "category".to_string(),
            format!("{}/{{value}}.arrow", output_dir.display()),
        )
        .with_exclude_columns(vec!["value".to_string()]);

        let created_files = converter.convert().await.unwrap();

        assert_eq!(created_files.output_files.len(), 1);
        assert!(output_dir.join("1.arrow").exists());

        let file = File::open(output_dir.join("1.arrow")).unwrap();
        let reader = FileReader::try_new(file, None).unwrap();
        let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 3);
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batches[0].num_columns(), 3);
        assert!(batches[0].column_by_name("category").is_some());
        assert!(batches[0].column_by_name("id").is_some());
        assert!(batches[0].column_by_name("name").is_some());
        assert!(batches[0].column_by_name("value").is_none());
    }
}
