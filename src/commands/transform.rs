use crate::{
    ArrowCompression, ArrowIPCFormat, BloomFilterConfig, ColumnEncodingConfig, DataFormat,
    ListOutputsFormat, ParquetCompression, ParquetEncoding, ParquetStatistics,
    ParquetWriterVersion, SortSpec, TransformCommand,
    io_strategies::{OutputFileInfo, output_strategy::SinkFactory, path_template::PathTemplate},
    operations::{query::QueryOperation, sort::SortOperation},
    pipeline::Pipeline,
    sinks::{
        arrow::{ArrowSink, ArrowSinkOptions},
        data_sink::DataSink,
        parquet::{ParquetSink, ParquetSinkOptions},
        vortex::{VortexSink, VortexSinkOptions},
    },
    sources::{
        arrow::ArrowDataSource, data_source::DataSource, parquet::ParquetDataSource,
        vortex::VortexDataSource,
    },
};
use anyhow::{Result, anyhow};
use arrow::datatypes::SchemaRef;
use glob::glob;
use owo_colors::OwoColorize;
use tabled::{builder::Builder, settings::Style};

pub async fn run(args: TransformCommand) -> Result<()> {
    let TransformCommand {
        from,
        from_many,
        to,
        to_many,
        by,
        exclude_columns,
        list_outputs,
        create_dirs,
        overwrite,
        query,
        dialect,
        sort_by,
        input_format,
        output_format,
        arrow_compression,
        arrow_format,
        arrow_record_batch_size,
        parquet_compression,
        parquet_bloom_all,
        parquet_bloom_column,
        parquet_row_group_size,
        parquet_parallelism,
        parquet_statistics,
        parquet_writer_version,
        parquet_no_dictionary,
        parquet_column_dictionary,
        parquet_column_no_dictionary,
        parquet_encoding,
        parquet_column_encoding,
        parquet_sorted_metadata,
        vortex_record_batch_size,
    } = args;

    let bloom_filter = if let Some(all_config) = parquet_bloom_all {
        BloomFilterConfig::All(all_config)
    } else if !parquet_bloom_column.is_empty() {
        BloomFilterConfig::Columns(parquet_bloom_column)
    } else {
        BloomFilterConfig::None
    };

    validate_encoding_version_compatibility(
        parquet_writer_version,
        parquet_encoding,
        &parquet_column_encoding,
    )?;

    let mut pipeline = Pipeline::new().with_query_dialect(dialect);

    let (input_paths, should_glob) = if let Some(single_input) = from {
        (vec![single_input], false)
    } else {
        (from_many, true)
    };

    let setup_result: Result<()> = {
        if !should_glob && input_paths.len() == 1 {
            let input_path = &input_paths[0];
            let detected_input_format = detect_format(input_path, input_format)?;

            let source: Box<dyn DataSource> = match detected_input_format {
                DataFormat::Arrow => Box::new(ArrowDataSource::new(input_path.clone())),
                DataFormat::Parquet => Box::new(ParquetDataSource::new(input_path.clone())),
                DataFormat::Vortex => Box::new(VortexDataSource::new(input_path.clone())),
            };

            pipeline = pipeline.with_input_strategy_with_single_source(source);
            Ok(())
        } else {
            let mut expanded_paths = Vec::new();

            for pattern in &input_paths {
                for entry in glob(pattern)
                    .map_err(|e| anyhow!("Error expanding glob pattern {}: {}", pattern, e))?
                {
                    expanded_paths.push(
                        entry
                            .map_err(|e| anyhow!("Error decoding file path: {}", e))?
                            .to_string_lossy()
                            .to_string(),
                    );
                }
            }

            expanded_paths.sort();
            expanded_paths.dedup();

            if expanded_paths.is_empty() {
                anyhow::bail!("No input files found");
            }

            let mut sources: Vec<Box<dyn DataSource>> = Vec::new();
            let mut schema: Option<SchemaRef> = None;
            for input_path in expanded_paths {
                let detected_input_format = detect_format(&input_path, input_format)?;
                let source: Box<dyn DataSource> = match detected_input_format {
                    DataFormat::Arrow => Box::new(ArrowDataSource::new(input_path.clone())),
                    DataFormat::Parquet => Box::new(ParquetDataSource::new(input_path.clone())),
                    DataFormat::Vortex => Box::new(VortexDataSource::new(input_path.clone())),
                };
                if let Some(ref schema) = schema {
                    let source_schema = source.schema()?;
                    if *schema != source_schema {
                        anyhow::bail!(
                            "Schema mismatch for input file {} (does not match other file(s))",
                            &input_path
                        );
                    }
                } else {
                    schema = Some(source.schema()?);
                }
                sources.push(source);
            }
            pipeline = pipeline.with_input_strategy_with_multiple_sources(sources);
            Ok(())
        }
    };

    setup_result?;

    let list_outputs_format = list_outputs;

    // The overall sort order is determined by the following:
    //
    //   1. The sort order specified by the partition columns
    //   2. The sort order specified by the user
    //
    // We need the data sorted by the partition columns first so that the data can
    // be partitioned into individual files per partition as we output the data. Any
    // other alternative would require us to either:
    //
    //   1. Keep the files open per partition for the entire duration of the partition,
    //      which would be inefficient and require us to manage a lot of open file handles
    //      and use a lot of memory.
    //   2. Write multiple files per partition, managing how many file handles are open
    //      at any given time and how much memory is currently being used. If you still
    //      wanted to have a single file per partition you would need to come back later
    //      and merge the files together.
    //
    // Once we have sorted the data for partitioning there is nothing to sort for those
    // columns within each file since they are just a single value, so we remove them from
    // the user-specified sort order.

    let partition_sort_spec = if let Some(ref by_cols) = by {
        SortSpec::from(
            by_cols
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<_>>(),
        )
    } else {
        SortSpec::default()
    };

    let user_sort_spec = sort_by.clone().unwrap_or(SortSpec::default());

    let user_sort_spec_without_partition_cols =
        user_sort_spec.without_columns_named(&partition_sort_spec.column_names());

    let parquet_sort_spec =
        if parquet_sorted_metadata && !user_sort_spec_without_partition_cols.is_empty() {
            Some(user_sort_spec_without_partition_cols.clone())
        } else {
            None
        };

    let mut full_sort_spec = partition_sort_spec.clone();
    full_sort_spec.extend(&user_sort_spec_without_partition_cols);

    let sink_factory = create_sink_factory(
        output_format,
        arrow_compression,
        arrow_format,
        arrow_record_batch_size,
        parquet_compression,
        parquet_row_group_size,
        parquet_parallelism,
        parquet_statistics,
        parquet_writer_version,
        parquet_no_dictionary,
        parquet_column_dictionary,
        parquet_column_no_dictionary,
        parquet_encoding,
        parquet_column_encoding,
        bloom_filter,
        parquet_sort_spec,
        vortex_record_batch_size,
    )?;

    if let Some(output_path) = to {
        pipeline = pipeline.with_output_strategy_with_single_sink(
            output_path,
            sink_factory,
            exclude_columns.clone(),
            create_dirs,
            overwrite,
        );
    } else if let Some(template) = to_many {
        let path_template = PathTemplate::new(template);
        // relying on the partition_sort_spec since it appropriately handles duplicate column names
        // and is used to perform the sort operation that we will be partitioning on later
        let partition_columns = partition_sort_spec.column_names();

        pipeline = pipeline.with_output_strategy_with_partitioned_sink(
            partition_columns,
            path_template,
            sink_factory,
            exclude_columns.clone(),
            create_dirs,
            overwrite,
            list_outputs.unwrap_or_default(),
        );
    }

    if let Some(q) = query {
        pipeline = pipeline.with_operation(Box::new(QueryOperation::new(q)));
    }

    if !full_sort_spec.is_empty() {
        pipeline = pipeline.with_operation(Box::new(SortOperation::new(full_sort_spec.columns)));
    }

    let files = pipeline.execute().await?;

    if let Some(format) = list_outputs_format {
        print_output_files(&files, format)?;
    }

    Ok(())
}

fn print_output_files(files: &[OutputFileInfo], format: ListOutputsFormat) -> Result<()> {
    match format {
        ListOutputsFormat::None => {}
        ListOutputsFormat::Text => {
            if files.is_empty() {
                return Ok(());
            }

            let mut builder = Builder::default();

            let mut header: Vec<String> = files
                .first()
                .map(|f| {
                    f.partition_values
                        .iter()
                        .map(|pv| to_title_case(&pv.column))
                        .collect()
                })
                .unwrap_or_default();
            header.push("Path".to_string());
            header.push("Row Count".to_string());

            let colored_header: Vec<String> = header.iter().map(|h| h.bold().to_string()).collect();
            builder.push_record(colored_header);

            // sort by path for consistent output
            let mut sorted_files = files.to_vec();
            sorted_files.sort_by(|a, b| a.path.cmp(&b.path));

            for file in &sorted_files {
                let mut row: Vec<String> = file
                    .partition_values
                    .iter()
                    .map(|pv| format_json_value(&pv.value).green().to_string())
                    .collect();
                row.push(file.path.clone());
                row.push(file.row_count.to_string().cyan().to_string());
                builder.push_record(row);
            }

            let table = builder.build().with(Style::rounded()).to_string();
            println!("{}", table);
        }
        ListOutputsFormat::Json => {
            let json = serde_json::to_string_pretty(files)?;
            println!("{}", json);
        }
    }
    Ok(())
}

fn format_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        _ => value.to_string(),
    }
}

fn to_title_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn detect_format(path: &str, explicit_format: Option<DataFormat>) -> Result<DataFormat> {
    if let Some(format) = explicit_format {
        return Ok(format);
    }

    let path_obj = std::path::Path::new(path);
    if let Some(ext) = path_obj.extension() {
        if ext.eq_ignore_ascii_case("arrow") {
            return Ok(DataFormat::Arrow);
        } else if ext.eq_ignore_ascii_case("parquet") {
            return Ok(DataFormat::Parquet);
        } else if ext.eq_ignore_ascii_case("vortex") {
            return Ok(DataFormat::Vortex);
        }
    }

    Err(anyhow!(
        "Could not detect format from path '{}'. Use --input-format or --output-format to specify explicitly.",
        path
    ))
}

fn validate_encoding_version_compatibility(
    writer_version: Option<ParquetWriterVersion>,
    default_encoding: Option<ParquetEncoding>,
    column_encodings: &[ColumnEncodingConfig],
) -> Result<()> {
    // some encodings are only supported in v2
    if !matches!(writer_version, Some(ParquetWriterVersion::V1)) {
        return Ok(());
    }

    let mut v2_encodings = Vec::new();

    if let Some(enc) = default_encoding
        && enc.requires_v2()
    {
        v2_encodings.push(format!("default encoding '{}'", enc));
    }

    for col_enc in column_encodings {
        if col_enc.encoding.requires_v2() {
            v2_encodings.push(format!(
                "column '{}' with encoding '{}'",
                col_enc.name, col_enc.encoding
            ));
        }
    }

    if !v2_encodings.is_empty() {
        anyhow::bail!(
            "v2-only encodings cannot be used with parquet-writer-version=v1:\n  - {}",
            v2_encodings.join("\n  - ")
        );
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn create_sink_factory(
    output_format: Option<DataFormat>,
    arrow_compression: Option<ArrowCompression>,
    arrow_format: Option<ArrowIPCFormat>,
    arrow_record_batch_size: Option<usize>,
    parquet_compression: Option<ParquetCompression>,
    parquet_row_group_size: Option<usize>,
    parquet_parallelism: Option<usize>,
    parquet_statistics: Option<ParquetStatistics>,
    parquet_writer_version: Option<ParquetWriterVersion>,
    parquet_no_dictionary: bool,
    parquet_column_dictionary: Vec<String>,
    parquet_column_no_dictionary: Vec<String>,
    parquet_encoding: Option<ParquetEncoding>,
    parquet_column_encoding: Vec<ColumnEncodingConfig>,
    parquet_bloom_filter: BloomFilterConfig,
    parquet_sort_spec: Option<SortSpec>,
    vortex_record_batch_size: Option<usize>,
) -> Result<SinkFactory> {
    Ok(Box::new(move |path: String, schema: SchemaRef| {
        let detected_format = detect_format(&path, output_format)?;

        let sink: Box<dyn DataSink> = match detected_format {
            DataFormat::Arrow => {
                let mut options = ArrowSinkOptions::new();
                if let Some(compression) = arrow_compression {
                    options = options.with_compression(compression);
                }
                if let Some(format) = arrow_format.clone() {
                    options = options.with_format(format);
                }
                if let Some(batch_size) = arrow_record_batch_size {
                    options = options.with_record_batch_size(batch_size);
                }
                Box::new(ArrowSink::create(path.into(), &schema, options)?)
            }
            DataFormat::Parquet => {
                let mut options = ParquetSinkOptions::new();
                if let Some(compression) = parquet_compression {
                    options = options.with_compression(compression);
                }
                if let Some(row_group_size) = parquet_row_group_size {
                    options = options.with_max_row_group_size(row_group_size);
                }
                options = options.with_max_parallelism(parquet_parallelism);
                if let Some(stats) = parquet_statistics {
                    options = options.with_statistics(stats);
                }
                if let Some(version) = parquet_writer_version {
                    options = options.with_writer_version(version);
                }
                if parquet_no_dictionary {
                    options = options.with_no_dictionary(true);
                }
                options = options
                    .with_column_dictionary(parquet_column_dictionary.clone())
                    .with_column_no_dictionary(parquet_column_no_dictionary.clone())
                    .with_encoding(parquet_encoding)
                    .with_column_encodings(parquet_column_encoding.clone())
                    .with_bloom_filters(parquet_bloom_filter.clone());
                if let Some(sort_spec) = parquet_sort_spec.clone() {
                    options = options.with_sort_spec(sort_spec);
                }
                Box::new(ParquetSink::create(path.into(), &schema, &options)?)
            }
            DataFormat::Vortex => {
                let mut options = VortexSinkOptions::new();
                if let Some(batch_size) = vortex_record_batch_size {
                    options = options.with_record_batch_size(batch_size);
                }
                Box::new(VortexSink::create(path.into(), &schema, options)?)
            }
        };

        Ok(sink)
    }))
}
