use crate::{
    ArrowCompression, BloomFilterConfig, DataFormat, InputSpec, ListOutputsFormat, OutputSpec,
    ParquetCompression, ParquetStatistics, ParquetWriterVersion, SortSpec, TransformCommand,
    io_strategies::{output_strategy::SinkFactory, path_template::PathTemplate},
    operations::{query::QueryOperation, sort::SortOperation},
    pipeline::Pipeline,
    sinks::{
        arrow::{ArrowSink, ArrowSinkOptions},
        data_sink::DataSink,
        parquet::{ParquetSink, ParquetSinkOptions},
    },
    sources::{
        arrow_file::ArrowFileDataSource, data_source::DataSource, parquet::ParquetDataSource,
    },
    utils::arrow_io::ArrowIPCFormat,
};
use anyhow::{Result, anyhow};
use arrow::datatypes::SchemaRef;

pub async fn run(args: TransformCommand) -> Result<()> {
    let TransformCommand {
        input,
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
        parquet_statistics,
        parquet_writer_version,
        parquet_no_dictionary,
        parquet_sorted_metadata,
    } = args;

    let bloom_filter = if let Some(all_config) = parquet_bloom_all {
        BloomFilterConfig::All(all_config)
    } else if !parquet_bloom_column.is_empty() {
        BloomFilterConfig::Columns(parquet_bloom_column)
    } else {
        BloomFilterConfig::None
    };

    let mut pipeline = Pipeline::new().with_query_dialect(dialect);

    let setup_result: Result<()> = match &input {
        InputSpec::From { input, .. } => {
            let input_path = input
                .path()
                .to_str()
                .ok_or_else(|| anyhow!("Invalid input path"))?;
            let detected_input_format = detect_format(input_path, input_format)?;

            let source: Box<dyn DataSource> = match detected_input_format {
                DataFormat::Arrow => Box::new(ArrowFileDataSource::new(input_path.to_string())),
                DataFormat::Parquet => Box::new(ParquetDataSource::new(input_path.to_string())),
            };

            pipeline = pipeline.with_input_strategy_with_single_source(source);
            Ok(())
        }
        InputSpec::FromMany { inputs, .. } => {
            let mut expanded_paths = Vec::new();

            for pattern in inputs {
                for entry in glob::glob(pattern)? {
                    expanded_paths.push(entry?.to_string_lossy().to_string());
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
                    DataFormat::Arrow => Box::new(ArrowFileDataSource::new(input_path.clone())),
                    DataFormat::Parquet => Box::new(ParquetDataSource::new(input_path.clone())),
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

    let output_spec = match input {
        InputSpec::From { to, .. } => to,
        InputSpec::FromMany { to, .. } => to,
    };

    let list_outputs_format = match &output_spec {
        OutputSpec::To { .. } => None,
        OutputSpec::ToMany { list_outputs, .. } => Some(*list_outputs),
    };

    let partition_sort_spec = match &output_spec {
        OutputSpec::ToMany { by, .. } => SortSpec::from(
            by.split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<_>>(),
        ),
        _ => SortSpec::default(),
    };

    let partition_columns = partition_sort_spec.column_names();

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
        parquet_statistics,
        parquet_writer_version,
        parquet_no_dictionary,
        bloom_filter,
        parquet_sort_spec,
    )?;

    match output_spec {
        OutputSpec::To { output } => {
            pipeline = pipeline.with_output_strategy_with_single_sink(
                output
                    .path()
                    .to_str()
                    .ok_or_else(|| anyhow!("Invalid output path"))?
                    .to_string(),
                sink_factory,
            );
        }
        OutputSpec::ToMany {
            template,
            by: _,
            exclude_columns,
            list_outputs,
            create_dirs,
            overwrite,
        } => {
            let path_template = PathTemplate::new(template);

            pipeline = pipeline.with_output_strategy_with_partitioned_sink(
                partition_columns,
                path_template,
                sink_factory,
                exclude_columns.clone(),
                create_dirs,
                overwrite,
                list_outputs,
            );
        }
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

fn print_output_files(files: &[String], format: ListOutputsFormat) -> Result<()> {
    match format {
        ListOutputsFormat::None => {}
        ListOutputsFormat::Text => {
            let mut sorted_files = files.to_vec();
            sorted_files.sort();
            for file in sorted_files {
                println!("{}", file);
            }
        }
        ListOutputsFormat::Json => {
            let json = serde_json::to_string_pretty(files)?;
            println!("{}", json);
        }
    }
    Ok(())
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
        }
    }

    Err(anyhow!(
        "Could not detect format from path '{}'. Use --input-format or --output-format to specify explicitly.",
        path
    ))
}

#[allow(clippy::too_many_arguments)]
fn create_sink_factory(
    output_format: Option<DataFormat>,
    arrow_compression: Option<ArrowCompression>,
    arrow_format: Option<ArrowIPCFormat>,
    arrow_record_batch_size: Option<usize>,
    parquet_compression: Option<ParquetCompression>,
    parquet_row_group_size: Option<usize>,
    parquet_statistics: Option<ParquetStatistics>,
    parquet_writer_version: Option<ParquetWriterVersion>,
    parquet_no_dictionary: bool,
    parquet_bloom_filter: BloomFilterConfig,
    parquet_sort_spec: Option<SortSpec>,
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
                if let Some(stats) = parquet_statistics {
                    options = options.with_statistics(stats);
                }
                if let Some(version) = parquet_writer_version {
                    options = options.with_writer_version(version);
                }
                if parquet_no_dictionary {
                    options = options.with_no_dictionary(true);
                }
                options = options.with_bloom_filters(parquet_bloom_filter.clone());
                if let Some(sort_spec) = parquet_sort_spec.clone() {
                    options = options.with_sort_spec(sort_spec);
                }
                Box::new(ParquetSink::create(path.into(), &schema, &options)?)
            }
        };

        Ok(sink)
    }))
}
