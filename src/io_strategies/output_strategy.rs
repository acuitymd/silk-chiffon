use anyhow::Result;
use arrow::datatypes::SchemaRef;

use crate::{converters::partition_arrow::OutputTemplate, sinks::data_sink::DataSink};

pub enum OutputStrategy {
    Single(Box<dyn DataSink>),
    Partitioned {
        column: String,
        template: OutputTemplate,
        sink_factory: Box<dyn Fn(&str, &SchemaRef) -> Result<Box<dyn DataSink>> + Send + Sync>,
        exclude_partition_column: bool,
    },
}
