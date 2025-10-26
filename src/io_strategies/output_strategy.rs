use anyhow::Result;
use arrow::datatypes::SchemaRef;
use datafusion::prelude::DataFrame;

use crate::{converters::partition_arrow::OutputTemplate, sinks::data_sink::DataSink};

pub type TableName = String;

pub type SinkFactory = Box<dyn Fn(TableName, SchemaRef) -> Result<Box<dyn DataSink>>>;

pub enum OutputStrategy {
    Single(Box<dyn DataSink>),
    Partitioned {
        column: String,
        template: OutputTemplate,
        sink_factory: SinkFactory,
        exclude_partition_column: bool,
    },
}

impl OutputStrategy {
    pub async fn write(&mut self, df: DataFrame) -> Result<()> {
        match self {
            OutputStrategy::Single(sink) => {
                sink.write_stream(df.execute_stream().await?).await?;
                Ok(())
            }
            OutputStrategy::Partitioned { sink_factory, .. } => {
                let mut sink =
                    sink_factory(TableName::from("output"), df.schema().inner().clone())?;
                sink.write_stream(df.execute_stream().await?).await?;
                Ok(())
            }
        }
    }
}
