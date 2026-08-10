use std::sync::Arc;

use anyhow::{Result, ensure};
use arrow::{
    array::{Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use clap::{Args, Command};
use datafusion::{
    catalog::TableProvider,
    datasource::MemTable,
    error::DataFusionError,
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
    prelude::SessionContext,
};
use futures::{TryStreamExt, future::BoxFuture};
use silk_chiffon_core::{
    DataSource, Replayability, ServiceInputDefinition, ServiceOutputDefinition, SinkCompletion,
};
use url::Url;

#[derive(Args)]
struct InputArgs {
    #[arg(long)]
    service_input_marker: usize,
}

#[derive(Args)]
struct OutputArgs {
    #[arg(long)]
    service_output_marker: usize,
}

struct TestSource {
    name: String,
    schema: SchemaRef,
}

#[async_trait]
impl DataSource for TestSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn replayability(&self) -> Replayability {
        Replayability::Replayable
    }

    async fn table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        Ok(Arc::new(MemTable::try_new(
            Arc::clone(&self.schema),
            vec![vec![batch]],
        )?))
    }
}

fn create_source<'a>(
    reference: &'a str,
    session: &'a SessionContext,
    settings: &'a InputArgs,
) -> BoxFuture<'a, Result<Box<dyn DataSource>>> {
    Box::pin(async move {
        ensure!(reference == "svc-in://project/table");
        ensure!(settings.service_input_marker == 17);
        ensure!(session.state().config_options().execution.target_partitions == 3);
        Ok(Box::new(TestSource {
            name: reference.to_owned(),
            schema: Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int32,
                false,
            )])),
        }) as Box<dyn DataSource>)
    })
}

fn write_output<'a>(
    target: &'a str,
    stream: SendableRecordBatchStream,
    settings: &'a OutputArgs,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        ensure!(target == "svc-out://project/table?mode=replace");
        ensure!(settings.service_output_marker == 23);
        let batches = stream.try_collect::<Vec<_>>().await?;
        ensure!(batches.iter().map(RecordBatch::num_rows).sum::<usize>() == 3);
        Ok(())
    })
}

#[test]
fn service_input_keeps_typed_settings_with_its_creator() {
    let definition = ServiceInputDefinition::with_args::<InputArgs>(create_source)
        .name("test-input")
        .schemes(["svc-in"])
        .build()
        .unwrap();
    let command = definition.augment_args(Command::new("test"));
    let matches = command
        .try_get_matches_from(["test", "--service-input-marker", "17"])
        .unwrap();
    let binding = definition.bind(&matches).unwrap();
    let session = SessionContext::new_with_config(
        datafusion::prelude::SessionConfig::new().with_target_partitions(3),
    );

    let source =
        futures::executor::block_on(binding.create_source("svc-in://project/table", &session))
            .unwrap();

    assert_eq!(definition.name(), "test-input");
    assert_eq!(definition.schemes(), ["svc-in"]);
    assert_eq!(source.name(), "svc-in://project/table");
}

#[test]
fn service_output_keeps_typed_settings_with_its_write_operation() {
    let definition = ServiceOutputDefinition::with_args::<OutputArgs>(write_output)
        .name("test-output")
        .schemes(["svc-out"])
        .build()
        .unwrap();
    let command = definition.augment_args(Command::new("test"));
    let matches = command
        .try_get_matches_from(["test", "--service-output-marker", "23"])
        .unwrap();
    let binding = definition.bind(&matches).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter([Ok::<_, DataFusionError>(batch)]),
    ));

    futures::executor::block_on(binding.write("svc-out://project/table?mode=replace", stream))
        .unwrap();

    assert_eq!(definition.name(), "test-output");
    assert_eq!(definition.schemes(), ["svc-out"]);
}

#[test]
fn sink_completion_always_has_a_durable_location() {
    let first = Url::parse("file:///tmp/one.arrow").unwrap();
    let second = Url::parse("file:///tmp/two.arrow").unwrap();
    let completion = SinkCompletion::new(first.clone(), [second.clone()], 9);

    assert_eq!(completion.durable_locations(), [first, second]);
    assert_eq!(completion.rows_written(), 9);
}
