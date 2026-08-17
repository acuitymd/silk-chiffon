use std::sync::Arc;

use anyhow::Result;
use datafusion::{
    catalog::TableProvider, physical_plan::SendableRecordBatchStream, prelude::SessionContext,
};
use futures::{TryStreamExt, future::BoxFuture};
use silk_chiffon_core::{
    FileInputGroup, FormatDefinition, FormatFuture, FormatInputVariant, InputDetection,
    InspectionDefinition, InspectionOutput, PresentationMode, ServiceInputDefinition,
    ServiceOutputDefinition, SinkBinding, SinkBindingConfig, TransformDefinition,
};
use silk_chiffon_storage::InputObject;

fn detect(_object: &InputObject) -> FormatFuture<'_, InputDetection> {
    Box::pin(async { Ok(InputDetection::Match(FormatInputVariant::new())) })
}

fn create_file_provider<'a>(
    _group: &'a FileInputGroup,
    _session: &'a SessionContext,
    _state: &'a (),
) -> FormatFuture<'a, Arc<dyn TableProvider>> {
    Box::pin(async { anyhow::bail!("the example has no file reader") })
}

fn bind_sink<'a>(
    _config: &'a SinkBindingConfig,
    _state: &'a (),
) -> FormatFuture<'a, Box<dyn SinkBinding>> {
    Box::pin(async { anyhow::bail!("the example has no writer") })
}

fn inspect<'a>(
    _object: &'a InputObject,
    _mode: PresentationMode,
    _state: &'a (),
) -> FormatFuture<'a, InspectionOutput> {
    Box::pin(async { anyhow::bail!("the example has no inspector") })
}

fn file_format() -> FormatDefinition {
    let transform = TransformDefinition::without_args()
        .input_provider(create_file_provider)
        .sink(bind_sink)
        .build();
    FormatDefinition::builder("example", "Example")
        .extensions(["example"])
        .detector(detect)
        .transform(transform)
        .inspection(InspectionDefinition::without_args(inspect))
        .build()
}

fn create_service_provider<'a>(
    _reference: &'a str,
    _session: &'a SessionContext,
    _state: &'a (),
) -> BoxFuture<'a, Result<Arc<dyn TableProvider>>> {
    Box::pin(async { anyhow::bail!("the example has no service client") })
}

fn service_input() -> ServiceInputDefinition {
    ServiceInputDefinition::without_args(create_service_provider)
        .name("example-input")
        .schemes(["example-input"])
        .build()
        .expect("the example definition is valid")
}

fn consume_service_output<'a>(
    _target: &'a str,
    mut stream: SendableRecordBatchStream,
    _state: &'a (),
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        while stream.try_next().await?.is_some() {}
        Ok(())
    })
}

fn service_output() -> ServiceOutputDefinition {
    ServiceOutputDefinition::without_args(consume_service_output)
        .name("example-output")
        .schemes(["example-output"])
        .build()
        .expect("the example definition is valid")
}

fn main() {
    assert_eq!(file_format().name(), "example");
    assert_eq!(service_input().schemes(), ["example-input"]);
    assert_eq!(service_output().schemes(), ["example-output"]);
}
