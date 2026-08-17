use thiserror::Error;

/// Failure before a final physical plan has been retained.
#[derive(Debug, Error)]
#[error("failed to prepare pipeline: {source}")]
pub struct PipelinePreparationError {
    #[source]
    pub(super) source: anyhow::Error,
}

impl PipelinePreparationError {
    pub(super) fn new(source: impl Into<anyhow::Error>) -> Self {
        Self {
            source: source.into(),
        }
    }
}

/// Failure while starting execution of an already prepared plan.
#[derive(Debug, Error)]
#[error("failed to begin pipeline execution: {source}")]
pub struct PipelineExecutionStartError {
    #[source]
    pub(super) source: datafusion::error::DataFusionError,
}
