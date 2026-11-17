use serde_json::Value;

/// Rendered inspection output returned by a format.
#[derive(Clone, Debug, PartialEq)]
pub enum InspectionOutput {
    Text(String),
    Json(Value),
}
