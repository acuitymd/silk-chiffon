//! Detect command for recognizing an input's data format.

use crate::DetectCommand;
use anyhow::Result;
use silk_chiffon_core::{DetectedFormat, PresentationMode};
use silk_chiffon_inspection_output::{dim, value};

pub(crate) async fn run(command: DetectCommand) -> Result<()> {
    let (input, presentation, storage, formats) = command.into_parts();
    let object = storage.lookup_input(&input).await?;
    let detected = formats.detect(&object).await?;

    if presentation.resolve() == PresentationMode::Json {
        let output = match &detected {
            Some(result) => {
                let mut object = serde_json::Map::new();
                object.insert("format".to_owned(), result.format().into());
                if let Some(variant) = result.variant_name() {
                    object.insert("variant".to_owned(), variant.into());
                }
                serde_json::Value::Object(object)
            }
            None => serde_json::json!({ "format": "unknown" }),
        };
        println!("{}", serde_json::to_string(&output)?);
    } else {
        println!("{}", detection_text(detected.as_ref()));
    }

    Ok(())
}

fn detection_text(detected: Option<&DetectedFormat>) -> String {
    let Some(detected) = detected else {
        return dim("Unknown");
    };
    let name = detected.display_name();
    match detected.variant_display_name() {
        Some(variant) => format!("{} {}", value(name), dim(format!("({variant})"))),
        None => value(name),
    }
}
