//! Detect command for recognizing an input's data format.

use anyhow::Result;
use silk_chiffon_core::DetectedFormat;
use silk_chiffon_storage::LocationInput;

use crate::{
    DetectCommand,
    inspection::style::{dim, value},
};

pub async fn run(command: DetectCommand) -> Result<()> {
    let (args, storage, formats) = command.into_parts();
    let location = LocationInput::parse(args.file.as_str())?;
    let object = storage.lookup_input(&location).await?;
    let detected = formats.detect(&object).await?;

    if args.format.resolves_to_json() {
        let output = match &detected {
            Some(result) => {
                let mut object = serde_json::Map::new();
                object.insert("format".to_owned(), result.format().into());
                if let Some(variant) = result.variant() {
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
