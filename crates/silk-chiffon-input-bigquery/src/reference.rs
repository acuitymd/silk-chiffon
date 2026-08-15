use std::{collections::BTreeMap, error::Error, fmt};

use percent_encoding::percent_decode_str;
use sha2::{Digest, Sha256};
use unicode_general_category::{GeneralCategory, get_general_category};
use url::Url;

use crate::snapshot::PinnedSnapshot;

const MAX_DATASET_ID_BYTES: usize = 1_024;
const MAX_TABLE_ID_BYTES: usize = 1_024;

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct BigQueryReference {
    table: TableResource,
    snapshot: Option<PinnedSnapshot>,
    expected_location: Option<String>,
}

impl BigQueryReference {
    pub(crate) fn parse(input: &str) -> Result<Self, BigQueryReferenceError> {
        if !input.starts_with("bqs:///") || !valid_percent_encoding(input) || input.contains('#') {
            return Err(BigQueryReferenceError::Syntax);
        }
        let url = Url::parse(input).map_err(|_| BigQueryReferenceError::Syntax)?;
        if url.scheme() != "bqs"
            || url.host().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.as_str() != input
        {
            return Err(BigQueryReferenceError::Syntax);
        }

        let raw_path = input["bqs:///".len()..]
            .split_once('?')
            .map_or_else(|| &input["bqs:///".len()..], |(path, _)| path);
        if raw_path.is_empty() || raw_path.ends_with('/') || url.path() != format!("/{raw_path}") {
            return Err(BigQueryReferenceError::Syntax);
        }
        let decoded = raw_path
            .split('/')
            .map(decode_component)
            .collect::<Result<Vec<_>, _>>()?;
        let [projects, project, datasets, dataset, tables, table] = decoded.as_slice() else {
            return Err(BigQueryReferenceError::Syntax);
        };
        if projects != "projects" || datasets != "datasets" || tables != "tables" {
            return Err(BigQueryReferenceError::Syntax);
        }
        let table = TableResource::new(project, dataset, table)?;

        let mut parameters = BTreeMap::new();
        if url.query() == Some("") {
            return Err(BigQueryReferenceError::Syntax);
        }
        for (name, value) in url.query_pairs() {
            if !matches!(name.as_ref(), "snapshot" | "location") {
                return Err(BigQueryReferenceError::UnknownParameter);
            }
            if parameters
                .insert(name.into_owned(), value.into_owned())
                .is_some()
            {
                return Err(BigQueryReferenceError::DuplicateParameter);
            }
        }
        let snapshot = parameters
            .remove("snapshot")
            .map(|value| PinnedSnapshot::from_rfc3339(&value))
            .transpose()
            .map_err(|_| BigQueryReferenceError::InvalidSnapshot)?;
        let expected_location = parameters.remove("location");
        if expected_location
            .as_deref()
            .is_some_and(|location| !valid_location(location))
        {
            return Err(BigQueryReferenceError::InvalidLocation);
        }

        Ok(Self {
            table,
            snapshot,
            expected_location,
        })
    }

    pub(crate) fn table_resource(&self) -> &str {
        &self.table.resource
    }

    pub(crate) fn table_project(&self) -> &str {
        &self.table.project_id
    }

    #[cfg(test)]
    pub(crate) fn dataset(&self) -> &str {
        &self.table.dataset_id
    }

    #[cfg(test)]
    pub(crate) fn table(&self) -> &str {
        &self.table.table_id
    }

    pub(crate) const fn snapshot(&self) -> Option<PinnedSnapshot> {
        self.snapshot
    }

    pub(crate) fn expected_location(&self) -> Option<&str> {
        self.expected_location.as_deref()
    }
}

impl fmt::Debug for BigQueryReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BigQueryReference")
            .field(
                "resource_sha256",
                &hex_digest(self.table.resource.as_bytes()),
            )
            .field("snapshot", &self.snapshot)
            .field("has_expected_location", &self.expected_location.is_some())
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TableResource {
    project_id: String,
    dataset_id: String,
    table_id: String,
    resource: String,
}

impl TableResource {
    fn new(project: &str, dataset: &str, table: &str) -> Result<Self, BigQueryReferenceError> {
        validate_project(project)?;
        validate_dataset(dataset)?;
        validate_table(table)?;
        Ok(Self {
            project_id: project.to_owned(),
            dataset_id: dataset.to_owned(),
            table_id: table.to_owned(),
            resource: format!("projects/{project}/datasets/{dataset}/tables/{table}"),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BigQueryReferenceError {
    Syntax,
    UnknownParameter,
    DuplicateParameter,
    InvalidProject,
    InvalidDataset,
    InvalidTable,
    UnsupportedTableDecorator,
    InvalidSnapshot,
    InvalidLocation,
}

impl fmt::Display for BigQueryReferenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Syntax => "BigQuery input must use canonical bqs:///projects/PROJECT/datasets/DATASET/tables/TABLE syntax",
            Self::UnknownParameter => "BigQuery input contains an unknown query parameter",
            Self::DuplicateParameter => "BigQuery input contains a duplicate query parameter",
            Self::InvalidProject => "invalid Google Cloud project component",
            Self::InvalidDataset => "invalid BigQuery dataset component",
            Self::InvalidTable => "invalid BigQuery table component",
            Self::UnsupportedTableDecorator => "Storage Read table decorators are not supported; use snapshot=RFC3339",
            Self::InvalidSnapshot => "BigQuery snapshot must be an RFC3339 timestamp",
            Self::InvalidLocation => "BigQuery location must be an ASCII location identifier",
        })
    }
}

impl Error for BigQueryReferenceError {}

fn decode_component(value: &str) -> Result<String, BigQueryReferenceError> {
    percent_decode_str(value)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| BigQueryReferenceError::Syntax)
}

fn valid_percent_encoding(input: &str) -> bool {
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return false;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    true
}

fn validate_project(value: &str) -> Result<(), BigQueryReferenceError> {
    if !value.is_empty()
        && value.len() <= 255
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b':'))
    {
        Ok(())
    } else {
        Err(BigQueryReferenceError::InvalidProject)
    }
}

fn validate_dataset(value: &str) -> Result<(), BigQueryReferenceError> {
    if !value.is_empty()
        && value.len() <= MAX_DATASET_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        Ok(())
    } else {
        Err(BigQueryReferenceError::InvalidDataset)
    }
}

fn validate_table(value: &str) -> Result<(), BigQueryReferenceError> {
    if value.contains(['$', '@']) {
        return Err(BigQueryReferenceError::UnsupportedTableDecorator);
    }
    let valid = !value.is_empty()
        && value.len() <= MAX_TABLE_ID_BYTES
        && value.chars().all(|character| {
            matches!(
                get_general_category(character),
                GeneralCategory::UppercaseLetter
                    | GeneralCategory::LowercaseLetter
                    | GeneralCategory::TitlecaseLetter
                    | GeneralCategory::ModifierLetter
                    | GeneralCategory::OtherLetter
                    | GeneralCategory::NonspacingMark
                    | GeneralCategory::SpacingMark
                    | GeneralCategory::EnclosingMark
                    | GeneralCategory::DecimalNumber
                    | GeneralCategory::LetterNumber
                    | GeneralCategory::OtherNumber
                    | GeneralCategory::ConnectorPunctuation
                    | GeneralCategory::DashPunctuation
                    | GeneralCategory::SpaceSeparator
            )
        });
    if valid {
        Ok(())
    } else {
        Err(BigQueryReferenceError::InvalidTable)
    }
}

fn valid_location(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::BigQueryReference;

    #[test]
    fn parses_the_canonical_table_reference_and_optional_input_policy() {
        let reference = BigQueryReference::parse(
            "bqs:///projects/table-project/datasets/dataset_1/tables/T%C3%A4ble?snapshot=2026-08-15T10%3A20%3A30.123Z&location=us-central1",
        )
        .unwrap();

        assert_eq!(
            reference.table_resource(),
            "projects/table-project/datasets/dataset_1/tables/Täble"
        );
        assert_eq!(reference.table_project(), "table-project");
        assert_eq!(reference.dataset(), "dataset_1");
        assert_eq!(reference.table(), "Täble");
        assert_eq!(
            reference.snapshot().unwrap().to_rfc3339(),
            "2026-08-15T10:20:30.123Z"
        );
        assert_eq!(reference.expected_location(), Some("us-central1"));
    }

    #[test]
    fn omitted_input_policy_remains_absent_for_later_server_time_pinning() {
        let reference =
            BigQueryReference::parse("bqs:///projects/project/datasets/dataset/tables/table")
                .unwrap();

        assert_eq!(reference.snapshot(), None);
        assert_eq!(reference.expected_location(), None);
    }

    #[test]
    fn rejects_noncanonical_or_policy_bearing_references() {
        for invalid in [
            "BQS:///projects/project/datasets/dataset/tables/table",
            "bqs://host/projects/project/datasets/dataset/tables/table",
            "bqs:/projects/project/datasets/dataset/tables/table",
            "bqs:////projects/project/datasets/dataset/tables/table",
            "bqs:///projects/project/datasets/dataset/tables/table/",
            "bqs:///projects/project/datasets/dataset/tables/table/extra",
            "bqs:///projects/project/datasets/dataset/tables/table#fragment",
            "bqs://user@/projects/project/datasets/dataset/tables/table",
            "bqs:///projects/project/datasets/dataset/tables/table?unknown=value",
            "bqs:///projects/project/datasets/dataset/tables/table?location=US&location=EU",
            "bqs:///projects/project/datasets/dataset/tables/table?snapshot=2026-08-15T00%3A00%3A00Z&snapshot=2026-08-16T00%3A00%3A00Z",
            "bqs:///projects/project/datasets/dataset/tables/table?location=",
            "bqs:///projects/project/datasets/dataset/tables/table?credentials=secret",
            "bqs:///projects/project/datasets/data%2Fset/tables/table",
            "bqs:///projects/project/datasets/dataset/tables/table%ZZ",
        ] {
            assert!(
                BigQueryReference::parse(invalid).is_err(),
                "{invalid:?} should be rejected"
            );
        }
    }

    #[test]
    fn debug_identity_does_not_expose_resource_names() {
        let reference = BigQueryReference::parse(
            "bqs:///projects/secret-project/datasets/secret_dataset/tables/secret_table",
        )
        .unwrap();
        let debug = format!("{reference:?}");

        assert!(!debug.contains("secret-project"));
        assert!(!debug.contains("secret_dataset"));
        assert!(!debug.contains("secret_table"));
    }
}
