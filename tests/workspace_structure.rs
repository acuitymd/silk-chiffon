use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Dependency {
    name: String,
    kind: Option<String>,
}

fn workspace_packages() -> BTreeMap<String, BTreeSet<Dependency>> {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--no-deps", "--locked"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo metadata should run");

    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    serde_json::from_slice::<Value>(&output.stdout)
        .expect("cargo metadata should return JSON")["packages"]
        .as_array()
        .expect("cargo metadata packages should be an array")
        .iter()
        .map(|package| {
            let name = package["name"]
                .as_str()
                .expect("package name should be a string")
                .to_owned();
            let dependencies = package["dependencies"]
                .as_array()
                .expect("package dependencies should be an array")
                .iter()
                .map(|dependency| Dependency {
                    name: dependency["name"]
                        .as_str()
                        .expect("dependency name should be a string")
                        .to_owned(),
                    kind: dependency["kind"].as_str().map(str::to_owned),
                })
                .collect();
            (name, dependencies)
        })
        .collect()
}

fn source_files_below(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_owned()];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).expect("workspace source directory should be readable") {
            let path = entry
                .expect("workspace source entry should be readable")
                .path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs")
                || path.file_name().is_some_and(|name| name == "Cargo.toml")
            {
                files.push(path);
            }
        }
    }
    files
}

#[test]
fn workspace_contains_foundation_packages() {
    let packages = workspace_packages();

    assert!(packages.contains_key("silk_chiffon"));
    assert!(packages.contains_key("silk-chiffon-core"));
    assert!(packages.contains_key("silk-chiffon-storage"));
    assert!(packages.contains_key("silk-chiffon-format-arrow"));
    assert!(packages.contains_key("silk-chiffon-format-parquet"));
    assert!(packages.contains_key("silk-chiffon-format-vortex"));
    assert!(packages.contains_key("silk-chiffon-input-bigquery"));
    assert!(packages.contains_key("silk-chiffon-inspection-output"));
    assert!(packages.contains_key("silk-chiffon-test-support"));
}

#[test]
fn root_defaults_compose_every_input_provider() {
    let manifest = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("workspace manifest should be readable");

    assert!(manifest.contains(r#"default = ["local-bare-paths", "gcs", "s3", "bigquery"]"#));
    assert!(manifest.contains(r#"bigquery = ["dep:silk-chiffon-input-bigquery"]"#));
}

#[test]
fn bigquery_input_dependencies_point_toward_foundation_crates() {
    let packages = workspace_packages();
    let connector = packages
        .get("silk-chiffon-input-bigquery")
        .expect("BigQuery input should be a workspace package");

    assert!(connector.contains(&Dependency {
        name: "silk-chiffon-core".to_owned(),
        kind: None,
    }));
    assert!(connector.iter().all(|dependency| {
        dependency.name != "silk_chiffon"
            && dependency.name != "silk-chiffon-storage"
            && !dependency.name.starts_with("silk-chiffon-format-")
    }));
}

#[test]
fn workspace_uses_cargo_resolver_three() {
    let manifest = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("workspace manifest should be readable");

    assert!(
        manifest
            .lines()
            .any(|line| line.trim() == r#"resolver = "3""#),
        "workspace must select Cargo resolver 3"
    );
}

#[test]
fn foundation_packages_do_not_depend_on_format_packages() {
    let packages = workspace_packages();

    for package in ["silk-chiffon-core", "silk-chiffon-storage"] {
        let dependencies = packages
            .get(package)
            .unwrap_or_else(|| panic!("{package} should be a workspace package"));
        assert!(
            dependencies
                .iter()
                .all(|dependency| !dependency.name.starts_with("silk-chiffon-format-")),
            "{package} must not depend on a concrete format package"
        );
    }
}

#[test]
fn format_packages_do_not_contain_cloud_provider_code() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let forbidden = [
        "AmazonS3",
        "GoogleCloudStorage",
        "object_store::aws",
        "object_store::gcp",
        "silk_chiffon_storage::gcs",
        "silk_chiffon_storage::s3",
        "gs://",
        "s3://",
        "--gcs-",
        "--s3-",
    ];
    for package in [
        "silk-chiffon-format-arrow",
        "silk-chiffon-format-parquet",
        "silk-chiffon-format-vortex",
    ] {
        for path in source_files_below(&workspace.join("crates").join(package)) {
            let source = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
            for term in forbidden {
                assert!(
                    !source.contains(term),
                    "format package {package} contains provider-specific term {term:?} in {}",
                    path.display()
                );
            }
        }
    }
}

#[test]
fn arrow_and_test_support_dependencies_have_one_direction() {
    let packages = workspace_packages();
    let root = packages.get("silk_chiffon").unwrap();
    let arrow = packages.get("silk-chiffon-format-arrow").unwrap();
    let support = packages.get("silk-chiffon-test-support").unwrap();

    assert!(root.contains(&Dependency {
        name: "silk-chiffon-format-arrow".to_owned(),
        kind: None,
    }));
    assert!(root.contains(&Dependency {
        name: "silk-chiffon-test-support".to_owned(),
        kind: Some("dev".to_owned()),
    }));
    assert!(arrow.contains(&Dependency {
        name: "silk-chiffon-core".to_owned(),
        kind: None,
    }));
    assert!(arrow.contains(&Dependency {
        name: "silk-chiffon-storage".to_owned(),
        kind: None,
    }));
    assert!(arrow.contains(&Dependency {
        name: "silk-chiffon-test-support".to_owned(),
        kind: Some("dev".to_owned()),
    }));
    assert!(support.iter().all(|dependency| {
        dependency.name != "silk_chiffon"
            && dependency.name != "silk-chiffon-core"
            && !dependency.name.starts_with("silk-chiffon-format-")
    }));
}

#[test]
fn parquet_and_inspection_output_dependencies_have_one_direction() {
    let packages = workspace_packages();
    let root = packages.get("silk_chiffon").unwrap();
    let parquet = packages.get("silk-chiffon-format-parquet").unwrap();
    let inspection = packages.get("silk-chiffon-inspection-output").unwrap();

    assert!(root.contains(&Dependency {
        name: "silk-chiffon-format-parquet".to_owned(),
        kind: None,
    }));
    assert!(parquet.contains(&Dependency {
        name: "silk-chiffon-core".to_owned(),
        kind: None,
    }));
    assert!(parquet.contains(&Dependency {
        name: "silk-chiffon-storage".to_owned(),
        kind: None,
    }));
    assert!(parquet.contains(&Dependency {
        name: "silk-chiffon-inspection-output".to_owned(),
        kind: None,
    }));
    assert!(inspection.iter().all(|dependency| {
        dependency.name != "silk_chiffon"
            && dependency.name != "silk-chiffon-core"
            && !dependency.name.starts_with("silk-chiffon-format-")
    }));
}

#[test]
fn vortex_dependencies_have_one_direction() {
    let packages = workspace_packages();
    let root = packages.get("silk_chiffon").unwrap();
    let vortex = packages.get("silk-chiffon-format-vortex").unwrap();

    assert!(root.contains(&Dependency {
        name: "silk-chiffon-format-vortex".to_owned(),
        kind: None,
    }));
    assert!(root.iter().all(|dependency| {
        dependency.name != "vortex" && dependency.name != "vortex-datafusion"
    }));
    assert!(vortex.contains(&Dependency {
        name: "silk-chiffon-core".to_owned(),
        kind: None,
    }));
    assert!(vortex.contains(&Dependency {
        name: "silk-chiffon-storage".to_owned(),
        kind: None,
    }));
    assert!(vortex.contains(&Dependency {
        name: "silk-chiffon-inspection-output".to_owned(),
        kind: None,
    }));
    assert!(vortex.contains(&Dependency {
        name: "silk-chiffon-test-support".to_owned(),
        kind: Some("dev".to_owned()),
    }));
}

#[test]
fn root_no_longer_owns_arrow_ipc_or_shared_fixtures() {
    let root = env!("CARGO_MANIFEST_DIR");
    for relative in [
        "src/sources/arrow",
        "src/sinks/arrow.rs",
        "src/inspection/arrow.rs",
        "src/sinks/object_sink_task.rs",
        "src/utils/test_data.rs",
        "src/utils/test_helpers.rs",
    ] {
        assert!(
            !std::path::Path::new(root).join(relative).exists(),
            "obsolete root path remains: {relative}"
        );
    }
}

#[test]
fn root_no_longer_owns_parquet_or_shared_inspection_output() {
    let root = env!("CARGO_MANIFEST_DIR");
    for relative in [
        "src/sources/parquet.rs",
        "src/sinks/parquet",
        "src/inspection/parquet.rs",
        "src/inspection/inspectable.rs",
        "src/inspection/style.rs",
        "src/utils/blocking.rs",
        "src/utils/parquet_inspection.rs",
    ] {
        assert!(
            !std::path::Path::new(root).join(relative).exists(),
            "obsolete root path remains: {relative}"
        );
    }
}

#[test]
fn root_no_longer_owns_vortex() {
    let root = env!("CARGO_MANIFEST_DIR");
    for relative in [
        "src/sources/vortex.rs",
        "src/sinks/vortex.rs",
        "src/inspection/vortex.rs",
        "src/inspection/magic.rs",
    ] {
        assert!(
            !std::path::Path::new(root).join(relative).exists(),
            "obsolete root path remains: {relative}"
        );
    }
}

#[test]
fn workspace_dependency_directions_match_runtime_ownership() {
    let packages = workspace_packages();
    let forbidden_format_dependencies = ["silk_chiffon", "silk-chiffon-input-bigquery"];

    for package in [
        "silk-chiffon-format-arrow",
        "silk-chiffon-format-parquet",
        "silk-chiffon-format-vortex",
    ] {
        let dependencies = packages.get(package).unwrap();
        assert!(dependencies.iter().all(|dependency| {
            !forbidden_format_dependencies.contains(&dependency.name.as_str())
        }));
    }

    let inspection = packages.get("silk-chiffon-inspection-output").unwrap();
    assert!(inspection.iter().all(|dependency| {
        dependency.name != "silk_chiffon"
            && dependency.name != "silk-chiffon-core"
            && dependency.name != "silk-chiffon-input-bigquery"
            && !dependency.name.starts_with("silk-chiffon-format-")
    }));

    let support = packages.get("silk-chiffon-test-support").unwrap();
    assert!(support.iter().all(|dependency| {
        dependency.name != "silk_chiffon"
            && dependency.name != "silk-chiffon-core"
            && dependency.name != "silk-chiffon-input-bigquery"
            && !dependency.name.starts_with("silk-chiffon-format-")
    }));

    let bigquery = packages.get("silk-chiffon-input-bigquery").unwrap();
    assert!(bigquery.iter().all(|dependency| {
        dependency.name != "silk_chiffon"
            && dependency.name != "silk-chiffon-storage"
            && dependency.name != "object_store"
            && !dependency.name.starts_with("silk-chiffon-format-")
    }));
}

#[test]
fn root_composes_formats_only_through_registered_definitions() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for path in source_files_below(&root) {
        let source = fs::read_to_string(&path).unwrap();
        for package in [
            "silk_chiffon_format_arrow",
            "silk_chiffon_format_parquet",
            "silk_chiffon_format_vortex",
        ] {
            if source.contains(package) {
                assert_eq!(path.file_name().unwrap(), "registration.rs");
                assert!(source.contains(&format!(".register({package}::definition())")));
            }
        }
    }
}

#[test]
fn service_input_connector_does_not_impersonate_file_storage() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("crates")
        .join("silk-chiffon-input-bigquery");
    for path in source_files_below(&root) {
        let source = fs::read_to_string(&path).unwrap();
        for forbidden in [
            "silk_chiffon_storage",
            "object_store::",
            "LocationInput",
            "LocationPattern",
            "InputObject",
        ] {
            assert!(
                !source.contains(forbidden),
                "BigQuery connector contains file-storage term {forbidden:?} in {}",
                path.display()
            );
        }
    }
}

#[test]
fn integration_protocol_hook_is_explicit_and_bounded() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root_manifest = fs::read_to_string(root.join("Cargo.toml")).unwrap();
    let connector_manifest =
        fs::read_to_string(root.join("crates/silk-chiffon-input-bigquery/Cargo.toml")).unwrap();
    let connector =
        fs::read_to_string(root.join("crates/silk-chiffon-input-bigquery/src/lib.rs")).unwrap();

    assert!(connector_manifest.contains("integration-test-support = []"));
    assert!(root_manifest.contains("silk-chiffon-input-bigquery/integration-test-support"));
    assert_eq!(
        root_manifest
            .matches("silk-chiffon-input-bigquery/integration-test-support")
            .count(),
        1
    );
    assert!(connector.contains("#[cfg(feature = \"integration-test-support\")]"));
    assert!(connector.contains("#[doc(hidden)]\npub mod integration_test_support"));
}

#[test]
fn transitional_surfaces_do_not_return() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let cases = [
        ("src/lib.rs", "pub fn parse_at_least_one"),
        ("src/lib.rs", "pub fn parse_nonzero_byte_size"),
        ("src/lib.rs", "pub fn default_thread_budget"),
        ("src/lib.rs", "pub enum ThreadBudgetSpec"),
        ("src/lib.rs", "pub enum MemoryBudgetSpec"),
        ("src/lib.rs", "pub enum PoolReserveSpec"),
        ("src/lib.rs", "pub enum PartitionStrategy"),
        ("src/lib.rs", "pub struct SortSpec"),
        ("src/lib.rs", "pub enum PresentationPreference"),
        ("src/lib.rs", "pub fn storage(&self)"),
        ("src/lib.rs", "pub fn inspection(&self)"),
        ("src/registration.rs", "ServiceInputBindings"),
        ("src/registration.rs", "ServiceOutputBindings"),
        ("crates/silk-chiffon-storage/src/local.rs", "pub fn session"),
        ("crates/silk-chiffon-storage/src/lib.rs", "RetryArgs"),
        ("crates/silk-chiffon-storage/src/lib.rs", "ObjectUploadArgs"),
        (
            "crates/silk-chiffon-storage/src/lib.rs",
            "ObjectUploadSettings",
        ),
        (
            "crates/silk-chiffon-storage/src/session.rs",
            "pub fn retry_configuration",
        ),
        (
            "crates/silk-chiffon-storage/src/session.rs",
            "pub fn object_upload_settings",
        ),
        (
            "crates/silk-chiffon-storage/src/handle.rs",
            "local_path_accessor!(PreparedOutputTarget)",
        ),
        (
            "crates/silk-chiffon-inspection-output/src/lib.rs",
            "render_metadata_map",
        ),
        (
            "crates/silk-chiffon-inspection-output/src/lib.rs",
            "truncate_for_display",
        ),
        (
            "crates/silk-chiffon-format-parquet/src/inspection/mod.rs",
            "Inspector::is_format",
        ),
        (
            "crates/silk-chiffon-format-parquet/src/inspection/mod.rs",
            "pub(crate) fn open(path",
        ),
        (
            "crates/silk-chiffon-test-support/src/lib.rs",
            "pub mod batch",
        ),
        (
            "crates/silk-chiffon-test-support/src/lib.rs",
            "pub mod verify",
        ),
        (
            "crates/silk-chiffon-test-support/src/batch.rs",
            "pub fn create_arrow_file_with_range_of_ids",
        ),
    ];

    for (relative, forbidden) in cases {
        let source = fs::read_to_string(root.join(relative)).unwrap();
        assert!(
            !source.contains(forbidden),
            "obsolete surface {forbidden:?} remains in {relative}"
        );
    }
}
