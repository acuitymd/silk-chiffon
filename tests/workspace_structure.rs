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
    assert!(packages.contains_key("silk-chiffon-inspection-output"));
    assert!(packages.contains_key("silk-chiffon-test-support"));
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
