use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    process::Command,
};

use serde_json::Value;

fn workspace_packages() -> BTreeMap<String, BTreeSet<String>> {
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
                .map(|dependency| {
                    dependency["name"]
                        .as_str()
                        .expect("dependency name should be a string")
                        .to_owned()
                })
                .collect();
            (name, dependencies)
        })
        .collect()
}

#[test]
fn workspace_contains_foundation_packages() {
    let packages = workspace_packages();

    assert!(packages.contains_key("silk_chiffon"));
    assert!(packages.contains_key("silk-chiffon-core"));
    assert!(packages.contains_key("silk-chiffon-storage"));
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
                .all(|dependency| !dependency.starts_with("silk-chiffon-format-")),
            "{package} must not depend on a concrete format package"
        );
    }
}
