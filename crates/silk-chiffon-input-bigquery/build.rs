use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

#[path = "build_support/proto_mirror.rs"]
mod proto_mirror;

const PROTO_ROOTS: &[&str] = &[
    "google/cloud/bigquery/storage/v1/storage.proto",
    "google/rpc/error_details.proto",
    "google/api/routing.proto",
];
const GENERATED_FILES: &[&str] = &[
    "bq_storage_descriptor.bin",
    "google.api.rs",
    "google.cloud.bigquery.storage.v1.rs",
    "google.rpc.rs",
];
const WELL_KNOWN_TYPES: &[&str] = &[
    "google/protobuf/any.proto",
    "google/protobuf/descriptor.proto",
    "google/protobuf/duration.proto",
    "google/protobuf/timestamp.proto",
    "google/protobuf/wrappers.proto",
];

fn watch_proto_tree(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut pending = vec![path.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                println!("cargo:rerun-if-changed={}", path.display());
            }
        }
    }
    Ok(())
}

fn generate_protos() -> Result<(), Box<dyn Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let update_requested = env::var_os("SILK_CHIFFON_BIGQUERY_PROTO_UPDATE").is_some();
    let source_override = env::var_os("SILK_CHIFFON_BIGQUERY_PROTO_SOURCE_ROOT");
    let output_override = env::var_os("SILK_CHIFFON_BIGQUERY_PROTO_UPDATE_ROOT");
    if !update_requested && (source_override.is_some() || output_override.is_some()) {
        return Err("candidate proto roots require SILK_CHIFFON_BIGQUERY_PROTO_UPDATE".into());
    }

    let proto_dir = source_override
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.join("proto"));
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let generated_dir = output_override
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.join("src/proto/generated"));
    let descriptor_path = out_dir.join("bq_storage_descriptor.bin");
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let protoc_include = protoc_bin_vendored::include_path()?;
    let proto_inputs = PROTO_ROOTS
        .iter()
        .map(|relative| proto_dir.join(relative))
        .collect::<Vec<_>>();

    for relative in WELL_KNOWN_TYPES {
        let vendored = fs::read(proto_dir.join(relative))?;
        let compiler_copy = fs::read(protoc_include.join(relative))?;
        if vendored != compiler_copy {
            return Err(
                format!("vendored {relative} differs from protoc-bin-vendored 3.2.0").into(),
            );
        }
    }

    let mut prost = prost_build::Config::new();
    prost.protoc_executable(protoc);
    prost.file_descriptor_set_path(descriptor_path);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_with_config(prost, &proto_inputs, &[proto_dir.clone(), protoc_include])?;

    watch_proto_tree(&manifest_dir.join("proto/google"))?;
    for relative in GENERATED_FILES {
        println!(
            "cargo:rerun-if-changed={}",
            generated_dir.join(relative).display()
        );
    }
    for variable in [
        "SILK_CHIFFON_BIGQUERY_PROTO_CHECK",
        "SILK_CHIFFON_BIGQUERY_PROTO_UPDATE",
        "SILK_CHIFFON_BIGQUERY_PROTO_SOURCE_ROOT",
        "SILK_CHIFFON_BIGQUERY_PROTO_UPDATE_ROOT",
    ] {
        println!("cargo:rerun-if-env-changed={variable}");
    }

    if update_requested {
        proto_mirror::update_generated_mirror(&out_dir, &generated_dir, GENERATED_FILES)?;
    }
    if env::var_os("SILK_CHIFFON_BIGQUERY_PROTO_CHECK").is_some() {
        proto_mirror::check_generated_mirror(&out_dir, &generated_dir, GENERATED_FILES)?;
    }
    Ok(())
}

fn main() {
    generate_protos().expect("Storage Read proto generation should succeed");
}
