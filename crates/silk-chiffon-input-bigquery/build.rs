use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

const PROTO_ROOTS: &[&str] = &[
    "google/cloud/bigquery/storage/v1/storage.proto",
    "google/rpc/error_details.proto",
    "google/api/routing.proto",
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
    let proto_dir = manifest_dir.join("proto");
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
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_with_config(prost, &proto_inputs, &[proto_dir.clone(), protoc_include])?;

    watch_proto_tree(&manifest_dir.join("proto/google"))?;
    Ok(())
}

fn main() {
    generate_protos().expect("Storage Read proto generation should succeed");
}
