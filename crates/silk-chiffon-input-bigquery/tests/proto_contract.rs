use std::{fs, path::Path};

#[test]
fn pinned_proto_closure_and_generated_mirror_are_complete() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in [
        "proto/PROVENANCE.json",
        "proto/licenses/LICENSE",
        "proto/licenses/PROTOBUF_LICENSE",
        "proto/google/cloud/bigquery/storage/v1/storage.proto",
        "proto/google/rpc/error_details.proto",
        "proto/google/api/routing.proto",
        "src/proto/generated/bq_storage_descriptor.bin",
        "src/proto/generated/google.api.rs",
        "src/proto/generated/google.cloud.bigquery.storage.v1.rs",
        "src/proto/generated/google.rpc.rs",
    ] {
        assert!(crate_root.join(relative).is_file(), "missing {relative}");
    }

    let provenance = fs::read_to_string(crate_root.join("proto/PROVENANCE.json")).unwrap();
    assert!(provenance.contains("5f933a6c53e57f87e47e3725fceca08cf24b5b16"));
    assert!(provenance.contains("protoc-bin-vendored"));
}

#[test]
fn proto_mirror_contains_no_host_metadata_files() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut pending = vec![crate_root.join("proto")];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let name = path.file_name().unwrap().to_string_lossy();
                assert!(
                    !name.starts_with("._"),
                    "host metadata file: {}",
                    path.display()
                );
            }
        }
    }
}
