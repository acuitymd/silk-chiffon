use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;
use sha2::{Digest, Sha256};

fn files_below(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap()
        .to_str()
        .unwrap()
        .replace('\\', "/")
}

fn sha256(path: &Path) -> String {
    let mut hexadecimal = String::with_capacity(64);
    for byte in Sha256::digest(fs::read(path).unwrap()) {
        write!(&mut hexadecimal, "{byte:02x}").unwrap();
    }
    hexadecimal
}

#[test]
fn vendored_proto_closure_matches_its_provenance_and_licenses() {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let proto_root = crate_root.join("proto");
    let provenance: Value =
        serde_json::from_slice(&fs::read(proto_root.join("PROVENANCE.json")).unwrap()).unwrap();

    let declared_files = provenance["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            let relative = entry["path"].as_str().unwrap();
            let source = entry["source"].as_str().unwrap();
            assert!(matches!(source, "googleapis" | "protobuf"));
            assert_eq!(
                entry["sha256"].as_str().unwrap(),
                sha256(&proto_root.join(relative))
            );
            relative.to_owned()
        })
        .collect::<BTreeSet<_>>();
    let actual_files = files_below(&proto_root.join("google"))
        .into_iter()
        .map(|path| {
            assert_eq!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("proto")
            );
            relative_path(&proto_root, &path)
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(declared_files, actual_files);

    for root in provenance["roots"].as_array().unwrap() {
        assert!(declared_files.contains(root.as_str().unwrap()));
    }

    let declared_licenses = provenance["licenses"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            let relative = entry["path"].as_str().unwrap();
            assert_eq!(
                entry["sha256"].as_str().unwrap(),
                sha256(&proto_root.join(relative))
            );
            (
                relative.to_owned(),
                entry["source"].as_str().unwrap().to_owned(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        declared_licenses.keys().cloned().collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "licenses/LICENSE".to_owned(),
            "licenses/PROTOBUF_LICENSE".to_owned(),
        ])
    );
    assert!(declared_licenses["licenses/LICENSE"].contains("googleapis"));
    assert!(declared_licenses["licenses/PROTOBUF_LICENSE"].contains("protocolbuffers"));
}

#[test]
fn vendored_proto_tree_contains_only_third_party_protocol_material() {
    let proto_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("proto");
    for path in files_below(&proto_root) {
        let relative = relative_path(&proto_root, &path);
        assert!(
            !path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("._")
        );
        assert!(
            relative == "PROVENANCE.json"
                || relative.starts_with("licenses/")
                || relative.starts_with("google/")
                    && Path::new(&relative)
                        .extension()
                        .is_some_and(|extension| extension.eq_ignore_ascii_case("proto")),
            "unexpected vendored file: {relative}"
        );
    }
}
