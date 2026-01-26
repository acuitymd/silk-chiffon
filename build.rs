use std::{env, path::Path, process::Command};

fn main() {
    let version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string());
    let git_hash = Command::new("git")
        .args(["rev-parse", "--short=8", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|hash| hash.trim().to_string())
        .filter(|hash| !hash.is_empty())
        .unwrap_or_default();

    let full_version = if git_hash.is_empty() {
        version.clone()
    } else {
        format!("{version}-{git_hash}")
    };

    println!("cargo:rustc-env=GIT_HASH={git_hash}");
    println!("cargo:rustc-env=SILK_CHIFFON_VERSION={full_version}");

    if Path::new(".git/HEAD").exists() {
        println!("cargo:rerun-if-changed=.git/HEAD");
    }
    if Path::new(".git/refs/heads").exists() {
        println!("cargo:rerun-if-changed=.git/refs/heads");
    }
}
