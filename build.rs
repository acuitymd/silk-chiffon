use std::{env, fs, path::Path, process::Command};

fn git_output(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|output| output.trim().to_string())
        .filter(|output| !output.is_empty())
}

fn main() {
    let version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string());
    let git_hash = git_output(&["rev-parse", "--short=8", "HEAD"]).unwrap_or_default();

    let full_version = if git_hash.is_empty() {
        version.clone()
    } else {
        format!("{version}-{git_hash}")
    };

    println!("cargo:rustc-env=GIT_HASH={git_hash}");
    println!("cargo:rustc-env=SILK_CHIFFON_VERSION={full_version}");

    if let Some(head_path) = git_output(&["rev-parse", "--git-path", "HEAD"]) {
        println!("cargo:rerun-if-changed={head_path}");

        if let Ok(head_contents) = fs::read_to_string(&head_path)
            && let Some(ref_path) = head_contents.trim().strip_prefix("ref: ")
            && let Some(ref_git_path) = git_output(&["rev-parse", "--git-path", ref_path])
        {
            println!("cargo:rerun-if-changed={ref_git_path}");
        }
    } else if Path::new(".git/HEAD").exists() {
        println!("cargo:rerun-if-changed=.git/HEAD");

        if let Ok(head_contents) = fs::read_to_string(".git/HEAD")
            && let Some(ref_path) = head_contents.trim().strip_prefix("ref: ")
        {
            let ref_path = format!(".git/{}", ref_path);
            println!("cargo:rerun-if-changed={ref_path}");
        }
    }
}
