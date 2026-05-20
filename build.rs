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

fn calver() -> String {
    chrono::Utc::now()
        .with_timezone(&chrono_tz::America::Chicago)
        .format("%Y-%m-%d_%H-%M-%S")
        .to_string()
}

fn main() {
    let version_override = env::var("SILK_CHIFFON_VERSION_OVERRIDE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let git_hash = git_output(&["rev-parse", "--short=8", "HEAD"]).unwrap_or_default();

    let full_version = match version_override {
        Some(version) => version,
        None if git_hash.is_empty() => "dev".to_string(),
        None => format!("dev-{}_{git_hash}", calver()),
    };

    println!("cargo:rustc-env=GIT_HASH={git_hash}");
    println!("cargo:rustc-env=SILK_CHIFFON_VERSION={full_version}");

    println!("cargo:rerun-if-env-changed=SILK_CHIFFON_VERSION_OVERRIDE");
    println!("cargo:rerun-if-env-changed=SILK_CHIFFON_WATCH_GIT_REF");
    let watch_git_ref = env::var("SILK_CHIFFON_WATCH_GIT_REF")
        .map(|value| {
            let value = value.to_ascii_lowercase();
            matches!(value.as_str(), "1" | "true" | "yes")
        })
        .unwrap_or(false);

    if watch_git_ref {
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
}
