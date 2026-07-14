//! Guards that `docs/CLI.md` is in sync with the CLI definitions.
//!
//! The committed reference is `dprint fmt` applied to the generated Markdown, so
//! the check formats the freshly generated output the same way before comparing.
//! Gated on the `docs` feature, so it runs inside the existing test job
//! (`cargo nextest run --all-features`) without adding a build. Needs `dprint` on
//! PATH (CI installs it via taiki-e; local dev already has it for `just`).
#![cfg(feature = "docs")]

use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn cli_docs_are_current() {
    let manifest = env!("CARGO_MANIFEST_DIR");

    // format the freshly generated Markdown through dprint (repo config) so it
    // matches how `just docs` writes the committed file
    let mut dprint = Command::new("dprint")
        .args(["fmt", "--stdin", "docs/CLI.md"])
        .current_dir(manifest)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("dprint not found on PATH; install it (see the justfile) to run this test");

    dprint
        .stdin
        .take()
        .unwrap()
        .write_all(silk_chiffon::cli_markdown().as_bytes())
        .unwrap();
    let out = dprint.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "dprint failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let generated = String::from_utf8(out.stdout).expect("dprint output is not UTF-8");

    let committed = std::fs::read_to_string(format!("{manifest}/docs/CLI.md"))
        .expect("docs/CLI.md is missing; run `just docs`");

    if generated != committed {
        panic!(
            "docs/CLI.md is out of date. Run `just docs` (or `just verify`) and commit the result."
        );
    }
}
