//! Guards that `docs/CLI.md` is in sync with the CLI definitions.
//!
//! Gated on the `docs` feature, so it runs inside the existing test job
//! (`cargo nextest run --all-features`) without adding a build or a CI job. A
//! plain `cargo test` without the feature compiles this file to nothing.
#![cfg(feature = "docs")]

#[test]
fn cli_docs_are_current() {
    let generated = silk_chiffon::cli_markdown();
    let committed = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/docs/CLI.md"))
        .expect("docs/CLI.md is missing; run `just docs`");

    // compare by hand rather than assert_eq! so a mismatch prints the fix, not
    // a many-KB diff of the whole reference
    if generated != committed {
        panic!(
            "docs/CLI.md is out of date. Run `just docs` (or `just verify`) and commit the result."
        );
    }
}
