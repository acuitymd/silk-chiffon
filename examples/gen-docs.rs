//! Regenerates `docs/CLI.md` from the clap command definitions.
//!
//! Run via `just docs` (which passes `--features docs`). `just verify` runs it
//! too, so the committed reference tracks the CLI without anyone remembering to.
//!
//! Declared in Cargo.toml with `required-features = ["docs"]`, so `--all-targets`
//! skips it unless the `docs` feature is on.

fn main() -> std::io::Result<()> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/docs");
    std::fs::create_dir_all(dir)?;
    let path = format!("{dir}/CLI.md");
    std::fs::write(&path, silk_chiffon::cli_markdown())?;
    eprintln!("wrote {path}");
    Ok(())
}
