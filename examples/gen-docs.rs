//! Regenerates `docs/CLI.md` from the clap command definitions.
//!
//! Run via `just docs` (which passes `--features docs`). `just verify` runs it
//! too, so the committed reference tracks the CLI without anyone remembering to.

#[cfg(feature = "docs")]
fn main() -> std::io::Result<()> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/docs");
    std::fs::create_dir_all(dir)?;
    let path = format!("{dir}/CLI.md");
    std::fs::write(&path, silk_chiffon::cli_markdown())?;
    eprintln!("wrote {path}");
    Ok(())
}

#[cfg(not(feature = "docs"))]
fn main() {
    eprintln!("re-run with `--features docs` (or use `just docs`)");
    std::process::exit(1);
}
