fn main() {
    if cfg!(target_os = "macos") {
        // support shared libs from homebrew
        println!("cargo:rustc-link-search=/opt/homebrew/lib");
    }

    if cfg!(target_os = "linux") {
        // support shared libs from apt
        println!("cargo:rustc-link-search=/usr/local/lib");
    }

    // link the duckdb library
    println!("cargo:rustc-link-lib=duckdb");
}
