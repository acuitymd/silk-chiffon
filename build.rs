fn main() {
    // support shared libs from homebrew
    println!("cargo:rustc-link-search=/opt/homebrew/lib");

    // link the duckdb library
    println!("cargo:rustc-link-lib=duckdb");
}
