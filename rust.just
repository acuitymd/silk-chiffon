build:
    cargo build --release

test:
    RUST_BACKTRACE=1 cargo test --verbose

type-check:
    cargo check --all-features

fmt-check:
    cargo fmt --check

fmt-fix:
    cargo fmt

lint-check:
    cargo clippy --all-targets --all-features -- -D warnings

lint-fix:
    cargo clippy --all-targets --all-features --fix -- -D warnings
