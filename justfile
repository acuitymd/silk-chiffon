@default:
    echo
    echo 'Usage:'
    echo
    echo '    Run `just` to list all tasks.'
    echo '    Run `just <task>` to run a task.'
    echo
    echo 'Tasks:'
    echo
    just --list --unsorted --list-heading '' --list-submodules
    echo

alias ls := default

benchmark:
    cargo bench

build:
    cargo build --release

build-native:
    RUSTFLAGS="-C link-arg=-fuse-ld=lld -C target-cpu=native" cargo build --profile native

build-profiling:
    cargo build --profile profiling

test *args:
    RUST_BACKTRACE=1 cargo nextest run --workspace --all-features --locked {{args}}

type-check:
    cargo check --workspace --all-features

_check-zigbuild:
    @which cargo-zigbuild > /dev/null || (echo "error: cargo-zigbuild not installed. Run: cargo install cargo-zigbuild && brew install zig" && exit 1)
    @rustup target list --installed | grep -q x86_64-unknown-linux-gnu || (echo "error: Linux target not installed. Run: rustup target add x86_64-unknown-linux-gnu" && exit 1)

type-check-linux: _check-zigbuild
    cargo zigbuild --workspace --all-features --target x86_64-unknown-linux-gnu

alias type := type-check
alias check := type-check

fmt-check:
    cargo fmt --check
    dprint check

fmt-fix:
    cargo fmt
    dprint fmt

alias fmt := fmt-fix

lint-check:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

lint-check-linux: _check-zigbuild
    cargo zigbuild clippy --workspace --target x86_64-unknown-linux-gnu --all-targets --all-features -- -D warnings

lint-fix:
    cargo clippy --workspace --all-targets --all-features --fix --allow-dirty -- -D warnings

lint-fix-linux: _check-zigbuild
    cargo zigbuild clippy --workspace --target x86_64-unknown-linux-gnu --all-targets --all-features --fix --allow-dirty -- -D warnings

alias lint := lint-fix

docs:
    cargo run --features docs,gcs,s3,bigquery --example gen-docs
    dprint fmt docs/CLI.md

test-bigquery:
    RUST_BACKTRACE=1 cargo nextest run -p silk-chiffon-input-bigquery --all-targets --locked

test-bigquery-adversarial:
    RUST_BACKTRACE=1 cargo nextest run -p silk-chiffon-input-bigquery -E 'test(/(decode|fault|pushdown|read_stream|retry|session|transport)/)'

check-bigquery-features:
    cargo check --no-default-features
    cargo check --no-default-features --features local-bare-paths
    cargo check --no-default-features --features gcs
    cargo check --no-default-features --features s3
    cargo check --no-default-features --features bigquery
    cargo check --no-default-features --features gcs,bigquery
    cargo check --no-default-features --features s3,bigquery
    cargo check --no-default-features --features local-bare-paths,gcs,s3,bigquery
    cargo check --all-features

test-bigquery-live:
    cargo test -p silk-chiffon-input-bigquery --lib provider::tests::live_small_table_writes_arrow_and_parquet -- --ignored --exact

test-cloud-live-soak:
    cargo test --test cloud_live_soak live_seeded_mixed_input_cross_provider_soak -- --ignored --exact --nocapture

verify: type-check fmt-fix lint-check docs test-bigquery check-bigquery-features
