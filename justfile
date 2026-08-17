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
    cargo bench --workspace --all-features --locked

build:
    cargo build --release

build-native:
    RUSTFLAGS="-C link-arg=-fuse-ld=lld -C target-cpu=native" cargo build --profile native

build-profiling:
    cargo build --profile profiling

test *args:
    RUST_BACKTRACE=1 cargo nextest run --workspace --all-features --locked {{args}}

type-check:
    cargo check --workspace --all-features --locked

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
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

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

docs-check:
    cargo nextest run --locked --all-features -p silk_chiffon --test cli_docs --test documentation_links
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked

check-links:
    find . -path ./target -prune -o -name '*.md' -print0 | xargs -0 lychee --no-progress

test-bigquery:
    RUST_BACKTRACE=1 cargo nextest run -p silk-chiffon-input-bigquery --all-targets --locked

test-bigquery-adversarial:
    RUST_BACKTRACE=1 cargo nextest run -p silk-chiffon-input-bigquery -E 'test(/(decode|fault|pushdown|read_stream|retry|session|transport)/)'

check-provider-features:
    #!/usr/bin/env bash
    set -euo pipefail
    feature_sets=(
        ""
        "local-bare-paths"
        "gcs"
        "s3"
        "bigquery"
        "local-bare-paths,gcs"
        "local-bare-paths,s3"
        "local-bare-paths,bigquery"
        "gcs,s3"
        "gcs,bigquery"
        "s3,bigquery"
        "local-bare-paths,gcs,s3"
        "local-bare-paths,gcs,bigquery"
        "local-bare-paths,s3,bigquery"
        "gcs,s3,bigquery"
        "local-bare-paths,gcs,s3,bigquery"
    )
    for features in "${feature_sets[@]}"; do
        arguments=(--locked --no-default-features)
        if [[ -n "${features}" ]]; then
            arguments+=(--features "${features}")
        fi
        cargo check --all-targets "${arguments[@]}"
        cargo nextest run -p silk_chiffon --test registration_cli "${arguments[@]}"
    done
    cargo check --locked --all-targets
    cargo nextest run --locked -p silk_chiffon --test registration_cli

check-storage-features:
    #!/usr/bin/env bash
    set -euo pipefail
    feature_sets=(
        ""
        "local"
        "local-bare-paths"
        "gcs"
        "s3"
        "local,gcs"
        "local,s3"
        "gcs,s3"
        "local,gcs,s3"
        "local-bare-paths,gcs"
        "local-bare-paths,s3"
        "local-bare-paths,gcs,s3"
    )
    for features in "${feature_sets[@]}"; do
        arguments=(--locked --no-default-features)
        if [[ -n "${features}" ]]; then
            arguments+=(--features "${features}")
        fi
        cargo nextest run -p silk-chiffon-storage "${arguments[@]}"
    done
    cargo nextest run --locked -p silk-chiffon-storage --all-features

alias check-bigquery-features := check-provider-features

test-bigquery-live:
    cargo test -p silk-chiffon-input-bigquery --lib provider::tests::live_small_table_writes_arrow_and_parquet -- --ignored --exact

test-bigquery-benchmark:
    uv run python -W error::ResourceWarning -m unittest scripts/tests/test_bqs_benchmark.py

benchmark-bigquery *args: build-native test-bigquery-benchmark
    uv run scripts/bqs_benchmark.py campaign {{args}}

test-cloud-live-soak:
    cargo test --test cloud_live_soak live_seeded_mixed_input_cross_provider_soak -- --ignored --exact --nocapture

check-live-targets:
    cargo test --locked -p silk-chiffon-storage --test cloud_live --no-default-features --features gcs --no-run
    cargo test --locked -p silk-chiffon-storage --test cloud_live --no-default-features --features s3 --no-run
    cargo test --locked --test cloud_live_e2e --no-default-features --features gcs --no-run
    cargo test --locked --test cloud_live_e2e --no-default-features --features s3 --no-run
    cargo test --locked --test bigquery_transform_integration --no-default-features --features bigquery-integration-tests --no-run
    cargo test --locked --test cloud_live_soak --no-run

test-gcs-live:
    cargo test --locked -p silk-chiffon-storage --test cloud_live --features gcs live_gcs_exact_patterns_ranges_outputs_multipart_claims_and_cleanup -- --ignored --exact
    cargo test --locked --test cloud_live_e2e --features gcs live_gcs_composed_cli_detects_inspects_transforms_verifies_and_cleans_up -- --ignored --exact

test-s3-live:
    cargo test --locked -p silk-chiffon-storage --test cloud_live --features s3 live_s3_exact_patterns_ranges_outputs_multipart_claims_and_cleanup -- --ignored --exact
    cargo test --locked --test cloud_live_e2e --features s3 live_s3_composed_cli_detects_inspects_transforms_verifies_and_cleans_up -- --ignored --exact

check-benchmarks:
    cargo bench --workspace --all-features --locked --no-run
    just test-bigquery-benchmark

check-package-inventory:
    bash scripts/check_package_inventory.sh

check-clean-archive:
    bash scripts/check_clean_archive.sh

ci: fmt-check type-check lint-check test docs-check check-provider-features check-storage-features check-live-targets check-benchmarks check-package-inventory check-clean-archive

verify: fmt-fix docs type-check lint-check test docs-check check-provider-features check-storage-features check-live-targets check-benchmarks check-package-inventory
