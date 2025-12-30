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
    RUST_BACKTRACE=1 cargo nextest run --all-features --locked {{args}}

type-check:
    cargo check --all-features

alias type := type-check
alias check := type-check

fmt-check:
    cargo fmt --check

fmt-fix:
    cargo fmt

alias fmt := fmt-fix

lint-check:
    cargo clippy --all-targets --all-features -- -D warnings

lint-fix:
    cargo clippy --all-targets --all-features --fix --allow-dirty -- -D warnings

alias lint := lint-fix
