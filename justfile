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

_check-zigbuild:
    @which cargo-zigbuild > /dev/null || (echo "error: cargo-zigbuild not installed. Run: cargo install cargo-zigbuild && brew install zig" && exit 1)
    @rustup target list --installed | grep -q x86_64-unknown-linux-gnu || (echo "error: Linux target not installed. Run: rustup target add x86_64-unknown-linux-gnu" && exit 1)

type-check-linux: _check-zigbuild
    cargo zigbuild --all-features --target x86_64-unknown-linux-gnu

alias type := type-check
alias check := type-check

fmt-check:
    cargo fmt --check

fmt-fix:
    cargo fmt

alias fmt := fmt-fix

lint-check:
    cargo clippy --all-targets --all-features -- -D warnings

lint-check-linux: _check-zigbuild
    cargo zigbuild clippy --target x86_64-unknown-linux-gnu --all-targets --all-features -- -D warnings

lint-fix:
    cargo clippy --all-targets --all-features --fix --allow-dirty -- -D warnings

lint-fix-linux: _check-zigbuild
    cargo zigbuild clippy --target x86_64-unknown-linux-gnu --all-targets --all-features --fix --allow-dirty -- -D warnings

alias lint := lint-fix
