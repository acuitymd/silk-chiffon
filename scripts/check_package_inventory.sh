#!/usr/bin/env bash
set -euo pipefail

inventory="$(cargo package -p silk_chiffon --locked --allow-dirty --list)"

for required in \
    Cargo.lock \
    Cargo.toml \
    LICENSE \
    README.md \
    docs/CLI.md \
    docs/README.md \
    docs/architecture.md \
    docs/cloud-testing.md \
    docs/development.md \
    docs/extending.md \
    src/lib.rs \
    src/main.rs; do
    grep -Fxq "${required}" <<<"${inventory}" || {
        echo "package inventory is missing ${required}" >&2
        exit 1
    }
done

for forbidden in target/ .git/; do
    if grep -Fq "${forbidden}" <<<"${inventory}"; then
        echo "package inventory contains ${forbidden}" >&2
        exit 1
    fi
done

if grep -Eq '^crates/[^/]+/src/' <<<"${inventory}"; then
    echo "root package inventory contains a nested crate source tree" >&2
    exit 1
fi
