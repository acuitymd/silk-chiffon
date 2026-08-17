#!/usr/bin/env bash
set -euo pipefail

if [[ -n "$(git status --porcelain)" ]]; then
    echo "clean-archive verification requires a clean worktree" >&2
    exit 1
fi

archive_dir="$(mktemp -d)"
trap 'rm -rf "${archive_dir}"' EXIT

git archive HEAD | tar -x -C "${archive_dir}"
export SILK_CHIFFON_VERSION_OVERRIDE=clean-archive
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$(pwd)/target}/clean-archive"

cd "${archive_dir}"
cargo metadata --locked --no-deps --format-version 1 >/dev/null
cargo check --locked --workspace --all-targets --all-features
bash scripts/check_package_inventory.sh
