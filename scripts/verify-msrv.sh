#!/usr/bin/env bash
# Check the minimum Rust version declared by the workspace.
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/tool-path.sh
msrv="$(sed -n 's/^rust-version = "\(.*\)"/\1/p' Cargo.toml)"
if [[ -z "$msrv" ]]; then
    echo 'error: Cargo.toml has no rust-version' >&2
    exit 1
fi
cargo +"$msrv" check --workspace --all-features --locked
