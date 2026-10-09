#!/usr/bin/env bash
# Run every code check that CI runs. Stop at the first failure.
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/tool-path.sh

readonly MIN_LINE_COVERAGE=90

require() {
    if ! command -v "$1" >/dev/null 2>&1 && ! cargo "$1" --version >/dev/null 2>&1; then
        echo "error: '$1' is missing. Install it with: $2" >&2
        exit 1
    fi
}

step() {
    echo "==> $*"
    "$@"
}

step scripts/test-tool-path.sh
step scripts/test-verify-merge.sh

require cargo-deny "cargo install --locked cargo-deny"
require cargo-llvm-cov "cargo install --locked cargo-llvm-cov"
require npx "install Node.js"
require initdb "install PostgreSQL (initdb, pg_ctl)"

step cargo fmt --all -- --check
step cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
step scripts/with-postgres.sh cargo test --workspace --all-features --locked
step cargo test --workspace --all-features --locked --doc
RUSTDOCFLAGS="-D warnings" step cargo doc --workspace --all-features --no-deps --locked
step cargo deny --locked check
step scripts/with-postgres.sh cargo llvm-cov --workspace --all-features --locked --fail-under-lines "$MIN_LINE_COVERAGE"
step npx --yes jscpd@4 --config .jscpd.json

echo "All checks passed."
