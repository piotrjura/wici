#!/usr/bin/env bash
# Run all local merge checks. Cross-platform CI must also pass.
set -euo pipefail

cd "$(dirname "$0")/.."
if [[ "$(uname -s)" != Darwin ]]; then
    echo 'error: merge verification requires macOS for Swift and Apple targets' >&2
    exit 1
fi
scripts/verify.sh
scripts/verify-msrv.sh
scripts/verify-swift.sh --all
echo 'Local merge checks passed. Required Linux and macOS CI must also pass.'
