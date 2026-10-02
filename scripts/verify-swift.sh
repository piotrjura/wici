#!/usr/bin/env bash
# Build the XCFramework and run the Swift tests against a real server.
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/tool-path.sh
scripts/build-xcframework.sh
cargo build -p wici-server --locked
scripts/with-postgres.sh scripts/swift-e2e.sh
