#!/usr/bin/env bash
# Run `swift test` with a wici-server on WICI_TEST_DATABASE_URL.
set -euo pipefail

cd "$(dirname "$0")/.."
port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
WICI_DATABASE_URL="$WICI_TEST_DATABASE_URL" WICI_LISTEN="127.0.0.1:$port" target/debug/wici-server &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
for _ in $(seq 50); do
    curl -fs "http://127.0.0.1:$port/health" >/dev/null && break
    sleep 0.1
done
cd swift
WICI_TEST_SERVER_URL="ws://127.0.0.1:$port/v1/ws" swift test
