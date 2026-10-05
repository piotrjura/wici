#!/usr/bin/env bash
# Run a command with WICI_TEST_DATABASE_URL set. Starts a temporary
# PostgreSQL cluster unless the variable is already set. SQLite test files
# go to a directory that is deleted on exit.
set -euo pipefail

if [[ -n "${WICI_TEST_DATABASE_URL:-}" ]]; then
    exec "$@"
fi

dir="$(mktemp -d)"
cleanup() {
    pg_ctl -D "$dir/data" -m immediate stop >/dev/null 2>&1 || true
    rm -rf "$dir"
}
trap cleanup EXIT

port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
initdb -D "$dir/data" -U postgres -A trust --no-sync >/dev/null
pg_ctl -D "$dir/data" -l "$dir/log" -w \
    -o "-p $port -k $dir -c listen_addresses=127.0.0.1 -c fsync=off -c max_connections=200" \
    start >/dev/null

export WICI_TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$port/postgres"
export WICI_TEST_SQLITE_DIR="$dir/sqlite"
"$@"
