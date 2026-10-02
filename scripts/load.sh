#!/usr/bin/env bash
# Load test: a release server on a temporary PostgreSQL, driven by wici-load.
# Prints the wici-load report, then CPU and memory of server and database.
# Options go to wici-load, for example: scripts/load.sh --users 2500 --active 250
# PostgreSQL runs with fsync=off, so latencies leave out disk flushes.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${WICI_TEST_DATABASE_URL:-}" ]]; then
    source scripts/tool-path.sh
    cargo build --release --locked -p wici-server -p wici-load
    exec scripts/with-postgres.sh "$0" "$@"
fi

# Each device needs a socket in wici-load and one in the server.
ulimit -n 65536 2>/dev/null || ulimit -n "$(ulimit -Hn)"

port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
db_port="${WICI_TEST_DATABASE_URL##*:}"
db_port="${db_port%%/*}"
db_pid="$(lsof -t -iTCP:"$db_port" -sTCP:LISTEN)"

WICI_DATABASE_URL="$WICI_TEST_DATABASE_URL" WICI_LISTEN="127.0.0.1:$port" \
    WICI_DATABASE_CONNECTIONS=32 RUST_LOG=warn target/release/wici-server &
server=$!
disown
samples="$(mktemp)"
cleanup() {
    kill "$server" "${sampler:-}" 2>/dev/null || true
    rm -f "$samples"
}
trap cleanup EXIT

until nc -z 127.0.0.1 "$port" 2>/dev/null; do sleep 0.1; done

# CPU % and RSS KB of a process and its children.
usage() {
    local pids="$1" children
    children="$(pgrep -d, -P "$1" || true)"
    [[ -n "$children" ]] && pids="$pids,$children"
    ps -o pcpu=,rss= -p "$pids" | awk '{ cpu += $1; rss += $2 } END { print cpu, rss }'
}
while true; do
    echo "$(usage "$server") $(usage "$db_pid")"
    sleep 1
done >"$samples" &
sampler=$!
disown

status=0
target/release/wici-load --url "ws://127.0.0.1:$port/v1/ws" "$@" || status=$?

awk '
    { n++; scs += $1; dcs += $3 }
    $1 > sc { sc = $1 } $2 > sr { sr = $2 } $3 > dc { dc = $3 } $4 > dr { dr = $4 }
    END {
        printf "server      CPU avg %.0f%%, peak %.0f%%, %.0f MB\n", scs / n, sc, sr / 1024
        printf "postgres    CPU avg %.0f%%, peak %.0f%%, %.0f MB (RSS sum)\n", dcs / n, dc, dr / 1024
    }' "$samples"
exit "$status"
