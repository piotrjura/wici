#!/usr/bin/env bash
# Build the server image, check health, a clean stop, and data kept on restart.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
image=wici-server:test
name="wici-test-$$"
volume="wici-test-$$"
port="${WICI_TEST_PORT:-18080}"
trap 'docker rm -f "$name" >/dev/null 2>&1 || true; docker volume rm -f "$volume" >/dev/null 2>&1 || true' EXIT

docker build -t "$image" "$root"

start() {
    docker run -d --name "$name" -p "127.0.0.1:$port:8080" -v "$volume:/data" "$image" >/dev/null
    for _ in $(seq 100); do
        if curl -sf "http://127.0.0.1:$port/health" >/dev/null; then return 0; fi
        sleep 0.1
    done
    docker logs "$name"
    echo "server did not become healthy" >&2
    return 1
}

# A clean stop exits 0 before the timeout. SIGKILL after the timeout exits 137.
stop() {
    docker stop -t 5 "$name" >/dev/null
    local code
    code="$(docker inspect -f '{{.State.ExitCode}}' "$name")"
    docker rm "$name" >/dev/null
    if [ "$code" != 0 ]; then
        echo "server exited with $code, expected a clean stop" >&2
        return 1
    fi
}

start
stop
start
docker run --rm -v "$volume:/data" busybox test -s /data/wici.db
stop
echo "docker image ok"
