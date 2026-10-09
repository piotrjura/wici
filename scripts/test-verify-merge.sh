#!/usr/bin/env bash
# Check merge gate ordering and failure propagation without running builds.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT
mkdir -p "$fixture/scripts" "$fixture/bin"
cp "$root/scripts/verify-merge.sh" "$fixture/scripts/"
cat > "$fixture/bin/uname" <<'STUB'
#!/usr/bin/env bash
echo "${TEST_PLATFORM:-Darwin}"
STUB
for name in verify verify-msrv verify-swift; do
    cat > "$fixture/scripts/$name.sh" <<'STUB'
#!/usr/bin/env bash
set -eu
name="$(basename "$0" .sh)"
echo "$name $*" >> "$TEST_LOG"
if [[ "${TEST_FAIL:-}" == "$name" ]]; then exit 23; fi
STUB
done
chmod +x "$fixture/bin/uname" "$fixture/scripts/"*.sh
export PATH="$fixture/bin:$PATH" TEST_LOG="$fixture/log"

"$fixture/scripts/verify-merge.sh" >/dev/null
printf 'verify \nverify-msrv \nverify-swift --all\n' > "$fixture/expected"
diff -u "$fixture/expected" "$TEST_LOG"

for name in verify verify-msrv verify-swift; do
    : > "$TEST_LOG"
    status=0
    TEST_FAIL="$name" "$fixture/scripts/verify-merge.sh" >/dev/null || status=$?
    [[ "$status" == 23 ]]
    [[ "$(tail -n 1 "$TEST_LOG")" == "$name "* ]]
done

: > "$TEST_LOG"
status=0
TEST_PLATFORM=Linux "$fixture/scripts/verify-merge.sh" > "$fixture/output" 2>&1 || status=$?
[[ "$status" == 1 && ! -s "$TEST_LOG" ]]
[[ "$(cat "$fixture/output")" == *'requires macOS'* ]]

# Read MSRV from the manifest and preserve Cargo failures.
cp "$root/scripts/verify-msrv.sh" "$fixture/scripts/"
printf '\n' > "$fixture/scripts/tool-path.sh"
cat > "$fixture/bin/cargo" <<'STUB'
#!/usr/bin/env bash
echo "$*" > "$TEST_LOG"
exit "${TEST_CARGO_STATUS:-0}"
STUB
chmod +x "$fixture/bin/cargo"
printf 'rust-version = "1.85"\n' > "$fixture/Cargo.toml"
"$fixture/scripts/verify-msrv.sh"
[[ "$(cat "$TEST_LOG")" == '+1.85 check --workspace --all-features --locked' ]]
status=0
TEST_CARGO_STATUS=23 "$fixture/scripts/verify-msrv.sh" || status=$?
[[ "$status" == 23 ]]
: > "$fixture/Cargo.toml"
: > "$TEST_LOG"
status=0
"$fixture/scripts/verify-msrv.sh" > "$fixture/output" 2>&1 || status=$?
[[ "$status" == 1 && ! -s "$TEST_LOG" ]]
[[ "$(cat "$fixture/output")" == *'has no rust-version'* ]]
echo 'Merge gate tests passed.'
