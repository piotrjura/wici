#!/usr/bin/env bash
# Build swift/build/WiciFFI.xcframework from wici-ffi.
# Default: macOS (arm64 + x86_64). With --all: also iOS and iOS simulator.
set -euo pipefail

cd "$(dirname "$0")/.."
source scripts/tool-path.sh

out="swift/build"
lib="libwici_ffi.a"
export MACOSX_DEPLOYMENT_TARGET=13.0
export IPHONEOS_DEPLOYMENT_TARGET=16.0

build() {
    cargo build -p wici-ffi --release --locked --target "$1" >&2
    echo "target/$1/release/$lib"
}

rm -rf "$out"
mkdir -p "$out/headers" "$out/macos"
cp crates/wici-ffi/include/wici.h "$out/headers/"
cat > "$out/headers/module.modulemap" <<'MAP'
module WiciFFI {
    header "wici.h"
    export *
}
MAP

lipo -create "$(build aarch64-apple-darwin)" "$(build x86_64-apple-darwin)" -output "$out/macos/$lib"
args=(-library "$out/macos/$lib" -headers "$out/headers")
if [[ "${1:-}" == "--all" ]]; then
    args+=(-library "$(build aarch64-apple-ios)" -headers "$out/headers")
    args+=(-library "$(build aarch64-apple-ios-sim)" -headers "$out/headers")
fi
xcodebuild -create-xcframework "${args[@]}" -output "$out/WiciFFI.xcframework" >/dev/null
echo "Built $out/WiciFFI.xcframework"
