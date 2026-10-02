#!/usr/bin/env bash
# Check GUI PATH recovery without depending on installed development tools.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
mkdir -p "$dir/nvm/bin" "$dir/preferred" "$dir/with spaces"
cat > "$dir/nvm/nvm.sh" <<'NVM'
[ "$1" = --no-use ] || exit 1
nvm() {
    [ "$*" = 'use --silent default' ] || exit 1
    PATH="$NVM_DIR/bin:$PATH"
}
NVM

# Keep fixture tool selection inside a subprocess.
env NVM_DIR="$dir/nvm" PATH=/usr/bin:/bin \
    /bin/bash -eu -c '
    # Force the missing-Node branch even on CI hosts with system Node.
    command() { return 1; }
    . "$1/scripts/tool-path.sh"
    unset -f command
    if [ -d "$HOME/.cargo/bin" ]; then
        case ":$PATH:" in *":$HOME/.cargo/bin:"*) ;; *) exit 1 ;; esac
    fi
    case ":$PATH:" in *":$NVM_DIR/bin:"*) ;; *) exit 1 ;; esac
    PATH="$2/preferred"
    wici_add_tool_path "$2/with spaces"
    wici_add_tool_path "$2/with spaces"
    wici_add_tool_path "$2/missing"
    [ "$PATH" = "$2/preferred:$2/with spaces" ]
    PATH=""
    wici_add_tool_path "$2/preferred"
    [ "$PATH" = "$2/preferred" ]
    ' test "$root" "$dir"

# Existing Node selections must not load nvm or change order.
for tool in node npx; do
    printf '#!/bin/sh\nexit 0\n' > "$dir/preferred/$tool"
    chmod +x "$dir/preferred/$tool"
done
printf 'exit 1\n' > "$dir/nvm/nvm.sh"
env NVM_DIR="$dir/nvm" PATH="$dir/preferred:/usr/bin:/bin" \
    /bin/bash -eu -c '. "$1/scripts/tool-path.sh"; [ "$(command -v npx)" = "$2/preferred/npx" ]' \
    test "$root" "$dir"

# Missing nvm is harmless; the caller can still report missing tools.
env NVM_DIR="$dir/missing" /bin/bash -eu -c '
    command() { return 1; }
    . "$1/scripts/tool-path.sh"
    ' test "$root"
