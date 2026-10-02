#!/usr/bin/env bash
# Source from build scripts: GUI Git clients may have only the system PATH.

wici_add_tool_path() {
    [ -d "$1" ] || return 0
    case ":${PATH:-}:" in
        *":$1:"*) ;;
        *) PATH="${PATH:+$PATH:}$1" ;;
    esac
}

# Keep caller-selected versions first.
wici_add_tool_path "${HOME}/.cargo/bin"
wici_add_tool_path /opt/homebrew/bin
wici_add_tool_path /usr/local/bin
export PATH

# nvm installs Node outside Homebrew. Use its configured default only when
# Node tools are absent; never load the user's interactive shell settings.
if ! command -v node >/dev/null 2>&1 || ! command -v npx >/dev/null 2>&1; then
    export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
    if [ -s "$NVM_DIR/nvm.sh" ]; then
        . "$NVM_DIR/nvm.sh" --no-use
        nvm use --silent default || true
    fi
fi
