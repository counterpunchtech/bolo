#!/bin/sh
# Bolo installer — P2P mesh platform
# Usage: curl -fsSL https://raw.githubusercontent.com/bolo-mesh/bolo/main/install.sh | bash
set -e

REPO="counterpunchtech/bolo"
INSTALL_DIR="${BOLO_INSTALL_DIR:-$HOME/.bolo/bin}"
VERSION="${BOLO_VERSION:-latest}"

# --- Helpers ---

info()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
err()   { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }
ok()    { printf '\033[1;32m✓\033[0m %s\n' "$*"; }

need_cmd() {
    if ! command -v "$1" > /dev/null 2>&1; then
        err "need '$1' (command not found)"
    fi
}

# --- Detect platform ---

detect_os() {
    case "$(uname -s)" in
        Darwin) echo "apple-darwin" ;;
        Linux)  echo "unknown-linux-gnu" ;;
        *)      err "unsupported OS: $(uname -s). Bolo supports macOS and Linux." ;;
    esac
}

detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)   echo "x86_64" ;;
        arm64|aarch64)  echo "aarch64" ;;
        *)              err "unsupported architecture: $(uname -m). Bolo supports x86_64 and aarch64." ;;
    esac
}

# --- Resolve version ---

resolve_version() {
    if [ "$VERSION" = "latest" ]; then
        need_cmd curl
        VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"//;s/".*//')
        if [ -z "$VERSION" ]; then
            err "failed to fetch latest version from GitHub"
        fi
    fi
    echo "$VERSION"
}

# --- Download and install ---

install() {
    need_cmd curl
    need_cmd uname
    need_cmd mkdir
    need_cmd chmod

    local os arch target version url tmpdir

    os=$(detect_os)
    arch=$(detect_arch)
    target="${arch}-${os}"
    version=$(resolve_version)

    info "Installing bolo ${version} for ${target}"

    url="https://github.com/${REPO}/releases/download/${version}/bolo-${target}.tar.gz"
    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' EXIT

    info "Downloading from ${url}"
    if ! curl -fsSL "$url" -o "${tmpdir}/bolo.tar.gz"; then
        err "download failed. Check that version '${version}' exists at https://github.com/${REPO}/releases"
    fi

    info "Extracting to ${INSTALL_DIR}"
    mkdir -p "$INSTALL_DIR"
    tar xzf "${tmpdir}/bolo.tar.gz" -C "$INSTALL_DIR"
    chmod +x "${INSTALL_DIR}/bolo"

    ok "Installed bolo to ${INSTALL_DIR}/bolo"

    # --- Add to PATH ---

    add_to_path

    # --- Initialize ---

    info "Initializing identity..."
    "${INSTALL_DIR}/bolo" daemon init 2>/dev/null || true

    # --- Done ---

    printf '\n'
    ok "Bolo installed successfully!"
    printf '\n'
    printf '  Run \033[1mbolo daemon start\033[0m to start the mesh daemon\n'
    printf '  Run \033[1mbolo peer add <node-id>\033[0m to connect to a peer\n'
    printf '  Run \033[1mbolo --help\033[0m for all commands\n'
    printf '\n'
    printf '  Docs: https://github.com/%s\n' "$REPO"
    printf '\n'

    # Warn if not on PATH yet (new shell needed)
    if ! command -v bolo > /dev/null 2>&1; then
        printf '  \033[33mNote:\033[0m Restart your shell or run:\n'
        printf '    export PATH="%s:$PATH"\n\n' "$INSTALL_DIR"
    fi
}

add_to_path() {
    local line="export PATH=\"${INSTALL_DIR}:\$PATH\""

    # Already on PATH?
    case ":${PATH}:" in
        *":${INSTALL_DIR}:"*) return ;;
    esac

    # Detect shell config file
    local rcfile=""
    case "${SHELL:-}" in
        */zsh)  rcfile="$HOME/.zshrc" ;;
        */bash)
            if [ -f "$HOME/.bash_profile" ]; then
                rcfile="$HOME/.bash_profile"
            else
                rcfile="$HOME/.bashrc"
            fi
            ;;
        */fish)
            # Fish uses a different syntax
            local fishline="fish_add_path ${INSTALL_DIR}"
            local fishrc="$HOME/.config/fish/config.fish"
            if [ -f "$fishrc" ] && ! grep -qF "$INSTALL_DIR" "$fishrc" 2>/dev/null; then
                echo "$fishline" >> "$fishrc"
                info "Added to ${fishrc}"
            fi
            return
            ;;
        *)
            if [ -f "$HOME/.profile" ]; then
                rcfile="$HOME/.profile"
            fi
            ;;
    esac

    if [ -n "$rcfile" ]; then
        if ! grep -qF "$INSTALL_DIR" "$rcfile" 2>/dev/null; then
            echo "" >> "$rcfile"
            echo "# Bolo mesh platform" >> "$rcfile"
            echo "$line" >> "$rcfile"
            info "Added ${INSTALL_DIR} to PATH in ${rcfile}"
        fi
    fi
}

install
