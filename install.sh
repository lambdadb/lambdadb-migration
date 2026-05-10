#!/bin/sh
set -eu

REPO="${LAMBDADB_MIGRATION_REPO:-lambdadb/lambdadb-migration}"
BINARY="lambdadb-migration"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
VERSION="${VERSION:-latest}"
VERIFY_CHECKSUM="${VERIFY_CHECKSUM:-1}"
DRY_RUN="${DRY_RUN:-0}"

usage() {
  cat <<'EOF'
Install lambdadb-migration from GitHub Releases.

Usage:
  install.sh [options]

Options:
  --repo OWNER/REPO       GitHub repository. Default: lambdadb/lambdadb-migration
  --version VERSION      Release version, for example v0.1.0. Default: latest
  --install-dir DIR      Directory to install the binary. Default: /usr/local/bin
  --no-verify            Skip checksum verification.
  --dry-run              Print what would be installed without downloading.
  -h, --help             Show this help.

Environment:
  LAMBDADB_MIGRATION_REPO
  VERSION
  INSTALL_DIR
  VERIFY_CHECKSUM=0
  DRY_RUN=1
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --repo)
      REPO="${2:-}"
      shift 2
      ;;
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --install-dir)
      INSTALL_DIR="${2:-}"
      shift 2
      ;;
    --no-verify)
      VERIFY_CHECKSUM="0"
      shift
      ;;
    --dry-run)
      DRY_RUN="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [ -z "$REPO" ] || [ -z "$VERSION" ] || [ -z "$INSTALL_DIR" ]; then
  echo "error: repo, version, and install dir must be non-empty" >&2
  exit 1
fi

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

need curl
need tar

detect_os() {
  case "$(uname -s)" in
    Darwin) echo "darwin" ;;
    Linux) echo "linux" ;;
    *)
      echo "error: unsupported OS: $(uname -s)" >&2
      exit 1
      ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "amd64" ;;
    arm64|aarch64) echo "arm64" ;;
    *)
      echo "error: unsupported architecture: $(uname -m)" >&2
      exit 1
      ;;
  esac
}

resolve_version() {
  if [ "$VERSION" != "latest" ]; then
    echo "$VERSION"
    return
  fi
  curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/$REPO/releases/latest" | sed 's#.*/##'
}

checksum_cmd() {
  if command -v sha256sum >/dev/null 2>&1; then
    echo "sha256sum"
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    echo "shasum -a 256"
    return
  fi
  echo ""
}

OS="$(detect_os)"
ARCH="$(detect_arch)"
RESOLVED_VERSION="$(resolve_version)"
ASSET="${BINARY}_${RESOLVED_VERSION#v}_${OS}_${ARCH}.tar.gz"
BASE_URL="https://github.com/$REPO/releases/download/$RESOLVED_VERSION"
ASSET_URL="$BASE_URL/$ASSET"
CHECKSUM_URL="$BASE_URL/checksums.txt"

cat <<EOF
Repository:  $REPO
Version:     $RESOLVED_VERSION
Platform:    ${OS}_${ARCH}
Asset:       $ASSET
Install dir: $INSTALL_DIR
EOF

if [ "$DRY_RUN" = "1" ]; then
  echo "Dry run: would download $ASSET_URL"
  exit 0
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT INT TERM

ARCHIVE="$TMP_DIR/$ASSET"
curl -fsSL "$ASSET_URL" -o "$ARCHIVE"

if [ "$VERIFY_CHECKSUM" = "1" ]; then
  SUM_CMD="$(checksum_cmd)"
  if [ -z "$SUM_CMD" ]; then
    echo "error: sha256sum or shasum is required for checksum verification" >&2
    exit 1
  fi
  CHECKSUMS="$TMP_DIR/checksums.txt"
  curl -fsSL "$CHECKSUM_URL" -o "$CHECKSUMS"
  EXPECTED="$(grep "  $ASSET\$" "$CHECKSUMS" | awk '{print $1}')"
  if [ -z "$EXPECTED" ]; then
    echo "error: checksum for $ASSET not found" >&2
    exit 1
  fi
  ACTUAL="$($SUM_CMD "$ARCHIVE" | awk '{print $1}')"
  if [ "$EXPECTED" != "$ACTUAL" ]; then
    echo "error: checksum mismatch for $ASSET" >&2
    exit 1
  fi
fi

tar -xzf "$ARCHIVE" -C "$TMP_DIR"
if [ ! -x "$TMP_DIR/$BINARY" ]; then
  chmod +x "$TMP_DIR/$BINARY"
fi

mkdir -p "$INSTALL_DIR"
if [ -w "$INSTALL_DIR" ]; then
  mv "$TMP_DIR/$BINARY" "$INSTALL_DIR/$BINARY"
else
  echo "Installing to $INSTALL_DIR requires elevated permissions."
  sudo mv "$TMP_DIR/$BINARY" "$INSTALL_DIR/$BINARY"
fi

echo "Installed $BINARY to $INSTALL_DIR/$BINARY"
"$INSTALL_DIR/$BINARY" --version
