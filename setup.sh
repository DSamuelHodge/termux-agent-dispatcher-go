#!/data/data/com.termux/files/usr/bin/sh
# Installer for the Go agent dispatcher (agentd) on Termux.
# No Python, no pip — one static binary + verbs.yaml + boot script + ui-open helper.
#
# Piped install (latest release):
#   curl -fsSL https://github.com/DSamuelHodge/termux-agent-dispatcher-go/releases/latest/download/setup.sh | sh
# Install a specific tag:
#   sh setup.sh v1.0.0
set -e

REPO="DSamuelHodge/termux-agent-dispatcher-go"
TAG="${1:-latest}"
if [ "$TAG" = "latest" ]; then
  BASE="https://github.com/$REPO/releases/latest/download"
else
  BASE="https://github.com/$REPO/releases/download/$TAG"
fi

case "$(uname -m)" in
  aarch64|arm64) ;;
  *) echo "setup.sh: unsupported arch $(uname -m) — agentd ships aarch64 only" >&2; exit 1 ;;
esac
command -v curl >/dev/null 2>&1 || { echo "setup.sh: curl not found — run: pkg install curl" >&2; exit 1; }

INSTALL_DIR="$HOME/agent"
BOOT_DIR="$HOME/.termux/boot"
mkdir -p "$INSTALL_DIR/logs" "$INSTALL_DIR/bin" "$BOOT_DIR"
cd "$INSTALL_DIR"

echo "setup.sh: downloading agentd ($TAG) ..."
curl -fsSL -o agentd "$BASE/agentd-android-arm64"
curl -fsSL -o verbs.yaml "$BASE/verbs.yaml"
curl -fsSL -o "$BOOT_DIR/01-start-agent" "$BASE/01-start-agent"
curl -fsSL -o "$INSTALL_DIR/bin/ui-open" "$BASE/ui-open"
chmod 755 agentd "$BOOT_DIR/01-start-agent" "$INSTALL_DIR/bin/ui-open"

# (Re)start the daemon now; Termux:Boot takes over after a reboot.
if pkill -x agentd 2>/dev/null; then
  echo "setup.sh: stopped previous agentd"
  sleep 1
fi
# shellcheck disable=SC2086
./agentd >> logs/daemon.out 2>&1 &
echo "setup.sh: installed to $INSTALL_DIR, daemon started (pid $!)."
echo "setup.sh: token: first run generates $INSTALL_DIR/.agent-token (chmod 600)."
echo "setup.sh: health: curl -H \"X-Agent-Token: \$(cat $INSTALL_DIR/.agent-token)\" http://127.0.0.1:8477/health"
