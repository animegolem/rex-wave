#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: ./scripts/install-local.sh [--from <built-bin>] [--dest <dest-bin>] [-h|--help]

Builds rex-wave in release mode (unless --from is given), ad-hoc codesigns on
macOS, and atomically installs via a private mktemp staging directory + rename
so running rex-wave processes are never overwritten in place.
EOF
}

CALLER_PWD="$(pwd)"
SRC_BIN=""
DEST="${HOME}/.cargo/bin/rex-wave"

resolve_caller_path() {
	local p="$1"
	if [[ "$p" == /* ]]; then
		printf '%s\n' "$p"
	else
		printf '%s/%s\n' "$CALLER_PWD" "$p"
	fi
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--from)
		if [[ $# -lt 2 ]]; then
			printf 'error: --from requires a path argument\n' >&2
			exit 2
		fi
		SRC_BIN="$(resolve_caller_path "$2")"
		shift 2
		;;
	--dest)
		if [[ $# -lt 2 ]]; then
			printf 'error: --dest requires a path argument\n' >&2
			exit 2
		fi
		DEST="$(resolve_caller_path "$2")"
		shift 2
		;;
	-h|--help)
		usage
		exit 0
		;;
	*)
		printf 'error: unknown option: %s\n' "$1" >&2
		usage >&2
		exit 2
		;;
	esac
done

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_LOG=""
TMP_DIR=""

cleanup() {
	if [[ -n "$BUILD_LOG" ]]; then
		rm -f "$BUILD_LOG"
	fi
	if [[ -n "$TMP_DIR" ]]; then
		rm -rf "$TMP_DIR"
	fi
}
trap cleanup EXIT

if [[ -z "$SRC_BIN" ]]; then
	BUILD_LOG="$(mktemp /tmp/rw-install-build.XXXXXX)"
	if ! (cd "$ROOT" && cargo build --release -p rex-wave --locked --message-format=json >"$BUILD_LOG"); then
		python3 - "$BUILD_LOG" <<'PY' >&2
import json, sys
with open(sys.argv[1], "r", encoding="utf-8", errors="replace") as fh:
    for line in fh:
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if obj.get("reason") == "compiler-message":
            rendered = (obj.get("message") or {}).get("rendered")
            if rendered:
                sys.stderr.write(rendered)
PY
		printf 'error: release build of rex-wave failed\n' >&2
		exit 1
	fi
	SRC_BIN="$(python3 - "$BUILD_LOG" <<'PY'
import json, sys
exe = ""
with open(sys.argv[1], "r", encoding="utf-8", errors="replace") as fh:
    for line in fh:
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if obj.get("reason") != "compiler-artifact":
            continue
        target = obj.get("target") or {}
        profile = obj.get("profile") or {}
        if not profile.get("test") and "bin" in (target.get("kind") or []) and target.get("name") == "rex-wave":
            exe = obj.get("executable") or ""
if not exe:
    sys.exit(1)
print(exe)
PY
)" || {
		printf 'error: failed to locate built rex-wave artifact from cargo JSON output\n' >&2
		exit 1
	}
fi

if [[ ! -f "$SRC_BIN" ]]; then
	printf 'error: binary not found at %s\n' "$SRC_BIN" >&2
	exit 1
fi

DEST_DIR="$(dirname "$DEST")"
mkdir -p "$DEST_DIR"
TMP_DIR="$(mktemp -d "$DEST_DIR/.rex-wave-install.XXXXXX")"
TMP_BIN="$TMP_DIR/rex-wave"

cp "$SRC_BIN" "$TMP_BIN"
chmod 0755 "$TMP_BIN"

if [[ "$(uname -s)" == "Darwin" ]]; then
	codesign -s - -f "$TMP_BIN"
fi

mv -f "$TMP_BIN" "$DEST"
printf 'Installed %s -> %s\n' "$SRC_BIN" "$DEST"
