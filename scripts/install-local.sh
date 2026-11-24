#!/usr/bin/env bash
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: ./scripts/install-local.sh [--from <built-bin>] [--dest <dest-bin>] [-h|--help]

Builds rex-wave in release mode (unless --from is given), ad-hoc codesigns on
macOS, and atomically installs via temp-copy + rename so running rex-wave
processes are never overwritten in place.
EOF
}

SRC_BIN=""
DEST="${HOME}/.cargo/bin/rex-wave"

while [[ $# -gt 0 ]]; do
	case "$1" in
	--from)
		if [[ $# -lt 2 ]]; then
			printf 'error: --from requires a path argument\n' >&2
			exit 2
		fi
		SRC_BIN="$2"
		shift 2
		;;
	--dest)
		if [[ $# -lt 2 ]]; then
			printf 'error: --dest requires a path argument\n' >&2
			exit 2
		fi
		DEST="$2"
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

resolve_target_dir() {
	if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
		printf '%s\n' "$CARGO_TARGET_DIR"
		return 0
	fi
	if command -v python3 >/dev/null 2>&1; then
		cargo metadata --no-deps --format-version 1 \
			| python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])'
		return 0
	fi
	printf '%s/target\n' "$ROOT"
}

if [[ -z "$SRC_BIN" ]]; then
	(
		cd "$ROOT"
		cargo build --release -p rex-wave --locked
	)
	TARGET_DIR="$(resolve_target_dir)"
	SRC_BIN="$TARGET_DIR/release/rex-wave"
fi

if [[ ! -f "$SRC_BIN" ]]; then
	printf 'error: binary not found at %s\n' "$SRC_BIN" >&2
	exit 1
fi

mkdir -p "$(dirname "$DEST")"

TMP_BIN="${DEST}.new.$$"
cleanup() {
	rm -f "$TMP_BIN"
}
trap cleanup EXIT

cp "$SRC_BIN" "$TMP_BIN"
chmod +x "$TMP_BIN"

if [[ "$(uname -s)" == "Darwin" ]]; then
	codesign -s - -f "$TMP_BIN"
fi

mv -f "$TMP_BIN" "$DEST"
printf 'Installed %s -> %s\n' "$SRC_BIN" "$DEST"
