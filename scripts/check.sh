#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TEMP_NODE_MODULES=0
cleanup() {
	if [[ "$TEMP_NODE_MODULES" -eq 1 && -L "$ROOT/bb-plugin/node_modules" ]]; then
		rm -f "$ROOT/bb-plugin/node_modules"
	fi
}
trap cleanup EXIT

step() {
	printf '\n==> %s\n' "$1"
}

resolve_target_dir() {
	if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
		printf '%s\n' "$CARGO_TARGET_DIR"
	elif command -v jq >/dev/null 2>&1; then
		cargo metadata --no-deps --format-version 1 | jq -er '.target_directory'
	else
		printf '%s/target\n' "$ROOT"
	fi
}

ensure_bb_plugin_deps() {
	if [[ -f "$ROOT/bb-plugin/node_modules/typescript/bin/tsc" ]]; then
		return 0
	fi
	if [[ ! -e "$ROOT/bb-plugin/node_modules" ]]; then
		local common_dir common_root
		if common_dir="$(git rev-parse --git-common-dir 2>/dev/null)"; then
			common_root="$(cd "$common_dir/.." && pwd)"
			if [[ -f "$common_root/bb-plugin/node_modules/typescript/bin/tsc" ]]; then
				ln -s "$common_root/bb-plugin/node_modules" "$ROOT/bb-plugin/node_modules"
				TEMP_NODE_MODULES=1
				return 0
			fi
		fi
	fi
	printf 'error: missing bb-plugin/node_modules; run npm install in bb-plugin first\n' >&2
	return 1
}

run_node() {
	if command -v node >/dev/null 2>&1; then
		node "$@"
	elif [[ -x "/Applications/bb.app/Contents/MacOS/bb" ]]; then
		ELECTRON_RUN_AS_NODE=1 /Applications/bb.app/Contents/MacOS/bb "$@"
	else
		printf 'error: node (>=22.18) or /Applications/bb.app is required for bb-plugin checks\n' >&2
		return 1
	fi
}

step "cargo fmt --all --check"
cargo fmt --all --check

step "cargo check --workspace --all-targets --locked"
cargo check --workspace --all-targets --locked

step "cargo clippy --workspace --all-targets --locked -- -D warnings"
cargo clippy --workspace --all-targets --locked -- -D warnings

step "cargo build -p rex-wave --locked"
cargo build -p rex-wave --locked
TARGET_DIR="$(resolve_target_dir)"
REX_WAVE_BIN="$TARGET_DIR/debug/rex-wave"

step "cargo test --workspace --locked"
cargo test --workspace --locked

step "rex-wave tui --self-test"
"$REX_WAVE_BIN" tui --self-test

step "rex-wave schema --bb diff against bb-plugin/tools.generated.json"
"$REX_WAVE_BIN" schema --bb | diff -u "$ROOT/bb-plugin/tools.generated.json" -

step "bb-plugin tsc --noEmit and protocol.test.ts"
ensure_bb_plugin_deps
run_node "$ROOT/bb-plugin/node_modules/typescript/bin/tsc" -p "$ROOT/bb-plugin/tsconfig.json" --noEmit
(
	cd "$ROOT/bb-plugin"
	run_node --test protocol.test.ts
)

printf '\ncheck.sh: all gates passed.\n'
