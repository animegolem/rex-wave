---
name: verify-rex-wave
description: Drive rex-wave's two-pane ratatui TUI and Rust daemon inside an isolated background Rex (libghostty) terminal session, exercise user journeys, and capture durable terminal and side-effect evidence. Use whenever verifying UI, TUI, CLI, Kanban, room chat, mail promotion, inbox, OSC 7501, or wave inspector behavior in rex-wave.
---

# Verify rex-wave

Drive `rex-wave` against a disposable `rex-wave daemon` and an isolated background `Rex` (`libghostty`) terminal block. Never connect to `~/.local/state/rex-wave`, never reuse a shared socket, and never steal focus in a connected `Rex.app` window.

Read [`features/README.md`](./features/README.md) before driving a specific surface, then follow the matching feature recipe.

## Launch

1. Build or locate the `rex-wave` binary from Cargo's configured target directory:

	```sh
	TARGET_DIR="${CARGO_TARGET_DIR:-$(cargo metadata --no-deps --format-version 1 | jq -er '.target_directory')}"
	cargo build -p rex-wave --locked
	REX_WAVE_BIN="$TARGET_DIR/debug/rex-wave"
	```

2. Create a per-run scratch directory under `/tmp/rex-verify-$$` with a marked mail sandbox, an isolated `protected-roots` file, and dynamic loopback ports so concurrent runs and the operator's live daemon never collide on `daemon.sock`, `~/.config/rex-wave/protected-roots`, or `127.0.0.1:7781`:

	```sh
	RUN_ID="${REX_VERIFY_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
	SCRATCH_DIR="/tmp/rex-verify-$$"
	EVIDENCE_DIR="/tmp/rex-wave-evidence/$RUN_ID"
	SOCK="$SCRATCH_DIR/daemon.sock"
	SANDBOX="$SCRATCH_DIR/mail-sandbox"
	CONFIG_DIR="$SCRATCH_DIR/config"
	mkdir -p "$SCRATCH_DIR" "$SANDBOX" "$CONFIG_DIR/rex-wave" "$EVIDENCE_DIR/steps"
	printf 'rex-verify-sandbox\n' > "$SANDBOX/.rex-wave-sandbox"
	: > "$CONFIG_DIR/rex-wave/protected-roots"

	BELL_PORT="$(node -e 'const s=require("node:net").createServer();s.listen(0,"127.0.0.1",()=>{console.log(s.address().port);s.close();})')"
	```

3. Start `rex-wave daemon` against `$SOCK`, `$SANDBOX`, `$CONFIG_DIR`, `$BELL_PORT`, and `--bb-url ws://127.0.0.1:9/bridge` (or start the loopback mock BB WebSocket server in `./scripts/verify-tui-rex.sh` when exercising live `AgentInspector` threads). Wait until `[[ -S "$SOCK" ]]` holds before seeding:

	```sh
	XDG_CONFIG_HOME="$CONFIG_DIR" "$REX_WAVE_BIN" --socket "$SOCK" daemon \
		--journal "$SCRATCH_DIR/journal.sqlite3" \
		--mail-sandbox "$SANDBOX" \
		--bb-url "ws://127.0.0.1:9/bridge" \
		--bb-token "rw_bb_verify_$$" \
		--operator owner \
		--wake-bell "127.0.0.1:${BELL_PORT}" \
		>"$SCRATCH_DIR/daemon.stdout.log" 2>"$SCRATCH_DIR/daemon.stderr.log" &
	DAEMON_PID="$!"
	```

4. Seed rooms, waves, Kanban cards, direct messages, and inbox notifications through `"$REX_WAVE_BIN" --socket "$SOCK"` and `rex-wave mcp --stdio` (`run_code`). Confirm readiness by checking `"$REX_WAVE_BIN" --socket "$SOCK" --json inbox` before opening the TUI.

5. Create an unattached background `Rex` session (`rex new` on Rex `0.1.0` creates a server session without selecting it in `Rex.app` and has no `--focus` flag), then launch `rex-wave tui` inside a pane with `--focus=false` and resize the unowned terminal block to `132x38`:

	```sh
	CREATED="$(rex new "rex-verify-$$" --window "harness" --json -- sleep 300)"
	REX_VERIFY_SESSION="$(printf '%s' "$CREATED" | jq -er '.session_id')"

	TUI_RUN="$(rex run -s "$REX_VERIFY_SESSION" --focus=false --keep-open --label "rex-wave-tui" --json -- "$REX_WAVE_BIN" --socket "$SOCK" tui)"
	REX_VERIFY_BLOCK="$(printf '%s' "$TUI_RUN" | jq -er '.block_ids[0]')"
	rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" resize '{"columns":132,"rows":38}'
	```

6. Poll `rex capture -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" --trim --unwrap` until both `Org Tree` and `Inbox` render on screen. Do not drive keys before the initial frame appears.

## Doctor

Run this read-only check before driving and whenever a view looks wedged:

```sh
test -S "$SOCK" &&
kill -0 "$DAEMON_PID" &&
test "$(rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" modes | jq -er '.screen')" = "alternate" &&
"$REX_WAVE_BIN" --socket "$SOCK" --json inbox --limit 5 | jq -e '.reply == "inbox_list"' >/dev/null
```

Require all four conditions:

- `$SOCK` exists as a Unix domain socket.
- `$DAEMON_PID` is alive and owned by this run.
- `rex block call ... modes` reports `"screen": "alternate"`, confirming `rex-wave tui` owns the full-screen terminal buffer rather than a shell prompt or crash trace.
- `"$REX_WAVE_BIN" --socket "$SOCK" --json inbox` returns `"reply": "inbox_list"`.

## Drive

Always pass explicit `-s "$REX_VERIFY_SESSION"` and `-b "$REX_VERIFY_BLOCK"` to every `rex` command. Never omit `-s` or `-b`, never call `rex focus`, and never call `rex do session.select`. If you open any helper split or tab in the verification session, pass `--focus=false`.

Use `rex send-key` for navigation and accelerators, and `rex send` for literal composer text:

- **Sidebar cursor movement:** `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j` (down), `k` (up), `g` (top), `shift+g` (bottom). `rex send-key` treats bare single letters as lowercase, so pass `shift+g` for `G` and `shift+p` for `P`.
- **Sidebar fold toggle & resize:** `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" space` on `Projects`, a project branch, a wave branch, `Channels`, or `Direct Messages` (while `Sidebar` has focus). `shift+tab` cycles global org visibility (`overview` / `contents` / `show all`). `<` and `>` resize the sidebar, and `\` hides or restores it.
- **Activate node or switch pane focus:** `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter` activates the selected sidebar node immediately (bypassing the 100 ms hover debounce). `tab` toggles focus between `Sidebar` and `Stage` (`shift+tab` returns from `Stage` to `Sidebar`).
- **Compose in Room, Kanban, or Agent Inspector:** press `i` while a Room, Kanban board, or Agent stage is loaded, run `rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" 'exact message text'` (or send bracketed paste `$'\e[200~line 1\nline 2\e[201~'` / `shift+enter` / `ctrl+j` for multiline room and steer drafts), and submit with `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter`. Press `esc` to cancel an unfinished draft.
- **Stage & overlay accelerators:** `?` opens the Keymap Help Overlay (generated from `KEYMAP` and showing the active `GraphicsMode`), and `v` cycles `GraphicsMode` (`Hybrid z=-1 + U+10EEEE`, `z=-1 only`, `U+10EEEE only`, `Pure Cells`). In `Room` and `Agent Inspector`, `j`/`k` selects messages or turns and `enter` expands collapsed `WaveConductor` briefs or opens the scrollable full-message/turn Pager (`esc`/`q` closes). In `Inbox`, `r` marks read, `x` dismisses a card, `shift+x` (`X`) dismisses all cards of the selected kind, `a` approves, and `d` denies. In `Kanban`, `m` moves the selected card to the next lane (`[`, `]` switch lanes, `shift+b` opens the active project's board). `shift+p` promotes room messages to a banked canon letter. `q` exits `rex-wave tui` cleanly.

## Evidence

Store every run's proof under `/tmp/rex-wave-evidence/<run-id>/` so evidence survives scratch cleanup:

- **Before and after each user action:** save `rex capture -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" --trim --unwrap` to `steps/<NN>-<slug>.screen.txt`, `rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" program_status` to `steps/<NN>-<slug>.program_status.json`, and `rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" modes` to `steps/<NN>-<slug>.modes.json`.
- **Real user path only:** drive `rex-wave tui` keystrokes and public `rex-wave` CLI or MCP `run_code` commands over `daemon.sock`. Do not mutate `journal.sqlite3` directly with `sqlite3`.
- **Second-view side-effect checks:** pair every TUI mutation with a read-only check outside the TUI. Verify Kanban edits with `"$REX_WAVE_BIN" --socket "$SOCK" --json task list --board <board>`, room posts with `"$REX_WAVE_BIN" --socket "$SOCK" --json call read ...`, canon letter promotions by inspecting `$SANDBOX/to-<seat>/*.md` (copying the banked file into `$EVIDENCE_DIR/`), and interaction approvals in the mock BB bridge ledger.
- **Boundary mocks only:** mock only the external BB WebSocket endpoint (`--bb-url`) when live BB provider threads are out of scope. Run the real `rex-wave daemon`, SQLite WAL `Journal`, `Herald`, `WaveConductor`, `MailBridge`, UDS `RemoteClient`, and `Rex` `libghostty` terminal emulator.

## Cleanup

Register an idempotent `EXIT INT TERM` trap before starting background processes. Kill only the exact session ID and PIDs created by the run:

```sh
cleanup() {
	[[ -n "${REX_VERIFY_SESSION:-}" ]] && rex kill "$REX_VERIFY_SESSION" >/dev/null 2>&1 || true
	if [[ -n "${DAEMON_PID:-}" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
		kill "$DAEMON_PID" 2>/dev/null || true
		wait "$DAEMON_PID" 2>/dev/null || true
	fi
	if [[ -n "${MOCK_BB_PID:-}" ]] && kill -0 "$MOCK_BB_PID" 2>/dev/null; then
		kill "$MOCK_BB_PID" 2>/dev/null || true
		wait "$MOCK_BB_PID" 2>/dev/null || true
	fi
	rm -rf "/tmp/rex-verify-$$"
}
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM
```

Never run `pkill rex-wave` or `killall rex`. After cleanup completes, confirm `/tmp/rex-verify-$$` is gone, confirm `rex ls --json` no longer lists `$REX_VERIFY_SESSION`, and confirm `/tmp/rex-wave-evidence/$RUN_ID/` still holds non-empty proof files.

## Helpers

- **Full live Rex CUA verification suite:**

	```sh
	./scripts/verify-tui-rex.sh
	```

	Boots an isolated loopback mock BB bridge and `rex-wave daemon` in `/tmp/rex-verify-$$`, seeds 2 projects, 2 waves, 3 live/blocked workers, 4 Kanban lanes, multi-line room messages, DMs, and 58+ ranked inbox notifications, drives all 5 mapped feature journeys inside a background `Rex` (`libghostty`) session, tears down all runtime processes, and verifies proof survival in `/tmp/rex-wave-evidence/<run-id>/summary.json`. Override `REX_VERIFY_RUN_ID`, `REX_WAVE_EVIDENCE_DIR`, or `REX_WAVE_BIN` when needed.

- **Fast deterministic commit and CI gate (no Rex server required):**

	```sh
	./scripts/check.sh
	```

	Runs `cargo check` (`-D warnings`), `cargo clippy`, `cargo test --workspace --locked`, `rex-wave tui --self-test`, `rex-wave schema --bb` drift check, and `bb-plugin` TypeScript `tsc --noEmit` plus `node --test protocol.test.ts`.

Use `/maintain-verification-skill` to update [`features/`](./features/README.md) whenever keybindings, stage layouts, or CLI verbs change.
