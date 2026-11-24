# rex-wave

`rex-wave` runs `#channels`, direct messages, flat threads with optimistic concurrency, leased task-threads with two-way Emacs `.org` sync, session-bound seat presence, multi-host waves (`BB` and `T3 Code`), an Emacs Org-compatible Kanban board, and ranked inbox notifications over a single Rust daemon. The two-pane TUI, zero-MCP bash CLI, loopback HTTP/SSE server, MCP `2025-11-25` server, `T3Bridge`, and `BbBridge` all call the same `rex_wave_api::Client` contract.

For the crate dependency graph, actor ownership rules, and protocol invariants, read [the architecture document](./ARCHITECTURE.md). For agent bash usage, read [the CLI skill](./.agents/skills/rex-wave-cli/SKILL.md).

## Build and verify

Run the hermetic verification gate (`cargo fmt`, repo hygiene, `bb-plugin` typecheck and tests, `cargo clippy -D warnings`, parallel workspace tests, `tui --self-test`, and `schema --bb` diff), enable the `pre-push` hook once per clone, and install a release binary atomically:

```sh
git config core.hooksPath .githooks
./check
./scripts/install-local.sh
```

To build and test completely offline once dependencies are cached:

```sh
cargo build --release --offline
cargo test --workspace --offline
./target/release/rex-wave tui --self-test
```

`rex-wave tui --self-test` renders the two-pane foldable org-tree, Kitty `z=-1` RGBA cards, `U+10EEEE` context bars, and `OSC 7501` status transitions in memory without a daemon.

## Start the daemon

1. Create a non-production mail sandbox directory with its `.rex-wave-sandbox` marker file:

```sh
mkdir -p /tmp/rex-mail-sandbox
touch /tmp/rex-mail-sandbox/.rex-wave-sandbox
```

2. Run `rex-wave daemon` against that sandbox, your BB bridge token, optional T3 Code runner flags (`--runner bb|t3|both`), and optional loopback HTTP/SSE listener (`--http 127.0.0.1:<port>`):

```sh
REX_WAVE_BB_TOKEN="$BB_PLUGIN_TOKEN" rex-wave daemon \
	--mail-sandbox /tmp/rex-mail-sandbox \
	--runner both \
	--bb-url ws://127.0.0.1:38886/api/v1/plugins/rex-wave/http/bridge \
	--wake-bell 127.0.0.1:7781 \
	--http 127.0.0.1:7783
```

The daemon refuses a sandbox that is the filesystem root, your home folder, or anything you have protected. Place `--mail-sandbox` outside your house folder (for example `~/mail-house`) and every other path listed in `protected-roots`—never inside a seat's own room under the house folder. It reads what to protect from `$XDG_CONFIG_HOME/rex-wave/protected-roots` (`~/.config/rex-wave/protected-roots` by default) and will not start without that file. If you have nothing to protect, create it empty:

```sh
mkdir -p ~/.config/rex-wave
touch ~/.config/rex-wave/protected-roots
```

To protect real mail folders, list them one per line:

```text
# folders the mail sandbox must never be, contain, or sit inside
~/mail/live
/srv/letters
# a folder name the sandbox path must never include, wherever it is
name: archive
```

A folder line is an absolute path or starts with `~/`, and cannot contain `..`. The file holds at most 64 entries. The daemon exits on a file that is missing, unreadable or malformed, so a mistake never leaves a folder unprotected.

Facts about your house live in an optional `$XDG_CONFIG_HOME/rex-wave/house.json` (`~/.config/rex-wave/house.json` by default). Without it, the operator surface seat is `operator` and seeded claims that no machine record confirms are witnessed by `postmaster`. Every field is optional:

```json
{
  "operator_seat": "operator",
  "witness_seat": "postmaster",
  "excluded_herdr_targets": ["w9:p1S"],
  "aliases": { "alice": ["alice_old", "alice-desk"] }
}
```

The operator surface seat can never be bound or claimed as an active seat, and letters to it never ring or raise `MailUnrung`. `aliases` folds historical names into a canonical seat when `presence seed-interchange` runs. `excluded_herdr_targets` lists multiplexer panes that never name a seat. The daemon and CLI refuse a `house.json` that is a symlink, malformed, larger than 64 KiB, or names an invalid seat.

By default, the daemon listens on `$XDG_STATE_HOME/rex-wave/daemon.sock`, stores its SQLite WAL journal in `journal.sqlite3` beside that socket, and binds `wake-bell` HTTP `/sink` on `127.0.0.1:7781`. Pass `--socket <path>` to any `rex-wave` command to target a custom Unix socket (on macOS and targets without `/proc/self/fd`, the socket parent directory must reside on the same filesystem as `/tmp` so staging stays descriptor-anchored).

## Open the two-pane TUI

In a terminal inside Rex, start the client:

```sh
rex-wave tui
```

The left pane shows `Inbox`, `Kanban`, `Projects`, `Channels`, and `Direct Messages` as a foldable tree. Selecting or hovering a node updates the right-hand stage and cancels any stale stage watch via `FocusGeneration`.

## Connect a seat over MCP

1. Mint a `0600` capability token for a seat. The running daemon reloads `tokens.json` on session open without a restart:

```sh
REX_WAVE_TOKEN="$(rex-wave token --seat sam)"
```

2. Start the MCP `2025-11-25` server over `stdio` for a local host, or over loopback HTTP with bearer authentication:

```sh
REX_WAVE_TOKEN="$REX_WAVE_TOKEN" rex-wave mcp --stdio
REX_WAVE_TOKEN="$REX_WAVE_TOKEN" rex-wave mcp --http 127.0.0.1:7782
```

HTTP clients must send `Authorization: Bearer <token>` with that same seat token alongside a loopback `Host` header. Each MCP session exposes audience-filtered atomic tools alongside `run_code`. Every `run_code` call allocates a fresh QuickJS runtime with an 8 MiB heap cap, a 512 KiB stack cap, and a frozen `globalThis.rex` binding.

## Install `bb-plugin/`

`bb-plugin/` bridges hidden BB worker threads to the `rex-wave` daemon over `/api/v1/plugins/rex-wave/http/bridge` using `@get-bb/plugin-sdk` `0.6.132`.

1. Register or link `./bb-plugin` in your local BB plugins directory so BB loads `bb-plugin/package.json` and `bb-plugin/server.ts`.
2. Verify TypeScript types and the bridge unit suite against the local BB runtime:

```sh
ELECTRON_RUN_AS_NODE=1 /Applications/bb.app/Contents/MacOS/bb \
	./bb-plugin/node_modules/typescript/bin/tsc -p ./bb-plugin/tsconfig.json --noEmit
ELECTRON_RUN_AS_NODE=1 /Applications/bb.app/Contents/MacOS/bb --test ./bb-plugin/protocol.test.ts
```

3. If you change `TOOLS` in `crates/rex-wave-api`, regenerate the checked-in tool catalog:

```sh
rex-wave schema --bb > bb-plugin/tools.generated.json
```

When BB spawns a thread carrying `pluginMetadata.rexWave`, `bb-plugin/server.ts` injects the `rex_*` atomic tools and `rex_run_code`. Non-wave BB threads receive zero `rex_*` tools.

## Start and steer a wave

Launch a multi-provider wave under a BB project:

```sh
rex-wave wave start auth-v2 \
	--project proj_123 \
	--goal "Rotate OAuth refresh tokens" \
	--account work \
	--output-dir runs/auth-v2 \
	--worker builder=codex/gpt-6.1-sol \
	--worker verifier=claude-code/claude-opus-5-5 \
	--worker-writes builder=src/auth.rs \
	--worker-writes verifier=tests/auth_test.rs
```

`WaveConductor` opens `#proj_123/wave-auth-v2`, creates the `project:proj_123` Kanban board, stamps each worker attempt with a deterministic `run_stamp`, enforces disjoint per-worker `--output-dir` / `--worker-output` / `--worker-writes` paths (structural one-writer), posts initial `@<role>` briefs, and spawns hidden worker threads. Coordinate with the wave from the CLI:

```sh
rex-wave post '#proj_123/wave-auth-v2' '@verifier please check card #1'
rex-wave task add 'Verify token replay rejection' --board project:proj_123 --lane todo --assign @auth-v2/verifier
rex-wave wave status auth-v2
rex-wave promote '#proj_123/wave-auth-v2' 1..4 --to owner --subject 'auth-v2 decision' --topic auth-v2
rex-wave wave finish auth-v2 --summary 'Merged and verified'
# If any worker refused (⊘) or errored (✕), completion requires explicit --allow-incomplete:
rex-wave wave finish auth-v2 --summary 'Partial result' --allow-incomplete
```

## Use the bash CLI and HTTP/SSE API

Any coding agent can drive `rex-wave` directly over bash without MCP using `--token <tok>`, `REX_WAVE_TOKEN`, or automatic `<state_dir>/worker-tokens/$T3CODE_THREAD_ID` lookup:

```sh
# Threads, optimistic concurrency, and race-free wait
rex-wave post '#general' 'Slice ready' --task 'Ship Phase 3' --priority high
rex-wave reply '#general' 1 'Checking tests' --after 1
rex-wave read '#general' --re 1
rex-wave wait '#general' --since 1 --re 1 --timeout 30000

# Leased tasks-as-threads and two-way Emacs .org sync
rex-wave tasks claim '#general' 1 --ttl 10m
rex-wave tasks transition '#general' 1 done --epoch 1 --note 'All checks green'
rex-wave tasks org-sync '#general' ./tasks.org --once

# Session-bound seat presence and read-only shadow house-mail scoring
rex-wave presence claim sam --role reviewer --project rex-wave
rex-wave presence bind --seat sam --from-claude-session --pid "$PPID"
rex-wave presence hold --seat sam
rex-wave mail shadow --offline --interchange /path/to/interchange --out /tmp/shadow.jsonl --once
rex-wave mail score --shadow /tmp/shadow.jsonl --acks /path/to/interchange/state/acks.jsonl --json
```

When `--http 127.0.0.1:<port>` is enabled, the daemon serves `/v1/health`, `/v1/sidebar`, `/v1/inbox`, `/v1/presence`, `/v1/rooms/<room>/messages`, `/v1/rooms/<room>/tasks`, `/v1/call/<tool>`, and `/v1/events` (SSE with `id: <cursor>` and `Last-Event-ID` resume). Loopback HTTP enforces loopback `Host`, matching `Origin`, `Sec-Fetch-Site`, CSRF headers on mutations, `StateDirAuthenticator` bearer auth, and `404 Not Found` masking on unauthorized DMs and inboxes.

## Run waves on multiple agent hosts

`Waves` and `WaveConductor` in `crates/rex-wave-waves` never import host wire types. They depend only on the five-method `AgentHost` port (`spawn`, `tell`, `resolve`, `retire`, `check_live`) and the read-side `AgentFeed` trait (`page`, `live`).

- **BB (`crates/rex-wave-bb` and `bb-plugin/`).** `BbHost` and `BbBridge` implement `AgentHost` and `AgentFeed` over the plugin WebSocket and enforce `threads.stop` settle polling before `threads.archive`.
- **T3 Code (`crates/rex-wave-t3`).** `T3Host`, `T3Feed`, and `T3Bridge` connect directly over T3 Code's Effect RPC WebSocket and REST API, auto-discover `<t3-home>/userdata/server-runtime.json`, and provision mode-`0600` worker token files under `<state_dir>/worker-tokens/<thread_id>`.
- **Multi-host composition (`CompositeHost` and `CompositeFeed`).** In `rex-wave daemon --runner both`, `t3:<provider>` and bare `antigravity` workers route to `T3Host` while `bb:<provider>` workers route to `BbHost`.
- **Direct CLI runners.** Implement `AgentHost` over child process groups or terminal sessions where `spawn` starts the CLI in a git worktree, `tell` delivers prompts, and `retire` stops the process group.

## License

Licensed under either of [Apache License, Version 2.0](./LICENSE-APACHE) or [MIT license](./LICENSE-MIT), at your option.
