# rex-wave

`rex-wave` runs `#channels`, direct messages, multi-provider waves, an Emacs Org-compatible Kanban board, and ranked inbox notifications over a single Rust daemon. The two-pane TUI, CLI verbs, MCP `2025-11-25` server, and BB plugin bridge all call the same `rex_wave_api::Client` contract.

For the crate dependency graph, actor ownership rules, and protocol invariants, read [the architecture document](./ARCHITECTURE.md).

## Build and verify offline

Build the binary and run the full workspace test suite offline:

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

2. Run `rex-wave daemon` against that sandbox and your BB plugin bridge token:

```sh
REX_WAVE_BB_TOKEN="$BB_PLUGIN_TOKEN" rex-wave daemon \
	--mail-sandbox /tmp/rex-mail-sandbox \
	--bb-url ws://127.0.0.1:38886/api/v1/plugins/rex-wave/http/bridge \
	--wake-bell 127.0.0.1:7781
```

By default, the daemon listens on `$XDG_STATE_HOME/rex-wave/daemon.sock`, stores its SQLite WAL journal in `journal.sqlite3` beside that socket, and binds `wake-bell` HTTP `/sink` on `127.0.0.1:7781`. Pass `--socket <path>` to any `rex-wave` command to target a custom Unix socket.

## Open the two-pane TUI

In a terminal inside Rex, start the client:

```sh
rex-wave tui
```

The left pane shows `Inbox`, `Kanban`, `Projects`, `Channels`, and `Direct Messages` as a foldable tree. Selecting or hovering a node updates the right-hand stage and cancels any stale stage watch via `FocusGeneration`.

## Connect a seat over MCP

1. Mint a `0600` capability token for a seat. The running daemon reloads `tokens.json` on session open without a restart:

```sh
REX_WAVE_TOKEN="$(rex-wave token --seat sol)"
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
	--worker builder=codex/gpt-6.1-sol \
	--worker verifier=claude-code/claude-opus-5-5
```

`WaveConductor` opens `#proj_123/wave-auth-v2`, creates the `project:proj_123` Kanban board, posts initial `@<role>` briefs, and spawns hidden worker threads. Coordinate with the wave from the CLI:

```sh
rex-wave post '#proj_123/wave-auth-v2' '@verifier please check card #1'
rex-wave task add 'Verify token replay rejection' --board project:proj_123 --lane todo --assign @auth-v2/verifier
rex-wave wave status auth-v2
rex-wave promote '#proj_123/wave-auth-v2' 1..4 --to owner --subject 'auth-v2 decision' --topic auth-v2
rex-wave wave finish auth-v2 --summary 'Merged and verified'
```

## Run waves on another agent host

`Waves` and `WaveConductor` in `crates/rex-wave-waves` never import BB types or WebSocket framing. They depend only on the four-method `AgentHost` port (`spawn`, `tell`, `resolve`, `retire`) and the read-side `AgentFeed` trait (`page`, `live`).

- **BB (`crates/rex-wave-bb` and `bb-plugin/`).** `BbHost` and `BbBridge` implement `AgentHost` and `AgentFeed` over the plugin WebSocket, enforcing `threads.stop` settle polling before `threads.archive`.
- **T3 Code or another IDE host.** Implement `AgentHost` and `AgentFeed` in a sibling adapter crate that maps `SpawnRequest`, `SteerDeliveryMode`, and `InteractionResolution` to that host's session RPCs, then pass that host to `WaveConductor::new` in `crates/rex-wave/src/daemon.rs`.
- **Direct CLI runners (`claude`, `codex`, or `pi` subprocesses).** Implement `AgentHost` over child process groups or tmux panes where `spawn` starts the CLI in a git worktree with `rex-wave mcp --stdio` configured, `tell` writes to stdin or a session socket, and `retire` terminates the process group cleanly.
