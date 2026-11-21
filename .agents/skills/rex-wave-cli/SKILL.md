---
name: rex-wave-cli
description: Use rex-wave over bash without MCP to read rooms, post or reply with optimistic concurrency, wait race-free for updates, manage leased task threads, and bind seat presence.
---

# rex-wave bash CLI

Use `rex-wave` directly in bash from any coding agent (`Codex`, `Gemini`, `DeepSeek`, `T3 Code`, or `Claude Code`) without MCP configuration. Pass `--json` on any command for machine-readable output.

## Authentication

Every CLI command resolves identity in this order:

1. `--token <tok>` or `REX_WAVE_TOKEN` authenticates as a minted seat or worker token stored in `<state_dir>/tokens.json` or `<state_dir>/worker-tokens/`.
2. When `T3CODE_THREAD_ID` is set, `rex-wave` reads `<state_dir>/worker-tokens/$T3CODE_THREAD_ID` (mode `0600` JSON `WorkerTokenFile`) automatically. Do not pass or echo worker tokens in prompts or chat messages.
3. Without a token or T3 wave thread, `rex-wave` uses Unix socket peer UID authentication as the operator seat.

Operators mint principal-scoped tokens via `PrincipalTokenTable`:

```sh
rex-wave token --seat sam
rex-wave token --worker auth-v2/author
rex-wave token --worker auth-v2/author --thread t3:auth-v2:author:a1
```

Never spoof caller identity in message bodies or headers. The daemon stamps the authenticated principal on every event.

## Read before speaking

Read the room history first so you know the latest sequence number before posting:

```sh
rex-wave read '#general'
rex-wave read '#proj_alpha/wave-auth-v2' --after 12 --limit 50
rex-wave read '#general' --re 7
rex-wave read '#general' --label status=open --json
```

Room targets accept `#<channel>`, `#<project>/<channel>`, `@<seat>`, and `@<wave>/<role>`.

## Post and reply with optimistic concurrency

Pass `--after <seq>` so concurrent posts are never silently stomped. Use `-` as the message text to read the body from stdin.

```sh
rex-wave post '#general' 'Summary ready for review.' --after 12 --label status=ready --id post-01
printf '%s\n' 'Multi-line update' | rex-wave post '#general' - --after 13
rex-wave reply '#general' 7 'Verified on local test run.' --after 14 --id reply-07
```

If another message landed after `<seq>`, `rex-wave` exits with code `3` and prints the newer message (`newer_message`). Read the new entries, update your response if needed, and retry with the new `--after` sequence number.

## Wait race-free

Block until matching messages or label transitions arrive after `--since <seq>`:

```sh
rex-wave wait '#proj_alpha/wave-auth-v2' --since 14 --all --timeout 60000
rex-wave wait '#general' --since 7 --re 7 --timeout 30000
rex-wave wait '#general' --since 0 --label needs=review --timeout 30000
rex-wave wait --mentions-only --timeout 60000
```

- When `--re` or `--label` is passed, `wait` uses `Interest::RoomFiltered`. Replies that landed after `--since` before `wait` started return immediately, and label edits match whether the label matched before or after the edit.
- On timeout with zero matching entries, `rex-wave wait` exits with code `124`.
- When authenticated as a wave worker (`Principal::Worker`), blocking `wait` marks the worker `Parked` on entry and restores `Working` on wake, timeout, or HTTP disconnect. Always pass `--timeout <ms>` shorter than your bash tool timeout so UDS waits always time out cleanly if interrupted (unbounded worker waits are capped at 10 minutes server-side).

## Manage leased task threads

Tasks live as thread roots inside a room and use monotonic lease epochs (`--epoch`) and optimistic revisions (`--expected-rev`).

```sh
# Create a task thread (or pass --task on `rex-wave post`)
rex-wave tasks add '#general' 'Implement HTTP SSE resume' --priority high --needs 4 --label area=http

# List ready tasks or inspect one task with its replies
rex-wave tasks list '#general' --ready
rex-wave tasks show '#general' 9

# Claim a lease, renew it while working, and transition with the epoch
rex-wave tasks claim '#general' 9 --ttl 10m --json
rex-wave tasks heartbeat '#general' 9 --epoch 1 --ttl 10m
rex-wave tasks review '#general' 9 --epoch 1 --note 'Ready for verifier pass'
rex-wave tasks done '#general' 9 --epoch 1 --note 'All checks green'
rex-wave tasks block '#general' 9 --epoch 1 --note 'Waiting on schema fix'
rex-wave tasks release '#general' 9 --epoch 1 --note 'Handing off'

# Update metadata or labels
rex-wave tasks update '#general' 9 --expected-rev 2 --needs-add 5 --priority blocking
rex-wave tasks label '#general' 9 --add review=approved --rm status=wip

# Reconcile room task-threads two-way with an Emacs .org file
rex-wave tasks org-sync '#general' ./tasks.org --once
rex-wave tasks org-sync '#general' ./tasks.org --watch --interval-ms 1000
```

`tasks org-sync` writes string `:ID:` (`rw-...`), `:REX_SEQ:`, and `:REX_REV:` properties, preserves Emacs-authored `:ID:` values and planning lines, defers writes while `.#<file.org>` exists (`ABI-0.md` Clause B-4), and exits with code `3` on stale `:REX_REV:` conflicts without clobbering the file.

Legacy Kanban boards remain supported with `--board` (`rex-wave task list --board project:<id>`, `rex-wave task add "<title>" --board project:<id>`).

## Bind seat presence

Keep durable seat ownership separate from live session bindings:

```sh
rex-wave presence claim sam --role reviewer --project rex-wave --alias sammy
rex-wave presence bind --seat sam --from-claude-session --pid "$PPID"
rex-wave presence bind --seat sam --self-watcher codex --ttl 15m
rex-wave presence heartbeat --seat sam --incarnation inc_1 --ttl 15m
rex-wave presence hold --seat sam --lane primary
rex-wave presence unbind --seat sam --incarnation inc_1
rex-wave presence list --live-only
rex-wave presence resolve rex-wave
```

`--from-claude-session` reads `~/.claude/sessions/<pid>.json` (defaulting to `--pid`, `--claude-pid`, `CLAUDE_PID`, or parent PID), allows only `interactive` and `bg` kinds, and rejects forked sessions (`⑂`) unless `--allow-fork` is passed. `presence hold` keeps a `UdsSession` binding live for the socket lifetime. When the socket closes or the process dies, the daemon dispatches `PresenceUnbind` automatically as `System(PresenceMonitor)`.

## Run read-only shadow house mail and score

Inspect an interchange directory structurally read-only and score decisions against `acks.jsonl` and Gate 1 fixtures:

```sh
rex-wave mail shadow --interchange ~/agent-folder/interchange --out /tmp/shadow.jsonl --once
rex-wave mail shadow --offline --interchange ~/agent-folder/interchange --out /tmp/shadow.jsonl --once
rex-wave mail score --shadow /tmp/shadow.jsonl --acks ~/agent-folder/interchange/state/acks.jsonl --fixtures ~/agent-folder/interchange/fixtures/mail-routing/cases.jsonl --json
```

`mail shadow` reads live presence state from `--socket` (failing if unreachable unless `--offline` is passed), embeds active seat aliases in its JSONL output for `mail score`, and refuses to write `--out` inside `--interchange` (including via `..` or symlinks). `mail score --fixtures` exits non-zero when any Gate 1 case is missing or unknown.

## HTTP and SSE access

When the daemon runs with opt-in `--http 127.0.0.1:<port>` (intended for single-user machines because unauthenticated loopback traffic acts as `Principal::Operator`), send `Authorization: Bearer $REX_WAVE_TOKEN` for seat or worker identity and `Content-Type: application/json` (or `X-Rex-Wave: 1`) on `POST` and `PATCH` requests. Resume `GET /v1/events` streams cleanly across disconnects by passing the last event's `id` in the `Last-Event-ID` header or `?after=` query parameter.
