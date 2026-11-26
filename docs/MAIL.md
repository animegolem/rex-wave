# `rex-wave mail` — Dissolved Content-Addressed Mail & Git Mirror

`rex-wave mail` replaces raw file-drop mailboxes with immutable, content-addressed (`sha256`) messages committed atomically to `journal.sqlite3`, projected directly into `Direct Messages` (1:1) and `#mail-<slug>` channels (multi-seat), indexed with SQLite FTS5, and mirrored atomically to human-readable Markdown + `state/acks.jsonl`.

## Guarantees (`specs/mail/MailStore.tla`)

- **Immutable `sha256` identity:** Existing `.md` files ingested from disk, drop folders, or historical interchanges keep their exact raw-byte `sha256`. Newly synthesized letters render deterministic YAML frontmatter + Markdown and are keyed by their exact canonical `sha256`.
- **Single commit point = SQLite transaction:** `mail send` and `mail ack` commit durably to `journal.sqlite3` (`StreamId::Mail`) **first**, then export atomically (`temp + fsync + rename`) to `<mirror>/objects/<sha256>.md`, `<mirror>/mail/<slug>.md`, and `<mirror>/state/acks.jsonl`.
- **Startup & periodic `reconcile`:** On daemon start and on a background poll interval, `rex-wave` exports any committed SQLite messages or acks missing on disk, and ingests any unindexed files from `<mirror>` or `<drop_dir>` (ignoring dotfiles, `.#*` Emacs lockfiles, files locked by `.#<name>`, `#*#` autosaves, and symlinks).
- **Post-ack edit rule (`EditNeverRingable`):** Editing a working-tree letter at a path that already has an acknowledged `sha256` produces a new hash recorded as `mutated_after_ack` (`flagged`). Flagged hashes are **never** ringable and cannot be acknowledged as fresh unread mail.
- **Lossless offline `rebuild` (`RebuildParity`):** Deleting `journal.sqlite3` and running `rex-wave mail rebuild --from <mirror> --journal <db>` reproduces 100% of messages, `sha256`s, ACKs, `mutated_after_ack` flags, FTS5 search index, and DM/channel projections.
- **`protected-roots` enforcement:** Mirror, drop, and rebuild journal targets are checked against `$XDG_CONFIG_HOME/rex-wave/protected-roots` (or `~/.config/rex-wave/protected-roots`) before any write.

## Four-Verb CLI

Global flags: `--socket <path>`, `--token <tok>` (`REX_WAVE_TOKEN`), `--json`.

### 1. `rex-wave mail send`

```sh
rex-wave mail send \
  --to bob [--to sam ...] [--cc postmaster ...] \
  --subject "Design sign-off" \
  --body "Approved with two notes." \
  [--body-file letter.md] \
  [--from alice] \
  [--priority low|normal|high|blocking] \
  [--in-reply-to <sha256-or-id>] \
  [--topic design-ruling] \
  [--ref <ref> ...] \
  [--wait-export] \
  [--id <idem-key>]
```

- 1:1 mail (`--to <one-seat>`, no `--cc`) dissolves into `RoomId::Direct` under `Direct Messages`.
- Multi-seat mail dissolves into a participant-scoped private channel `#mail-<topic-or-subject>-<hash16>` readable only by its participants and the Operator. Global channel names prefixed with `mail-` are reserved for dissolved multi-seat mail and cannot be opened manually by non-Operator seats.
- Pass `--body -` to read the body from stdin.
- Retries are idempotent when `--id <idem-key>` is supplied (`replayed: true`), scoped per sender principal; reusing `--id` with a different mail payload is refused (`exit 2` / `HTTP 409`). Without `--id`, each call renders a new `created_at` timestamp and distinct `sha256`.
- Returns `delivery_notices` immediately when a recipient seat is unregistered, dormant, retired, or offline.

### 2. `rex-wave mail check`

```sh
rex-wave mail check [--seat <seat>] [--all] [--limit <n>]
```

- Fast session-start hook returning unread/unacked letters for `<seat>` plus sender `delivery_notices` for previously sent letters whose recipients are unregistered, dormant, retired, or offline.
- Non-Operator seats may only check their own seat.

### 3. `rex-wave mail ack`

```sh
rex-wave mail ack <sha256-or-path> [--seat <seat>] [--sha256 <hex>] [--note <note>]
```

- Idempotent per `(seat, sha256)`.
- Commits the immutable receipt to SQLite first, then appends idempotently to `<mirror>/state/acks.jsonl`.
- Refuses (`exit 2`) to acknowledge `mutated_after_ack` messages, `sha256` mismatches, or mail not addressed to the caller's seat.

### 4. `rex-wave mail search`

```sh
rex-wave mail search <query> [--seat <seat>] [--limit <n>]
```

- BM25-ranked SQLite FTS5 full-text search across message `id`, subject, body, sender, recipients, topic, and path.
- Non-Operator seats are strictly scoped to mail they sent or received.

## Mirror Operations & Verification

### `rex-wave mail reconcile`

```sh
rex-wave mail reconcile [--json]
```

Triggers an immediate Operator reconciliation pass on the running daemon.

### `rex-wave mail rebuild`

```sh
rex-wave mail rebuild --from <mirror-dir> --journal <journal-path> [--json]
```

Offline lossless rebuild from `<mirror-dir>` (`objects/*.md`, working tree, and `state/acks.jsonl`) into `<journal-path>` (parent verified outside `protected-roots`).

### `rex-wave mail migrate-verify`

```sh
rex-wave mail migrate-verify --interchange <ro-dir> [--work-dir <tmp-dir>] [--json]
```

Opens `<ro-dir>` strictly read-only, imports all letters and `state/acks.jsonl` into an isolated temporary SQLite journal + mirror outside `protected-roots` and `<ro-dir>`, verifies 100% `sha256` parity, deletes the journal, runs `rebuild`, and asserts 100% parity across `db`, `acked`, `mutated_after_ack`, and `Direct Messages` rooms.

### Legacy Shadow & Promotion Verbs (Preserved)

- `rex-wave mail shadow --interchange <ro-dir> --out <jsonl> [--once|--watch] [--offline]`
- `rex-wave mail score --shadow <jsonl> --acks <acks.jsonl> [--fixtures <cases.jsonl>]`
- `rex-wave promote <room> <first..last> --to <seat> --subject <subj> --topic <topic>`

## Mirror & Drop-Folder Layout

```text
<mirror>/
  .rex-wave-sandbox          # required marker when used as --mail-sandbox
  objects/<sha256>.md        # immutable content-addressed blob backup
  mail/<slug>.md             # human-readable working-tree letter
  to-<seat>/<slug>.md        # optional legacy per-seat drop directory
  state/acks.jsonl           # append-only receipt ledger (at_ms + note preserved)
  state/mail-index.jsonl     # append-only provenance ledger for lossless rebuild
```

`state/mail-index.jsonl` records local mirror provenance (`to`, `cc`, `mutated_after_ack`, `send_idem`, and `send_by` journal attribution) and is trusted as part of the operator-owned mirror directory; `rex-wave mail rebuild` validates every row and requires matching working-tree or `objects/<sha256>.md` content bytes for every indexed digest.

Configuration keys (in `config.toml` or `rex-wave config set`):

- `mail.export_enabled` (`true` | `false`): toggle mirror export without affecting SQLite commits.
- `mail.require_canary` (`true` | `false`): require canary confirmation for push-courier Presence binds by default.
- `mail.mirror_dir`: optional mirror root override (checked against `protected-roots`).
- `mail.drop_dir`: optional external drop folder scanned for hand-dropped `.md` files and consumed into `<mirror>` + SQLite after durable commit.
- `mail.couriers_dir`: optional directory containing executable output adapters (defaults to `<config_dir>/couriers.d/`).
- `mail.hub_owner_seat`: seat notified loudly on courier refusals, give-ups, and `AdapterDegraded` events (default `postmaster`).
- `mail.defer_backoff_ms`: base retry backoff in milliseconds for `"deferred"` rings (default `30000`, per-step cap `300000`).
- `mail.max_defer_tries`: maximum retry attempts before loud `give_up` escalation (default `8`).

## Executable `couriers.d/` Adapters (`specs/mail/MailMachines.tla`)

`rex-wave daemon` runs the `CourierHub` reactor to push body-blind wake notifications to seats bound on the local machine (`OneMachineRings`).

### Adapter Discovery & Isolation

- Adapters live as regular executable (`0755`) files directly inside `mail.couriers_dir` (or `<config_dir>/couriers.d/`).
- Every adapter is verified via descriptor-anchored `O_DIRECTORY | O_NOFOLLOW` + `protected-roots` traversal and staged from the verified open file descriptor into a private `0700` directory under `/tmp/.rex-exec-*` before spawning (symlinks and directories are rejected). Note that `$0` / `__file__` inside a running adapter points to the staged `/tmp/.rex-exec-*` path rather than `couriers.d/`; the original verified path is passed in `REX_WAVE_COURIER_ORIGIN`.
- Each invocation spawns in its own process group (`process_group(0)`), enforces a router timeout (`10 s` default) covering both process exit and pipe drain, and reaps the entire process group with `SIGTERM` followed by `SIGKILL` on timeout.
- Endpoint mapping:
  - `TransportEndpoint::T3Thread` -> `couriers.d/t3-thread`
  - `TransportEndpoint::ClaudeSession` -> `couriers.d/claude-session`
  - `TransportEndpoint::BbThread` -> `couriers.d/bb-thread`
  - `TransportEndpoint::OperatorToast` -> `couriers.d/macos-toast`
  - `TransportEndpoint::UdsSession { client: "codex-app-server" | "codex" }` -> `couriers.d/codex-app-server`
  - `TransportEndpoint::SelfWatcher` (`own_knocker`) -> none (ages unacked mail without invoking external push couriers)

Executable reference scripts are shipped in `examples/couriers.d/`: `t3-thread`, `claude-session`, `codex-app-server`, `bb-thread`, and `macos-toast`.

### Body-Blind Wire Protocol

`CourierHub` writes one JSON `DeliveryClaim` object to adapter `stdin` (never raw letter bodies):

```json
{
  "schema": 1,
  "seat": "bob",
  "machine_id": "workstation-a",
  "incarnation_id": "inc-bob-1",
  "endpoint": { "transport": "t3_thread", "thread_id": "thr-42" },
  "referents": [
    {
      "sha256": "...",
      "id": "1795261554000-alice-to-bob-a1b2c3d4",
      "from": "alice",
      "subject": "Design sign-off",
      "path": "mail/1795261554000-alice-to-bob-a1b2c3d4.md",
      "priority": "normal",
      "created_at_ms": 1795261554000
    }
  ],
  "canary_token": null
}
```

The adapter writes one JSON outcome object to `stdout`:

```json
{ "outcome": "rung" | "deferred" | "refused" | "unknown", "reason": "optional", "detail": "optional" }
```

- **`rung`:** Marks each `(seat, sha256)` rung in `StreamId::Mail` (`NoDoubleRing`) and resets the adapter's consecutive-unknown counter for that seat to `0`.
- **`unknown` (including non-zero exit, malformed stdout, timeout, or interrupted pre-dispatch claim on daemon restart):** Counts as rung on `(seat, sha256)` so an ambiguous post-spawn delivery is **never** double-rung (pre-spawn setup failures before a child process starts record `unknown` toward adapter health without consuming the letter's push eligibility). After `K = 3` consecutive `unknown` outcomes, the courier is marked `degraded = true` and `AdapterDegraded` is escalated to `mail.hub_owner_seat` (`DegradedLoud`).
- **`deferred`:** Retried with exponential backoff (`mail.defer_backoff_ms`, default `30 s`, capped at `5 m` per step) up to `mail.max_defer_tries` (`8` default, persisted in `StreamId::Mail`) before loud `give_up` escalation.
- **`refused` (first occurrence, e.g. `reason: "address-gone"` or `"name-shared"`):** Immediately expires the dead binding (`BindingStatus::Expired`) with **zero retries** on that incarnation, records `MailEvent::DeliveryFailed`, and escalates loudly to `mail.hub_owner_seat` and the original sender seat (visible in `rex-wave mail check` for the sender and in `rex-wave config show`). Unacked letters remain eligible if the seat later binds a new live session incarnation.

### Operator Attention Rule & Passive TUI Indicator

Courier refusals (`address-gone`), `give_up` failures, and `AdapterDegraded` alerts **never** ring, toast, or raise unread/blocking inbox cards on the human `Operator` principal. Instead:

- Failure notices and `NotifyKind::MailDeliveryFailed` inbox entries go exclusively to `mail.hub_owner_seat` and the original sender seat, reconciled idempotently from durable `MailEvent::DeliveryFailed` entries across daemon restarts.
- `rex-wave tui` displays a passive, non-interrupting clickable indicator on `⚙ Settings` (`SidebarTreeSnapshot.mail_health`) without affecting `InboxBadge` or `OSC 7501`. Clicking `⚙ Settings` (or pressing `,`) opens the Settings view displaying live courier counters and the `Mail Delivery Failures` log.

## Canary-Gated Routes (`specs/mail/MailHub.tla`)

Sessions can self-claim a route behind a canary confirmation gate (either per bind with `--require-canary` or daemon-wide for all push-courier endpoints via `mail.require_canary = true` in `config.toml`) so normal mail never rings an unverified or forked destination (`RightSeat`, `NoFalseGiveUp`):

```sh
# 1. Bind in pending_canary state (mints a 128-bit canary token, redacted from Presence replies/reads):
rex-wave presence bind --seat bob --t3-thread thr-42 --require-canary

# 2. CourierHub dispatches a body-blind canary DeliveryClaim (referents: [], canary_token: "rwc_...")
#    to the candidate endpoint; normal mail waits until confirmation.

# 3. Confirm from inside the claimed session (distinct from global --token auth) to promote PendingCanary -> Live:
rex-wave presence confirm-canary --seat bob --incarnation <incarnation-id> --canary <canary-token>
```

## Multi-Machine Ownership (`OneMachineRings`)

Each daemon resolves and persists `<state_dir>/machine-id` (`--machine-id` / `REX_WAVE_MACHINE_ID` override, seeded from the host short name on first boot so transient DHCP hostname changes across restarts do not orphan local bindings). `CourierHub` enforces `OneMachineRings` directly across hubs sharing the same `Presence` and `Mail` journal by resolving one deterministic global winning route across all machines and lanes first and ringing locally only when that winner belongs to the local `machine_id`. For multi-store deployments running separate journals, `MachineMailSyncState` and `RouteSyncTable` provide the commutative, associative, idempotent CRDT replica-merge primitives (total `(claimed_at, epoch, machine_id, incarnation_id, confirmed)` ordering with per-incarnation withdrawal tombstones alongside union sync on content-addressed messages and ACKs); independent stores must synchronize `MachineMailSyncState` before local dispatch to maintain single-owner delivery across separate journals.

## Loopback HTTP API (`rex-wave daemon --http 127.0.0.1:<port>`)

| Method & Path | Action | Notes |
| --- | --- | --- |
| `GET /v1/mail` | `mail check` | Optional `?seat=`, `?all=1`, `?limit=`. |
| `GET /v1/mail?q=<query>` | `mail search` | SQLite FTS5 search; optional `?seat=`, `?limit=`. |
| `POST /v1/mail` | `mail send` | JSON body `{to, cc?, subject, body, from?, priority?, topic?, in_reply_to?, refs?, wait_export?, idem?}`; returns `201 Created` (`200 OK` on replay). |
| `POST /v1/mail/ack` | `mail ack` | JSON body `{target? | path?, sha256?, seat?, note?}`; returns `200 OK`. |

All routes enforce loopback-only CSRF headers and strict principal scoping; unauthorized seat access is masked as `404 Not Found`.

## CLI Exit Codes

| Exit Code | Meaning |
| --- | --- |
| `0` | Success. |
| `1` | Runtime, I/O, transport, or internal failure (or parity mismatch in `migrate-verify`). |
| `2` | Refused, denied, invalid arguments, `mutated_after_ack`/hash conflict refusal, or `protected-roots` violation. |
