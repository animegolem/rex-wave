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
- Multi-seat mail dissolves into a participant-scoped private channel `#mail-<topic-or-subject>-<hash8>` readable only by its participants and the Operator.
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

Configuration keys (in `config.toml` or `rex-wave config set`):

- `mail.export_enabled` (`true` | `false`): toggle mirror export without affecting SQLite commits.
- `mail.mirror_dir`: optional mirror root override (checked against `protected-roots`).
- `mail.drop_dir`: optional external drop folder scanned for hand-dropped `.md` files and consumed into `<mirror>` + SQLite after durable commit.

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
