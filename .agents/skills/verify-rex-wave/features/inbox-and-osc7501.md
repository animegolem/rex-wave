# Inbox and OSC 7501 status

The ranked `Inbox` surfaces pending worker interactions, blocking mail, direct messages, channel mentions, stuck workers, banked letters, task assignments, and finished waves in priority order, renders Kitty `z=-1` card backdrops, and emits `OSC 7501` program status updates.

## Sub-features

- `inbox-rank-and-scroll` lists unread notifications by descending rank and scrolls cleanly across 55+ cards without clipping off-screen cards into row 0.
- `inbox-graphics-cycle` cycles `GraphicsMode` across `Hybrid z=-1 + U+10EEEE`, `z=-1 only`, `U+10EEEE only`, and `Pure Cells` via `v`.
- `inbox-cli` queries unread and completed items via `rex-wave inbox [--all] [--limit N]`.
- `osc7501-status` reports terminal program status transitions (`blocked`, `working`, `idle`, `clear`) to Rex.

## How to get to it (user POV)

- Open `rex-wave tui`, which loads `Inbox` on the right-hand stage by default, or select `Inbox` at the top of `Org Tree`.
- Press `tab` to focus `Inbox` cards on `Stage`, then `j`, `k`, `g`, and `G` (`shift+g`) to scroll.
- Press `v` outside the composer to cycle `GraphicsMode`.
- Run `rex-wave --socket "$SOCK" inbox` in a terminal, or `rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" program_status` to read Rex's stored `OSC 7501` records.

## Driving it with rex

Preconditions:

- `Doctor` passes with at least 55 unread notifications seeded (including one `Interaction` card at rank `9000` for `@auth-v2/migrator`).
- `Stage` shows `Inbox` in `Hybrid z=-1 + U+10EEEE` mode.

- **Verify top-ranked card and badge.** Capture the initial frame. Run `rex capture -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" --trim --unwrap`. `Org Tree` displays `Inbox` with `◐1` and `[58]` (or higher), and the top card shows `@auth-v2/migrator waiting on interaction`.
- **Scroll across 55+ cards.** Focus `Stage` and jump to the bottom and back. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" tab`, `rex send-key ... j`, `rex send-key ... shift+g`, capture `inbox-bottom.screen.txt`, then run `rex send-key ... g`. Both captures preserve the `Inbox` header border without top-row card bleed.
- **Cycle graphics modes.** Press `v` four times, capturing after each press. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" v` repeatedly. The top-right stage badge advances from `Hybrid z=-1 + U+10EEEE` to `z=-1 only`, `U+10EEEE only`, `Pure Cells`, and back to `Hybrid z=-1 + U+10EEEE`.
- **Inspect OSC 7501 status and clean quit.** Query `rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" program_status` at startup, after approving `migrator`'s pending interaction, and after pressing `q`. Startup reports `{"id":"wave-auth-v2","state":"blocked","kind":"permission","app":"rex-wave"}`, resolving `int_mig_1` transitions the record to `"state": "working"`, and pressing `q` clears `.records` to `[]` while `modes.screen` returns `"primary"` and `process.last_exit.exit_code` is `0`.
- **Proof.** Save all four graphics-mode captures, `inbox-bottom.screen.txt`, and `*.program_status.json` under `/tmp/rex-wave-evidence/$RUN_ID/steps/`.

## Gotchas

- **Known `OSC 7501` `title`/`msg` base64 defect:** `crates/rex-wave-tui/src/kitty.rs` (`encode_osc7501`) writes raw `title` and `msg` text instead of standard base64 UTF-8 per Rex's Program Status Protocol (`https://www.superlogical.com/rex/docs/build/program-status.md`). When `title` (`#wave-auth-v2`) and `msg` (`1 worker blocked`) contain `#` or spaces, Rex skips those malformed `key=value` pairs and still stores `id`, `state`, `kind`, `progress`, and `app`. However, when `title` and `msg` contain only `[A-Za-z0-9_.,+/=-]*` characters (such as idle `msg=ready`), Rex parses them as invalid base64 (`5 % 4 != 0`) and discards the entire report. Base64-encoding `title` and `msg` in `encode_osc7501` fixes both cases.
- `rex-wave --socket "$SOCK" --json inbox` wraps its payload in `WireReply` (`{"reply":"inbox_list","value":{"unread":...}}`), so read `.value.unread` rather than `.inbox_list.unread`.
