# Inbox and OSC 7501 status

The ranked `Inbox` surfaces pending worker interactions, blocking mail, direct messages, channel mentions, stuck workers, banked letters, task assignments, and finished waves in priority order, renders Kitty `z=-1` card backdrops, and emits `OSC 7501` program status updates.

## Sub-features

- `inbox-rank-and-scroll` lists unread notifications by descending rank and scrolls cleanly across 55+ cards without clipping off-screen cards into row 0.
- `inbox-card-keys` marks cards read (`r`), dismisses a card (`x`) or an entire kind group (`X`), and resolves pending worker interactions (`a`, `s`, `d`) from the keyboard.
- `inbox-graphics-cycle` displays the active `GraphicsMode` inside the `?` Keymap Help Overlay and cycles across `Hybrid z=-1 + U+10EEEE`, `z=-1 only`, `U+10EEEE only`, and `Pure Cells` via `v`.
- `inbox-cli` queries unread and completed items via `rex-wave inbox [--all] [--limit N]`.
- `osc7501-status` reports terminal program status transitions (`blocked`, `working`, `idle`, `clear`) with UTF-8 base64-encoded `title` and `msg` fields to Rex.

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
- **Mark card read from keyboard.** Select a mention or DM card and press `r`. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j` then `rex send-key ... r`. The card renders a `✓` seen marker and `rex-wave --socket "$SOCK" --json inbox` reports a lower `.value.unread` count.
- **Open `?` help overlay and cycle graphics modes.** Send `?` (`rex send ... '?'`) to open `Keys · rex-wave`, then press `v` four times (`rex send-key ... v`), capturing after each press. `Graphics:` advances from `Hybrid z=-1 + U+10EEEE` to `z=-1 only`, `U+10EEEE only`, `Pure Cells`, and back to `Hybrid z=-1 + U+10EEEE`; `esc` closes the overlay.
- **Inspect OSC 7501 status and clean quit.** Query `rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" program_status` at startup, after approving `migrator`'s pending interaction, and after pressing `q`. Startup reports `{"id":"wave-auth-v2","state":"blocked","kind":"permission","app":"rex-wave","title":"#wave-auth-v2",...}` with decoded UTF-8 `title` and `msg`, resolving `int_mig_1` transitions `"state"` to `"working"`, and pressing `q` clears `.records` to `[]` while `modes.screen` returns `"primary"` and `process.last_exit.exit_code` is `0`.
- **Proof.** Save help-overlay + graphics-mode captures, `inbox-marked-read.screen.txt`, `inbox-bottom.screen.txt`, and `*.program_status.json` under `/tmp/rex-wave-evidence/$RUN_ID/steps/`.

## Gotchas

- `OSC 7501` requires `title` and `msg` values to be padded standard base64 UTF-8 (`crates/rex-wave-tui/src/kitty.rs::encode_osc7501`); Rex decodes them before returning `program_status` JSON.
- `rex-wave --socket "$SOCK" --json inbox` wraps its payload in `WireReply` (`{"reply":"inbox_list","value":{"unread":...}}`), so read `.value.unread` rather than `.inbox_list.unread`.
