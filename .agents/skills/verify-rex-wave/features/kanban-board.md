# Kanban board

The Kanban board renders Org-compatible workflow lanes (`todo`, `doing`, `review`, `done`), advances cards across lanes with compare-and-swap revisions, and creates new tasks inline from the TUI composer or `rex-wave task` CLI.

## Sub-features

- `kanban-view` displays lane counts, card numbers, priorities, titles, and `@assignee` badges for the active project or seat board.
- `kanban-move` advances the selected card to the next lane with `m` (or switches lanes with `[`, `]`, `h`, and `l`).
- `kanban-new-task` opens the composer on `n`, `i`, or `enter` and appends a new card to the focused lane.
- `kanban-cli` creates, moves, completes, archives, and lists cards via `rex-wave task`.

## How to get to it (user POV)

- Select `Kanban` in `Org Tree` and press `enter` (or `tab`) to focus the board stage.
- Press `h`, `l`, `[`, or `]` to choose a lane, `j` or `k` to choose a card, `m` to move the selected card to the next lane, and `n` or `i` to add a card.
- Run `rex-wave task list --board project:proj_alpha` or `rex-wave task add <title> --board <board> --lane <lane>`.

## Driving it with rex

Preconditions:

- `Doctor` passes and `project:proj_alpha` has card `#1` (`Implement EdDSA refresh rotation`) in `todo`.
- Focus is on `Org Tree` at `Inbox`.

- **Open Kanban stage.** Move down once to `Kanban` and focus `Stage`. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j` and wait for `Kanban · project:proj_alpha`, then run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter`. Cursor highlights card `#1` in the `todo` lane.
- **Advance card across lanes.** Move card `#1` from `todo` to `doing`. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" m`. Card `#1` renders under `doing` on stage.
- **Create task from composer.** Open the inline task composer in `todo`, type a title, and submit. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" i`, `rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" 'Verify Rex CUA Kanban card'`, and `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter`. `rex capture --trim --unwrap` shows `#5 Verify Rex CUA…` in `todo` (lane columns ellipsize titles past column width).
- **Confirm journal persistence.** Query the board from the CLI. Run `"$REX_WAVE_BIN" --socket "$SOCK" --json task list --board project:proj_alpha`. `.value.lanes` shows card `#1` in `doing` with incremented revision and the full title `Verify Rex CUA Kanban card` in `todo`.
- **Proof.** Save `kanban-initial.screen.txt`, `kanban-mutated.screen.txt`, and `kanban-board.json` to `/tmp/rex-wave-evidence/$RUN_ID/`.

## Gotchas

- Pressing `h` while on lane `0` (`todo`) returns focus from `Stage` to `Sidebar` instead of wrapping to the last lane; use `[` and `]` when testing lane-only movement.
- Each Kanban column splits the stage width across 4 lanes (`~21` inner cells at `132` columns), so rendered card titles truncate with `…`. Assert the prefix on screen and the full title in `rex-wave --json task list`.
- `default_board_id` selects the first board in `overview.kanban` (or `project:<first_project>` before any board exists), so seed `project:proj_alpha` before expecting `Kanban` to open that project's board.
