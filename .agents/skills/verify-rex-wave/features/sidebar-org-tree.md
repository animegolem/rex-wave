# Sidebar org-tree

The left-pane org-tree lets a user move across `Inbox`, `Kanban`, `Projects` (with nested `#channels`, `#wave-<slug>` rooms, and child wave workers), `Channels`, and `Direct Messages`, fold or unfold any branch, and update the right-hand stage without stale frames.

## Sub-features

- `tree-move` moves the cursor across focusable rows with `j`, `k`, `g`, and `G` while skipping the horizontal divider.
- `tree-fold` collapses and expands section headers, project branches, and wave branches with `space`, `h`, and `l`.
- `tree-activate` retargets the right-hand stage after the 100 ms hover debounce or immediately on `enter`.
- `tree-watch-cli` streams live `SidebarTreeSnapshot` frames over `rex-wave watch sidebar`.

## How to get to it (user POV)

- Open `rex-wave tui`, which starts with focus on `Org Tree` at `Inbox`.
- Press `tab` or `h` from `Stage` when at the leftmost column to return focus to `Org Tree`.
- Run `rex-wave watch sidebar` in a terminal to observe live tree snapshots as JSON.

## Driving it with rex

Preconditions:

- `Doctor` passes on `$REX_VERIFY_SESSION` and `$REX_VERIFY_BLOCK` at `132x38`.
- Focus is on `Org Tree` at `Inbox`, with `proj_alpha`, `proj_beta`, `#lounge`, `@alice`, and `@sam` seeded.

- **Move across top nodes.** Step from `Inbox` to `Kanban` and `Projects`. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j` once, wait for `Kanban · project:proj_alpha` in `rex capture -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" --trim --unwrap`, then run `rex send-key ... j` a second time. Cursor lands on `▾ Projects`, skipping the `─` divider row.
- **Collapse branch.** Fold `Projects`. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" space`. `rex capture --trim --unwrap` shows `▸ Projects` and hides `proj_alpha`, `proj_beta`, and their child waves.
- **Expand branch.** Unfold `Projects`. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" space`. `rex capture --trim --unwrap` shows `▾ Projects`, `proj_alpha`, `auth-v2` (redundant `#wave-` prefix stripped under project branches), `author-jwt [codex]`, `migrator [claude]`, and `proj_beta`.
- **Resize or hide sidebar.** Send `>`, `<`, and `\` via `rex send` (or drag the pane divider with the mouse) to widen, narrow, or hide and restore `Org Tree`.
- **Jump to bottom DM.** Jump to the last focusable row. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" shift+g`. After 150 ms, `rex capture --trim --unwrap` shows the `@sam` direct-message transcript on the right-hand stage.
- **Proof.** Save folded, unfolded, and bottom-jump captures to `/tmp/rex-wave-evidence/$RUN_ID/steps/` alongside `program_status` and `modes` JSON snapshots.

## Gotchas

- `j` and `k` trigger a 100 ms `HOVER_DEBOUNCE` before opening the new stage watch. Poll `rex capture` for the new stage header or press `enter` to retarget immediately.
- `rex send-key G` sends lowercase `g` (move to top). Always pass `shift+g` for `Action::Bottom`.
- Collapsing an ancestor while the cursor sits on a descendant moves the cursor up to the nearest visible ancestor.
