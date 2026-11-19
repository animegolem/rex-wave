# rex-wave verification map

This directory is the maintained source for verifying the user-facing behavior of `rex-wave`. Read this index before driving the TUI or CLI, then follow the matching feature file as the recipe.

## Baseline preconditions

- Build `rex-wave` and resolve its binary path from Cargo metadata or `REX_WAVE_BIN`.
- Create `/tmp/rex-verify-$$` with `mail-sandbox/.rex-wave-sandbox`, `config/rex-wave/protected-roots`, and dynamic loopback ports for `--wake-bell` and `--bb-url`.
- Start an isolated `XDG_CONFIG_HOME=/tmp/rex-verify-$$/config rex-wave daemon --socket /tmp/rex-verify-$$/daemon.sock` owned by this run.
- Seed `proj_alpha` (`#general`, `#wave-auth-v2` with `author-jwt` and `migrator`), `proj_beta` (`#ops`, `#wave-search-idx` with `indexer`), `#lounge`, `@sam` and `@alice` DMs, `project:proj_alpha` Kanban cards, and 58+ unread `Inbox` notifications.
- Create an unattached `Rex` session (`rex new "rex-verify-$$" --window "harness" --json -- sleep 300`), start `rex-wave tui` via `rex run -s "$REX_VERIFY_SESSION" --focus=false --keep-open --json -- ...`, and resize `$REX_VERIFY_BLOCK` to `132x38`.
- Pass the `Doctor` check in [`../SKILL.md`](../SKILL.md) before sending keys.
- Never drive the operator's `~/.local/state/rex-wave/daemon.sock` or switch focus in `Rex.app`.

## Driving conventions

- Start each recipe from the baseline state unless its preconditions state otherwise.
- Scope every `rex` command with `-s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK"`, and pass `--focus=false` to any command that creates a pane or window.
- Poll `rex capture -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" --trim --unwrap` for expected text rather than relying on fixed sleeps alone.
- Pass `shift+g` and `shift+p` to `rex send-key` for uppercase `G` and `P` shortcuts.
- Use `rex send` for literal text inside the composer, followed by `rex send-key ... enter`.
- Preserve all proof under `/tmp/rex-wave-evidence/$RUN_ID/` across teardown.

## Proof and skip reporting

- Capture both the before state and the resulting state for each journey step.
- TUI proof pairs `rex capture --trim --unwrap` with `rex block call ... program_status` and `rex block call ... modes`.
- Mutation proof pairs the rendered screen with a read-only check from `rex-wave --socket "$SOCK" --json ...`, `$SANDBOX/to-owner/*.md`, or `mock-bb-state.json`.
- Record the feature ID and step slug with each captured file.
- Report an unreachable or failing path with the exact command and unmet precondition or defect. Never weaken an assertion to hide a product bug.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the user-visible behavior. It then uses four H2 sections in this exact order:

1. `Sub-features` lists short IDs with one line for each behavior.
2. `How to get to it (user POV)` lists user entry points across the TUI and CLI.
3. `Driving it with rex` starts with `Preconditions:` and uses labeled bullets that pair each user action with an exact command and observable result.
4. `Gotchas` lists traps and known protocol caveats that can invalidate a verification run.

## Features

- [Sidebar org-tree](./sidebar-org-tree.md) covers tree navigation, divider skipping, branch folding and unfolding, stage retargeting, and bottom jump to direct messages.
- [Room chat and canon promote](./room-chat-and-promote.md) covers channel and DM viewing, TUI message composition, CLI posting, and promoting message ranges to banked canon letters.
- [Inbox and OSC 7501 status](./inbox-and-osc7501.md) covers ranked notification cards, 55+ card viewport scrolling, Kitty `GraphicsMode` cycling, and `OSC 7501` `program_status` inspection.
- [Kanban board](./kanban-board.md) covers lane inspection, card lane advancement, inline task creation in the composer, and CLI board verification.
- [Agent inspector](./agent-inspector.md) covers live worker turn transcripts, the 24-cell `U+10EEEE` context window bar, sidebar percentage sync, and blocked interaction resolution.
