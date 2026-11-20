# Agent inspector

The `Agent Inspector` streams a wave worker's BB turn events (`TurnStarted`, `UserMessage`, `ToolCall`, `ToolResult`, `AssistantText`), renders the 24-cell `U+10EEEE` context-window bar with category token counts, syncs context percentage back to the sidebar worker badge, steers live threads from the composer, and resolves pending permission or question prompts.

## Sub-features

- `inspector-events-and-usage` displays the worker's provider thread ID, activity state, turn log, token usage breakdown, and 24-cell `U+10EEEE` bar, updating `[<provider> · <pct>%]` in `Org Tree`.
- `inspector-resolve` approves (`a`) or denies (`d`) a blocked worker's `PendingInteraction` and forwards the resolution to `AgentHost::resolve`.
- `inspector-steer` sends a `WaveTell` message (`mode: auto`) from the TUI composer (`i`) or `rex-wave wave steer`.

## How to get to it (user POV)

- Expand `Projects > <project> > #wave-<slug>` in `Org Tree` and move the cursor onto a worker leaf (`author-jwt` or `migrator`), pressing `enter` to focus the inspector.
- Press `a` to allow once or `d` to deny when a pending interaction banner is shown, or `i` to steer the worker.
- Run `rex-wave wave status <wave>`, `rex-wave wave approve <wave> <role> <interaction>`, or `rex-wave watch agent:<wave>/<role>`.

## Driving it with rex

Preconditions:

- `Doctor` passes with mock or live BB bridge connected, `auth-v2/author-jwt` active (`42.0k / 200.0k` tokens), and `auth-v2/migrator` blocked on `int_mig_1`.

- **Inspect working agent and context bar.** Navigate to `author-jwt` under `auth-v2`. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j` until `author-jwt` is selected. `rex capture --trim --unwrap` shows `Agent Inspector · @auth-v2/author-jwt`, wrapped multi-line turn output with `TEXT ★ latest output` pinned on the latest assistant message, tool-call history (`cargo_test`), token usage (`42.0k / 200.0k (21%) · $0.38 · compact@160.0k`), and `Org Tree` updates the row tag to `[codex · 21%]`. `j`/`k` + `enter` opens a turn in the full-screen Pager.
- **Inspect blocked agent.** Move down one row to `migrator`. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j`. `rex capture --trim --unwrap` shows `◐ migrator`, `int_mig_1`, and `Write migrations/004_oauth_refresh.sql in git-worktree`.
- **Approve blocked interaction.** Focus `Stage` and press `a`. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter` followed by `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" a`. The mock BB bridge records `{"interaction_id":"int_mig_1","resolution":{"decision":"allow_once","grantedPermissions":null}}` in `mock-bb-state.json`, and `"$REX_WAVE_BIN" --socket "$SOCK" wave status auth-v2` transitions `migrator` out of `Blocked`.
- **Proof.** Save `agent-inspector-working.screen.txt`, `agent-inspector-blocked.screen.txt`, `agent-inspector-resolved.screen.txt`, and `mock-bb-state.json` under `/tmp/rex-wave-evidence/$RUN_ID/`.

## Gotchas

- `U+10EEEE` placeholder cells render visually in `Hybrid` and `PlaceholderOnly` graphics modes; in plain-text `rex capture`, they appear as Private Use Area codepoints with combining diacritics alongside the textual token summary.
- Sidebar context percentage (`[codex · 21%]`) populates only after `ThreadPage` or a live `AgentEventKind::Usage` frame has been loaded for that worker.
- Moving the sidebar cursor away from an agent node increments `FocusGeneration` and closes the server-side `WatchLeaseGuard` (`unwatch_thread`) on the BB bridge.
