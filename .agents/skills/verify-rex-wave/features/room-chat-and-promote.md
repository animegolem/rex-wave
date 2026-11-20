# Room chat and canon promote

Room chat lets a user read `#channels`, `#<project>/wave-<slug>` rooms, and direct messages, compose new posts from the TUI or CLI, and promote a room message range into an immutable canon letter inside the verified mail sandbox.

## Sub-features

- `room-read` loads full room history across pages and streams live posts on the right-hand stage.
- `room-compose-tui` posts a message from the TUI composer to the active channel or DM.
- `room-post-cli` posts single-line or multi-line messages from `rex-wave post` or MCP `rex.room_post`.
- `room-promote` writes a canon Markdown letter to `<mail-sandbox>/to-<seat>/` and displays the banked path banner.

## How to get to it (user POV)

- Navigate to `#<channel>`, `#wave-<slug>`, or `@<seat>` in `Org Tree` and press `enter`.
- Press `i` (or `enter` while `Stage` has focus) to open the room composer.
- Press `P` (`shift+p`) in a room with posted messages to promote the selected or full range to a canon letter.
- Run `rex-wave post '<#room|@seat>' '<text>'` or `rex-wave promote '<#room>' <first>..<last> --to <seat> --subject <subj> --topic <topic>`.

## Driving it with rex

Preconditions:

- `Doctor` passes and `#proj_alpha/wave-auth-v2` exists under `Projects > proj_alpha`.
- `$SANDBOX` contains `.rex-wave-sandbox` and no prior letter in `$SANDBOX/to-owner/`.

- **Open wave room.** Select `auth-v2` in `Org Tree` and activate `Stage`. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter`. `rex capture --trim --unwrap` shows `Room · #proj_alpha/wave-auth-v2`, `[P] Promote`, and collapsed `@system:WaveConductor · brief for @<role> [Enter to expand]` summaries.
- **Compose multiline markdown and submit.** Open the composer with `i`, send bracketed paste `$'\e[200~# Council Review\nFirst line with **bold** and `code`.\nSecond wrapped line.\e[201~'` plus `ctrl+j` / `shift+enter` for additional lines, and submit with `enter`. Every wrapped line renders in the room feed without single-line clipping, and `j`/`k` + `enter` opens the scrollable full-message Pager (`esc` closes).
- **Promote to canon letter.** Trigger promotion from the room stage. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" shift+p`. `rex capture --trim --unwrap` displays a `Banked /tmp/rex-verify-.../mail-sandbox/to-owner/...md` status banner.
- **Confirm disk side effect.** Inspect `$SANDBOX/to-owner/*.md` outside the TUI. A Markdown file with YAML frontmatter (`author`, `to`, `topic`, `subject`) and all multiline message lines exists on disk, and `rex-wave --socket "$SOCK" --json inbox` records a `letter_banked` notification.
- **Proof.** Copy the banked `.md` file and before/after room screen captures into `/tmp/rex-wave-evidence/$RUN_ID/`.

## Gotchas

- Pressing `j`, `k`, `v`, or `q` while `Composer` has focus types characters into the draft instead of navigating or quitting (`ctrl+<char>` other than `ctrl+j`/`ctrl+u`/`ctrl+w`/`ctrl+a`/`ctrl+e`/`ctrl+c` is ignored). Press `esc` to exit `Composer` first.
- `rex send-key p` sends lowercase `p`, which is unbound. Pass `shift+p` to trigger `Action::Promote`.
- Inside a wave's own `#wave-<slug>` room, worker sender labels shorten from `@<slug>/<role>` to `@<role>`.
- Mention notifications only fire for principals who have already joined the channel (`room_post` or `room_join`). Have the target principal join or post in the channel before testing `@<handle>` mentions.
