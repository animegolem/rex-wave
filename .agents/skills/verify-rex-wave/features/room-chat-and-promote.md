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

- **Open wave room.** Select `#wave-auth-v2` in `Org Tree` and activate `Stage`. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter`. `rex capture --trim --unwrap` shows `Room · #proj_alpha/wave-auth-v2` and `[P] Promote`.
- **Compose and submit message.** Open the composer, enter text, and send. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" i`, `rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" '@author-jwt Rex CUA live check passed'`, and `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter`. The message `@author-jwt Rex CUA live check passed` appears in the room transcript with a journal sequence number.
- **Promote to canon letter.** Trigger promotion from the room stage. Run `rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" shift+p`. `rex capture --trim --unwrap` displays a `Banked /tmp/rex-verify-.../mail-sandbox/to-owner/...md` status banner.
- **Confirm disk side effect.** Inspect `$SANDBOX/to-owner/*.md` outside the TUI. A Markdown file with YAML frontmatter (`author`, `to`, `topic`, `subject`) and `@author-jwt Rex CUA live check passed` exists on disk, and `rex-wave --socket "$SOCK" --json inbox` records a `letter_banked` notification.
- **Proof.** Copy the banked `.md` file and before/after room screen captures into `/tmp/rex-wave-evidence/$RUN_ID/`.

## Gotchas

- Pressing `j`, `k`, `v`, or `q` while `Composer` has focus types characters into the draft instead of navigating or quitting. Press `esc` to exit `Composer` first.
- `rex send-key p` sends lowercase `p`, which is unbound. Pass `shift+p` to trigger `Action::Promote`.
- Room stage lines currently render single-line summaries truncated with `…` when a post contains embedded newlines; verify full multi-line bodies via `rex-wave --socket "$SOCK" --json call read` or the banked canon letter file.
- Mention notifications only fire for principals who have already joined the channel (`room_post` or `room_join`). Have the target principal join or post in the channel before testing `@<handle>` mentions.
