#!/usr/bin/env bash
set -euo pipefail

export PATH="${HOME:-}/.local/bin:${HOME:-}/.cargo/bin:/Applications/Rex.app/Contents/Helpers/Rex Server.rex/Contents/MacOS:/opt/homebrew/bin:/usr/local/bin:$PATH"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_ID="${REX_VERIFY_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
SCRATCH_DIR="/tmp/rex-verify-$$"
EVIDENCE_DIR="${REX_WAVE_EVIDENCE_DIR:-/tmp/rex-wave-evidence/$RUN_ID}"
STEPS_DIR="$EVIDENCE_DIR/steps"

SOCK="$SCRATCH_DIR/daemon.sock"
JOURNAL="$SCRATCH_DIR/journal.sqlite3"
SANDBOX="$SCRATCH_DIR/mail-sandbox"
CONFIG_DIR="$SCRATCH_DIR/config"
BB_DATA_DIR="$SCRATCH_DIR/bb-data"
MOCK_BB_STATE="$SCRATCH_DIR/mock-bb-state.json"

DAEMON_PID=""
MOCK_BB_PID=""
REX_VERIFY_SESSION=""
REX_VERIFY_BLOCK=""
TORN_DOWN=0
STEP_NO=0
START_MS=""

now_ms() {
	node -e 'process.stdout.write(String(Date.now()))'
}

free_port() {
	node -e '
		const net = require("node:net");
		const srv = net.createServer();
		srv.listen(0, "127.0.0.1", () => {
			const { port } = srv.address();
			srv.close(() => process.stdout.write(String(port)));
		});
	'
}

resolve_rex_wave_bin() {
	if [[ -n "${REX_WAVE_BIN:-}" && -x "${REX_WAVE_BIN}" ]]; then
		printf '%s\n' "$REX_WAVE_BIN"
		return 0
	fi
	local target_dir
	if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
		target_dir="$CARGO_TARGET_DIR"
	else
		target_dir="$(cargo metadata --no-deps --format-version 1 | jq -er '.target_directory')"
	fi
	cargo build -p rex-wave --locked >&2
	printf '%s\n' "$target_dir/debug/rex-wave"
}

teardown_runtime() {
	if [[ "$TORN_DOWN" -eq 1 ]]; then
		return 0
	fi
	TORN_DOWN=1

	if [[ -f "$MOCK_BB_STATE" && -d "$EVIDENCE_DIR" ]]; then
		cp "$MOCK_BB_STATE" "$EVIDENCE_DIR/mock-bb-state.json" 2>/dev/null || true
	fi
	if [[ -f "$SCRATCH_DIR/daemon.stderr.log" && -d "$EVIDENCE_DIR" ]]; then
		cp "$SCRATCH_DIR/daemon.stderr.log" "$EVIDENCE_DIR/daemon.stderr.log" 2>/dev/null || true
	fi

	if [[ -n "$REX_VERIFY_SESSION" ]]; then
		rex kill "$REX_VERIFY_SESSION" >/dev/null 2>&1 || true
	else
		local label_sess
		label_sess="$(rex ls --json 2>/dev/null | jq -r --arg lbl "rex-verify-$$" '.sessions[]? | select(.label == $lbl) | .session_id' || true)"
		if [[ -n "$label_sess" ]]; then
			while IFS= read -r sid; do
				[[ -n "$sid" ]] && rex kill "$sid" >/dev/null 2>&1 || true
			done <<< "$label_sess"
		fi
	fi
	if [[ -n "$DAEMON_PID" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
		kill "$DAEMON_PID" 2>/dev/null || true
		wait "$DAEMON_PID" 2>/dev/null || true
	fi
	if [[ -n "$MOCK_BB_PID" ]] && kill -0 "$MOCK_BB_PID" 2>/dev/null; then
		kill "$MOCK_BB_PID" 2>/dev/null || true
		wait "$MOCK_BB_PID" 2>/dev/null || true
	fi
	rm -rf "$SCRATCH_DIR"
}
trap teardown_runtime EXIT
trap 'teardown_runtime; exit 130' INT
trap 'teardown_runtime; exit 143' TERM

fail() {
	printf 'FAIL: %s\n' "$1" >&2
	exit 1
}

assert_contains() {
	local haystack="$1"
	local needle="$2"
	local label="$3"
	if [[ "$haystack" != *"$needle"* ]]; then
		printf 'Assertion failed (%s): expected to find %q\n' "$label" "$needle" >&2
		exit 1
	fi
}

assert_not_contains() {
	local haystack="$1"
	local needle="$2"
	local label="$3"
	if [[ "$haystack" == *"$needle"* ]]; then
		printf 'Assertion failed (%s): expected NOT to find %q\n' "$label" "$needle" >&2
		exit 1
	fi
}

capture_screen() {
	rex capture -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" --trim --unwrap
}

wait_screen_contains() {
	local needle="$1"
	local label="$2"
	local timeout_ms="${3:-6000}"
	local deadline=$(( $(now_ms) + timeout_ms ))
	local screen=""
	while (( $(now_ms) < deadline )); do
		screen="$(capture_screen 2>/dev/null || true)"
		if [[ "$screen" == *"$needle"* ]]; then
			return 0
		fi
		sleep 0.08
	done
	printf 'Timed out waiting for %q (%s). Last screen:\n%s\n' "$needle" "$label" "$screen" >&2
	exit 1
}

wait_screen_not_contains() {
	local needle="$1"
	local label="$2"
	local timeout_ms="${3:-6000}"
	local deadline=$(( $(now_ms) + timeout_ms ))
	local screen=""
	while (( $(now_ms) < deadline )); do
		screen="$(capture_screen 2>/dev/null || true)"
		if [[ "$screen" != *"$needle"* ]]; then
			return 0
		fi
		sleep 0.08
	done
	printf 'Timed out waiting for %q to disappear (%s). Last screen:\n%s\n' "$needle" "$label" "$screen" >&2
	exit 1
}

record_step() {
	local slug="$1"
	STEP_NO=$(( STEP_NO + 1 ))
	local prefix
	prefix="$(printf '%02d-%s' "$STEP_NO" "$slug")"
	capture_screen > "$STEPS_DIR/${prefix}.screen.txt"
	rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" program_status \
		> "$STEPS_DIR/${prefix}.program_status.json"
	rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" modes \
		> "$STEPS_DIR/${prefix}.modes.json"
	printf '  [evidence] %s\n' "$prefix"
}

write_mock_bb_server() {
	cat > "$SCRATCH_DIR/mock-bb.mjs" <<'EOF'
import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import http from "node:http";

const [portArg, expectedToken, statePath] = process.argv.slice(2);
const port = Number(portArg);
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const state = {
	connected: false,
	kv: {},
	metaByThread: {},
	spawned: [],
	sends: [],
	resolves: [],
	retired: [],
};

function persist() {
	writeFileSync(statePath, JSON.stringify(state, null, 2));
}

function sendJson(sock, obj) {
	const payload = Buffer.from(JSON.stringify(obj), "utf8");
	let header;
	if (payload.length < 126) {
		header = Buffer.from([0x81, payload.length]);
	} else if (payload.length < 65536) {
		header = Buffer.alloc(4);
		header[0] = 0x81;
		header[1] = 126;
		header.writeUInt16BE(payload.length, 2);
	} else {
		header = Buffer.alloc(10);
		header[0] = 0x81;
		header[1] = 127;
		header.writeBigUInt64BE(BigInt(payload.length), 2);
	}
	sock.write(Buffer.concat([header, payload]));
}

function eventsForThread(threadId) {
	const meta = state.metaByThread[threadId] ?? { role: "worker", waveId: "wave" };
	if (meta.role === "migrator") {
		return [
			{ seq: 1, createdAt: 1700000150, type: "turn/started", data: {} },
			{
				seq: 2,
				createdAt: 1700000320,
				type: "item/started",
				data: {
					item: {
						type: "fileChange",
						name: "write_file",
						path: "migrations/004_oauth_refresh.sql",
					},
				},
			},
		];
	}
	return [
		{ seq: 1, createdAt: 1700000100, type: "turn/started", data: {} },
		{
			seq: 2,
			createdAt: 1700000120,
			type: "system/manager/user_message",
			data: { text: `Execute ${meta.waveId}/${meta.role} verification slice.` },
		},
		{
			seq: 3,
			createdAt: 1700000250,
			type: "item/started",
			data: {
				item: {
					type: "commandExecution",
					name: "cargo_test",
					command: "cargo test --workspace --locked",
				},
			},
		},
		{
			seq: 4,
			createdAt: 1700000610,
			type: "item/completed",
			data: {
				item: {
					type: "commandExecution",
					name: "cargo_test",
					command: "72 passed; 0 failed",
					status: "completed",
				},
			},
		},
		{
			seq: 5,
			createdAt: 1700000700,
			type: "item/completed",
			data: {
				item: {
					type: "agentMessage",
					text: `Slice ready for @owner review in #wave-${meta.waveId}.`,
				},
			},
		},
	];
}

function contextForThread(threadId) {
	const meta = state.metaByThread[threadId] ?? { role: "worker" };
	if (meta.role === "migrator") {
		return {
			used_tokens: 128000,
			model_context_window: 200000,
			estimated: false,
			auto_compact_at_tokens: 160000,
			cost_usd: 1.14,
			categories: [
				{ id: "msgs", label: "Messages", kind: "used", tokens: 88000 },
				{ id: "tools", label: "Tools", kind: "used", tokens: 40000 },
				{ id: "free", label: "Free", kind: "free", tokens: 72000 },
			],
		};
	}
	return {
		used_tokens: 42000,
		model_context_window: 200000,
		estimated: false,
		auto_compact_at_tokens: 160000,
		cost_usd: 0.38,
		categories: [
			{ id: "msgs", label: "Messages", kind: "used", tokens: 26000 },
			{ id: "tools", label: "Tools", kind: "used", tokens: 16000 },
			{ id: "sys", label: "Reserved", kind: "reserved", tokens: 12000 },
			{ id: "free", label: "Free", kind: "free", tokens: 146000 },
		],
	};
}

const server = http.createServer((_req, res) => {
	res.writeHead(404);
	res.end();
});

server.on("upgrade", (req, sock, head) => {
	if (req.headers["x-bb-plugin-token"] !== expectedToken) {
		sock.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
		sock.destroy();
		return;
	}
	const key = req.headers["sec-websocket-key"];
	if (!key || typeof key !== "string") {
		sock.destroy();
		return;
	}
	const accept = createHash("sha1").update(key + WS_GUID).digest("base64");
	sock.write(
		"HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			`Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
	);

	state.connected = true;
	persist();
	sendJson(sock, { t: "hello", plugin_version: "0.1.0", sdk_version: "0.6.132" });

	let buf = Buffer.from(head);
	const drain = () => {
		while (buf.length >= 2) {
			const opcode = buf[0] & 0x0f;
			const masked = (buf[1] & 0x80) !== 0;
			let len = buf[1] & 0x7f;
			let offset = 2;
			if (len === 126) {
				if (buf.length < 4) return;
				len = buf.readUInt16BE(2);
				offset = 4;
			} else if (len === 127) {
				if (buf.length < 10) return;
				len = Number(buf.readBigUInt64BE(2));
				offset = 10;
			}
			const maskBytes = masked ? 4 : 0;
			if (buf.length < offset + maskBytes + len) return;
			const payload = Buffer.from(buf.subarray(offset + maskBytes, offset + maskBytes + len));
			if (masked) {
				const mask = buf.subarray(offset, offset + 4);
				for (let i = 0; i < payload.length; i += 1) {
					payload[i] ^= mask[i & 3];
				}
			}
			buf = buf.subarray(offset + maskBytes + len);

			if (opcode === 0x8) {
				sock.end();
				return;
			}
			if (opcode === 0x9) {
				sock.write(Buffer.from([0x8a, 0x00]));
				continue;
			}
			if (opcode !== 0x1) continue;

			const down = JSON.parse(payload.toString("utf8"));
			handleDown(sock, down);
		}
	};

	sock.on("data", (chunk) => {
		buf = Buffer.concat([buf, chunk]);
		drain();
	});
	drain();
});

function handleDown(sock, down) {
	const reqId = down.req_id ?? 0;
	switch (down.t) {
		case "find_by_meta": {
			const m = down.meta;
			const k = `spawn:${m.waveId}/${m.role}/${m.attempt}`;
			sendJson(sock, { t: "result", req_id: reqId, ok: true, value: { thread_id: state.kv[k] ?? null } });
			break;
		}
		case "spawn": {
			const args = down.args;
			const meta = args.pluginMetadata.rexWave;
			const tid = `th_${meta.waveId}_${meta.role}`;
			state.kv[`spawn:${meta.waveId}/${meta.role}/${meta.attempt}`] = tid;
			state.metaByThread[tid] = meta;
			state.spawned.push({ tid, projectId: args.projectId, role: meta.role, waveId: meta.waveId });
			persist();
			sendJson(sock, { t: "result", req_id: reqId, ok: true, value: { thread_id: tid } });
			sendJson(sock, {
				t: "thread",
				signal: "active",
				thread_id: tid,
				project_id: args.projectId,
				meta,
				error: null,
			});
			if (meta.role === "migrator") {
				setTimeout(() => {
					sendJson(sock, {
						t: "interaction",
						thread_id: tid,
						meta,
						interaction: {
							id: "int_mig_1",
							payload: {
								kind: "approval",
								reason: "Write migrations/004_oauth_refresh.sql in git-worktree",
							},
						},
					});
				}, 100);
			} else if (meta.role === "crash-bot") {
				setTimeout(() => {
					sendJson(sock, {
						t: "thread",
						signal: "failed",
						thread_id: tid,
						project_id: args.projectId,
						meta,
						error: "Insufficient balance (402)",
					});
				}, 80);
			}
			break;
		}
		case "events_page": {
			let rows = eventsForThread(down.thread_id);
			if (typeof down.after_seq === "number") {
				rows = rows.filter((r) => r.seq > down.after_seq);
			}
			if (typeof down.before_seq === "number") {
				rows = rows.filter((r) => r.seq < down.before_seq);
			}
			if (down.order === "desc") {
				rows = [...rows].reverse();
			}
			if (typeof down.limit === "number") {
				rows = rows.slice(0, down.limit);
			}
			sendJson(sock, { t: "result", req_id: reqId, ok: true, value: rows });
			break;
		}
		case "thread_context": {
			sendJson(sock, { t: "result", req_id: reqId, ok: true, value: contextForThread(down.thread_id) });
			break;
		}
		case "send": {
			state.sends.push({ thread_id: down.thread_id, mode: down.mode, text: down.text });
			persist();
			sendJson(sock, { t: "result", req_id: reqId, ok: true, value: { delivery: "sent" } });
			break;
		}
		case "resolve": {
			state.resolves.push({
				thread_id: down.thread_id,
				interaction_id: down.interaction_id,
				resolution: down.resolution,
			});
			persist();
			sendJson(sock, { t: "result", req_id: reqId, ok: true, value: null });
			const meta = state.metaByThread[down.thread_id] ?? null;
			sendJson(sock, {
				t: "thread",
				signal: "active",
				thread_id: down.thread_id,
				project_id: "proj_alpha",
				meta,
				error: null,
			});
			break;
		}
		case "retire": {
			state.retired.push(down.thread_id);
			persist();
			sendJson(sock, { t: "result", req_id: reqId, ok: true, value: null });
			break;
		}
		default:
			break;
	}
}

persist();
server.listen(port, "127.0.0.1");
EOF
}

seed_workspace_via_cli_and_mcp() {
	local bin="$1"
	"$bin" --socket "$SOCK" post '#lounge' $'Workspace verification online.\nLine 2: checking wrapped multi-line room chat.\nLine 3: @all welcome.' >/dev/null
	"$bin" --socket "$SOCK" post '#proj_alpha/general' $'Alpha release room initialized.\nTracking refresh rotation and DB migrations.' >/dev/null
	"$bin" --socket "$SOCK" post '#proj_beta/ops' $'Beta indexing ops channel.\nMonitoring shard compaction latency.' >/dev/null

	"$bin" --socket "$SOCK" wave start auth-v2 \
		--project proj_alpha \
		--goal $'Rotate OAuth EdDSA refresh tokens without replay gaps.\nCoordinate schema migration before flipping verifier.' \
		--worker author-jwt=codex/gpt-6.1-sol \
		--worker migrator=claude-code/claude-opus-5-5 >/dev/null

	"$bin" --socket "$SOCK" wave start search-idx \
		--project proj_beta \
		--goal "Build incremental Tantivy segment merger" \
		--worker indexer=codex/gpt-6.1-sol >/dev/null

	"$bin" --socket "$SOCK" wave start release-v1 \
		--project proj_beta \
		--goal "Verify terminal outcome glyphs and auto-fold" \
		--worker clean-bot=codex/gpt-6.1-sol \
		--worker crash-bot=claude-code/claude-opus-5-5 >/dev/null

	local rel_deadline=$(( $(now_ms) + 8000 ))
	local rel_ready=0
	while (( $(now_ms) < rel_deadline )); do
		local st
		st="$("$bin" --socket "$SOCK" --json wave status release-v1 2>/dev/null || true)"
		if printf '%s' "$st" | jq -e '
			(.value.workers | any(.[0] == "clean-bot" and .[2].phase == "live")) and
			(.value.workers | any(.[0] == "crash-bot" and .[2].phase == "live" and .[2].activity.activity == "errored"))
		' >/dev/null 2>&1; then
			rel_ready=1
			break
		fi
		sleep 0.08
	done
	(( rel_ready == 1 )) || fail "release-v1 workers did not reach live + errored before finish"

	local finish_blocked=0
	"$bin" --socket "$SOCK" wave finish release-v1 \
		--summary "Release smoke finished with crash-bot provider error" >/dev/null 2>&1 || finish_blocked=$?
	(( finish_blocked == 3 )) || fail "expected wave finish without --allow-incomplete to exit 3 when crash-bot is errored (got $finish_blocked)"

	"$bin" --socket "$SOCK" wave finish release-v1 \
		--summary "Release smoke finished with crash-bot provider error" \
		--allow-incomplete >/dev/null

	local ret_deadline=$(( $(now_ms) + 8000 ))
	local ret_ready=0
	while (( $(now_ms) < ret_deadline )); do
		local st
		st="$("$bin" --socket "$SOCK" --json wave status release-v1 2>/dev/null || true)"
		if printf '%s' "$st" | jq -e '
			.value.status.status == "finished" and
			(.value.workers | any(.[0] == "clean-bot" and .[2].phase == "retired" and (.[2].errored == null))) and
			(.value.workers | any(.[0] == "crash-bot" and .[2].phase == "retired" and .[2].errored == "Insufficient balance (402)"))
		' >/dev/null 2>&1; then
			ret_ready=1
			break
		fi
		sleep 0.08
	done
	(( ret_ready == 1 )) || fail "release-v1 workers did not retire cleanly/errored after wave finish"

	"$bin" --socket "$SOCK" task add "Implement EdDSA refresh rotation" \
		--board project:proj_alpha --lane todo --assign @auth-v2/author-jwt --priority high >/dev/null
	"$bin" --socket "$SOCK" task add "Draft 004_oauth_refresh.sql migration" \
		--board project:proj_alpha --lane doing --assign @auth-v2/migrator --priority blocking >/dev/null
	"$bin" --socket "$SOCK" task add "Audit replay window clock skew" \
		--board project:proj_alpha --lane review --assign @owner --priority normal >/dev/null
	"$bin" --socket "$SOCK" task add "Pin JWKS cache TTL" \
		--board project:proj_alpha --lane done --assign @sam --priority low >/dev/null

	"$bin" --socket "$SOCK" post '@sam' $'Can you post the 50-shard verification matrix updates?\nInclude latency numbers per shard.' >/dev/null
	"$bin" --socket "$SOCK" post '@alice' 'Ready for cross-review once auth-v2 finishes.' >/dev/null

	local sam_token alice_token
	sam_token="$("$bin" --socket "$SOCK" token --seat sam)"
	alice_token="$("$bin" --socket "$SOCK" token --seat alice)"

	REX_WAVE_BIN_PATH="$bin" REX_SOCK_PATH="$SOCK" SAM_TOK="$sam_token" ALICE_TOK="$alice_token" node -e '
		const { spawn } = require("node:child_process");
		const readline = require("node:readline");

		function runMcpCode(token, code) {
			return new Promise((resolve, reject) => {
				const child = spawn(process.env.REX_WAVE_BIN_PATH, ["--socket", process.env.REX_SOCK_PATH, "mcp", "--stdio"], {
					env: { ...process.env, REX_WAVE_TOKEN: token },
					stdio: ["pipe", "pipe", "inherit"],
				});
				const rl = readline.createInterface({ input: child.stdout });
				const replies = new Map();
				rl.on("line", (line) => {
					if (!line.trim()) return;
					const msg = JSON.parse(line);
					if (msg.id !== undefined) replies.set(msg.id, msg);
					if (replies.has(2)) {
						const r2 = replies.get(2);
						child.stdin.end();
						if (r2.error || r2.result?.isError) {
							reject(new Error(JSON.stringify(r2)));
						} else {
							resolve(r2.result);
						}
					}
				});
				child.on("error", reject);
				child.stdin.write(JSON.stringify({
					jsonrpc: "2.0",
					id: 1,
					method: "initialize",
					params: { protocolVersion: "2025-11-25", capabilities: {}, clientInfo: { name: "rex-verify", version: "0.1.0" } }
				}) + "\n");
				child.stdin.write(JSON.stringify({
					jsonrpc: "2.0",
					id: 2,
					method: "tools/call",
					params: { name: "run_code", arguments: { code, timeout_ms: 20000 } }
				}) + "\n");
			});
		}

		(async () => {
			await runMcpCode(process.env.SAM_TOK, `
				await rex.room_join({ room: { ref: "channel", scope: { scope: "project", project: "proj_alpha" }, name: "general" } });
				await rex.room_join({ room: { ref: "channel", scope: { scope: "project", project: "proj_beta" }, name: "ops" } });
				await rex.room_join({ room: { ref: "channel", scope: { scope: "global" }, name: "lounge" } });
				for (let i = 1; i <= 32; i += 1) {
					await rex.room_post({
						room: { ref: "channel", scope: { scope: "project", project: "proj_alpha" }, name: "general" },
						body: "@owner shard-" + String(i).padStart(2, "0") + " replay vector green\\nVerified EdDSA signature and rotation nonce."
					});
				}
				for (let i = 1; i <= 20; i += 1) {
					await rex.room_post({
						room: { ref: "channel", scope: { scope: "project", project: "proj_beta" }, name: "ops" },
						body: "@owner index-segment-" + String(i).padStart(2, "0") + " compacted in " + (10 + i) + "ms"
					});
				}
				for (let i = 1; i <= 4; i += 1) {
					await rex.room_post({
						room: { ref: "dm", with: { kind: "operator", seat: "owner" } },
						body: "DM batch #" + i + " from @sam:\\nAll 32 alpha shards reported nominal p99."
					});
				}
				return { ok: true };
			`);
			await runMcpCode(process.env.ALICE_TOK, `
				await rex.room_join({ room: { ref: "channel", scope: { scope: "global" }, name: "lounge" } });
				for (let i = 1; i <= 3; i += 1) {
					await rex.room_post({
						room: { ref: "channel", scope: { scope: "global" }, name: "lounge" },
						body: "@owner lounge check #" + i + " from @alice"
					});
				}
				await rex.room_post({
					room: { ref: "dm", with: { kind: "operator", seat: "owner" } },
					body: "Standing by for PR review."
				});
				return { ok: true };
			`);
		})().catch((err) => {
			console.error(err);
			process.exit(1);
		});
	'

	local deadline=$(( $(now_ms) + 8000 ))
	local unread=0
	while (( $(now_ms) < deadline )); do
		unread="$("$bin" --socket "$SOCK" --json inbox --limit 200 | jq -er '.value.unread')"
		if (( unread >= 58 )); then
			printf '  [seed] inbox unread=%d (>=55 target met)\n' "$unread"
			return 0
		fi
		sleep 0.1
	done
	fail "expected >=58 unread inbox notifications after seeding, got $unread"
}

START_MS="$(now_ms)"
rm -rf "$SCRATCH_DIR"
if [[ -d "$STEPS_DIR" ]] && [[ -n "$(ls -A "$STEPS_DIR" 2>/dev/null)" ]]; then
	fail "evidence steps dir $STEPS_DIR already contains files; choose a fresh REX_VERIFY_RUN_ID"
fi
mkdir -p "$SCRATCH_DIR" "$SANDBOX" "$CONFIG_DIR/rex-wave" "$BB_DATA_DIR" "$STEPS_DIR"
printf 'rex-verify-sandbox\n' > "$SANDBOX/.rex-wave-sandbox"
: > "$CONFIG_DIR/rex-wave/protected-roots"
cat > "$BB_DATA_DIR/projects.json" <<'EOF'
{
  "projects": [
    { "id": "proj_alpha", "name": "auth-platform" },
    { "id": "proj_beta", "name": "search-engine" }
  ]
}
EOF

REX_WAVE_BIN="$(resolve_rex_wave_bin)"
printf '==> Using rex-wave binary: %s\n' "$REX_WAVE_BIN"
printf '==> Scratch runtime dir:   %s\n' "$SCRATCH_DIR"
printf '==> Evidence output dir:   %s\n' "$EVIDENCE_DIR"

BB_PORT="$(free_port)"
BELL_PORT="$(free_port)"
BB_TOKEN="rw_bb_verify_$$"

write_mock_bb_server
node "$SCRATCH_DIR/mock-bb.mjs" "$BB_PORT" "$BB_TOKEN" "$MOCK_BB_STATE" &
MOCK_BB_PID="$!"

start_daemon() {
	XDG_CONFIG_HOME="$CONFIG_DIR" BB_DATA_DIR="$BB_DATA_DIR" "$REX_WAVE_BIN" --socket "$SOCK" daemon \
		--journal "$JOURNAL" \
		--mail-sandbox "$SANDBOX" \
		--bb-url "ws://127.0.0.1:${BB_PORT}/api/v1/plugins/rex-wave/http/bridge" \
		--bb-token "$BB_TOKEN" \
		--operator owner \
		--wake-bell "127.0.0.1:${BELL_PORT}" \
		>>"$SCRATCH_DIR/daemon.stdout.log" 2>>"$SCRATCH_DIR/daemon.stderr.log" &
	DAEMON_PID="$!"

	local sock_deadline=$(( $(now_ms) + 6000 ))
	while [[ ! -S "$SOCK" ]]; do
		if (( $(now_ms) >= sock_deadline )); then
			fail "daemon socket $SOCK did not appear"
		fi
		sleep 0.05
	done
}

start_daemon

printf '==> Seeding waves, 55+ inbox cards, rooms, Kanban tasks, and DMs\n'
seed_workspace_via_cli_and_mcp "$REX_WAVE_BIN"

printf '==> Launching rex-wave tui inside isolated background Rex session\n'
# `rex new` on Rex 0.1.0 creates an unattached server session without switching
# any GUI client's active session; `rex run --focus=false` keeps background focus intact.
CREATED_SESSION_JSON="$(rex new "rex-verify-$$" --window "harness" --json -- sleep 300)"
REX_VERIFY_SESSION="$(printf '%s' "$CREATED_SESSION_JSON" | jq -er '.session_id')"

TUI_RUN_JSON="$(rex run -s "$REX_VERIFY_SESSION" --focus=false --keep-open --label "rex-wave-tui" --json -- "$REX_WAVE_BIN" --socket "$SOCK" tui)"
REX_VERIFY_BLOCK="$(printf '%s' "$TUI_RUN_JSON" | jq -er '.block_ids[0]')"

rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" resize '{"columns":132,"rows":38}' >/dev/null

wait_screen_contains "Org Tree" "TUI initial frame"
wait_screen_contains "Inbox" "Sidebar Inbox node"
wait_screen_contains "migrator waiting on interaction" "Top-ranked interaction card in Inbox"

printf '==> Running Doctor checks\n'
[[ -S "$SOCK" ]] || fail "daemon.sock missing"
kill -0 "$DAEMON_PID" 2>/dev/null || fail "daemon process not alive"
MODES_JSON="$(rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" modes)"
SCREEN_MODE="$(printf '%s' "$MODES_JSON" | jq -er '.screen')"
[[ "$SCREEN_MODE" == "alternate" ]] || fail "expected alternate screen mode, got $SCREEN_MODE"
SIZE_JSON="$(rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" size)"
PROC_JSON="$(rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" process)"
INBOX_COUNT="$("$REX_WAVE_BIN" --socket "$SOCK" --json inbox --limit 200 | jq -er '.value.unread')"
(( INBOX_COUNT >= 58 )) || fail "doctor expected >=58 unread cards, got $INBOX_COUNT"

jq -n \
	--arg run_id "$RUN_ID" \
	--arg session_id "$REX_VERIFY_SESSION" \
	--arg block_id "$REX_VERIFY_BLOCK" \
	--arg socket "$SOCK" \
	--argjson daemon_pid "$DAEMON_PID" \
	--argjson inbox_unread "$INBOX_COUNT" \
	--argjson modes "$MODES_JSON" \
	--argjson size "$SIZE_JSON" \
	--argjson process "$PROC_JSON" \
	'{
		ok: true,
		run_id: $run_id,
		session_id: $session_id,
		block_id: $block_id,
		socket: $socket,
		daemon_pid: $daemon_pid,
		inbox_unread: $inbox_unread,
		screen: $modes.screen,
		size: { columns: $size.columns, rows: $size.rows },
		foreground_pid: $process.foreground.pid,
		foreground_name: $process.foreground.name
	}' > "$EVIDENCE_DIR/doctor.json"

record_step "inbox-initial"
INITIAL_SCREEN="$(cat "$STEPS_DIR/01-inbox-initial.screen.txt")"
assert_contains "$INITIAL_SCREEN" "● connected" "connected status pill in top chrome"
assert_contains "$INITIAL_SCREEN" "[claude]" "shortened claude provider badge in sidebar"
assert_contains "$INITIAL_SCREEN" "auth-platform" "human BB project name for proj_alpha in sidebar"
assert_contains "$INITIAL_SCREEN" "search-engine" "human BB project name for proj_beta in sidebar"
assert_contains "$INITIAL_SCREEN" "▸ ✕ release-v1" "finished wave auto-folded on first sight with error glyph"
assert_not_contains "$INITIAL_SCREEN" "clean-bot" "auto-folded finished wave hides clean-bot worker"
assert_not_contains "$INITIAL_SCREEN" "crash-bot" "auto-folded finished wave hides crash-bot worker"
INITIAL_OSC_STATE="$(jq -er '.records[0].state' "$STEPS_DIR/01-inbox-initial.program_status.json")"
INITIAL_OSC_KIND="$(jq -er '.records[0].kind' "$STEPS_DIR/01-inbox-initial.program_status.json")"
INITIAL_OSC_TITLE="$(jq -er '.records[0].title // empty' "$STEPS_DIR/01-inbox-initial.program_status.json")"
INITIAL_OSC_MSG="$(jq -er '(.records[0].msg // .records[0].message) // empty' "$STEPS_DIR/01-inbox-initial.program_status.json")"
[[ "$INITIAL_OSC_STATE" == "blocked" && "$INITIAL_OSC_KIND" == "permission" ]] \
	|| fail "expected initial OSC 7501 state=blocked kind=permission, got state=$INITIAL_OSC_STATE kind=$INITIAL_OSC_KIND"
[[ "$INITIAL_OSC_TITLE" == "#wave-auth-v2" && -n "$INITIAL_OSC_MSG" ]] \
	|| fail "expected base64-decoded OSC 7501 title='#wave-auth-v2' and non-empty msg, got title='$INITIAL_OSC_TITLE' msg='$INITIAL_OSC_MSG'"

printf '==> Journey 1: Inbox 55+ card scroll, [r] mark-read, [?] help overlay + GraphicsMode [v] cycle\n'
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" tab
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" shift+g
wait_screen_contains "shard-01 replay vector green" "Inbox scrolled to bottom card"
record_step "inbox-bottom"
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" k
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" g
wait_screen_contains "migrator waiting on interaction" "Inbox scrolled back to top card"

# Select second card (unread mention/DM) and mark read via [r] keyboard shortcut.
UNREAD_BEFORE="$("$REX_WAVE_BIN" --socket "$SOCK" --json inbox --limit 200 | jq -er '.value.unread')"
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
sleep 0.1
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" r
MARK_READ_DEADLINE=$(( $(now_ms) + 5000 ))
UNREAD_AFTER="$UNREAD_BEFORE"
while (( $(now_ms) < MARK_READ_DEADLINE )); do
	UNREAD_AFTER="$("$REX_WAVE_BIN" --socket "$SOCK" --json inbox --limit 200 | jq -er '.value.unread')"
	if (( UNREAD_AFTER < UNREAD_BEFORE )); then
		break
	fi
	sleep 0.08
done
(( UNREAD_AFTER < UNREAD_BEFORE )) \
	|| fail "expected [r] keyboard mark-read to reduce unread count below $UNREAD_BEFORE, got $UNREAD_AFTER"
wait_screen_contains "✓ " "Marked-read checkmark rendered on Inbox card"
record_step "inbox-marked-read"

# Open [?] keymap help overlay and cycle GraphicsMode [v] inside it.
rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" '?'
wait_screen_contains "Keys · rex-wave" "Keymap Help Overlay opened"
wait_screen_contains "Hybrid z=-1 + U+10EEEE" "GraphicsMode Hybrid shown in Help Overlay"
wait_screen_contains "mark card read" "[r] mark card read documented in Help Overlay"
wait_screen_contains "archive / unarchive node" "[z] archive documented in Help Overlay"
wait_screen_contains "show / hide archived" "[A] show archived documented in Help Overlay"
record_step "help-overlay-open"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" v
wait_screen_contains "z=-1 only" "GraphicsMode BackdropOnly"
record_step "graphics-backdrop-only"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" v
wait_screen_contains "U+10EEEE only" "GraphicsMode PlaceholderOnly"
record_step "graphics-placeholder-only"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" v
wait_screen_contains "Pure Cells" "GraphicsMode CellsOnly"
record_step "graphics-pure-cells"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" v
wait_screen_contains "Hybrid z=-1 + U+10EEEE" "GraphicsMode Hybrid restored"
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" esc
wait_screen_not_contains "Keys · rex-wave" "Keymap Help Overlay closed"

# Resize and toggle-hide sidebar via > / < / \.
rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" '>'
rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" '<'
rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" '\'
wait_screen_not_contains "Org Tree" "Sidebar hidden via backslash toggle"
record_step "sidebar-hidden"
rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" '\'
wait_screen_contains "Org Tree" "Sidebar restored via backslash toggle"
record_step "sidebar-restored"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" tab

printf '==> Journey 2: Kanban board navigation, card move [m], and composer new task [i]\n'
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
wait_screen_contains "Kanban · auth-platform" "Kanban stage loaded with human project name"
record_step "kanban-initial"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter
sleep 0.1
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" m
sleep 0.15
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" i
rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" 'Verify Rex CUA Kanban card'
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter
wait_screen_contains "#5 Verify Rex CUA" "New Kanban task rendered on stage"
record_step "kanban-mutated"

BOARD_JSON="$("$REX_WAVE_BIN" --socket "$SOCK" --json task list --board project:proj_alpha)"
printf '%s\n' "$BOARD_JSON" > "$EVIDENCE_DIR/kanban-board.json"
DOING_HAS_CARD1="$(printf '%s' "$BOARD_JSON" | jq -er '[.value.lanes[] | select(.lane=="doing") | .cards[].no] | index(1) != null')"
[[ "$DOING_HAS_CARD1" == "true" ]] || fail "expected card #1 to move to doing lane in journal"
TODO_HAS_NEW="$(printf '%s' "$BOARD_JSON" | jq -er '[.value.lanes[] | select(.lane=="todo") | .cards[].title] | index("Verify Rex CUA Kanban card") != null')"
[[ "$TODO_HAS_NEW" == "true" ]] || fail "expected new Kanban task persisted in journal"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" tab

printf '==> Journey 3: Sidebar Org-Tree fold/unfold [space]\n'
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
sleep 0.1
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" space
wait_screen_contains "▸ Projects" "Projects branch collapsed"
wait_screen_not_contains "auth-platform" "auth-platform hidden while collapsed"
record_step "sidebar-projects-folded"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" space
wait_screen_contains "▾ Projects" "Projects branch expanded"
wait_screen_contains "auth-platform" "auth-platform visible after unfold"
record_step "sidebar-projects-unfolded"

printf '==> Journey 4: Wave room wrapped markdown, collapsed brief, multiline paste/newline compose, pager, and promote [P]\n'
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter
wait_screen_contains "Room · #auth-platform/wave-auth-v2" "Wave room stage loaded with human project name"
wait_screen_contains "brief for @author-jwt [Enter to expand]" "WaveConductor brief collapsed to 1-line summary"
record_step "wave-room-before-post"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" i
rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" $'\e[200~# Council Review\n@author-jwt Rex CUA live check passed with **bold** and `verify_jwt()`.\nSecond wrapped line rendered without single-line clipping.\e[201~'
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" ctrl+j
rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" 'Third line via ctrl+j newline.'
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" shift+enter
rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" 'Fourth line via shift+enter.'
record_step "wave-room-multiline-composer"
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter
wait_screen_contains "Council Review" "Markdown heading rendered in wave room"
wait_screen_contains "Second wrapped line rendered without single-line clipping." "Wrapped second line rendered in wave room"
wait_screen_contains "Third line via ctrl+j newline." "Third line rendered in wave room"
wait_screen_contains "Fourth line via shift+enter." "Fourth line via shift+enter rendered without premature submit"
record_step "wave-room-multiline-wrapped"

# Select message with k and open full-message Pager overlay with enter, then close with esc.
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" k
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter
wait_screen_contains "esc/q/? close" "Full-message Pager overlay opened"
record_step "wave-room-pager"
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" esc
wait_screen_not_contains "esc/q/? close" "Full-message Pager overlay closed"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" shift+p
wait_screen_contains "Banked " "Room messages promoted to canon letter"
record_step "wave-room-after-post-and-promote"

LETTER_FILE="$(find "$SANDBOX/to-owner" -maxdepth 1 -type f -name '*.md' | head -n 1)"
[[ -n "$LETTER_FILE" && -s "$LETTER_FILE" ]] || fail "expected banked canon letter in $SANDBOX/to-owner"
cp "$LETTER_FILE" "$EVIDENCE_DIR/promoted-letter.md"
assert_contains "$(cat "$EVIDENCE_DIR/promoted-letter.md")" "Rex CUA live check passed" "banked canon letter body"
assert_contains "$(cat "$EVIDENCE_DIR/promoted-letter.md")" "Third line via ctrl+j newline." "multiline newline preserved in banked letter"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" tab

printf '==> Journey 5: Agent Inspector context bar + blocked interaction approval [a]\n'
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
wait_screen_contains "Agent Inspector · @auth-v2/author-jwt" "Agent Inspector for author-jwt"
wait_screen_contains "42.0k / 200.0k (21%)" "Agent Inspector token usage"
record_step "agent-inspector-working"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
wait_screen_contains "int_mig_1" "Blocked interaction int_mig_1 in Agent Inspector"
record_step "agent-inspector-blocked"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" enter
sleep 0.1
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" a
RESOLVE_DEADLINE=$(( $(now_ms) + 5000 ))
while (( $(now_ms) < RESOLVE_DEADLINE )); do
	if [[ "$(jq -r '.resolves | length' "$MOCK_BB_STATE" 2>/dev/null || echo 0)" -ge 1 ]]; then
		break
	fi
	sleep 0.08
done
[[ "$(jq -r '.resolves[0].interaction_id // empty' "$MOCK_BB_STATE")" == "int_mig_1" ]] \
	|| fail "expected mock BB bridge to receive allow_once resolution for int_mig_1"
OSC_WORKING_DEADLINE=$(( $(now_ms) + 5000 ))
while (( $(now_ms) < OSC_WORKING_DEADLINE )); do
	CUR_OSC="$(rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" program_status)"
	if [[ "$(printf '%s' "$CUR_OSC" | jq -r '.records[0].state // empty')" == "working" ]]; then
		break
	fi
	sleep 0.08
done
record_step "agent-inspector-resolved"
RESOLVED_OSC_FILE="$(ls -1 "$STEPS_DIR"/*-agent-inspector-resolved.program_status.json | tail -n 1)"
RESOLVED_OSC_STATE="$(jq -er '.records[0].state' "$RESOLVED_OSC_FILE")"
RESOLVED_OSC_TITLE="$(jq -er '.records[0].title // empty' "$RESOLVED_OSC_FILE")"
[[ "$RESOLVED_OSC_STATE" == "working" && "$RESOLVED_OSC_TITLE" == "#wave-auth-v2" ]] \
	|| fail "expected OSC 7501 state=working title='#wave-auth-v2' after interaction approval, got state=$RESOLVED_OSC_STATE title=$RESOLVED_OSC_TITLE"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" tab

printf '==> Journey 6: Human project summary, retired-clean/errored glyphs, auto-fold unfold, and [z]/[A] archive round-trip\n'
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
wait_screen_contains "Project · search-engine" "Project summary header renders human BB project name"
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
wait_screen_contains "Room · #search-engine/wave-release-v1" "Finished wave room loaded on stage"
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" space
wait_screen_contains "✓ clean-bot [codex]" "Retired-clean worker renders checkmark glyph"
wait_screen_contains "✕ crash-bot [claude]" "Retired-errored worker renders error glyph"
record_step "finished-wave-unfolded"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
wait_screen_contains "✓ RETIRED" "Agent Inspector shows clean retired status for clean-bot"
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
wait_screen_contains "✕ RETIRED (ERRORED)" "Agent Inspector shows errored retired status for crash-bot"
wait_screen_contains "Insufficient balance (402)" "Agent Inspector preserves worker error detail"
record_step "retired-errored-inspector"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" k
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" k
wait_screen_contains "Room · #search-engine/wave-release-v1" "Returned to release-v1 wave node"
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" z
wait_screen_not_contains "release-v1" "Archived wave hidden from sidebar and stage"
ARCH_STATE="$("$REX_WAVE_BIN" --socket "$SOCK" --json call sidebar '{}' | jq -r '.value.projects[] | select(.project=="proj_beta") | .waves[] | select(.wave=="release-v1") | .archived')"
[[ "$ARCH_STATE" == "true" ]] || fail "expected release-v1 archived=true in sidebar snapshot, got $ARCH_STATE"
record_step "wave-archived-hidden"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" shift+a
wait_screen_contains "Org Tree [archived]" "Sidebar header displays active archived filter"
wait_screen_contains "release-v1" "Archived wave reappears when show_archived is toggled ON"
wait_screen_contains "[archived]" "Archived wave displays [archived] tag"
record_step "wave-show-archived"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" j
sleep 0.1
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" z
UNARCH_DEADLINE=$(( $(now_ms) + 5000 ))
UNARCH_STATE="true"
while (( $(now_ms) < UNARCH_DEADLINE )); do
	UNARCH_STATE="$("$REX_WAVE_BIN" --socket "$SOCK" --json call sidebar '{}' | jq -r '.value.projects[] | select(.project=="proj_beta") | .waves[] | select(.wave=="release-v1") | .archived')"
	if [[ "$UNARCH_STATE" == "false" ]]; then
		break
	fi
	sleep 0.08
done
[[ "$UNARCH_STATE" == "false" ]] || fail "expected release-v1 archived=false after second z press, got $UNARCH_STATE"
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" shift+a
wait_screen_not_contains "Org Tree [archived]" "Sidebar archived filter toggled OFF"
wait_screen_contains "release-v1" "Unarchived wave stays visible after hiding archived items"
record_step "wave-unarchived-visible"

printf '==> Journey 7: Live daemon disconnect -> reconnecting badge + stage error -> restart -> auto StageLoader recovery\n'
kill "$DAEMON_PID" 2>/dev/null || true
wait "$DAEMON_PID" 2>/dev/null || true
DAEMON_PID=""
wait_screen_contains "○ reconnecting" "Top-chrome badge transitions live to reconnecting on daemon drop"
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" shift+g
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" k
wait_screen_contains "Stage Error" "Stage enters error state when retargeted during daemon outage"
wait_screen_contains "reconnecting to daemon" "Stage error surfaces reconnecting transport cause"
record_step "daemon-reconnecting"

start_daemon
wait_screen_contains "● connected" "Top-chrome badge transitions back to connected on daemon restart"
wait_screen_contains "DM batch #1 from @sam" "StageLoader automatically recovers Direct Message stage with zero keypresses"
wait_screen_contains "All 32 alpha shards reported nominal p99." "Wrapped second line in recovered Direct Message stage"
record_step "daemon-reconnected"

printf '==> Journey 7b: Interactive Settings stage [,] + cycle runners.default_runner + config.toml sync\n'
rex send -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" ','
wait_screen_contains "Settings · Operator Configuration" "Settings stage opened via comma hotkey"
wait_screen_contains "runners.default_runner" "Settings stage displays runners.default_runner"
record_step "settings-initial"

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" space
sleep 0.15
rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" space
CFG_DEADLINE=$(( $(now_ms) + 5000 ))
CFG_DEFAULT=""
while (( $(now_ms) < CFG_DEADLINE )); do
	CFG_DEFAULT="$("$REX_WAVE_BIN" --socket "$SOCK" --json config show | jq -r '.value.config.runners.default_runner // empty')"
	if [[ "$CFG_DEFAULT" == "t3" ]]; then
		break
	fi
	sleep 0.08
done
[[ "$CFG_DEFAULT" == "t3" ]] || fail "expected runners.default_runner=t3 after cycling twice in Settings, got '$CFG_DEFAULT'"
"$REX_WAVE_BIN" --socket "$SOCK" --json config show > "$EVIDENCE_DIR/config-show.json"
[[ -s "$SCRATCH_DIR/config.toml" ]] || fail "expected $SCRATCH_DIR/config.toml to be written by Settings update"
assert_contains "$(cat "$SCRATCH_DIR/config.toml")" 'default_runner = "t3"' "config.toml synced with cycled default_runner"
record_step "settings-cycled"

printf '==> Journey 8: Clean TUI quit [q] and OSC 7501 clear\n'

rex send-key -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" q
QUIT_DEADLINE=$(( $(now_ms) + 5000 ))
EXITED_CLEAN=0
while (( $(now_ms) < QUIT_DEADLINE )); do
	AFTER_MODES="$(rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" modes)"
	AFTER_PROC="$(rex block call -s "$REX_VERIFY_SESSION" -b "$REX_VERIFY_BLOCK" process)"
	if [[ "$(printf '%s' "$AFTER_MODES" | jq -r '.screen')" == "primary" ]] \
		&& [[ "$(printf '%s' "$AFTER_PROC" | jq -r '.last_exit.exit_code // empty')" == "0" ]]; then
		EXITED_CLEAN=1
		break
	fi
	sleep 0.08
done
[[ "$EXITED_CLEAN" -eq 1 ]] || fail "expected rex-wave tui to leave alternate screen and exit 0 on q"
record_step "after-clean-quit"
QUIT_OSC_FILE="$(ls -1 "$STEPS_DIR"/*-after-clean-quit.program_status.json | tail -n 1)"
QUIT_OSC_LEN="$(jq -er '.records | length' "$QUIT_OSC_FILE")"
[[ "$QUIT_OSC_LEN" == "0" ]] || fail "expected OSC 7501 records cleared on quit, got $QUIT_OSC_LEN"

OSC_RECORDS_OBSERVED="$(jq -s '[.[].records | length] | add // 0' "$STEPS_DIR"/*.program_status.json)"

printf '==> Tearing down isolated Rex session and daemon\n'
SAVED_SESSION="$REX_VERIFY_SESSION"
SAVED_DAEMON_PID="$DAEMON_PID"
SAVED_MOCK_PID="$MOCK_BB_PID"
teardown_runtime

[[ ! -e "$SCRATCH_DIR" ]] || fail "scratch dir $SCRATCH_DIR was not removed"
! kill -0 "$SAVED_DAEMON_PID" 2>/dev/null || fail "daemon PID $SAVED_DAEMON_PID survived cleanup"
! kill -0 "$SAVED_MOCK_PID" 2>/dev/null || fail "mock BB PID $SAVED_MOCK_PID survived cleanup"
SESSION_STILL_PRESENT="$(rex ls --json | jq --arg s "$SAVED_SESSION" '[.sessions[]? | select(.session_id == $s)] | length')"
[[ "$SESSION_STILL_PRESENT" == "0" ]] || fail "Rex session $SAVED_SESSION survived cleanup"

END_MS="$(now_ms)"
ELAPSED_MS=$(( END_MS - START_MS ))
SCREEN_COUNT="$(find "$STEPS_DIR" -maxdepth 1 -name '*.screen.txt' | wc -l | tr -d ' ')"
STATUS_COUNT="$(find "$STEPS_DIR" -maxdepth 1 -name '*.program_status.json' | wc -l | tr -d ' ')"

jq -n \
	--arg run_id "$RUN_ID" \
	--arg evidence_dir "$EVIDENCE_DIR" \
	--argjson elapsed_ms "$ELAPSED_MS" \
	--argjson steps_captured "$STEP_NO" \
	--argjson screen_files "$SCREEN_COUNT" \
	--argjson program_status_files "$STATUS_COUNT" \
	--argjson inbox_seeded "$INBOX_COUNT" \
	--argjson osc7501_records_observed "$OSC_RECORDS_OBSERVED" \
	'{
		ok: true,
		run_id: $run_id,
		evidence_dir: $evidence_dir,
		elapsed_ms: $elapsed_ms,
		steps_captured: $steps_captured,
		screen_snapshots: $screen_files,
		program_status_snapshots: $program_status_files,
		inbox_seeded: $inbox_seeded,
		osc7501_records_observed: $osc7501_records_observed,
		osc7501_note: "OSC 7501 blocked->working->clear verified live with base64-encoded UTF-8 title and msg fields",
		verified_cleanup: {
			scratch_removed: true,
			rex_session_killed: true,
			daemon_stopped: true,
			evidence_preserved: true
		}
	}' > "$EVIDENCE_DIR/summary.json"

[[ -s "$EVIDENCE_DIR/doctor.json" ]] || fail "doctor.json missing after cleanup"
[[ -s "$EVIDENCE_DIR/promoted-letter.md" ]] || fail "promoted-letter.md missing after cleanup"
[[ -s "$EVIDENCE_DIR/kanban-board.json" ]] || fail "kanban-board.json missing after cleanup"
[[ -s "$EVIDENCE_DIR/mock-bb-state.json" ]] || fail "mock-bb-state.json missing after cleanup"
(( SCREEN_COUNT >= 12 )) || fail "expected >=12 screen snapshots preserved, got $SCREEN_COUNT"

printf '\nverify-tui-rex.sh: PASS in %dms (%d step snapshots preserved in %s)\n' \
	"$ELAPSED_MS" "$STEP_NO" "$EVIDENCE_DIR"
