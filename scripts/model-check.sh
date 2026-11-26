#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TLA_VERSION="1.7.4"
TLA_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"

java_works() {
	[[ -x "$1" ]] && "$1" -version >/dev/null 2>&1
}

resolve_java() {
	if [[ -n "${JAVA_BIN:-}" ]] && java_works "${JAVA_BIN}"; then
		printf '%s\n' "${JAVA_BIN}"
		return 0
	fi
	if [[ -n "${JAVA_HOME:-}" ]] && java_works "${JAVA_HOME}/bin/java"; then
		printf '%s\n' "${JAVA_HOME}/bin/java"
		return 0
	fi
	local candidate
	if candidate="$(command -v java 2>/dev/null)" && java_works "$candidate"; then
		printf '%s\n' "$candidate"
		return 0
	fi
	for candidate in \
		"/opt/homebrew/opt/openjdk/bin/java" \
		"/opt/homebrew/opt/openjdk@21/bin/java" \
		"/opt/homebrew/opt/openjdk@17/bin/java" \
		"/usr/local/opt/openjdk/bin/java" \
		"/usr/bin/java"; do
		if java_works "$candidate"; then
			printf '%s\n' "$candidate"
			return 0
		fi
	done
	return 1
}

resolve_tla_jar() {
	local candidate
	for candidate in \
		"${TLA2TOOLS_JAR:-}" \
		"${HOME}/.local/lib/tlaplus-${TLA_VERSION}/tla2tools.jar" \
		"${XDG_CACHE_HOME:-${HOME}/.cache}/rex-wave/tlaplus/tla2tools-${TLA_VERSION}.jar"; do
		if [[ -n "$candidate" && -f "$candidate" ]]; then
			printf '%s\n' "$candidate"
			return 0
		fi
	done

	if [[ "${REX_WAVE_FETCH_TLA:-0}" == "1" ]]; then
		local cache_dir="${XDG_CACHE_HOME:-${HOME}/.cache}/rex-wave/tlaplus"
		local dest="${cache_dir}/tla2tools-${TLA_VERSION}.jar"
		local tmp="${dest}.tmp.$$"
		mkdir -p "$cache_dir"
		curl -fsSL \
			"https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar" \
			-o "$tmp"
		python3 - "$tmp" "$TLA_SHA256" <<'PY'
import hashlib, os, sys
path, want = sys.argv[1], sys.argv[2]
with open(path, "rb") as fh:
    got = hashlib.sha256(fh.read()).hexdigest()
if want and got != want:
    os.unlink(path)
    sys.stderr.write(f"error: tla2tools.jar sha256 mismatch: expected {want}, got {got}\n")
    sys.exit(1)
PY
		mv -f "$tmp" "$dest"
		printf '%s\n' "$dest"
		return 0
	fi
	return 1
}

JAVA_EXE="$(resolve_java || true)"
if [[ -z "$JAVA_EXE" ]]; then
	printf 'error: java runtime not found (install OpenJDK or set JAVA_BIN)\n' >&2
	exit 1
fi

JAR_PATH="$(resolve_tla_jar || true)"
if [[ -z "$JAR_PATH" ]]; then
	printf 'error: tla2tools.jar not found (set TLA2TOOLS_JAR, install under ~/.local/lib/tlaplus-%s/tla2tools.jar, or set REX_WAVE_FETCH_TLA=1)\n' "$TLA_VERSION" >&2
	exit 1
fi

WORK_TMP="$(mktemp -d /tmp/rw-tlc.XXXXXX)"
cleanup() {
	rm -rf "$WORK_TMP"
}
trap cleanup EXIT

python3 - "$ROOT" "$JAVA_EXE" "$JAR_PATH" "$WORK_TMP" <<'PY'
import concurrent.futures
import os
import re
import shutil
import subprocess
import sys
import time

root, java_exe, jar_path, work_tmp = sys.argv[1:5]

# (spec_dir, module, cfg, expect_kind, expected_violation)
# expect_kind: "pass" | "witness" | "mutant"
CHECKS = [
    # 1. WaveLifecycle
    ("specs/waves", "MCWaveLifecycle.tla", "WL-none.cfg", "pass", None),
    ("specs/waves", "MCWaveLifecycle.tla", "WL-sat-NeverCompleted.cfg", "witness", "NeverCompleted"),
    ("specs/waves", "MCWaveLifecycle.tla", "WL-sat-NeverLateSpawnRetired.cfg", "witness", "NeverLateSpawnRetired"),
    ("specs/waves", "MCWaveLifecycle.tla", "WL-sat-NeverRespawnedAttempt2.cfg", "witness", "NeverRespawnedAttempt2"),
    ("specs/waves", "MCWaveLifecycle.tla", "WL-sat-NeverRefusedWaveFailed.cfg", "witness", "NeverRefusedWaveFailed"),
    ("specs/waves", "MCWaveLifecycle.tla", "WL-drop_late_spawn.cfg", "mutant", "Temporal properties were violated"),
    ("specs/waves", "MCWaveLifecycle.tla", "WL-unord_activity.cfg", "mutant", "MonotonicActivitySeq"),
    ("specs/waves", "MCWaveLifecycle.tla", "WL-stalled_retire_observe.cfg", "mutant", "Temporal properties were violated"),
    ("specs/waves", "MCWaveLifecycle.tla", "WL-errored_over_refused.cfg", "mutant", "RefusalStickyOverErrored"),
    ("specs/waves", "MCWaveLifecycle.tla", "WL-unfenced_thread_obs.cfg", "mutant", "StaleThreadNeverMutatesCurrentAttempt"),
    ("specs/waves", "MCWaveLifecycle.tla", "WL-single_slot_pre_spawn.cfg", "mutant", "SuppressedNeverLostOnPreSpawnCrossTalk"),
    ("specs/waves", "MCWaveLifecycle.tla", "WL-complete_with_errored.cfg", "mutant", "NoCompletedWaveWithFailedWorker"),

    # 2. PresenceLeases
    ("specs/presence", "MCPresenceLeases.tla", "PL-none.cfg", "pass", None),
    ("specs/presence", "MCPresenceLeases.tla", "PL-sat-NeverReboundAfterDisconnect.cfg", "witness", "NeverReboundAfterDisconnect"),
    ("specs/presence", "MCPresenceLeases.tla", "PL-sat-NeverHeartbeatedBeforeQueuedExpire.cfg", "witness", "NeverHeartbeatedBeforeQueuedExpire"),
    ("specs/presence", "MCPresenceLeases.tla", "PL-sat-NeverSupersededOnTakeover.cfg", "witness", "NeverSupersededOnTakeover"),
    ("specs/presence", "MCPresenceLeases.tla", "PL-no_epoch_bump_on_rebind.cfg", "mutant", "StaleDisconnectNeverKillsReboundIncarnation"),
    ("specs/presence", "MCPresenceLeases.tla", "PL-unfenced_unbind.cfg", "mutant", "StaleDisconnectNeverKillsReboundIncarnation"),
    ("specs/presence", "MCPresenceLeases.tla", "PL-unfenced_expire.cfg", "mutant", "StaleExpireNeverKillsHeartbeatedBinding"),
    ("specs/presence", "MCPresenceLeases.tla", "PL-allow_duplicate_lane.cfg", "mutant", "AtMostOneLivePerLane"),
    ("specs/presence", "MCPresenceLeases.tla", "PL-operator_bindable.cfg", "mutant", "OperatorSurfaceNeverBound"),

    # 3. MailHub
    ("specs/mail", "MCMailHub.tla", "MC-none.cfg", "pass", None),
    ("specs/mail", "MCMailHub.tla", "MC-sat-NeverRung.cfg", "witness", "NeverRung"),
    ("specs/mail", "MCMailHub.tla", "MC-sat-NeverRefused.cfg", "witness", "NeverRefused"),
    ("specs/mail", "MCMailHub.tla", "MC-sat-NeverForked.cfg", "witness", "NeverForked"),
    ("specs/mail", "MCMailHub.tla", "MC-no_key.cfg", "mutant", "NoDoubleRing"),
    ("specs/mail", "MCMailHub.tla", "MC-pinned_name.cfg", "mutant", "RightSeat"),
    ("specs/mail", "MCMailHub.tla", "MC-pinned-deaf.cfg", "mutant", "NoFalseGiveUp"),
    ("specs/mail", "MCMailHub.tla", "MC-silent_giveup.cfg", "mutant", "LoudGiveUp"),

    # 4. MailStore
    ("specs/mail", "MCMailStore.tla", "MS-none.cfg", "pass", None),
    ("specs/mail", "MCMailStore.tla", "MS-sat-NeverFlagged.cfg", "witness", "NeverFlagged"),
    ("specs/mail", "MCMailStore.tla", "MS-sat-NeverRebuiltAfterCrash.cfg", "witness", "NeverRebuiltAfterCrash"),
    ("specs/mail", "MCMailStore.tla", "MS-success_before_commit.cfg", "mutant", "DurableSend"),
    ("specs/mail", "MCMailStore.tla", "MS-no_reconcile.cfg", "mutant", "Temporal properties were violated"),
    ("specs/mail", "MCMailStore.tla", "MS-edit_reingests.cfg", "mutant", "EditNeverRingable"),
    ("specs/mail", "MCMailStore.tla", "MS-ack_not_mirrored.cfg", "mutant", "RebuildParity"),

    # 5. MailMachines
    ("specs/mail", "MCMailMachines.tla", "MM-none.cfg", "pass", None),
    ("specs/mail", "MCMailMachines.tla", "MM-sat-NeverDegraded.cfg", "witness", "NeverDegraded"),
    ("specs/mail", "MCMailMachines.tla", "MM-sat-NeverCrossMachine.cfg", "witness", "NeverCrossMachine"),
    ("specs/mail", "MCMailMachines.tla", "MM-ring_any_machine.cfg", "mutant", "OneMachineRings"),
    ("specs/mail", "MCMailMachines.tla", "MM-retry_unknown.cfg", "mutant", "NoDoubleRing"),
    ("specs/mail", "MCMailMachines.tla", "MM-no_degrade.cfg", "mutant", "DegradedLoud"),
    ("specs/mail", "MCMailMachines.tla", "MM-sync_overwrite.cfg", "mutant", "NoLostLetter"),
    ("specs/mail", "MCMailMachines.tla", "MM-ring_after_ack.cfg", "mutant", "NoRingAfterAck"),
]

states_re = re.compile(r"(\d[\d,]*)\s+states generated,\s+(\d[\d,]*)\s+distinct states found")

def run_one(idx, entry):
    spec_dir, module, cfg, kind, expected = entry
    run_dir = os.path.join(work_tmp, f"run-{idx}")
    meta_dir = os.path.join(run_dir, "meta")
    java_tmp = os.path.join(run_dir, "tmp")
    os.makedirs(meta_dir, exist_ok=True)
    os.makedirs(java_tmp, exist_ok=True)
    src_dir = os.path.join(root, spec_dir)
    for fname in os.listdir(src_dir):
        if fname.endswith((".tla", ".cfg")):
            shutil.copy2(os.path.join(src_dir, fname), os.path.join(run_dir, fname))

    cmd = [
        java_exe,
        f"-Djava.io.tmpdir={java_tmp}",
        "-XX:+UseParallelGC",
        "-Xmx512m",
        "-cp",
        jar_path,
        "tlc2.TLC",
        "-workers",
        "1",
        "-cleanup",
        "-metadir",
        meta_dir,
        "-config",
        cfg,
        module,
    ]
    # MailHub / MailStore / MailMachines intentionally reach finite terminal states.
    if spec_dir == "specs/mail":
        cmd.insert(cmd.index("-cleanup"), "-deadlock")

    t0 = time.monotonic()
    proc = subprocess.run(
        cmd,
        cwd=run_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    dt = time.monotonic() - t0
    out = proc.stdout
    m_st = states_re.search(out)
    distinct = int(m_st.group(2).replace(",", "")) if m_st else 0

    ok = False
    reason = ""
    if kind == "pass":
        if proc.returncode == 0 and "Model checking completed. No error has been found." in out and distinct > 1:
            ok = True
            reason = f"{distinct:,} distinct states"
        else:
            reason = f"expected clean pass with >1 distinct states (rc={proc.returncode}, distinct={distinct})"
    else:
        if expected == "Temporal properties were violated":
            if proc.returncode == 13 and "Error: Temporal properties were violated." in out:
                ok = True
                reason = "caught temporal property violation"
            else:
                reason = f"expected TLC temporal violation rc=13 (rc={proc.returncode})"
        else:
            inv_banner = f"Error: Invariant {expected} is violated."
            if proc.returncode in (11, 12) and inv_banner in out:
                ok = True
                reason = f"caught {expected}"
            else:
                reason = f"expected TLC invariant violation '{inv_banner}' rc=11|12 (rc={proc.returncode})"

    return {
        "idx": idx,
        "label": f"{spec_dir}/{cfg}",
        "kind": kind,
        "ok": ok,
        "dt": dt,
        "reason": reason,
        "output": out,
    }

t_start = time.monotonic()
workers = min(8, os.cpu_count() or 4)
print(f"==> TLC proof-firewall gate ({len(CHECKS)} checks across 5 models, workers={workers})")

results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
    futs = [pool.submit(run_one, i, item) for i, item in enumerate(CHECKS)]
    for fut in concurrent.futures.as_completed(futs):
        results.append(fut.result())

results.sort(key=lambda r: r["idx"])
failures = [r for r in results if not r["ok"]]

for r in results:
    if r["kind"] == "pass" or not r["ok"]:
        tag = "ok" if r["ok"] else "FAIL"
        print(f"  [{tag}] {r['label']} ({r['reason']}, {r['dt']:.2f}s)")

if failures:
    for f in failures:
        sys.stderr.write(f"\n--- [FAIL] {f['label']} ({f['reason']}) ---\n{f['output']}\n")
    sys.stderr.write(f"\nmodel-check failed: {len(failures)} of {len(CHECKS)} TLC checks failed.\n")
    sys.exit(1)

passes = sum(1 for r in results if r["kind"] == "pass")
witnesses = sum(1 for r in results if r["kind"] == "witness")
mutants = sum(1 for r in results if r["kind"] == "mutant")
total_dt = time.monotonic() - t_start
print(
    f"  [ok] {passes} base models passed, {witnesses} anti-vacuity witnesses reached, "
    f"{mutants} planted mutants caught in {total_dt:.2f}s"
)
PY
