#!/usr/bin/env bash
# tests/heavy/sync_lock_regression.sh
# Sync writer-lock concurrency regression test.
#
# Asserts the actual contract of performSync (src/commands/sync.ts):
#   - it takes `gbrain-sync` from gbrain_cycle_locks via tryAcquireDbLock
#     (an UPSERT whose ON CONFLICT only supersedes an EXPIRED holder);
#   - on contention it fails FAST with "Another sync is in progress"
#     — it does not queue and wait;
#   - the lock row is released on exit (no leaks).
#
# Two phases, deliberately different in strength:
#
#   Phase 1 (hard assertion, deterministic) — we hold the lock row ourselves
#   with a far-future TTL, then run one sync. The UPSERT's ON CONFLICT WHERE
#   fails, tryAcquireDbLock returns null, and the sync must fail fast. Then we
#   release and the same sync must succeed.
#
#   Phase 2 (informational smoke) — spawn N parallel syncs. Simultaneity is NOT
#   guaranteed: `bun run` startup staggers the processes by a few hundred ms,
#   so a fast winner can release before a slow starter reaches the UPSERT. That
#   makes exact winner/loser counts a property of the machine, not of the lock.
#   We therefore assert only what the lock actually guarantees here (no sync
#   failed for a non-lock reason, at least one sync completed, no leaked rows)
#   and print the counts.
#
# History: the original script asserted "exactly 1 winner, N-1 losers" from the
# parallel phase alone and went red on 2026-10-28 with winners=2 losers=2 — the
# processes had not all overlapped. The eng-review-flagged v1 plan had also
# asserted the wrong semantics ("N-1 wait then complete one at a time") and
# snapshotted the wrong table (`pg_locks`); both reviewers caught it.
#
# Postgres-only (no DATABASE_URL = graceful skip with hint).

set -euo pipefail

cd "$(dirname "$0")/../.."

if [ -z "${DATABASE_URL:-}" ]; then
  echo "[sync_lock_regression] DATABASE_URL not set; skipping (informational)." >&2
  echo "  Local: docker run -d --name gbrain-test-pg -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=gbrain_test -p 5434:5432 pgvector/pgvector:pg16" >&2
  echo "  Then: export DATABASE_URL=postgresql://postgres:postgres@localhost:5434/gbrain_test" >&2
  exit 0
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "[sync_lock_regression] psql required. Install postgresql-client." >&2
  exit 2
fi

TS=$(date -u +%Y%m%d-%H%M%SZ)
# Isolate from the developer's real ~/.gbrain so writing sync.repo_path doesn't
# clobber their config. Restored on exit.
TMP_GBRAIN_HOME=$(mktemp -d -t gbrain-sync-lock-home-XXXXXX)
export GBRAIN_HOME="$TMP_GBRAIN_HOME"
LOG_DIR="$GBRAIN_HOME/audit"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/heavy-sync_lock_regression-$TS.log"
# Surface the log path so it survives the EXIT trap that nukes GBRAIN_HOME.
SURFACE_LOG="${TMPDIR:-/tmp}/heavy-sync_lock_regression-$TS.log"
trap 'rm -rf "$TMP_GBRAIN_HOME"; cp -f "$LOG" "$SURFACE_LOG" 2>/dev/null || true' EXIT

NUM_PARALLEL="${NUM_PARALLEL:-4}"
# Far-future TTL: the UPSERT only supersedes a holder whose ttl_expires_at < NOW().
HOLD_UNTIL="2030-01-01 00:00:00+00"

echo "[sync_lock_regression] DATABASE_URL=$DATABASE_URL"
echo "[sync_lock_regression] log=$LOG"

FAIL=0

# Step 1: ensure schema is up-to-date by running doctor once
echo "[sync_lock_regression] init schema via gbrain doctor..." | tee -a "$LOG"
timeout 180s bun run src/cli.ts doctor --json > /dev/null 2>>"$LOG"

# Step 2: create a tiny brain dir + register it as sync.repo_path so each sync
# call has something legitimate to do.
BRAIN_DIR=$(mktemp -d -t gbrain-sync-lock-XXXXXX)
# Compose with the earlier GBRAIN_HOME-cleanup trap (NOT overwrite it).
trap 'rm -rf "$BRAIN_DIR" "$TMP_GBRAIN_HOME"; cp -f "$LOG" "$SURFACE_LOG" 2>/dev/null || true' EXIT

mkdir -p "$BRAIN_DIR"
cat > "$BRAIN_DIR/page-a.md" <<'EOF'
---
title: Lock Test Page A
---
# Lock Test Page A
Trivial content for sync-lock-regression heavy test.
EOF
cat > "$BRAIN_DIR/page-b.md" <<'EOF'
---
title: Lock Test Page B
---
# Lock Test Page B
Trivial content for sync-lock-regression heavy test.
EOF

# git-init so sync's diff-walk has something to anchor (sync expects a git repo)
(cd "$BRAIN_DIR" && git init -q && git add . && git -c user.email=test@test -c user.name=test commit -q -m "seed" >/dev/null 2>&1) || true

bun run src/cli.ts config set sync.repo_path "$BRAIN_DIR" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# Phase 1 — deterministic: we hold the lock, one sync must fail fast.
# ---------------------------------------------------------------------------
echo "[sync_lock_regression] phase 1: hold lock row (ttl=$HOLD_UNTIL), expect one fast failure" | tee -a "$LOG"

psql "$DATABASE_URL" -q -c "INSERT INTO gbrain_cycle_locks (id, holder_pid, holder_host, acquired_at, ttl_expires_at) VALUES ('gbrain-sync', 1, 'heavy-test', NOW(), '$HOLD_UNTIL');" >>"$LOG" 2>&1

HELD_OUT=$(mktemp -t sync-lock-held-XXXXXX)
HELD_RC=0
timeout 180s bun run src/cli.ts sync --dir "$BRAIN_DIR" >"$HELD_OUT" 2>&1 || HELD_RC=$?
echo "  [held] rc=$HELD_RC" | tee -a "$LOG"

if grep -q "Another sync is in progress" "$HELD_OUT"; then
  echo "  [held] lock-busy: 'Another sync is in progress' (fast, no queue) — OK" | tee -a "$LOG"
else
  echo "[sync_lock_regression] FAIL: with the lock held, sync did not report lock-busy (rc=$HELD_RC)" >&2
  head -5 "$HELD_OUT" 2>/dev/null | sed 's/^/    > /' | tee -a "$LOG"
  FAIL=1
fi

# A queueing implementation would exit 0 after waiting; the contract is fail-fast.
if [ "$HELD_RC" = "0" ]; then
  echo "[sync_lock_regression] FAIL: sync exited 0 while the lock was held — it queued instead of failing fast" >&2
  FAIL=1
fi

# Release our hold and confirm the same sync now succeeds.
psql "$DATABASE_URL" -q -c "DELETE FROM gbrain_cycle_locks WHERE id = 'gbrain-sync';" >>"$LOG" 2>&1
FREE_OUT=$(mktemp -t sync-lock-free-XXXXXX)
FREE_RC=0
timeout 180s bun run src/cli.ts sync --dir "$BRAIN_DIR" >"$FREE_OUT" 2>&1 || FREE_RC=$?
echo "  [free] rc=$FREE_RC (lock released)" | tee -a "$LOG"
if [ "$FREE_RC" != "0" ]; then
  echo "[sync_lock_regression] FAIL: sync did not succeed after the lock was released (rc=$FREE_RC)" >&2
  head -5 "$FREE_OUT" 2>/dev/null | sed 's/^/    > /' | tee -a "$LOG"
  FAIL=1
fi

rm -f "$HELD_OUT" "$FREE_OUT"

# ---------------------------------------------------------------------------
# Phase 2 — informational smoke: N parallel syncs. Simultaneity is not
# guaranteed, so only assert what the lock guarantees unconditionally.
# ---------------------------------------------------------------------------
echo "[sync_lock_regression] phase 2: spawning $NUM_PARALLEL parallel sync processes..." | tee -a "$LOG"

PIDS=()
EXIT_FILES=()
OUT_FILES=()
for ((i=1; i<=NUM_PARALLEL; i+=1)); do
  EXIT_F=$(mktemp -t sync-lock-exit-XXXXXX)
  OUT_F=$(mktemp -t sync-lock-out-XXXXXX)
  EXIT_FILES+=("$EXIT_F")
  OUT_FILES+=("$OUT_F")
  ( bun run src/cli.ts sync --dir "$BRAIN_DIR" >"$OUT_F" 2>&1; echo $? > "$EXIT_F" ) &
  PIDS+=($!)
done

echo "[sync_lock_regression] waiting on ${#PIDS[@]} pids..."
for pid in "${PIDS[@]}"; do
  wait "$pid" 2>/dev/null || true
done

WINNERS=0
LOSERS=0
UNKNOWN=0
for ((i=0; i<NUM_PARALLEL; i+=1)); do
  rc=$(cat "${EXIT_FILES[$i]}" 2>/dev/null || echo "?")
  out_file="${OUT_FILES[$i]}"
  if [ "$rc" = "0" ]; then
    WINNERS=$((WINNERS + 1))
    echo "  [sync $((i+1))] rc=0 (winner)" | tee -a "$LOG"
  elif grep -q "Another sync is in progress" "$out_file" 2>/dev/null; then
    LOSERS=$((LOSERS + 1))
    echo "  [sync $((i+1))] rc=$rc (lock-busy: 'Another sync is in progress')" | tee -a "$LOG"
  else
    UNKNOWN=$((UNKNOWN + 1))
    echo "  [sync $((i+1))] rc=$rc (unknown failure — see $out_file)" | tee -a "$LOG"
    head -5 "$out_file" 2>/dev/null | sed 's/^/    > /' | tee -a "$LOG"
  fi
done

rm -f "${EXIT_FILES[@]}" "${OUT_FILES[@]}"

echo "[sync_lock_regression] outcomes: winners=$WINNERS losers=$LOSERS unknown=$UNKNOWN" | tee -a "$LOG"
echo "[sync_lock_regression] (counts are informational — bun startup staggers the processes," | tee -a "$LOG"
echo "[sync_lock_regression]  so a fast winner can release before a slow starter reaches the UPSERT)" | tee -a "$LOG"

# Assert no leaked gbrain_cycle_locks rows. The pkey column is `id`,
# not `lock_id` (column name confirmed via \d gbrain_cycle_locks).
LEAKED=$(psql "$DATABASE_URL" -t -A -c "SELECT COUNT(*) FROM gbrain_cycle_locks WHERE id = 'gbrain-sync';" 2>>"$LOG" | tr -d ' ')
echo "[sync_lock_regression] post-run gbrain_cycle_locks(gbrain-sync) row count: $LEAKED" | tee -a "$LOG"

# Phase 2 verdict — only unconditional guarantees.
if [ "$UNKNOWN" -ne 0 ]; then
  echo "[sync_lock_regression] FAIL: $UNKNOWN sync(s) failed for a reason other than the lock — that taints the measurement" >&2
  FAIL=1
fi

if [ "$WINNERS" -lt 1 ]; then
  echo "[sync_lock_regression] FAIL: no sync completed (winners=0) — we cannot tell whether the lock mattered" >&2
  FAIL=1
fi

if [ "$LEAKED" != "0" ]; then
  echo "[sync_lock_regression] FAIL: $LEAKED leaked gbrain_cycle_locks(gbrain-sync) row(s) after all syncs exited" >&2
  FAIL=1
fi

if [ "$FAIL" -ne 0 ]; then
  echo "[sync_lock_regression] FAILED. See $LOG for details." >&2
  exit 1
fi

echo "[sync_lock_regression] OK — lock blocks a contender (phase 1), $LOSERS lock-busy loser(s) / $WINNERS winner(s) across $NUM_PARALLEL parallel syncs (phase 2), no leaked lock rows."
