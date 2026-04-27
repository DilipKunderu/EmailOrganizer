#!/bin/bash
# scripts/sleep_wake_drill.sh
#
# Active validation that daemon.py's SLEEP_WAKE_GAP_SECONDS detector and
# _handle_sleep_wake() handler correctly recover from a long event-loop gap.
# Simulates a laptop sleep via SIGSTOP -> sleep -> SIGCONT against the agent.
#
# Why this script and not an inline shell call:
#   - The active drill must quarantine watchdog/remediator/deadman/canary
#     before SIGSTOP-ing the agent, otherwise remediator's wedged_procs policy
#     (see src/remediator.py _check_wedged_procs) will kickstart -k the agent
#     mid-drill and replace the PID.
#   - The drill must be idempotent: a previous interrupted run cannot leave
#     the daemon in T state, and a new run cannot stomp on an in-flight one.
#     Lockdir + EXIT trap make this safe under Ctrl-C / SIGTERM.
#
# Pass: error_log gains a SleepWakeDetected (info) row dated >= drill start.
# Fail: any of (lock taken, agent PID not running, no SleepWakeDetected row).
#
# Tradeoff note: the daemon also has an in-process heartbeat guard
# (HEARTBEAT_TIMEOUT_SECONDS=600) that will os._exit() if no _write_health
# fires for 10 min. With DRILL_STOP_SECONDS=480, the guard fires only if the
# last heartbeat happened >120s before the SIGSTOP. In that case the daemon
# restarts via launchd and SleepWakeDetected is never written -> DRILL: FAIL.
# That is a real signal: the gap detector did not get to run in-process.

set -u

# ---- Config ----
DRILL_STOP_SECONDS=480        # > daemon.SLEEP_WAKE_GAP_SECONDS (420), < HEARTBEAT_TIMEOUT_SECONDS (600)
POST_RESUME_SECONDS=45        # let the loop iterate past wait_for_mail and call _handle_sleep_wake
QUARANTINE_LABELS="watchdog remediator deadman canary"
LOCK_DIR=~/.emailorganizer/.sleep_wake_drill.lock
PLIST_DIR=~/Library/LaunchAgents
DB=~/.emailorganizer/state.db
LOG_DIR=~/.emailorganizer/logs
LOG="$LOG_DIR/sleep_wake_drill.log"

mkdir -p "$LOG_DIR"

log() {
  echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG" >&2
}

fail() {
  echo "DRILL: FAIL $*"
  log "FAIL $*"
  exit 1
}

# ---- Lock (atomic mkdir; portable on macOS without flock(1)) ----
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "DRILL: FAIL lock held by previous drill at $LOCK_DIR"
  exit 1
fi

# Track unloaded labels (space-separated, set -u-safe vs. empty arrays).
UNLOADED=""
AGENT_PID=""

cleanup() {
  local rc=$?
  log "cleanup running (rc=$rc)"
  if [ -n "$AGENT_PID" ]; then
    if kill -0 "$AGENT_PID" 2>/dev/null; then
      kill -CONT "$AGENT_PID" 2>/dev/null && log "kill -CONT $AGENT_PID issued in cleanup" || log "WARN: kill -CONT $AGENT_PID failed in cleanup"
    fi
  fi
  for label in $UNLOADED; do
    plist="$PLIST_DIR/com.emailorganizer.${label}.plist"
    if [ -f "$plist" ]; then
      if launchctl load "$plist" >/dev/null 2>&1; then
        log "reloaded $label"
      else
        log "FAILED to reload $label (plist: $plist)"
      fi
    fi
  done
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

log "drill starting (stop=${DRILL_STOP_SECONDS}s, post_resume=${POST_RESUME_SECONDS}s)"

# ---- Quarantine sidecars BEFORE resolving AGENT_PID ----
# (the remediator's 15-min cycle could kickstart -k the agent the moment
# we run, replacing the PID we'd otherwise capture)
log "quarantining sidecars: $QUARANTINE_LABELS"
for label in $QUARANTINE_LABELS; do
  plist="$PLIST_DIR/com.emailorganizer.${label}.plist"
  if [ ! -f "$plist" ]; then
    log "skip $label (plist not found at $plist)"
    continue
  fi
  if launchctl unload "$plist" 2>/dev/null; then
    UNLOADED="$UNLOADED $label"
    log "unloaded $label"
  else
    log "WARN: unload $label failed (already unloaded?); proceeding"
  fi
done

# Brief pause so any in-flight remediator cycle finishes before we capture PID
sleep 2

# ---- Resolve agent PID AFTER unload ----
AGENT_PID=$(launchctl list 2>/dev/null | awk '/com\.emailorganizer\.agent/ {print $1}')
if [ -z "$AGENT_PID" ] || [ "$AGENT_PID" = "-" ]; then
  fail "agent PID not running (launchctl list shows no live PID)"
fi
log "agent PID resolved: $AGENT_PID"

# ---- Snapshot drill start (used as the error_log query lower bound) ----
DRILL_START_ISO=$(date -u +%FT%TZ)
log "DRILL_START_ISO=$DRILL_START_ISO"

# ---- SIGSTOP for > 7 min to exceed SLEEP_WAKE_GAP_SECONDS ----
log "kill -STOP $AGENT_PID for ${DRILL_STOP_SECONDS}s"
if ! kill -STOP "$AGENT_PID"; then
  fail "kill -STOP $AGENT_PID failed (insufficient permissions or PID gone)"
fi

sleep "$DRILL_STOP_SECONDS"

log "kill -CONT $AGENT_PID"
if ! kill -CONT "$AGENT_PID"; then
  fail "kill -CONT $AGENT_PID failed (PID may have been replaced by launchd despite quarantine)"
fi

# ---- Wait for daemon loop to advance past wait_for_mail() and trigger _handle_sleep_wake ----
log "waiting ${POST_RESUME_SECONDS}s for daemon to register the gap"
sleep "$POST_RESUME_SECONDS"

# ---- Pass query: SleepWakeDetected row since DRILL_START_ISO ----
COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM error_log WHERE error_class='SleepWakeDetected' AND severity='info' AND ts >= '$DRILL_START_ISO';")
log "SleepWakeDetected rows since $DRILL_START_ISO: $COUNT"

if [ "${COUNT:-0}" -ge 1 ]; then
  ROW=$(sqlite3 "$DB" "SELECT ts || ' | ' || message FROM error_log WHERE error_class='SleepWakeDetected' AND severity='info' AND ts >= '$DRILL_START_ISO' ORDER BY ts DESC LIMIT 1;")
  log "row: $ROW"
  echo "DRILL: PASS"
  exit 0
fi

# Diagnostic: did the heartbeat guard fire instead? (also info severity, but
# means the daemon got restarted by launchd before the gap detector ran)
HB_COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM error_log WHERE error_class='HeartbeatRestart' AND ts >= '$DRILL_START_ISO';")
if [ "${HB_COUNT:-0}" -ge 1 ]; then
  fail "gap detector did not fire; heartbeat-guard restart fired instead (HeartbeatRestart count=$HB_COUNT). Daemon was restarted via launchd before _handle_sleep_wake could run."
fi

fail "no SleepWakeDetected row since $DRILL_START_ISO; gap detector did not fire and no heartbeat-guard restart either. Inspect ~/.emailorganizer/logs/agent-stderr.log."
