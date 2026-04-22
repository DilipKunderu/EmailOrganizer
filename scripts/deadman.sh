#!/bin/bash
# Dead-man's switch for the watchdog itself.
# Fires a macOS notification if the watchdog heartbeat file is older than threshold.
# Installed as com.emailorganizer.deadman launchd job (runs every 6h).
#
# Threshold: 1 hour. Watchdog runs every 15 min so 1h = 4 missed cycles = clearly wedged.

set -u

HEARTBEAT=~/.emailorganizer/watchdog_heartbeat
THRESHOLD_SECONDS=3600

if [ ! -f "$HEARTBEAT" ]; then
  MSG="Watchdog heartbeat file missing at $HEARTBEAT. Watchdog may have never started."
  osascript -e "display notification \"$MSG\" with title \"EmailOrganizer FATAL\" sound name \"Basso\"" >/dev/null 2>&1 || true
  # Write a breadcrumb so we know the dead-man fired
  echo "$(date -u +%FT%TZ) DEADMAN: $MSG" >>~/.emailorganizer/logs/deadman.log
  exit 1
fi

MTIME=$(stat -f %m "$HEARTBEAT" 2>/dev/null || echo 0)
NOW=$(date +%s)
AGE=$(( NOW - MTIME ))

if [ "$AGE" -gt "$THRESHOLD_SECONDS" ]; then
  MSG="Watchdog heartbeat is stale (${AGE}s old, threshold=${THRESHOLD_SECONDS}s). Watchdog is dead. Check launchctl list | grep emailorganizer."
  osascript -e "display notification \"$MSG\" with title \"EmailOrganizer FATAL\" sound name \"Basso\"" >/dev/null 2>&1 || true
  echo "$(date -u +%FT%TZ) DEADMAN: $MSG" >>~/.emailorganizer/logs/deadman.log
  exit 1
fi

# Healthy; log a heartbeat of our own sporadically (size-capped externally via log rotation if needed)
echo "$(date -u +%FT%TZ) DEADMAN OK age=${AGE}s" >>~/.emailorganizer/logs/deadman.log
exit 0
