"""Process 8: Auto-remediation sidecar.

Detects six classes of failures from error_log + HealthStatus + DB state
and applies safe corrective actions. Sits alongside the watchdog:
watchdog ALERTS, remediator ACTS.

Safety invariants:
  - Never kickstart self, watchdog, or deadman (would break observability).
  - Every action is recorded to error_log with severity='remediation' so
    cooldowns/budgets survive restarts and are fully auditable.
  - Cooldowns and daily budgets are enforced per category.
  - When a daily budget is exhausted, escalate to CRITICAL alert and stop
    auto-remediating that category until the next UTC day.
  - Never touches user data (mail, labels, rules, sender_stats). Only
    process state, cursors, and llm provider swaps.
"""

from __future__ import annotations

import json
import logging
import os
import signal as signal_mod
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from src.adapters.factory import AdapterSet
from src.alerting import send_alert
from src.models import DEFAULT_TENANT_ID

logger = logging.getLogger(__name__)

# -- Configuration --

STATUS_PATH = Path("~/.emailorganizer/status.json").expanduser()
DB_PATH = Path("~/.emailorganizer/state.db").expanduser()
WAL_PATH = Path("~/.emailorganizer/state.db-wal").expanduser()
SETTINGS_PATH = Path("config/settings.yaml")

# Labels we manage. EXCLUDES watchdog, deadman, remediator (self) so we
# never take down the observation or remediation chain itself.
REMEDIABLE_LABELS = {
    "agent", "crawl", "canary", "janitor", "digest", "learner",
}

# Expected cadence seconds per sidecar, used for staleness checks.
# Keep aligned with watchdog.SIDECAR_INTERVALS.
SIDECAR_INTERVALS = {
    "agent":    300,
    "crawl":    3600,
    "janitor":  3600,
    "digest":   86400,
    "learner":  86400,
    "canary":   3600,
}

# Per-category cooldown in minutes: how long to wait after a SUCCESSFUL
# remediation before re-attempting the same category.
COOLDOWNS_MINUTES = {
    "wedged_procs": 15,
    "error_streak": 60,
    "gmail_cursor": 360,   # 6h
    "llm_quota":    30,
    "canary_fail":  10,
    "db_health":    60,
}

# Per-category daily budget: max successful remediations in a rolling 24h
# window before we stop and escalate.
MAX_PER_DAY = {
    "wedged_procs": 10,
    "error_streak": 10,
    "gmail_cursor": 3,
    "llm_quota":    2,
    "canary_fail":  5,
    "db_health":    5,
}

# Error streak thresholds per source (errors in last 1h before we act).
ERROR_STREAK_THRESHOLDS = {
    "agent":   50,
    "crawl":   20,
    "default": 30,
}

WAL_BYTES_THRESHOLD = 100 * 1024 * 1024  # 100 MB


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


class Remediator:
    def __init__(self, adapters: AdapterSet):
        self._adapters = adapters

    async def run_once(self) -> dict[str, Any]:
        logger.info("Remediator cycle starting")
        await self._adapters.state_store.initialize()

        health = await self._adapters.process_manager.read_health()
        errors_1h = await self._adapters.state_store.get_recent_errors(hours=1)
        errors_24h = await self._adapters.state_store.get_recent_errors(hours=24)

        actions_taken: list[dict[str, Any]] = []
        actions_skipped: list[dict[str, Any]] = []
        escalations: list[dict[str, Any]] = []

        # Policy checks in order. Each returns a list of proposed actions.
        proposed: list[dict[str, Any]] = []
        if health is not None:
            proposed.extend(self._check_wedged_procs(health))
        proposed.extend(self._check_error_streak(errors_1h))
        if health is not None:
            proposed.extend(self._check_gmail_cursor(health, errors_1h))
            proposed.extend(self._check_llm_quota(health))
            proposed.extend(self._check_canary_fail(health, errors_1h, errors_24h))
        proposed.extend(self._check_db_health())

        for action in proposed:
            category = action["category"]

            # Daily budget check
            recent_successes = await self._count_recent_remediations(
                category, action.get("target"), hours=24, result="success",
            )
            if recent_successes >= MAX_PER_DAY.get(category, 5):
                # Escalate once per cooldown period
                last_escalation = await self._count_recent_remediations(
                    category, action.get("target"), hours=1, result="escalated",
                )
                if last_escalation == 0:
                    await self._escalate(category, action, recent_successes)
                    escalations.append({**action, "reason": "daily budget exhausted"})
                else:
                    actions_skipped.append({
                        **action,
                        "skip_reason": f"budget_exhausted_{recent_successes}",
                    })
                continue

            # Cooldown check
            cooldown_min = COOLDOWNS_MINUTES.get(category, 60)
            recent_cooldown = await self._count_recent_remediations(
                category, action.get("target"),
                hours=cooldown_min / 60, result="success",
            )
            if recent_cooldown > 0:
                actions_skipped.append({
                    **action,
                    "skip_reason": f"cooldown ({cooldown_min}m)",
                })
                continue

            # Execute
            try:
                result = await self._apply(action)
                await self._record(action, result="success", detail=result)
                actions_taken.append({**action, "detail": result})
            except Exception as exc:
                logger.error("Remediation %s/%s failed: %s",
                             action["category"], action.get("target", ""), exc,
                             exc_info=True)
                await self._record(action, result="failed", detail=str(exc))
                actions_skipped.append({
                    **action,
                    "skip_reason": f"execution_failed: {exc}",
                })

        summary = {
            "proposed": len(proposed),
            "taken": len(actions_taken),
            "skipped": len(actions_skipped),
            "escalations": len(escalations),
            "actions": actions_taken,
            "skipped_actions": actions_skipped,
            "escalated": escalations,
        }

        # Update sidecar heartbeat
        await self._stamp_sidecar()

        await self._adapters.state_store.close()
        logger.info(
            "Remediator done: proposed=%d taken=%d skipped=%d escalated=%d",
            len(proposed), len(actions_taken), len(actions_skipped), len(escalations),
        )
        return summary

    # ------------------------------------------------------------------
    # Policy checks
    # ------------------------------------------------------------------

    def _check_wedged_procs(self, health) -> list[dict[str, Any]]:
        """Sidecar heartbeats > 2x schedule interval OR agent silent > 15m."""
        proposed: list[dict[str, Any]] = []
        sidecar_ts = health.sidecar_timestamps or {}
        now = _now()

        for name, interval in SIDECAR_INTERVALS.items():
            ts = _parse_iso(sidecar_ts.get(name, ""))
            if ts is None:
                # Never ran yet; launchd will schedule. Don't remediate.
                continue
            age = (now - ts).total_seconds()
            if age > 2 * interval:
                proposed.append({
                    "category": "wedged_procs",
                    "target": name,
                    "reason": f"heartbeat stale ({int(age)}s > 2x{interval}s)",
                    "action": "kickstart",
                })

        # Agent-specific: daemon is up but not writing (older than 15m even
        # though uptime > 10m). The heartbeat guard inside the daemon should
        # catch this, but belt-and-suspenders.
        if (health.uptime_seconds or 0) > 600:
            last_sync = _parse_iso(health.last_sync)
            if last_sync and (now - last_sync).total_seconds() > 900:
                # Only add if not already in the list from heartbeat check
                if not any(a["target"] == "agent" for a in proposed):
                    proposed.append({
                        "category": "wedged_procs",
                        "target": "agent",
                        "reason": (
                            f"last_sync stale "
                            f"({int((now - last_sync).total_seconds())}s > 900s) "
                            f"while uptime={int(health.uptime_seconds)}s"
                        ),
                        "action": "kickstart",
                    })

        return proposed

    def _check_error_streak(self, errors_1h: list[dict[str, Any]]) -> list[dict[str, Any]]:
        counts: dict[str, int] = {}
        for e in errors_1h:
            if e.get("severity") in ("error", "fatal"):
                src = e.get("source") or "unknown"
                counts[src] = counts.get(src, 0) + 1

        proposed: list[dict[str, Any]] = []
        for src, count in counts.items():
            if src == "remediator" or src not in REMEDIABLE_LABELS:
                continue  # never remediate ourselves or non-launchd sources
            threshold = ERROR_STREAK_THRESHOLDS.get(
                src, ERROR_STREAK_THRESHOLDS["default"],
            )
            if count >= threshold:
                proposed.append({
                    "category": "error_streak",
                    "target": src,
                    "reason": f"{count} errors in last 1h (threshold={threshold})",
                    "action": "kickstart",
                })
        return proposed

    def _check_gmail_cursor(
        self, health, errors_1h: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """History cursor stuck despite the daemon's built-in recovery."""
        cursor_errors = [
            e for e in errors_1h
            if e.get("source") == "agent"
            and (e.get("error_class") == "HistoryExpiredError"
                 or ("404" in (e.get("message") or ""))
                 or "startHistoryId" in (e.get("message") or ""))
            and _parse_iso(e.get("ts", "")) is not None
            and (_now() - _parse_iso(e.get("ts", ""))).total_seconds() < 1800
        ]
        if not cursor_errors:
            return []

        # Only act if last_successful_cycle is also stale (> 30m old)
        last_ok = _parse_iso(health.last_successful_cycle)
        if last_ok is not None and (_now() - last_ok).total_seconds() < 1800:
            return []

        return [{
            "category": "gmail_cursor",
            "target": "history_id",
            "reason": (
                f"{len(cursor_errors)} history errors in 30m, "
                f"last_successful_cycle stale"
            ),
            "action": "reset_cursor_and_kickstart_agent",
        }]

    def _check_llm_quota(self, health) -> list[dict[str, Any]]:
        """LLM primary is out of quota and fallback is available."""
        if not health.llm_quota_exhausted:
            return []

        last_ts = _parse_iso(health.llm_quota_last_ts)
        if last_ts is None or (_now() - last_ts).total_seconds() < 1800:
            # Needs to have been exhausted for >30 min before we swap
            return []

        try:
            raw = yaml.safe_load(SETTINGS_PATH.read_text()) or {}
        except Exception:
            return []
        llm = raw.get("llm", {})
        primary = llm.get("provider", "")
        fallback = llm.get("fallback", "")
        if not fallback or fallback == "none" or fallback == primary:
            return []  # no viable swap

        return [{
            "category": "llm_quota",
            "target": f"{primary}->{fallback}",
            "reason": (
                f"primary '{primary}' quota exhausted since {health.llm_quota_last_ts}; "
                f"swapping to fallback '{fallback}'"
            ),
            "action": "swap_llm_providers",
            "primary": primary,
            "fallback": fallback,
        }]

    def _check_canary_fail(
        self,
        health,
        errors_1h: list[dict[str, Any]],
        errors_24h: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Act only on CONSECUTIVE canary failures (those not followed by a
        successful verify). A failure followed by a success is treated as a
        transient blip -- logged, but not escalated. Tier is based on how
        many back-to-back failures have piled up since the last success."""
        canary_fails_1h = [
            e for e in errors_1h
            if e.get("source") == "canary"
            and e.get("error_class") == "CanaryNotProcessed"
        ]
        if not canary_fails_1h:
            return []

        # Self-heal check: the canary stamps sidecar_timestamps["canary"] only
        # on successful verification. If that heartbeat is newer than the
        # most recent failure, the pipeline already recovered on its own.
        def _ts(e):
            return _parse_iso(e.get("ts", "")) or _now()
        most_recent_fail_ts = max(_ts(e) for e in canary_fails_1h)
        heartbeat = _parse_iso((health.sidecar_timestamps or {}).get("canary", ""))
        if heartbeat is not None and heartbeat > most_recent_fail_ts:
            logger.info(
                "Canary self-healed: last success %s newer than last failure %s; "
                "no remediation needed",
                heartbeat.isoformat(), most_recent_fail_ts.isoformat(),
            )
            return []

        # Count CONSECUTIVE failures: those with ts > last successful heartbeat.
        # If heartbeat is absent (never had a success), count all today's
        # failures -- pipeline has never verified itself, treat conservatively.
        if heartbeat is not None:
            consecutive_fails = [
                e for e in errors_24h
                if e.get("source") == "canary"
                and e.get("error_class") == "CanaryNotProcessed"
                and _ts(e) > heartbeat
            ]
        else:
            consecutive_fails = [
                e for e in errors_24h
                if e.get("source") == "canary"
                and e.get("error_class") == "CanaryNotProcessed"
            ]
        n = len(consecutive_fails)

        if n >= 3:
            return [{
                "category": "canary_fail",
                "target": "agent",
                "reason": f"{n} consecutive canary failures (no success since); pipeline likely broken",
                "action": "escalate_only",
            }]
        elif n == 2:
            return [{
                "category": "canary_fail",
                "target": "agent",
                "reason": "2 consecutive canary failures (no success in between)",
                "action": "reset_cursor_and_kickstart_agent",
            }]
        else:
            return [{
                "category": "canary_fail",
                "target": "agent",
                "reason": "1 unrecovered canary failure",
                "action": "kickstart",
            }]

    def _check_db_health(self) -> list[dict[str, Any]]:
        if not WAL_PATH.exists():
            return []
        try:
            size = WAL_PATH.stat().st_size
        except OSError:
            return []
        if size < WAL_BYTES_THRESHOLD:
            return []
        return [{
            "category": "db_health",
            "target": "wal",
            "reason": f"WAL size {size // (1024*1024)}MB > {WAL_BYTES_THRESHOLD // (1024*1024)}MB",
            "action": "wal_checkpoint",
            "wal_bytes": size,
        }]

    # ------------------------------------------------------------------
    # Action application
    # ------------------------------------------------------------------

    async def _apply(self, action: dict[str, Any]) -> str:
        a = action["action"]
        if a == "kickstart":
            return self._launchctl_kickstart(action["target"])
        if a == "reset_cursor_and_kickstart_agent":
            self._reset_history_cursor_sync()
            return "cursor_deleted; " + self._launchctl_kickstart("agent")
        if a == "swap_llm_providers":
            return await self._swap_llm_providers(
                action["primary"], action["fallback"],
            )
        if a == "wal_checkpoint":
            return self._wal_checkpoint()
        if a == "escalate_only":
            await send_alert(
                f"Remediator: canary escalation ({action['target']})",
                (f"Canary has failed {action['reason']}. "
                 "Remediator will not take further action. "
                 "Run: python -m src.main --status --mode local"),
                "CRITICAL",
                gmail_client=None,
            )
            return "escalated"
        raise ValueError(f"Unknown remediation action: {a}")

    # -- Launchctl --

    @staticmethod
    def _launchctl_kickstart(name: str) -> str:
        if name not in REMEDIABLE_LABELS:
            raise ValueError(f"Refusing to kickstart non-remediable service: {name}")
        label = f"com.emailorganizer.{name}"
        uid = os.getuid()
        cmd = ["launchctl", "kickstart", "-k", f"gui/{uid}/{label}"]
        logger.info("Running: %s", " ".join(cmd))
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"launchctl kickstart failed (rc={result.returncode}): "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
        return f"launchctl kickstart -k {label}"

    # -- Cursor reset --

    def _reset_history_cursor_sync(self) -> None:
        """Synchronous cursor delete via aiosqlite doesn't work here cleanly
        because we're inside an async cycle; use sqlite3 stdlib for a quick
        one-shot write. Safe because the daemon commits on every set_sync_value."""
        import sqlite3
        con = sqlite3.connect(str(DB_PATH), timeout=5)
        try:
            con.execute(
                "DELETE FROM sync_state WHERE tenant_id=? AND key='history_id'",
                (DEFAULT_TENANT_ID,),
            )
            con.commit()
        finally:
            con.close()

    # -- LLM provider swap --

    async def _swap_llm_providers(self, primary: str, fallback: str) -> str:
        """Swap provider and fallback in settings.yaml, then SIGHUP the agent."""
        raw = yaml.safe_load(SETTINGS_PATH.read_text()) or {}
        llm = raw.setdefault("llm", {})
        llm["provider"] = fallback
        llm["fallback"] = primary
        SETTINGS_PATH.write_text(
            yaml.safe_dump(raw, default_flow_style=False, sort_keys=False),
        )

        # SIGHUP agent (best-effort)
        sighup_status = self._sighup_agent()

        await send_alert(
            f"LLM providers swapped: {primary} -> {fallback}",
            (f"Primary LLM quota was exhausted on '{primary}'. "
             f"Remediator swapped to fallback '{fallback}' in "
             f"config/settings.yaml and SIGHUP'd the agent ({sighup_status}).\n\n"
             "Top up credits on the original provider and the swap will "
             "reverse itself on next quota exhaustion, or restore manually."),
            "WARNING",
            gmail_client=None,
        )
        return f"swapped {primary}<->{fallback}; sighup={sighup_status}"

    @staticmethod
    def _sighup_agent() -> str:
        try:
            data = json.loads(STATUS_PATH.read_text())
            pid = int(data.get("pid", 0))
            if pid <= 0:
                return "no_pid"
            os.kill(pid, signal_mod.SIGHUP)
            return f"sent SIGHUP to pid {pid}"
        except ProcessLookupError:
            return "agent pid not running"
        except Exception as exc:
            return f"sighup_failed: {exc}"

    # -- DB checkpoint --

    @staticmethod
    def _wal_checkpoint() -> str:
        import sqlite3
        con = sqlite3.connect(str(DB_PATH), timeout=10)
        try:
            cur = con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            row = cur.fetchone()
            con.commit()
        finally:
            con.close()

        after = WAL_PATH.stat().st_size if WAL_PATH.exists() else 0
        return f"wal_checkpoint rc={row} wal_after={after // (1024*1024)}MB"

    # ------------------------------------------------------------------
    # Record + escalate + cooldown helpers
    # ------------------------------------------------------------------

    async def _record(
        self, action: dict[str, Any], result: str, detail: str = "",
    ) -> None:
        ctx = {
            "category": action["category"],
            "target": action.get("target", ""),
            "action": action.get("action", ""),
            "reason": action.get("reason", ""),
            "result": result,
            "detail": detail[:500],
        }
        await self._adapters.state_store.log_error(
            "remediator", "remediation",
            f"Remediation/{action['category']}/{result}",
            f"{action['category']}/{action.get('target', '')}: "
            f"{action.get('action', '')} -> {result}",
            context=ctx,
        )

    async def _escalate(
        self, category: str, action: dict[str, Any], budget_used: int,
    ) -> None:
        title = f"Remediator exhausted for {category}"
        body = (
            f"Auto-remediation for category '{category}' has hit daily "
            f"budget ({budget_used}/{MAX_PER_DAY.get(category)}). "
            f"Stopping automatic fixes for this category until UTC rolls.\n\n"
            f"Latest issue target: {action.get('target', '')}\n"
            f"Reason: {action.get('reason', '')}\n\n"
            "Run: python -m src.main --status --mode local"
        )
        await send_alert(title, body, "CRITICAL", gmail_client=None)
        await self._record(action, result="escalated", detail=title)

    async def _count_recent_remediations(
        self,
        category: str,
        target: str | None,
        *,
        hours: float,
        result: str,
    ) -> int:
        cutoff = (_now() - timedelta(hours=hours)).isoformat()
        # Pull recent remediator rows and filter in Python; error_log is
        # indexed on ts so this is cheap for a 24h window.
        rows = await self._adapters.state_store.get_recent_errors(
            hours=max(hours, 24), source="remediator",
        )
        count = 0
        for r in rows:
            ts = r.get("ts", "")
            if ts < cutoff:
                continue
            ctx_raw = r.get("context_json")
            if not ctx_raw:
                continue
            try:
                ctx = json.loads(ctx_raw)
            except Exception:
                continue
            if ctx.get("category") != category:
                continue
            if ctx.get("result") != result:
                continue
            if target is not None and ctx.get("target", "") != target:
                continue
            count += 1
        return count

    async def _stamp_sidecar(self) -> None:
        try:
            existing = await self._adapters.process_manager.read_health()
            if existing is None:
                return
            ts = existing.sidecar_timestamps or {}
            ts["remediator"] = _now().isoformat()
            existing.sidecar_timestamps = ts
            await self._adapters.process_manager.write_health(existing)
        except Exception as exc:
            logger.debug("Failed to stamp remediator sidecar: %s", exc)
