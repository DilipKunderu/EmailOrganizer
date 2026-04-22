"""Process 6: Watchdog sidecar.

Single responsibility: every 15 min, evaluate SLOs over HealthStatus + error_log,
fire macOS notifications + self-email alerts on breach, write a heartbeat file
so the dead-man job can detect if the watchdog itself has stopped running.

SLOs (plan Step 6f):
  * health.needs_reauth == True                                  -> CRITICAL
  * elapsed_since(last_successful_cycle) > 30 min (awake time)   -> CRITICAL
  * consecutive_error_count > MAX_CONSECUTIVE_ERRORS              -> CRITICAL
  * threads_processed_24h == 0 AND actions_taken_24h == 0        -> WARNING
    (only when agent has been up > 1h to avoid startup false positives)
  * sidecar heartbeat age > 2x expected interval                 -> WARNING
  * len(errors last 1h) > HIGH_ERROR_RATE_THRESHOLD              -> WARNING
"""

from __future__ import annotations

import json
import logging
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.adapters.factory import AdapterSet
from src.alerting import send_alert
from src.gmail_client import GmailClient
from src.models import DEFAULT_TENANT_ID

logger = logging.getLogger(__name__)

HEARTBEAT_PATH = Path("~/.emailorganizer/watchdog_heartbeat").expanduser()

STALENESS_SLO_SECONDS = 30 * 60
MAX_CONSECUTIVE_ERRORS = 10
HIGH_ERROR_RATE_THRESHOLD = 50     # errors in last 1h

# Known sidecar cadences (seconds); staleness tolerance = 2x these.
SIDECAR_INTERVALS = {
    "agent":    300,        # writes on every live-sync poll (fallback cadence)
    "crawl":    3600,       # once per crawl cycle (10min default, be lenient)
    "janitor":  3600,       # hourly
    "digest":   86400,      # daily
    "learner":  86400,      # daily
    "canary":   3600,       # hourly
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _pretty_age(delta: timedelta) -> str:
    secs = int(delta.total_seconds())
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m"
    if secs < 86400:
        return f"{secs // 3600}h {(secs % 3600) // 60}m"
    return f"{secs // 86400}d {(secs % 86400) // 3600}h"


class Watchdog:
    """Process 6: SLO evaluator + alerter."""

    def __init__(self, adapters: AdapterSet):
        self._adapters = adapters

    async def run_once(self) -> dict[str, Any]:
        logger.info("Watchdog evaluating SLOs")

        await self._adapters.state_store.initialize()
        health = await self._adapters.process_manager.read_health()

        alerts: list[tuple[str, str, str]] = []  # (severity, title, body)

        # 1. needs_reauth is always CRITICAL
        if health and health.needs_reauth:
            alerts.append((
                "CRITICAL",
                "Gmail re-auth required",
                "Refresh token revoked or expired. Run:\n\n"
                "    python -m src.main --reauth --mode local\n\n"
                f"last_error: {health.last_error}\n"
                f"last_error_ts: {health.last_error_ts}",
            ))

        # 2. Staleness — gate by agent uptime so nighttime sleep doesn't false-alarm
        if health:
            uptime_seconds = health.uptime_seconds or 0
            last_ok = _parse_iso(health.last_successful_cycle)
            if last_ok and uptime_seconds > STALENESS_SLO_SECONDS:
                age = _now() - last_ok
                if age.total_seconds() > STALENESS_SLO_SECONDS:
                    alerts.append((
                        "CRITICAL",
                        f"No successful sync in {_pretty_age(age)}",
                        f"SLO: <{STALENESS_SLO_SECONDS // 60}m.\n"
                        f"last_successful_cycle: {health.last_successful_cycle}\n"
                        f"consecutive_errors: {health.consecutive_error_count}\n"
                        f"last_error: {health.last_error}",
                    ))

        # 3. Error streak
        if health and health.consecutive_error_count >= MAX_CONSECUTIVE_ERRORS:
            alerts.append((
                "CRITICAL",
                f"Agent error streak: {health.consecutive_error_count}",
                f"Last error: {health.last_error}\n"
                f"Will force-restart via fail-fast contract.",
            ))

        # 4. High error rate
        try:
            recent_errors = await self._adapters.state_store.get_recent_errors(hours=1)
        except Exception as exc:
            logger.error("Failed to read error_log: %s", exc)
            recent_errors = []

        if len(recent_errors) > HIGH_ERROR_RATE_THRESHOLD:
            top = Counter(e.get("error_class") or "Unknown" for e in recent_errors).most_common(5)
            alerts.append((
                "WARNING",
                f"High error rate: {len(recent_errors)} in last hour",
                "Top error classes:\n" + "\n".join(f"  {cls}: {cnt}" for cls, cnt in top),
            ))

        # 5. Zero-activity 24h (only if agent has been up a full hour)
        if health and (health.uptime_seconds or 0) > 3600:
            if health.threads_processed_24h == 0 and health.actions_taken_24h == 0:
                alerts.append((
                    "WARNING",
                    "Zero mail processed in last 24h",
                    "Either a very quiet inbox or a silent pipeline failure.\n"
                    "Run: python -m src.main --status --mode local",
                ))

        # 5b. LLM quota exhaustion (warning, not critical: fallback to rules works)
        if health and health.llm_quota_exhausted:
            provider = health.llm_quota_provider or "unknown"
            alerts.append((
                "WARNING",
                f"LLM quota exhausted ({provider})",
                (f"Classifications are falling back to rules only. "
                 f"Last quota error at {health.llm_quota_last_ts}. "
                 "Fix: top up credits for the provider, or edit "
                 "config/settings.yaml to switch providers."),
            ))

        # 6. Sidecar staleness
        if health:
            sidecar_ts = health.sidecar_timestamps or {}
            for name, interval in SIDECAR_INTERVALS.items():
                if name == "agent":
                    continue  # agent's freshness is covered by last_successful_cycle
                ts = _parse_iso(sidecar_ts.get(name, ""))
                if ts is None:
                    continue  # never ran yet; tolerate until first successful run
                age = _now() - ts
                if age.total_seconds() > 2 * interval:
                    alerts.append((
                        "WARNING",
                        f"Sidecar '{name}' stale ({_pretty_age(age)})",
                        f"Expected to run every {interval // 60}m; last at {sidecar_ts[name]}.\n"
                        "Check launchctl list | grep emailorganizer.",
                    ))

        # Fire alerts. Try to construct a Gmail client for the email channel.
        gmail = await self._best_effort_gmail_client()
        for severity, title, body in alerts:
            await send_alert(title, body, severity, gmail_client=gmail)

        # Update sidecar_timestamps['watchdog'] + write heartbeat
        await self._stamp_sidecar("watchdog")
        self._write_heartbeat()

        await self._adapters.state_store.close()

        summary = {
            "alerts_fired": len(alerts),
            "critical": sum(1 for a in alerts if a[0] == "CRITICAL"),
            "warning": sum(1 for a in alerts if a[0] == "WARNING"),
            "recent_errors_1h": len(recent_errors),
        }
        logger.info("Watchdog summary: %s", summary)
        return summary

    async def _best_effort_gmail_client(self) -> GmailClient | None:
        """Try to build a GmailClient from valid creds. Silently return None if unavailable."""
        try:
            creds = await self._adapters.auth.get_credentials(DEFAULT_TENANT_ID)
            return GmailClient(creds)
        except Exception as exc:
            logger.info("Gmail self-email channel unavailable: %s", exc)
            return None

    async def _stamp_sidecar(self, name: str) -> None:
        try:
            existing = await self._adapters.process_manager.read_health()
            if existing is None:
                return
            ts = existing.sidecar_timestamps or {}
            ts[name] = _now().isoformat()
            existing.sidecar_timestamps = ts
            await self._adapters.process_manager.write_health(existing)
        except Exception as exc:
            logger.debug("Failed to stamp sidecar %s: %s", name, exc)

    @staticmethod
    def _write_heartbeat() -> None:
        HEARTBEAT_PATH.parent.mkdir(parents=True, exist_ok=True)
        HEARTBEAT_PATH.write_text(json.dumps({
            "ts": _now().isoformat(),
            "pid": os.getpid(),
        }))
