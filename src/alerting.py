"""Alerting primitives for proactive failure notification.

Best-effort fallback chain per alert:
    1. macOS notification via osascript (always attempted; zero-dependency).
    2. Self-email via Gmail API (if a valid GmailClient is provided).
    3. Local mail command (last resort; delivers to local user mailbox only).

Alerts are deduplicated within the process over a 1-hour window so a wedged
watchdog doesn't spam dozens of identical notifications.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import shutil
import subprocess
import time
from collections import OrderedDict
from typing import Any

logger = logging.getLogger(__name__)

# In-process dedup: (title, body_hash) -> last_sent_monotonic_seconds
_RECENT_ALERTS: "OrderedDict[tuple[str, str], float]" = OrderedDict()
# Severity-aware dedup windows. CRITICAL is kept short so a persistent
# problem keeps pinging the user (launchd respawns watchdog every 15 min,
# so a 15 min dedup means one alert per watchdog tick while broken).
# WARNING stays at 1h to avoid nagging on low-priority chronic issues.
_DEDUP_WINDOW_BY_SEVERITY = {
    "CRITICAL": 15 * 60,
    "WARNING":  60 * 60,
    "INFO":     60 * 60,
}
_DEDUP_FALLBACK_SECONDS = 60 * 60
_DEDUP_MAX_ENTRIES = 256


def _dedup_window(severity: str) -> int:
    return _DEDUP_WINDOW_BY_SEVERITY.get(severity.upper(), _DEDUP_FALLBACK_SECONDS)


def _dedup_should_suppress(title: str, body: str, severity: str) -> bool:
    window = _dedup_window(severity)
    key = (title, hashlib.sha1(body.encode("utf-8", errors="ignore")).hexdigest())
    now = time.monotonic()
    # Purge entries older than the longest configured window so the dict
    # doesn't grow unboundedly across severities.
    max_window = max(_DEDUP_WINDOW_BY_SEVERITY.values())
    stale = [k for k, ts in _RECENT_ALERTS.items() if now - ts > max_window]
    for k in stale:
        _RECENT_ALERTS.pop(k, None)
    last = _RECENT_ALERTS.get(key)
    if last is not None and (now - last) < window:
        return True
    _RECENT_ALERTS[key] = now
    while len(_RECENT_ALERTS) > _DEDUP_MAX_ENTRIES:
        _RECENT_ALERTS.popitem(last=False)
    return False


def _shell_quote(s: str) -> str:
    # AppleScript string literal: escape backslashes and double quotes
    return s.replace("\\", "\\\\").replace('"', '\\"')


async def _notify_macos(title: str, body: str, severity: str) -> bool:
    """Fire a native macOS notification. Returns True on success."""
    if shutil.which("osascript") is None:
        return False

    # Limit body to a reasonable size; long notifications get truncated by macOS
    snippet = body if len(body) <= 400 else body[:397] + "..."
    sound = {
        "CRITICAL": "Basso",
        "WARNING": "Tink",
    }.get(severity.upper(), "Ping")

    script = (
        f'display notification "{_shell_quote(snippet)}" '
        f'with title "EmailOrganizer" '
        f'subtitle "{_shell_quote(title)}" '
        f'sound name "{sound}"'
    )
    try:
        proc = await asyncio.create_subprocess_exec(
            "osascript", "-e", script,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await asyncio.wait_for(proc.communicate(), timeout=5.0)
        if proc.returncode == 0:
            return True
        logger.debug("osascript failed (rc=%s): %s", proc.returncode, err)
        return False
    except (asyncio.TimeoutError, FileNotFoundError, OSError) as exc:
        logger.debug("macOS notify failed: %s", exc)
        return False


def _notify_email(title: str, body: str, severity: str, gmail_client: Any) -> bool:
    if gmail_client is None:
        return False
    try:
        subject = f"[EmailOrganizer {severity.upper()}] {title}"
        html = (
            "<html><body style='font-family: -apple-system, sans-serif;'>"
            f"<h2 style='color: {'#cc0000' if severity.upper() == 'CRITICAL' else '#cc6600'};'>"
            f"EmailOrganizer {severity.upper()}: {title}</h2>"
            f"<pre style='background:#f5f5f5; padding:12px; border-radius:4px;'>"
            f"{body}</pre>"
            "<p style='color:#666; font-size:0.9em;'>Sent by the EmailOrganizer watchdog. "
            "If you're seeing this, something needs your attention. "
            "Run <code>python -m src.main --status --mode local</code> for details.</p>"
            "</body></html>"
        )
        gmail_client.send_self_email(subject=subject, body_html=html)
        return True
    except Exception as exc:
        logger.debug("Gmail self-email alert failed: %s", exc)
        return False


def _notify_local_mail(title: str, body: str, severity: str) -> bool:
    """Last-resort local mail via macOS 'mail' command."""
    if shutil.which("mail") is None:
        return False
    try:
        import getpass
        subject = f"[EmailOrganizer {severity.upper()}] {title}"
        subprocess.run(
            ["mail", "-s", subject, getpass.getuser()],
            input=body.encode("utf-8"),
            check=False,
            timeout=5,
        )
        return True
    except Exception as exc:
        logger.debug("Local mail alert failed: %s", exc)
        return False


async def send_alert(
    title: str,
    body: str,
    severity: str = "WARNING",
    gmail_client: Any = None,
) -> dict[str, bool]:
    """Fire an alert on all available channels with dedup.

    Returns dict of channel -> success bool.
    """
    if _dedup_should_suppress(title, body, severity):
        logger.debug("Alert suppressed by dedup: %s", title)
        return {"suppressed": True}

    result = {
        "macos": await _notify_macos(title, body, severity),
        "gmail": _notify_email(title, body, severity, gmail_client),
    }
    if not result["macos"] and not result["gmail"]:
        result["local_mail"] = _notify_local_mail(title, body, severity)

    logger.info("Alert fired: [%s] %s (channels=%s)", severity, title,
                ",".join(k for k, v in result.items() if v))
    return result
