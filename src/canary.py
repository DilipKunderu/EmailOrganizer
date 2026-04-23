"""Process 7: End-to-end canary sidecar.

Single responsibility: once an hour, send self a marker email and verify
the *previous* canary made it through the live-sync pipeline.

State machine (per run):
  1. Read previous canary_token from sync_state.
  2. If one exists and is at least CANARY_MIN_AGE_SECONDS old:
        Search Gmail for the token; if found, inspect whether it hit
        processed_threads. If yes -> stamp sidecar_timestamps['canary']=now.
        If no, AND the canary is older than CANARY_TIMEOUT_SECONDS -> alert.
  3. Send a new canary email, record the new token.

The canary subject embeds a random UUID + timestamp. Canaries get the
'_auto/quarantine' label filter-style through the existing pipeline — we
don't touch user's inbox in any special way; we just use the normal
classification plumbing.
"""

from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

from src.adapters.factory import AdapterSet
from src.gmail_client import GmailClient
from src.models import DEFAULT_TENANT_ID

logger = logging.getLogger(__name__)

CANARY_TOKEN_KEY = "canary_token"
CANARY_SENT_AT_KEY = "canary_sent_at"
CANARY_MIN_AGE_SECONDS = 300        # wait 5 min before expecting delivery
CANARY_TIMEOUT_SECONDS = 3600       # alert if still not processed after 1h
CANARY_SUBJECT_PREFIX = "[EmailOrganizer CANARY]"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Canary:
    def __init__(self, adapters: AdapterSet):
        self._adapters = adapters

    async def run_once(self) -> dict[str, Any]:
        logger.info("Canary cycle starting")

        await self._adapters.state_store.initialize()
        store = self._adapters.state_store

        # `logged_failure` is set when a previous canary timed out. We never
        # alert directly from here -- the remediator decides whether a
        # pattern of failures warrants escalation (see _check_canary_fail).
        result: dict[str, Any] = {"verified": False, "sent": False, "logged_failure": False}

        try:
            creds = await self._adapters.auth.get_credentials(DEFAULT_TENANT_ID)
        except Exception as exc:
            logger.error("Canary: auth failed (%s); cannot run", exc)
            await store.log_error(
                "canary", "error", type(exc).__name__,
                f"Cannot get creds: {exc}",
            )
            await store.close()
            return result

        gmail = GmailClient(creds)

        # -- Phase 1: verify previous canary
        prev_token = await store.get_sync_value(DEFAULT_TENANT_ID, CANARY_TOKEN_KEY)
        prev_sent_at_str = await store.get_sync_value(DEFAULT_TENANT_ID, CANARY_SENT_AT_KEY)

        if prev_token and prev_sent_at_str:
            try:
                prev_sent_at = datetime.fromisoformat(prev_sent_at_str)
            except Exception:
                prev_sent_at = _now()

            age = (_now() - prev_sent_at).total_seconds()
            if age >= CANARY_MIN_AGE_SECONDS:
                verified = await self._verify(gmail, prev_token)
                if verified:
                    logger.info("Canary verified: token=%s age=%.0fs", prev_token, age)
                    await self._stamp_sidecar("canary")
                    result["verified"] = True
                elif age > CANARY_TIMEOUT_SECONDS:
                    # Failure of a single canary roundtrip is NOT an emergency
                    # in itself -- transient IMAP/network/sleep blips happen.
                    # We log it here for root-cause visibility, but escalation
                    # is the remediator's job: it counts consecutive failures
                    # (i.e. since the last verified canary) and only alerts
                    # when the pipeline stays broken across multiple cycles.
                    msg = (
                        f"Canary token '{prev_token}' sent at {prev_sent_at_str} "
                        f"({age / 60:.0f} minutes ago) was never processed "
                        "within the timeout. Logged for RCA; remediator will "
                        "escalate only if this persists across cycles."
                    )
                    logger.warning(msg)
                    await store.log_error(
                        "canary", "warning", "CanaryNotProcessed", msg,
                        context={"token": prev_token, "age_seconds": age},
                    )
                    result["logged_failure"] = True
                else:
                    logger.info(
                        "Canary not yet verified (age=%.0fs < timeout=%.0fs)",
                        age, CANARY_TIMEOUT_SECONDS,
                    )

        # -- Phase 2: send a fresh canary
        new_token = secrets.token_hex(8)
        subject = f"{CANARY_SUBJECT_PREFIX} {new_token}"
        body = (
            "<html><body style='font-family:-apple-system,sans-serif;color:#888;'>"
            "<p>Synthetic canary email from your EmailOrganizer watchdog.</p>"
            f"<p>Token: <code>{new_token}</code></p>"
            f"<p>Sent at: {_now().isoformat()}</p>"
            "<p>This message exists so a missed processing event can be detected. "
            "You can safely archive or delete it.</p>"
            "</body></html>"
        )
        try:
            gmail.send_self_email(subject=subject, body_html=body)
            await store.set_sync_value(DEFAULT_TENANT_ID, CANARY_TOKEN_KEY, new_token)
            await store.set_sync_value(
                DEFAULT_TENANT_ID, CANARY_SENT_AT_KEY, _now().isoformat(),
            )
            result["sent"] = True
            logger.info("Canary sent: token=%s", new_token)
        except Exception as exc:
            logger.error("Failed to send canary: %s", exc)
            await store.log_error(
                "canary", "error", type(exc).__name__,
                f"send_self_email failed: {exc}",
            )

        await store.close()
        return result

    async def _verify(self, gmail: GmailClient, token: str) -> bool:
        """Return True iff a Gmail message with our canary token exists and was
        already marked processed by the live-sync pipeline.

        We search with `in:anywhere` so a canary that got trashed, archived, or
        otherwise moved after processing still counts as verified. The real
        contract we care about is "did the agent's pipeline process this
        thread" -- tested by `is_thread_processed` below -- not "is the email
        still in the default Gmail search view." Without `in:anywhere`, Gmail
        defaults to `-in:trash -in:spam` and self-sent canaries that Gmail (or
        the user) moved to Trash look like pipeline failures when they aren't.
        """
        query = f"in:anywhere subject:\"{CANARY_SUBJECT_PREFIX} {token}\""
        try:
            threads, _, _ = gmail.list_threads(query=query, max_results=5)
        except Exception as exc:
            logger.warning("Canary verify search failed: %s", exc)
            return False

        if not threads:
            return False

        store = self._adapters.state_store
        for t in threads:
            tid = t.get("id")
            if tid and await store.is_thread_processed(DEFAULT_TENANT_ID, tid):
                return True
        return False

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
