from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

import aiohttp

from src.gmail_client import GmailClient
from src.models import DEFAULT_TENANT_ID, SenderStats, Settings, ThreadMetadata
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class UnsubscribeEngine:
    """Aggressive unsubscribe via RFC 8058 one-click POST or mailto fallback."""

    def __init__(
        self,
        gmail: GmailClient,
        state_store: StateStorePort,
        settings: Settings,
        tenant_id: str = DEFAULT_TENANT_ID,
    ):
        self._gmail = gmail
        self._store = state_store
        self._settings = settings
        self._tenant = tenant_id

    async def execute_unsubscribe(self, meta: ThreadMetadata) -> bool:
        """Attempt to unsubscribe. Returns True if an action was taken."""
        if meta.unsubscribe_post and meta.unsubscribe_header:
            success = await self._rfc8058_unsubscribe(meta)
            if success:
                await self._post_unsubscribe(meta)
                return True

        if meta.unsubscribe_header:
            mailto = self._extract_mailto(meta.unsubscribe_header)
            if mailto:
                success = self._mailto_unsubscribe(meta, mailto)
                if success:
                    await self._post_unsubscribe(meta)
                    return True

            url = self._extract_url(meta.unsubscribe_header)
            if url:
                logger.info(
                    "URL-only unsubscribe for %s: %s (logged for manual action)",
                    meta.sender, url,
                )

        return False

    async def _rfc8058_unsubscribe(self, meta: ThreadMetadata) -> bool:
        """Send a one-click unsubscribe POST per RFC 8058."""
        url = self._extract_url(meta.unsubscribe_header or "")
        if not url:
            return False
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    url,
                    data="List-Unsubscribe=One-Click",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                ) as resp:
                    if resp.status < 400:
                        logger.info("RFC 8058 unsubscribe success for %s", meta.sender)
                        return True
                    logger.warning("RFC 8058 unsubscribe got status %d for %s", resp.status, meta.sender)
        except Exception as exc:
            logger.warning("RFC 8058 unsubscribe failed for %s: %s", meta.sender, exc)
        return False

    def _mailto_unsubscribe(self, meta: ThreadMetadata, mailto: str) -> bool:
        """Send an unsubscribe email via Gmail API."""
        try:
            self._gmail.send_email(
                to=mailto,
                subject="Unsubscribe",
                body="Please unsubscribe this email address.",
            )
            logger.info("Mailto unsubscribe sent to %s for %s", mailto, meta.sender)
            return True
        except Exception as exc:
            logger.warning("Mailto unsubscribe failed for %s: %s", meta.sender, exc)
            return False

    async def _post_unsubscribe(self, meta: ThreadMetadata) -> None:
        """Archive sender's backlog and blocklist them."""
        await self._store.blocklist_sender(self._tenant, meta.sender)

        threads, _, _ = self._gmail.list_threads(query=f"from:{meta.sender}", max_results=100)
        if threads:
            mods = [{"thread_id": t["id"], "remove_labels": ["INBOX"]} for t in threads]
            self._gmail.batch_modify_threads(mods)
            logger.info("Archived %d threads from %s post-unsubscribe", len(threads), meta.sender)

    async def check_persistence(self, sender: str) -> bool:
        """Check if a blocklisted sender is still sending. If so, escalate to spam."""
        stats = await self._store.get_sender_stats(self._tenant, sender)
        if not stats or not stats.blocklisted:
            return False

        threads, _, _ = self._gmail.list_threads(
            query=f"from:{sender} newer_than:{self._settings.unsub_persistence_days}d",
            max_results=5,
        )
        if threads:
            logger.warning(
                "Sender %s still sending after unsubscribe, escalating to spam",
                sender,
            )
            mods = [
                {"thread_id": t["id"], "add_labels": ["SPAM"], "remove_labels": ["INBOX"]}
                for t in threads
            ]
            self._gmail.batch_modify_threads(mods)
            return True
        return False

    @staticmethod
    def _extract_mailto(header: str) -> str | None:
        match = re.search(r"<mailto:([^>]+)>", header)
        return match.group(1).split("?")[0] if match else None

    @staticmethod
    def _extract_url(header: str) -> str | None:
        match = re.search(r"<(https?://[^>]+)>", header)
        return match.group(1) if match else None
