"""Process 4: Janitor.

Single responsibility: run housekeeping tasks (quarantine expiration,
unsubscribe persistence checks). Short-lived process, runs hourly via launchd timer.
"""

from __future__ import annotations

import logging

from src.adapters.factory import AdapterSet
from src.executor import BatchExecutor
from src.gmail_client import GmailClient
from src.models import DEFAULT_TENANT_ID
from src.quarantine_manager import QuarantineManager
from src.unsubscribe import UnsubscribeEngine

logger = logging.getLogger(__name__)


class Janitor:
    """Process 4: Quarantine expiration + unsubscribe persistence."""

    def __init__(self, adapters: AdapterSet):
        self._adapters = adapters

    async def run(self) -> dict[str, int]:
        logger.info("Janitor starting")
        settings = await self._adapters.config_loader.load_settings()
        await self._adapters.state_store.initialize()

        creds = await self._adapters.auth.get_credentials(DEFAULT_TENANT_ID)
        gmail = GmailClient(creds)
        gmail.provision_labels()

        executor = BatchExecutor(gmail, self._adapters.state_store, DEFAULT_TENANT_ID)
        qm = QuarantineManager(
            gmail, self._adapters.state_store, executor, settings, DEFAULT_TENANT_ID,
        )
        unsub = UnsubscribeEngine(
            gmail, self._adapters.state_store, settings, DEFAULT_TENANT_ID,
        )

        stats: dict[str, int] = {}

        # Quarantine expiration
        executed, skipped = await qm.check_expirations()
        stats["quarantine_executed"] = executed
        stats["quarantine_skipped"] = skipped

        # Unsubscribe persistence check
        senders = await self._adapters.state_store.get_senders_by_engagement(
            DEFAULT_TENANT_ID, max_score=0.0, min_emails=1, limit=500,
        )
        escalated = 0
        for s in senders:
            if s.blocklisted:
                if await unsub.check_persistence(s.sender):
                    escalated += 1
        stats["unsub_escalated"] = escalated

        await self._adapters.state_store.close()
        logger.info("Janitor complete: %s", stats)
        return stats
