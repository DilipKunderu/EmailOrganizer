from __future__ import annotations

import logging
from datetime import datetime, timezone

from src.models import DEFAULT_TENANT_ID, SenderStats, ThreadMetadata
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class EngagementTracker:
    def __init__(self, state_store: StateStorePort, tenant_id: str = DEFAULT_TENANT_ID):
        self._store = state_store
        self._tenant = tenant_id

    async def update_from_thread(self, meta: ThreadMetadata) -> SenderStats:
        """Update sender stats from a thread's metadata."""
        existing = await self._store.get_sender_stats(self._tenant, meta.sender)
        if existing:
            stats = existing
            stats.total_received += 1
        else:
            stats = SenderStats(
                tenant_id=self._tenant,
                sender=meta.sender,
                sender_domain=meta.sender_domain,
                total_received=1,
            )

        if not meta.is_unread:
            stats.opened += 1
        if meta.has_user_reply:
            stats.replied += 1

        stats.engagement_score = self._compute_score(stats)
        stats.last_updated = datetime.now(timezone.utc).isoformat()

        await self._store.upsert_sender_stats(stats)
        return stats

    @staticmethod
    def _compute_score(stats: SenderStats) -> float:
        if stats.total_received == 0:
            return 0.0
        return (stats.opened + 5 * stats.replied) / stats.total_received

    @staticmethod
    def compute_score(opened: int, replied: int, total: int) -> float:
        if total == 0:
            return 0.0
        return (opened + 5 * replied) / total
