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
        self._seen_threads: set[str] = set()

    async def update_from_thread(self, meta: ThreadMetadata) -> SenderStats:
        """Update sender stats from a thread's metadata. Deduplicates by thread_id."""
        existing = await self._store.get_sender_stats(self._tenant, meta.sender)
        if existing:
            stats = existing
        else:
            stats = SenderStats(
                tenant_id=self._tenant,
                sender=meta.sender,
                sender_domain=meta.sender_domain,
                first_seen_at=datetime.now(timezone.utc).isoformat(),
            )

        is_first_encounter = meta.thread_id not in self._seen_threads
        if is_first_encounter:
            already_processed = await self._store.is_thread_processed(self._tenant, meta.thread_id)
            if not already_processed:
                stats.total_received += 1
                self._seen_threads.add(meta.thread_id)

        if not meta.is_unread:
            stats.opened += 1
        if meta.has_user_reply:
            stats.replied += 1

        stats.engagement_score = self._compute_score(stats)
        stats.last_updated = datetime.now(timezone.utc).isoformat()

        await self._store.upsert_sender_stats(stats)
        return stats

    async def record_label_event(self, sender: str, event_type: str) -> None:
        """Record a user label action detected from Gmail history."""
        stats = await self._store.get_sender_stats(self._tenant, sender)
        if not stats:
            return

        if event_type == "opened":
            stats.opened += 1
        elif event_type == "starred":
            stats.starred += 1
        elif event_type == "archived":
            stats.manually_archived += 1
        elif event_type == "trashed":
            stats.trashed += 1
        elif event_type == "spammed":
            stats.spam_marked += 1

        stats.engagement_score = self._compute_score(stats)
        stats.last_updated = datetime.now(timezone.utc).isoformat()
        await self._store.upsert_sender_stats(stats)

    @staticmethod
    def _compute_score(stats: SenderStats) -> float:
        if stats.total_received == 0:
            return 0.0
        # Positive signals: opens (1x), replies (5x).
        # Negative signals: trash (-1x, routine cleanup is soft-negative),
        # spam_marked (-3x, unambiguous "this was wrong").
        # manually_archived stays neutral (often just "seen, done").
        raw = (
            stats.opened
            + 5 * stats.replied
            - stats.trashed
            - 3 * stats.spam_marked
        )
        return max(0.0, raw / stats.total_received)

    @staticmethod
    def compute_score(
        opened: int,
        replied: int,
        total: int,
        trashed: int = 0,
        spam_marked: int = 0,
    ) -> float:
        if total == 0:
            return 0.0
        raw = opened + 5 * replied - trashed - 3 * spam_marked
        return max(0.0, raw / total)
