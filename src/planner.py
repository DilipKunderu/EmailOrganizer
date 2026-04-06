from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from src.models import (
    ActionType,
    Classification,
    ProposedAction,
    RiskLevel,
    SenderStats,
    Settings,
    ThreadMetadata,
    UnsubscribeTier,
)

logger = logging.getLogger(__name__)


class ActionPlanner:
    """Maps classifications and engagement data to concrete proposed actions."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def plan(
        self,
        meta: ThreadMetadata,
        classification: Classification,
        sender_stats: SenderStats | None = None,
        is_crawl: bool = False,
    ) -> list[ProposedAction]:
        actions: list[ProposedAction] = []
        s = self._settings

        # Category label
        if classification.category:
            actions.append(ProposedAction(
                thread_id=meta.thread_id,
                action_type=ActionType.LABEL,
                risk_level=RiskLevel.SAFE,
                confidence=classification.confidence,
                reason=classification.reason,
                classified_by=classification.classified_by,
                label_name=classification.category,
            ))

        # Action label
        if classification.action_label:
            thread_age = self._thread_age_days(meta)
            if not is_crawl or thread_age <= s.crawl_star_max_age_days:
                actions.append(ProposedAction(
                    thread_id=meta.thread_id,
                    action_type=ActionType.LABEL,
                    risk_level=RiskLevel.SAFE,
                    confidence=classification.confidence,
                    reason=classification.reason,
                    classified_by=classification.classified_by,
                    label_name=classification.action_label,
                ))

        # Star
        if classification.should_star:
            thread_age = self._thread_age_days(meta)
            if not is_crawl or thread_age <= s.crawl_star_max_age_days:
                actions.append(ProposedAction(
                    thread_id=meta.thread_id,
                    action_type=ActionType.STAR,
                    risk_level=RiskLevel.SAFE,
                    confidence=classification.confidence,
                    reason="Classified as important",
                    classified_by=classification.classified_by,
                ))

        # Archive
        if classification.should_archive:
            thread_age = self._thread_age_days(meta)
            if not is_crawl or thread_age >= s.crawl_archive_age_days:
                actions.append(ProposedAction(
                    thread_id=meta.thread_id,
                    action_type=ActionType.ARCHIVE,
                    risk_level=RiskLevel.LOW,
                    confidence=classification.confidence,
                    reason="Low priority, auto-archive",
                    classified_by=classification.classified_by,
                ))

        # Unsubscribe evaluation
        if sender_stats and meta.has_unsubscribe:
            tier = self._evaluate_unsubscribe_tier(sender_stats)
            if tier in (UnsubscribeTier.IMMEDIATE, UnsubscribeTier.QUARANTINE):
                actions.append(ProposedAction(
                    thread_id=meta.thread_id,
                    action_type=ActionType.UNSUBSCRIBE,
                    risk_level=RiskLevel.MEDIUM,
                    confidence=1.0 - sender_stats.engagement_score,
                    reason=f"Unsub tier {tier.value}: engagement={sender_stats.engagement_score:.2f}, emails={sender_stats.total_received}",
                    classified_by=classification.classified_by,
                    metadata={"tier": tier.value, "sender": meta.sender},
                ))

        return actions

    def _evaluate_unsubscribe_tier(self, stats: SenderStats) -> UnsubscribeTier:
        s = self._settings
        if stats.engagement_score == 0.0 and stats.total_received >= s.unsub_tier1_min:
            return UnsubscribeTier.IMMEDIATE
        if stats.engagement_score < s.unsub_tier2_max_engagement and stats.total_received >= s.unsub_tier2_min:
            return UnsubscribeTier.QUARANTINE
        if stats.engagement_score < s.unsub_tier3_max_engagement:
            return UnsubscribeTier.FLAG_ONLY
        return UnsubscribeTier.KEEP

    @staticmethod
    def _thread_age_days(meta: ThreadMetadata) -> int:
        if not meta.date:
            return 0
        delta = datetime.now(timezone.utc) - meta.date
        return max(0, delta.days)
