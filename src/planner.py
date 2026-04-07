from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
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
    """Maps classifications and engagement data to concrete proposed actions.

    In priority inbox mode, archives everything except @Action and @Waiting,
    marks noise as read, and manages Gmail importance markers.
    """

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
        action_label = classification.action_label or "@Reference"

        # Category label
        if classification.category:
            actions.append(self._label_action(
                meta, classification, classification.category,
            ))

        # Action label
        thread_age = self._thread_age_days(meta)
        if not is_crawl or thread_age <= s.crawl_star_max_age_days:
            actions.append(self._label_action(meta, classification, action_label))

        # Priority inbox logic
        if s.inbox_mode == "priority":
            self._plan_priority_inbox(
                actions, meta, classification, action_label, is_crawl,
            )
        else:
            self._plan_relaxed_inbox(actions, meta, classification, is_crawl)

        # Star @Action threads
        if action_label == "@Action" and (not is_crawl or thread_age <= s.crawl_star_max_age_days):
            actions.append(ProposedAction(
                thread_id=meta.thread_id,
                action_type=ActionType.STAR,
                risk_level=RiskLevel.SAFE,
                confidence=classification.confidence,
                reason="@Action thread, starring",
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
                    reason=f"Unsub tier {tier.value}: engagement={sender_stats.engagement_score:.2f}",
                    classified_by=classification.classified_by,
                    metadata={"tier": tier.value, "sender": meta.sender},
                ))

        return actions

    def _plan_priority_inbox(
        self,
        actions: list[ProposedAction],
        meta: ThreadMetadata,
        classification: Classification,
        action_label: str,
        is_crawl: bool,
    ) -> None:
        s = self._settings
        category = classification.category or ""
        thread_age = self._thread_age_days(meta)

        # Archive decision: everything except inbox-keepers
        should_archive = action_label not in s.inbox_archive_all_except
        if should_archive and (not is_crawl or thread_age >= s.crawl_archive_age_days):
            actions.append(ProposedAction(
                thread_id=meta.thread_id,
                action_type=ActionType.ARCHIVE,
                risk_level=RiskLevel.LOW,
                confidence=classification.confidence,
                reason=f"Priority inbox: {action_label} auto-archived",
                classified_by=classification.classified_by,
            ))

        # Mark-as-read for noise categories/keywords
        if self._should_mark_read(meta, category):
            actions.append(ProposedAction(
                thread_id=meta.thread_id,
                action_type=ActionType.MARK_READ,
                risk_level=RiskLevel.LOW,
                confidence=classification.confidence,
                reason=f"Noise pattern: {category}",
                classified_by=classification.classified_by,
            ))

        # Importance markers
        if action_label in s.inbox_mark_important:
            actions.append(ProposedAction(
                thread_id=meta.thread_id,
                action_type=ActionType.LABEL,
                risk_level=RiskLevel.SAFE,
                confidence=classification.confidence,
                reason=f"Mark important: {action_label}",
                classified_by=classification.classified_by,
                label_name="IMPORTANT",
            ))
        elif category in s.inbox_mark_not_important:
            actions.append(ProposedAction(
                thread_id=meta.thread_id,
                action_type=ActionType.REMOVE_LABEL,
                risk_level=RiskLevel.SAFE,
                confidence=classification.confidence,
                reason=f"Mark not important: {category}",
                classified_by=classification.classified_by,
                label_name="IMPORTANT",
            ))

    def _plan_relaxed_inbox(
        self,
        actions: list[ProposedAction],
        meta: ThreadMetadata,
        classification: Classification,
        is_crawl: bool,
    ) -> None:
        if classification.should_archive:
            thread_age = self._thread_age_days(meta)
            if not is_crawl or thread_age >= self._settings.crawl_archive_age_days:
                actions.append(ProposedAction(
                    thread_id=meta.thread_id,
                    action_type=ActionType.ARCHIVE,
                    risk_level=RiskLevel.LOW,
                    confidence=classification.confidence,
                    reason="Relaxed mode: explicit archive",
                    classified_by=classification.classified_by,
                ))

    def _should_mark_read(self, meta: ThreadMetadata, category: str) -> bool:
        s = self._settings
        if category in s.inbox_mark_read_categories:
            return True
        subject_lower = meta.subject.lower()
        for kw in s.inbox_mark_read_keywords:
            if kw.lower() in subject_lower:
                return True
        return False

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
    def _label_action(
        meta: ThreadMetadata, classification: Classification, label_name: str,
    ) -> ProposedAction:
        return ProposedAction(
            thread_id=meta.thread_id,
            action_type=ActionType.LABEL,
            risk_level=RiskLevel.SAFE,
            confidence=classification.confidence,
            reason=classification.reason,
            classified_by=classification.classified_by,
            label_name=label_name,
        )

    @staticmethod
    def _thread_age_days(meta: ThreadMetadata) -> int:
        if not meta.date:
            return 0
        delta = datetime.now(timezone.utc) - meta.date
        return max(0, delta.days)
