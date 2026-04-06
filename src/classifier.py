from __future__ import annotations

import logging
import re
from typing import Any

from src.llm_provider import LLMProviderChain
from src.models import (
    AutoRule,
    Classification,
    ClassifiedBy,
    ManualRule,
    Settings,
    ThreadMetadata,
)

logger = logging.getLogger(__name__)


class HybridClassifier:
    """Three-stage classifier: manual rules -> auto-rules -> LLM fallback."""

    def __init__(
        self,
        settings: Settings,
        manual_rules: list[ManualRule],
        auto_rules: list[AutoRule],
        llm_chain: LLMProviderChain,
        classify_prompt: str = "",
    ):
        self._settings = settings
        self._manual_rules = manual_rules
        self._auto_rules = [r for r in auto_rules if r.status == "active"]
        self._llm = llm_chain
        self._prompt = classify_prompt

    def update_rules(
        self, manual: list[ManualRule] | None = None, auto: list[AutoRule] | None = None
    ) -> None:
        if manual is not None:
            self._manual_rules = manual
        if auto is not None:
            self._auto_rules = [r for r in auto if r.status == "active"]

    async def classify(self, meta: ThreadMetadata) -> Classification:
        # Stage 1: manual rules
        result = self._match_rules(meta, self._manual_rules, ClassifiedBy.MANUAL_RULE)
        if result and result.confidence >= self._settings.confidence_threshold:
            return result

        # Stage 2: auto-rules
        auto_result = self._match_auto_rules(meta)
        if auto_result and auto_result.confidence >= self._settings.confidence_threshold:
            return auto_result

        best_rule_result = result or auto_result

        # Stage 3: LLM (if confidence is still below threshold)
        if self._settings.llm_provider != "none" and self._llm.can_call():
            llm_result = await self._llm.classify_thread(meta, self._prompt)
            if llm_result and llm_result.confidence > 0:
                llm_result.classified_by = ClassifiedBy.LLM
                return llm_result

        # Fall back to best rule result (even if low confidence) or empty
        if best_rule_result:
            return best_rule_result
        return self._classify_by_gmail_category(meta)

    def _match_rules(
        self,
        meta: ThreadMetadata,
        rules: list[ManualRule],
        source: ClassifiedBy,
    ) -> Classification | None:
        best: Classification | None = None
        for rule in rules:
            matched = False
            if rule.type == "sender":
                matched = bool(re.search(rule.pattern, meta.sender, re.IGNORECASE))
            elif rule.type == "domain":
                matched = bool(re.search(rule.pattern, meta.sender_domain, re.IGNORECASE))
            elif rule.type == "keyword":
                matched = bool(re.search(rule.pattern, meta.subject, re.IGNORECASE))

            if matched:
                c = Classification(
                    category=rule.classification,
                    action_label=rule.action_label,
                    should_archive="archive" in rule.actions,
                    should_star="star" in rule.actions,
                    confidence=rule.confidence,
                    reason=f"{rule.type} rule: {rule.pattern}",
                    classified_by=source,
                )
                if best is None or c.confidence > best.confidence:
                    best = c
        return best

    def _match_auto_rules(self, meta: ThreadMetadata) -> Classification | None:
        best: Classification | None = None
        for rule in self._auto_rules:
            matched = False
            if rule.type == "sender":
                matched = bool(re.search(rule.pattern, meta.sender, re.IGNORECASE))
            elif rule.type == "domain":
                matched = bool(re.search(rule.pattern, meta.sender_domain, re.IGNORECASE))
            elif rule.type == "keyword":
                matched = bool(re.search(rule.pattern, meta.subject, re.IGNORECASE))

            if matched:
                c = Classification(
                    category=rule.classification,
                    action_label=rule.action_label,
                    should_archive="archive" in rule.actions,
                    should_star="star" in rule.actions,
                    confidence=rule.confidence,
                    reason=f"auto-{rule.type} rule: {rule.pattern}",
                    classified_by=ClassifiedBy.AUTO_RULE,
                )
                if best is None or c.confidence > best.confidence:
                    best = c
        return best

    @staticmethod
    def _classify_by_gmail_category(meta: ThreadMetadata) -> Classification:
        """Last resort: use Gmail's built-in categories."""
        category_map = {
            "CATEGORY_PROMOTIONS": ("Newsletters", True, False),
            "CATEGORY_SOCIAL": ("Personal", False, False),
            "CATEGORY_UPDATES": ("Accounts", True, False),
            "CATEGORY_FORUMS": ("Newsletters", False, False),
            "CATEGORY_PERSONAL": ("Personal", False, False),
        }
        for cat_id in meta.gmail_categories:
            if cat_id in category_map:
                cat_name, archive, star = category_map[cat_id]
                return Classification(
                    category=cat_name,
                    should_archive=archive,
                    should_star=star,
                    confidence=0.5,
                    reason=f"Gmail category: {cat_id}",
                    classified_by=ClassifiedBy.GMAIL_CATEGORY,
                )
        return Classification(
            confidence=0.3,
            reason="No rule matched, no Gmail category",
            classified_by=ClassifiedBy.GMAIL_CATEGORY,
        )
