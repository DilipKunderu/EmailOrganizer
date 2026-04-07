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
            return self._ensure_action_label(result, meta)

        # Stage 2: auto-rules
        auto_result = self._match_auto_rules(meta)
        if auto_result and auto_result.confidence >= self._settings.confidence_threshold:
            return self._ensure_action_label(auto_result, meta)

        best_rule_result = result or auto_result

        # Stage 3: LLM (if confidence is still below threshold)
        if self._settings.llm_provider != "none" and self._llm.can_call():
            llm_result = await self._llm.classify_thread(meta, self._prompt)
            if llm_result and llm_result.confidence > 0:
                llm_result.classified_by = ClassifiedBy.LLM
                return self._ensure_action_label(llm_result, meta)

        # Fall back to best rule result (even if low confidence) or empty
        if best_rule_result:
            return self._ensure_action_label(best_rule_result, meta)
        return self._ensure_action_label(self._classify_by_gmail_category(meta), meta)

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
            "CATEGORY_PROMOTIONS": ("Newsletters", "@Reference"),
            "CATEGORY_SOCIAL": ("Personal", "@Reference"),
            "CATEGORY_UPDATES": ("Accounts", "@Reference"),
            "CATEGORY_FORUMS": ("Newsletters", "@Read"),
            "CATEGORY_PERSONAL": ("Personal", "@Reference"),
        }
        for cat_id in meta.gmail_categories:
            if cat_id in category_map:
                cat_name, action_label = category_map[cat_id]
                return Classification(
                    category=cat_name,
                    action_label=action_label,
                    confidence=0.5,
                    reason=f"Gmail category: {cat_id}",
                    classified_by=ClassifiedBy.GMAIL_CATEGORY,
                )
        return Classification(
            action_label="@Reference",
            confidence=0.3,
            reason="No rule matched, no Gmail category",
            classified_by=ClassifiedBy.GMAIL_CATEGORY,
        )

    @staticmethod
    def _ensure_action_label(c: Classification, meta: ThreadMetadata) -> Classification:
        """Guarantee every classification has an action label."""
        if c.action_label:
            return c
        if c.category in ("Newsletters",):
            c.action_label = "@Read" if not meta.has_unsubscribe else "@Reference"
        elif c.category in ("Shopping", "Accounts"):
            c.action_label = "@Reference"
        elif c.category in ("Finance", "Travel"):
            c.action_label = "@Reference"
        elif c.category == "Personal":
            c.action_label = "@Action"
        else:
            c.action_label = "@Reference"
        return c
