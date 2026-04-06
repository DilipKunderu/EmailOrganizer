from __future__ import annotations

import logging
import re

from src.models import AutoRule, DEFAULT_TENANT_ID, Settings
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class Backtester:
    """Replay candidate rules against historical action_log to measure accuracy."""

    def __init__(self, store: StateStorePort, settings: Settings, tenant_id: str = DEFAULT_TENANT_ID):
        self._store = store
        self._settings = settings
        self._tenant = tenant_id

    async def test_rule(self, rule: AutoRule) -> float:
        """Test a candidate rule against historical data. Returns accuracy 0.0-1.0."""
        actions = await self._store.get_actions(self._tenant, limit=5000)
        relevant = [
            a for a in actions
            if a.classified_by == "llm" and a.action_type == "label"
        ]

        matches = 0
        correct = 0
        for a in relevant:
            if self._rule_would_match(rule, a):
                matches += 1
                if a.label_name == rule.classification:
                    correct += 1

        if matches == 0:
            return 0.0

        accuracy = correct / matches
        rule.accuracy = accuracy
        rule.evidence_count = matches
        logger.info(
            "Backtest rule %s: %d/%d correct (%.1f%%)",
            rule.id, correct, matches, accuracy * 100,
        )
        return accuracy

    def _rule_would_match(self, rule: AutoRule, action: dict | object) -> bool:
        reason = getattr(action, "reason", "") or ""
        if rule.type == "sender":
            return bool(re.search(rule.pattern, reason, re.IGNORECASE))
        elif rule.type == "domain":
            return bool(re.search(rule.pattern, reason, re.IGNORECASE))
        elif rule.type == "keyword":
            return bool(re.search(rule.pattern, reason, re.IGNORECASE))
        return False
