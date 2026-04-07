from __future__ import annotations

import logging
import re

from src.models import ActionRecord, AutoRule, DEFAULT_TENANT_ID, Settings
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class Backtester:
    """Replay candidate rules against historical action_log with temporal holdout."""

    def __init__(self, store: StateStorePort, settings: Settings, tenant_id: str = DEFAULT_TENANT_ID):
        self._store = store
        self._settings = settings
        self._tenant = tenant_id

    async def test_rule(self, rule: AutoRule, holdout_ratio: float = 0.3) -> float:
        """Test a rule against historical data with temporal train/test split.

        Mines patterns from the first (1 - holdout_ratio) of data,
        tests on the last holdout_ratio. Returns accuracy 0.0-1.0.
        """
        actions = await self._store.get_actions(self._tenant, limit=5000)
        relevant = [
            a for a in actions
            if a.classified_by == "llm" and a.action_type == "label" and a.sender
        ]

        if not relevant:
            return 0.0

        relevant.sort(key=lambda a: a.created_at)
        split_idx = int(len(relevant) * (1 - holdout_ratio))
        test_set = relevant[split_idx:]

        if not test_set:
            test_set = relevant

        matches = 0
        correct = 0
        for a in test_set:
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
            "Backtest rule %s: %d/%d correct (%.1f%%) [test set: %d items]",
            rule.id, correct, matches, accuracy * 100, len(test_set),
        )
        return accuracy

    @staticmethod
    def _rule_would_match(rule: AutoRule, action: ActionRecord) -> bool:
        if rule.type == "sender" and action.sender:
            return bool(re.search(rule.pattern, action.sender, re.IGNORECASE))
        elif rule.type == "domain" and action.sender_domain:
            return bool(re.search(rule.pattern, action.sender_domain, re.IGNORECASE))
        elif rule.type == "keyword":
            return bool(re.search(rule.pattern, action.reason, re.IGNORECASE))
        return False
