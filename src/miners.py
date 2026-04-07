from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone

from src.models import AutoRule, AutoRuleSource, DEFAULT_TENANT_ID, Settings
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class SenderMiner:
    """Discover sender-based rules from LLM classification consensus."""

    def __init__(self, store: StateStorePort, settings: Settings, tenant_id: str = DEFAULT_TENANT_ID):
        self._store = store
        self._settings = settings
        self._tenant = tenant_id

    async def mine(self) -> list[AutoRule]:
        actions = await self._store.get_actions(
            self._tenant, limit=10000,
        )
        llm_actions = [a for a in actions if a.classified_by == "llm" and a.status == "executed"]

        sender_groups: dict[str, list[str]] = defaultdict(list)
        for a in llm_actions:
            if a.action_type == "label" and a.label_name and a.sender:
                sender_groups[a.sender].append(a.label_name)

        candidates: list[AutoRule] = []
        for sender, classifications in sender_groups.items():
            if len(classifications) < self._settings.learner_sender_min_threads:
                continue
            counter = Counter(classifications)
            top_class, top_count = counter.most_common(1)[0]
            if top_count == len(classifications):
                overrides = await self._store.get_overrides(self._tenant, sender=sender)
                if overrides:
                    continue
                rule_id = f"auto_sender_{hashlib.md5(sender.encode()).hexdigest()[:12]}"
                candidates.append(AutoRule(
                    id=rule_id,
                    type="sender",
                    pattern=re.escape(sender),
                    classification=top_class,
                    actions=["label"],
                    status="candidate",
                    confidence=top_count / len(classifications),
                    source=AutoRuleSource.LLM_CONSENSUS.value,
                    evidence_count=len(classifications),
                    created_at=datetime.now(timezone.utc).isoformat(),
                    last_validated=datetime.now(timezone.utc).isoformat(),
                ))
        return candidates


class DomainMiner:
    """Aggregate sender rules to domain-level rules."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def mine(self, sender_rules: list[AutoRule]) -> list[AutoRule]:
        domain_groups: dict[str, list[AutoRule]] = defaultdict(list)
        for r in sender_rules:
            domain_match = re.search(r"@([\w.-]+)", r.pattern)
            if domain_match:
                domain_groups[domain_match.group(1).lower()].append(r)

        candidates: list[AutoRule] = []
        for domain, rules in domain_groups.items():
            if len(rules) < self._settings.learner_domain_min_senders:
                continue
            classifications = [r.classification for r in rules]
            counter = Counter(classifications)
            top_class, top_count = counter.most_common(1)[0]
            if top_count == len(rules):
                rule_id = f"auto_domain_{hashlib.md5(domain.encode()).hexdigest()[:12]}"
                candidates.append(AutoRule(
                    id=rule_id,
                    type="domain",
                    pattern=f".*@{re.escape(domain)}",
                    classification=top_class,
                    actions=["label"],
                    status="candidate",
                    confidence=min(r.confidence for r in rules),
                    source=AutoRuleSource.LLM_CONSENSUS.value,
                    evidence_count=sum(r.evidence_count for r in rules),
                    created_at=datetime.now(timezone.utc).isoformat(),
                    last_validated=datetime.now(timezone.utc).isoformat(),
                ))
        return candidates


class EngagementMiner:
    """Generate permanent blocklist rules from long-term zero-engagement senders."""

    def __init__(self, store: StateStorePort, settings: Settings, tenant_id: str = DEFAULT_TENANT_ID):
        self._store = store
        self._settings = settings
        self._tenant = tenant_id

    async def mine(self) -> list[AutoRule]:
        senders = await self._store.get_senders_by_engagement(
            self._tenant, max_score=0.0, min_emails=10, limit=500,
        )
        candidates: list[AutoRule] = []
        for s in senders:
            if s.blocklisted:
                continue
            rule_id = f"auto_block_{hashlib.md5(s.sender.encode()).hexdigest()[:12]}"
            candidates.append(AutoRule(
                id=rule_id,
                type="sender",
                pattern=re.escape(s.sender),
                classification="Newsletters",
                actions=["archive", "unsubscribe"],
                status="candidate",
                confidence=1.0,
                source=AutoRuleSource.ENGAGEMENT.value,
                evidence_count=s.total_received,
                created_at=datetime.now(timezone.utc).isoformat(),
                last_validated=datetime.now(timezone.utc).isoformat(),
            ))
        return candidates


class KeywordMiner:
    """Discover keyword-based rules from subject-line patterns in LLM-classified threads."""

    def __init__(self, store: StateStorePort, settings: Settings, tenant_id: str = DEFAULT_TENANT_ID):
        self._store = store
        self._settings = settings
        self._tenant = tenant_id

    async def mine(self) -> list[AutoRule]:
        actions = await self._store.get_actions(self._tenant, limit=10000)
        llm_actions = [
            a for a in actions
            if a.classified_by == "llm" and a.action_type == "label"
            and a.label_name and a.reason
        ]

        # Extract tokens from reasons/subjects, group by classification
        token_class_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        token_total: dict[str, int] = defaultdict(int)
        stop_words = {"the", "a", "an", "is", "are", "was", "were", "your", "you",
                      "to", "from", "for", "in", "on", "at", "of", "and", "or",
                      "has", "have", "been", "this", "that", "with", "not", "no"}

        for a in llm_actions:
            words = re.findall(r'\b[a-zA-Z]{3,}\b', a.reason.lower())
            unique_words = set(words) - stop_words
            for word in unique_words:
                token_class_counts[word][a.label_name] += 1
                token_total[word] += 1

        candidates: list[AutoRule] = []
        min_correlation = self._settings.learner_keyword_min_correlation
        min_evidence = 5

        for token, class_counts in token_class_counts.items():
            total = token_total[token]
            if total < min_evidence:
                continue
            top_class, top_count = Counter(class_counts).most_common(1)[0]
            correlation = top_count / total
            if correlation >= min_correlation:
                rule_id = f"auto_kw_{hashlib.md5(token.encode()).hexdigest()[:12]}"
                candidates.append(AutoRule(
                    id=rule_id,
                    type="keyword",
                    pattern=re.escape(token),
                    classification=top_class,
                    actions=["label"],
                    status="candidate",
                    confidence=correlation,
                    source=AutoRuleSource.KEYWORD.value,
                    evidence_count=total,
                    created_at=datetime.now(timezone.utc).isoformat(),
                    last_validated=datetime.now(timezone.utc).isoformat(),
                ))

        return candidates


class OverrideMiner:
    """Generate rules from repeated user corrections."""

    def __init__(self, store: StateStorePort, tenant_id: str = DEFAULT_TENANT_ID):
        self._store = store
        self._tenant = tenant_id

    async def mine(self) -> list[AutoRule]:
        overrides = await self._store.get_overrides(self._tenant, limit=1000)

        sender_data: dict[str, list[str]] = defaultdict(list)
        for o in overrides:
            sender = o.get("sender", "")
            if not sender:
                continue
            details = o.get("details", "")
            try:
                detail_obj = json.loads(details) if details else {}
            except (json.JSONDecodeError, TypeError):
                detail_obj = {}
            user_label = detail_obj.get("user_added_label", "")
            if user_label:
                sender_data[sender].append(user_label)
            else:
                sender_data[sender].append("")

        candidates: list[AutoRule] = []
        for sender, labels in sender_data.items():
            if len(labels) < 2:
                continue
            non_empty = [l for l in labels if l]
            if non_empty:
                classification = Counter(non_empty).most_common(1)[0][0]
            else:
                classification = "Personal"
            rule_id = f"auto_override_{hashlib.md5(sender.encode()).hexdigest()[:12]}"
            candidates.append(AutoRule(
                id=rule_id,
                type="sender",
                pattern=re.escape(sender),
                classification=classification,
                actions=["label"],
                status="candidate",
                confidence=0.95,
                source=AutoRuleSource.USER_OVERRIDE.value,
                evidence_count=len(labels),
                created_at=datetime.now(timezone.utc).isoformat(),
                last_validated=datetime.now(timezone.utc).isoformat(),
            ))
        return candidates
