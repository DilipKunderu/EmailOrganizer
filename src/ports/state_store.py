from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from src.models import (
    ActionRecord,
    AutoRule,
    CrawlState,
    SenderStats,
    DEFAULT_TENANT_ID,
)


class StateStorePort(ABC):
    """Persistent state for processed threads, actions, sender stats, crawl, and rules."""

    @abstractmethod
    async def initialize(self) -> None:
        """Create tables / run migrations."""

    @abstractmethod
    async def close(self) -> None: ...

    # -- Processed threads --

    @abstractmethod
    async def mark_thread_processed(
        self,
        tenant_id: str,
        thread_id: str,
        classification: str,
        classified_by: str,
        confidence: float,
        applied_labels: list[str] | None = None,
    ) -> None: ...

    @abstractmethod
    async def is_thread_processed(self, tenant_id: str, thread_id: str) -> bool: ...

    @abstractmethod
    async def get_thread_classification(
        self, tenant_id: str, thread_id: str
    ) -> dict[str, Any] | None: ...

    # -- Action log --

    @abstractmethod
    async def log_action(self, record: ActionRecord) -> int:
        """Insert an action record, return its id."""

    @abstractmethod
    async def get_actions(
        self,
        tenant_id: str,
        *,
        thread_id: str | None = None,
        status: str | None = None,
        since: str | None = None,
        limit: int = 100,
    ) -> list[ActionRecord]: ...

    @abstractmethod
    async def update_action_status(self, action_id: int, new_status: str) -> None: ...

    @abstractmethod
    async def get_last_run_actions(self, tenant_id: str) -> list[ActionRecord]: ...

    @abstractmethod
    async def count_actions_today(
        self, tenant_id: str, min_risk_level: int
    ) -> int: ...

    @abstractmethod
    async def get_quarantined_expired(
        self, tenant_id: str, max_age_days: int
    ) -> list[ActionRecord]:
        """Get quarantined actions older than max_age_days."""

    @abstractmethod
    async def get_actions_for_thread(
        self, tenant_id: str, thread_id: str
    ) -> list[ActionRecord]:
        """Get all agent-applied actions for a specific thread."""

    # -- Sender stats --

    @abstractmethod
    async def upsert_sender_stats(self, stats: SenderStats) -> None: ...

    @abstractmethod
    async def get_sender_stats(
        self, tenant_id: str, sender: str
    ) -> SenderStats | None: ...

    @abstractmethod
    async def get_senders_by_engagement(
        self,
        tenant_id: str,
        max_score: float,
        min_emails: int,
        limit: int = 100,
    ) -> list[SenderStats]: ...

    @abstractmethod
    async def blocklist_sender(self, tenant_id: str, sender: str) -> None: ...

    @abstractmethod
    async def is_sender_blocklisted(self, tenant_id: str, sender: str) -> bool: ...

    # -- Sync state (key-value) --

    @abstractmethod
    async def get_sync_value(self, tenant_id: str, key: str) -> str | None: ...

    @abstractmethod
    async def set_sync_value(self, tenant_id: str, key: str, value: str) -> None: ...

    # -- Crawl --

    @abstractmethod
    async def get_crawl_state(self, tenant_id: str) -> CrawlState: ...

    @abstractmethod
    async def update_crawl_state(self, tenant_id: str, state: CrawlState) -> None: ...

    @abstractmethod
    async def reset_crawl(self, tenant_id: str) -> None: ...

    # -- User overrides --

    @abstractmethod
    async def record_override(
        self, tenant_id: str, thread_id: str, sender: str, details: str
    ) -> None: ...

    @abstractmethod
    async def get_overrides(
        self, tenant_id: str, sender: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]: ...

    # -- LLM usage --

    @abstractmethod
    async def log_llm_usage(
        self,
        tenant_id: str,
        provider: str,
        model: str,
        tokens_in: int,
        tokens_out: int,
        latency_ms: int,
        cost_estimate: float,
    ) -> None: ...

    @abstractmethod
    async def get_llm_usage_today(self, tenant_id: str) -> dict[str, Any]: ...

    # -- Rule candidates --

    @abstractmethod
    async def upsert_rule_candidate(self, tenant_id: str, rule: AutoRule) -> None: ...

    @abstractmethod
    async def get_rule_candidates(
        self, tenant_id: str, status: str | None = None
    ) -> list[AutoRule]: ...

    @abstractmethod
    async def update_rule_status(
        self, tenant_id: str, rule_id: str, new_status: str
    ) -> None: ...

    @abstractmethod
    async def log_rule_event(
        self, tenant_id: str, rule_id: str, event: str, details: str = ""
    ) -> None: ...

    # -- LLM dependency tracking --

    @abstractmethod
    async def record_llm_dependency(
        self, tenant_id: str, date: str, ratio: float
    ) -> None: ...

    @abstractmethod
    async def get_llm_dependency_trend(
        self, tenant_id: str, days: int = 30
    ) -> list[dict[str, Any]]: ...

    # -- Aggregations for learner / digest --

    @abstractmethod
    async def get_llm_classified_senders(
        self, tenant_id: str, min_threads: int
    ) -> list[dict[str, Any]]:
        """Senders with >= min_threads LLM-classified threads, grouped."""

    @abstractmethod
    async def count_classifications_by_source(
        self, tenant_id: str, since: str | None = None
    ) -> dict[str, int]:
        """Count of classifications grouped by classified_by."""
