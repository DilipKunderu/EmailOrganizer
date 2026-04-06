from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from src.models import (
    ActionStatus,
    ActionType,
    DEFAULT_TENANT_ID,
    ProposedAction,
    RiskLevel,
    Settings,
    ThreadMetadata,
)
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class Guardrails:
    """Tiered risk enforcement, dry-run, quarantine, never-touch, circuit breaker."""

    def __init__(
        self,
        settings: Settings,
        state_store: StateStorePort,
        tenant_id: str = DEFAULT_TENANT_ID,
        allowlist: dict[str, list[str]] | None = None,
    ):
        self._settings = settings
        self._store = state_store
        self._tenant = tenant_id
        self._allowlist = allowlist or {"domains": [], "addresses": []}
        self._destructive_this_run = 0

    async def is_dry_run_active(self) -> bool:
        start = await self._store.get_sync_value(self._tenant, "dry_run_start")
        if not start:
            await self._store.set_sync_value(
                self._tenant, "dry_run_start",
                datetime.now(timezone.utc).isoformat(),
            )
            return True
        start_dt = datetime.fromisoformat(start)
        return (datetime.now(timezone.utc) - start_dt).days < self._settings.dry_run_days

    async def evaluate(
        self, action: ProposedAction, meta: ThreadMetadata
    ) -> ActionStatus:
        """Decide the action status: executed, dry_run, quarantine, or skipped."""
        # Level 0: always execute
        if action.risk_level == RiskLevel.SAFE:
            return ActionStatus.EXECUTED

        # Never-touch rules (override everything for L1+)
        if action.risk_level >= RiskLevel.LOW and self._is_never_touch(meta):
            logger.debug("Never-touch match for thread %s", action.thread_id)
            return ActionStatus.SKIPPED

        # Dry-run check for L1+
        dry_run = await self.is_dry_run_active()
        if dry_run and action.risk_level >= RiskLevel.LOW:
            return ActionStatus.DRY_RUN

        # Level 1: execute after dry-run
        if action.risk_level == RiskLevel.LOW:
            return ActionStatus.EXECUTED

        # Level 2-3: circuit breaker
        if self._destructive_this_run >= self._settings.circuit_breaker_threshold:
            logger.warning(
                "Circuit breaker triggered: %d destructive actions this run",
                self._destructive_this_run,
            )
            return ActionStatus.QUARANTINE

        # Daily cap
        daily_count = await self._store.count_actions_today(
            self._tenant, RiskLevel.MEDIUM
        )
        if daily_count >= self._settings.daily_destructive_cap:
            logger.warning("Daily destructive cap reached (%d)", daily_count)
            return ActionStatus.QUARANTINE

        # Level 2-3: quarantine
        self._destructive_this_run += 1
        if action.risk_level >= RiskLevel.HIGH:
            return ActionStatus.QUARANTINE
        return ActionStatus.QUARANTINE

    def reset_run_counters(self) -> None:
        self._destructive_this_run = 0

    def _is_never_touch(self, meta: ThreadMetadata) -> bool:
        if meta.has_user_reply:
            return True
        if "STARRED" in meta.label_ids:
            return True
        if meta.date:
            age = datetime.now(timezone.utc) - meta.date
            if age < timedelta(hours=24):
                return True
        sender_lower = meta.sender.lower()
        for addr in self._allowlist.get("addresses", []):
            if addr.lower() in sender_lower:
                return True
        for domain in self._allowlist.get("domains", []):
            if meta.sender_domain.lower() == domain.lower():
                return True
        return False
