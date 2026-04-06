from __future__ import annotations

import json
import logging

from src.executor import BatchExecutor
from src.gmail_client import GmailClient
from src.models import (
    ActionStatus,
    ActionType,
    ClassifiedBy,
    DEFAULT_TENANT_ID,
    ProposedAction,
    RiskLevel,
    Settings,
)
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class QuarantineManager:
    """Handles quarantine lifecycle: expiration, user override detection, execution."""

    def __init__(
        self,
        gmail: GmailClient,
        state_store: StateStorePort,
        executor: BatchExecutor,
        settings: Settings,
        tenant_id: str = DEFAULT_TENANT_ID,
    ):
        self._gmail = gmail
        self._store = state_store
        self._executor = executor
        self._settings = settings
        self._tenant = tenant_id

    async def check_expirations(self) -> tuple[int, int]:
        """Check and process expired quarantine actions.

        Returns (executed_count, skipped_count).
        """
        expired = await self._store.get_quarantined_expired(
            self._tenant, self._settings.quarantine_hold_days,
        )
        if not expired:
            return 0, 0

        executed_count = 0
        skipped_count = 0

        for action_record in expired:
            try:
                meta = self._gmail.get_thread_metadata(action_record.thread_id)
                quarantine_label_id = self._gmail.get_label_id("_auto/quarantine")

                if quarantine_label_id and quarantine_label_id not in meta.label_ids:
                    await self._store.update_action_status(
                        action_record.id, ActionStatus.SKIPPED.value
                    )
                    await self._store.record_override(
                        self._tenant, action_record.thread_id,
                        action_record.sender,
                        json.dumps({
                            "type": "quarantine_rejected",
                            "action": action_record.action_type,
                        }),
                    )
                    skipped_count += 1
                else:
                    proposed = ProposedAction(
                        thread_id=action_record.thread_id,
                        action_type=ActionType(action_record.action_type),
                        risk_level=RiskLevel(action_record.risk_level),
                        confidence=0.9,
                        reason=f"Quarantine expired: {action_record.reason}",
                        classified_by=ClassifiedBy(action_record.classified_by),
                        label_name=action_record.label_name,
                    )
                    await self._executor.execute(proposed, ActionStatus.EXECUTED, meta)
                    await self._store.update_action_status(
                        action_record.id, "expired_executed"
                    )
                    self._gmail.modify_thread(
                        action_record.thread_id, remove_labels=["_auto/quarantine"]
                    )
                    executed_count += 1

            except Exception as exc:
                logger.error(
                    "Quarantine expiry error for %s: %s",
                    action_record.thread_id, exc,
                )

        if executed_count or skipped_count:
            logger.info(
                "Quarantine expiry: %d executed, %d skipped (user overrides)",
                executed_count, skipped_count,
            )
        return executed_count, skipped_count
