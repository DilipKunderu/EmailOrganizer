from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from src.gmail_client import GmailClient
from src.models import (
    ActionRecord,
    ActionStatus,
    ActionType,
    DEFAULT_TENANT_ID,
    ProposedAction,
    RiskLevel,
    ThreadMetadata,
)
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class BatchExecutor:
    """Executes proposed actions via Gmail API and logs to state store."""

    def __init__(
        self,
        gmail: GmailClient,
        state_store: StateStorePort,
        tenant_id: str = DEFAULT_TENANT_ID,
    ):
        self._gmail = gmail
        self._store = state_store
        self._tenant = tenant_id

    async def execute(
        self,
        action: ProposedAction,
        status: ActionStatus,
        meta: ThreadMetadata,
    ) -> ActionRecord:
        record = ActionRecord(
            tenant_id=self._tenant,
            thread_id=action.thread_id,
            action_type=action.action_type.value,
            risk_level=action.risk_level,
            status=status.value,
            reason=action.reason,
            classified_by=action.classified_by.value,
            label_name=action.label_name,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

        if status == ActionStatus.EXECUTED:
            reversal = self._perform_action(action, meta)
            record.reversal_data = json.dumps(reversal) if reversal else None
        elif status == ActionStatus.DRY_RUN:
            self._apply_dry_run_label(action)
        elif status == ActionStatus.QUARANTINE:
            self._apply_quarantine_label(action)

        record.id = await self._store.log_action(record)
        return record

    async def execute_batch(
        self,
        actions_with_status: list[tuple[ProposedAction, ActionStatus, ThreadMetadata]],
    ) -> list[ActionRecord]:
        records: list[ActionRecord] = []
        batch_mods: list[dict] = []

        for action, status, meta in actions_with_status:
            record = ActionRecord(
                tenant_id=self._tenant,
                thread_id=action.thread_id,
                action_type=action.action_type.value,
                risk_level=action.risk_level,
                status=status.value,
                reason=action.reason,
                classified_by=action.classified_by.value,
                label_name=action.label_name,
                created_at=datetime.now(timezone.utc).isoformat(),
            )

            if status == ActionStatus.EXECUTED:
                mod = self._build_modification(action)
                if mod:
                    batch_mods.append(mod)
                    record.reversal_data = json.dumps(self._build_reversal(action))
            elif status == ActionStatus.DRY_RUN:
                dry_label = self._gmail.get_label_id("_auto/dry-run")
                if dry_label:
                    batch_mods.append({
                        "thread_id": action.thread_id,
                        "add_labels": ["_auto/dry-run"],
                    })
            elif status == ActionStatus.QUARANTINE:
                q_label = self._gmail.get_label_id("_auto/quarantine")
                if q_label:
                    batch_mods.append({
                        "thread_id": action.thread_id,
                        "add_labels": ["_auto/quarantine"],
                    })

            record.id = await self._store.log_action(record)
            records.append(record)

        if batch_mods:
            try:
                self._gmail.batch_modify_threads(batch_mods)
            except Exception as exc:
                logger.error("Batch modify failed: %s", exc)

        return records

    async def undo_actions(self, actions: list[ActionRecord]) -> int:
        undone = 0
        for action in actions:
            if action.status != ActionStatus.EXECUTED.value:
                continue
            if not action.reversal_data:
                continue
            try:
                reversal = json.loads(action.reversal_data)
                self._perform_reversal(action.thread_id, reversal)
                await self._store.update_action_status(action.id, ActionStatus.UNDONE.value)  # type: ignore
                undone += 1
            except Exception as exc:
                logger.error("Undo failed for action %s: %s", action.id, exc)
        return undone

    def _perform_action(self, action: ProposedAction, meta: ThreadMetadata) -> dict | None:
        at = action.action_type
        if at == ActionType.LABEL and action.label_name:
            self._gmail.modify_thread(action.thread_id, add_labels=[action.label_name])
            return {"type": "remove_label", "label": action.label_name}
        elif at == ActionType.REMOVE_LABEL and action.label_name:
            self._gmail.modify_thread(action.thread_id, remove_labels=[action.label_name])
            return {"type": "add_label", "label": action.label_name}
        elif at == ActionType.ARCHIVE:
            self._gmail.modify_thread(action.thread_id, remove_labels=["INBOX"])
            return {"type": "unarchive"}
        elif at == ActionType.UNARCHIVE:
            self._gmail.modify_thread(action.thread_id, add_labels=["INBOX"])
            return None
        elif at == ActionType.STAR:
            self._gmail.modify_thread(action.thread_id, add_labels=["STARRED"])
            return {"type": "unstar"}
        elif at == ActionType.MARK_READ:
            self._gmail.modify_thread(action.thread_id, remove_labels=["UNREAD"])
            return {"type": "mark_unread"}
        elif at == ActionType.SPAM:
            self._gmail.modify_thread(action.thread_id, add_labels=["SPAM"], remove_labels=["INBOX"])
            return {"type": "unspam"}
        elif at == ActionType.TRASH:
            self._gmail.trash_thread(action.thread_id)
            return {"type": "untrash"}
        return None

    def _apply_dry_run_label(self, action: ProposedAction) -> None:
        try:
            self._gmail.modify_thread(action.thread_id, add_labels=["_auto/dry-run"])
        except Exception:
            pass

    def _apply_quarantine_label(self, action: ProposedAction) -> None:
        try:
            self._gmail.modify_thread(action.thread_id, add_labels=["_auto/quarantine"])
        except Exception:
            pass

    def _build_modification(self, action: ProposedAction) -> dict | None:
        at = action.action_type
        if at == ActionType.LABEL and action.label_name:
            return {"thread_id": action.thread_id, "add_labels": [action.label_name]}
        elif at == ActionType.ARCHIVE:
            return {"thread_id": action.thread_id, "remove_labels": ["INBOX"]}
        elif at == ActionType.STAR:
            return {"thread_id": action.thread_id, "add_labels": ["STARRED"]}
        elif at == ActionType.MARK_READ:
            return {"thread_id": action.thread_id, "remove_labels": ["UNREAD"]}
        elif at == ActionType.SPAM:
            return {"thread_id": action.thread_id, "add_labels": ["SPAM"], "remove_labels": ["INBOX"]}
        return None

    def _build_reversal(self, action: ProposedAction) -> dict:
        at = action.action_type
        if at == ActionType.LABEL:
            return {"type": "remove_label", "label": action.label_name}
        elif at == ActionType.ARCHIVE:
            return {"type": "unarchive"}
        elif at == ActionType.STAR:
            return {"type": "unstar"}
        elif at == ActionType.MARK_READ:
            return {"type": "mark_unread"}
        elif at == ActionType.SPAM:
            return {"type": "unspam"}
        elif at == ActionType.TRASH:
            return {"type": "untrash"}
        return {}

    def _perform_reversal(self, thread_id: str, reversal: dict) -> None:
        rtype = reversal.get("type", "")
        if rtype == "remove_label":
            self._gmail.modify_thread(thread_id, remove_labels=[reversal["label"]])
        elif rtype == "add_label":
            self._gmail.modify_thread(thread_id, add_labels=[reversal["label"]])
        elif rtype == "unarchive":
            self._gmail.modify_thread(thread_id, add_labels=["INBOX"])
        elif rtype == "unstar":
            self._gmail.modify_thread(thread_id, remove_labels=["STARRED"])
        elif rtype == "mark_unread":
            self._gmail.modify_thread(thread_id, add_labels=["UNREAD"])
        elif rtype == "unspam":
            self._gmail.modify_thread(thread_id, remove_labels=["SPAM"], add_labels=["INBOX"])
        elif rtype == "untrash":
            self._gmail.untrash_thread(thread_id)
