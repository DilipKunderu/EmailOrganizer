from __future__ import annotations

import json
import logging

from src.engagement import EngagementTracker
from src.models import ALL_MANAGED_LABELS, CATEGORY_LABELS, ACTION_LABELS, DEFAULT_TENANT_ID
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class LabelChangeHandler:
    """Detects user overrides, records structured feedback, and tracks engagement."""

    def __init__(
        self,
        state_store: StateStorePort,
        engagement: EngagementTracker,
        tenant_id: str = DEFAULT_TENANT_ID,
        label_id_to_name: dict[str, str] | None = None,
    ):
        self._store = state_store
        self._engagement = engagement
        self._tenant = tenant_id
        self._label_id_to_name = label_id_to_name or {}

    async def handle(self, item: dict, added: bool) -> None:
        msg_data = item.get("message", {})
        thread_id = msg_data.get("threadId", "")
        label_ids = item.get("labelIds", [])
        if not thread_id or not label_ids:
            return

        for label_id in label_ids:
            label_name = self._label_id_to_name.get(label_id, label_id)

            # Engagement signals
            if not added and label_id == "UNREAD":
                await self._record_engagement(thread_id, "opened")
            if added and label_id == "STARRED":
                await self._record_engagement(thread_id, "starred")
            if added and label_id == "TRASH":
                await self._record_engagement(thread_id, "trashed")
            if added and label_id == "SPAM":
                await self._record_engagement(thread_id, "spammed")

            # Action outcome feedback: user undoing agent actions
            if added and label_id == "INBOX":
                await self._check_action_undo(thread_id, "un_archive")
            if not added and label_id == "STARRED":
                await self._check_action_undo(thread_id, "un_star")
            if not added and label_id == "SPAM":
                await self._check_action_undo(thread_id, "un_spam")

            # Override detection + structured feedback
            if not added and label_name in ALL_MANAGED_LABELS:
                await self._check_label_override(thread_id, label_name, removed=True)
            if added and label_name in ALL_MANAGED_LABELS:
                await self._check_label_override(thread_id, label_name, removed=False)

    async def _record_engagement(self, thread_id: str, event_type: str) -> None:
        thread_info = await self._store.get_thread_classification(self._tenant, thread_id)
        if not thread_info:
            return
        actions = await self._store.get_actions_for_thread(self._tenant, thread_id)
        if actions:
            await self._engagement.record_label_event(actions[0].sender, event_type)

    async def _check_action_undo(self, thread_id: str, undo_type: str) -> None:
        """Detect when user undoes an agent action (un-archive, un-star, un-spam)."""
        thread_info = await self._store.get_thread_classification(self._tenant, thread_id)
        if not thread_info:
            return

        actions = await self._store.get_actions_for_thread(self._tenant, thread_id)
        agent_did_action = False
        for a in actions:
            if undo_type == "un_archive" and a.action_type == "archive":
                agent_did_action = True
            elif undo_type == "un_star" and a.action_type == "star":
                agent_did_action = True
            elif undo_type == "un_spam" and a.action_type == "spam":
                agent_did_action = True

        if not agent_did_action:
            return

        sender = actions[0].sender if actions else ""
        await self._store.record_feedback(
            self._tenant, thread_id, sender,
            original_category=thread_info.get("classification", ""),
            original_action_label="",
            original_source=thread_info.get("classified_by", ""),
            corrected_category="",
            corrected_action_label="",
            feedback_type=undo_type,
        )
        logger.info("Action undo detected: %s on thread %s", undo_type, thread_id)

    async def _check_label_override(
        self, thread_id: str, label_name: str, removed: bool
    ) -> None:
        thread_info = await self._store.get_thread_classification(self._tenant, thread_id)
        if not thread_info:
            return

        applied = json.loads(thread_info.get("applied_labels", "[]"))
        actions = await self._store.get_actions_for_thread(self._tenant, thread_id)
        sender = actions[0].sender if actions else ""

        if removed and label_name in applied:
            details = json.dumps({
                "type": "label_removed",
                "label": label_name,
                "agent_classification": thread_info.get("classification", ""),
            })
            await self._store.record_override(self._tenant, thread_id, sender, details)

            await self._store.record_feedback(
                self._tenant, thread_id, sender,
                original_category=thread_info.get("classification", ""),
                original_action_label=self._find_action_label(applied),
                original_source=thread_info.get("classified_by", ""),
                corrected_category="" if label_name in ACTION_LABELS else "",
                corrected_action_label="" if label_name in CATEGORY_LABELS else "",
                feedback_type="label_removed",
            )
            logger.info("Override: user removed '%s' from thread %s", label_name, thread_id)

        elif not removed and label_name not in applied:
            corrected_cat = label_name if label_name in CATEGORY_LABELS else ""
            corrected_action = label_name if label_name in ACTION_LABELS else ""

            details = json.dumps({
                "type": "label_added",
                "user_added_label": label_name,
                "agent_classification": thread_info.get("classification", ""),
            })
            await self._store.record_override(self._tenant, thread_id, sender, details)

            await self._store.record_feedback(
                self._tenant, thread_id, sender,
                original_category=thread_info.get("classification", ""),
                original_action_label=self._find_action_label(applied),
                original_source=thread_info.get("classified_by", ""),
                corrected_category=corrected_cat,
                corrected_action_label=corrected_action,
                feedback_type="label_added",
            )
            logger.info("Override: user added '%s' to thread %s", label_name, thread_id)

    @staticmethod
    def _find_action_label(labels: list[str]) -> str:
        for l in labels:
            if l in ACTION_LABELS:
                return l
        return ""
