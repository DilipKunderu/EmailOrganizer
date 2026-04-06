from __future__ import annotations

import json
import logging

from src.engagement import EngagementTracker
from src.models import ALL_MANAGED_LABELS, DEFAULT_TENANT_ID
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class LabelChangeHandler:
    """Detects user overrides and engagement signals from Gmail label history events."""

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

            # Override detection: user removed an agent-applied managed label
            if not added and label_name in ALL_MANAGED_LABELS:
                await self._check_label_override(thread_id, label_name, removed=True)

            # Override detection: user added a managed label the agent didn't apply
            if added and label_name in ALL_MANAGED_LABELS:
                await self._check_label_override(thread_id, label_name, removed=False)

    async def _record_engagement(self, thread_id: str, event_type: str) -> None:
        thread_info = await self._store.get_thread_classification(self._tenant, thread_id)
        if not thread_info:
            return
        actions = await self._store.get_actions_for_thread(self._tenant, thread_id)
        if actions:
            await self._engagement.record_label_event(actions[0].sender, event_type)

    async def _check_label_override(
        self, thread_id: str, label_name: str, removed: bool
    ) -> None:
        thread_info = await self._store.get_thread_classification(self._tenant, thread_id)
        if not thread_info:
            return

        applied = json.loads(thread_info.get("applied_labels", "[]"))

        if removed and label_name in applied:
            actions = await self._store.get_actions_for_thread(self._tenant, thread_id)
            sender = actions[0].sender if actions else ""
            details = json.dumps({
                "type": "label_removed",
                "label": label_name,
                "agent_classification": thread_info.get("classification", ""),
            })
            await self._store.record_override(self._tenant, thread_id, sender, details)
            logger.info("Override: user removed '%s' from thread %s", label_name, thread_id)

        elif not removed and label_name not in applied:
            actions = await self._store.get_actions_for_thread(self._tenant, thread_id)
            sender = actions[0].sender if actions else ""
            details = json.dumps({
                "type": "label_added",
                "user_added_label": label_name,
                "agent_classification": thread_info.get("classification", ""),
            })
            await self._store.record_override(self._tenant, thread_id, sender, details)
            logger.info("Override: user added '%s' to thread %s", label_name, thread_id)
