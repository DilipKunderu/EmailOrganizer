from __future__ import annotations

import json
import logging

from src.classifier import HybridClassifier
from src.engagement import EngagementTracker
from src.executor import BatchExecutor
from src.gmail_client import GmailClient
from src.guardrails import Guardrails
from src.models import ALL_MANAGED_LABELS, DEFAULT_TENANT_ID
from src.planner import ActionPlanner
from src.ports.state_store import StateStorePort
from src.unsubscribe import UnsubscribeEngine

logger = logging.getLogger(__name__)


class ThreadPipeline:
    """Shared classify-plan-guard-execute pipeline used by both live sync and crawl."""

    def __init__(
        self,
        gmail: GmailClient,
        state_store: StateStorePort,
        classifier: HybridClassifier,
        planner: ActionPlanner,
        guardrails: Guardrails,
        executor: BatchExecutor,
        engagement: EngagementTracker,
        unsubscribe: UnsubscribeEngine,
        tenant_id: str = DEFAULT_TENANT_ID,
        label_id_to_name: dict[str, str] | None = None,
    ):
        self._gmail = gmail
        self._store = state_store
        self._classifier = classifier
        self._planner = planner
        self._guardrails = guardrails
        self._executor = executor
        self._engagement = engagement
        self._unsubscribe = unsubscribe
        self._tenant = tenant_id
        self._label_id_to_name = label_id_to_name or {}

    async def process(self, thread_id: str, is_crawl: bool) -> None:
        try:
            meta = self._gmail.get_thread_metadata(thread_id)

            if await self._store.is_sender_blocklisted(self._tenant, meta.sender):
                self._gmail.modify_thread(thread_id, remove_labels=["INBOX"])
                logger.debug("Blocklisted sender %s, auto-archived thread %s", meta.sender, thread_id)
                return

            prev = await self._store.get_thread_classification(self._tenant, thread_id)
            if prev:
                prev_labels = json.loads(prev.get("applied_labels", "[]"))
                current_managed = [
                    self._label_id_to_name.get(lid, lid)
                    for lid in meta.label_ids
                    if self._label_id_to_name.get(lid, lid) in ALL_MANAGED_LABELS
                ]
                if set(prev_labels) != set(current_managed) and prev_labels:
                    logger.debug("Thread %s has user label changes, skipping re-classification", thread_id)
                    await self._engagement.update_from_thread(meta)
                    return

            stats = await self._engagement.update_from_thread(meta)
            classification = await self._classifier.classify(meta)

            actions = self._planner.plan(meta, classification, stats, is_crawl=is_crawl)

            applied_labels: list[str] = []
            for action in actions:
                if action.label_name:
                    applied_labels.append(action.label_name)

                status = await self._guardrails.evaluate(action, meta)
                await self._executor.execute(action, status, meta)

                if (
                    action.action_type.value == "unsubscribe"
                    and status.value == "executed"
                ):
                    await self._unsubscribe.execute_unsubscribe(meta)

            await self._store.mark_thread_processed(
                self._tenant, thread_id,
                classification.category or "",
                classification.classified_by.value,
                classification.confidence,
                applied_labels=applied_labels,
            )

        except Exception as exc:
            logger.error("Error processing thread %s: %s", thread_id, exc, exc_info=True)
