from __future__ import annotations

import asyncio
import gc
import json
import logging
import os
import signal
import time
from datetime import datetime, timedelta, timezone

from src.adapters.factory import AdapterSet
from src.classifier import HybridClassifier
from src.crawl import BackgroundCrawler
from src.digest import DigestBuilder
from src.engagement import EngagementTracker
from src.executor import BatchExecutor
from src.gmail_client import GmailClient
from src.guardrails import Guardrails
from src.llm_provider import LLMProviderChain, create_llm_provider
from src.models import (
    ALL_MANAGED_LABELS,
    ActionStatus,
    DEFAULT_TENANT_ID,
    HealthStatus,
    Settings,
)
from src.planner import ActionPlanner
from src.rule_learner import RuleLearner
from src.unsubscribe import UnsubscribeEngine

logger = logging.getLogger(__name__)


class Daemon:
    """Main daemon: orchestrates IMAP IDLE, background crawl, and maintenance."""

    def __init__(self, adapters: AdapterSet):
        self._adapters = adapters
        self._settings: Settings | None = None
        self._gmail: GmailClient | None = None
        self._llm: LLMProviderChain | None = None
        self._classifier: HybridClassifier | None = None
        self._planner: ActionPlanner | None = None
        self._guardrails: Guardrails | None = None
        self._executor: BatchExecutor | None = None
        self._engagement: EngagementTracker | None = None
        self._unsubscribe: UnsubscribeEngine | None = None
        self._crawler: BackgroundCrawler | None = None
        self._digest: DigestBuilder | None = None
        self._rule_learner: RuleLearner | None = None
        self._running = False
        self._start_time = 0.0
        self._last_live_count = 0
        self._label_id_to_name: dict[str, str] = {}

    async def start(self) -> None:
        logger.info("Starting Email Organizer daemon")
        self._start_time = time.monotonic()

        self._settings = await self._adapters.config_loader.load_settings()
        await self._adapters.state_store.initialize()

        creds = await self._adapters.auth.get_credentials(DEFAULT_TENANT_ID)
        self._gmail = GmailClient(creds)
        user_email = self._gmail.get_user_email()
        logger.info("Authenticated as %s", user_email)

        label_map = self._gmail.provision_labels()
        self._label_id_to_name = {v: k for k, v in label_map.items()}

        self._llm = create_llm_provider(self._settings)
        manual_rules = await self._adapters.config_loader.load_manual_rules()
        auto_rules = await self._adapters.config_loader.load_auto_rules()
        classify_prompt = ""
        try:
            classify_prompt = await self._adapters.config_loader.load_prompt("classify")
        except FileNotFoundError:
            pass

        self._classifier = HybridClassifier(
            self._settings, manual_rules, auto_rules, self._llm, classify_prompt,
        )
        self._planner = ActionPlanner(self._settings)

        allowlist = await self._adapters.config_loader.load_allowlist()
        self._guardrails = Guardrails(
            self._settings, self._adapters.state_store,
            DEFAULT_TENANT_ID, allowlist,
        )
        self._executor = BatchExecutor(
            self._gmail, self._adapters.state_store, DEFAULT_TENANT_ID,
        )
        self._engagement = EngagementTracker(
            self._adapters.state_store, DEFAULT_TENANT_ID,
        )
        self._unsubscribe = UnsubscribeEngine(
            self._gmail, self._adapters.state_store,
            self._settings, DEFAULT_TENANT_ID,
        )
        self._crawler = BackgroundCrawler(
            self._gmail, self._adapters.state_store,
            self._settings, DEFAULT_TENANT_ID,
        )
        self._digest = DigestBuilder(
            self._gmail, self._adapters.state_store,
            self._llm, self._settings, DEFAULT_TENANT_ID,
        )
        self._rule_learner = RuleLearner(
            self._adapters.state_store, self._adapters.config_loader,
            self._settings, DEFAULT_TENANT_ID,
        )

        history_id = await self._adapters.state_store.get_sync_value(
            DEFAULT_TENANT_ID, "history_id"
        )
        if not history_id:
            history_id = self._gmail.get_current_history_id()
            await self._adapters.state_store.set_sync_value(
                DEFAULT_TENANT_ID, "history_id", history_id,
            )

        await self._adapters.process_manager.start_health_server()
        await self._write_health()

        self._running = True
        logger.info("Daemon startup complete")

    async def run_forever(self) -> None:
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, self._handle_sigterm)
        loop.add_signal_handler(signal.SIGHUP, self._handle_sighup)

        await asyncio.gather(
            self._live_sync_loop(),
            self._crawl_loop(),
            self._maintenance_loop(),
        )

    async def run_once(self) -> None:
        await self._live_sync_cycle()
        if not await self._crawler.is_complete():
            await self._crawl_cycle()

    async def stop(self) -> None:
        logger.info("Stopping daemon")
        self._running = False
        await self._adapters.mail_notifier.disconnect()
        await self._adapters.state_store.close()
        await self._adapters.process_manager.stop_health_server()

    # -- Workstream 1: Live sync --

    async def _live_sync_loop(self) -> None:
        token = await self._adapters.auth.get_access_token(DEFAULT_TENANT_ID)
        try:
            await self._adapters.mail_notifier.connect(token)
        except Exception as exc:
            logger.error("Initial IMAP connect failed: %s", exc)

        while self._running:
            try:
                new_mail = await self._adapters.mail_notifier.wait_for_mail()
                if new_mail:
                    await self._live_sync_cycle()
                await self._write_health()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Live sync error: %s", exc, exc_info=True)
                await asyncio.sleep(30)

    # -- Workstream 2: Background crawl --

    async def _crawl_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._settings.crawl_interval_minutes * 60)
                if self._last_live_count > self._settings.crawl_skip_threshold:
                    logger.debug("Skipping crawl: live sync was busy (%d threads)", self._last_live_count)
                    continue
                await self._crawl_cycle()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Crawl error: %s", exc, exc_info=True)

    # -- Workstream 3: Maintenance --

    async def _maintenance_loop(self) -> None:
        last_quarantine_check = 0.0
        last_daily_tasks = 0.0

        while self._running:
            try:
                await asyncio.sleep(300)  # check every 5 minutes
                now = time.monotonic()

                # Hourly: quarantine expiration
                if now - last_quarantine_check >= 3600:
                    await self._check_quarantine_expirations()
                    last_quarantine_check = now

                # Daily: rule learner, unsub persistence, digest
                if now - last_daily_tasks >= 86400:
                    await self._run_daily_tasks()
                    last_daily_tasks = now

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Maintenance error: %s", exc, exc_info=True)

    async def _run_daily_tasks(self) -> None:
        logger.info("Running daily maintenance tasks")

        # Rule learner
        try:
            stats = await self._rule_learner.run()
            logger.info("Rule learner: %s", stats)
            auto_rules = await self._adapters.config_loader.load_auto_rules()
            self._classifier.update_rules(auto=auto_rules)
        except Exception as exc:
            logger.error("Rule learner failed: %s", exc)

        # Unsubscribe persistence check
        try:
            senders = await self._adapters.state_store.get_senders_by_engagement(
                DEFAULT_TENANT_ID, max_score=0.0, min_emails=1, limit=500,
            )
            blocklisted = [s for s in senders if s.blocklisted]
            for s in blocklisted:
                await self._unsubscribe.check_persistence(s.sender)
        except Exception as exc:
            logger.error("Unsub persistence check failed: %s", exc)

        # Digest
        try:
            if await self._digest.should_send():
                crawl_state = await self._crawler.get_progress()
                await self._digest.build_and_send(crawl_state)
        except Exception as exc:
            logger.error("Digest send failed: %s", exc)

    # -- Live sync cycle --

    async def _live_sync_cycle(self) -> None:
        store = self._adapters.state_store
        history_id = await store.get_sync_value(DEFAULT_TENANT_ID, "history_id")
        if not history_id:
            return

        try:
            history, new_id = self._gmail.get_history(history_id)
        except Exception as exc:
            logger.error("History fetch failed: %s", exc)
            return

        new_thread_ids: set[str] = set()
        updated_thread_ids: set[str] = set()

        for record in history:
            # New messages
            for msg in record.get("messagesAdded", []):
                tid = msg.get("message", {}).get("threadId")
                if tid:
                    new_thread_ids.add(tid)

            # Label changes -- detect user overrides and engagement signals
            for item in record.get("labelsAdded", []):
                await self._handle_label_change(item, added=True)
                tid = item.get("message", {}).get("threadId")
                if tid:
                    updated_thread_ids.add(tid)

            for item in record.get("labelsRemoved", []):
                await self._handle_label_change(item, added=False)
                tid = item.get("message", {}).get("threadId")
                if tid:
                    updated_thread_ids.add(tid)

        self._llm.reset_run_counter()
        self._guardrails.reset_run_counters()

        # Process genuinely new threads
        processed = 0
        for tid in new_thread_ids:
            if await store.is_thread_processed(DEFAULT_TENANT_ID, tid):
                # Re-fetch metadata to update engagement (new reply, read status)
                try:
                    meta = self._gmail.get_thread_metadata(tid)
                    await self._engagement.update_from_thread(meta)
                except Exception:
                    pass
                continue
            await self._process_thread(tid, is_crawl=False)
            processed += 1

        self._last_live_count = processed
        await store.set_sync_value(DEFAULT_TENANT_ID, "history_id", new_id)
        if processed:
            logger.info("Live sync: processed %d new threads, %d label changes",
                        processed, len(updated_thread_ids))

    async def _handle_label_change(self, item: dict, added: bool) -> None:
        """Detect user overrides and engagement signals from label changes."""
        msg_data = item.get("message", {})
        thread_id = msg_data.get("threadId", "")
        label_ids = item.get("labelIds", [])
        if not thread_id or not label_ids:
            return

        store = self._adapters.state_store

        for label_id in label_ids:
            label_name = self._label_id_to_name.get(label_id, label_id)

            # Engagement signals from label changes
            if not added and label_id == "UNREAD":
                thread_info = await store.get_thread_classification(DEFAULT_TENANT_ID, thread_id)
                if thread_info:
                    # Look up sender from action log
                    actions = await store.get_actions_for_thread(DEFAULT_TENANT_ID, thread_id)
                    if actions:
                        await self._engagement.record_label_event(actions[0].sender, "opened")

            if added and label_id == "STARRED":
                actions = await store.get_actions_for_thread(DEFAULT_TENANT_ID, thread_id)
                if actions:
                    await self._engagement.record_label_event(actions[0].sender, "starred")

            if added and label_id == "TRASH":
                actions = await store.get_actions_for_thread(DEFAULT_TENANT_ID, thread_id)
                if actions:
                    await self._engagement.record_label_event(actions[0].sender, "trashed")

            if added and label_id == "SPAM":
                actions = await store.get_actions_for_thread(DEFAULT_TENANT_ID, thread_id)
                if actions:
                    await self._engagement.record_label_event(actions[0].sender, "spammed")

            # Override detection: user removed an agent-applied managed label
            if not added and label_name in ALL_MANAGED_LABELS:
                thread_info = await store.get_thread_classification(DEFAULT_TENANT_ID, thread_id)
                if not thread_info:
                    continue
                applied = json.loads(thread_info.get("applied_labels", "[]"))
                if label_name in applied:
                    actions = await store.get_actions_for_thread(DEFAULT_TENANT_ID, thread_id)
                    sender = actions[0].sender if actions else ""
                    details = json.dumps({
                        "type": "label_removed",
                        "label": label_name,
                        "agent_classification": thread_info.get("classification", ""),
                    })
                    await store.record_override(DEFAULT_TENANT_ID, thread_id, sender, details)
                    logger.info("Override detected: user removed '%s' from thread %s", label_name, thread_id)

            # Override detection: user added a managed label the agent didn't apply
            if added and label_name in ALL_MANAGED_LABELS:
                thread_info = await store.get_thread_classification(DEFAULT_TENANT_ID, thread_id)
                if not thread_info:
                    continue
                applied = json.loads(thread_info.get("applied_labels", "[]"))
                if label_name not in applied:
                    actions = await store.get_actions_for_thread(DEFAULT_TENANT_ID, thread_id)
                    sender = actions[0].sender if actions else ""
                    details = json.dumps({
                        "type": "label_added",
                        "user_added_label": label_name,
                        "agent_classification": thread_info.get("classification", ""),
                    })
                    await store.record_override(DEFAULT_TENANT_ID, thread_id, sender, details)
                    logger.info("Override detected: user added '%s' to thread %s", label_name, thread_id)

    # -- Quarantine expiration --

    async def _check_quarantine_expirations(self) -> None:
        store = self._adapters.state_store
        expired = await store.get_quarantined_expired(
            DEFAULT_TENANT_ID, self._settings.quarantine_hold_days,
        )
        if not expired:
            return

        executed_count = 0
        skipped_count = 0
        for action_record in expired:
            try:
                meta = self._gmail.get_thread_metadata(action_record.thread_id)
                quarantine_label_id = self._gmail.get_label_id("_auto/quarantine")

                # If user removed quarantine label, treat as rejection
                if quarantine_label_id and quarantine_label_id not in meta.label_ids:
                    await store.update_action_status(action_record.id, ActionStatus.SKIPPED.value)
                    await store.record_override(
                        DEFAULT_TENANT_ID, action_record.thread_id,
                        action_record.sender,
                        json.dumps({"type": "quarantine_rejected", "action": action_record.action_type}),
                    )
                    skipped_count += 1
                else:
                    # User didn't intervene: execute the pending action
                    from src.models import ActionType, ProposedAction, RiskLevel, ClassifiedBy
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
                    await store.update_action_status(action_record.id, "expired_executed")
                    # Remove quarantine label
                    self._gmail.modify_thread(action_record.thread_id, remove_labels=["_auto/quarantine"])
                    executed_count += 1
            except Exception as exc:
                logger.error("Quarantine expiry error for %s: %s", action_record.thread_id, exc)

        if executed_count or skipped_count:
            logger.info("Quarantine expiry: %d executed, %d skipped (user overrides)", executed_count, skipped_count)

    # -- Thread processing --

    async def _crawl_cycle(self) -> None:
        if await self._crawler.is_complete():
            return
        thread_ids = await self._crawler.fetch_next_batch()
        for tid in thread_ids:
            await self._process_thread(tid, is_crawl=True)
        if thread_ids:
            logger.info("Crawl: processed %d threads", len(thread_ids))

    async def _process_thread(self, thread_id: str, is_crawl: bool) -> None:
        try:
            store = self._adapters.state_store

            # Blocklist short-circuit
            meta = self._gmail.get_thread_metadata(thread_id)
            if await store.is_sender_blocklisted(DEFAULT_TENANT_ID, meta.sender):
                self._gmail.modify_thread(thread_id, remove_labels=["INBOX"])
                logger.debug("Blocklisted sender %s, auto-archived thread %s", meta.sender, thread_id)
                return

            # Check for user override on previously processed thread
            prev = await store.get_thread_classification(DEFAULT_TENANT_ID, thread_id)
            if prev:
                prev_labels = json.loads(prev.get("applied_labels", "[]"))
                current_managed = [
                    self._label_id_to_name.get(lid, lid)
                    for lid in meta.label_ids
                    if self._label_id_to_name.get(lid, lid) in ALL_MANAGED_LABELS
                ]
                if set(prev_labels) != set(current_managed) and prev_labels:
                    # Labels diverged: user made changes, skip re-classification
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

            await store.mark_thread_processed(
                DEFAULT_TENANT_ID, thread_id,
                classification.category or "",
                classification.classified_by.value,
                classification.confidence,
                applied_labels=applied_labels,
            )

        except Exception as exc:
            logger.error("Error processing thread %s: %s", thread_id, exc, exc_info=True)

    # -- Signal handlers --

    def _handle_sigterm(self) -> None:
        logger.info("SIGTERM received, shutting down")
        self._running = False

    def _handle_sighup(self) -> None:
        logger.info("SIGHUP received, reloading config")
        asyncio.ensure_future(self._reload_config())

    async def _reload_config(self) -> None:
        try:
            await self._adapters.config_loader.reload()
            self._settings = await self._adapters.config_loader.load_settings()
            manual_rules = await self._adapters.config_loader.load_manual_rules()
            auto_rules = await self._adapters.config_loader.load_auto_rules()
            self._classifier.update_rules(manual_rules, auto_rules)
            self._llm = create_llm_provider(self._settings)
            logger.info("Config reloaded successfully")
        except Exception as exc:
            logger.error("Config reload failed: %s", exc)

    # -- Health --

    async def _write_health(self) -> None:
        uptime = time.monotonic() - self._start_time
        import resource
        mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)

        crawl_state = await self._crawler.get_progress()
        crawl_pct = 0.0
        if crawl_state.total_estimate > 0:
            crawl_pct = (crawl_state.threads_processed / crawl_state.total_estimate) * 100

        status = HealthStatus(
            pid=os.getpid(),
            uptime_seconds=uptime,
            memory_mb=mem,
            notifier_status=self._adapters.mail_notifier.status().value,
            last_sync=datetime.now(timezone.utc).isoformat(),
            threads_processed_last_run=self._last_live_count,
            crawl_progress_pct=crawl_pct,
            crawl_threads_done=crawl_state.threads_processed,
            crawl_total_estimate=crawl_state.total_estimate,
            llm_provider=self._llm.provider_name() if self._llm else "none",
            llm_model=self._llm.model_name() if self._llm else "",
            deploy_mode=self._settings.deploy_mode.value if self._settings else "local",
        )
        await self._adapters.process_manager.write_health(status)

        if self._settings and mem > self._settings.memory_cap_mb:
            logger.warning("Memory usage %.0fMB exceeds cap %dMB, forcing GC", mem, self._settings.memory_cap_mb)
            gc.collect()
