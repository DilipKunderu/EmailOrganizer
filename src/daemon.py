from __future__ import annotations

import asyncio
import gc
import logging
import os
import signal
import time
from datetime import datetime, timezone

from src.adapters.factory import AdapterSet
from src.classifier import HybridClassifier
from src.crawl import BackgroundCrawler
from src.digest import DigestBuilder
from src.engagement import EngagementTracker
from src.executor import BatchExecutor
from src.gmail_client import GmailClient
from src.guardrails import Guardrails
from src.llm_provider import LLMProviderChain, create_llm_provider
from src.models import DEFAULT_TENANT_ID, HealthStatus, Settings
from src.planner import ActionPlanner
from src.unsubscribe import UnsubscribeEngine

logger = logging.getLogger(__name__)


class Daemon:
    """Main daemon: orchestrates IMAP IDLE listener + background crawl."""

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
        self._running = False
        self._start_time = 0.0
        self._last_live_count = 0

    async def start(self) -> None:
        """Full startup sequence."""
        logger.info("Starting Email Organizer daemon")
        self._start_time = time.monotonic()

        self._settings = await self._adapters.config_loader.load_settings()
        await self._adapters.state_store.initialize()

        creds = await self._adapters.auth.get_credentials(DEFAULT_TENANT_ID)
        self._gmail = GmailClient(creds)
        user_email = self._gmail.get_user_email()
        logger.info("Authenticated as %s", user_email)

        self._gmail.provision_labels()

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
        """Run the two concurrent workstreams until stopped."""
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, self._handle_sigterm)
        loop.add_signal_handler(signal.SIGHUP, self._handle_sighup)

        await asyncio.gather(
            self._live_sync_loop(),
            self._crawl_loop(),
        )

    async def run_once(self) -> None:
        """Single sync cycle for debugging."""
        await self._live_sync_cycle()
        if not await self._crawler.is_complete():
            await self._crawl_cycle()

    async def stop(self) -> None:
        logger.info("Stopping daemon")
        self._running = False
        await self._adapters.mail_notifier.disconnect()
        await self._adapters.state_store.close()
        await self._adapters.process_manager.stop_health_server()

    # -- Workstreams --

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
                else:
                    if await self._digest.should_send():
                        crawl_state = await self._crawler.get_progress()
                        await self._digest.build_and_send(crawl_state)
                await self._write_health()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Live sync error: %s", exc, exc_info=True)
                await asyncio.sleep(30)

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

    # -- Processing --

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

        thread_ids: set[str] = set()
        for record in history:
            for msg in record.get("messagesAdded", []):
                tid = msg.get("message", {}).get("threadId")
                if tid:
                    thread_ids.add(tid)

        self._last_live_count = len(thread_ids)
        self._llm.reset_run_counter()
        self._guardrails.reset_run_counters()

        processed = 0
        for tid in thread_ids:
            if await store.is_thread_processed(DEFAULT_TENANT_ID, tid):
                continue
            await self._process_thread(tid, is_crawl=False)
            processed += 1

        await store.set_sync_value(DEFAULT_TENANT_ID, "history_id", new_id)
        if processed:
            logger.info("Live sync: processed %d threads", processed)

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
            meta = self._gmail.get_thread_metadata(thread_id)
            stats = await self._engagement.update_from_thread(meta)
            classification = await self._classifier.classify(meta)

            await self._adapters.state_store.mark_thread_processed(
                DEFAULT_TENANT_ID, thread_id,
                classification.category or "",
                classification.classified_by.value,
                classification.confidence,
            )

            actions = self._planner.plan(meta, classification, stats, is_crawl=is_crawl)

            for action in actions:
                status = await self._guardrails.evaluate(action, meta)
                await self._executor.execute(action, status, meta)

                if (
                    action.action_type.value == "unsubscribe"
                    and status.value == "executed"
                ):
                    await self._unsubscribe.execute_unsubscribe(meta)

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
