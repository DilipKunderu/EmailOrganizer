"""Process 1: Live Sync daemon.

Single responsibility: react to new email in real-time via IMAP IDLE.
Owns: IMAP IDLE listener, live sync cycle, label change handling,
signal handling (SIGTERM/SIGHUP), health status writing.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
import resource
import signal
import time
from datetime import datetime, timezone

from src.adapters.factory import AdapterSet
from src.classifier import HybridClassifier
from src.engagement import EngagementTracker
from src.executor import BatchExecutor
from src.gmail_client import GmailClient
from src.guardrails import Guardrails
from src.label_change_handler import LabelChangeHandler
from src.llm_provider import create_llm_provider
from src.models import DEFAULT_TENANT_ID, HealthStatus, Settings
from src.pipeline import ThreadPipeline
from src.planner import ActionPlanner
from src.unsubscribe import UnsubscribeEngine

logger = logging.getLogger(__name__)


class Daemon:
    """Process 1: IMAP IDLE listener + live sync pipeline."""

    def __init__(self, adapters: AdapterSet):
        self._adapters = adapters
        self._settings: Settings | None = None
        self._gmail: GmailClient | None = None
        self._pipeline: ThreadPipeline | None = None
        self._label_handler: LabelChangeHandler | None = None
        self._engagement: EngagementTracker | None = None
        self._running = False
        self._start_time = 0.0
        self._last_live_count = 0

    async def start(self) -> None:
        logger.info("Starting live sync daemon")
        self._start_time = time.monotonic()

        self._settings = await self._adapters.config_loader.load_settings()
        await self._adapters.state_store.initialize()

        creds = await self._adapters.auth.get_credentials(DEFAULT_TENANT_ID)
        self._gmail = GmailClient(creds)
        user_email = self._gmail.get_user_email()
        logger.info("Authenticated as %s", user_email)

        label_map = self._gmail.provision_labels()
        label_id_to_name = {v: k for k, v in label_map.items()}

        llm = create_llm_provider(self._settings)
        manual_rules = await self._adapters.config_loader.load_manual_rules()
        auto_rules = await self._adapters.config_loader.load_auto_rules()
        classify_prompt = ""
        try:
            classify_prompt = await self._adapters.config_loader.load_prompt("classify")
        except FileNotFoundError:
            pass

        classifier = HybridClassifier(
            self._settings, manual_rules, auto_rules, llm, classify_prompt,
        )
        planner = ActionPlanner(self._settings)
        allowlist = await self._adapters.config_loader.load_allowlist()
        guardrails = Guardrails(
            self._settings, self._adapters.state_store, DEFAULT_TENANT_ID, allowlist,
        )
        executor = BatchExecutor(
            self._gmail, self._adapters.state_store, DEFAULT_TENANT_ID,
        )
        self._engagement = EngagementTracker(
            self._adapters.state_store, DEFAULT_TENANT_ID,
        )
        unsubscribe = UnsubscribeEngine(
            self._gmail, self._adapters.state_store, self._settings, DEFAULT_TENANT_ID,
        )

        self._pipeline = ThreadPipeline(
            gmail=self._gmail,
            state_store=self._adapters.state_store,
            classifier=classifier,
            planner=planner,
            guardrails=guardrails,
            executor=executor,
            engagement=self._engagement,
            unsubscribe=unsubscribe,
            tenant_id=DEFAULT_TENANT_ID,
            label_id_to_name=label_id_to_name,
        )
        self._label_handler = LabelChangeHandler(
            state_store=self._adapters.state_store,
            engagement=self._engagement,
            tenant_id=DEFAULT_TENANT_ID,
            label_id_to_name=label_id_to_name,
        )

        # Store references for config reload
        self._classifier = classifier
        self._llm = llm

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
        logger.info("Live sync daemon started")

    async def run_forever(self) -> None:
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, self._handle_sigterm)
        loop.add_signal_handler(signal.SIGHUP, self._handle_sighup)

        await self._live_sync_loop()

    async def run_once(self) -> None:
        await self._live_sync_cycle()

    async def stop(self) -> None:
        logger.info("Stopping live sync daemon")
        self._running = False
        await self._adapters.mail_notifier.disconnect()
        await self._adapters.state_store.close()
        await self._adapters.process_manager.stop_health_server()

    # -- IMAP IDLE loop --

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

    # -- Sync cycle --

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

        for record in history:
            for msg in record.get("messagesAdded", []):
                tid = msg.get("message", {}).get("threadId")
                if tid:
                    new_thread_ids.add(tid)

            for item in record.get("labelsAdded", []):
                await self._label_handler.handle(item, added=True)

            for item in record.get("labelsRemoved", []):
                await self._label_handler.handle(item, added=False)

        self._pipeline._guardrails.reset_run_counters()
        if hasattr(self._llm, 'reset_run_counter'):
            self._llm.reset_run_counter()

        processed = 0
        for tid in new_thread_ids:
            if await store.is_thread_processed(DEFAULT_TENANT_ID, tid):
                try:
                    meta = self._gmail.get_thread_metadata(tid)
                    await self._engagement.update_from_thread(meta)
                except Exception:
                    pass
                continue
            await self._pipeline.process(tid, is_crawl=False)
            processed += 1

        self._last_live_count = processed
        await store.set_sync_value(DEFAULT_TENANT_ID, "history_id", new_id)
        if processed:
            logger.info("Live sync: processed %d threads", processed)

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
        mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)

        status = HealthStatus(
            pid=os.getpid(),
            uptime_seconds=uptime,
            memory_mb=mem,
            notifier_status=self._adapters.mail_notifier.status().value,
            last_sync=datetime.now(timezone.utc).isoformat(),
            threads_processed_last_run=self._last_live_count,
            llm_provider=self._llm.provider_name() if self._llm else "none",
            llm_model=self._llm.model_name() if self._llm else "",
            deploy_mode=self._settings.deploy_mode.value if self._settings else "local",
        )
        await self._adapters.process_manager.write_health(status)

        if self._settings and mem > self._settings.memory_cap_mb:
            logger.warning("Memory %.0fMB exceeds cap %dMB, forcing GC", mem, self._settings.memory_cap_mb)
            gc.collect()
