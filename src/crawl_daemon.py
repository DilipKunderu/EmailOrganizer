"""Process 2: Background Crawl daemon.

Single responsibility: gradually process the entire mailbox in time-budgeted batches.
Runs on its own timer with its own pipeline instance and rate budgets.
Exits when the crawl is complete.
"""

from __future__ import annotations

import asyncio
import logging

from src.adapters.factory import AdapterSet
from src.classifier import HybridClassifier
from src.crawl import BackgroundCrawler
from src.engagement import EngagementTracker
from src.executor import BatchExecutor
from src.gmail_client import GmailClient
from src.guardrails import Guardrails
from src.llm_provider import create_llm_provider
from src.models import DEFAULT_TENANT_ID
from src.pipeline import ThreadPipeline
from src.planner import ActionPlanner
from src.unsubscribe import UnsubscribeEngine

logger = logging.getLogger(__name__)


class CrawlDaemon:
    """Process 2: Background mailbox crawl with its own pipeline instance."""

    def __init__(self, adapters: AdapterSet):
        self._adapters = adapters
        self._crawler: BackgroundCrawler | None = None
        self._pipeline: ThreadPipeline | None = None
        self._settings = None
        self._running = False

    async def start(self) -> None:
        logger.info("Starting crawl daemon")
        self._settings = await self._adapters.config_loader.load_settings()
        await self._adapters.state_store.initialize()

        creds = await self._adapters.auth.get_credentials(DEFAULT_TENANT_ID)
        gmail = GmailClient(creds)
        gmail.provision_labels()
        label_map = {name: lid for name, lid in gmail._label_cache.items()}
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
        executor = BatchExecutor(gmail, self._adapters.state_store, DEFAULT_TENANT_ID)
        engagement = EngagementTracker(self._adapters.state_store, DEFAULT_TENANT_ID)
        unsubscribe = UnsubscribeEngine(
            gmail, self._adapters.state_store, self._settings, DEFAULT_TENANT_ID,
        )

        self._pipeline = ThreadPipeline(
            gmail=gmail, state_store=self._adapters.state_store,
            classifier=classifier, planner=planner, guardrails=guardrails,
            executor=executor, engagement=engagement, unsubscribe=unsubscribe,
            tenant_id=DEFAULT_TENANT_ID, label_id_to_name=label_id_to_name,
        )
        self._crawler = BackgroundCrawler(
            gmail, self._adapters.state_store, self._settings, DEFAULT_TENANT_ID,
        )
        self._running = True
        logger.info("Crawl daemon started")

    async def run_forever(self) -> None:
        while self._running:
            try:
                if await self._crawler.is_complete():
                    logger.info("Crawl complete, exiting")
                    break
                await self._crawl_cycle()
                await asyncio.sleep(self._settings.crawl_interval_minutes * 60)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Crawl error: %s", exc, exc_info=True)
                await asyncio.sleep(60)

    async def run_once(self) -> None:
        if await self._crawler.is_complete():
            logger.info("Crawl already complete")
            return
        await self._crawl_cycle()

    async def stop(self) -> None:
        logger.info("Stopping crawl daemon")
        self._running = False
        await self._adapters.state_store.close()

    async def _crawl_cycle(self) -> None:
        thread_ids = await self._crawler.fetch_next_batch()
        for tid in thread_ids:
            await self._pipeline.process(tid, is_crawl=True)
        if thread_ids:
            logger.info("Crawl: processed %d threads", len(thread_ids))
