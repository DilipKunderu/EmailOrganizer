from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from src.gmail_client import GmailClient
from src.models import CrawlState, DEFAULT_TENANT_ID, Settings
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class BackgroundCrawler:
    """Gradually processes the entire mailbox in time-budgeted batches."""

    def __init__(
        self,
        gmail: GmailClient,
        state_store: StateStorePort,
        settings: Settings,
        tenant_id: str = DEFAULT_TENANT_ID,
    ):
        self._gmail = gmail
        self._store = state_store
        self._settings = settings
        self._tenant = tenant_id

    async def is_complete(self) -> bool:
        state = await self._store.get_crawl_state(self._tenant)
        return state.is_complete

    async def get_progress(self) -> CrawlState:
        return await self._store.get_crawl_state(self._tenant)

    async def fetch_next_batch(self) -> list[str]:
        """Fetch the next batch of thread IDs for the crawl.

        Returns a list of thread IDs to process, or empty if crawl is complete.
        """
        state = await self._store.get_crawl_state(self._tenant)
        if state.is_complete:
            return []

        if not state.started_at:
            state.started_at = datetime.now(timezone.utc).isoformat()

        query = "in:anywhere"
        if self._settings.crawl_order == "newest_first":
            query += " -in:trash -in:spam"

        threads, next_token, estimate = self._gmail.list_threads(
            query=query,
            max_results=self._settings.crawl_batch_size,
            page_token=state.page_token if state.page_token else None,
        )

        state.total_estimate = max(state.total_estimate, estimate)
        thread_ids: list[str] = []

        start = time.monotonic()
        for t in threads:
            if time.monotonic() - start > self._settings.crawl_time_budget_seconds:
                logger.info("Crawl time budget exceeded, stopping batch early")
                break
            tid = t["id"]
            already = await self._store.is_thread_processed(self._tenant, tid)
            if not already:
                thread_ids.append(tid)
            state.threads_processed += 1

        if next_token:
            state.page_token = next_token
        else:
            state.is_complete = True
            logger.info("Crawl complete: %d threads processed total", state.threads_processed)

        await self._store.update_crawl_state(self._tenant, state)
        return thread_ids

    async def reset(self) -> None:
        await self._store.reset_crawl(self._tenant)
        logger.info("Crawl state reset")
