"""One-shot: scan current Gmail Trash and backfill sender_stats.trashed.

Recovers learning signal lost during downtime windows where the daemon was
down or predates Gmail's 7-day history retention.

Reads Trash for sender-level aggregate signal only. Does NOT re-classify
trashed messages or feed classification_feedback.
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone

from src.adapters.factory import AdapterSet
from src.gmail_client import GmailClient
from src.models import DEFAULT_TENANT_ID, SenderStats

logger = logging.getLogger(__name__)

DEFAULT_MAX_THREADS = 5000  # cap API use; pass -1 for unbounded
PAGE_SIZE = 100


class TrashLearner:
    """Scan Gmail Trash and backfill sender_stats.trashed counters."""

    def __init__(self, adapters: AdapterSet):
        self._adapters = adapters

    async def run_once(self, max_threads: int = DEFAULT_MAX_THREADS) -> dict:
        await self._adapters.state_store.initialize()

        try:
            creds = await self._adapters.auth.get_credentials(DEFAULT_TENANT_ID)
        except Exception as exc:
            logger.error("TrashLearner auth failed: %s", exc)
            try:
                await self._adapters.state_store.log_error(
                    "trash_learner", "error", type(exc).__name__,
                    f"Cannot get creds: {exc}",
                )
            except Exception:
                pass
            await self._adapters.state_store.close()
            raise

        gmail = GmailClient(creds)

        page_token: str | None = None
        scanned = 0
        meta_errors = 0
        sender_counts: Counter[str] = Counter()
        sender_domain: dict[str, str] = {}

        logger.info(
            "TrashLearner starting: max_threads=%s, page_size=%s",
            max_threads if max_threads > 0 else "unbounded", PAGE_SIZE,
        )

        while True:
            try:
                threads, page_token, _ = gmail.list_threads(
                    query="in:trash",
                    max_results=PAGE_SIZE,
                    page_token=page_token,
                )
            except Exception as exc:
                logger.error("TrashLearner list_threads failed: %s", exc)
                await self._adapters.state_store.log_error(
                    "trash_learner", "error", type(exc).__name__,
                    f"list_threads failed at scanned={scanned}: {exc}",
                )
                break

            if not threads:
                break

            for t in threads:
                tid = t.get("id")
                if not tid:
                    continue
                try:
                    meta = gmail.get_thread_metadata(tid)
                    if meta.sender:
                        sender_counts[meta.sender] += 1
                        if meta.sender_domain:
                            sender_domain[meta.sender] = meta.sender_domain
                except Exception as exc:
                    meta_errors += 1
                    logger.debug("TrashLearner metadata fetch failed for %s: %s", tid, exc)

                scanned += 1
                if max_threads > 0 and scanned >= max_threads:
                    break

            if max_threads > 0 and scanned >= max_threads:
                logger.info("TrashLearner: hit max_threads cap (%d)", max_threads)
                break
            if not page_token:
                break

        # -- Apply counts to sender_stats --
        affected = 0
        now = datetime.now(timezone.utc).isoformat()

        for sender, count in sender_counts.items():
            existing = await self._adapters.state_store.get_sender_stats(
                DEFAULT_TENANT_ID, sender,
            )
            if existing is None:
                existing = SenderStats(
                    tenant_id=DEFAULT_TENANT_ID,
                    sender=sender,
                    sender_domain=sender_domain.get(sender, ""),
                    first_seen_at=now,
                )

            existing.trashed += count

            # Seed total_received for never-seen senders so the denominator isn't
            # zero (which would flatten the score to 0). For existing senders we
            # don't bump total_received: these aren't fresh arrivals, they're
            # catchup signal for mail we already counted (or that pre-dates us).
            if existing.total_received == 0:
                existing.total_received = count

            # Recompute engagement score with the updated trash count
            from src.engagement import EngagementTracker
            existing.engagement_score = EngagementTracker._compute_score(existing)
            existing.last_updated = now

            await self._adapters.state_store.upsert_sender_stats(existing)
            affected += 1

        top_offenders = sender_counts.most_common(10)

        # Record run in error_log so the learner cycle has a breadcrumb
        try:
            await self._adapters.state_store.log_error(
                "trash_learner", "info", "RunComplete",
                (f"scanned={scanned} senders_affected={affected} "
                 f"meta_errors={meta_errors} "
                 f"top5={sender_counts.most_common(5)}"),
                context={
                    "scanned": scanned,
                    "senders_affected": affected,
                    "meta_errors": meta_errors,
                    "top_offenders": top_offenders,
                    "max_threads": max_threads,
                },
            )
        except Exception:
            pass

        await self._adapters.state_store.close()

        logger.info(
            "TrashLearner complete: scanned=%d senders_affected=%d meta_errors=%d",
            scanned, affected, meta_errors,
        )

        return {
            "scanned": scanned,
            "senders_affected": affected,
            "meta_errors": meta_errors,
            "top_offenders": top_offenders,
        }
