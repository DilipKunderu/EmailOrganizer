from __future__ import annotations

import pytest

from src.adapters.local.sqlite_store import SQLiteStateStore
from src.engagement import EngagementTracker
from src.models import DEFAULT_TENANT_ID, ThreadMetadata


def _make_meta(**kwargs) -> ThreadMetadata:
    defaults = dict(
        thread_id="t1", subject="Test", sender="news@company.com",
        sender_domain="company.com", snippet="test",
        is_unread=True, has_user_reply=False,
    )
    defaults.update(kwargs)
    return ThreadMetadata(**defaults)


@pytest.mark.asyncio
async def test_first_email_unread():
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    tracker = EngagementTracker(store, DEFAULT_TENANT_ID)
    meta = _make_meta(is_unread=True, has_user_reply=False)
    stats = await tracker.update_from_thread(meta)
    assert stats.total_received == 1
    assert stats.opened == 0
    assert stats.replied == 0
    assert stats.engagement_score == 0.0
    await store.close()


@pytest.mark.asyncio
async def test_opened_email():
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    tracker = EngagementTracker(store, DEFAULT_TENANT_ID)
    meta = _make_meta(is_unread=False)
    stats = await tracker.update_from_thread(meta)
    assert stats.opened == 1
    assert stats.engagement_score == 1.0  # 1/1
    await store.close()


@pytest.mark.asyncio
async def test_replied_email():
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    tracker = EngagementTracker(store, DEFAULT_TENANT_ID)
    meta = _make_meta(is_unread=False, has_user_reply=True)
    stats = await tracker.update_from_thread(meta)
    assert stats.replied == 1
    assert stats.engagement_score == 6.0  # (1 + 5*1) / 1
    await store.close()


def test_score_computation():
    assert EngagementTracker.compute_score(0, 0, 0) == 0.0
    assert EngagementTracker.compute_score(5, 0, 10) == 0.5
    assert EngagementTracker.compute_score(0, 1, 10) == 0.5
    assert EngagementTracker.compute_score(2, 1, 10) == 0.7
