"""Tests for SRP decomposition: ThreadPipeline, QuarantineManager, LabelChangeHandler
operate independently of the daemon."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.adapters.local.sqlite_store import SQLiteStateStore
from src.engagement import EngagementTracker
from src.label_change_handler import LabelChangeHandler
from src.models import (
    ActionRecord,
    ActionStatus,
    DEFAULT_TENANT_ID,
)


# -- LabelChangeHandler tests --

@pytest.mark.asyncio
async def test_label_handler_detects_opened():
    """UNREAD removal should record an opened engagement signal."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    engagement = EngagementTracker(store, DEFAULT_TENANT_ID)
    handler = LabelChangeHandler(store, engagement, DEFAULT_TENANT_ID)

    await store.mark_thread_processed(
        DEFAULT_TENANT_ID, "t1", "Finance", "llm", 0.9,
        applied_labels=["Finance"],
    )
    await store.log_action(ActionRecord(
        tenant_id=DEFAULT_TENANT_ID, thread_id="t1", action_type="label",
        risk_level=0, status="executed", reason="test", classified_by="llm",
        sender="billing@co.com", sender_domain="co.com",
        created_at=datetime.now(timezone.utc).isoformat(),
    ))

    from src.models import SenderStats
    await store.upsert_sender_stats(SenderStats(
        tenant_id=DEFAULT_TENANT_ID, sender="billing@co.com",
        sender_domain="co.com", total_received=5, opened=2,
    ))

    item = {"message": {"threadId": "t1"}, "labelIds": ["UNREAD"]}
    await handler.handle(item, added=False)

    stats = await store.get_sender_stats(DEFAULT_TENANT_ID, "billing@co.com")
    assert stats.opened == 3
    await store.close()


@pytest.mark.asyncio
async def test_label_handler_detects_override_removed():
    """Removing an agent-applied label should record an override."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    engagement = EngagementTracker(store, DEFAULT_TENANT_ID)

    label_map = {"Label_30": "Finance"}
    handler = LabelChangeHandler(store, engagement, DEFAULT_TENANT_ID, label_id_to_name=label_map)

    await store.mark_thread_processed(
        DEFAULT_TENANT_ID, "t1", "Finance", "llm", 0.9,
        applied_labels=["Finance"],
    )
    await store.log_action(ActionRecord(
        tenant_id=DEFAULT_TENANT_ID, thread_id="t1", action_type="label",
        risk_level=0, status="executed", reason="test", classified_by="llm",
        sender="test@example.com", sender_domain="example.com",
        created_at=datetime.now(timezone.utc).isoformat(),
    ))

    item = {"message": {"threadId": "t1"}, "labelIds": ["Label_30"]}
    await handler.handle(item, added=False)

    overrides = await store.get_overrides(DEFAULT_TENANT_ID)
    assert len(overrides) == 1
    details = json.loads(overrides[0]["details"])
    assert details["type"] == "label_removed"
    assert details["label"] == "Finance"
    await store.close()


@pytest.mark.asyncio
async def test_label_handler_detects_override_added():
    """Adding a managed label the agent didn't apply should record an override."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    engagement = EngagementTracker(store, DEFAULT_TENANT_ID)

    label_map = {"Label_31": "Travel"}
    handler = LabelChangeHandler(store, engagement, DEFAULT_TENANT_ID, label_id_to_name=label_map)

    await store.mark_thread_processed(
        DEFAULT_TENANT_ID, "t1", "Finance", "llm", 0.9,
        applied_labels=["Finance"],
    )
    await store.log_action(ActionRecord(
        tenant_id=DEFAULT_TENANT_ID, thread_id="t1", action_type="label",
        risk_level=0, status="executed", reason="test", classified_by="llm",
        sender="test@example.com", sender_domain="example.com",
        created_at=datetime.now(timezone.utc).isoformat(),
    ))

    item = {"message": {"threadId": "t1"}, "labelIds": ["Label_31"]}
    await handler.handle(item, added=True)

    overrides = await store.get_overrides(DEFAULT_TENANT_ID)
    assert len(overrides) == 1
    details = json.loads(overrides[0]["details"])
    assert details["type"] == "label_added"
    assert details["user_added_label"] == "Travel"
    await store.close()


# -- Daemon SRP verification --

def test_daemon_does_not_import_crawl():
    """Daemon should not import crawl, rule_learner, digest, or janitor modules."""
    import importlib
    import src.daemon as daemon_mod
    source = open(daemon_mod.__file__).read()
    assert "from src.crawl" not in source
    assert "from src.rule_learner" not in source
    assert "from src.janitor" not in source
    assert "BackgroundCrawler" not in source
    assert "RuleLearner" not in source
    assert "_maintenance_loop" not in source
    assert "_crawl_loop" not in source


def test_daemon_does_not_contain_quarantine_expiration():
    """Quarantine expiration logic should be in quarantine_manager, not daemon."""
    import src.daemon as daemon_mod
    source = open(daemon_mod.__file__).read()
    assert "_check_quarantine_expirations" not in source
    assert "quarantine_hold_days" not in source
