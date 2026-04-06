"""Tests for feedback loop fixes: overrides, quarantine expiry, engagement dedup,
L2 guardrail fix, and SenderMiner with sender field."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.adapters.local.sqlite_store import SQLiteStateStore
from src.engagement import EngagementTracker
from src.guardrails import Guardrails
from src.miners import OverrideMiner, SenderMiner
from src.models import (
    ActionRecord,
    ActionStatus,
    ActionType,
    ClassifiedBy,
    DEFAULT_TENANT_ID,
    ProposedAction,
    RiskLevel,
    Settings,
    ThreadMetadata,
)


def _make_meta(**kwargs) -> ThreadMetadata:
    defaults = dict(
        thread_id="t1", subject="Test", sender="test@example.com",
        sender_domain="example.com", snippet="test",
        label_ids=["INBOX"],
        date=datetime.now(timezone.utc) - timedelta(days=3),
    )
    defaults.update(kwargs)
    return ThreadMetadata(**defaults)


def _make_action(risk: RiskLevel = RiskLevel.SAFE, **kwargs) -> ProposedAction:
    defaults = dict(
        thread_id="t1", action_type=ActionType.LABEL,
        risk_level=risk, confidence=0.9, reason="test",
        classified_by=ClassifiedBy.MANUAL_RULE,
    )
    defaults.update(kwargs)
    return ProposedAction(**defaults)


# -- Gap 22: L2 guardrails fix --

@pytest.mark.asyncio
async def test_l2_medium_risk_executes_after_dry_run():
    """Medium-risk actions should EXECUTE after dry-run, not permanently quarantine."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    settings = Settings(dry_run_days=0)  # dry-run expired
    g = Guardrails(settings, store, DEFAULT_TENANT_ID)
    action = _make_action(RiskLevel.MEDIUM, action_type=ActionType.SPAM)
    meta = _make_meta()
    result = await g.evaluate(action, meta)
    assert result == ActionStatus.EXECUTED
    await store.close()


@pytest.mark.asyncio
async def test_l3_high_risk_always_quarantines():
    """High-risk actions should always quarantine."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    settings = Settings(dry_run_days=0)
    g = Guardrails(settings, store, DEFAULT_TENANT_ID)
    action = _make_action(RiskLevel.HIGH, action_type=ActionType.TRASH)
    meta = _make_meta()
    result = await g.evaluate(action, meta)
    assert result == ActionStatus.QUARANTINE
    await store.close()


@pytest.mark.asyncio
async def test_circuit_breaker_triggers():
    """Circuit breaker should quarantine when threshold exceeded."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    settings = Settings(dry_run_days=0, circuit_breaker_threshold=2)
    g = Guardrails(settings, store, DEFAULT_TENANT_ID)
    meta = _make_meta()
    # First two pass
    await g.evaluate(_make_action(RiskLevel.MEDIUM, action_type=ActionType.SPAM), meta)
    await g.evaluate(_make_action(RiskLevel.MEDIUM, action_type=ActionType.SPAM), meta)
    # Third triggers circuit breaker
    result = await g.evaluate(_make_action(RiskLevel.MEDIUM, action_type=ActionType.SPAM), meta)
    assert result == ActionStatus.QUARANTINE
    await store.close()


# -- Gap 5: ActionRecord sender fields --

@pytest.mark.asyncio
async def test_action_record_stores_sender():
    """ActionRecord should persist sender and sender_domain."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    record = ActionRecord(
        tenant_id=DEFAULT_TENANT_ID,
        thread_id="t1",
        action_type="label",
        risk_level=0,
        status="executed",
        reason="test",
        classified_by="llm",
        label_name="Finance",
        sender="billing@company.com",
        sender_domain="company.com",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    rid = await store.log_action(record)
    actions = await store.get_actions(DEFAULT_TENANT_ID, thread_id="t1")
    assert actions[0].sender == "billing@company.com"
    assert actions[0].sender_domain == "company.com"
    await store.close()


# -- Gap 4: SenderMiner uses ActionRecord.sender --

@pytest.mark.asyncio
async def test_sender_miner_uses_sender_field():
    """SenderMiner should group by ActionRecord.sender, not parse reason string."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    settings = Settings()
    settings.learner_sender_min_threads = 3

    for i in range(3):
        await store.log_action(ActionRecord(
            tenant_id=DEFAULT_TENANT_ID,
            thread_id=f"t{i}",
            action_type="label",
            risk_level=0,
            status="executed",
            reason="LLM classified",
            classified_by="llm",
            label_name="Finance",
            sender="billing@bigcorp.com",
            sender_domain="bigcorp.com",
            created_at=datetime.now(timezone.utc).isoformat(),
        ))

    miner = SenderMiner(store, settings, DEFAULT_TENANT_ID)
    candidates = await miner.mine()
    assert len(candidates) == 1
    assert candidates[0].classification == "Finance"
    assert "bigcorp" in candidates[0].pattern
    await store.close()


# -- Gap 3: OverrideMiner uses actual user classification --

@pytest.mark.asyncio
async def test_override_miner_uses_user_label():
    """OverrideMiner should read the user's label from override details, not hardcode Personal."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()

    for i in range(3):
        details = json.dumps({"type": "label_added", "user_added_label": "Finance"})
        await store.record_override(DEFAULT_TENANT_ID, f"t{i}", "reports@bank.com", details)

    miner = OverrideMiner(store, DEFAULT_TENANT_ID)
    candidates = await miner.mine()
    assert len(candidates) == 1
    assert candidates[0].classification == "Finance"
    await store.close()


# -- Gap 16: Engagement deduplication --

@pytest.mark.asyncio
async def test_engagement_deduplicates_total_received():
    """total_received should not double-count on re-processing the same thread."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    tracker = EngagementTracker(store, DEFAULT_TENANT_ID)

    meta = _make_meta(sender="news@site.com", is_unread=True, thread_id="t1")
    stats1 = await tracker.update_from_thread(meta)
    assert stats1.total_received == 1

    # Mark thread as processed to simulate re-encounter
    await store.mark_thread_processed(DEFAULT_TENANT_ID, "t1", "Newsletters", "llm", 0.9)

    stats2 = await tracker.update_from_thread(meta)
    assert stats2.total_received == 1  # NOT 2
    await store.close()


# -- Gap 21: applied_labels stored in processed_threads --

@pytest.mark.asyncio
async def test_applied_labels_stored():
    """mark_thread_processed should persist applied_labels."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    await store.mark_thread_processed(
        DEFAULT_TENANT_ID, "t1", "Finance", "manual_rule", 0.9,
        applied_labels=["Finance", "@Reference"],
    )
    info = await store.get_thread_classification(DEFAULT_TENANT_ID, "t1")
    assert info is not None
    labels = json.loads(info["applied_labels"])
    assert "Finance" in labels
    assert "@Reference" in labels
    await store.close()


# -- Gap 7: Quarantine expiration query --

@pytest.mark.asyncio
async def test_quarantined_expired_query():
    """get_quarantined_expired should return actions older than N days."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()

    old_time = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    await store.log_action(ActionRecord(
        tenant_id=DEFAULT_TENANT_ID,
        thread_id="t_old",
        action_type="spam",
        risk_level=2,
        status="quarantine",
        reason="test",
        classified_by="llm",
        created_at=old_time,
    ))
    await store.log_action(ActionRecord(
        tenant_id=DEFAULT_TENANT_ID,
        thread_id="t_new",
        action_type="spam",
        risk_level=2,
        status="quarantine",
        reason="test",
        classified_by="llm",
        created_at=datetime.now(timezone.utc).isoformat(),
    ))

    expired = await store.get_quarantined_expired(DEFAULT_TENANT_ID, max_age_days=3)
    assert len(expired) == 1
    assert expired[0].thread_id == "t_old"
    await store.close()


# -- Gap 13: Rich engagement signals --

@pytest.mark.asyncio
async def test_record_label_event_starred():
    """record_label_event should update starred count."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    tracker = EngagementTracker(store, DEFAULT_TENANT_ID)

    meta = _make_meta(sender="friend@example.com", is_unread=False)
    await tracker.update_from_thread(meta)

    await tracker.record_label_event("friend@example.com", "starred")
    stats = await store.get_sender_stats(DEFAULT_TENANT_ID, "friend@example.com")
    assert stats.starred == 1
    await store.close()
