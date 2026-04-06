from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.adapters.local.sqlite_store import SQLiteStateStore
from src.guardrails import Guardrails
from src.models import (
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
        label_ids=["INBOX"], date=datetime.now(timezone.utc),
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


@pytest.mark.asyncio
async def test_safe_always_executes():
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    g = Guardrails(Settings(), store, DEFAULT_TENANT_ID)
    action = _make_action(RiskLevel.SAFE)
    result = await g.evaluate(action, _make_meta())
    assert result == ActionStatus.EXECUTED
    await store.close()


@pytest.mark.asyncio
async def test_low_risk_dry_run():
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    settings = Settings(dry_run_days=7)
    g = Guardrails(settings, store, DEFAULT_TENANT_ID)
    action = _make_action(RiskLevel.LOW, action_type=ActionType.ARCHIVE)
    old_date = datetime.now(timezone.utc) - timedelta(days=3)
    result = await g.evaluate(action, _make_meta(date=old_date))
    assert result == ActionStatus.DRY_RUN
    await store.close()


@pytest.mark.asyncio
async def test_never_touch_starred():
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    g = Guardrails(Settings(), store, DEFAULT_TENANT_ID)
    meta = _make_meta(label_ids=["INBOX", "STARRED"])
    action = _make_action(RiskLevel.LOW, action_type=ActionType.ARCHIVE)
    result = await g.evaluate(action, meta)
    assert result == ActionStatus.SKIPPED
    await store.close()


@pytest.mark.asyncio
async def test_never_touch_replied():
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    g = Guardrails(Settings(), store, DEFAULT_TENANT_ID)
    meta = _make_meta(has_user_reply=True)
    action = _make_action(RiskLevel.MEDIUM, action_type=ActionType.SPAM)
    result = await g.evaluate(action, meta)
    assert result == ActionStatus.SKIPPED
    await store.close()
