"""Tests for accuracy improvement: ConfusionAnalyzer, confidence calibration,
KeywordMiner, and backtester temporal split."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.accuracy import ConfusionAnalyzer
from src.adapters.local.sqlite_store import SQLiteStateStore
from src.backtester import Backtester
from src.miners import KeywordMiner
from src.models import (
    ActionRecord,
    AutoRule,
    DEFAULT_TENANT_ID,
    Settings,
)


# -- ConfusionAnalyzer tests --

@pytest.mark.asyncio
async def test_confusion_matrix_from_feedback():
    """Confusion matrix should aggregate predicted vs actual pairs."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()

    await store.record_feedback(
        DEFAULT_TENANT_ID, "t1", "test@co.com",
        "Finance", "@Reference", "llm", "Accounts", "@Reference", "label_added",
    )
    await store.record_feedback(
        DEFAULT_TENANT_ID, "t2", "test@co.com",
        "Finance", "@Reference", "llm", "Accounts", "@Reference", "label_added",
    )
    await store.record_feedback(
        DEFAULT_TENANT_ID, "t3", "news@co.com",
        "Newsletters", "@Read", "llm", "Personal", "@Action", "label_added",
    )

    analyzer = ConfusionAnalyzer(store, DEFAULT_TENANT_ID)
    analysis = await analyzer.analyze()

    confusion = analysis["top_confusion_pairs"]
    assert len(confusion) >= 1
    top = confusion[0]
    assert top["predicted"] == "Finance"
    assert top["actual"] == "Accounts"
    assert top["cnt"] == 2
    await store.close()


@pytest.mark.asyncio
async def test_accuracy_by_category():
    """Per-category accuracy should reflect feedback counts."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()

    await store.mark_thread_processed(DEFAULT_TENANT_ID, "t1", "Finance", "llm", 0.9)
    await store.mark_thread_processed(DEFAULT_TENANT_ID, "t2", "Finance", "llm", 0.9)
    await store.mark_thread_processed(DEFAULT_TENANT_ID, "t3", "Shopping", "llm", 0.8)

    await store.record_feedback(
        DEFAULT_TENANT_ID, "t1", "s@co.com",
        "Finance", "", "llm", "Accounts", "", "label_added",
    )

    by_cat = await store.get_accuracy_by_category(DEFAULT_TENANT_ID)
    assert "Finance" in by_cat
    assert by_cat["Finance"]["total"] == 2
    assert by_cat["Finance"]["feedback_count"] == 1
    assert by_cat["Finance"]["accuracy"] == 50.0
    assert by_cat["Shopping"]["accuracy"] == 100.0
    await store.close()


@pytest.mark.asyncio
async def test_accuracy_by_source():
    """Per-source accuracy should differentiate LLM vs rules."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()

    await store.mark_thread_processed(DEFAULT_TENANT_ID, "t1", "Finance", "llm", 0.9)
    await store.mark_thread_processed(DEFAULT_TENANT_ID, "t2", "Finance", "manual_rule", 0.9)

    await store.record_feedback(
        DEFAULT_TENANT_ID, "t1", "s@co.com",
        "Finance", "", "llm", "Accounts", "", "label_added",
    )

    by_src = await store.get_accuracy_by_source(DEFAULT_TENANT_ID)
    assert by_src["llm"]["accuracy"] == 0.0
    assert by_src["manual_rule"]["accuracy"] == 100.0
    await store.close()


# -- Confidence calibration --

@pytest.mark.asyncio
async def test_confidence_buckets():
    """Confidence calibration should bucket classifications correctly."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()

    await store.mark_thread_processed(DEFAULT_TENANT_ID, "t1", "Finance", "llm", 0.95)
    await store.mark_thread_processed(DEFAULT_TENANT_ID, "t2", "Finance", "llm", 0.95)
    await store.mark_thread_processed(DEFAULT_TENANT_ID, "t3", "Finance", "llm", 0.55)

    await store.record_feedback(
        DEFAULT_TENANT_ID, "t1", "s@co.com",
        "Finance", "", "llm", "Accounts", "", "label_added",
    )

    buckets = await store.get_confidence_buckets(DEFAULT_TENANT_ID)
    high_bucket = next((b for b in buckets if b["bucket"] == "0.9-1.0"), None)
    assert high_bucket is not None
    assert high_bucket["total"] == 2
    assert high_bucket["with_feedback"] == 1
    assert high_bucket["accuracy"] == 50.0
    await store.close()


@pytest.mark.asyncio
async def test_accuracy_daily_persistence():
    """ConfusionAnalyzer should persist daily accuracy snapshots."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()

    await store.mark_thread_processed(DEFAULT_TENANT_ID, "t1", "Finance", "llm", 0.9)

    analyzer = ConfusionAnalyzer(store, DEFAULT_TENANT_ID)
    await analyzer.persist_daily()

    trend = await store.get_accuracy_trend(DEFAULT_TENANT_ID, days=7)
    assert len(trend) == 1
    assert trend[0]["overall_accuracy"] == 100.0
    await store.close()


# -- KeywordMiner --

@pytest.mark.asyncio
async def test_keyword_miner_extracts_patterns():
    """KeywordMiner should find subject-line tokens correlated with categories."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    settings = Settings()
    settings.learner_keyword_min_correlation = 0.9

    for i in range(6):
        await store.log_action(ActionRecord(
            tenant_id=DEFAULT_TENANT_ID,
            thread_id=f"t{i}",
            action_type="label",
            risk_level=0,
            status="executed",
            reason=f"invoice payment #{i}",
            classified_by="llm",
            label_name="Finance",
            sender=f"billing{i}@co.com",
            sender_domain="co.com",
            created_at=datetime.now(timezone.utc).isoformat(),
        ))

    miner = KeywordMiner(store, settings, DEFAULT_TENANT_ID)
    candidates = await miner.mine()
    kw_patterns = [c.pattern for c in candidates]
    assert any("invoice" in p for p in kw_patterns) or any("payment" in p for p in kw_patterns)
    for c in candidates:
        assert c.classification == "Finance"
        assert c.source == "keyword"
    await store.close()


# -- Backtester temporal split --

@pytest.mark.asyncio
async def test_backtester_uses_sender_field():
    """Backtester should match against ActionRecord.sender, not reason text."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    settings = Settings()

    for i in range(10):
        await store.log_action(ActionRecord(
            tenant_id=DEFAULT_TENANT_ID,
            thread_id=f"t{i}",
            action_type="label",
            risk_level=0,
            status="executed",
            reason="some reason",
            classified_by="llm",
            label_name="Finance",
            sender="billing@bigbank.com",
            sender_domain="bigbank.com",
            created_at=(datetime.now(timezone.utc) - timedelta(days=10-i)).isoformat(),
        ))

    rule = AutoRule(
        id="test_rule",
        type="sender",
        pattern="billing@bigbank\\.com",
        classification="Finance",
    )

    backtester = Backtester(store, settings, DEFAULT_TENANT_ID)
    accuracy = await backtester.test_rule(rule)
    assert accuracy == 1.0
    await store.close()


@pytest.mark.asyncio
async def test_backtester_temporal_holdout():
    """Backtester should test on the holdout set (last 30% by default)."""
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    settings = Settings()

    for i in range(10):
        label = "Finance" if i < 7 else "Shopping"
        await store.log_action(ActionRecord(
            tenant_id=DEFAULT_TENANT_ID,
            thread_id=f"t{i}",
            action_type="label",
            risk_level=0,
            status="executed",
            reason="test",
            classified_by="llm",
            label_name=label,
            sender="test@co.com",
            sender_domain="co.com",
            created_at=(datetime.now(timezone.utc) - timedelta(days=10-i)).isoformat(),
        ))

    rule = AutoRule(
        id="test_rule",
        type="sender",
        pattern="test@co\\.com",
        classification="Finance",
    )

    backtester = Backtester(store, settings, DEFAULT_TENANT_ID)
    accuracy = await backtester.test_rule(rule, holdout_ratio=0.3)
    assert accuracy == 0.0  # last 3 items are all Shopping, rule says Finance
    await store.close()
