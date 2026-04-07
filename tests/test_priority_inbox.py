"""Tests for priority inbox: aggressive archiving, mark-read, importance, decisive classifier."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.classifier import HybridClassifier
from src.llm_provider import LLMProviderChain
from src.llm_providers.nollm_provider import NoLLMProvider
from src.models import (
    ActionType,
    ClassifiedBy,
    Classification,
    ManualRule,
    RiskLevel,
    Settings,
    SenderStats,
    ThreadMetadata,
)
from src.planner import ActionPlanner


def _make_meta(**kwargs) -> ThreadMetadata:
    defaults = dict(
        thread_id="t1", subject="Test", sender="test@example.com",
        sender_domain="example.com", snippet="test",
        gmail_categories=[], label_ids=["INBOX"],
        date=datetime.now(timezone.utc) - timedelta(days=3),
    )
    defaults.update(kwargs)
    return ThreadMetadata(**defaults)


def _priority_settings() -> Settings:
    s = Settings()
    s.inbox_mode = "priority"
    s.inbox_archive_all_except = ["@Action", "@Waiting"]
    s.inbox_mark_read_categories = ["Accounts"]
    s.inbox_mark_read_keywords = ["shipped", "delivered", "receipt", "password reset"]
    s.inbox_mark_important = ["@Action", "@Waiting", "@Read"]
    s.inbox_mark_not_important = ["Newsletters", "Shopping", "Accounts"]
    return s


# -- Aggressive archiving --

def test_reference_thread_is_archived():
    """@Reference threads should be archived in priority mode."""
    planner = ActionPlanner(_priority_settings())
    meta = _make_meta()
    classification = Classification(
        category="Finance", action_label="@Reference",
        confidence=0.9, classified_by=ClassifiedBy.MANUAL_RULE,
    )
    actions = planner.plan(meta, classification)
    archive_actions = [a for a in actions if a.action_type == ActionType.ARCHIVE]
    assert len(archive_actions) == 1


def test_action_thread_stays_in_inbox():
    """@Action threads should NOT be archived."""
    planner = ActionPlanner(_priority_settings())
    meta = _make_meta()
    classification = Classification(
        category="Personal", action_label="@Action",
        confidence=0.9, classified_by=ClassifiedBy.MANUAL_RULE,
    )
    actions = planner.plan(meta, classification)
    archive_actions = [a for a in actions if a.action_type == ActionType.ARCHIVE]
    assert len(archive_actions) == 0


def test_waiting_thread_stays_in_inbox():
    """@Waiting threads should NOT be archived."""
    planner = ActionPlanner(_priority_settings())
    meta = _make_meta()
    classification = Classification(
        category="Personal", action_label="@Waiting",
        confidence=0.9, classified_by=ClassifiedBy.MANUAL_RULE,
    )
    actions = planner.plan(meta, classification)
    archive_actions = [a for a in actions if a.action_type == ActionType.ARCHIVE]
    assert len(archive_actions) == 0


def test_read_thread_is_archived():
    """@Read threads should be archived (but marked important)."""
    planner = ActionPlanner(_priority_settings())
    meta = _make_meta()
    classification = Classification(
        category="Newsletters", action_label="@Read",
        confidence=0.9, classified_by=ClassifiedBy.MANUAL_RULE,
    )
    actions = planner.plan(meta, classification)
    archive_actions = [a for a in actions if a.action_type == ActionType.ARCHIVE]
    assert len(archive_actions) == 1


def test_action_thread_is_starred():
    """@Action threads should be starred."""
    planner = ActionPlanner(_priority_settings())
    meta = _make_meta()
    classification = Classification(
        category="Personal", action_label="@Action",
        confidence=0.9, classified_by=ClassifiedBy.MANUAL_RULE,
    )
    actions = planner.plan(meta, classification)
    star_actions = [a for a in actions if a.action_type == ActionType.STAR]
    assert len(star_actions) == 1


# -- Mark-as-read --

def test_accounts_category_marked_read():
    """Accounts category threads should be marked as read."""
    planner = ActionPlanner(_priority_settings())
    meta = _make_meta(subject="Your verification code")
    classification = Classification(
        category="Accounts", action_label="@Reference",
        confidence=0.9, classified_by=ClassifiedBy.MANUAL_RULE,
    )
    actions = planner.plan(meta, classification)
    read_actions = [a for a in actions if a.action_type == ActionType.MARK_READ]
    assert len(read_actions) == 1


def test_shipped_keyword_marked_read():
    """Threads with shipping keywords should be marked as read."""
    planner = ActionPlanner(_priority_settings())
    meta = _make_meta(subject="Your order has been shipped!")
    classification = Classification(
        category="Shopping", action_label="@Reference",
        confidence=0.9, classified_by=ClassifiedBy.MANUAL_RULE,
    )
    actions = planner.plan(meta, classification)
    read_actions = [a for a in actions if a.action_type == ActionType.MARK_READ]
    assert len(read_actions) == 1


def test_personal_action_not_marked_read():
    """Personal @Action threads should NOT be marked as read."""
    planner = ActionPlanner(_priority_settings())
    meta = _make_meta(subject="Can we meet tomorrow?")
    classification = Classification(
        category="Personal", action_label="@Action",
        confidence=0.9, classified_by=ClassifiedBy.MANUAL_RULE,
    )
    actions = planner.plan(meta, classification)
    read_actions = [a for a in actions if a.action_type == ActionType.MARK_READ]
    assert len(read_actions) == 0


# -- Importance markers --

def test_action_thread_marked_important():
    """@Action threads should get IMPORTANT label."""
    planner = ActionPlanner(_priority_settings())
    meta = _make_meta()
    classification = Classification(
        category="Personal", action_label="@Action",
        confidence=0.9, classified_by=ClassifiedBy.MANUAL_RULE,
    )
    actions = planner.plan(meta, classification)
    imp_actions = [a for a in actions if a.label_name == "IMPORTANT" and a.action_type == ActionType.LABEL]
    assert len(imp_actions) == 1


def test_newsletter_marked_not_important():
    """Newsletters should have IMPORTANT label removed."""
    planner = ActionPlanner(_priority_settings())
    meta = _make_meta()
    classification = Classification(
        category="Newsletters", action_label="@Reference",
        confidence=0.9, classified_by=ClassifiedBy.MANUAL_RULE,
    )
    actions = planner.plan(meta, classification)
    unimp_actions = [a for a in actions if a.label_name == "IMPORTANT" and a.action_type == ActionType.REMOVE_LABEL]
    assert len(unimp_actions) == 1


# -- Decisive classifier --

@pytest.mark.asyncio
async def test_classifier_always_assigns_action_label():
    """Every classification should have an action label, never None."""
    settings = Settings()
    settings.confidence_threshold = 0.7
    settings.llm_provider = "none"
    chain = LLMProviderChain(NoLLMProvider())
    classifier = HybridClassifier(settings, [], [], chain)

    meta = _make_meta(gmail_categories=["CATEGORY_PROMOTIONS"])
    result = await classifier.classify(meta)
    assert result.action_label is not None
    assert result.action_label in ("@Action", "@Waiting", "@Read", "@Reference")


@pytest.mark.asyncio
async def test_classifier_personal_defaults_to_action():
    """Personal emails with no explicit action should default to @Action."""
    settings = Settings()
    settings.confidence_threshold = 0.7
    settings.llm_provider = "none"
    chain = LLMProviderChain(NoLLMProvider())
    rules = [ManualRule(type="domain", pattern=".*\\.com", classification="Personal", confidence=0.8)]
    classifier = HybridClassifier(settings, rules, [], chain)

    meta = _make_meta(sender="friend@personal.com", sender_domain="personal.com")
    result = await classifier.classify(meta)
    assert result.action_label == "@Action"


@pytest.mark.asyncio
async def test_classifier_accounts_defaults_to_reference():
    """Accounts emails should default to @Reference."""
    settings = Settings()
    settings.confidence_threshold = 0.7
    settings.llm_provider = "none"
    chain = LLMProviderChain(NoLLMProvider())
    rules = [ManualRule(type="keyword", pattern="password reset", classification="Accounts", confidence=0.9)]
    classifier = HybridClassifier(settings, rules, [], chain)

    meta = _make_meta(subject="password reset request")
    result = await classifier.classify(meta)
    assert result.action_label == "@Reference"


# -- Relaxed mode (no aggressive archive) --

def test_relaxed_mode_no_auto_archive():
    """In relaxed mode, only explicitly marked threads are archived."""
    s = Settings()
    s.inbox_mode = "relaxed"
    planner = ActionPlanner(s)
    meta = _make_meta()
    classification = Classification(
        category="Finance", action_label="@Reference",
        confidence=0.9, classified_by=ClassifiedBy.MANUAL_RULE,
        should_archive=False,
    )
    actions = planner.plan(meta, classification)
    archive_actions = [a for a in actions if a.action_type == ActionType.ARCHIVE]
    assert len(archive_actions) == 0
