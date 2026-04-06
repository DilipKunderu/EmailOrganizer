from __future__ import annotations

import pytest

from src.classifier import HybridClassifier
from src.llm_provider import LLMProviderChain
from src.llm_providers.nollm_provider import NoLLMProvider
from src.models import ClassifiedBy, ManualRule, Settings, ThreadMetadata


def _make_meta(**kwargs) -> ThreadMetadata:
    defaults = dict(
        thread_id="t1", subject="", sender="", sender_domain="",
        snippet="", gmail_categories=[], label_ids=[],
    )
    defaults.update(kwargs)
    return ThreadMetadata(**defaults)


def _make_classifier(rules: list[ManualRule] | None = None) -> HybridClassifier:
    settings = Settings()
    settings.confidence_threshold = 0.7
    settings.llm_provider = "none"
    chain = LLMProviderChain(NoLLMProvider())
    return HybridClassifier(settings, rules or [], [], chain)


@pytest.mark.asyncio
async def test_keyword_rule_matches():
    rules = [ManualRule(type="keyword", pattern="invoice", classification="Finance", confidence=0.9)]
    classifier = _make_classifier(rules)
    meta = _make_meta(subject="Your invoice #12345")
    result = await classifier.classify(meta)
    assert result.category == "Finance"
    assert result.classified_by == ClassifiedBy.MANUAL_RULE
    assert result.confidence >= 0.9


@pytest.mark.asyncio
async def test_sender_rule_matches():
    rules = [ManualRule(type="sender", pattern="noreply@bank\\.com", classification="Finance", confidence=0.85)]
    classifier = _make_classifier(rules)
    meta = _make_meta(sender="noreply@bank.com", subject="Statement")
    result = await classifier.classify(meta)
    assert result.category == "Finance"


@pytest.mark.asyncio
async def test_domain_rule_matches():
    rules = [ManualRule(type="domain", pattern=".*\\.edu", classification="Personal", confidence=0.8)]
    classifier = _make_classifier(rules)
    meta = _make_meta(sender="prof@university.edu", sender_domain="university.edu")
    result = await classifier.classify(meta)
    assert result.category == "Personal"


@pytest.mark.asyncio
async def test_no_rule_falls_to_gmail_category():
    classifier = _make_classifier([])
    meta = _make_meta(gmail_categories=["CATEGORY_PROMOTIONS"])
    result = await classifier.classify(meta)
    assert result.category == "Newsletters"
    assert result.classified_by == ClassifiedBy.GMAIL_CATEGORY


@pytest.mark.asyncio
async def test_highest_confidence_wins():
    rules = [
        ManualRule(type="keyword", pattern="invoice", classification="Finance", confidence=0.9),
        ManualRule(type="keyword", pattern="invoice", classification="Shopping", confidence=0.6),
    ]
    classifier = _make_classifier(rules)
    meta = _make_meta(subject="Your invoice")
    result = await classifier.classify(meta)
    assert result.category == "Finance"
