from __future__ import annotations

import json
import logging
import os

from src.llm_provider import LLMProvider
from src.models import ActionRecord, Classification, ClassifiedBy, Settings, ThreadMetadata

logger = logging.getLogger(__name__)


class AnthropicProvider(LLMProvider):
    def __init__(self, settings: Settings):
        self._model = settings.llm_model or "claude-3-5-haiku-latest"
        self._temperature = settings.llm_temperature
        self._max_tokens = settings.llm_max_tokens
        self._timeout = settings.llm_timeout
        api_key = os.environ.get(settings.llm_api_key_env or "ANTHROPIC_API_KEY", "")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY not set")
        import anthropic
        self._client = anthropic.AsyncAnthropic(api_key=api_key, timeout=self._timeout)

    async def classify_thread(self, meta: ThreadMetadata, prompt_template: str) -> Classification:
        prompt = prompt_template.format(
            subject=meta.subject,
            sender=meta.sender,
            snippet=meta.snippet[:200],
            gmail_categories=", ".join(meta.gmail_categories) or "none",
            has_unsubscribe=str(meta.has_unsubscribe).lower(),
        )
        resp = await self._client.messages.create(
            model=self._model,
            max_tokens=self._max_tokens,
            messages=[{"role": "user", "content": prompt}],
            temperature=self._temperature,
        )
        text = resp.content[0].text if resp.content else "{}"
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            data = {}
        return Classification(
            category=data.get("category"),
            action_label=data.get("action_label"),
            should_archive=data.get("should_archive", False),
            should_star=data.get("should_star", False),
            confidence=float(data.get("confidence", 0.5)),
            reason=data.get("reason", ""),
            classified_by=ClassifiedBy.LLM,
        )

    async def summarize_digest(self, actions: list[ActionRecord], prompt_template: str) -> str:
        prompt = prompt_template.format(
            threads_processed=len(actions),
            labels_applied=sum(1 for a in actions if a.action_type == "label"),
            threads_archived=sum(1 for a in actions if a.action_type == "archive"),
            threads_starred=sum(1 for a in actions if a.action_type == "star"),
            unsubscribes_executed=sum(1 for a in actions if a.action_type == "unsubscribe" and a.status == "executed"),
            unsubscribes_pending=sum(1 for a in actions if a.action_type == "unsubscribe" and a.status == "quarantine"),
            quarantined=sum(1 for a in actions if a.status == "quarantine"),
            dry_run_count=sum(1 for a in actions if a.status == "dry_run"),
            llm_calls=0, llm_dependency_pct=0, rules_promoted=0, crawl_pct=0,
        )
        resp = await self._client.messages.create(
            model=self._model, max_tokens=300,
            messages=[{"role": "user", "content": prompt}], temperature=0.3,
        )
        return resp.content[0].text if resp.content else ""

    def provider_name(self) -> str:
        return "anthropic"

    def model_name(self) -> str:
        return self._model
