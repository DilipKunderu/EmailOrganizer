from __future__ import annotations

import json
import logging

import aiohttp

from src.llm_provider import LLMProvider
from src.models import ActionRecord, Classification, ClassifiedBy, Settings, ThreadMetadata

logger = logging.getLogger(__name__)


class LlamaCppProvider(LLMProvider):
    """Calls a llama.cpp server with OpenAI-compatible API."""

    def __init__(self, settings: Settings):
        self._model = settings.llm_model or "local"
        self._endpoint = (settings.llm_endpoint or "http://localhost:8080").rstrip("/")
        self._temperature = settings.llm_temperature
        self._max_tokens = settings.llm_max_tokens
        self._timeout = settings.llm_timeout

    async def classify_thread(self, meta: ThreadMetadata, prompt_template: str) -> Classification:
        prompt = prompt_template.format(
            subject=meta.subject,
            sender=meta.sender,
            snippet=meta.snippet[:200],
            gmail_categories=", ".join(meta.gmail_categories) or "none",
            has_unsubscribe=str(meta.has_unsubscribe).lower(),
        )
        text = await self._complete(prompt)
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return Classification(confidence=0.3, reason="Parse error", classified_by=ClassifiedBy.LLM)

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
            unsubscribes_executed=0, unsubscribes_pending=0, quarantined=0,
            dry_run_count=0, llm_calls=0, llm_dependency_pct=0,
            rules_promoted=0, crawl_pct=0,
        )
        return await self._complete(prompt)

    async def _complete(self, prompt: str) -> str:
        payload = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": self._temperature,
            "max_tokens": self._max_tokens,
        }
        timeout = aiohttp.ClientTimeout(total=self._timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{self._endpoint}/v1/chat/completions", json=payload) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data["choices"][0]["message"]["content"]

    def provider_name(self) -> str:
        return "llamacpp"

    def model_name(self) -> str:
        return self._model
