from __future__ import annotations

import json
import logging

import aiohttp

from src.llm_provider import LLMProvider
from src.models import ActionRecord, Classification, ClassifiedBy, Settings, ThreadMetadata

logger = logging.getLogger(__name__)


class OllamaProvider(LLMProvider):
    def __init__(self, settings: Settings):
        self._model = settings.llm_model or "llama3.2:8b"
        self._endpoint = (settings.llm_endpoint or "http://localhost:11434").rstrip("/")
        self._temperature = settings.llm_temperature
        self._timeout = settings.llm_timeout

    async def classify_thread(self, meta: ThreadMetadata, prompt_template: str) -> Classification:
        prompt = prompt_template.format(
            subject=meta.subject,
            sender=meta.sender,
            snippet=meta.snippet[:200],
            gmail_categories=", ".join(meta.gmail_categories) or "none",
            has_unsubscribe=str(meta.has_unsubscribe).lower(),
        )
        text = await self._chat(prompt)
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return Classification(confidence=0.3, reason="Failed to parse Ollama response", classified_by=ClassifiedBy.LLM)

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
        return await self._chat(prompt)

    async def _chat(self, prompt: str) -> str:
        payload = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "format": "json",
            "options": {"temperature": self._temperature},
        }
        timeout = aiohttp.ClientTimeout(total=self._timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{self._endpoint}/api/chat", json=payload) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data.get("message", {}).get("content", "")

    def provider_name(self) -> str:
        return "ollama"

    def model_name(self) -> str:
        return self._model
