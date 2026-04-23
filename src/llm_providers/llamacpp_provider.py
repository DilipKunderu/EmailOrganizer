from __future__ import annotations

import json
import logging
import re
from typing import Any

import aiohttp

from src.llm_provider import LLMProvider
from src.models import ActionRecord, Classification, ClassifiedBy, Settings, ThreadMetadata

logger = logging.getLogger(__name__)


CLASSIFICATION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "category": {"type": "string"},
        "action_label": {"type": "string"},
        "should_archive": {"type": "boolean"},
        "should_star": {"type": "boolean"},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "reason": {"type": "string"},
    },
    # Prompt only asks the model for category/action_label/confidence/reason.
    # should_archive/should_star are kept in the schema so the model may supply
    # them when relevant, but are not required — we default to False otherwise.
    "required": ["category", "action_label", "confidence", "reason"],
    "additionalProperties": False,
}


_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)


def _extract_json(text: str) -> str:
    """Strip Markdown code fences and surrounding prose some local models emit."""
    cleaned = _FENCE_RE.sub("", text).strip()
    # Fall back to the largest {...} span if the model added commentary.
    if not cleaned.startswith("{"):
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end > start:
            cleaned = cleaned[start : end + 1]
    return cleaned


class LlamaCppProvider(LLMProvider):
    """Calls any OpenAI-compatible local server (llama.cpp, LM Studio, vLLM, Ollama's /v1)."""

    def __init__(self, settings: Settings):
        self._model = settings.llm_model or "local"
        self._endpoint = (settings.llm_endpoint or "http://localhost:8080").rstrip("/")
        self._temperature = settings.llm_temperature
        self._max_tokens = settings.llm_max_tokens
        self._timeout = settings.llm_timeout
        # Flips to False the first time the server rejects response_format, so we
        # stop sending it for the rest of this process and avoid per-request 400s.
        self._supports_structured_output = True

    async def classify_thread(self, meta: ThreadMetadata, prompt_template: str) -> Classification:
        prompt = prompt_template.format(
            subject=meta.subject,
            sender=meta.sender,
            snippet=meta.snippet[:200],
            gmail_categories=", ".join(meta.gmail_categories) or "none",
            has_unsubscribe=str(meta.has_unsubscribe).lower(),
        )
        response_format = None
        if self._supports_structured_output:
            response_format = {
                "type": "json_schema",
                "json_schema": {
                    "name": "email_classification",
                    "strict": True,
                    "schema": CLASSIFICATION_SCHEMA,
                },
            }

        text = await self._complete(prompt, response_format=response_format)
        try:
            data = json.loads(_extract_json(text))
        except json.JSONDecodeError:
            logger.warning("LlamaCppProvider: could not parse JSON from model output: %r", text[:200])
            return Classification(confidence=0.3, reason="Parse error", classified_by=ClassifiedBy.LLM)

        return Classification(
            category=data.get("category"),
            action_label=data.get("action_label"),
            should_archive=bool(data.get("should_archive", False)),
            should_star=bool(data.get("should_star", False)),
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
        return await self._complete(prompt, response_format=None)

    async def _complete(self, prompt: str, response_format: dict[str, Any] | None) -> str:
        payload: dict[str, Any] = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": self._temperature,
            "max_tokens": self._max_tokens,
        }
        if response_format is not None:
            payload["response_format"] = response_format

        timeout = aiohttp.ClientTimeout(total=self._timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(f"{self._endpoint}/v1/chat/completions", json=payload) as resp:
                if resp.status == 400 and response_format is not None:
                    # Backend doesn't understand our structured-output request.
                    # Disable it for the rest of this process and retry plain.
                    body = await resp.text()
                    logger.info(
                        "LlamaCppProvider: server rejected response_format (%s); "
                        "falling back to unstructured output.", body[:200],
                    )
                    self._supports_structured_output = False
                    payload.pop("response_format", None)
                    async with session.post(
                        f"{self._endpoint}/v1/chat/completions", json=payload
                    ) as resp2:
                        resp2.raise_for_status()
                        data = await resp2.json()
                        return data["choices"][0]["message"]["content"]
                resp.raise_for_status()
                data = await resp.json()
                return data["choices"][0]["message"]["content"]

    def provider_name(self) -> str:
        return "llamacpp"

    def model_name(self) -> str:
        return self._model
