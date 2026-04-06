from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from src.models import Classification, ClassifiedBy, Settings, ThreadMetadata, ActionRecord

logger = logging.getLogger(__name__)


class LLMProvider(ABC):
    @abstractmethod
    async def classify_thread(self, meta: ThreadMetadata, prompt_template: str) -> Classification:
        """Classify a thread when rules are uncertain."""

    @abstractmethod
    async def summarize_digest(self, actions: list[ActionRecord], prompt_template: str) -> str:
        """Generate a human-readable summary for the daily digest."""

    @abstractmethod
    def provider_name(self) -> str: ...

    @abstractmethod
    def model_name(self) -> str: ...


class LLMProviderChain:
    """Wraps primary + fallback providers with automatic failover."""

    def __init__(self, primary: LLMProvider, fallback: LLMProvider | None = None):
        self.primary = primary
        self.fallback = fallback
        self._calls_this_run = 0
        self._max_calls = 50

    def configure(self, settings: Settings) -> None:
        self._max_calls = settings.llm_max_calls_per_run

    @property
    def calls_this_run(self) -> int:
        return self._calls_this_run

    def reset_run_counter(self) -> None:
        self._calls_this_run = 0

    def can_call(self) -> bool:
        return self._calls_this_run < self._max_calls

    async def classify_thread(self, meta: ThreadMetadata, prompt_template: str) -> Classification | None:
        if not self.can_call():
            return None

        self._calls_this_run += 1
        try:
            return await self.primary.classify_thread(meta, prompt_template)
        except Exception as exc:
            logger.warning("Primary LLM failed: %s", exc)
            if self.fallback:
                try:
                    return await self.fallback.classify_thread(meta, prompt_template)
                except Exception as exc2:
                    logger.warning("Fallback LLM also failed: %s", exc2)
            return None

    async def summarize_digest(self, actions: list[ActionRecord], prompt_template: str) -> str:
        try:
            return await self.primary.summarize_digest(actions, prompt_template)
        except Exception:
            if self.fallback:
                try:
                    return await self.fallback.summarize_digest(actions, prompt_template)
                except Exception:
                    pass
            return ""

    def provider_name(self) -> str:
        return self.primary.provider_name()

    def model_name(self) -> str:
        return self.primary.model_name()


def create_llm_provider(settings: Settings) -> LLMProviderChain:
    primary = _create_single(settings.llm_provider, settings)
    fallback = None
    if settings.llm_fallback and settings.llm_fallback != "none":
        fallback = _create_single(settings.llm_fallback, settings)
    chain = LLMProviderChain(primary, fallback)
    chain.configure(settings)
    return chain


def _create_single(name: str, settings: Settings) -> LLMProvider:
    if name == "none" or not name:
        from src.llm_providers.nollm_provider import NoLLMProvider
        return NoLLMProvider()
    elif name == "openai":
        from src.llm_providers.openai_provider import OpenAIProvider
        return OpenAIProvider(settings)
    elif name == "anthropic":
        from src.llm_providers.anthropic_provider import AnthropicProvider
        return AnthropicProvider(settings)
    elif name == "ollama":
        from src.llm_providers.ollama_provider import OllamaProvider
        return OllamaProvider(settings)
    elif name == "llamacpp":
        from src.llm_providers.llamacpp_provider import LlamaCppProvider
        return LlamaCppProvider(settings)
    else:
        logger.warning("Unknown LLM provider '%s', falling back to none", name)
        from src.llm_providers.nollm_provider import NoLLMProvider
        return NoLLMProvider()
