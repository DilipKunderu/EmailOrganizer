from __future__ import annotations

from src.llm_provider import LLMProvider
from src.models import ActionRecord, Classification, ClassifiedBy, ThreadMetadata


class NoLLMProvider(LLMProvider):
    """Rules-only fallback. Returns low-confidence default classification."""

    async def classify_thread(self, meta: ThreadMetadata, prompt_template: str) -> Classification:
        return Classification(
            confidence=0.0,
            reason="No LLM configured",
            classified_by=ClassifiedBy.MANUAL_RULE,
        )

    async def summarize_digest(self, actions: list[ActionRecord], prompt_template: str) -> str:
        return ""

    def provider_name(self) -> str:
        return "none"

    def model_name(self) -> str:
        return ""
