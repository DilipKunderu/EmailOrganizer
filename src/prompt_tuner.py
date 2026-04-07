"""Adaptive prompt evolution: injects learned correction examples into the
classify prompt based on the most common confusion pairs."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any

from src.models import DEFAULT_TENANT_ID
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)

LEARNED_SECTION_MARKER = "\n## Learned corrections (auto-generated, do not edit below this line)\n"


class PromptTuner:
    """Evolves the classify prompt by appending few-shot correction examples."""

    def __init__(
        self,
        state_store: StateStorePort,
        prompt_path: Path = Path("config/prompts/classify.txt"),
        tenant_id: str = DEFAULT_TENANT_ID,
    ):
        self._store = state_store
        self._path = prompt_path
        self._tenant = tenant_id

    async def tune(self) -> str | None:
        """Update the prompt with top confusion corrections. Returns new version hash or None."""
        confusion = await self._store.get_confusion_matrix(self._tenant)
        if not confusion:
            logger.debug("No confusion data yet, skipping prompt tuning")
            return None

        # Only use pairs where predicted != actual (actual errors)
        error_pairs = [
            c for c in confusion
            if c["predicted"] and c["actual"] and c["predicted"] != c["actual"]
        ]
        if not error_pairs:
            return None

        top_errors = error_pairs[:5]
        corrections = self._build_correction_text(top_errors)

        base_prompt = self._read_base_prompt()
        new_prompt = base_prompt + LEARNED_SECTION_MARKER + corrections

        version_hash = hashlib.sha256(new_prompt.encode()).hexdigest()[:12]

        self._path.write_text(new_prompt)
        logger.info(
            "Prompt updated with %d correction examples (version: %s)",
            len(top_errors), version_hash,
        )
        return version_hash

    def _read_base_prompt(self) -> str:
        """Read the prompt, stripping any previous learned corrections section."""
        text = self._path.read_text()
        marker_pos = text.find(LEARNED_SECTION_MARKER)
        if marker_pos >= 0:
            return text[:marker_pos].rstrip()
        return text.rstrip()

    @staticmethod
    def _build_correction_text(error_pairs: list[dict[str, Any]]) -> str:
        lines = ["Common mistakes to avoid:"]
        for pair in error_pairs:
            predicted = pair["predicted"]
            actual = pair["actual"]
            count = pair["cnt"]
            lines.append(
                f"- Do NOT classify as {predicted} when the correct category is {actual} "
                f"({count} past errors)"
            )
        return "\n".join(lines) + "\n"

    def get_current_version(self) -> str:
        """Return a hash of the current prompt for version tracking."""
        if not self._path.exists():
            return ""
        text = self._path.read_text()
        return hashlib.sha256(text.encode()).hexdigest()[:12]
