from __future__ import annotations

from abc import ABC, abstractmethod

from src.models import AutoRule, ManualRule, Settings


class ConfigLoaderPort(ABC):
    """Load and reload configuration (rules, settings, prompts)."""

    @abstractmethod
    async def load_settings(self) -> Settings:
        """Load the full settings object."""

    @abstractmethod
    async def load_manual_rules(self) -> list[ManualRule]:
        """Load hand-written classification rules."""

    @abstractmethod
    async def load_auto_rules(self) -> list[AutoRule]:
        """Load machine-generated auto-rules."""

    @abstractmethod
    async def save_auto_rules(self, rules: list[AutoRule]) -> None:
        """Persist auto-rules (sidecar writes these)."""

    @abstractmethod
    async def load_allowlist(self) -> dict[str, list[str]]:
        """Return {'domains': [...], 'addresses': [...]}."""

    @abstractmethod
    async def load_blocklist(self) -> dict[str, list[str]]:
        """Return {'domains': [...], 'addresses': [...]}."""

    @abstractmethod
    async def load_prompt(self, name: str) -> str:
        """Load a prompt template by name (e.g. 'classify', 'digest')."""

    @abstractmethod
    async def reload(self) -> None:
        """Re-read all config from source (for SIGHUP hot-reload)."""
