from __future__ import annotations

from abc import ABC, abstractmethod

from src.models import NotifierStatus


class MailNotifierPort(ABC):
    """Wait for new mail events from Gmail."""

    @abstractmethod
    async def connect(self, oauth_token: str) -> None:
        """Establish the notification channel (IMAP IDLE, Pub/Sub, etc.)."""

    @abstractmethod
    async def wait_for_mail(self) -> bool:
        """Block until new mail arrives or a timeout/re-issue is needed.

        Returns True if new mail was signalled, False on timeout (housekeeping cycle).
        """

    @abstractmethod
    async def disconnect(self) -> None:
        """Cleanly close the notification channel."""

    @abstractmethod
    def status(self) -> NotifierStatus:
        """Current connection status."""
