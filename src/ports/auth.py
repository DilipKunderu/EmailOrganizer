from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class AuthPort(ABC):
    """Manage OAuth tokens for Gmail API and IMAP XOAUTH2."""

    @abstractmethod
    async def get_credentials(self, tenant_id: str) -> Any:
        """Return a google.oauth2.credentials.Credentials object (or equivalent).

        Handles token refresh transparently.
        """

    @abstractmethod
    async def get_access_token(self, tenant_id: str) -> str:
        """Return a raw access token string for IMAP XOAUTH2."""

    @abstractmethod
    async def authorize_new_user(self) -> str:
        """Run the OAuth consent flow. Return the tenant_id of the new user.

        Local: opens browser, waits for redirect.
        Cloud: returns URL for user to visit, stores tokens on callback.
        """

    @abstractmethod
    async def revoke(self, tenant_id: str) -> None:
        """Revoke stored credentials for a tenant."""

    @abstractmethod
    async def list_tenants(self) -> list[str]:
        """List all tenant IDs with stored credentials."""
