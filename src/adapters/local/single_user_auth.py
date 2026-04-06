from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from src.models import DEFAULT_TENANT_ID
from src.ports.auth import AuthPort

logger = logging.getLogger(__name__)

SCOPES = ["https://mail.google.com/"]
TOKEN_PATH = Path("~/.emailorganizer/token.json").expanduser()
CREDENTIALS_PATH = Path("~/.emailorganizer/credentials.json").expanduser()


class SingleUserAuth(AuthPort):
    """Local single-user OAuth via google-auth-oauthlib."""

    def __init__(
        self,
        token_path: Path = TOKEN_PATH,
        credentials_path: Path = CREDENTIALS_PATH,
    ):
        self._token_path = token_path
        self._credentials_path = credentials_path
        self._creds: Any = None

    async def get_credentials(self, tenant_id: str) -> Any:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials

        if self._creds and self._creds.valid:
            return self._creds

        if self._token_path.exists():
            self._creds = Credentials.from_authorized_user_file(
                str(self._token_path), SCOPES
            )

        if self._creds and self._creds.expired and self._creds.refresh_token:
            logger.info("Refreshing OAuth token")
            self._creds.refresh(Request())
            self._save_token()
        elif not self._creds or not self._creds.valid:
            await self.authorize_new_user()

        return self._creds

    async def get_access_token(self, tenant_id: str) -> str:
        creds = await self.get_credentials(tenant_id)
        return creds.token

    async def authorize_new_user(self) -> str:
        from google_auth_oauthlib.flow import InstalledAppFlow

        if not self._credentials_path.exists():
            raise FileNotFoundError(
                f"OAuth credentials not found at {self._credentials_path}. "
                "Download from Google Cloud Console and save as credentials.json"
            )

        flow = InstalledAppFlow.from_client_secrets_file(
            str(self._credentials_path), SCOPES
        )
        self._creds = flow.run_local_server(port=0)
        self._save_token()
        logger.info("OAuth authorization complete")
        return DEFAULT_TENANT_ID

    async def revoke(self, tenant_id: str) -> None:
        if self._token_path.exists():
            self._token_path.unlink()
        self._creds = None

    async def list_tenants(self) -> list[str]:
        if self._token_path.exists():
            return [DEFAULT_TENANT_ID]
        return []

    def _save_token(self) -> None:
        self._token_path.parent.mkdir(parents=True, exist_ok=True)
        self._token_path.write_text(self._creds.to_json())
