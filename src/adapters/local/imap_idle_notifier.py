from __future__ import annotations

import asyncio
import base64
import logging
import ssl

from src.models import NotifierStatus
from src.ports.mail_notifier import MailNotifierPort

logger = logging.getLogger(__name__)

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993
IDLE_TIMEOUT_SECONDS = 25 * 60  # 25 min (RFC 2177 max is 29)
RECONNECT_DELAYS = [5, 10, 30, 60, 120, 300]


def _build_xoauth2_string(user: str, token: str) -> str:
    auth_string = f"user={user}\x01auth=Bearer {token}\x01\x01"
    return base64.b64encode(auth_string.encode()).decode()


class IMAPIdleNotifier(MailNotifierPort):
    """IMAP IDLE-based mail notifier using aioimaplib."""

    def __init__(self, user_email: str = "", idle_timeout: int = IDLE_TIMEOUT_SECONDS):
        self._user_email = user_email
        self._idle_timeout = idle_timeout
        self._status = NotifierStatus.DISCONNECTED
        self._client: object | None = None
        self._token: str = ""
        self._reconnect_attempt = 0

    async def connect(self, oauth_token: str) -> None:
        self._token = oauth_token
        await self._do_connect()

    async def _do_connect(self) -> None:
        try:
            from aioimaplib import IMAP4_SSL

            self._status = NotifierStatus.RECONNECTING
            ssl_ctx = ssl.create_default_context()
            client = IMAP4_SSL(host=IMAP_HOST, port=IMAP_PORT, ssl_context=ssl_ctx)
            await client.wait_hello_from_server()

            xoauth2 = _build_xoauth2_string(self._user_email, self._token)
            resp = await client.authenticate("XOAUTH2", lambda _: xoauth2)
            if resp.result != "OK":
                raise ConnectionError(f"IMAP auth failed: {resp}")

            await client.select("INBOX")
            self._client = client
            self._status = NotifierStatus.CONNECTED
            self._reconnect_attempt = 0
            logger.info("IMAP IDLE connected to %s", IMAP_HOST)
        except Exception as exc:
            logger.error("IMAP connection failed: %s", exc)
            self._status = NotifierStatus.FALLBACK_POLLING
            self._client = None
            raise

    async def wait_for_mail(self) -> bool:
        if self._status == NotifierStatus.FALLBACK_POLLING or self._client is None:
            return await self._fallback_poll()

        try:
            idle_resp = await self._client.idle_start(timeout=self._idle_timeout)  # type: ignore[union-attr]
            msg = await asyncio.wait_for(
                self._client.idle_done(),  # type: ignore[union-attr]
                timeout=self._idle_timeout + 30,
            )
            if msg and any("EXISTS" in str(line) for line in msg if line):
                logger.debug("IMAP IDLE: new mail detected")
                return True
            return False
        except asyncio.TimeoutError:
            logger.debug("IMAP IDLE timeout, re-issuing")
            return False
        except Exception as exc:
            logger.warning("IMAP IDLE error: %s, switching to fallback", exc)
            self._status = NotifierStatus.FALLBACK_POLLING
            await self._schedule_reconnect()
            return await self._fallback_poll()

    async def disconnect(self) -> None:
        if self._client:
            try:
                await self._client.logout()  # type: ignore[union-attr]
            except Exception:
                pass
            self._client = None
        self._status = NotifierStatus.DISCONNECTED

    def status(self) -> NotifierStatus:
        return self._status

    async def _fallback_poll(self) -> bool:
        """Sleep for the fallback interval, then signal the caller to check Gmail API."""
        logger.debug("Fallback polling: sleeping 5 minutes")
        await asyncio.sleep(5 * 60)
        return True

    async def _schedule_reconnect(self) -> None:
        delay = RECONNECT_DELAYS[min(self._reconnect_attempt, len(RECONNECT_DELAYS) - 1)]
        self._reconnect_attempt += 1
        logger.info("Scheduling IMAP reconnect in %ds (attempt %d)", delay, self._reconnect_attempt)
        await asyncio.sleep(delay)
        try:
            await self._do_connect()
        except Exception:
            pass
