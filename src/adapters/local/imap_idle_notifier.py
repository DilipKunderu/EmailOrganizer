from __future__ import annotations

import asyncio
import logging
import ssl

from src.models import NotifierStatus
from src.ports.mail_notifier import MailNotifierPort

logger = logging.getLogger(__name__)

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993
# RFC 2177 max is 29 min, but we use 5 min to reconnect often enough to
# detect silently-dead sockets after laptop sleep/wake or network blips.
# Combined with the daemon's heartbeat guard, this keeps wait_for_mail()
# from silently hanging longer than ~10 minutes.
IDLE_TIMEOUT_SECONDS = 5 * 60
RECONNECT_DELAYS = [5, 10, 30, 60, 120, 300]


class IMAPIdleNotifier(MailNotifierPort):
    """IMAP IDLE-based mail notifier using aioimaplib."""

    def __init__(self, user_email: str = "", idle_timeout: int = IDLE_TIMEOUT_SECONDS):
        self._user_email = user_email
        self._idle_timeout = idle_timeout
        self._status = NotifierStatus.DISCONNECTED
        self._client: object | None = None
        self._token: str = ""
        self._reconnect_attempt = 0

    def set_user_email(self, user_email: str) -> None:
        """Caller (daemon) sets this after get_user_email() succeeds."""
        self._user_email = user_email

    async def connect(self, oauth_token: str) -> None:
        self._token = oauth_token
        await self._do_connect()

    async def _do_connect(self) -> None:
        try:
            from aioimaplib import IMAP4_SSL

            if not self._user_email:
                raise ConnectionError(
                    "IMAP IDLE: user_email not set. Call set_user_email() before connect()."
                )

            self._status = NotifierStatus.RECONNECTING
            ssl_ctx = ssl.create_default_context()
            client = IMAP4_SSL(host=IMAP_HOST, port=IMAP_PORT, ssl_context=ssl_ctx)
            await client.wait_hello_from_server()

            resp = await client.xoauth2(self._user_email, self._token)
            if resp.result != "OK":
                raise ConnectionError(f"IMAP XOAUTH2 auth failed: {resp}")

            await client.select("INBOX")
            self._client = client
            self._status = NotifierStatus.CONNECTED
            self._reconnect_attempt = 0
            logger.info("IMAP IDLE connected to %s as %s", IMAP_HOST, self._user_email)
        except Exception as exc:
            logger.error("IMAP connection failed: %s", exc)
            self._status = NotifierStatus.FALLBACK_POLLING
            self._client = None
            raise

    async def wait_for_mail(self) -> bool:
        if self._status == NotifierStatus.FALLBACK_POLLING or self._client is None:
            return await self._fallback_poll()

        # Proper aioimaplib IDLE loop:
        #   1. idle_start() returns a Future that resolves when the server
        #      acknowledges our eventual DONE.
        #   2. wait_server_push() blocks on server pushes (e.g., EXISTS).
        #   3. idle_done() is a synchronous call that sends DONE.
        idle_future = None
        try:
            idle_future = await self._client.idle_start(  # type: ignore[union-attr]
                timeout=self._idle_timeout,
            )
            try:
                msg = await asyncio.wait_for(
                    self._client.wait_server_push(),  # type: ignore[union-attr]
                    timeout=self._idle_timeout - 30,
                )
            except asyncio.TimeoutError:
                msg = None

            # Tell the server we're done with IDLE and wait for ack
            self._client.idle_done()  # type: ignore[union-attr]
            try:
                await asyncio.wait_for(idle_future, timeout=10)
            except asyncio.TimeoutError:
                logger.debug("IMAP IDLE done-ack timeout (non-fatal)")

            if msg:
                # Stringify and scan for EXISTS (new mail) or EXPUNGE (removed)
                txt = " ".join(str(x) for x in (msg if isinstance(msg, list) else [msg]))
                if "EXISTS" in txt:
                    logger.debug("IMAP IDLE: new mail detected")
                    return True
            return False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("IMAP IDLE error: %s, switching to fallback", exc)
            self._status = NotifierStatus.FALLBACK_POLLING
            # Try to close the IDLE future cleanly
            if idle_future is not None and not idle_future.done():
                try:
                    self._client.idle_done()  # type: ignore[union-attr]
                except Exception:
                    pass
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
