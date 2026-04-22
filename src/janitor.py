"""Process 4: Janitor.

Single responsibility: run housekeeping tasks (quarantine expiration,
unsubscribe persistence checks). Short-lived process, runs hourly via launchd timer.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from src.adapters.factory import AdapterSet
from src.executor import BatchExecutor
from src.gmail_client import GmailClient
from src.models import DEFAULT_TENANT_ID, ExitCode
from src.observability import run_with_retries
from src.quarantine_manager import QuarantineManager
from src.unsubscribe import UnsubscribeEngine

logger = logging.getLogger(__name__)


def _is_refresh_error(exc: BaseException) -> bool:
    try:
        from google.auth.exceptions import RefreshError
        if isinstance(exc, RefreshError):
            return True
    except Exception:
        pass
    return "invalid_grant" in str(exc) or "Token has been expired" in str(exc)


class Janitor:
    """Process 4: Quarantine expiration + unsubscribe persistence."""

    def __init__(self, adapters: AdapterSet):
        self._adapters = adapters
        self._exit_code: ExitCode = ExitCode.OK

    @property
    def exit_code(self) -> ExitCode:
        return self._exit_code

    async def run(self) -> dict[str, int]:
        logger.info("Janitor starting")

        async def _core() -> dict[str, int]:
            settings = await self._adapters.config_loader.load_settings()
            await self._adapters.state_store.initialize()

            try:
                creds = await self._adapters.auth.get_credentials(DEFAULT_TENANT_ID)
            except Exception as exc:
                if _is_refresh_error(exc):
                    await self._adapters.state_store.log_error(
                        "janitor", "fatal", type(exc).__name__,
                        "Refresh token revoked — run: python -m src.main --reauth --mode local",
                    )
                    self._exit_code = ExitCode.NEEDS_REAUTH
                raise

            gmail = GmailClient(creds)
            gmail.provision_labels()

            executor = BatchExecutor(gmail, self._adapters.state_store, DEFAULT_TENANT_ID)
            qm = QuarantineManager(
                gmail, self._adapters.state_store, executor, settings, DEFAULT_TENANT_ID,
            )
            unsub = UnsubscribeEngine(
                gmail, self._adapters.state_store, settings, DEFAULT_TENANT_ID,
            )

            stats: dict[str, int] = {}
            executed, skipped = await qm.check_expirations()
            stats["quarantine_executed"] = executed
            stats["quarantine_skipped"] = skipped

            senders = await self._adapters.state_store.get_senders_by_engagement(
                DEFAULT_TENANT_ID, max_score=0.0, min_emails=1, limit=500,
            )
            escalated = 0
            for s in senders:
                if s.blocklisted:
                    if await unsub.check_persistence(s.sender):
                        escalated += 1
            stats["unsub_escalated"] = escalated

            await self._stamp_sidecar()
            await self._adapters.state_store.close()
            logger.info("Janitor complete: %s", stats)
            return stats

        try:
            return await run_with_retries(
                _core,
                source="janitor",
                logger=logger,
                on_error=self._log_error,
            )
        except Exception as exc:
            if _is_refresh_error(exc):
                self._exit_code = ExitCode.NEEDS_REAUTH
            else:
                self._exit_code = ExitCode.UNEXPECTED
                try:
                    await self._adapters.state_store.log_error(
                        "janitor", "fatal", type(exc).__name__, str(exc),
                    )
                except Exception:
                    pass
            raise

    async def _log_error(
        self, source: str, severity: str, error_class: str, message: str
    ) -> None:
        try:
            await self._adapters.state_store.log_error(
                source, severity, error_class, message,
            )
        except Exception:
            pass

    async def _stamp_sidecar(self) -> None:
        try:
            existing = await self._adapters.process_manager.read_health()
            if existing is None:
                return
            ts = existing.sidecar_timestamps or {}
            ts["janitor"] = datetime.now(timezone.utc).isoformat()
            existing.sidecar_timestamps = ts
            await self._adapters.process_manager.write_health(existing)
        except Exception as exc:
            logger.debug("Failed to stamp janitor sidecar: %s", exc)
