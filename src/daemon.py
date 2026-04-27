"""Process 1: Live Sync daemon.

Single responsibility: react to new email in real-time via IMAP IDLE.
Owns: IMAP IDLE listener, live sync cycle, label change handling,
signal handling (SIGTERM/SIGHUP), health status writing.

Error handling contract (see plan Step 6d):
  - google.auth.exceptions.RefreshError or 'invalid_grant' -> needs_reauth=true, ExitCode.NEEDS_REAUTH
  - HistoryExpiredError -> reset cursor, continue
  - ConfigLoadError -> ExitCode.CONFIG_ERROR
  - DB / sqlite errors -> ExitCode.DB_ERROR
  - > MAX_CONSECUTIVE_ERRORS in a row -> ExitCode.ERROR_STREAK
  - Anything else fatal -> ExitCode.UNEXPECTED
All fatal exits first sleep FATAL_SLEEP_SECONDS so launchd's ThrottleInterval
doesn't turn into a tight flap loop, giving the watchdog time to alert.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
import resource
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

from src.adapters.factory import AdapterSet
from src.classifier import HybridClassifier
from src.engagement import EngagementTracker
from src.executor import BatchExecutor
from src.gmail_client import GmailClient, HistoryExpiredError
from src.guardrails import Guardrails
from src.label_change_handler import LabelChangeHandler
from src.llm_provider import create_llm_provider
from src.models import DEFAULT_TENANT_ID, ExitCode, HealthStatus, Settings
from src.observability import is_transient
from src.pipeline import ThreadPipeline
from src.planner import ActionPlanner
from src.unsubscribe import UnsubscribeEngine

logger = logging.getLogger(__name__)

MAX_CONSECUTIVE_ERRORS = 10
FATAL_SLEEP_SECONDS = 600  # 10 minutes before process exit on fatal

# In-process heartbeat: if _write_health hasn't fired in this many seconds,
# the guard task will sys.exit so launchd restarts us. Protects against the
# daemon going silent while wedged inside wait_for_mail() or a blocked await.
HEARTBEAT_TIMEOUT_SECONDS = 600   # 10 min; watchdog sidecar runs every 15 min
HEARTBEAT_CHECK_INTERVAL_SECONDS = 60
SLEEP_WAKE_GAP_SECONDS = 420  # 7 min: above normal 5 min IDLE, below guard.


def _is_refresh_error(exc: BaseException) -> bool:
    """Detect token-revoked / invalid_grant across google-auth exception variants."""
    try:
        from google.auth.exceptions import RefreshError
        if isinstance(exc, RefreshError):
            return True
    except Exception:
        pass
    msg = str(exc)
    return "invalid_grant" in msg or "Token has been expired or revoked" in msg


class DaemonFatalExit(SystemExit):
    """Wraps an ExitCode so start() can short-circuit main()."""

    def __init__(self, code: ExitCode, reason: str):
        super().__init__(int(code))
        self.reason = reason


class Daemon:
    """Process 1: IMAP IDLE listener + live sync pipeline."""

    def __init__(self, adapters: AdapterSet):
        self._adapters = adapters
        self._settings: Settings | None = None
        self._gmail: GmailClient | None = None
        self._pipeline: ThreadPipeline | None = None
        self._label_handler: LabelChangeHandler | None = None
        self._engagement: EngagementTracker | None = None
        self._running = False
        self._start_time = 0.0
        self._last_live_count = 0
        self._last_successful_cycle: str = ""
        self._consecutive_errors: int = 0
        self._last_error: str = ""
        self._last_error_ts: str = ""
        self._needs_reauth: bool = False
        self._user_email: str = ""
        self._exit_code: ExitCode = ExitCode.OK
        # Wall-clock of last successful _write_health, used by the heartbeat guard
        self._last_heartbeat_ts: float = 0.0
        self._heartbeat_task: asyncio.Task | None = None
        # Track already-recorded LLM quota signature so we don't spam error_log
        # on every cycle while the quota remains exhausted.
        self._last_quota_sig: str = ""

    async def start(self) -> None:
        logger.info("Starting live sync daemon")
        self._start_time = time.monotonic()

        try:
            self._settings = await self._adapters.config_loader.load_settings()
        except Exception as exc:
            await self._record_fatal("agent", "ConfigLoadError", str(exc))
            raise DaemonFatalExit(ExitCode.CONFIG_ERROR, f"config load failed: {exc}")

        try:
            await self._adapters.state_store.initialize()
        except Exception as exc:
            await self._record_fatal("agent", "DBInitError", str(exc))
            raise DaemonFatalExit(ExitCode.DB_ERROR, f"state store init failed: {exc}")

        try:
            creds = await self._adapters.auth.get_credentials(DEFAULT_TENANT_ID)
        except Exception as exc:
            if _is_refresh_error(exc):
                self._needs_reauth = True
                await self._record_fatal(
                    "agent", type(exc).__name__,
                    "Refresh token revoked — run: python -m src.main --reauth --mode local",
                )
                await self._write_health()
                raise DaemonFatalExit(
                    ExitCode.NEEDS_REAUTH,
                    "Gmail refresh token revoked/expired",
                )
            await self._record_fatal("agent", type(exc).__name__, str(exc))
            raise DaemonFatalExit(ExitCode.UNEXPECTED, f"auth failed: {exc}")

        gmail = GmailClient(creds)
        self._user_email = gmail.get_user_email()
        logger.info("Authenticated as %s", self._user_email)

        # Propagate user email to the notifier so XOAUTH2 works
        if hasattr(self._adapters.mail_notifier, "set_user_email"):
            self._adapters.mail_notifier.set_user_email(self._user_email)

        await self._configure_pipeline(gmail)

        history_id = await self._adapters.state_store.get_sync_value(
            DEFAULT_TENANT_ID, "history_id"
        )
        if not history_id:
            try:
                history_id = self._gmail.get_current_history_id()
            except Exception as exc:
                if _is_refresh_error(exc):
                    self._needs_reauth = True
                    await self._record_fatal(
                        "agent", type(exc).__name__,
                        "Refresh token revoked — run: python -m src.main --reauth --mode local",
                    )
                    await self._write_health()
                    raise DaemonFatalExit(
                        ExitCode.NEEDS_REAUTH,
                        "Gmail refresh token revoked at history bootstrap",
                    )
                await self._record_fatal("agent", type(exc).__name__, str(exc))
                raise DaemonFatalExit(ExitCode.UNEXPECTED, f"history bootstrap failed: {exc}")
            await self._adapters.state_store.set_sync_value(
                DEFAULT_TENANT_ID, "history_id", history_id,
            )

        await self._adapters.process_manager.start_health_server()
        await self._write_health()

        self._running = True
        logger.info("Live sync daemon started")

    async def _configure_pipeline(self, gmail: GmailClient) -> None:
        """Build or rebuild the processing pipeline around a Gmail client.

        This is called both at startup and after a sleep/wake reconnect so the
        pipeline's executor/unsubscribe components don't hold stale HTTP
        sessions.
        """
        assert self._settings is not None, "Settings must be loaded before pipeline setup"

        label_map = gmail.provision_labels()
        label_id_to_name = {v: k for k, v in label_map.items()}

        llm = create_llm_provider(self._settings)
        manual_rules = await self._adapters.config_loader.load_manual_rules()
        auto_rules = await self._adapters.config_loader.load_auto_rules()
        classify_prompt = ""
        try:
            classify_prompt = await self._adapters.config_loader.load_prompt("classify")
        except FileNotFoundError:
            pass

        classifier = HybridClassifier(
            self._settings, manual_rules, auto_rules, llm, classify_prompt,
        )
        planner = ActionPlanner(self._settings)
        allowlist = await self._adapters.config_loader.load_allowlist()
        guardrails = Guardrails(
            self._settings, self._adapters.state_store, DEFAULT_TENANT_ID, allowlist,
        )
        executor = BatchExecutor(gmail, self._adapters.state_store, DEFAULT_TENANT_ID)
        self._engagement = EngagementTracker(
            self._adapters.state_store, DEFAULT_TENANT_ID,
        )
        unsubscribe = UnsubscribeEngine(
            gmail, self._adapters.state_store, self._settings, DEFAULT_TENANT_ID,
        )

        self._gmail = gmail
        self._pipeline = ThreadPipeline(
            gmail=gmail,
            state_store=self._adapters.state_store,
            classifier=classifier,
            planner=planner,
            guardrails=guardrails,
            executor=executor,
            engagement=self._engagement,
            unsubscribe=unsubscribe,
            tenant_id=DEFAULT_TENANT_ID,
            label_id_to_name=label_id_to_name,
        )
        self._label_handler = LabelChangeHandler(
            state_store=self._adapters.state_store,
            engagement=self._engagement,
            tenant_id=DEFAULT_TENANT_ID,
            label_id_to_name=label_id_to_name,
        )

        # Store references for config reload
        self._classifier = classifier
        self._llm = llm

    async def run_forever(self) -> None:
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, self._handle_sigterm)
        loop.add_signal_handler(signal.SIGHUP, self._handle_sighup)

        # Prime the heartbeat and start the guard task before entering the loop
        self._last_heartbeat_ts = time.monotonic()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_guard())

        try:
            await self._live_sync_loop()
        finally:
            if self._heartbeat_task is not None:
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except (asyncio.CancelledError, Exception):
                    pass

    async def run_once(self) -> None:
        await self._live_sync_cycle()
        await self._write_health()

    async def stop(self) -> None:
        logger.info("Stopping live sync daemon")
        self._running = False
        try:
            await self._adapters.mail_notifier.disconnect()
        except Exception:
            pass
        await self._adapters.state_store.close()
        await self._adapters.process_manager.stop_health_server()

    @property
    def exit_code(self) -> ExitCode:
        return self._exit_code

    # -- Live sync loop --

    async def _live_sync_loop(self) -> None:
        token = await self._adapters.auth.get_access_token(DEFAULT_TENANT_ID)
        try:
            await self._adapters.mail_notifier.connect(token)
        except Exception as exc:
            logger.error("Initial IMAP connect failed: %s", exc)
            await self._adapters.state_store.log_error(
                "agent", "warning", type(exc).__name__,
                f"Initial IMAP IDLE connect failed: {exc}",
            )

        # Write health immediately after connect so the status reflects the
        # true notifier_status; the next write only happens after wait_for_mail
        # returns (up to 25 min later for IMAP IDLE).
        await self._write_health()

        last_iter_monotonic = time.monotonic()
        while self._running:
            try:
                gap = time.monotonic() - last_iter_monotonic
                if gap > SLEEP_WAKE_GAP_SECONDS:
                    await self._handle_sleep_wake(gap)
                    last_iter_monotonic = time.monotonic()
                    continue

                # wait_for_mail() returns True on a new-mail push, False on timeout
                # or fallback_poll tick. We run a sync cycle either way so the
                # history cursor + last_successful_cycle advance even on a quiet
                # inbox (prevents false "no successful sync in 30 min" alerts).
                await self._adapters.mail_notifier.wait_for_mail()
                gap = time.monotonic() - last_iter_monotonic
                if gap > SLEEP_WAKE_GAP_SECONDS:
                    await self._handle_sleep_wake(gap)
                    last_iter_monotonic = time.monotonic()
                    continue
                await self._live_sync_cycle()
                await self._write_health()
                last_iter_monotonic = time.monotonic()

                if self._consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    await self._record_fatal(
                        "agent", "ErrorStreak",
                        f"{self._consecutive_errors} consecutive cycle errors; forcing restart",
                    )
                    self._exit_code = ExitCode.ERROR_STREAK
                    self._running = False
                    await asyncio.sleep(FATAL_SLEEP_SECONDS)
                    break

                if self._needs_reauth:
                    self._exit_code = ExitCode.NEEDS_REAUTH
                    self._running = False
                    await asyncio.sleep(FATAL_SLEEP_SECONDS)
                    break
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Live sync loop error: %s", exc, exc_info=True)
                await self._record_error("agent", "error", type(exc).__name__, str(exc))
                await asyncio.sleep(30)

    async def _handle_sleep_wake(self, gap_seconds: float) -> None:
        """Rebuild Gmail HTTPS + IMAP connections after a long event-loop gap.

        Long gaps usually mean laptop sleep/wake. Existing HTTPS/IMAP sockets
        are often stale afterwards, so reset them proactively and skip the
        would-be stale history fetch for this iteration.
        """
        logger.info(
            "Long loop gap detected (%ds); resetting Gmail + IMAP connections",
            int(gap_seconds),
        )
        try:
            await self._adapters.state_store.log_error(
                "agent", "info", "SleepWakeDetected",
                f"Loop gap of {int(gap_seconds)}s; resetting connections",
                context={"gap_seconds": int(gap_seconds)},
            )
        except Exception:
            pass

        try:
            creds = await self._adapters.auth.get_credentials(DEFAULT_TENANT_ID)
            new_gmail = GmailClient(creds)
            self._user_email = new_gmail.get_user_email()
            await self._configure_pipeline(new_gmail)
            if hasattr(self._adapters.mail_notifier, "set_user_email"):
                self._adapters.mail_notifier.set_user_email(self._user_email)
        except Exception as exc:
            logger.warning("Gmail client reset after sleep/wake failed: %s", exc)

        try:
            await self._adapters.mail_notifier.disconnect()
        except Exception:
            pass
        try:
            token = await self._adapters.auth.get_access_token(DEFAULT_TENANT_ID)
            await self._adapters.mail_notifier.connect(token)
        except Exception as exc:
            logger.warning("IMAP reconnect after sleep/wake failed: %s", exc)

        await self._write_health()

    # -- Sync cycle --

    async def _live_sync_cycle(self) -> None:
        store = self._adapters.state_store
        history_id = await store.get_sync_value(DEFAULT_TENANT_ID, "history_id")
        if not history_id:
            return

        try:
            history, new_id = self._gmail.get_history(history_id)
        except HistoryExpiredError as exc:
            logger.warning(
                "History cursor expired, resetting. Backfill falls to crawl sidecar. %s",
                exc,
            )
            await store.log_error(
                "agent", "warning", "HistoryExpiredError", str(exc),
                context={"stale_history_id": history_id},
            )
            try:
                fresh_id = self._gmail.get_current_history_id()
                await store.set_sync_value(DEFAULT_TENANT_ID, "history_id", fresh_id)
            except Exception as reset_exc:
                await self._record_error(
                    "agent", "error", type(reset_exc).__name__,
                    f"Failed to reset history cursor after expiry: {reset_exc}",
                )
                self._consecutive_errors += 1
            return
        except Exception as exc:
            if _is_refresh_error(exc):
                self._needs_reauth = True
                await self._record_fatal(
                    "agent", type(exc).__name__,
                    "Refresh token revoked — run: python -m src.main --reauth --mode local",
                )
                self._running = False
                return
            if is_transient(exc):
                logger.warning("History fetch transient error (will retry): %s", exc)
                await self._record_error("agent", "warning", type(exc).__name__, str(exc))
                return
            logger.error("History fetch failed: %s", exc)
            await self._record_error("agent", "error", type(exc).__name__, str(exc))
            self._consecutive_errors += 1
            return

        new_thread_ids: set[str] = set()

        for record in history:
            for msg in record.get("messagesAdded", []):
                tid = msg.get("message", {}).get("threadId")
                if tid:
                    new_thread_ids.add(tid)

            for item in record.get("labelsAdded", []):
                try:
                    await self._label_handler.handle(item, added=True)
                except Exception as exc:
                    await self._record_error(
                        "agent", "warning", type(exc).__name__,
                        f"label added handler: {exc}",
                    )

            for item in record.get("labelsRemoved", []):
                try:
                    await self._label_handler.handle(item, added=False)
                except Exception as exc:
                    await self._record_error(
                        "agent", "warning", type(exc).__name__,
                        f"label removed handler: {exc}",
                    )

        self._pipeline._guardrails.reset_run_counters()
        if hasattr(self._llm, 'reset_run_counter'):
            self._llm.reset_run_counter()

        processed = 0
        for tid in new_thread_ids:
            if await store.is_thread_processed(DEFAULT_TENANT_ID, tid):
                try:
                    meta = self._gmail.get_thread_metadata(tid)
                    await self._engagement.update_from_thread(meta)
                except Exception:
                    pass
                continue
            await self._pipeline.process(tid, is_crawl=False)
            processed += 1

        self._last_live_count = processed
        await store.set_sync_value(DEFAULT_TENANT_ID, "history_id", new_id)
        self._last_successful_cycle = datetime.now(timezone.utc).isoformat()
        self._consecutive_errors = 0  # reset streak on success
        if processed:
            logger.info("Live sync: processed %d threads", processed)

    # -- Signal handlers --

    def _handle_sigterm(self) -> None:
        logger.info("SIGTERM received, shutting down")
        self._running = False

    def _handle_sighup(self) -> None:
        logger.info("SIGHUP received, reloading config")
        asyncio.ensure_future(self._reload_config())

    async def _reload_config(self) -> None:
        try:
            await self._adapters.config_loader.reload()
            self._settings = await self._adapters.config_loader.load_settings()
            manual_rules = await self._adapters.config_loader.load_manual_rules()
            auto_rules = await self._adapters.config_loader.load_auto_rules()
            self._classifier.update_rules(manual_rules, auto_rules)
            self._llm = create_llm_provider(self._settings)
            logger.info("Config reloaded successfully")
        except Exception as exc:
            logger.error("Config reload failed: %s", exc)
            await self._record_error("agent", "warning", type(exc).__name__, str(exc))

    # -- Health --

    async def _write_health(self) -> None:
        uptime = time.monotonic() - self._start_time
        mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)

        # 24h activity counters (best-effort; DB may be closed in unusual paths)
        store = self._adapters.state_store
        threads_24h = actions_24h = 0
        since_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        try:
            threads_24h = await store.count_threads_processed_since(
                DEFAULT_TENANT_ID, since_24h
            )
            actions_24h = await store.count_actions_since(DEFAULT_TENANT_ID, since_24h)
        except Exception:
            pass

        # Merge existing sidecar_timestamps from disk so the agent doesn't
        # stomp on crawl/janitor/digest/canary entries each write
        existing = await self._adapters.process_manager.read_health()
        sidecar_ts = dict(existing.sidecar_timestamps) if existing else {}
        sidecar_ts["agent"] = datetime.now(timezone.utc).isoformat()

        # Harvest LLM quota state from the provider chain (if present)
        llm_quota_exhausted = False
        llm_quota_provider = ""
        llm_quota_last_ts = ""
        if self._llm is not None:
            errs = getattr(self._llm, "quota_errors_this_run", 0)
            if errs and getattr(self._llm, "last_quota_error_ts", ""):
                llm_quota_exhausted = True
                llm_quota_provider = getattr(self._llm, "last_quota_error_provider", "")
                llm_quota_last_ts = getattr(self._llm, "last_quota_error_ts", "")
                sig = f"{llm_quota_provider}|{llm_quota_last_ts}"
                if sig != self._last_quota_sig:
                    self._last_quota_sig = sig
                    msg = (
                        f"LLM quota exhausted on provider '{llm_quota_provider}': "
                        f"{getattr(self._llm, 'last_quota_error_message', '')}"
                    )
                    try:
                        await store.log_error(
                            "agent", "warning", "LLMQuotaExhausted", msg,
                            context={"provider": llm_quota_provider},
                        )
                    except Exception:
                        pass

        status = HealthStatus(
            pid=os.getpid(),
            uptime_seconds=uptime,
            memory_mb=mem,
            notifier_status=self._adapters.mail_notifier.status().value,
            last_sync=datetime.now(timezone.utc).isoformat(),
            threads_processed_last_run=self._last_live_count,
            llm_provider=self._llm.provider_name() if self._llm else "none",
            llm_model=self._llm.model_name() if self._llm else "",
            deploy_mode=self._settings.deploy_mode.value if self._settings else "local",
            needs_reauth=self._needs_reauth,
            last_successful_cycle=self._last_successful_cycle,
            last_error=self._last_error,
            last_error_ts=self._last_error_ts,
            consecutive_error_count=self._consecutive_errors,
            threads_processed_24h=threads_24h,
            actions_taken_24h=actions_24h,
            sidecar_timestamps=sidecar_ts,
            user_email=self._user_email,
            llm_quota_exhausted=llm_quota_exhausted,
            llm_quota_provider=llm_quota_provider,
            llm_quota_last_ts=llm_quota_last_ts,
        )
        await self._adapters.process_manager.write_health(status)
        # Heartbeat: tick the guard so it knows we're alive
        self._last_heartbeat_ts = time.monotonic()

        if self._settings and mem > self._settings.memory_cap_mb:
            logger.warning("Memory %.0fMB exceeds cap %dMB, forcing GC", mem, self._settings.memory_cap_mb)
            gc.collect()

    # -- In-process heartbeat guard --

    async def _heartbeat_guard(self) -> None:
        """Watch _last_heartbeat_ts and sys.exit if it gets stale.

        Protects against silent wedges inside wait_for_mail, aioimaplib, or
        any other long-awaiting coroutine. The guard task is itself a simple
        sleep-and-check loop so it's very unlikely to wedge in sync with
        whatever blocked the main loop.
        """
        # Wait for the first successful health write before enforcing
        await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL_SECONDS)
        while self._running:
            try:
                await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL_SECONDS)
                silent_for = time.monotonic() - self._last_heartbeat_ts
                if silent_for > HEARTBEAT_TIMEOUT_SECONDS:
                    msg = (
                        f"Heartbeat stalled: no _write_health in {int(silent_for)}s "
                        f"(threshold={HEARTBEAT_TIMEOUT_SECONDS}s). Likely wedged "
                        "inside wait_for_mail or a blocked await. Forcing restart."
                    )
                    logger.warning(msg)
                    try:
                        await self._adapters.state_store.log_error(
                            "agent", "info", "HeartbeatRestart", msg,
                            context={"silent_for_seconds": int(silent_for)},
                        )
                    except Exception:
                        pass
                    # Hard-exit so launchd restarts us; avoid touching loops
                    # that might themselves be blocked.
                    os._exit(int(ExitCode.ERROR_STREAK))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Heartbeat guard error: %s", exc)

    # -- Error recording helpers --

    async def _record_error(
        self, source: str, severity: str, error_class: str, message: str
    ) -> None:
        self._last_error = f"{error_class}: {message}"[:500]
        self._last_error_ts = datetime.now(timezone.utc).isoformat()
        try:
            await self._adapters.state_store.log_error(
                source, severity, error_class, message,
            )
        except Exception:
            pass  # never let error logging itself crash the loop

    async def _record_fatal(
        self, source: str, error_class: str, message: str
    ) -> None:
        await self._record_error(source, "fatal", error_class, message)
