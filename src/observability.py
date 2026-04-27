"""Shared observability primitives: structured JSON logging + sidecar retry helpers.

Kept separate from `daemon.py` because sidecars (crawl, janitor, digest, watchdog,
canary) all need the same retry-with-backoff behaviour but don't need the
daemon's full lifecycle.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, TypeVar

T = TypeVar("T")

# -- Structured JSON logging --


class JSONFormatter(logging.Formatter):
    """Emit one JSON object per log record.

    Preserves 'extra' attrs attached via logger.error(..., extra={...}) so
    callers can attach error_class, consecutive_error_count, etc.
    """

    _RESERVED = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "asctime",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "pid": record.process,
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for k, v in record.__dict__.items():
            if k not in self._RESERVED and not k.startswith("_"):
                try:
                    json.dumps(v)  # test serializability
                    payload[k] = v
                except (TypeError, ValueError):
                    payload[k] = repr(v)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: int = logging.INFO, json_logs: bool | None = None) -> None:
    """Wire the root logger. JSON logs when running under launchd or if env set."""
    if json_logs is None:
        json_logs = os.environ.get("EMAILORGANIZER_JSON_LOGS", "").lower() in (
            "1", "true", "yes",
        )
        # Default to JSON if stderr is not a TTY (i.e., running under launchd)
        if not json_logs and not sys.stderr.isatty():
            json_logs = True

    root = logging.getLogger()
    # Clear existing handlers (avoid duplicates when main is re-imported)
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stderr)
    if json_logs:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
    root.addHandler(handler)
    root.setLevel(level)


# -- Retry with exponential backoff for sidecars --

BACKOFF_SECONDS = [5, 15, 60, 300, 600]  # 5s, 15s, 1m, 5m, 10m

# Error classes we consider transient and retryable
TRANSIENT_EXCEPTION_NAMES = {
    "OSError", "TimeoutError", "ConnectionResetError", "ConnectionError",
    "BrokenPipeError", "TransportError", "ServerNotFoundError",
    "NameResolutionError", "socket.gaierror", "gaierror",
}


def is_transient(exc: BaseException) -> bool:
    """Return True for transient network / transport failures that should be
    retried or treated as self-healing noise, not permanent failures."""
    if isinstance(exc, (OSError, TimeoutError, asyncio.TimeoutError)):
        return True
    name = type(exc).__name__
    if name in TRANSIENT_EXCEPTION_NAMES:
        return True
    # Cross-package: google.auth.exceptions.TransportError, etc.
    mod = type(exc).__module__
    if "httplib2" in mod or "google.auth" in mod and "Transport" in name:
        return True
    return False


async def run_with_retries(
    fn: Callable[[], Awaitable[T]],
    *,
    source: str,
    logger: logging.Logger,
    max_attempts: int = 5,
    on_error: Callable[[str, str, str, str], Awaitable[None]] | None = None,
) -> T:
    """Run an async fn with exponential backoff on transient errors.

    on_error(source, severity, error_class, message) is awaited on every
    caught transient error (used to push to error_log).
    Permanent errors re-raise immediately.
    """
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return await fn()
        except BaseException as exc:  # noqa: BLE001
            last_exc = exc
            if not is_transient(exc):
                raise
            delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            logger.warning(
                "Transient error in %s (attempt %d/%d): %s %s — retrying in %ds",
                source, attempt + 1, max_attempts,
                type(exc).__name__, exc, delay,
            )
            if on_error is not None:
                try:
                    await on_error(
                        source, "warning", type(exc).__name__,
                        f"attempt {attempt + 1}/{max_attempts}: {exc}",
                    )
                except Exception:
                    pass
            await asyncio.sleep(delay)
    # Exhausted retries
    assert last_exc is not None
    raise last_exc
