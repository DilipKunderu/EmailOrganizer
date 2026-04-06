from __future__ import annotations

import fcntl
import logging
from pathlib import Path

from src.ports.lock_manager import LockManagerPort

logger = logging.getLogger(__name__)


class FlockLockManager(LockManagerPort):
    """File-based locking using fcntl.flock for local mode."""

    def __init__(self, lock_dir: Path | None = None):
        self._dir = lock_dir or Path("~/.emailorganizer").expanduser()
        self._handles: dict[str, object] = {}

    async def acquire(self, tenant_id: str) -> bool:
        self._dir.mkdir(parents=True, exist_ok=True)
        lock_path = self._dir / f"{tenant_id}.lock"
        try:
            fh = open(lock_path, "w")
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._handles[tenant_id] = fh
            logger.debug("Lock acquired for tenant %s", tenant_id)
            return True
        except (OSError, IOError):
            logger.warning("Could not acquire lock for tenant %s", tenant_id)
            return False

    async def release(self, tenant_id: str) -> None:
        fh = self._handles.pop(tenant_id, None)
        if fh:
            fcntl.flock(fh, fcntl.LOCK_UN)  # type: ignore[arg-type]
            fh.close()  # type: ignore[union-attr]
            logger.debug("Lock released for tenant %s", tenant_id)
