from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import AsyncIterator


class LockManagerPort(ABC):
    """Prevent concurrent processing of the same tenant."""

    @abstractmethod
    async def acquire(self, tenant_id: str) -> bool:
        """Try to acquire a tenant lock. Return True if acquired."""

    @abstractmethod
    async def release(self, tenant_id: str) -> None:
        """Release the tenant lock."""

    @asynccontextmanager
    async def hold(self, tenant_id: str) -> AsyncIterator[bool]:
        """Context manager for lock acquisition."""
        acquired = await self.acquire(tenant_id)
        try:
            yield acquired
        finally:
            if acquired:
                await self.release(tenant_id)
