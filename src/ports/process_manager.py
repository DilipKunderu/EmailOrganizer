from __future__ import annotations

from abc import ABC, abstractmethod

from src.models import HealthStatus


class ProcessManagerPort(ABC):
    """Lifecycle management: install, uninstall, health checks."""

    @abstractmethod
    async def install_service(self, executable_path: str, args: list[str]) -> None:
        """Install the agent as a system service (launchd plist, etc.)."""

    @abstractmethod
    async def uninstall_service(self) -> None:
        """Remove the system service."""

    @abstractmethod
    async def write_health(self, status: HealthStatus) -> None:
        """Persist health status (file, HTTP endpoint, etc.)."""

    @abstractmethod
    async def read_health(self) -> HealthStatus | None:
        """Read the last written health status."""

    @abstractmethod
    async def start_health_server(self) -> None:
        """Start an HTTP /health endpoint (cloud mode). No-op in local mode."""

    @abstractmethod
    async def stop_health_server(self) -> None: ...
