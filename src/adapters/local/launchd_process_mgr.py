from __future__ import annotations

import json
import logging
import plistlib
import subprocess
import sys
from pathlib import Path

from src.models import HealthStatus
from src.ports.process_manager import ProcessManagerPort

logger = logging.getLogger(__name__)

PLIST_DIR = Path("~/Library/LaunchAgents").expanduser()
AGENT_LABEL = "com.emailorganizer.agent"
LEARNER_LABEL = "com.emailorganizer.learner"
STATUS_PATH = Path("~/.emailorganizer/status.json").expanduser()
LOG_DIR = Path("~/.emailorganizer/logs").expanduser()


class LaunchdProcessManager(ProcessManagerPort):
    def __init__(self, label: str = AGENT_LABEL):
        self._label = label

    async def install_service(self, executable_path: str, args: list[str]) -> None:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        plist = {
            "Label": self._label,
            "ProgramArguments": [sys.executable, "-m", "src.main"] + args,
            "WorkingDirectory": str(Path.cwd()),
            "RunAtLoad": True,
            "KeepAlive": True,
            "ThrottleInterval": 10,
            "StandardOutPath": str(LOG_DIR / "stdout.log"),
            "StandardErrorPath": str(LOG_DIR / "stderr.log"),
            "ProcessType": "Background",
            "SoftResourceLimits": {"NumberOfFiles": 1024},
        }
        plist_path = PLIST_DIR / f"{self._label}.plist"
        PLIST_DIR.mkdir(parents=True, exist_ok=True)
        with open(plist_path, "wb") as f:
            plistlib.dump(plist, f)
        subprocess.run(["launchctl", "load", str(plist_path)], check=True)
        logger.info("Service installed: %s", plist_path)

    async def uninstall_service(self) -> None:
        plist_path = PLIST_DIR / f"{self._label}.plist"
        if plist_path.exists():
            subprocess.run(["launchctl", "unload", str(plist_path)], check=False)
            plist_path.unlink()
            logger.info("Service uninstalled: %s", self._label)
        else:
            logger.warning("Service plist not found: %s", plist_path)

    async def write_health(self, status: HealthStatus) -> None:
        STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        from dataclasses import asdict
        STATUS_PATH.write_text(json.dumps(asdict(status), indent=2))

    async def read_health(self) -> HealthStatus | None:
        if not STATUS_PATH.exists():
            return None
        try:
            data = json.loads(STATUS_PATH.read_text())
            return HealthStatus(**data)
        except Exception:
            return None

    async def start_health_server(self) -> None:
        pass

    async def stop_health_server(self) -> None:
        pass
