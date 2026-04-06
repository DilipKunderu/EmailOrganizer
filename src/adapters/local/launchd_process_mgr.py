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

# Long-running daemons: restart on exit
_KEEPALIVE_LABELS = {
    "com.emailorganizer.agent",
    "com.emailorganizer.crawl",
}

# Timer-based services: run on schedule, exit, wait for next interval
_TIMER_INTERVALS = {
    "com.emailorganizer.learner": 86400,   # daily (24h)
    "com.emailorganizer.janitor": 3600,    # hourly
    "com.emailorganizer.digest": 86400,    # daily (24h)
}


class LaunchdProcessManager(ProcessManagerPort):
    def __init__(self, label: str = AGENT_LABEL):
        self._label = label

    async def install_service(self, executable_path: str, args: list[str]) -> None:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_name = self._label.rsplit(".", 1)[-1]
        plist: dict = {
            "Label": self._label,
            "ProgramArguments": [sys.executable, "-m", "src.main"] + args,
            "WorkingDirectory": str(Path.cwd()),
            "StandardOutPath": str(LOG_DIR / f"{log_name}-stdout.log"),
            "StandardErrorPath": str(LOG_DIR / f"{log_name}-stderr.log"),
            "ProcessType": "Background",
            "SoftResourceLimits": {"NumberOfFiles": 1024},
        }

        if self._label in _KEEPALIVE_LABELS:
            plist["KeepAlive"] = True
            plist["RunAtLoad"] = True
            plist["ThrottleInterval"] = 10
        elif self._label in _TIMER_INTERVALS:
            plist["StartInterval"] = _TIMER_INTERVALS[self._label]
            plist["RunAtLoad"] = True
        else:
            plist["KeepAlive"] = True
            plist["RunAtLoad"] = True
            plist["ThrottleInterval"] = 10

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
