from __future__ import annotations

import os
from dataclasses import dataclass

from src.models import DeployMode
from src.ports.auth import AuthPort
from src.ports.config_loader import ConfigLoaderPort
from src.ports.lock_manager import LockManagerPort
from src.ports.mail_notifier import MailNotifierPort
from src.ports.process_manager import ProcessManagerPort
from src.ports.state_store import StateStorePort


@dataclass
class AdapterSet:
    state_store: StateStorePort
    mail_notifier: MailNotifierPort
    process_manager: ProcessManagerPort
    config_loader: ConfigLoaderPort
    lock_manager: LockManagerPort
    auth: AuthPort


def create_adapters(mode: DeployMode | None = None) -> AdapterSet:
    if mode is None:
        env = os.environ.get("DEPLOY_MODE", "local")
        mode = DeployMode(env)

    if mode == DeployMode.LOCAL:
        return _create_local_adapters()
    elif mode == DeployMode.CLOUD:
        return _create_cloud_adapters()
    else:
        raise ValueError(f"Unknown deploy mode: {mode}")


def _create_local_adapters() -> AdapterSet:
    from src.adapters.local.flock_lock_mgr import FlockLockManager
    from src.adapters.local.file_config_loader import FileConfigLoader
    from src.adapters.local.imap_idle_notifier import IMAPIdleNotifier
    from src.adapters.local.launchd_process_mgr import LaunchdProcessManager
    from src.adapters.local.single_user_auth import SingleUserAuth
    from src.adapters.local.sqlite_store import SQLiteStateStore

    return AdapterSet(
        state_store=SQLiteStateStore(),
        mail_notifier=IMAPIdleNotifier(),
        process_manager=LaunchdProcessManager(),
        config_loader=FileConfigLoader(),
        lock_manager=FlockLockManager(),
        auth=SingleUserAuth(),
    )


def _create_cloud_adapters() -> AdapterSet:
    # Cloud adapters are stubs for now -- implemented when deploying to cloud
    raise NotImplementedError(
        "Cloud adapters not yet implemented. Set DEPLOY_MODE=local or implement "
        "PostgreSQL, Pub/Sub, and multi-tenant adapters in src/adapters/cloud/"
    )
