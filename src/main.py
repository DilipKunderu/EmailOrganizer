from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("emailorganizer")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="emailorganizer",
        description="Self-improving Gmail organizer agent",
    )
    parser.add_argument("--mode", choices=["local", "cloud"], default=None,
                        help="Deployment mode (default: auto-detect from DEPLOY_MODE env)")
    parser.add_argument("--once", action="store_true",
                        help="Run a single sync cycle then exit")
    parser.add_argument("--install-service", action="store_true",
                        help="Install as a launchd service (local mode)")
    parser.add_argument("--uninstall-service", action="store_true",
                        help="Uninstall the launchd service")
    parser.add_argument("--status", action="store_true",
                        help="Show daemon health status")
    parser.add_argument("--reset-crawl", action="store_true",
                        help="Reset the background crawl cursor")
    parser.add_argument("--undo-last-run", action="store_true",
                        help="Undo all actions from the most recent run")
    parser.add_argument("--learn-now", action="store_true",
                        help="Run the rule learner sidecar immediately")
    parser.add_argument("--install-learner", action="store_true",
                        help="Install the rule learner as a launchd service")
    parser.add_argument("--uninstall-learner", action="store_true",
                        help="Uninstall the rule learner service")
    parser.add_argument("--learner-status", action="store_true",
                        help="Show rule learner status")
    parser.add_argument("--reset-rules", action="store_true",
                        help="Wipe all auto-rules and restart learning")

    args = parser.parse_args()

    if args.mode:
        import os
        os.environ["DEPLOY_MODE"] = args.mode

    try:
        asyncio.run(_dispatch(args))
    except KeyboardInterrupt:
        logger.info("Interrupted")
        sys.exit(0)


async def _dispatch(args: argparse.Namespace) -> None:
    from src.adapters.factory import create_adapters
    from src.models import DeployMode

    mode = DeployMode(args.mode) if args.mode else None
    adapters = create_adapters(mode)

    if args.install_service:
        await adapters.process_manager.install_service(sys.executable, ["--mode", "local"])
        logger.info("Service installed")
        return

    if args.uninstall_service:
        await adapters.process_manager.uninstall_service()
        logger.info("Service uninstalled")
        return

    if args.status:
        health = await adapters.process_manager.read_health()
        if health:
            from dataclasses import asdict
            print(json.dumps(asdict(health), indent=2))
        else:
            print("No health status available. Is the daemon running?")
        return

    if args.install_learner:
        from src.adapters.local.launchd_process_mgr import LaunchdProcessManager, LEARNER_LABEL
        mgr = LaunchdProcessManager(label=LEARNER_LABEL)
        await mgr.install_service(sys.executable, ["--learn-now", "--mode", "local"])
        logger.info("Learner service installed")
        return

    if args.uninstall_learner:
        from src.adapters.local.launchd_process_mgr import LaunchdProcessManager, LEARNER_LABEL
        mgr = LaunchdProcessManager(label=LEARNER_LABEL)
        await mgr.uninstall_service()
        logger.info("Learner service uninstalled")
        return

    await adapters.state_store.initialize()

    if args.reset_crawl:
        from src.models import DEFAULT_TENANT_ID
        await adapters.state_store.reset_crawl(DEFAULT_TENANT_ID)
        logger.info("Crawl state reset")
        await adapters.state_store.close()
        return

    if args.reset_rules:
        from src.models import DEFAULT_TENANT_ID
        rules = await adapters.state_store.get_rule_candidates(DEFAULT_TENANT_ID)
        for r in rules:
            await adapters.state_store.update_rule_status(DEFAULT_TENANT_ID, r.id, "rejected")
        await adapters.config_loader.save_auto_rules([])
        logger.info("All auto-rules reset")
        await adapters.state_store.close()
        return

    if args.undo_last_run:
        from src.models import DEFAULT_TENANT_ID
        creds = await adapters.auth.get_credentials(DEFAULT_TENANT_ID)
        from src.gmail_client import GmailClient
        gmail = GmailClient(creds)
        from src.executor import BatchExecutor
        executor = BatchExecutor(gmail, adapters.state_store, DEFAULT_TENANT_ID)
        actions = await adapters.state_store.get_last_run_actions(DEFAULT_TENANT_ID)
        undone = await executor.undo_actions(actions)
        logger.info("Undone %d actions", undone)
        await adapters.state_store.close()
        return

    if args.learn_now or args.learner_status:
        await _run_learner(adapters, args)
        return

    from src.daemon import Daemon
    daemon = Daemon(adapters)
    await daemon.start()

    if args.once:
        await daemon.run_once()
    else:
        await daemon.run_forever()

    await daemon.stop()


async def _run_learner(adapters, args: argparse.Namespace) -> None:
    from src.models import DEFAULT_TENANT_ID

    if args.learner_status:
        trend = await adapters.state_store.get_llm_dependency_trend(DEFAULT_TENANT_ID, days=7)
        rules = await adapters.state_store.get_rule_candidates(DEFAULT_TENANT_ID)
        active = sum(1 for r in rules if r.status == "active")
        staged = sum(1 for r in rules if r.status == "staged")
        deprecated = sum(1 for r in rules if r.status == "deprecated")
        print(f"Auto-rules: {active} active, {staged} staged, {deprecated} deprecated")
        if trend:
            print(f"LLM dependency (latest): {trend[0]['ratio']:.1f}%")
            print("Trend (7d):")
            for d in reversed(trend):
                print(f"  {d['date']}: {d['ratio']:.1f}%")
        await adapters.state_store.close()
        return

    settings = await adapters.config_loader.load_settings()
    from src.rule_learner import RuleLearner
    learner = RuleLearner(adapters.state_store, adapters.config_loader, settings, DEFAULT_TENANT_ID)
    stats = await learner.run()
    print(f"Learning complete: {stats}")
    await adapters.state_store.close()


if __name__ == "__main__":
    main()
