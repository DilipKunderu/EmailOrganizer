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
        description="Self-improving Gmail organizer agent (5-process architecture)",
    )
    parser.add_argument("--mode", choices=["local", "cloud"], default=None,
                        help="Deployment mode (default: auto-detect from DEPLOY_MODE env)")

    # Process 1: Live sync
    parser.add_argument("--once", action="store_true",
                        help="Run a single live sync cycle then exit")

    # Process 2: Crawl
    parser.add_argument("--crawl", action="store_true",
                        help="Run the background crawl process")

    # Process 3: Rule learner
    parser.add_argument("--learn-now", action="store_true",
                        help="Run the rule learner sidecar immediately")
    parser.add_argument("--learner-status", action="store_true",
                        help="Show rule learner stats and LLM dependency trend")

    # Process 4: Janitor
    parser.add_argument("--janitor", action="store_true",
                        help="Run janitor (quarantine expiry + unsub persistence)")

    # Process 5: Digest
    parser.add_argument("--digest", action="store_true",
                        help="Build and send the daily digest now")

    # Service management
    parser.add_argument("--install-service", action="store_true",
                        help="Install live sync as a launchd service")
    parser.add_argument("--uninstall-service", action="store_true",
                        help="Uninstall the live sync launchd service")
    parser.add_argument("--install-all", action="store_true",
                        help="Install all 5 processes as launchd services")
    parser.add_argument("--uninstall-all", action="store_true",
                        help="Uninstall all launchd services")
    parser.add_argument("--status", action="store_true",
                        help="Show daemon health status")

    # State management
    parser.add_argument("--reset-crawl", action="store_true",
                        help="Reset the background crawl cursor")
    parser.add_argument("--undo-last-run", action="store_true",
                        help="Undo all actions from the most recent run")
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

    # -- Service management (no state store needed) --

    if args.install_service:
        await adapters.process_manager.install_service(sys.executable, ["--mode", "local"])
        logger.info("Live sync service installed")
        return

    if args.uninstall_service:
        await adapters.process_manager.uninstall_service()
        logger.info("Live sync service uninstalled")
        return

    if args.install_all:
        await _install_all_services(adapters)
        return

    if args.uninstall_all:
        await _uninstall_all_services()
        return

    if args.status:
        health = await adapters.process_manager.read_health()
        if health:
            from dataclasses import asdict
            print(json.dumps(asdict(health), indent=2))
        else:
            print("No health status available. Is the daemon running?")
        return

    # -- Commands that need state store --

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

    if args.learner_status:
        await _show_learner_status(adapters)
        return

    # -- Process entry points --

    if args.learn_now:
        await _run_learner(adapters)
        return

    if args.janitor:
        await _run_janitor(adapters)
        return

    if args.digest:
        await _run_digest(adapters)
        return

    if args.crawl:
        await _run_crawl(adapters, once=False)
        return

    # Default: Process 1 (live sync daemon)
    from src.daemon import Daemon
    daemon = Daemon(adapters)
    await daemon.start()

    if args.once:
        await daemon.run_once()
    else:
        await daemon.run_forever()

    await daemon.stop()


# -- Process runners --

async def _run_crawl(adapters, once: bool = False) -> None:
    from src.crawl_daemon import CrawlDaemon
    crawl = CrawlDaemon(adapters)
    await crawl.start()
    if once:
        await crawl.run_once()
    else:
        await crawl.run_forever()
    await crawl.stop()


async def _run_learner(adapters) -> None:
    from src.models import DEFAULT_TENANT_ID
    settings = await adapters.config_loader.load_settings()
    from src.rule_learner import RuleLearner
    learner = RuleLearner(adapters.state_store, adapters.config_loader, settings, DEFAULT_TENANT_ID)
    stats = await learner.run()
    print(f"Learning complete: {stats}")
    await adapters.state_store.close()


async def _run_janitor(adapters) -> None:
    from src.janitor import Janitor
    janitor = Janitor(adapters)
    stats = await janitor.run()
    print(f"Janitor complete: {stats}")


async def _run_digest(adapters) -> None:
    from src.models import DEFAULT_TENANT_ID
    settings = await adapters.config_loader.load_settings()
    creds = await adapters.auth.get_credentials(DEFAULT_TENANT_ID)
    from src.gmail_client import GmailClient
    gmail = GmailClient(creds)
    from src.llm_provider import create_llm_provider
    llm = create_llm_provider(settings)
    from src.digest import DigestBuilder
    from src.crawl import BackgroundCrawler
    digest = DigestBuilder(
        gmail, adapters.state_store, llm, settings, DEFAULT_TENANT_ID,
        config_loader=adapters.config_loader,
    )
    crawler = BackgroundCrawler(gmail, adapters.state_store, settings, DEFAULT_TENANT_ID)
    crawl_state = await crawler.get_progress()
    await digest.build_and_send(crawl_state)
    logger.info("Digest sent")
    await adapters.state_store.close()


async def _show_learner_status(adapters) -> None:
    from src.models import DEFAULT_TENANT_ID
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


# -- Service management --

async def _install_all_services(adapters) -> None:
    from src.adapters.local.launchd_process_mgr import LaunchdProcessManager

    services = [
        ("com.emailorganizer.agent", ["--mode", "local"], True),
        ("com.emailorganizer.crawl", ["--crawl", "--mode", "local"], True),
        ("com.emailorganizer.learner", ["--learn-now", "--mode", "local"], False),
        ("com.emailorganizer.janitor", ["--janitor", "--mode", "local"], False),
        ("com.emailorganizer.digest", ["--digest", "--mode", "local"], False),
    ]
    for label, args, keep_alive in services:
        mgr = LaunchdProcessManager(label=label)
        await mgr.install_service(sys.executable, args)
        logger.info("Installed %s", label)
    logger.info("All 5 services installed")


async def _uninstall_all_services() -> None:
    from src.adapters.local.launchd_process_mgr import LaunchdProcessManager

    labels = [
        "com.emailorganizer.agent",
        "com.emailorganizer.crawl",
        "com.emailorganizer.learner",
        "com.emailorganizer.janitor",
        "com.emailorganizer.digest",
    ]
    for label in labels:
        mgr = LaunchdProcessManager(label=label)
        await mgr.uninstall_service()
    logger.info("All services uninstalled")


if __name__ == "__main__":
    main()
