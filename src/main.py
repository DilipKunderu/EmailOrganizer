from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

from dotenv import load_dotenv

load_dotenv()

from src.observability import configure_logging

configure_logging()
logger = logging.getLogger("emailorganizer")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="emailorganizer",
        description="Self-improving Gmail organizer agent (8-process architecture with observability)",
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

    # Process 6: Watchdog (observability)
    parser.add_argument("--watchdog", action="store_true",
                        help="Run one watchdog cycle: evaluate SLOs, fire alerts if needed")

    # Process 7: Canary (observability)
    parser.add_argument("--canary", action="store_true",
                        help="Run one canary cycle: verify previous canary + send new one")

    # Auth
    parser.add_argument("--reauth", action="store_true",
                        help="Force re-authorization (opens browser). Use when --status shows needs_reauth.")

    # Retroactive learning
    parser.add_argument("--learn-from-trash", action="store_true",
                        help="One-shot: scan current Trash folder, backfill sender_stats.trashed, "
                             "and recompute engagement scores. Use after an outage or first setup.")
    parser.add_argument("--learn-from-trash-max", type=int, default=5000,
                        help="Max trashed threads to scan (default 5000; -1 for all)")

    # Service management
    parser.add_argument("--install-service", action="store_true",
                        help="Install live sync as a launchd service")
    parser.add_argument("--uninstall-service", action="store_true",
                        help="Uninstall the live sync launchd service")
    parser.add_argument("--install-all", action="store_true",
                        help="Install all 8 processes as launchd services")
    parser.add_argument("--uninstall-all", action="store_true",
                        help="Uninstall all launchd services")
    parser.add_argument("--status", action="store_true",
                        help="Show opinionated health status. Exits 0 healthy, 1 degraded, 2 critical.")

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
        exit_code = asyncio.run(_dispatch(args))
    except KeyboardInterrupt:
        logger.info("Interrupted")
        sys.exit(0)
    sys.exit(int(exit_code or 0))


async def _dispatch(args: argparse.Namespace) -> int:
    from src.adapters.factory import create_adapters
    from src.models import DeployMode, ExitCode

    mode = DeployMode(args.mode) if args.mode else None
    adapters = create_adapters(mode)

    # -- Service management (no state store needed) --

    if args.install_service:
        await adapters.process_manager.install_service(sys.executable, ["--mode", "local"])
        logger.info("Live sync service installed")
        return ExitCode.OK

    if args.uninstall_service:
        await adapters.process_manager.uninstall_service()
        logger.info("Live sync service uninstalled")
        return ExitCode.OK

    if args.install_all:
        await _install_all_services(adapters)
        return ExitCode.OK

    if args.uninstall_all:
        await _uninstall_all_services()
        return ExitCode.OK

    if args.status:
        return await _print_status(adapters)

    if args.reauth:
        return await _run_reauth(adapters)

    # -- Commands that need state store --

    await adapters.state_store.initialize()

    if args.reset_crawl:
        from src.models import DEFAULT_TENANT_ID
        await adapters.state_store.reset_crawl(DEFAULT_TENANT_ID)
        logger.info("Crawl state reset")
        await adapters.state_store.close()
        return ExitCode.OK

    if args.reset_rules:
        from src.models import DEFAULT_TENANT_ID
        rules = await adapters.state_store.get_rule_candidates(DEFAULT_TENANT_ID)
        for r in rules:
            await adapters.state_store.update_rule_status(DEFAULT_TENANT_ID, r.id, "rejected")
        await adapters.config_loader.save_auto_rules([])
        logger.info("All auto-rules reset")
        await adapters.state_store.close()
        return ExitCode.OK

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
        return ExitCode.OK

    if args.learner_status:
        await _show_learner_status(adapters)
        return ExitCode.OK

    # -- Process entry points --

    if args.watchdog:
        return await _run_watchdog(adapters)

    if args.canary:
        return await _run_canary(adapters)

    if args.learn_from_trash:
        return await _run_learn_from_trash(adapters, args.learn_from_trash_max)

    if args.learn_now:
        await _run_learner(adapters)
        return ExitCode.OK

    if args.janitor:
        return await _run_janitor(adapters)

    if args.digest:
        return await _run_digest(adapters)

    if args.crawl:
        return await _run_crawl(adapters, once=False)

    # Default: Process 1 (live sync daemon)
    return await _run_live_sync(adapters, once=args.once)


# -- Process runners --


async def _run_live_sync(adapters, once: bool = False) -> int:
    from src.daemon import Daemon, DaemonFatalExit
    from src.models import ExitCode

    daemon = Daemon(adapters)
    try:
        await daemon.start()
    except DaemonFatalExit as exc:
        logger.error("Daemon fatal: %s (exit=%d)", exc.reason, exc.code)
        try:
            await adapters.state_store.close()
        except Exception:
            pass
        return exc.code
    except Exception as exc:
        logger.error("Daemon start unexpected error: %s", exc, exc_info=True)
        try:
            await adapters.state_store.log_error(
                "agent", "fatal", type(exc).__name__, str(exc),
            )
            await adapters.state_store.close()
        except Exception:
            pass
        return int(ExitCode.UNEXPECTED)

    if once:
        await daemon.run_once()
    else:
        await daemon.run_forever()

    await daemon.stop()
    return int(daemon.exit_code)


async def _run_crawl(adapters, once: bool = False) -> int:
    from src.crawl_daemon import CrawlDaemon
    from src.models import ExitCode

    crawl = CrawlDaemon(adapters)
    try:
        await crawl.start()
    except Exception as exc:
        logger.error("Crawl start failed: %s", exc)
        try:
            await adapters.state_store.close()
        except Exception:
            pass
        return int(crawl.exit_code or ExitCode.UNEXPECTED)

    if once:
        await crawl.run_once()
    else:
        await crawl.run_forever()
    await crawl.stop()
    return int(crawl.exit_code)


async def _run_learner(adapters) -> None:
    from src.models import DEFAULT_TENANT_ID
    from datetime import datetime, timezone

    settings = await adapters.config_loader.load_settings()
    from src.rule_learner import RuleLearner
    learner = RuleLearner(adapters.state_store, adapters.config_loader, settings, DEFAULT_TENANT_ID)
    stats = await learner.run()
    print(f"Learning complete: {stats}")

    # Stamp sidecar heartbeat
    try:
        existing = await adapters.process_manager.read_health()
        if existing is not None:
            ts = existing.sidecar_timestamps or {}
            ts["learner"] = datetime.now(timezone.utc).isoformat()
            existing.sidecar_timestamps = ts
            await adapters.process_manager.write_health(existing)
    except Exception:
        pass

    await adapters.state_store.close()


async def _run_janitor(adapters) -> int:
    from src.janitor import Janitor
    from src.models import ExitCode

    janitor = Janitor(adapters)
    try:
        stats = await janitor.run()
        print(f"Janitor complete: {stats}")
        return int(janitor.exit_code)
    except Exception as exc:
        logger.error("Janitor failed: %s", exc)
        return int(janitor.exit_code or ExitCode.UNEXPECTED)


async def _run_digest(adapters) -> int:
    from src.models import DEFAULT_TENANT_ID, ExitCode
    from datetime import datetime, timezone

    settings = await adapters.config_loader.load_settings()
    try:
        creds = await adapters.auth.get_credentials(DEFAULT_TENANT_ID)
    except Exception as exc:
        if _is_refresh_error(exc):
            await adapters.state_store.log_error(
                "digest", "fatal", type(exc).__name__,
                "Refresh token revoked — run: python -m src.main --reauth --mode local",
            )
            await adapters.state_store.close()
            return int(ExitCode.NEEDS_REAUTH)
        await adapters.state_store.log_error(
            "digest", "fatal", type(exc).__name__, str(exc),
        )
        await adapters.state_store.close()
        return int(ExitCode.UNEXPECTED)

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

    try:
        from src.observability import run_with_retries

        async def _send():
            await digest.build_and_send(crawl_state)

        async def _log_err(src, sev, ec, msg):
            try:
                await adapters.state_store.log_error(src, sev, ec, msg)
            except Exception:
                pass

        await run_with_retries(
            _send, source="digest", logger=logger, on_error=_log_err,
        )
    except Exception as exc:
        logger.error("Digest failed: %s", exc, exc_info=True)
        await adapters.state_store.log_error(
            "digest", "fatal", type(exc).__name__, str(exc),
        )
        await adapters.state_store.close()
        return int(ExitCode.UNEXPECTED)

    logger.info("Digest sent")

    # Stamp sidecar
    try:
        existing = await adapters.process_manager.read_health()
        if existing is not None:
            ts = existing.sidecar_timestamps or {}
            ts["digest"] = datetime.now(timezone.utc).isoformat()
            existing.sidecar_timestamps = ts
            await adapters.process_manager.write_health(existing)
    except Exception:
        pass

    await adapters.state_store.close()
    return int(ExitCode.OK)


async def _run_watchdog(adapters) -> int:
    from src.watchdog import Watchdog
    from src.models import ExitCode
    try:
        watchdog = Watchdog(adapters)
        await watchdog.run_once()
        return int(ExitCode.OK)
    except Exception as exc:
        logger.error("Watchdog error: %s", exc, exc_info=True)
        return int(ExitCode.UNEXPECTED)


async def _run_canary(adapters) -> int:
    from src.canary import Canary
    from src.models import ExitCode
    try:
        canary = Canary(adapters)
        await canary.run_once()
        return int(ExitCode.OK)
    except Exception as exc:
        logger.error("Canary error: %s", exc, exc_info=True)
        return int(ExitCode.UNEXPECTED)


async def _run_learn_from_trash(adapters, max_threads: int) -> int:
    from src.trash_learner import TrashLearner
    from src.models import ExitCode
    try:
        tl = TrashLearner(adapters)
        stats = await tl.run_once(max_threads=max_threads)
        print(
            f"Trash scan complete: scanned={stats['scanned']}, "
            f"senders_affected={stats['senders_affected']}, "
            f"meta_errors={stats['meta_errors']}"
        )
        if stats["top_offenders"]:
            print("Top trash contributors:")
            for sender, count in stats["top_offenders"]:
                print(f"  {count:5d}  {sender}")
        return int(ExitCode.OK)
    except Exception as exc:
        logger.error("TrashLearner error: %s", exc, exc_info=True)
        return int(ExitCode.UNEXPECTED)


async def _run_reauth(adapters) -> int:
    """Trigger interactive OAuth re-authorization. Opens a browser."""
    from src.models import DEFAULT_TENANT_ID, ExitCode
    try:
        await adapters.auth.authorize_new_user()
        logger.info("Re-authorization complete. Token saved to ~/.emailorganizer/token.json")
        # Clear needs_reauth from health on success
        try:
            existing = await adapters.process_manager.read_health()
            if existing is not None:
                existing.needs_reauth = False
                existing.last_error = ""
                await adapters.process_manager.write_health(existing)
        except Exception:
            pass
        return int(ExitCode.OK)
    except Exception as exc:
        logger.error("Re-authorization failed: %s", exc)
        return int(ExitCode.UNEXPECTED)


def _is_refresh_error(exc: BaseException) -> bool:
    try:
        from google.auth.exceptions import RefreshError
        if isinstance(exc, RefreshError):
            return True
    except Exception:
        pass
    return "invalid_grant" in str(exc) or "Token has been expired" in str(exc)


# -- Opinionated --status --


async def _print_status(adapters) -> int:
    """Read HealthStatus + error_log and print an opinionated report.
    Returns exit code: 0 healthy, 1 degraded, 2 critical.
    """
    from datetime import datetime, timedelta, timezone
    from src.models import ExitCode
    from collections import Counter

    health = await adapters.process_manager.read_health()
    if health is None:
        print("UNKNOWN: no status.json found. Daemon has never started.")
        print("  ACTION: python -m src.main --mode local --once")
        return 1

    now = datetime.now(timezone.utc)

    # Compute ages (best-effort)
    def _age_str(iso: str) -> str:
        try:
            dt = datetime.fromisoformat(iso)
            delta = now - dt
            secs = int(delta.total_seconds())
            if secs < 60:
                return f"{secs}s ago"
            if secs < 3600:
                return f"{secs // 60}m ago"
            if secs < 86400:
                return f"{secs // 3600}h {(secs % 3600) // 60}m ago"
            return f"{secs // 86400}d {(secs % 86400) // 3600}h ago"
        except Exception:
            return "unknown"

    critical_msgs: list[str] = []
    warning_msgs: list[str] = []

    if health.needs_reauth:
        critical_msgs.append(
            f"needs_reauth=true ({_age_str(health.last_error_ts)})\n"
            f"    ACTION: python -m src.main --reauth --mode local"
        )

    if health.last_successful_cycle:
        try:
            last = datetime.fromisoformat(health.last_successful_cycle)
            delta = now - last
            if delta > timedelta(minutes=30) and health.uptime_seconds > 1800:
                critical_msgs.append(
                    f"last_successful_cycle: {_age_str(health.last_successful_cycle)} (SLO: <30m)"
                )
        except Exception:
            pass
    elif health.uptime_seconds > 300:
        warning_msgs.append("no successful cycle recorded yet")

    if health.consecutive_error_count >= 10:
        critical_msgs.append(
            f"consecutive_errors={health.consecutive_error_count} (threshold: 10)"
        )
    elif health.consecutive_error_count >= 3:
        warning_msgs.append(f"consecutive_errors={health.consecutive_error_count}")

    if health.notifier_status == "fallback_polling":
        warning_msgs.append("notifier_status=fallback_polling (IMAP IDLE not connected)")
    elif health.notifier_status == "disconnected":
        warning_msgs.append("notifier_status=disconnected")

    if (health.threads_processed_24h == 0 and health.actions_taken_24h == 0
            and health.uptime_seconds > 3600):
        warning_msgs.append("zero threads/actions in last 24h (either quiet inbox or silent failure)")

    if health.llm_quota_exhausted:
        provider = health.llm_quota_provider or "unknown"
        ts = health.llm_quota_last_ts
        warning_msgs.append(
            f"LLM quota exhausted on provider '{provider}' (last seen {_age_str(ts)}). "
            f"Classifications are falling back to rules only. "
            f"Top up credits or switch providers in config/settings.yaml."
        )

    # Recent errors
    await adapters.state_store.initialize()
    recent = await adapters.state_store.get_recent_errors(hours=24)
    await adapters.state_store.close()

    # Sidecar staleness
    from src.watchdog import SIDECAR_INTERVALS
    sidecar_ts = health.sidecar_timestamps or {}
    for name, interval in SIDECAR_INTERVALS.items():
        if name == "agent":
            continue
        ts = sidecar_ts.get(name)
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts)
            if (now - dt).total_seconds() > 2 * interval:
                warning_msgs.append(f"sidecar '{name}' stale ({_age_str(ts)})")
        except Exception:
            pass

    # Overall verdict
    if critical_msgs:
        verdict = "CRITICAL"
        rc = int(ExitCode.UNEXPECTED)  # 2 in pragmatic terms? The contract was 2=critical in docs
        rc = 2
    elif warning_msgs:
        verdict = "DEGRADED"
        rc = 1
    else:
        verdict = "HEALTHY"
        rc = 0

    print(f"{verdict}: agent")
    print(f"  user_email:               {health.user_email or '(unknown)'}")
    print(f"  pid:                      {health.pid}")
    print(f"  uptime:                   {int(health.uptime_seconds)}s")
    print(f"  notifier_status:          {health.notifier_status}")
    print(f"  last_sync:                {_age_str(health.last_sync)}")
    print(f"  last_successful_cycle:    {_age_str(health.last_successful_cycle)}")
    print(f"  consecutive_errors:       {health.consecutive_error_count}")
    print(f"  threads_processed_24h:    {health.threads_processed_24h}")
    print(f"  actions_taken_24h:        {health.actions_taken_24h}")
    if health.last_error:
        print(f"  last_error:               {health.last_error}")

    if sidecar_ts:
        print()
        print("  sidecar heartbeats:")
        for name in sorted(sidecar_ts):
            print(f"    {name:10s} {_age_str(sidecar_ts[name])}")

    if critical_msgs:
        print()
        print("  CRITICAL:")
        for m in critical_msgs:
            print(f"    - {m}")

    if warning_msgs:
        print()
        print("  WARNINGS:")
        for m in warning_msgs:
            print(f"    - {m}")

    if recent:
        top = Counter(
            (e.get("source", "?"), e.get("error_class") or "?") for e in recent
        ).most_common(5)
        print()
        print(f"  recent errors ({len(recent)} in last 24h, top 5 classes):")
        for (src, cls), cnt in top:
            print(f"    {src}: {cls} x{cnt}")

    return rc


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
        ("com.emailorganizer.agent",    ["--mode", "local"]),
        ("com.emailorganizer.crawl",    ["--crawl", "--mode", "local"]),
        ("com.emailorganizer.learner",  ["--learn-now", "--mode", "local"]),
        ("com.emailorganizer.janitor",  ["--janitor", "--mode", "local"]),
        ("com.emailorganizer.digest",   ["--digest", "--mode", "local"]),
        ("com.emailorganizer.watchdog", ["--watchdog", "--mode", "local"]),
        ("com.emailorganizer.canary",   ["--canary", "--mode", "local"]),
        ("com.emailorganizer.deadman",  []),  # shell script; args ignored
    ]
    for label, args in services:
        mgr = LaunchdProcessManager(label=label)
        await mgr.install_service(sys.executable, args)
        logger.info("Installed %s", label)
    logger.info("All 8 services installed")


async def _uninstall_all_services() -> None:
    from src.adapters.local.launchd_process_mgr import LaunchdProcessManager

    labels = [
        "com.emailorganizer.agent",
        "com.emailorganizer.crawl",
        "com.emailorganizer.learner",
        "com.emailorganizer.janitor",
        "com.emailorganizer.digest",
        "com.emailorganizer.watchdog",
        "com.emailorganizer.canary",
        "com.emailorganizer.deadman",
    ]
    for label in labels:
        mgr = LaunchdProcessManager(label=label)
        await mgr.uninstall_service()
    logger.info("All services uninstalled")


if __name__ == "__main__":
    main()
