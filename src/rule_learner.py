from __future__ import annotations

import json
import logging
import os
import signal as signal_mod
from datetime import datetime, timezone
from pathlib import Path

from src.backtester import Backtester
from src.miners import DomainMiner, EngagementMiner, KeywordMiner, OverrideMiner, SenderMiner
from src.models import AutoRule, DEFAULT_TENANT_ID, Settings
from src.ports.config_loader import ConfigLoaderPort
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class RuleLearner:
    """Sidecar daemon: mines patterns, backtests, promotes/depreciates auto-rules."""

    def __init__(
        self,
        state_store: StateStorePort,
        config_loader: ConfigLoaderPort,
        settings: Settings,
        tenant_id: str = DEFAULT_TENANT_ID,
    ):
        self._store = state_store
        self._config = config_loader
        self._settings = settings
        self._tenant = tenant_id

    async def run(self) -> dict[str, int]:
        """Run a full learning cycle. Returns stats."""
        stats = {"candidates": 0, "promoted": 0, "staged": 0, "deprecated": 0, "rejected": 0}

        # Mine candidates
        candidates = await self._mine_all()
        stats["candidates"] = len(candidates)

        # Backtest each candidate
        backtester = Backtester(self._store, self._settings, self._tenant)
        for rule in candidates:
            existing = await self._store.get_rule_candidates(self._tenant)
            if any(r.id == rule.id for r in existing):
                continue

            accuracy = await backtester.test_rule(rule)
            if accuracy >= 0.95:
                rule.accuracy = accuracy
                if (
                    accuracy >= self._settings.learner_auto_promote_confidence
                    and rule.evidence_count >= self._settings.learner_auto_promote_min_evidence
                ):
                    rule.status = "active"
                    stats["promoted"] += 1
                    await self._store.log_rule_event(self._tenant, rule.id, "promoted")
                else:
                    rule.status = "staged"
                    stats["staged"] += 1
                    await self._store.log_rule_event(self._tenant, rule.id, "staged")
                await self._store.upsert_rule_candidate(self._tenant, rule)
            else:
                rule.status = "rejected"
                stats["rejected"] += 1
                await self._store.upsert_rule_candidate(self._tenant, rule)
                await self._store.log_rule_event(
                    self._tenant, rule.id, "rejected",
                    f"accuracy={accuracy:.2f}",
                )

        # Revalidate active rules
        deprecated = await self._revalidate()
        stats["deprecated"] = deprecated

        # Auto-promote staged rules past staging window
        promoted = await self._auto_promote_staged()
        stats["promoted"] += promoted

        # Write updated auto-rules to config
        await self._sync_to_config()

        # Notify live sync daemon to reload rules
        self._signal_daemon_reload()

        # Update LLM dependency ratio
        await self._update_dependency_ratio()

        # Accuracy analysis + prompt tuning
        await self._run_accuracy_analysis()
        await self._run_prompt_tuning()

        logger.info("Rule learner completed: %s", stats)
        return stats

    async def _mine_all(self) -> list[AutoRule]:
        candidates: list[AutoRule] = []

        sender_miner = SenderMiner(self._store, self._settings, self._tenant)
        sender_rules = await sender_miner.mine()
        candidates.extend(sender_rules)

        domain_miner = DomainMiner(self._settings)
        candidates.extend(domain_miner.mine(sender_rules))

        eng_miner = EngagementMiner(self._store, self._settings, self._tenant)
        candidates.extend(await eng_miner.mine())

        override_miner = OverrideMiner(self._store, self._tenant)
        candidates.extend(await override_miner.mine())

        keyword_miner = KeywordMiner(self._store, self._settings, self._tenant)
        candidates.extend(await keyword_miner.mine())

        return candidates

    async def _revalidate(self) -> int:
        active_rules = await self._store.get_rule_candidates(self._tenant, status="active")
        backtester = Backtester(self._store, self._settings, self._tenant)
        deprecated = 0

        for rule in active_rules:
            accuracy = await backtester.test_rule(rule)
            if accuracy < self._settings.learner_deprecation_accuracy:
                await self._store.update_rule_status(self._tenant, rule.id, "deprecated")
                await self._store.log_rule_event(
                    self._tenant, rule.id, "deprecated",
                    f"accuracy dropped to {accuracy:.2f}",
                )
                deprecated += 1
            else:
                rule.last_validated = datetime.now(timezone.utc).isoformat()
                rule.accuracy = accuracy
                await self._store.upsert_rule_candidate(self._tenant, rule)

        return deprecated

    async def _auto_promote_staged(self) -> int:
        staged = await self._store.get_rule_candidates(self._tenant, status="staged")
        promoted = 0
        now = datetime.now(timezone.utc)

        for rule in staged:
            if not rule.created_at:
                continue
            created = datetime.fromisoformat(rule.created_at)
            if (now - created).days >= self._settings.learner_staging_days:
                await self._store.update_rule_status(self._tenant, rule.id, "active")
                await self._store.log_rule_event(self._tenant, rule.id, "auto_promoted_from_staging")
                promoted += 1

        return promoted

    async def _sync_to_config(self) -> None:
        all_rules = await self._store.get_rule_candidates(self._tenant)
        exportable = [r for r in all_rules if r.status in ("active", "staged")]
        await self._config.save_auto_rules(exportable)

    async def _update_dependency_ratio(self) -> None:
        counts = await self._store.count_classifications_by_source(self._tenant)
        total = sum(counts.values()) or 1
        llm_count = counts.get("llm", 0)
        ratio = (llm_count / total) * 100
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        await self._store.record_llm_dependency(self._tenant, date_str, ratio)

    async def _run_accuracy_analysis(self) -> None:
        try:
            from src.accuracy import ConfusionAnalyzer
            analyzer = ConfusionAnalyzer(self._store, self._tenant)
            await analyzer.persist_daily()
        except Exception as exc:
            logger.error("Accuracy analysis failed: %s", exc)

    async def _run_prompt_tuning(self) -> None:
        try:
            from src.prompt_tuner import PromptTuner
            tuner = PromptTuner(self._store, tenant_id=self._tenant)
            version = await tuner.tune()
            if version:
                logger.info("Prompt tuned to version %s", version)
        except Exception as exc:
            logger.error("Prompt tuning failed: %s", exc)

    @staticmethod
    def _signal_daemon_reload() -> None:
        """Send SIGHUP to the live sync daemon to reload rules."""
        status_path = Path("~/.emailorganizer/status.json").expanduser()
        if not status_path.exists():
            logger.debug("No daemon status file found, skipping SIGHUP")
            return
        try:
            data = json.loads(status_path.read_text())
            pid = data.get("pid", 0)
            if pid and pid != os.getpid():
                os.kill(pid, signal_mod.SIGHUP)
                logger.info("Sent SIGHUP to live sync daemon (pid=%d)", pid)
        except (ProcessLookupError, PermissionError):
            logger.debug("Daemon process not found, skipping SIGHUP")
        except Exception as exc:
            logger.warning("Failed to signal daemon: %s", exc)
