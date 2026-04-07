from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from src.models import AutoRule, DeployMode, ManualRule, Settings
from src.ports.config_loader import ConfigLoaderPort


class FileConfigLoader(ConfigLoaderPort):
    def __init__(self, config_dir: str | Path = "config"):
        self._dir = Path(config_dir)
        self._cache: dict[str, Any] = {}

    async def load_settings(self) -> Settings:
        raw = self._read_yaml("settings.yaml")
        s = Settings()
        s.deploy_mode = DeployMode(raw.get("deploy_mode", "local"))

        daemon = raw.get("daemon", {})
        s.memory_cap_mb = daemon.get("memory_cap_mb", s.memory_cap_mb)

        ls = raw.get("live_sync", {})
        s.imap_idle_reissue_minutes = ls.get("imap_idle_reissue_minutes", s.imap_idle_reissue_minutes)
        s.fallback_poll_minutes = ls.get("fallback_poll_minutes", s.fallback_poll_minutes)

        cr = raw.get("crawl", {})
        s.crawl_batch_size = cr.get("batch_size", s.crawl_batch_size)
        s.crawl_interval_minutes = cr.get("interval_minutes", s.crawl_interval_minutes)
        s.crawl_time_budget_seconds = cr.get("time_budget_seconds", s.crawl_time_budget_seconds)
        s.crawl_order = cr.get("order", s.crawl_order)
        s.crawl_skip_threshold = cr.get("skip_if_live_sync_exceeds", s.crawl_skip_threshold)
        s.crawl_archive_age_days = cr.get("archive_age_days", s.crawl_archive_age_days)
        s.crawl_star_max_age_days = cr.get("star_max_age_days", s.crawl_star_max_age_days)

        eng = raw.get("engagement", {})
        s.engagement_window_days = eng.get("score_window_days", s.engagement_window_days)

        unsub = raw.get("unsubscribe", {})
        s.unsub_tier1_min = unsub.get("tier1_min_emails", s.unsub_tier1_min)
        s.unsub_tier2_min = unsub.get("tier2_min_emails", s.unsub_tier2_min)
        s.unsub_tier2_max_engagement = unsub.get("tier2_max_engagement", s.unsub_tier2_max_engagement)
        s.unsub_tier3_max_engagement = unsub.get("tier3_max_engagement", s.unsub_tier3_max_engagement)
        s.unsub_persistence_days = unsub.get("persistence_check_days", s.unsub_persistence_days)
        s.unsub_quarantine_days = unsub.get("quarantine_hold_days", s.unsub_quarantine_days)

        gr = raw.get("guardrails", {})
        s.dry_run_days = gr.get("dry_run_days", s.dry_run_days)
        s.confidence_threshold = gr.get("confidence_threshold", s.confidence_threshold)
        s.quarantine_hold_days = gr.get("quarantine_hold_days", s.quarantine_hold_days)
        s.circuit_breaker_threshold = gr.get("circuit_breaker_threshold", s.circuit_breaker_threshold)
        s.daily_destructive_cap = gr.get("daily_destructive_cap", s.daily_destructive_cap)

        dg = raw.get("digest", {})
        s.digest_time = dg.get("time", s.digest_time)

        llm = raw.get("llm", {})
        s.llm_provider = llm.get("provider", s.llm_provider)
        s.llm_model = llm.get("model", s.llm_model)
        s.llm_endpoint = llm.get("endpoint", s.llm_endpoint)
        s.llm_api_key_env = llm.get("api_key_env", s.llm_api_key_env)
        s.llm_temperature = llm.get("temperature", s.llm_temperature)
        s.llm_max_tokens = llm.get("max_tokens", s.llm_max_tokens)
        s.llm_timeout = llm.get("timeout_seconds", s.llm_timeout)
        s.llm_fallback = llm.get("fallback", s.llm_fallback)
        s.llm_send_body = llm.get("send_body", s.llm_send_body)
        s.llm_max_calls_per_run = llm.get("max_calls_per_run", s.llm_max_calls_per_run)

        lr = raw.get("learner", {})
        s.learner_auto_promote_confidence = lr.get("auto_promote_confidence", s.learner_auto_promote_confidence)
        s.learner_auto_promote_min_evidence = lr.get("auto_promote_min_evidence", s.learner_auto_promote_min_evidence)
        s.learner_staging_days = lr.get("staging_days", s.learner_staging_days)
        s.learner_revalidation_days = lr.get("revalidation_window_days", s.learner_revalidation_days)
        s.learner_deprecation_accuracy = lr.get("deprecation_accuracy", s.learner_deprecation_accuracy)
        s.learner_sender_min_threads = lr.get("sender_min_threads", s.learner_sender_min_threads)
        s.learner_domain_min_senders = lr.get("domain_min_senders", s.learner_domain_min_senders)
        s.learner_keyword_min_correlation = lr.get("keyword_min_correlation", s.learner_keyword_min_correlation)

        inbox = raw.get("inbox", {})
        s.inbox_mode = inbox.get("mode", s.inbox_mode)
        s.inbox_archive_all_except = inbox.get("archive_all_except", s.inbox_archive_all_except)
        s.inbox_mark_read_categories = inbox.get("mark_read_categories", s.inbox_mark_read_categories)
        s.inbox_mark_read_keywords = inbox.get("mark_read_keywords", s.inbox_mark_read_keywords)
        s.inbox_mark_important = inbox.get("mark_important", s.inbox_mark_important)
        s.inbox_mark_not_important = inbox.get("mark_not_important", s.inbox_mark_not_important)
        s.inbox_sync_gmail_filters = inbox.get("sync_gmail_filters", s.inbox_sync_gmail_filters)
        s.inbox_filter_min_confidence = inbox.get("filter_min_confidence", s.inbox_filter_min_confidence)
        s.inbox_filter_min_evidence = inbox.get("filter_min_evidence", s.inbox_filter_min_evidence)

        return s

    async def load_manual_rules(self) -> list[ManualRule]:
        raw = self._read_yaml("rules.yaml")
        rules: list[ManualRule] = []
        for r in raw.get("rules", []):
            rules.append(ManualRule(
                type=r["type"],
                pattern=r["pattern"],
                classification=r.get("classification", ""),
                actions=r.get("actions", []),
                action_label=r.get("action_label"),
                confidence=r.get("confidence", 0.8),
            ))
        return rules

    async def load_auto_rules(self) -> list[AutoRule]:
        raw = self._read_yaml("auto_rules.yaml")
        rules: list[AutoRule] = []
        for r in raw.get("rules", []):
            rules.append(AutoRule(
                id=r["id"],
                type=r["type"],
                pattern=r["pattern"],
                classification=r.get("classification", ""),
                actions=r.get("actions", []),
                action_label=r.get("action_label"),
                status=r.get("status", "staged"),
                confidence=r.get("confidence", 0.0),
                source=r.get("source", ""),
                evidence_count=r.get("evidence_count", 0),
                created_at=r.get("created_at", ""),
                last_validated=r.get("last_validated", ""),
                accuracy=r.get("accuracy", 0.0),
            ))
        return rules

    async def save_auto_rules(self, rules: list[AutoRule]) -> None:
        data = {"rules": []}
        for r in rules:
            data["rules"].append({
                "id": r.id,
                "type": r.type,
                "pattern": r.pattern,
                "classification": r.classification,
                "actions": r.actions,
                "action_label": r.action_label,
                "status": r.status,
                "confidence": r.confidence,
                "source": r.source,
                "evidence_count": r.evidence_count,
                "created_at": r.created_at,
                "last_validated": r.last_validated,
                "accuracy": r.accuracy,
            })
        path = self._dir / "auto_rules.yaml"
        path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))

    async def load_allowlist(self) -> dict[str, list[str]]:
        raw = self._read_yaml("rules.yaml")
        al = raw.get("allowlist", {})
        return {"domains": al.get("domains", []), "addresses": al.get("addresses", [])}

    async def load_blocklist(self) -> dict[str, list[str]]:
        raw = self._read_yaml("rules.yaml")
        bl = raw.get("blocklist", {})
        return {"domains": bl.get("domains", []), "addresses": bl.get("addresses", [])}

    async def load_prompt(self, name: str) -> str:
        path = self._dir / "prompts" / f"{name}.txt"
        if not path.exists():
            raise FileNotFoundError(f"Prompt template not found: {path}")
        return path.read_text()

    async def reload(self) -> None:
        self._cache.clear()

    def _read_yaml(self, filename: str) -> dict[str, Any]:
        if filename not in self._cache:
            path = self._dir / filename
            if not path.exists():
                self._cache[filename] = {}
            else:
                with open(path) as f:
                    self._cache[filename] = yaml.safe_load(f) or {}
        return self._cache[filename]
