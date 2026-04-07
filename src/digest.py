from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from src.gmail_client import GmailClient
from src.llm_provider import LLMProviderChain
from src.models import ActionRecord, CrawlState, DEFAULT_TENANT_ID, Settings
from src.ports.config_loader import ConfigLoaderPort
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class DigestBuilder:
    """Builds and sends the daily digest email."""

    def __init__(
        self,
        gmail: GmailClient,
        state_store: StateStorePort,
        llm_chain: LLMProviderChain,
        settings: Settings,
        tenant_id: str = DEFAULT_TENANT_ID,
        config_loader: ConfigLoaderPort | None = None,
    ):
        self._gmail = gmail
        self._store = state_store
        self._llm = llm_chain
        self._settings = settings
        self._tenant = tenant_id
        self._config = config_loader

    async def should_send(self) -> bool:
        last_sent = await self._store.get_sync_value(self._tenant, "last_digest_sent")
        if not last_sent:
            return True
        last_dt = datetime.fromisoformat(last_sent)
        return (datetime.now(timezone.utc) - last_dt) >= timedelta(hours=20)

    async def build_and_send(self, crawl_state: CrawlState | None = None) -> None:
        since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

        actions = await self._store.get_actions(self._tenant, since=since, limit=1000)
        llm_usage = await self._store.get_llm_usage_today(self._tenant)
        class_counts = await self._store.count_classifications_by_source(self._tenant, since=since)
        dependency_trend = await self._store.get_llm_dependency_trend(self._tenant, days=7)
        overrides = await self._store.get_overrides(self._tenant, limit=100)
        recent_overrides = [o for o in overrides if o.get("created_at", "") >= since]

        total = sum(class_counts.values()) or 1
        llm_count = class_counts.get("llm", 0)
        llm_pct = (llm_count / total) * 100

        # Accuracy analysis
        accuracy_data: dict[str, Any] = {}
        try:
            from src.accuracy import ConfusionAnalyzer
            analyzer = ConfusionAnalyzer(self._store, self._tenant)
            accuracy_data = await analyzer.analyze(since)
        except Exception:
            pass

        html = self._render_html(actions, llm_usage, class_counts, llm_pct,
                                 dependency_trend, crawl_state, recent_overrides,
                                 accuracy_data)

        digest_prompt = ""
        if self._config:
            try:
                digest_prompt = await self._config.load_prompt("digest")
            except Exception:
                pass

        llm_summary = ""
        if digest_prompt and self._settings.llm_provider != "none":
            llm_summary = await self._llm.summarize_digest(actions, digest_prompt)

        if llm_summary:
            html = f"<h3>AI Summary</h3><p>{llm_summary}</p><hr/>" + html

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self._gmail.send_self_email(
            subject=f"📬 Email Organizer Digest - {date_str}",
            body_html=html,
        )

        await self._store.set_sync_value(
            self._tenant, "last_digest_sent",
            datetime.now(timezone.utc).isoformat(),
        )
        await self._store.record_llm_dependency(self._tenant, date_str, llm_pct)
        logger.info("Digest sent for %s", date_str)

    def _render_html(
        self,
        actions: list[ActionRecord],
        llm_usage: dict[str, Any],
        class_counts: dict[str, int],
        llm_pct: float,
        dependency_trend: list[dict[str, Any]],
        crawl_state: CrawlState | None,
        overrides: list[dict[str, Any]] | None = None,
        accuracy_data: dict[str, Any] | None = None,
    ) -> str:
        executed = [a for a in actions if a.status == "executed"]
        dry_run = [a for a in actions if a.status == "dry_run"]
        quarantined = [a for a in actions if a.status == "quarantine"]

        labels = sum(1 for a in executed if a.action_type == "label")
        archived = sum(1 for a in executed if a.action_type == "archive")
        starred = sum(1 for a in executed if a.action_type == "star")
        unsubscribed = sum(1 for a in executed if a.action_type == "unsubscribe")
        spammed = sum(1 for a in executed if a.action_type == "spam")

        sections = [
            "<h2>Email Organizer - Daily Digest</h2>",
            "<h3>Actions Taken (last 24h)</h3>",
            "<ul>",
            f"<li>Labels applied: {labels}</li>",
            f"<li>Threads archived: {archived}</li>",
            f"<li>Threads starred: {starred}</li>",
            f"<li>Unsubscribes executed: {unsubscribed}</li>",
            f"<li>Spam: {spammed}</li>",
            f"<li>Quarantined (pending review): {len(quarantined)}</li>",
            f"<li>Dry-run (would-do): {len(dry_run)}</li>",
            "</ul>",
        ]

        sections.extend([
            "<h3>Classification Sources</h3>",
            "<ul>",
            *[f"<li>{src}: {cnt}</li>" for src, cnt in class_counts.items()],
            "</ul>",
        ])

        sections.extend([
            "<h3>LLM Usage</h3>",
            "<ul>",
            f"<li>Calls today: {llm_usage.get('calls', 0)}</li>",
            f"<li>Tokens: {llm_usage.get('total_tokens_in', 0)} in / {llm_usage.get('total_tokens_out', 0)} out</li>",
            f"<li>Est. cost: ${llm_usage.get('total_cost', 0):.4f}</li>",
            f"<li>LLM dependency: {llm_pct:.1f}%</li>",
            "</ul>",
        ])

        if dependency_trend:
            sections.append("<h3>LLM Dependency Trend (7d)</h3><ul>")
            for d in reversed(dependency_trend):
                sections.append(f"<li>{d['date']}: {d['ratio']:.1f}%</li>")
            sections.append("</ul>")

        # Agent performance section
        if accuracy_data:
            overall_acc = accuracy_data.get("overall_accuracy", 100.0)
            total_fb = accuracy_data.get("total_feedback", 0)
            sections.extend([
                "<h3>Agent Performance</h3>",
                "<ul>",
                f"<li>Overall accuracy: {overall_acc:.1f}%</li>",
                f"<li>Total corrections: {total_fb}</li>",
                "</ul>",
            ])

            by_cat = accuracy_data.get("by_category", {})
            if by_cat:
                sections.append("<h4>Accuracy by Category</h4><ul>")
                for cat, data in sorted(by_cat.items(), key=lambda x: x[1].get("accuracy", 100)):
                    sections.append(
                        f"<li>{cat}: {data.get('accuracy', 100):.1f}% "
                        f"({data.get('feedback_count', 0)} corrections / {data.get('total', 0)} total)</li>"
                    )
                sections.append("</ul>")

            by_src = accuracy_data.get("by_source", {})
            if by_src:
                sections.append("<h4>Accuracy by Source</h4><ul>")
                for src, data in sorted(by_src.items(), key=lambda x: x[1].get("accuracy", 100)):
                    sections.append(
                        f"<li>{src}: {data.get('accuracy', 100):.1f}% "
                        f"({data.get('feedback_count', 0)} corrections / {data.get('total', 0)} total)</li>"
                    )
                sections.append("</ul>")

            confusion = accuracy_data.get("top_confusion_pairs", [])
            if confusion:
                sections.append("<h4>Top Confusion Pairs</h4><ul>")
                for pair in confusion[:5]:
                    sections.append(
                        f"<li>{pair.get('predicted', '?')} misclassified as "
                        f"{pair.get('actual', '?')}: {pair.get('cnt', 0)} times</li>"
                    )
                sections.append("</ul>")

            calibration = accuracy_data.get("confidence_calibration", [])
            if calibration:
                sections.append("<h4>Confidence Calibration</h4><ul>")
                for bucket in calibration:
                    sections.append(
                        f"<li>Confidence {bucket['bucket']}: "
                        f"{bucket['accuracy']:.1f}% actual accuracy "
                        f"({bucket['total']} classifications)</li>"
                    )
                sections.append("</ul>")
        else:
            override_count = len(overrides) if overrides else 0
            total_executed = len(executed)
            accuracy = ((total_executed - override_count) / total_executed * 100) if total_executed > 0 else 100.0
            sections.extend([
                "<h3>Agent Performance</h3>",
                "<ul>",
                f"<li>User overrides (corrections): {override_count}</li>",
                f"<li>Accuracy rate: {accuracy:.1f}%</li>",
                "</ul>",
            ])

        if crawl_state and not crawl_state.is_complete:
            pct = 0.0
            if crawl_state.total_estimate > 0:
                pct = (crawl_state.threads_processed / crawl_state.total_estimate) * 100
            sections.extend([
                "<h3>Background Crawl</h3>",
                "<ul>",
                f"<li>Threads processed: {crawl_state.threads_processed:,}</li>",
                f"<li>Estimated total: {crawl_state.total_estimate:,}</li>",
                f"<li>Progress: {pct:.1f}%</li>",
                "</ul>",
            ])
        elif crawl_state and crawl_state.is_complete:
            sections.append("<p><b>Background crawl complete.</b></p>")

        return "\n".join(sections)
