from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from src.models import ActionRecord, AutoRule, CrawlState, SenderStats, DEFAULT_TENANT_ID
from src.ports.state_store import StateStorePort

_SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_threads (
    tenant_id TEXT NOT NULL DEFAULT 'default',
    thread_id TEXT NOT NULL,
    classification TEXT,
    classified_by TEXT,
    confidence REAL,
    applied_labels TEXT,
    last_action TEXT,
    updated_at TEXT,
    PRIMARY KEY (tenant_id, thread_id)
);

CREATE TABLE IF NOT EXISTS action_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    thread_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    risk_level INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    reason TEXT,
    classified_by TEXT,
    label_name TEXT,
    reversal_data TEXT,
    sender TEXT,
    sender_domain TEXT,
    run_id TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sender_stats (
    tenant_id TEXT NOT NULL DEFAULT 'default',
    sender TEXT NOT NULL,
    sender_domain TEXT,
    total_received INTEGER DEFAULT 0,
    opened INTEGER DEFAULT 0,
    replied INTEGER DEFAULT 0,
    starred INTEGER DEFAULT 0,
    manually_archived INTEGER DEFAULT 0,
    trashed INTEGER DEFAULT 0,
    spam_marked INTEGER DEFAULT 0,
    engagement_score REAL DEFAULT 0.0,
    blocklisted INTEGER DEFAULT 0,
    first_seen_at TEXT,
    last_updated TEXT,
    PRIMARY KEY (tenant_id, sender)
);

CREATE TABLE IF NOT EXISTS sync_state (
    tenant_id TEXT NOT NULL DEFAULT 'default',
    key TEXT NOT NULL,
    value TEXT,
    PRIMARY KEY (tenant_id, key)
);

CREATE TABLE IF NOT EXISTS user_overrides (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    thread_id TEXT,
    sender TEXT,
    details TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS llm_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    provider TEXT NOT NULL,
    model TEXT,
    tokens_in INTEGER DEFAULT 0,
    tokens_out INTEGER DEFAULT 0,
    latency_ms INTEGER DEFAULT 0,
    cost_estimate REAL DEFAULT 0.0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rule_candidates (
    tenant_id TEXT NOT NULL DEFAULT 'default',
    rule_id TEXT NOT NULL,
    type TEXT NOT NULL,
    pattern TEXT NOT NULL,
    classification TEXT,
    actions TEXT,
    action_label TEXT,
    status TEXT NOT NULL DEFAULT 'candidate',
    confidence REAL DEFAULT 0.0,
    source TEXT,
    evidence_count INTEGER DEFAULT 0,
    backtest_accuracy REAL,
    created_at TEXT,
    last_validated TEXT,
    PRIMARY KEY (tenant_id, rule_id)
);

CREATE TABLE IF NOT EXISTS rule_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    rule_id TEXT NOT NULL,
    event TEXT NOT NULL,
    details TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS llm_dependency_daily (
    tenant_id TEXT NOT NULL DEFAULT 'default',
    date TEXT NOT NULL,
    ratio REAL NOT NULL,
    PRIMARY KEY (tenant_id, date)
);

CREATE INDEX IF NOT EXISTS idx_action_log_tenant_thread
    ON action_log(tenant_id, thread_id);
CREATE INDEX IF NOT EXISTS idx_action_log_tenant_status
    ON action_log(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_action_log_tenant_created
    ON action_log(tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_action_log_run
    ON action_log(tenant_id, run_id);
CREATE INDEX IF NOT EXISTS idx_sender_stats_engagement
    ON sender_stats(tenant_id, engagement_score);

CREATE TABLE IF NOT EXISTS classification_feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    thread_id TEXT NOT NULL,
    sender TEXT,
    original_category TEXT,
    original_action_label TEXT,
    original_source TEXT,
    corrected_category TEXT,
    corrected_action_label TEXT,
    feedback_type TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS accuracy_daily (
    tenant_id TEXT NOT NULL DEFAULT 'default',
    date TEXT NOT NULL,
    overall_accuracy REAL,
    accuracy_by_category TEXT,
    accuracy_by_source TEXT,
    confusion_top5 TEXT,
    total_feedback INTEGER DEFAULT 0,
    PRIMARY KEY (tenant_id, date)
);

CREATE INDEX IF NOT EXISTS idx_feedback_tenant_created
    ON classification_feedback(tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_feedback_tenant_thread
    ON classification_feedback(tenant_id, thread_id);
CREATE INDEX IF NOT EXISTS idx_processed_classified_by
    ON processed_threads(tenant_id, classified_by);
CREATE INDEX IF NOT EXISTS idx_overrides_created
    ON user_overrides(tenant_id, created_at);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class SQLiteStateStore(StateStorePort):
    def __init__(self, db_path: str | Path = "~/.emailorganizer/state.db"):
        self._path = Path(db_path).expanduser()
        self._db: aiosqlite.Connection | None = None

    async def initialize(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Clean orphaned WAL/SHM files if the main DB doesn't exist
        if str(self._path) != ":memory:" and not self._path.exists():
            for suffix in ("-wal", "-shm"):
                orphan = Path(str(self._path) + suffix)
                if orphan.exists():
                    orphan.unlink()
        self._db = await aiosqlite.connect(str(self._path))
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "StateStore not initialized"
        return self._db

    # -- Processed threads --

    async def mark_thread_processed(
        self, tenant_id: str, thread_id: str, classification: str,
        classified_by: str, confidence: float,
        applied_labels: list[str] | None = None,
    ) -> None:
        labels_json = json.dumps(applied_labels) if applied_labels else "[]"
        await self.db.execute(
            """INSERT INTO processed_threads
               (tenant_id, thread_id, classification, classified_by, confidence,
                applied_labels, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, thread_id) DO UPDATE SET
                 classification=excluded.classification,
                 classified_by=excluded.classified_by,
                 confidence=excluded.confidence,
                 applied_labels=excluded.applied_labels,
                 updated_at=excluded.updated_at""",
            (tenant_id, thread_id, classification, classified_by, confidence,
             labels_json, _now()),
        )
        await self.db.commit()

    async def is_thread_processed(self, tenant_id: str, thread_id: str) -> bool:
        cur = await self.db.execute(
            "SELECT 1 FROM processed_threads WHERE tenant_id=? AND thread_id=?",
            (tenant_id, thread_id),
        )
        return await cur.fetchone() is not None

    async def get_thread_classification(
        self, tenant_id: str, thread_id: str
    ) -> dict[str, Any] | None:
        cur = await self.db.execute(
            "SELECT * FROM processed_threads WHERE tenant_id=? AND thread_id=?",
            (tenant_id, thread_id),
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    # -- Action log --

    async def log_action(self, record: ActionRecord) -> int:
        cur = await self.db.execute(
            """INSERT INTO action_log
               (tenant_id, thread_id, action_type, risk_level, status, reason,
                classified_by, label_name, reversal_data, sender, sender_domain,
                run_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                record.tenant_id, record.thread_id, record.action_type,
                record.risk_level, record.status, record.reason,
                record.classified_by, record.label_name, record.reversal_data,
                record.sender, record.sender_domain,
                None, record.created_at or _now(),
            ),
        )
        await self.db.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_actions(
        self, tenant_id: str, *, thread_id: str | None = None,
        status: str | None = None, since: str | None = None, limit: int = 100,
    ) -> list[ActionRecord]:
        clauses = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]
        if thread_id:
            clauses.append("thread_id = ?")
            params.append(thread_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if since:
            clauses.append("created_at >= ?")
            params.append(since)
        params.append(limit)
        sql = f"SELECT * FROM action_log WHERE {' AND '.join(clauses)} ORDER BY created_at DESC LIMIT ?"
        cur = await self.db.execute(sql, params)
        return [self._row_to_action(r) for r in await cur.fetchall()]

    async def update_action_status(self, action_id: int, new_status: str) -> None:
        await self.db.execute(
            "UPDATE action_log SET status=? WHERE id=?", (new_status, action_id)
        )
        await self.db.commit()

    async def get_last_run_actions(self, tenant_id: str) -> list[ActionRecord]:
        cur = await self.db.execute(
            """SELECT * FROM action_log WHERE tenant_id=?
               AND run_id = (SELECT run_id FROM action_log WHERE tenant_id=?
                             ORDER BY created_at DESC LIMIT 1)
               ORDER BY created_at DESC""",
            (tenant_id, tenant_id),
        )
        return [self._row_to_action(r) for r in await cur.fetchall()]

    async def count_actions_today(self, tenant_id: str, min_risk_level: int) -> int:
        today = _today()
        cur = await self.db.execute(
            """SELECT COUNT(*) FROM action_log
               WHERE tenant_id=? AND risk_level>=? AND status='executed'
               AND created_at >= ?""",
            (tenant_id, min_risk_level, today),
        )
        row = await cur.fetchone()
        return row[0] if row else 0

    async def get_quarantined_expired(
        self, tenant_id: str, max_age_days: int
    ) -> list[ActionRecord]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
        cur = await self.db.execute(
            """SELECT * FROM action_log
               WHERE tenant_id=? AND status='quarantine' AND created_at <= ?
               ORDER BY created_at ASC""",
            (tenant_id, cutoff),
        )
        return [self._row_to_action(r) for r in await cur.fetchall()]

    async def get_actions_for_thread(
        self, tenant_id: str, thread_id: str
    ) -> list[ActionRecord]:
        cur = await self.db.execute(
            """SELECT * FROM action_log
               WHERE tenant_id=? AND thread_id=? AND status='executed'
               ORDER BY created_at DESC""",
            (tenant_id, thread_id),
        )
        return [self._row_to_action(r) for r in await cur.fetchall()]

    # -- Sender stats --

    async def upsert_sender_stats(self, stats: SenderStats) -> None:
        await self.db.execute(
            """INSERT INTO sender_stats
               (tenant_id, sender, sender_domain, total_received, opened, replied,
                starred, manually_archived, trashed, spam_marked,
                engagement_score, blocklisted, first_seen_at, last_updated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, sender) DO UPDATE SET
                 total_received=excluded.total_received,
                 opened=excluded.opened,
                 replied=excluded.replied,
                 starred=excluded.starred,
                 manually_archived=excluded.manually_archived,
                 trashed=excluded.trashed,
                 spam_marked=excluded.spam_marked,
                 engagement_score=excluded.engagement_score,
                 blocklisted=excluded.blocklisted,
                 last_updated=excluded.last_updated""",
            (
                stats.tenant_id, stats.sender, stats.sender_domain,
                stats.total_received, stats.opened, stats.replied,
                stats.starred, stats.manually_archived, stats.trashed, stats.spam_marked,
                stats.engagement_score, int(stats.blocklisted),
                stats.first_seen_at or _now(), stats.last_updated or _now(),
            ),
        )
        await self.db.commit()

    async def get_sender_stats(self, tenant_id: str, sender: str) -> SenderStats | None:
        cur = await self.db.execute(
            "SELECT * FROM sender_stats WHERE tenant_id=? AND sender=?",
            (tenant_id, sender),
        )
        row = await cur.fetchone()
        return self._row_to_sender(row) if row else None

    async def get_senders_by_engagement(
        self, tenant_id: str, max_score: float, min_emails: int, limit: int = 100,
    ) -> list[SenderStats]:
        cur = await self.db.execute(
            """SELECT * FROM sender_stats
               WHERE tenant_id=? AND engagement_score<=? AND total_received>=?
               AND blocklisted=0
               ORDER BY engagement_score ASC LIMIT ?""",
            (tenant_id, max_score, min_emails, limit),
        )
        return [self._row_to_sender(r) for r in await cur.fetchall()]

    async def blocklist_sender(self, tenant_id: str, sender: str) -> None:
        await self.db.execute(
            "UPDATE sender_stats SET blocklisted=1 WHERE tenant_id=? AND sender=?",
            (tenant_id, sender),
        )
        await self.db.commit()

    async def is_sender_blocklisted(self, tenant_id: str, sender: str) -> bool:
        cur = await self.db.execute(
            "SELECT blocklisted FROM sender_stats WHERE tenant_id=? AND sender=?",
            (tenant_id, sender),
        )
        row = await cur.fetchone()
        return bool(row and row[0])

    # -- Sync state --

    async def get_sync_value(self, tenant_id: str, key: str) -> str | None:
        cur = await self.db.execute(
            "SELECT value FROM sync_state WHERE tenant_id=? AND key=?",
            (tenant_id, key),
        )
        row = await cur.fetchone()
        return row[0] if row else None

    async def set_sync_value(self, tenant_id: str, key: str, value: str) -> None:
        await self.db.execute(
            """INSERT INTO sync_state (tenant_id, key, value) VALUES (?, ?, ?)
               ON CONFLICT(tenant_id, key) DO UPDATE SET value=excluded.value""",
            (tenant_id, key, value),
        )
        await self.db.commit()

    # -- Crawl --

    async def get_crawl_state(self, tenant_id: str) -> CrawlState:
        page_token = await self.get_sync_value(tenant_id, "crawl_page_token")
        threads_done = await self.get_sync_value(tenant_id, "crawl_threads_processed")
        total = await self.get_sync_value(tenant_id, "crawl_total_estimate")
        complete = await self.get_sync_value(tenant_id, "crawl_complete")
        started = await self.get_sync_value(tenant_id, "crawl_started_at")
        return CrawlState(
            page_token=page_token,
            threads_processed=int(threads_done) if threads_done else 0,
            total_estimate=int(total) if total else 0,
            is_complete=complete == "true",
            started_at=started or "",
        )

    async def update_crawl_state(self, tenant_id: str, state: CrawlState) -> None:
        await self.set_sync_value(tenant_id, "crawl_page_token", state.page_token or "")
        await self.set_sync_value(tenant_id, "crawl_threads_processed", str(state.threads_processed))
        await self.set_sync_value(tenant_id, "crawl_total_estimate", str(state.total_estimate))
        await self.set_sync_value(tenant_id, "crawl_complete", str(state.is_complete).lower())
        if state.started_at:
            await self.set_sync_value(tenant_id, "crawl_started_at", state.started_at)

    async def reset_crawl(self, tenant_id: str) -> None:
        for key in ("crawl_page_token", "crawl_threads_processed",
                     "crawl_total_estimate", "crawl_complete", "crawl_started_at"):
            await self.db.execute(
                "DELETE FROM sync_state WHERE tenant_id=? AND key=?", (tenant_id, key)
            )
        await self.db.commit()

    # -- User overrides --

    async def record_override(
        self, tenant_id: str, thread_id: str, sender: str, details: str
    ) -> None:
        await self.db.execute(
            """INSERT INTO user_overrides (tenant_id, thread_id, sender, details, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (tenant_id, thread_id, sender, details, _now()),
        )
        await self.db.commit()

    async def get_overrides(
        self, tenant_id: str, sender: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        if sender:
            cur = await self.db.execute(
                "SELECT * FROM user_overrides WHERE tenant_id=? AND sender=? ORDER BY created_at DESC LIMIT ?",
                (tenant_id, sender, limit),
            )
        else:
            cur = await self.db.execute(
                "SELECT * FROM user_overrides WHERE tenant_id=? ORDER BY created_at DESC LIMIT ?",
                (tenant_id, limit),
            )
        return [dict(r) for r in await cur.fetchall()]

    # -- LLM usage --

    async def log_llm_usage(
        self, tenant_id: str, provider: str, model: str,
        tokens_in: int, tokens_out: int, latency_ms: int, cost_estimate: float,
    ) -> None:
        await self.db.execute(
            """INSERT INTO llm_usage
               (tenant_id, provider, model, tokens_in, tokens_out, latency_ms, cost_estimate, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (tenant_id, provider, model, tokens_in, tokens_out, latency_ms, cost_estimate, _now()),
        )
        await self.db.commit()

    async def get_llm_usage_today(self, tenant_id: str) -> dict[str, Any]:
        today = _today()
        cur = await self.db.execute(
            """SELECT COUNT(*) as calls,
                      COALESCE(SUM(tokens_in), 0) as total_tokens_in,
                      COALESCE(SUM(tokens_out), 0) as total_tokens_out,
                      COALESCE(SUM(cost_estimate), 0.0) as total_cost
               FROM llm_usage WHERE tenant_id=? AND created_at >= ?""",
            (tenant_id, today),
        )
        row = await cur.fetchone()
        return dict(row) if row else {"calls": 0, "total_tokens_in": 0, "total_tokens_out": 0, "total_cost": 0.0}

    # -- Rule candidates --

    async def upsert_rule_candidate(self, tenant_id: str, rule: AutoRule) -> None:
        await self.db.execute(
            """INSERT INTO rule_candidates
               (tenant_id, rule_id, type, pattern, classification, actions,
                action_label, status, confidence, source, evidence_count,
                backtest_accuracy, created_at, last_validated)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, rule_id) DO UPDATE SET
                 status=excluded.status, confidence=excluded.confidence,
                 evidence_count=excluded.evidence_count,
                 backtest_accuracy=excluded.backtest_accuracy,
                 last_validated=excluded.last_validated""",
            (
                tenant_id, rule.id, rule.type, rule.pattern, rule.classification,
                json.dumps(rule.actions), rule.action_label, rule.status,
                rule.confidence, rule.source, rule.evidence_count,
                rule.accuracy, rule.created_at or _now(), rule.last_validated or _now(),
            ),
        )
        await self.db.commit()

    async def get_rule_candidates(
        self, tenant_id: str, status: str | None = None
    ) -> list[AutoRule]:
        if status:
            cur = await self.db.execute(
                "SELECT * FROM rule_candidates WHERE tenant_id=? AND status=?",
                (tenant_id, status),
            )
        else:
            cur = await self.db.execute(
                "SELECT * FROM rule_candidates WHERE tenant_id=?", (tenant_id,)
            )
        return [self._row_to_auto_rule(r) for r in await cur.fetchall()]

    async def update_rule_status(self, tenant_id: str, rule_id: str, new_status: str) -> None:
        await self.db.execute(
            "UPDATE rule_candidates SET status=? WHERE tenant_id=? AND rule_id=?",
            (new_status, tenant_id, rule_id),
        )
        await self.db.commit()

    async def log_rule_event(
        self, tenant_id: str, rule_id: str, event: str, details: str = ""
    ) -> None:
        await self.db.execute(
            "INSERT INTO rule_history (tenant_id, rule_id, event, details, created_at) VALUES (?, ?, ?, ?, ?)",
            (tenant_id, rule_id, event, details, _now()),
        )
        await self.db.commit()

    # -- LLM dependency tracking --

    async def record_llm_dependency(self, tenant_id: str, date: str, ratio: float) -> None:
        await self.db.execute(
            """INSERT INTO llm_dependency_daily (tenant_id, date, ratio)
               VALUES (?, ?, ?)
               ON CONFLICT(tenant_id, date) DO UPDATE SET ratio=excluded.ratio""",
            (tenant_id, date, ratio),
        )
        await self.db.commit()

    async def get_llm_dependency_trend(
        self, tenant_id: str, days: int = 30
    ) -> list[dict[str, Any]]:
        cur = await self.db.execute(
            """SELECT date, ratio FROM llm_dependency_daily
               WHERE tenant_id=? ORDER BY date DESC LIMIT ?""",
            (tenant_id, days),
        )
        return [dict(r) for r in await cur.fetchall()]

    # -- Aggregations --

    async def get_llm_classified_senders(
        self, tenant_id: str, min_threads: int
    ) -> list[dict[str, Any]]:
        cur = await self.db.execute(
            """SELECT p.classification, COUNT(*) as cnt,
                      -- extract sender from action_log reason or processed_threads
                      p.thread_id
               FROM processed_threads p
               WHERE p.tenant_id=? AND p.classified_by='llm'
               GROUP BY p.classification
               HAVING cnt >= ?
               ORDER BY cnt DESC""",
            (tenant_id, min_threads),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def count_classifications_by_source(
        self, tenant_id: str, since: str | None = None
    ) -> dict[str, int]:
        if since:
            cur = await self.db.execute(
                """SELECT classified_by, COUNT(*) as cnt FROM processed_threads
                   WHERE tenant_id=? AND updated_at >= ? GROUP BY classified_by""",
                (tenant_id, since),
            )
        else:
            cur = await self.db.execute(
                "SELECT classified_by, COUNT(*) as cnt FROM processed_threads WHERE tenant_id=? GROUP BY classified_by",
                (tenant_id,),
            )
        return {row["classified_by"]: row["cnt"] for row in await cur.fetchall()}

    # -- Classification feedback --

    async def record_feedback(
        self, tenant_id: str, thread_id: str, sender: str,
        original_category: str, original_action_label: str, original_source: str,
        corrected_category: str, corrected_action_label: str,
        feedback_type: str,
    ) -> None:
        await self.db.execute(
            """INSERT INTO classification_feedback
               (tenant_id, thread_id, sender, original_category, original_action_label,
                original_source, corrected_category, corrected_action_label,
                feedback_type, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (tenant_id, thread_id, sender, original_category, original_action_label,
             original_source, corrected_category, corrected_action_label,
             feedback_type, _now()),
        )
        await self.db.commit()

    async def get_confusion_matrix(
        self, tenant_id: str, since: str | None = None
    ) -> list[dict[str, Any]]:
        if since:
            cur = await self.db.execute(
                """SELECT original_category AS predicted, corrected_category AS actual,
                          COUNT(*) AS cnt
                   FROM classification_feedback
                   WHERE tenant_id=? AND created_at >= ?
                     AND corrected_category IS NOT NULL AND corrected_category != ''
                   GROUP BY original_category, corrected_category
                   ORDER BY cnt DESC""",
                (tenant_id, since),
            )
        else:
            cur = await self.db.execute(
                """SELECT original_category AS predicted, corrected_category AS actual,
                          COUNT(*) AS cnt
                   FROM classification_feedback
                   WHERE tenant_id=?
                     AND corrected_category IS NOT NULL AND corrected_category != ''
                   GROUP BY original_category, corrected_category
                   ORDER BY cnt DESC""",
                (tenant_id,),
            )
        return [dict(r) for r in await cur.fetchall()]

    async def get_accuracy_by_category(
        self, tenant_id: str, since: str | None = None
    ) -> dict[str, dict[str, Any]]:
        where = "WHERE p.tenant_id=?"
        params: list[Any] = [tenant_id]
        if since:
            where += " AND p.updated_at >= ?"
            params.append(since)

        cur = await self.db.execute(
            f"""SELECT p.classification AS cat,
                       COUNT(*) AS total,
                       COUNT(f.id) AS feedback_count
                FROM processed_threads p
                LEFT JOIN classification_feedback f
                  ON p.tenant_id = f.tenant_id AND p.thread_id = f.thread_id
                {where}
                GROUP BY p.classification""",
            params,
        )
        result: dict[str, dict[str, Any]] = {}
        for row in await cur.fetchall():
            cat = row["cat"] or "Unknown"
            total = row["total"]
            fb = row["feedback_count"]
            result[cat] = {
                "total": total,
                "feedback_count": fb,
                "accuracy": ((total - fb) / total * 100) if total > 0 else 100.0,
            }
        return result

    async def get_accuracy_by_source(
        self, tenant_id: str, since: str | None = None
    ) -> dict[str, dict[str, Any]]:
        where = "WHERE p.tenant_id=?"
        params: list[Any] = [tenant_id]
        if since:
            where += " AND p.updated_at >= ?"
            params.append(since)

        cur = await self.db.execute(
            f"""SELECT p.classified_by AS src,
                       COUNT(*) AS total,
                       COUNT(f.id) AS feedback_count
                FROM processed_threads p
                LEFT JOIN classification_feedback f
                  ON p.tenant_id = f.tenant_id AND p.thread_id = f.thread_id
                {where}
                GROUP BY p.classified_by""",
            params,
        )
        result: dict[str, dict[str, Any]] = {}
        for row in await cur.fetchall():
            src = row["src"] or "unknown"
            total = row["total"]
            fb = row["feedback_count"]
            result[src] = {
                "total": total,
                "feedback_count": fb,
                "accuracy": ((total - fb) / total * 100) if total > 0 else 100.0,
            }
        return result

    async def get_confidence_buckets(
        self, tenant_id: str
    ) -> list[dict[str, Any]]:
        cur = await self.db.execute(
            """SELECT
                 CASE
                   WHEN p.confidence < 0.5 THEN '0.0-0.5'
                   WHEN p.confidence < 0.7 THEN '0.5-0.7'
                   WHEN p.confidence < 0.9 THEN '0.7-0.9'
                   ELSE '0.9-1.0'
                 END AS bucket,
                 COUNT(*) AS total,
                 COUNT(f.id) AS with_feedback
               FROM processed_threads p
               LEFT JOIN classification_feedback f
                 ON p.tenant_id = f.tenant_id AND p.thread_id = f.thread_id
               WHERE p.tenant_id=?
               GROUP BY bucket
               ORDER BY bucket""",
            (tenant_id,),
        )
        results: list[dict[str, Any]] = []
        for row in await cur.fetchall():
            total = row["total"]
            fb = row["with_feedback"]
            results.append({
                "bucket": row["bucket"],
                "total": total,
                "with_feedback": fb,
                "accuracy": ((total - fb) / total * 100) if total > 0 else 100.0,
            })
        return results

    async def save_accuracy_daily(
        self, tenant_id: str, date: str, overall: float,
        by_category: str, by_source: str, confusion: str, total_feedback: int,
    ) -> None:
        await self.db.execute(
            """INSERT INTO accuracy_daily
               (tenant_id, date, overall_accuracy, accuracy_by_category,
                accuracy_by_source, confusion_top5, total_feedback)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant_id, date) DO UPDATE SET
                 overall_accuracy=excluded.overall_accuracy,
                 accuracy_by_category=excluded.accuracy_by_category,
                 accuracy_by_source=excluded.accuracy_by_source,
                 confusion_top5=excluded.confusion_top5,
                 total_feedback=excluded.total_feedback""",
            (tenant_id, date, overall, by_category, by_source, confusion, total_feedback),
        )
        await self.db.commit()

    async def get_accuracy_trend(
        self, tenant_id: str, days: int = 30
    ) -> list[dict[str, Any]]:
        cur = await self.db.execute(
            """SELECT * FROM accuracy_daily
               WHERE tenant_id=? ORDER BY date DESC LIMIT ?""",
            (tenant_id, days),
        )
        return [dict(r) for r in await cur.fetchall()]

    # -- Row converters --

    @staticmethod
    def _row_to_action(row: aiosqlite.Row) -> ActionRecord:
        return ActionRecord(
            id=row["id"],
            tenant_id=row["tenant_id"],
            thread_id=row["thread_id"],
            action_type=row["action_type"],
            risk_level=row["risk_level"],
            status=row["status"],
            reason=row["reason"] or "",
            classified_by=row["classified_by"] or "",
            label_name=row["label_name"],
            reversal_data=row["reversal_data"],
            sender=row["sender"] or "",
            sender_domain=row["sender_domain"] or "",
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_sender(row: aiosqlite.Row) -> SenderStats:
        return SenderStats(
            tenant_id=row["tenant_id"],
            sender=row["sender"],
            sender_domain=row["sender_domain"] or "",
            total_received=row["total_received"],
            opened=row["opened"],
            replied=row["replied"],
            starred=row["starred"],
            manually_archived=row["manually_archived"],
            trashed=row["trashed"],
            spam_marked=row["spam_marked"],
            engagement_score=row["engagement_score"],
            blocklisted=bool(row["blocklisted"]),
            first_seen_at=row["first_seen_at"] or "",
            last_updated=row["last_updated"] or "",
        )

    @staticmethod
    def _row_to_auto_rule(row: aiosqlite.Row) -> AutoRule:
        actions = json.loads(row["actions"]) if row["actions"] else []
        return AutoRule(
            id=row["rule_id"],
            type=row["type"],
            pattern=row["pattern"],
            classification=row["classification"] or "",
            actions=actions,
            action_label=row["action_label"],
            status=row["status"],
            confidence=row["confidence"],
            source=row["source"] or "",
            evidence_count=row["evidence_count"],
            created_at=row["created_at"] or "",
            last_validated=row["last_validated"] or "",
            accuracy=row["backtest_accuracy"] or 0.0,
        )
