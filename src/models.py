from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


# -- Enums --

class DeployMode(enum.Enum):
    LOCAL = "local"
    CLOUD = "cloud"


class RiskLevel(enum.IntEnum):
    SAFE = 0       # labels, stars
    LOW = 1        # archive, mark read
    MEDIUM = 2     # spam, unsubscribe
    HIGH = 3       # trash, post-unsub spam escalation


class ActionType(enum.Enum):
    LABEL = "label"
    REMOVE_LABEL = "remove_label"
    ARCHIVE = "archive"
    UNARCHIVE = "unarchive"
    STAR = "star"
    UNSTAR = "unstar"
    MARK_READ = "mark_read"
    SPAM = "spam"
    UNSPAM = "unspam"
    TRASH = "trash"
    UNTRASH = "untrash"
    UNSUBSCRIBE = "unsubscribe"


class ActionStatus(enum.Enum):
    EXECUTED = "executed"
    DRY_RUN = "dry_run"
    QUARANTINE = "quarantine"
    SKIPPED = "skipped"
    UNDONE = "undone"


class ClassifiedBy(enum.Enum):
    MANUAL_RULE = "manual_rule"
    AUTO_RULE = "auto_rule"
    LLM = "llm"
    GMAIL_CATEGORY = "gmail_category"


class UnsubscribeTier(enum.IntEnum):
    IMMEDIATE = 1
    QUARANTINE = 2
    FLAG_ONLY = 3
    KEEP = 4


class AutoRuleStatus(enum.Enum):
    CANDIDATE = "candidate"
    BACKTESTED = "backtested"
    STAGED = "staged"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    REJECTED = "rejected"


class AutoRuleSource(enum.Enum):
    LLM_CONSENSUS = "llm_consensus"
    USER_OVERRIDE = "user_override"
    ENGAGEMENT = "engagement"
    KEYWORD = "keyword"


class NotifierStatus(enum.Enum):
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FALLBACK_POLLING = "fallback_polling"
    DISCONNECTED = "disconnected"


# -- Label Constants --

ACTION_LABELS = ("@Action", "@Waiting", "@Read", "@Reference")
CATEGORY_LABELS = ("Finance", "Travel", "Shopping", "Accounts", "Newsletters", "Personal")
SYSTEM_LABELS = ("_auto/quarantine", "_auto/unsubscribe", "_auto/dry-run")
ALL_MANAGED_LABELS = ACTION_LABELS + CATEGORY_LABELS + SYSTEM_LABELS

LABEL_COLORS = {
    "@Action": {"backgroundColor": "#fb4c2f", "textColor": "#ffffff"},
    "@Waiting": {"backgroundColor": "#4986e7", "textColor": "#ffffff"},
    "@Read": {"backgroundColor": "#b99aff", "textColor": "#ffffff"},
    "@Reference": {"backgroundColor": "#68dfa9", "textColor": "#000000"},
    "_auto/quarantine": {"backgroundColor": "#cccccc", "textColor": "#000000"},
    "_auto/unsubscribe": {"backgroundColor": "#cccccc", "textColor": "#000000"},
    "_auto/dry-run": {"backgroundColor": "#cccccc", "textColor": "#000000"},
}

LABEL_VISIBILITY = {
    **{label: "labelShow" for label in ACTION_LABELS + CATEGORY_LABELS},
    **{label: "labelShowIfUnread" for label in SYSTEM_LABELS},
}

DEFAULT_TENANT_ID = "default"
MAX_CUSTOM_LABELS = 15


# -- Data Classes --

@dataclass
class ThreadMetadata:
    thread_id: str
    subject: str
    sender: str
    sender_domain: str
    snippet: str
    gmail_categories: list[str] = field(default_factory=list)
    label_ids: list[str] = field(default_factory=list)
    has_unsubscribe: bool = False
    unsubscribe_header: str | None = None
    unsubscribe_post: str | None = None
    is_unread: bool = False
    has_user_reply: bool = False
    message_count: int = 1
    date: datetime | None = None
    history_id: str | None = None
    raw_headers: dict[str, str] = field(default_factory=dict)


@dataclass
class Classification:
    category: str | None = None
    action_label: str | None = None
    should_archive: bool = False
    should_star: bool = False
    confidence: float = 0.0
    reason: str = ""
    classified_by: ClassifiedBy = ClassifiedBy.MANUAL_RULE


@dataclass
class ProposedAction:
    thread_id: str
    action_type: ActionType
    risk_level: RiskLevel
    confidence: float
    reason: str
    classified_by: ClassifiedBy
    label_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ActionRecord:
    id: int | None = None
    tenant_id: str = DEFAULT_TENANT_ID
    thread_id: str = ""
    action_type: str = ""
    risk_level: int = 0
    status: str = ""
    reason: str = ""
    classified_by: str = ""
    label_name: str | None = None
    reversal_data: str | None = None
    created_at: str = ""


@dataclass
class SenderStats:
    tenant_id: str = DEFAULT_TENANT_ID
    sender: str = ""
    sender_domain: str = ""
    total_received: int = 0
    opened: int = 0
    replied: int = 0
    engagement_score: float = 0.0
    blocklisted: bool = False
    last_updated: str = ""


@dataclass
class CrawlState:
    page_token: str | None = None
    query: str = ""
    threads_processed: int = 0
    total_estimate: int = 0
    is_complete: bool = False
    started_at: str = ""


@dataclass
class AutoRule:
    id: str
    type: str                          # sender | domain | keyword
    pattern: str
    classification: str
    actions: list[str] = field(default_factory=list)
    action_label: str | None = None
    status: str = "candidate"
    confidence: float = 0.0
    source: str = "llm_consensus"
    evidence_count: int = 0
    created_at: str = ""
    last_validated: str = ""
    accuracy: float = 0.0


@dataclass
class HealthStatus:
    pid: int = 0
    uptime_seconds: float = 0.0
    memory_mb: float = 0.0
    notifier_status: str = "disconnected"
    last_sync: str = ""
    threads_processed_last_run: int = 0
    actions_taken_last_run: int = 0
    crawl_progress_pct: float = 0.0
    crawl_threads_done: int = 0
    crawl_total_estimate: int = 0
    llm_provider: str = "none"
    llm_model: str = ""
    llm_dependency_pct: float = 0.0
    last_error: str = ""
    deploy_mode: str = "local"


@dataclass
class ManualRule:
    type: str                          # sender | domain | keyword
    pattern: str
    classification: str
    actions: list[str] = field(default_factory=list)
    action_label: str | None = None
    confidence: float = 0.8


@dataclass
class Settings:
    deploy_mode: DeployMode = DeployMode.LOCAL
    imap_idle_reissue_minutes: int = 25
    fallback_poll_minutes: int = 5
    crawl_batch_size: int = 25
    crawl_interval_minutes: int = 10
    crawl_time_budget_seconds: int = 60
    crawl_order: str = "newest_first"
    crawl_skip_threshold: int = 50
    crawl_archive_age_days: int = 30
    crawl_star_max_age_days: int = 7
    max_custom_labels: int = 15
    engagement_window_days: int = 90
    unsub_tier1_min: int = 3
    unsub_tier2_min: int = 5
    unsub_tier2_max_engagement: float = 0.1
    unsub_tier3_max_engagement: float = 0.3
    unsub_persistence_days: int = 7
    unsub_quarantine_days: int = 2
    dry_run_days: int = 7
    confidence_threshold: float = 0.7
    quarantine_hold_days: int = 3
    circuit_breaker_threshold: int = 20
    daily_destructive_cap: int = 25
    digest_time: str = "08:00"
    llm_provider: str = "none"
    llm_model: str = ""
    llm_endpoint: str = ""
    llm_api_key_env: str = ""
    llm_temperature: float = 0.1
    llm_max_tokens: int = 512
    llm_timeout: int = 30
    llm_fallback: str = "none"
    llm_send_body: bool = False
    llm_max_calls_per_run: int = 50
    memory_cap_mb: int = 256
    learner_auto_promote_confidence: float = 0.98
    learner_auto_promote_min_evidence: int = 10
    learner_staging_days: int = 7
    learner_revalidation_days: int = 30
    learner_deprecation_accuracy: float = 0.90
    learner_sender_min_threads: int = 5
    learner_domain_min_senders: int = 3
    learner_keyword_min_correlation: float = 0.90
