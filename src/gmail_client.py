from __future__ import annotations

import asyncio
import base64
import logging
import re
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Any

from src.models import (
    ALL_MANAGED_LABELS,
    LABEL_COLORS,
    LABEL_VISIBILITY,
    MAX_CUSTOM_LABELS,
    ThreadMetadata,
)

logger = logging.getLogger(__name__)


class GmailClient:
    """Wrapper around the Gmail API for thread-level operations."""

    def __init__(self, credentials: Any):
        from googleapiclient.discovery import build

        self._service = build("gmail", "v1", credentials=credentials)
        self._label_cache: dict[str, str] = {}  # name -> id
        self._user_email: str = ""

    # -- Initialization --

    def get_user_email(self) -> str:
        if not self._user_email:
            profile = self._service.users().getProfile(userId="me").execute()
            self._user_email = profile["emailAddress"]
        return self._user_email

    def provision_labels(self) -> dict[str, str]:
        """Create managed labels if missing. Returns name->id mapping."""
        existing = self._service.users().labels().list(userId="me").execute()
        existing_labels = {l["name"]: l["id"] for l in existing.get("labels", [])}

        custom_count = sum(
            1 for l in existing.get("labels", []) if l["type"] == "user"
        )

        self._label_cache = {}
        for label_name in ALL_MANAGED_LABELS:
            if label_name in existing_labels:
                self._label_cache[label_name] = existing_labels[label_name]
                continue

            if custom_count >= MAX_CUSTOM_LABELS:
                logger.warning(
                    "Cannot create label %s: would exceed cap of %d",
                    label_name, MAX_CUSTOM_LABELS,
                )
                continue

            body: dict[str, Any] = {
                "name": label_name,
                "labelListVisibility": LABEL_VISIBILITY.get(label_name, "labelShow"),
                "messageListVisibility": "show",
            }
            if label_name in LABEL_COLORS:
                body["color"] = LABEL_COLORS[label_name]

            try:
                result = self._service.users().labels().create(
                    userId="me", body=body
                ).execute()
                self._label_cache[label_name] = result["id"]
                custom_count += 1
                logger.info("Created label: %s (id=%s)", label_name, result["id"])
            except Exception as exc:
                logger.error("Failed to create label %s: %s", label_name, exc)

        return self._label_cache

    def get_label_id(self, name: str) -> str | None:
        return self._label_cache.get(name)

    # -- Thread fetching --

    def get_history(self, start_history_id: str) -> tuple[list[dict], str]:
        """Fetch changes since a history ID. Returns (history_records, new_history_id)."""
        results: list[dict] = []
        page_token = None
        latest_id = start_history_id

        while True:
            resp = (
                self._service.users()
                .history()
                .list(
                    userId="me",
                    startHistoryId=start_history_id,
                    historyTypes=["messageAdded", "labelAdded", "labelRemoved"],
                    pageToken=page_token,
                )
                .execute()
            )
            results.extend(resp.get("history", []))
            latest_id = resp.get("historyId", latest_id)
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        return results, latest_id

    def list_threads(
        self,
        query: str = "",
        max_results: int = 25,
        page_token: str | None = None,
    ) -> tuple[list[dict], str | None, int]:
        """List threads. Returns (threads, next_page_token, result_size_estimate)."""
        resp = (
            self._service.users()
            .threads()
            .list(
                userId="me",
                q=query,
                maxResults=max_results,
                pageToken=page_token,
            )
            .execute()
        )
        threads = resp.get("threads", [])
        return threads, resp.get("nextPageToken"), resp.get("resultSizeEstimate", 0)

    def get_thread(self, thread_id: str, fmt: str = "metadata") -> dict:
        return (
            self._service.users()
            .threads()
            .get(userId="me", id=thread_id, format=fmt)
            .execute()
        )

    def get_thread_metadata(self, thread_id: str) -> ThreadMetadata:
        """Fetch a thread and extract structured metadata."""
        thread = self.get_thread(thread_id, fmt="metadata")
        messages = thread.get("messages", [])
        if not messages:
            return ThreadMetadata(thread_id=thread_id, subject="", sender="", sender_domain="", snippet="")

        first_msg = messages[0]
        headers = {h["name"].lower(): h["value"] for h in first_msg.get("payload", {}).get("headers", [])}

        sender = headers.get("from", "")
        domain_match = re.search(r"@([\w.-]+)", sender)
        domain = domain_match.group(1).lower() if domain_match else ""

        label_ids = first_msg.get("labelIds", [])
        gmail_cats = [lid for lid in label_ids if lid.startswith("CATEGORY_")]

        unsub_header = headers.get("list-unsubscribe", "")
        unsub_post = headers.get("list-unsubscribe-post", "")

        user_email = self.get_user_email()
        has_reply = any(
            any(
                h["value"].lower().find(user_email.lower()) >= 0
                for h in m.get("payload", {}).get("headers", [])
                if h["name"].lower() == "from"
            )
            for m in messages
        )

        date_str = headers.get("date", "")
        msg_date = None
        try:
            internal_ts = int(first_msg.get("internalDate", "0")) / 1000
            msg_date = datetime.fromtimestamp(internal_ts, tz=timezone.utc)
        except (ValueError, TypeError):
            pass

        return ThreadMetadata(
            thread_id=thread_id,
            subject=headers.get("subject", ""),
            sender=sender,
            sender_domain=domain,
            snippet=thread.get("snippet", ""),
            gmail_categories=gmail_cats,
            label_ids=label_ids,
            has_unsubscribe=bool(unsub_header),
            unsubscribe_header=unsub_header or None,
            unsubscribe_post=unsub_post or None,
            is_unread="UNREAD" in label_ids,
            has_user_reply=has_reply,
            message_count=len(messages),
            date=msg_date,
            history_id=thread.get("historyId"),
            raw_headers=headers,
        )

    # -- Thread modification --

    def modify_thread(
        self,
        thread_id: str,
        add_labels: list[str] | None = None,
        remove_labels: list[str] | None = None,
    ) -> None:
        body: dict[str, list[str]] = {}
        if add_labels:
            body["addLabelIds"] = [self._resolve_label(l) for l in add_labels if self._resolve_label(l)]
        if remove_labels:
            body["removeLabelIds"] = [self._resolve_label(l) for l in remove_labels if self._resolve_label(l)]
        if body:
            self._service.users().threads().modify(
                userId="me", id=thread_id, body=body
            ).execute()

    def batch_modify_threads(
        self,
        modifications: list[dict[str, Any]],
    ) -> None:
        """Batch modify threads. Each item: {thread_id, add_labels?, remove_labels?}."""
        from googleapiclient.http import BatchHttpRequest

        def _callback(request_id: str, response: Any, exception: Any) -> None:
            if exception:
                logger.error("Batch modify error for %s: %s", request_id, exception)

        for i in range(0, len(modifications), 100):
            batch = self._service.new_batch_http_request(callback=_callback)
            for mod in modifications[i : i + 100]:
                body: dict[str, list[str]] = {}
                if mod.get("add_labels"):
                    body["addLabelIds"] = [
                        self._resolve_label(l) for l in mod["add_labels"]
                        if self._resolve_label(l)
                    ]
                if mod.get("remove_labels"):
                    body["removeLabelIds"] = [
                        self._resolve_label(l) for l in mod["remove_labels"]
                        if self._resolve_label(l)
                    ]
                batch.add(
                    self._service.users().threads().modify(
                        userId="me", id=mod["thread_id"], body=body
                    ),
                    request_id=mod["thread_id"],
                )
            batch.execute()

    def trash_thread(self, thread_id: str) -> None:
        self._service.users().threads().trash(userId="me", id=thread_id).execute()

    def untrash_thread(self, thread_id: str) -> None:
        self._service.users().threads().untrash(userId="me", id=thread_id).execute()

    # -- Sending --

    def send_self_email(self, subject: str, body_html: str) -> None:
        user_email = self.get_user_email()
        msg = MIMEText(body_html, "html")
        msg["to"] = user_email
        msg["from"] = user_email
        msg["subject"] = subject
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        self._service.users().messages().send(
            userId="me", body={"raw": raw}
        ).execute()

    def send_email(self, to: str, subject: str, body: str) -> None:
        user_email = self.get_user_email()
        msg = MIMEText(body)
        msg["to"] = to
        msg["from"] = user_email
        msg["subject"] = subject
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        self._service.users().messages().send(
            userId="me", body={"raw": raw}
        ).execute()

    # -- Profile --

    def get_current_history_id(self) -> str:
        profile = self._service.users().getProfile(userId="me").execute()
        return str(profile["historyId"])

    # -- Gmail filters --

    def list_filters(self) -> list[dict]:
        resp = self._service.users().settings().filters().list(userId="me").execute()
        return resp.get("filter", [])

    def create_filter(
        self,
        from_address: str,
        add_labels: list[str] | None = None,
        remove_labels: list[str] | None = None,
        should_mark_read: bool = False,
    ) -> dict | None:
        """Create a Gmail server-side filter for a sender pattern."""
        criteria = {"from": from_address}
        action: dict = {}

        label_ids_add = []
        label_ids_remove = []

        if add_labels:
            for l in add_labels:
                lid = self._resolve_label(l)
                if lid:
                    label_ids_add.append(lid)
        if remove_labels:
            for l in remove_labels:
                lid = self._resolve_label(l)
                if lid:
                    label_ids_remove.append(lid)

        # Skip inbox = remove INBOX label
        if "INBOX" not in (label_ids_remove or []):
            label_ids_remove.append("INBOX")

        if label_ids_add:
            action["addLabelIds"] = label_ids_add
        if label_ids_remove:
            action["removeLabelIds"] = label_ids_remove

        body = {"criteria": criteria, "action": action}
        try:
            result = self._service.users().settings().filters().create(
                userId="me", body=body
            ).execute()
            logger.info("Created Gmail filter for '%s' (id=%s)", from_address, result.get("id"))
            return result
        except Exception as exc:
            logger.error("Failed to create filter for '%s': %s", from_address, exc)
            return None

    def delete_filter(self, filter_id: str) -> None:
        try:
            self._service.users().settings().filters().delete(
                userId="me", id=filter_id
            ).execute()
        except Exception as exc:
            logger.warning("Failed to delete filter %s: %s", filter_id, exc)

    def sync_filters_from_rules(
        self,
        auto_rules: list,
        min_confidence: float = 0.98,
        min_evidence: int = 10,
        existing_filter_senders: set[str] | None = None,
    ) -> int:
        """Create Gmail filters from high-confidence auto-rules.

        Returns the number of new filters created.
        """
        if existing_filter_senders is None:
            existing_filters = self.list_filters()
            existing_filter_senders = set()
            for f in existing_filters:
                criteria = f.get("criteria", {})
                if criteria.get("from"):
                    existing_filter_senders.add(criteria["from"].lower())

        created = 0
        for rule in auto_rules:
            if rule.status != "active":
                continue
            if rule.confidence < min_confidence:
                continue
            if rule.evidence_count < min_evidence:
                continue
            if rule.type != "sender":
                continue

            sender = rule.pattern.replace("\\", "")
            if sender.lower() in existing_filter_senders:
                continue

            add_labels = []
            if rule.classification:
                add_labels.append(rule.classification)

            should_read = rule.classification in ("Shopping", "Accounts", "Newsletters")

            result = self.create_filter(
                from_address=sender,
                add_labels=add_labels,
                remove_labels=["INBOX"],
                should_mark_read=should_read,
            )
            if result:
                existing_filter_senders.add(sender.lower())
                created += 1

        if created:
            logger.info("Synced %d Gmail filters from auto-rules", created)
        return created

    # -- Helpers --

    def _resolve_label(self, name_or_id: str) -> str | None:
        if name_or_id in self._label_cache:
            return self._label_cache[name_or_id]
        if name_or_id.startswith("CATEGORY_") or name_or_id in (
            "INBOX", "UNREAD", "STARRED", "SPAM", "TRASH", "SENT", "DRAFT",
            "IMPORTANT",
        ):
            return name_or_id
        return None
