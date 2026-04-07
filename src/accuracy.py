"""Accuracy measurement: confusion matrix, per-category/source accuracy,
confidence calibration, and daily trend persistence."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from src.models import DEFAULT_TENANT_ID
from src.ports.state_store import StateStorePort

logger = logging.getLogger(__name__)


class ConfusionAnalyzer:
    """Computes and persists accuracy metrics from classification feedback."""

    def __init__(self, state_store: StateStorePort, tenant_id: str = DEFAULT_TENANT_ID):
        self._store = state_store
        self._tenant = tenant_id

    async def analyze(self, since: str | None = None) -> dict[str, Any]:
        """Run a full accuracy analysis. Returns a summary dict."""
        confusion = await self._store.get_confusion_matrix(self._tenant, since)
        by_category = await self._store.get_accuracy_by_category(self._tenant, since)
        by_source = await self._store.get_accuracy_by_source(self._tenant, since)
        calibration = await self._store.get_confidence_buckets(self._tenant)

        total_classifications = sum(v["total"] for v in by_category.values())
        total_feedback = sum(v["feedback_count"] for v in by_category.values())
        overall_accuracy = (
            ((total_classifications - total_feedback) / total_classifications * 100)
            if total_classifications > 0 else 100.0
        )

        top_confusion = confusion[:5] if confusion else []

        return {
            "overall_accuracy": overall_accuracy,
            "total_classifications": total_classifications,
            "total_feedback": total_feedback,
            "by_category": by_category,
            "by_source": by_source,
            "top_confusion_pairs": top_confusion,
            "confidence_calibration": calibration,
        }

    async def persist_daily(self) -> None:
        """Compute and save today's accuracy snapshot."""
        analysis = await self.analyze()
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        await self._store.save_accuracy_daily(
            self._tenant,
            date_str,
            analysis["overall_accuracy"],
            json.dumps(analysis["by_category"]),
            json.dumps(analysis["by_source"]),
            json.dumps(analysis["top_confusion_pairs"]),
            analysis["total_feedback"],
        )
        logger.info(
            "Accuracy snapshot saved: %.1f%% overall (%d feedback from %d classifications)",
            analysis["overall_accuracy"],
            analysis["total_feedback"],
            analysis["total_classifications"],
        )

    async def get_calibration_report(self) -> list[dict[str, Any]]:
        """Return confidence calibration data for the digest."""
        return await self._store.get_confidence_buckets(self._tenant)

    async def get_trend(self, days: int = 7) -> list[dict[str, Any]]:
        """Return accuracy trend for the digest."""
        return await self._store.get_accuracy_trend(self._tenant, days)
