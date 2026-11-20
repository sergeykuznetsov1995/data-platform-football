"""Achievements coverage and physical reconciliation DQ."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from scrapers.transfermarkt.achievement_observations import AchievementUniverse
from scrapers.transfermarkt.achievement_state import AchievementState

CONCLUSIVE = {"observed_present", "observed_empty"}


def build_achievement_dq(
    universe: AchievementUniverse,
    state: AchievementState,
    *,
    reconciliation_violations: int,
    evaluated_at: datetime,
) -> dict[str, Any]:
    if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
        raise ValueError("DQ evaluation time must be timezone-aware")
    manifest = universe.assessment_manifest_id
    assessment = state.assessment(manifest)
    if assessment is None:
        raise ValueError("assessment is not registered")
    counts = state.counts(manifest)
    verified = state.verified_recipients(manifest)
    per_type = {}
    for kind in ("player", "club", "coach"):
        expected = sum(item.recipient_type == kind for item in universe.recipients)
        conclusive = sum(
            item.recipient_type == kind
            and (kind, item.recipient_id) in verified
            for item in universe.recipients
        )
        unresolved = expected - conclusive
        per_type[kind] = {
            "expected": expected,
            "conclusive": conclusive,
            "unresolved": unresolved,
            "conclusive_rate": conclusive / expected if expected else None,
            "unresolved_rate": unresolved / expected if expected else None,
            "status_counts": counts.get(kind, {}),
        }
    coverage_passed = all(
        item["expected"] > 0
        and item["conclusive_rate"] >= 0.95
        and item["unresolved_rate"] <= 0.05
        for item in per_type.values()
    )
    completed = assessment["completed_at"] is not None
    return {
        "version": "tm-achievement-dq-v1",
        "assessment_manifest_id": manifest,
        "universe_version": universe.universe_version,
        "evaluated_at": evaluated_at.astimezone(timezone.utc).isoformat(),
        "completed": completed,
        "per_type": per_type,
        "verified_recipients": len(verified),
        "reconciliation_violations": reconciliation_violations,
        "status": "GREEN" if completed and coverage_passed and reconciliation_violations == 0 else "RED",
    }


def achievement_reconciliation_sql() -> str:
    return """
WITH physical AS (
    SELECT recipient_type, recipient_id, source_capture_id,
           COUNT(*) AS row_count,
           COUNT(DISTINCT source_row_id) AS distinct_rows,
           SUM(CASE WHEN source_url IS NULL OR source_body_hash IS NULL
                    OR raw_capture_id IS NULL OR captured_at IS NULL
                    OR observed_at IS NULL OR extracted_at IS NULL
                    OR parser_revision IS NULL OR schema_revision IS NULL
                    OR run_id IS NULL OR universe_version IS NULL
                    OR assessment_manifest_id IS NULL OR _batch_id IS NULL
                    OR _ingested_at IS NULL THEN 1 ELSE 0 END) AS null_lineage
    FROM iceberg.bronze.transfermarkt_achievement_observations
    GROUP BY recipient_type, recipient_id, source_capture_id
), duplicate_keys AS (
    SELECT recipient_type, recipient_id, source_capture_id, source_row_id
    FROM iceberg.bronze.transfermarkt_achievement_observations
    GROUP BY recipient_type, recipient_id, source_capture_id, source_row_id
    HAVING COUNT(*) <> 1
)
SELECT COUNT(*) FROM (
    SELECT r.receipt_id
    FROM iceberg.ops.transfermarkt_achievement_receipts_v1 r
    LEFT JOIN physical p
      ON p.recipient_type = r.recipient_type
     AND p.recipient_id = r.recipient_id
     AND p.source_capture_id = r.source_capture_id
    WHERE p.row_count IS NULL OR p.row_count <> r.row_count
       OR p.distinct_rows <> r.row_count OR p.null_lineage <> 0
    UNION ALL
    SELECT NULL FROM duplicate_keys
)
"""


def run_achievement_dq(
    connection,
    universe: AchievementUniverse,
    state: AchievementState,
    *,
    evaluated_at: datetime | None = None,
) -> dict[str, Any]:
    cursor = connection.cursor()
    try:
        cursor.execute(achievement_reconciliation_sql())
        rows = list(cursor.fetchall())
    finally:
        cursor.close()
    if len(rows) != 1 or len(rows[0]) != 1:
        raise ValueError("achievement reconciliation DQ must return one scalar")
    return build_achievement_dq(
        universe, state, reconciliation_violations=int(rows[0][0]),
        evaluated_at=evaluated_at or datetime.now(timezone.utc),
    )
