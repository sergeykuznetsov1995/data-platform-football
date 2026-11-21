"""Durable SQLite cursor for bounded achievement captures."""
from __future__ import annotations

from datetime import datetime, timezone
import sqlite3
from pathlib import Path
from typing import Any

from scrapers.transfermarkt.achievement_observations import (
    AchievementRecipient,
    AchievementUniverse,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS assessments (
    manifest_id TEXT PRIMARY KEY,
    universe_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    post_season_proof TEXT
);
CREATE TABLE IF NOT EXISTS recipients (
    manifest_id TEXT NOT NULL,
    recipient_type TEXT NOT NULL,
    recipient_id TEXT NOT NULL,
    slug TEXT,
    status TEXT NOT NULL,
    source_capture_id TEXT,
    receipt_id TEXT,
    error TEXT,
    updated_at TEXT,
    PRIMARY KEY (manifest_id, recipient_type, recipient_id)
);
"""


def _utc(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("achievement state timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat()


class AchievementState:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=30)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(SCHEMA)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def start_assessment(
        self,
        universe: AchievementUniverse,
        *,
        created_at: datetime,
        post_season_proof: str | None = None,
    ) -> bool:
        if not isinstance(universe, AchievementUniverse):
            raise ValueError("universe must be AchievementUniverse")
        if post_season_proof is not None and not post_season_proof.strip():
            raise ValueError("post-season proof must be nonempty")
        with self.connection:
            existing = self.connection.execute(
                "SELECT universe_version FROM assessments WHERE manifest_id=?",
                (universe.assessment_manifest_id,),
            ).fetchone()
            if existing:
                if existing[0] != universe.universe_version:
                    raise ValueError("assessment manifest has conflicting universe version")
                return False
            self.connection.execute(
                "INSERT INTO assessments VALUES (?, ?, ?, NULL, ?)",
                (universe.assessment_manifest_id, universe.universe_version, _utc(created_at), post_season_proof),
            )
            self.connection.executemany(
                "INSERT INTO recipients VALUES (?, ?, ?, ?, 'pending', NULL, NULL, NULL, NULL)",
                [
                    (universe.assessment_manifest_id, item.recipient_type, item.recipient_id, item.slug)
                    for item in universe.recipients
                ],
            )
        return True

    def due_recipients(self, manifest_id: str, *, limit: int) -> tuple[AchievementRecipient, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("recipient limit must be nonnegative")
        rows = self.connection.execute(
            "SELECT recipient_type, recipient_id, slug FROM recipients "
            "WHERE manifest_id=? AND status IN ('pending','error') "
            "ORDER BY recipient_type, recipient_id LIMIT ?",
            (manifest_id, limit),
        ).fetchall()
        return tuple(AchievementRecipient(*row) for row in rows)

    def mark_success(self, manifest_id: str, recipient: AchievementRecipient, extraction, receipt: dict[str, Any], at: datetime) -> None:
        if not receipt.get("verified") or not receipt.get("receipt_readback"):
            raise ValueError("recipient success requires verified physical and receipt readback")
        if receipt.get("source_capture_id") != extraction.raw_capture_id:
            raise ValueError("recipient receipt capture differs from extraction")
        status = extraction.rows[0]["observation_status"]
        with self.connection:
            self.connection.execute(
                "UPDATE recipients SET status=?, source_capture_id=?, receipt_id=?, error=NULL, updated_at=? "
                "WHERE manifest_id=? AND recipient_type=? AND recipient_id=?",
                (
                    status, extraction.raw_capture_id, str(receipt["receipt_id"]), _utc(at),
                    manifest_id, recipient.recipient_type, recipient.recipient_id,
                ),
            )
            self._complete_if_finished(manifest_id, at)

    def mark_error(self, manifest_id: str, recipient: AchievementRecipient, reason: str, at: datetime) -> None:
        safe_reason = getattr(reason, "reason", None) or str(reason or "unknown")[:512]
        with self.connection:
            self.connection.execute(
                "UPDATE recipients SET status='error', error=?, updated_at=? "
                "WHERE manifest_id=? AND recipient_type=? AND recipient_id=?",
                (safe_reason, _utc(at), manifest_id, recipient.recipient_type, recipient.recipient_id),
            )

    def _complete_if_finished(self, manifest_id: str, at: datetime) -> None:
        pending = self.connection.execute(
            "SELECT COUNT(*) FROM recipients WHERE manifest_id=? AND status='pending'",
            (manifest_id,),
        ).fetchone()[0]
        eligible = not pending
        for (kind, total, conclusive) in self.connection.execute(
            "SELECT recipient_type, COUNT(*), "
            "SUM(CASE WHEN status IN ('observed_present','observed_empty') THEN 1 ELSE 0 END) "
            "FROM recipients WHERE manifest_id=? GROUP BY recipient_type",
            (manifest_id,),
        ):
            eligible = eligible and total > 0 and conclusive / total >= 0.95
        if eligible:
            self.connection.execute(
                "UPDATE assessments SET completed_at=? WHERE manifest_id=? AND completed_at IS NULL",
                (_utc(at), manifest_id),
            )

    def active_assessment(self) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT manifest_id, universe_version, created_at, post_season_proof FROM assessments "
            "WHERE completed_at IS NULL ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return dict(zip(("manifest_id", "universe_version", "created_at", "post_season_proof"), row))

    def universe(self, manifest_id: str) -> AchievementUniverse:
        assessment = self.assessment(manifest_id)
        if assessment is None:
            raise ValueError("unknown assessment")
        rows = self.connection.execute(
            "SELECT recipient_type, recipient_id, slug FROM recipients WHERE manifest_id=?",
            (manifest_id,),
        ).fetchall()
        return AchievementUniverse(
            assessment["universe_version"],
            tuple(AchievementRecipient(*row) for row in rows),
        )

    def assessment(self, manifest_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT universe_version, created_at, completed_at, post_season_proof FROM assessments WHERE manifest_id=?",
            (manifest_id,),
        ).fetchone()
        if row is None:
            return None
        return dict(zip(("universe_version", "created_at", "completed_at", "post_season_proof"), row))

    def counts(self, manifest_id: str) -> dict[str, dict[str, int]]:
        rows = self.connection.execute(
            "SELECT recipient_type, status, COUNT(*) FROM recipients WHERE manifest_id=? "
            "GROUP BY recipient_type, status ORDER BY recipient_type, status",
            (manifest_id,),
        ).fetchall()
        result: dict[str, dict[str, int]] = {}
        for kind, status, count in rows:
            result.setdefault(kind, {"expected": 0})[status] = int(count)
            result[kind]["expected"] += int(count)
        return result

    def verified_recipients(self, manifest_id: str) -> set[tuple[str, str]]:
        rows = self.connection.execute(
            "SELECT recipient_type, recipient_id FROM recipients "
            "WHERE manifest_id=? AND status IN ('observed_present','observed_empty') "
            "AND source_capture_id IS NOT NULL AND receipt_id IS NOT NULL",
            (manifest_id,),
        ).fetchall()
        return {(str(kind), str(identifier)) for kind, identifier in rows}
