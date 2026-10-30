"""Computed history queue and local atomic scope leases; never stores completion.

Coverage and candidate availability always come from the Bronze journal. The
SQLite file belongs in the existing persistent Airflow logs volume.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import sqlite3
import uuid
from datetime import date
from pathlib import Path

from .catalog import CLASS_A_TOURNAMENT_IDS, PROBE_SCOPE_SPECS
from .domain import SeasonFormat

CHUNK_SIZE = 25
COOLDOWN_SECONDS = 3600
# Schedule sync covers the entire season; historical lineups rarely change.
DEFERRED_REFRESH_SECONDS = 24 * 3600
# Longer than the task's eight-hour execution timeout, including kill grace.
LEASE_SECONDS = 9 * 3600
# After browser-level Cloudflare evidence only worker 0 keeps working (#1481).
DEGRADED_SECONDS = 3600
TOP_FIVE = {2, 3, 4, 5, 22}

def current_running() -> bool:
    from airflow.models.dagrun import DagRun
    from airflow.utils.session import create_session

    with create_session() as session:
        return session.query(DagRun).filter(
            DagRun.dag_id == "dag_ingest_whoscored", DagRun.state == "running",
        ).first() is not None


def share(key: str, workers: int) -> int:
    """Stable worker index of a scope; Python's hash() differs per process."""
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:16], 16) % workers


def queue(catalog, *, today: date) -> list[dict]:
    """N/N-1 + explicit gap/probes, then ten years top-five first, then deeper."""
    grouped = {}
    for season in catalog.enabled_scopes():
        if season.start is not None and season.start > today:
            continue
        grouped.setdefault(season.scope.competition_id, []).append(season)
    result = []
    for league, seasons in grouped.items():
        tid = catalog.competition(league).tournament_id
        if tid not in CLASS_A_TOURNAMENT_IDS:
            seasons = [s for s in seasons if s.scope.spec in PROBE_SCOPE_SPECS]
        seasons.sort(key=lambda s: s.source_season_id or 0, reverse=True)
        for depth, season in enumerate(seasons):
            if season.source_season_id is None:
                raise ValueError(f"history requires source_season_id: {season.scope.spec}")
            label = str(season.scope.season_id).split("-")[0]
            start_year = (season.start.year if season.start else
                          int(label) if season.scope.season_format is SeasonFormat.SINGLE_YEAR
                          else 2000 + int(label[:2]) if len(label) == 4 and label.isdigit()
                          else None)
            wave = (1 if depth < 2 or label in {"2025", "2026", "2526"}
                    or season.scope.spec in PROBE_SCOPE_SPECS else
                    2 if start_year is not None and 2015 <= start_year <= 2024 else 3)
            result.append({"season": season, "wave": wave, "top": tid in TOP_FIVE,
                           "key": season.scope.spec})
    return sorted(result, key=lambda s: (
        s["wave"], 0 if s["top"] else 1,
        -(s["season"].source_season_id or 0), s["key"],
    ))


def first_wave_ready(scopes: list[dict]) -> bool:
    first = [s["coverage"] for s in scopes if s["wave"] == 1]
    return bool(first) and all(c["schedule_ready"] and not c["unknown_stages"] for c in first) and (
        sum(c["closed"] for c in first) * 100 >=
        sum(c["closeable"] for c in first) * 99
    )


def pace(scopes: list[dict], *, parked: int) -> dict:
    closed = sum(s["coverage"]["closed_day"] for s in scopes)
    remaining = sum(s["coverage"]["closeable"] - s["coverage"]["closed"] for s in scopes)
    unresolved = sum(not s["coverage"]["schedule_ready"] or bool(s["coverage"]["unknown_stages"])
                     for s in scopes)
    return {"closed_day": closed, "remaining": remaining, "parked": parked,
            "unresolved_scopes": unresolved,
            "alert": closed == 0 and (remaining > 0 or unresolved > 0),
            "text": f"темп истории: {closed} матчей/сутки; остаток {remaining}; парковки {parked}"}


class HistoryState:
    def __init__(self, path: str | Path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            con.execute("CREATE TABLE IF NOT EXISTS scopes (scope TEXT PRIMARY KEY, "
                        "token TEXT, lease_until REAL NOT NULL, next_at REAL NOT NULL, "
                        "duration REAL NOT NULL DEFAULT 0, failed INTEGER NOT NULL DEFAULT 0)")
            columns = {row[1] for row in con.execute("PRAGMA table_info(scopes)")}
            if "schedule_next_at" not in columns:
                con.execute("ALTER TABLE scopes ADD COLUMN schedule_next_at REAL NOT NULL DEFAULT 0")
            con.execute("CREATE TABLE IF NOT EXISTS circuit (name TEXT PRIMARY KEY, "
                        "until REAL NOT NULL)")

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def reserve(self, scopes: list[dict], *, now: float):
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            # A crashed process is parked once its lease expires, rather than
            # immediately monopolising the next run again.
            con.execute("UPDATE scopes SET token=NULL, next_at=?, failed=1 "
                        "WHERE token IS NOT NULL AND lease_until<=?",
                        (now + COOLDOWN_SECONDS, now))
            for scope in scopes:
                row = con.execute("SELECT token, next_at FROM scopes WHERE scope=?",
                                  (scope["key"],)).fetchone()
                if row and (row[0] is not None or row[1] > now):
                    continue
                token = uuid.uuid4().hex
                con.execute("INSERT INTO scopes(scope,token,lease_until,next_at) VALUES(?,?,?,0) "
                            "ON CONFLICT(scope) DO UPDATE SET token=excluded.token, "
                            "lease_until=excluded.lease_until",
                            (scope["key"], token, now + LEASE_SECONDS))
                return scope, token
        return None

    def deferred_refresh_due(self, key: str, token: str, *, now: float) -> bool:
        """Check the cooldown without spending an attempt before the egress probe."""
        with self._connect() as con:
            row = con.execute("SELECT schedule_next_at FROM scopes "
                              "WHERE scope=? AND token=? AND lease_until>?",
                              (key, token, now)).fetchone()
            if row is None:
                raise RuntimeError("history scope lease was lost")
            return row[0] <= now

    def reserve_deferred_refresh(self, key: str, token: str, *, now: float) -> bool:
        """Persist the attempt before schedule sync, even if the worker crashes."""
        with self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT schedule_next_at FROM scopes "
                              "WHERE scope=? AND token=? AND lease_until>?",
                              (key, token, now)).fetchone()
            if row is None:
                raise RuntimeError("history scope lease was lost")
            if row[0] > now:
                return False
            con.execute("UPDATE scopes SET schedule_next_at=? WHERE scope=? AND token=?",
                        (now + DEFERRED_REFRESH_SECONDS, key, token))
            return True

    def release(self, key: str, token: str, *, now: float, failed: bool, duration: float, idle=False):
        with self._connect() as con:
            changed = con.execute("UPDATE scopes SET token=NULL, next_at=?, duration=?, failed=? "
                                  "WHERE scope=? AND token=?",
                                  (now + (COOLDOWN_SECONDS if failed or idle else 30), duration, int(failed), key, token))
            if changed.rowcount != 1:
                raise RuntimeError("history scope lease was lost")

    def parked(self, *, now: float) -> int:
        with self._connect() as con:
            return con.execute("SELECT COUNT(*) FROM scopes WHERE failed=1 AND next_at>?", (now,)).fetchone()[0]

    def trip_degraded(self, *, now: float) -> None:
        """Shared flag: every worker but 0 stops at its next scope boundary."""
        with self._connect() as con:
            con.execute("INSERT INTO circuit(name, until) VALUES('degraded', ?) "
                        "ON CONFLICT(name) DO UPDATE SET until=MAX(until, excluded.until)",
                        (now + DEGRADED_SECONDS,))

    def degraded(self, *, now: float) -> bool:
        with self._connect() as con:
            row = con.execute("SELECT until FROM circuit WHERE name='degraded'").fetchone()
            return row is not None and row[0] > now
