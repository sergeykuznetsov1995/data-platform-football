"""Season depth registry and edition publication lock (#1513)."""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import fcntl
import hashlib
import os
from pathlib import Path

from threading import RLock
from .journal import _execute, _literal

DEPTH_TABLE = "iceberg.ops.espn_depth_v1"
DEPTH_COLUMNS = (
    ("competition_slug", "varchar"), ("source_season_year", "integer"),
    ("plays_count", "integer"), ("deep_level", "varchar"), ("has_xg", "boolean"),
    ("sample_event_id", "bigint"), ("checked_at", "timestamp(6)"),
    ("manifest_json", "varchar"), ("error", "varchar"),
)


@dataclass(frozen=True)
class Depth:
    competition_slug: str
    source_season_year: int
    plays_count: int | None = None
    deep_level: str | None = None
    has_xg: bool | None = None
    sample_event_id: int | None = None
    checked_at: datetime | None = None
    manifest_json: str | None = None
    error: str | None = None


def ensure_depth_table(conn):
    _execute(conn, "CREATE SCHEMA IF NOT EXISTS iceberg.ops")
    columns = ", ".join(f"{name} {kind}" for name, kind in DEPTH_COLUMNS)
    _execute(conn, f"CREATE TABLE IF NOT EXISTS {DEPTH_TABLE} ({columns})")


def load_depth(trino, slug, year):
    names = ", ".join(name for name, _ in DEPTH_COLUMNS)
    rows = trino.execute_query(
        f"SELECT {names} FROM {DEPTH_TABLE} WHERE competition_slug = ? AND source_season_year = ?",
        (slug, year),
    )
    if len(rows) > 1:
        raise ValueError("duplicate depth registry key")
    return Depth(*rows[0]) if rows else None


def save_depth(conn, depth):
    names = [name for name, _ in DEPTH_COLUMNS]
    values = ", ".join(("true" if depth.has_xg else "false")
                       if kind == "boolean" and depth.has_xg is not None
                       else _literal(getattr(depth, name), kind)
                       for name, kind in DEPTH_COLUMNS)
    updates = ", ".join(f"{name} = s.{name}" for name in names[2:])
    _execute(conn, f"MERGE INTO {DEPTH_TABLE} t USING (VALUES ({values})) s ({', '.join(names)}) "
             "ON t.competition_slug = s.competition_slug AND t.source_season_year = s.source_season_year "
             f"WHEN MATCHED THEN UPDATE SET {updates} WHEN NOT MATCHED THEN INSERT "
             f"({', '.join(names)}) VALUES ({', '.join('s.' + name for name in names)})")


_offline_lock = RLock()


def client_state_dir(client):
    path = getattr(getattr(client, "gate", None), "state_path", None)
    return Path(path).parent if path is not None else None


@contextmanager
def edition_lock(slug, year, state_dir=None):
    """Use the caller's shared state volume; offline calls stay in-process."""
    if state_dir is None:
        with _offline_lock:
            yield
        return
    root = Path(state_dir) / "edition-locks"
    root.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(f"{slug}:{year}".encode()).hexdigest()
    descriptor = os.open(root / f"{key}.lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
