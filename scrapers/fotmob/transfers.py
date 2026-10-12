"""Durable daily budget and freshness evidence for the transfer-only lane.

SQLite is local scheduler state, never a replacement for Bronze. Completion is
recorded here only after its Bronze manifest and data have been flushed. Budget
is reserved before network work; an unfinalized/crashed reservation consumes
the remaining day, so retrying cannot silently reset the source allowance.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .planner import deterministic_plan_signature

TRANSFER_MODE = "transfers"
TRANSFER_PROFILE = "fotmob-transfers-v1"
TRANSFER_MAX_REQUESTS = 6000
TRANSFER_MAX_DIRECT_MIB = 512
TRANSFER_RPM = 60
TRANSFER_MAX_AGE = timedelta(hours=48)
TRANSFER_POLICY = {
    "window": "1year",
    "pagination": "unique_hits",
    "completion_scope": "included_ids",
    "completion_signature": "catalog_contract",
}


def transfer_state_path() -> str:
    return os.environ.get(
        "FOTMOB_TRANSFER_STATE_PATH", "/opt/airflow/logs/fotmob-transfers.sqlite3"
    )


def utc(value: datetime) -> datetime:
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )


def _iso(value: datetime) -> str:
    return utc(value).isoformat()


def _parse(value: str) -> datetime:
    return utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def transfer_journal_signature() -> str:
    return deterministic_plan_signature(
        {"transfers"}, {"profile": TRANSFER_PROFILE, "window": "1year"}
    )


def _empty() -> dict[str, Any]:
    return {
        "schema": TRANSFER_PROFILE,
        "catalog": None,
        "completions": {},
        "budgets": {},
    }


def _load(connection: sqlite3.Connection) -> dict[str, Any]:
    row = connection.execute("SELECT payload FROM transfer_state WHERE id=1").fetchone()
    state = json.loads(row[0]) if row else _empty()
    if state.get("schema") != TRANSFER_PROFILE:
        raise ValueError("unsupported FotMob transfer state schema")
    return state


def _status(state: dict[str, Any], now: datetime) -> dict[str, Any]:
    now = utc(now)
    day = now.date().isoformat()
    catalog = state.get("catalog") or {}
    ids = catalog.get("included_ids", [])
    completed = state["completions"]
    ages = []
    unknown = stale = fresh = 0
    completed_today = []
    timestamps = {}
    for competition_id in ids:
        stamp = completed.get(str(competition_id))
        if stamp is None:
            unknown += 1
            continue
        observed = _parse(stamp)
        age = (now - observed).total_seconds()
        if age < 0:
            unknown += 1
            continue
        timestamps[str(competition_id)] = stamp
        ages.append(age)
        if age > TRANSFER_MAX_AGE.total_seconds():
            stale += 1
        else:
            fresh += 1
        if observed.date().isoformat() == day:
            completed_today.append(competition_id)
    catalog_at = _parse(catalog["checked_at"]) if catalog else None
    catalog_fresh = bool(
        catalog_at
        and timedelta(0) <= now - catalog_at <= TRANSFER_MAX_AGE
        and catalog.get("complete")
        and ids
    )
    budget = state["budgets"].get(day, {})
    daily_budget = {
        "day": day,
        "max_requests": TRANSFER_MAX_REQUESTS,
        "max_direct_bytes": TRANSFER_MAX_DIRECT_MIB * 1024 * 1024,
        "max_proxy_bytes": 0,
        "requests": budget.get("requests", 0),
        "direct_bytes": budget.get("direct_bytes", 0),
        "proxy_bytes": budget.get("proxy_bytes", 0),
        "reserved": bool(budget.get("reservation")),
    }
    return {
        "day": day,
        "checked_at": _iso(now),
        "catalog_complete": bool(catalog.get("complete")),
        "catalog_checked_at": catalog.get("checked_at"),
        "included_ids": ids,
        "completion_timestamps": timestamps,
        "completed_transfer_competition_ids": sorted(completed_today),
        "daily_complete": bool(
            catalog_fresh
            and catalog_at.date().isoformat() == day
            and len(completed_today) == len(ids)
        ),
        "budget_exhausted": bool(
            daily_budget["requests"] > TRANSFER_MAX_REQUESTS - 4
            or daily_budget["direct_bytes"] >= TRANSFER_MAX_DIRECT_MIB * 1024 * 1024
            or daily_budget["proxy_bytes"]
            or daily_budget["reserved"]
        ),
        "daily_budget": daily_budget,
        "family_summary": {
            "included_count": len(ids),
            "fresh_count": fresh,
            "stale_count": stale,
            "unknown_count": unknown,
            "max_age_seconds": max(ages) if ages else None,
            "status": "green" if catalog_fresh and not stale and not unknown else "red",
            "checked_at": _iso(now),
        },
    }


def read_transfer_status(
    now: datetime, *, path: str | Path | None = None
) -> dict[str, Any]:
    """Read-only health check: a missing file is unknown and never created."""
    target = Path(path or transfer_state_path())
    if not target.exists():
        return _status(_empty(), now)
    with sqlite3.connect(
        target.resolve().as_uri() + "?mode=ro", uri=True
    ) as connection:
        return _status(_load(connection), now)


class TransferState:
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path or transfer_state_path())
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS transfer_state (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"
            )

    def _update(self, mutate):
        with sqlite3.connect(self.path, timeout=30) as connection:
            connection.execute("BEGIN IMMEDIATE")
            state = _load(connection)
            result = mutate(state)
            connection.execute(
                "INSERT OR REPLACE INTO transfer_state VALUES (1, ?)",
                (json.dumps(state, sort_keys=True),),
            )
            return result

    def reserve(self, now: datetime) -> dict[str, Any] | None:
        day = utc(now).date().isoformat()

        def mutate(state):
            budget = state["budgets"].setdefault(day, {})
            requests = TRANSFER_MAX_REQUESTS - budget.get("requests", 0)
            direct_bytes = TRANSFER_MAX_DIRECT_MIB * 1024 * 1024 - budget.get(
                "direct_bytes", 0
            )
            if (
                budget.get("reservation")
                or requests < 4
                or direct_bytes <= 0
                or budget.get("proxy_bytes")
            ):
                return None
            reservation = {
                "token": uuid.uuid4().hex,
                "day": day,
                "requests": requests,
                "direct_bytes": direct_bytes,
            }
            budget.update(
                {
                    "requests": TRANSFER_MAX_REQUESTS,
                    "direct_bytes": TRANSFER_MAX_DIRECT_MIB * 1024 * 1024,
                    "reservation": reservation,
                }
            )
            return reservation

        return self._update(mutate)

    def finalize(
        self,
        reservation: dict[str, Any],
        *,
        requests: int,
        direct_bytes: int,
        proxy_bytes: int = 0,
    ) -> None:
        """Refund only measured unused capacity; an overrun remains fail-closed."""

        def mutate(state):
            budget = state["budgets"][reservation["day"]]
            if budget.get("reservation") != reservation:
                raise ValueError("FotMob transfer reservation ownership differs")
            if min(requests, direct_bytes, proxy_bytes) < 0:
                raise ValueError("negative FotMob transfer consumption")
            if (
                requests > reservation["requests"]
                or direct_bytes > reservation["direct_bytes"]
                or proxy_bytes
            ):
                budget["requests"] += max(0, requests - reservation["requests"])
                budget["direct_bytes"] += max(
                    0, direct_bytes - reservation["direct_bytes"]
                )
                budget["proxy_bytes"] = proxy_bytes
                return
            budget["requests"] -= reservation["requests"] - requests
            budget["direct_bytes"] -= reservation["direct_bytes"] - direct_bytes
            budget.pop("reservation")

        self._update(mutate)

    def record_catalog(
        self, included_ids: list[int], now: datetime, *, complete: bool
    ) -> None:
        def mutate(state):
            state["catalog"] = {
                "included_ids": sorted(set(included_ids)),
                "checked_at": _iso(now),
                "complete": complete,
            }

        self._update(mutate)

    def record_completion(self, competition_id: int, now: datetime) -> None:
        def mutate(state):
            state["completions"][str(competition_id)] = _iso(now)

        self._update(mutate)
