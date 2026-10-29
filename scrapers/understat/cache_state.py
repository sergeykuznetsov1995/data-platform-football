"""Validated cache envelopes and shared daily HTTP accounting for Understat."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta
import fcntl
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Iterator
import uuid


logger = logging.getLogger(__name__)
CACHE_VERSION = 2
DAILY_LIMIT = 500
ROTATION_LIMIT = 72
HTML_RESERVE = 60
# Leave room for later leagues, weekly probes and retries. Mandatory work is
# never dropped; the ledger warns if it alone exhausts the daily allowance.
ROTATION_RESERVE = 178


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode()).hexdigest()


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return None


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


class CacheState:
    def __init__(self, cache_dir: Path | str | None):
        self.root = Path(cache_dir) if cache_dir is not None else None
        self.pending: dict[str, dict[str, Any]] = {}

    def invalidate(self, name: str) -> None:
        # Do this before HTTP, including refreshes returning identical payloads.
        if self.root is not None:
            (self.root / "validated" / name).unlink(missing_ok=True)

    def confirm(self) -> None:
        """Persist staged envelopes only after the scope has been published."""
        if self.root is not None:
            for name, record in self.pending.items():
                atomic_json(self.root / "validated" / name, record)
        self.pending.clear()

    def load(self, name: str, identity: list[Any]) -> dict[str, Any] | None:
        if self.root is None:
            return None
        record = read_json(self.root / "validated" / name)
        payload = read_json(self.root / name)
        if (not isinstance(record, dict) or record.get("version") != CACHE_VERSION
                or record.get("identity") != identity or not isinstance(payload, dict)
                or record.get("payload_hash") != digest(payload)
                or not isinstance(record.get("checked_on"), str)):
            return None
        return {**record, "payload": payload}

    def save(self, name: str, identity: list[Any], fingerprint: str,
             payload: dict[str, Any], today: date, *, match_date: str | None = None,
             staged: bool = False) -> None:
        if self.root is None:
            return
        # The response hash binds this state to the exact validated payload.
        # A crash or concurrent replacement can only cause a cache miss.
        record = {
            "version": CACHE_VERSION, "identity": identity,
            "fingerprint": fingerprint, "payload_hash": digest(payload),
            "checked_on": today.isoformat(), "match_date": match_date,
        }
        if staged:
            self.pending[name] = record
        else:
            atomic_json(self.root / "validated" / name, record)

    @contextmanager
    def ledger(self, today: date) -> Iterator[dict[str, Any]]:
        if self.root is None:
            yield {"requests": 0, "rotated": []}
            return
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / "daily-http.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            target = self.root / f"daily-http-{today.isoformat()}.json"
            state = read_json(target)
            if not isinstance(state, dict) or not state:
                state = {"requests": 0, "rotated": []}
                if target.exists():
                    state["accounting_unreliable"] = True
            yield state
            atomic_json(target, state)

    def count_request(self, today: date) -> None:
        with self.ledger(today) as state:
            state["requests"] += 1
            if state["requests"] > DAILY_LIMIT:
                logger.warning("Understat daily HTTP budget exceeded: %s > %s; "
                               "mandatory data retained", state["requests"], DAILY_LIMIT)

    def claim_html(self, today: date, match_id: int | str | None = None) -> bool:
        """Reserve one optional GET atomically; untrustworthy accounting waits."""
        if self.root is None:
            return False
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / "daily-http.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            target = self.root / f"daily-http-{today.isoformat()}.json"
            state = read_json(target)
            if not isinstance(state, dict) or state.get("accounting_unreliable"):
                return False
            requests = state.get("requests")
            html = state.get("html_requests", 0)
            matches = state.get("html_matches", [])
            if (not isinstance(matches, list) or (match_id is not None and str(match_id) in matches)):
                return False
            if (type(requests) is not int or type(html) is not int
                    or requests < 0 or html < 0
                    or requests >= DAILY_LIMIT or html >= HTML_RESERVE):
                return False
            state["requests"] += 1
            state["html_requests"] = html + 1
            if match_id is not None:
                state["html_matches"] = [*matches, str(match_id)]
            atomic_json(target, state)
            return True

    def rotation(self, today: date, mandatory: int) -> set[str]:
        if self.root is None:
            return set()
        with self.ledger(today) as state:
            if "rotation_candidates" not in state:
                candidates = []
                from .catalog import LEAGUE_BY_CANONICAL
                for path in (self.root / "validated").glob("match_*.json"):
                    record = read_json(path)
                    if not isinstance(record, dict):
                        continue
                    identity = record.get("identity", [])
                    if len(identity) != 3 or record.get("checked_on", "") >= today.isoformat():
                        continue
                    if record.get("match_date") and record["match_date"] >= (today - timedelta(days=2)).isoformat():
                        continue
                    league, year, _game = identity
                    definition = LEAGUE_BY_CANONICAL.get(league)
                    if definition is None:
                        continue
                    confirmed = read_json(self.root / "confirmed" / f"{definition.source_league}_{year}.json") or {}
                    if confirmed.get("is_closed", year < today.year - 1):
                        continue
                    candidates.append((record["checked_on"], path.name))
                state["rotation_candidates"] = [name for _, name in sorted(candidates)[:ROTATION_LIMIT]]
            available = max(0, DAILY_LIMIT - state["requests"] - mandatory - ROTATION_RESERVE - HTML_RESERVE)
            names = [name for name in state["rotation_candidates"] if name not in state["rotated"]]
            return set(names[:available])

    def claim_rotation(self, name: str, today: date) -> bool:
        with self.ledger(today) as state:
            if (name in state["rotated"] or len(state["rotated"]) >= ROTATION_LIMIT
                    or state["requests"] >= DAILY_LIMIT - ROTATION_RESERVE - HTML_RESERVE):
                return False
            state["rotated"].append(name)
            return True
