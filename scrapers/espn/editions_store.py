"""Open editions of the live tournaments between waves (#1504).

A small file cache next to the gate state (``$AIRFLOW_HOME/state/espn/
editions.json``, env ``ESPN_EDITIONS_STATE_PATH``).  It is derived entirely
from ESPN core: when it is missing, unreadable or older than 24 hours the wave
planner reads ``leagues/{slug}`` of every live target and moves the editions
through ``plan_editions`` (season transition, #1501).  Losing the file is
harmless — the next wave recomputes it (~161 requests a day).
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, Iterator, Mapping, Sequence

from . import urls
from .core_lists import parse_league_current_season
from .denominator import DenominatorRow
from .editions import EditionState, plan_editions
from .parser_common import EspnParseError
from .transport_contracts import (
    DirectTransportError,
    HttpStatusError,
    InvalidJsonError,
    ResponseTooLarge,
    RetryExhausted,
)

logger = logging.getLogger(__name__)

EDITIONS_STATE_ENV = "ESPN_EDITIONS_STATE_PATH"
MAX_AGE = timedelta(hours=24)
STATE_VERSION = 1
# One league failing is not a wave failure: its known editions stay.  Gate
# closures (AllOriginsBlocked, LaneClosed, DailyCapExceeded) propagate.
_LEAGUE_ERRORS = (
    DirectTransportError,
    EspnParseError,
    HttpStatusError,
    InvalidJsonError,
    ResponseTooLarge,
    RetryExhausted,
)


def default_state_path() -> Path:
    configured = os.environ.get(EDITIONS_STATE_ENV, "").strip()
    if configured:
        return Path(configured)
    home = os.environ.get("AIRFLOW_HOME", "").strip() or "/opt/airflow"
    return Path(home) / "state" / "espn" / "editions.json"


@dataclass(frozen=True, slots=True)
class EditionsSnapshot:
    refreshed_at: datetime
    editions: tuple[EditionState, ...]

    def of(self, slug: str) -> tuple[EditionState, ...]:
        return tuple(state for state in self.editions if state.competition_slug == slug)

    def open_of(self, slug: str) -> tuple[EditionState, ...]:
        return tuple(state for state in self.of(slug) if state.open)

    def edition(self, slug: str, year: int) -> EditionState | None:
        return next((state for state in self.of(slug) if state.year == year), None)


def _encode(snapshot: EditionsSnapshot) -> str:
    return json.dumps(
        {
            "version": STATE_VERSION,
            "refreshed_at": snapshot.refreshed_at.isoformat(),
            "editions": [
                {
                    "slug": state.competition_slug,
                    "year": state.year,
                    "display_name": state.display_name,
                    "start": state.start.isoformat(),
                    "end": state.end.isoformat(),
                    "open": state.open,
                }
                for state in snapshot.editions
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _decode(text: str) -> EditionsSnapshot:
    document = json.loads(text)
    if not isinstance(document, dict) or document.get("version") != STATE_VERSION:
        raise ValueError("unknown editions state format")
    refreshed_at = datetime.fromisoformat(document["refreshed_at"])
    if refreshed_at.tzinfo is None:
        raise ValueError("editions state refreshed_at must be timezone-aware")
    return EditionsSnapshot(
        refreshed_at,
        tuple(
            EditionState(
                item["slug"],
                int(item["year"]),
                item["display_name"],
                date.fromisoformat(item["start"]),
                date.fromisoformat(item["end"]),
                open=bool(item["open"]),
            )
            for item in document["editions"]
        ),
    )


def load(path: Path) -> EditionsSnapshot | None:
    """The cached snapshot; None when missing or unreadable (recomputed)."""

    try:
        return _decode(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (ValueError, KeyError, TypeError) as exc:
        logger.warning("ESPN editions state %s is unreadable, recomputing: %s", path, exc)
        return None


def save(path: Path, snapshot: EditionsSnapshot) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    temporary.write_text(_encode(snapshot), encoding="utf-8")
    os.replace(temporary, path)


@contextmanager
def _locked(path: Path) -> Iterator[None]:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path.with_name(path.name + ".lock"), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def refresh(
    client,
    rows: Iterable[DenominatorRow],
    known: Sequence[EditionState],
    schedule_terminal: Mapping[str, Mapping[int, bool]],
    *,
    now: datetime,
) -> EditionsSnapshot:
    """Read every live target's current season from core and plan its editions.

    ``schedule_terminal[slug][year]`` is True when every bronze match of that
    edition is terminal (an older edition closes only then).
    """

    editions: list[EditionState] = []
    for row in sorted(rows, key=lambda item: item.slug):
        previous = [state for state in known if state.competition_slug == row.slug]
        request = urls.league_detail(row.slug)
        try:
            result = client.fetch_json(
                request.url, request.endpoint, request.params, force_refresh=True
            )
            current = parse_league_current_season(result.json_data)
            plan = plan_editions(
                current,
                previous,
                schedule_terminal.get(row.slug, {}),
                competition_slug=row.slug,
            )
        except _LEAGUE_ERRORS as exc:
            logger.warning(
                "ESPN editions of %s not refreshed, %d known kept: %s: %s",
                row.slug,
                len(previous),
                type(exc).__name__,
                exc,
            )
            editions.extend(previous)
            continue
        editions.extend(plan.open_now + plan.close_now + plan.keep)
    return EditionsSnapshot(now, tuple(editions))


def load_or_refresh(
    path: Path,
    *,
    client,
    rows: Sequence[DenominatorRow],
    schedule_terminal: Callable[[], Mapping[str, Mapping[int, bool]]],
    now: datetime,
) -> EditionsSnapshot:
    """Cached snapshot when younger than 24 h, else a refreshed and saved one."""

    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    with _locked(path):
        snapshot = load(path)
        if snapshot is not None and now - snapshot.refreshed_at < MAX_AGE:
            return snapshot
        known = snapshot.editions if snapshot is not None else ()
        fresh = refresh(client, rows, known, schedule_terminal(), now=now)
        save(path, fresh)
        return fresh


__all__ = [
    "EDITIONS_STATE_ENV",
    "EditionsSnapshot",
    "MAX_AGE",
    "default_state_path",
    "load",
    "load_or_refresh",
    "refresh",
    "save",
]
