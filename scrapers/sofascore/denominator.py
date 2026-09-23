"""The SofaScore denominator registry: one file for every queue (#1353).

``configs/sofascore/denominator.tsv`` classifies every tournament we count as
ours (or explicitly not): the frozen all-men snapshot plus the daily
registry's tournaments.  Planners use it as a filter and an order on top of
the snapshot, whose id set stays locked by the campaign policy:

* ``queue_priority 1`` — core, planned in the lanes' usual order;
* ``queue_priority 9`` — disputed buckets, planned only after all core;
* ``queue_priority 0`` — outside both queues (esoccer, student).
"""

from __future__ import annotations

import csv
import logging
import os
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DENOMINATOR_PATH = (
    Path(__file__).resolve().parents[2]
    / "configs"
    / "sofascore"
    / "denominator.tsv"
)
COLUMNS = (
    "tournament_id", "capture_key", "name", "class", "queue_priority", "basis",
)
CLASS_PRIORITY = {
    "core": 1,
    "esoccer": 0,
    "student": 0,
    "youth": 9,
    "reserve": 9,
    "amateur": 9,
    "show": 9,
    "women": 9,
    "unknown": 9,
}
MISSING_PRIORITY = 9


class DenominatorError(ValueError):
    """The denominator file is malformed; planners must not guess."""


@dataclass(frozen=True)
class DenominatorRow:
    tournament_id: int
    capture_key: str
    name: str
    tournament_class: str
    queue_priority: int
    basis: str


@dataclass(frozen=True)
class Denominator:
    rows: dict[int, DenominatorRow]

    def queue_priority(self, tournament_id: int) -> int:
        row = self.rows.get(int(tournament_id))
        if row is None:
            logger.warning(
                "tournament %s is not in the denominator file; queued last "
                "(priority %s)", tournament_id, MISSING_PRIORITY,
            )
            return MISSING_PRIORITY
        return row.queue_priority

    def is_core(self, tournament_id: int) -> bool:
        return self.class_of(tournament_id) == "core"

    def class_of(self, tournament_id: int) -> str | None:
        row = self.rows.get(int(tournament_id))
        return row.tournament_class if row is not None else None

    def core_capture_keys(self) -> frozenset[str]:
        return frozenset(
            row.capture_key for row in self.rows.values()
            if row.tournament_class == "core"
        )


def denominator_path() -> Path:
    configured = os.environ.get("SOFASCORE_DENOMINATOR_PATH", "").strip()
    return Path(configured) if configured else DEFAULT_DENOMINATOR_PATH


def load_denominator(path: Path | None = None) -> Denominator:
    """Read and validate the file; any format error fails closed."""

    source = Path(path) if path is not None else denominator_path()
    try:
        with source.open(encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle, delimiter="\t", quoting=csv.QUOTE_NONE)
            header = next(reader, None)
            if tuple(header or ()) != COLUMNS:
                raise DenominatorError(f"{source}: unexpected header {header}")
            rows: dict[int, DenominatorRow] = {}
            for line_no, fields in enumerate(reader, start=2):
                row = _parse_row(source, line_no, fields)
                if row.tournament_id in rows:
                    raise DenominatorError(
                        f"{source}:{line_no}: duplicate tournament_id "
                        f"{row.tournament_id}"
                    )
                rows[row.tournament_id] = row
    except OSError as exc:
        raise DenominatorError(f"cannot read denominator file {source}") from exc
    if not rows:
        raise DenominatorError(f"{source}: no rows")
    return Denominator(rows=rows)


def _parse_row(source: Path, line_no: int, fields: list[str]) -> DenominatorRow:
    where = f"{source}:{line_no}"
    if len(fields) != len(COLUMNS):
        raise DenominatorError(f"{where}: expected {len(COLUMNS)} columns")
    raw_id, capture_key, name, klass, raw_priority, basis = fields
    try:
        tournament_id = int(raw_id)
        priority = int(raw_priority)
    except ValueError as exc:
        raise DenominatorError(f"{where}: non-integer id or priority") from exc
    if tournament_id < 1:
        raise DenominatorError(f"{where}: tournament_id must be positive")
    if klass not in CLASS_PRIORITY:
        raise DenominatorError(f"{where}: unknown class {klass!r}")
    if priority != CLASS_PRIORITY[klass]:
        raise DenominatorError(
            f"{where}: class {klass} must have queue_priority "
            f"{CLASS_PRIORITY[klass]}, got {priority}"
        )
    if not capture_key.strip() or not basis.strip():
        raise DenominatorError(f"{where}: capture_key and basis are required")
    return DenominatorRow(
        tournament_id=tournament_id,
        capture_key=capture_key,
        name=name,
        tournament_class=klass,
        queue_priority=priority,
        basis=basis,
    )
