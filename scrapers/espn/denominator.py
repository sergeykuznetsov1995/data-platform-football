"""The ESPN denominator registry: every known ESPN league with its class (#1499).

``configs/espn/denominator.tsv`` lists each ESPN soccer league we know of,
including the ones we do not collect, so that new-league discovery does not
report them again.  Only ``in_target = 1`` rows (``senior_official`` and not
``hidden``) count toward the freshness percentage.  The history queue uses
``QUEUE_PRIORITY``:

* ``1`` — ``senior_official``;
* ``9`` — youth, olympic, friendly, reserve: planned last;
* ``0`` — women, college: not collected.

Any format error fails closed: consumers must not guess the target set.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_DENOMINATOR_PATH = (
    Path(__file__).resolve().parents[2] / "configs" / "espn" / "denominator.tsv"
)
COLUMNS = (
    "slug",
    "espn_id",
    "name",
    "espn_gender",
    "class",
    "class_reason",
    "in_target",
    "hidden",
    "live",
    "current_season_year",
    "deep_level",
    "fotmob_id",
    "sofascore_id",
    "note",
)
QUEUE_PRIORITY = {
    "senior_official": 1,
    "youth": 9,
    "olympic": 9,
    "friendly": 9,
    "reserve": 9,
    "women": 0,
    "college": 0,
}


class DenominatorError(ValueError):
    """The denominator file is malformed; consumers must not guess."""


@dataclass(frozen=True)
class DenominatorRow:
    slug: str
    espn_id: int | None
    name: str
    espn_gender: str
    tournament_class: str
    class_reason: str
    in_target: bool
    hidden: bool
    live: bool
    current_season_year: int | None
    deep_level: str
    fotmob_id: str
    sofascore_id: str
    note: str


@dataclass(frozen=True)
class Denominator:
    rows: dict[str, DenominatorRow]

    def row(self, slug: str) -> DenominatorRow | None:
        return self.rows.get(slug)

    def targets(self) -> frozenset[str]:
        return frozenset(slug for slug, row in self.rows.items() if row.in_target)

    def is_target(self, slug: str) -> bool:
        row = self.rows.get(slug)
        return row is not None and row.in_target

    def live_targets(self) -> frozenset[str]:
        return frozenset(
            slug for slug, row in self.rows.items() if row.in_target and row.live
        )

    def queue_priority(self, slug: str) -> int | None:
        """History-queue priority of a known slug; ``None`` for an unknown one."""

        row = self.rows.get(slug)
        return QUEUE_PRIORITY[row.tournament_class] if row is not None else None


def denominator_path() -> Path:
    configured = os.environ.get("ESPN_DENOMINATOR_PATH", "").strip()
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
            rows: dict[str, DenominatorRow] = {}
            for line_no, fields in enumerate(reader, start=2):
                row = _parse_row(f"{source}:{line_no}", fields)
                if row.slug in rows:
                    raise DenominatorError(
                        f"{source}:{line_no}: duplicate slug {row.slug}"
                    )
                rows[row.slug] = row
    except OSError as exc:
        raise DenominatorError(f"cannot read denominator file {source}") from exc
    if not rows:
        raise DenominatorError(f"{source}: no rows")
    return Denominator(rows=rows)


def _flag(where: str, column: str, value: str) -> bool:
    if value not in ("0", "1"):
        raise DenominatorError(f"{where}: {column} must be 0 or 1, got {value!r}")
    return value == "1"


def _optional_int(where: str, column: str, value: str) -> int | None:
    if not value:
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise DenominatorError(f"{where}: non-integer {column} {value!r}") from exc


def _parse_row(where: str, fields: list[str]) -> DenominatorRow:
    if len(fields) != len(COLUMNS):
        raise DenominatorError(f"{where}: expected {len(COLUMNS)} columns")
    raw = dict(zip(COLUMNS, fields))
    slug = raw["slug"].strip()
    if not slug or slug != raw["slug"]:
        raise DenominatorError(f"{where}: empty or padded slug {raw['slug']!r}")
    klass = raw["class"]
    if klass not in QUEUE_PRIORITY:
        raise DenominatorError(f"{where}: unknown class {klass!r}")
    reason = raw["class_reason"]
    if not (reason.startswith("rule:") or reason.startswith("manual:")):
        raise DenominatorError(f"{where}: class_reason must be rule:… or manual:…")
    in_target = _flag(where, "in_target", raw["in_target"])
    hidden = _flag(where, "hidden", raw["hidden"])
    live = _flag(where, "live", raw["live"])
    if in_target != (klass == "senior_official" and not hidden):
        raise DenominatorError(
            f"{where}: in_target must be 1 exactly for senior_official "
            f"and not hidden (class {klass}, hidden {int(hidden)})"
        )
    return DenominatorRow(
        slug=slug,
        espn_id=_optional_int(where, "espn_id", raw["espn_id"]),
        name=raw["name"],
        espn_gender=raw["espn_gender"],
        tournament_class=klass,
        class_reason=reason,
        in_target=in_target,
        hidden=hidden,
        live=live,
        current_season_year=_optional_int(
            where, "current_season_year", raw["current_season_year"]
        ),
        deep_level=raw["deep_level"],
        fotmob_id=raw["fotmob_id"],
        sofascore_id=raw["sofascore_id"],
        note=raw["note"],
    )


__all__ = [
    "COLUMNS",
    "DEFAULT_DENOMINATOR_PATH",
    "Denominator",
    "DenominatorError",
    "DenominatorRow",
    "QUEUE_PRIORITY",
    "denominator_path",
    "load_denominator",
]
