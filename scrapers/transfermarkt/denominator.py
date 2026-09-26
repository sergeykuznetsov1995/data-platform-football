"""The Transfermarkt denominator: one file for every queue (#1390).

``configs/transfermarkt/denominator.tsv`` classifies every competition of the
registry.  The milestone-1 denominator is the set of live core competitions
(``live=1`` and class ``core_club`` or ``core_national``); youth and reserve
competitions are collected after core, outside the percentage; amateur and
archive competitions are not planned by the current lane.  Any format error
fails closed: planners must not guess.
"""

from __future__ import annotations

import csv
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_DENOMINATOR_PATH = (
    Path(__file__).resolve().parents[2]
    / "configs"
    / "transfermarkt"
    / "denominator.tsv"
)
COLUMNS = (
    "id", "name", "country", "confederation", "tier", "class", "route", "live",
    "current_saison_id", "step2_saison_ids", "fotmob_id", "sofascore_id",
    "fbref_id", "reason",
)
CORE_CLASSES = frozenset({"core_club", "core_national"})
TAIL_CLASSES = frozenset({"youth", "reserve"})
CLASSES = CORE_CLASSES | TAIL_CLASSES | frozenset({"amateur", "archive"})
ROUTES = frozenset({"wettbewerb", "pokal"})
REQUIRED = ("id", "name", "country", "confederation", "tier", "class", "route",
            "live", "current_saison_id", "reason")
_SAISON_ID = re.compile(r"(?:18|19|20|21)\d{2}")


class DenominatorError(ValueError):
    """The denominator file is malformed; planners must not guess."""


@dataclass(frozen=True)
class DenominatorRow:
    competition_id: str
    name: str
    country: str
    confederation: str
    tier: str
    competition_class: str
    route: str
    live: bool
    current_saison_id: int
    step2_saison_ids: tuple[int, ...]
    fotmob_id: str
    sofascore_id: str
    fbref_id: str
    reason: str

    @property
    def is_core(self) -> bool:
        return self.live and self.competition_class in CORE_CLASSES


@dataclass(frozen=True)
class Denominator:
    rows: dict[str, DenominatorRow]

    def row(self, competition_id: str) -> DenominatorRow | None:
        return self.rows.get(str(competition_id))

    def denominator_ids(self) -> frozenset[str]:
        """Milestone-1 denominator: live core competitions."""

        return frozenset(key for key, row in self.rows.items() if row.is_core)

    def queue_rank(self, competition_id: str) -> int | None:
        """0 = core, 1 = youth/reserve tail, None = not planned.

        A competition missing from the file is queued with the tail and
        logged, so a newly discovered competition is never silently lost.
        """

        row = self.rows.get(str(competition_id))
        if row is None:
            logger.warning(
                "competition %s is not in the denominator file; queued after core",
                competition_id,
            )
            return 1
        if not row.live:
            return None
        if row.competition_class in CORE_CLASSES:
            return 0
        if row.competition_class in TAIL_CLASSES:
            return 1
        return None


def denominator_path() -> Path:
    configured = os.environ.get("TRANSFERMARKT_DENOMINATOR_PATH", "").strip()
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
                if row.competition_id in rows:
                    raise DenominatorError(
                        f"{source}:{line_no}: duplicate id {row.competition_id}"
                    )
                rows[row.competition_id] = row
    except OSError as exc:
        raise DenominatorError(f"cannot read denominator file {source}") from exc
    if not any(row.is_core for row in rows.values()):
        raise DenominatorError(f"{source}: no live core competitions")
    return Denominator(rows=rows)


def denominator_ids(path: Path | None = None) -> frozenset[str]:
    """The competition ids that form the milestone-1 denominator."""

    return load_denominator(path).denominator_ids()


def _saison_ids(where: str, value: str) -> tuple[int, ...]:
    if not value:
        return ()
    items = value.split(",")
    if not all(_SAISON_ID.fullmatch(item) for item in items):
        raise DenominatorError(f"{where}: invalid step2_saison_ids {value!r}")
    return tuple(int(item) for item in items)


def _parse_row(where: str, fields: list[str]) -> DenominatorRow:
    if len(fields) != len(COLUMNS):
        raise DenominatorError(f"{where}: expected {len(COLUMNS)} columns")
    values = dict(zip(COLUMNS, (item.strip() for item in fields)))
    missing = [name for name in REQUIRED if not values[name]]
    if missing:
        raise DenominatorError(f"{where}: empty required fields {missing}")
    if not any(char.isalpha() for char in values["name"]):
        raise DenominatorError(f"{where}: name has no letters {values['name']!r}")
    if values["class"] not in CLASSES:
        raise DenominatorError(f"{where}: unknown class {values['class']!r}")
    if values["route"] not in ROUTES:
        raise DenominatorError(f"{where}: unknown route {values['route']!r}")
    if values["live"] not in ("0", "1"):
        raise DenominatorError(f"{where}: live must be 0 or 1")
    live = values["live"] == "1"
    if (values["class"] == "archive") == live:
        raise DenominatorError(f"{where}: class archive is exactly live=0")
    if not _SAISON_ID.fullmatch(values["current_saison_id"]):
        raise DenominatorError(f"{where}: invalid current_saison_id")
    return DenominatorRow(
        competition_id=values["id"],
        name=values["name"],
        country=values["country"],
        confederation=values["confederation"],
        tier=values["tier"],
        competition_class=values["class"],
        route=values["route"],
        live=live,
        current_saison_id=int(values["current_saison_id"]),
        step2_saison_ids=_saison_ids(where, values["step2_saison_ids"]),
        fotmob_id=values["fotmob_id"],
        sofascore_id=values["sofascore_id"],
        fbref_id=values["fbref_id"],
        reason=values["reason"],
    )


__all__ = [
    "COLUMNS",
    "CORE_CLASSES",
    "DEFAULT_DENOMINATOR_PATH",
    "Denominator",
    "DenominatorError",
    "DenominatorRow",
    "TAIL_CLASSES",
    "denominator_ids",
    "denominator_path",
    "load_denominator",
]
