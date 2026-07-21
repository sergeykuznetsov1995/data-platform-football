"""Canonical legacy-name mapping for FotMob competition identifiers.

Native ingestion uses the numeric FotMob id as its source identity.  The
legacy league name is still part of Silver/Gold contracts, so all consumers
must derive that compatibility mapping from ``configs/fotmob/competitions.json``
instead of maintaining their own copies.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Mapping


CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "configs"
    / "fotmob"
    / "competitions.json"
)


def _load_league_ids(path: Path = CONFIG_PATH) -> dict[str, str]:
    """Load and validate the source-id registry.

    Values stay as strings for compatibility with the long-standing
    ``LEAGUE_IDS`` public contract; SQL rendering converts them to integers.
    Invalid or ambiguous configuration fails at import time.
    """

    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("version") != 1:
        raise ValueError(f"{path}: expected an object with version=1")

    rows = document.get("competitions")
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{path}: competitions must be a non-empty list")

    result: dict[str, str] = {}
    ids: dict[int, str] = {}
    for index, row in enumerate(rows):
        location = f"{path}: competitions[{index}]"
        if not isinstance(row, dict) or set(row) != {"league", "competition_id"}:
            raise ValueError(
                f"{location}: expected exactly league and competition_id"
            )
        league = row["league"]
        competition_id = row["competition_id"]
        if not isinstance(league, str) or not league.strip() or league != league.strip():
            raise ValueError(f"{location}: league must be a trimmed non-empty string")
        if (
            isinstance(competition_id, bool)
            or not isinstance(competition_id, int)
            or competition_id <= 0
        ):
            raise ValueError(f"{location}: competition_id must be a positive integer")
        if league in result:
            raise ValueError(f"{location}: duplicate league {league!r}")
        if competition_id in ids:
            raise ValueError(
                f"{location}: competition_id {competition_id} is already owned by "
                f"{ids[competition_id]!r}"
            )
        result[league] = str(competition_id)
        ids[competition_id] = league
    return result


LEAGUE_IDS: Mapping[str, str] = MappingProxyType(_load_league_ids())


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def league_map_values_sql(*, indent: str = "        ") -> str:
    """Render deterministic Trino ``VALUES`` rows for Silver/xref templates."""

    rows = [
        f"({int(competition_id)}, {_sql_string(league)})"
        for league, competition_id in LEAGUE_IDS.items()
    ]
    return (",\n" + indent).join(rows)


_LEAGUE_MAP_PLACEHOLDER_RE = re.compile(
    r"^(?P<indent>[ \t]*)\{\{\s*fotmob_league_map_values_sql\s*\}\}[ \t]*$",
    re.MULTILINE,
)


def render_fotmob_sql(sql: str) -> str:
    """Expand the standalone FotMob league-map placeholder in SQL text."""

    def replace(match: re.Match[str]) -> str:
        indent = match.group("indent")
        return indent + league_map_values_sql(indent=indent)

    return _LEAGUE_MAP_PLACEHOLDER_RE.sub(replace, sql)


__all__ = [
    "CONFIG_PATH",
    "LEAGUE_IDS",
    "league_map_values_sql",
    "render_fotmob_sql",
]
