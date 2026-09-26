"""The one Transfermarkt rule between ``saison_id`` and the season it names (#1390).

Transfermarkt keys every edition by a four-digit ``saison_id``:

* a split-year edition (``25/26``) is keyed by the year it opens in —
  ``saison_id=2025``;
* a calendar-year edition (``2026``) is keyed by the year BEFORE it —
  ``saison_id=2025``.  Leagues with a season selector show it (BRA1
  ``saison_id=2025`` is labelled "2026"), and so do cup pages without one:
  their ``<tm-competition-homepage season-id>`` follows this rule on all 303
  pages of the 17.07.2026 discovery cache that carry it (177 calendar, e.g.
  Copa do Brasil "2026" -> 2025).  The tmapi ``competition/{id}/club``
  parameter is not measured yet (#1390): the review of 23.09 saw
  ``season.id=2025`` for the "2026" edition of Copa do Brasil.

The canonical season is the Bronze slug: ``2526`` for a split year, ``2026``
for a calendar year.  Edition labels printed by the site ("25/26", "91/92",
"2026") are parsed here too, so no caller keeps its own copy of the rule.

``scraper._normalise_event_season`` (the season a transfer belongs to, by the
July rule) is a different notion and deliberately does not use this module.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Optional

SPLIT_YEAR = "split_year"
SINGLE_YEAR = "single_year"

_PAIR_RE = re.compile(
    r"(?P<start>(?:18|19|20|21)?\d{2})\s*[/\-]\s*"
    r"(?P<end>(?:18|19|20|21)?\d{2})"
)
_YEAR_RE = re.compile(r"(?:18|19|20|21)\d{2}")


class SeasonRuleError(ValueError):
    """A value that the season rule cannot read without guessing."""


def _format(season_format: Any) -> str:
    value = str(getattr(season_format, "value", season_format) or "").strip()
    if value not in (SPLIT_YEAR, SINGLE_YEAR):
        raise SeasonRuleError(f"season format cannot produce a season: {value!r}")
    return value


def _two_digit_year(value: int, today: Optional[date]) -> int:
    """A two-digit year by the century window: at most next year is 20xx."""

    current = (today or date.today()).year
    candidate = 2000 + value
    return candidate if candidate <= current + 1 else 1900 + value


def split_year_bounds(raw: Any, *, today: Optional[date] = None) -> tuple[int, int]:
    """The two calendar years a split-year label or ``saison_id`` spans.

    ``"25/26"`` and ``2025`` are both ``(2025, 2026)``; ``"91/92"`` is
    ``(1991, 1992)`` — never 2091.
    """

    text = str(raw).strip()
    pair = _PAIR_RE.fullmatch(text)
    if pair is not None:
        start_text, end_text = pair.group("start"), pair.group("end")
        start = (
            int(start_text)
            if len(start_text) == 4
            else _two_digit_year(int(start_text), today)
        )
        if len(end_text) == 4:
            end = int(end_text)
        else:
            end = (start // 100) * 100 + int(end_text)
            if end < start:
                end += 100
        if end != start + 1:
            raise SeasonRuleError(f"split-year edition must span one year: {text!r}")
        return start, end
    if _YEAR_RE.fullmatch(text):
        return int(text), int(text) + 1
    raise SeasonRuleError(f"invalid split-year edition: {text!r}")


def label_to_season(label: Any, season_format: Any) -> str:
    """The canonical season of a label the site prints ("25/26", "2026")."""

    fmt = _format(season_format)
    text = str(label).strip()
    if fmt == SINGLE_YEAR:
        if not _YEAR_RE.fullmatch(text):
            raise SeasonRuleError(f"invalid single-year edition: {text!r}")
        return text
    start, end = split_year_bounds(text)
    return f"{start % 100:02d}{end % 100:02d}"


def season_to_saison_id(season: Any, season_format: Any) -> int:
    """The ``saison_id`` of a canonical season.

    A split-year season is the four-digit slug (``2526``, ``9192``) — four
    digits are never read as a year here, since slug ``2021`` (20/21) and
    year 2021 look alike.
    """

    fmt = _format(season_format)
    text = str(season).strip()
    if fmt == SINGLE_YEAR:
        if not _YEAR_RE.fullmatch(text):
            raise SeasonRuleError(f"invalid single-year season: {text!r}")
        return int(text) - 1
    if not re.fullmatch(r"\d{4}", text):
        raise SeasonRuleError(f"invalid split-year season slug: {text!r}")
    return split_year_bounds(f"{text[:2]}/{text[2:]}")[0]


def saison_id_to_season(saison_id: Any, season_format: Any) -> str:
    """The canonical season a ``saison_id`` names."""

    fmt = _format(season_format)
    text = str(saison_id).strip()
    if not _YEAR_RE.fullmatch(text):
        raise SeasonRuleError(f"invalid saison_id: {text!r}")
    value = int(text)
    if fmt == SINGLE_YEAR:
        return str(value + 1)
    return f"{value % 100:02d}{(value + 1) % 100:02d}"


__all__ = [
    "SINGLE_YEAR",
    "SPLIT_YEAR",
    "SeasonRuleError",
    "label_to_season",
    "saison_id_to_season",
    "season_to_saison_id",
    "split_year_bounds",
]
