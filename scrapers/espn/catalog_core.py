"""Cheap new-league discovery over ESPN's core league list (#1499).

Once a day the core list ``/v2/sports/soccer/leagues?limit=500`` (about 22 KB)
is compared with ``configs/espn/denominator.tsv``; only the new slugs need a
``leagues/{slug}`` detail, which ``propose_row`` turns into a candidate row
through the class rule.  The request itself and the daily call belong to the
refresh DAG (#1501/#1504); this module holds only the pure functions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from scrapers.espn.classify import classify
from scrapers.espn.core_lists import parse_ref_page
from scrapers.espn.denominator import Denominator, DenominatorRow
from scrapers.espn.discovery import CompetitionDetail
from scrapers.espn.parser_common import EspnParseError

_REF_SLUG_RE = re.compile(r"/leagues/([^/?#]+)(?:[?#]|$)")


@dataclass(frozen=True)
class CatalogDiff:
    new: frozenset[str]  # in the core list, absent from the file
    gone: frozenset[str]  # in the file, absent from the core list


def parse_core_league_refs(payload: Mapping[str, Any]) -> list[str]:
    """Slugs of one core ``leagues`` page; anything but one full page fails."""

    if not isinstance(payload, Mapping):
        raise EspnParseError("core leagues list must be an object")
    page = parse_ref_page(payload, "core leagues list")
    if page.page_count != 1:
        raise EspnParseError(
            f"core leagues list must be one page, got pageCount={page.page_count!r}"
        )
    slugs: list[str] = []
    for ref in page.refs:
        match = _REF_SLUG_RE.search(ref)
        if match is None:
            raise EspnParseError(f"core leagues item has no league $ref: {ref!r}")
        slugs.append(match.group(1))
    if len(set(slugs)) != len(slugs):
        raise EspnParseError("core leagues list repeats a slug")
    return slugs


def diff_catalog(denominator: Denominator, core_slugs: Iterable[str]) -> CatalogDiff:
    core = frozenset(core_slugs)
    known = frozenset(denominator.rows)
    return CatalogDiff(new=core - known, gone=known - core)


def propose_row(detail: CompetitionDetail) -> DenominatorRow:
    """Candidate file row for a new league; an operator reviews and commits it."""

    result = classify(detail.slug, detail.name, detail.gender.value)
    return DenominatorRow(
        slug=detail.slug,
        espn_id=detail.espn_id,
        name=detail.name,
        espn_gender=detail.gender.value,
        tournament_class=result.cls,
        class_reason=result.reason,
        in_target=result.cls == "senior_official",
        hidden=False,
        live=True,
        current_season_year=detail.source_season_year,
        deep_level="",
        fotmob_id="",
        sofascore_id="",
        note="",
    )


__all__ = [
    "CatalogDiff",
    "diff_catalog",
    "parse_core_league_refs",
    "propose_row",
]
