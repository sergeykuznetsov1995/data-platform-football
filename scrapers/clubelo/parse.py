"""Fail-closed parsers of clubelo.com pages (#1462).

Ported from the review prototype (``/root/clubelo-review-20260924/proto_parse.py``).
Every parser either returns fully parsed data or raises ``LayoutChanged``:
the site was rebuilt recently and keeps changing, so a page that does not look
exactly as expected must not be half-written (R-54). The raw page is stored
before parsing, so a fixed parser can re-read it later.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from lxml import html as lxml_html

MATCH_TABLE_COLUMNS = 14
# Positional parse: the 14 headers must be exactly these, in this order. The
# first one is "<year> Date" (the year span changes), so only "Date" is fixed.
MATCH_TABLE_HEADERS = (
    "Date", "H/A", "Opponent", "Prior Δ", "HFA", "Elo %", "FT", "ET", "P",
    "Game Δ", "Post-Game Δ", "Elo +/-", "New Elo", "New Rank",
)
MATCH_ROW_CELLS = (6, 14)  # 6 = fixture without a result, 14 = played match

_H1_HREF = re.compile(r'<h1><a href="/(\d{4}-\d{2}-\d{2})/([^"]+)">')
_HEADER = re.compile(
    r"Elo:\s*(-?\d+)\s*\(Best:\s*(-?\d+),\s*reached on (\d{4}-\d{2}-\d{2})\)"
    r"(?:,\s*Golo:\s*(-?\d+(?:\.\d+)?))?"
)
_VEGA_MARKER = "var vegaJson = "
_NUMBER = re.compile(r"^-?\d+(?:\.\d+)?$")
_VALUE_SIGMA = re.compile(r"^(-?\d+(?:\.\d+)?)(?:\s*±\s*(\d+(?:\.\d+)?))?$")
_POINT_KEYS = ("Date", "Elo", "Golo", "segment_id")


class LayoutChanged(Exception):
    """The page does not have the expected layout — write nothing parsed."""


@dataclass
class ClubPage:
    captured_rating_date: date
    club_name: str
    header: Dict[str, Any]
    points: List[Dict[str, Any]] = field(default_factory=list)
    matches: List[Dict[str, Any]] = field(default_factory=list)


def _iso_date(text: str, what: str) -> date:
    try:
        return date.fromisoformat(text)
    except (TypeError, ValueError):
        raise LayoutChanged(f"{what}: not an ISO date: {text!r}") from None


def _text(el) -> str:
    return " ".join(el.text_content().replace("−", "-").split())


def page_h1(html: str) -> Tuple[date, str]:
    """``(rating date, page path)`` from ``<h1><a href="/<date>/<page>">``."""

    match = _H1_HREF.search(html)
    if not match:
        raise LayoutChanged("h1 rating date not found")
    return _iso_date(match.group(1), "h1 date"), match.group(2)


def parse_ranking_slugs(html: str) -> Tuple[date, List[str]]:
    """Rating date and the club slugs linked from the country tables.

    Only clubs with a link ``href="/<slug>"`` in a country section of /Ranking
    have a club page (M-05); the country flag link ``href="/<CC>"`` wraps an
    ``<img>``, not ``span.NonAst``, so it never matches.
    """

    rating_date, page = page_h1(html)
    if page != "Ranking":
        raise LayoutChanged(f"h1 points to {page!r}, expected 'Ranking'")
    doc = lxml_html.fromstring(html)
    sections = doc.xpath(
        '//div[@class="stamm"]//div[@class="accordion-item"]'
        '[div[@class="accordion-header"]/a/@href]'
    )
    if not sections:
        raise LayoutChanged("no country sections on /Ranking")
    slugs: List[str] = []
    seen = set()
    for section in sections:
        for href in section.xpath('.//table[@class="ast"]//a[span[@class="NonAst"]]/@href'):
            if not href.startswith("/") or "/" in href[1:] or len(href) < 2:
                raise LayoutChanged(f"unexpected club link {href!r}")
            slug = href[1:]
            if slug not in seen:
                seen.add(slug)
                slugs.append(slug)
    if not slugs:
        raise LayoutChanged("no club links in the country sections of /Ranking")
    return rating_date, slugs


def _vega_points(html: str) -> List[Dict[str, Any]]:
    start = html.find(_VEGA_MARKER)
    if start < 0:
        raise LayoutChanged("vegaJson not found")
    try:
        vega, _ = json.JSONDecoder().raw_decode(html[start + len(_VEGA_MARKER):])
        datasets = vega["datasets"]
    except (ValueError, KeyError, TypeError) as exc:
        raise LayoutChanged(f"vegaJson unreadable: {exc}") from None
    if not isinstance(datasets, dict) or not all(isinstance(v, list) for v in datasets.values()):
        raise LayoutChanged("vegaJson datasets is not a mapping of lists")
    # The dataset key is a content hash (data-<hash>): take the only non-empty one.
    filled = [rows for rows in datasets.values() if rows]
    if len(filled) != 1:
        raise LayoutChanged(f"expected one non-empty vega dataset, got {len(filled)}")
    points = []
    for seq, row in enumerate(filled[0]):
        if not isinstance(row, dict) or any(k not in row for k in _POINT_KEYS):
            raise LayoutChanged(f"vega point {seq} lacks {_POINT_KEYS}: {row!r}")
        segment = row["segment_id"]
        if not isinstance(segment, int) or isinstance(segment, bool):
            raise LayoutChanged(f"vega point {seq}: segment_id is not an integer: {segment!r}")
        points.append(
            {
                "point_seq": seq,
                "point_date": _iso_date(str(row["Date"])[:10], "vega Date"),
                "elo": _finite(row["Elo"], f"vega point {seq} Elo"),
                "golo": None if row["Golo"] is None else _finite(row["Golo"], f"vega point {seq} Golo"),
                "segment_id": segment,
            }
        )
    return points


def _finite(value: Any, what: str) -> float:
    """A JSON number that is finite ("NaN"/"Infinity" strings or values fail)."""

    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise LayoutChanged(f"{what}: not a finite number: {value!r}")
    return float(value)


def _number(text: str, what: str) -> Optional[float]:
    if text == "":
        return None
    if not _NUMBER.match(text):
        raise LayoutChanged(f"{what}: not a number: {text!r}")
    return float(text)


def _integer(text: str, what: str) -> Optional[int]:
    value = _number(text, what)
    if value is not None and value != int(value):
        raise LayoutChanged(f"{what}: not an integer: {text!r}")
    return None if value is None else int(value)


def _value_sigma(text: str, what: str) -> Tuple[Optional[float], Optional[float]]:
    """``'55 ±80'`` → ``(55.0, 80.0)``; empty → ``(None, None)``."""

    if text == "":
        return None, None
    match = _VALUE_SIGMA.match(text)
    if not match:
        raise LayoutChanged(f"{what}: not 'value ±sigma': {text!r}")
    sigma = match.group(2)
    return float(match.group(1)), (None if sigma is None else float(sigma))


def _first(values: List[str]) -> Optional[str]:
    return values[0] if values else None


def _match_rows(doc) -> List[Dict[str, Any]]:
    tables = [t for t in doc.xpath("//table") if "Post-Game" in _text(t) and "New Elo" in _text(t)]
    if len(tables) != 1:
        raise LayoutChanged(f"expected one match table, got {len(tables)}")
    table = tables[0]
    header = table.xpath("./tr[th]|./thead/tr[th]|./tbody/tr[th]")
    if len(header) != 1 or len(header[0].xpath("./th")) != MATCH_TABLE_COLUMNS:
        raise LayoutChanged("match table header is not one row of 14 th")
    names = [_text(th) for th in header[0].xpath("./th")]
    names[0] = re.sub(r"^\d{4}\s*", "", names[0])  # "2026Date" → "Date"
    if tuple(names) != MATCH_TABLE_HEADERS:
        raise LayoutChanged(f"match table headers changed: {names}")
    rows = []
    for seq, tr in enumerate(table.xpath(".//tr[td]")):
        tds = tr.xpath("./td")
        if len(tds) not in MATCH_ROW_CELLS:
            raise LayoutChanged(f"match row {seq} has {len(tds)} cells")
        cells = [_text(td) for td in tds] + [""] * (MATCH_TABLE_COLUMNS - len(tds))
        date_href = _first(tds[0].xpath(".//a/@href")) or ""
        opp = tds[2]
        opp_href = _first(opp.xpath('.//a[span[@class="max640"]]/@href'))
        opp_rank = _first(opp.xpath(".//small/text()"))
        elo_pct = "".join(tds[5].xpath('./span[@class="min1081"]/text()')).strip()
        if not elo_pct:
            raise LayoutChanged(f"match row {seq}: Elo % (span.min1081) is empty")
        prior, prior_sigma = _value_sigma(cells[3], "Prior Δ")
        game, game_sigma = _value_sigma(cells[9], "Game Δ")
        post, post_sigma = _value_sigma(cells[10], "Post-Game Δ")
        if cells[1] not in ("H", "A", "N"):
            raise LayoutChanged(f"match row {seq}: H/A is {cells[1]!r}")
        rows.append(
            {
                "row_seq": seq,
                "match_date": _iso_date(date_href.lstrip("/"), "match date"),
                "venue": cells[1],
                "opp_slug": opp_href.lstrip("/") if opp_href else None,
                "opp_tlc": "".join(opp.xpath('.//span[@class="max640"]/text()')) or None,
                # Long names are split: first 16 chars in span.min641, the rest
                # in span.min1081 ("Argentinos Junio" + "rs").
                "opp_name": "".join(opp.xpath(
                    './/span[@class="min641"]/text() | .//span[@class="min1081"]/text()'
                )) or None,
                "opp_country": _first(opp.xpath(".//img/@alt")),
                "opp_rank": _integer(opp_rank.strip(), "opponent rank") if opp_rank else None,
                "prior_delta": prior,
                "prior_delta_sigma": prior_sigma,
                "hfa": _number(cells[4], "HFA"),
                "elo_pct": _number(elo_pct, "Elo %"),
                "ft": cells[6] or None,
                "et": cells[7] or None,
                "pen": cells[8] or None,
                "game_delta": game,
                "game_delta_sigma": game_sigma,
                "post_game_delta": post,
                "post_game_delta_sigma": post_sigma,
                "elo_change": _number(cells[11], "Elo +/-"),
                "new_elo": _integer(cells[12], "New Elo"),
                "new_rank": _integer(cells[13], "New Rank"),
                "has_result": bool(cells[6]),
            }
        )
    if not rows:
        raise LayoutChanged("match table has no rows")
    return rows


def parse_club_page(html: str, slug: str) -> ClubPage:
    """Parse ``/{slug}``: h1 date, header, vega points, last-16 match table."""

    captured, page = page_h1(html)
    if page != slug:
        raise LayoutChanged(f"h1 points to {page!r}, expected {slug!r}")
    doc = lxml_html.fromstring(html)
    header_match = _HEADER.search(_text(doc))
    if not header_match:
        raise LayoutChanged("header 'Elo: … (Best: …, reached on …)' not found")
    elo, best, reached, golo = header_match.groups()
    return ClubPage(
        captured_rating_date=captured,
        club_name=_text(doc.xpath("//h1")[0]),
        header={
            "elo": int(elo),
            "elo_best": int(best),
            "elo_best_reached_on": _iso_date(reached, "Best reached on"),
            "golo": None if golo is None else float(golo),
        },
        points=_vega_points(html),
        matches=_match_rows(doc),
    )
