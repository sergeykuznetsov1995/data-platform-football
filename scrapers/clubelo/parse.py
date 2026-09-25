"""Fail-closed parsers of clubelo.com pages (#1462).

Ported from the review prototype (``/root/clubelo-review-20260924/proto_parse.py``).
Every parser either returns fully parsed data or raises ``LayoutChanged``:
the site was rebuilt recently and keeps changing, so a page that does not look
exactly as expected must not be half-written (R-54). The raw page is stored
before parsing, so a fixed parser can re-read it later.
"""

from __future__ import annotations

import ast
import json
import math
import re
from dataclasses import dataclass, field
from datetime import date, datetime
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


# ---------------------------------------------------------------------------
# /Ranking and /Results — the daily snapshot (#1463)
# ---------------------------------------------------------------------------

RANKING_MIN_ROWS = 1500  # eloData rows; 1741 on 2026-09-22 (C5)
LEVELS_MIN_RATIO = 0.97  # eloData clubs found in a country table (1722/1741)
# Club-page links of the country tables: 498 on 22–25.09.2026 (the history
# queue, same floor as history.MIN_QUEUE). Fewer = the link markup changed and
# the new-slug branch would silently see nothing (Sol r1 #5).
RANKING_MIN_LINKED = 400
RESULTS_HEADERS = (
    "Home", "Away", "Prior Δ", "HFA", "Elo %", "FT", "ET", "P", "Game Δ", "Post-Game Δ",
)
RESULT_ROW_CELLS = (5, 10)  # 5 = fixture without a result, 10 = played match

_PAGE_CREATED = re.compile(r"Page created on (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
_ELO_DATA_MARKER = "eloData = ["
# eloData cell 0: optional flag link, optional rank, the club link.
_ELO_CELL = re.compile(
    r'^<td class="l">(?:<a href="/(?P<cc>[^"]*)"><img src="[^"]*" alt="(?P<alt>[^"]*)"  '
    r'style="width:20px; opacity:0\.8;"></a>)? (?:<small> (?P<rank>\d+) </small>)?'
    r'<a href="/(?P<slug>[^"/]+)">(?P<n1>[^<]*)<span class="min481">(?P<n2>[^<]*)</span></a></td>$'
)
_ELO_INT = re.compile(r"^-?\d+$")
_ELO_DECIMAL = re.compile(r"^-?\d+\.\d+$")
_ELO_DELTA = re.compile(r"^[-+]?\d+\.\d+$")  # "-0.00" and "+0.00" both occur (25.09)
_TABLE_ELO = re.compile(r"^(-?\d+)(p?)$")  # "2046", provisional "1255p" / "-29p"
_LEVEL_GROUP = re.compile(r"^Level (\d+) \((\d+) teams\)")


@dataclass
class RankingPage:
    rating_date: date
    page_created_at: datetime
    rows: List[Dict[str, Any]]  # eloData clubs first, then provisional clubs
    elo_rows: int
    provisional: int
    levels_matched: int
    elo_precise_matched: int
    elo_precise_ambiguous: int
    linked_slugs: List[str]

    @property
    def levels_matched_pct(self) -> float:
        return round(100.0 * self.levels_matched / self.elo_rows, 2) if self.elo_rows else 0.0


@dataclass
class ResultsPage:
    rating_date: date
    page_created_at: datetime
    rows: List[Dict[str, Any]]
    duplicates: int = 0


def page_created_at(html: str) -> datetime:
    """"Page created on YYYY-MM-DD HH:MM:SS" as a naive UTC timestamp (M-15)."""

    match = _PAGE_CREATED.search(html)
    if not match:
        raise LayoutChanged("C1 'Page created on …' not found")
    return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")


def _elo_data(html: str) -> List[Dict[str, Any]]:
    start = html.find(_ELO_DATA_MARKER)
    if start < 0:
        raise LayoutChanged("C1 eloData not found")
    start += len(_ELO_DATA_MARKER) - 1
    end = html.find("];", start)
    if end < 0:
        raise LayoutChanged("C1 eloData is not closed by '];'")
    try:
        data = ast.literal_eval(html[start:end + 1])
    except (ValueError, SyntaxError) as exc:
        raise LayoutChanged(f"C2 eloData unreadable: {exc}") from None
    if not isinstance(data, list):
        raise LayoutChanged("C2 eloData is not a list")
    rows = []
    for seq, row in enumerate(data):
        if not isinstance(row, (list, tuple)) or len(row) != 4 or not all(
            isinstance(v, str) for v in row
        ):
            raise LayoutChanged(f"C2 eloData row {seq} is not 4 strings: {row!r:.200}")
        cell = _ELO_CELL.match(row[0])
        if not cell:
            raise LayoutChanged(f"C3 eloData row {seq} club cell unparsed: {row[0]!r:.200}")
        if not _ELO_INT.match(row[1]):
            raise LayoutChanged(f"C3 eloData row {seq} Elo is not an integer: {row[1]!r}")
        if not _ELO_DELTA.match(row[2]):
            raise LayoutChanged(f"C3 eloData row {seq} 1-day Δ unparsed: {row[2]!r}")
        if not _ELO_DECIMAL.match(row[3]):
            raise LayoutChanged(f"C3 eloData row {seq} Golo unparsed: {row[3]!r}")
        rows.append(
            {
                "slug": cell["slug"],
                "name": cell["n1"] + cell["n2"],
                "country": cell["alt"] or None,
                "rank": int(cell["rank"]) if cell["rank"] else None,
                "elo": int(row[1]),
                "elo_delta_1d_raw": row[2],
                "golo": float(row[3]),
            }
        )
    return rows


def _vega_ranking(html: str) -> List[Dict[str, Any]]:
    """The /Ranking vega dataset (top 100): rows with Name, FedURL and a precise Elo.

    Unlike a club page it has no slug or URL: a row is matched to eloData by
    ``(FedURL, Name)`` (100/100 unique on 2026-09-22).
    """

    start = html.find(_VEGA_MARKER)
    if start < 0:
        raise LayoutChanged("C1 vegaJson not found")
    try:
        vega, _ = json.JSONDecoder().raw_decode(html[start + len(_VEGA_MARKER):])
        datasets = vega["datasets"]
    except (ValueError, KeyError, TypeError) as exc:
        raise LayoutChanged(f"C1 vegaJson unreadable: {exc}") from None
    if not isinstance(datasets, dict) or not all(isinstance(v, list) for v in datasets.values()):
        raise LayoutChanged("C1 vegaJson datasets is not a mapping of lists")
    filled = [rows for rows in datasets.values() if rows]
    if len(filled) != 1:
        raise LayoutChanged(f"C1 expected one non-empty vega dataset, got {len(filled)}")
    out = []
    for seq, row in enumerate(filled[0]):
        if not isinstance(row, dict) or any(k not in row for k in ("Name", "FedURL", "Elo")):
            raise LayoutChanged(f"C3 vega row {seq} lacks Name/FedURL/Elo: {row!r:.200}")
        out.append({"name": row["Name"], "country": row["FedURL"],
                    "elo": _finite(row["Elo"], f"vega row {seq} Elo")})
    return out


def _country_tables(doc) -> List[Dict[str, Any]]:
    """Rows of the per-country accordion tables (sections with a flag link)."""

    sections = doc.xpath(
        '//div[@class="stamm"]//div[@class="accordion-item"]'
        '[div[@class="accordion-header"]/a/@href]'
    )
    if not sections:
        raise LayoutChanged("C1 no country tables on /Ranking")
    rows = []
    for section in sections:
        section_name = _text(section.xpath('./div[@class="accordion-header"]')[0])
        group: Optional[str] = None
        level: Optional[int] = None
        for tr in section.xpath('.//table[@class="ast"]//tr'):
            if tr.xpath('./td[@class="l"]/i'):
                group = " ".join(_text(td) for td in tr.xpath("./td"))  # "Level 1 (18 teams) ⌀1751"
                match = _LEVEL_GROUP.match(group)
                if not match and group.split(" ")[0] != "Lower":
                    # only "Level N (k teams)" and "Lower" exist: a renamed
                    # header would leave every level NULL (Sol r1 #3)
                    raise LayoutChanged(f"C3 country table {section_name!r}: group header {group!r}")
                level = int(match.group(1)) if match else None
                continue
            tds = tr.xpath("./td")
            if not tds:
                continue  # header row (th) or an empty <tr> of the broken markup
            if len(tds) != 2:
                raise LayoutChanged(f"C3 country table {section_name!r}: row with {len(tds)} cells")
            club, elo_cell = tds
            href = _first(club.xpath('.//a[span[@class="NonAst"]]/@href'))
            elo_raw = _text(elo_cell)
            elo_match = _TABLE_ELO.match(elo_raw)
            if not elo_match:
                raise LayoutChanged(f"C3 country table {section_name!r}: Elo cell {elo_raw!r}")
            rank = _first(club.xpath(".//small/text()"))
            rows.append(
                {
                    "level_section": section_name,
                    "level_group": group,
                    "level": level,
                    "slug": href[1:] if href and href.startswith("/") and len(href) > 1 else None,
                    "name": "".join(club.xpath('.//span[@class="Ast"]/text()')),
                    "country": _first(club.xpath(".//img/@alt")),
                    "rank": _integer(rank.strip(), "country table rank") if rank else None,
                    "elo_raw": elo_raw,
                    "elo": int(elo_match.group(1)),
                    "is_provisional": bool(elo_match.group(2)),
                }
            )
    return rows


def _unique(candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return candidates[0] if len(candidates) == 1 else None


def parse_ranking(html: str) -> RankingPage:
    """Parse ``/Ranking`` into snapshot rows; structural breaks raise ``LayoutChanged``.

    - eloData (1741 clubs): slug, name, country, rank, integer Elo, raw 1-day Δ, Golo;
    - vegaJson (top 100): ``elo_precise`` matched by ``(country, name)``;
    - country tables: level / level_group / level_section, matched by slug, or
      for a club without a page link by ``(country, Elo)`` and a name prefix
      (the table name is cut at 16 characters);
    - provisional clubs ("1255p", "-29p") exist only in the country tables:
      ``slug`` NULL (no link), ``club_key`` = ``~CC:Name``.

    Thresholds (rows, matched levels) are checked by ``check_ranking``.
    """

    rating_date, page = page_h1(html)
    if page != "Ranking":
        raise LayoutChanged(f"C1 h1 points to {page!r}, expected 'Ranking'")
    created = page_created_at(html)
    elo_rows = _elo_data(html)
    vega = _vega_ranking(html)
    table_rows = _country_tables(lxml_html.fromstring(html))

    precise: Dict[Tuple[Optional[str], str], List[float]] = {}
    for row in vega:
        precise.setdefault((row["country"], row["name"]), []).append(row["elo"])
    by_slug: Dict[str, List[Dict[str, Any]]] = {}
    unlinked: Dict[Tuple[Optional[str], str], List[Dict[str, Any]]] = {}
    for row in table_rows:
        if row["slug"]:
            by_slug.setdefault(row["slug"], []).append(row)
        elif not row["is_provisional"]:
            unlinked.setdefault((row["country"], row["elo_raw"]), []).append(row)

    rows: List[Dict[str, Any]] = []
    levels_matched = precise_matched = precise_ambiguous = 0
    for club in elo_rows:
        if club["slug"] in by_slug:
            table = _unique(by_slug[club["slug"]])
        else:
            table = _unique([
                r for r in unlinked.get((club["country"], str(club["elo"])), [])
                if club["name"].startswith(r["name"])
            ])
        levels_matched += table is not None
        values = precise.get((club["country"], club["name"]), [])
        precise_matched += len(values) == 1
        precise_ambiguous += len(values) > 1
        rows.append(
            {
                "club_key": club["slug"],
                **club,
                "level": table["level"] if table else None,
                "level_group": table["level_group"] if table else None,
                "level_section": table["level_section"] if table else None,
                "elo_precise": values[0] if len(values) == 1 else None,
                "is_provisional": False,
            }
        )
    provisional = 0
    for row in table_rows:
        if not row["is_provisional"]:
            continue
        if not row["slug"] and not (row["country"] and row["name"]):
            raise LayoutChanged(f"C3 provisional club without country or name: {row!r:.200}")
        provisional += 1
        rows.append(
            {
                "club_key": row["slug"] or f"~{row['country'] or ''}:{row['name']}",
                "slug": row["slug"],
                "name": row["name"],
                "country": row["country"],
                "rank": row["rank"],
                "elo": row["elo"],
                "elo_delta_1d_raw": None,
                "golo": None,
                "level": row["level"],
                "level_group": row["level_group"],
                "level_section": row["level_section"],
                "elo_precise": None,
                "is_provisional": True,
            }
        )
    keys = [row["club_key"] for row in rows]
    if len(set(keys)) != len(keys):
        twins = sorted({k for k in keys if keys.count(k) > 1})[:5]
        raise LayoutChanged(f"C3 club_key not unique: {twins}")
    return RankingPage(
        rating_date=rating_date,
        page_created_at=created,
        rows=rows,
        elo_rows=len(elo_rows),
        provisional=provisional,
        levels_matched=levels_matched,
        elo_precise_matched=precise_matched,
        elo_precise_ambiguous=precise_ambiguous,
        linked_slugs=list(dict.fromkeys(r["slug"] for r in table_rows if r["slug"])),
    )


def check_ranking(
    page: RankingPage,
    *,
    min_rows: int = RANKING_MIN_ROWS,
    min_levels_ratio: float = LEVELS_MIN_RATIO,
    min_linked: int = RANKING_MIN_LINKED,
) -> None:
    """Fail-closed thresholds of a parsed /Ranking (C5 rows, C6 levels, C7 links)."""

    if page.elo_rows < min_rows:
        raise LayoutChanged(f"C5 eloData has {page.elo_rows} rows, expected >= {min_rows}")
    if page.levels_matched < min_levels_ratio * page.elo_rows:
        raise LayoutChanged(
            f"C6 levels matched {page.levels_matched}/{page.elo_rows} "
            f"({page.levels_matched_pct}%), expected >= {min_levels_ratio:.0%}"
        )
    if len(page.linked_slugs) < min_linked:
        raise LayoutChanged(
            f"C7 country tables link {len(page.linked_slugs)} club pages, expected >= {min_linked}"
        )


def _team(td) -> Dict[str, Any]:
    slug = _first(td.xpath('.//a[span[@class="max640"]]/@href'))
    rank = _first(td.xpath(".//small/text()"))
    team = {
        "slug": slug[1:] if slug and slug.startswith("/") and len(slug) > 1 else None,
        "tlc": "".join(td.xpath('.//span[@class="max640"]/text()')) or None,
        "name": "".join(td.xpath(
            './/span[@class="min641"]/text() | .//span[@class="min1081"]/text()'
        )) or None,
        "country": _first(td.xpath(".//img/@alt")),
        "rank": _integer(rank.strip(), "team rank") if rank and rank.strip() else None,
    }
    if not team["slug"] and not (team["country"] and team["name"]):
        raise LayoutChanged(f"C3 result team without link, country or name: {_text(td)!r}")
    team["key"] = team["slug"] or f"~{team['country'] or ''}:{team['name']}"
    return team


def parse_results(html: str) -> ResultsPage:
    """Parse ``/Results``: rows under date separators, the first block is the h1 date.

    The markup is broken (``<tr><tr>`` after a separator); lxml repairs it into
    an empty ``<tr>`` that has no cells and is skipped. A row without a score
    is kept (``ft`` NULL); ``is_final`` is true when "Game Δ" is filled. A key
    ``(match_date, home_key, away_key)`` seen twice keeps the first row and is
    counted in ``duplicates``.
    """

    rating_date, page = page_h1(html)
    if page != "Results":
        raise LayoutChanged(f"C1 h1 points to {page!r}, expected 'Results'")
    created = page_created_at(html)
    doc = lxml_html.fromstring(html)
    tables = [t for t in doc.xpath("//table") if "Post-Game" in _text(t) and "Home" in _text(t)]
    if len(tables) != 1:
        raise LayoutChanged(f"C1 expected one results table, got {len(tables)}")
    header = tables[0].xpath("./tr[th]|./thead/tr[th]|./tbody/tr[th]")
    names = tuple(_text(th) for th in header[0].xpath("./th")) if len(header) == 1 else ()
    if names != RESULTS_HEADERS:
        raise LayoutChanged(f"C1 results table headers changed: {names}")
    rows: List[Dict[str, Any]] = []
    seen = set()
    duplicates = separators = 0
    current = rating_date
    for tr in tables[0].xpath(".//tr[td]"):
        tds = tr.xpath("./td")
        if len(tds) == 1 and tds[0].get("colspan"):
            day = _iso_date(_text(tds[0]), "C3 results date separator")
            if day >= current:
                raise LayoutChanged(f"C3 results date separator {day} is not before {current}")
            current = day
            separators += 1
            continue
        if len(tds) not in RESULT_ROW_CELLS:
            raise LayoutChanged(f"C3 results row {len(rows)} has {len(tds)} cells")
        cells = [_text(td) for td in tds] + [""] * (10 - len(tds))
        home, away = _team(tds[0]), _team(tds[1])
        elo_pct = "".join(tds[4].xpath('./span[@class="min1081"]/text()')).strip()
        if not elo_pct:
            raise LayoutChanged(f"C3 results row {len(rows)}: Elo % (span.min1081) is empty")
        # "NEW" instead of a number: a club without a prior rating (25.09)
        prior, prior_sigma = (None, None) if cells[2] == "NEW" else _value_sigma(cells[2], "Prior Δ")
        game, game_sigma = _value_sigma(cells[8], "Game Δ")
        post, post_sigma = _value_sigma(cells[9], "Post-Game Δ")
        key = (current, home["key"], away["key"])
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        row = {"match_date": current, "row_seq": len(rows) + duplicates}
        for side, team in (("home", home), ("away", away)):
            for name in ("key", "slug", "tlc", "name", "country", "rank"):
                row[f"{side}_{name}"] = team[name]
        row.update(
            prior_delta=prior,
            prior_delta_sigma=prior_sigma,
            prior_delta_raw=cells[2],
            hfa=_number(cells[3], "HFA"),
            elo_pct=_number(elo_pct, "Elo %"),
            ft=cells[5] or None,
            et=cells[6] or None,
            pen=cells[7] or None,
            game_delta=game,
            game_delta_sigma=game_sigma,
            post_game_delta=post,
            post_game_delta_sigma=post_sigma,
            is_final=game is not None,
        )
        rows.append(row)
    if not rows:
        raise LayoutChanged("C5 results table has no rows")
    if not separators:
        # the ~3-day window always has older dates; without separators every
        # row would get the h1 date (Sol r1 #6)
        raise LayoutChanged("C3 results table has no date separators")
    return ResultsPage(rating_date=rating_date, page_created_at=created, rows=rows,
                       duplicates=duplicates)
