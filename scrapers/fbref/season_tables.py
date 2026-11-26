"""Source tables for competition standings and squad rosters in typed Bronze."""

from __future__ import annotations

import re
from urllib.parse import urlsplit

import pandas as pd
from bs4 import BeautifulSoup, Tag

from scrapers.fbref.discovery import (
    _document_soups,
    _has_season_component,
    _inside_global_chrome,
)
from scrapers.fbref.match_parser import DatasetParseResult, DatasetStatus
from scrapers.fbref.parsers.id_extractors import PLAYER_ID_PATTERN, TEAM_ID_PATTERN
from scrapers.fbref.parsers.table_parser import extract_tables_from_comments


_STANDINGS_ID = re.compile(r"results[\w-]*_overall", re.I)
_SQUAD_STATS_ID = re.compile(r"stats_standard(?:_(?:combined|all|\d+))?", re.I)
_YEAR = re.compile(r"\d{4}(?:-\d{4})?")
_SQUAD_PATH = re.compile(r"^/en/squads/([^/]+)(?:/|$)")
_INT_FIELDS = {
    "standings": (
        "rank",
        "games",
        "wins",
        "ties",
        "losses",
        "goals_for",
        "goals_against",
        "goal_diff",
        "points",
    ),
    "squad_roster": ("games", "games_starts", "minutes"),
}
_FLOAT_FIELDS = {
    "standings": ("points_avg", "xg_for", "xg_against", "xg_diff"),
    "squad_roster": (),
}
_TEXT_FIELDS = {
    "standings": ("notes",),
    "squad_roster": ("nationality", "position", "age", "shirtnumber"),
}


def _text(cell):
    return cell.get_text(" ", strip=True) if cell is not None else ""


def _number(value):
    if not value or value in {"—", "–"}:
        return None
    return value.replace(",", "").replace("−", "-")


def _body_rows(table: Tag):
    for row in table.find_all("tr"):
        if row.find_parent("table") is not table or row.find_parent(["thead", "tfoot"]):
            continue
        if set(row.get("class") or ()) & {"thead", "over_header", "spacer"}:
            continue
        cells = row.find_all(["th", "td"], recursive=False)
        if (
            not cells
            or not _text(row)
            or all(cell.get("scope") == "col" for cell in cells)
        ):
            continue
        # Direct (unwrapped) column headers sometimes omit thead and scope.
        if all(
            cell.name == "th"
            and cell.get("scope") != "row"
            and not cell.find("a", href=True)
            for cell in cells
        ) and any(
            _text(cell).casefold() in {"player", "squad", "team"} for cell in cells
        ):
            continue
        yield {cell.get("data-stat"): cell for cell in cells if cell.get("data-stat")}


def _source_entity(cell: Tag, pattern) -> tuple[str | None, str]:
    name = _text(cell)
    if not name:
        raise ValueError("Missing source row name")
    ids = {
        match.group(1)
        for anchor in cell.find_all("a", href=True)
        if (match := pattern.search(str(anchor["href"])))
    }
    append_id = str(cell.get("data-append-csv") or "").strip()
    if re.fullmatch(r"[a-f0-9]{8}", append_id):
        ids.add(append_id)
    if len(ids) > 1:
        raise ValueError("Conflicting source IDs in one identity cell")
    return next(iter(ids), None), name


def _roster_row_season(player_cell: Tag) -> str | None:
    seasons = set()
    for cell in player_cell.parent.find_all(
        ["th", "td"], attrs={"data-stat": ["season", "year_id"]}, recursive=False
    ):
        value = _text(cell)
        if not value or value in {"-", "–", "—"}:
            continue
        value = value.replace("–", "-")
        if not _YEAR.fullmatch(value):
            raise ValueError("Unrecognised season value on roster row")
        seasons.add(value)
    if len(seasons) > 1:
        raise ValueError("Conflicting season values on roster row")
    return next(iter(seasons), None)


def _table_result(dataset: str, tables: list[Tag]) -> DatasetParseResult:
    if not tables:
        return DatasetParseResult(
            dataset,
            DatasetStatus.NOT_APPLICABLE,
            reason="typed_source_table_not_published",
        )
    is_squad = dataset == "squad_roster"
    identity = "player" if is_squad else "team"
    source_rows = {}
    try:
        for table in tables:
            table_id = str(table["id"])
            caption = _text(table.find("caption", recursive=False)) or None
            records = []
            for cells in _body_rows(table):
                cell = cells.get(identity)
                if cell is None and not is_squad:
                    cell = cells.get("squad")
                if cell is None:
                    raise ValueError(f"{table_id}: data row has no {identity} cell")
                entity_id, name = _source_entity(
                    cell, PLAYER_ID_PATTERN if is_squad else TEAM_ID_PATTERN
                )
                record = {
                    "source_table_id": table_id,
                    "source_table_caption": caption,
                    "source_row_index": len(records),
                    identity: name,
                    f"{identity}_id": entity_id,
                }
                if is_squad:
                    record["source_season_id"] = _roster_row_season(cell)
                for key in _TEXT_FIELDS[dataset]:
                    record[key] = _text(cells.get(key)) or None
                for key in (*_INT_FIELDS[dataset], *_FLOAT_FIELDS[dataset]):
                    record[key] = _number(_text(cells.get(key)))
                records.append(record)
            allowed_headers = [identity] if is_squad else [identity, "squad"]
            if not records and not any(
                cell.find_parent("thead") is not None
                or cell.get("scope") == "col"
                or _text(cell).casefold() in {"player", "squad", "team"}
                for cell in table.find_all("th", attrs={"data-stat": allowed_headers})
            ):
                raise ValueError(f"{table_id}: empty table lacks identity headers")
            # The same source ID can identify several named groups. An exact
            # copy in a comment should not duplicate the original group.
            key = table_id, caption
            if key in source_rows and source_rows[key] != records:
                raise ValueError(
                    f"{table_id}: different tables share the same source ID and caption"
                )
            source_rows[key] = records

        rows = [row for group in source_rows.values() for row in group]
        if not rows:
            return DatasetParseResult(
                dataset, DatasetStatus.EMPTY, reason="typed_source_table_empty"
            )
        frame = pd.DataFrame(rows)
        for key in _INT_FIELDS[dataset]:
            frame[key] = pd.to_numeric(frame[key], errors="raise").astype("Int64")
        for key in _FLOAT_FIELDS[dataset]:
            frame[key] = pd.to_numeric(frame[key], errors="raise").astype("Float64")
        for key in (
            identity,
            f"{identity}_id",
            "source_table_id",
            "source_table_caption",
            *_TEXT_FIELDS[dataset],
        ):
            frame[key] = frame[key].astype("string")
        return DatasetParseResult(dataset, DatasetStatus.AVAILABLE, frame=frame)
    except (TypeError, ValueError) as exc:
        return DatasetParseResult(
            dataset,
            DatasetStatus.ERROR,
            reason="typed_source_table_unparsed",
            error_type="SeasonPageSchemaDriftError",
            error_message=str(exc),
        )


def _page_addresses(soup, canonical_url):
    if canonical_url:
        yield canonical_url
    yield from (
        str(link["href"]) for link in soup.select('link[rel~="canonical"][href]')
    )
    yield from (
        str(meta["content"]) for meta in soup.select('meta[property="og:url"][content]')
    )


def standings_result(
    tables: list[Tag],
    *,
    soup=None,
    canonical_url=None,
    competition_id=None,
    season_id=None,
) -> DatasetParseResult:
    tables = [
        table for table in tables if _STANDINGS_ID.fullmatch(str(table.get("id") or ""))
    ]
    if (
        tables
        and soup is not None
        and competition_id is not None
        and season_id is not None
    ):
        addresses = _page_addresses(soup, canonical_url)
        for address in addresses:
            url = urlsplit(address)
            parts = [part for part in url.path.split("/") if part]
            route = parts[1:]
            if (
                url.hostname not in (None, "fbref.com", "www.fbref.com")
                or len(parts) < 4
                or parts[:3] != ["en", "comps", competition_id]
                or (_has_season_component(route) and route[2] != season_id)
            ):
                return _invalid_standings_scope()
        if _YEAR.fullmatch(season_id):
            heading = re.match(r"^(\d{4}(?:-\d{4})?)(?:\s|$)", _text(soup.find("h1")))
            if heading and heading[1] != season_id:
                return _invalid_standings_scope()
    return _table_result("standings", tables)


def _invalid_standings_scope() -> DatasetParseResult:
    return DatasetParseResult(
        "standings",
        DatasetStatus.ERROR,
        reason="standings_page_identity_invalid",
        error_type="StandingsPageContractError",
        error_message="Page competition or edition differs from target",
    )


def parse_standings_html(
    html: str | bytes,
    *,
    canonical_url=None,
    competition_id=None,
    season_id=None,
) -> DatasetParseResult:
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")
    try:
        return standings_result(
            [*soup.find_all("table"), *extract_tables_from_comments(soup)],
            soup=soup,
            canonical_url=canonical_url,
            competition_id=competition_id,
            season_id=season_id,
        )
    finally:
        soup.decompose()


def _squad_scope(
    soup, canonical_url: str, squad_id: str
) -> tuple[str | None, str | None]:
    """Use source-owned edition/competition evidence, never a referring league."""
    seasons, competitions = set(), set()
    for address in _page_addresses(soup, canonical_url):
        url = urlsplit(address)
        squad_path = _SQUAD_PATH.match(url.path)
        if (
            url.hostname not in (None, "fbref.com", "www.fbref.com")
            or not squad_path
            or squad_path[1] != squad_id
        ):
            raise ValueError("Response squad identity differs from requested target")
        tail = url.path[squad_path.end() :].split("/")
        if _YEAR.fullmatch(tail[0]):
            seasons.add(tail.pop(0))
        if tail and re.fullmatch(r"c\d+", tail[0]):
            competitions.add(tail[0][1:])
    heading = _text(
        next((h for h in soup.find_all("h1") if not _inside_global_chrome(h)), None)
    )
    season = re.match(r"^(\d{4}(?:-\d{4})?)(?:\s|$)", heading)
    if season:
        seasons.add(season[1])
    if len(seasons) > 1 or len(competitions) > 1:
        raise ValueError("Conflicting editions or competitions on squad page")
    return next(iter(seasons), None), next(iter(competitions), None)


def parse_squad_roster_html(
    html: str | bytes, *, canonical_url: str, squad_id: str
) -> tuple[DatasetParseResult, str | None]:
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    documents = _document_soups(html)
    try:
        season_id, competition_id = _squad_scope(documents[0], canonical_url, squad_id)
        tables = [
            table
            for document in documents
            for table in document.find_all("table")
            if not _inside_global_chrome(table)
        ]
        rosters = [t for t in tables if t.get("id") in {"roster", "roster_details"}]
        if not rosters:
            rosters = [
                t for t in tables if _SQUAD_STATS_ID.fullmatch(str(t.get("id") or ""))
            ]
        result = _table_result("squad_roster", rosters)
        if result.frame is not None:
            frame = result.frame
            row_seasons = frame["source_season_id"]
            editions = set(row_seasons.dropna())
            if season_id is not None:
                editions.add(season_id)
            elif editions and row_seasons.isna().any():
                # A snapshot replaces one whole edition. No page-level
                # evidence assigns the undated players to their neighbours.
                raise ValueError(
                    "Roster mixes dated and undated rows without a page season"
                )
            if len(editions) > 1:
                raise ValueError("Conflicting editions on squad page and roster rows")
            season_id = next(iter(editions), None)
            competitions = frame["source_table_id"].str.extract(
                r"^stats_standard_(\d+)$", expand=False
            )
            if competition_id is not None:
                if (competitions.dropna() != competition_id).any():
                    raise ValueError(
                        "Squad statistics table conflicts with page competition"
                    )
                # Combined tables aggregate multiple competitions even if a
                # competition-specific address advertises them.
                combined = frame["source_table_id"].str.fullmatch(
                    r"stats_standard_(?:all|combined)"
                )
                competitions = competitions.mask(
                    competitions.isna() & ~combined, competition_id
                )
            frame["source_competition_id"] = competitions
            frame["source_season_id"] = pd.Series(
                season_id, index=frame.index, dtype="string"
            )
            frame["squad_id"] = squad_id
        return result, season_id
    except (TypeError, ValueError) as exc:
        return DatasetParseResult(
            "squad_roster",
            DatasetStatus.ERROR,
            reason="squad_page_identity_invalid",
            error_type="SquadPageContractError",
            error_message=str(exc),
        ), None
    finally:
        for document in documents:
            document.decompose()
