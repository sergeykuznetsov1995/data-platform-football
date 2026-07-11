"""Pure, offline discovery parsers for stored FBref pages.

The parsers in this module only follow links present in the supplied HTML.
They never construct a competition, season, schedule, or match URL from a
hard-coded competition registry.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Comment, Tag

from scrapers.fbref.match_parser import DatasetStatus
from scrapers.fbref.raw_store import (
    canonicalize_fbref_url,
    match_page_target,
)


DISCOVERY_PARSER_VERSION = "fbref-discovery-parser-v1"

_HISTORY_PATH_RE = re.compile(
    r"^/en/comps/(?P<comp_id>[^/]+)/history(?:/|$)", re.IGNORECASE
)
_MATCH_PATH_RE = re.compile(
    r"/en/matches/(?P<match_id>[0-9a-f]{8})(?:/|$)", re.IGNORECASE
)
_SPLIT_YEAR_RE = re.compile(r"^\d{4}-\d{4}$")
_SINGLE_YEAR_RE = re.compile(r"^\d{4}$")


class CompetitionFormat(str, Enum):
    LEAGUE = "league"
    CUP = "cup"
    OTHER = "other"


class ParticipantType(str, Enum):
    CLUB = "club"
    NATIONAL_TEAM = "national_team"
    UNKNOWN = "unknown"


class CompetitionGender(str, Enum):
    MALE = "M"
    FEMALE = "F"
    UNKNOWN = "unknown"


class CalendarType(str, Enum):
    SPLIT_YEAR = "split_year"
    SINGLE_YEAR = "single_year"
    TOURNAMENT = "tournament"


# Short aliases are convenient for callers and keep the public vocabulary
# close to the field names used by the records.
Gender = CompetitionGender
Participants = ParticipantType


@dataclass(frozen=True)
class CompetitionRef:
    comp_id: str
    name: str
    format: CompetitionFormat
    participants: ParticipantType
    gender: CompetitionGender
    source_section: str
    country: Optional[str]
    governing_body: Optional[str]
    tier: Optional[str]
    first_season: Optional[str]
    last_season: Optional[str]
    history_url: str

    @property
    def competition_id(self) -> str:
        """Compatibility name used by discovery orchestration."""
        return self.comp_id


@dataclass(frozen=True)
class SeasonRef:
    comp_id: str
    season_id: str
    label: str
    calendar_type: CalendarType
    season_url: str

    @property
    def competition_id(self) -> str:
        return self.comp_id

    @property
    def season_label(self) -> str:
        return self.label


@dataclass(frozen=True)
class ScheduleRef:
    comp_id: str
    season_id: str
    schedule_url: str

    @property
    def competition_id(self) -> str:
        return self.comp_id


@dataclass(frozen=True)
class MatchRef:
    match_id: str
    comp_id: str
    season_id: str
    canonical_url: str

    @property
    def competition_id(self) -> str:
        return self.comp_id


@dataclass
class DiscoveryDatasetResult:
    dataset: str
    status: DatasetStatus
    records: List[Any] = field(default_factory=list)
    reason: Optional[str] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def row_count(self) -> int:
        return len(self.records)


@dataclass
class DiscoveryPageResult:
    parser_version: str
    parsed_at: str
    status: DatasetStatus
    datasets: Dict[str, DiscoveryDatasetResult]

    @property
    def has_errors(self) -> bool:
        return any(
            dataset.status == DatasetStatus.ERROR
            for dataset in self.datasets.values()
        )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dataset(
    name: str,
    records: Optional[Iterable[Any]] = None,
    *,
    status: Optional[DatasetStatus] = None,
    reason: Optional[str] = None,
    error_type: Optional[str] = None,
    error_message: Optional[str] = None,
) -> DiscoveryDatasetResult:
    materialized = list(records or [])
    if status is None:
        status = (
            DatasetStatus.AVAILABLE if materialized else DatasetStatus.EMPTY
        )
    return DiscoveryDatasetResult(
        dataset=name,
        status=status,
        records=materialized,
        reason=reason,
        error_type=error_type,
        error_message=error_message,
    )


def _page(
    datasets: Sequence[DiscoveryDatasetResult],
    *,
    parser_version: str,
) -> DiscoveryPageResult:
    mapped = {dataset.dataset: dataset for dataset in datasets}
    if any(item.status == DatasetStatus.ERROR for item in datasets):
        status = DatasetStatus.ERROR
    elif any(item.status == DatasetStatus.AVAILABLE for item in datasets):
        status = DatasetStatus.AVAILABLE
    else:
        status = DatasetStatus.EMPTY
    return DiscoveryPageResult(
        parser_version=parser_version,
        parsed_at=_now(),
        status=status,
        datasets=mapped,
    )


def _document_soups(html: str) -> List[BeautifulSoup]:
    """Return the DOM plus comment fragments that can contain FBref tables."""
    root = BeautifulSoup(html or "", "html.parser")
    documents = [root]
    for comment in root.find_all(string=lambda value: isinstance(value, Comment)):
        text = str(comment)
        if "<table" in text or "/en/comps/" in text:
            documents.append(BeautifulSoup(text, "html.parser"))
    return documents


def _text(value: Optional[Tag]) -> Optional[str]:
    if value is None:
        return None
    rendered = value.get_text(" ", strip=True)
    return rendered or None


def _cell_text(row: Tag, *names: str) -> Optional[str]:
    for name in names:
        cell = row.find(["th", "td"], attrs={"data-stat": name})
        rendered = _text(cell)
        if rendered is not None:
            return rendered
    return None


def _href_path(href: str) -> str:
    return urlparse(str(href).strip()).path


def _source_section(anchor: Tag) -> str:
    heading = anchor.find_previous(["h2", "h3"])
    if heading is not None:
        rendered = _text(heading)
        if rendered:
            return rendered
    table = anchor.find_parent("table")
    if table is not None:
        caption = table.find("caption")
        rendered = _text(caption)
        if rendered:
            return re.sub(r"\s+Table$", "", rendered, flags=re.IGNORECASE)
        table_id = str(table.get("id") or "").strip()
        if table_id:
            return table_id.replace("_", " ")
    return "Unknown"


def _classify_section(
    source_section: str,
) -> tuple[CompetitionFormat, ParticipantType]:
    normalized = re.sub(r"\s+", " ", source_section).strip().lower()
    if "league" in normalized:
        competition_format = CompetitionFormat.LEAGUE
    elif "cup" in normalized:
        competition_format = CompetitionFormat.CUP
    else:
        competition_format = CompetitionFormat.OTHER

    if "national team" in normalized:
        participants = ParticipantType.NATIONAL_TEAM
    elif any(
        marker in normalized
        for marker in ("club", "domestic", "league", "cup", "youth")
    ):
        participants = ParticipantType.CLUB
    else:
        participants = ParticipantType.UNKNOWN
    return competition_format, participants


def _parse_gender(value: Optional[str], source_section: str) -> CompetitionGender:
    normalized = (value or "").strip().lower()
    if normalized in {"m", "male", "men", "men's"}:
        return CompetitionGender.MALE
    if normalized in {"f", "female", "women", "women's"}:
        return CompetitionGender.FEMALE
    section = source_section.lower()
    if "women" in section or "female" in section:
        return CompetitionGender.FEMALE
    if re.search(r"\bmen(?:'s)?\b", section):
        return CompetitionGender.MALE
    return CompetitionGender.UNKNOWN


def _is_popular(source_section: str) -> bool:
    return "popular" in source_section.lower()


def _competition_from_anchor(anchor: Tag) -> CompetitionRef:
    href = str(anchor.get("href") or "")
    match = _HISTORY_PATH_RE.match(_href_path(href))
    if match is None:
        raise ValueError(f"Not a competition history link: {href!r}")
    row = anchor.find_parent("tr")
    source_section = _source_section(anchor)
    competition_format, participants = _classify_section(source_section)
    row = row if row is not None else anchor
    return CompetitionRef(
        comp_id=match.group("comp_id"),
        name=_text(anchor) or _cell_text(row, "comp_name", "comp") or "",
        format=competition_format,
        participants=participants,
        gender=_parse_gender(_cell_text(row, "gender"), source_section),
        source_section=source_section,
        country=_cell_text(row, "country"),
        governing_body=_cell_text(row, "governing_body", "governing"),
        tier=_cell_text(row, "tier"),
        first_season=_cell_text(row, "first_season", "minseason"),
        last_season=_cell_text(row, "last_season", "maxseason"),
        history_url=canonicalize_fbref_url(href),
    )


def parse_competition_index_html(
    html: str,
    *,
    parser_version: str = DISCOVERY_PARSER_VERSION,
) -> DiscoveryPageResult:
    """Parse every source-advertised competition history link from /en/comps/."""
    documents = _document_soups(html)
    competitions: Dict[str, CompetitionRef] = {}
    errors: List[str] = []

    for document in documents:
        for anchor in document.find_all("a", href=True):
            href = str(anchor.get("href") or "")
            if _HISTORY_PATH_RE.match(_href_path(href)) is None:
                continue
            try:
                candidate = _competition_from_anchor(anchor)
            except Exception as exc:
                errors.append(f"{href!r}: {type(exc).__name__}: {exc}")
                continue
            previous = competitions.get(candidate.comp_id)
            if previous is None or (
                _is_popular(previous.source_section)
                and not _is_popular(candidate.source_section)
            ):
                competitions[candidate.comp_id] = candidate

    records = list(competitions.values())
    if errors:
        result = _dataset(
            "competitions",
            records,
            status=DatasetStatus.ERROR,
            reason="competition_link_parse_failed",
            error_type="CompetitionDiscoveryError",
            error_message="; ".join(errors)[:1000],
        )
    elif not records:
        result = _dataset(
            "competitions",
            status=DatasetStatus.ERROR,
            reason="competition_history_links_missing",
            error_type="CompetitionIndexContractError",
            error_message="No /en/comps/{id}/history links were found",
        )
    else:
        result = _dataset("competitions", records)
    return _page([result], parser_version=parser_version)


def _find_tables(
    documents: Sequence[BeautifulSoup], predicate
) -> List[Tag]:
    tables: List[Tag] = []
    seen_markup: set[str] = set()
    for document in documents:
        for table in document.find_all("table"):
            table_id = str(table.get("id") or "")
            if not predicate(table_id):
                continue
            markup = str(table)
            if markup in seen_markup:
                continue
            seen_markup.add(markup)
            tables.append(table)
    return tables


def _season_link(row: Tag, comp_id: str) -> Optional[Tag]:
    preferred = row.find(
        ["th", "td"],
        attrs={
            "data-stat": lambda value: value
            in {"season", "season_id", "year", "year_id"}
        },
    )
    anchors = preferred.find_all("a", href=True) if preferred else []
    anchors.extend(row.find_all("a", href=True))
    prefix = f"/en/comps/{comp_id}/"
    for anchor in anchors:
        path = _href_path(str(anchor.get("href") or ""))
        if path.startswith(prefix) and "/history" not in path:
            return anchor
    return None


def _season_id_from_url(comp_id: str, season_url: str, label: str) -> str:
    parts = [part for part in urlparse(season_url).path.split("/") if part]
    try:
        comp_position = next(
            index
            for index, value in enumerate(parts)
            if value == comp_id and parts[max(0, index - 1)] == "comps"
        )
    except StopIteration:
        return label
    remaining = parts[comp_position + 1 :]
    if not remaining:
        return label
    candidate = remaining[0]
    route_names = {"history", "schedule", "stats"}
    if candidate.lower() in route_names or candidate.lower().endswith(
        ("-stats", "-seasons", "-scores-and-fixtures")
    ):
        return label
    return candidate


def _calendar_type(
    competition: CompetitionRef, label: str
) -> CalendarType:
    format_value = getattr(competition.format, "value", competition.format)
    participants_value = getattr(
        competition.participants, "value", competition.participants
    )
    if (
        format_value == CompetitionFormat.CUP.value
        or participants_value == ParticipantType.NATIONAL_TEAM.value
    ):
        return CalendarType.TOURNAMENT
    if _SPLIT_YEAR_RE.fullmatch(label):
        return CalendarType.SPLIT_YEAR
    if _SINGLE_YEAR_RE.fullmatch(label):
        return CalendarType.SINGLE_YEAR
    raise ValueError(f"Unsupported season label {label!r}")


def parse_competition_html(
    html: str,
    competition: CompetitionRef,
    *,
    parser_version: str = DISCOVERY_PARSER_VERSION,
) -> DiscoveryPageResult:
    """Parse exact season links from one competition's history table."""
    documents = _document_soups(html)
    tables = _find_tables(documents, lambda table_id: table_id == "seasons")
    if not tables:
        result = _dataset(
            "seasons",
            status=DatasetStatus.ERROR,
            reason="season_history_table_missing",
            error_type="CompetitionPageContractError",
            error_message="Expected table#seasons",
        )
        return _page([result], parser_version=parser_version)

    seasons: Dict[str, SeasonRef] = {}
    errors: List[str] = []
    for row in tables[0].find_all("tr"):
        anchor = _season_link(row, competition.comp_id)
        if anchor is None:
            continue
        href = str(anchor.get("href") or "")
        label = (
            _cell_text(row, "season", "season_id", "year", "year_id")
            or _text(anchor)
            or ""
        )
        if not label:
            errors.append(f"{href!r}: empty season label")
            continue
        try:
            season_url = canonicalize_fbref_url(href)
            season_id = _season_id_from_url(
                competition.comp_id, season_url, label
            )
            calendar_type = _calendar_type(competition, label)
        except Exception as exc:
            errors.append(f"{label!r}: {type(exc).__name__}: {exc}")
            continue
        seasons.setdefault(
            season_id,
            SeasonRef(
                comp_id=competition.comp_id,
                season_id=season_id,
                label=label,
                calendar_type=calendar_type,
                season_url=season_url,
            ),
        )

    records = list(seasons.values())
    if errors:
        result = _dataset(
            "seasons",
            records,
            status=DatasetStatus.ERROR,
            reason="season_row_parse_failed",
            error_type="SeasonDiscoveryError",
            error_message="; ".join(errors)[:1000],
        )
    elif not records:
        result = _dataset(
            "seasons",
            status=DatasetStatus.ERROR,
            reason="season_links_missing",
            error_type="CompetitionPageContractError",
            error_message="table#seasons has no season links",
        )
    else:
        result = _dataset("seasons", records)
    return _page([result], parser_version=parser_version)


def parse_season_html(
    html: str,
    season: SeasonRef,
    *,
    parser_version: str = DISCOVERY_PARSER_VERSION,
) -> DiscoveryPageResult:
    """Find the exact Scores & Fixtures link advertised by a season page."""
    documents = _document_soups(html)
    schedule: Optional[ScheduleRef] = None
    errors: List[str] = []
    expected_prefix = f"/en/comps/{season.comp_id}/"

    for document in documents:
        for anchor in document.find_all("a", href=True):
            href = str(anchor.get("href") or "")
            path = _href_path(href)
            components = [part.lower() for part in path.split("/") if part]
            if not path.startswith(expected_prefix) or "schedule" not in components:
                continue
            try:
                schedule_url = canonicalize_fbref_url(href)
            except Exception as exc:
                errors.append(f"{href!r}: {type(exc).__name__}: {exc}")
                continue
            schedule = ScheduleRef(
                comp_id=season.comp_id,
                season_id=season.season_id,
                schedule_url=schedule_url,
            )
            break
        if schedule is not None:
            break

    if schedule is not None and not errors:
        result = _dataset("schedules", [schedule])
    elif schedule is not None:
        result = _dataset(
            "schedules",
            [schedule],
            status=DatasetStatus.ERROR,
            reason="schedule_link_parse_failed",
            error_type="ScheduleDiscoveryError",
            error_message="; ".join(errors)[:1000],
        )
    else:
        result = _dataset(
            "schedules",
            status=DatasetStatus.ERROR,
            reason="schedule_link_missing",
            error_type="SeasonPageContractError",
            error_message="No source-provided /schedule/ link was found",
        )
    return _page([result], parser_version=parser_version)


def _schedule_tables(documents: Sequence[BeautifulSoup]) -> List[Tag]:
    preferred = _find_tables(
        documents, lambda table_id: table_id.lower() == "sched_all"
    )
    if preferred:
        return preferred[:1]
    return _find_tables(
        documents, lambda table_id: table_id.lower().startswith("sched")
    )


def _column_names(table: Tag) -> List[str]:
    thead = table.find("thead")
    header_rows = (thead or table).find_all("tr")
    for row in reversed(header_rows):
        cells = row.find_all("th", recursive=False)
        # A body row often starts with one scope=row <th>; it is not a header
        # definition when a malformed page has no explicit <thead>.
        if not cells or (thead is None and row.find("td", recursive=False)):
            continue
        names = []
        for index, cell in enumerate(cells):
            name = str(cell.get("data-stat") or "").strip()
            if not name:
                name = re.sub(
                    r"[^a-z0-9]+", "_", (_text(cell) or "").lower()
                ).strip("_")
            names.append(name or f"column_{index}")
        return names
    return []


def _schedule_row_record(
    row: Tag,
    *,
    headers: Sequence[str],
    table_id: str,
    source_row_index: int,
    season: SeasonRef,
) -> Optional[Dict[str, Any]]:
    if "thead" in set(row.get("class") or []):
        return None
    cells = row.find_all(["th", "td"], recursive=False)
    if not cells or not any((_text(cell) or "") for cell in cells):
        return None

    record: Dict[str, Any] = {
        "comp_id": season.comp_id,
        "competition_id": season.comp_id,
        "season_id": season.season_id,
        "table_id": table_id,
        "source_row_index": source_row_index,
    }
    for index, cell in enumerate(cells):
        key = str(cell.get("data-stat") or "").strip()
        if not key:
            key = headers[index] if index < len(headers) else f"column_{index}"
        if key in record:
            key = f"{key}_{index}"
        record[key] = _text(cell) or ""

    match_href: Optional[str] = None
    for anchor in row.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        if _MATCH_PATH_RE.search(_href_path(href)):
            match_href = href
            break
    record["match_url"] = (
        match_page_target(match_href).canonical_url if match_href else None
    )
    return record


def parse_schedule_html(
    html: str,
    season: SeasonRef,
    *,
    parser_version: str = DISCOVERY_PARSER_VERSION,
) -> DiscoveryPageResult:
    """Parse schedule rows and deduplicated match targets without network I/O."""
    documents = _document_soups(html)
    tables = _schedule_tables(documents)
    if not tables:
        rows_result = _dataset(
            "schedule_rows",
            status=DatasetStatus.ERROR,
            reason="schedule_tables_missing",
            error_type="SchedulePageContractError",
            error_message="No table whose id starts with 'sched' was found",
        )
        matches_result = _dataset(
            "matches",
            status=DatasetStatus.NOT_APPLICABLE,
            reason="schedule_tables_missing",
        )
        return _page(
            [rows_result, matches_result], parser_version=parser_version
        )

    records: List[Dict[str, Any]] = []
    matches: Dict[str, MatchRef] = {}
    errors: List[str] = []
    for table in tables:
        table_id = str(table.get("id") or "")
        headers = _column_names(table)
        body = table.find("tbody")
        rows = (body or table).find_all("tr", recursive=False)
        for source_row_index, row in enumerate(rows):
            try:
                record = _schedule_row_record(
                    row,
                    headers=headers,
                    table_id=table_id,
                    source_row_index=source_row_index,
                    season=season,
                )
            except Exception as exc:
                errors.append(
                    f"{table_id}[{source_row_index}]: "
                    f"{type(exc).__name__}: {exc}"
                )
                continue
            if record is None:
                continue
            records.append(record)
            match_url = record.get("match_url")
            if not match_url:
                continue
            target = match_page_target(str(match_url))
            match_id = target.source_ids["match_id"]
            matches.setdefault(
                match_id,
                MatchRef(
                    match_id=match_id,
                    comp_id=season.comp_id,
                    season_id=season.season_id,
                    canonical_url=target.canonical_url,
                ),
            )

    if errors:
        rows_result = _dataset(
            "schedule_rows",
            records,
            status=DatasetStatus.ERROR,
            reason="schedule_row_parse_failed",
            error_type="ScheduleDiscoveryError",
            error_message="; ".join(errors)[:1000],
        )
    else:
        rows_result = _dataset(
            "schedule_rows",
            records,
            reason=None if records else "schedule_tables_have_no_rows",
        )
    matches_result = _dataset(
        "matches",
        matches.values(),
        reason=None if matches else "no_match_report_links",
    )
    return _page(
        [rows_result, matches_result], parser_version=parser_version
    )


__all__ = [
    "DISCOVERY_PARSER_VERSION",
    "CalendarType",
    "CompetitionFormat",
    "CompetitionGender",
    "CompetitionRef",
    "DiscoveryDatasetResult",
    "DiscoveryPageResult",
    "Gender",
    "MatchRef",
    "ParticipantType",
    "Participants",
    "ScheduleRef",
    "SeasonRef",
    "parse_competition_html",
    "parse_competition_index_html",
    "parse_schedule_html",
    "parse_season_html",
]
