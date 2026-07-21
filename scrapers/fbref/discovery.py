"""Pure, offline discovery parsers for stored FBref pages.

The parsers in this module only follow links present in the supplied HTML.
They never construct a competition, season, schedule, or match URL from a
hard-coded competition registry.
"""

from __future__ import annotations

import re
import hashlib
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Comment, Tag

from scrapers.fbref.constants import UNAVAILABLE_SEASON_STAT_ROUTES
from scrapers.fbref.match_parser import DatasetStatus
from scrapers.fbref.raw_store import (
    canonicalize_fbref_url,
    match_page_target,
)


from scrapers.fbref.policy import DISCOVERY_PARSER_VERSION  # canonical definition

_PAGE_SOURCE_ID_KEYS = {
    "competition": ("competition_id",),
    "season": ("competition_id", "season_id"),
    "season_stats": ("competition_id", "season_id", "stat_route"),
    "schedule": ("competition_id", "season_id"),
    "standings": ("competition_id", "season_id"),
    "squad": ("squad_id", "squad_discriminator"),
    "player": ("player_id",),
    "matchlog": (
        "player_id", "matchlog_season_id", "matchlog_discriminator"
    ),
    "match": ("competition_id", "season_id", "match_id"),
}


def normalize_page_source_ids(
    page_kind: str, source_ids: Mapping[str, object]
) -> Dict[str, str]:
    """Keep only source IDs that belong to this page identity."""

    allowed = _PAGE_SOURCE_ID_KEYS.get(str(page_kind), ())
    return {
        key: str(source_ids[key])
        for key in allowed
        if source_ids.get(key) is not None and str(source_ids[key]).strip()
    }

_HISTORY_PATH_RE = re.compile(
    r"^/en/comps/(?P<comp_id>[^/]+)/history(?:/|$)", re.IGNORECASE
)
_MATCH_PATH_RE = re.compile(
    r"/en/matches/(?P<match_id>[0-9a-f]{8})(?:/|$)", re.IGNORECASE
)
_SPLIT_YEAR_RE = re.compile(r"^\d{4}-\d{4}$")
_SINGLE_YEAR_RE = re.compile(r"^\d{4}$")
_SEASON_STAT_ROUTES = {
    "standard",
    "shooting",
    "playingtime",
    "misc",
    "keepers",
}
# FBref serves per-player *standard* season stats under the on-page URL segment
# ``stats`` (e.g. /en/comps/9/stats/Premier-League-Stats), while its display
# label is "standard".  Map the segment to the canonical stat_route so the page
# is minted as season_stats/standard instead of being mistaken for an opaque
# season id — which quarantined it and left player standard stats uncollected
# (see #949).
_STAT_ROUTE_URL_SEGMENTS = {"stats": "standard"}
_COMPETITION_ID_RE = re.compile(r"^\d+$")
# Route segments FBref places directly under a competition; a segment in this
# position is therefore a sub-page, never a season id.
_COMP_SUBPAGE_ROUTES = (
    {"history", "schedule", "standings"}
    | _SEASON_STAT_ROUTES
    | set(_STAT_ROUTE_URL_SEGMENTS)
    | set(UNAVAILABLE_SEASON_STAT_ROUTES)
)


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


class CompetitionEligibility(str, Enum):
    """Decision made before any competition child target is created."""

    ELIGIBLE = "eligible"
    SKIPPED_FEMALE = "skipped_female"
    QUARANTINED_UNKNOWN = "quarantined_unknown_gender"


class CalendarType(str, Enum):
    SPLIT_YEAR = "split_year"
    SINGLE_YEAR = "single_year"
    TOURNAMENT = "tournament"
    OPAQUE = "opaque"


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


@dataclass(frozen=True)
class DiscoveredPageLink:
    """One canonical, source-advertised link found during offline parsing."""

    page_kind: str
    canonical_url: str
    source_ids: Mapping[str, str]


def competition_eligibility(
    competition: CompetitionRef,
) -> CompetitionEligibility:
    """Classify scope without following a female or unknown-gender URL."""

    if competition.gender == CompetitionGender.MALE:
        return CompetitionEligibility.ELIGIBLE
    if competition.gender == CompetitionGender.FEMALE:
        return CompetitionEligibility.SKIPPED_FEMALE
    return CompetitionEligibility.QUARANTINED_UNKNOWN


def partition_competitions(
    competitions: Iterable[CompetitionRef],
) -> Dict[CompetitionEligibility, List[CompetitionRef]]:
    output = {status: [] for status in CompetitionEligibility}
    for competition in competitions:
        output[competition_eligibility(competition)].append(competition)
    return output


def _normalized_name(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value or "")
    ascii_text = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", ascii_text.casefold()).strip()


def sentinel_coverage(
    competitions: Iterable[CompetitionRef],
    sentinel_names: Iterable[str],
) -> Dict[str, dict]:
    """Report sentinels found in source data; never use them to seed scope."""

    materialized = list(competitions)
    by_name = {_normalized_name(item.name): item for item in materialized}
    report: Dict[str, dict] = {}
    for requested in sentinel_names:
        normalized_requested = _normalized_name(requested)
        found = by_name.get(normalized_requested)
        if found is None:
            requested_tokens = set(normalized_requested.split())
            candidates = [
                item
                for item in materialized
                if requested_tokens.issubset(
                    set(_normalized_name(item.name).split())
                )
            ]
            if candidates:
                found = min(
                    candidates,
                    key=lambda item: (
                        len(_normalized_name(item.name).split()),
                        _normalized_name(item.name),
                    ),
                )
        report[str(requested)] = {
            "published": found is not None,
            "competition_id": found.competition_id if found else None,
            "gender": found.gender.value if found else None,
            "eligibility": (
                competition_eligibility(found).value if found else None
            ),
        }
    return report


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


_GLOBAL_CHROME_IDS = frozenset({"header", "nav", "footer", "sidebar"})
_GLOBAL_CHROME_ROLES = frozenset({"banner", "navigation", "contentinfo"})
_GLOBAL_CHROME_CLASSES = frozenset({"sidebar", "side-bar"})


def _inside_global_chrome(node: object) -> bool:
    """Recognize both semantic tags and FBref's production div wrappers."""

    lineage = [node, *getattr(node, "parents", ())]
    for ancestor in lineage:
        if not isinstance(ancestor, Tag):
            continue
        if ancestor.name in {"aside", "header", "footer", "nav"}:
            return True
        ancestor_id = str(ancestor.get("id") or "").strip().casefold()
        role = str(ancestor.get("role") or "").strip().casefold()
        classes = {
            str(value).strip().casefold()
            for value in ancestor.get("class", ())
            if str(value).strip()
        }
        if (
            ancestor_id in _GLOBAL_CHROME_IDS
            or role in _GLOBAL_CHROME_ROLES
            or classes.intersection(_GLOBAL_CHROME_CLASSES)
        ):
            return True
    return False


def _document_soups(html: str) -> List[BeautifulSoup]:
    """Return the DOM plus comment fragments that can contain FBref tables."""
    root = BeautifulSoup(html or "", "html.parser")
    documents = [root]
    for comment in root.find_all(string=lambda value: isinstance(value, Comment)):
        if _inside_global_chrome(comment):
            continue
        text = str(comment)
        if "<table" in text or "href=" in text or "/en/" in text:
            fragment = BeautifulSoup(text, "html.parser")
            # Comments are detached from their source ancestry when parsed.
            # Mark the fragment as content so discovery can retain FBref's
            # comment-hidden tables/links without reopening root-page chrome.
            content = fragment.new_tag("main")
            for child in list(fragment.contents):
                content.append(child.extract())
            fragment.append(content)
            documents.append(fragment)
    return documents


def _has_page_owned_table(documents: Sequence[BeautifulSoup]) -> bool:
    """Return whether source content, rather than global chrome, has a table."""

    return any(
        not _inside_global_chrome(table)
        for document in documents
        for table in document.find_all("table")
    )


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


def has_valid_zero_table_season_signature(
    html: str,
    *,
    competition_id: str,
    season_id: str,
    season_label: Optional[str] = None,
    canonical_url: Optional[str] = None,
) -> bool:
    """Prove that a table-free response is a real FBref season page.

    Some source-owned single-match editions legitimately publish no tables.
    A generic HTML/error shell has the same coarse shape, so accepting it
    requires two independent positive signals: the expected season identity
    in the page heading or canonical URL, and a source-content backlink to the
    expected competition history.  Navigation/sidebar comments never count.
    """

    documents = _document_soups(html)
    if _has_page_owned_table(documents):
        return False

    expected_competition_id = str(competition_id).strip()
    expected_season_id = str(season_id).strip()
    expected_history = (
        f"/en/comps/{expected_competition_id}/history"
    ).casefold()
    history_paths = (
        _href_path(str(anchor.get("href") or "")).rstrip("/").casefold()
        for document in documents
        for anchor in _discovery_anchors(document)
    )
    has_history_backlink = any(
        path == expected_history or path.startswith(expected_history + "/")
        for path in history_paths
    )
    if not has_history_backlink:
        return False

    identity_values = " ".join(
        value
        for value in (expected_season_id, str(season_label or "").strip())
        if value
    )
    identity_years = set(re.findall(r"(?<!\d)(?:19|20)\d{2}(?!\d)", identity_values))
    heading_identity = False
    for document in documents:
        for heading in document.find_all("h1"):
            if _inside_global_chrome(heading):
                continue
            rendered = heading.get_text(" ", strip=True)
            rendered_years = set(
                re.findall(r"(?<!\d)(?:19|20)\d{2}(?!\d)", rendered)
            )
            if identity_years.intersection(rendered_years):
                heading_identity = True
                break
            if (
                not identity_years
                and expected_season_id.casefold() in rendered.casefold()
            ):
                heading_identity = True
                break
        if heading_identity:
            break

    expected_canonical_path = (
        _href_path(canonical_url).rstrip("/").casefold()
        if canonical_url
        else ""
    )
    canonical_identity = False
    for document in documents:
        for link in document.find_all("link", href=True):
            relation = link.get("rel") or ()
            if isinstance(relation, str):
                relation = relation.split()
            if "canonical" not in {
                str(value).casefold() for value in relation
            }:
                continue
            candidate_path = _href_path(str(link.get("href") or ""))
            normalized_candidate = candidate_path.rstrip("/").casefold()
            if expected_canonical_path and (
                normalized_candidate == expected_canonical_path
            ):
                canonical_identity = True
                break
            route = [part for part in candidate_path.split("/") if part]
            if (
                len(route) >= 4
                and route[:2] == ["en", "comps"]
                and route[2].casefold() == expected_competition_id.casefold()
                and route[3].casefold() == expected_season_id.casefold()
            ):
                canonical_identity = True
                break
        if canonical_identity:
            break

    return heading_identity or canonical_identity


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
        for anchor in _discovery_anchors(document):
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
            if _inside_global_chrome(table):
                continue
            table_id = str(table.get("id") or "")
            if not predicate(table_id):
                continue
            markup = str(table)
            if markup in seen_markup:
                continue
            seen_markup.add(markup)
            tables.append(table)
    return tables


def _season_cell(row: Tag) -> Optional[Tag]:
    return row.find(
        ["th", "td"],
        attrs={
            "data-stat": lambda value: value
            in {"season", "season_id", "year", "year_id"}
        },
    )


def _season_link(row: Tag, comp_id: str) -> Optional[Tag]:
    season_cell = _season_cell(row)
    if season_cell is None:
        return None
    prefix = f"/en/comps/{comp_id}/"
    for anchor in season_cell.find_all("a", href=True):
        path = _href_path(str(anchor.get("href") or ""))
        if path.startswith(prefix) and "/history" not in path:
            return anchor
    return None


def _direct_match_links(row: Tag) -> List[Tag]:
    """Return source-advertised match reports from one edition row.

    A valid edition row must carry a season/year cell.  Requiring that cell
    prevents unrelated match links in table chrome from being assigned the
    competition's season identity.
    """

    season_cell = _season_cell(row)
    if season_cell is None or _text(season_cell) is None:
        return []
    return [
        anchor
        for anchor in season_cell.find_all("a", href=True)
        if _MATCH_PATH_RE.search(
            _href_path(str(anchor.get("href") or ""))
        )
    ]


def _season_cards(documents: Sequence[BeautifulSoup], comp_id: str) -> List[Tag]:
    """Season links from the card layout used when there is no seasons table.

    A competition whose editions are standalone tournaments (FBref comp 255,
    the World Cup inter-confederation play-offs) publishes its history as cards
    in ``div.content_grid`` and carries no ``table#seasons`` at all.  The grid
    also links to other competitions, squads and players, so only routes that
    address a season *of this competition* are taken.
    """
    anchors: List[Tag] = []
    prefix = f"/en/comps/{comp_id}/"
    for document in documents:
        for grid in document.find_all("div", class_="content_grid"):
            for anchor in grid.find_all("a", href=True):
                if _inside_global_chrome(anchor):
                    continue
                path = _href_path(str(anchor.get("href") or ""))
                if not path.startswith(prefix):
                    continue
                route = [part for part in path.split("/") if part][1:]
                if _has_season_component(route):
                    anchors.append(anchor)
    return anchors


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
    # The source URL/ID is authoritative.  An unfamiliar display label must
    # never make discovery invent a year pair or discard the season.
    return CalendarType.OPAQUE


def parse_competition_html(
    html: str,
    competition: CompetitionRef,
    *,
    parser_version: str = DISCOVERY_PARSER_VERSION,
) -> DiscoveryPageResult:
    """Parse exact season and direct-match links from a competition history."""
    documents = _document_soups(html)
    tables = _find_tables(documents, lambda table_id: table_id == "seasons")
    if tables:
        body = tables[0].find("tbody")
        rows = [
            row
            for row in (body or tables[0]).find_all("tr", recursive=False)
            if "thead" not in set(row.get("class") or [])
            and row.find(["th", "td"], recursive=False) is not None
            and _text(row)
        ]
        row_contract_errors = [
            f"row[{index}]: no recognized season/year cell"
            for index, row in enumerate(rows)
            if _season_cell(row) is None or _text(_season_cell(row)) is None
        ]
        rows = [
            row
            for row in rows
            if _season_cell(row) is not None and _text(_season_cell(row))
        ]
        season_candidates = [
            (row, _season_link(row, competition.comp_id))
            for row in rows
        ]
        direct_match_candidates = [
            (row, anchor)
            for row in rows
            for anchor in _direct_match_links(row)
        ]
    else:
        row_contract_errors = []
        season_candidates = [
            (None, anchor)
            for anchor in _season_cards(documents, competition.comp_id)
        ]
        direct_match_candidates = []
    if not season_candidates and not tables:
        seasons_result = _dataset(
            "seasons",
            status=DatasetStatus.ERROR,
            reason="season_history_table_missing",
            error_type="CompetitionPageContractError",
            error_message="Expected table#seasons or a season card grid",
        )
        matches_result = _dataset(
            "matches",
            reason="no_direct_match_links",
        )
        return _page(
            [seasons_result, matches_result], parser_version=parser_version
        )

    seasons: Dict[str, SeasonRef] = {}
    season_ids_by_row: Dict[int, str] = {}
    season_errors: List[str] = row_contract_errors + [
        f"{_cell_text(row, 'season', 'season_id', 'year', 'year_id')!r}: "
        "season cell has neither a season link nor a direct match link"
        for row, anchor in season_candidates
        if anchor is None and not _direct_match_links(row)
    ]
    season_candidates = [
        (row, anchor)
        for row, anchor in season_candidates
        if anchor is not None
    ]
    for row, anchor in season_candidates:
        href = str(anchor.get("href") or "")
        label = (
            (_cell_text(row, "season", "season_id", "year", "year_id")
             if row is not None else None)
            or _text(anchor)
            or ""
        )
        if not label:
            season_errors.append(f"{href!r}: empty season label")
            continue
        try:
            season_url = canonicalize_fbref_url(href)
            season_id = _season_id_from_url(
                competition.comp_id, season_url, label
            )
            calendar_type = _calendar_type(competition, label)
        except Exception as exc:
            season_errors.append(
                f"{label!r}: {type(exc).__name__}: {exc}"
            )
            continue
        if row is not None:
            season_ids_by_row[id(row)] = season_id
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

    direct_matches: Dict[str, MatchRef] = {}
    match_errors: List[str] = []
    for row, anchor in direct_match_candidates:
        href = str(anchor.get("href") or "")
        label = _cell_text(row, "season", "season_id", "year", "year_id")
        season_id = season_ids_by_row.get(id(row)) or label
        if not season_id:
            match_errors.append(f"{href!r}: empty season label")
            continue
        try:
            target = match_page_target(href)
            match_id = target.source_ids["match_id"]
        except Exception as exc:
            match_errors.append(
                f"{href!r}: {type(exc).__name__}: {exc}"
            )
            continue
        direct_matches.setdefault(
            match_id,
            MatchRef(
                match_id=match_id,
                comp_id=competition.comp_id,
                season_id=season_id,
                canonical_url=target.canonical_url,
            ),
        )

    season_records = list(seasons.values())
    match_records = list(direct_matches.values())
    if season_errors:
        completely_unlinked = not season_records and not match_records
        seasons_result = _dataset(
            "seasons",
            season_records,
            status=DatasetStatus.ERROR,
            reason=(
                "season_links_missing"
                if completely_unlinked
                else "season_row_parse_failed"
            ),
            error_type=(
                "CompetitionPageContractError"
                if completely_unlinked
                else "SeasonDiscoveryError"
            ),
            error_message="; ".join(season_errors)[:1000],
        )
    elif not season_records and match_records:
        # A one-match competition (the FA Community Shield, the super cups) has
        # no season pages at all: every row of its history table links straight
        # to the final's match report. The page is intact and fully populated —
        # a season page simply does not exist to follow — so the wave must not
        # fail on it. Rows carrying no links at all stay a contract error below.
        seasons_result = _dataset(
            "seasons",
            status=DatasetStatus.NOT_APPLICABLE,
            reason="competition_publishes_no_season_pages",
        )
    elif not season_records:
        seasons_result = _dataset(
            "seasons",
            status=DatasetStatus.ERROR,
            reason="season_links_missing",
            error_type="CompetitionPageContractError",
            error_message="table#seasons has no season links",
        )
    else:
        seasons_result = _dataset("seasons", season_records)

    if match_errors:
        matches_result = _dataset(
            "matches",
            match_records,
            status=DatasetStatus.ERROR,
            reason="direct_match_link_parse_failed",
            error_type="MatchDiscoveryError",
            error_message="; ".join(match_errors)[:1000],
        )
    else:
        matches_result = _dataset(
            "matches",
            match_records,
            reason=None if match_records else "no_direct_match_links",
        )
    return _page(
        [seasons_result, matches_result], parser_version=parser_version
    )


def parse_season_html(
    html: str,
    season: SeasonRef,
    *,
    parser_version: str = DISCOVERY_PARSER_VERSION,
) -> DiscoveryPageResult:
    """Find the exact Scores & Fixtures link advertised by a season page."""
    documents = _document_soups(html)
    has_page_owned_table = _has_page_owned_table(documents)
    schedule: Optional[ScheduleRef] = None
    errors: List[str] = []
    expected_prefix = f"/en/comps/{season.comp_id}/"

    for document in documents:
        for anchor in _discovery_anchors(document):
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
    elif not has_page_owned_table and (
        has_valid_zero_table_season_signature(
            html,
            competition_id=season.comp_id,
            season_id=season.season_id,
            season_label=season.label,
            canonical_url=season.season_url,
        )
    ):
        # A single-match edition (the UEFA Super Cup) publishes a season page
        # with no fixtures and no tables at all: there is nothing to schedule.
        # A season page that does carry tables but advertises no schedule is a
        # broken contract and still fails below.
        result = _dataset(
            "schedules",
            status=DatasetStatus.NOT_APPLICABLE,
            reason="season_publishes_no_schedule",
        )
    elif not has_page_owned_table:
        result = _dataset(
            "schedules",
            status=DatasetStatus.ERROR,
            reason="zero_table_season_identity_missing",
            error_type="SeasonPageContractError",
            error_message=(
                "A table-free season page must prove its source identity with "
                "the expected heading/canonical URL and competition history "
                "backlink"
            ),
        )
    elif not _COMPETITION_ID_RE.match(season.comp_id):
        # FBref addresses its aggregate views by name rather than by id (comp
        # "Big5", the Big 5 European Leagues Combined — the only non-numeric id
        # it publishes). An aggregate has stat tables but plays no fixtures of
        # its own: its matches belong to the five leagues it sums up, which are
        # crawled in their own right.
        result = _dataset(
            "schedules",
            status=DatasetStatus.NOT_APPLICABLE,
            reason="aggregate_competition_has_no_fixtures",
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


def _has_season_component(route: Sequence[str]) -> bool:
    """True when a comps route carries an explicit season segment.

    The season segment is source-owned and may be opaque (``edition-42``), so
    it is recognised structurally rather than by shape: it sits at ``route[2]``
    ahead of the page slug, and a known sub-page route in that position means
    the URL addresses the current season instead.
    """
    return len(route) >= 4 and route[2].casefold() not in _COMP_SUBPAGE_ROUTES


def _current_season_competition(url: Optional[str]) -> Optional[str]:
    """Return the competition whose current season this page *is*, if any.

    FBref addresses a competition's current season without a season component
    (``/en/comps/9/Premier-League-Stats``), so only such a page may lend its
    season identity to the equally season-less links it carries.
    """
    if not url:
        return None
    route = [part for part in urlparse(str(url)).path.split("/") if part][1:]
    if len(route) < 3 or route[0] != "comps":
        return None
    if not _COMPETITION_ID_RE.fullmatch(route[1]) or "history" in route:
        return None
    return None if _has_season_component(route) else route[1]


def _discovery_anchors(document: BeautifulSoup) -> List[Tag]:
    """Return links from page-owned content, excluding global navigation.

    Production FBref pages keep their source data below ``main``/``#content``
    or in tables (including comment-unwrapped tables). Header, footer,
    navigation and other root-page chrome are never source-owned discovery
    evidence. Bare stored/test fragments retain their root links because they
    have no full-document ``html``/``body`` chrome to confuse with content.
    """

    contexts: List[Tag] = []
    contexts.extend(document.find_all("main"))
    contexts.extend(document.find_all(id="content"))
    contexts.extend(document.find_all("table"))
    scoped: List[Tag] = []
    seen: set[int] = set()
    if contexts:
        anchor_groups = (
            context.find_all("a", href=True) for context in contexts
        )
    elif document.find(["html", "body"]) is None:
        anchor_groups = (document.find_all("a", href=True),)
    else:
        anchor_groups = ()
    for anchors in anchor_groups:
        for anchor in anchors:
            key = id(anchor)
            if key in seen:
                continue
            seen.add(key)
            scoped.append(anchor)
    return [anchor for anchor in scoped if not _inside_global_chrome(anchor)]


def discover_page_links(
    html: str,
    *,
    parent_source_ids: Optional[Mapping[str, str]] = None,
    parent_url: Optional[str] = None,
) -> List[DiscoveredPageLink]:
    """Inventory supported FBref page links without constructing any URL.

    The result is deliberately broader than the typed discovery graph: it is
    used by the durable frontier to enqueue squad/profile/standings pages while
    the existing typed parsers continue to expose their compatibility records.
    """

    inherited = dict(parent_source_ids or {})
    current_season_parent = _current_season_competition(parent_url)
    found: Dict[tuple[str, str], DiscoveredPageLink] = {}
    for document in _document_soups(html):
        for anchor in _discovery_anchors(document):
            href = str(anchor.get("href") or "").strip()
            if not href:
                continue
            try:
                canonical = canonicalize_fbref_url(href)
            except ValueError:
                continue
            parts = [part for part in urlparse(canonical).path.split("/") if part]
            if not parts or parts[0] != "en":
                continue
            route = parts[1:]
            source_ids = dict(inherited)
            page_kind: Optional[str] = None

            if len(route) >= 2 and route[0] == "matches":
                try:
                    target = match_page_target(canonical)
                except ValueError:
                    continue
                page_kind = "match"
                canonical = target.canonical_url
                source_ids = dict(target.source_ids)
            elif len(route) >= 2 and route[0] == "squads":
                page_kind = "squad"
                source_ids["squad_id"] = route[1]
                source_ids["squad_discriminator"] = hashlib.sha256(
                    canonical.encode("utf-8")
                ).hexdigest()[:20]
            elif len(route) >= 2 and route[0] == "players":
                source_ids["player_id"] = route[1]
                if "matchlogs" in route:
                    matchlogs_index = route.index("matchlogs")
                    suffix = route[matchlogs_index + 1 :]
                    # A navigation root such as ``.../matchlogs/`` is not a
                    # paid page identity.  Require source route components
                    # plus a display slug, then collapse duplicate/nav links
                    # onto the source-owned structural discriminator.
                    if len(suffix) < 3:
                        continue
                    discriminator = "/".join(suffix[:-1]).strip("/")
                    if not discriminator:
                        continue
                    source_ids = {
                        "player_id": route[1],
                        "matchlog_season_id": suffix[0],
                        "matchlog_discriminator": discriminator,
                    }
                    page_kind = "matchlog"
                else:
                    page_kind = "player"
            elif len(route) >= 2 and route[0] == "comps":
                if not _COMPETITION_ID_RE.fullmatch(route[1]):
                    # e.g. the /en/comps/season/<year> navigation index, which
                    # is not a competition page at all.
                    continue
                source_ids["competition_id"] = route[1]
                if "history" in route:
                    page_kind = "competition"
                elif len(route) >= 3:
                    if _has_season_component(route):
                        season_id = route[2]
                        sub_route = route[3] if len(route) >= 4 else None
                    else:
                        # A season-less comps link addresses the competition's
                        # current season, whose id no page states.  Only a
                        # current-season page of the same competition may lend
                        # it: inheriting a historical parent's season would
                        # mint a target whose canonical URL already belongs to
                        # the registry-seeded current-season target.
                        if current_season_parent != route[1]:
                            continue
                        season_id = str(inherited.get("season_id") or "").strip()
                        if not season_id:
                            continue
                        sub_route = route[2]
                    source_ids["season_id"] = season_id
                    sub_route = (sub_route or "").casefold()
                    # FBref's on-page segment for standard stats is ``stats``;
                    # normalise it to the canonical stat_route label so the
                    # page mints as season_stats/standard.
                    stat_route = _STAT_ROUTE_URL_SEGMENTS.get(
                        sub_route, sub_route
                    )
                    if sub_route == "schedule":
                        page_kind = "schedule"
                    elif sub_route == "standings":
                        page_kind = "standings"
                    elif sub_route in UNAVAILABLE_SEASON_STAT_ROUTES:
                        # These links are still advertised by FBref, but live
                        # availability audits found only restricted/empty
                        # statistical cells.  Skipping before frontier fan-out
                        # avoids a paid request and, importantly, prevents the
                        # route from falling through as a season overview.
                        continue
                    elif stat_route in _SEASON_STAT_ROUTES:
                        # Stats subpages share a competition/season identity
                        # but are distinct canonical pages.  Keeping the route
                        # discriminator prevents them from colliding with the
                        # season overview in the durable frontier.
                        page_kind = "season_stats"
                        source_ids["stat_route"] = stat_route
                    else:
                        page_kind = "season"
            if page_kind is None:
                continue
            source_ids = normalize_page_source_ids(page_kind, source_ids)
            found.setdefault(
                (page_kind, canonical),
                DiscoveredPageLink(
                    page_kind=page_kind,
                    canonical_url=canonical,
                    source_ids=source_ids,
                ),
            )
    return list(found.values())


__all__ = [
    "DISCOVERY_PARSER_VERSION",
    "CalendarType",
    "CompetitionFormat",
    "CompetitionGender",
    "CompetitionEligibility",
    "CompetitionRef",
    "DiscoveryDatasetResult",
    "DiscoveryPageResult",
    "DiscoveredPageLink",
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
    "competition_eligibility",
    "discover_page_links",
    "has_valid_zero_table_season_signature",
    "partition_competitions",
    "normalize_page_source_ids",
    "sentinel_coverage",
]
