"""Offline-testable HTML discovery for the Transfermarkt registry.

The adapter owns traversal and parsing, not HTTP.  Callers inject a proxy-only
``fetch(url) -> FetchOutcome[str]`` function, a persistent mutable checkpoint,
and the same shared traffic ledger used by that transport.  Any failed
required page or structural drift aborts the whole snapshot.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional, Protocol
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag

from scrapers.transfermarkt.models import FetchOutcome, FetchStatus, stable_payload_hash
from scrapers.transfermarkt.raw_store import RawResponseStore, RawStoreError
from scrapers.transfermarkt.registry import (
    AgeCategory,
    ClassificationEvidence,
    CompetitionParticipant,
    CompetitionRecord,
    CompetitionType,
    EditionRecord,
    EvidenceOrigin,
    Gender,
    RegistryPage,
    SeasonFormat,
    TeamType,
    UnknownCompetitionError,
    canonical_season,
    narrowest_signals,
    participant_list_hash,
    resolve_competition,
)


BASE_URL = "https://www.transfermarkt.com"
API_BASE_URL = "https://tmapi.transfermarkt.technology"
CATALOGUE_ROUTE = "/navigation/wettbewerbe"
# Compatibility aliases for callers which report the discovery roots.  The
# canonical catalogue is now the sole authority; every region/country page is
# reached through first-party links in that catalogue graph.
SEED_ROUTES: tuple[str, ...] = (CATALOGUE_ROUTE,)
SEED_URLS: tuple[str, ...] = tuple(BASE_URL + route for route in SEED_ROUTES)

_COUNTRY_ROUTE_RE = re.compile(
    r"^/wettbewerbe/national/wettbewerbe/[A-Za-z0-9_-]+(?:/.*)?$"
)
_REGION_ROUTE_RE = re.compile(r"^/wettbewerbe/(?!national(?:/|$))[A-Za-z0-9_-]+/?$")
_COMPETITION_ROUTE_RE = re.compile(
    r"^/(?P<slug>[^/?#]+)/(?:[^?#]*/)?(?P<section>[^/?#]+)/"
    r"(?P<kind>pokalwettbewerb|wettbewerb)/"
    r"(?P<competition_id>[A-Za-z0-9_-]+)(?:/.*)?$"
)
_CANONICAL_SECTION = "startseite"
# A snapshot is what the source said *as read by this parser*: the same pages
# yield different records once the parser changes, and Silver snapshots are
# immutable, so the parser revision is part of the snapshot identity. Bump it
# whenever parsing or classification changes — otherwise a restated catalogue
# cannot be published over the snapshot id it would otherwise reuse.
PARSER_REVISION = "tm-source-discovery-v4"
SCHEMA_REVISION = "3"
# The catalogue states a competition's taxonomy at three levels: a broad section
# heading, a group separator inside the tables, and the "National Team
# Competitions" section, which names the entrants themselves — a table group can
# say "cup" about a national-team tournament, but not that clubs play in it.
_SECTION_PRECEDENCE = 1
_GROUP_PRECEDENCE = 2
_ENTRANT_PRECEDENCE = 3
_EDITION_PATH_RE = re.compile(r"/saison_id/(?P<edition_id>\d{4})(?:/|$)")
_TEAM_ROUTE_RE = re.compile(
    r"^/(?P<slug>[^/?#]+)/(?:[^/?#]+/)*verein/(?P<team_id>\d+)(?:/.*)?$"
)
_API_REGULATION_ROUTE_RE = re.compile(
    r"^/competition/(?P<competition_id>[A-Za-z0-9_-]+)/regulation$"
)
_API_PARTICIPANTS_ROUTE_RE = re.compile(
    r"^/competition/(?P<competition_id>[A-Za-z0-9_-]+)/club$"
)
_API_CLUBS_ROUTE = "/clubs"
_API_ENTITY_BATCH_SIZE = 250


@dataclass(frozen=True)
class DiscoveryLimits:
    """Hard safety bounds for one otherwise source-driven catalogue crawl."""

    listing_pages: int = 4096
    competitions: int = 8192
    editions: int = 100_000
    participants_per_edition: int = 1024
    participants: int = 1_000_000
    documents: int = 120_000

    def __post_init__(self) -> None:
        for name in (
            "listing_pages",
            "competitions",
            "editions",
            "participants_per_edition",
            "participants",
            "documents",
        ):
            if int(getattr(self, name)) < 1:
                raise ValueError(f"{name} must be positive")


DEFAULT_DISCOVERY_LIMITS = DiscoveryLimits()


class DiscoveryError(RuntimeError):
    """Base error for an aborted discovery snapshot."""


class DiscoveryFetchError(DiscoveryError):
    """A required page did not return an authoritative HTTP 200."""


class DiscoverySchemaError(DiscoveryError):
    """A required page no longer matches the expected source structure."""


class DiscoveryCheckpointError(DiscoveryError):
    """A persisted response checkpoint is incomplete or corrupt."""


class TrafficLedger(Protocol):
    """Subset of ``SharedTrafficLedger`` needed by discovery orchestration."""

    def ensure_request_allowed(self) -> None:
        """Reject a paid request before I/O when the shared budget is spent."""

    def record_cache_hit(self, *, entity: str, duration_seconds: float) -> None:
        """Attribute a persistent response-cache hit."""


@dataclass(frozen=True)
class _SectionSignals:
    competition_type: Optional[CompetitionType] = None
    gender: Optional[Gender] = None
    team_type: Optional[TeamType] = None
    age_category: Optional[AgeCategory] = None


@dataclass(frozen=True)
class _ListingContext:
    country: str
    confederation: str


@dataclass
class _CompetitionCandidate:
    competition_id: str
    slug: str
    name: str
    profile_url: str
    country: str
    confederation: str
    owner_url: str
    evidence: list[ClassificationEvidence] = field(default_factory=list)


@dataclass(frozen=True)
class _ParticipantCandidate:
    team_id: str
    team_name: str
    source_url: str


@dataclass(frozen=True)
class _TeamEntity:
    team_id: str
    team_name: str
    source_url: str
    is_national_team: bool


@dataclass(frozen=True)
class _Document:
    url: str
    body: str
    payload_hash: str
    raw_capture_id: Optional[str] = None

    @property
    def source_body_hash(self) -> str:
        return self.payload_hash


@dataclass(frozen=True)
class _JsonDocument:
    url: str
    value: Mapping[str, Any]
    payload_hash: str
    source_body_hash: str
    raw_capture_id: Optional[str] = None


def _payload_hash(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8", errors="replace")).hexdigest()


def _normalise_text(value: Any) -> str:
    return " ".join(str(value).split())


def _is_metric_label(value: str) -> bool:
    """Reject table metrics which share a competition/team profile href."""

    label = _normalise_text(value)
    if not label or "%" in label:
        return True
    if re.fullmatch(r"[\d\s.,:+\-/()]+", label):
        return True
    return (
        re.fullmatch(
            r"(?:[€$£¥]\s*)?[+-]?\d[\d.,]*\s*(?:k|m|mn|bn|b)?",
            label,
            flags=re.IGNORECASE,
        )
        is not None
    )


def _anchor_name(anchor: Tag) -> Optional[str]:
    """Prefer the anchor's real visible label, then accessible image labels."""

    image = anchor.find("img")
    values = (
        anchor.get_text(" ", strip=True),
        anchor.get("title"),
        anchor.get("aria-label"),
        image.get("alt") if image else None,
        image.get("title") if image else None,
    )
    for raw in values:
        label = _normalise_text(raw or "")
        if label and not _is_metric_label(label):
            return label
    return None


def _canonical_url(value: str, *, base_url: str = BASE_URL) -> Optional[str]:
    absolute = urljoin(base_url + "/", value)
    parsed = urlsplit(absolute)
    if parsed.scheme not in {"http", "https"}:
        return None
    if parsed.hostname not in {"transfermarkt.com", "www.transfermarkt.com"}:
        return None
    query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    path = re.sub(r"//+", "/", parsed.path).rstrip("/") or "/"
    return urlunsplit(("https", "www.transfermarkt.com", path, query, ""))


def _canonical_api_url(value: str) -> Optional[str]:
    """Allow only the three first-party JSON routes discovery actually uses."""

    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme != "https"
        or parsed.hostname != "tmapi.transfermarkt.technology"
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.fragment
    ):
        return None
    query = parse_qsl(parsed.query, keep_blank_values=True)
    regulation = _API_REGULATION_ROUTE_RE.fullmatch(parsed.path)
    participants = _API_PARTICIPANTS_ROUTE_RE.fullmatch(parsed.path)
    if regulation is not None:
        if query:
            return None
    elif participants is not None:
        if len(query) != 1 or query[0][0] != "season":
            return None
        if re.fullmatch(r"\d{4}", query[0][1]) is None:
            return None
    elif parsed.path == _API_CLUBS_ROUTE:
        if not query or any(key != "ids[]" for key, _ in query):
            return None
        ids = [value for _, value in query]
        if len(ids) > _API_ENTITY_BATCH_SIZE or any(
            re.fullmatch(r"\d+", team_id) is None for team_id in ids
        ):
            return None
        query = [("ids[]", team_id) for team_id in sorted(set(ids), key=int)]
    else:
        return None
    return urlunsplit(
        ("https", "tmapi.transfermarkt.technology", parsed.path, urlencode(query), "")
    )


def _regulation_url(competition_id: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_-]+", competition_id) is None:
        raise DiscoverySchemaError(f"invalid competition id: {competition_id!r}")
    return f"{API_BASE_URL}/competition/{competition_id}/regulation"


def _participants_url(competition_id: str, edition_id: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_-]+", competition_id) is None or re.fullmatch(
        r"\d{4}", edition_id
    ) is None:
        raise DiscoverySchemaError(
            f"invalid competition edition: {competition_id!r}/{edition_id!r}"
        )
    return (
        f"{API_BASE_URL}/competition/{competition_id}/club?"
        + urlencode({"season": edition_id})
    )


def _clubs_url(team_ids: Iterable[str]) -> str:
    ordered = sorted({str(team_id) for team_id in team_ids}, key=int)
    if not ordered or len(ordered) > _API_ENTITY_BATCH_SIZE or any(
        re.fullmatch(r"\d+", team_id) is None for team_id in ordered
    ):
        raise DiscoverySchemaError("invalid club entity batch")
    return f"{API_BASE_URL}{_API_CLUBS_ROUTE}?" + urlencode(
        [("ids[]", team_id) for team_id in ordered]
    )


def _profile_identity(url: str) -> Optional[tuple[str, str, str, str]]:
    parsed = urlsplit(url)
    match = _COMPETITION_ROUTE_RE.match(parsed.path)
    if match is None:
        return None
    return (
        match.group("competition_id"),
        match.group("slug"),
        match.group("kind"),
        match.group("section"),
    )


def _preferred_name(existing: str, candidate: str) -> str:
    """Pick the fuller label for one competition.

    Cards label the same competition differently (``World Cup`` beside ``FIFA
    World Cup``, a bare table figure beside the listing name); a trailing year
    is card context, not part of the name.
    """
    valid = tuple(
        value for value in (existing, candidate) if not _is_metric_label(value)
    )
    if not valid:
        raise DiscoverySchemaError("competition has only numeric/metric labels")
    return min(
        valid,
        key=lambda value: (
            bool(re.search(r"\b(?:19|20)\d{2}$", value)),
            -len(value),
            value.casefold(),
        ),
    )


def _route_rank(url: str) -> tuple[bool, bool, str]:
    identity = _profile_identity(url)
    if identity is None:
        raise DiscoverySchemaError(f"invalid profile route: {url}")
    return (
        identity[2] != "wettbewerb",
        identity[3] != _CANONICAL_SECTION,
        url,
    )


def _profile_url(url: str) -> Optional[str]:
    canonical = _canonical_url(url)
    if canonical is None or _profile_identity(canonical) is None:
        return None
    parsed = urlsplit(canonical)
    path = _EDITION_PATH_RE.sub("", parsed.path).rstrip("/")
    query = [
        pair
        for pair in parse_qsl(parsed.query, keep_blank_values=True)
        if pair[0].lower() not in {"saison_id", "season_id"}
    ]
    return urlunsplit((parsed.scheme, parsed.netloc, path, urlencode(query), ""))


def _is_catalogue_listing(url: str) -> bool:
    return urlsplit(url).path.rstrip("/") == CATALOGUE_ROUTE


def _is_region_listing(url: str) -> bool:
    return _REGION_ROUTE_RE.match(urlsplit(url).path) is not None


def _in_site_chrome(anchor: Any) -> bool:
    """True for anchors in the global navbar, which every page repeats.

    The navbar advertises a fixed set of headline competitions (the World Cup
    among them).  Reading them as listing entries would attribute the hosting
    page's country to a competition that merely appears in the site chrome.
    """
    for parent in anchor.parents:
        classes = parent.get("class") or () if hasattr(parent, "get") else ()
        if any(str(cls).startswith("main-navbar") for cls in classes):
            return True
    return False


def _has_listing_query_only(url: str) -> bool:
    """True unless the URL carries query state other than pagination.

    Confederation listings render the same rows under ``?sort=`` links in the
    table head; following them re-buys an identical page.
    """
    query = parse_qsl(urlsplit(url).query, keep_blank_values=True)
    return all(key == "page" for key, _ in query)


def _is_country_listing(url: str) -> bool:
    return _COUNTRY_ROUTE_RE.match(urlsplit(url).path.rstrip("/")) is not None


def _is_recognised_listing(url: str) -> bool:
    return (
        _is_catalogue_listing(url)
        or _is_region_listing(url)
        or _is_country_listing(url)
    )


def _listing_page_number(url: str, *, limit: int) -> int:
    query = parse_qsl(urlsplit(url).query, keep_blank_values=True)
    if not query:
        return 1
    if len(query) != 1 or query[0][0] != "page":
        raise DiscoverySchemaError(f"invalid listing query: {url}")
    raw = query[0][1]
    if re.fullmatch(r"\d+", raw) is None:
        raise DiscoverySchemaError(f"invalid pagination page {raw!r}: {url}")
    page = int(raw)
    if page < 1 or page > limit:
        raise DiscoverySchemaError(f"pagination page out of bounds (1..{limit}): {url}")
    return page


def _listing_page_url(url: str, page: int) -> str:
    parsed = urlsplit(url)
    query = "" if page == 1 else urlencode({"page": page})
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))


def _section_signals(label: str) -> _SectionSignals:
    """Map Transfermarkt section taxonomy; never inspect a competition name."""

    normalised = _normalise_text(label).casefold()
    has_phrase = lambda phrase: re.search(  # noqa: E731 - local taxonomy predicate
        rf"(?<!\w){re.escape(phrase)}(?!\w)", normalised
    ) is not None
    gender = (
        Gender.WOMEN
        if any(
            token in normalised
            for token in (
                "women",
                "frauen",
                "femenino",
                "feminino",
                "femminile",
            )
        )
        else None
    )
    if any(token in normalised for token in ("reserve", "second teams")):
        return _SectionSignals(
            competition_type=CompetitionType.DOMESTIC_LEAGUE,
            gender=gender,
            team_type=TeamType.RESERVE,
            age_category=AgeCategory.SENIOR,
        )
    youth = any(
        token in normalised for token in ("youth", "under-", "under ", "u21", "u19")
    )
    age_category = AgeCategory.UXX if youth else AgeCategory.SENIOR
    if any(token in normalised for token in ("mixed", "co-ed", "coed")):
        return _SectionSignals(
            gender=Gender.MIXED,
            team_type=TeamType.MIXED,
            age_category=age_category,
        )
    if any(
        has_phrase(token)
        for token in ("international cups", "international cup competitions")
    ):
        return _SectionSignals(
            competition_type=CompetitionType.NATIONAL_TEAM_TOURNAMENT,
            gender=gender,
            team_type=TeamType.NATIONAL_TEAM,
            age_category=age_category,
        )
    if any(
        has_phrase(token)
        for token in (
            "national team competitions",
            "national-team competitions",
            "fifa tournaments",
            "international tournaments",
        )
    ):
        return _SectionSignals(
            competition_type=CompetitionType.NATIONAL_TEAM_TOURNAMENT,
            gender=gender,
            team_type=TeamType.NATIONAL_TEAM,
            age_category=age_category,
        )
    if any(has_phrase(token) for token in ("national cups", "domestic cups")):
        return _SectionSignals(
            competition_type=CompetitionType.DOMESTIC_CUP,
            gender=gender,
            team_type=TeamType.CLUB,
            age_category=age_category,
        )
    if any(has_phrase(token) for token in ("national leagues", "domestic leagues")):
        return _SectionSignals(
            competition_type=CompetitionType.DOMESTIC_LEAGUE,
            gender=gender,
            team_type=TeamType.CLUB,
            age_category=age_category,
        )
    if any(
        has_phrase(token)
        for token in (
            "international club competitions",
            "continental club competitions",
            "club competitions",
            # The confederation catalogues head their continental club section
            # "Cups" / "International cups" / "International cup competitions".
            "international cups",
            "international cup competitions",
            "cup competitions",
            "cups",
        )
    ):
        return _SectionSignals(
            competition_type=CompetitionType.CONTINENTAL_CLUB,
            gender=gender,
            team_type=TeamType.CLUB,
            age_category=age_category,
        )
    return _SectionSignals(
        gender=gender,
        age_category=AgeCategory.UXX if youth else None,
    )


def _row_group_label(anchor: Tag) -> str:
    """The catalogue table's own group heading above this competition's row.

    The confederation listings group their rows under structural separators —
    ``First Tier``, ``Domestic Cup``, ``Youth league``, ``Reserve league`` — and
    that grouping, not the competition's name, is what states its type, age and
    team category.
    """
    row = anchor.find_parent("tr")
    while row is not None:
        previous = row.find_previous_sibling("tr")
        while previous is not None:
            separator = previous.select_one("td.extrarow")
            if separator is not None:
                return _normalise_text(separator.get_text(" ", strip=True))
            previous = previous.find_previous_sibling("tr")
        row = row.find_parent("tr")
    return ""


def _group_signals(label: str) -> _SectionSignals:
    """Map the table's group heading; never inspect a competition name."""

    normalised = _normalise_text(label).casefold()
    if not normalised:
        return _SectionSignals()
    if re.search(r"(?<!\w)international cups?(?!\w)", normalised):
        return _SectionSignals(
            competition_type=CompetitionType.NATIONAL_TEAM_TOURNAMENT,
            team_type=TeamType.NATIONAL_TEAM,
            age_category=AgeCategory.SENIOR,
        )
    if "reserve" in normalised:
        return _SectionSignals(
            competition_type=CompetitionType.DOMESTIC_LEAGUE,
            team_type=TeamType.RESERVE,
            age_category=AgeCategory.SENIOR,
        )
    age_category = AgeCategory.UXX if "youth" in normalised else AgeCategory.SENIOR
    if re.search(r"(?<!\w)(?:cups?|domestic cups?)(?!\w)", normalised):
        return _SectionSignals(
            competition_type=CompetitionType.DOMESTIC_CUP,
            team_type=TeamType.CLUB,
            age_category=age_category,
        )
    if any(
        token in normalised
        for token in ("tier", "league", "championship", "play-off", "playoff")
    ):
        return _SectionSignals(
            competition_type=CompetitionType.DOMESTIC_LEAGUE,
            team_type=TeamType.CLUB,
            age_category=age_category,
        )
    return _SectionSignals()


def _section_precedence(label: str) -> int:
    """How narrowly a section heading speaks about the rows under it.

    "National Team Competitions" names who plays, which no table group can
    contradict; the broad rubrics ("Cups", "International cup competitions")
    bracket club and national-team tournaments together and merely locate them.
    """
    normalised = _normalise_text(label).casefold()
    if any(
        token in normalised
        for token in (
            "national team competitions",
            "national-team competitions",
            "fifa tournaments",
            # These say who plays, not where the competition sits: the UEFA
            # Youth League is listed under "Youth Competitions" and again under
            # the "Cups" rubric, which cannot make its entrants senior.
            "youth competitions",
            "women",
            "frauen",
        )
    ):
        return _ENTRANT_PRECEDENCE
    narrow = any(
        token in normalised
        for token in (
            "national leagues",
            "domestic leagues",
            "national cups",
            "domestic cups",
        )
    )
    return _GROUP_PRECEDENCE if narrow else _SECTION_PRECEDENCE


def _section_label(anchor: Tag) -> str:
    current: Optional[Tag] = anchor
    while current is not None:
        classes = set(current.get("class", ()))
        if current.name == "section" or "box" in classes:
            header = current.select_one(
                ".content-box-headline, [data-section-label], h1, h2, h3"
            )
            if header is not None:
                return _normalise_text(
                    header.get("data-section-label") or header.get_text(" ", strip=True)
                )
        parent = current.parent
        current = parent if isinstance(parent, Tag) else None
    return ""


def _listing_context(soup: BeautifulSoup, url: str) -> _ListingContext:
    body = soup.body
    country_meta = soup.select_one('meta[name="tm-country"]')
    confed_meta = soup.select_one('meta[name="tm-confederation"]')
    country = (country_meta.get("content") if country_meta else None) or (
        body.get("data-country") if body else None
    )
    confederation = (confed_meta.get("content") if confed_meta else None) or (
        body.get("data-confederation") if body else None
    )
    path = urlsplit(url).path.rstrip("/")
    defaults = {
        "/navigation/wettbewerbe": ("World", "International"),
        "/wettbewerbe/europa": ("Europe", "UEFA"),
        "/wettbewerbe/amerika": ("Americas", "Americas"),
        "/wettbewerbe/asien": ("Asia", "AFC"),
        "/wettbewerbe/afrika": ("Africa", "CAF"),
        "/wettbewerbe/fifa": ("World", "FIFA"),
    }
    default_country, default_confed = defaults.get(path, ("Unknown", "Unknown"))
    if not country and _is_country_listing(url):
        headings = " ".join(
            item.get_text(" ", strip=True)
            for item in soup.select("h1, title, .content-box-headline")
        )
        match = re.search(
            r"(?:competitions|football)\s+(?:in|[-–])\s+"
            r"(?P<country>[A-Za-z][A-Za-z .'-]+)",
            headings,
            flags=re.IGNORECASE,
        )
        if match is not None:
            country = _normalise_text(match.group("country"))
    if not confederation and _is_country_listing(url):
        region_confederations = {
            "/wettbewerbe/europa": "UEFA",
            "/wettbewerbe/amerika": "Americas",
            "/wettbewerbe/asien": "AFC",
            "/wettbewerbe/afrika": "CAF",
            "/wettbewerbe/fifa": "FIFA",
        }
        for anchor in soup.select("a[href]"):
            linked = _canonical_url(str(anchor.get("href")), base_url=url)
            if linked is None:
                continue
            region = urlsplit(linked).path.rstrip("/")
            if region in region_confederations:
                confederation = region_confederations[region]
                break
    return _ListingContext(
        country=_normalise_text(country or default_country),
        confederation=_normalise_text(confederation or default_confed),
    )


def _signal_evidence(
    *,
    label: str,
    source_url: str,
    signals: _SectionSignals,
    precedence: int = _SECTION_PRECEDENCE,
) -> ClassificationEvidence:
    return ClassificationEvidence(
        source_field="section_label",
        source_value=label or "unclassified",
        source_url=source_url,
        origin=EvidenceOrigin.SOURCE_PAGE,
        precedence=precedence,
        competition_type=signals.competition_type,
        gender=signals.gender,
        team_type=signals.team_type,
        age_category=signals.age_category,
    )


_NAME_AGE_RE = re.compile(r"\b[uU]-?(?:1[4-9]|2[0-3])\b|\byouth\b", re.IGNORECASE)
_NAME_WOMEN_RE = re.compile(
    r"\bwomen(?:'s)?\b|\bfrauen\b|\bfeminin\w*\b|\bfemenin\w*\b|\bfemminil\w*\b",
    re.IGNORECASE,
)
_NAME_MIXED_RE = re.compile(r"\bmixed\b|\bco[- ]?ed\b", re.IGNORECASE)


def _name_exclusion_evidence(
    name: str,
    source_url: str,
) -> Optional[ClassificationEvidence]:
    """What the competition's own name rules out.

    Age is stated structurally only for leagues, under the catalogue's "Youth
    league" group. A youth *tournament* is listed beside the senior ones — the
    U17 World Cup sits in the same "National Team Competitions" section as the
    World Cup — and says so only in its name. This evidence can exclude a
    competition from the crawl; ``_classification`` never lets it admit one.
    """
    age = AgeCategory.UXX if _NAME_AGE_RE.search(name) else None
    gender = Gender.WOMEN if _NAME_WOMEN_RE.search(name) else None
    if gender is None and _NAME_MIXED_RE.search(name):
        gender = Gender.MIXED
    if age is None and gender is None:
        return None
    return ClassificationEvidence(
        source_field="competition_name",
        source_value=name,
        source_url=source_url,
        origin=EvidenceOrigin.NAME,
        age_category=age,
        gender=gender,
    )


def _taxonomy_evidence(source_url: str) -> ClassificationEvidence:
    return ClassificationEvidence(
        source_field="transfermarkt_taxonomy",
        source_value="main men's competitions taxonomy",
        source_url=source_url,
        origin=EvidenceOrigin.STRUCTURED,
        gender=Gender.MEN,
    )


def _is_pagination_anchor(anchor: Tag) -> bool:
    classes = set(anchor.get("class", ()))
    parent_classes = set(anchor.parent.get("class", ())) if anchor.parent else set()
    in_pagination_container = any(
        (
            any(
                "pagination" in str(item).casefold() for item in parent.get("class", ())
            )
            or "pagination" in str(parent.get("aria-label", "")).casefold()
        )
        for parent in anchor.parents
        if isinstance(parent, Tag)
    )
    return bool(
        anchor.has_attr("data-page")
        or classes.intersection({"page-link", "tm-pagination-link"})
        or parent_classes.intersection({"page-item", "tm-pagination"})
        or in_pagination_container
        or set(anchor.get("rel", ())).intersection({"next", "prev", "previous"})
    )


def _pagination_urls(
    soup: BeautifulSoup,
    page_url: str,
    *,
    limit: int,
) -> tuple[str, ...]:
    """Expand one listing's declared pagination and reject ambiguous loops."""

    current_page = _listing_page_number(page_url, limit=limit)
    declared_pages = {current_page}
    family_path = urlsplit(page_url).path.rstrip("/")
    for anchor in soup.select("a[href]"):
        if not _is_pagination_anchor(anchor):
            continue
        canonical = _canonical_url(str(anchor.get("href")), base_url=page_url)
        if canonical is None:
            raise DiscoverySchemaError(
                f"pagination leaves Transfermarkt: {page_url} -> {anchor.get('href')}"
            )
        if urlsplit(canonical).path.rstrip("/") != family_path:
            raise DiscoverySchemaError(
                f"pagination changes listing route: {page_url} -> {canonical}"
            )
        target_page = _listing_page_number(canonical, limit=limit)
        declared = _normalise_text(anchor.get("data-page") or "")
        if declared:
            if re.fullmatch(r"\d+", declared) is None or int(declared) != target_page:
                raise DiscoverySchemaError(
                    f"pagination label/URL mismatch: {page_url} -> {canonical}"
                )
        text_page = _normalise_text(anchor.get_text(" ", strip=True))
        if re.fullmatch(r"\d+", text_page) and int(text_page) != target_page:
            raise DiscoverySchemaError(
                f"pagination text/URL mismatch: {page_url} -> {canonical}"
            )
        relations = set(anchor.get("rel", ()))
        if "next" in relations and target_page <= current_page:
            raise DiscoverySchemaError(f"pagination next-link loop: {page_url}")
        if relations.intersection({"prev", "previous"}) and target_page >= current_page:
            raise DiscoverySchemaError(f"pagination previous-link loop: {page_url}")
        declared_pages.add(target_page)

    last_page = max(declared_pages)
    if last_page > limit:
        raise DiscoverySchemaError(
            f"pagination page count out of bounds (max={limit}): {page_url}"
        )
    return tuple(_listing_page_url(page_url, page) for page in range(1, last_page + 1))


def _listing_links(
    soup: BeautifulSoup,
    page_url: str,
    *,
    pagination_limit: int,
) -> tuple[str, ...]:
    links: set[str] = set()
    for anchor in soup.select("a[href]"):
        if _in_site_chrome(anchor):
            continue
        canonical = _canonical_url(str(anchor.get("href")), base_url=page_url)
        if canonical is None:
            continue
        if _is_recognised_listing(canonical):
            if not _has_listing_query_only(canonical):
                continue
            page = _listing_page_number(canonical, limit=pagination_limit)
            if page == 1:
                links.add(_listing_page_url(canonical, 1))
            elif _is_pagination_anchor(anchor):
                links.add(canonical)
            continue
    links.update(_pagination_urls(soup, page_url, limit=pagination_limit))
    return tuple(sorted(links))


def _listing_candidates(
    soup: BeautifulSoup,
    *,
    page_url: str,
    pagination_limit: int = DEFAULT_DISCOVERY_LIMITS.listing_pages,
) -> tuple[_CompetitionCandidate, ...]:
    context = _listing_context(soup, page_url)
    candidates: list[_CompetitionCandidate] = []

    def source_country(anchor: Tag) -> str:
        explicit = _normalise_text(anchor.get("data-country") or "")
        if explicit:
            return explicit
        row = anchor.find_parent("tr")
        if row is not None:
            for element in row.select("[data-country], .flaggenrahmen[title], img[title]"):
                value = _normalise_text(
                    element.get("data-country") or element.get("title") or ""
                )
                if value and not _is_metric_label(value):
                    return value
        return context.country

    seen_links = 0
    for anchor in soup.select("a[href]"):
        if _in_site_chrome(anchor):
            continue
        profile_url = _profile_url(str(anchor.get("href")))
        if profile_url is None:
            continue
        identity = _profile_identity(profile_url)
        if identity is None:
            continue
        seen_links += 1
        competition_id, slug, _kind, _section = identity
        name = _anchor_name(anchor)
        if name is None:
            name = ""
        section_label = _section_label(anchor)
        group_label = _row_group_label(anchor)
        stated = (
            (
                section_label,
                _section_signals(section_label),
                _section_precedence(section_label),
            ),
            (group_label, _group_signals(group_label), _GROUP_PRECEDENCE),
        )
        country = _normalise_text(source_country(anchor))
        confederation = _normalise_text(
            anchor.get("data-confederation") or context.confederation
        )
        # A source section that explicitly says women is authoritative audience
        # evidence.  Adding the catalog's default men's signal as well would
        # manufacture a conflict and block the whole registry instead of
        # source-backed exclusion of that competition.
        evidence = []
        excluded_by_name = _name_exclusion_evidence(name, page_url) if name else None
        if excluded_by_name is not None:
            evidence.append(excluded_by_name)
        if all(
            signals.gender not in {Gender.WOMEN, Gender.MIXED}
            for _, signals, _p in stated
        ):
            evidence.append(_taxonomy_evidence(page_url))
        for label, signals, precedence in stated:
            if any(
                value is not None
                for value in (
                    signals.competition_type,
                    signals.gender,
                    signals.team_type,
                    signals.age_category,
                )
            ):
                evidence.append(
                    _signal_evidence(
                        label=label,
                        source_url=page_url,
                        signals=signals,
                        precedence=precedence,
                    )
                )
        candidates.append(
            _CompetitionCandidate(
                competition_id=competition_id,
                slug=slug,
                name=name,
                profile_url=profile_url,
                country=country,
                confederation=confederation,
                owner_url=page_url,
                evidence=evidence,
            )
        )

    body = soup.body
    explicit_empty = (
        body is not None
        and str(body.get("data-registry-empty", "")).casefold() == "true"
    )
    canonical_page = _listing_page_url(
        page_url,
        _listing_page_number(page_url, limit=pagination_limit),
    )
    has_navigation = any(
        linked != canonical_page
        for linked in _listing_links(soup, page_url, pagination_limit=pagination_limit)
    )
    if not seen_links and not has_navigation and not explicit_empty:
        raise DiscoverySchemaError(
            f"listing page has no registry structure: {page_url}"
        )
    if (
        seen_links
        and _is_country_listing(page_url)
        and (context.country == "Unknown" or context.confederation == "Unknown")
    ):
        raise DiscoverySchemaError(
            f"country listing lacks country/confederation context: {page_url}"
        )
    return tuple(candidates)


def _merge_candidate(
    target: dict[str, _CompetitionCandidate],
    candidate: _CompetitionCandidate,
) -> None:
    existing = target.get(candidate.competition_id)
    if existing is None:
        target[candidate.competition_id] = candidate
        return
    # Transfermarkt publishes several routes for the same competition ID: the
    # legacy ``.../pokalwettbewerb/FIWC`` cup route beside the canonical
    # ``.../wettbewerb/FIWC`` one, a secondary tab
    # (``.../gastarbeiter/wettbewerb/EGY1``) beside the profile itself, and a
    # renamed competition under its historical slug (``torneo-intermedio``
    # beside ``liga-auf-intermedio`` for URUI).  The source ID is the identity
    # and the slug is only URL decoration, so resolve the route deterministically
    # rather than failing: prefer the generic competition route, then the
    # ``startseite`` profile section.  Genuine source disagreement still fails
    # closed on country/confederation below and on classification conflicts.
    existing_identity = _profile_identity(existing.profile_url)
    candidate_identity = _profile_identity(candidate.profile_url)
    if existing_identity is None or candidate_identity is None:
        raise DiscoverySchemaError(
            f"invalid profile route for {candidate.competition_id}"
        )
    if existing.profile_url != candidate.profile_url:
        preferred = min(
            (existing, candidate),
            key=lambda item: _route_rank(item.profile_url),
        )
        existing.slug = preferred.slug
        existing.profile_url = preferred.profile_url
        existing.name = _preferred_name(existing.name, candidate.name)
    elif existing.name != candidate.name:
        existing.name = _preferred_name(existing.name, candidate.name)
    for name in ("country", "confederation"):
        old = getattr(existing, name)
        new = getattr(candidate, name)
        generic = {"Unknown", "International", "Worldwide", "World"}
        if old in generic and new not in generic:
            setattr(existing, name, new)
        elif new not in generic and old != new:
            raise DiscoverySchemaError(
                f"conflicting {name} for {candidate.competition_id}: {old!r}/{new!r}"
            )
    existing.owner_url = min(existing.owner_url, candidate.owner_url)
    known_evidence = {
        json.dumps(item.as_dict(), sort_keys=True) for item in existing.evidence
    }
    for item in candidate.evidence:
        serialised = json.dumps(item.as_dict(), sort_keys=True)
        if serialised not in known_evidence:
            existing.evidence.append(item)
            known_evidence.add(serialised)


def _has_season_markup(soup: BeautifulSoup) -> bool:
    return bool(
        soup.select('select[name*="saison"] option[value]')
        or soup.select('a[href*="saison_id"]')
        or soup.select("tm-competition-homepage[competition-id][season-id]")
    )


def _component_season_id(
    soup: BeautifulSoup,
    *,
    competition_id: str,
    source_url: str,
) -> Optional[str]:
    components = soup.select(
        "tm-competition-homepage[competition-id][season-id]"
    )
    if not components:
        return None
    identities = {
        (
            _normalise_text(component.get("competition-id") or ""),
            _normalise_text(component.get("season-id") or ""),
        )
        for component in components
    }
    if len(identities) != 1:
        raise DiscoverySchemaError(f"conflicting competition component: {source_url}")
    declared_competition_id, edition_id = identities.pop()
    if (
        declared_competition_id != competition_id
        or re.fullmatch(r"\d{4}", edition_id) is None
    ):
        raise DiscoverySchemaError(f"competition component identity mismatch: {source_url}")
    return edition_id


def _canonical_profile_route(soup: BeautifulSoup, profile_url: str) -> Optional[str]:
    """The route the source itself calls canonical, when it differs.

    A cup is listed under both the generic ``/wettbewerb/`` route and its own
    ``/pokalwettbewerb/`` one, but only the canonical route carries the season
    selector — the generic one answers with a season-less page.
    """
    tag = soup.select_one('link[rel="canonical"]')
    href = str(tag.get("href") or "") if tag is not None else ""
    canonical = _profile_url(href) if href else None
    if canonical is None or canonical == profile_url:
        return None
    return canonical


_TITLE_SEASON_RE = re.compile(
    r"\b(?P<label>(?:18|19|20|21)?\d{2}\s*/\s*\d{2}|(?:18|19|20|21)\d{2})\b"
)


def _title_edition(
    soup: BeautifulSoup,
    profile_url: str,
) -> dict[str, tuple[str, bool, Mapping[str, Any]]]:
    """The single edition a season-less profile is showing.

    Cups and qualifiers — a third of the catalogue — carry no season selector
    at all, not even on their canonical route: the profile shows the current
    edition only, and names it in the page title ("CAF-Champions League 25/26").
    Take the last season in the title, since an earlier one can belong to the
    competition's own name ("AFC Challenge Cup (- 2014) 2013").
    """
    title = soup.find("title")
    heading = _normalise_text(title.get_text(" ", strip=True)) if title else ""
    heading = heading.split("|")[0]
    matches = _TITLE_SEASON_RE.findall(heading)
    if not matches:
        raise DiscoverySchemaError(f"edition selector missing: {profile_url}")
    label = _normalise_text(matches[-1])
    season_format = _label_season_format(label, profile_url)
    if season_format is SeasonFormat.SINGLE_YEAR:
        edition_id = label
    else:
        start = re.split(r"\s*[/\-]\s*", label)[0]
        edition_id = start if len(start) == 4 else f"20{start}"
    return {edition_id: (label, True, {})}


def _selector_options(
    soup: BeautifulSoup,
    *,
    profile_url: str,
) -> tuple[tuple[str, str, bool, Mapping[str, Any]], ...]:
    values: dict[str, tuple[str, bool, Mapping[str, Any]]] = {}
    for option in soup.select('select[name*="saison"] option[value]'):
        edition_id = _normalise_text(option.get("value"))
        if re.fullmatch(r"\d{4}", edition_id) is None:
            continue
        label = _normalise_text(option.get_text(" ", strip=True))
        selected = option.has_attr("selected")
        attrs = dict(option.attrs)
        previous = values.get(edition_id)
        current = (label, selected, attrs)
        if previous is not None and previous[:2] != current[:2]:
            raise DiscoverySchemaError(
                f"conflicting edition selector {edition_id}: {profile_url}"
            )
        values[edition_id] = current

    if not values:
        for anchor in soup.select('a[href*="saison_id"]'):
            canonical = _canonical_url(str(anchor.get("href")), base_url=profile_url)
            if canonical is None:
                continue
            path_match = _EDITION_PATH_RE.search(urlsplit(canonical).path)
            query = dict(parse_qsl(urlsplit(canonical).query))
            edition_id = (
                path_match.group("edition_id")
                if path_match
                else query.get("saison_id", "")
            )
            if re.fullmatch(r"\d{4}", edition_id) is None:
                continue
            label = _normalise_text(anchor.get_text(" ", strip=True))
            if not edition_id or not label:
                continue
            selected = "active" in set(anchor.get("class", ())) or str(
                anchor.get("aria-current", "")
            ).casefold() in {"true", "page"}
            values[edition_id] = (label, selected, dict(anchor.attrs))

    if not values:
        values = _title_edition(soup, profile_url)
    selected_ids = [key for key, value in values.items() if value[1]]
    if len(selected_ids) != 1:
        raise DiscoverySchemaError(
            f"edition selector must mark exactly one current edition: {profile_url}"
        )
    return tuple(
        (edition_id, *values[edition_id]) for edition_id in sorted(values, reverse=True)
    )


def _label_season_format(label: str, profile_url: str) -> SeasonFormat:
    if re.fullmatch(r"(?:18|19|20|21)\d{2}", label):
        return SeasonFormat.SINGLE_YEAR
    if re.fullmatch(
        r"(?:(?:18|19|20|21)\d{2}|\d{2})\s*[/\-]\s*"
        r"(?:\d{2}|(?:18|19|20|21)\d{2})",
        label,
    ):
        return SeasonFormat.SPLIT_YEAR
    raise DiscoverySchemaError(f"unrecognised edition label {label!r}: {profile_url}")


def _season_format(
    options: Iterable[tuple[str, str, bool, Mapping[str, Any]]],
    profile_url: str,
) -> SeasonFormat:
    """The format a competition runs on now.

    Competitions switch format over their history — Australia played 1977 as a
    calendar year and every season since as a split year — so the format is a
    property of each edition, and the competition carries the one its current
    edition uses.
    """
    formats = {
        edition_id: _label_season_format(label, profile_url)
        for edition_id, label, _selected, _attrs in options
    }
    current = [
        edition_id for edition_id, _label, selected, _attrs in options if selected
    ]
    if len(current) != 1:
        raise DiscoverySchemaError(
            f"edition selector must mark exactly one current edition: {profile_url}"
        )
    return formats[current[0]]


def _api_payload_data(
    document: _JsonDocument,
    *,
    expected_type: type,
) -> Any:
    value = document.value
    if value.get("success") is not True or "data" not in value:
        raise DiscoverySchemaError(f"unsuccessful discovery API response: {document.url}")
    data = value["data"]
    if type(data) is not expected_type:
        raise DiscoverySchemaError(
            f"discovery API data must be {expected_type.__name__}: {document.url}"
        )
    return data


def _api_date(value: Any, *, field_name: str, source_url: str) -> Optional[str]:
    if value is None:
        return None
    if type(value) is not str:
        raise DiscoverySchemaError(
            f"{field_name} must be an ISO timestamp: {source_url}"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DiscoverySchemaError(
            f"{field_name} must be an ISO timestamp: {source_url}"
        ) from exc
    return parsed.date().isoformat()


def _regulation_options(
    document: _JsonDocument,
    *,
    competition_id: str,
    profile_url: str,
) -> tuple[tuple[str, str, bool, Mapping[str, Any]], ...]:
    rows = _api_payload_data(document, expected_type=list)
    if not rows:
        raise DiscoverySchemaError(f"regulation has no editions: {document.url}")
    values: dict[str, tuple[str, bool, Mapping[str, Any]]] = {}
    for row in rows:
        if type(row) is not dict or row.get("competitionId") != competition_id:
            raise DiscoverySchemaError(
                f"regulation competition mismatch: {document.url}"
            )
        season = row.get("season")
        if type(season) is not dict:
            raise DiscoverySchemaError(f"regulation season missing: {document.url}")
        raw_id = season.get("id")
        if type(raw_id) is not int or isinstance(raw_id, bool):
            raise DiscoverySchemaError(f"regulation season id invalid: {document.url}")
        edition_id = str(raw_id)
        if re.fullmatch(r"\d{4}", edition_id) is None:
            raise DiscoverySchemaError(f"regulation season id invalid: {document.url}")
        label = _normalise_text(season.get("display") or "")
        _label_season_format(label, profile_url)
        current = row.get("isCurrentSeason")
        if type(current) is not bool:
            raise DiscoverySchemaError(
                f"regulation current-season flag invalid: {document.url}"
            )
        attrs = {
            "data-start-date": _api_date(
                row.get("tournamentStart"),
                field_name="tournamentStart",
                source_url=document.url,
            ),
            "data-end-date": _api_date(
                row.get("tournamentEnd"),
                field_name="tournamentEnd",
                source_url=document.url,
            ),
        }
        attrs = {key: value for key, value in attrs.items() if value is not None}
        if edition_id in values:
            raise DiscoverySchemaError(
                f"duplicate regulation edition {edition_id}: {document.url}"
            )
        values[edition_id] = (label, current, attrs)
    current_ids = [edition_id for edition_id, value in values.items() if value[1]]
    if len(current_ids) != 1:
        raise DiscoverySchemaError(
            f"regulation must mark exactly one current edition: {document.url}"
        )
    return tuple(
        (edition_id, *values[edition_id])
        for edition_id in sorted(values, reverse=True)
    )


def _api_participant_ids(
    document: _JsonDocument,
    *,
    competition_id: str,
    edition_id: str,
    limit: int,
) -> tuple[str, ...]:
    data = _api_payload_data(document, expected_type=dict)
    if data.get("competitionId") != competition_id:
        raise DiscoverySchemaError(
            f"participant competition mismatch: {document.url}"
        )
    season_id = data.get("seasonId")
    season = data.get("season")
    if (
        type(season_id) is not int
        or isinstance(season_id, bool)
        or str(season_id) != edition_id
        or (
            season is not None
            and (
                type(season) is not dict
                or type(season.get("id")) is not int
                or isinstance(season.get("id"), bool)
                or str(season.get("id")) != edition_id
            )
        )
    ):
        raise DiscoverySchemaError(f"participant season mismatch: {document.url}")
    raw_ids = data.get("clubIds")
    if type(raw_ids) is not list:
        raise DiscoverySchemaError(f"participant ids missing: {document.url}")
    team_ids: list[str] = []
    for raw_id in raw_ids:
        if type(raw_id) not in {str, int} or isinstance(raw_id, bool):
            raise DiscoverySchemaError(f"participant id invalid: {document.url}")
        team_id = str(raw_id)
        if re.fullmatch(r"\d+", team_id) is None:
            raise DiscoverySchemaError(f"participant id invalid: {document.url}")
        team_ids.append(team_id)
    if len(team_ids) != len(set(team_ids)):
        raise DiscoverySchemaError(f"duplicate participant id: {document.url}")
    if len(team_ids) > limit:
        raise DiscoverySchemaError(
            f"participant list exceeds bound {limit}: {document.url}"
        )
    return tuple(sorted(team_ids, key=int))


def _api_team_entities(
    document: _JsonDocument,
    *,
    requested_ids: Iterable[str],
) -> dict[str, _TeamEntity]:
    rows = _api_payload_data(document, expected_type=list)
    requested = {str(team_id) for team_id in requested_ids}
    entities: dict[str, _TeamEntity] = {}
    for row in rows:
        if type(row) is not dict:
            raise DiscoverySchemaError(f"club entity is not an object: {document.url}")
        raw_id = row.get("id")
        if type(raw_id) not in {str, int} or isinstance(raw_id, bool):
            raise DiscoverySchemaError(f"club entity id invalid: {document.url}")
        team_id = str(raw_id)
        name = _normalise_text(row.get("name") or "")
        relative_url = row.get("relativeUrl")
        base_details = row.get("baseDetails")
        if (
            re.fullmatch(r"\d+", team_id) is None
            or not name
            or _is_metric_label(name)
            or type(relative_url) is not str
            or type(base_details) is not dict
            or type(base_details.get("isNationalTeam")) is not bool
        ):
            raise DiscoverySchemaError(f"club entity fields invalid: {document.url}")
        canonical = _canonical_url(relative_url)
        identity = _team_identity(canonical) if canonical is not None else None
        if identity is None or identity[0] != team_id:
            raise DiscoverySchemaError(f"club entity URL mismatch: {document.url}")
        if team_id in entities:
            raise DiscoverySchemaError(f"duplicate club entity {team_id}: {document.url}")
        entities[team_id] = _TeamEntity(
            team_id=team_id,
            team_name=name,
            source_url=canonical,
            is_national_team=base_details["isNationalTeam"],
        )
    if set(entities) != requested:
        missing = sorted(requested - set(entities), key=int)
        extra = sorted(set(entities) - requested, key=int)
        raise DiscoverySchemaError(
            f"club entity response incomplete: {document.url}; "
            f"missing={missing[:3]}, extra={extra[:3]}"
        )
    return entities


def _edition_url(profile_url: str, edition_id: str) -> str:
    parsed = urlsplit(profile_url)
    path = _EDITION_PATH_RE.sub("", parsed.path).rstrip("/")
    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            f"{path}/saison_id/{edition_id}",
            "",
            "",
        )
    )


def _validate_edition_document(
    soup: BeautifulSoup,
    *,
    competition_id: str,
    edition_id: str,
    source_url: str,
) -> None:
    declared_id = soup.select_one("[data-competition-id]")
    if (
        declared_id is not None
        and str(declared_id.get("data-competition-id")) != competition_id
    ):
        raise DiscoverySchemaError(f"edition competition mismatch: {source_url}")
    selected = {
        _normalise_text(option.get("value"))
        for option in soup.select('select[name*="saison"] option[selected][value]')
    }
    if selected and selected != {edition_id}:
        raise DiscoverySchemaError(
            f"edition page selected {sorted(selected)}, expected {edition_id}: {source_url}"
        )


def _team_identity(url: str) -> Optional[tuple[str, str]]:
    match = _TEAM_ROUTE_RE.match(urlsplit(url).path)
    if match is None:
        return None
    return match.group("team_id"), match.group("slug")


def _participant_candidates(
    soup: BeautifulSoup,
    *,
    source_url: str,
    limit: int,
) -> tuple[_ParticipantCandidate, ...]:
    """Read the exact participant table; never infer it from a displayed count."""

    explicit = soup.select("[data-participant-list], [data-edition-participants]")
    containers: list[Tag] = [item for item in explicit if isinstance(item, Tag)]
    if not containers:
        team_tables = [
            table
            for table in soup.select("table.items")
            if isinstance(table, Tag)
            and any(
                _team_identity(canonical) is not None
                for anchor in table.select("a[href]")
                if (
                    canonical := _canonical_url(
                        str(anchor.get("href")), base_url=source_url
                    )
                )
                is not None
            )
        ]
        participant_prefixes = (
            "clubs",
            "participants",
            "participating teams",
            "teams",
            "teilnehmer",
            "mannschaften",
            "vereine",
        )
        labelled_tables = [
            table
            for table in team_tables
            if _section_label(table).casefold().startswith(participant_prefixes)
        ]
        if labelled_tables:
            containers = labelled_tables
        elif len(team_tables) == 1:
            containers = team_tables
        elif team_tables:
            raise DiscoverySchemaError(f"participant table is ambiguous: {source_url}")
    empty_marker = soup.select_one('[data-participants-empty="true"]')
    if not containers:
        if empty_marker is not None:
            return ()
        raise DiscoverySchemaError(f"participant list missing: {source_url}")

    by_team: dict[str, _ParticipantCandidate] = {}
    unlabeled: set[str] = set()
    for container in containers:
        for anchor in container.select("a[href]"):
            canonical = _canonical_url(str(anchor.get("href")), base_url=source_url)
            if canonical is None:
                continue
            identity = _team_identity(canonical)
            if identity is None:
                continue
            team_id, _slug = identity
            team_name = _anchor_name(anchor)
            if team_name is None:
                unlabeled.add(team_id)
                continue
            unlabeled.discard(team_id)
            candidate = _ParticipantCandidate(team_id, team_name, canonical)
            previous = by_team.get(team_id)
            if previous is None:
                by_team[team_id] = candidate
            else:
                preferred_name = _preferred_name(previous.team_name, team_name)
                preferred_url = min(previous.source_url, canonical)
                by_team[team_id] = _ParticipantCandidate(
                    team_id, preferred_name, preferred_url
                )
    unresolved = unlabeled - set(by_team)
    if unresolved:
        raise DiscoverySchemaError(
            f"participant has only numeric/metric labels: {source_url} -> "
            f"team {sorted(unresolved)[0]}"
        )
    if not by_team and empty_marker is None:
        raise DiscoverySchemaError(f"participant table is empty: {source_url}")
    if len(by_team) > limit:
        raise DiscoverySchemaError(
            f"participant list exceeds bound {limit}: {source_url}"
        )
    return tuple(by_team[key] for key in sorted(by_team))


def _unique_signal(evidence: Iterable[ClassificationEvidence], name: str, unknown):
    values = narrowest_signals(evidence, name)
    return next(iter(values)) if len(values) == 1 else unknown


class TransfermarktCompetitionDiscovery:
    """Traverse all official competition catalogs into complete registry pages."""

    def __init__(
        self,
        *,
        fetch: Callable[[str], FetchOutcome[str]],
        fetch_json: Optional[
            Callable[[str, str], FetchOutcome[Mapping[str, Any]]]
        ] = None,
        checkpoint: MutableMapping[str, Any],
        traffic_ledger: TrafficLedger,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        limits: DiscoveryLimits = DEFAULT_DISCOVERY_LIMITS,
        raw_store: Optional[RawResponseStore] = None,
    ) -> None:
        if traffic_ledger is None:
            raise TypeError("traffic_ledger is required")
        self._fetch = fetch
        self._fetch_json = fetch_json
        self._checkpoint = checkpoint
        self._traffic_ledger = traffic_ledger
        self._clock = clock
        self._limits = limits
        self._raw_required = os.environ.get(
            "TRANSFERMARKT_REQUIRE_RAW_STORE", "false"
        ).strip().lower() in {"1", "true", "yes", "on"}
        if raw_store is None and self._raw_required:
            raw_store = RawResponseStore.from_env(optional=False)
        self._raw_store = raw_store
        self._documents: dict[str, _Document] = {}
        self._api_documents: dict[str, _JsonDocument] = {}

    def _get(self, url: str) -> _Document:
        canonical = _canonical_url(url)
        if canonical is None:
            raise DiscoveryFetchError(f"non-Transfermarkt URL: {url!r}")
        existing = self._documents.get(canonical)
        if existing is not None:
            return existing
        if len(self._documents) + len(self._api_documents) >= self._limits.documents:
            raise DiscoverySchemaError(
                f"discovery document bound exceeded ({self._limits.documents})"
            )
        cached = self._checkpoint.get(canonical)
        if cached is not None:
            if not isinstance(cached, Mapping):
                raise DiscoveryCheckpointError(
                    f"checkpoint entry is not an object: {canonical}"
                )
            body = cached.get("body")
            expected_hash = cached.get("payload_hash")
            raw_capture_id = cached.get("raw_capture_id")
            if cached.get("status") != FetchStatus.OK.value or not isinstance(
                body, str
            ):
                raise DiscoveryCheckpointError(
                    f"checkpoint is not an authoritative success: {canonical}"
                )
            if self._raw_required and not raw_capture_id:
                raise DiscoveryCheckpointError(
                    f"checkpoint has no raw capture lineage: {canonical}"
                )
            if raw_capture_id and self._raw_store is not None:
                try:
                    raw_body, raw_record = self._raw_store.load_capture(
                        str(raw_capture_id)
                    )
                    body = raw_body.decode("utf-8", errors="replace")
                except (RawStoreError, UnicodeDecodeError) as exc:
                    raise DiscoveryCheckpointError(
                        f"checkpoint raw replay failed: {canonical}"
                    ) from exc
                if raw_record.url != canonical:
                    raise DiscoveryCheckpointError(
                        f"checkpoint raw URL mismatch: {canonical}"
                    )
            actual_hash = _payload_hash(body)
            if expected_hash != actual_hash:
                raise DiscoveryCheckpointError(
                    f"checkpoint payload hash mismatch: {canonical}"
                )
            self._traffic_ledger.record_cache_hit(
                entity="competition_registry", duration_seconds=0.0
            )
            document = _Document(
                canonical,
                body,
                actual_hash,
                str(raw_capture_id) if raw_capture_id else None,
            )
            self._documents[canonical] = document
            return document

        self._traffic_ledger.ensure_request_allowed()
        outcome = self._fetch(canonical)
        if not isinstance(outcome, FetchOutcome):
            raise DiscoveryFetchError(
                f"fetch returned {type(outcome).__name__}, expected FetchOutcome: {canonical}"
            )
        if outcome.status is not FetchStatus.OK or outcome.status_code != 200:
            raise DiscoveryFetchError(
                "required discovery page failed: "
                f"url={canonical}, status={outcome.status.value}, "
                f"http={outcome.status_code or 0}"
            )
        if not isinstance(outcome.value, str) or not outcome.value.strip():
            raise DiscoveryFetchError(
                f"required discovery page has no HTML body: {canonical}"
            )
        body_hash = _payload_hash(outcome.value)
        if outcome.payload_hash is not None and outcome.payload_hash != body_hash:
            raise DiscoveryFetchError(f"transport payload hash mismatch: {canonical}")
        if self._raw_required and (
            not outcome.raw_capture_id or not outcome.raw_body_hash
        ):
            raise DiscoveryFetchError(
                f"required discovery page has no raw lineage: {canonical}"
            )
        if outcome.raw_body_hash and outcome.raw_body_hash != body_hash:
            raise DiscoveryFetchError(f"raw response hash mismatch: {canonical}")
        self._checkpoint[canonical] = {
            "attempts": outcome.attempts,
            "body": outcome.value,
            "decoded_body_bytes": outcome.decoded_body_bytes,
            "payload_hash": body_hash,
            "raw_capture_id": outcome.raw_capture_id,
            "raw_body_hash": outcome.raw_body_hash,
            "status": FetchStatus.OK.value,
            "status_code": 200,
        }
        document = _Document(
            canonical, outcome.value, body_hash, outcome.raw_capture_id
        )
        self._documents[canonical] = document
        return document

    def _get_json(self, url: str, *, endpoint: str) -> _JsonDocument:
        canonical = _canonical_api_url(url)
        if canonical is None:
            raise DiscoveryFetchError(f"unsupported Transfermarkt API URL: {url!r}")
        existing = self._api_documents.get(canonical)
        if existing is not None:
            return existing
        if self._fetch_json is None:
            raise DiscoveryFetchError("Transfermarkt discovery JSON fetcher is required")
        if len(self._documents) + len(self._api_documents) >= self._limits.documents:
            raise DiscoverySchemaError(
                f"discovery document bound exceeded ({self._limits.documents})"
            )
        cached = self._checkpoint.get(canonical)
        if cached is not None:
            if not isinstance(cached, Mapping):
                raise DiscoveryCheckpointError(
                    f"checkpoint entry is not an object: {canonical}"
                )
            value = cached.get("value")
            expected_hash = cached.get("payload_hash")
            expected_raw_hash = cached.get("raw_body_hash")
            raw_capture_id = cached.get("raw_capture_id")
            if (
                cached.get("status") != FetchStatus.OK.value
                or type(value) is not dict
            ):
                raise DiscoveryCheckpointError(
                    f"checkpoint is not an authoritative JSON success: {canonical}"
                )
            if self._raw_required and not raw_capture_id:
                raise DiscoveryCheckpointError(
                    f"checkpoint has no raw capture lineage: {canonical}"
                )
            if raw_capture_id and self._raw_store is not None:
                try:
                    raw_body, raw_record = self._raw_store.load_capture(
                        str(raw_capture_id)
                    )
                    replayed = json.loads(raw_body.decode("utf-8-sig", errors="strict"))
                except (RawStoreError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise DiscoveryCheckpointError(
                        f"checkpoint raw JSON replay failed: {canonical}"
                    ) from exc
                if type(replayed) is not dict or raw_record.url != canonical:
                    raise DiscoveryCheckpointError(
                        f"checkpoint raw JSON identity mismatch: {canonical}"
                    )
                value = replayed
                actual_raw_hash = hashlib.sha256(raw_body).hexdigest()
                if (
                    raw_record.content_hash != actual_raw_hash
                    or expected_raw_hash != actual_raw_hash
                ):
                    raise DiscoveryCheckpointError(
                        f"checkpoint raw JSON hash mismatch: {canonical}"
                    )
            actual_hash = stable_payload_hash(value)
            if expected_hash != actual_hash:
                raise DiscoveryCheckpointError(
                    f"checkpoint JSON payload hash mismatch: {canonical}"
                )
            self._traffic_ledger.record_cache_hit(
                entity=endpoint, duration_seconds=0.0
            )
            document = _JsonDocument(
                url=canonical,
                value=value,
                payload_hash=actual_hash,
                source_body_hash=str(expected_raw_hash or actual_hash),
                raw_capture_id=str(raw_capture_id) if raw_capture_id else None,
            )
            self._api_documents[canonical] = document
            return document

        self._traffic_ledger.ensure_request_allowed()
        outcome = self._fetch_json(canonical, endpoint)
        if not isinstance(outcome, FetchOutcome):
            raise DiscoveryFetchError(
                f"JSON fetch returned {type(outcome).__name__}, expected FetchOutcome: "
                f"{canonical}"
            )
        if outcome.status is not FetchStatus.OK or outcome.status_code != 200:
            raise DiscoveryFetchError(
                "required discovery API failed: "
                f"url={canonical}, status={outcome.status.value}, "
                f"http={outcome.status_code or 0}"
            )
        if type(outcome.value) is not dict:
            raise DiscoveryFetchError(
                f"required discovery API has no JSON object: {canonical}"
            )
        body_hash = stable_payload_hash(outcome.value)
        if outcome.payload_hash is not None and outcome.payload_hash != body_hash:
            raise DiscoveryFetchError(
                f"transport JSON payload hash mismatch: {canonical}"
            )
        if self._raw_required and (
            not outcome.raw_capture_id or not outcome.raw_body_hash
        ):
            raise DiscoveryFetchError(
                f"required discovery API has no raw lineage: {canonical}"
            )
        self._checkpoint[canonical] = {
            "attempts": outcome.attempts,
            "value": outcome.value,
            "decoded_body_bytes": outcome.decoded_body_bytes,
            "payload_hash": body_hash,
            "raw_capture_id": outcome.raw_capture_id,
            "raw_body_hash": outcome.raw_body_hash,
            "status": FetchStatus.OK.value,
            "status_code": 200,
        }
        document = _JsonDocument(
            url=canonical,
            value=outcome.value,
            payload_hash=body_hash,
            source_body_hash=str(outcome.raw_body_hash or body_hash),
            raw_capture_id=outcome.raw_capture_id,
        )
        self._api_documents[canonical] = document
        return document

    @staticmethod
    def _soup(document: _Document) -> BeautifulSoup:
        soup = BeautifulSoup(document.body, "html.parser")
        if soup.html is None or soup.body is None:
            raise DiscoverySchemaError(
                f"required page is not a complete HTML document: {document.url}"
            )
        return soup

    def discover(self) -> tuple[RegistryPage, ...]:
        listing_documents: dict[str, _Document] = {}
        candidates: dict[str, _CompetitionCandidate] = {}
        pending = list(SEED_URLS)
        queued = set(pending)
        listing_family_pages: dict[str, set[int]] = {}
        while pending:
            url = pending.pop(0)
            document = self._get(url)
            soup = self._soup(document)
            listing_documents[document.url] = document
            family = urlsplit(document.url).path.rstrip("/")
            listing_family_pages.setdefault(family, set()).add(
                _listing_page_number(
                    document.url,
                    limit=self._limits.listing_pages,
                )
            )
            for candidate in _listing_candidates(
                soup,
                page_url=document.url,
                pagination_limit=self._limits.listing_pages,
            ):
                _merge_candidate(candidates, candidate)
                if len(candidates) > self._limits.competitions:
                    raise DiscoverySchemaError(
                        f"competition bound exceeded ({self._limits.competitions})"
                    )
            for linked_url in _listing_links(
                soup,
                document.url,
                pagination_limit=self._limits.listing_pages,
            ):
                if linked_url not in queued:
                    if len(queued) >= self._limits.listing_pages:
                        raise DiscoverySchemaError(
                            "listing-page bound exceeded "
                            f"({self._limits.listing_pages})"
                        )
                    queued.add(linked_url)
                    pending.append(linked_url)
            pending.sort()

        if not candidates:
            raise DiscoverySchemaError("complete catalog contains no competitions")
        unnamed = sorted(
            item.competition_id for item in candidates.values() if not item.name
        )
        if unnamed:
            raise DiscoverySchemaError(
                "competitions have only numeric/metric labels: " + ", ".join(unnamed)
            )
        for family, pages in listing_family_pages.items():
            expected = set(range(1, max(pages) + 1))
            if pages != expected:
                raise DiscoverySchemaError(
                    f"incomplete listing pagination for {family}: "
                    f"missing={sorted(expected - pages)}, "
                    f"extra={sorted(pages - expected)}"
                )

        profiles: dict[str, tuple[_Document, BeautifulSoup]] = {}
        for candidate in sorted(
            candidates.values(), key=lambda item: item.competition_id
        ):
            document = self._get(candidate.profile_url)
            soup = self._soup(document)
            if not _has_season_markup(soup):
                canonical = _canonical_profile_route(soup, candidate.profile_url)
                if canonical is not None:
                    document = self._get(canonical)
                    soup = self._soup(document)
                    identity = _profile_identity(canonical)
                    if identity is None or identity[0] != candidate.competition_id:
                        raise DiscoverySchemaError(
                            f"canonical route changes identity: {canonical}"
                        )
                    candidate.slug = identity[1]
                    candidate.profile_url = canonical
            declared_id = soup.select_one("[data-competition-id]")
            if (
                declared_id is not None
                and str(declared_id.get("data-competition-id"))
                != candidate.competition_id
            ):
                raise DiscoverySchemaError(
                    f"profile identity mismatch: {candidate.profile_url}"
                )
            profiles[candidate.competition_id] = (document, soup)

        options_by_competition: dict[
            str, tuple[tuple[str, str, bool, Mapping[str, Any]], ...]
        ] = {}
        edition_documents: dict[
            tuple[str, str], _Document | _JsonDocument
        ] = {}
        participants_by_edition: dict[
            tuple[str, str], tuple[_ParticipantCandidate, ...]
        ] = {}
        participant_ids_by_edition: dict[tuple[str, str], tuple[str, ...]] = {}
        api_regulation_documents: dict[str, _JsonDocument] = {}
        edition_count = 0
        participant_count = 0
        for competition_id, candidate in sorted(candidates.items()):
            profile_document, profile_soup = profiles[competition_id]
            if self._fetch_json is not None:
                regulation_document = self._get_json(
                    _regulation_url(competition_id), endpoint="competition_regulation"
                )
                options = _regulation_options(
                    regulation_document,
                    competition_id=competition_id,
                    profile_url=candidate.profile_url,
                )
                _component_season_id(
                    profile_soup,
                    competition_id=competition_id,
                    source_url=candidate.profile_url,
                )
                # Some national tournaments expose the cyclical display year
                # in this component (FIWC uses 2026 while the API key is 2025),
                # so regulation remains the authoritative edition inventory.
                api_regulation_documents[competition_id] = regulation_document
            else:
                options = _selector_options(
                    profile_soup, profile_url=candidate.profile_url
                )
            edition_count += len(options)
            if edition_count > self._limits.editions:
                raise DiscoverySchemaError(
                    f"edition bound exceeded ({self._limits.editions})"
                )
            options_by_competition[competition_id] = options
            for edition_id, _label, current, _attrs in options:
                edition_source_url = _edition_url(candidate.profile_url, edition_id)
                if self._fetch_json is not None:
                    edition_document = api_regulation_documents[competition_id]
                    exact_ids = _api_participant_ids(
                        self._get_json(
                            _participants_url(competition_id, edition_id),
                            endpoint="competition_participants",
                        ),
                        competition_id=competition_id,
                        edition_id=edition_id,
                        limit=self._limits.participants_per_edition,
                    )
                    participant_ids_by_edition[(competition_id, edition_id)] = exact_ids
                    participant_count += len(exact_ids)
                else:
                    if current:
                        edition_document, edition_soup = profile_document, profile_soup
                    else:
                        edition_document = self._get(edition_source_url)
                        edition_soup = self._soup(edition_document)
                    _validate_edition_document(
                        edition_soup,
                        competition_id=competition_id,
                        edition_id=edition_id,
                        source_url=edition_source_url,
                    )
                    exact_participants = _participant_candidates(
                        edition_soup,
                        source_url=edition_source_url,
                        limit=self._limits.participants_per_edition,
                    )
                    participants_by_edition[(competition_id, edition_id)] = (
                        exact_participants
                    )
                    participant_count += len(exact_participants)
                edition_documents[(competition_id, edition_id)] = edition_document
                if participant_count > self._limits.participants:
                    raise DiscoverySchemaError(
                        f"participant bound exceeded ({self._limits.participants})"
                    )
        team_national_by_id: dict[str, bool] = {}
        if self._fetch_json is not None:
            all_team_ids = {
                team_id
                for team_ids in participant_ids_by_edition.values()
                for team_id in team_ids
            }
            teams_by_id: dict[str, _TeamEntity] = {}
            ordered_ids = sorted(all_team_ids, key=int)
            for offset in range(0, len(ordered_ids), _API_ENTITY_BATCH_SIZE):
                batch = tuple(ordered_ids[offset : offset + _API_ENTITY_BATCH_SIZE])
                entity_document = self._get_json(
                    _clubs_url(batch), endpoint="competition_participant_entities"
                )
                entities = _api_team_entities(entity_document, requested_ids=batch)
                teams_by_id.update(entities)
                team_national_by_id.update(
                    {team_id: entity.is_national_team for team_id, entity in entities.items()}
                )
            for key, team_ids in participant_ids_by_edition.items():
                participants_by_edition[key] = tuple(
                    _ParticipantCandidate(
                        team_id=team_id,
                        team_name=teams_by_id[team_id].team_name,
                        source_url=teams_by_id[team_id].source_url,
                    )
                    for team_id in team_ids
                )

        snapshot_material = {
            "pages": {
                url: document.payload_hash
                for url, document in sorted(self._documents.items())
            },
            "api": {
                url: document.source_body_hash
                for url, document in sorted(self._api_documents.items())
            },
            "parser_revision": PARSER_REVISION,
            "schema_revision": SCHEMA_REVISION,
        }
        snapshot_digest = hashlib.sha256(
            json.dumps(
                snapshot_material, separators=(",", ":"), sort_keys=True
            ).encode()
        ).hexdigest()
        snapshot_id = "tm-discovery-" + snapshot_digest[:24]
        discovered_at = self._clock()
        if discovered_at.tzinfo is None or discovered_at.utcoffset() is None:
            raise DiscoverySchemaError("discovery clock must be timezone-aware")

        competition_records: dict[str, CompetitionRecord] = {}
        edition_records: dict[str, tuple[EditionRecord, ...]] = {}
        participant_records: dict[str, tuple[CompetitionParticipant, ...]] = {}
        for competition_id, candidate in sorted(candidates.items()):
            profile_document, _profile_soup = profiles[competition_id]
            options = options_by_competition[competition_id]
            season_format = _season_format(options, candidate.profile_url)
            season_evidence = ClassificationEvidence(
                source_field="edition_selector",
                source_value=",".join(item[1] for item in options),
                source_url=candidate.profile_url,
                origin=EvidenceOrigin.STRUCTURED,
                season_format=season_format,
            )
            evidence = tuple(candidate.evidence) + (season_evidence,)
            competition_type = _unique_signal(
                evidence,
                "competition_type",
                CompetitionType.UNKNOWN,
            )
            gender = _unique_signal(evidence, "gender", Gender.UNKNOWN)
            team_type = _unique_signal(evidence, "team_type", TeamType.UNKNOWN)
            age_category = _unique_signal(evidence, "age_category", AgeCategory.UNKNOWN)
            try:
                canonical_id = resolve_competition(
                    competition_id
                ).canonical_competition_id
            except UnknownCompetitionError:
                canonical_id = None
            competition_records[competition_id] = CompetitionRecord(
                competition_id=competition_id,
                slug=candidate.slug,
                name=candidate.name,
                country=candidate.country,
                confederation=candidate.confederation,
                competition_type=competition_type,
                gender=gender,
                team_type=team_type,
                age_category=age_category,
                season_format=season_format,
                active=True,
                source_url=candidate.profile_url,
                discovered_at=discovered_at,
                canonical_competition_id=canonical_id,
                evidence=evidence,
                registry_snapshot_id=snapshot_id,
                source_body_hash=profile_document.source_body_hash,
                raw_capture_id=profile_document.raw_capture_id,
                parser_revision=PARSER_REVISION,
                schema_revision=SCHEMA_REVISION,
            )

            editions = []
            competition_participants: list[CompetitionParticipant] = []
            for edition_id, label, current, attrs in options:
                edition_source_url = _edition_url(candidate.profile_url, edition_id)
                edition_document = edition_documents[(competition_id, edition_id)]
                edition_format = _label_season_format(label, candidate.profile_url)
                if self._fetch_json is not None:
                    expected_national = team_type is TeamType.NATIONAL_TEAM
                    for item in participant_ids_by_edition[(competition_id, edition_id)]:
                        if team_national_by_id[item] is not expected_national:
                            raise DiscoverySchemaError(
                                "participant team taxonomy disagrees with competition: "
                                f"{competition_id}/{edition_id}/{item}"
                            )
                exact_participants = tuple(
                    CompetitionParticipant(
                        competition_id=competition_id,
                        edition_id=edition_id,
                        team_id=item.team_id,
                        team_name=item.team_name,
                        source_url=item.source_url,
                        discovered_at=discovered_at,
                        registry_snapshot_id=snapshot_id,
                        source_body_hash=edition_document.source_body_hash,
                        raw_capture_id=edition_document.raw_capture_id,
                        parser_revision=PARSER_REVISION,
                        schema_revision=SCHEMA_REVISION,
                    )
                    for item in participants_by_edition[(competition_id, edition_id)]
                )
                competition_participants.extend(exact_participants)
                editions.append(
                    EditionRecord(
                        competition_id=competition_id,
                        edition_id=edition_id,
                        edition_label=label,
                        canonical_season=canonical_season(label, edition_format),
                        season_format=edition_format,
                        start_date=attrs.get("data-start-date"),
                        end_date=attrs.get("data-end-date"),
                        active="disabled" not in attrs,
                        current=current,
                        participant_count=len(exact_participants),
                        participant_hash=participant_list_hash(exact_participants),
                        source_url=edition_source_url,
                        discovered_at=discovered_at,
                        registry_snapshot_id=snapshot_id,
                        source_body_hash=edition_document.source_body_hash,
                        raw_capture_id=edition_document.raw_capture_id,
                        parser_revision=PARSER_REVISION,
                        schema_revision=SCHEMA_REVISION,
                    )
                )
            edition_records[competition_id] = tuple(editions)
            participant_records[competition_id] = tuple(competition_participants)

        listing_urls = tuple(sorted(listing_documents))
        page_number = {url: index + 1 for index, url in enumerate(listing_urls)}
        pages = []
        for url in listing_urls:
            owned_ids = sorted(
                competition_id
                for competition_id, candidate in candidates.items()
                if candidate.owner_url == url
            )
            pages.append(
                RegistryPage(
                    snapshot_id=snapshot_id,
                    page_number=page_number[url],
                    page_count=len(listing_urls),
                    source_url=url,
                    source_body_hash=listing_documents[url].payload_hash,
                    competitions=tuple(
                        competition_records[competition_id]
                        for competition_id in owned_ids
                    ),
                    editions=tuple(
                        edition
                        for competition_id in owned_ids
                        for edition in edition_records[competition_id]
                    ),
                    participants=tuple(
                        participant
                        for competition_id in owned_ids
                        for participant in participant_records[competition_id]
                    ),
                )
            )
        return tuple(pages)


def discover_competition_registry(
    *,
    fetch: Callable[[str], FetchOutcome[str]],
    fetch_json: Optional[
        Callable[[str, str], FetchOutcome[Mapping[str, Any]]]
    ] = None,
    checkpoint: MutableMapping[str, Any],
    traffic_ledger: TrafficLedger,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    limits: DiscoveryLimits = DEFAULT_DISCOVERY_LIMITS,
) -> tuple[RegistryPage, ...]:
    """Convenience API for one complete fail-closed discovery snapshot."""

    return TransfermarktCompetitionDiscovery(
        fetch=fetch,
        fetch_json=fetch_json,
        checkpoint=checkpoint,
        traffic_ledger=traffic_ledger,
        clock=clock,
        limits=limits,
    ).discover()


__all__ = [
    "BASE_URL",
    "CATALOGUE_ROUTE",
    "DEFAULT_DISCOVERY_LIMITS",
    "SEED_ROUTES",
    "SEED_URLS",
    "DiscoveryCheckpointError",
    "DiscoveryError",
    "DiscoveryFetchError",
    "DiscoveryLimits",
    "DiscoverySchemaError",
    "TrafficLedger",
    "TransfermarktCompetitionDiscovery",
    "discover_competition_registry",
]
