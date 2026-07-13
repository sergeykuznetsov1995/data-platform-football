"""Offline-testable HTML discovery for the Transfermarkt registry.

The adapter owns traversal and parsing, not HTTP.  Callers inject a proxy-only
``fetch(url) -> FetchOutcome[str]`` function, a persistent mutable checkpoint,
and the same shared traffic ledger used by that transport.  Any failed
required page or structural drift aborts the whole snapshot.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional, Protocol
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag

from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from scrapers.transfermarkt.registry import (
    AgeCategory,
    ClassificationEvidence,
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
    resolve_competition,
)


BASE_URL = "https://www.transfermarkt.com"
SEED_ROUTES: tuple[str, ...] = (
    "/navigation/wettbewerbe",
    "/wettbewerbe/europa",
    "/wettbewerbe/amerika",
    "/wettbewerbe/asien",
    "/wettbewerbe/afrika",
    "/wettbewerbe/fifa",
)
SEED_URLS: tuple[str, ...] = tuple(BASE_URL + route for route in SEED_ROUTES)

_COUNTRY_ROUTE_RE = re.compile(
    r"^/wettbewerbe/national/wettbewerbe/[A-Za-z0-9_-]+(?:/.*)?$"
)
_COMPETITION_ROUTE_RE = re.compile(
    r"^/(?P<slug>[^/?#]+)/(?:[^?#]*/)?(?P<section>[^/?#]+)/"
    r"(?P<kind>pokalwettbewerb|wettbewerb)/"
    r"(?P<competition_id>[A-Za-z0-9_-]+)(?:/.*)?$"
)
_CANONICAL_SECTION = "startseite"
# The catalogue states a competition's taxonomy at three levels: a broad section
# heading, a group separator inside the tables, and the "National Team
# Competitions" section, which names the entrants themselves — a table group can
# say "cup" about a national-team tournament, but not that clubs play in it.
_SECTION_PRECEDENCE = 1
_GROUP_PRECEDENCE = 2
_ENTRANT_PRECEDENCE = 3
_EDITION_PATH_RE = re.compile(r"/saison_id/(?P<edition_id>\d{4})(?:/|$)")


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
    listing_hashes: set[str] = field(default_factory=set)
    evidence: list[ClassificationEvidence] = field(default_factory=list)


@dataclass(frozen=True)
class _Document:
    url: str
    body: str
    payload_hash: str


def _payload_hash(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8", errors="replace")).hexdigest()


def _normalise_text(value: Any) -> str:
    return " ".join(str(value).split())


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
    return min(
        (existing, candidate),
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


def _is_seed_listing(url: str) -> bool:
    parsed = urlsplit(url)
    path = parsed.path.rstrip("/")
    return path in SEED_ROUTES


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


def _is_listing_url(url: str) -> bool:
    return _is_seed_listing(url) or _is_country_listing(url)


def _section_signals(label: str) -> _SectionSignals:
    """Map Transfermarkt section taxonomy; never inspect a competition name."""

    normalised = _normalise_text(label).casefold()
    gender = (
        Gender.WOMEN
        if any(token in normalised for token in ("women", "frauen"))
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
        token in normalised
        for token in ("youth", "under-", "under ", "u21", "u19")
    )
    age_category = AgeCategory.UXX if youth else AgeCategory.SENIOR
    if any(token in normalised for token in ("national cups", "domestic cups")):
        return _SectionSignals(
            competition_type=CompetitionType.DOMESTIC_CUP,
            gender=gender,
            team_type=TeamType.CLUB,
            age_category=age_category,
        )
    if any(token in normalised for token in ("national leagues", "domestic leagues")):
        return _SectionSignals(
            competition_type=CompetitionType.DOMESTIC_LEAGUE,
            gender=gender,
            team_type=TeamType.CLUB,
            age_category=age_category,
        )
    if any(
        token in normalised
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
    if any(
        token in normalised
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
    if "reserve" in normalised:
        return _SectionSignals(
            competition_type=CompetitionType.DOMESTIC_LEAGUE,
            team_type=TeamType.RESERVE,
            age_category=AgeCategory.SENIOR,
        )
    age_category = (
        AgeCategory.UXX if "youth" in normalised else AgeCategory.SENIOR
    )
    if "cup" in normalised:
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
    country = (
        (country_meta.get("content") if country_meta else None)
        or (body.get("data-country") if body else None)
    )
    confederation = (
        (confed_meta.get("content") if confed_meta else None)
        or (body.get("data-confederation") if body else None)
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
_NAME_WOMEN_RE = re.compile(r"\bwomen(?:'s)?\b|\bfrauen\b|\bfeminin\w*\b", re.IGNORECASE)


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


def _listing_links(soup: BeautifulSoup, page_url: str) -> tuple[str, ...]:
    links: set[str] = set()
    for anchor in soup.select("a[href]"):
        if _in_site_chrome(anchor):
            continue
        canonical = _canonical_url(str(anchor.get("href")), base_url=page_url)
        if canonical is None:
            continue
        if _is_seed_listing(canonical) or _is_country_listing(canonical):
            if _has_listing_query_only(canonical):
                links.add(canonical)
            continue
        classes = set(anchor.get("class", ()))
        parent_classes = set(anchor.parent.get("class", ())) if anchor.parent else set()
        if (
            anchor.has_attr("data-page")
            or classes.intersection({"page-link", "tm-pagination-link"})
            or parent_classes.intersection({"page-item", "tm-pagination"})
        ):
            parsed = urlsplit(canonical)
            base_path = urlsplit(page_url).path.rstrip("/")
            if parsed.path.rstrip("/") == base_path:
                links.add(canonical)
    return tuple(sorted(links))


def _listing_candidates(
    soup: BeautifulSoup,
    *,
    page_url: str,
    page_hash: str,
) -> tuple[_CompetitionCandidate, ...]:
    context = _listing_context(soup, page_url)
    candidates: list[_CompetitionCandidate] = []
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
        image = anchor.find("img")
        name = _normalise_text(
            anchor.get_text(" ", strip=True)
            or anchor.get("title")
            or (image.get("alt") if image else "")
            or (image.get("title") if image else "")
        )
        if not name:
            raise DiscoverySchemaError(
                f"competition link has no name: {page_url} -> {profile_url}"
            )
        section_label = _section_label(anchor)
        group_label = _row_group_label(anchor)
        stated = (
            (section_label, _section_signals(section_label),
             _section_precedence(section_label)),
            (group_label, _group_signals(group_label), _GROUP_PRECEDENCE),
        )
        country = _normalise_text(anchor.get("data-country") or context.country)
        confederation = _normalise_text(
            anchor.get("data-confederation") or context.confederation
        )
        # A source section that explicitly says women is authoritative audience
        # evidence.  Adding the catalog's default men's signal as well would
        # manufacture a conflict and block the whole registry instead of
        # source-backed exclusion of that competition.
        evidence = []
        excluded_by_name = _name_exclusion_evidence(name, page_url)
        if excluded_by_name is not None:
            evidence.append(excluded_by_name)
        if all(signals.gender is not Gender.WOMEN for _, signals, _p in stated):
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
                listing_hashes={page_hash},
                evidence=evidence,
            )
        )

    body = soup.body
    explicit_empty = body is not None and str(
        body.get("data-registry-empty", "")
    ).casefold() == "true"
    has_navigation = bool(_listing_links(soup, page_url))
    if not seen_links and not has_navigation and not explicit_empty:
        raise DiscoverySchemaError(
            f"listing page has no registry structure: {page_url}"
        )
    if seen_links and _is_country_listing(page_url) and (
        context.country == "Unknown" or context.confederation == "Unknown"
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
    existing.listing_hashes.update(candidate.listing_hashes)
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
    )


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
    raise DiscoverySchemaError(
        f"unrecognised edition label {label!r}: {profile_url}"
    )


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
        edition_id
        for edition_id, _label, selected, _attrs in options
        if selected
    ]
    if len(current) != 1:
        raise DiscoverySchemaError(
            f"edition selector must mark exactly one current edition: {profile_url}"
        )
    return formats[current[0]]


def _unique_signal(evidence: Iterable[ClassificationEvidence], name: str, unknown):
    values = narrowest_signals(evidence, name)
    return next(iter(values)) if len(values) == 1 else unknown


class TransfermarktCompetitionDiscovery:
    """Traverse all official competition catalogs into complete registry pages."""

    def __init__(
        self,
        *,
        fetch: Callable[[str], FetchOutcome[str]],
        checkpoint: MutableMapping[str, Any],
        traffic_ledger: TrafficLedger,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        if traffic_ledger is None:
            raise TypeError("traffic_ledger is required")
        self._fetch = fetch
        self._checkpoint = checkpoint
        self._traffic_ledger = traffic_ledger
        self._clock = clock
        self._documents: dict[str, _Document] = {}

    def _get(self, url: str) -> _Document:
        canonical = _canonical_url(url)
        if canonical is None:
            raise DiscoveryFetchError(f"non-Transfermarkt URL: {url!r}")
        cached = self._checkpoint.get(canonical)
        if cached is not None:
            if not isinstance(cached, Mapping):
                raise DiscoveryCheckpointError(
                    f"checkpoint entry is not an object: {canonical}"
                )
            body = cached.get("body")
            expected_hash = cached.get("payload_hash")
            if cached.get("status") != FetchStatus.OK.value or not isinstance(body, str):
                raise DiscoveryCheckpointError(
                    f"checkpoint is not an authoritative success: {canonical}"
                )
            actual_hash = _payload_hash(body)
            if expected_hash != actual_hash:
                raise DiscoveryCheckpointError(
                    f"checkpoint payload hash mismatch: {canonical}"
                )
            self._traffic_ledger.record_cache_hit(
                entity="competition_registry", duration_seconds=0.0
            )
            document = _Document(canonical, body, actual_hash)
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
        self._checkpoint[canonical] = {
            "attempts": outcome.attempts,
            "body": outcome.value,
            "decoded_body_bytes": outcome.decoded_body_bytes,
            "payload_hash": body_hash,
            "status": FetchStatus.OK.value,
            "status_code": 200,
        }
        document = _Document(canonical, outcome.value, body_hash)
        self._documents[canonical] = document
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
        while pending:
            url = pending.pop(0)
            document = self._get(url)
            soup = self._soup(document)
            listing_documents[url] = document
            for candidate in _listing_candidates(
                soup, page_url=url, page_hash=document.payload_hash
            ):
                _merge_candidate(candidates, candidate)
            for linked_url in _listing_links(soup, url):
                if linked_url not in queued:
                    queued.add(linked_url)
                    pending.append(linked_url)
            pending.sort()

        if not candidates:
            raise DiscoverySchemaError("complete catalog contains no competitions")

        profiles: dict[str, tuple[_Document, BeautifulSoup]] = {}
        for candidate in sorted(candidates.values(), key=lambda item: item.competition_id):
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
            if declared_id is not None and str(
                declared_id.get("data-competition-id")
            ) != candidate.competition_id:
                raise DiscoverySchemaError(
                    f"profile identity mismatch: {candidate.profile_url}"
                )
            profiles[candidate.competition_id] = (document, soup)

        snapshot_material = {
            url: document.payload_hash
            for url, document in sorted(self._documents.items())
        }
        snapshot_digest = hashlib.sha256(
            json.dumps(snapshot_material, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest()
        snapshot_id = "tm-discovery-" + snapshot_digest[:24]
        discovered_at = self._clock()
        if discovered_at.tzinfo is None or discovered_at.utcoffset() is None:
            raise DiscoverySchemaError("discovery clock must be timezone-aware")

        competition_records: dict[str, CompetitionRecord] = {}
        edition_records: dict[str, tuple[EditionRecord, ...]] = {}
        editionless: list[str] = []
        for competition_id, candidate in sorted(candidates.items()):
            profile_document, profile_soup = profiles[competition_id]
            try:
                options = _selector_options(
                    profile_soup, profile_url=candidate.profile_url
                )
            except DiscoverySchemaError as exc:
                if "edition selector missing" not in str(exc):
                    raise
                # The source publishes no edition at all for these — a Brazilian
                # relegation play-off, Japan's "100 Year Vision" leagues — so
                # there is nothing to crawl and nothing to register.
                editionless.append(competition_id)
                continue
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
            age_category = _unique_signal(
                evidence, "age_category", AgeCategory.UNKNOWN
            )
            try:
                canonical_id = resolve_competition(competition_id).canonical_competition_id
            except UnknownCompetitionError:
                canonical_id = None
            combined_hash = hashlib.sha256(
                "|".join(
                    sorted(candidate.listing_hashes | {profile_document.payload_hash})
                ).encode()
            ).hexdigest()
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
                source_body_hash=combined_hash,
                parser_revision="tm-html-discovery-v1",
                schema_revision="1",
            )

            editions = []
            for edition_id, label, current, attrs in options:
                edition_source_url = (
                    candidate.profile_url.rstrip("/") + f"/saison_id/{edition_id}"
                )
                edition_format = _label_season_format(label, candidate.profile_url)
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
                        participant_count=attrs.get("data-participant-count"),
                        participant_hash=attrs.get("data-participant-hash"),
                        source_url=edition_source_url,
                        discovered_at=discovered_at,
                        registry_snapshot_id=snapshot_id,
                        source_body_hash=profile_document.payload_hash,
                        parser_revision="tm-html-discovery-v1",
                        schema_revision="1",
                    )
                )
            edition_records[competition_id] = tuple(editions)

        for competition_id in editionless:
            candidates.pop(competition_id, None)
        if not candidates:
            raise DiscoverySchemaError("complete catalog contains no competitions")

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
                )
            )
        return tuple(pages)


def discover_competition_registry(
    *,
    fetch: Callable[[str], FetchOutcome[str]],
    checkpoint: MutableMapping[str, Any],
    traffic_ledger: TrafficLedger,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> tuple[RegistryPage, ...]:
    """Convenience API for one complete fail-closed discovery snapshot."""

    return TransfermarktCompetitionDiscovery(
        fetch=fetch,
        checkpoint=checkpoint,
        traffic_ledger=traffic_ledger,
        clock=clock,
    ).discover()


__all__ = [
    "BASE_URL",
    "SEED_ROUTES",
    "SEED_URLS",
    "DiscoveryCheckpointError",
    "DiscoveryError",
    "DiscoveryFetchError",
    "DiscoverySchemaError",
    "TrafficLedger",
    "TransfermarktCompetitionDiscovery",
    "discover_competition_registry",
]
