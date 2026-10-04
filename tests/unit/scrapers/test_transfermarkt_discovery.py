from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scrapers.transfermarkt.discovery import (
    BASE_URL,
    SEED_ROUTES,
    SEED_URLS,
    Country,
    DiscoveryCheckpointError,
    DiscoveryError,
    DiscoveryFetchError,
    DiscoverySchemaError,
    ExtraCompetition,
    PreviousRegistry,
    discover_competition_registry,
    load_countries,
)
from scrapers.transfermarkt.tmapi import competition_regulation_url
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from scrapers.transfermarkt.registry import (
    ClassificationEvidence,
    ClassificationStatus,
    CompetitionType,
    EvidenceOrigin,
    Gender,
    AgeCategory,
    SeasonFormat,
    TeamType,
    reconcile_registry_pages,
)


FIXTURES = Path(__file__).parents[2] / "fixtures" / "transfermarkt" / "discovery"
NOW = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)


URL_FIXTURES = {
    BASE_URL + "/navigation/wettbewerbe": "navigation.html",
    BASE_URL + "/wettbewerbe/europa": "europa.html",
    BASE_URL + "/wettbewerbe/europa?page=2": "europa_page_2.html",
    BASE_URL + "/wettbewerbe/amerika": "amerika.html",
    BASE_URL + "/wettbewerbe/asien": "asien.html",
    BASE_URL + "/wettbewerbe/afrika": "afrika.html",
    BASE_URL + "/wettbewerbe/fifa": "fifa.html",
    BASE_URL + "/wettbewerbe/national/wettbewerbe/189": "england.html",
    BASE_URL
    + "/wettbewerbe/national/wettbewerbe/189?page=2": "england_page_2.html",
    BASE_URL + "/premier-league/startseite/wettbewerb/GB1": "profile_gb1.html",
    BASE_URL
    + "/womens-super-league/startseite/wettbewerb/GB1W": "profile_gb1w.html",
    BASE_URL + "/fa-cup/startseite/pokalwettbewerb/FAC": "profile_fac.html",
    BASE_URL
    + "/uefa-champions-league/startseite/pokalwettbewerb/CL": "profile_cl.html",
    BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN": "profile_afcn.html",
    BASE_URL
    + "/uefa-nations-league-a/startseite/pokalwettbewerb/UNLA": "profile_unla.html",
    BASE_URL + "/world-cup/startseite/wettbewerb/FIWC": "profile_fiwc.html",
    BASE_URL
    + "/mens-senior-mystery-league/startseite/wettbewerb/MYSTERY": "profile_mystery.html",
}


class LedgerSpy:
    def __init__(self) -> None:
        self.ensure_calls = 0
        self.cache_hits = 0
        self.cache_entities: list[str] = []

    def ensure_request_allowed(self) -> None:
        self.ensure_calls += 1

    def record_cache_hit(self, *, entity: str, duration_seconds: float) -> None:
        assert duration_seconds == 0.0
        self.cache_hits += 1
        self.cache_entities.append(entity)


class FixtureFetch:
    def __init__(self, overrides=None) -> None:
        self.calls: list[str] = []
        self.overrides = overrides or {}

    def __call__(self, url: str) -> FetchOutcome[str]:
        self.calls.append(url)
        if url in self.overrides:
            override = self.overrides[url]
            if isinstance(override, FetchOutcome):
                return override
            body = override
        else:
            body = (FIXTURES / URL_FIXTURES[url]).read_text(encoding="utf-8")
        payload_hash = hashlib.sha256(body.encode()).hexdigest()
        return FetchOutcome(
            status=FetchStatus.OK,
            value=body,
            status_code=200,
            attempts=1,
            label="competition_registry",
            decoded_body_bytes=len(body.encode()),
            payload_hash=payload_hash,
        )


def _discover(fetch=None, checkpoint=None, ledger=None):
    fetch = fetch or FixtureFetch()
    checkpoint = {} if checkpoint is None else checkpoint
    ledger = ledger or LedgerSpy()
    pages = discover_competition_registry(
        fetch=fetch,
        checkpoint=checkpoint,
        traffic_ledger=ledger,
        clock=lambda: NOW,
    )
    return pages, fetch, checkpoint, ledger


def test_official_seed_routes_are_complete_and_fixed() -> None:
    assert SEED_ROUTES == (
        "/navigation/wettbewerbe",
        "/wettbewerbe/europa",
        "/wettbewerbe/amerika",
        "/wettbewerbe/asien",
        "/wettbewerbe/afrika",
        "/wettbewerbe/fifa",
    )
    assert SEED_URLS == tuple(BASE_URL + route for route in SEED_ROUTES)


def test_discovery_prefers_canonical_route_over_legacy_alias_for_same_id() -> None:
    navigation = (FIXTURES / "navigation.html").read_text(encoding="utf-8")
    alias = (
        '<a href="/weltmeisterschaft/startseite/pokalwettbewerb/FIWC">'
        "World Cup 2026</a>"
    )
    canonical = (
        '<a href="/world-cup/startseite/wettbewerb/FIWC">World Cup</a>'
    )
    navigation = navigation.replace("</body>", alias + canonical + "</body>")

    pages, fetch, *_ = _discover(
        fetch=FixtureFetch(
            {BASE_URL + "/navigation/wettbewerbe": navigation}
        )
    )
    snapshot = reconcile_registry_pages(pages)
    world_cup = next(
        item for item in snapshot.competitions if item.competition_id == "FIWC"
    )

    assert world_cup.slug == "world-cup"
    assert world_cup.name == "FIFA World Cup"
    assert world_cup.source_url == (
        BASE_URL + "/world-cup/startseite/wettbewerb/FIWC"
    )
    assert BASE_URL + "/weltmeisterschaft/startseite/pokalwettbewerb/FIWC" not in (
        fetch.calls
    )


def test_discovery_prefers_profile_section_over_secondary_tab_for_same_id() -> None:
    england = (FIXTURES / "england.html").read_text(encoding="utf-8")
    secondary = (
        '<a href="/premier-league/gastarbeiter/wettbewerb/GB1">Premier League</a>'
    )
    england = england.replace("</body>", secondary + "</body>")

    pages, fetch, *_ = _discover(
        fetch=FixtureFetch(
            {BASE_URL + "/wettbewerbe/national/wettbewerbe/189": england}
        )
    )
    snapshot = reconcile_registry_pages(pages)
    premier_league = next(
        item for item in snapshot.competitions if item.competition_id == "GB1"
    )

    assert premier_league.source_url == (
        BASE_URL + "/premier-league/startseite/wettbewerb/GB1"
    )
    assert BASE_URL + "/premier-league/gastarbeiter/wettbewerb/GB1" not in fetch.calls


def test_discovery_resolves_renamed_slug_aliases_for_same_id() -> None:
    navigation = (FIXTURES / "navigation.html").read_text(encoding="utf-8")
    historical = (
        '<a href="/torneo-intermedio/startseite/wettbewerb/GB1">3</a>'
    )
    navigation = navigation.replace("</body>", historical + "</body>")

    pages, fetch, *_ = _discover(
        fetch=FixtureFetch({BASE_URL + "/navigation/wettbewerbe": navigation})
    )
    snapshot = reconcile_registry_pages(pages)
    premier_league = next(
        item for item in snapshot.competitions if item.competition_id == "GB1"
    )

    assert premier_league.slug == "premier-league"
    assert premier_league.name == "Premier League"
    assert BASE_URL + "/torneo-intermedio/startseite/wettbewerb/GB1" not in fetch.calls


def test_discovery_follows_the_canonical_route_when_a_profile_has_no_seasons() -> None:
    afrika = (FIXTURES / "afrika.html").read_text(encoding="utf-8")
    generic_route = '<a href="/afrika-cup/startseite/wettbewerb/AFCN">Africa Cup</a>'
    afrika = afrika.replace("</body>", generic_route + "</body>")
    season_less = (
        '<!doctype html><html lang="en"><head>'
        '<link rel="canonical" '
        'href="https://www.transfermarkt.com/afrika-cup/startseite/pokalwettbewerb/AFCN">'
        '</head><body><h1 data-competition-id="AFCN">Africa Cup of Nations</h1>'
        "</body></html>"
    )

    pages, fetch, *_ = _discover(
        fetch=FixtureFetch(
            {
                BASE_URL + "/wettbewerbe/afrika": afrika,
                BASE_URL + "/afrika-cup/startseite/wettbewerb/AFCN": season_less,
            }
        )
    )
    snapshot = reconcile_registry_pages(pages)
    afcn = next(
        item for item in snapshot.competitions if item.competition_id == "AFCN"
    )

    assert afcn.source_url == (
        BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN"
    )
    assert BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN" in fetch.calls
    editions = [e for e in snapshot.editions if e.competition_id == "AFCN"]
    assert len(editions) == 2


def test_discovery_keeps_the_format_of_each_edition_when_it_changed() -> None:
    profile = (
        '<!doctype html><html lang="en"><body>'
        '<h1 data-competition-id="GB1">Premier League</h1>'
        '<select name="saison_id">'
        '<option value="2025" selected>25/26</option>'
        '<option value="1899">1899/00</option>'
        '<option value="1977">1977</option>'
        "</select></body></html>"
    )

    pages, *_ = _discover(
        fetch=FixtureFetch(
            {BASE_URL + "/premier-league/startseite/wettbewerb/GB1": profile}
        )
    )
    snapshot = reconcile_registry_pages(pages)
    competition = next(
        item for item in snapshot.competitions if item.competition_id == "GB1"
    )
    editions = {
        item.edition_id: item
        for item in snapshot.editions
        if item.competition_id == "GB1"
    }

    assert competition.season_format is SeasonFormat.SPLIT_YEAR
    assert editions["2025"].season_format is SeasonFormat.SPLIT_YEAR
    assert editions["1899"].season_format is SeasonFormat.SPLIT_YEAR
    assert editions["1977"].season_format is SeasonFormat.SINGLE_YEAR


def test_discovery_reads_a_cups_only_edition_from_its_title() -> None:
    afrika = (FIXTURES / "afrika.html").read_text(encoding="utf-8")
    cup = (
        '<!doctype html><html lang="en"><head>'
        "<title>CAF Champions League 25/26 | Transfermarkt</title>"
        '</head><body><h1 data-competition-id="AFCN">CAF Champions League</h1>'
        "</body></html>"
    )

    pages, *_ = _discover(
        fetch=FixtureFetch(
            {BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN": cup}
        )
    )
    snapshot = reconcile_registry_pages(pages)
    editions = [item for item in snapshot.editions if item.competition_id == "AFCN"]

    assert len(editions) == 1
    assert editions[0].edition_id == "2025"
    assert editions[0].canonical_season == "2526"
    assert editions[0].current is True


@pytest.mark.parametrize(
    ("title", "edition_id", "season"),
    [
        # A calendar edition is keyed by the year before it, as leagues are.
        ("Africa Cup of Nations 2026", "2025", "2026"),
        # A two-digit split label is read by the century window, not 20xx.
        ("1992 King Fahd Cup 91/92", "1991", "9192"),
        # A stated century is kept.
        ("Campeonato Sudamericano 1920/21", "1920", "2021"),
    ],
)
def test_title_edition_id_is_the_source_saison_id(title, edition_id, season) -> None:
    cup = (
        '<!doctype html><html lang="en"><head>'
        f"<title>{title} | Transfermarkt</title>"
        '</head><body><h1 data-competition-id="AFCN">Cup</h1>'
        "</body></html>"
    )
    pages, *_ = _discover(
        fetch=FixtureFetch(
            {BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN": cup}
        )
    )
    snapshot = reconcile_registry_pages(pages)
    editions = [item for item in snapshot.editions if item.competition_id == "AFCN"]

    assert [(item.edition_id, item.canonical_season) for item in editions] == [
        (edition_id, season)
    ]
    assert editions[0].source_url.endswith(f"/saison_id/{edition_id}")


def test_discovery_drops_a_competition_the_source_never_staged() -> None:
    afrika = (FIXTURES / "afrika.html").read_text(encoding="utf-8")
    unstaged = (
        '<!doctype html><html lang="en"><head>'
        "<title>J1 100 Year Vision League | Transfermarkt</title>"
        '</head><body><h1 data-competition-id="AFCN">J1 League</h1>'
        "</body></html>"
    )

    pages, *_ = _discover(
        fetch=FixtureFetch(
            {BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN": unstaged}
        )
    )
    snapshot = reconcile_registry_pages(pages)

    assert not [
        item for item in snapshot.competitions if item.competition_id == "AFCN"
    ]
    assert snapshot.competitions


@pytest.mark.parametrize(
    "body",
    [
        # A split label spanning two years cannot be read as a season.
        '<!doctype html><html lang="en"><head>'
        "<title>Africa Cup of Nations 25/27 | Transfermarkt</title>"
        '</head><body><h1 data-competition-id="AFCN">Cup</h1></body></html>',
        # The profile declares another competition's identity.
        '<!doctype html><html lang="en"><head>'
        "<title>Africa Cup of Nations 2026 | Transfermarkt</title>"
        '</head><body><h1 data-competition-id="OTHER">Cup</h1></body></html>',
        # The canonical route points at another competition.
        '<!doctype html><html lang="en"><head>'
        "<title>Africa Cup of Nations 2026 | Transfermarkt</title>"
        '<link rel="canonical" href="https://www.transfermarkt.com/'
        'other-cup/startseite/pokalwettbewerb/OTHER">'
        '</head><body><h1>Cup</h1></body></html>',
    ],
)
def test_one_unreadable_profile_is_quarantined_and_reported(body) -> None:
    pages, *_ = _discover(
        fetch=FixtureFetch(
            {BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN": body}
        )
    )
    snapshot = reconcile_registry_pages(pages)

    assert "AFCN" in snapshot.quarantined_competition_ids
    assert "AFCN" not in {item.competition_id for item in snapshot.competitions}
    assert "AFCN" not in {item.competition_id for item in snapshot.editions}
    assert {"GB1", "CL"} <= {item.competition_id for item in snapshot.competitions}


def test_catalog_table_groups_classify_rows_the_section_only_brackets() -> None:
    listing = (
        '<!doctype html><html lang="en"><head>'
        '<meta name="tm-country" content="England">'
        '<meta name="tm-confederation" content="UEFA">'
        "</head><body>"
        '<div class="box"><h2>European leagues &amp; cups</h2>'
        '<table class="items"><tbody>'
        '<tr><td class="extrarow">First Tier</td></tr>'
        '<tr><td><a href="/premier-league/startseite/wettbewerb/GB1">'
        "Premier League</a></td></tr>"
        '<tr><td class="extrarow">Youth league</td></tr>'
        '<tr><td><a href="/u18-premier-league/startseite/wettbewerb/GB18">'
        "U18 Premier League</a></td></tr>"
        "</tbody></table></div></body></html>"
    )
    profile = (
        '<!doctype html><html lang="en"><body>'
        '<h1 data-competition-id="GB18">U18 Premier League</h1>'
        '<select name="saison_id"><option value="2025" selected>25/26</option>'
        "</select></body></html>"
    )

    pages, *_ = _discover(
        fetch=FixtureFetch(
            {
                BASE_URL + "/wettbewerbe/europa": listing,
                BASE_URL + "/u18-premier-league/startseite/wettbewerb/GB18": profile,
            }
        )
    )
    snapshot = reconcile_registry_pages(pages)
    by_id = {item.competition_id: item for item in snapshot.competitions}

    assert by_id["GB1"].classification_status is ClassificationStatus.ELIGIBLE
    assert by_id["GB18"].classification_status is ClassificationStatus.EXCLUDED
    assert by_id["GB18"].age_category is not by_id["GB1"].age_category


def test_a_youth_tournament_is_excluded_even_where_the_source_marks_no_age() -> None:
    navigation = (FIXTURES / "navigation.html").read_text(encoding="utf-8")
    youth_tournament = (
        '<a href="/u17-world-cup/startseite/wettbewerb/17WC">U17 World Cup</a>'
    )
    navigation = navigation.replace("</body>", youth_tournament + "</body>")
    profile = (
        '<!doctype html><html lang="en"><body>'
        '<h1 data-competition-id="17WC">U17 World Cup</h1>'
        '<select name="saison_id"><option value="2026" selected>2026</option>'
        "</select></body></html>"
    )

    pages, *_ = _discover(
        fetch=FixtureFetch(
            {
                BASE_URL + "/navigation/wettbewerbe": navigation,
                BASE_URL + "/u17-world-cup/startseite/wettbewerb/17WC": profile,
            }
        )
    )
    snapshot = reconcile_registry_pages(pages)
    tournament = next(
        item for item in snapshot.competitions if item.competition_id == "17WC"
    )
    senior = next(
        item for item in snapshot.competitions if item.competition_id == "GB1"
    )

    assert tournament.classification_status is ClassificationStatus.EXCLUDED
    assert senior.classification_status is ClassificationStatus.ELIGIBLE


def test_discovery_ignores_navbar_entries_that_every_page_repeats() -> None:
    afrika = (FIXTURES / "afrika.html").read_text(encoding="utf-8")
    navbar = (
        '<nav class="main-navbar"><a href="/world-cup/startseite/wettbewerb/FIWC">'
        "World Cup</a></nav>"
    )
    afrika = afrika.replace("<body>", "<body>" + navbar)

    pages, fetch, *_ = _discover(
        fetch=FixtureFetch({BASE_URL + "/wettbewerbe/afrika": afrika})
    )
    snapshot = reconcile_registry_pages(pages)
    world_cup = next(
        item for item in snapshot.competitions if item.competition_id == "FIWC"
    )

    assert world_cup.country != "Africa"


def test_discovery_does_not_follow_sort_variants_of_a_listing() -> None:
    afrika = (FIXTURES / "afrika.html").read_text(encoding="utf-8")
    sorted_link = '<a href="/wettbewerbe/afrika?sort=marktwert">Market value</a>'
    afrika = afrika.replace("</body>", sorted_link + "</body>")

    _, fetch, *_ = _discover(
        fetch=FixtureFetch({BASE_URL + "/wettbewerbe/afrika": afrika})
    )

    assert BASE_URL + "/wettbewerbe/afrika?sort=marktwert" not in fetch.calls


def test_discovery_traverses_every_seed_page_country_pagination_and_profile() -> None:
    pages, fetch, checkpoint, ledger = _discover()

    assert set(fetch.calls) == set(URL_FIXTURES)
    assert len(fetch.calls) == len(URL_FIXTURES) == 17
    assert len(fetch.calls) == len(set(fetch.calls))
    assert ledger.ensure_calls == 17
    assert ledger.cache_hits == 0
    assert set(checkpoint) == set(URL_FIXTURES)
    assert len(pages) == 9  # six seeds + Europe page 2 + two England pages

    assert BASE_URL + "/wettbewerbe/europa?page=2" in fetch.calls
    assert BASE_URL + "/wettbewerbe/national/wettbewerbe/189" in fetch.calls
    assert (
        BASE_URL + "/wettbewerbe/national/wettbewerbe/189?page=2"
        in fetch.calls
    )
    assert BASE_URL + "/fa-cup/startseite/pokalwettbewerb/FAC" in fetch.calls
    assert (
        BASE_URL + "/uefa-champions-league/startseite/pokalwettbewerb/CL"
        in fetch.calls
    )
    assert (
        BASE_URL + "/uefa-nations-league-a/startseite/pokalwettbewerb/UNLA"
        in fetch.calls
    )


def test_discovered_records_cover_all_required_competition_types_and_seasons() -> None:
    pages, *_ = _discover()
    snapshot = reconcile_registry_pages(pages)
    competitions = {item.competition_id: item for item in snapshot.competitions}

    assert set(competitions) == {
        "GB1",
        "GB1W",
        "FAC",
        "CL",
        "AFCN",
        "UNLA",
        "FIWC",
    }
    # The name-only competition is quarantined, not published (#1390).
    assert snapshot.quarantined_competition_ids == ("MYSTERY",)
    assert competitions["GB1"].competition_type is CompetitionType.DOMESTIC_LEAGUE
    assert competitions["GB1W"].competition_type is CompetitionType.DOMESTIC_LEAGUE
    assert competitions["GB1W"].gender is Gender.WOMEN
    assert (
        competitions["GB1W"].classification_status
        is ClassificationStatus.EXCLUDED
    )
    assert competitions["FAC"].competition_type is CompetitionType.DOMESTIC_CUP
    assert competitions["CL"].competition_type is CompetitionType.CONTINENTAL_CLUB
    assert (
        competitions["AFCN"].competition_type
        is CompetitionType.NATIONAL_TEAM_TOURNAMENT
    )
    assert (
        competitions["UNLA"].competition_type
        is CompetitionType.NATIONAL_TEAM_TOURNAMENT
    )
    assert (
        competitions["FIWC"].competition_type
        is CompetitionType.NATIONAL_TEAM_TOURNAMENT
    )

    editions = {
        (item.competition_id, item.edition_id): item
        for item in snapshot.editions
    }
    assert editions[("GB1", "2025")].canonical_season == "2526"
    assert editions[("UNLA", "2026")].canonical_season == "2627"
    assert editions[("AFCN", "2025")].canonical_season == "2025"
    assert editions[("FIWC", "2026")].canonical_season == "2026"
    assert editions[("GB1", "2025")].participant_count == 20
    assert editions[("FIWC", "2026")].participant_count == 48


def test_section_taxonomy_and_main_taxonomy_are_source_evidence_not_names() -> None:
    pages, *_ = _discover()
    snapshot = reconcile_registry_pages(pages)
    gb1 = next(item for item in snapshot.competitions if item.competition_id == "GB1")
    section = next(item for item in gb1.evidence if item.source_field == "section_label")
    audience = next(
        item for item in gb1.evidence if item.source_field == "transfermarkt_taxonomy"
    )
    assert section.source_value == "National leagues"
    assert section.origin is EvidenceOrigin.SOURCE_PAGE
    assert section.competition_type is CompetitionType.DOMESTIC_LEAGUE
    assert audience.source_value == "main men's competitions taxonomy"
    assert audience.origin is EvidenceOrigin.STRUCTURED


def test_womens_section_is_source_backed_exclusion_without_default_mens_signal() -> None:
    pages, *_ = _discover()
    snapshot = reconcile_registry_pages(pages)
    women = next(
        item for item in snapshot.competitions if item.competition_id == "GB1W"
    )

    assert women.classification_status is ClassificationStatus.EXCLUDED
    assert women.gender is Gender.WOMEN
    section = next(
        item for item in women.evidence if item.source_field == "section_label"
    )
    assert section.source_value == "Women's national leagues"
    assert section.gender is Gender.WOMEN
    assert all(
        item.source_field != "transfermarkt_taxonomy" for item in women.evidence
    )
    assert snapshot.blocked_competition_ids == ()
    assert snapshot.quarantined_competition_ids == ("MYSTERY",)


def test_name_only_unknown_classification_is_quarantined_and_snapshot_promotes() -> None:
    pages, *_ = _discover()
    mystery = next(
        item
        for page in pages
        for item in page.competitions
        if item.competition_id == "MYSTERY"
    )
    assert mystery.name == "Men's Senior Mystery League"
    assert mystery.classification_status is ClassificationStatus.UNKNOWN
    snapshot = reconcile_registry_pages(pages)
    # One unknown competition is quarantined; the rest of the snapshot
    # publishes (#1390).
    assert "MYSTERY" not in {item.competition_id for item in snapshot.competitions}
    assert snapshot.quarantined_competition_ids == ("MYSTERY",)
    assert snapshot.blocked_competition_ids == ()
    assert snapshot.promotable is True
    assert {item.competition_id for item in snapshot.crawl_scopes()} == {
        "GB1",
        "FAC",
        "CL",
        "AFCN",
        "UNLA",
        "FIWC",
    }


def test_persistent_checkpoint_resume_performs_zero_fetches() -> None:
    first_pages, _, checkpoint, _ = _discover()
    ledger = LedgerSpy()

    def unexpected_fetch(url: str):
        raise AssertionError(f"fetch called during cached resume: {url}")

    second_pages = discover_competition_registry(
        fetch=unexpected_fetch,
        checkpoint=checkpoint,
        traffic_ledger=ledger,
        clock=lambda: NOW,
    )

    assert second_pages == first_pages
    assert ledger.ensure_calls == 0
    assert ledger.cache_hits == len(URL_FIXTURES) == 17
    assert set(ledger.cache_entities) == {"competition_registry"}


@pytest.mark.parametrize(
    ("status", "status_code", "expected_http"),
    [
        (FetchStatus.RETRY_EXHAUSTED, 504, "http=504"),
        (FetchStatus.RETRY_EXHAUSTED, None, "http=0"),
        (FetchStatus.SCHEMA_ERROR, 404, "http=404"),
    ],
)
def test_404_504_and_http_zero_on_a_seed_page_abort_without_partial_snapshot(
    status, status_code, expected_http
) -> None:
    # #1391: only a seed page still aborts; other pages carry their rows.
    first_url = SEED_URLS[0]
    outcome = FetchOutcome[str](
        status=status,
        status_code=status_code,
        attempts=1,
        error="fixture failure",
    )
    fetch = FixtureFetch({first_url: outcome})
    with pytest.raises(DiscoveryFetchError, match=expected_http):
        _discover(fetch=fetch)


def test_listing_schema_drift_aborts_snapshot() -> None:
    drift = "<!doctype html><html><body><p>new layout</p></body></html>"
    fetch = FixtureFetch({SEED_URLS[0]: drift})
    with pytest.raises(DiscoverySchemaError, match="no registry structure"):
        _discover(fetch=fetch)


def test_a_catalog_whose_profiles_all_lost_their_editions_aborts_snapshot() -> None:
    drift = {
        url: (
            '<!doctype html><html><body>'
            f'<h1 data-competition-id="{url.rsplit("/", 1)[-1]}">x</h1>'
            "</body></html>"
        )
        for url in URL_FIXTURES
        if "wettbewerb/" in url
    }

    with pytest.raises(DiscoverySchemaError, match="no competitions"):
        _discover(fetch=FixtureFetch(drift))


def test_corrupt_cached_payload_fails_closed_without_refetch() -> None:
    url = SEED_URLS[0]
    checkpoint = {
        url: {
            "status": FetchStatus.OK.value,
            "body": "<html><body></body></html>",
            "payload_hash": "not-the-real-hash",
        }
    }
    fetch = FixtureFetch()
    with pytest.raises(DiscoveryCheckpointError, match="hash mismatch"):
        _discover(fetch=fetch, checkpoint=checkpoint)
    assert fetch.calls == []


def test_transport_payload_hash_mismatch_fails_closed() -> None:
    url = SEED_URLS[0]
    body = (FIXTURES / "navigation.html").read_text(encoding="utf-8")
    outcome = FetchOutcome[str](
        status=FetchStatus.OK,
        value=body,
        status_code=200,
        attempts=1,
        decoded_body_bytes=len(body.encode()),
        payload_hash="wrong",
    )
    with pytest.raises(DiscoveryFetchError, match="payload hash mismatch"):
        _discover(fetch=FixtureFetch({url: outcome}))


def test_naive_discovery_clock_is_rejected() -> None:
    with pytest.raises(DiscoverySchemaError, match="timezone-aware"):
        discover_competition_registry(
            fetch=FixtureFetch(),
            checkpoint={},
            traffic_ledger=LedgerSpy(),
            clock=lambda: datetime(2026, 7, 11),
        )


# --------------------------------------------------------------------- #1391


def _json_outcome(value):
    return FetchOutcome(
        status=FetchStatus.OK,
        value=value,
        status_code=200,
        attempts=1,
        label="competition_registry",
    )


class RegulationFetch:
    """tmapi regulation answers from fixtures; anything else is a 404."""

    def __init__(self, overrides=None) -> None:
        self.calls: list[str] = []
        self.overrides = overrides or {}
        self.payloads = {
            competition_regulation_url(competition_id): json.loads(
                (FIXTURES / f"regulation_{competition_id.lower()}.json").read_text(
                    encoding="utf-8"
                )
            )
            for competition_id in ("FAC", "AFCN", "RSK1")
        }

    def __call__(self, url: str):
        self.calls.append(url)
        if url in self.overrides:
            override = self.overrides[url]
            return override if isinstance(override, FetchOutcome) else _json_outcome(override)
        if url in self.payloads:
            return _json_outcome(self.payloads[url])
        return FetchOutcome(
            status=FetchStatus.SCHEMA_ERROR, status_code=404, attempts=1,
            error="fixture: no regulation",
        )


def _failed(status_code):
    return FetchOutcome[str](
        status=(
            FetchStatus.BLOCKED if status_code == 405
            else FetchStatus.SCHEMA_ERROR if status_code == 404
            else FetchStatus.RETRY_EXHAUSTED
        ),
        status_code=status_code,
        attempts=2,
        error="fixture failure",
    )


def _run(fetch=None, *, now=NOW, **options):
    report: dict = {}
    pages = discover_competition_registry(
        fetch=fetch or FixtureFetch(),
        checkpoint={},
        traffic_ledger=LedgerSpy(),
        clock=lambda: now,
        report=report,
        **options,
    )
    return reconcile_registry_pages(pages), report


def _previous(snapshot) -> PreviousRegistry:
    return PreviousRegistry.from_rows(
        snapshot.snapshot_id,
        [item.as_dict() for item in snapshot.competitions],
        [item.as_dict() for item in snapshot.editions],
    )


def _previous_competition(snapshot, competition_id: str) -> PreviousRegistry:
    return PreviousRegistry.from_rows(
        snapshot.snapshot_id,
        [
            item.as_dict()
            for item in snapshot.competitions
            if item.competition_id == competition_id
        ],
        [
            item.as_dict()
            for item in snapshot.editions
            if item.competition_id == competition_id
        ],
    )


def test_regulation_gives_every_season_of_a_cup_and_marks_its_current() -> None:
    snapshot, report = _run(fetch_json=RegulationFetch())
    editions = sorted(
        (item for item in snapshot.editions if item.competition_id == "FAC"),
        key=lambda item: item.edition_id,
    )
    fac = next(item for item in snapshot.competitions if item.competition_id == "FAC")

    # The HTML selector had two FA Cup seasons; the regulation has twelve.
    assert len(editions) == 12
    assert [item.edition_id for item in editions if item.current] == ["2025"]
    assert editions[0].canonical_season == "1415"
    assert editions[-1].source_url == (
        BASE_URL + "/fa-cup/startseite/pokalwettbewerb/FAC/saison_id/2025"
    )
    assert any(item.source_field == "regulation" for item in fac.evidence)
    assert fac.classification_status is ClassificationStatus.ELIGIBLE
    assert report["regulation_current"]["FAC"] == "2025"
    # Competitions whose regulation is unavailable keep the HTML editions.
    assert "GB1" in report["regulation_unavailable"]
    assert {
        item.edition_id for item in snapshot.editions if item.competition_id == "GB1"
    } == {"2025", "2024"}


def test_a_failed_regulation_after_a_full_run_carries_the_cup_history() -> None:
    first, _ = _run(fetch=_big_catalogue(), fetch_json=RegulationFetch())
    later = NOW + timedelta(days=7)
    fetch_json = RegulationFetch(
        {competition_regulation_url("FAC"): _failed(504)}
    )

    snapshot, report = _run(
        fetch=_big_catalogue(), fetch_json=fetch_json, now=later,
        previous=_previous(first),
    )
    fac = [item for item in snapshot.editions if item.competition_id == "FAC"]

    # The HTML selector lists 2 of the 12 FA Cup seasons: the cup is carried
    # whole with its own discovery time instead of losing ten seasons.
    assert report["carried_competition_ids"] == ["FAC"]
    assert "2 of 12" in report["carried"]["FAC"]
    assert len(fac) == 12
    assert {item.discovered_at for item in fac} == {NOW}
    # A fallback that keeps every previous edition is an ordinary refresh.
    gb1 = next(item for item in snapshot.competitions if item.competition_id == "GB1")
    assert gb1.discovered_at == later


def test_national_team_regulation_current_is_the_last_played_edition() -> None:
    snapshot, report = _run(fetch_json=RegulationFetch())
    editions = {
        item.edition_id: item
        for item in snapshot.editions
        if item.competition_id == "AFCN"
    }

    assert editions["2024"].current is True
    assert editions["2024"].canonical_season == "2025"
    # Listed ahead of the current one: registered, never planned.
    assert editions["2026"].active is False
    assert editions["2026"].current is False
    # The HTML profile calls 2025 current: reported, not fatal.
    assert {
        "competition_id": "AFCN", "html": "2025", "regulation": "2024",
    } in report["current_mismatches"]


def test_a_title_only_edition_is_reported_when_no_regulation_answers() -> None:
    cup = (
        '<!doctype html><html lang="en"><head>'
        "<title>CAF Champions League 25/26 | Transfermarkt</title>"
        '</head><body><h1 data-competition-id="AFCN">CAF Champions League</h1>'
        "</body></html>"
    )
    regulation = RegulationFetch(
        {competition_regulation_url("AFCN"): _failed(405)}
    )

    snapshot, report = _run(
        fetch=FixtureFetch(
            {BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN": cup}
        ),
        fetch_json=regulation,
    )

    assert "AFCN" in report["title_only_competition_ids"]
    assert [
        item.edition_id for item in snapshot.editions if item.competition_id == "AFCN"
    ] == ["2025"]


def test_country_page_brings_the_fa_cup_with_its_real_country() -> None:
    europa = (FIXTURES / "europa.html").read_text(encoding="utf-8").replace(
        '<a href="/wettbewerbe/national/wettbewerbe/189">England</a>', ""
    )
    fetch = FixtureFetch({BASE_URL + "/wettbewerbe/europa": europa})

    without, _ = _run(fetch=FixtureFetch({BASE_URL + "/wettbewerbe/europa": europa}))
    snapshot, _ = _run(
        fetch=fetch, countries=(Country("189", "England", "UEFA"),)
    )

    assert "FAC" not in {item.competition_id for item in without.competitions}
    fac = next(item for item in snapshot.competitions if item.competition_id == "FAC")
    assert fac.country == "England"
    assert fac.confederation == "UEFA"
    assert BASE_URL + "/wettbewerbe/national/wettbewerbe/189" in fetch.calls


@pytest.mark.parametrize("suffix", ["/saison_id/2026", "/saison_id/2026/plus/1?page=2"])
def test_country_descendants_keep_configured_context(suffix: str) -> None:
    root = BASE_URL + "/wettbewerbe/national/wettbewerbe/189"
    listing = (FIXTURES / "england.html").read_text().replace(
        root.removeprefix(BASE_URL) + "?page=2",
        root.removeprefix(BASE_URL) + suffix,
    )
    cups = (FIXTURES / "england_page_2.html").read_text().replace(
        '<meta name="tm-country" content="England">', ""
    ).replace('<meta name="tm-confederation" content="UEFA">', "")
    snapshot, report = _run(
        fetch=FixtureFetch({root: listing, root + suffix: cups}),
        countries=(Country("189", "England", "UEFA"),),
    )
    fac = next(item for item in snapshot.competitions if item.competition_id == "FAC")
    assert (fac.country, fac.confederation) == ("England", "UEFA")
    assert not report["listing_failures"]


def test_committed_country_list_is_well_formed() -> None:
    countries = load_countries()

    assert len(countries) >= 200
    by_id = {item.country_id: item for item in countries}
    assert by_id["189"].country == "England"
    assert by_id["50"].country == "France"
    assert by_id["122"].country == "Netherlands"
    assert {item.confederation for item in countries} <= {
        "UEFA", "CAF", "AFC", "Americas", "OFC",
    }
    assert sum(item.confederation == "CAF" for item in countries) >= 50


@pytest.mark.parametrize("suffix", ["/gruppe/QR", "/gruppe/A", "", "/"])
def test_profile_normalization_preserves_competition_boundary(suffix: str) -> None:
    from scrapers.transfermarkt.discovery import _profile_identity, _profile_url

    route = BASE_URL + "/afc/spieltag/pokalwettbewerb/AC2Q"
    normalized = _profile_url(route + "/saison_id/2026" + suffix)
    assert normalized == (route + suffix).rstrip("/")
    assert _profile_identity(normalized)[0] == "AC2Q"


def test_extra_competition_joins_when_no_page_lists_it() -> None:
    extra = ExtraCompetition(
        competition_id="KLUB",
        slug="fifa-klub-wm",
        route="pokalwettbewerb",
        name="FIFA Club World Cup",
        country="World",
        confederation="FIFA",
        competition_type=CompetitionType.CONTINENTAL_CLUB,
        team_type=TeamType.CLUB,
        age_category=AgeCategory.SENIOR,
    )
    profile = (
        '<!doctype html><html lang="en"><body>'
        '<h1 data-competition-id="KLUB">FIFA Club World Cup</h1>'
        '<select name="saison_id"><option value="2024" selected>2025</option>'
        "</select></body></html>"
    )

    snapshot, _ = _run(
        fetch=FixtureFetch({extra.profile_url: profile}),
        extra_competitions=(extra,),
    )
    klub = next(item for item in snapshot.competitions if item.competition_id == "KLUB")

    assert klub.classification_status is ClassificationStatus.ELIGIBLE
    assert klub.competition_type is CompetitionType.CONTINENTAL_CLUB


# A catalogue big enough for the 10 % rule: twenty leagues on Europe's page.
LEAGUES = tuple(f"L{index:02d}" for index in range(1, 21))


def _league_url(competition_id: str) -> str:
    return BASE_URL + f"/league-{competition_id.lower()}/startseite/wettbewerb/{competition_id}"


def _big_catalogue(failures=None) -> FixtureFetch:
    rows = "".join(
        f'<tr><td><a href="/league-{item.lower()}/startseite/wettbewerb/{item}">'
        f"League {item}</a></td></tr>"
        for item in LEAGUES
    )
    europa = (FIXTURES / "europa.html").read_text(encoding="utf-8").replace(
        "</body>",
        '<div class="box"><h2 class="content-box-headline">National leagues</h2>'
        f'<table class="items"><tbody>{rows}</tbody></table></div></body>',
    )
    overrides = {BASE_URL + "/wettbewerbe/europa": europa}
    for item in LEAGUES:
        overrides[_league_url(item)] = (
            '<!doctype html><html lang="en"><body>'
            f'<h1 data-competition-id="{item}">League {item}</h1>'
            '<select name="saison_id"><option value="2025" selected>25/26</option>'
            '<option value="2024">24/25</option></select></body></html>'
        )
    overrides.update(failures or {})
    return FixtureFetch(overrides)


def test_two_unavailable_profiles_publish_a_partial_snapshot_with_carried_rows() -> None:
    first, _ = _run(fetch=_big_catalogue())
    later = NOW + timedelta(days=7)

    snapshot, report = _run(
        fetch=_big_catalogue(
            {_league_url("L01"): _failed(404), _league_url("L02"): _failed(405)}
        ),
        now=later,
        previous=_previous(first),
    )
    by_id = {item.competition_id: item for item in snapshot.competitions}

    assert report["carried_competition_ids"] == ["L01", "L02"]
    assert "http=405" in report["carried"]["L02"]
    assert set(by_id) == {item.competition_id for item in first.competitions}
    # Carried rows keep their own discovery time; refreshed rows are new.
    assert by_id["L01"].discovered_at == NOW
    assert by_id["L03"].discovered_at == later
    assert by_id["L01"].registry_snapshot_id == snapshot.snapshot_id
    assert {
        item.edition_id for item in snapshot.editions if item.competition_id == "L01"
    } == {"2025", "2024"}
    assert snapshot.snapshot_id != first.snapshot_id


def test_more_than_ten_percent_unavailable_drops_the_snapshot() -> None:
    first, _ = _run(fetch=_big_catalogue())
    failures = {_league_url(item): _failed(504) for item in LEAGUES[:3]}

    # 3 of 27 published competitions = 11 %.
    with pytest.raises(DiscoveryError, match="carried over"):
        _run(fetch=_big_catalogue(failures), previous=_previous(first))


def test_a_new_competition_with_an_unavailable_page_is_not_published() -> None:
    snapshot, report = _run(
        fetch=_big_catalogue({_league_url("L05"): _failed(405)})
    )

    assert "L05" not in {item.competition_id for item in snapshot.competitions}
    assert "L05" in report["unavailable_new"]
    assert report["carried_competition_ids"] == []


def test_an_unavailable_listing_page_carries_what_only_it_listed() -> None:
    first, _ = _run(fetch=_big_catalogue())
    fetch = _big_catalogue(
        {BASE_URL + "/wettbewerbe/national/wettbewerbe/189?page=2": _failed(502)}
    )

    snapshot, report = _run(fetch=fetch, previous=_previous(first))

    assert report["listing_failures"][0]["url"].endswith("189?page=2")
    # The FA Cup and the WSL are listed on that page only: carried, not dropped.
    assert report["carried_competition_ids"] == ["FAC", "GB1W"]
    assert {item.competition_id for item in snapshot.competitions} == {
        item.competition_id for item in first.competitions
    }


def _k_league_previous() -> PreviousRegistry:
    """K League 1 as the registry holds it in December 2026."""

    competition = {
        "competition_id": "RSK1",
        "slug": "k-league-1",
        "name": "K League 1",
        "country": "Korea, South",
        "confederation": "AFC",
        "competition_type": "domestic_league",
        "gender": "men",
        "team_type": "club",
        "age_category": "senior",
        "season_format": "single_year",
        "active": True,
        "source_url": BASE_URL + "/k-league-1/startseite/wettbewerb/RSK1",
        "discovered_at": datetime(2026, 12, 28, 18, 0),
        "canonical_competition_id": None,
        "classification_evidence": json.dumps(
            [
                ClassificationEvidence(
                    source_field="section_label",
                    source_value="National leagues",
                    source_url=BASE_URL + "/wettbewerbe/asien",
                    origin=EvidenceOrigin.SOURCE_PAGE,
                    precedence=2,
                    competition_type=CompetitionType.DOMESTIC_LEAGUE,
                    team_type=TeamType.CLUB,
                    age_category=AgeCategory.SENIOR,
                ).as_dict(),
                ClassificationEvidence(
                    source_field="transfermarkt_taxonomy",
                    source_value="main men's competitions taxonomy",
                    source_url=BASE_URL + "/wettbewerbe/asien",
                    origin=EvidenceOrigin.STRUCTURED,
                    gender=Gender.MEN,
                ).as_dict(),
                ClassificationEvidence(
                    source_field="edition_selector",
                    source_value="2026,2025",
                    source_url=BASE_URL + "/k-league-1/startseite/wettbewerb/RSK1",
                    origin=EvidenceOrigin.STRUCTURED,
                    season_format=SeasonFormat.SINGLE_YEAR,
                ).as_dict(),
            ]
        ),
        "source_body_hash": "a" * 64,
        "parser_revision": "tm-html-discovery-v3",
        "schema_revision": "1",
    }
    editions = [
        {
            "competition_id": "RSK1",
            "edition_id": saison,
            "edition_label": label,
            "canonical_season": label,
            "season_format": "single_year",
            "start_date": None,
            "end_date": None,
            "active": True,
            "is_current": saison == "2025",
            "participant_count": None,
            "participant_hash": None,
            "source_url": BASE_URL
            + f"/k-league-1/startseite/wettbewerb/RSK1/saison_id/{saison}",
            "discovered_at": datetime(2026, 12, 31, 18, 0),
            "source_body_hash": "b" * 64,
            "parser_revision": "tm-html-discovery-v3",
            "schema_revision": "1",
        }
        for saison, label in (("2025", "2026"), ("2024", "2025"))
    ]
    return PreviousRegistry.from_rows(
        "tm-discovery-" + "c" * 24, [competition], editions
    )


def test_january_2027_mine_daily_run_moves_a_calendar_league_to_2027() -> None:
    """The regulation marks K League 2027 current; the planner takes 2027."""

    from dags.utils.transfermarkt_scope_planner import eligible_registry_scopes

    january = datetime(2027, 1, 2, 18, 0, tzinfo=timezone.utc)
    previous = _k_league_previous()
    fetch = FixtureFetch()

    snapshot, report = _run(
        fetch=fetch,
        now=january,
        mode="daily",
        previous=previous,
        fetch_json=RegulationFetch(),
    )

    assert fetch.calls == []  # daily mode reads no HTML
    current = [item for item in snapshot.editions if item.current]
    assert [(item.edition_id, item.canonical_season) for item in current] == [
        ("2026", "2027")
    ]
    assert current[0].discovered_at == january
    competition = snapshot.competitions[0]
    # The competition row keeps the last full crawl's time.
    assert competition.discovered_at == datetime(2026, 12, 28, 18, 0, tzinfo=timezone.utc)
    assert report["new_current_editions"] == [
        {"competition_id": "RSK1", "previous": "2025", "current": "2026"}
    ]

    rows = [
        {
            "competition_id": competition.competition_id,
            "slug": competition.slug,
            "name": competition.name,
            "country": competition.country,
            "confederation": competition.confederation,
            "competition_type": competition.competition_type.value,
            "gender": competition.gender.value,
            "team_type": competition.team_type.value,
            "age_category": competition.age_category.value,
            "competition_season_format": competition.season_format.value,
            "competition_active": competition.active,
            "competition_source_url": competition.source_url,
            "competition_discovered_at": competition.discovered_at.isoformat(),
            "classification_status": competition.classification_status.value,
            "classification_evidence": json.dumps(
                [item.as_dict() for item in competition.evidence]
            ),
            "registry_snapshot_id": snapshot.snapshot_id,
            "edition_id": edition.edition_id,
            "edition_label": edition.edition_label,
            "canonical_season": edition.canonical_season,
            "edition_season_format": edition.season_format.value,
            "edition_active": edition.active,
            "is_current": edition.current,
            "edition_source_url": edition.source_url,
            "edition_discovered_at": edition.discovered_at.isoformat(),
        }
        for edition in snapshot.editions
    ]
    targets = [item for item in eligible_registry_scopes(rows) if item.current]
    assert [(item.edition_id, item.canonical_season) for item in targets] == [
        ("2026", "2027")
    ]


def test_daily_uses_strict_html_fallback_for_a_known_competition() -> None:
    full, _ = _run(fetch_json=RegulationFetch())
    previous = _previous_competition(full, "GB1")
    later = NOW + timedelta(days=1)
    fetch = FixtureFetch()

    snapshot, report = _run(
        fetch=fetch,
        fetch_json=RegulationFetch(),
        mode="daily",
        previous=previous,
        now=later,
    )

    assert fetch.calls == [BASE_URL + "/premier-league/startseite/wettbewerb/GB1"]
    assert report["daily_html_fallback_competition_ids"] == ["GB1"]
    assert report["daily_html_fallback_rejected"] == {}
    assert report["carried_competition_ids"] == []
    editions = sorted(snapshot.editions, key=lambda item: item.edition_id)
    assert [(item.edition_id, item.current) for item in editions] == [
        ("2024", False),
        ("2025", True),
    ]
    assert {item.discovered_at for item in editions} == {later}
    assert snapshot.competitions[0].discovered_at == NOW


def test_daily_falls_back_when_regulation_editions_cannot_be_built() -> None:
    full, _ = _run(fetch_json=RegulationFetch())
    previous = _previous_competition(full, "GB1")
    bad_regulation = {
        "success": True,
        "message": "OK",
        "data": [
            {
                "competitionId": "GB1",
                "season": {
                    "id": 2025,
                    "display": "25/27",
                    "cyclicalName": "25/27",
                    "nonCyclicalName": "25/27",
                },
                "isCurrentSeason": True,
            }
        ],
    }

    snapshot, report = _run(
        fetch=FixtureFetch(),
        fetch_json=RegulationFetch(
            {competition_regulation_url("GB1"): bad_regulation}
        ),
        mode="daily",
        previous=previous,
        now=NOW + timedelta(days=1),
    )

    assert report["daily_html_fallback_competition_ids"] == ["GB1"]
    assert report["regulation_unavailable"]["GB1"].startswith("not applicable:")
    assert {item.edition_id for item in snapshot.editions} == {"2024", "2025"}


def test_daily_html_fallback_never_drops_known_history() -> None:
    full, _ = _run(fetch_json=RegulationFetch())
    previous = _previous_competition(full, "FAC")
    fetch = FixtureFetch()
    report: dict = {}

    with pytest.raises(DiscoveryError, match="refreshed no competition"):
        discover_competition_registry(
            fetch=fetch,
            checkpoint={},
            traffic_ledger=LedgerSpy(),
            clock=lambda: NOW + timedelta(days=1),
            report=report,
            fetch_json=RegulationFetch(
                {competition_regulation_url("FAC"): _failed(504)}
            ),
            mode="daily",
            previous=previous,
        )

    assert fetch.calls == [BASE_URL + "/fa-cup/startseite/pokalwettbewerb/FAC"]
    assert report["daily_html_fallback_competition_ids"] == []
    assert "2 of 12 previous editions" in report["daily_html_fallback_rejected"]["FAC"]


def test_daily_html_fallback_rejects_title_only_profile() -> None:
    full, _ = _run(fetch_json=RegulationFetch())
    previous = _previous_competition(full, "AFCN")
    title_only = (
        '<!doctype html><html lang="en"><head>'
        "<title>Africa Cup 2025 | Transfermarkt</title>"
        '</head><body><h1 data-competition-id="AFCN">Africa Cup</h1>'
        "</body></html>"
    )
    report: dict = {}

    with pytest.raises(DiscoveryError, match="refreshed no competition"):
        discover_competition_registry(
            fetch=FixtureFetch(
                {
                    BASE_URL
                    + "/afrika-cup/startseite/pokalwettbewerb/AFCN": title_only
                }
            ),
            checkpoint={},
            traffic_ledger=LedgerSpy(),
            clock=lambda: NOW + timedelta(days=1),
            report=report,
            fetch_json=RegulationFetch(
                {competition_regulation_url("AFCN"): _failed(405)}
            ),
            mode="daily",
            previous=previous,
        )

    assert report["title_only_competition_ids"] == ["AFCN"]
    assert "season selector" in report["daily_html_fallback_rejected"]["AFCN"]


def test_daily_html_fallback_rejects_a_foreign_profile() -> None:
    full, _ = _run(fetch_json=RegulationFetch())
    previous = _previous_competition(full, "GB1")
    profile_url = BASE_URL + "/premier-league/startseite/wettbewerb/GB1"
    foreign = (FIXTURES / "profile_gb1.html").read_text().replace(
        'data-competition-id="GB1"', 'data-competition-id="ES1"'
    )
    report: dict = {}

    with pytest.raises(DiscoveryError, match="refreshed no competition"):
        discover_competition_registry(
            fetch=FixtureFetch({profile_url: foreign}),
            checkpoint={},
            traffic_ledger=LedgerSpy(),
            clock=lambda: NOW + timedelta(days=1),
            report=report,
            fetch_json=RegulationFetch(),
            mode="daily",
            previous=previous,
        )

    assert report["daily_html_fallback_competition_ids"] == []
    assert "profile identity mismatch" in report["daily_html_fallback_rejected"]["GB1"]


def test_daily_html_fallback_rejects_a_foreign_edition_anchor() -> None:
    full, _ = _run(fetch_json=RegulationFetch())
    previous = _previous_competition(full, "GB1")
    profile_url = BASE_URL + "/premier-league/startseite/wettbewerb/GB1"
    foreign_anchor = (
        '<!doctype html><html lang="en"><body>'
        '<h1 data-competition-id="GB1">Premier League</h1>'
        '<a href="/la-liga/startseite/wettbewerb/ES1/saison_id/2025" '
        'class="active">25/26</a>'
        '<a href="/premier-league/startseite/wettbewerb/GB1/saison_id/2024">'
        '24/25</a></body></html>'
    )
    report: dict = {}

    with pytest.raises(DiscoveryError, match="refreshed no competition"):
        discover_competition_registry(
            fetch=FixtureFetch({profile_url: foreign_anchor}),
            checkpoint={},
            traffic_ledger=LedgerSpy(),
            clock=lambda: NOW + timedelta(days=1),
            report=report,
            fetch_json=RegulationFetch(),
            mode="daily",
            previous=previous,
        )

    assert "changes competition identity" in (
        report["daily_html_fallback_rejected"]["GB1"]
    )


def test_daily_html_fallback_rejects_conflicting_edition_anchors() -> None:
    full, _ = _run(fetch_json=RegulationFetch())
    previous = _previous_competition(full, "GB1")
    profile_url = BASE_URL + "/premier-league/startseite/wettbewerb/GB1"
    conflicting = (
        '<!doctype html><html lang="en"><body>'
        '<h1 data-competition-id="GB1">Premier League</h1>'
        '<a href="/premier-league/startseite/wettbewerb/GB1/saison_id/2025" '
        'class="active">25/26</a>'
        '<a href="/premier-league/startseite/wettbewerb/GB1/saison_id/2025">'
        '2025</a>'
        '<a href="/premier-league/startseite/wettbewerb/GB1/saison_id/2024">'
        '24/25</a></body></html>'
    )
    report: dict = {}

    with pytest.raises(DiscoveryError, match="refreshed no competition"):
        discover_competition_registry(
            fetch=FixtureFetch({profile_url: conflicting}),
            checkpoint={},
            traffic_ledger=LedgerSpy(),
            clock=lambda: NOW + timedelta(days=1),
            report=report,
            fetch_json=RegulationFetch(),
            mode="daily",
            previous=previous,
        )

    assert "conflicting edition selector" in (
        report["daily_html_fallback_rejected"]["GB1"]
    )


def test_daily_html_fallback_respects_the_existing_request_guard() -> None:
    full, _ = _run(fetch_json=RegulationFetch())
    previous = _previous_competition(full, "GB1")
    fetch = FixtureFetch()
    affordability = iter((True, False))
    report: dict = {}

    with pytest.raises(DiscoveryError, match="refreshed no competition"):
        discover_competition_registry(
            fetch=fetch,
            checkpoint={},
            traffic_ledger=LedgerSpy(),
            clock=lambda: NOW + timedelta(days=1),
            report=report,
            fetch_json=RegulationFetch(),
            mode="daily",
            previous=previous,
            can_spend=lambda requests: next(affordability),
        )

    assert fetch.calls == []
    assert report["daily_html_fallback_rejected"]["GB1"] == "request budget spent"


def test_daily_run_that_refreshes_nothing_fails() -> None:
    previous = _k_league_previous()
    regulation = RegulationFetch({competition_regulation_url("RSK1"): _failed(504)})

    with pytest.raises(DiscoveryError, match="refreshed no competition"):
        _run(
            fetch=FixtureFetch(
                {
                    BASE_URL
                    + "/k-league-1/startseite/wettbewerb/RSK1": _failed(504)
                }
            ),
            mode="daily",
            previous=previous,
            fetch_json=regulation,
        )


def test_budget_guard_carries_known_and_defers_new_competitions() -> None:
    first, _ = _run(fetch=_big_catalogue())
    fetch = _big_catalogue()
    spent = {"calls": 0}

    def can_spend(requests):
        spent["calls"] += 1
        # Budget for every known competition but the last two checks.
        return spent["calls"] <= len(first.competitions) - 2

    with pytest.raises(DiscoveryError, match="carried over"):
        # Carrying never hides a spent budget beyond the 10 % rule.
        _run(fetch=fetch, previous=_previous(first), can_spend=lambda n: False)

    snapshot, report = _run(
        fetch=_big_catalogue(), previous=_previous(first), can_spend=can_spend
    )
    assert len(report["carried_competition_ids"]) == 2
    assert all(
        reason == "request budget spent" for reason in report["carried"].values()
    )


def _participation_candidates(soup, *, country_page=True):
    from scrapers.transfermarkt.discovery import _listing_candidates

    page_url = (BASE_URL + "/wettbewerbe/national/wettbewerbe/50"
                if country_page else BASE_URL + "/wettbewerbe/europa")
    return _listing_candidates(soup, page_url=page_url, page_hash="fixture")


@pytest.mark.parametrize("competition_id,slug,name", [
    ("FIC1", "fifa-intercontinental-cup", "FIFA Intercontinental Cup"),
    ("ACL", "caf-champions-league", "CAF-Champions League"),
])
def test_empty_participation_round_preserves_named_competition_and_cup(
    competition_id, slug, name,
):
    from bs4 import BeautifulSoup

    html = (FIXTURES / "country_participation.html").read_text()
    html = html.replace("FIC1", competition_id).replace("fifa-intercontinental-cup", slug)
    html = html.replace("FIFA Intercontinental Cup", name)
    soup = BeautifulSoup(html, "html.parser")
    candidates = _participation_candidates(soup)
    assert [(c.competition_id, c.name) for c in candidates] == [
        ("TESTCUP", "Fixture Cup"), (competition_id, name),
    ]
    assert any(e.competition_type is CompetitionType.DOMESTIC_CUP
               for e in candidates[0].evidence)
    # Participation is not itself proof of competition type or age.
    assert all(e.competition_type is None and e.age_category is None
               for e in candidates[1].evidence)


@pytest.mark.parametrize("damage", [
    "nameless_header", "missing_header", "different_id", "different_slug",
    "different_route", "different_table", "intervening_header", "missing_club",
    "wrong_panel", "wrong_route", "not_country", "standalone", "no_round_column",
    "reordered_round_column", "heading_colspan", "club_colspan", "round_colspan",
    "heading_rowspan", "club_rowspan",
])
def test_unproven_empty_competition_links_still_fail_closed(damage):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup((FIXTURES / "country_participation.html").read_text(), "html.parser")
    panel = soup.select_one("#participation")
    header = panel.select_one("tr.bg_blau_20")
    header_link = header.select_one("a")
    empty = panel.select_one('a[href*="/spieltag/"]')
    if damage == "nameless_header":
        header_link.clear()
        header_link["title"] = ""
    elif damage == "missing_header":
        header.decompose()
    elif damage in {"different_id", "different_slug", "different_route"}:
        old, new = {"different_id": ("FIC1", "OTHER"),
                    "different_slug": ("fifa-intercontinental-cup", "other-cup"),
                    "different_route": ("pokalwettbewerb", "wettbewerb")}[damage]
        empty["href"] = empty["href"].replace(old, new)
    elif damage == "different_table":
        table = soup.new_tag("table")
        panel.insert(1, table)
        table.append(header.extract())
    elif damage == "intervening_header":
        other = BeautifulSoup(str(header).replace("FIC1", "OTHER"), "html.parser").tr
        header.insert_after(other)
    elif damage == "missing_club":
        for anchor in panel.select('a[href*="/verein/"]'):
            anchor.decompose()
    elif damage == "wrong_panel":
        panel.h2.string = "Other competitions"
    elif damage == "wrong_route":
        empty["href"] = empty["href"].replace("spieltag", "startseite")
    elif damage == "no_round_column":
        panel.thead.decompose()
    elif damage == "reordered_round_column":
        headings = panel.thead.find_all("th")
        headings[1].string = "Opponent"
        headings[2].string = "Round achieved"
    elif damage == "heading_colspan":
        panel.thead.th["colspan"] = "2"
    elif damage == "heading_rowspan":
        panel.thead.th["rowspan"] = "2"
    elif damage in {"club_colspan", "round_colspan", "club_rowspan"}:
        cells = empty.find_parent("tr").find_all("td", recursive=False)
        cell = cells[3] if damage == "round_colspan" else cells[0]
        cell["rowspan" if damage == "club_rowspan" else "colspan"] = "2"
    elif damage == "standalone":
        soup.body.append(empty.extract())
    with pytest.raises(DiscoverySchemaError, match="competition link has no name"):
        _participation_candidates(soup, country_page=damage != "not_country")
