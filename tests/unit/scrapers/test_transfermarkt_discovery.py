from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scrapers.transfermarkt.discovery import (
    BASE_URL,
    SEED_ROUTES,
    SEED_URLS,
    DiscoveryCheckpointError,
    DiscoveryFetchError,
    DiscoverySchemaError,
    discover_competition_registry,
)
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from scrapers.transfermarkt.registry import (
    ClassificationStatus,
    CompetitionType,
    EvidenceOrigin,
    Gender,
    SeasonFormat,
    UnsafeCrawlError,
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
        "MYSTERY",
    }
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
    assert snapshot.blocked_competition_ids == ("MYSTERY",)


def test_name_only_unknown_classification_blocks_snapshot_promotion() -> None:
    pages, *_ = _discover()
    snapshot = reconcile_registry_pages(pages)
    mystery = next(
        item for item in snapshot.competitions if item.competition_id == "MYSTERY"
    )
    assert mystery.name == "Men's Senior Mystery League"
    assert mystery.classification_status is ClassificationStatus.UNKNOWN
    assert snapshot.blocked_competition_ids == ("MYSTERY",)
    assert snapshot.promotable is False
    with pytest.raises(UnsafeCrawlError, match="MYSTERY"):
        snapshot.crawl_scopes()
    assert {item.competition_id for item in snapshot.crawl_scopes(strict=False)} == {
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
def test_404_504_and_http_zero_abort_without_partial_snapshot(
    status, status_code, expected_http
) -> None:
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
