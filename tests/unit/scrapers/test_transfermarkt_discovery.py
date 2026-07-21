from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scrapers.transfermarkt.discovery import (
    BASE_URL,
    SEED_ROUTES,
    SEED_URLS,
    DiscoveryCheckpointError,
    DiscoveryFetchError,
    DiscoveryLimits,
    DiscoverySchemaError,
    discover_competition_registry,
)
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus, stable_payload_hash
from scrapers.transfermarkt.registry import (
    ClassificationStatus,
    CompetitionType,
    EvidenceOrigin,
    Gender,
    SeasonFormat,
    UnsafeCrawlError,
    participant_list_hash,
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
    BASE_URL + "/wettbewerbe/national/wettbewerbe/189?page=2": "england_page_2.html",
    BASE_URL + "/premier-league/startseite/wettbewerb/GB1": "profile_gb1.html",
    BASE_URL + "/womens-super-league/startseite/wettbewerb/GB1W": "profile_gb1w.html",
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

HISTORICAL_EDITION_URLS = {
    BASE_URL + "/premier-league/startseite/wettbewerb/GB1/saison_id/2024",
    BASE_URL + "/womens-super-league/startseite/wettbewerb/GB1W/saison_id/2024",
    BASE_URL + "/fa-cup/startseite/pokalwettbewerb/FAC/saison_id/2024",
    BASE_URL + "/uefa-champions-league/startseite/pokalwettbewerb/CL/saison_id/2024",
    BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN/saison_id/2023",
    BASE_URL + "/uefa-nations-league-a/startseite/pokalwettbewerb/UNLA/saison_id/2024",
    BASE_URL + "/world-cup/startseite/wettbewerb/FIWC/saison_id/2022",
}
EXPECTED_URLS = set(URL_FIXTURES) | HISTORICAL_EDITION_URLS


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
        elif match := re.search(r"/saison_id/(?P<edition_id>\d{4})$", url):
            profile_url = url[: match.start()]
            if profile_url in self.overrides:
                body = self.overrides[profile_url]
            else:
                body = (FIXTURES / URL_FIXTURES[profile_url]).read_text(
                    encoding="utf-8"
                )
            body = body.replace(" selected", "")
            body = body.replace(
                f'<option value="{match.group("edition_id")}">',
                f'<option value="{match.group("edition_id")}" selected>',
            )
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


def test_canonical_catalogue_is_the_only_fixed_discovery_root() -> None:
    assert SEED_ROUTES == ("/navigation/wettbewerbe",)
    assert SEED_URLS == tuple(BASE_URL + route for route in SEED_ROUTES)


def test_discovery_prefers_canonical_route_over_legacy_alias_for_same_id() -> None:
    navigation = (FIXTURES / "navigation.html").read_text(encoding="utf-8")
    alias = (
        '<a href="/weltmeisterschaft/startseite/pokalwettbewerb/FIWC">'
        "World Cup 2026</a>"
    )
    canonical = '<a href="/world-cup/startseite/wettbewerb/FIWC">World Cup</a>'
    navigation = navigation.replace("</body>", alias + canonical + "</body>")

    pages, fetch, *_ = _discover(
        fetch=FixtureFetch({BASE_URL + "/navigation/wettbewerbe": navigation})
    )
    snapshot = reconcile_registry_pages(pages)
    world_cup = next(
        item for item in snapshot.competitions if item.competition_id == "FIWC"
    )

    assert world_cup.slug == "world-cup"
    assert world_cup.name == "FIFA World Cup"
    assert world_cup.source_url == (BASE_URL + "/world-cup/startseite/wettbewerb/FIWC")
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
    historical = '<a href="/torneo-intermedio/startseite/wettbewerb/GB1">3</a>'
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


def test_metric_anchor_text_can_never_replace_the_competition_name() -> None:
    navigation = (FIXTURES / "navigation.html").read_text(encoding="utf-8")
    metrics = (
        '<a href="/premier-league/startseite/wettbewerb/GB1">87.5%</a>'
        '<a href="/premier-league/startseite/wettbewerb/GB1">3</a>'
    )
    navigation = navigation.replace("</body>", metrics + "</body>")

    pages, *_ = _discover(
        fetch=FixtureFetch({BASE_URL + "/navigation/wettbewerbe": navigation})
    )
    snapshot = reconcile_registry_pages(pages)

    premier_league = next(
        item for item in snapshot.competitions if item.competition_id == "GB1"
    )
    assert premier_league.name == "Premier League"
    assert "%" not in premier_league.name


def test_metric_only_competition_label_fails_closed() -> None:
    navigation = (FIXTURES / "navigation.html").read_text(encoding="utf-8")
    navigation = navigation.replace(
        "</body>",
        '<a href="/metric/startseite/wettbewerb/METRIC">64.2%</a></body>',
    )

    with pytest.raises(DiscoverySchemaError, match="numeric/metric labels: METRIC"):
        _discover(
            fetch=FixtureFetch({BASE_URL + "/navigation/wettbewerbe": navigation})
        )


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
    afcn = next(item for item in snapshot.competitions if item.competition_id == "AFCN")

    assert afcn.source_url == (BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN")
    assert BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN" in fetch.calls
    editions = [e for e in snapshot.editions if e.competition_id == "AFCN"]
    assert len(editions) == 2


def test_discovery_keeps_the_format_of_each_edition_when_it_changed() -> None:
    profile = (
        '<!doctype html><html lang="en"><body data-participants-empty="true">'
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
    cup = (
        '<!doctype html><html lang="en"><head>'
        "<title>CAF Champions League 25/26 | Transfermarkt</title>"
        '</head><body data-participants-empty="true">'
        '<h1 data-competition-id="AFCN">CAF Champions League</h1>'
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


def test_live_component_profiles_use_authoritative_json_inventory_and_participants():
    navigation_url = BASE_URL + "/navigation/wettbewerbe"
    profile_url = BASE_URL + "/international-cup/startseite/pokalwettbewerb/IC"
    regulation_url = "https://tmapi.transfermarkt.technology/competition/IC/regulation"
    participant_urls = {
        "2025": (
            "https://tmapi.transfermarkt.technology/competition/IC/club?season=2025"
        ),
        "2024": (
            "https://tmapi.transfermarkt.technology/competition/IC/club?season=2024"
        ),
    }
    clubs_url = (
        "https://tmapi.transfermarkt.technology/clubs?ids%5B%5D=11&ids%5B%5D=281"
    )
    navigation = """<!doctype html><html><head>
      <meta name="tm-country" content="International">
      <meta name="tm-confederation" content="UEFA">
      </head><body><div class="box"><div class="content-box-headline">Cups</div><table class="items"><tr>
      <td><a href="/international-cup/startseite/pokalwettbewerb/IC">International Cup</a></td>
      </tr></table></div></body></html>"""
    profile = """<!doctype html><html><body>
      <h1 data-competition-id="IC">International Cup</h1>
      <tm-competition-homepage competition-id="IC" season-id="2025"></tm-competition-homepage>
      </body></html>"""
    api_values = {
        regulation_url: {
            "success": True,
            "message": "OK",
            "data": [
                {
                    "competitionId": "IC",
                    "season": {"id": 2025, "display": "25/26"},
                    "isCurrentSeason": True,
                    "tournamentStart": "2025-08-01T00:00:00+02:00",
                    "tournamentEnd": "2026-05-30T00:00:00+02:00",
                },
                {
                    "competitionId": "IC",
                    "season": {"id": 2024, "display": "24/25"},
                    "isCurrentSeason": False,
                    "tournamentStart": None,
                    "tournamentEnd": None,
                },
            ],
        },
        participant_urls["2025"]: {
            "success": True,
            "message": "OK",
            "data": {
                "competitionId": "IC",
                "clubIds": ["281", "11"],
                "seasonId": 2025,
                "season": {"id": 2025, "display": "25/26"},
            },
        },
        participant_urls["2024"]: {
            "success": True,
            "message": "OK",
            "data": {
                "competitionId": "IC",
                "clubIds": ["11"],
                "seasonId": 2024,
                "season": {"id": 2024, "display": "24/25"},
            },
        },
        clubs_url: {
            "success": True,
            "message": "OK",
            "data": [
                {
                    "id": "11",
                    "name": "Arsenal FC",
                    "relativeUrl": "/arsenal/startseite/verein/11",
                    "baseDetails": {"isNationalTeam": False},
                },
                {
                    "id": "281",
                    "name": "Manchester City",
                    "relativeUrl": "/manchester-city/startseite/verein/281",
                    "baseDetails": {"isNationalTeam": False},
                },
            ],
        },
    }

    html_fetch = FixtureFetch(
        {navigation_url: navigation, profile_url: profile}
    )
    json_calls: list[tuple[str, str]] = []

    def json_fetch(url: str, endpoint: str) -> FetchOutcome[dict]:
        json_calls.append((url, endpoint))
        value = api_values[url]
        return FetchOutcome(
            status=FetchStatus.OK,
            value=value,
            status_code=200,
            attempts=1,
            label=endpoint,
            payload_hash=stable_payload_hash(value),
        )

    pages = discover_competition_registry(
        fetch=html_fetch,
        fetch_json=json_fetch,
        checkpoint={},
        traffic_ledger=LedgerSpy(),
        clock=lambda: NOW,
    )
    snapshot = reconcile_registry_pages(pages)

    competition = next(item for item in snapshot.competitions if item.competition_id == "IC")
    assert competition.competition_type is CompetitionType.CONTINENTAL_CLUB
    assert [item.edition_id for item in snapshot.editions if item.competition_id == "IC"] == [
        "2024",
        "2025",
    ]
    assert {
        (item.edition_id, item.team_id, item.team_name)
        for item in snapshot.participants
        if item.competition_id == "IC"
    } == {("2025", "11", "Arsenal FC"), ("2025", "281", "Manchester City"), ("2024", "11", "Arsenal FC")}
    assert [endpoint for _url, endpoint in json_calls] == [
        "competition_regulation",
        "competition_participants",
        "competition_participants",
        "competition_participant_entities",
    ]


def test_discovery_blocks_when_a_competition_has_no_authoritative_editions() -> None:
    unstaged = (
        '<!doctype html><html lang="en"><head>'
        "<title>J1 100 Year Vision League | Transfermarkt</title>"
        '</head><body><h1 data-competition-id="AFCN">J1 League</h1>'
        "</body></html>"
    )

    with pytest.raises(DiscoverySchemaError, match="edition selector missing"):
        _discover(
            fetch=FixtureFetch(
                {BASE_URL + "/afrika-cup/startseite/pokalwettbewerb/AFCN": unstaged}
            )
        )


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
        '<!doctype html><html lang="en"><body data-participants-empty="true">'
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
        '<!doctype html><html lang="en"><body data-participants-empty="true">'
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


def test_catalogue_recursively_discovers_a_new_first_party_region() -> None:
    navigation = (FIXTURES / "navigation.html").read_text(encoding="utf-8")
    navigation = navigation.replace(
        "</body>", '<a href="/wettbewerbe/ozeanien">Oceania</a></body>'
    )
    oceania_url = BASE_URL + "/wettbewerbe/ozeanien"
    empty_region = (
        '<!doctype html><html><body data-registry-empty="true" '
        'data-country="Oceania" data-confederation="OFC"></body></html>'
    )

    _, fetch, *_ = _discover(
        fetch=FixtureFetch(
            {
                BASE_URL + "/navigation/wettbewerbe": navigation,
                oceania_url: empty_region,
            }
        )
    )

    assert oceania_url in fetch.calls


def test_declared_last_page_enumerates_intermediate_pagination_pages() -> None:
    europa = (FIXTURES / "europa.html").read_text(encoding="utf-8")
    europa = europa.replace(
        "</nav>",
        '<a class="page-link" data-page="3" href="/wettbewerbe/europa?page=3">'
        "3</a></nav>",
    )
    page_three_url = BASE_URL + "/wettbewerbe/europa?page=3"
    empty_page = (
        '<!doctype html><html><body data-registry-empty="true" '
        'data-country="Europe" data-confederation="UEFA"></body></html>'
    )

    _, fetch, *_ = _discover(
        fetch=FixtureFetch(
            {
                BASE_URL + "/wettbewerbe/europa": europa,
                page_three_url: empty_page,
            }
        )
    )

    assert BASE_URL + "/wettbewerbe/europa?page=2" in fetch.calls
    assert page_three_url in fetch.calls


def test_pagination_label_url_mismatch_fails_closed() -> None:
    europa = (FIXTURES / "europa.html").read_text(encoding="utf-8")
    europa = europa.replace('data-page="2"', 'data-page="3"')

    with pytest.raises(DiscoverySchemaError, match="label/URL mismatch"):
        _discover(fetch=FixtureFetch({BASE_URL + "/wettbewerbe/europa": europa}))


def test_pagination_bound_is_enforced_before_fetching_out_of_range_page() -> None:
    europa = (FIXTURES / "europa.html").read_text(encoding="utf-8")
    europa = europa.replace('data-page="2"', 'data-page="13"').replace(
        "europa?page=2", "europa?page=13"
    )
    fetch = FixtureFetch({BASE_URL + "/wettbewerbe/europa": europa})

    with pytest.raises(DiscoverySchemaError, match="out of bounds"):
        discover_competition_registry(
            fetch=fetch,
            checkpoint={},
            traffic_ledger=LedgerSpy(),
            clock=lambda: NOW,
            limits=DiscoveryLimits(listing_pages=12),
        )
    assert BASE_URL + "/wettbewerbe/europa?page=13" not in fetch.calls


def test_discovery_traverses_every_seed_page_country_pagination_and_profile() -> None:
    pages, fetch, checkpoint, ledger = _discover()

    assert set(fetch.calls) == EXPECTED_URLS
    assert len(fetch.calls) == len(EXPECTED_URLS) == 24
    assert len(fetch.calls) == len(set(fetch.calls))
    assert ledger.ensure_calls == 24
    assert ledger.cache_hits == 0
    assert set(checkpoint) == EXPECTED_URLS
    assert len(pages) == 9  # six seeds + Europe page 2 + two England pages

    assert BASE_URL + "/wettbewerbe/europa?page=2" in fetch.calls
    assert BASE_URL + "/wettbewerbe/national/wettbewerbe/189" in fetch.calls
    assert BASE_URL + "/wettbewerbe/national/wettbewerbe/189?page=2" in fetch.calls
    assert BASE_URL + "/fa-cup/startseite/pokalwettbewerb/FAC" in fetch.calls
    assert (
        BASE_URL + "/uefa-champions-league/startseite/pokalwettbewerb/CL" in fetch.calls
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
    assert competitions["GB1W"].classification_status is ClassificationStatus.EXCLUDED
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
        (item.competition_id, item.edition_id): item for item in snapshot.editions
    }
    assert editions[("GB1", "2025")].canonical_season == "2526"
    assert editions[("UNLA", "2026")].canonical_season == "2627"
    assert editions[("AFCN", "2025")].canonical_season == "2025"
    assert editions[("FIWC", "2026")].canonical_season == "2026"
    assert editions[("GB1", "2025")].participant_count == 2
    assert editions[("FIWC", "2026")].participant_count == 2


def test_editions_derive_count_and_hash_from_exact_typed_participant_rows() -> None:
    pages, *_ = _discover()
    snapshot = reconcile_registry_pages(pages)
    edition = next(
        item
        for item in snapshot.editions
        if (item.competition_id, item.edition_id) == ("GB1", "2025")
    )
    participants = tuple(
        item
        for item in snapshot.participants
        if (item.competition_id, item.edition_id) == ("GB1", "2025")
    )

    assert [(item.team_id, item.team_name) for item in participants] == [
        ("11", "Arsenal FC"),
        ("631", "Chelsea FC"),
    ]
    assert edition.participant_count == len(participants)
    assert edition.participant_hash == participant_list_hash(participants)
    assert len(edition.participant_hash) == 64
    assert all(
        item.registry_snapshot_id == snapshot.snapshot_id for item in participants
    )
    assert all(item.source_body_hash for item in participants)

    historical = {
        item.edition_id
        for item in snapshot.participants
        if item.competition_id == "GB1"
    }
    assert historical == {"2024", "2025"}


def test_section_taxonomy_and_main_taxonomy_are_source_evidence_not_names() -> None:
    pages, *_ = _discover()
    snapshot = reconcile_registry_pages(pages)
    gb1 = next(item for item in snapshot.competitions if item.competition_id == "GB1")
    section = next(
        item for item in gb1.evidence if item.source_field == "section_label"
    )
    audience = next(
        item for item in gb1.evidence if item.source_field == "transfermarkt_taxonomy"
    )
    assert section.source_value == "National leagues"
    assert section.origin is EvidenceOrigin.SOURCE_PAGE
    assert section.competition_type is CompetitionType.DOMESTIC_LEAGUE
    assert audience.source_value == "main men's competitions taxonomy"
    assert audience.origin is EvidenceOrigin.STRUCTURED


def test_womens_section_is_source_backed_exclusion_without_default_mens_signal() -> (
    None
):
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
    assert all(item.source_field != "transfermarkt_taxonomy" for item in women.evidence)
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
    assert ledger.cache_hits == len(EXPECTED_URLS) == 24
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
            "<!doctype html><html><body>"
            f'<h1 data-competition-id="{url.rsplit("/", 1)[-1]}">x</h1>'
            "</body></html>"
        )
        for url in URL_FIXTURES
        if "wettbewerb/" in url
    }

    with pytest.raises(DiscoverySchemaError, match="edition selector missing"):
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
