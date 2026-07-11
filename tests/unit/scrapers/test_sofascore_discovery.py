"""Direct-only SofaScore discovery, merge, and traffic contracts."""

from __future__ import annotations

import json
import stat
from copy import deepcopy
from pathlib import Path

import pytest

from scrapers.sofascore.catalog import SofaScoreCatalog
from scrapers.sofascore.discovery import (
    CATALOG_PATH,
    CATEGORIES_FALLBACK_PATH,
    CATEGORIES_PATH,
    TOURNAMENT_PATH,
    DirectSofaScoreClient,
    DiscoveryConcurrentUpdate,
    DiscoveryHTTPError,
    DiscoverySchemaError,
    classify_season_year,
    discover_registry,
    parse_catalog_payload,
    parse_categories_payload,
    parse_seasons_payload,
    write_registry_atomic,
)


TOURNAMENTS = [
    (17, "Premier League", "premier-league", "England", "england", 76986, "25/26"),
    (16, "World Cup", "world-championship", "World", "world", 58210, "2026"),
    (7, "UEFA Champions League", "uefa-champions-league", "Europe", "europe", 76953, "25/26"),
    (203, "Premier League", "premier-liga", "Russia", "russia", 77001, "25/26"),
    (270, "Africa Cup of Nations", "africa-cup-of-nations", "Africa", "africa", 71636, "2025"),
]


def _catalog_item(source_id, name, slug, category_name, category_slug):
    return {
        "id": source_id,
        "name": name,
        "slug": slug,
        "gender": "M",
        "category": {
            "id": source_id + 1000,
            "name": category_name,
            "slug": category_slug,
            "sport": {"id": 1, "name": "Football", "slug": "football"},
        },
    }


def _catalog_payload(items=TOURNAMENTS):
    return {
        "uniqueTournaments": [
            _catalog_item(*item[:5]) for item in items
        ]
    }


def _categories_payload():
    return {
        "categories": [
            _catalog_item(*item[:5])["category"] for item in TOURNAMENTS
        ]
    }


def _category_payload(item):
    return {
        "groups": [{
            "name": "All",
            "uniqueTournaments": [_catalog_item(*item[:5])],
        }]
    }


def _season_payload(item):
    source_id, name, _, _, _, season_id, year = item
    return {
        "seasons": [{
            "id": season_id,
            "name": f"{name} {year}",
            "year": year,
        }]
    }


class _FakeClient:
    def __init__(self):
        # UCL, RPL, and AFCON deliberately exist only in the category scan.
        # This prevents a regression back to SofaScore's curated major list.
        self.payloads = {
            CATALOG_PATH: _catalog_payload(TOURNAMENTS[:2]),
            CATEGORIES_PATH: _categories_payload(),
        }
        self.payloads.update({
            f"/category/{item[0] + 1000}/unique-tournaments": (
                _category_payload(item)
            )
            for item in TOURNAMENTS
        })
        self.payloads.update({
            f"/unique-tournament/{item[0]}/seasons": _season_payload(item)
            for item in TOURNAMENTS
        })
        self.calls = []

    def get_json(self, path):
        self.calls.append(path)
        result = self.payloads[path]
        if isinstance(result, Exception):
            raise result
        return deepcopy(result)

    @property
    def stats(self):
        return {
            "requests": len(self.calls),
            "direct_response_bytes": 12345,
            "paid_proxy_bytes": 0,
            "browser_sessions": 0,
            "browser_navigations": 0,
        }


class _Response:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"ok": True}
        self.content = json.dumps(self._payload).encode()

    def json(self):
        return deepcopy(self._payload)


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}
        self.trust_env = True
        self.proxies = {"https": "http://paid.invalid"}
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)

    def close(self):
        pass


@pytest.mark.unit
@pytest.mark.parametrize(
    "label, expected",
    [
        ("25/26", ("split_year", "2526")),
        ("2025/26", ("split_year", "2526")),
        ("2025/2026", ("split_year", "2526")),
        ("2026", ("single_year", "2026")),
        ("2025/27", ("unknown", None)),
        ("Apertura 2026", ("unknown", None)),
    ],
)
def test_season_year_classification(label, expected):
    assert classify_season_year(label) == expected


@pytest.mark.unit
def test_five_required_tournaments_and_seasons_are_parsed_correctly():
    parsed = parse_catalog_payload(_catalog_payload())
    by_id = {item["unique_tournament_id"]: item for item in parsed}

    assert by_id[17]["page_path"] == "football/england/premier-league"
    assert by_id[16]["page_path"] == "football/world/world-championship"
    assert by_id[7]["page_path"] == "football/europe/uefa-champions-league"
    assert by_id[203]["page_path"] == "football/russia/premier-liga"
    assert by_id[270]["page_path"] == "football/africa/africa-cup-of-nations"

    expected = {
        17: ("split_year", "2526"),
        16: ("single_year", "2026"),
        7: ("split_year", "2526"),
        203: ("split_year", "2526"),
        270: ("single_year", "2025"),
    }
    for item in TOURNAMENTS:
        season = parse_seasons_payload(_season_payload(item), item[0])[0]
        assert (season["season_format"], season["canonical_season"]) == (
            expected[item[0]]
        )


@pytest.mark.unit
def test_category_index_and_grouped_tournaments_are_parsed():
    categories = parse_categories_payload(_categories_payload())
    assert [category["id"] for category in categories] == sorted(
        item[0] + 1000 for item in TOURNAMENTS
    )

    grouped = parse_catalog_payload(_category_payload(TOURNAMENTS[0]))
    assert grouped[0]["unique_tournament_id"] == 17
    assert parse_catalog_payload({"groups": []}) == []


@pytest.mark.unit
def test_discovery_preserves_activation_and_defaults_new_records_off():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    merged, report = discover_registry(existing, _FakeClient())
    catalog = SofaScoreCatalog.from_mapping(merged)

    assert catalog.enabled_competition_ids() == (
        "ENG-Premier League",
        "INT-World Cup",
    )
    assert catalog.tournament(7).canonical_id is None
    assert catalog.tournament(7).enabled is False
    assert catalog.tournament(203).canonical_id is None
    assert catalog.resolve_season_id(7, "2526") == 76953
    assert catalog.resolve_season_id(270, "2025") == 71636
    assert report["catalog_tournaments"] == 5
    assert report["curated_tournaments"] == 2
    assert report["category_tournaments"] == 5
    assert report["categories"] == 5
    assert report["traffic"]["paid_proxy_bytes"] == 0
    assert report["traffic"]["browser_sessions"] == 0


@pytest.mark.unit
def test_category_all_404_uses_compatible_index_fallback():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    client = _FakeClient()
    client.payloads[CATEGORIES_FALLBACK_PATH] = client.payloads[CATEGORIES_PATH]
    client.payloads[CATEGORIES_PATH] = DiscoveryHTTPError(
        "not found",
        status_code=404,
    )

    _, report = discover_registry(existing, client)

    assert CATEGORIES_FALLBACK_PATH in client.calls
    assert report["categories"] == 5


@pytest.mark.unit
def test_active_reviewed_scope_skips_catalog_and_refreshes_only_reviewed():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    client = _FakeClient()
    for item in TOURNAMENTS[:2]:
        client.payloads[TOURNAMENT_PATH.format(
            unique_tournament_id=item[0]
        )] = {"uniqueTournament": _catalog_item(*item[:5])}

    _, report = discover_registry(existing, client, scope="active-reviewed")

    assert CATALOG_PATH not in client.calls
    assert CATEGORIES_PATH not in client.calls
    assert report["scope"] == "active-reviewed"
    assert report["catalog_tournaments"] == 2


@pytest.mark.unit
def test_empty_category_index_fails_closed():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    client = _FakeClient()
    client.payloads[CATEGORIES_PATH] = {"categories": []}

    with pytest.raises(DiscoverySchemaError, match="must not be empty"):
        discover_registry(existing, client)


@pytest.mark.unit
def test_category_fanout_404_fails_instead_of_writing_curated_subset():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    client = _FakeClient()
    first_category = TOURNAMENTS[0][0] + 1000
    client.payloads[f"/category/{first_category}/unique-tournaments"] = (
        DiscoveryHTTPError("not found", status_code=404)
    )

    with pytest.raises(DiscoveryHTTPError, match="incomplete category scan"):
        discover_registry(existing, client)


@pytest.mark.unit
def test_missing_enabled_tournament_seasons_fails_closed():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    client = _FakeClient()
    client.payloads["/unique-tournament/17/seasons"] = DiscoveryHTTPError(
        "not found",
        status_code=404,
    )

    with pytest.raises(DiscoverySchemaError, match="enabled tournament 17"):
        discover_registry(existing, client)


@pytest.mark.unit
def test_partial_season_traversal_cannot_shrink_existing_registry():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    client = _FakeClient()
    client.payloads["/unique-tournament/270/seasons"] = {"seasons": []}

    with pytest.raises(DiscoverySchemaError, match="season traversal shrank"):
        discover_registry(existing, client)


@pytest.mark.unit
def test_missing_old_tournaments_and_seasons_are_preserved():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    merged, _ = discover_registry(existing, _FakeClient())
    catalog = SofaScoreCatalog.from_mapping(merged)

    assert catalog.competition("ESP-La Liga").unique_tournament_id == 8
    assert catalog.resolve_season_id(16, "2026") == 58210


@pytest.mark.unit
def test_source_season_id_replacement_does_not_create_a_duplicate():
    existing = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    epl = next(
        item for item in existing["tournaments"]
        if item["unique_tournament_id"] == 17
    )
    epl["seasons"][0]["season_id"] = 111
    epl["seasons"][0]["aliases"].append("operator-alias")
    client = _FakeClient()

    merged, _ = discover_registry(existing, client)
    catalog = SofaScoreCatalog.from_mapping(merged)

    assert catalog.resolve_season_id(17, "2526") == 76986
    assert [season.season_id for season in catalog.tournament(17).seasons] == [
        76986
    ]
    assert "operator-alias" in catalog.tournament(17).seasons[0].aliases


@pytest.mark.unit
def test_exact_catalog_duplicate_is_deduped_but_conflict_fails():
    item = _catalog_item(*TOURNAMENTS[0][:5])
    assert len(parse_catalog_payload({"uniqueTournaments": [item, item]})) == 1

    conflicting = deepcopy(item)
    conflicting["slug"] = "wrong-league"
    with pytest.raises(DiscoverySchemaError, match="conflicting duplicate"):
        parse_catalog_payload({"uniqueTournaments": [item, conflicting]})


@pytest.mark.unit
def test_exact_season_duplicate_is_deduped_but_conflict_fails():
    season = _season_payload(TOURNAMENTS[0])["seasons"][0]
    assert len(parse_seasons_payload({"seasons": [season, season]}, 17)) == 1

    conflicting = deepcopy(season)
    conflicting["year"] = "26/27"
    with pytest.raises(DiscoverySchemaError, match="conflicting season_id"):
        parse_seasons_payload({"seasons": [season, conflicting]}, 17)


@pytest.mark.unit
def test_two_source_labels_cannot_resolve_to_one_canonical_season():
    first = {"id": 1, "name": "League 25/26", "year": "25/26"}
    second = {
        "id": 2,
        "name": "League 2025/2026",
        "year": "2025/2026",
    }

    with pytest.raises(DiscoverySchemaError, match="ambiguous canonical_season"):
        parse_seasons_payload({"seasons": [first, second]}, 17)


@pytest.mark.unit
def test_direct_client_ignores_poison_proxy_environment(monkeypatch):
    for name in (
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
        "http_proxy", "https_proxy", "all_proxy", "PROXY_FILE",
    ):
        monkeypatch.setenv(name, "http://paid.invalid:9999")
    session = _Session([_Response(payload={"value": 1})])
    client = DirectSofaScoreClient(
        session_factory=lambda: session,
        sleeper=lambda _: None,
    )

    assert client.get_json("/test") == {"value": 1}
    assert session.trust_env is False
    assert session.proxies == {}
    assert session.calls[0][1]["proxies"] == {}
    assert session.headers["X-Requested-With"] == "XMLHttpRequest"
    assert session.headers["Origin"] == "https://www.sofascore.com"
    assert any(
        int(option) == 10004 and value == ""
        for option, value in session.curl_options.items()
    )
    assert client.stats["paid_proxy_bytes"] == 0
    assert client.stats["browser_sessions"] == 0
    assert client.stats["browser_navigations"] == 0


@pytest.mark.unit
def test_direct_403_fails_immediately_and_counts_only_direct_bytes():
    session = _Session([_Response(403, {"error": {"code": 403}})])
    client = DirectSofaScoreClient(
        session_factory=lambda: session,
        max_attempts=3,
        sleeper=lambda _: pytest.fail("403 must not retry"),
    )

    with pytest.raises(DiscoveryHTTPError) as exc_info:
        client.get_json("/blocked")

    assert exc_info.value.status_code == 403
    assert client.stats["requests"] == 1
    assert client.stats["direct_response_bytes"] > 0
    assert client.stats["paid_proxy_bytes"] == 0


@pytest.mark.unit
def test_retryable_status_has_a_bounded_direct_retry():
    session = _Session([
        _Response(503, {"error": "busy"}),
        _Response(200, {"ok": True}),
    ])
    client = DirectSofaScoreClient(
        session_factory=lambda: session,
        max_attempts=2,
        sleeper=lambda _: None,
    )

    assert client.get_json("/retry") == {"ok": True}
    assert client.stats["requests"] == 2
    assert client.stats["paid_proxy_bytes"] == 0


@pytest.mark.unit
def test_atomic_writer_is_idempotent(tmp_path):
    document = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    path = tmp_path / "registry.json"

    assert write_registry_atomic(path, document) is True
    before = path.read_bytes()
    before_stat = path.stat().st_mtime_ns
    assert write_registry_atomic(
        path,
        deepcopy(document),
        expected_current=document,
    ) is False
    assert path.read_bytes() == before
    assert path.stat().st_mtime_ns == before_stat


@pytest.mark.unit
def test_atomic_writer_preserves_registry_read_mode(tmp_path):
    document = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    path = tmp_path / "registry.json"
    path.write_text("{}", encoding="utf-8")
    path.chmod(0o644)

    assert write_registry_atomic(path, document) is True
    assert stat.S_IMODE(path.stat().st_mode) == 0o644


@pytest.mark.unit
def test_atomic_writer_rejects_concurrent_activation_change(tmp_path):
    expected = json.loads(
        Path("configs/sofascore/tournaments.json").read_text(encoding="utf-8")
    )
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(expected), encoding="utf-8")

    concurrent = deepcopy(expected)
    laliga = next(
        item for item in concurrent["tournaments"]
        if item["unique_tournament_id"] == 8
    )
    laliga["enabled"] = True
    path.write_text(json.dumps(concurrent), encoding="utf-8")

    discovered = deepcopy(expected)
    discovered["tournaments"][0]["name"] += " refreshed"
    with pytest.raises(DiscoveryConcurrentUpdate, match="changed during discovery"):
        write_registry_atomic(
            path,
            discovered,
            expected_current=expected,
        )

    assert json.loads(path.read_text(encoding="utf-8")) == concurrent
