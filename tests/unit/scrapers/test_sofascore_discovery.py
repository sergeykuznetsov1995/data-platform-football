"""Direct-only SofaScore discovery, merge, and traffic contracts."""

from __future__ import annotations

import json
import stat
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from scrapers.sofascore.catalog import SofaScoreCatalog
from scrapers.sofascore.discovery import (
    CATALOG_PATH,
    CATEGORIES_FALLBACK_PATH,
    CATEGORIES_PATH,
    DISCOVERY_HEADERS,
    TOURNAMENT_PATH,
    DirectSofaScoreClient,
    DiscoveryConcurrentUpdate,
    DiscoveryError,
    DiscoveryHTTPError,
    DiscoverySchemaError,
    LeaseProxySofaScoreClient,
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

# LaLiga is deliberately absent from TOURNAMENTS: it only ever lives in the
# existing registry, never in a _FakeClient response, so it is the tournament
# that proves a record missing from the source scan is preserved.
LALIGA = (8, "LaLiga", "laliga", "Spain", "spain", 61643, "25/26")


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


def _pending_review_record():
    return {
        "status": "pending",
        "confirmed": {
            "sport": None,
            "gender": None,
            "age_group": None,
            "team_level": None,
        },
        "reviewed_by": None,
        "reviewed_at": None,
        "evidence": [],
        "notes": None,
    }


def _approved_review_record():
    return {
        "status": "approved",
        "confirmed": {
            "sport": "football",
            "gender": "male",
            "age_group": "adult",
            "team_level": "first_team",
        },
        "reviewed_by": "registry-v2-migration",
        "reviewed_at": "2026-07-11",
        "evidence": [{
            "type": "repository",
            "reference": "configs/medallion/competitions.yaml",
            "note": "adult men's first-team football",
        }],
        "notes": "Existing production activation retained after review.",
    }


def _registry_classification(source_id):
    return {
        "sport": "football",
        "gender": "unknown",
        "age_group": "unknown",
        "team_level": "unknown",
        "status": "unknown",
        "exclusion_reasons": [],
        "evidence": [{
            "type": "source_field",
            "endpoint": f"/unique-tournament/{source_id}",
            "field": "category.sport.slug",
            "value": "football",
        }],
    }


def _registry_season(item):
    source_id, name, _, _, _, season_id, year = item
    split_year = "/" in year
    label = f"{name} {year}"
    return {
        "season_id": season_id,
        "name": label,
        "source_name": label,
        "year": year,
        "format": "split_year" if split_year else "calendar_year",
        "season_format": "split_year" if split_year else "single_year",
        "canonical_season": year.replace("/", ""),
        "start_date": None,
        "end_date": None,
        "aliases": [year, label],
        "evidence": [{
            "type": "source_field",
            "endpoint": f"/unique-tournament/{source_id}/seasons",
            "field": "name/year",
            "value": f"{label}|{year}",
        }],
    }


def _registry_tournament(
    item,
    *,
    canonical_id=None,
    enabled=False,
    approved=False,
    seasons=True,
):
    source_id, name, slug, category_name, category_slug, _, _ = item
    return {
        "unique_tournament_id": source_id,
        "name": name,
        "slug": slug,
        "category": {
            "id": source_id + 1000,
            "name": category_name,
            "slug": category_slug,
        },
        "sport_slug": "football",
        "page_path": f"football/{category_slug}/{slug}",
        "canonical_id": canonical_id,
        "enabled": enabled,
        "classification": _registry_classification(source_id),
        "review": (
            _approved_review_record() if approved
            else _pending_review_record()
        ),
        "seasons": [_registry_season(item)] if seasons else [],
    }


def _existing_registry():
    """A minimal operator-owned registry, independent of the shipped file.

    The discovery contracts are about merge behaviour, not about whatever the
    production registry happens to contain today, so these tests own their
    input. Only the records the contracts need are present:

    * 17/16 are the activated tournaments (canonical_id + approved review).
    * 270 is an existing, non-activated tournament with one known season.
    * 8 (LaLiga) never appears in a _FakeClient response.
    * 7/203 are absent, so discovery sees them as brand-new records.

    Every season here matches the one _FakeClient serves for that tournament,
    so an unmodified scan never trips the "season traversal shrank" guard.
    """

    return {
        "source": "sofascore",
        "schema_version": 2,
        "operator_owned_fields": [
            "canonical_id",
            "enabled",
            "review",
            "seasons[].aliases",
            "seasons[].canonical_season_override",
        ],
        "tournaments": [
            _registry_tournament(LALIGA, canonical_id="ESP-La Liga", seasons=False),
            _registry_tournament(
                TOURNAMENTS[1],
                canonical_id="INT-World Cup",
                enabled=True,
                approved=True,
            ),
            _registry_tournament(
                TOURNAMENTS[0],
                canonical_id="ENG-Premier League",
                enabled=True,
                approved=True,
            ),
            _registry_tournament(
                TOURNAMENTS[4],
                canonical_id="INT-Africa Cup of Nations",
            ),
        ],
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
    existing = _existing_registry()
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
    existing = _existing_registry()
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
    existing = _existing_registry()
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
    existing = _existing_registry()
    client = _FakeClient()
    client.payloads[CATEGORIES_PATH] = {"categories": []}

    with pytest.raises(DiscoverySchemaError, match="must not be empty"):
        discover_registry(existing, client)


@pytest.mark.unit
def test_category_fanout_404_fails_instead_of_writing_curated_subset():
    existing = _existing_registry()
    client = _FakeClient()
    first_category = TOURNAMENTS[0][0] + 1000
    client.payloads[f"/category/{first_category}/unique-tournaments"] = (
        DiscoveryHTTPError("not found", status_code=404)
    )

    with pytest.raises(DiscoveryHTTPError, match="incomplete category scan"):
        discover_registry(existing, client)


@pytest.mark.unit
def test_missing_enabled_tournament_seasons_fails_closed():
    existing = _existing_registry()
    client = _FakeClient()
    client.payloads["/unique-tournament/17/seasons"] = DiscoveryHTTPError(
        "not found",
        status_code=404,
    )

    with pytest.raises(DiscoverySchemaError, match="enabled tournament 17"):
        discover_registry(existing, client)


@pytest.mark.unit
def test_partial_season_traversal_cannot_shrink_existing_registry():
    existing = _existing_registry()
    client = _FakeClient()
    client.payloads["/unique-tournament/270/seasons"] = {"seasons": []}

    with pytest.raises(DiscoverySchemaError, match="season traversal shrank"):
        discover_registry(existing, client)


@pytest.mark.unit
def test_missing_old_tournaments_and_seasons_are_preserved():
    existing = _existing_registry()
    merged, _ = discover_registry(existing, _FakeClient())
    catalog = SofaScoreCatalog.from_mapping(merged)

    assert catalog.competition("ESP-La Liga").unique_tournament_id == 8
    assert catalog.resolve_season_id(16, "2026") == 58210


@pytest.mark.unit
def test_source_season_id_replacement_does_not_create_a_duplicate():
    existing = _existing_registry()
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
def test_parallel_divisions_share_one_year_label_without_failing_the_scan():
    # Tournament 65 really ships both divisions under the same year label.
    east = {"id": 8298, "name": "2nd Division East 14/15", "year": "14/15"}
    west = {"id": 8300, "name": "2nd Division West 14/15", "year": "14/15"}

    seasons = parse_seasons_payload({"seasons": [east, west]}, 65)

    assert [season["season_id"] for season in seasons] == [8298, 8300]
    assert {season["year"] for season in seasons} == {"14/15"}
    # The ambiguity is not resolved here — it fails closed at resolve time.
    assert {season["canonical_season"] for season in seasons} == {"1415"}


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
    assert session.headers["x-requested-with"] == "XMLHttpRequest"
    assert session.headers["origin"] == "https://www.sofascore.com"
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


# --- targeted scope (machine layer for the batch onboarding, #946) ------------


@pytest.mark.unit
def test_targeted_scope_refreshes_only_the_named_tournaments():
    existing = _existing_registry()
    client = _FakeClient()
    laliga_detail = _catalog_item(*LALIGA[:5])
    client.payloads[TOURNAMENT_PATH.format(unique_tournament_id=8)] = {
        "uniqueTournament": laliga_detail,
    }
    client.payloads["/unique-tournament/8/seasons"] = _season_payload(LALIGA)

    merged, report = discover_registry(
        existing, client, scope="targeted", target_tournament_ids=[8],
    )

    assert client.calls == [
        "/unique-tournament/8",
        "/unique-tournament/8/seasons",
    ]
    assert report["scope"] == "targeted"
    assert report["catalog_tournaments"] == 1
    laliga = next(
        item for item in merged["tournaments"]
        if item["unique_tournament_id"] == 8
    )
    # The detail endpoint is the only place the source states gender, and it is
    # recorded as source evidence, not as a guess.
    assert laliga["classification"]["gender"] == "male"
    assert {
        "type": "source_field",
        "endpoint": "/unique-tournament/8",
        "field": "gender",
        "value": "M",
    } in laliga["classification"]["evidence"]
    assert [season["canonical_season"] for season in laliga["seasons"]] == ["2526"]


@pytest.mark.unit
def test_targeted_scope_keeps_untargeted_enabled_tournaments_untouched():
    # A targeted pass is explicitly partial. The complete-scan guard ("omitted
    # enabled tournaments") must not fire, and every other record must survive.
    existing = _existing_registry()
    client = _FakeClient()
    client.payloads[TOURNAMENT_PATH.format(unique_tournament_id=8)] = {
        "uniqueTournament": _catalog_item(*LALIGA[:5]),
    }
    client.payloads["/unique-tournament/8/seasons"] = _season_payload(LALIGA)

    merged, _ = discover_registry(
        existing, client, scope="targeted", target_tournament_ids=[8],
    )

    before = {
        item["unique_tournament_id"]: item
        for item in existing["tournaments"]
    }
    after = {
        item["unique_tournament_id"]: item
        for item in merged["tournaments"]
    }
    assert set(after) == set(before)
    for source_id in (16, 17, 270):
        assert after[source_id] == before[source_id]
    assert after[17]["enabled"] is True


@pytest.mark.unit
def test_targeted_scope_requires_ids_and_rejects_them_elsewhere():
    existing = _existing_registry()
    client = _FakeClient()

    with pytest.raises(ValueError, match="at least one tournament id"):
        discover_registry(existing, client, scope="targeted")
    with pytest.raises(ValueError, match="only meaningful for scope"):
        discover_registry(existing, client, target_tournament_ids=[8])
    assert client.calls == []


@pytest.mark.unit
def test_targeted_detail_response_for_another_tournament_fails_closed():
    existing = _existing_registry()
    client = _FakeClient()
    client.payloads[TOURNAMENT_PATH.format(unique_tournament_id=8)] = {
        "uniqueTournament": _catalog_item(*TOURNAMENTS[0][:5]),
    }

    with pytest.raises(DiscoverySchemaError, match="detail response for 8"):
        discover_registry(
            existing, client, scope="targeted", target_tournament_ids=[8],
        )


# --- metered lease-proxy transport (opt-in) ----------------------------------


CONTROL_URL = "http://proxy_filter:8899"
CONTROL_TOKEN = "c" * 32


class _ControlResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return deepcopy(self._payload)


class _FakeProxyFilter:
    """In-memory proxy-filter lease API and its byte meter (no sockets)."""

    def __init__(self, *, ttl_seconds=3600, now=1_000_000.0, repins=0):
        self.now = now
        self.ttl_seconds = ttl_seconds
        self.repins = repins
        self.trust_env = True
        self.leases = {}
        self.tokens = {}
        self.acquired = []
        self.closed = []
        self.close_error = None

    # --- control plane (requests.Session-shaped) ---
    def request(self, method, url, *, json=None, headers=None, timeout=None):
        assert headers["X-Proxy-Control-Token"] == CONTROL_TOKEN
        path = url[len(CONTROL_URL):]
        if method == "POST" and path == "/v1/leases":
            assert json["dag_id"] == "dag_discover_sofascore_registry"
            assert json["source"] == "sofascore_discovery"
            if any(not item["closed"] for item in self.leases.values()):
                return _ControlResponse(
                    429, {"error": "discovery concurrency limit reached"}
                )
            lease_id = f"lease-{len(self.leases) + 1}"
            token = f"token-{len(self.leases) + 1}"
            self.leases[lease_id] = {
                "id": lease_id,
                "source": "sofascore_discovery",
                "up_bytes": 0,
                "down_bytes": 0,
                "closed": False,
                "budget_exceeded": False,
                "upstream_repins": self.repins,
                "max_bytes": int(json["max_bytes"]),
            }
            self.tokens[token] = lease_id
            self.acquired.append(dict(json))
            return _ControlResponse(201, {
                "id": lease_id,
                "token": token,
                "proxy_url": "http://proxy_filter:8900",
                "max_bytes": int(json["max_bytes"]),
                "expires_at": self.now + int(json["ttl_seconds"]),
            })
        lease_id = path.split("/")[3]
        lease = self.leases[lease_id]
        if path.endswith("/close"):
            if self.close_error is not None:
                return _ControlResponse(500, {"error": self.close_error})
            lease["closed"] = True
            self.closed.append(lease_id)
        state = dict(lease)
        state["total_bytes"] = state["up_bytes"] + state["down_bytes"]
        return _ControlResponse(200, state)

    # --- data plane meter ---
    def bill(self, token, size):
        lease = self.leases[self.tokens[token]]
        lease["up_bytes"] += 100
        lease["down_bytes"] += size


class _LeaseSession:
    """Data-plane session pinned to exactly one lease's authenticated exit."""

    def __init__(self, proxy_filter, proxy_url, responses):
        from urllib.parse import urlsplit

        self.proxy_filter = proxy_filter
        self.proxy_url = proxy_url
        parsed = urlsplit(proxy_url)
        assert parsed.username == "lease"
        self.token = parsed.password
        self.headers = dict(DISCOVERY_HEADERS)
        self.trust_env = False
        self.responses = responses
        self.calls = []
        self.closed = False

    def get(self, url, *, timeout):
        self.calls.append(url)
        response = self.responses.popleft()
        if isinstance(response, Exception):
            raise response
        self.proxy_filter.bill(self.token, len(response.content))
        return response

    def close(self):
        self.closed = True


def _lease_client(proxy_filter, responses, **kwargs):
    from collections import deque

    from scrapers.sofascore.lease_client import _DiscoveryLeaseProvider

    queue = deque(responses)
    sessions = []

    def factory(proxy_url):
        session = _LeaseSession(proxy_filter, proxy_url, queue)
        sessions.append(session)
        return session

    client = LeaseProxySofaScoreClient(
        control_url=CONTROL_URL,
        budget_cap_bytes=kwargs.pop("budget_cap_bytes", 10_000_000),
        run_id="discovery__test",
        lease_provider=_DiscoveryLeaseProvider(
            CONTROL_URL, control_token=CONTROL_TOKEN, session=proxy_filter
        ),
        session_factory=factory,
        rate_limiter=SimpleNamespace(acquire=lambda: True),
        sleeper=lambda _seconds: None,
        clock=lambda: proxy_filter.now,
        **kwargs,
    )
    return client, sessions


@pytest.mark.unit
def test_lease_transport_meters_paid_bytes_and_never_opens_a_browser():
    proxy_filter = _FakeProxyFilter()
    client, sessions = _lease_client(
        proxy_filter, [_Response(200, {"seasons": []})]
    )

    payload = client.get_json("/unique-tournament/8/seasons")
    client.close()

    assert payload == {"seasons": []}
    # The exit is the lease's exit: Basic-auth lease URL, direct fingerprint.
    assert sessions[0].proxy_url.startswith("http://lease:token-1@proxy_filter:8900")
    assert sessions[0].headers["x-requested-with"] == "XMLHttpRequest"
    assert sessions[0].calls == [
        "https://api.sofascore.com/api/v1/unique-tournament/8/seasons"
    ]
    assert sessions[0].closed is True
    stats = client.stats
    lease = proxy_filter.leases["lease-1"]
    assert stats["paid_proxy_bytes"] == lease["up_bytes"] + lease["down_bytes"]
    assert stats["paid_proxy_bytes"] > 0
    assert stats["requests"] == 1
    assert stats["lease_count"] == 1
    assert stats["direct_response_bytes"] == 0
    assert stats["browser_sessions"] == 0
    assert stats["browser_navigations"] == 0
    assert proxy_filter.closed == ["lease-1"]


@pytest.mark.unit
def test_exhausted_lease_rotates_to_a_fresh_exit_and_bills_each_once():
    proxy_filter = _FakeProxyFilter(repins=1)
    payload = {"seasons": [], "padding": "x" * 4000}
    client, sessions = _lease_client(
        proxy_filter,
        [_Response(200, payload) for _ in range(3)],
        per_lease_max_bytes=5000,
    )

    for _ in range(3):
        client.get_json("/unique-tournament/8/seasons")
    client.close()

    # Two responses no longer fit under one 5000-byte lease, so the scan
    # continues on a second, freshly pinned exit.
    assert len(sessions) >= 2
    assert client.stats["lease_count"] == len(sessions)
    billed = sum(
        item["up_bytes"] + item["down_bytes"]
        for item in proxy_filter.leases.values()
    )
    assert client.stats["paid_proxy_bytes"] == billed
    assert client.stats["upstream_repins"] == len(sessions)
    # Every lease is closed exactly once: no lease is billed twice.
    assert sorted(proxy_filter.closed) == sorted(proxy_filter.leases)
    assert len(proxy_filter.closed) == len(set(proxy_filter.closed))
    assert all(item.closed for item in sessions)


@pytest.mark.unit
def test_expiring_lease_is_rotated_before_the_control_plane_kills_it():
    proxy_filter = _FakeProxyFilter(ttl_seconds=120)
    client, sessions = _lease_client(
        proxy_filter,
        [_Response(200, {"seasons": []}) for _ in range(2)],
        lease_ttl_seconds=120,
    )

    client.get_json("/unique-tournament/8/seasons")
    proxy_filter.now += 90  # inside the 60s expiry margin
    client.get_json("/unique-tournament/8/seasons")
    client.close()

    assert len(sessions) == 2
    assert client.stats["lease_count"] == 2
    assert proxy_filter.closed == ["lease-1", "lease-2"]


@pytest.mark.unit
def test_429_and_transport_errors_rotate_the_lease_without_double_billing():
    proxy_filter = _FakeProxyFilter()
    client, sessions = _lease_client(
        proxy_filter,
        [
            _Response(429, {"error": "slow down"}),
            ConnectionError("silent exit"),
            _Response(200, {"seasons": []}),
        ],
    )

    payload = client.get_json("/unique-tournament/8/seasons")
    client.close()

    assert payload == {"seasons": []}
    assert client.stats["requests"] == 3
    assert client.stats["lease_count"] == 3
    assert len(proxy_filter.closed) == 3
    assert len(set(proxy_filter.closed)) == 3
    assert client.stats["paid_proxy_bytes"] == sum(
        item["up_bytes"] + item["down_bytes"]
        for item in proxy_filter.leases.values()
    )


@pytest.mark.unit
def test_403_from_a_residential_exit_is_surfaced_without_retrying():
    proxy_filter = _FakeProxyFilter()
    client, sessions = _lease_client(
        proxy_filter, [_Response(403, {"error": "forbidden"})]
    )

    with pytest.raises(DiscoveryHTTPError) as excinfo:
        client.get_json("/unique-tournament/8/seasons")
    client.close()

    assert excinfo.value.status_code == 403
    assert client.stats["requests"] == 1
    assert client.stats["lease_count"] == 1
    # The refused response was still paid for, and it is reported.
    assert client.stats["paid_proxy_bytes"] > 0


@pytest.mark.unit
def test_scan_stops_at_the_client_budget_cap():
    proxy_filter = _FakeProxyFilter()
    payload = {"seasons": [], "padding": "x" * 3000}
    client, _ = _lease_client(
        proxy_filter,
        [_Response(200, payload) for _ in range(4)],
        per_lease_max_bytes=3200,
        budget_cap_bytes=4000,
    )

    with pytest.raises(DiscoveryError, match="paid-byte"):
        for _ in range(4):
            client.get_json("/unique-tournament/8/seasons")
    client.close()

    assert client.stats["paid_proxy_bytes"] <= 4000 + 3200
    # A lease is never issued for more than the remaining authorized bytes.
    assert all(
        int(item["max_bytes"]) <= 4000 for item in proxy_filter.acquired
    )


@pytest.mark.unit
def test_lease_transport_requires_an_explicit_positive_budget():
    for cap in (0, -1):
        with pytest.raises(ValueError, match="budget_cap_bytes"):
            LeaseProxySofaScoreClient(
                control_url=CONTROL_URL,
                budget_cap_bytes=cap,
                run_id="discovery__test",
                lease_provider=object(),
                rate_limiter=SimpleNamespace(acquire=lambda: True),
            )


@pytest.mark.unit
def test_unmeasurable_lease_close_charges_the_whole_allowance():
    # A control plane that cannot report the final counters is never assumed to
    # have billed zero bytes.
    proxy_filter = _FakeProxyFilter()
    proxy_filter.close_error = "meter unavailable"
    client, _ = _lease_client(
        proxy_filter, [_Response(200, {"seasons": []})], per_lease_max_bytes=4096,
    )

    client.get_json("/unique-tournament/8/seasons")
    client.close()

    assert client.stats["paid_proxy_bytes"] == 4096


@pytest.mark.unit
def test_discovery_traversal_runs_on_the_lease_transport_unchanged():
    # The traversal contract is transport agnostic: the same fake catalog served
    # through metered leases produces the same registry as the direct client.
    proxy_filter = _FakeProxyFilter()
    direct = _FakeClient()
    expected, _ = discover_registry(_existing_registry(), direct)

    responses = [
        _Response(200, direct.payloads[path]) for path in direct.calls
    ]
    client, _ = _lease_client(
        proxy_filter, responses, per_lease_max_bytes=1_000_000,
    )
    merged, report = discover_registry(_existing_registry(), client)
    client.close()

    assert merged == expected
    assert report["traffic"]["browser_sessions"] == 0
    assert client.stats["requests"] == len(direct.calls)
    assert client.stats["paid_proxy_bytes"] > 0
