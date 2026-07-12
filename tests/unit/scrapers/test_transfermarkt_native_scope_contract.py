"""Offline end-to-end contracts for exact Transfermarkt native scopes."""
from __future__ import annotations

from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlsplit

import pytest

import scrapers.transfermarkt.scraper as tm
from scrapers.transfermarkt.client import TransfermarktHttpClient
from scrapers.transfermarkt.models import ProxyRequiredError
from scrapers.transfermarkt.registry import (
    CompetitionRecord,
    deterministic_scope_id,
    resolve_competition,
)
from scrapers.transfermarkt.scraper import (
    TransfermarktError,
    TransfermarktScraper,
    _competition_listing_url,
)


pytestmark = pytest.mark.unit


def _patch_one_squad(monkeypatch, scraper: TransfermarktScraper):
    """Return one deterministic squad row while recording logical fetches."""

    calls = []
    monkeypatch.setattr(
        tm,
        "_parse_club_listing",
        lambda _html: [
            {
                "club_id": "10",
                "club_slug": "example-team",
                "club_name": "Example Team",
            }
        ],
    )
    monkeypatch.setattr(
        tm,
        "_parse_squad_page",
        lambda _html, club_id: [
            {
                "player_id": "100",
                "player_slug": "example-player",
                "name": "Example Player",
                "club_id": club_id,
                "position": "Forward",
                "dob": None,
                "age": 25,
                "height_cm": 180,
                "foot": "right",
                "nationality": "Example",
                "contract_until": None,
                "market_value_eur": 1_000_000,
            }
        ],
    )

    def _fetch(url, label="html", context=None):
        calls.append({"url": url, "label": label, "context": context})
        return "<html/>"

    monkeypatch.setattr(scraper, "_fetch_html", _fetch)
    return calls


def test_no_static_transfermarkt_league_map_remains():
    assert "TM_LEAGUE_MAP" not in vars(tm)


def test_split_and_single_year_scopes_reach_native_output(monkeypatch):
    scraper = TransfermarktScraper()
    calls = _patch_one_squad(monkeypatch, scraper)

    premier_league = scraper.read_squad_data("GB1", 2025)
    world_cup = scraper.read_squad_data("FIWC", 2026)

    assert premier_league["memberships"].iloc[0]["season"] == "2526"
    assert premier_league["memberships"].iloc[0]["edition_id"] == "2025"
    assert world_cup["memberships"].iloc[0]["season"] == "2026"
    assert world_cup["memberships"].iloc[0]["edition_id"] == "2026"
    assert [call["label"] for call in calls].count("squad") == 2


def test_one_squad_response_materializes_three_stable_native_entities(
    monkeypatch,
):
    monkeypatch.setenv("TM_RUN_ID", "cycle-fixture")
    scraper = TransfermarktScraper()
    calls = _patch_one_squad(monkeypatch, scraper)

    bundle = scraper.read_squad_data("GB1", 2025)
    memberships = bundle["memberships"]
    observations = bundle["attribute_observations"]
    contracts = bundle["contract_observations"]

    assert [call["label"] for call in calls] == ["listing", "squad"]
    assert len(memberships) == len(observations) == len(contracts) == 1
    assert not memberships.duplicated(
        ["competition_id", "edition_id", "club_id", "player_id"]
    ).any()
    assert not observations.duplicated(
        ["competition_id", "edition_id", "club_id", "player_id", "observed_at"]
    ).any()
    assert not contracts.duplicated(
        ["competition_id", "edition_id", "team_id", "player_id", "observed_at"]
    ).any()

    scope_id = deterministic_scope_id("GB1", "2025")
    squad_url = next(call["url"] for call in calls if call["label"] == "squad")
    for frame in (memberships, observations, contracts):
        row = frame.iloc[0]
        assert row["scope_id"] == scope_id
        assert row["cycle_id"] == "cycle-fixture"
        assert row["source_url"] == squad_url
        assert row["source_body_hash"] == resolve_competition("GB1").source_body_hash
    assert memberships.iloc[0]["source_competition_id"] == "GB1"
    assert observations.iloc[0]["source_edition_id"] == "2025"
    assert contracts.iloc[0]["applicability_status"] == "ok"
    capture = scraper.get_scope_capture()
    assert capture["listing_status"] == "ok"
    assert capture["expected_team_ids"] == ["10"]
    assert capture["observed_team_ids"] == ["10"]
    assert capture["endpoint_status_by_team"] == {"10": "ok"}
    assert capture["competition_type"] == "domestic_league"


def test_national_team_contracts_are_explicitly_not_applicable(monkeypatch):
    scraper = TransfermarktScraper()
    calls = _patch_one_squad(monkeypatch, scraper)

    bundle = scraper.read_squad_data("FIWC", 2026)

    assert len(bundle["memberships"]) == 1
    assert len(bundle["attribute_observations"]) == 1
    assert bundle["contract_observations"].empty
    assert bundle["contract_observations"].attrs["fetch_status"] == "not_applicable"
    assert [call["label"] for call in calls].count("squad") == 1


def test_unknown_classification_fails_before_fetch(monkeypatch):
    unknown = CompetitionRecord.from_mapping(
        {
            "competition_id": "UNK",
            "slug": "unknown",
            "name": "Unknown competition",
            "country": "Unknown",
            "confederation": "Unknown",
            "competition_type": "unknown",
            "gender": "unknown",
            "team_type": "unknown",
            "age_category": "unknown",
            "season_format": "unknown",
            "active": True,
            "source_url": (
                "https://www.transfermarkt.com/unknown/startseite/wettbewerb/UNK"
            ),
            "discovered_at": "2026-07-11T00:00:00+00:00",
        }
    )
    scraper = TransfermarktScraper(competition_records=[unknown])
    fetch = MagicMock(side_effect=AssertionError("fetch must not run"))
    monkeypatch.setattr(scraper, "_fetch_html", fetch)

    with pytest.raises(TransfermarktError, match="classification blocks crawl"):
        scraper.read_squad_data("UNK", 2026)

    fetch.assert_not_called()


@pytest.mark.parametrize(
    ("competition_id", "route"),
    [("CL", "pokalwettbewerb"), ("FIWC", "wettbewerb")],
)
def test_listing_url_preserves_discovered_source_route(competition_id, route):
    record = resolve_competition(competition_id)
    url = _competition_listing_url(record, 2026)
    parsed = urlsplit(url)

    assert parsed.hostname == "www.transfermarkt.us"
    assert f"/{route}/{competition_id}/" in parsed.path
    assert parse_qs(parsed.query) == {"saison_id": ["2026"]}


def test_global_market_value_and_transfer_rows_retain_scope_lineage(monkeypatch):
    monkeypatch.setenv("TM_RUN_ID", "cycle-global")
    scraper = TransfermarktScraper()

    def _fetch(_url, label="json", context=None):
        if label == "market_value_points":
            return {
                "list": [
                    {
                        "datum_mw": "Jan 1, 2026",
                        "y": 1_000_000,
                        "mw": "€1.00m",
                        "verein": "Example Team",
                        "age": "25",
                    }
                ]
            }
        if label == "transfer_events":
            return {
                "transfers": [
                    {
                        "id": "source-transfer-1",
                        "date": "Jul 1, 2025",
                        "season": "25/26",
                        "from": {"clubName": "A", "href": "/verein/1/"},
                        "to": {"clubName": "B", "href": "/verein/2/"},
                        "fee": "€1.00m",
                        "marketValue": "€2.00m",
                        "upcoming": False,
                    }
                ]
            }
        raise AssertionError((label, context))

    monkeypatch.setattr(scraper, "_fetch_json", _fetch)
    market_values = scraper.read_market_value_points(
        "GB1", 2025, player_ids=["100"]
    )
    transfers = scraper.read_transfer_events("GB1", 2025, player_ids=["100"])

    expected_scope = deterministic_scope_id("GB1", "2025")
    for frame in (market_values, transfers):
        row = frame.iloc[0]
        assert row["source_competition_id"] == "GB1"
        assert row["source_edition_id"] == "2025"
        assert row["scope_id"] == expected_scope
        assert row["cycle_id"] == "cycle-global"
        assert row["source_url"].startswith("https://www.transfermarkt.us/")
        assert "league" not in frame.columns
        assert "season" not in frame.columns


def test_metered_lease_requirement_fails_before_transport_construction(
    monkeypatch,
):
    monkeypatch.setenv("TM_REQUIRE_METERED_PROXY", "true")
    monkeypatch.delenv("TM_PROXY_CONTROL_URL", raising=False)
    transport = MagicMock(side_effect=AssertionError("transport must not be built"))
    monkeypatch.setattr(tm, "TransfermarktHttpClient", transport)

    with pytest.raises(TransfermarktError, match="requires TM_PROXY_CONTROL_URL"):
        TransfermarktScraper()

    transport.assert_not_called()


def test_missing_proxy_client_fails_before_any_network_client_is_built():
    network_factory = MagicMock(side_effect=AssertionError("network I/O"))
    client = TransfermarktHttpClient(client_factory=network_factory)

    with pytest.raises(ProxyRequiredError, match="requires a residential proxy"):
        client.fetch("https://www.transfermarkt.us/test", as_json=False)

    network_factory.assert_not_called()
