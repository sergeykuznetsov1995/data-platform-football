from unittest.mock import MagicMock

from scrapers.fbref.discovery_service import FBrefDiscoveryService
from scrapers.fbref.raw_store import RawPageStore


INDEX_HTML = """
<h2>Domestic Leagues - 1st Tier</h2>
<table id="comps_test"><tbody><tr>
  <th data-stat="league_name">
    <a href="/en/comps/999/history/Test-League-Seasons">Test League</a>
  </th>
  <td data-stat="gender">F</td><td data-stat="country">xx TST</td>
  <td data-stat="first_season">2024</td>
  <td data-stat="last_season">2025</td>
</tr></tbody></table>
"""

COMPETITION_HTML = """
<table id="seasons"><tbody><tr>
  <th data-stat="year_id">
    <a href="/en/comps/999/edition-1/2025-Test-League-Stats">2025</a>
  </th>
</tr></tbody></table>
"""

SEASON_HTML = """
<nav><a href="/en/comps/999/edition-1/schedule/source-fixtures">
Scores &amp; Fixtures</a></nav>
"""

SCHEDULE_HTML = """
<table id="sched_all"><tbody>
  <tr><th data-stat="date">2025-01-01</th>
    <td data-stat="home_team">Alpha</td>
    <td data-stat="away_team">Beta</td>
    <td data-stat="match_report">
      <a href="/en/matches/abcdef12/source-match">Match Report</a>
    </td>
  </tr>
  <tr><th data-stat="date">2025-01-08</th>
    <td data-stat="home_team">Gamma</td>
    <td data-stat="away_team">Delta</td>
  </tr>
</tbody></table>
"""


def _store(tmp_path):
    return RawPageStore.from_uri(tmp_path.as_uri())


def _loader():
    pages = {
        "https://fbref.com/en/comps": INDEX_HTML,
        "https://fbref.com/en/comps/999/history/Test-League-Seasons": (
            COMPETITION_HTML
        ),
        "https://fbref.com/en/comps/999/edition-1/2025-Test-League-Stats": (
            SEASON_HTML
        ),
        "https://fbref.com/en/comps/999/edition-1/schedule/source-fixtures": (
            SCHEDULE_HTML
        ),
    }
    return MagicMock(side_effect=lambda url, _kind: pages[url])


def test_discovery_graph_commits_each_page_then_replays_offline(tmp_path):
    store = _store(tmp_path)
    loader = _loader()
    online = FBrefDiscoveryService(
        store,
        loader=loader,
        max_network_pages=4,
    )

    first = online.discover_graph(["999"])

    assert first.ok
    assert loader.call_count == 4
    assert first.raw_writes == 4
    assert first.raw_hits == 0
    assert first.network_pages == 4
    assert first.competitions[0].competition_id == "999"
    assert first.seasons[0].season_id == "edition-1"
    assert first.schedules == [{
        "comp_id": "999",
        "season_id": "edition-1",
        "schedule_url": (
            "https://fbref.com/en/comps/999/edition-1/schedule/"
            "source-fixtures"
        ),
        "row_count": 2,
        "match_count": 1,
        "status": "available",
    }]
    assert [match.match_id for match in first.matches] == ["abcdef12"]
    assert len(first.page_manifests) == 4

    offline = FBrefDiscoveryService(
        store,
        offline=True,
        max_network_pages=0,
    )
    second = offline.discover_graph(["999"])

    assert second.ok
    assert second.raw_hits == 4
    assert second.raw_writes == 0
    assert second.network_pages == 0
    assert second.matches == first.matches
    assert second.page_manifests == first.page_manifests


def test_network_budget_stops_before_schedule_fetch(tmp_path):
    loader = _loader()
    service = FBrefDiscoveryService(
        _store(tmp_path),
        loader=loader,
        max_network_pages=3,
    )

    result = service.discover_graph(["999"])

    assert not result.ok
    assert loader.call_count == 3
    assert result.network_pages == 3
    assert result.matches == []
    assert result.errors[-1]["error_type"] == "NetworkPageBudgetExceeded"


def test_offline_missing_raw_never_constructs_or_calls_a_loader(tmp_path):
    service = FBrefDiscoveryService(
        _store(tmp_path),
        offline=True,
        max_network_pages=0,
    )

    result = service.discover_index()

    assert not result.ok
    assert result.network_pages == 0
    assert result.raw_writes == 0
    assert result.errors[0]["error_type"] == "RawPageNotFound"


def test_failed_loader_attempt_consumes_the_network_budget(tmp_path):
    loader = MagicMock(return_value=None)
    service = FBrefDiscoveryService(
        _store(tmp_path),
        loader=loader,
        max_network_pages=1,
    )

    first = service.discover_index()
    second = service.discover_index()

    assert not first.ok and not second.ok
    assert first.network_pages == 1
    assert second.network_pages == 1
    assert loader.call_count == 1
    assert first.errors[0]["error_type"] == "RawStoreError"
    assert second.errors[0]["error_type"] == "NetworkPageBudgetExceeded"


def test_explicit_unknown_competition_is_an_error_not_an_empty_success(tmp_path):
    loader = _loader()
    service = FBrefDiscoveryService(
        _store(tmp_path),
        loader=loader,
        max_network_pages=1,
    )

    result = service.discover_graph(["does-not-exist"])

    assert not result.ok
    assert loader.call_count == 1
    assert result.errors[0]["reason"] == "competition_not_discovered"
