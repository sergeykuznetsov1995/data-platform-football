from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from scrapers.fbref.discovery_queue import (
    DiscoveryQueueScopeMismatch,
    FBrefDiscoveryQueue,
)
from scrapers.fbref.discovery_service import FBrefDiscoveryService
from scrapers.fbref.raw_store import RawPageCorrupt, RawPageStore


COMPETITIONS = [
    ("10", "Test League", "Domestic Leagues - 1st Tier", "M", "aaaab010"),
    ("2", "Test Cup", "Domestic Cups", "M", "aaaab002"),
    (
        "30",
        "Test Nations League",
        "National Team Competitions",
        "M",
        "aaaab030",
    ),
    (
        "40",
        "Test Women's League",
        "Women's Domestic Leagues",
        "F",
        "aaaab040",
    ),
]


def _index_html() -> str:
    sections = []
    for competition_id, name, section, gender, _match_id in COMPETITIONS:
        slug = name.replace(" ", "-").replace("'", "")
        sections.append(f"""
<h2>{section}</h2>
<table><tbody><tr>
  <th data-stat="league_name">
    <a href="/en/comps/{competition_id}/history/{slug}-Seasons">{name}</a>
  </th>
  <td data-stat="gender">{gender}</td>
  <td data-stat="country">xx TST</td>
</tr></tbody></table>
""")
    return "".join(sections)


def _pages() -> dict:
    pages = {"https://fbref.com/en/comps": _index_html()}
    for competition_id, name, _section, _gender, match_id in COMPETITIONS:
        slug = name.replace(" ", "-").replace("'", "")
        history = f"https://fbref.com/en/comps/{competition_id}/history/{slug}-Seasons"
        season = f"https://fbref.com/en/comps/{competition_id}/2025/{slug}-Stats"
        schedule = (
            f"https://fbref.com/en/comps/{competition_id}/2025/schedule/"
            f"{slug}-Scores-and-Fixtures"
        )
        pages[history] = f"""
<table id="seasons"><tbody><tr><th data-stat="year_id">
  <a href="/en/comps/{competition_id}/2025/{slug}-Stats">2025</a>
</th></tr></tbody></table>
"""
        pages[season] = f"""
<nav><a href="/en/comps/{competition_id}/2025/schedule/{slug}-Scores-and-Fixtures">
Scores &amp; Fixtures</a></nav>
"""
        pages[schedule] = f"""
<table id="sched_all"><tbody><tr>
  <th data-stat="date">2025-01-01</th>
  <td data-stat="home_team">Alpha</td>
  <td data-stat="away_team">Beta</td>
  <td data-stat="match_report">
    <a href="/en/matches/{match_id}/{slug}">Match Report</a>
  </td>
</tr></tbody></table>
"""
    return pages


def _store(tmp_path) -> RawPageStore:
    return RawPageStore.from_uri(tmp_path.as_uri())


def _service(store, loader, max_network_pages=20):
    return FBrefDiscoveryService(
        store,
        loader=loader,
        max_network_pages=max_network_pages,
    )


def _loader(pages=None):
    source = pages or _pages()
    return MagicMock(side_effect=lambda url, _kind: source.get(url))


def test_queue_processes_stable_bounded_batches_and_resumes(tmp_path):
    store = _store(tmp_path)
    first_loader = _loader()
    first = FBrefDiscoveryQueue(
        store,
        _service(store, first_loader, max_network_pages=7),
    ).run("all-current", max_competitions=2)

    assert first.result.ok
    assert first.queue["attempted_competition_ids"] == ["2", "10"]
    assert first.queue["completed"] == 2
    assert first.queue["pending"] == 2
    assert first.queue["discovered_matches"] == 2
    assert not first.queue["complete"]
    assert first_loader.call_count == 7
    assert all(
        "/en/matches/" not in call.args[0]
        for call in first_loader.call_args_list
    )

    plan = store.read_discovery_queue_plan("all-current")
    assert plan["competition_ids"] == ["2", "10", "30", "40"]
    formats = {
        item["comp_id"]: item["format"]
        for item in plan["competitions"]
    }
    participants = {
        item["comp_id"]: item["participants"]
        for item in plan["competitions"]
    }
    genders = {
        item["comp_id"]: item["gender"]
        for item in plan["competitions"]
    }
    assert formats["2"] == "cup"
    assert participants["30"] == "national_team"
    assert genders["40"] == "F"

    second_loader = _loader()
    second = FBrefDiscoveryQueue(
        store,
        _service(store, second_loader, max_network_pages=6),
    ).run("all-current", max_competitions=2)

    assert second.result.ok
    assert second.queue["attempted_competition_ids"] == ["30", "40"]
    assert second.queue["completed"] == 4
    assert second.queue["pending"] == 0
    assert second.queue["complete"]
    assert second.queue["drained"]
    assert second.queue["stop_reason"] == "queue_drained"
    assert second.queue["discovered_matches"] == 4
    assert len(second.queue["all_item_manifests"]) == 4
    assert second_loader.call_count == 6
    second_item = store.read_discovery_queue_item("all-current", "40")
    assert second_item["result"]["raw"] == {
        "hits": 1,
        "writes": 3,
        "network_pages": 3,
    }

    third_loader = _loader()
    third = FBrefDiscoveryQueue(
        store,
        _service(store, third_loader, max_network_pages=1),
    ).run("all-current", max_competitions=2)

    assert third.result.ok
    assert third.queue["attempted_competition_ids"] == []
    assert third.queue["stop_reason"] == "queue_drained"
    assert third_loader.call_count == 0


def test_budget_pause_is_not_an_item_failure(tmp_path):
    store = _store(tmp_path)
    loader = _loader()
    run = FBrefDiscoveryQueue(
        store,
        _service(store, loader, max_network_pages=1),
    ).run("small-budget", max_competitions=2)

    assert run.result.ok
    assert run.result.network_pages == 1
    assert run.queue["stop_reason"] == "network_budget_exhausted"
    assert run.queue["attempted_competition_ids"] == ["2"]
    assert run.queue["completed"] == 0
    assert run.queue["pending"] == 4
    assert not store.has_discovery_queue_item("small-budget", "2")
    assert loader.call_count == 1


def test_offline_missing_raw_pauses_without_poisoning_online_resume(tmp_path):
    store = _store(tmp_path)
    seed_loader = _loader()
    seeded = FBrefDiscoveryQueue(
        store,
        _service(store, seed_loader, max_network_pages=1),
    ).run("offline-safe", max_competitions=1)
    assert seeded.queue["stop_reason"] == "network_budget_exhausted"

    offline = FBrefDiscoveryQueue(
        store,
        FBrefDiscoveryService(store, offline=True, max_network_pages=0),
    ).run("offline-safe", max_competitions=1)

    assert offline.result.ok
    assert offline.queue["status"] == "paused"
    assert offline.queue["stop_reason"] == "offline_raw_missing"
    assert not store.has_discovery_queue_item("offline-safe", "2")

    resume_loader = _loader()
    resumed = FBrefDiscoveryQueue(
        store,
        _service(store, resume_loader, max_network_pages=3),
    ).run("offline-safe", max_competitions=1)

    assert resumed.result.ok
    assert resumed.queue["completed"] == 1
    assert store.read_discovery_queue_item("offline-safe", "2")[
        "attempts"
    ] == 1
    assert resume_loader.call_count == 3


def test_failed_item_does_not_block_its_neighbor(tmp_path):
    store = _store(tmp_path)
    pages = _pages()
    pages.pop("https://fbref.com/en/comps/2/history/Test-Cup-Seasons")
    loader = _loader(pages)
    run = FBrefDiscoveryQueue(
        store,
        _service(store, loader, max_network_pages=8),
    ).run("one-broken", max_competitions=2)

    assert not run.result.ok
    assert run.queue["attempted_competition_ids"] == ["2", "10"]
    assert run.queue["completed"] == 1
    assert run.queue["retryable"] == 1
    broken = store.read_discovery_queue_item("one-broken", "2")
    healthy = store.read_discovery_queue_item("one-broken", "10")
    assert broken["status"] == "error"
    assert broken["attempts"] == 1
    assert healthy["status"] == "complete"


def test_new_queue_can_replay_complete_raw_graph_without_transport(tmp_path):
    store = _store(tmp_path)
    online_loader = _loader()
    FBrefDiscoveryQueue(
        store,
        _service(store, online_loader, max_network_pages=7),
    ).run("online", max_competitions=2)

    offline_service = FBrefDiscoveryService(
        store,
        offline=True,
        max_network_pages=0,
    )
    offline = FBrefDiscoveryQueue(store, offline_service).run(
        "offline-replay",
        max_competitions=2,
    )

    assert offline.result.ok
    assert offline.result.network_pages == 0
    assert offline.result.raw_writes == 0
    assert offline.queue["completed"] == 2
    assert [match.match_id for match in offline.result.matches] == [
        "aaaab002",
        "aaaab010",
    ]


def test_existing_queue_rejects_changed_scope(tmp_path):
    store = _store(tmp_path)
    FBrefDiscoveryQueue(
        store,
        _service(store, _loader(), max_network_pages=4),
    ).run("fixed-scope", max_competitions=1)

    with pytest.raises(DiscoveryQueueScopeMismatch):
        FBrefDiscoveryQueue(
            store,
            _service(store, _loader(), max_network_pages=1),
        ).run(
            "fixed-scope",
            max_competitions=1,
            max_seasons_per_competition=2,
        )


def test_existing_queue_rejects_a_new_parser_version(tmp_path):
    store = _store(tmp_path)
    FBrefDiscoveryQueue(
        store,
        _service(store, _loader(), max_network_pages=4),
    ).run("fixed-parser", max_competitions=1)

    with (
        patch(
            "scrapers.fbref.discovery_queue.DISCOVERY_PARSER_VERSION",
            "fbref-discovery-parser-v2",
        ),
        pytest.raises(DiscoveryQueueScopeMismatch),
    ):
        FBrefDiscoveryQueue(
            store,
            _service(store, _loader(), max_network_pages=1),
        ).run("fixed-parser", max_competitions=1)


def test_queue_item_is_bound_to_its_exact_plan(tmp_path):
    store = _store(tmp_path)
    FBrefDiscoveryQueue(
        store,
        _service(store, _loader(), max_network_pages=4),
    ).run("bound-item", max_competitions=1)
    item_key = store.discovery_queue_item_key("bound-item", "2")
    item = store.read_manifest(item_key)
    item["snapshot_fingerprint"] = "different-plan"
    store._write_json(item_key, item)

    with pytest.raises(RawPageCorrupt, match="competition 2"):
        FBrefDiscoveryQueue(
            store,
            _service(store, _loader(), max_network_pages=1),
        ).run("bound-item", max_competitions=1)


def test_python_queue_api_enforces_the_same_safety_caps(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(ValueError, match="max_competitions must be at most"):
        FBrefDiscoveryQueue(
            store,
            _service(store, _loader(), max_network_pages=20),
        ).run("too-wide", max_competitions=26)

    with pytest.raises(ValueError, match="max_network_pages must be at most"):
        FBrefDiscoveryQueue(
            store,
            _service(store, _loader(), max_network_pages=101),
        ).run("too-many-pages", max_competitions=1)


def test_drained_queue_with_terminal_item_stays_a_visible_failure(tmp_path):
    store = _store(tmp_path)
    pages = _pages()
    pages.pop("https://fbref.com/en/comps/2/history/Test-Cup-Seasons")
    first_loader = _loader(pages)
    first = FBrefDiscoveryQueue(
        store,
        _service(store, first_loader, max_network_pages=11),
    ).run(
        "terminal-error",
        max_competitions=4,
        max_attempts=1,
    )

    assert not first.result.ok
    assert first.queue["failed"] == 1
    assert first.queue["drained"]
    assert first.queue["stop_reason"] == "queue_drained_with_failures"

    second_loader = _loader(pages)
    second = FBrefDiscoveryQueue(
        store,
        _service(store, second_loader, max_network_pages=1),
    ).run(
        "terminal-error",
        max_competitions=4,
        max_attempts=1,
    )

    assert not second.result.ok
    assert second.result.errors[0]["error_type"] == (
        "DiscoveryQueueIncompleteError"
    )
    assert second.queue["attempted_competition_ids"] == []
    assert second_loader.call_count == 0
