import gzip
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from scrapers.fbref.match_parser import (
    DatasetStatus,
    MATCH_PARSER_VERSION,
    not_applicable,
    parse_match_html,
)
from scrapers.fbref.raw_store import (
    RawPageCorrupt,
    RawPageNotFound,
    RawPageStore,
    RawStoreError,
    canonicalize_fbref_url,
    competition_index_target,
    competition_page_target,
    match_page_target,
    schedule_page_target,
    season_page_target,
)


FIXTURE_DIR = Path(__file__).parents[2] / "fixtures" / "fbref" / "matches"


def _store(tmp_path) -> RawPageStore:
    return RawPageStore.from_uri(tmp_path.as_uri())


def _fixture_html(match_id="a071faa8") -> str:
    with gzip.open(FIXTURE_DIR / f"{match_id}.html.gz", "rt") as fh:
        return fh.read()


def test_match_target_canonicalizes_slug_query_and_host():
    bare = match_page_target("a071faa8")
    full = match_page_target(
        "https://www.fbref.com/en/matches/a071faa8/Some-Slug?x=1#fragment"
    )
    assert full == bare
    assert bare.canonical_url == "https://fbref.com/en/matches/a071faa8"
    with pytest.raises(ValueError):
        match_page_target("https://example.com/en/matches/a071faa8/x")


def test_canonicalize_fbref_url_accepts_relative_and_normalizes_origin():
    expected = "https://fbref.com/en/comps"
    assert canonicalize_fbref_url("/en/comps/?x=1#section") == expected
    assert canonicalize_fbref_url("en/comps/") == expected
    assert canonicalize_fbref_url("http://www.fbref.com/en/comps/") == expected
    assert canonicalize_fbref_url("https://fbref.com/") == "https://fbref.com/"

    with pytest.raises(ValueError):
        canonicalize_fbref_url("https://example.com/en/comps/")
    with pytest.raises(ValueError):
        canonicalize_fbref_url("ftp://fbref.com/en/comps/")
    with pytest.raises(ValueError):
        canonicalize_fbref_url("")


def test_page_targets_use_discovered_urls_and_stable_manifest_keys(tmp_path):
    store = _store(tmp_path)
    index = competition_index_target()
    competition = competition_page_target(
        "9",
        "/en/comps/9/history/Premier-League-Seasons?from=index",
    )
    season = season_page_target(
        "9",
        "2025-2026",
        "/en/comps/9/2025-2026/Premier-League-Stats/",
    )
    schedule = schedule_page_target(
        "9",
        "2025/26",
        "/en/comps/9/2025-2026/schedule/Premier-League-Scores-and-Fixtures/",
    )

    assert index.canonical_url == "https://fbref.com/en/comps"
    assert competition.canonical_url.endswith(
        "/en/comps/9/history/Premier-League-Seasons"
    )
    assert season.source_ids == {
        "competition_id": "9",
        "season_id": "2025-2026",
    }
    assert store._target_manifest_key(index) == (
        "targets/competition_index/all.json"
    )
    assert store._target_manifest_key(competition) == "targets/competition/9.json"
    assert store._target_manifest_key(season) == "targets/season/9/2025-2026.json"
    assert store._target_manifest_key(schedule) == (
        "targets/schedule/9/2025%2F26.json"
    )

    relative_match = match_page_target(
        "/en/matches/a071faa8/Some-Slug?from=schedule#report"
    )
    assert relative_match == match_page_target("a071faa8")
    assert store._target_manifest_key(relative_match) == (
        "targets/match/a071faa8.json"
    )


def test_target_manifest_components_cannot_escape_their_directory(tmp_path):
    store = _store(tmp_path)
    target = season_page_target(
        "../9",
        "../../2025",
        "/en/comps/9/2025/Premier-League-Stats",
    )
    assert store._target_manifest_key(target) == (
        "targets/season/%2E%2E%2F9/%2E%2E%2F%2E%2E%2F2025.json"
    )


def test_round_trip_is_content_addressed_and_deterministic(tmp_path):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    html = _fixture_html()
    first = store.store_html(target, html, fetched_at="2026-07-10T00:00:00+00:00")
    second = store.store_html(target, html, fetched_at="2026-07-10T00:00:00+00:00")

    loaded, record = store.load_html(target)
    assert loaded == html
    assert first.content_hash == second.content_hash == record.content_hash
    assert first.blob_key == second.blob_key
    assert record.decoded_bytes == len(html.encode("utf-8"))
    encoded = store._read_bytes(record.blob_key)
    assert gzip.decompress(encoded) == html.encode("utf-8")


def test_get_or_fetch_calls_loader_once_for_equivalent_urls(tmp_path):
    store = _store(tmp_path)
    calls = []

    def loader(url):
        calls.append(url)
        return _fixture_html()

    first = match_page_target(
        "https://fbref.com/en/matches/a071faa8/Old-Slug"
    )
    second = match_page_target("a071faa8")
    _, first_record, first_hit = store.get_or_fetch(first, loader)
    _, second_record, second_hit = store.get_or_fetch(
        second, lambda _: pytest.fail("loader called for stored page")
    )

    assert calls == ["https://fbref.com/en/matches/a071faa8"]
    assert first_hit is False
    assert second_hit is True
    assert first_record.content_hash == second_record.content_hash


def test_offline_missing_and_corrupt_blob_fail_without_loader(tmp_path):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    with pytest.raises(RawPageNotFound):
        store.load_html(target)

    record = store.store_html(target, _fixture_html())
    store._write_bytes(record.blob_key, b"not-gzip")
    with pytest.raises(RawPageCorrupt):
        store.load_html(target)


def test_status_contract_and_error_manifests(tmp_path):
    store = _store(tmp_path)
    target = match_page_target("a071faa8")
    record = store.store_html(target, "<html><div id='all_shots'></div></html>")

    with patch(
        "scrapers.fbref.match_parser.parse_player_match_stats_tables",
        return_value=None,
    ):
        result = parse_match_html(
            "<html><div id='all_shots'></div></html>",
            match_id="a071faa8",
            league="ENG-Premier League",
            season=2025,
        )
    key = store.write_parse_manifests(record, result)
    match_manifest = store.read_manifest(key)
    player_key = match_manifest["datasets"]["match_player_stats"]
    player_manifest = store.read_manifest(player_key)

    assert result.status == DatasetStatus.ERROR
    assert result.datasets["shot_events"].status == DatasetStatus.RESTRICTED
    assert result.datasets["match_events"].status == DatasetStatus.EMPTY
    assert player_manifest["status"] == "error"
    assert player_manifest["row_count"] == 0
    assert match_manifest["status"] == "error"

    applicable = not_applicable("schedule", "not a match-page dataset")
    assert applicable.status == DatasetStatus.NOT_APPLICABLE


def test_generic_page_manifests_are_flat_and_page_summary_is_last(tmp_path):
    store = _store(tmp_path)
    target = schedule_page_target(
        "9",
        "2025-2026",
        "/en/comps/9/2025-2026/schedule/Premier-League-Scores-and-Fixtures",
    )
    record = store.store_html(target, "<html></html>")
    result = SimpleNamespace(
        parser_version="fbref-discovery-v1",
        parsed_at="2026-07-10T10:00:00+00:00",
        status=DatasetStatus.AVAILABLE,
        datasets={
            "matches": SimpleNamespace(
                status=DatasetStatus.AVAILABLE,
                row_count=2,
                reason=None,
                error_type=None,
                error_message=None,
            ),
            "warnings": {
                "status": "empty",
                "row_count": 0,
                "reason": "no warnings",
                "error_type": None,
                "error_message": None,
            },
        },
    )

    with patch.object(store, "_write_json", wraps=store._write_json) as writer:
        page_key = store.write_page_parse_manifests(record, result)

    prefix = f"9/2025-2026/{record.content_hash}/fbref-discovery-v1.json"
    matches_key = f"manifests/datasets/schedule/matches/{prefix}"
    warnings_key = f"manifests/datasets/schedule/warnings/{prefix}"
    assert [call.args[0] for call in writer.call_args_list] == [
        matches_key,
        warnings_key,
        f"manifests/pages/schedule/{prefix}",
    ]
    assert page_key == f"manifests/pages/schedule/{prefix}"

    matches_manifest = store.read_manifest(matches_key)
    page_manifest = store.read_manifest(page_key)
    assert matches_manifest["status"] == "available"
    assert matches_manifest["row_count"] == 2
    assert page_manifest["source_ids"] == {
        "competition_id": "9",
        "season_id": "2025-2026",
    }
    assert page_manifest["row_count"] == 2
    assert page_manifest["datasets"] == {
        "matches": matches_key,
        "warnings": warnings_key,
    }


def test_discovery_queue_manifests_are_scoped_and_cannot_escape(tmp_path):
    store = _store(tmp_path)
    plan = {"manifest_version": "queue-v1", "competition_ids": ["9"]}
    item = {"status": "complete", "competition_id": "../9"}

    plan_key = store.write_discovery_queue_plan("../../all", plan)
    item_key = store.write_discovery_queue_item("../../all", "../9", item)

    assert plan_key == "queues/discovery/%2E%2E%2F%2E%2E%2Fall/plan.json"
    assert item_key == (
        "queues/discovery/%2E%2E%2F%2E%2E%2Fall/items/%2E%2E%2F9.json"
    )
    assert store.has_discovery_queue_plan("../../all")
    assert store.has_discovery_queue_item("../../all", "../9")
    assert store.read_discovery_queue_plan("../../all") == plan
    assert store.read_discovery_queue_item("../../all", "../9") == item

    assert store.write_discovery_queue_plan("../../all", plan) == plan_key
    with pytest.raises(RawStoreError, match="immutable"):
        store.write_discovery_queue_plan(
            "../../all",
            {"manifest_version": "queue-v2", "competition_ids": ["9"]},
        )


def test_available_status_has_row_count():
    frame = pd.DataFrame({"player": ["A", "B"]})
    with patch(
        "scrapers.fbref.match_parser.parse_player_match_stats_tables",
        return_value=frame,
    ):
        result = parse_match_html(
            "<html></html>",
            match_id="a071faa8",
            league="ENG-Premier League",
            season=2025,
        )
    player = result.datasets["match_player_stats"]
    assert player.status == DatasetStatus.AVAILABLE
    assert player.row_count == 2
    assert result.parser_version == MATCH_PARSER_VERSION
