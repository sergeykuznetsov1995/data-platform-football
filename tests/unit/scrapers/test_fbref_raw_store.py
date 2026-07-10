import gzip
from pathlib import Path
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
    match_page_target,
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
