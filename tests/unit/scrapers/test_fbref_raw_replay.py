import gzip
from collections import Counter
from pathlib import Path
from unittest.mock import MagicMock

from scrapers.fbref.data_readers import FBrefDataReaderMixin
from scrapers.fbref.match_parser import DatasetStatus, parse_match_html
from scrapers.fbref.raw_store import RawPageStore, match_page_target


FIXTURE_DIR = Path(__file__).parents[2] / "fixtures" / "fbref" / "matches"
MATCH_IDS = sorted(path.name.split(".", 1)[0] for path in FIXTURE_DIR.glob("*.html.gz"))


def _load_fixture(match_id: str) -> str:
    with gzip.open(FIXTURE_DIR / f"{match_id}.html.gz", "rt") as fh:
        return fh.read()


def test_all_ten_saved_matches_replay_offline_with_manifests(tmp_path):
    assert len(MATCH_IDS) == 10
    store = RawPageStore.from_uri(tmp_path.as_uri())
    totals = Counter()

    # Import the already captured corpus. This is the only loader pass and it
    # reads local fixtures, not FBref or a proxy.
    for match_id in MATCH_IDS:
        target = match_page_target(match_id)
        html, record, cache_hit = store.get_or_fetch(
            target, lambda _, mid=match_id: _load_fixture(mid)
        )
        assert cache_hit is False
        result = parse_match_html(
            html,
            match_id=match_id,
            league="ENG-Premier League",
            season=2025,
        )
        store.write_parse_manifests(record, result)
        assert result.status == DatasetStatus.AVAILABLE
        assert result.datasets["match_player_stats"].status == DatasetStatus.AVAILABLE
        assert result.datasets["shot_events"].status == DatasetStatus.RESTRICTED
        totals.update({name: dataset.row_count for name, dataset in result.datasets.items()})

    assert totals == {
        "shot_events": 0,
        "match_events": 134,
        "lineups": 400,
        "match_team_stats": 10,
        "match_player_stats": 299,
        "match_managers": 20,
        "match_officials": 10,
        "match_keeper_stats": 20,
    }

    # A second pass has no loader at all. Loading and parsing therefore cannot
    # spend proxy traffic, even when the parser version changes later.
    for match_id in MATCH_IDS:
        target = match_page_target(match_id)
        html, record, cache_hit = store.get_or_fetch(
            target,
            lambda _: (_ for _ in ()).throw(
                AssertionError("network loader called during replay")
            ),
        )
        assert cache_hit is True
        result = parse_match_html(
            html,
            match_id=match_id,
            league="ENG-Premier League",
            season=2025,
        )
        store.write_parse_manifests(record, result)
        assert result.datasets["match_player_stats"].status == DatasetStatus.AVAILABLE


def test_active_match_reader_uses_raw_before_source_fetch(tmp_path):
    class Reader(FBrefDataReaderMixin):
        def __init__(self):
            self._raw_page_store = RawPageStore.from_uri(tmp_path.as_uri())
            self._stats = {}
            self._last_validation_failure = None
            self._fetch_page = MagicMock(return_value=_load_fixture("a071faa8"))

        @staticmethod
        def _add_metadata(frame, _entity_type):
            return frame

    reader = Reader()
    for _ in range(2):
        buffers = [[] for _ in range(8)]
        got = reader._process_single_match(
            "a071faa8",
            "ENG-Premier League",
            2025,
            *buffers,
        )
        assert "match_player_stats" in got

    reader._fetch_page.assert_called_once()
    assert reader._stats == {"raw_page_writes": 1, "raw_page_hits": 1}
