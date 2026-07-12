import gzip
from collections import Counter
from pathlib import Path

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
