"""#1431: closed seasons leave the daily plan; weekly league-hash check."""

from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

from scrapers.understat import UnderstatHTTPError, UnderstatSource
from scrapers.understat.closed_check import (
    LEAGUE_HASH_ENTITIES,
    split_daily_plan,
    weekly_check_day,
)
from scrapers.understat.quality import dataframe_content_hash
from tests.unit.scrapers.test_understat_native import _SourceClient


def _scope(league: str, source_id: int) -> SimpleNamespace:
    return SimpleNamespace(league=league, source_season_id=source_id)


def _ids(scopes) -> list[tuple[str, int]]:
    return [(scope.league, scope.source_season_id) for scope in scopes]


def test_weekly_check_day_is_monday_only():
    assert weekly_check_day(datetime(2026, 9, 28, 9, tzinfo=timezone.utc)) is True
    assert weekly_check_day(date(2026, 10, 5)) is True
    for day in range(29, 31):
        assert weekly_check_day(datetime(2026, 9, day, 9, tzinfo=timezone.utc)) is False
    assert weekly_check_day(date(2026, 10, 4)) is False


def test_split_keeps_closed_scopes_out_except_monday_and_current_first():
    scopes = [
        _scope("ENG", 2025),
        _scope("ENG", 2026),
        _scope("ESP", 2025),
        _scope("ESP", 2026),
    ]

    current, closed = split_daily_plan(
        scopes, datetime(2026, 9, 29, 9, tzinfo=timezone.utc)
    )
    assert _ids(current) == [("ENG", 2026), ("ESP", 2026)]
    assert closed == []

    current, closed = split_daily_plan(
        scopes, datetime(2026, 9, 28, 9, tzinfo=timezone.utc)
    )
    assert _ids(current) == [("ENG", 2026), ("ESP", 2026)]
    assert _ids(closed) == [("ENG", 2025), ("ESP", 2025)]


def test_next_season_probe_stays_in_the_daily_plan():
    # January 2027 (a Monday): window 2025/26 + 2026/27, probe 2027/28.
    scopes = [_scope("ENG", 2025), _scope("ENG", 2026), _scope("ENG", 2027)]

    current, closed = split_daily_plan(scopes, date(2027, 1, 4))

    assert _ids(current) == [("ENG", 2026), ("ENG", 2027)]
    assert _ids(closed) == [("ENG", 2025)]


class _LeagueCountingClient(_SourceClient):
    def __init__(self, *, empty_matches: bool = False):
        super().__init__()
        self.league_refreshes: list[bool] = []
        self.empty_matches = empty_matches

    def get_league_data(self, source_league, source_season_id, *, force_refresh=False):
        self.league_refreshes.append(force_refresh)
        return super().get_league_data(
            source_league, source_season_id, force_refresh=force_refresh
        )

    def get_match_data(self, match_id, *, force_refresh=False):
        payload = super().get_match_data(match_id, force_refresh=force_refresh)
        return {} if self.empty_matches else payload


def test_snapshot_is_one_fresh_league_request_and_matches_scrape_hashes():
    client = _LeagueCountingClient()
    source = UnderstatSource(client, today=date(2026, 9, 28))

    first = source.league_snapshot("ENG-Premier League", "2526", 2025)
    second = source.league_snapshot("ENG-Premier League", "2526", 2025)

    assert client.league_refreshes == [True, True]
    assert client.match_calls == [] and client.team_calls == []
    assert tuple(first) == LEAGUE_HASH_ENTITIES
    assert all(len(value) == 64 for value in first.values())
    assert first == second

    source.scrape_scope("ENG-Premier League", "2526", 2025)
    assert source.last_league_hashes == first


def test_league_hashes_ignore_the_has_data_rewrite_from_match_responses():
    with_rows = UnderstatSource(_LeagueCountingClient(), today=date(2026, 9, 28))
    without_rows = UnderstatSource(
        _LeagueCountingClient(empty_matches=True), today=date(2026, 9, 28)
    )

    rows_frames = with_rows.scrape_scope("ENG-Premier League", "2526", 2025)
    empty_frames = without_rows.scrape_scope("ENG-Premier League", "2526", 2025)

    # The stored schedule hash follows has_data; the league fingerprint does not.
    assert dataframe_content_hash(rows_frames["understat_schedule"]) != (
        dataframe_content_hash(empty_frames["understat_schedule"])
    )
    assert with_rows.last_league_hashes == without_rows.last_league_hashes


def test_unpublished_league_snapshot_is_empty():
    class _Missing(_SourceClient):
        def get_league_data(self, *args, **kwargs):
            raise UnderstatHTTPError("https://understat.com/getLeagueData/EPL/2025", 404)

    source = UnderstatSource(_Missing(), today=date(2026, 9, 28))

    assert source.league_snapshot("ENG-Premier League", "2526", 2025) == {}
