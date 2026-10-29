"""Replay the recorded RFPL league and explicitly synthetic match/team data."""

from collections import Counter
from copy import deepcopy
from types import SimpleNamespace
from urllib.parse import quote

import pandas as pd
import pytest

from scrapers.understat import TABLE_CONTRACTS, UnderstatClient, UnderstatScraper
from tests.unit.scrapers.test_understat_native import _Clock
from tests.unit.scrapers.test_understat_scraper import base_dependencies
from tests.unit.scrapers.test_understat_season_start import (
    LEAGUE, _base_league, _match_payload, _player, _team_payload,
)


class _ReplaySession:
    def __init__(self, league):
        self.headers = {}
        self.calls = []
        self.closed = False
        self.responses = {
            "https://understat.com": {},  # synthetic cookie bootstrap
            "https://understat.com/getLeagueData/RFPL/2026": league,
        }
        self.responses.update({
            f"https://understat.com/getMatchData/{match['id']}": _match_payload(match)
            for match in league["dates"] if match["isResult"]
        })
        self.responses.update({
            "https://understat.com/getTeamData/"
            f"{quote(team['title'].replace(' ', '_'), safe='_-')}/2026":
                _team_payload(league, team)
            for team in league["teams"].values() if team["history"]
        })

    def get(self, url, **kwargs):
        self.calls.append(url)
        if url not in self.responses:
            pytest.fail(f"Unrecorded Understat replay URL: {url}")
        return SimpleNamespace(
            status_code=200, json=lambda: deepcopy(self.responses[url])
        )

    def close(self):
        self.closed = True


@pytest.fixture
def replay_scraper(base_dependencies, tmp_path, request):
    league = _base_league()
    # The recorded league has no players; these rows are synthetic too.
    league["players"] = [_player(team) for team in league["teams"].values()]
    session = _ReplaySession(league)
    clock = _Clock()
    client = UnderstatClient(
        session=session, cache_dir=tmp_path, max_attempts=1,
        sleep=clock.sleep, monotonic=clock.monotonic,
    )
    with UnderstatScraper(
        leagues=getattr(request, "param", [LEAGUE]), seasons=["2627"], client=client
    ) as scraper:
        yield scraper, session, league
    assert session.closed


@pytest.mark.parametrize(
    "replay_scraper", [[LEAGUE], ["USA-MLS", LEAGUE]],
    indirect=True, ids=["supported", "mixed-league-filter"],
)
def test_scrape_scope_replays_all_seven_tables(replay_scraper):
    scraper, session, league = replay_scraper
    assert scraper.leagues == [LEAGUE]
    frames = scraper.scrape_scope(LEAGUE, "2627", 2026, force_refresh=True)

    assert set(frames) == {contract.table_name for contract in TABLE_CONTRACTS}
    for contract in TABLE_CONTRACTS:
        frame = frames[contract.table_name]
        assert not frame.empty, contract.table_name
        assert set(contract.required_columns) <= set(frame.columns)
        assert not frame.duplicated(list(contract.natural_key)).any()
        assert set(frame["league"]) == {LEAGUE}
        assert set(frame["season"]) == {"2627"}
        assert set(frame["source_season_id"]) == {2026}
        assert set(frame["season_id"]) == {2026}
        assert set(frame["league_id"]) == {"6"}

    schedule = frames["understat_schedule"].set_index("game_id")
    assert set(schedule.index) == {int(match["id"]) for match in league["dates"]}
    for match in league["dates"]:
        row = schedule.loc[int(match["id"])]
        assert row["home_team"] == match["h"]["title"]
        assert row["away_team"] == match["a"]["title"]
        assert row["date"] == pd.Timestamp(match["datetime"])
        assert bool(row["is_result"]) == match["isResult"]
        assert bool(row["has_data"]) == match["isResult"]
        if match["isResult"]:
            assert row["home_goals"] == int(match["goals"]["h"])
            assert row["away_goals"] == int(match["goals"]["a"])
    assert Counter(session.calls) == Counter(session.responses.keys())
    scraper._iceberg_writer.write_dataframe.assert_not_called()


def test_replay_rejects_unrecorded_request(replay_scraper):
    scraper, session, _league = replay_scraper
    with pytest.raises(pytest.fail.Exception, match="Unrecorded Understat replay URL"):
        scraper.client.get_league_data("EPL", 2026)
    assert session.calls[-1] == "https://understat.com/getLeagueData/EPL/2026"
