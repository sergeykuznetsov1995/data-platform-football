"""#1428: season start without manual help.

A league whose first round is played by only part of the teams must load as
``complete``: unplayed teams are skipped, completeness is measured on played
teams only. Synthetic payloads are cut from the real RFPL 2026/27 league
response of 23.09.2026 (first five rounds + eight fixtures; players are synthetic), no live
requests.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
import json
from pathlib import Path

import pandas as pd
import pytest

from dags.scripts import run_understat_scraper as runner
from scrapers.understat import (
    UnderstatCatalog,
    UnderstatClient,
    UnderstatPayloadError,
    UnderstatSchemaDrift,
    UnderstatSource,
)
from scrapers.understat.catalog import UnderstatScope
from scrapers.understat.coverage import coverage_exceptions_for_scope
from scrapers.understat.manifest import ManifestStatus, ScopeKey
from scrapers.understat.parsers import parse_team_payload, validate_league_payload
from scrapers.understat.parsers import validate_team_payload
from scrapers.understat.quality import validate_understat_scope


FIXTURE = Path(__file__).resolve().parents[2] / (
    "fixtures/understat/league_RFPL_2026_first5rounds.json"
)
LEAGUE = "RUS-Premier League"
DIMENSIONS = (
    "situation", "formation", "gameState", "timing", "shotZone",
    "attackSpeed", "result",
)


def _base_league() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _results(payload: dict) -> list[dict]:
    return sorted(
        (match for match in payload["dates"] if match["isResult"]),
        key=lambda match: (match["datetime"], match["id"]),
    )


def _league(played_ids: set[str], base: dict | None = None) -> dict:
    """Keep only ``played_ids`` as results; the rest become future fixtures."""

    payload = deepcopy(base or _base_league())
    keys: set[tuple[str, str]] = set()
    for match in payload["dates"]:
        if match["id"] in played_ids:
            keys.add((match["datetime"], match["h"]["id"]))
            keys.add((match["datetime"], match["a"]["id"]))
        elif match["isResult"]:
            match["isResult"] = False
            match["goals"] = {"h": None, "a": None}
            match["xG"] = {"h": None, "a": None}
            match.pop("forecast", None)
    for team in payload["teams"].values():
        team["history"] = [
            item for item in team["history"]
            if (item["date"], str(team["id"])) in keys
        ]
    payload["players"] = [
        _player(team) for team in payload["teams"].values() if team["history"]
    ]
    return payload


def _shift_season(payload: dict, years: int = 1) -> dict:
    """Move a league payload to another season by rewriting date strings."""

    text = json.dumps(payload)
    for year in (2027, 2026):
        text = text.replace(f'"{year}-', f'"{year + years}-')
    return json.loads(text)


def _roster(player_id: int, team_id: str, side: str, row_id: int) -> dict:
    return {
        "id": str(row_id), "player_id": str(player_id), "team_id": team_id,
        "player": f"Player {player_id}", "position": "F", "positionOrder": "2",
        "time": "90", "goals": "0", "own_goals": "0", "shots": "1", "xG": "0.1",
        "xGChain": "0.1", "xGBuildup": "0", "assists": "0", "xA": "0",
        "key_passes": "0", "yellow_card": "0", "red_card": "0",
        "roster_in": "0", "roster_out": "0", "h_a": side,
    }


def _shot(match: dict, side: str, player_id: int) -> dict:
    return {
        "id": f"{match['id']}{0 if side == 'h' else 1}", "minute": "10",
        "result": "MissedShots", "X": "0.8", "Y": "0.5", "xG": "0.1",
        "player": f"Player {player_id}", "h_a": side, "player_id": str(player_id),
        "situation": "OpenPlay", "season": match["datetime"][:4],
        "shotType": "RightFoot", "match_id": match["id"],
        "h_team": match["h"]["title"], "a_team": match["a"]["title"],
        "h_goals": match["goals"]["h"], "a_goals": match["goals"]["a"],
        "date": match["datetime"], "player_assisted": None, "lastAction": "Pass",
    }


def _player_id(team_id: str) -> int:
    return int(team_id) * 100 + 1


def _player(team: dict) -> dict:
    player_id = _player_id(str(team["id"]))
    return {
        "id": str(player_id), "player_name": f"Player {player_id}",
        "games": "1", "time": "90", "goals": "0", "xG": "0.1",
        "assists": "0", "xA": "0", "shots": "1", "key_passes": "0",
        "yellow_cards": "0", "red_cards": "0", "position": "F",
        "team_title": team["title"], "npg": "0", "npxG": "0.1",
        "xGChain": "0.1", "xGBuildup": "0",
    }


def _match_payload(match: dict) -> dict:
    home, away = match["h"]["id"], match["a"]["id"]
    return {
        "rosters": {
            "h": [_roster(_player_id(home), home, "h", int(match["id"]) * 10)],
            "a": [_roster(_player_id(away), away, "a", int(match["id"]) * 10 + 1)],
        },
        "shots": {
            "h": [_shot(match, "h", _player_id(home))],
            "a": [_shot(match, "a", _player_id(away))],
        },
        "tmpl": {},
    }


def _statistics() -> dict:
    values = {
        "shots": 1, "goals": 0, "xG": 0.1,
        "against": {"shots": 1, "goals": 0, "xG": 0.1},
    }
    return {dimension: {"All": dict(values)} for dimension in DIMENSIONS}


def _team_payload(league: dict, team: dict, statistics=None) -> dict:
    dates = []
    for match in league["dates"]:
        side = "h" if match["h"]["id"] == str(team["id"]) else (
            "a" if match["a"]["id"] == str(team["id"]) else None
        )
        if side is None:
            continue
        record = {key: value for key, value in match.items() if key != "forecast"}
        dates.append({**record, "side": side, "result": "d"})
    return {
        "dates": dates,
        "players": [_player(team)],
        "statistics": _statistics() if statistics is None else statistics,
    }


_UNSET = object()


class _Client:
    """Replays synthetic payloads; records every getTeamData call."""

    def __init__(self, league: dict, *, unplayed_statistics=_UNSET,
                 team_overrides: dict | None = None, cache_dir=None):
        self.league = league
        self.unplayed_statistics = [] if unplayed_statistics is _UNSET else (
            unplayed_statistics
        )
        self.team_overrides = team_overrides or {}
        self.team_calls: list[str] = []
        self.match_overrides: dict[str, dict] = {}
        if cache_dir is not None:
            self.cache_dir = cache_dir

    def get_league_data(self, source_league, source_season_id, *, force_refresh=False):
        return deepcopy(self.league)

    def get_match_data(self, match_id, *, force_refresh=False):
        match = next(m for m in self.league["dates"] if m["id"] == str(match_id))
        return deepcopy(self.match_overrides.get(str(match_id), _match_payload(match)))

    def get_team_data(self, team_name, source_season_id, *, force_refresh=False):
        self.team_calls.append(team_name)
        if team_name in self.team_overrides:
            return deepcopy(self.team_overrides[team_name])
        team = next(
            t for t in self.league["teams"].values() if t.get("title") == team_name
        )
        if not team["history"]:
            return {
                "dates": [], "players": [],
                "statistics": deepcopy(self.unplayed_statistics),
            }
        return _team_payload(self.league, team)


def _scope_key(source_season_id: int = 2026) -> ScopeKey:
    return ScopeKey(
        league=LEAGUE,
        season=f"{source_season_id % 100:02d}{(source_season_id + 1) % 100:02d}",
        source_league="RFPL",
        source_season_id=str(source_season_id),
    )


def _run(client: _Client, *, today: date, source_season_id: int = 2026):
    scope = _scope_key(source_season_id)
    frames = UnderstatSource(client, today=today).scrape_scope(
        LEAGUE, scope.season, source_season_id, mode="current"
    )
    frames = runner._decorate_frames(frames, batch_id="synthetic")
    return validate_understat_scope(
        frames,
        scope=scope,
        active=True,
        previous_row_counts={},
        batch_id="synthetic",
        coverage_exceptions=coverage_exceptions_for_scope(scope),
    )


def _team_ids(league: dict, *, played: bool) -> list[str]:
    return sorted(
        str(team["id"]) for team in league["teams"].values()
        if bool(team["history"]) is played
    )


def _team_titles_by_id(league: dict, *, played: bool) -> list[str]:
    teams = sorted(
        (int(team["id"]), team["title"]) for team in league["teams"].values()
        if bool(team["history"]) is played
    )
    return [title for _, title in teams]


def _hard(report) -> list[str]:
    return [issue.code for issue in report.issues if issue.status is not None]


# --------------------------------------------------------------------------
# First round played by part of the teams: three shapes of empty statistics.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("empty_statistics", [[], None, {}], ids=["list", "null", "object"])
def test_partial_first_round_is_complete_and_measured_on_played_teams(empty_statistics):
    first_five = {match["id"] for match in _results(_base_league())[:5]}
    league = _league(first_five)
    client = _Client(league, unplayed_statistics=empty_statistics)

    report = _run(client, today=date(2026, 7, 26))

    assert report.status is ManifestStatus.COMPLETE, _hard(report)
    assert client.team_calls == _team_titles_by_id(league, played=True)
    assert len(client.team_calls) == 10
    quality = report.to_dict()
    assert quality["teams_pending_first_match"] == _team_ids(league, played=False)
    assert len(quality["teams_pending_first_match"]) == 6
    assert quality["covered_game_ids"] == sorted(first_five)
    assert quality["site_result_game_ids"] == sorted(first_five)


@pytest.mark.parametrize("empty_statistics", [[], None, {}], ids=["list", "null", "object"])
def test_team_validator_accepts_three_shapes_of_no_statistics(empty_statistics):
    payload = {"dates": [], "players": [], "statistics": empty_statistics}

    validate_team_payload(payload)
    _, breakdowns = parse_team_payload(
        payload,
        _scope(),
        team_id=1,
        team_name="Unplayed",
    )
    assert breakdowns.empty


@pytest.mark.parametrize("statistics", [["situation"], "none", 0, True])
def test_team_validator_still_rejects_other_non_object_statistics(statistics):
    with pytest.raises(UnderstatSchemaDrift, match="statistics: expected an object"):
        validate_team_payload({"dates": [], "players": [], "statistics": statistics})


def _scope():
    return UnderstatScope(
        league=LEAGUE, source_league="RFPL", source_league_id=6,
        season="2627", source_season_id=2026, is_closed=False,
    )


# --------------------------------------------------------------------------
# A league "teams" placeholder without id/title.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("placeholder", [{"history": []}, {}], ids=["empty-history", "no-history"])
def test_team_placeholder_without_identity_and_history_is_skipped(placeholder):
    first_five = {match["id"] for match in _results(_base_league())[:5]}
    league = _league(first_five)
    league["teams"]["placeholder"] = placeholder
    validate_league_payload(league)

    client = _Client(league)
    report = _run(client, today=date(2026, 7, 26))

    assert report.status is ManifestStatus.COMPLETE, _hard(report)
    assert len(client.team_calls) == 10


def test_team_without_identity_but_with_history_is_still_drift():
    league = _league({match["id"] for match in _results(_base_league())[:5]})
    played = next(t for t in league["teams"].values() if t["history"])
    league["teams"]["broken"] = {"history": deepcopy(played["history"])}

    with pytest.raises(UnderstatSchemaDrift, match="missing required fields"):
        validate_league_payload(league)


# --------------------------------------------------------------------------
# One team's first match postponed by four weeks.
# --------------------------------------------------------------------------

def test_first_match_postponed_four_weeks_is_complete_every_day():
    base = _base_league()
    results = _results(base)
    postponed = results[0]
    late_team = postponed["a"]["id"]  # Baltika in the real RFPL response
    start = date(2026, 7, 27)

    for day in range(4):
        today = start + timedelta(days=7 * day)
        played = {
            match["id"] for match in results
            if match["datetime"] < today.isoformat()
            and late_team not in (match["h"]["id"], match["a"]["id"])
        }
        league = _league(played, base)
        client = _Client(league)

        report = _run(client, today=today)

        assert report.status is ManifestStatus.COMPLETE, (today, _hard(report))
        pending = report.to_dict()["teams_pending_first_match"]
        assert late_team in pending, today
        assert late_team not in {
            str(t["id"]) for t in league["teams"].values()
            if t["title"] in client.team_calls
        }
        assert report.to_dict()["covered_game_ids"] == sorted(played)

    everything = {match["id"] for match in results}
    client = _Client(_league(everything, base))
    report = _run(client, today=start + timedelta(days=28))
    assert report.status is ManifestStatus.COMPLETE, _hard(report)
    assert report.to_dict()["teams_pending_first_match"] == []


# --------------------------------------------------------------------------
# Date window: January-June probe of the next season and the 1 July start.
# --------------------------------------------------------------------------

class _StatClient:
    def get_stat_data(self, *, force_refresh=True):
        rows = []
        for league, league_id in (
            ("EPL", 1), ("La liga", 4), ("Bundesliga", 3), ("Serie A", 2),
            ("Ligue 1", 5), ("RFPL", 6),
        ):
            for year, month in ((2025, 8), (2026, 8)):
                rows.append({
                    "league_id": str(league_id), "league": league, "h": "1.2",
                    "a": "1.0", "hxg": "1.1", "axg": "0.9", "year": str(year),
                    "month": str(month), "matches": "10",
                })
        return {"stat": rows}


@pytest.mark.parametrize(
    "today, expected",
    [
        (date(2027, 1, 15), {2025: True, 2026: True, 2027: False}),
        (date(2027, 6, 30), {2025: True, 2026: True, 2027: False}),
        (date(2027, 7, 1), {2026: True, 2027: False}),
    ],
)
def test_rolling_window_around_the_july_rollover(today, expected):
    scopes = [
        scope for scope in UnderstatCatalog(_StatClient(), today=today).rolling_scopes()
        if scope.league == LEAGUE
    ]

    assert {scope.source_season_id: scope.discovered for scope in scopes} == expected
    closed = {scope.source_season_id for scope in scopes if scope.is_closed}
    assert closed == {sid for sid in expected if sid < (2027 if today.month >= 7 else 2026)}


@pytest.mark.parametrize("today", [date(2027, 1, 15), date(2027, 6, 30)])
def test_next_season_probe_without_results_is_upstream_pending(today):
    league = _shift_season(_league(set()))
    client = _Client(league)

    report = _run(client, today=today, source_season_id=2027)

    assert report.status is ManifestStatus.UPSTREAM_PENDING, _hard(report)
    assert report.passed
    assert client.team_calls == []
    assert len(report.to_dict()["teams_pending_first_match"]) == 16


def test_first_of_july_start_with_partial_round_is_complete():
    base = _shift_season(_base_league())
    first_three = {match["id"] for match in _results(base)[:3]}
    league = _league(first_three, base)
    client = _Client(league)

    report = _run(client, today=date(2027, 7, 1), source_season_id=2027)

    assert report.status is ManifestStatus.COMPLETE, _hard(report)
    assert len(client.team_calls) == 6
    assert len(report.to_dict()["teams_pending_first_match"]) == 10


# --------------------------------------------------------------------------
# getTeamData silently answering with the team's previous season (R-38).
# --------------------------------------------------------------------------

def test_team_payload_from_previous_season_is_drift():
    league = _league({match["id"] for match in _results(_base_league())[:5]})
    team = next(t for t in league["teams"].values() if t["history"])
    stale = _team_payload(_shift_season(league, years=-1), team)
    client = _Client(league, team_overrides={team["title"]: stale})

    with pytest.raises(UnderstatSchemaDrift, match="previous season"):
        _run(client, today=date(2026, 7, 26))


def test_team_season_guard_is_a_lower_bound_only():
    league = _league({match["id"] for match in _results(_base_league())[:5]})
    team = next(t for t in league["teams"].values() if t["history"])
    payload = _team_payload(league, team)

    validate_team_payload(payload, source_season_id=2026)
    # 2019/20 ran into August 2020: a later date is never a mismatch.
    validate_team_payload(_team_payload(_shift_season(league), team), source_season_id=2026)
    with pytest.raises(UnderstatSchemaDrift, match="previous season"):
        validate_team_payload(payload, source_season_id=2027)
    # Without the season the validator keeps its old contract.
    validate_team_payload(payload)


# --------------------------------------------------------------------------
# A played team with no breakdowns is a real hole, not a season start.
# --------------------------------------------------------------------------

def test_played_team_with_empty_statistics_is_contract_failure():
    league = _league({match["id"] for match in _results(_base_league())[:5]})
    team = next(t for t in league["teams"].values() if t["history"])
    hollow = _team_payload(league, team, statistics={})
    client = _Client(league, team_overrides={team["title"]: hollow})

    report = _run(client, today=date(2026, 7, 26))

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    assert "team_breakdown_coverage_mismatch" in _hard(report)
    issue = next(i for i in report.issues if i.code == "team_breakdown_coverage_mismatch")
    assert issue.details["missing_team_ids"] == [str(team["id"])]


def test_breakdowns_of_a_team_absent_from_schedule_still_fail():
    league = _league({match["id"] for match in _results(_base_league())[:5]})
    scope = _scope_key()
    client = _Client(league)
    frames = runner._decorate_frames(
        UnderstatSource(client, today=date(2026, 7, 26)).scrape_scope(
            LEAGUE, "2627", 2026
        ),
        batch_id="synthetic",
    )
    breakdowns = frames["understat_team_season_breakdowns"]
    stranger = breakdowns[breakdowns["team_id"] == breakdowns["team_id"].iloc[0]].copy()
    stranger["team_id"] = 999999
    frames["understat_team_season_breakdowns"] = (
        pd.concat([breakdowns, stranger], ignore_index=True)
    )

    report = validate_understat_scope(
        frames, scope=scope, active=True, batch_id="synthetic",
        coverage_exceptions=coverage_exceptions_for_scope(scope),
    )

    assert report.status is ManifestStatus.CONTRACT_FAILURE
    issue = next(i for i in report.issues if i.code == "team_breakdown_coverage_mismatch")
    assert issue.details["extra_team_ids"] == ["999999"]


# --------------------------------------------------------------------------
# Schema drift keeps the offending response in <cache_dir>/schema_drift/.
# --------------------------------------------------------------------------

def _drift_files(tmp_path: Path) -> list[Path]:
    folder = tmp_path / "schema_drift"
    return sorted(folder.iterdir()) if folder.exists() else []


def _assert_drift_status(exc: BaseException) -> None:
    status, exit_code, _ = runner._classify_exception(exc)
    assert status is ManifestStatus.SCHEMA_DRIFT
    assert exit_code == 1


def test_league_drift_payload_is_saved(tmp_path):
    league = _league({match["id"] for match in _results(_base_league())[:5]})
    league["surprise"] = {"new": 1}
    client = _Client(league, cache_dir=tmp_path)

    with pytest.raises(UnderstatSchemaDrift, match="unknown fields") as caught:
        UnderstatSource(client, today=date(2026, 7, 26)).scrape_scope(LEAGUE, "2627", 2026)

    _assert_drift_status(caught.value)
    [saved] = _drift_files(tmp_path)
    assert saved.name.endswith("_league_RFPL_2026.json")
    assert json.loads(saved.read_text(encoding="utf-8")) == league


def test_league_snapshot_drift_payload_is_saved(tmp_path):
    league = _league({match["id"] for match in _results(_base_league())[:5]})
    league["surprise"] = {"new": 1}
    client = _Client(league, cache_dir=tmp_path)

    with pytest.raises(UnderstatSchemaDrift):
        UnderstatSource(client, today=date(2026, 7, 26)).league_snapshot(
            LEAGUE, "2627", 2026
        )

    [saved] = _drift_files(tmp_path)
    assert saved.name.endswith("_league_RFPL_2026.json")


def test_match_drift_payload_is_saved(tmp_path):
    first = _results(_base_league())[0]
    league = _league({first["id"]})
    client = _Client(league, cache_dir=tmp_path)
    broken = _match_payload(first)
    broken["shots"]["h"][0]["new_metric"] = "surprise"
    client.match_overrides[first["id"]] = broken

    with pytest.raises(UnderstatSchemaDrift, match="unknown fields") as caught:
        UnderstatSource(client, today=date(2026, 7, 26)).scrape_scope(LEAGUE, "2627", 2026)

    _assert_drift_status(caught.value)
    [saved] = _drift_files(tmp_path)
    assert saved.name.endswith(f"_match_{first['id']}.json")
    assert json.loads(saved.read_text(encoding="utf-8")) == broken


def test_team_drift_payload_is_saved(tmp_path):
    league = _league({match["id"] for match in _results(_base_league())[:5]})
    team = next(t for t in league["teams"].values() if t["history"])
    broken = _team_payload(league, team, statistics=["unexpected"])
    client = _Client(league, team_overrides={team["title"]: broken}, cache_dir=tmp_path)

    with pytest.raises(UnderstatSchemaDrift, match="statistics") as caught:
        UnderstatSource(client, today=date(2026, 7, 26)).scrape_scope(LEAGUE, "2627", 2026)

    _assert_drift_status(caught.value)
    [saved] = _drift_files(tmp_path)
    slug = team["title"].replace(" ", "_")
    assert saved.name.endswith(f"_team_{slug}_2026.json")
    assert json.loads(saved.read_text(encoding="utf-8")) == broken


def test_drift_is_raised_unchanged_without_cache_or_when_saving_fails(tmp_path):
    league = _league(set())
    league["surprise"] = 1

    with pytest.raises(UnderstatSchemaDrift, match="unknown fields"):
        UnderstatSource(_Client(league), today=date(2026, 7, 26)).scrape_scope(
            LEAGUE, "2627", 2026
        )

    blocked = tmp_path / "cache"
    blocked.mkdir()
    (blocked / "schema_drift").write_text("not a directory", encoding="utf-8")
    with pytest.raises(UnderstatSchemaDrift, match="unknown fields"):
        UnderstatSource(
            _Client(league, cache_dir=blocked), today=date(2026, 7, 26)
        ).scrape_scope(LEAGUE, "2627", 2026)


# --------------------------------------------------------------------------
# Regression: a fully played round behaves exactly as before.
# --------------------------------------------------------------------------

def test_control_all_teams_played_keeps_calls_and_result():
    base = _base_league()
    league = _league({match["id"] for match in _results(base)}, base)
    client = _Client(league)

    report = _run(client, today=date(2026, 8, 25))

    assert report.status is ManifestStatus.COMPLETE, _hard(report)
    assert _hard(report) == []
    assert client.team_calls == _team_titles_by_id(league, played=True)
    assert len(client.team_calls) == 16
    assert report.to_dict()["teams_pending_first_match"] == []
    assert len(report.to_dict()["covered_game_ids"]) == 40


# --------------------------------------------------------------------------
# The same rule for drift found by the real client, parse_* and discovery.
# --------------------------------------------------------------------------

class _HTTPResponse:
    def __init__(self, text: str):
        self.status_code = 200
        self.headers = {}
        self.text = text

    def json(self):
        return json.loads(self.text)


class _HTTPSession:
    def __init__(self, body: str):
        self.body = body
        self.headers = {}

    def get(self, url, **kwargs):
        return _HTTPResponse(self.body)


def _real_client(body: str, cache_dir: Path) -> UnderstatClient:
    return UnderstatClient(
        session=_HTTPSession(body),
        cache_dir=cache_dir,
        sleep=lambda _seconds: None,
        jitter=lambda _start, _end: 0,
    )


@pytest.mark.parametrize("body", ["[]", "null", "not json"], ids=["list", "null", "invalid"])
def test_real_client_saves_a_non_object_response(tmp_path, body):
    client = _real_client(body, tmp_path)

    with pytest.raises(UnderstatPayloadError) as caught:
        UnderstatSource(client, today=date(2026, 7, 26)).scrape_scope(
            LEAGUE, "2627", 2026
        )

    _assert_drift_status(caught.value)
    [saved] = _drift_files(tmp_path)
    assert saved.name.endswith("_league_RFPL_2026.json")
    assert saved.read_text(encoding="utf-8") == body
    assert not (tmp_path / "league_RFPL_2026.json").exists()


def test_real_client_saves_a_non_object_team_response(tmp_path):
    client = _real_client("[]", tmp_path)

    with pytest.raises(UnderstatPayloadError):
        client.get_team_data("CSKA Moscow", 2026, force_refresh=True)

    [saved] = _drift_files(tmp_path)
    assert saved.name.endswith("_team_CSKA_Moscow_2026.json")


def test_league_parse_drift_payload_is_saved(tmp_path):
    first = _results(_base_league())[0]
    league = _league({first["id"]})
    played = next(m for m in league["dates"] if m["id"] == first["id"])
    played["goals"]["h"] = "not-a-number"  # structurally valid, parse fails
    validate_league_payload(league)
    client = _Client(league, cache_dir=tmp_path)

    with pytest.raises(UnderstatSchemaDrift, match="invalid integer") as caught:
        UnderstatSource(client, today=date(2026, 7, 26)).scrape_scope(LEAGUE, "2627", 2026)

    _assert_drift_status(caught.value)
    [saved] = _drift_files(tmp_path)
    assert saved.name.endswith("_league_RFPL_2026.json")
    assert json.loads(saved.read_text(encoding="utf-8")) == league


def test_match_parse_drift_payload_is_saved(tmp_path):
    first = _results(_base_league())[0]
    league = _league({first["id"]})
    client = _Client(league, cache_dir=tmp_path)
    broken = _match_payload(first)
    broken["shots"]["h"][0]["minute"] = "not-a-number"
    client.match_overrides[first["id"]] = broken

    with pytest.raises(UnderstatSchemaDrift):
        UnderstatSource(client, today=date(2026, 7, 26)).scrape_scope(LEAGUE, "2627", 2026)

    [saved] = _drift_files(tmp_path)
    assert saved.name.endswith(f"_match_{first['id']}.json")
    assert json.loads(saved.read_text(encoding="utf-8")) == broken


def test_discovery_drift_payload_is_saved(tmp_path):
    class _BrokenStat(_StatClient):
        cache_dir = tmp_path

        def get_stat_data(self, *, force_refresh=True):
            payload = super().get_stat_data(force_refresh=force_refresh)
            payload["stat"][0]["surprise"] = "1"
            return payload

    with pytest.raises(UnderstatPayloadError, match="field contract mismatch"):
        UnderstatCatalog(_BrokenStat(), today=date(2026, 9, 25)).discover_scopes()

    [saved] = _drift_files(tmp_path)
    assert saved.name.endswith("_stat.json")
