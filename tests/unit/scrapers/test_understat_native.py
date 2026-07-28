"""Focused contract tests for the source-native Understat ingestion."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
import pandas as pd
import pytest

from scrapers.understat import (
    PRODUCTION_LEAGUES,
    TABLE_CONTRACTS,
    UnderstatCatalog,
    UnderstatClient,
    UnderstatHTTPError,
    UnderstatPayloadError,
    UnderstatSchemaDrift,
    UnderstatSource,
    season_slug,
    source_season_id_from_slug,
)
from scrapers.understat.parsers import (
    parse_match_payload,
    parse_schedule,
    validate_league_payload,
    validate_match_payload,
    validate_team_payload,
)


class _Response:
    def __init__(self, status: int, payload=None, headers=None):
        self.status_code = status
        self._payload = {} if payload is None else payload
        self.headers = headers or {}

    def json(self):
        return self._payload


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}
        self.closed = False

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)

    def close(self):
        self.closed = True


class _Clock:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.value += seconds


def test_client_bootstraps_cookie_headers_and_honors_retry_after_without_real_sleep():
    session = _Session(
        [
            _Response(200),
            _Response(429, headers={"Retry-After": "3"}),
            _Response(200, {"stat": []}),
        ]
    )
    clock = _Clock()
    client = UnderstatClient(
        session=session,
        requests_per_minute=30,
        backoff_base_seconds=1,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        jitter=lambda _start, _end: 0,
        now=lambda: datetime(2026, 7, 27, tzinfo=timezone.utc),
    )

    assert client.get_stat_data() == {"stat": []}
    assert len(session.calls) == 3
    assert session.calls[0][0] == "https://understat.com"
    assert session.calls[1][1]["headers"] == {"X-Requested-With": "XMLHttpRequest"}
    assert session.calls[2][1]["headers"] == {"X-Requested-With": "XMLHttpRequest"}
    assert clock.sleeps == [2.0, 3.0]
    assert "Mozilla" in session.headers["User-Agent"]


def test_client_context_manager_closes_the_session():
    session = _Session([])

    with UnderstatClient(session=session) as client:
        assert client.session is session

    assert session.closed is True


class _CatalogClient:
    def get_stat_data(self, *, force_refresh=True):
        def row(league, league_id, year, month):
            return {
                "league_id": str(league_id),
                "league": league,
                "h": "1.2",
                "a": "1.0",
                "hxg": "1.1",
                "axg": "0.9",
                "year": str(year),
                "month": str(month),
                "matches": "10",
            }

        return {
            "stat": [
                row("EPL", 1, 2014, 8),
                row("EPL", 1, 2026, 5),
                row("RFPL", 6, 2025, 8),
                row("Ligue 1", 5, 2014, 8),
                row("Bundesliga", 3, 2014, 8),
                row("Serie A", 2, 2014, 8),
                row("La liga", 4, 2014, 8),
                row("not-supported", 99, 2025, 8),
            ]
        }


def test_catalog_has_six_canonical_leagues_and_no_post_rollover_extra_probe():
    assert PRODUCTION_LEAGUES == (
        "ENG-Premier League",
        "ESP-La Liga",
        "GER-Bundesliga",
        "ITA-Serie A",
        "FRA-Ligue 1",
        "RUS-Premier League",
    )
    catalog = UnderstatCatalog(_CatalogClient(), today=date(2026, 7, 27))
    scopes = catalog.discover_scopes()
    assert {
        (scope.league, scope.season, scope.source_season_id)
        for scope in scopes
    } == {
        ("ENG-Premier League", "1415", 2014),
        ("ENG-Premier League", "2526", 2025),
        ("ESP-La Liga", "1415", 2014),
        ("GER-Bundesliga", "1415", 2014),
        ("ITA-Serie A", "1415", 2014),
        ("FRA-Ligue 1", "1415", 2014),
        ("RUS-Premier League", "2526", 2025),
    }
    assert all(scope.is_closed for scope in scopes)
    source_ids = {scope.league: scope.source_league_id for scope in scopes}
    assert source_ids["ESP-La Liga"] == 4
    assert source_ids["ITA-Serie A"] == 2

    rolling = catalog.rolling_scopes(window=2, probe_next=True)
    assert len(rolling) == 12
    assert {scope.season for scope in rolling} == {"2526", "2627"}
    assert "2728" not in {scope.season for scope in rolling}


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda payload: payload.update({"stat": {}}), "must be a list"),
        (lambda payload: payload["stat"][0].pop("year"), "missing"),
        (lambda payload: payload["stat"][0].update(month="13"), "1..12"),
        (lambda payload: payload["stat"][4].update(league_id="4"), "does not match"),
        (
            lambda payload: payload.update(
                stat=[row for row in payload["stat"] if row["league"] != "RFPL"]
            ),
            "absent from discovery",
        ),
    ],
)
def test_catalog_malformed_discovery_fails_closed(mutate, message):
    payload = _CatalogClient().get_stat_data()
    mutate(payload)

    class Client:
        def get_stat_data(self, *, force_refresh=True):
            return payload

    with pytest.raises(UnderstatPayloadError, match=message):
        UnderstatCatalog(Client(), today=date(2026, 7, 27)).discover_scopes()


def test_catalog_allows_absent_source_id_but_validates_it_when_exposed():
    payload = _CatalogClient().get_stat_data()
    for row in payload["stat"]:
        row.pop("league_id")

    class Client:
        def get_stat_data(self, *, force_refresh=True):
            return payload

    scopes = UnderstatCatalog(Client(), today=date(2026, 7, 27)).discover_scopes()
    assert {scope.league for scope in scopes} == set(PRODUCTION_LEAGUES)


def test_canonical_seasons_are_strict_and_round_trip():
    assert season_slug(2025) == "2526"
    assert source_season_id_from_slug("2122") == 2021
    with pytest.raises(ValueError):
        source_season_id_from_slug("2025")
    with pytest.raises(TypeError):
        season_slug("2025")


LEAGUE_PAYLOAD = {
    "dates": [
        {
            "id": "100",
            "isResult": True,
            "h": {"id": "1", "title": "Home", "short_title": "HOM"},
            "a": {"id": "2", "title": "Away", "short_title": "AWY"},
            "goals": {"h": "0", "a": "0"},
            "xG": {"h": "0", "a": "0"},
            "datetime": "2025-08-01 12:00:00",
            "forecast": {"w": "0.4", "d": "0.3", "l": "0.3"},
        }
    ],
    "players": [
        {
            "id": "7",
            "player_name": "Transfer Player",
            "games": "2",
            "time": "120",
            "goals": "1",
            "xG": "0.8",
            "assists": "1",
            "xA": "0.2",
            "shots": "3",
            "key_passes": "2",
            "yellow_cards": "0",
            "red_cards": "0",
            "position": "F",
            "team_title": "Home, Away",
            "npg": "1",
            "npxG": "0.8",
            "xGChain": "1.1",
            "xGBuildup": "0.1",
        }
    ],
    "teams": {
        "1": {
            "id": "1",
            "title": "Home",
            "history": [
                {
                    "h_a": "h", "xG": 0, "xGA": 0, "npxG": 0, "npxGA": 0,
                    "ppda": {"att": 20, "def": 4},
                    "ppda_allowed": {"att": 10, "def": 2},
                    "deep": 1, "deep_allowed": 2, "scored": 0, "missed": 0,
                    "xpts": 1.2, "result": "d", "date": "2025-08-01 12:00:00",
                    "wins": 0, "draws": 1, "loses": 0, "pts": 1, "npxGD": 0,
                }
            ],
        },
        "2": {
            "id": "2",
            "title": "Away",
            "history": [
                {
                    "h_a": "a", "xG": 0, "xGA": 0, "npxG": 0, "npxGA": 0,
                    "ppda": {"att": 12, "def": 3},
                    "ppda_allowed": {"att": 16, "def": 4},
                    "deep": 2, "deep_allowed": 1, "scored": 0, "missed": 0,
                    "xpts": 1.1, "result": "d", "date": "2025-08-01 12:00:00",
                    "wins": 0, "draws": 1, "loses": 0, "pts": 1, "npxGD": 0,
                }
            ],
        },
    },
}

MATCH_PAYLOAD = {
    "rosters": {
        "h": [
            {
                "id": "10", "player_id": "7", "team_id": "1", "player": "Shooter",
                "position": "F", "positionOrder": "2", "time": "10", "goals": "0",
                "own_goals": "0", "shots": "1", "xG": "0.7", "xGChain": "0.8",
                "xGBuildup": "0.1", "assists": "0", "xA": "0", "key_passes": "0",
                "yellow_card": "0", "red_card": "0", "roster_in": "0",
                "roster_out": "0", "h_a": "h",
            },
            {
                "id": "11", "player_id": "7", "team_id": "1", "player": "Shooter",
                "position": "F", "positionOrder": "2", "time": "90", "goals": "1",
                "own_goals": "0", "shots": "1", "xG": "0.7", "xGChain": "0.8",
                "xGBuildup": "0.1", "assists": "0", "xA": "0", "key_passes": "0",
                "yellow_card": "0", "red_card": "0", "roster_in": "20",
                "roster_out": "21", "h_a": "h",
            },
            {
                "id": "12", "player_id": "8", "team_id": "1", "player": "Helper",
                "position": "M", "positionOrder": "3", "time": "90", "goals": "0",
                "own_goals": "0", "shots": "0", "xG": "0", "xGChain": "0.3",
                "xGBuildup": "0.2", "assists": "1", "xA": "0.5", "key_passes": "1",
                "yellow_card": "0", "red_card": "0", "roster_in": "0",
                "roster_out": "0", "h_a": "h",
            },
        ],
        "a": {
            "20": {
                "id": "20", "player_id": "9", "team_id": "2", "player": "Keeper",
                "position": "GK", "positionOrder": "1", "time": "90", "goals": "0",
                "own_goals": "0", "shots": "0", "xG": "0", "xGChain": "0",
                "xGBuildup": "0", "assists": "0", "xA": "0", "key_passes": "0",
                "yellow_card": "0", "red_card": "0", "roster_in": "0",
                "roster_out": "0", "h_a": "a",
            }
        },
    },
    "shots": {
        "h": [
            {
                "id": "50", "minute": "90", "result": "Goal", "X": "0.9", "Y": "0.5",
                "xG": "0.76", "player": "Shooter", "h_a": "h", "player_id": "7",
                "situation": "Penalty", "season": "2025", "shotType": "Head",
                "match_id": "100", "h_team": "Home", "a_team": "Away", "h_goals": "0",
                "a_goals": "0", "date": "2025-08-01 12:00:00",
                "player_assisted": "Helper", "lastAction": "Pass",
            }
        ],
        "a": [],
    },
    "tmpl": {"home": "ignored", "away": "ignored"},
}


def _team_payload(player_id):
    breakdown = {
        "Penalty": {
            "shots": 1,
            "goals": 1,
            "xG": 0.76,
            "against": {"shots": 0, "goals": 0, "xG": 0},
        }
    }
    return {
        "dates": [],
        "players": [
            {
                "id": str(player_id), "player_name": f"Player {player_id}", "games": "1",
                "time": "90", "goals": "0", "xG": "0.1", "assists": "0", "xA": "0",
                "shots": "1", "key_passes": "0", "yellow_cards": "0", "red_cards": "0",
                "position": "M", "team_title": "ignored", "npg": "0", "npxG": "0.1",
                "xGChain": "0.2", "xGBuildup": "0.1",
            }
        ],
        "statistics": {
            dimension: breakdown
            for dimension in (
                "situation",
                "formation",
                "gameState",
                "timing",
                "shotZone",
                "attackSpeed",
                "result",
            )
        },
    }


class _SourceClient:
    def __init__(self):
        self.match_calls = []
        self.match_refreshes = []
        self.team_calls = []

    def get_league_data(self, source_league, source_season_id, *, force_refresh=False):
        assert source_league == "EPL"
        return LEAGUE_PAYLOAD

    def get_match_data(self, match_id, *, force_refresh=False):
        self.match_calls.append(match_id)
        self.match_refreshes.append(force_refresh)
        return MATCH_PAYLOAD

    def get_team_data(self, team_name, source_season_id, *, force_refresh=False):
        self.team_calls.append(team_name)
        return _team_payload(101 if team_name == "Home" else 102)


def test_scope_fetches_each_payload_once_and_produces_all_seven_contracts():
    client = _SourceClient()
    source = UnderstatSource(client, today=date(2026, 7, 27))
    frames = source.scrape_scope("ENG-Premier League", "2526", 2025)

    assert tuple(frames) == tuple(contract.table_name for contract in TABLE_CONTRACTS)
    assert client.match_calls == [100]
    assert client.match_refreshes == [True]
    assert client.team_calls == ["Home", "Away"]
    for contract in TABLE_CONTRACTS:
        assert set(contract.required_columns) <= set(frames[contract.table_name].columns)
        frame = frames[contract.table_name]
        if not frame.empty:
            assert set(frame["league_id"]) == {"1"}
            assert set(frame["league"]) == {"ENG-Premier League"}
            assert set(frame["season"]) == {"2526"}
            assert set(frame["source_season_id"]) == {2025}

    schedule = frames["understat_schedule"].iloc[0]
    assert bool(schedule["has_data"]) is True  # successful match payload, despite xG=0
    assert schedule["forecast_home_win"] == pytest.approx(0.4)

    shot = frames["understat_shots"].iloc[0]
    assert shot["situation"] == "Penalty"
    assert shot["body_part"] == "Head"
    assert shot["last_action"] == "Pass"
    assert shot["assist_player_id"] == 8  # true player id, never roster row id 12

    player_match = frames["understat_player_match_stats"]
    shooter = player_match[player_match["player_id"] == 7].iloc[0]
    assert len(player_match) == 3
    assert shooter["minutes"] == 90
    assert shooter["roster_entry_ids"] == "10,11"
    assert shooter["roster_in"] == 20
    assert shooter["roster_out"] == 21

    aggregate = frames["understat_players"].iloc[0]
    assert bool(aggregate["is_multi_team"]) is True
    assert aggregate["source_team_title"] == "Home, Away"
    assert pd.isna(aggregate["team_id"])
    assert pd.isna(aggregate["team"])

    team_match = frames["understat_team_match_stats"].iloc[0]
    assert team_match["home_ppda_att"] == 20
    assert team_match["home_ppda_def"] == 4
    assert team_match["away_ppda_allowed_att"] == 16
    assert len(frames["understat_player_team_season_stats"]) == 2
    assert len(frames["understat_team_season_breakdowns"]) == 14


def test_history_mode_can_reuse_valid_match_cache():
    client = _SourceClient()
    UnderstatSource(client, today=date(2026, 7, 27)).scrape_scope(
        "ENG-Premier League", "2526", 2025, mode="history"
    )

    assert client.match_refreshes == [False]


def test_match_shape_drift_fails_closed():
    payload = {
        **MATCH_PAYLOAD,
        "shots": {
            "h": [{**MATCH_PAYLOAD["shots"]["h"][0], "new_metric": "surprise"}],
            "a": [],
        },
    }
    with pytest.raises(UnderstatSchemaDrift, match="new_metric"):
        validate_match_payload(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("shotType", "Tentacle"),
        ("situation", "CounterAttack"),
        ("result", "Disallowed"),
    ],
)
def test_unknown_non_null_shot_enums_fail_closed(field, value):
    shot = {**MATCH_PAYLOAD["shots"]["h"][0], field: value}
    payload = {
        **MATCH_PAYLOAD,
        "shots": {"h": [shot], "a": []},
    }

    with pytest.raises(UnderstatSchemaDrift, match="unknown non-null"):
        validate_match_payload(payload)

    with pytest.raises(UnderstatSchemaDrift, match="unknown non-null"):
        parse_match_payload(
            payload,
            UnderstatCatalog(_CatalogClient(), today=date(2026, 7, 27))
            .discover_scopes()[0],
            {
                "game_id": 100,
                "game": "Home-Away",
                "home_team_id": 1,
                "away_team_id": 2,
                "home_team": "Home",
                "away_team": "Away",
            },
        )


@pytest.mark.parametrize(
    ("container", "outer_side", "wrong_side"),
    [
        ("shots", "h", "a"),
        ("shots", "h", "x"),
        ("rosters", "a", "h"),
        ("rosters", "a", "x"),
    ],
)
def test_match_records_must_agree_with_their_outer_side(
    container, outer_side, wrong_side
):
    payload = deepcopy(MATCH_PAYLOAD)
    records = payload[container][outer_side]
    record = next(iter(records.values())) if isinstance(records, dict) else records[0]
    record["h_a"] = wrong_side

    with pytest.raises(UnderstatSchemaDrift, match="expected outer side"):
        validate_match_payload(payload)


def test_team_history_side_is_strict_and_matches_schedule_team():
    invalid = deepcopy(LEAGUE_PAYLOAD)
    invalid["teams"]["1"]["history"][0]["h_a"] = "x"
    with pytest.raises(UnderstatSchemaDrift, match="expected 'h' or 'a'"):
        validate_league_payload(invalid)

    swapped = deepcopy(LEAGUE_PAYLOAD)
    swapped["teams"]["1"]["history"][0]["h_a"] = "a"
    swapped["teams"]["2"]["history"][0]["h_a"] = "h"
    with pytest.raises(UnderstatSchemaDrift, match="expected 'h'.*date/team"):
        validate_league_payload(swapped)


def test_cancelled_schedule_placeholders_do_not_define_history_sides():
    payload = deepcopy(LEAGUE_PAYLOAD)
    first_cancelled = deepcopy(LEAGUE_PAYLOAD["dates"][0])
    first_cancelled.update(
        id="101",
        isResult=False,
        datetime="2020-07-09 23:00:00",
    )
    second_cancelled = deepcopy(first_cancelled)
    second_cancelled.update(
        id="102",
        h=deepcopy(first_cancelled["a"]),
        a=deepcopy(first_cancelled["h"]),
    )
    payload["dates"].extend([first_cancelled, second_cancelled])

    validate_league_payload(payload)


def test_completed_schedule_side_conflicts_still_fail_closed():
    payload = deepcopy(LEAGUE_PAYLOAD)
    conflicting_result = deepcopy(LEAGUE_PAYLOAD["dates"][0])
    conflicting_result.update(
        id="101",
        h=deepcopy(LEAGUE_PAYLOAD["dates"][0]["a"]),
        a=deepcopy(LEAGUE_PAYLOAD["dates"][0]["h"]),
    )
    payload["dates"].append(conflicting_result)

    with pytest.raises(UnderstatSchemaDrift, match="conflicting side mapping"):
        validate_league_payload(payload)


@pytest.mark.parametrize(
    ("field_path", "value", "message"),
    [
        (("dates", 0, "goals", "h"), "not-a-number", "invalid integer"),
        (("dates", 0, "xG", "a"), "not-a-number", "invalid numeric"),
        (("dates", 0, "xG", "a"), float("nan"), "invalid numeric"),
        (("dates", 0, "datetime"), "not-a-date", "invalid timestamp"),
        (("dates", 0, "datetime"), 123, "invalid timestamp"),
        (("dates", 0, "isResult"), "maybe", "invalid boolean"),
    ],
)
def test_nonempty_invalid_source_scalars_fail_closed(field_path, value, message):
    payload = {
        **LEAGUE_PAYLOAD,
        "dates": [
            {
                **LEAGUE_PAYLOAD["dates"][0],
                "goals": {**LEAGUE_PAYLOAD["dates"][0]["goals"]},
                "xG": {**LEAGUE_PAYLOAD["dates"][0]["xG"]},
            }
        ],
    }
    target = payload
    for key in field_path[:-1]:
        target = target[key]
    target[field_path[-1]] = value

    scope = UnderstatCatalog(_CatalogClient(), today=date(2026, 7, 27)) \
        .rolling_scopes(window=2)[0]
    with pytest.raises(UnderstatSchemaDrift, match=message):
        parse_schedule(payload, scope)


def test_payload_validators_accept_empty_envelopes_but_reject_missing_or_bad_shapes():
    validate_league_payload({"dates": [], "players": [], "teams": {}})
    validate_league_payload({"dates": [], "players": [], "teams": []})
    validate_match_payload(
        {
            "rosters": {"h": [], "a": []},
            "shots": {"h": [], "a": []},
            "tmpl": {},
        }
    )
    validate_team_payload({"dates": [], "players": [], "statistics": {}})

    with pytest.raises(UnderstatSchemaDrift, match="missing required"):
        validate_league_payload({"dates": [], "players": []})
    with pytest.raises(UnderstatSchemaDrift, match="expected a list"):
        validate_league_payload({"dates": {}, "players": [], "teams": {}})
    with pytest.raises(UnderstatSchemaDrift, match="missing required"):
        validate_match_payload(
            {
                "rosters": {"h": [], "a": []},
                "shots": {"h": [{"id": "1"}], "a": []},
                "tmpl": {},
            }
        )
    with pytest.raises(UnderstatSchemaDrift, match="exactly|missing required"):
        validate_team_payload(
            {
                "dates": [],
                "players": [],
                "statistics": {"situation": {}},
            }
        )


def test_unpublished_scope_returns_seven_contract_shaped_empty_frames():
    class NotPublishedClient:
        def get_league_data(self, *_args, **_kwargs):
            raise UnderstatHTTPError("https://understat.test/scope", 404)

    frames = UnderstatSource(
        NotPublishedClient(), today=date(2026, 7, 27)
    ).scrape_scope("ENG-Premier League", "2627", 2026)
    assert tuple(frames) == tuple(contract.table_name for contract in TABLE_CONTRACTS)
    for contract in TABLE_CONTRACTS:
        assert frames[contract.table_name].empty
        assert set(contract.required_columns) <= set(frames[contract.table_name].columns)
