"""Offline HTTP regressions for current-season cache reuse (#1436)."""

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json

import pandas as pd
import pytest

from scrapers.understat import UnderstatClient, UnderstatSource
from scrapers.understat.cache_state import CacheState, atomic_json, digest
from scrapers.understat.parsers import TEAM_BREAKDOWN_DIMENSIONS
from tests.unit.scrapers.test_understat_native import (
    LEAGUE_PAYLOAD, MATCH_PAYLOAD, _team_payload, _Response,
)


LEAGUE = "ENG-Premier League"
TODAY = date(2025, 8, 10)


class Session:
    def __init__(self):
        self.headers = {}
        self.calls = []
        self.league = deepcopy(LEAGUE_PAYLOAD)
        future = deepcopy(self.league["dates"][0])
        future.update(id="101", isResult=False, datetime="2026-05-01 12:00:00")
        self.league["dates"].append(future)
        self.match = deepcopy(MATCH_PAYLOAD)
        self.statuses = []

    def get(self, url, **kwargs):
        path = url.removeprefix("https://understat.com")
        self.calls.append(path)
        if self.statuses:
            return _Response(self.statuses.pop(0))
        if path.startswith("/getLeagueData"):
            payload = self.league
        elif path.startswith("/getMatchData"):
            payload = self.match
        elif path.startswith("/getTeamData/Home"):
            payload = _team_payload(101)
        elif path.startswith("/getTeamData/Away"):
            payload = _team_payload(102)
        else:
            assert path == ""  # bootstrap only; never real HTTP
            payload = {}
        return _Response(200, deepcopy(payload))


def source(tmp_path, session=None, today=TODAY):
    session = session or Session()
    client = UnderstatClient(
        session=session, cache_dir=tmp_path, sleep=lambda _: None,
        monotonic=lambda: 0, jitter=lambda *_: 0,
        now=lambda: datetime.combine(today, datetime.min.time(), timezone.utc),
    )
    return UnderstatSource(client, today=today), session


def run(src, **kwargs):
    frames = src.scrape_scope(LEAGUE, "2526", 2025, **kwargs)
    # Extraction-only tests simulate the runner's successful publication hook.
    src.confirm_cache()
    return frames


def test_warm_current_uses_only_fresh_league_and_returns_full_seven_tables(tmp_path):
    src, session = source(tmp_path)
    first = run(src)
    assert len(session.calls) == src.client.request_count == 5
    session.calls.clear()
    second = run(src)
    assert session.calls == ["/getLeagueData/EPL/2025"]
    for table in first:
        pd.testing.assert_frame_equal(first[table], second[table])
    session.calls.clear()
    full = run(src, mode="reparse")
    assert len(session.calls) == 4
    assert len(full) == 7
    for table in full:
        pd.testing.assert_frame_equal(full[table], second[table])


@pytest.mark.parametrize("field", ["xG", "goals", "isResult"])
def test_changed_league_row_refreshes_match_and_teams(tmp_path, field):
    src, session = source(tmp_path)
    if field == "isResult":
        session.league["dates"][0][field] = False
        for team in session.league["teams"].values():
            team["history"] = []
    run(src)
    if field == "isResult":
        session.league["dates"][0][field] = True
        session.league["teams"] = deepcopy(LEAGUE_PAYLOAD["teams"])
    else:
        session.league["dates"][0][field]["h"] = "1"
    session.calls.clear()
    run(src)
    assert session.calls == ["/getLeagueData/EPL/2025", "/getMatchData/100",
                             "/getTeamData/Home/2025", "/getTeamData/Away/2025"]


@pytest.mark.parametrize("damage", ["missing", "corrupt", "mismatch", "empty", "state"])
def test_unusable_cache_is_repaired(tmp_path, damage):
    src, session = source(tmp_path)
    run(src)
    path = tmp_path / "match_100.json"
    if damage == "missing":
        path.unlink()
    elif damage == "corrupt":
        path.write_text("{")
    elif damage == "mismatch":
        path.write_text(json.dumps({"shots": {"h": [], "a": []}}))
    elif damage == "empty":
        atomic_json(path, {})
        CacheState(tmp_path).save(path.name, [LEAGUE, 2025, 100],
                                 digest(session.league["dates"][0]), {}, TODAY)
    else:
        (tmp_path / "validated" / path.name).unlink()
    session.calls.clear()
    run(src)
    assert "/getMatchData/100" in session.calls


def test_empty_http_response_is_not_reused_next_run(tmp_path):
    src, session = source(tmp_path)
    session.match = {"shots": {"h": [], "a": []}, "rosters": {"h": [], "a": []}, "tmpl": {}}
    first = run(src)
    assert not bool(first["understat_schedule"].iloc[0]["has_data"])
    session.match = deepcopy(MATCH_PAYLOAD)
    session.calls.clear()
    assert bool(run(src)["understat_schedule"].iloc[0]["has_data"])
    assert "/getMatchData/100" in session.calls


@pytest.mark.parametrize("age, refreshed", [(0, True), (1, True), (2, True), (3, False)])
def test_two_day_window(tmp_path, age, refreshed):
    today = date(2025, 8, 1) + timedelta(days=age)
    src, session = source(tmp_path, today=today)
    run(src)
    session.calls.clear()
    run(src)
    assert ("/getMatchData/100" in session.calls) is refreshed


def test_rotation_finds_hidden_match_correction_and_invalidates_teams(tmp_path):
    src, session = source(tmp_path, today=date(2025, 8, 1))
    run(src)
    src, session = source(tmp_path, today=TODAY)
    session.match["rosters"]["h"][0]["time"] = "45"
    result = run(src)
    assert "/getMatchData/100" in session.calls
    assert "/getTeamData/Home/2025" in session.calls
    assert result["understat_player_match_stats"]["minutes"].max() == 90
    with CacheState(tmp_path).ledger(TODAY) as ledger:
        assert ledger["rotated"] == ["match_100.json"]


def test_daily_ledger_counts_bootstrap_retry_and_warns_without_dropping_data(tmp_path, caplog):
    src, session = source(tmp_path)
    cache = CacheState(tmp_path)
    with cache.ledger(TODAY) as state:
        state["requests"] = 499
    session.statuses = [200, 503]
    run(src)
    assert src.client.request_count == len(session.calls) == 6
    with cache.ledger(TODAY) as state:
        assert state["requests"] == 505
    assert "daily HTTP budget exceeded" in caplog.text


def test_rotation_is_global_oldest_first_and_yields_to_mandatory_budget(tmp_path):
    cache = CacheState(tmp_path)
    for i in range(100):
        name = f"match_{i}.json"
        atomic_json(tmp_path / name, MATCH_PAYLOAD)
        cache.save(name, [LEAGUE, 2025, i], "row", MATCH_PAYLOAD,
                   TODAY - timedelta(days=100 - i))
    selected = cache.rotation(TODAY, mandatory=0)
    assert selected == {f"match_{i}.json" for i in range(72)}
    for name in selected:
        assert cache.claim_rotation(name, TODAY)
    assert not cache.rotation(TODAY, mandatory=0)
    with cache.ledger(TODAY + timedelta(days=1)) as state:
        state["requests"] = 320
    assert not cache.rotation(TODAY + timedelta(days=1), mandatory=5)


class CatalogSession(Session):
    def __init__(self, last_match, new_published=False):
        super().__init__()
        self.last_match = last_match
        self.new_published = new_published

    def get(self, url, **kwargs):
        path = url.removeprefix("https://understat.com")
        if path == "/getStatData":
            from scrapers.understat.catalog import LEAGUES
            self.calls.append(path)
            return _Response(200, {"stat": [
                dict(league=item.source_labels[0], h=1, a=1, hxg=1, axg=1,
                     year=2025, month=8, matches=1) for item in LEAGUES
            ]})
        if path.startswith("/getLeagueData"):
            self.calls.append(path)
            year = int(path.rsplit("/", 1)[1])
            if year == 2026 and not self.new_published:
                return _Response(404)
            payload = deepcopy(self.league)
            payload["dates"][-1]["datetime"] = self.last_match
            return _Response(200, payload)
        return super().get(url, **kwargs)


def test_january_does_not_probe_next_season(tmp_path):
    src, session = source(tmp_path, CatalogSession("2026-05-01 12:00:00"),
                          today=date(2026, 1, 15))
    scopes = src.catalog.rolling_scopes()
    assert all(not path.endswith("/2026") for path in session.calls)
    assert all(scope.source_season_id <= 2025 for scope in scopes)
    assert any(not scope.is_closed for scope in scopes)


def test_published_next_season_does_not_close_continuing_july_season(tmp_path):
    from scrapers.understat.closed_check import split_daily_plan
    today = date(2026, 7, 20)
    src, session = source(tmp_path, CatalogSession("2026-07-31 12:00:00", True), today)
    scopes = src.catalog.rolling_scopes()
    current, _ = split_daily_plan(scopes, today)
    assert {scope.source_season_id for scope in current} == {2025, 2026}
    assert len(current) == 12
    # Offline fallback retains these exact classifications even after July.
    session.calls.clear()
    src.catalog.today = date(2026, 8, 1)
    assert all(not scope.is_closed for scope in src.catalog.calendar_scopes())
    assert not session.calls


def test_negative_next_season_probe_has_seven_day_ttl(tmp_path):
    src, session = source(tmp_path, CatalogSession("2026-07-31 12:00:00"), date(2026, 7, 20))
    src.catalog.rolling_scopes()
    assert sum(path.endswith("/2026") for path in session.calls) == 6
    session.calls.clear()
    src.catalog.today += timedelta(days=1)
    scopes = src.catalog.rolling_scopes()
    assert not any(path.endswith("/2026") for path in session.calls)
    assert {scope.source_season_id for scope in scopes} == {2024, 2025}
    session.calls.clear()
    src.catalog.today += timedelta(days=6)
    src.catalog.rolling_scopes()
    assert sum(path.endswith("/2026") for path in session.calls) == 6


def test_july_stat_month_uses_confirmed_schedule_identity(tmp_path):
    from scrapers.understat.catalog import LEAGUE_BY_CANONICAL
    from tests.unit.scrapers.test_understat_native import _CatalogClient
    src, _ = source(tmp_path, today=date(2026, 7, 20))
    payload = deepcopy(LEAGUE_PAYLOAD)
    payload["dates"][0]["datetime"] = "2026-07-21 12:00:00"
    src.catalog.record_league(LEAGUE_BY_CANONICAL[LEAGUE], 2025, payload)
    registry = _CatalogClient().get_stat_data()
    registry["stat"][1].update(year="2026", month="7")
    scopes = src.catalog._scopes_from(registry)
    assert any(scope.league == LEAGUE and scope.source_season_id == 2025
               and not scope.is_closed for scope in scopes)
    assert not any(scope.league == LEAGUE and scope.source_season_id == 2026 for scope in scopes)


def test_empty_team_envelope_is_retried_after_failed_quality_attempt(tmp_path):
    class EmptyTeamSession(Session):
        empty_team = True

        def get(self, url, **kwargs):
            if self.empty_team and "/getTeamData/" in url:
                self.calls.append(url.removeprefix("https://understat.com"))
                return _Response(200, {"dates": [], "players": [], "statistics": {}})
            return super().get(url, **kwargs)

    session = EmptyTeamSession()
    src, _ = source(tmp_path, session)
    first = run(src)
    assert first["understat_player_team_season_stats"].empty
    session.empty_team = False
    session.calls.clear()
    assert not run(src)["understat_player_team_season_stats"].empty
    assert session.calls == ["/getLeagueData/EPL/2025", "/getTeamData/Home/2025",
                             "/getTeamData/Away/2025"]


@pytest.mark.parametrize("dimension", sorted(TEAM_BREAKDOWN_DIMENSIONS))
@pytest.mark.parametrize("validated_cache", [False, True])
def test_partial_team_breakdowns_recover_on_unchanged_league(tmp_path, dimension, validated_cache):
    from dags.scripts.run_understat_scraper import _decorate_frames
    from scrapers.understat.manifest import ScopeKey
    from scrapers.understat.quality import validate_understat_scope

    def team_payload(home):
        payload = _team_payload(7 if home else 9)
        if home:
            payload["players"].extend(_team_payload(8)["players"])
        return payload

    class PartialTeamSession(Session):
        partial = True

        def __init__(self):
            super().__init__()
            for player_id in (8, 9):
                self.league["players"].extend(_team_payload(player_id)["players"])

        def get(self, url, **kwargs):
            response = super().get(url, **kwargs)
            if "/getTeamData/" in url:
                response._payload = team_payload("/Home/" in url)
            if self.partial and "/getTeamData/Home/" in url:
                response._payload["statistics"][dimension] = {}
            return response

    def quality(frames):
        return validate_understat_scope(
            _decorate_frames(frames, batch_id="team-recovery"),
            scope=ScopeKey(LEAGUE, "2526", "EPL", 2025), active=True,
            observed_at=datetime(2025, 8, 10, tzinfo=timezone.utc),
        )

    session = PartialTeamSession()
    src, _ = source(tmp_path, session)
    first = run(src)
    report = quality(first)
    assert not report.passed
    assert "team_breakdown_dimension_mismatch" in {issue.code for issue in report.issues}
    cache = CacheState(tmp_path)
    name = "team_Home_2025.json"
    identity = [LEAGUE, 2025, 1]
    if validated_cache:
        # Reproduce an incomplete entry accepted by the previous implementation.
        payload = team_payload(True)
        payload["statistics"][dimension] = {}
        fingerprint = digest([session.league["teams"]["1"], [
            [100, digest(session.league["dates"][0]), digest(session.match)],
        ]])
        cache.save(name, identity, fingerprint, payload, TODAY)
    else:
        assert cache.load(name, identity) is None
    session.partial = False
    session.calls.clear()
    recovered = run(src)
    assert session.calls == ["/getLeagueData/EPL/2025", "/getTeamData/Home/2025"]
    recovered_report = quality(recovered)
    assert recovered_report.passed, recovered_report.issues
    session.calls.clear()
    warm = run(src)
    assert session.calls == ["/getLeagueData/EPL/2025"]
    for table in recovered:
        pd.testing.assert_frame_equal(recovered[table], warm[table])


def test_confirmed_closed_scope_is_not_probed_daily(tmp_path):
    from scrapers.understat.catalog import LEAGUES
    today = date(2026, 6, 15)
    src, session = source(tmp_path, CatalogSession("2026-05-01 12:00:00"), today)
    for definition in LEAGUES:
        league = deepcopy(LEAGUE_PAYLOAD)
        src.catalog.record_league(definition, 2025, league)
        from types import SimpleNamespace
        src.catalog.note_quality(definition.league, 2025,
                                 SimpleNamespace(passed=True, match_quarantine=[]))
    src.catalog.today += timedelta(days=1)
    src.catalog.rolling_scopes()
    assert not any(path.startswith("/getLeagueData") for path in session.calls)


@pytest.mark.parametrize("block", ["shots", "rosters"])
def test_partial_match_is_retried_on_unchanged_league(tmp_path, block):
    src, session = source(tmp_path)
    session.match[block] = {"h": [], "a": []}
    run(src)
    session.match = deepcopy(MATCH_PAYLOAD)
    session.calls.clear()
    run(src)
    assert "/getMatchData/100" in session.calls


def test_summer_probe_covers_long_break_before_new_schedule(tmp_path):
    today = date(2026, 8, 1)
    src, session = source(tmp_path, CatalogSession("2026-05-31 12:00:00", True), today)
    scopes = src.catalog.rolling_scopes()
    assert any(scope.source_season_id == 2026 and not scope.is_closed for scope in scopes)
    assert sum(path.endswith("/2026") for path in session.calls) == 6


def test_cold_july_stat_does_not_close_previous_continuing_season(tmp_path):
    class JulySession(CatalogSession):
        def get(self, url, **kwargs):
            response = super().get(url, **kwargs)
            if url.endswith("/getStatData"):
                for row in response._payload["stat"]:
                    row.update(year=2026, month=7)
            return response

    src, session = source(tmp_path, JulySession("2026-07-31 12:00:00"), date(2026, 7, 20))
    scopes = src.catalog.rolling_scopes()
    assert sum(path.endswith("/2025") for path in session.calls) == 6
    assert sum(path.endswith("/2026") for path in session.calls) == 6
    assert any(scope.source_season_id == 2025 and not scope.is_closed for scope in scopes)
    assert not any(scope.source_season_id == 2026 for scope in scopes)


@pytest.mark.parametrize("is_result", [False, "false", "False", "0"])
def test_false_result_does_not_close_season(tmp_path, is_result):
    from types import SimpleNamespace
    from scrapers.understat.catalog import LEAGUE_BY_CANONICAL
    src, _ = source(tmp_path)
    league = deepcopy(LEAGUE_PAYLOAD)
    league["dates"][0]["isResult"] = is_result
    src.catalog.record_league(LEAGUE_BY_CANONICAL[LEAGUE], 2025, league)
    src.catalog.note_quality(LEAGUE, 2025, SimpleNamespace(passed=True, match_quarantine=[]))
    assert not src.catalog._confirmed(LEAGUE_BY_CANONICAL[LEAGUE])["2025"]["is_closed"]


def test_last_match_quarantine_keeps_daily_until_existing_retry_deadline(tmp_path):
    from types import SimpleNamespace
    from scrapers.understat.catalog import LEAGUE_BY_CANONICAL
    src, _ = source(tmp_path)
    definition = LEAGUE_BY_CANONICAL[LEAGUE]
    src.catalog.record_league(definition, 2025, LEAGUE_PAYLOAD)
    until = "2025-08-17T09:00:00+00:00"
    report = SimpleNamespace(passed=True, match_quarantine=[
        SimpleNamespace(state="waiting_data", retry_until=until),
    ])
    src.catalog.note_quality(LEAGUE, 2025, report)
    for day in range(11, 18):
        src.catalog.today = date(2025, 8, day)
        state = src.catalog.record_league(definition, 2025, LEAGUE_PAYLOAD)
        assert not state["is_closed"]
    src.catalog.note_quality(LEAGUE, 2025, SimpleNamespace(passed=False, match_quarantine=[]))
    src.catalog.today = date(2025, 8, 18)
    assert not src.catalog.record_league(definition, 2025, LEAGUE_PAYLOAD)["is_closed"]
    src.catalog.note_quality(LEAGUE, 2025, SimpleNamespace(passed=True, match_quarantine=[]))
    assert src.catalog.record_league(definition, 2025, LEAGUE_PAYLOAD)["is_closed"]


def test_ledger_write_failure_does_not_increment_actual_http_count(tmp_path, monkeypatch):
    src, session = source(tmp_path)
    def reject(*args):
        raise OSError("offline ledger failure")
    monkeypatch.setattr(CacheState, "count_request", reject)
    with pytest.raises(OSError, match="ledger failure"):
        run(src)
    assert not session.calls
    assert src.client.request_count == 0


def test_changed_league_cannot_reuse_previous_publication_readiness(tmp_path):
    from types import SimpleNamespace
    from scrapers.understat.catalog import LEAGUE_BY_CANONICAL
    src, _ = source(tmp_path)
    definition = LEAGUE_BY_CANONICAL[LEAGUE]
    src.catalog.record_league(definition, 2025, LEAGUE_PAYLOAD)
    src.catalog.note_quality(LEAGUE, 2025, SimpleNamespace(passed=True, match_quarantine=[]))
    changed = deepcopy(LEAGUE_PAYLOAD)
    changed["dates"][0]["xG"]["h"] = "1"
    assert not src.catalog.record_league(definition, 2025, changed)["is_closed"]


def test_recorded_rfpl_schedule_writes_same_full_batch_after_warm_cache(tmp_path):
    from urllib.parse import quote
    from dags.scripts import run_understat_scraper as runner
    from tests.unit.scrapers.test_understat_season_start import (
        _base_league, _league, _results, _match_payload,
        _team_payload as team_payload,
    )
    from tests.unit.scrapers.test_run_understat_scraper import (
        _Repository, _args, _scraper_factory,
    )
    league = _league({row["id"] for row in _results(_base_league())[:5]})
    responses = {"": {}, "/getLeagueData/RFPL/2026": league}
    for match in _results(league):
        responses[f"/getMatchData/{match['id']}"] = _match_payload(match)
    for team in league["teams"].values():
        if team["history"]:
            slug = quote(team["title"].replace(" ", "_"), safe="_-")
            responses[f"/getTeamData/{slug}/2026"] = team_payload(league, team)

    class Replay:
        headers = {}
        calls = []

        def get(self, url, **kwargs):
            path = url.removeprefix("https://understat.com")
            self.calls.append(path)
            return _Response(200, deepcopy(responses[path]))

    src, session = source(tmp_path, Replay(), date(2026, 10, 26))
    def scrape(**kwargs):
        frames = src.scrape_scope("RUS-Premier League", "2627", 2026, **kwargs)
        src.confirm_cache()
        return frames
    scrape()
    session.calls.clear()
    cached = scrape()
    assert session.calls == ["/getLeagueData/RFPL/2026"]
    full = scrape(mode="reparse")
    # Exercise the actual runner's metadata, DQ, replace guard and fence with
    # a storage double; all seven written frames must equal a full scrape.
    args = _args(source_year=2026)
    args.league = "RUS-Premier League"
    factory, writer = _scraper_factory(cached)
    repository = _Repository()
    payload, code = runner.run_scope(args, scraper_factory=factory, repository=repository)
    assert code == 0
    assert writer.save_to_iceberg.call_count == 7
    expected = runner._decorate_frames(full, batch_id=payload["batch_id"])
    for call in writer.save_to_iceberg.call_args_list:
        pd.testing.assert_frame_equal(call.kwargs["df"], expected[call.kwargs["table_name"]])
        assert call.kwargs["min_replace_ratio"] == 0.9
    assert repository.verified


@pytest.mark.parametrize("side", ["h", "a"])
@pytest.mark.parametrize("validated_cache", [False, True])
def test_one_sided_roster_is_not_reused(tmp_path, side, validated_cache):
    src, session = source(tmp_path)
    session.match["rosters"][side] = []
    run(src)
    cache = CacheState(tmp_path)
    if validated_cache:
        # Also repair partial entries written by the previous implementation.
        cache.save("match_100.json", [LEAGUE, 2025, 100],
                   digest(session.league["dates"][0]), session.match, TODAY)
    else:
        assert cache.load("match_100.json", [LEAGUE, 2025, 100]) is None
    session.match = deepcopy(MATCH_PAYLOAD)
    session.calls.clear()
    frames = run(src)
    assert "/getMatchData/100" in session.calls
    assert set(frames["understat_player_match_stats"]["team_side"]) == {"h", "a"}


@pytest.mark.parametrize("placeholder", [{}, {"history": []}])
def test_placeholder_before_played_teams_is_ignored(tmp_path, placeholder):
    src, session = source(tmp_path)
    session.league["teams"] = {"placeholder": placeholder, **session.league["teams"]}
    first = run(src)
    session.calls.clear()
    second = run(src)
    assert session.calls == ["/getLeagueData/EPL/2025"]
    for table in first:
        pd.testing.assert_frame_equal(first[table], second[table])


@pytest.mark.parametrize("failure", ["players", "match", "write", "physical", "terminal"])
def test_failed_scope_retries_only_unconfirmed_responses(tmp_path, monkeypatch, failure):
    from dags.scripts import run_understat_scraper as runner
    from scrapers.understat.manifest import ManifestStatus
    from scrapers.understat.scraper import UnderstatScraper
    from tests.unit.scrapers.test_run_understat_scraper import (
        _Repository, _args, _freeze_runner_time, _scraper_factory,
    )

    class ValidSession(Session):
        broken = False

        def __init__(self):
            super().__init__()
            for player_id in (8, 9):
                self.league["players"].extend(_team_payload(player_id)["players"])

        def get(self, url, **kwargs):
            response = super().get(url, **kwargs)
            if "/getTeamData/" in url:
                home = "/Home/" in url
                response._payload = _team_payload(7 if home else 9)
                if home and not (self.broken and failure == "players"):
                    response._payload["players"].extend(_team_payload(8)["players"])
            if self.broken and failure == "match" and "/getMatchData/" in url:
                response._payload["shots"]["h"][0]["xG"] = "2"
            return response

    _freeze_runner_time(monkeypatch, datetime(2025, 8, 10, tzinfo=timezone.utc))
    session = ValidSession()
    src, _ = source(tmp_path, session)
    wrapper = UnderstatScraper.__new__(UnderstatScraper)
    wrapper.source = src
    factory, scraper = _scraper_factory({})
    scraper.scrape_scope.side_effect = wrapper.scrape_scope
    scraper.confirm_cache.side_effect = getattr(wrapper, "confirm_cache", lambda: None)
    scraper.note_quality.side_effect = wrapper.note_quality
    repository = _Repository()
    args = _args()
    assert runner.run_scope(args, scraper_factory=factory, repository=repository)[1] == 0

    # Make only one previously confirmed response require an HTTP refresh.
    target = "match_100.json" if failure == "match" else "team_Home_2025.json"
    path = tmp_path / "validated" / target
    record = json.loads(path.read_text())
    record["fingerprint"] = "stale"
    atomic_json(path, record)
    session.broken = True
    repository.physical = failure != "physical"
    append = repository.append_attempt

    def append_attempt(attempt):
        if session.broken and failure == "terminal" and attempt.status is ManifestStatus.COMPLETE:
            raise ValueError("offline terminal manifest failure")
        return append(attempt)

    repository.append_attempt = append_attempt
    if failure == "write":
        scraper.save_to_iceberg.side_effect = ValueError("offline write failure")
    for _ in range(2):
        session.calls.clear()
        payload, code = runner.run_scope(args, scraper_factory=factory, repository=repository)
        assert code == 2, payload
        if failure in {"players", "match"}:
            assert repository.failures[-1].status is ManifestStatus.DQ_FAILURE
            codes = {issue["code"] for issue in payload["scope_attempt"]["quality"]["issues"]}
            assert ("player_team_reference_gap" if failure == "players" else "invalid_xg") in codes
        assert f"/get{'MatchData/100' if failure == 'match' else 'TeamData/Home/2025'}" in session.calls
        if failure != "match":
            assert session.calls == ["/getLeagueData/EPL/2025", "/getTeamData/Home/2025"]
    assert not path.exists()

    session.broken = False
    repository.physical = True
    scraper.save_to_iceberg.side_effect = lambda **kwargs: kwargs["table_name"]
    session.calls.clear()
    payload, code = runner.run_scope(args, scraper_factory=factory, repository=repository)
    assert code == 0, payload
    assert payload["status"] == "complete"
    assert (tmp_path / "validated" / target).exists()
    session.calls.clear()
    assert runner.run_scope(args, scraper_factory=factory, repository=repository)[1] == 0
    assert session.calls == ["/getLeagueData/EPL/2025"]


def test_previous_cache_version_requires_publication_confirmation(tmp_path):
    src, session = source(tmp_path)
    run(src)
    path = tmp_path / "validated" / "team_Home_2025.json"
    record = json.loads(path.read_text())
    record["version"] = 1
    atomic_json(path, record)
    session.calls.clear()
    src.scrape_scope(LEAGUE, "2526", 2025)
    assert session.calls == ["/getLeagueData/EPL/2025", "/getTeamData/Home/2025"]
    assert not path.exists()
    src.confirm_cache()
    assert json.loads(path.read_text())["version"] == 2
