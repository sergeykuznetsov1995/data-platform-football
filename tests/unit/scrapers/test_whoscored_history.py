"""Offline history queue, leases and bounded execution regressions (#1480)."""
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scrapers.whoscored import history, history_state as state
from scrapers.whoscored.catalog import CatalogCompetition, CatalogSeason, WhoScoredCatalog
from scrapers.whoscored.domain import WhoScoredScope

NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)


def season(league="ENG-Premier League", label="2526", source=100):
    return CatalogSeason(WhoScoredScope(league, label, "split_year"),
                         source_season_id=source, stage_ids=(1,), end=date(2026, 6, 1))


def catalog(*competitions):
    return WhoScoredCatalog(tuple(CatalogCompetition(league, tuple(seasons), True,
                                tournament_id=tid) for league, tid, seasons in competitions))


def coverage(**changes):
    result = dict(schedule_ready=True, closeable=100, closed=0, unknown_stages=0, closed_day=0, stage_ids=(1,))
    result.update(changes)
    return result


def test_wave_order_source_ids_and_calendar_gap():
    eng = [season(label=s, source=i) for s, i in
           [("2627", 400), ("2526", 300), ("2425", 200), ("1516", 100), ("1415", 90)]]
    mls = [CatalogSeason(WhoScoredScope("MLS", s, "single_year"), source_season_id=i)
           for s, i in [("2026", 450), ("2025", 350), ("2024", 250)]]
    queued = state.queue(catalog(("ENG-Premier League", 2, eng), ("MLS", 85, mls)), today=NOW.date())
    assert [s["season"].scope.season_id for s in queued if s["wave"] == 1] == ["2627", "2526", "2026", "2025"]
    assert [s["season"].scope.season_id for s in queued if s["wave"] == 2] == ["2425", "1516", "2024"]
    assert queued[-1]["season"].scope.season_id == "1415"


@pytest.mark.parametrize("changes, ready", [
    ({"closed": 99}, True), ({"closed": 98}, False),
    ({"closed": 100, "unknown_stages": 1}, False),
    ({"closed": 100, "schedule_ready": False}, False),
])
def test_wave_gate_keeps_delayed_retries_and_unknown_stages(changes, ready):
    assert state.first_wave_ready([{"wave": 1, "coverage": coverage(**changes)}]) is ready


def test_atomic_reservation_park_a_work_b_then_retry_a(tmp_path):
    checkpoint = state.HistoryState(tmp_path / "history.sqlite")
    scopes = [{"key": "A"}, {"key": "B"}]
    a, token = checkpoint.reserve(scopes, now=0)
    assert a["key"] == "A"
    second = state.HistoryState(checkpoint.path)
    assert second.reserve(scopes, now=0)[0]["key"] == "B"
    checkpoint.release("A", token, now=1, failed=True, duration=1)
    assert checkpoint.reserve(scopes, now=2) is None
    assert checkpoint.reserve(scopes, now=3601)[0]["key"] == "A"
    with pytest.raises(RuntimeError, match="lease was lost"):
        checkpoint.release("A", token, now=3602, failed=False, duration=1)


def test_crashed_lease_is_parked_before_recovery(tmp_path):
    checkpoint = state.HistoryState(tmp_path / "history.sqlite")
    scopes = [{"key": "A"}, {"key": "B"}]
    checkpoint.reserve(scopes, now=0)
    assert checkpoint.reserve(scopes, now=state.LEASE_SECONDS)[0]["key"] == "B"
    assert checkpoint.reserve(scopes, now=state.LEASE_SECONDS + state.COOLDOWN_SECONDS)[0]["key"] == "A"


def setup_chunk(tmp_path):
    s = season()
    cat = catalog((s.scope.competition_id, 2, [s]))
    repo = MagicMock()
    repo.history_coverage.return_value = {(s.scope.competition_id, s.scope.season_id): coverage()}
    repo.list_match_candidates.return_value = [SimpleNamespace(game_id=1)]
    repo.list_preview_candidates.return_value = []
    repo.list_profile_candidates.return_value = []
    service = MagicMock()
    service.__enter__.return_value = service
    service.sync_matches.return_value = SimpleNamespace(
        attempted=25, metadata={"history_match_ids": list(range(25))})
    service_class = MagicMock(return_value=service)
    checkpoint = state.HistoryState(tmp_path / "history.sqlite")
    kwargs = dict(catalog=cat, repository=repo, checkpoint=checkpoint, service_cls=service_class,
                  enabled=lambda: True, current_running=lambda: False, consume=lambda r: None, now=NOW)
    return kwargs, s, repo, service


def test_chunk_bound_and_reselection_from_journal(tmp_path):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    report = history.run_chunk(**kwargs)
    service.sync_matches.assert_called_once_with(limit=25, lane="history")
    service.sync_previews.assert_not_called()
    repo.list_profile_candidates.assert_called_once_with(scopes=[s.scope], limit=25, once=True)
    assert report["pace"]["alert"] is True
    assert repo.history_coverage.call_count == 2
    # No remembered match IDs or success marker: next chunk queries again.
    kwargs["now"] = datetime(2026, 10, 28, tzinfo=timezone.utc)
    history.run_chunk(**kwargs)
    assert service.sync_matches.call_count == 2


def test_missing_schedule_runs_only_schedule(tmp_path):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    repo.history_coverage.return_value = {}
    history.run_chunk(**kwargs)
    service.sync_schedule.assert_called_once_with()
    service.sync_matches.assert_not_called()
    service.sync_stage_feeds.assert_not_called()


@pytest.mark.parametrize("disabled, busy", [(True, False), (False, True)])
def test_disabled_or_current_run_does_not_reserve_or_call_source(tmp_path, disabled, busy):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    kwargs.update(enabled=lambda: not disabled, current_running=lambda: busy)
    assert history.run_chunk(**kwargs)["attempted"] == 0
    repo.history_coverage.assert_not_called()
    service.probe_egress.assert_not_called()


def test_current_run_appears_after_reservation(tmp_path):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    busy = iter([False, True])
    kwargs["current_running"] = lambda: next(busy)
    assert history.run_chunk(**kwargs)["attempted"] == 0
    service.probe_egress.assert_not_called()


def test_failure_parks_scope_and_propagates(tmp_path):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    service.sync_matches.side_effect = RuntimeError("chunk failed")
    with pytest.raises(RuntimeError, match="chunk failed"):
        history.run_chunk(**kwargs)
    assert kwargs["checkpoint"].parked(now=NOW.timestamp()) == 1
    assert history.run_chunk(**kwargs)["reason"] == "idle"


def test_final_feed_snapshot_not_repeated(tmp_path):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    repo.latest_stage_feed_snapshot.return_value = {
        "parser_version": history.PARSER_VERSION, "feed_policy": history.STAGE_XG_FEED_POLICY,
        "feed_checks": {"1": {"final": True, "checked_at": NOW.isoformat()}},
        "feed_states": {"1:team:xg-teamstats:summary": "present",
                        "1:player:xg-stats:summary": "present"},
    }
    history.run_chunk(**kwargs)
    service.sync_stage_feeds.assert_not_called()
    repo.latest_stage_feed_snapshot.return_value["feed_checks"]["1"]["final"] = False
    kwargs["now"] = datetime(2026, 10, 28, tzinfo=timezone.utc)
    history.run_chunk(**kwargs)
    service.sync_stage_feeds.assert_called_once_with()


def test_pace_alerts_only_with_debt_and_zero_progress():
    scopes = [{"coverage": coverage(closed=100)}]
    assert state.pace(scopes, parked=0)["alert"] is False
    scopes[0]["coverage"] = coverage(closed_day=1)
    assert state.pace(scopes, parked=0)["alert"] is False
    scopes[0]["coverage"] = coverage(schedule_ready=False)
    assert state.pace(scopes, parked=1)["alert"] is True



def test_empty_scopes_do_not_starve_fifth_or_second_wave(tmp_path):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    seasons = [season(league=f"league-{i}") for i in range(6)]
    kwargs["catalog"] = catalog(*[(s.scope.competition_id, 2, [s]) for s in seasons])
    repo.history_coverage.return_value = {(s.scope.competition_id, s.scope.season_id):
                                         coverage(closed=100) for s in seasons}
    repo.list_match_candidates.side_effect = lambda league, *a, **k: [1] if league == "league-5" else []
    repo.latest_stage_feed_snapshot.return_value = {
        "parser_version": history.PARSER_VERSION, "feed_policy": history.STAGE_XG_FEED_POLICY,
        "feed_checks": {"1": {"final": True, "checked_at": NOW.isoformat()}},
        "feed_states": {"1:team:xg-teamstats:summary": "present",
                        "1:player:xg-stats:summary": "present"},
    }
    result = history.run_chunk(**kwargs)
    assert result["scope"] == "league-5=2526"
    assert kwargs["service_cls"].call_count == 1
    assert repo.list_match_candidates.call_count == 6


def test_preview_backlog_recovers_after_match_already_succeeded(tmp_path):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    repo.list_match_candidates.return_value = []
    repo.list_preview_candidates.return_value = [{"game_id": 7}]
    service.sync_previews.side_effect = RuntimeError("preview failed")
    with pytest.raises(RuntimeError, match="preview failed"):
        history.run_chunk(**kwargs)
    service.sync_previews.side_effect = None
    kwargs["now"] = datetime(2026, 10, 28, tzinfo=timezone.utc)
    history.run_chunk(**kwargs)
    assert service.sync_previews.call_count == 2
    service.sync_previews.assert_called_with(match_ids=[7], historical=True)
    service.sync_matches.assert_not_called()


def test_missing_catalog_stage_blocks_wave_gate(tmp_path):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    repo.history_coverage.return_value = {(s.scope.competition_id, s.scope.season_id):
                                         coverage(closed=100, stage_ids=(2,))}
    scopes = history.inventory(kwargs["catalog"], repo, today=NOW.date())
    assert scopes[0]["coverage"]["unknown_stages"] == 1
    assert scopes[0]["coverage"]["schedule_ready"] is False
    assert state.first_wave_ready(scopes) is False


@pytest.mark.parametrize("fault", ["future", "naive", "missing_time", "missing_feed"])
def test_incomplete_feed_proof_is_not_final(tmp_path, fault):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    snapshot = {"parser_version": history.PARSER_VERSION, "feed_policy": history.STAGE_XG_FEED_POLICY,
        "feed_checks": {"1": {"final": True, "checked_at": NOW.isoformat()}},
        "feed_states": {"1:team:xg-teamstats:summary": "present",
                        "1:player:xg-stats:summary": "present"}}
    if fault == "future":
        snapshot["feed_checks"]["1"]["checked_at"] = "2027-01-01T00:00:00+00:00"
    elif fault == "naive":
        snapshot["feed_checks"]["1"]["checked_at"] = "2026-01-01T00:00:00"
    elif fault == "missing_time":
        del snapshot["feed_checks"]["1"]["checked_at"]
    else:
        snapshot["feed_states"] = {}
    repo.latest_stage_feed_snapshot.return_value = snapshot
    assert history.final_feeds(repo, s, moment=NOW) is False


def test_future_season_is_not_in_first_wave():
    from dataclasses import replace
    future = replace(season(label="2728", source=999), start=date(2027, 7, 1))
    current = season(label="2627", source=500)
    previous = season(label="2526", source=400)
    scopes = state.queue(catalog((current.scope.competition_id, 2, [future, current, previous])), today=NOW.date())
    assert [s["key"] for s in scopes] == [current.scope.spec, previous.scope.spec]


def test_wave_two_excludes_previews_and_probes_one_stage(tmp_path, monkeypatch):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    queued = [{"key": s.scope.spec, "wave": 2, "season": s, "coverage":
               coverage(unknown_stages=3)}]
    monkeypatch.setattr(history, "inventory", lambda *a, **k: queued)
    monkeypatch.setattr(state, "first_wave_ready", lambda scopes: True)
    history.run_chunk(**kwargs)
    service.sync_matches.assert_called_once_with(limit=1, lane="history")
    repo.list_preview_candidates.assert_not_called()
    service.sync_previews.assert_not_called()


def test_idle_cooldown_is_not_reported_as_a_failed_scope(tmp_path):
    checkpoint = state.HistoryState(tmp_path / "history.sqlite")
    _, token = checkpoint.reserve([{"key": "A"}], now=0)
    checkpoint.release("A", token, now=1, failed=False, idle=True, duration=1)
    assert checkpoint.reserve([{"key": "A"}], now=2) is None
    assert checkpoint.parked(now=2) == 0


@pytest.mark.parametrize("stage_ids", [(), (2,)])
def test_checked_empty_or_absent_stage_does_not_repeat_schedule(tmp_path, stage_ids):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    repo.history_coverage.return_value = {(s.scope.competition_id, s.scope.season_id):
                                         coverage(closed=100, stage_ids=stage_ids)}
    repo.latest_schedule_stage_ids.return_value = (1, 2)
    scopes = history.inventory(kwargs["catalog"], repo, today=NOW.date())
    assert state.first_wave_ready(scopes) is True
    history.run_chunk(**kwargs)
    service.sync_schedule.assert_not_called()
    service.sync_matches.assert_called_once_with(limit=25, lane="history")


def test_deferred_stage_refresh_is_cooled_down_without_blocking_wave_two(tmp_path):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    seasons = [season(label=label, source=source) for label, source in
               [("2627", 300), ("2526", 200), ("2425", 100)]]
    kwargs["catalog"] = catalog((s.scope.competition_id, 2, seasons))
    repo.history_coverage.return_value = {(s.scope.competition_id, s.scope.season_id):
        coverage(closed=100, deferred_stages=int(s.scope.season_id == "2627")) for s in seasons}
    repo.list_match_candidates.side_effect = lambda league, slug, **k: [1] if slug == "2425" else []
    repo.latest_stage_feed_snapshot.return_value = {
        "parser_version": history.PARSER_VERSION, "feed_policy": history.STAGE_XG_FEED_POLICY,
        "feed_checks": {"1": {"final": True, "checked_at": NOW.isoformat()}},
        "feed_states": {"1:team:xg-teamstats:summary": "available",
                        "1:player:xg-stats:summary": "available"}}
    assert history.run_chunk(**kwargs)["scope"].endswith("2627")
    service.sync_schedule.assert_called_once_with()
    assert history.run_chunk(**kwargs)["scope"].endswith("2425")
    service.sync_matches.assert_called_once_with(limit=25, lane="history")
    kwargs["now"] = datetime.fromtimestamp(NOW.timestamp() + 3601, timezone.utc)
    assert history.run_chunk(**kwargs)["scope"].endswith("2425")
    assert service.sync_schedule.call_count == 1
    kwargs["now"] = NOW + timedelta(seconds=state.DEFERRED_REFRESH_SECONDS)
    assert history.run_chunk(**kwargs)["scope"].endswith("2627")
    assert service.sync_schedule.call_count == 2


@pytest.mark.parametrize("other_scope", [False, True])
def test_skipped_feeds_defer_scope_and_continue_queue(tmp_path, other_scope):
    from dataclasses import replace

    kwargs, s, repo, service = setup_chunk(tmp_path)
    recent = replace(s, end=NOW.date().replace(day=25), is_active=True)
    older = season(label="2425", source=50)
    seasons = [recent, older] if other_scope else [recent]
    kwargs["catalog"] = catalog((s.scope.competition_id, 2, seasons))
    repo.history_coverage.return_value = {
        (s.scope.competition_id, s.scope.season_id): coverage(closed=100) for s in seasons}
    repo.list_match_candidates.side_effect = lambda league, slug, **k: [1] if slug == "2425" else []
    repo.latest_stage_feed_snapshot.return_value = {
        "parser_version": history.PARSER_VERSION, "feed_policy": history.STAGE_XG_FEED_POLICY,
        "feed_checks": {"1": {"final": False, "checked_at": NOW.isoformat()}},
    }
    service.sync_stage_feeds.return_value = SimpleNamespace(skipped=1)
    result = history.run_chunk(**kwargs)
    if other_scope:
        assert result["scope"] == older.scope.spec
        assert result["did_work"] is True
        service.sync_matches.assert_called_once_with(limit=25, lane="history")
    else:
        assert result["reason"] == "idle"
        service.sync_matches.assert_not_called()
    calls = service.sync_stage_feeds.call_count
    assert calls == (2 if other_scope else 1)
    assert kwargs["checkpoint"].parked(now=NOW.timestamp()) == 0
    kwargs["now"] = datetime.fromtimestamp(NOW.timestamp() + 31, timezone.utc)
    history.run_chunk(**kwargs)
    assert service.sync_stage_feeds.call_count == calls + int(other_scope)
    kwargs["now"] = datetime.fromtimestamp(NOW.timestamp() + 3601, timezone.utc)
    history.run_chunk(**kwargs)
    assert service.sync_stage_feeds.call_count == calls + int(other_scope) + (2 if other_scope else 1)


def test_runner_records_each_scope_after_skipped_feeds(tmp_path, monkeypatch):
    from functools import partial

    from dags.scripts import run_whoscored_scraper as runner

    kwargs, recent, repo, service = setup_chunk(tmp_path)
    older = season(label="2425", source=50)
    cat = catalog((recent.scope.competition_id, 2, [recent, older]))
    repo.load_catalog_generation_snapshot.return_value = ({"catalog_batch_id": "test"}, cat)
    repo.history_coverage.return_value = {
        (s.scope.competition_id, s.scope.season_id): coverage(closed=100)
        for s in (recent, older)
    }
    repo.list_match_candidates.side_effect = lambda league, slug, **k: [1] if slug == "2425" else []

    def result(entity, scope, **changes):
        return SimpleNamespace(entity=entity, scope=scope.spec, counts={}, tables=[],
                               errors=[], retryable=[], metadata={}, **changes)

    service.sync_stage_feeds.side_effect = [
        result("stages", recent.scope, skipped=1),
        result("stages", older.scope, skipped=1),
    ]
    service.sync_matches.return_value = result("matches", older.scope, attempted=25)
    service.traffic_stats.return_value = {}
    monkeypatch.setattr(history, "run_chunk", partial(history.run_chunk, now=NOW))
    monkeypatch.setattr(runner, "_new_repository", lambda: repo)
    monkeypatch.setattr(state, "HistoryState", lambda path: kwargs["checkpoint"])
    monkeypatch.setattr(state, "current_running", lambda: False)
    monkeypatch.setenv("WHOSCORED_SCHEMA_READY", "0")
    args = SimpleNamespace(split_lanes="true", output=str(tmp_path / "report.json"))
    report = runner._new_report("history", [])

    assert runner._run_history(args, kwargs["service_cls"], report) == 0
    assert report["status"] == "success"
    assert report["errors"] == []
    assert [s["scope"] for s in report["scopes"]] == [recent.scope.spec, older.scope.spec]
    assert all(s["status"] == "success" for s in report["scopes"])
    assert report["history"]["scope"] == older.scope.spec
    assert report["history"]["attempted"] == 25
    assert kwargs["checkpoint"].parked(now=NOW.timestamp()) == 0
    assert service.sync_stage_feeds.call_count == 2


def mixed_stage_coverage():
    from tests.unit.scrapers.test_whoscored_repository_sql_semantics import (
        LEAGUE, SEASON, _attempt, _connection, _game, _history_repository,
    )

    con = _connection()
    try:
        con.execute("CREATE TABLE iceberg.bronze.whoscored_scope_ingest_manifest "
                    "(league VARCHAR, season VARCHAR, state VARCHAR, entity_counts_json VARCHAR)")
        con.execute("INSERT INTO iceberg.bronze.whoscored_scope_ingest_manifest VALUES "
                    "(?, ?, 'success', '{\"whoscored_schedule\":2}')", [LEAGUE, SEASON])
        _game(con, 1, 1, 30)
        _attempt(con, 1, "success", 1)
        _game(con, 2, 1, 20, lineup=False)
        return _history_repository(con).history_coverage()[(LEAGUE, SEASON)]
    finally:
        con.close()


def deferred_chunk(tmp_path):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    repo.history_coverage.return_value = {(s.scope.competition_id, s.scope.season_id):
                                         coverage(closed=100, deferred_stages=1)}
    repo.list_match_candidates.return_value = []
    repo.latest_stage_feed_snapshot.return_value = {
        "parser_version": history.PARSER_VERSION, "feed_policy": history.STAGE_XG_FEED_POLICY,
        "feed_checks": {"1": {"final": True, "checked_at": NOW.isoformat()}},
        "feed_states": {"1:team:xg-teamstats:summary": "present",
                        "1:player:xg-stats:summary": "present"},
    }
    return kwargs, s, repo, service


@pytest.mark.parametrize("confirm_day", [None, 29, 31])
def test_deferred_refresh_ends_at_thirty_days(tmp_path, monkeypatch, confirm_day):
    from tests.unit.scrapers import test_whoscored_repository_sql_semantics as sql

    kwargs, s, repo, service = deferred_chunk(tmp_path)
    start = NOW.replace(tzinfo=None)
    monkeypatch.setattr(sql, "NOW", start)
    con = sql._connection()
    try:
        con.execute("CREATE TABLE iceberg.bronze.whoscored_scope_ingest_manifest "
                    "(league VARCHAR, season VARCHAR, state VARCHAR, entity_counts_json VARCHAR)")
        con.execute("INSERT INTO iceberg.bronze.whoscored_scope_ingest_manifest VALUES "
                    "(?, ?, 'success', '{\"whoscored_schedule\":2}')", [sql.LEAGUE, sql.SEASON])
        sql._game(con, 1, 1, 40)
        sql._attempt(con, 1, "success", 1)
        sql._game(con, 2, 1, 28, lineup=False)
        real_repo = sql._history_repository(con)
        key = (s.scope.competition_id, s.scope.season_id)
        repo.history_coverage.side_effect = lambda: {
            key: real_repo.history_coverage()[(sql.LEAGUE, sql.SEASON)]}
        repo.list_match_candidates.side_effect = lambda *a, **k: real_repo.list_match_candidates(
            sql.LEAGUE, sql.SEASON, **k)
        for age in (28, 29, 30, 31):
            moment = NOW + timedelta(days=age - 28)
            monkeypatch.setattr(sql, "NOW", moment.replace(tzinfo=None))
            kwargs["now"] = moment
            kwargs["checkpoint"] = state.HistoryState(kwargs["checkpoint"].path)
            if age == confirm_day:
                con.execute("UPDATE iceberg.bronze.whoscored_schedule_current "
                            "SET is_lineup_confirmed=TRUE WHERE game_id=2")
            row = repo.history_coverage()[key]
            confirmed = confirm_day is not None and age >= confirm_day
            assert row["closeable"] == 1 + int(confirmed)
            assert row["closed"] == 1
            assert row["unknown_stages"] == 0
            history.run_chunk(**kwargs)
            if confirmed:
                service.sync_matches.assert_called_once_with(limit=25, lane="history")
                break
            assert service.sync_schedule.call_count == min(age - 27, 2)
            assert row["deferred_stages"] == int(age < 30)
        if confirm_day is None:
            service.sync_matches.assert_not_called()
    finally:
        con.close()


def test_failed_probe_does_not_spend_daily_refresh(tmp_path):
    kwargs, s, repo, service = deferred_chunk(tmp_path)
    service.probe_egress.side_effect = RuntimeError("probe failed")
    with pytest.raises(RuntimeError, match="probe failed"):
        history.run_chunk(**kwargs)
    with kwargs["checkpoint"]._connect() as con:
        assert con.execute("SELECT schedule_next_at FROM scopes").fetchone() == (0,)
    service.sync_schedule.assert_not_called()
    kwargs["checkpoint"] = state.HistoryState(kwargs["checkpoint"].path)
    kwargs["now"] = NOW + timedelta(seconds=state.COOLDOWN_SECONDS + 1)
    service.probe_egress.side_effect = None
    history.run_chunk(**kwargs)
    service.sync_schedule.assert_called_once_with()


@pytest.mark.parametrize("delay, expired", [(120, False), (state.LEASE_SECONDS, True)])
def test_deferred_reservation_uses_clock_after_probe(tmp_path, delay, expired):
    kwargs, s, repo, service = deferred_chunk(tmp_path)
    current = NOW
    kwargs["now"] = lambda: current

    def probe():
        nonlocal current
        current += timedelta(seconds=delay)

    service.probe_egress.side_effect = probe
    if expired:
        with pytest.raises(RuntimeError, match="lease was lost"):
            history.run_chunk(**kwargs)
        service.sync_schedule.assert_not_called()
    else:
        history.run_chunk(**kwargs)
        service.sync_schedule.assert_called_once_with()
    with kwargs["checkpoint"]._connect() as con:
        deadline = con.execute("SELECT schedule_next_at FROM scopes").fetchone()[0]
    assert deadline == (0 if expired else current.timestamp() + state.DEFERRED_REFRESH_SECONDS)
    if not expired:
        current += timedelta(seconds=state.COOLDOWN_SECONDS + 1)
        assert history.run_chunk(**kwargs)["reason"] == "idle"
        service.probe_egress.assert_called_once_with()


def test_deferred_due_check_uses_clock_after_inventory(tmp_path):
    kwargs, s, repo, service = deferred_chunk(tmp_path)
    history.run_chunk(**kwargs)
    current = NOW + timedelta(seconds=state.DEFERRED_REFRESH_SECONDS - 1)
    kwargs["now"] = lambda: current

    def candidates(*args, **kw):
        nonlocal current
        current += timedelta(seconds=2)
        return []

    repo.list_match_candidates.side_effect = candidates
    history.run_chunk(**kwargs)
    assert service.sync_schedule.call_count == 2


@pytest.mark.parametrize("failed", [False, True])
def test_mixed_stage_refresh_survives_restart_and_failure(tmp_path, failed):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    row = mixed_stage_coverage()
    assert row["deferred_stages"] == 1
    assert row["closeable"] == row["closed"] == 1
    assert row["unknown_stages"] == 0
    repo.history_coverage.return_value = {(s.scope.competition_id, s.scope.season_id): row}
    repo.list_match_candidates.return_value = []
    service.sync_stage_feeds.return_value = SimpleNamespace(skipped=0)
    # Useful work takes priority over refreshing the deferred schedule.
    history.run_chunk(**kwargs)
    service.sync_schedule.assert_not_called()
    repo.latest_stage_feed_snapshot.return_value = {
        "parser_version": history.PARSER_VERSION, "feed_policy": history.STAGE_XG_FEED_POLICY,
        "feed_checks": {"1": {"final": True, "checked_at": NOW.isoformat()}},
        "feed_states": {"1:team:xg-teamstats:summary": "present",
                        "1:player:xg-stats:summary": "present"},
    }
    start = NOW + timedelta(seconds=31)
    kwargs["now"] = start
    if failed:
        service.sync_schedule.side_effect = RuntimeError("schedule failed")
        with pytest.raises(RuntimeError, match="schedule failed"):
            history.run_chunk(**kwargs)
        service.sync_schedule.side_effect = None
    else:
        history.run_chunk(**kwargs)
    service.sync_schedule.assert_called_once_with()
    kwargs["checkpoint"] = state.HistoryState(kwargs["checkpoint"].path)
    kwargs["now"] = start + timedelta(hours=1, seconds=1)
    assert history.run_chunk(**kwargs)["reason"] == "idle"
    service.sync_schedule.assert_called_once_with()
    # The daily schedule cooldown must not hold up newly available matches.
    repo.list_match_candidates.return_value = [SimpleNamespace(game_id=2)]
    kwargs["now"] = start + timedelta(hours=2, seconds=2)
    history.run_chunk(**kwargs)
    service.sync_matches.assert_called_once_with(limit=25, lane="history")
    repo.list_match_candidates.return_value = []
    kwargs["now"] = start + timedelta(seconds=state.DEFERRED_REFRESH_SECONDS - 1)
    assert history.run_chunk(**kwargs)["reason"] == "idle"
    service.sync_schedule.assert_called_once_with()
    # The preceding idle scan still has the ordinary one-hour queue cooldown.
    kwargs["now"] = start + timedelta(seconds=state.DEFERRED_REFRESH_SECONDS + 3600)
    history.run_chunk(**kwargs)
    assert service.sync_schedule.call_count == 2


def test_schedule_refresh_migrates_checkpoint_and_isolates_seasons(tmp_path):
    import sqlite3

    path = tmp_path / "history.sqlite"
    with sqlite3.connect(path) as con:
        con.execute("CREATE TABLE scopes (scope TEXT PRIMARY KEY, token TEXT, "
                    "lease_until REAL NOT NULL, next_at REAL NOT NULL, "
                    "duration REAL NOT NULL DEFAULT 0, failed INTEGER NOT NULL DEFAULT 0)")
        con.execute("INSERT INTO scopes VALUES ('A', NULL, 0, 0, 42, 1)")
    state.HistoryState(path)
    checkpoint = state.HistoryState(path)  # Migration is idempotent.
    with sqlite3.connect(path) as con:
        assert con.execute("SELECT duration, failed, schedule_next_at FROM scopes").fetchone() == (42, 1, 0)
    scopes = [{"key": "A"}, {"key": "B"}]
    _, token = checkpoint.reserve(scopes, now=0)
    with pytest.raises(RuntimeError, match="lease was lost"):
        checkpoint.reserve_deferred_refresh("A", "wrong", now=0)
    assert checkpoint.reserve_deferred_refresh("A", token, now=0)
    assert not checkpoint.reserve_deferred_refresh("A", token, now=0)
    _, other_token = checkpoint.reserve(scopes, now=0)
    assert checkpoint.reserve_deferred_refresh("B", other_token, now=0)
    # Simulate a crash without release; recovery must preserve the daily deadline.
    checkpoint = state.HistoryState(path)
    assert checkpoint.reserve(scopes, now=state.LEASE_SECONDS) is None
    now = state.LEASE_SECONDS + state.COOLDOWN_SECONDS
    _, new_token = checkpoint.reserve(scopes, now=now)
    with pytest.raises(RuntimeError, match="lease was lost"):
        checkpoint.reserve_deferred_refresh("A", token, now=now)
    assert not checkpoint.reserve_deferred_refresh("A", new_token, now=now)
    checkpoint.release("A", new_token, now=now, failed=False, idle=True, duration=1)
    _, new_token = checkpoint.reserve(scopes[:1], now=state.DEFERRED_REFRESH_SECONDS)
    assert checkpoint.reserve_deferred_refresh("A", new_token, now=state.DEFERRED_REFRESH_SECONDS)
    with pytest.raises(RuntimeError, match="lease was lost"):
        checkpoint.reserve_deferred_refresh("A", new_token,
                                            now=state.DEFERRED_REFRESH_SECONDS + state.LEASE_SECONDS)
