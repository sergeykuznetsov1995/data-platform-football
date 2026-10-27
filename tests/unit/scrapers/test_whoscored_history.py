"""Offline history queue, leases and bounded execution regressions (#1480)."""
from datetime import date, datetime, timezone
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
