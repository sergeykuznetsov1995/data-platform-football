"""Parallel WhoScored history workers (#1481), offline."""
import json
import multiprocessing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dags.scripts import run_whoscored_scraper as runner
from scrapers.whoscored import history, history_state as state
from tests.unit.scrapers.test_whoscored_history import setup_chunk


KEYS = [f"L{i}=25{i:02d}" for i in range(40)]


def test_share_is_stable_and_splits_scopes_disjointly():
    # sha256, not hash(): every worker process computes the same owner.
    assert [state.share(key, 3) for key in KEYS[:6]] == [
        state.share(key, 3) for key in list(KEYS[:6])
    ]
    assert state.share("ENG-Premier League=2526", 3) == int(
        __import__("hashlib").sha256(b"ENG-Premier League=2526").hexdigest()[:16], 16) % 3
    shares = [{k for k in KEYS if state.share(k, 3) == i} for i in range(3)]
    assert set().union(*shares) == set(KEYS)
    assert sum(map(len, shares)) == len(KEYS)
    assert all(shares)


def _reserve_all(path, queue):
    checkpoint = state.HistoryState(path)
    taken = []
    while True:
        reservation = checkpoint.reserve([{"key": k} for k in KEYS], now=0)
        if reservation is None:
            break
        taken.append(reservation[0]["key"])
    queue.put(taken)


def test_parallel_processes_never_reserve_one_scope_twice(tmp_path):
    # Guard (no fix needed): BEGIN IMMEDIATE + busy timeout serialise claims.
    path = tmp_path / "history.sqlite"
    state.HistoryState(path)
    context = multiprocessing.get_context("fork")
    queue = context.Queue()
    workers = [context.Process(target=_reserve_all, args=(path, queue)) for _ in range(4)]
    for worker in workers:
        worker.start()
    taken = [key for _ in workers for key in queue.get(timeout=60)]
    for worker in workers:
        worker.join(timeout=60)
    assert sorted(taken) == sorted(KEYS)


def test_run_chunk_reserves_only_owned_scopes(tmp_path):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    report = history.run_chunk(**kwargs, owns=lambda key: False)
    assert report["reason"] == "idle"
    service.sync_matches.assert_not_called()
    assert kwargs["checkpoint"].reserve([{"key": s.scope.spec}], now=0) is not None

    kwargs, s, repo, service = setup_chunk(tmp_path / "own")
    report = history.run_chunk(**kwargs, owns=lambda key: key == s.scope.spec)
    assert report["scope"] == s.scope.spec


def test_degraded_flag_expires_after_cooldown(tmp_path):
    checkpoint = state.HistoryState(tmp_path / "history.sqlite")
    assert checkpoint.degraded(now=0) is False
    checkpoint.trip_degraded(now=100)
    other = state.HistoryState(checkpoint.path)
    assert other.degraded(now=101) is True
    checkpoint.trip_degraded(now=50)  # an older trip never shortens the window
    assert other.degraded(now=100 + state.DEGRADED_SECONDS - 1) is True
    assert other.degraded(now=100 + state.DEGRADED_SECONDS) is False


def _worker_env(tmp_path, monkeypatch, traffic):
    kwargs, s, repo, service = setup_chunk(tmp_path)
    repo.load_catalog_generation_snapshot.return_value = ({"catalog_batch_id": "c"}, kwargs["catalog"])
    service.sync_matches.return_value = SimpleNamespace(
        entity="matches", scope=s.scope.spec, counts={}, tables=[], errors=[],
        retryable=[], metadata={}, attempted=25)
    service.traffic_stats.return_value = traffic
    monkeypatch.setattr(state, "current_running", lambda: False)
    monkeypatch.setattr(history, "final_feeds", lambda *a, **k: True)
    monkeypatch.setattr(history, "run_chunk", __import__("functools").partial(
        history.run_chunk, now=kwargs["now"]))
    monkeypatch.setenv("WHOSCORED_PROXY_MEMBER", "")  # restored after the test
    monkeypatch.delenv("WHOSCORED_PROXY_MEMBER")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "run-1")
    args = SimpleNamespace(split_lanes="true", output=str(tmp_path / "r.json"))
    return kwargs, s, repo, args


def test_worker_owns_its_share_takes_a_pool_line_and_trips_on_browser_cf(tmp_path, monkeypatch):
    kwargs, s, repo, args = _worker_env(tmp_path, monkeypatch, {"browser_challenges": 1})
    owner = state.share(s.scope.spec, 3)
    report = runner._new_report("history", [])
    runner._run_history_chunk(args, kwargs["service_cls"], report, repo,
                              kwargs["checkpoint"], (owner, 3))
    assert report["history"]["scope"] == s.scope.spec
    assert runner.os.environ["WHOSCORED_PROXY_MEMBER"].isdigit()
    assert kwargs["checkpoint"].degraded(now=__import__("time").time()) is True

    # Degraded: worker 0 takes every scope, others leave before any query.
    other = runner._new_report("history", [])
    repo.load_catalog_generation_snapshot.reset_mock()
    runner._run_history_chunk(args, kwargs["service_cls"], other, repo,
                              kwargs["checkpoint"], (1, 3))
    assert other["history"] == {"reason": "degraded", "attempted": 0}
    repo.load_catalog_generation_snapshot.assert_not_called()


def test_degraded_worker_zero_takes_scopes_outside_its_share(tmp_path, monkeypatch):
    kwargs, s, repo, args = _worker_env(tmp_path, monkeypatch, {})
    if state.share(s.scope.spec, 3) == 0:
        pytest.skip("fixture scope already belongs to worker 0")
    kwargs["checkpoint"].trip_degraded(now=__import__("time").time())
    report = runner._new_report("history", [])
    runner._run_history_chunk(args, kwargs["service_cls"], report, repo,
                              kwargs["checkpoint"], (0, 3))
    assert report["history"]["scope"] == s.scope.spec


@pytest.mark.parametrize("raw, expected", [("3", 3), ("9", 4), ("0", 1), ("abc", 1), ("1", 1)])
def test_workers_variable_is_clamped(raw, expected):
    args = runner._build_parser().parse_args(["history", "--workers", raw])
    assert args.workers == expected


@pytest.mark.parametrize("argv", [["daily", "--workers", "3"],
                                  ["history", "--worker", "3/3"],
                                  ["history", "--worker", "0/5"]])
def test_worker_flags_are_history_only_and_bounded(argv):
    parser = runner._build_parser()
    with pytest.raises(SystemExit):
        runner._validate_args(parser, parser.parse_args(argv))


def _child(index, *, status="success", attempted=25, errors=()):
    report = runner._new_report("history", [])
    scope = f"L{index}=2526"
    report.update(status=status, errors=list(errors), rows=10, catalog_batch_id="c",
                  scopes=[{"scope": scope, "status": status, "entities": {}, "errors": []}],
                  traffic={"route_requests": {"direct_http": 5}},
                  traffic_by_scope={scope: {"route_requests": {"direct_http": 5}}},
                  error_details=[{"scope": scope, "entity": "matches", "type": "X",
                                  "message": e, "retryable": False} for e in errors])
    report["producer_commits"]["match"] = [f"b{index}"]
    report["history"] = {"scope": scope, "attempted": attempted, "did_work": attempted > 0}
    return report


def test_parent_runs_schema_once_and_merges_every_worker(tmp_path, monkeypatch):
    repo = MagicMock()
    monkeypatch.setattr(runner, "_new_repository", lambda: repo)
    monkeypatch.setattr(state, "HistoryState", lambda path: MagicMock())
    monkeypatch.setenv("WHOSCORED_SCHEMA_READY", "")  # restored after the test
    monkeypatch.delenv("WHOSCORED_SCHEMA_READY")
    children = {0: (0, _child(0)), 1: (1, _child(1, status="failed", errors=["boom"])),
                2: (1, None)}
    spawned = []

    def spawn(args, index, count, output):
        spawned.append((index, count))
        code, child = children[index]
        if child is not None:
            Path(output).write_text(json.dumps(child), encoding="utf-8")
        return SimpleNamespace(wait=lambda: code)

    monkeypatch.setattr(runner, "_spawn_history_worker", spawn)
    output = tmp_path / "result.json"
    args = runner._build_parser().parse_args(
        ["history", "--split-lanes", "true", "--workers", "3",
         "--transport-policy", "direct_only", "--output", str(output)])
    rc = runner._run_history(args, MagicMock(), runner._new_report("history", []))
    report = json.loads(output.read_text(encoding="utf-8"))

    repo.ensure_schema.assert_called_once_with()
    repo.load_catalog_generation_snapshot.assert_not_called()
    assert spawned == [(0, 3), (1, 3), (2, 3)]
    assert rc == 1 and report["status"] == "failed"
    # The failed worker turns the pass red, but worker 0's work is kept.
    assert [w["attempted"] for w in report["history"]["workers"]] == [25, 25, 0]
    assert report["history"]["attempted"] == 50
    assert [s["scope"] for s in report["scopes"]] == ["L0=2526", "L1=2526"]
    assert report["producer_commits"]["match"] == ["b0", "b1"]
    assert report["traffic"]["route_requests"]["direct_http"] == 10
    assert any("boom" in e for e in report["errors"])
    assert any("worker 2 exited 1 without a report" in e for e in report["errors"])


def test_one_worker_keeps_the_in_process_path(tmp_path, monkeypatch):
    repo = MagicMock()
    repo.load_catalog_generation_snapshot.return_value = ({"catalog_batch_id": "c"}, object())
    monkeypatch.setattr(runner, "_new_repository", lambda: repo)
    monkeypatch.setattr(state, "HistoryState", lambda path: MagicMock())
    monkeypatch.setattr(runner, "_spawn_history_worker", MagicMock(side_effect=AssertionError))
    chunk = MagicMock(return_value={"reason": "idle", "attempted": 0})
    monkeypatch.setattr(history, "run_chunk", chunk)
    monkeypatch.setenv("WHOSCORED_PROXY_MEMBER", "")  # restored after the test
    monkeypatch.delenv("WHOSCORED_PROXY_MEMBER")
    output = tmp_path / "result.json"
    args = runner._build_parser().parse_args(
        ["history", "--split-lanes", "true", "--workers", "1",
         "--transport-policy", "direct_only", "--output", str(output)])
    assert runner._run_history(args, MagicMock(), runner._new_report("history", [])) == 0
    chunk.assert_called_once()
    # One worker keeps the old random sticky pool member.
    assert "WHOSCORED_PROXY_MEMBER" not in runner.os.environ
