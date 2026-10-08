from __future__ import annotations

from types import SimpleNamespace

import pytest

from dags.scripts.run_sofascore_scraper import (
    ENTITY_MATCH_CAPTURE,
    ENTITY_PLAYER_CAPTURE,
    _load_runtime_workload_plan,
    _logical_capture_traffic,
    _merge_live_traffic,
    _planned_freshness_key,
)
from scrapers.sofascore.workload_plan import (
    WorkloadBudgetPolicy,
    WorkloadClassBudget,
    match_workload_class,
    player_workload_class,
    production_match_shape,
    production_player_shape,
    workload_shape_digest,
)
from scrapers.sofascore.workload_runtime import (
    PartitionWorkload,
    build_partitioned_plan,
    target_ids,
    write_plan,
)


TOKEN = "runner-workload-control-token-at-least-32-bytes"
MATCH_WORKLOAD_CLASS = match_workload_class()
PLAYER_WORKLOAD_CLASS = player_workload_class()
MATCH_SHAPE_DIGEST = workload_shape_digest(production_match_shape())
PLAYER_SHAPE_DIGEST = workload_shape_digest(production_player_shape())


def _match_budget() -> WorkloadClassBudget:
    return WorkloadClassBudget(
        MATCH_WORKLOAD_CLASS,
        "match",
        25,
        100,
        ("event",),
        MATCH_SHAPE_DIGEST,
    )


def _player_budget() -> WorkloadClassBudget:
    return WorkloadClassBudget(
        PLAYER_WORKLOAD_CLASS,
        "player",
        50,
        200,
        ("player_profile",),
        PLAYER_SHAPE_DIGEST,
    )


def _target_plan(tmp_path):
    policy = WorkloadBudgetPolicy(
        "c" * 64,
        {
            MATCH_WORKLOAD_CLASS: _match_budget(),
            PLAYER_WORKLOAD_CLASS: _player_budget(),
        },
    )
    plan = build_partitioned_plan(
        policy,
        dag_id="dag_ingest_sofascore",
        run_id="scheduled-1::targets",
        partitions=[
            PartitionWorkload(
                "ENG-Premier League",
                "2526",
                17,
                pending_match_ids=tuple(str(value) for value in range(1, 28)),
            )
        ],
        control_token=TOKEN,
    )
    return write_plan(tmp_path / "targets.json", plan)


def _player_plan(tmp_path):
    policy = WorkloadBudgetPolicy(
        "c" * 64,
        {
            MATCH_WORKLOAD_CLASS: _match_budget(),
            PLAYER_WORKLOAD_CLASS: _player_budget(),
        },
    )
    plan = build_partitioned_plan(
        policy,
        dag_id="dag_ingest_sofascore",
        run_id="scheduled-1::players",
        partitions=[
            PartitionWorkload(
                "ENG-Premier League",
                "2526",
                17,
                player_universe_ids=tuple(str(value) for value in range(1, 56)),
                pending_player_ids=tuple(str(value) for value in range(1, 56)),
            )
        ],
        control_token=TOKEN,
    )
    return write_plan(tmp_path / "players.json", plan)


@pytest.mark.parametrize(
    ("entity", "plan_factory", "expected_phase", "expected_sizes"),
    [
        (ENTITY_MATCH_CAPTURE, _target_plan, "targets", [25, 2]),
        (ENTITY_PLAYER_CAPTURE, _player_plan, "players", [50, 5]),
    ],
)
def test_runner_selects_every_deterministic_signed_batch(
    tmp_path, monkeypatch, entity, plan_factory, expected_phase, expected_sizes
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_ingest_sofascore")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled-1")
    path = plan_factory(tmp_path)

    plan, allocations = _load_runtime_workload_plan(
        str(path),
        entity=entity,
        league="ENG-Premier League",
        season=2025,
        offline_replay=False,
    )

    assert plan.run_id == f"scheduled-1::{expected_phase}"
    assert [len(target_ids(item)) for item in allocations] == expected_sizes
    flattened = [target for item in allocations for target in target_ids(item)]
    assert len(flattened) == len(set(flattened)) == sum(expected_sizes)


def test_runner_accepts_backfill_dag_with_the_actual_airflow_run_id(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv(
        "AIRFLOW_CTX_DAG_ID", "dag_backfill_sofascore_all_mens"
    )
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled-backfill-1")
    policy = WorkloadBudgetPolicy(
        "c" * 64,
        {MATCH_WORKLOAD_CLASS: _match_budget()},
    )
    plan = build_partitioned_plan(
        policy,
        dag_id="dag_backfill_sofascore_all_mens",
        run_id="scheduled-backfill-1::targets",
        partitions=[PartitionWorkload(
            "SS-17", "2526", 17, pending_match_ids=("1",)
        )],
        control_token=TOKEN,
    )
    path = write_plan(tmp_path / "backfill-targets.json", plan)

    loaded, allocations = _load_runtime_workload_plan(
        str(path),
        entity=ENTITY_MATCH_CAPTURE,
        league="SS-17",
        season=2526,
        offline_replay=False,
    )

    assert loaded.dag_id == "dag_backfill_sofascore_all_mens"
    assert loaded.run_id == "scheduled-backfill-1::targets"
    assert len(allocations) == 1


def test_runner_prefers_the_scope_run_id_over_the_dagrun_id(tmp_path, monkeypatch):
    # One DagRun of the history campaign can carry several scopes; each scope
    # cycle plans under its own SOFASCORE_RUN_ID, which must win over the
    # shared AIRFLOW_CTX_DAG_RUN_ID or the plan is rejected as foreign.
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv(
        "AIRFLOW_CTX_DAG_ID", "dag_backfill_sofascore_all_mens"
    )
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled-backfill-1")
    monkeypatch.setenv("SOFASCORE_RUN_ID", "scheduled-backfill-1--17-1725")
    policy = WorkloadBudgetPolicy(
        "c" * 64,
        {MATCH_WORKLOAD_CLASS: _match_budget()},
    )
    plan = build_partitioned_plan(
        policy,
        dag_id="dag_backfill_sofascore_all_mens",
        run_id="scheduled-backfill-1--17-1725::targets",
        partitions=[PartitionWorkload(
            "SS-17", "2526", 17, pending_match_ids=("1",)
        )],
        control_token=TOKEN,
    )
    path = write_plan(tmp_path / "scope-targets.json", plan)

    loaded, allocations = _load_runtime_workload_plan(
        str(path),
        entity=ENTITY_MATCH_CAPTURE,
        league="SS-17",
        season=2526,
        offline_replay=False,
    )

    assert loaded.run_id == "scheduled-backfill-1--17-1725::targets"
    assert len(allocations) == 1


def test_runner_rejects_target_plan_for_season_capture(tmp_path, monkeypatch):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_ingest_sofascore")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled-1")
    with pytest.raises(RuntimeError, match="wrong workload-plan phase"):
        _load_runtime_workload_plan(
            str(_target_plan(tmp_path)),
            entity="all",
            league="ENG-Premier League",
            season=2025,
            offline_replay=False,
        )


def test_runner_rejects_match_plan_for_player_capture(tmp_path, monkeypatch):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_ingest_sofascore")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled-1")
    with pytest.raises(RuntimeError, match="wrong workload-plan phase"):
        _load_runtime_workload_plan(
            str(_target_plan(tmp_path)),
            entity=ENTITY_PLAYER_CAPTURE,
            league="ENG-Premier League",
            season=2025,
            offline_replay=False,
        )


def test_runner_ignores_environment_fallback_when_freshness_is_signed(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    plan = build_partitioned_plan(
        WorkloadBudgetPolicy(
            "c" * 64,
            {MATCH_WORKLOAD_CLASS: _match_budget()},
        ),
        dag_id="dag_ingest_sofascore",
        run_id="scheduled-1::targets",
        freshness_keys={
            "season": "day-signed",
            "match": "repair-signed",
            "player": "week-signed",
        },
        partitions=[
            PartitionWorkload(
                "ENG-Premier League",
                "2526",
                17,
                pending_match_ids=("1",),
            )
        ],
        control_token=TOKEN,
    )

    assert _planned_freshness_key(plan, "match", "poison-env-value") == (
        "repair-signed"
    )


def test_batch_traffic_merge_keeps_exact_request_map():
    merged = _merge_live_traffic(
        [
            {
                "provider_total_bytes": 30,
                "paid_proxy_bytes": 30,
                "browser_sessions": 1,
                "endpoint_provider_bytes": {"event": 30},
                "endpoint_request_provider_bytes": {"event": [10, 20]},
            },
            {
                "provider_total_bytes": 7,
                "paid_proxy_bytes": 7,
                "browser_sessions": 1,
                "endpoint_provider_bytes": {"event": 7},
                "endpoint_request_provider_bytes": {"event": [7]},
            },
        ]
    )
    assert merged["provider_total_bytes"] == 37
    assert merged["browser_sessions"] == 2
    assert merged["endpoint_provider_bytes"] == {"event": 37}
    assert merged["endpoint_request_provider_bytes"] == {"event": [10, 20, 7]}


def test_multi_batch_final_metrics_come_from_one_logical_engine_snapshot():
    merged = _merge_live_traffic(
        [
            {
                "provider_total_bytes": 30,
                "paid_proxy_bytes": 30,
                "browser_sessions": 1,
                "request_count": 1,
                "endpoint_provider_bytes": {"event": 30},
                "endpoint_request_provider_bytes": {"event": [30]},
            },
            {
                "provider_total_bytes": 7,
                "paid_proxy_bytes": 7,
                # Deliberately cumulative-looking batch counters: final logical
                # metrics must never sum these values or their rates/percentiles.
                "browser_sessions": 2,
                "request_count": 2,
                "endpoint_provider_bytes": {"event": 7},
                "endpoint_request_provider_bytes": {"event": [7]},
            },
        ]
    )
    snapshot = {
        "paid_proxy_bytes": 37,
        "paid_proxy_mb": 37 / 1_048_576,
        "endpoint_provider_bytes": {"event": 37},
        "endpoint_request_provider_bytes": {"event": [30, 7]},
        "browser_sessions": 2,
        "navigations": 2,
        "request_count": 2,
        "completed_matches": 50,
        "completed_players": 0,
        "elapsed_seconds": 10.0,
        "matches_per_second": 5.0,
        "players_per_second": 0.0,
        "p50_duration_ms": 11,
        "p95_duration_ms": 19,
        "cache_hit_rate": 0.25,
        "replay_hit_rate": 0.1,
        "endpoint_completeness": 1.0,
    }
    engine = SimpleNamespace(metrics=SimpleNamespace(snapshot=lambda: dict(snapshot)))

    final = _logical_capture_traffic(engine, merged)

    assert merged["browser_sessions"] == 3
    assert final["browser_sessions"] == 2
    assert final["browser_navigations"] == 2
    assert final["completed_matches"] == 50
    assert final["matches_per_second"] == 5.0
    assert final["p50_duration_ms"] == 11
    assert final["p95_duration_ms"] == 19
    assert final["cache_hit_rate"] == 0.25
    assert final["replay_hit_rate"] == 0.1
    assert final["provider_total_bytes"] == 37


def test_batch_traffic_merge_sums_lease_relaunches():
    merged = _merge_live_traffic(
        [
            {"provider_total_bytes": 30, "paid_proxy_bytes": 30, "lease_relaunches": 1},
            {"provider_total_bytes": 7, "paid_proxy_bytes": 7},
            {"provider_total_bytes": 5, "paid_proxy_bytes": 5, "lease_relaunches": 1},
        ]
    )
    assert merged["lease_relaunches"] == 2
def test_batch_traffic_merge_sums_source_429_counts():
    merged = _merge_live_traffic(
        [
            {"provider_total_bytes": 0, "http_429": 1},
            {"provider_total_bytes": 0, "http_429": 2},
            {"provider_total_bytes": 0},
        ]
    )
    assert merged["http_429"] == 3


@pytest.mark.parametrize(
    "dag_id",
    [
        "dag_ingest_sofascore",
        "dag_backfill_sofascore_all_mens",
        # Sol r12 #3: the refresh lane runs the very same paid match phase, and
        # a broken plan hand-off used to fall back to an unplanned capture.
        "dag_refresh_sofascore_all_mens",
    ],
)
def test_every_production_dag_refuses_to_capture_without_a_plan(dag_id, monkeypatch):
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", dag_id)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled-1")

    with pytest.raises(RuntimeError, match="requires --workload-plan"):
        _load_runtime_workload_plan(
            None,
            entity="all",
            league="ENG-Premier League",
            season=2025,
            offline_replay=False,
        )


def test_runner_takes_the_allocations_in_the_plan_s_target_order(
    tmp_path, monkeypatch
):
    """#1359: the order file beside a refresh plan puts the allocation with
    the nearest deadline first; it never adds or drops an allocation."""
    from scrapers.sofascore.workload_runtime import write_target_order

    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "dag_backfill_sofascore_all_mens")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "scheduled-backfill-1")
    monkeypatch.delenv("SOFASCORE_RUN_ID", raising=False)
    policy = WorkloadBudgetPolicy("c" * 64, {MATCH_WORKLOAD_CLASS: _match_budget()})
    ids = tuple(str(value) for value in range(1, 31))
    plan = build_partitioned_plan(
        policy,
        dag_id="dag_backfill_sofascore_all_mens",
        run_id="scheduled-backfill-1::targets",
        partitions=[PartitionWorkload("SS-17", "2526", 17, pending_match_ids=ids)],
        control_token=TOKEN,
    )
    path = write_plan(tmp_path / "targets.json", plan)

    _loaded, plain = _load_runtime_workload_plan(
        str(path), entity=ENTITY_MATCH_CAPTURE, league="SS-17", season=2526,
        offline_replay=False,
    )
    write_target_order(path, ("30", "1"))
    _loaded, ordered = _load_runtime_workload_plan(
        str(path), entity=ENTITY_MATCH_CAPTURE, league="SS-17", season=2526,
        offline_replay=False,
    )

    assert [item.batch_index for item in plain] == [0, 1]
    assert "30" in target_ids(ordered[0])
    assert [item.batch_index for item in ordered] == [1, 0]
    assert set(ordered) == set(plain)


def _history_environment(monkeypatch, ids):
    import json
    monkeypatch.setenv("SOFASCORE_PROXY_CONTROL_TOKEN", TOKEN)
    monkeypatch.setenv("SOFASCORE_HISTORY_MATCH_IDS_JSON", json.dumps(ids))
    monkeypatch.setenv("SOFASCORE_HISTORY_PHASE", "matches")
    monkeypatch.setenv("SOFASCORE_HISTORY_SEASON_EVIDENCE", "bronze")


def test_history_runtime_uses_only_signed_ids_absent_from_schedule(tmp_path, monkeypatch):
    import json
    from dags.scripts import run_sofascore_scraper as runner
    from scrapers.sofascore.workload_runtime import load_plan
    from scrapers.sofascore.manifest import InMemoryManifestStore

    ids = [str(value) for value in range(1, 28)]
    _history_environment(monkeypatch, ids)
    plan = load_plan(_target_plan(tmp_path))
    monkeypatch.setattr(runner, "_source_context", lambda *a: (17, 76986))
    monkeypatch.setattr(runner, "_resolve_match_ids_from_bronze", lambda *a, **k: pytest.fail("schedule must not determine exact universe"))
    seen = []
    def terminal_resume(store, specs):
        seen.extend(specs)
        return {}
    monkeypatch.setattr("scrapers.sofascore.pipeline.endpoint_resume_plan", terminal_resume)
    runtime = SimpleNamespace(manifest_store=InMemoryManifestStore())
    output = tmp_path / "report.json"
    assert runner._run_match_capture(
        ["ENG-Premier League"], 2025, None, str(output), capture_runtime=runtime,
        workload_plan=plan, workload_allocations=plan.allocations,
    ) == 0
    assert {spec.key.target_id for spec in seen} == set(ids)
    assert {spec.key.source_season_id for spec in seen} == {"76986"}
    assert {spec.key.source_tournament_id for spec in seen} == {"17"}
    assert {spec.key.freshness_key for spec in seen} == {"final"}
    assert json.loads(output.read_text())["matches_skipped_existing"] == len(ids)


@pytest.mark.parametrize("changed_ids", [["900"], ["1"], [str(value) for value in range(1, 29)]])
def test_history_runtime_rejects_changed_or_expanded_unsigned_ids(tmp_path, monkeypatch, changed_ids):
    from dags.scripts import run_sofascore_scraper as runner
    from scrapers.sofascore.workload_runtime import load_plan
    _history_environment(monkeypatch, changed_ids)
    plan = load_plan(_target_plan(tmp_path))
    with pytest.raises(RuntimeError, match="differ from the signed"):
        runner._signed_history_match_ids(plan, plan.allocations)


@pytest.mark.parametrize("overrides", [{"force_replace": True}, {"offline_replay": True}])
def test_history_runtime_rejects_overrides(tmp_path, monkeypatch, overrides):
    from dags.scripts import run_sofascore_scraper as runner
    from scrapers.sofascore.workload_runtime import load_plan
    _history_environment(monkeypatch, [str(value) for value in range(1, 28)])
    plan = load_plan(_target_plan(tmp_path))
    with pytest.raises(RuntimeError, match="without force/offline overrides"):
        runner._signed_history_match_ids(plan, plan.allocations, **overrides)


def test_signed_history_capture_replays_raw_before_planned_missing_endpoints(tmp_path, monkeypatch):
    from pyarrow import fs
    from scrapers.sofascore.capture_engine import EndpointSpec, SofaScoreCaptureEngine
    from scrapers.sofascore.live_capture import capture_live_specs
    from scrapers.sofascore.manifest import InMemoryManifestStore, ManifestKey
    from scrapers.sofascore.pipeline import CaptureRuntime, DeferredCaptureSink
    from scrapers.sofascore.raw_store import RawPayloadStore
    from scrapers.sofascore.workload_runtime import load_plan
    from dags.scripts import run_sofascore_scraper as runner

    ids = [str(value) for value in range(1, 28)]
    _history_environment(monkeypatch, ids)
    plan = load_plan(_target_plan(tmp_path))
    assert runner._signed_history_match_ids(plan, plan.allocations) == tuple(ids)
    raw = RawPayloadStore(fs.LocalFileSystem(), str(tmp_path / "raw"))
    manifests = InMemoryManifestStore()
    allocation = plan.allocations[0]
    engine = SofaScoreCaptureEngine(
        raw_store=raw, manifest_store=manifests, transport=SimpleNamespace(),
        sink=DeferredCaptureSink(), run_id=plan.run_id, task_id=allocation.task_id,
        budget=SimpleNamespace(policy=SimpleNamespace(
            artifact_id=plan.artifact_id, hard_run_bytes=allocation.budget_bytes,
        )),
    )
    runtime = CaptureRuntime(engine, manifests, raw)
    def spec(target):
        return EndpointSpec(
            key=ManifestKey("17", "76986", "event", target, "event", "final"),
            url=f"https://www.sofascore.com/api/v1/event/{target}",
            schema_validator=lambda payload: isinstance(payload.get("items"), list),
            empty_predicate=lambda payload: payload["items"] == [],
            parsers={"items": lambda payload: payload["items"]}, paid_proxy=True,
        )
    saved, missing = spec("1"), spec("2")
    raw.store_bytes(saved.raw_target, b'{"items":[{"id":1}]}', request_url=saved.url,
                    http_status=200, response_headers={"content-type": "application/json"})
    class NoSource(RuntimeError):
        pass
    def no_source(*args, **kwargs):
        # The missing endpoint reached its signed allocation only after the
        # saved nonterminal payload was replayed. No actual source is opened.
        assert manifests.get(saved.key).error_type == "DeferredMaterialization"
        assert engine.metrics.snapshot()["replay_hits"] == 1
        assert manifests.get(missing.key) is None
        raise NoSource("planned missing endpoint reached transport boundary")
    with pytest.raises(NoSource):
        capture_live_specs(
            runtime, [missing, saved], canonical_url="https://www.sofascore.com/tournament/17",
            scope="ENG-Premier League:2526", entity="match_capture", workload_plan=plan,
            allocation_id=allocation.allocation_id, transport_factory=no_source,
        )
    assert engine.metrics.snapshot()["source_request_count"] == 0
