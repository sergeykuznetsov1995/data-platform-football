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
    MATCH_WORKLOAD_CLASS,
    PLAYER_WORKLOAD_CLASS,
    WorkloadBudgetPolicy,
    WorkloadClassBudget,
)
from scrapers.sofascore.workload_runtime import (
    PartitionWorkload,
    build_partitioned_plan,
    target_ids,
    write_plan,
)


TOKEN = "runner-workload-control-token-at-least-32-bytes"


def _target_plan(tmp_path):
    policy = WorkloadBudgetPolicy(
        "c" * 64,
        {
            MATCH_WORKLOAD_CLASS: WorkloadClassBudget(
                MATCH_WORKLOAD_CLASS,
                "match",
                25,
                100,
                ("event",),
                20,
                5,
                source_tournament_id="17",
            ),
            PLAYER_WORKLOAD_CLASS: WorkloadClassBudget(
                PLAYER_WORKLOAD_CLASS,
                "player",
                50,
                200,
                ("player_profile",),
                20,
                5,
                source_tournament_id="17",
            ),
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
            MATCH_WORKLOAD_CLASS: WorkloadClassBudget(
                MATCH_WORKLOAD_CLASS,
                "match",
                25,
                100,
                ("event",),
                20,
                5,
                source_tournament_id="17",
            ),
            PLAYER_WORKLOAD_CLASS: WorkloadClassBudget(
                PLAYER_WORKLOAD_CLASS,
                "player",
                50,
                200,
                ("player_profile",),
                20,
                5,
                source_tournament_id="17",
            ),
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
            {
                MATCH_WORKLOAD_CLASS: WorkloadClassBudget(
                    MATCH_WORKLOAD_CLASS,
                    "match",
                    25,
                    100,
                    ("event",),
                    20,
                    5,
                    source_tournament_id="17",
                ),
            },
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
