"""Airflow topology/config contracts for ESPN Native Bronze v2."""

from __future__ import annotations

import importlib
from contextlib import nullcontext
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[3]


def _reload(module_name: str):
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    sys.modules.pop(module_name, None)
    sys.modules.pop(f"dags.{module_name}", None)
    return importlib.import_module(module_name)


def _tasks():
    from airflow.operators.python import PythonOperator

    return {task.task_id: task for task in PythonOperator._instances}


def _daily_parent_context():
    logical_date = datetime(2026, 7, 31, 14, tzinfo=timezone.utc)
    run_id = "scheduled__2026-07-31T14:00:00+00:00"
    child_run_id = f"espn_daily__dag_trigger_espn_daily__{run_id}"
    parent = {
        "schema": "espn-daily-parent-v2",
        "owner_profile": "espn-isolated-v1",
        "parent_dag_id": "dag_trigger_espn_daily",
        "parent_task_id": "trigger_espn_ingest",
        "parent_run_id": run_id,
        "parent_run_type": "scheduled",
        "logical_date": logical_date.isoformat(),
        "data_interval_start": logical_date.isoformat(),
        "data_interval_end": (logical_date + timedelta(days=1)).isoformat(),
        "child_dag_id": "dag_ingest_espn",
        "child_run_id": child_run_id,
    }
    trigger = SimpleNamespace(
        dag_id="dag_trigger_espn_daily",
        task_id="trigger_espn_ingest",
        state="running",
        run_id=run_id,
    )
    owner = SimpleNamespace(
        dag_id="dag_trigger_espn_daily",
        run_id=run_id,
        run_type="scheduled",
        state="running",
        logical_date=logical_date,
        data_interval_start=logical_date,
        data_interval_end=logical_date + timedelta(days=1),
        get_task_instance=lambda **_kwargs: trigger,
    )
    context = {
        "dag": SimpleNamespace(dag_id="dag_ingest_espn"),
        "dag_run": SimpleNamespace(
            dag_id="dag_ingest_espn",
            run_id=child_run_id,
            conf={"espn_parent": parent},
        ),
        "logical_date": logical_date,
        "run_id": child_run_id,
    }
    return context, owner


def _generated_registry(count: int):
    from scrapers.espn.models import (
        AgeClass,
        CapabilityState,
        Competition,
        Edition,
        EntityCapabilities,
        Gender,
    )
    from scrapers.espn.registry import Registry

    capabilities = EntityCapabilities(
        schedule=CapabilityState.PROVEN,
        lineup=CapabilityState.PARTIAL,
        matchsheet=CapabilityState.PARTIAL,
    )
    competitions = tuple(
        Competition(
            espn_id=10_000 + index,
            slug=f"male-{index:03d}",
            name=f"Male Competition {index:03d}",
            gender=Gender.MALE,
            age_class=AgeClass.UNKNOWN,
            enabled=True,
            editions=(
                Edition(
                    source_season_year=2026,
                    display_name=f"2026 Male Competition {index:03d}",
                    start_date=date(2026, 1, 1),
                    end_date=date(2026, 12, 31),
                    current=True,
                    capabilities=capabilities,
                ),
            ),
            gender_evidence=("core-detail.gender=MALE",),
        )
        for index in range(count)
    )
    return Registry(
        schema_version=1,
        registry_version=f"generated-{count}",
        as_of=date(2026, 8, 2),
        competitions=competitions,
    )


def _scope_ids(registry):
    return tuple(
        sorted(
            competition.scope_id(competition.current_edition)
            for competition in registry.promoted
        )
    )


def _head(scope_id: str):
    return SimpleNamespace(scope_id=scope_id)


def test_daily_has_one_isolated_owner_and_real_two_wave_chain():
    module = _reload("dag_ingest_espn")
    tasks = _tasks()

    assert module.dag.schedule is None
    assert module.dag._dag_kwargs["catchup"] is False
    assert module.DAG_MODE == "daily"
    assert tasks["fetch_scoreboard_batches"].is_mapped is True
    assert tasks["fetch_summary_batches"].is_mapped is True
    assert tasks["offline_parse"].is_mapped is True
    assert tasks["staging_dq"].is_mapped is True
    assert tasks["publish_scopes"].is_mapped is True
    assert tasks["published_dq"].is_mapped is True
    assert tasks["reduce_raw_manifests"]._init_kwargs["trigger_rule"] == "none_failed"
    for task_id in (
        "acquire_scope_leases",
        "build_signed_scope_plans",
        "plan_summary_batches",
        "persist_run_manifests",
    ):
        assert tasks[task_id]._init_kwargs["multiple_outputs"] is True
    assert tasks["persist_run_manifests"]._init_kwargs["trigger_rule"] == "none_failed"

    expected_chain = (
        ("validate_registry_and_admission", "acquire_scope_leases"),
        ("acquire_scope_leases", "build_signed_scope_plans"),
        ("build_signed_scope_plans", "select_network_scope_bindings"),
        ("select_network_scope_bindings", "fetch_scoreboard_batches"),
        ("fetch_scoreboard_batches", "plan_summary_batches"),
        ("plan_summary_batches", "select_summary_batches"),
        ("select_summary_batches", "fetch_summary_batches"),
        ("reduce_raw_manifests", "offline_parse"),
        ("offline_parse", "staging_dq"),
        ("staging_dq", "publish_scopes"),
        ("publish_scopes", "persist_run_manifests"),
        ("persist_run_manifests", "select_publications"),
        ("select_publications", "published_dq"),
        ("published_dq", "terminal_verdict"),
        ("terminal_verdict", "record_health_metrics"),
        ("terminal_verdict", "release_scope_leases"),
        ("record_health_metrics", "propagate_terminal_failure"),
        ("release_scope_leases", "propagate_terminal_failure"),
    )
    for upstream, downstream in expected_chain:
        assert upstream in tasks[downstream].upstream_task_ids


def test_only_network_waves_use_one_slot_http_pool():
    _reload("dag_ingest_espn")
    tasks = _tasks()
    pooled = {
        task_id: task._init_kwargs.get("pool")
        for task_id, task in tasks.items()
        if task._init_kwargs.get("pool") is not None
    }

    assert pooled == {
        "fetch_scoreboard_batches": "espn_http_pool",
        "fetch_summary_batches": "espn_http_pool",
    }
    for task_id in pooled:
        assert tasks[task_id]._init_kwargs["pool_slots"] == 1


def test_ingest_timeout_lease_and_mapping_bounds_cover_bounded_onboarding():
    from dags.utils import espn_native_tasks

    module = _reload("dag_ingest_espn")
    tasks = _tasks()

    assert module.dag._dag_kwargs["dagrun_timeout"] == timedelta(hours=11)
    assert espn_native_tasks.LEASE_TTL == timedelta(hours=12)
    assert espn_native_tasks.MAX_INGEST_SCOPE_MAP_ITEMS == 300
    assert espn_native_tasks.MAX_SUMMARY_BATCH_MAP_ITEMS == 1024
    assert tasks["select_network_scope_bindings"]._init_kwargs["op_kwargs"][
        "max_items"
    ] == 300
    assert tasks["select_summary_batches"]._init_kwargs["op_kwargs"][
        "max_items"
    ] == 1024


def test_terminal_health_release_and_propagator_cannot_mask_failure():
    _reload("dag_ingest_espn")
    tasks = _tasks()

    assert tasks["terminal_verdict"]._init_kwargs["trigger_rule"] == "all_done"
    assert tasks["record_health_metrics"]._init_kwargs["trigger_rule"] == "all_done"
    assert tasks["release_scope_leases"]._init_kwargs["trigger_rule"] == "all_done"
    assert (
        tasks["propagate_terminal_failure"]._init_kwargs["trigger_rule"] == "all_done"
    )
    assert tasks["propagate_terminal_failure"]._init_kwargs["op_kwargs"][
        "cleanup_task_ids"
    ] == ["record_health_metrics", "release_scope_leases"]
    producer_task_ids = set(
        tasks["terminal_verdict"]._init_kwargs["op_kwargs"]["producer_task_ids"]
    )
    assert producer_task_ids == {
        "validate_registry_and_admission",
        "acquire_scope_leases",
        "build_signed_scope_plans",
        "select_network_scope_bindings",
        "fetch_scoreboard_batches",
        "plan_summary_batches",
        "select_summary_batches",
        "fetch_summary_batches",
        "reduce_raw_manifests",
        "offline_parse",
        "staging_dq",
        "publish_scopes",
        "persist_run_manifests",
        "select_publications",
        "published_dq",
    }
    assert tasks["terminal_verdict"].upstream_task_ids == producer_task_ids
    assert set(tasks["terminal_verdict"]._init_kwargs["op_kwargs"]) == {
        "producer_task_ids"
    }
    assert set(tasks["release_scope_leases"]._init_kwargs["op_kwargs"]) == {
        "lease_acquisition_ref"
    }


@pytest.mark.parametrize(
    ("module_name", "mode"),
    [
        ("dag_repair_espn", "repair"),
        ("dag_backfill_espn", "backfill"),
    ],
)
def test_manual_network_modes_reuse_same_contract_without_schedule(module_name, mode):
    module = _reload(module_name)
    tasks = _tasks()

    assert module.dag.schedule is None
    assert module.DAG_MODE == mode
    assert tasks["fetch_scoreboard_batches"]._init_kwargs["pool"] == "espn_http_pool"
    assert tasks["fetch_summary_batches"]._init_kwargs["pool"] == "espn_http_pool"
    assert "publish_scopes" in tasks
    assert tasks["terminal_verdict"].upstream_task_ids == set(
        tasks["terminal_verdict"]._init_kwargs["op_kwargs"]["producer_task_ids"]
    )


def test_replay_dag_has_no_network_operator_at_all():
    module = _reload("dag_replay_espn")
    tasks = _tasks()

    assert module.dag.schedule is None
    assert module.DAG_MODE == "replay"
    assert "fetch_scoreboard_batches" not in tasks
    assert "fetch_summary_batches" not in tasks
    assert "bind_replay_raw_manifests" in tasks
    assert all(task._init_kwargs.get("pool") is None for task in tasks.values())
    assert tasks["terminal_verdict"].upstream_task_ids == set(
        tasks["terminal_verdict"]._init_kwargs["op_kwargs"]["producer_task_ids"]
    )


def test_weekly_discovery_publishes_generated_registry_and_monitor_is_network_free():
    discovery = _reload("dag_discover_espn_registry")
    discovery_tasks = _tasks()
    assert discovery.dag.schedule == "0 7 * * 1"
    assert discovery_tasks["fetch_discovery_catalog"]._init_kwargs["pool"] == (
        "espn_http_pool"
    )
    assert (
        discovery_tasks["fetch_discovery_catalog"]._init_kwargs["multiple_outputs"]
        is True
    )
    assert "publish_discovered_male_registry" in discovery_tasks
    assert (
        discovery_tasks["plan_discovery_detail_batches"]._init_kwargs.get("pool")
        is None
    )
    assert (
        discovery_tasks["select_discovery_detail_batches"]._init_kwargs.get("pool")
        is None
    )
    assert discovery_tasks["fetch_discovery_detail_batches"].is_mapped is True
    assert discovery_tasks["fetch_discovery_detail_batches"]._init_kwargs["pool"] == (
        "espn_http_pool"
    )
    assert (
        discovery_tasks["fetch_discovery_detail_batches"]._init_kwargs["pool_slots"]
        == 1
    )
    assert (
        "select_discovery_detail_batches"
        in discovery_tasks["fetch_discovery_detail_batches"].upstream_task_ids
    )
    assert (
        "fetch_discovery_detail_batches"
        in discovery_tasks["publish_discovered_male_registry"].upstream_task_ids
    )
    assert (
        discovery_tasks["publish_discovered_male_registry"]._init_kwargs[
            "trigger_rule"
        ]
        == "none_failed"
    )

    monitor = _reload("dag_monitor_espn")
    monitor_tasks = _tasks()
    assert monitor.dag.schedule == "0 */6 * * *"
    assert all(task._init_kwargs.get("pool") is None for task in monitor_tasks.values())
    assert "check_36h_freshness_and_alerts" in monitor_tasks


def test_core_catalog_refs_normalize_to_all_slugs():
    from dags.utils.espn_native_tasks import _normalize_discovery_dropdown

    catalog = {
        "count": 2,
        "pageCount": 1,
        "items": [
            {
                "$ref": "http://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1?lang=en"
            },
            {
                "$ref": "http://sports.core.api.espn.com/v2/sports/soccer/leagues/uefa.champions?lang=en"
            },
        ],
    }

    normalized = _normalize_discovery_dropdown(catalog)

    assert [row["slug"] for row in normalized["leagues"]] == [
        "eng.1",
        "uefa.champions",
    ]


@pytest.mark.parametrize(
    "catalog",
    [
        {
            "count": 2,
            "pageCount": 1,
            "items": [
                {
                    "$ref": "https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1"
                }
            ],
        },
        {
            "count": 1,
            "pageCount": 2,
            "items": [
                {
                    "$ref": "https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1"
                }
            ],
        },
        {
            "count": 1,
            "pageCount": 1,
            "items": [{"$ref": "https://example.com/v2/sports/soccer/leagues/eng.1"}],
        },
        {
            "count": 1,
            "pageCount": 1,
            "items": [
                {
                    "$ref": "https://sports.core.api.espn.com:bad/v2/sports/soccer/leagues/eng.1"
                }
            ],
        },
        {
            "count": 1,
            "pageCount": 1,
            "items": [
                {
                    "$ref": "https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1"
                },
                {
                    "$ref": "https://sports.core.api.espn.com/v2/sports/soccer/leagues/eng.1?lang=en"
                },
            ],
        },
    ],
)
def test_core_catalog_rejects_partial_or_malformed_coverage(catalog):
    from dags.utils import espn_native_tasks

    with pytest.raises(espn_native_tasks.OperationsError):
        espn_native_tasks._normalize_discovery_dropdown(catalog)


def test_core_catalog_default_fetches_complete_single_page(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.transport_contracts import EndpointType

    calls = []
    monkeypatch.delenv("ESPN_DISCOVERY_CATALOG_URL", raising=False)
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_identity",
        lambda _context: (
            "dag_discover_espn_registry",
            "run-1",
            datetime(2026, 8, 2, tzinfo=timezone.utc),
        ),
    )
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(espn_native_tasks, "_raw_store_uri", lambda: "s3://raw")
    monkeypatch.setattr(espn_native_tasks, "_optional_payload", lambda *_a, **_k: None)
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore,
        "from_uri",
        lambda _uri: SimpleNamespace(),
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: SimpleNamespace(migrate=lambda: None)),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_http_client",
        lambda *_a, **_k: SimpleNamespace(
            fetch_json=lambda *args, **kwargs: (
                calls.append((args, kwargs))
                or SimpleNamespace(
                    raw_uri="s3://raw/catalog.json",
                    content_hash="a" * 64,
                    direct_bytes=1,
                    proxy_bytes=0,
                )
            )
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, _payload: {"uri": uri, "sha256": "b" * 64},
    )

    espn_native_tasks.fetch_discovery_catalog()

    assert calls == [
        (
            (
                "https://sports.core.api.espn.com/v2/sports/soccer/leagues?limit=500&lang=en&region=us",
                EndpointType.CATALOG,
            ),
            {"force_refresh": True},
        )
    ]


def test_every_mapped_input_is_a_selector_or_producer_return_value():
    _reload("dag_ingest_espn")
    tasks = _tasks()

    expected_producers = {
        "fetch_scoreboard_batches": "select_network_scope_bindings",
        "fetch_summary_batches": "select_summary_batches",
        "offline_parse": "reduce_raw_manifests",
        "staging_dq": "offline_parse",
        "publish_scopes": "staging_dq",
        "published_dq": "select_publications",
    }
    for task_id, producer_id in expected_producers.items():
        mapped = tasks[task_id]._expand_kwargs["op_kwargs"]
        assert mapped.operator.task_id == producer_id
        assert mapped.key is None

    _reload("dag_discover_espn_registry")
    discovery_tasks = _tasks()
    mapped = discovery_tasks["fetch_discovery_detail_batches"]._expand_kwargs[
        "op_kwargs"
    ]
    assert mapped.operator.task_id == "select_discovery_detail_batches"
    assert mapped.key is None


def test_selectors_read_whole_return_values_and_are_not_multiple_outputs():
    from dags.utils import espn_native_tasks

    _reload("dag_ingest_espn")
    tasks = _tasks()
    expected = {
        "select_network_scope_bindings": (
            "build_signed_scope_plans",
            "network_scope_binding_refs",
            "scope_binding_ref",
            espn_native_tasks.MAX_INGEST_SCOPE_MAP_ITEMS,
        ),
        "select_summary_batches": (
            "plan_summary_batches",
            "summary_batch_refs",
            "summary_batch_ref",
            espn_native_tasks.MAX_SUMMARY_BATCH_MAP_ITEMS,
        ),
        "select_publications": (
            "persist_run_manifests",
            "publication_refs",
            "publication_ref",
            espn_native_tasks.MAX_INGEST_SCOPE_MAP_ITEMS,
        ),
    }
    for task_id, contract in expected.items():
        selector = tasks[task_id]
        source = selector._init_kwargs["op_kwargs"]["source"]
        actual = (
            source.operator.task_id,
            selector._init_kwargs["op_kwargs"]["source_key"],
            selector._init_kwargs["op_kwargs"]["descriptor_key"],
            selector._init_kwargs["op_kwargs"]["max_items"],
        )
        assert actual == contract
        assert source.key is None
        assert selector._init_kwargs.get("multiple_outputs") is not True

    _reload("dag_replay_espn")
    replay_tasks = _tasks()
    producers = set(
        replay_tasks["terminal_verdict"]._init_kwargs["op_kwargs"]["producer_task_ids"]
    )
    assert "select_publications" in producers
    assert "select_network_scope_bindings" not in producers
    assert "select_summary_batches" not in producers


def test_summary_planner_receives_the_exact_plan_index_guard():
    _reload("dag_ingest_espn")
    tasks = _tasks()

    planner_kwargs = tasks["plan_summary_batches"]._init_kwargs["op_kwargs"]
    plan_index_ref = planner_kwargs["plan_index_ref"]
    assert plan_index_ref.operator.task_id == "build_signed_scope_plans"
    assert plan_index_ref.key == "plan_index_ref"


def test_mapping_selector_accepts_only_bounded_uri_sha_descriptors():
    from dags.utils import espn_native_tasks

    row = {
        "scope_binding_ref": {
            "uri": "s3://artifacts/scope-binding.json",
            "sha256": "a" * 64,
        }
    }
    source = {"network_scope_binding_refs": [row]}

    assert espn_native_tasks.select_mapping_descriptors(
        source=source,
        source_key="network_scope_binding_refs",
        descriptor_key="scope_binding_ref",
        max_items=1,
    ) == [row]
    assert (
        espn_native_tasks.select_mapping_descriptors(
            source={"network_scope_binding_refs": []},
            source_key="network_scope_binding_refs",
            descriptor_key="scope_binding_ref",
            max_items=1,
        )
        == []
    )


def test_summary_reducer_accepts_airflow_none_only_for_signed_zero_map(monkeypatch):
    from dags.utils import espn_native_tasks

    summary_index_ref = {"uri": "file:///summary-index.json", "sha256": "a" * 64}
    summary_index = {
        "kind": "espn-summary-wave-index-v1",
        "schema_version": 1,
        "expected_map_count": 0,
        "expected_scoreboard_map_count": 0,
        "scopes": [
            {
                "scope_id": "700:2024",
                "scope_binding_ref": {
                    "uri": "file:///scope-binding.json",
                    "sha256": "b" * 64,
                },
                "scoreboard_phase_ref": None,
                "summary_batch_refs": [],
                "budget_exhausted": False,
            }
        ],
    }
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, *, kind=None: (
            summary_index
            if ref == summary_index_ref and kind == "espn-summary-wave-index-v1"
            else pytest.fail(f"unexpected artifact read: {ref!r}, {kind!r}")
        ),
    )
    binding_ref = summary_index["scopes"][0]["scope_binding_ref"]
    loaded = SimpleNamespace(
        bindings={"700:2024": SimpleNamespace(active=False, prior=object())}
    )
    monkeypatch.setattr(
        espn_native_tasks, "_heartbeat_scope_binding", lambda _ref: None
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda ref: (
            (
                None,
                {
                    "scope_root": "file:///run/scopes/700-2024",
                    "raw_manifest_uri": "file:///raw-manifest.json",
                },
                loaded,
                SimpleNamespace(scope_id="700:2024"),
                None,
            )
            if ref == binding_ref
            else pytest.fail(f"unexpected binding: {ref!r}")
        ),
    )
    monkeypatch.setattr(espn_native_tasks.runner, "_manifest_base", lambda *_: {})
    monkeypatch.setattr(espn_native_tasks.runner, "_seal_manifest", lambda value: value)
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, _payload, **_kwargs: {"uri": uri, "sha256": "c" * 64},
    )

    reduced = espn_native_tasks.reduce_raw_manifest_wave(
        summary_index_ref=summary_index_ref,
        summary_phase_refs=None,
    )
    assert len(reduced) == 1
    assert set(reduced[0]) == {"raw_phase_ref"}


@pytest.mark.parametrize(
    ("expected_map_count", "mapped_output"),
    [
        (1, None),
        (1, []),
        (0, [{"summary_phase_ref": {"uri": "file:///extra", "sha256": "b" * 64}}]),
        (1, [None]),
        (False, None),
        (-1, None),
    ],
)
def test_summary_reducer_rejects_missing_extra_or_malformed_mapped_output(
    monkeypatch, expected_map_count, mapped_output
):
    from dags.utils import espn_native_tasks

    summary_index_ref = {"uri": "file:///summary-index.json", "sha256": "a" * 64}
    summary_index = {
        "kind": "espn-summary-wave-index-v1",
        "schema_version": 1,
        "expected_map_count": expected_map_count,
        "expected_scoreboard_map_count": 0,
        "scopes": [
            {
                "scope_id": "700:2026",
                "summary_batch_refs": (
                    [{"uri": "file:///batch", "sha256": "c" * 64}]
                    if type(expected_map_count) is int and expected_map_count == 1
                    else []
                ),
            }
        ],
    }
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, *, kind=None: (
            summary_index
            if ref == summary_index_ref and kind == "espn-summary-wave-index-v1"
            else pytest.fail(f"unexpected artifact read: {ref!r}, {kind!r}")
        ),
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="Summary"):
        espn_native_tasks.reduce_raw_manifest_wave(
            summary_index_ref=summary_index_ref,
            summary_phase_refs=mapped_output,
        )


def test_summary_planner_accepts_airflow_none_for_signed_zero_scoreboard_map(
    monkeypatch,
):
    from dags.utils import espn_native_tasks

    scope_id = "700:2024"
    plan_index_ref = {"uri": "file:///plan-index.json", "sha256": "a" * 64}
    scope_binding_ref = {"uri": "file:///scope-binding.json", "sha256": "b" * 64}
    plan_index = {
        "kind": "espn-plan-index-v1",
        "schema_version": 1,
        "scope_ids": [scope_id],
        "network_scope_ids": [],
        "expected_scoreboard_map_count": 0,
    }
    descriptor = {"scope_root": "file:///run/scopes/700-2024"}
    loaded = SimpleNamespace(
        bindings={scope_id: SimpleNamespace(active=False, prior=object())}
    )
    scope = SimpleNamespace(scope_id=scope_id)
    written = {}

    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, *, kind=None: (
            plan_index
            if ref == plan_index_ref and kind == "espn-plan-index-v1"
            else pytest.fail(f"unexpected artifact read: {ref!r}, {kind!r}")
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_heartbeat_scope_binding", lambda _ref: None
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda ref: (
            (
                None,
                descriptor,
                loaded,
                scope,
                None,
            )
            if ref == scope_binding_ref
            else pytest.fail(f"unexpected binding: {ref!r}")
        ),
    )

    def capture_write(uri, payload, **_kwargs):
        written[uri] = payload
        return {"uri": uri, "sha256": "c" * 64}

    monkeypatch.setattr(espn_native_tasks, "_write_payload", capture_write)

    result = espn_native_tasks.plan_summary_batch_wave(
        plan_index_ref=plan_index_ref,
        scoreboard_phase_refs=None,
        scope_binding_refs=[{"scope_binding_ref": scope_binding_ref}],
    )

    assert result["summary_batch_refs"] == []
    index = next(iter(written.values()))
    assert index["expected_scoreboard_map_count"] == 0
    assert index["expected_map_count"] == 0


def test_summary_planner_rejects_airflow_none_when_scoreboard_map_was_expected(
    monkeypatch,
):
    from dags.utils import espn_native_tasks

    plan_index_ref = {"uri": "file:///plan-index.json", "sha256": "a" * 64}
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, *, kind=None: (
            {
                "kind": "espn-plan-index-v1",
                "schema_version": 1,
                "scope_ids": ["700:2026"],
                "network_scope_ids": ["700:2026"],
                "expected_scoreboard_map_count": 1,
            }
            if ref == plan_index_ref and kind == "espn-plan-index-v1"
            else pytest.fail(f"unexpected artifact read: {ref!r}, {kind!r}")
        ),
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="scoreboard phase"):
        espn_native_tasks.plan_summary_batch_wave(
            plan_index_ref=plan_index_ref,
            scoreboard_phase_refs=None,
            scope_binding_refs=[],
        )


def test_publication_reducer_runs_after_zero_map_and_rejects_none(monkeypatch):
    from dags.utils import espn_native_tasks

    plan_index_ref = {"uri": "file:///plan-index.json", "sha256": "a" * 64}
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, *, kind=None: (
            {
                "kind": "espn-plan-index-v1",
                "scope_ids": ["700:2026"],
            }
            if ref == plan_index_ref and kind == "espn-plan-index-v1"
            else pytest.fail(f"unexpected artifact read: {ref!r}, {kind!r}")
        ),
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="publication"):
        espn_native_tasks.persist_run_manifests(
            plan_index_ref=plan_index_ref,
            publication_refs=None,
        )


def test_discovery_reducer_runs_after_zero_map_and_rejects_none(monkeypatch):
    from dags.utils import espn_native_tasks

    index_ref = {"uri": "file:///discovery-index.json", "sha256": "a" * 64}
    batch_ref = {"uri": "file:///batch.json", "sha256": "b" * 64}
    detail_index = {
        "kind": "espn-discovery-detail-index-v1",
        "competition_count": 1,
        "competition_cap": espn_native_tasks.MAX_DISCOVERY_COMPETITIONS,
        "detail_request_cap": espn_native_tasks.MAX_DISCOVERY_DETAIL_REQUESTS,
        "batch_ids": ["batch-1"],
        "batch_refs": [batch_ref],
    }
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_identity",
        lambda _context: (
            "dag_discover_espn_registry",
            "manual__zero-map",
            datetime(2026, 8, 1, tzinfo=timezone.utc),
        ),
    )
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "file:///tmp")
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, *, kind=None: (
            detail_index
            if ref == index_ref and kind == "espn-discovery-detail-index-v1"
            else pytest.fail(f"unexpected artifact read: {ref!r}, {kind!r}")
        ),
    )

    with pytest.raises(
        espn_native_tasks.OperationsError, match="discovery detail phase"
    ):
        espn_native_tasks.write_reviewable_discovery_diff(
            discovery_detail_index_ref=index_ref,
            discovery_detail_phase_refs=None,
        )


@pytest.mark.parametrize(
    "source",
    [
        [],
        {},
        {"rows": ()},
        {"rows": [None]},
        {"rows": [{}]},
        {"rows": [{"ref": {"uri": "s3://a", "sha256": "a" * 64}, "x": 1}]},
        {"rows": [{"ref": {"uri": "", "sha256": "a" * 64}}]},
        {
            "rows": [
                {
                    "ref": {
                        "uri": "s3://" + "a" * 4096,
                        "sha256": "a" * 64,
                    }
                }
            ]
        },
        {"rows": [{"ref": {"uri": "s3://a", "sha256": "A" * 64}}]},
        {"rows": [{"ref": {"uri": "s3://a", "sha256": "a" * 64, "x": 1}}]},
    ],
)
def test_mapping_selector_rejects_missing_or_wrong_shape(source):
    from dags.utils import espn_native_tasks

    with pytest.raises(espn_native_tasks.OperationsError):
        espn_native_tasks.select_mapping_descriptors(
            source=source,
            source_key="rows",
            descriptor_key="ref",
            max_items=2,
        )


def test_mapping_selector_rejects_unbounded_payload():
    from dags.utils import espn_native_tasks

    row = {"ref": {"uri": "s3://a", "sha256": "a" * 64}}
    with pytest.raises(espn_native_tasks.OperationsError, match="bound"):
        espn_native_tasks.select_mapping_descriptors(
            source={"rows": [row, row]},
            source_key="rows",
            descriptor_key="ref",
            max_items=1,
        )

    with pytest.raises(espn_native_tasks.OperationsError, match="static bound"):
        espn_native_tasks.select_mapping_descriptors(
            source={"rows": []},
            source_key="rows",
            descriptor_key="ref",
            max_items=espn_native_tasks.MAX_SUMMARY_BATCH_MAP_ITEMS + 1,
        )

    with pytest.raises(espn_native_tasks.OperationsError, match="duplicates"):
        espn_native_tasks.select_mapping_descriptors(
            source={"rows": [row, row]},
            source_key="rows",
            descriptor_key="ref",
            max_items=2,
        )


def test_production_compose_provisions_exactly_one_espn_http_slot():
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    command = compose.split("airflow-init:", 1)[1].split("airflow-scheduler:", 1)[0]
    scheduler = compose.split("airflow-scheduler:", 1)[1].split(
        "airflow-triggerer:", 1
    )[0]

    assert command.count("airflow pools set 'espn_http_pool' 1") == 1
    assert scheduler.count("target: /opt/airflow/configs/espn") == 1
    espn_mount = scheduler.split("source: ./configs/espn", 1)[1].split(
        "- type: bind", 1
    )[0]
    assert "read_only: true" in espn_mount
    assert "create_host_path: false" in espn_mount


def test_shared_master_envelope_is_legacy_and_cannot_satisfy_daily_v2_admission():
    _reload("dag_master_pipeline")
    tasks = _tasks()
    trigger = tasks["ingestion_triggers.trigger_espn"]
    required_gate = tasks["validate_required_sources"]
    conf = trigger._init_kwargs["conf"]["espn_parent"]

    assert trigger.upstream_task_ids == set()
    assert trigger.task_id not in required_gate.upstream_task_ids
    assert trigger._init_kwargs["allowed_states"] == ["success", "failed"]
    assert trigger._init_kwargs["failed_states"] == []
    assert trigger._init_kwargs["reset_dag_run"] is False
    assert trigger._init_kwargs["trigger_run_id"].startswith("espn_daily__")
    assert trigger._init_kwargs["logical_date"] == "{{ logical_date.isoformat() }}"
    assert conf == {
        "schema": "espn-master-parent-v1",
        "parent_dag_id": "dag_master_pipeline",
        "parent_run_id": "{{ run_id }}",
        "logical_date": "{{ logical_date.isoformat() }}",
        "data_interval_start": "{{ data_interval_start.isoformat() }}",
        "data_interval_end": "{{ data_interval_end.isoformat() }}",
        "child_run_id": trigger._init_kwargs["trigger_run_id"],
    }


def test_daily_admission_binds_real_exact_isolated_owner_run(monkeypatch):
    from dags.utils import espn_native_tasks

    context, owner = _daily_parent_context()
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda *_args: owner)

    parent = espn_native_tasks._daily_parent(context)

    assert parent["parent_run_id"] == owner.run_id
    assert parent["owner_profile"] == "espn-isolated-v1"


def test_daily_admission_pins_exact_all_male_registry_and_first_bootstrap_cohort():
    from dags.utils.espn_native_tasks import _bounded_daily_scopes

    registry = _generated_registry(181)
    heads = {scope: _head(scope) for scope in _scope_ids(registry)[:9]}

    target, selected, bootstrap = _bounded_daily_scopes(registry, heads)

    assert len(target) == 181
    assert len(selected) == 19
    assert bootstrap == target[9:19]
    assert set(selected) == set(target[:19])


def test_daily_bootstrap_never_skips_the_first_failed_scope():
    from dags.utils.espn_native_tasks import _bounded_daily_scopes

    registry = _generated_registry(181)

    target, selected, bootstrap = _bounded_daily_scopes(registry, {})

    assert selected == bootstrap == target[:10]


def test_daily_admission_v2_persists_exact_discovery_and_coverage(monkeypatch):
    from dags.utils import espn_native_tasks

    now = datetime(2026, 8, 2, 12, tzinfo=timezone.utc)
    registry = _generated_registry(181)
    target = _scope_ids(registry)
    state_ref = {
        "uri": "s3://artifacts/discovery/latest-state.json",
        "sha256": "a" * 64,
    }
    registry_ref = {
        "uri": "s3://artifacts/discovery/run/male-registry.json",
        "sha256": "b" * 64,
    }
    candidate_ref = {
        "uri": "s3://artifacts/discovery/run/candidate.json",
        "sha256": "c" * 64,
    }
    discovery = {
        "discovery_state_ref": state_ref,
        "male_registry_ref": registry_ref,
        "candidate_ref": candidate_ref,
        "selection_policy": "explicit-core-gender-MALE-v1",
        "male_scope_count": 181,
    }
    writes = []

    class Store:
        def migrate(self):
            pass

        def current_time(self):
            return now

        def read_scope_heads(self, scope_ids):
            assert tuple(scope_ids) == target
            return {scope: _head(scope) for scope in target[:9]}

    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: Store()),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_load_discovered_registry",
        lambda *, now: (registry, discovery),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_identity",
        lambda _context: ("dag_ingest_espn", "run-1", now),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_daily_parent", lambda _context: {"schema": "owner"}
    )
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(espn_native_tasks, "_raw_store_uri", lambda: "s3://raw")
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **kwargs: (
            writes.append((uri, payload, kwargs))
            or {"uri": uri, "sha256": "d" * 64}
        ),
    )

    espn_native_tasks.validate_registry_and_admission(mode="daily", params={})

    assert len(writes) == 1
    admission_uri, admission, kwargs = writes[0]
    assert admission_uri.endswith("/admission.json")
    assert kwargs == {"immutable": True}
    assert admission["kind"] == "espn-airflow-admission-v2"
    assert admission["schema_version"] == 2
    assert admission["registry_ref"] == registry_ref
    assert admission["registry_signature"] == registry.signature()
    assert admission["discovery_state_ref"] == state_ref
    assert admission["candidate_ref"] == candidate_ref
    assert admission["selection_policy"] == "explicit-core-gender-MALE-v1"
    assert admission["male_scope_count"] == 181
    assert admission["target_scope_ids"] == list(target)
    assert admission["scope_ids"] == list(target[:19])
    assert admission["bootstrap_scope_ids"] == list(target[9:19])


def test_manual_admission_keeps_explicit_generated_scope_without_reading_heads(
    monkeypatch,
):
    from dags.utils import espn_native_tasks

    now = datetime(2026, 8, 2, 12, tzinfo=timezone.utc)
    registry = _generated_registry(20)
    target = _scope_ids(registry)
    discovery = {
        "discovery_state_ref": {"uri": "state", "sha256": "a" * 64},
        "male_registry_ref": {"uri": "registry", "sha256": "b" * 64},
        "candidate_ref": {"uri": "candidate", "sha256": "c" * 64},
        "selection_policy": "explicit-core-gender-MALE-v1",
        "male_scope_count": 20,
    }
    writes = []

    class Store:
        def migrate(self):
            pass

        def current_time(self):
            return now

        def read_scope_heads(self, _scope_ids):
            pytest.fail("manual admission must not read current heads")

    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: Store()),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_load_discovered_registry",
        lambda *, now: (registry, discovery),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_identity",
        lambda _context: ("dag_repair_espn", "run-1", now),
    )
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(espn_native_tasks, "_raw_store_uri", lambda: "s3://raw")
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **_kwargs: (
            writes.append((uri, payload)) or {"uri": uri, "sha256": "d" * 64}
        ),
    )
    context = {
        "params": {"scopes": [target[7]]},
        "dag_run": SimpleNamespace(conf={}),
    }

    espn_native_tasks.validate_registry_and_admission(mode="repair", **context)

    admission = writes[0][1]
    assert admission["target_scope_ids"] == list(target)
    assert admission["scope_ids"] == [target[7]]
    assert admission["bootstrap_scope_ids"] == []

    context["params"] = {"scopes": ["999999:2026"]}
    with pytest.raises(espn_native_tasks.OperationsError, match="unpromoted"):
        espn_native_tasks.validate_registry_and_admission(mode="repair", **context)


def test_discovery_registry_rejects_future_state_and_enabled_non_male(monkeypatch):
    from dags.utils import espn_native_tasks
    from dataclasses import replace
    from scrapers.espn.models import Gender

    now = datetime(2026, 8, 2, 12, tzinfo=timezone.utc)
    state_ref = {
        "uri": "s3://artifacts/discovery/latest-state.json",
        "sha256": "a" * 64,
    }
    registry = _generated_registry(1)
    state = {
        "kind": "espn-discovery-state-v2",
        "observed_at": (now + timedelta(minutes=5, seconds=1)).isoformat(),
        "male_scope_count": 1,
    }
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(espn_native_tasks, "_ref_for_uri", lambda _uri: state_ref)
    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_a, **_k: state)
    monkeypatch.setattr(
        espn_native_tasks,
        "_load_discovery_state_v2",
        lambda _state: ({}, registry, {}),
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="future"):
        espn_native_tasks._load_discovered_registry(now=now)

    state["observed_at"] = now.isoformat()
    female = replace(registry.competitions[0], gender=Gender.FEMALE)
    non_male_registry = replace(registry, competitions=(female,))
    monkeypatch.setattr(
        espn_native_tasks,
        "_load_discovery_state_v2",
        lambda _state: ({}, non_male_registry, {}),
    )
    with pytest.raises(espn_native_tasks.OperationsError, match="non-MALE"):
        espn_native_tasks._load_discovered_registry(now=now)


@pytest.mark.parametrize("fault", ["missing", "stale", "hash", "count"])
def test_discovery_registry_fault_fails_before_leases(monkeypatch, fault):
    from dags.utils import espn_native_tasks

    now = datetime(2026, 8, 2, 12, tzinfo=timezone.utc)
    state_ref = {
        "uri": "s3://artifacts/discovery/latest-state.json",
        "sha256": "a" * 64,
    }
    state = {
        "kind": "espn-discovery-state-v2",
        "observed_at": (
            now - timedelta(days=8, seconds=1)
            if fault == "stale"
            else now
        ).isoformat(),
    }
    registry = _generated_registry(1)
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")

    if fault == "missing":
        monkeypatch.setattr(
            espn_native_tasks,
            "_ref_for_uri",
            lambda _uri: (_ for _ in ()).throw(
                FileNotFoundError("missing discovery state")
            ),
        )
    else:
        monkeypatch.setattr(espn_native_tasks, "_ref_for_uri", lambda _uri: state_ref)
        monkeypatch.setattr(
            espn_native_tasks,
            "_read_ref",
            lambda *_args, **_kwargs: (
                (_ for _ in ()).throw(
                    espn_native_tasks.OperationsError("artifact reference hash mismatch")
                )
                if fault == "hash"
                else state
            ),
        )
        monkeypatch.setattr(
            espn_native_tasks,
            "_load_discovery_state_v2",
            lambda _state: (
                (_ for _ in ()).throw(
                    espn_native_tasks.OperationsError(
                        "discovery state male registry count mismatch"
                    )
                )
                if fault == "count"
                else ({}, registry, {})
            ),
        )

    with pytest.raises(espn_native_tasks.OperationsError):
        espn_native_tasks._load_discovered_registry(now=now)


def test_daily_admission_rejects_forged_child_or_stale_interval(monkeypatch):
    from dags.utils import espn_native_tasks

    context, owner = _daily_parent_context()
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda *_args: owner)
    context["dag_run"].conf["espn_parent"]["child_run_id"] = "forged"
    with pytest.raises(espn_native_tasks.OperationsError, match="deterministic child"):
        espn_native_tasks._daily_parent(context)

    context, owner = _daily_parent_context()
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda *_args: owner)
    context["dag_run"].conf["espn_parent"]["data_interval_end"] = (
        context["logical_date"] + timedelta(hours=23)
    ).isoformat()
    with pytest.raises(espn_native_tasks.OperationsError, match="data_interval_end"):
        espn_native_tasks._daily_parent(context)


def test_daily_admission_rejects_inactive_owner_trigger(monkeypatch):
    from dags.utils import espn_native_tasks

    context, owner = _daily_parent_context()
    owner.get_task_instance = lambda **_kwargs: SimpleNamespace(
        dag_id="dag_trigger_espn_daily",
        task_id="trigger_espn_ingest",
        state="failed",
        run_id=owner.run_id,
    )
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda *_args: owner)

    with pytest.raises(espn_native_tasks.OperationsError, match="not active"):
        espn_native_tasks._daily_parent(context)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("owner_profile", "unknown-v9", "profile"),
        ("schema", "espn-master-parent-v1", "schema"),
        ("parent_dag_id", "dag_master_pipeline", "DAG"),
        ("parent_task_id", "ingestion_triggers.trigger_espn", "task"),
        ("parent_run_type", "manual", "run type"),
        ("child_dag_id", "dag_repair_espn", "child DAG"),
    ],
)
def test_daily_admission_rejects_unknown_profile_and_identity_drift(
    monkeypatch, field, value, message
):
    from dags.utils import espn_native_tasks

    context, owner = _daily_parent_context()
    context["dag_run"].conf["espn_parent"][field] = value
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda *_args: owner)

    with pytest.raises(espn_native_tasks.OperationsError, match=message):
        espn_native_tasks._daily_parent(context)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("run_type", "manual", "scheduled"),
        ("run_id", "scheduled__forged", "run ID"),
        ("dag_id", "dag_master_pipeline", "DAG"),
    ],
)
def test_daily_admission_rejects_metadata_owner_drift(
    monkeypatch, field, value, message
):
    from dags.utils import espn_native_tasks

    context, owner = _daily_parent_context()
    setattr(owner, field, value)
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda *_args: owner)

    with pytest.raises(espn_native_tasks.OperationsError, match=message):
        espn_native_tasks._daily_parent(context)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dag_id", "dag_master_pipeline"),
        ("task_id", "ingestion_triggers.trigger_espn"),
        ("run_id", "scheduled__forged"),
    ],
)
def test_daily_admission_rejects_metadata_trigger_identity_drift(
    monkeypatch, field, value
):
    from dags.utils import espn_native_tasks

    context, owner = _daily_parent_context()
    trigger = owner.get_task_instance()
    setattr(trigger, field, value)
    owner.get_task_instance = lambda **_kwargs: trigger
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda *_args: owner)

    with pytest.raises(espn_native_tasks.OperationsError, match="task identity"):
        espn_native_tasks._daily_parent(context)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dag_id", "dag_repair_espn"),
        ("run_id", "manual__forged"),
    ],
)
def test_daily_admission_rejects_child_dagrun_identity_drift(monkeypatch, field, value):
    from dags.utils import espn_native_tasks

    context, owner = _daily_parent_context()
    setattr(context["dag_run"], field, value)
    monkeypatch.setattr(espn_native_tasks, "_exact_parent_run", lambda *_args: owner)

    with pytest.raises(espn_native_tasks.OperationsError, match="child DagRun"):
        espn_native_tasks._daily_parent(context)


def test_discovery_rollover_is_detected_from_frozen_detail_without_network():
    from dags.utils import espn_native_tasks
    from scrapers.espn.discovery import quarantine_new_editions

    metadata = json.loads(
        (ROOT / "tests/fixtures/espn/native_competition_detail.json").read_text()
    )
    metadata["id"] = "700"
    metadata["slug"] = "eng.1"
    metadata["name"] = "English Premier League"
    metadata["season"] = {
        "year": 2027,
        "displayName": "2027-28 English Premier League",
        "startDate": "2027-07-01T00:00:00Z",
        "endDate": "2028-06-30T23:59:59Z",
    }
    scoreboard = json.loads(
        (ROOT / "tests/fixtures/espn/native_scoreboard.json").read_text()
    )
    scoreboard["leagues"][0].update({"id": "700", "slug": "eng.1"})
    detail = espn_native_tasks._competition_detail_document(
        metadata,
        scoreboard,
        json.loads((ROOT / "tests/fixtures/espn/native_summary.json").read_text()),
        espn_id=700,
        slug="eng.1",
        name="English Premier League",
    )
    snapshot = espn_native_tasks._discovery_snapshot(
        {
            "dropdown": {
                "leagues": [
                    {
                        "id": "700",
                        "slug": "eng.1",
                        "name": "English Premier League",
                    }
                ]
            },
            "details_by_slug": {"eng.1": detail},
        },
        captured_at="2027-07-01T00:00:00+00:00",
    )
    registry = SimpleNamespace(
        by_id={
            700: SimpleNamespace(
                current_edition=SimpleNamespace(source_season_year=2026)
            )
        }
    )

    assert quarantine_new_editions(snapshot, registry) == {"700:2027"}


def test_nested_discovery_plans_220_unique_details_as_eleven_bounded_batches(
    monkeypatch,
):
    from dags.utils import espn_native_tasks

    flattened = json.loads(
        (ROOT / "tests/fixtures/espn/dropdown_2026-07-31.json").read_text()
    )
    dropdown = {"sports": [{"name": "soccer", "leagues": flattened["leagues"]}]}
    raw = {
        "kind": "espn-discovery-raw-v1",
        "schema_version": 1,
        "captured_at": "2026-08-01T00:00:00+00:00",
        "raw_uri": "s3://raw/dropdown.json",
        "raw_sha256": "a" * 64,
        "direct_bytes": 1,
        "proxy_bytes": 0,
    }
    payloads = []

    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_a, **_k: raw)
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore,
        "from_uri",
        lambda _uri: SimpleNamespace(
            load_exact=lambda *_args: json.dumps(dropdown).encode()
        ),
    )
    monkeypatch.setattr(espn_native_tasks, "_raw_store_uri", lambda: "s3://raw")
    monkeypatch.setattr(espn_native_tasks, "_optional_payload", lambda *_a, **_k: None)
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_identity",
        lambda _context: (
            "dag_discover_espn_registry",
            "run-1",
            datetime(2026, 8, 1, tzinfo=timezone.utc),
        ),
    )

    def write(uri, payload, **_kwargs):
        payloads.append(payload)
        return {"uri": uri, "sha256": hashlib.sha256(uri.encode()).hexdigest()}

    monkeypatch.setattr(espn_native_tasks, "_write_payload", write)

    result = espn_native_tasks.plan_discovery_detail_batches(
        discovery_raw_ref={"uri": "raw", "sha256": "b" * 64}
    )
    batches = [
        payload
        for payload in payloads
        if payload.get("kind") == "espn-discovery-detail-batch-v1"
    ]

    assert len(result["discovery_detail_batch_refs"]) == 11
    assert [len(batch["competitions"]) for batch in batches] == [20] * 11
    assert (
        len(
            {
                (item["espn_id"], item["slug"])
                for batch in batches
                for item in batch["competitions"]
            }
        )
        == 220
    )


def test_discovery_rejects_catalog_above_static_run_request_cap(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn import discovery

    raw = {
        "kind": "espn-discovery-raw-v1",
        "raw_uri": "s3://raw/dropdown.json",
        "raw_sha256": "a" * 64,
    }
    candidates = tuple(
        SimpleNamespace(espn_id=index, slug=f"league.{index}", name=f"League {index}")
        for index in range(1, espn_native_tasks.MAX_DISCOVERY_COMPETITIONS + 2)
    )
    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_a, **_k: raw)
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore,
        "from_uri",
        lambda _uri: SimpleNamespace(load_exact=lambda *_args: b"{}"),
    )
    monkeypatch.setattr(espn_native_tasks, "_raw_store_uri", lambda: "s3://raw")
    monkeypatch.setattr(discovery, "parse_soccer_dropdown", lambda _payload: candidates)
    monkeypatch.setattr(
        espn_native_tasks, "_normalize_discovery_dropdown", lambda _payload: {}
    )

    with pytest.raises(
        espn_native_tasks.OperationsError, match="competition cap exceeded"
    ):
        espn_native_tasks.plan_discovery_detail_batches(
            discovery_raw_ref={"uri": "raw", "sha256": "b" * 64}
        )


def test_discovery_rejects_empty_catalog_before_dynamic_mapping(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn import discovery

    raw = {
        "kind": "espn-discovery-raw-v1",
        "raw_uri": "s3://raw/dropdown.json",
        "raw_sha256": "a" * 64,
    }
    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_a, **_k: raw)
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore,
        "from_uri",
        lambda _uri: SimpleNamespace(load_exact=lambda *_args: b"{}"),
    )
    monkeypatch.setattr(espn_native_tasks, "_raw_store_uri", lambda: "s3://raw")
    monkeypatch.setattr(discovery, "parse_soccer_dropdown", lambda _payload: ())
    monkeypatch.setattr(
        espn_native_tasks, "_normalize_discovery_dropdown", lambda _payload: {}
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="no competitions"):
        espn_native_tasks.plan_discovery_detail_batches(
            discovery_raw_ref={"uri": "raw", "sha256": "b" * 64}
        )


def test_discovery_detail_fetch_saves_three_exact_evidence_documents(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.transport_contracts import EndpointType

    batch_ref = {"uri": "s3://artifacts/batch.json", "sha256": "a" * 64}
    batch = {
        "kind": "espn-discovery-detail-batch-v1",
        "batch_id": "batch-1",
        "competitions": [{"espn_id": 730, "slug": "ita.1", "name": "Italian Serie A"}],
    }
    scoreboard = json.loads(
        (ROOT / "tests/fixtures/espn/native_scoreboard.json").read_text()
    )
    calls = []
    client_bounds = []

    class Client:
        def fetch_json(
            self,
            url,
            endpoint,
            params=None,
            *,
            competition_id=None,
            event_id=None,
            force_refresh=False,
        ):
            calls.append(
                (url, endpoint, params, competition_id, event_id, force_refresh)
            )
            documents = {
                EndpointType.CATALOG: {"id": "730", "slug": "ita.1"},
                EndpointType.SCOREBOARD: scoreboard,
                EndpointType.SUMMARY: {"header": {"id": "401000001"}},
            }
            index = len(calls)
            return SimpleNamespace(
                json_data=documents[endpoint],
                raw_uri=f"s3://raw/{index}.json",
                content_hash=str(index) * 64,
                direct_bytes=index,
                proxy_bytes=0,
            )

    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_a, **_k: batch)
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore,
        "from_uri",
        lambda _uri: SimpleNamespace(),
    )
    monkeypatch.setattr(espn_native_tasks, "_raw_store_uri", lambda: "s3://raw")
    monkeypatch.setattr(
        espn_native_tasks, "_optional_payload", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_http_client",
        lambda _store, **bounds: client_bounds.append(bounds) or Client(),
    )
    written = []
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **_kwargs: (
            written.append(payload) or {"uri": uri, "sha256": "f" * 64}
        ),
    )

    espn_native_tasks.fetch_discovery_detail_batch(discovery_detail_batch_ref=batch_ref)

    detail = next(
        payload["details"][0]
        for payload in written
        if payload.get("kind") == "espn-discovery-detail-phase-v1"
    )
    assert [call[1] for call in calls] == [
        EndpointType.CATALOG,
        EndpointType.SCOREBOARD,
        EndpointType.SUMMARY,
    ]
    assert calls[2][4] == "401000001"
    assert client_bounds == [
        {"max_summary_events": 1, "max_competitions": 1, "max_requests": 12}
    ]
    assert detail["request_count"] == 3
    assert detail["direct_bytes"] == 6
    assert detail["summary_event_id"] == "401000001"


def test_discovery_catalog_retry_reuses_committed_raw_checkpoint(monkeypatch):
    from dags.utils import espn_native_tasks

    captured_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    existing = {
        "kind": "espn-discovery-raw-v1",
        "schema_version": 1,
        "captured_at": captured_at.isoformat(),
        "raw_uri": "s3://raw/catalog.json",
        "raw_sha256": "a" * 64,
        "direct_bytes": 123,
        "proxy_bytes": 0,
    }
    loaded = []
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_identity",
        lambda _context: ("dag_discover_espn_registry", "run-1", captured_at),
    )
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(espn_native_tasks, "_raw_store_uri", lambda: "s3://raw")
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore,
        "from_uri",
        lambda _uri: SimpleNamespace(
            load_exact=lambda uri, digest: loaded.append((uri, digest)) or b"old"
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_optional_payload", lambda *_args, **_kwargs: existing
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_ref_for_uri",
        lambda uri: {"uri": uri, "sha256": "b" * 64},
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_http_client",
        lambda *_args, **_kwargs: pytest.fail("retry must not observe the network"),
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: pytest.fail("retry must not touch the control store")),
    )

    result = espn_native_tasks.fetch_discovery_catalog()

    assert loaded == [(existing["raw_uri"], existing["raw_sha256"])]
    assert result["discovery_raw_ref"]["uri"].endswith("/discovery-raw.json")


def test_discovery_detail_retry_reuses_committed_phase_without_network(monkeypatch):
    from dags.utils import espn_native_tasks

    batch_ref = {"uri": "s3://artifacts/batch.json", "sha256": "a" * 64}
    competition = {"espn_id": 730, "slug": "ita.1", "name": "Italian Serie A"}
    batch = {
        "kind": "espn-discovery-detail-batch-v1",
        "batch_id": "batch-1",
        "competitions": [competition],
    }
    detail = {
        **competition,
        "metadata_raw_uri": "s3://raw/metadata.json",
        "metadata_raw_sha256": "b" * 64,
        "scoreboard_raw_uri": "s3://raw/scoreboard.json",
        "scoreboard_raw_sha256": "c" * 64,
        "summary_raw_uri": None,
        "summary_raw_sha256": None,
        "summary_event_id": None,
        "request_count": 2,
        "direct_bytes": 20,
        "proxy_bytes": 0,
    }
    phase = {
        "kind": "espn-discovery-detail-phase-v1",
        "schema_version": 1,
        "batch_id": "batch-1",
        "discovery_detail_batch_ref": batch_ref,
        "details": [detail],
    }
    loaded = []
    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_args, **_kwargs: batch)
    monkeypatch.setattr(espn_native_tasks, "_raw_store_uri", lambda: "s3://raw")
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore,
        "from_uri",
        lambda _uri: SimpleNamespace(
            load_exact=lambda uri, digest: loaded.append((uri, digest)) or b"old"
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_optional_payload", lambda *_args, **_kwargs: phase
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_ref_for_uri",
        lambda uri: {"uri": uri, "sha256": "d" * 64},
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_http_client",
        lambda *_args, **_kwargs: pytest.fail("retry must not observe the network"),
    )

    result = espn_native_tasks.fetch_discovery_detail_batch(
        discovery_detail_batch_ref=batch_ref
    )

    assert loaded == [
        (detail["metadata_raw_uri"], detail["metadata_raw_sha256"]),
        (detail["scoreboard_raw_uri"], detail["scoreboard_raw_sha256"]),
    ]
    assert result["discovery_detail_phase_ref"]["uri"].endswith(".phase.json")


def test_discovery_detail_retry_reuses_partial_item_and_fetches_only_remainder(
    monkeypatch,
):
    from dags.utils import espn_native_tasks
    from scrapers.espn.transport_contracts import EndpointType

    batch_ref = {"uri": "s3://artifacts/batch.json", "sha256": "a" * 64}
    old_competition = {
        "espn_id": 730,
        "slug": "ita.1",
        "name": "Italian Serie A",
    }
    remaining_competition = {
        "espn_id": 731,
        "slug": "fra.1",
        "name": "French Ligue 1",
    }
    batch = {
        "kind": "espn-discovery-detail-batch-v1",
        "batch_id": "batch-1",
        "competitions": [old_competition, remaining_competition],
    }
    old_detail = {
        **old_competition,
        "metadata_raw_uri": "s3://raw/old-metadata.json",
        "metadata_raw_sha256": "b" * 64,
        "scoreboard_raw_uri": "s3://raw/old-scoreboard.json",
        "scoreboard_raw_sha256": "c" * 64,
        "summary_raw_uri": None,
        "summary_raw_sha256": None,
        "summary_event_id": None,
        "request_count": 2,
        "direct_bytes": 20,
        "proxy_bytes": 0,
    }
    old_item_key = hashlib.sha256(
        espn_native_tasks._canonical_bytes(
            {
                "kind": "espn-discovery-detail-item-id-v1",
                "batch_id": batch["batch_id"],
                "identity": old_competition,
            }
        )
    ).hexdigest()
    old_item_uri = batch_ref["uri"] + f".item-{old_item_key}.json"
    old_checkpoint = {
        "kind": "espn-discovery-detail-item-v1",
        "schema_version": 1,
        "batch_id": batch["batch_id"],
        "discovery_detail_batch_ref": batch_ref,
        "detail": old_detail,
    }
    scoreboard = json.loads(
        (ROOT / "tests/fixtures/espn/native_scoreboard.json").read_text()
    )
    scoreboard["leagues"][0].update({"id": "731", "slug": "fra.1"})
    calls = []

    class Client:
        def fetch_json(
            self,
            url,
            endpoint,
            params=None,
            *,
            competition_id=None,
            event_id=None,
            force_refresh=False,
        ):
            calls.append((endpoint, competition_id, event_id))
            documents = {
                EndpointType.CATALOG: {"id": "731", "slug": "fra.1"},
                EndpointType.SCOREBOARD: scoreboard,
                EndpointType.SUMMARY: {"header": {"id": "401000001"}},
            }
            index = len(calls)
            return SimpleNamespace(
                json_data=documents[endpoint],
                raw_uri=f"s3://raw/new-{index}.json",
                content_hash=str(index) * 64,
                direct_bytes=index,
                proxy_bytes=0,
            )

    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_args, **_kwargs: batch)
    monkeypatch.setattr(espn_native_tasks, "_raw_store_uri", lambda: "s3://raw")
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore,
        "from_uri",
        lambda _uri: SimpleNamespace(load_exact=lambda *_args: b"old"),
    )

    def optional(uri, **_kwargs):
        if uri.endswith(".phase.json"):
            return None
        return old_checkpoint if uri == old_item_uri else None

    monkeypatch.setattr(espn_native_tasks, "_optional_payload", optional)
    monkeypatch.setattr(
        espn_native_tasks, "_http_client", lambda *_args, **_kwargs: Client()
    )
    written = []
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **_kwargs: (
            written.append((uri, payload))
            or {"uri": uri, "sha256": hashlib.sha256(uri.encode()).hexdigest()}
        ),
    )

    espn_native_tasks.fetch_discovery_detail_batch(discovery_detail_batch_ref=batch_ref)

    assert [call[0] for call in calls] == [
        EndpointType.CATALOG,
        EndpointType.SCOREBOARD,
        EndpointType.SUMMARY,
    ]
    assert {call[1] for call in calls} == {731}
    assert all(uri != old_item_uri for uri, _payload in written)
    phase = next(
        payload
        for _uri, payload in written
        if payload.get("kind") == "espn-discovery-detail-phase-v1"
    )
    assert [item["espn_id"] for item in phase["details"]] == [730, 731]


@pytest.mark.parametrize("foreign_first", [True, False])
def test_discovery_rejects_mixed_scoreboard_league_ownership(foreign_first):
    from dags.utils import espn_native_tasks

    metadata = json.loads(
        (ROOT / "tests/fixtures/espn/native_competition_detail.json").read_text()
    )
    scoreboard = json.loads(
        (ROOT / "tests/fixtures/espn/native_scoreboard.json").read_text()
    )
    summary = json.loads((ROOT / "tests/fixtures/espn/native_summary.json").read_text())
    foreign_league = {"id": "999", "slug": "foreign.1"}
    foreign_event = {
        "id": "499999999",
        "season": {"year": 2020},
        "competitions": [
            {
                "competitors": [
                    {"team": {"id": "91"}},
                    {"team": {"id": "92"}},
                ]
            }
        ],
    }
    if foreign_first:
        scoreboard["leagues"].insert(0, foreign_league)
        scoreboard["events"].insert(0, foreign_event)
    else:
        scoreboard["leagues"].append(foreign_league)
        scoreboard["events"].append(foreign_event)

    with pytest.raises(espn_native_tasks.OperationsError, match="own exactly"):
        espn_native_tasks._competition_detail_document(
            metadata,
            scoreboard,
            summary,
            espn_id=730,
            slug="ita.1",
            name="Italian Serie A",
        )


def test_discovery_null_dropdown_id_binds_core_and_scoreboard_identity():
    from dags.utils import espn_native_tasks

    metadata = json.loads(
        (ROOT / "tests/fixtures/espn/native_competition_detail.json").read_text()
    )
    scoreboard = json.loads(
        (ROOT / "tests/fixtures/espn/native_scoreboard.json").read_text()
    )
    summary = json.loads((ROOT / "tests/fixtures/espn/native_summary.json").read_text())
    resolved = espn_native_tasks._competition_detail_document(
        metadata,
        scoreboard,
        summary,
        espn_id=None,
        slug="ita.1",
        name="Italian Serie A",
    )
    assert resolved["id"] == 730

    scoreboard["leagues"][0]["id"] = "999"
    with pytest.raises(espn_native_tasks.OperationsError, match="own exactly"):
        espn_native_tasks._competition_detail_document(
            metadata,
            scoreboard,
            summary,
            espn_id=None,
            slug="ita.1",
            name="Italian Serie A",
        )


def test_shipped_null_id_discovery_row_remains_unpromotable_with_slug_evidence():
    from dags.utils import espn_native_tasks

    dropdown = json.loads(
        (ROOT / "tests/fixtures/espn/dropdown_2026-07-31.json").read_text()
    )
    row = next(item for item in dropdown["leagues"] if item["slug"] == "fifa.wwcq.ply")
    assert row["id"] is None
    metadata = json.loads(
        (ROOT / "tests/fixtures/espn/native_competition_detail.json").read_text()
    )
    metadata.update({"id": None, "slug": row["slug"], "name": row["name"]})
    scoreboard = json.loads(
        (ROOT / "tests/fixtures/espn/native_scoreboard.json").read_text()
    )
    scoreboard["leagues"][0].update({"id": None, "slug": row["slug"]})
    detail = espn_native_tasks._competition_detail_document(
        metadata,
        scoreboard,
        json.loads((ROOT / "tests/fixtures/espn/native_summary.json").read_text()),
        espn_id=None,
        slug=row["slug"],
        name=row["name"],
    )
    snapshot = espn_native_tasks._discovery_snapshot(
        {
            "dropdown": {"leagues": [row]},
            "details_by_slug": {row["slug"]: detail},
        },
        captured_at=dropdown["captured_at"],
    )

    assert snapshot.candidates[0].espn_id is None
    assert snapshot.candidates[0].slug == "fifa.wwcq.ply"
    assert snapshot.candidates[0].capabilities.lineup.value == "unknown"


@pytest.mark.parametrize(
    "malformed_case",
    [
        "empty_athlete",
        "wrong_roster_side",
        "invalid_captain",
        "empty_statistic",
        "unnamed_statistic",
    ],
)
def test_discovery_sample_requires_parser_equivalent_capability_rows(malformed_case):
    from dags.utils import espn_native_tasks

    metadata = json.loads(
        (ROOT / "tests/fixtures/espn/native_competition_detail.json").read_text()
    )
    scoreboard = json.loads(
        (ROOT / "tests/fixtures/espn/native_scoreboard.json").read_text()
    )
    summary = json.loads((ROOT / "tests/fixtures/espn/native_summary.json").read_text())
    if malformed_case == "empty_athlete":
        summary["rosters"][0]["roster"] = [{}]
    elif malformed_case == "wrong_roster_side":
        summary["rosters"][0]["homeAway"] = "home"
    elif malformed_case == "invalid_captain":
        summary["rosters"][0]["roster"][0]["captain"] = "yes"
    elif malformed_case == "empty_statistic":
        summary["boxscore"]["teams"][0]["statistics"] = [{}]
    else:
        summary["boxscore"]["teams"][0]["statistics"].insert(0, {})

    detail = espn_native_tasks._competition_detail_document(
        metadata,
        scoreboard,
        summary,
        espn_id=730,
        slug="ita.1",
        name="Italian Serie A",
    )

    assert detail["capabilities"] == {
        "schedule": "proven",
        "lineup": "unknown",
        "matchsheet": "unknown",
    }


def test_saved_detail_evidence_reaches_male_registry_artifact(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.registry import validate_registry_document

    dropdown_ref = {"uri": "raw-ref", "sha256": "a" * 64}
    index_ref = {"uri": "index-ref", "sha256": "b" * 64}
    batch_ref = {"uri": "batch-ref", "sha256": "c" * 64}
    phase_ref = {"uri": "phase-ref", "sha256": "d" * 64}
    raw = {
        "kind": "espn-discovery-raw-v1",
        "captured_at": "2026-08-01T00:00:00+00:00",
        "raw_uri": "s3://raw/dropdown.json",
        "raw_sha256": "e" * 64,
        "proxy_bytes": 0,
    }
    competition = {"espn_id": 730, "slug": "ita.1", "name": "Italian Serie A"}
    batch = {
        "kind": "espn-discovery-detail-batch-v1",
        "batch_id": "batch-1",
        "discovery_raw_ref": dropdown_ref,
        "competitions": [competition],
    }
    index = {
        "kind": "espn-discovery-detail-index-v1",
        "discovery_raw_ref": dropdown_ref,
        "captured_at": raw["captured_at"],
        "batch_ids": ["batch-1"],
        "batch_refs": [batch_ref],
        "competition_count": 1,
        "competition_cap": espn_native_tasks.MAX_DISCOVERY_COMPETITIONS,
        "detail_request_cap": espn_native_tasks.MAX_DISCOVERY_DETAIL_REQUESTS,
    }
    phase = {
        "kind": "espn-discovery-detail-phase-v1",
        "batch_id": "batch-1",
        "discovery_detail_batch_ref": batch_ref,
        "details": [
            {
                **competition,
                "metadata_raw_uri": "s3://raw/ita-metadata.json",
                "metadata_raw_sha256": "f" * 64,
                "scoreboard_raw_uri": "s3://raw/ita-scoreboard.json",
                "scoreboard_raw_sha256": "1" * 64,
                "summary_raw_uri": "s3://raw/ita-summary.json",
                "summary_raw_sha256": "2" * 64,
                "summary_event_id": "401000001",
                "request_count": 3,
                "direct_bytes": 3,
                "proxy_bytes": 0,
            }
        ],
    }
    payload_by_uri = {
        dropdown_ref["uri"]: raw,
        index_ref["uri"]: index,
        batch_ref["uri"]: batch,
        phase_ref["uri"]: phase,
    }
    dropdown = {
        "sports": [
            {
                "leagues": [
                    {"id": "730", "slug": "ita.1", "displayName": "Italian Serie A"}
                ]
            }
        ]
    }
    saved = {
        "s3://raw/ita-metadata.json": (
            ROOT / "tests/fixtures/espn/native_competition_detail.json"
        ).read_bytes(),
        "s3://raw/ita-scoreboard.json": (
            ROOT / "tests/fixtures/espn/native_scoreboard.json"
        ).read_bytes(),
        "s3://raw/ita-summary.json": (
            ROOT / "tests/fixtures/espn/native_summary.json"
        ).read_bytes(),
    }
    written = []
    alert_snapshots = []
    registry = validate_registry_document(
        {
            "schema_version": 1,
            "registry_version": "fixture-2020",
            "as_of": "2020-08-01",
            "competitions": [
                {
                    "espn_id": 730,
                    "slug": "ita.1",
                    "name": "Italian Serie A",
                    "enabled": True,
                    "gender": "MALE",
                    "age_class": "SENIOR",
                    "gender_evidence": ["fixture core detail"],
                    "age_class_evidence": ["fixture operator review"],
                    "legacy": None,
                    "editions": [
                        {
                            "source_season_year": 2020,
                            "display_name": "2020-21 Italian Serie A",
                            "start_date": "2020-08-01",
                            "end_date": "2021-07-31",
                            "current": True,
                            "capabilities": {
                                "schedule": "proven",
                                "lineup": "partial",
                                "matchsheet": "partial",
                            },
                        }
                    ],
                }
            ],
        }
    )

    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, **_kwargs: payload_by_uri[ref["uri"]],
    )
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore,
        "from_uri",
        lambda _uri: SimpleNamespace(
            load_exact=lambda uri, _sha: (
                json.dumps(dropdown).encode() if uri == raw["raw_uri"] else saved[uri]
            )
        ),
    )
    monkeypatch.setattr(espn_native_tasks, "_raw_store_uri", lambda: "s3://raw")
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(espn_native_tasks, "_optional_payload", lambda *_a, **_k: None)
    monkeypatch.setattr(espn_native_tasks, "load_registry", lambda _path: registry)
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(
            lambda _cls: SimpleNamespace(
                current_time=lambda: datetime(2026, 8, 1, tzinfo=timezone.utc)
            )
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "evaluate_alerts",
        lambda snapshot, **_kwargs: alert_snapshots.append(snapshot) or (),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_identity",
        lambda _context: (
            "dag_discover_espn_registry",
            "run-1",
            datetime(2026, 8, 1, tzinfo=timezone.utc),
        ),
    )
    monkeypatch.setattr(espn_native_tasks, "_attempt", lambda _context: 1)
    monkeypatch.delenv("ESPN_DISCOVERY_PREVIOUS_URI", raising=False)

    def write(uri, payload, **_kwargs):
        written.append(payload)
        return espn_native_tasks._payload_ref(uri, payload)

    monkeypatch.setattr(espn_native_tasks, "_write_payload", write)

    result = espn_native_tasks.publish_discovered_male_registry(
        discovery_detail_index_ref=index_ref,
        discovery_detail_phase_refs=[{"discovery_detail_phase_ref": phase_ref}],
        params={"attempt": 1},
    )

    candidate = next(
        payload for payload in written if payload.get("source") is not None
    )["candidates"][0]
    review = next(
        payload
        for payload in written
        if payload.get("kind") == "espn-discovery-review-v2"
    )
    assert candidate["name"] == "Italian Serie A"
    assert candidate["gender"] == "MALE"
    assert candidate["gender_evidence"] == ["core-detail.gender=MALE"]
    assert candidate["source_season_year"] == 2020
    assert candidate["edition_display_name"] == "2020-21 Italian Serie A"
    assert candidate["start_date"] == "2020-08-01"
    assert candidate["end_date"] == "2021-07-31"
    assert candidate["capabilities"] == {
        "schedule": "proven",
        "lineup": "partial",
        "matchsheet": "partial",
    }
    assert alert_snapshots[0]["direct_requests"] == 4
    assert alert_snapshots[0]["request_budget"] == (
        1 + espn_native_tasks.MAX_DISCOVERY_DETAIL_REQUESTS
    )
    male_registry = next(
        payload
        for payload in written
        if payload.get("registry_version", "").startswith("discovery-male-")
    )
    male_registry_ref = review["male_registry_ref"]
    assert [row["espn_id"] for row in male_registry["competitions"]] == [730]
    assert review["selection_policy"] == "explicit-core-gender-MALE-v1"
    assert review["male_scope_count"] == 1
    assert review["male_registry_signature"] == validate_registry_document(
        male_registry
    ).signature()
    assert male_registry_ref["uri"].endswith("/male-registry.json")
    assert review["unresolved_discovery_diffs"] is False
    assert review["quarantined_scopes"] == []
    assert review["promotion_performed"] is False
    assert result["discovery_review_ref"]["uri"].endswith("reviewable-diff.json")


def test_discovery_review_retry_replays_checkpoint_without_recomputation(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.discovery import CatalogSnapshot
    from scrapers.espn.registry import validate_registry_document

    index_ref = {"uri": "s3://artifacts/index.json", "sha256": "a" * 64}
    phase_ref = {"uri": "s3://artifacts/phase.json", "sha256": "b" * 64}
    candidate_ref = {"uri": "s3://artifacts/candidate.json", "sha256": "c" * 64}
    candidate = {
        "schema_version": 1,
        "captured_at": "2026-08-01T00:00:00+00:00",
        "candidates": [
            {
                "espn_id": 730,
                "slug": "ita.1",
                "name": "Italian Serie A",
                "group": "ESPN soccer dropdown",
                "source_order": 0,
                "gender": "MALE",
                "age_class": "UNKNOWN",
                "source_season_year": 2020,
                "edition_display_name": "2020-21 Italian Serie A",
                "start_date": "2020-08-01",
                "end_date": "2021-07-31",
                "capabilities": {
                    "schedule": "proven",
                    "lineup": "partial",
                    "matchsheet": "partial",
                },
                "gender_evidence": ["core-detail.gender=MALE"],
            }
        ],
        "source": "committed retry fixture",
    }
    candidate_signature = CatalogSnapshot.from_dict(candidate).signature()
    male_registry_ref = {
        "uri": "s3://artifacts/male-registry.json",
        "sha256": "d" * 64,
    }
    male_registry = {
        "schema_version": 1,
        "registry_version": "discovery-male-retry",
        "as_of": "2026-08-01",
        "competitions": [
            {
                "espn_id": 730,
                "slug": "ita.1",
                "name": "Italian Serie A",
                "enabled": True,
                "gender": "MALE",
                "age_class": "UNKNOWN",
                "gender_evidence": ["core-detail.gender=MALE"],
                "age_class_evidence": [],
                "legacy": None,
                "editions": [
                    {
                        "source_season_year": 2020,
                        "display_name": "2020-21 Italian Serie A",
                        "start_date": "2020-08-01",
                        "end_date": "2021-07-31",
                        "current": True,
                        "capabilities": {
                            "schedule": "proven",
                            "lineup": "partial",
                            "matchsheet": "partial",
                        },
                    }
                ],
            }
        ],
    }
    male_registry_signature = validate_registry_document(male_registry).signature()
    review = {
        "kind": "espn-discovery-review-v2",
        "schema_version": 2,
        "discovery_detail_index_ref": index_ref,
        "discovery_detail_phase_refs": [phase_ref],
        "candidate_ref": candidate_ref,
        "candidate_signature": candidate_signature,
        "male_registry_ref": male_registry_ref,
        "male_registry_signature": male_registry_signature,
        "male_scope_count": 1,
        "selection_policy": "explicit-core-gender-MALE-v1",
        "quarantined_scopes": [],
        "changes": [],
        "change_count": 0,
        "unresolved_discovery_diffs": False,
        "alerts": [],
        "promotion_performed": False,
        "observed_at": "2026-08-01T00:00:00+00:00",
    }
    review_ref = {"uri": "s3://artifacts/reviewable-diff.json", "sha256": "e" * 64}
    writes = []
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_identity",
        lambda _context: (
            "dag_discover_espn_registry",
            "run-1",
            datetime(2026, 8, 1, tzinfo=timezone.utc),
        ),
    )
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(
        espn_native_tasks,
        "_optional_payload",
        lambda uri, **_kwargs: review if uri.endswith("reviewable-diff.json") else None,
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, **_kwargs: (
            candidate
            if ref == candidate_ref
            else male_registry
            if ref == male_registry_ref
            else pytest.fail("retry must not reread discovery inputs")
        ),
    )
    monkeypatch.setattr(espn_native_tasks, "_ref_for_uri", lambda _uri: review_ref)
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **kwargs: (
            writes.append((uri, payload, kwargs)) or {"uri": uri, "sha256": "f" * 64}
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(
            lambda _cls: pytest.fail("retry must not read a new database clock")
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "load_registry",
        lambda _path: pytest.fail("retry must not recompute mutable registry state"),
    )

    result = espn_native_tasks.write_reviewable_discovery_diff(
        discovery_detail_index_ref=index_ref,
        discovery_detail_phase_refs=[{"discovery_detail_phase_ref": phase_ref}],
    )

    assert result == {
        "discovery_review_ref": review_ref,
        "male_registry_ref": male_registry_ref,
    }
    assert len(writes) == 1
    latest_uri, latest_state, kwargs = writes[0]
    assert latest_uri.endswith("/discovery/latest-state.json")
    assert latest_state["candidate_signature"] == candidate_signature
    assert latest_state["male_registry_ref"] == male_registry_ref
    assert latest_state["male_registry_signature"] == male_registry_signature
    assert latest_state["review_ref"] == review_ref
    assert kwargs == {"immutable": False}


def test_discovery_partial_publication_replays_sealed_reducer_checkpoint(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.discovery import CatalogSnapshot
    from scrapers.espn.registry import validate_registry_document

    index_ref = {"uri": "s3://artifacts/index.json", "sha256": "a" * 64}
    phase_ref = {"uri": "s3://artifacts/phase.json", "sha256": "b" * 64}
    root = "s3://artifacts/discovery/frozen-run"
    candidate_uri = f"{root}/candidate.json"
    registry_uri = f"{root}/male-registry.json"
    review_uri = f"{root}/reviewable-diff.json"
    checkpoint_uri = f"{root}/reducer-checkpoint.json"
    candidate = {
        "schema_version": 1,
        "captured_at": "2026-08-01T00:00:00+00:00",
        "source": "sealed partial publication",
        "candidates": [
            {
                "espn_id": 730,
                "slug": "ita.1",
                "name": "Italian Serie A",
                "group": "ESPN soccer dropdown",
                "source_order": 0,
                "gender": "MALE",
                "age_class": "UNKNOWN",
                "source_season_year": 2020,
                "edition_display_name": "2020-21 Italian Serie A",
                "start_date": "2020-08-01",
                "end_date": "2021-07-31",
                "capabilities": {
                    "schedule": "proven",
                    "lineup": "partial",
                    "matchsheet": "partial",
                },
                "gender_evidence": ["core-detail.gender=MALE"],
            }
        ],
    }
    male_registry = {
        "schema_version": 1,
        "registry_version": "discovery-male-sealed",
        "as_of": "2026-08-01",
        "competitions": [
            {
                "espn_id": 730,
                "slug": "ita.1",
                "name": "Italian Serie A",
                "enabled": True,
                "gender": "MALE",
                "age_class": "UNKNOWN",
                "gender_evidence": ["core-detail.gender=MALE"],
                "age_class_evidence": [],
                "legacy": None,
                "editions": [
                    {
                        "source_season_year": 2020,
                        "display_name": "2020-21 Italian Serie A",
                        "start_date": "2020-08-01",
                        "end_date": "2021-07-31",
                        "current": True,
                        "capabilities": {
                            "schedule": "proven",
                            "lineup": "partial",
                            "matchsheet": "partial",
                        },
                    }
                ],
            }
        ],
    }

    def ref(uri, payload):
        return {
            "uri": uri,
            "sha256": hashlib.sha256(
                espn_native_tasks._canonical_bytes(payload)
            ).hexdigest(),
        }

    candidate_ref = ref(candidate_uri, candidate)
    registry_ref = ref(registry_uri, male_registry)
    review = {
        "kind": "espn-discovery-review-v2",
        "schema_version": 2,
        "discovery_detail_index_ref": index_ref,
        "discovery_detail_phase_refs": [phase_ref],
        "candidate_ref": candidate_ref,
        "candidate_signature": CatalogSnapshot.from_dict(candidate).signature(),
        "male_registry_ref": registry_ref,
        "male_registry_signature": validate_registry_document(
            male_registry
        ).signature(),
        "male_scope_count": 1,
        "selection_policy": "explicit-core-gender-MALE-v1",
        "quarantined_scopes": [],
        "changes": [],
        "change_count": 0,
        "unresolved_discovery_diffs": False,
        "alerts": [],
        "promotion_performed": False,
        "observed_at": "2026-08-01T00:00:00+00:00",
    }
    review_ref = ref(review_uri, review)
    checkpoint = {
        "kind": "espn-discovery-reducer-checkpoint-v1",
        "schema_version": 1,
        "discovery_detail_index_ref": index_ref,
        "discovery_detail_phase_refs": [phase_ref],
        "candidate_payload": candidate,
        "candidate_ref": candidate_ref,
        "candidate_signature": review["candidate_signature"],
        "male_registry_payload": male_registry,
        "male_registry_ref": registry_ref,
        "male_registry_signature": review["male_registry_signature"],
        "male_scope_count": 1,
        "review_payload": review,
        "review_ref": review_ref,
        "observed_at": review["observed_at"],
    }
    published = {checkpoint_uri: checkpoint}
    writes = []
    fail_review = [True]
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_identity",
        lambda _context: (
            "dag_discover_espn_registry",
            "run-1",
            datetime(2026, 8, 1, tzinfo=timezone.utc),
        ),
    )
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_key",
        lambda _dag_id, _run_id: "frozen-run",
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_optional_payload",
        lambda uri, **_kwargs: published.get(uri),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda *_a, **_k: pytest.fail("sealed replay must not reread discovery inputs"),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "load_registry",
        lambda *_a, **_k: pytest.fail("sealed replay must not read mutable registry"),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "build_discovered_male_registry",
        lambda *_a, **_k: pytest.fail("sealed replay must not rebuild registry"),
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: pytest.fail("sealed replay must not read DB clock")),
    )

    def write(uri, payload, **kwargs):
        if uri in published:
            assert published[uri] == payload
        if uri == review_uri and fail_review[0]:
            raise RuntimeError("simulated failure before review publication")
        published[uri] = payload
        writes.append((uri, payload, kwargs))
        return ref(uri, payload)

    monkeypatch.setattr(espn_native_tasks, "_write_payload", write)

    with pytest.raises(RuntimeError, match="before review publication"):
        espn_native_tasks._replay_discovery_reducer_checkpoint(
            checkpoint,
            discovery_detail_index_ref=index_ref,
            discovery_detail_phase_refs=[phase_ref],
            candidate_uri=candidate_uri,
            male_registry_uri=registry_uri,
            review_uri=review_uri,
            latest_state_uri="s3://artifacts/discovery/latest-state.json",
        )
    assert published[candidate_uri] == candidate
    assert published[registry_uri] == male_registry
    assert review_uri not in published
    fail_review[0] = False
    writes.clear()

    result = espn_native_tasks.publish_discovered_male_registry(
        discovery_detail_index_ref=index_ref,
        discovery_detail_phase_refs=[{"discovery_detail_phase_ref": phase_ref}],
    )

    assert result == {
        "discovery_review_ref": review_ref,
        "male_registry_ref": registry_ref,
    }
    assert [uri for uri, _payload, _kwargs in writes] == [
        candidate_uri,
        registry_uri,
        review_uri,
        "s3://artifacts/discovery/latest-state.json",
    ]


def test_discovery_monitor_reads_v1_state_during_v2_transition(monkeypatch):
    from dags.utils import espn_native_tasks

    state = {
        "kind": "espn-discovery-state-v1",
        "review_ref": {"uri": "s3://artifacts/review-v1.json", "sha256": "a" * 64},
    }
    review = {
        "kind": "espn-discovery-review-v1",
        "quarantined_scopes": ["700:2027"],
        "unresolved_discovery_diffs": True,
    }
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(
        espn_native_tasks,
        "_optional_payload",
        lambda _uri, *, kind=None: (
            state if kind is None else pytest.fail("transition read must accept v1")
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, *, kind=None: (
            review
            if ref == state["review_ref"] and kind == "espn-discovery-review-v1"
            else pytest.fail("unexpected transition artifact read")
        ),
    )

    assert espn_native_tasks._latest_discovery_flags("700:2026") == (True, True)


def test_discovery_state_v2_rejects_mixed_review_projection(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn import discovery

    candidate_ref = {"uri": "s3://artifacts/candidate.json", "sha256": "a" * 64}
    registry_ref = {"uri": "s3://artifacts/male-registry.json", "sha256": "b" * 64}
    review_ref = {"uri": "s3://artifacts/review.json", "sha256": "c" * 64}
    candidate_signature = "d" * 64
    registry_signature = "e" * 64
    state = {
        "kind": "espn-discovery-state-v2",
        "schema_version": 2,
        "candidate_ref": candidate_ref,
        "candidate_signature": candidate_signature,
        "review_ref": review_ref,
        "male_registry_ref": registry_ref,
        "male_registry_signature": registry_signature,
        "male_scope_count": 1,
        "selection_policy": "explicit-core-gender-MALE-v1",
        "observed_at": "2026-08-02T00:00:00+00:00",
    }
    review = {
        "kind": "espn-discovery-review-v2",
        "schema_version": 2,
        "discovery_detail_index_ref": {"uri": "index", "sha256": "1" * 64},
        "discovery_detail_phase_refs": [{"uri": "phase", "sha256": "2" * 64}],
        "candidate_ref": candidate_ref,
        "candidate_signature": "f" * 64,
        "male_registry_ref": registry_ref,
        "male_registry_signature": registry_signature,
        "male_scope_count": 1,
        "selection_policy": "explicit-core-gender-MALE-v1",
        "quarantined_scopes": [],
        "changes": [],
        "change_count": 0,
        "unresolved_discovery_diffs": False,
        "alerts": [],
        "promotion_performed": False,
        "observed_at": state["observed_at"],
    }
    monkeypatch.setattr(
        discovery.CatalogSnapshot,
        "from_dict",
        classmethod(lambda _cls, _value: SimpleNamespace(signature=lambda: candidate_signature)),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "validate_registry_document",
        lambda _value: SimpleNamespace(signature=lambda: registry_signature, promoted=(1,)),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, **_kwargs: review if ref == review_ref else {},
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="projection"):
        espn_native_tasks._load_discovery_state_v2(state)


def test_shipped_finished_current_scopes_plan_safely_for_every_daily_shard(
    monkeypatch,
):
    """Euro/Copa must be signed no-ops on non-reconciliation weekdays."""

    from dags.utils import espn_native_tasks
    from scrapers.espn.registry import DEFAULT_REGISTRY_PATH, load_registry

    registry = load_registry(DEFAULT_REGISTRY_PATH)
    finished = [registry.by_id[781], registry.by_id[780]]
    head = SimpleNamespace(
        snapshot_uri="s3://artifacts/prior.json",
        snapshot_sha256="a" * 64,
        scope_id=None,
        generation_id="prior-generation",
        generation_signature="b" * 64,
        manifest_sha256="c" * 64,
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "load_scope_snapshot",
        lambda *_args, expected_scope_id, **_kwargs: SimpleNamespace(
            plan=espn_native_tasks._scope_plan(registry, expected_scope_id),
            schedule=(),
        ),
    )

    for competition in finished:
        scope = espn_native_tasks._scope_plan(
            registry, competition.scope_id(competition.current_edition)
        )
        observed_activity = []
        for offset in range(7):
            as_of = date(2026, 8, 1) + timedelta(days=offset)
            current_head = SimpleNamespace(
                **{**head.__dict__, "scope_id": scope.scope_id}
            )
            binding = espn_native_tasks._scope_binding(
                head=current_head,
                scope=scope,
                run_id=f"daily-{as_of}",
                attempt=1,
                mode="daily",
                root="s3://artifacts/run",
                ingested_at=datetime.combine(
                    as_of, datetime.min.time(), tzinfo=timezone.utc
                ),
                as_of=as_of,
            )
            typed = espn_native_tasks.runner._scope_binding(binding, scope.scope_id)
            requests = espn_native_tasks.runner._scoreboard_requests(
                scope, typed, as_of=as_of, mode="daily"
            )
            assert binding["active"] is bool(requests)
            if not requests:
                assert binding["prior"] is not None
            observed_activity.append(binding["active"])
        assert any(observed_activity)
        assert not all(observed_activity)


def test_expired_same_owner_acquisition_reclaims_instead_of_failing(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.operations import LeaseLost, MemoryScopeLeaseStore

    now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
    expired = MemoryScopeLeaseStore().acquire_many(
        ("700:2026",),
        owner_id="dag_ingest_espn/run-1/1",
        plan_signature="a" * 64,
        now=now - timedelta(hours=10),
        ttl=timedelta(hours=1),
    )[0]
    replacement = replace(
        expired,
        epoch=expired.epoch + 1,
        token_sha256="b" * 64,
        acquired_at=now,
        expires_at=now + timedelta(hours=9),
    )
    calls = []

    class Store:
        def migrate(self):
            pass

        def read_owner_leases(self, _owner):
            return (expired,)

        def assert_owned(self, _lease, **_kwargs):
            raise LeaseLost("expired")

        def reclaim_owner_many(self, *_args, **_kwargs):
            calls.append(_kwargs)
            return (replacement,)

    admission = {
        "kind": "espn-airflow-admission-v2",
        "dag_id": "dag_ingest_espn",
        "run_id": "run-1",
        "attempt": 1,
        "scope_ids": ["700:2026"],
        "artifact_root": "s3://artifacts/run",
        "logical_date": now.isoformat(),
    }
    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_a, **_k: admission)
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_identity",
        lambda _context: ("dag_ingest_espn", "run-1", now),
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: Store()),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, _payload, **_kwargs: {"uri": uri, "sha256": "d" * 64},
    )

    result = espn_native_tasks.acquire_scope_leases(
        admission_ref={"uri": "admission", "sha256": "a" * 64}
    )

    assert result["lease_acquisition_ref"]["uri"].endswith("lease-acquisition.json")
    assert len(calls) == 1
    assert calls[0]["ttl"] >= timedelta(hours=8)


def test_bound_lease_heartbeat_renews_at_long_running_boundary(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.operations import MemoryScopeLeaseStore

    now = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
    lease = MemoryScopeLeaseStore().acquire_many(
        ("700:2026",),
        owner_id="dag/run/1",
        plan_signature="a" * 64,
        now=now,
        ttl=timedelta(hours=1),
    )[0]
    renewed = replace(lease, expires_at=now + timedelta(hours=9))
    calls = []

    class Store:
        def renew(self, current, **kwargs):
            calls.append((current, kwargs))
            return renewed

    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda _ref: ({}, {}, SimpleNamespace(), SimpleNamespace(), lease),
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: Store()),
    )

    assert (
        espn_native_tasks._heartbeat_scope_binding({"uri": "x", "sha256": "e" * 64})
        == renewed
    )
    assert calls[0][0] == lease
    assert calls[0][1]["ttl"] >= timedelta(hours=8)


def test_stale_cleanup_cannot_release_same_owner_reclaimed_epoch(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.operations import MemoryScopeLeaseStore

    store = MemoryScopeLeaseStore()
    acquired_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    owner = "dag_ingest_espn/run-1/1"
    old = store.acquire_many(
        ("700:2026",),
        owner_id=owner,
        plan_signature="a" * 64,
        now=acquired_at,
        ttl=timedelta(hours=1),
    )[0]
    replacement = store.acquire_many(
        ("700:2026",),
        owner_id=owner,
        plan_signature="a" * 64,
        now=acquired_at + timedelta(hours=2),
        ttl=timedelta(hours=9),
    )[0]
    acquisition = {
        "kind": "espn-lease-acquisition-v2",
        "schema_version": 2,
        "owner_id": owner,
        "scope_ids": ["700:2026"],
        "leases": [espn_native_tasks._lease_to_dict(old)],
    }
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda *_args, **_kwargs: acquisition,
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: store),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_run_identity",
        lambda _context: (
            "dag_ingest_espn",
            "run-1",
            acquired_at + timedelta(hours=2),
        ),
    )
    monkeypatch.setattr(espn_native_tasks, "_attempt", lambda _context: 1)
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, _payload, **_kwargs: {"uri": uri, "sha256": "b" * 64},
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="cleanup failures"):
        espn_native_tasks.release_scope_leases(
            lease_acquisition_ref={"uri": "acquisition", "sha256": "c" * 64}
        )

    assert store.current("700:2026") == replacement


def test_operational_metrics_read_exact_raw_and_summary_budget(monkeypatch):
    from dags.utils import espn_native_tasks

    descriptor_ref = {"uri": "descriptor", "sha256": "a" * 64}
    descriptor = {
        "kind": "espn-scope-plan-descriptor-v1",
        "scope_id": "700:2026",
        "raw_manifest_uri": "raw-manifest",
        "scoreboard_checkpoint_uri": "scoreboard-checkpoint",
    }
    monkeypatch.setattr(
        espn_native_tasks,
        "_read_ref",
        lambda ref, **_kwargs: descriptor if ref == descriptor_ref else {},
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda _uri: (
            b'{"checkpoints":[{"requests":[{"proxy_bytes":0},{"proxy_bytes":7}]}]}'
        ),
    )

    metrics = espn_native_tasks._scope_operational_metrics(
        {
            "scope_ids": ["700:2026"],
            "scope_plan_refs": [descriptor_ref],
            "mode": "daily",
        },
        {
            "scopes": [
                {
                    "scope_id": "700:2026",
                    "budget_used": 800,
                    "budget_limit": 1000,
                }
            ]
        },
    )

    assert metrics["700:2026"] == {
        "network_requests": 2,
        "proxy_bytes": 7,
        "budget_used": 800,
        "budget_limit": 1000,
        "schema_drift": False,
    }


def test_generation_binding_rejects_stale_current_run_snapshot():
    from dags.utils import espn_native_tasks

    now = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
    scope = SimpleNamespace(scope_id="700:2026")
    binding = SimpleNamespace(
        generation_id="generation-current",
        batch_id="batch-current",
        ingested_at=now,
        prior=None,
    )
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current", registry_signature="b" * 64),
        registry_snapshot_uri="s3://artifacts/registry.json",
        signature="a" * 64,
        bindings={scope.scope_id: binding},
    )
    generation = SimpleNamespace(
        plan=scope,
        run_id="run-old",
        registry_snapshot_uri=loaded.registry_snapshot_uri,
        registry_signature=loaded.plan.registry_signature,
        plan_signature=loaded.signature,
        generation_id=binding.generation_id,
        batch_id=binding.batch_id,
        ingested_at=now,
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="current signed plan"):
        espn_native_tasks._assert_generation_binding(
            generation=generation,
            loaded=loaded,
            scope=scope,
            state="staged",
        )


def test_generation_binding_accepts_only_exact_signed_noop_prior():
    from dags.utils import espn_native_tasks

    scope = SimpleNamespace(scope_id="700:2026")
    prior = SimpleNamespace(
        generation_id="generation-prior",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
    )
    loaded = SimpleNamespace(bindings={scope.scope_id: SimpleNamespace(prior=prior)})
    generation = SimpleNamespace(
        plan=scope,
        generation_id=prior.generation_id,
        generation_signature=prior.generation_signature,
        manifest_sha256=prior.manifest_sha256,
    )

    espn_native_tasks._assert_generation_binding(
        generation=generation, loaded=loaded, scope=scope, state="noop"
    )
    generation.manifest_sha256 = "e" * 64
    with pytest.raises(espn_native_tasks.OperationsError, match="signed prior"):
        espn_native_tasks._assert_generation_binding(
            generation=generation,
            loaded=loaded,
            scope=scope,
            state="noop",
        )


def _artifact_ref(artifacts, uri, payload):
    body = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    artifacts[uri] = body
    return {"uri": uri, "sha256": hashlib.sha256(body).hexdigest()}


def test_summary_mapped_retry_resumes_only_missing_exact_request(monkeypatch):
    from dags.utils import espn_native_tasks

    artifacts = {}
    expected = espn_native_tasks.make_summary_batches(
        (101, 102, 103),
        run_id="run-current",
        attempt=2,
        scope_id="700:2026",
        plan_signature="a" * 64,
        max_events=3,
    )["batches"][0]
    batch_ref = _artifact_ref(
        artifacts,
        "s3://artifacts/summary-plan.json",
        {
            "kind": "espn-summary-batch-plan-v1",
            "schema_version": 1,
            "scope_binding_ref": {"uri": "binding", "sha256": "f" * 64},
            "expected_batch": expected,
            "checkpoint_uri": "s3://artifacts/summary-final.json",
        },
    )
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current"), attempt=2, signature="a" * 64
    )
    scope = SimpleNamespace(scope_id="700:2026", slug="eng.1", espn_id=700)
    descriptor = {"raw_store_uri": "s3://raw"}
    calls = []
    failed = False

    class Client:
        def fetch_json(self, _url, _endpoint, params, **kwargs):
            nonlocal failed
            event_id = params["event"]
            calls.append((event_id, kwargs["force_refresh"]))
            if event_id == 103 and not failed:
                failed = True
                raise RuntimeError("late request failed")
            return SimpleNamespace(event_id=event_id)

    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (
            artifacts[uri]
            if uri in artifacts
            else (_ for _ in ()).throw(FileNotFoundError(uri))
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_write_artifact",
        lambda uri, body, **_kwargs: artifacts.setdefault(uri, body),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda _ref: ({}, descriptor, loaded, scope, object()),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_heartbeat_scope_binding", lambda _ref: object()
    )
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore, "from_uri", lambda _uri: object()
    )
    monkeypatch.setattr(
        espn_native_tasks, "_http_client", lambda *_args, **_kwargs: Client()
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_raw_request_from_fetch",
        lambda **kwargs: {
            "request_id": kwargs["request_id"],
            "scope_id": kwargs["scope_id"],
            "endpoint": kwargs["endpoint"],
            "event_id": kwargs["event_id"],
            "url_fingerprint": "b" * 64,
            "raw_uri": f"s3://raw/{kwargs['request_id']}.json.gz",
            "raw_sha256": "c" * 64,
            "fetched_at": "2026-07-31T12:00:00Z",
            "http_status": 200,
            "direct_bytes": 1,
            "proxy_bytes": 0,
            "query_start": None,
            "query_end": None,
        },
    )

    with pytest.raises(RuntimeError, match="late request"):
        espn_native_tasks.fetch_summary_batch(summary_batch_ref=batch_ref)
    assert len([uri for uri in artifacts if "-requests/" in uri]) == 2

    result = espn_native_tasks.fetch_summary_batch(summary_batch_ref=batch_ref)

    assert calls == [(101, True), (102, True), (103, True), (103, True)]
    assert result["summary_phase_ref"]["uri"].endswith(".phase.json")


def test_scoreboard_mapped_retry_resumes_only_missing_exact_request(monkeypatch):
    from dags.utils import espn_native_tasks

    artifacts = {}
    requests = [
        SimpleNamespace(
            # Logical date order intentionally differs from the canonical
            # descriptor order used for set identity.
            request_id=f"scoreboard:{3 - day}",
            url="https://example.test/scoreboard",
            params={"day": day},
            query_start=f"2026-07-0{day}",
            query_end=f"2026-07-0{day}",
        )
        for day in (1, 2)
    ]
    expected = espn_native_tasks.seal_raw_batch_descriptor(
        endpoint="scoreboard",
        run_id="run-current",
        attempt=1,
        scope_id="700:2026",
        plan_signature="a" * 64,
        batch_id="scoreboard-batch",
        request_ids=[item.request_id for item in requests],
        event_ids=(),
    )
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current", as_of=datetime(2026, 7, 31).date()),
        attempt=1,
        signature="a" * 64,
        mode="daily",
        bindings={"700:2026": object()},
    )
    scope = SimpleNamespace(scope_id="700:2026", espn_id=700)
    descriptor = {
        "raw_store_uri": "s3://raw",
        "expected_scoreboard_batch": expected,
        "scoreboard_checkpoint_uri": "s3://artifacts/scoreboard-final.json",
        "scope_root": "s3://artifacts/scope",
    }
    calls = []
    failed = False

    class Client:
        def fetch_json(self, _url, _endpoint, params, **kwargs):
            nonlocal failed
            day = params["day"]
            calls.append((day, kwargs["force_refresh"]))
            if day == 2 and not failed:
                failed = True
                raise RuntimeError("late scoreboard failed")
            return SimpleNamespace(day=day)

    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (
            artifacts[uri]
            if uri in artifacts
            else (_ for _ in ()).throw(FileNotFoundError(uri))
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_write_artifact",
        lambda uri, body, **_kwargs: artifacts.setdefault(uri, body),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda _ref: ({}, descriptor, loaded, scope, object()),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_heartbeat_scope_binding", lambda _ref: object()
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_scoreboard_requests",
        lambda *_args, **_kwargs: requests,
    )
    monkeypatch.setattr(
        espn_native_tasks.runner, "_effective_mode", lambda _loaded: "daily"
    )
    monkeypatch.setattr(
        espn_native_tasks.EspnRawStore, "from_uri", lambda _uri: object()
    )
    monkeypatch.setattr(
        espn_native_tasks, "_http_client", lambda *_args, **_kwargs: Client()
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_raw_request_from_fetch",
        lambda **kwargs: {
            "request_id": kwargs["request_id"],
            "scope_id": kwargs["scope_id"],
            "endpoint": kwargs["endpoint"],
            "event_id": None,
            "url_fingerprint": "b" * 64,
            "raw_uri": f"s3://raw/{kwargs['request_id']}.json.gz",
            "raw_sha256": "c" * 64,
            "fetched_at": "2026-07-31T12:00:00Z",
            "http_status": 200,
            "direct_bytes": 1,
            "proxy_bytes": 0,
            "query_start": kwargs["query_start"],
            "query_end": kwargs["query_end"],
        },
    )

    binding_ref = {"uri": "binding", "sha256": "f" * 64}
    with pytest.raises(RuntimeError, match="late scoreboard"):
        espn_native_tasks.fetch_scoreboard_batch(scope_binding_ref=binding_ref)
    assert len([uri for uri in artifacts if "-requests/" in uri]) == 1

    espn_native_tasks.fetch_scoreboard_batch(scope_binding_ref=binding_ref)

    assert calls == [(1, True), (2, True), (2, True)]

    requests[1].request_id = "scoreboard:unexpected"
    with pytest.raises(
        espn_native_tasks.OperationsError, match="scoreboard request plan drift"
    ):
        espn_native_tasks.fetch_scoreboard_batch(scope_binding_ref=binding_ref)


def test_complete_head_requires_exact_snapshot_and_physical_complete(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.operations import ScopeHead

    published_at = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
    head = ScopeHead(
        dag_id="dag_ingest_espn",
        scope_id="700:2026",
        generation_id="generation-1",
        generation_signature="a" * 64,
        manifest_sha256="b" * 64,
        snapshot_uri="s3://artifacts/snapshot.json",
        snapshot_sha256="c" * 64,
        registry_signature="d" * 64,
        plan_signature="e" * 64,
        run_id="subject-run",
        published_at=published_at,
    )
    generation = SimpleNamespace(
        plan=SimpleNamespace(scope_id=head.scope_id),
        generation_id=head.generation_id,
        generation_signature=head.generation_signature,
        manifest_sha256=head.manifest_sha256,
        run_id=head.run_id,
        registry_signature=head.registry_signature,
        plan_signature=head.plan_signature,
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "load_scope_snapshot",
        lambda *_args, **_kwargs: generation,
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "EspnBronzeRepository",
        lambda: SimpleNamespace(
            verify_published_scope=lambda value: SimpleNamespace(
                passed=value is generation
            )
        ),
    )

    assert espn_native_tasks._verified_complete_head(head) == (head, "complete")
    generation.run_id = "prior-or-stale-run"
    assert espn_native_tasks._verified_complete_head(head) == (None, "incomplete")


def test_publish_scope_recovers_crash_after_complete_from_durable_intent(
    monkeypatch,
):
    from dags.utils import espn_native_tasks
    from scrapers.espn.operations import PublicationFence, ScopeHead
    from scrapers.espn.repository import ScopePublicationState

    artifacts = {}
    crash_evidence_once = True
    prepared_at = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
    logical_completed_at = prepared_at - timedelta(days=1)
    clock_values = iter(prepared_at.replace(second=second) for second in range(8))
    scope = SimpleNamespace(scope_id="700:2026")
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current", registry_signature="b" * 64),
        attempt=2,
        signature="a" * 64,
    )
    generation = SimpleNamespace(
        plan=scope,
        generation_id="generation-current",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
        run_id="run-current",
        registry_signature="b" * 64,
        plan_signature="a" * 64,
        ingested_at=logical_completed_at,
    )
    old_generation = SimpleNamespace(
        plan=scope,
        generation_id="generation-old",
        generation_signature="8" * 64,
        manifest_sha256="9" * 64,
        run_id="run-old",
        registry_signature="7" * 64,
        plan_signature="6" * 64,
        ingested_at=logical_completed_at - timedelta(days=1),
    )
    lease = SimpleNamespace(epoch=7)
    descriptor = {
        "dag_id": "dag_ingest_espn",
        "scope_root": "s3://artifacts/scope",
    }
    dq = {
        "scope_binding_ref": {"uri": "binding", "sha256": "e" * 64},
        "snapshot_ref": {
            "uri": "s3://artifacts/scope/generation.json",
            "sha256": "f" * 64,
        },
        "state": "complete",
        "quality": {"passed": True},
        "direct_requests": 3,
        "proxy_bytes": 0,
    }
    recorded = []
    order = []
    legacy_head = ScopeHead(
        dag_id=descriptor["dag_id"],
        scope_id=scope.scope_id,
        generation_id=old_generation.generation_id,
        generation_signature=old_generation.generation_signature,
        manifest_sha256=old_generation.manifest_sha256,
        snapshot_uri="s3://artifacts/scope/generation-old.json",
        snapshot_sha256="5" * 64,
        registry_signature=old_generation.registry_signature,
        plan_signature=old_generation.plan_signature,
        run_id=old_generation.run_id,
        published_at=prepared_at - timedelta(days=1),
        completed_at=None,
    )

    def write_artifact(uri, body, **_kwargs):
        nonlocal crash_evidence_once
        if uri.endswith("run-evidence.json") and crash_evidence_once:
            crash_evidence_once = False
            raise RuntimeError("crash after COMPLETE")
        existing = artifacts.setdefault(uri, body)
        if existing != body:
            raise AssertionError(f"immutable conflict at {uri}")

    class Store:
        def __init__(self):
            self.head = legacy_head

        def renew(self, current, **_kwargs):
            return current

        def read_scope_heads_owned(self, _leases, **_kwargs):
            return {scope.scope_id: self.head}

        def hydrate_head_completed_at(
            self, _lease, expected, *, completed_at, **_kwargs
        ):
            assert expected == self.head
            self.head = replace(expected, completed_at=completed_at)
            return self.head

        def publication_guard(self, _lease, **_kwargs):
            def record(head, evidence):
                recorded.append((head, evidence))
                if head is not None:
                    self.head = head
                return self.head

            fence = PublicationFence(
                lambda: None,
                record,
                lambda: next(clock_values),
            )
            return nullcontext(fence)

    store = Store()
    bronze_current = {"generation": old_generation}
    publication_states = iter(
        (ScopePublicationState.PUBLISHED, ScopePublicationState.IDEMPOTENT)
    )

    class Repository:
        def verify_current_scope_selection(self, current_generation):
            order.append(f"verify-{current_generation.generation_id}")
            if current_generation is not bronze_current["generation"]:
                raise RuntimeError("generation is not current")
            return {}

        def exact_complete_exists(self, current_generation):
            return current_generation is bronze_current["generation"]

        def publish_scope(self, current_generation, *, publication_fence):
            order.append("publish")
            publication_fence()
            state = next(publication_states)
            bronze_current["generation"] = current_generation
            return SimpleNamespace(
                state=state,
                manifest_sha256=current_generation.manifest_sha256,
            )

    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_args, **_kwargs: dq)
    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda _ref: ({}, descriptor, loaded, scope, lease),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "load_scope_snapshot",
        lambda uri, **_kwargs: (
            old_generation if uri == legacy_head.snapshot_uri else generation
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_assert_generation_binding", lambda **_: None
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "validate_scope_generation",
        lambda _generation: SimpleNamespace(passed=True),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_quality_payload", lambda _quality: {"passed": True}
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: store),
    )
    monkeypatch.setattr(espn_native_tasks, "EspnBronzeRepository", Repository)
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (
            artifacts[uri]
            if uri in artifacts
            else (_ for _ in ()).throw(FileNotFoundError(uri))
        ),
    )
    monkeypatch.setattr(espn_native_tasks.runner, "_write_artifact", write_artifact)

    with pytest.raises(RuntimeError, match="crash after COMPLETE"):
        espn_native_tasks.publish_scope(
            staging_dq_ref={"uri": "dq", "sha256": "0" * 64}
        )

    result = espn_native_tasks.publish_scope(
        staging_dq_ref={"uri": "dq", "sha256": "0" * 64}
    )

    assert result["publication_ref"]["uri"].endswith("publication-result.json")
    assert recorded[0][0].published_at == prepared_at
    assert recorded[0][0].completed_at == logical_completed_at
    assert recorded[0][0].completed_at != recorded[0][0].published_at
    assert recorded[0][1].recorded_at == prepared_at
    assert any(uri.endswith("publication-intent.json") for uri in artifacts)
    assert store.head.generation_id == generation.generation_id
    assert order == [
        "verify-generation-current",
        "verify-generation-old",
        "publish",
        "verify-generation-current",
        "verify-generation-current",
        "publish",
    ]


def test_publish_scope_reconciles_legacy_old_head_to_existing_current_complete(
    monkeypatch,
):
    from dags.utils import espn_native_tasks
    from scrapers.espn.operations import PublicationFence, ScopeHead
    from scrapers.espn.repository import ScopePublicationState

    artifacts = {}
    publication_time = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
    old_completed_at = publication_time - timedelta(days=2)
    new_completed_at = publication_time - timedelta(days=1)
    scope = SimpleNamespace(scope_id="700:2026")
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-new", registry_signature="b" * 64),
        attempt=1,
        signature="a" * 64,
    )
    old_generation = SimpleNamespace(
        plan=scope,
        generation_id="generation-old",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
        run_id="run-old",
        registry_signature="b" * 64,
        plan_signature="9" * 64,
        ingested_at=old_completed_at,
    )
    generation = SimpleNamespace(
        plan=scope,
        generation_id="generation-new",
        generation_signature="e" * 64,
        manifest_sha256="f" * 64,
        run_id="run-new",
        registry_signature="b" * 64,
        plan_signature="a" * 64,
        ingested_at=new_completed_at,
    )
    snapshot_ref = {
        "uri": "s3://artifacts/scope/generation-new.json",
        "sha256": "1" * 64,
    }
    old_head = ScopeHead(
        dag_id="dag_ingest_espn",
        scope_id=scope.scope_id,
        generation_id=old_generation.generation_id,
        generation_signature=old_generation.generation_signature,
        manifest_sha256=old_generation.manifest_sha256,
        snapshot_uri="s3://artifacts/scope/generation-old.json",
        snapshot_sha256="2" * 64,
        registry_signature=old_generation.registry_signature,
        plan_signature=old_generation.plan_signature,
        run_id=old_generation.run_id,
        published_at=publication_time - timedelta(days=1, hours=12),
        completed_at=None,
    )
    lease = SimpleNamespace(epoch=4)
    dq = {
        "scope_binding_ref": {"uri": "binding", "sha256": "3" * 64},
        "snapshot_ref": snapshot_ref,
        "state": "complete",
        "quality": {"passed": True},
        "direct_requests": 3,
        "proxy_bytes": 0,
    }
    descriptor = {
        "dag_id": "dag_ingest_espn",
        "scope_root": "s3://artifacts/scope",
    }
    intent_prepared_at = publication_time - timedelta(hours=6)
    intent_identity = espn_native_tasks._publication_intent_identity(
        dag_id=descriptor["dag_id"],
        loaded=loaded,
        scope=scope,
        generation=generation,
        snapshot_ref=snapshot_ref,
    )
    artifacts["s3://artifacts/scope/publication-intent.json"] = (
        espn_native_tasks._canonical_bytes(
            {
                "kind": "espn-publication-intent-v1",
                "schema_version": 1,
                **intent_identity,
                "prepared_at": intent_prepared_at.isoformat(),
            }
        )
    )
    verify_order = []
    publish_calls = []

    class Store:
        def __init__(self):
            self.head = old_head
            self.hydrated = False

        def renew(self, current, **_kwargs):
            return current

        def read_scope_heads_owned(self, _leases, **_kwargs):
            return {scope.scope_id: self.head}

        def hydrate_head_completed_at(
            self, _lease, expected, *, completed_at, **_kwargs
        ):
            assert expected == self.head
            assert completed_at == old_completed_at
            self.hydrated = True
            self.head = replace(expected, completed_at=completed_at)
            return self.head

        def publication_guard(self, _lease, **_kwargs):
            def record(head, _evidence):
                assert self.hydrated is True
                self.head = head
                return head

            return nullcontext(
                PublicationFence(lambda: None, record, lambda: publication_time)
            )

    store = Store()

    class Repository:
        def verify_current_scope_selection(self, current_generation):
            verify_order.append(current_generation.generation_id)
            if current_generation is old_generation:
                raise RuntimeError("old generation is no longer current")
            assert current_generation is generation
            return {"schedule": 1}

        def exact_complete_exists(self, current_generation):
            assert (
                current_generation is generation or current_generation is old_generation
            )
            return True

        def publish_scope(self, current_generation, *, publication_fence):
            assert current_generation is generation
            publish_calls.append(current_generation.generation_id)
            publication_fence()
            return SimpleNamespace(
                state=ScopePublicationState.IDEMPOTENT,
                manifest_sha256=current_generation.manifest_sha256,
            )

    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_a, **_k: dq)
    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda _ref: ({}, descriptor, loaded, scope, lease),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "load_scope_snapshot",
        lambda uri, **_kwargs: (
            old_generation if uri == old_head.snapshot_uri else generation
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_assert_generation_binding", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "validate_scope_generation",
        lambda _generation: SimpleNamespace(passed=True),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_quality_payload", lambda _quality: {"passed": True}
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: store),
    )
    monkeypatch.setattr(espn_native_tasks, "EspnBronzeRepository", Repository)
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (
            artifacts[uri]
            if uri in artifacts
            else (_ for _ in ()).throw(FileNotFoundError(uri))
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_write_artifact",
        lambda uri, body, **_kwargs: artifacts.setdefault(uri, body),
    )

    espn_native_tasks.publish_scope(staging_dq_ref={"uri": "dq", "sha256": "4" * 64})

    intent = json.loads(
        next(
            body
            for uri, body in artifacts.items()
            if uri.endswith("publication-intent.json")
        )
    )
    assert intent["schema_version"] == 1
    assert set(intent) == {
        "kind",
        "schema_version",
        *intent_identity,
        "prepared_at",
    }
    assert store.head.generation_id == generation.generation_id
    assert store.head.completed_at == new_completed_at
    assert store.head.published_at == intent_prepared_at
    assert publish_calls == [generation.generation_id]
    assert verify_order == [
        generation.generation_id,
        old_generation.generation_id,
        generation.generation_id,
    ]


def test_resumed_noop_hydrates_legacy_head_before_recording_evidence(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.operations import PublicationFence, ScopeHead

    completed_at = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
    scope = SimpleNamespace(scope_id="700:2026")
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-noop", registry_signature="b" * 64),
        attempt=1,
        signature="a" * 64,
    )
    generation = SimpleNamespace(
        plan=scope,
        generation_id="generation-prior",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
        run_id="prior-run",
        registry_signature="b" * 64,
        plan_signature="a" * 64,
        ingested_at=completed_at,
    )
    snapshot_ref = {"uri": "s3://scope/generation.json", "sha256": "f" * 64}
    head = ScopeHead(
        dag_id="dag_ingest_espn",
        scope_id=scope.scope_id,
        generation_id=generation.generation_id,
        generation_signature=generation.generation_signature,
        manifest_sha256=generation.manifest_sha256,
        snapshot_uri=snapshot_ref["uri"],
        snapshot_sha256=snapshot_ref["sha256"],
        registry_signature=generation.registry_signature,
        plan_signature=generation.plan_signature,
        run_id=generation.run_id,
        published_at=completed_at,
        completed_at=None,
    )
    lease = SimpleNamespace(epoch=4)
    payloads = []
    order = []

    class Store:
        def renew(self, current, **_kwargs):
            return current

        def read_scope_heads_owned(self, _leases, **_kwargs):
            return {scope.scope_id: head}

        def hydrate_head_completed_at(
            self, _lease, expected, *, completed_at, **_kwargs
        ):
            order.append("hydrate")
            return replace(expected, completed_at=completed_at)

        def publication_guard(self, _lease, **_kwargs):
            return nullcontext(
                PublicationFence(
                    lambda: None,
                    lambda published_head, _evidence: (
                        order.append("record") or published_head
                    ),
                    lambda: completed_at,
                )
            )

    class Repository:
        def exact_complete_exists(self, current_generation):
            assert current_generation is generation
            return True

        def verify_current_scope_selection(self, current_generation):
            order.append("verify-current")
            assert current_generation is generation
            return {}

        def publish_scope(self, *_args, **_kwargs):
            raise AssertionError("no-op must not append a COMPLETE manifest")

    dq = {
        "kind": "espn-staging-dq-result-v1",
        "scope_binding_ref": {"uri": "binding", "sha256": "e" * 64},
        "snapshot_ref": snapshot_ref,
        "state": "noop",
        "quality": {"passed": True},
        "direct_requests": 0,
        "proxy_bytes": 0,
    }
    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_a, **_k: dq)
    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda _ref: (
            {},
            {"dag_id": "dag_ingest_espn", "scope_root": "s3://scope"},
            loaded,
            scope,
            lease,
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "load_scope_snapshot",
        lambda *_args, **_kwargs: generation,
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (_ for _ in ()).throw(FileNotFoundError(uri)),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_assert_generation_binding", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "validate_scope_generation",
        lambda _generation: SimpleNamespace(passed=True),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_quality_payload", lambda _quality: {"passed": True}
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: Store()),
    )
    monkeypatch.setattr(espn_native_tasks, "EspnBronzeRepository", Repository)
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **_kwargs: (
            payloads.append(payload) or {"uri": uri, "sha256": "0" * 64}
        ),
    )

    espn_native_tasks.publish_scope(staging_dq_ref={"uri": "dq", "sha256": "1" * 64})

    publication = next(
        payload
        for payload in payloads
        if payload.get("kind") == "espn-publication-result-v1"
    )
    assert publication["selected_head"]["completed_at"] == completed_at.isoformat()
    assert order[:3] == ["verify-current", "hydrate", "record"]


@pytest.mark.parametrize(
    ("current_route", "current_mismatch"),
    ((None, False), ("legacy", False), ("native", False), ("native", True)),
)
def test_published_dq_verifies_current_views_for_the_active_route(
    monkeypatch, current_route, current_mismatch
):
    from dags.utils import espn_native_tasks
    from scrapers.espn.operations import ScopeHead

    completed_at = datetime(2026, 8, 1, 20, tzinfo=timezone.utc)
    scope = SimpleNamespace(scope_id="700:2026")
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="canary-run", registry_signature="a" * 64),
        attempt=1,
        signature="b" * 64,
    )
    generation = SimpleNamespace(
        plan=scope,
        generation_id="generation-complete",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
        run_id=loaded.plan.run_id,
        registry_signature=loaded.plan.registry_signature,
        plan_signature=loaded.signature,
        ingested_at=completed_at,
    )
    snapshot_ref = {
        "uri": "s3://artifacts/scope/generation.json",
        "sha256": "e" * 64,
    }
    head = ScopeHead(
        dag_id="dag_repair_espn",
        scope_id=scope.scope_id,
        generation_id=generation.generation_id,
        generation_signature=generation.generation_signature,
        manifest_sha256=generation.manifest_sha256,
        snapshot_uri=snapshot_ref["uri"],
        snapshot_sha256=snapshot_ref["sha256"],
        registry_signature=generation.registry_signature,
        plan_signature=generation.plan_signature,
        run_id=generation.run_id,
        published_at=completed_at + timedelta(minutes=1),
        completed_at=completed_at,
    )
    publication_ref = {
        "uri": "s3://artifacts/scope/publication-result.json",
        "sha256": "f" * 64,
    }
    evidence_ref = {
        "uri": "s3://artifacts/scope/run-evidence.json",
        "sha256": "1" * 64,
    }
    publication = {
        "kind": "espn-publication-result-v1",
        "scope_binding_ref": {"uri": "binding", "sha256": "2" * 64},
        "snapshot_ref": snapshot_ref,
        "publication_intent_ref": {"uri": "intent", "sha256": "3" * 64},
        "evidence_ref": evidence_ref,
        "state": "complete",
        "selected_head": espn_native_tasks._head_to_dict(head),
    }
    evidence = {"recorded_at": completed_at.isoformat()}
    descriptor = {
        "dag_id": head.dag_id,
        "scope_root": "s3://artifacts/scope",
    }
    lease = SimpleNamespace(epoch=7)
    repository_calls = []
    validation_calls = []
    writes = []

    class Store:
        def read_scope_heads(self, scope_ids):
            assert scope_ids == (scope.scope_id,)
            return {scope.scope_id: head}

    class Repository:
        def verify_published_scope(self, current_generation):
            assert current_generation is generation
            repository_calls.append("verify-published")
            return SimpleNamespace(passed=True, failures=())

        def current_scope_route(self, current_scope_id):
            assert current_scope_id == scope.scope_id
            repository_calls.append("current-route")
            return current_route

        def verify_current_scope_selection(self, current_generation):
            assert current_generation is generation
            repository_calls.append("verify-current")
            if current_mismatch:
                raise RuntimeError("native current selection differs")
            if current_route != "native":
                raise AssertionError("an unpromoted generation is not current")
            return {"schedule": 380, "lineup": 0, "matchsheet": 0}

        def verify_current_scope_absence(self, current_scope_id):
            assert current_scope_id == scope.scope_id
            repository_calls.append("verify-absence")
            return {"schedule": 0, "lineup": 0, "matchsheet": 0}

    def read_ref(ref, *, kind=None):
        if ref == publication_ref:
            assert kind == "espn-publication-result-v1"
            return publication
        if ref == evidence_ref:
            assert kind == "espn-run-manifest-evidence-v1"
            return evidence
        raise AssertionError(f"unexpected artifact read: {ref!r}")

    def load_scope_snapshot(uri, *, artifact_sha256, expected_scope_id):
        assert (uri, artifact_sha256, expected_scope_id) == (
            snapshot_ref["uri"],
            snapshot_ref["sha256"],
            scope.scope_id,
        )
        return generation

    monkeypatch.setattr(espn_native_tasks, "_read_ref", read_ref)
    monkeypatch.setattr(
        espn_native_tasks,
        "_binding",
        lambda _ref: ({}, descriptor, loaded, scope, lease),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner, "load_scope_snapshot", load_scope_snapshot
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_validate_publication_intent_for_result",
        lambda *args, **kwargs: validation_calls.append(("intent", args, kwargs)),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_assert_generation_binding",
        lambda **kwargs: validation_calls.append(("binding", kwargs)),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_validate_evidence_payload",
        lambda *args, **kwargs: validation_calls.append(("evidence", args, kwargs)),
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: Store()),
    )
    monkeypatch.setattr(espn_native_tasks, "EspnBronzeRepository", Repository)
    monkeypatch.setattr(
        espn_native_tasks,
        "_quality_payload",
        lambda report: {"passed": report.passed, "schedule_rows": 380},
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload: (
            writes.append((uri, payload)) or {"uri": uri, "sha256": "4" * 64}
        ),
    )

    if current_mismatch:
        with pytest.raises(RuntimeError, match="native current selection differs"):
            espn_native_tasks.published_dq_scope(publication_ref=publication_ref)
        assert repository_calls == [
            "verify-published",
            "verify-published",
            "current-route",
            "verify-current",
        ]
        assert [call[0] for call in validation_calls] == ["intent", "binding"]
        assert writes == []
        return

    result = espn_native_tasks.published_dq_scope(publication_ref=publication_ref)

    assert result["published_dq_ref"]["uri"].endswith("/published-dq.json")
    current_check = "verify-current" if current_route == "native" else "verify-absence"
    assert repository_calls == [
        "verify-published",
        "verify-published",
        "current-route",
        current_check,
    ]
    assert [call[0] for call in validation_calls] == [
        "intent",
        "binding",
        "evidence",
    ]
    assert writes == [
        (
            "s3://artifacts/scope/published-dq.json",
            {
                "kind": "espn-published-dq-result-v1",
                "schema_version": 1,
                "dag_id": head.dag_id,
                "scope_id": scope.scope_id,
                "run_id": loaded.plan.run_id,
                "attempt": loaded.attempt,
                "plan_signature": loaded.signature,
                "registry_signature": loaded.plan.registry_signature,
                "publication_ref": publication_ref,
                "current_selection": espn_native_tasks._head_to_dict(head),
                "quality": {"passed": True, "schedule_rows": 380},
            },
        )
    ]


def test_legacy_head_divergence_fails_before_hydration(monkeypatch):
    from dags.utils import espn_native_tasks
    from scrapers.espn.operations import ScopeHead

    logical = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
    head = ScopeHead(
        dag_id="dag_ingest_espn",
        scope_id="700:2026",
        generation_id="delayed-older",
        generation_signature="a" * 64,
        manifest_sha256="b" * 64,
        snapshot_uri="s3://scope/older.json",
        snapshot_sha256="c" * 64,
        registry_signature="d" * 64,
        plan_signature="e" * 64,
        run_id="older-run",
        published_at=logical + timedelta(days=1),
        completed_at=None,
    )
    generation = SimpleNamespace(
        plan=SimpleNamespace(scope_id=head.scope_id),
        generation_id=head.generation_id,
        generation_signature=head.generation_signature,
        manifest_sha256=head.manifest_sha256,
        run_id=head.run_id,
        registry_signature=head.registry_signature,
        plan_signature=head.plan_signature,
        ingested_at=logical,
    )
    candidate = SimpleNamespace(
        plan=SimpleNamespace(scope_id=head.scope_id),
        generation_id="unrelated-candidate",
        generation_signature="f" * 64,
        manifest_sha256="1" * 64,
        run_id="unrelated-run",
        registry_signature="2" * 64,
        plan_signature="3" * 64,
        ingested_at=logical + timedelta(hours=1),
    )
    hydrated = []
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "load_scope_snapshot",
        lambda *_args, **_kwargs: generation,
    )
    repository = SimpleNamespace(
        verify_current_scope_selection=lambda _generation: (_ for _ in ()).throw(
            RuntimeError("newer Bronze generation is current")
        )
    )
    store = SimpleNamespace(
        hydrate_head_completed_at=lambda *_args, **_kwargs: hydrated.append(True)
    )

    with pytest.raises(
        espn_native_tasks.OperationsError, match="repair the control head"
    ):
        espn_native_tasks._prepare_scope_head_for_publication(
            store=store,
            lease=SimpleNamespace(),
            head=head,
            repository=repository,
            state="complete",
            dag_id="dag_ingest_espn",
            generation=candidate,
            snapshot_ref={"uri": "candidate", "sha256": "4" * 64},
        )

    assert hydrated == []


def test_reclaimed_retry_accepts_exact_immutable_evidence_from_prior_epoch():
    from dags.utils import espn_native_tasks

    recorded_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-1", registry_signature="b" * 64),
        attempt=1,
        signature="a" * 64,
    )
    scope = SimpleNamespace(scope_id="700:2026")
    generation = SimpleNamespace(
        generation_id="generation-1",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
    )
    payload = espn_native_tasks._evidence_payload(
        dag_id="dag_ingest_espn",
        loaded=loaded,
        scope=scope,
        state="complete",
        generation=generation,
        lease=SimpleNamespace(epoch=7),
        recorded_at=recorded_at,
        publication_intent_ref={"uri": "intent", "sha256": "e" * 64},
    )

    espn_native_tasks._validate_evidence_payload(
        payload,
        dag_id="dag_ingest_espn",
        loaded=loaded,
        scope=scope,
        state="complete",
        generation=generation,
        lease=SimpleNamespace(epoch=8),
        recorded_at=recorded_at,
        publication_intent_ref={"uri": "intent", "sha256": "e" * 64},
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="not recoverable"):
        espn_native_tasks._validate_evidence_payload(
            payload,
            dag_id="dag_ingest_espn",
            loaded=loaded,
            scope=scope,
            state="complete",
            generation=generation,
            lease=SimpleNamespace(epoch=6),
            recorded_at=recorded_at,
            publication_intent_ref={"uri": "intent", "sha256": "e" * 64},
        )


def test_publication_intent_cannot_be_created_after_exact_complete(monkeypatch):
    from dags.utils import espn_native_tasks

    writes = []
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current", registry_signature="b" * 64),
        attempt=2,
        signature="a" * 64,
    )
    scope = SimpleNamespace(scope_id="700:2026")
    generation = SimpleNamespace(
        generation_id="generation-current",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
    )
    snapshot_ref = {"uri": "s3://artifacts/generation.json", "sha256": "e" * 64}
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (_ for _ in ()).throw(FileNotFoundError(uri)),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_write_artifact",
        lambda uri, body, **_kwargs: writes.append((uri, body)),
    )

    with pytest.raises(espn_native_tasks.OperationsError, match="predates intent"):
        espn_native_tasks._load_or_create_publication_intent(
            uri="s3://artifacts/publication-intent.json",
            dag_id="dag_ingest_espn",
            loaded=loaded,
            scope=scope,
            generation=generation,
            snapshot_ref=snapshot_ref,
            prepared_at=datetime(2026, 7, 31, 12, tzinfo=timezone.utc),
            complete_exists=lambda: True,
        )

    assert writes == []


def test_publication_intent_reference_is_revalidated_downstream(monkeypatch):
    from dags.utils import espn_native_tasks

    artifacts = {}
    loaded = SimpleNamespace(
        plan=SimpleNamespace(run_id="run-current", registry_signature="b" * 64),
        attempt=2,
        signature="a" * 64,
    )
    scope = SimpleNamespace(scope_id="700:2026")
    generation = SimpleNamespace(
        generation_id="generation-current",
        generation_signature="c" * 64,
        manifest_sha256="d" * 64,
    )
    snapshot_ref = {"uri": "s3://artifacts/generation.json", "sha256": "e" * 64}
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_read_artifact",
        lambda uri: (
            artifacts[uri]
            if uri in artifacts
            else (_ for _ in ()).throw(FileNotFoundError(uri))
        ),
    )
    monkeypatch.setattr(
        espn_native_tasks.runner,
        "_write_artifact",
        lambda uri, body, **_kwargs: artifacts.setdefault(uri, body),
    )
    _, intent_ref, _ = espn_native_tasks._load_or_create_publication_intent(
        uri="s3://artifacts/publication-intent.json",
        dag_id="dag_ingest_espn",
        loaded=loaded,
        scope=scope,
        generation=generation,
        snapshot_ref=snapshot_ref,
        prepared_at=datetime(2026, 7, 31, 12, tzinfo=timezone.utc),
        complete_exists=lambda: False,
    )
    loaded.plan.run_id = "different-run"

    with pytest.raises(espn_native_tasks.OperationsError, match="intent identity"):
        espn_native_tasks._validate_publication_intent_ref(
            intent_ref,
            dag_id="dag_ingest_espn",
            loaded=loaded,
            scope=scope,
            generation=generation,
            snapshot_ref=snapshot_ref,
        )


def test_monitor_uses_database_clock_and_distinguishes_missing_subject(monkeypatch):
    from dags.utils import espn_native_tasks

    logical = datetime(2026, 7, 1, tzinfo=timezone.utc)
    database_now = datetime(2026, 7, 31, 13, tzinfo=timezone.utc)
    observed = []

    class Store:
        def migrate(self):
            pass

        def read_scope_heads(self, _scope_ids):
            return {}

        def current_time(self):
            return database_now

    registry = SimpleNamespace(signature=lambda: "a" * 64)
    monkeypatch.setattr(espn_native_tasks, "load_registry", lambda _path: registry)
    monkeypatch.setattr(
        espn_native_tasks, "_selected_scopes", lambda *_args: ("700:2026",)
    )
    monkeypatch.setattr(
        espn_native_tasks.PostgresEspnControlStore,
        "from_env",
        classmethod(lambda _cls: Store()),
    )
    monkeypatch.setattr(
        espn_native_tasks, "_latest_discovery_flags", lambda _scope: (False, False)
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "evaluate_alerts",
        lambda snapshot, *, observed_at: observed.append((snapshot, observed_at)) or (),
    )
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **_kwargs: {"uri": uri, "sha256": "f" * 64},
    )
    monkeypatch.setenv(espn_native_tasks.ARTIFACT_ROOT_ENV, "s3://artifacts")
    context = {
        "dag": SimpleNamespace(dag_id="dag_monitor_espn"),
        "run_id": "monitor-run",
        "logical_date": logical,
        "params": {"attempt": 1},
    }

    espn_native_tasks.check_36h_freshness_and_alerts(**context)

    snapshot, observed_at = observed[0]
    assert observed_at == database_now
    assert snapshot["run_id"] == "monitor-run"
    assert snapshot["subject_dag_id"] is None
    assert snapshot["subject_run_id"] is None
    assert snapshot["identity_kind"] == "monitor-subject"


def test_first_discovery_bootstraps_source_only_catalog_without_empty_diff():
    from dags.utils import espn_native_tasks
    from scrapers.espn.discovery import CatalogCandidate, CatalogSnapshot, diff_catalogs
    from scrapers.espn.models import (
        AgeClass,
        CapabilityState,
        EntityCapabilities,
        Gender,
    )

    capabilities = EntityCapabilities(
        CapabilityState.PROVEN, CapabilityState.PROVEN, CapabilityState.PROVEN
    )
    promoted = SimpleNamespace(
        espn_id=700,
        slug="eng.1",
        name="Premier League",
        gender=Gender.MALE,
        age_class=AgeClass.SENIOR,
        gender_evidence=("reviewed",),
        current_edition=SimpleNamespace(
            source_season_year=2026,
            display_name="2026-27 Premier League",
            start_date=datetime(2026, 7, 1).date(),
            end_date=datetime(2027, 6, 30).date(),
            capabilities=capabilities,
        ),
    )
    current = CatalogSnapshot(
        captured_at="2026-07-31T12:00:00+00:00",
        candidates=(
            CatalogCandidate(
                espn_id=700,
                slug="eng.1",
                name="Premier League",
                group="England",
                source_order=0,
                gender=Gender.MALE,
                source_season_year=2026,
                edition_display_name="2026-27 Premier League",
                start_date="2026-07-01",
                end_date="2027-06-30",
                capabilities=capabilities,
            ),
            CatalogCandidate(
                espn_id=999,
                slug="existing.extra",
                name="Existing Extra League",
                group="Other",
                source_order=1,
            ),
        ),
    )
    registry = SimpleNamespace(by_id={700: promoted}, competitions=(promoted,))

    baseline = espn_native_tasks._promoted_registry_baseline(current, registry)

    assert diff_catalogs(baseline, current).changes == ()


@pytest.mark.parametrize(
    ("artifact_dag_id", "artifact_run_id"),
    [("dag_repair_espn", "current-run"), ("dag_ingest_espn", "prior-run")],
)
def test_terminal_rejects_stale_or_prior_plan_artifact(
    monkeypatch, artifact_dag_id, artifact_run_id
):
    from dags.utils import espn_native_tasks

    index = {
        "kind": "espn-plan-index-v1",
        "dag_id": artifact_dag_id,
        "run_id": artifact_run_id,
        "attempt": 1,
        "scope_ids": [],
        "scope_plan_refs": [],
        "mode": "daily",
        "registry_signature": "a" * 64,
    }
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(espn_native_tasks, "_read_ref", lambda *_args, **_kwargs: index)
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **_kwargs: {"uri": uri, "sha256": "f" * 64},
    )
    context = {
        "dag": SimpleNamespace(dag_id="dag_ingest_espn"),
        "dag_run": SimpleNamespace(get_task_instances=lambda: ()),
        "run_id": "current-run",
        "logical_date": datetime(2026, 7, 31, tzinfo=timezone.utc),
        "params": {"attempt": 1},
    }

    with pytest.raises(Exception, match="terminal plan identity mismatch"):
        espn_native_tasks.terminal_verdict(
            producer_task_ids=("validate_registry_and_admission",),
            plan_index_ref={"uri": "index", "sha256": "b" * 64},
            **context,
        )


def test_final_leaf_seals_success_only_after_health_and_release(monkeypatch):
    from dags.utils import espn_native_tasks

    verdict_ref = {"uri": "verdict", "sha256": "1" * 64}
    descriptor_ref = {"uri": "descriptor", "sha256": "2" * 64}
    payloads = {
        "verdict": {
            "dag_id": "dag_ingest_espn",
            "run_id": "child-run",
            "attempt": 1,
            "status": "complete",
            "failures": [],
        },
        "admission.json": {
            "dag_id": "dag_ingest_espn",
            "run_id": "child-run",
            "attempt": 1,
            "mode": "daily",
            "as_of": "2026-07-31",
            "logical_date": "2026-07-31T00:00:00+00:00",
            "parent": {"schema": "espn-master-parent-v1"},
            "registry_ref": {"uri": "registry", "sha256": "3" * 64},
        },
        "plan-index.json": {
            "dag_id": "dag_ingest_espn",
            "run_id": "child-run",
            "attempt": 1,
            "mode": "daily",
            "scope_ids": ["700:2026"],
            "registry_signature": "4" * 64,
            "scope_plan_refs": [descriptor_ref],
        },
        "durable-run-manifest.json": {
            "dag_id": "dag_ingest_espn",
            "run_id": "child-run",
            "attempt": 1,
        },
        "health.json": {
            "run_id": "child-run",
            "attempt": 1,
            "status": "complete",
            "verdict_ref": verdict_ref,
            "alerts": [],
        },
        "lease-release.json": {
            "dag_id": "dag_ingest_espn",
            "run_id": "child-run",
            "attempt": 1,
            "released": ["700:2026"],
            "failures": [],
        },
        "descriptor": {
            "scope_id": "700:2026",
            "scope_root": "s3://artifacts/run/scopes/700-2026",
        },
    }

    def ref_for_uri(uri):
        return {"uri": uri, "sha256": hashlib.sha256(uri.encode()).hexdigest()}

    def read_ref(ref, **_kwargs):
        return payloads[ref["uri"].rsplit("/", 1)[-1]]

    written = {}
    monkeypatch.setattr(espn_native_tasks, "_artifact_root", lambda: "s3://artifacts")
    monkeypatch.setattr(espn_native_tasks, "_ref_for_uri", ref_for_uri)
    monkeypatch.setattr(espn_native_tasks, "_read_ref", read_ref)
    monkeypatch.setattr(
        espn_native_tasks,
        "_write_payload",
        lambda uri, payload, **_kwargs: (
            written.update(uri=uri, payload=payload) or ref_for_uri(uri)
        ),
    )
    task_instances = (
        SimpleNamespace(task_id="record_health_metrics", state="success"),
        SimpleNamespace(task_id="release_scope_leases", state="success"),
    )
    context = {
        "dag": SimpleNamespace(dag_id="dag_ingest_espn"),
        "dag_run": SimpleNamespace(get_task_instances=lambda: task_instances),
        "run_id": "child-run",
        "logical_date": datetime(2026, 7, 31, tzinfo=timezone.utc),
        "params": {"attempt": 1},
    }

    result = espn_native_tasks.propagate_terminal_failure(
        verdict_ref=verdict_ref,
        cleanup_task_ids=("record_health_metrics", "release_scope_leases"),
        **context,
    )

    assert result["success_receipt_ref"]["uri"].endswith("/run-success.json")
    assert written["payload"]["kind"] == "espn-run-success-receipt-v1"
    assert written["payload"]["health_ref"]["uri"].endswith("/health.json")
    assert written["payload"]["lease_release_ref"]["uri"].endswith(
        "/lease-release.json"
    )
    assert len(written["payload"]["receipt_sha256"]) == 64
