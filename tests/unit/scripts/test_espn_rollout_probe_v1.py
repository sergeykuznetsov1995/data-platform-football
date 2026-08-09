from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
from io import StringIO
import json
from pathlib import Path
import subprocess
import sys

import pytest

from scrapers.espn.layout import (
    COMPACT6_INTERNAL_REQUIRED_OBJECTS,
    COMPACT6_PUBLIC_OBJECTS,
    LEGACY14_PUBLIC_OBJECTS,
)
from scripts.espn_rollout_probe_v1 import (
    ARM_ORDER,
    ESPN_DAG_IDS,
    EXPECTED_PARSER_VERSION,
    EXPECTED_RUNTIME_VERSION,
    KNOWN_LEAGUES_CUP_EVENT_IDS,
    READ_METHODS,
    main,
    run_probe,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 8, 15, 0, tzinfo=UTC)
ARM_NOW = datetime(2026, 8, 8, 14, 0, tzinfo=UTC)
REGISTRY_SIGNATURE = "a" * 64
KNOWN_SCOPE = "19425:2026"


def _scope_ids(count: int = 181) -> list[str]:
    return sorted([KNOWN_SCOPE, *(f"{index}:2026" for index in range(1, count))])


def _canonical_bytes(value, *, newline=False):
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    if newline:
        encoded += "\n"
    return encoded.encode()


def _target_sha256(scope_ids):
    return hashlib.sha256(_canonical_bytes(scope_ids)).hexdigest()


TARGET_SHA256 = _target_sha256(_scope_ids())


def _inventory(objects, schema):
    return [
        {
            "table_schema": schema,
            "table_name": name,
            "table_type": kind,
        }
        for name, kind in objects.items()
    ]


def _ref(label):
    return {
        "uri": f"s3://espn/{label}.json",
        "sha256": hashlib.sha256(label.encode()).hexdigest(),
    }


def _success_receipt(scope_ids):
    release_identity = {
        "release_commit": "d" * 40,
        "release_tree_sha256": "e" * 64,
        "registry_signature": REGISTRY_SIGNATURE,
        "target_scope_sha256": _target_sha256(scope_ids),
    }
    release = {
        **release_identity,
        "target_scope_ids": scope_ids,
        "campaign_id": hashlib.sha256(_canonical_bytes(release_identity)).hexdigest(),
        "parser_version": "espn-native-parser-v3",
        "runtime_version": "espn-native-runtime-v4",
    }
    parent_run_id = "scheduled__2026-08-08T14:00:00+00:00"
    child_run_id = f"espn_daily__dag_trigger_espn_daily__{parent_run_id}"
    qualification_scopes = []
    for scope_id in scope_ids:
        events = []
        if scope_id == KNOWN_SCOPE:
            events = [
                {
                    "event_id": event_id,
                    "played_final": True,
                    "summary_required": True,
                    "entities": {
                        entity: {"state": "captured", "failures": []}
                        for entity in ("lineup", "matchsheet")
                    },
                    "failures": [],
                }
                for event_id in KNOWN_LEAGUES_CUP_EVENT_IDS
            ]
        qualification_scopes.append(
            {
                "scope_id": scope_id,
                "outcome": "complete_new",
                "schedule": {"state": "captured", "failures": []},
                "events": events,
                "raw_evidence": [
                    {
                        "request_id": f"scoreboard:{scope_id}",
                        "endpoint": "scoreboard",
                        "event_id": None,
                        "state": "captured",
                        "raw_uri": f"s3://raw/espn/{scope_id}.json",
                        "raw_sha256": hashlib.sha256(scope_id.encode()).hexdigest(),
                    }
                ],
                "failures": [],
            }
        )
    receipt = {
        "kind": "espn-run-success-receipt-v1",
        "schema_version": 1,
        "dag_id": "dag_ingest_espn",
        "run_id": child_run_id,
        "attempt": 1,
        "mode": "daily",
        "as_of": "2026-08-08",
        "logical_date": "2026-08-08T14:00:00+00:00",
        "parent": {
            "schema": "espn-daily-parent-v2",
            "owner_profile": "espn-isolated-v1",
            "parent_dag_id": "dag_trigger_espn_daily",
            "parent_task_id": "trigger_espn_ingest",
            "parent_run_id": parent_run_id,
            "parent_run_type": "scheduled",
            "logical_date": "2026-08-08T14:00:00+00:00",
            "data_interval_start": "2026-08-08T14:00:00+00:00",
            "data_interval_end": "2026-08-09T14:00:00+00:00",
            "child_dag_id": "dag_ingest_espn",
            "child_run_id": child_run_id,
        },
        "scope_ids": scope_ids,
        "registry_ref": _ref("registry"),
        "registry_signature": REGISTRY_SIGNATURE,
        "release": release,
        "canary_campaign": None,
        "qualification": {
            "scope_count": len(scope_ids),
            "complete_new": len(scope_ids),
            "noop_revalidated": 0,
            "failures": [],
            "scopes": qualification_scopes,
        },
        "admission_ref": _ref("admission"),
        "plan_index_ref": _ref("plan-index"),
        "durable_manifest_ref": _ref("durable-run-manifest"),
        "published_dq_refs": [
            {"scope_id": scope_id, "published_dq_ref": _ref(f"dq-{scope_id}")}
            for scope_id in scope_ids
        ],
        "verdict_ref": _ref("verdict"),
        "health_ref": _ref("health"),
        "lease_release_ref": _ref("lease-release"),
    }
    receipt["receipt_sha256"] = hashlib.sha256(
        _canonical_bytes(receipt, newline=True)
    ).hexdigest()
    return {
        "artifact": receipt,
        "completed_at": (NOW - timedelta(hours=1)).isoformat(),
    }


def _healthy_snapshot() -> dict:
    scope_ids = _scope_ids()
    receipt = _success_receipt(scope_ids)
    return {
        "container": {
            "name": "espn-airflow-airflow-metadb-1",
            "status": "running",
            "health": "healthy",
        },
        "ui_health": {
            "url": "http://127.0.0.1:8086/health",
            "status_code": 200,
            "body": {
                "metadatabase": {"status": "healthy"},
                "scheduler": {"status": "healthy"},
                "triggerer": None,
                "dag_processor": None,
            },
        },
        "dags": {dag_id: True for dag_id in ESPN_DAG_IDS},
        "parent_child": {
            "parent_created": True,
            "parent_dag_id": "dag_trigger_espn_daily",
            "parent_run_id": "scheduled__2026-08-08T14:00:00+00:00",
            "parent_run_type": "scheduled",
            "parent_logical_date": "2026-08-08T14:00:00+00:00",
            "parent_data_interval_start": "2026-08-08T14:00:00+00:00",
            "parent_data_interval_end": "2026-08-09T14:00:00+00:00",
            "parent_state": "success",
            "child_dag_id": "dag_ingest_espn",
            "child_run_id": (
                "espn_daily__dag_trigger_espn_daily__"
                "scheduled__2026-08-08T14:00:00+00:00"
            ),
            "child_state": "success",
        },
        "receipt": receipt,
        "registry": {
            "configured_signature": REGISTRY_SIGNATURE,
            "frozen_signature": REGISTRY_SIGNATURE,
            "configured_scope_ids": scope_ids,
            "frozen_scope_ids": scope_ids,
            "target_scope_sha256": TARGET_SHA256,
            "frozen_target_scope_sha256": TARGET_SHA256,
        },
        "target": {
            "scope_ids": scope_ids,
            "target_scope_sha256": TARGET_SHA256,
        },
        "scope_heads": [
            {
                "scope_id": scope_id,
                "state": "complete",
                "registry_signature": REGISTRY_SIGNATURE,
                "target_scope_sha256": TARGET_SHA256,
                "parser_version": "espn-native-parser-v3",
                "runtime_version": "espn-native-runtime-v4",
                "physical_verified": True,
                "last_complete_at": (NOW - timedelta(hours=2)).isoformat(),
            }
            for scope_id in scope_ids
        ],
        "dispositions": receipt["artifact"]["qualification"],
        "active_leases": [],
        "known_events": {
            "scope_id": KNOWN_SCOPE,
            "event_ids": list(KNOWN_LEAGUES_CUP_EVENT_IDS),
        },
        "layout": {
            "layout_mode": "legacy14",
            "inventory": _inventory(LEGACY14_PUBLIC_OBJECTS, "bronze"),
            "serving_relation": "iceberg.bronze.espn_schedule_current",
            "serving_readable": True,
            "parity": {},
        },
    }


class InjectedReaders:
    def __init__(self, snapshot=None, *, errors=None):
        self.snapshot = deepcopy(snapshot or _healthy_snapshot())
        self.errors = dict(errors or {})
        self.calls = []

    def _read(self, name):
        self.calls.append(name)
        if name in self.errors:
            raise self.errors[name]
        return deepcopy(self.snapshot[name.removeprefix("read_")])


for _method_name in READ_METHODS:
    setattr(
        InjectedReaders,
        _method_name,
        lambda self, _name=_method_name: self._read(_name),
    )


def _run(snapshot=None, *, now=NOW, errors=None):
    readers = InjectedReaders(snapshot, errors=errors)
    report = run_probe(readers, observed_at=now)
    return report, readers


def _results(report):
    return {item["code"]: item for item in report["results"]}


def _pause_result(snapshot, *, now=ARM_NOW):
    report, _ = _run(snapshot, now=now)
    return _results(report)["airflow.pause_posture"]


@pytest.mark.unit
def test_all_paused_healthy_report_is_versioned_structured_and_json_safe():
    report, readers = _run()

    assert report["kind"] == "espn-rollout-probe-v1"
    assert report["schema_version"] == 1
    assert report["status"] == "ok"
    assert [item["code"] for item in report["results"]] == [
        "runtime.metadb_container",
        "runtime.ui_health",
        "airflow.dag_inventory",
        "airflow.pause_posture",
        "airflow.parent_child",
        "artifact.final_receipt",
        "registry.frozen",
        "target.exact_181",
        "heads.physical_versions",
        "qualification.dispositions",
        "freshness.per_scope",
        "leases.zero_active",
        "events.leagues_cup_five",
        "serving.layout_parity",
    ]
    assert all(item["status"] == "ok" for item in report["results"])
    assert readers.calls == list(READ_METHODS)
    json.dumps(report, sort_keys=True)


@pytest.mark.unit
def test_frozen_probe_versions_match_the_release_runtime_contract():
    from scrapers.espn import runner
    from scrapers.espn.parser_contracts import PARSER_VERSION

    assert EXPECTED_PARSER_VERSION == PARSER_VERSION
    assert EXPECTED_RUNTIME_VERSION == runner.RUNTIME_VERSION


@pytest.mark.unit
@pytest.mark.parametrize("mutation", ["missing", "unexpected"])
def test_dag_inventory_must_be_exactly_the_reviewed_seven(mutation):
    snapshot = _healthy_snapshot()
    if mutation == "missing":
        snapshot["dags"].pop("dag_replay_espn")
    else:
        snapshot["dags"]["dag_master_pipeline"] = True

    report, _ = _run(snapshot)

    assert _results(report)["airflow.dag_inventory"]["status"] == "fail"


@pytest.mark.unit
@pytest.mark.parametrize("prefix_length", range(len(ARM_ORDER) + 1))
def test_arm_window_accepts_only_each_ordered_unpaused_prefix(prefix_length):
    snapshot = _healthy_snapshot()
    snapshot["parent_child"] = {
        "parent_created": False,
        "parent_dag_id": "dag_trigger_espn_daily",
        "parent_run_id": None,
        "parent_state": None,
        "child_dag_id": "dag_ingest_espn",
        "child_run_id": None,
        "child_state": None,
    }
    for dag_id in ARM_ORDER[:prefix_length]:
        snapshot["dags"][dag_id] = False

    assert _pause_result(snapshot)["status"] == "ok"


@pytest.mark.unit
@pytest.mark.parametrize(
    "unpaused",
    [
        {"dag_monitor_espn"},
        {"dag_ingest_espn", "dag_discover_espn_registry"},
        {"dag_ingest_espn", "dag_monitor_espn", "dag_trigger_espn_daily"},
        {"dag_repair_espn"},
    ],
)
def test_arm_window_rejects_illegal_prefixes(unpaused):
    snapshot = _healthy_snapshot()
    snapshot["parent_child"]["parent_created"] = False
    for dag_id in unpaused:
        snapshot["dags"][dag_id] = False

    result = _pause_result(snapshot)

    assert result["status"] == "fail"
    assert result["severity"] == "hard"


@pytest.mark.unit
def test_parent_created_posture_pauses_parent_while_exact_child_drains():
    snapshot = _healthy_snapshot()
    for dag_id in ARM_ORDER[:3]:
        snapshot["dags"][dag_id] = False
    snapshot["parent_child"]["child_state"] = "running"

    report, _ = _run(snapshot, now=ARM_NOW)
    results = _results(report)

    assert results["airflow.pause_posture"]["status"] == "ok"
    assert results["airflow.parent_child"]["status"] == "ok"


@pytest.mark.unit
def test_unpaused_dag_outside_half_open_arm_window_is_hard_failure():
    for now in (
        datetime(2026, 8, 8, 13, 49, 59, tzinfo=UTC),
        datetime(2026, 8, 8, 14, 15, tzinfo=UTC),
    ):
        snapshot = _healthy_snapshot()
        snapshot["dags"]["dag_ingest_espn"] = False
        assert _pause_result(snapshot, now=now)["status"] == "fail"


@pytest.mark.unit
def test_unknown_arm_prerequisite_is_unknown_never_green():
    snapshot = _healthy_snapshot()
    snapshot["dags"]["dag_ingest_espn"] = False
    snapshot["parent_child"]["parent_created"] = None

    result = _pause_result(snapshot)

    assert result["status"] == "unknown"
    assert result["severity"] == "hard"


@pytest.mark.unit
def test_absent_parent_evidence_outside_arm_window_is_not_green():
    snapshot = _healthy_snapshot()
    snapshot["parent_child"] = {
        "parent_created": False,
        "parent_dag_id": "dag_trigger_espn_daily",
        "parent_run_id": None,
        "parent_state": None,
        "child_dag_id": "dag_ingest_espn",
        "child_run_id": None,
        "child_state": None,
    }

    report, _ = _run(snapshot)

    assert _results(report)["airflow.parent_child"]["status"] == "unknown"


@pytest.mark.unit
def test_failed_or_wrongly_derived_child_fails_parent_child_identity():
    for change in (
        {"child_state": "failed"},
        {"child_run_id": "manual__forged"},
        {"child_dag_id": "dag_repair_espn"},
    ):
        snapshot = _healthy_snapshot()
        snapshot["parent_child"].update(change)
        report, _ = _run(snapshot)
        assert _results(report)["airflow.parent_child"]["status"] == "fail"


@pytest.mark.unit
@pytest.mark.parametrize("identity", ["manual", "wrong_hour", "wrong_interval"])
def test_parent_must_be_the_standard_1400_utc_scheduler_interval(identity):
    snapshot = _healthy_snapshot()
    parent = snapshot["parent_child"]
    if identity == "manual":
        parent["parent_run_id"] = "manual__operator"
        parent["parent_run_type"] = "manual"
    elif identity == "wrong_hour":
        parent["parent_run_id"] = "scheduled__2026-08-08T08:00:00+00:00"
        parent["parent_logical_date"] = "2026-08-08T08:00:00+00:00"
        parent["parent_data_interval_start"] = "2026-08-08T08:00:00+00:00"
        parent["parent_data_interval_end"] = "2026-08-09T08:00:00+00:00"
    else:
        parent["parent_data_interval_end"] = "2026-08-10T14:00:00+00:00"
    parent["child_run_id"] = (
        "espn_daily__dag_trigger_espn_daily__" + parent["parent_run_id"]
    )

    report, _ = _run(snapshot)

    assert _results(report)["airflow.parent_child"]["status"] == "fail"


@pytest.mark.unit
def test_actual_run_success_artifact_shape_and_checksum_are_accepted_directly():
    snapshot = _healthy_snapshot()
    snapshot["receipt"] = snapshot["receipt"]["artifact"]

    report, _ = _run(snapshot)

    assert _results(report)["artifact.final_receipt"]["status"] == "ok"


@pytest.mark.unit
def test_rehashed_or_unrehashed_receipt_tamper_is_rejected():
    for reseal in (False, True):
        snapshot = _healthy_snapshot()
        artifact = snapshot["receipt"]["artifact"]
        artifact["release"]["target_scope_sha256"] = "f" * 64
        if reseal:
            unsigned = {
                key: value for key, value in artifact.items() if key != "receipt_sha256"
            }
            artifact["receipt_sha256"] = hashlib.sha256(
                _canonical_bytes(unsigned, newline=True)
            ).hexdigest()

        report, _ = _run(snapshot)

        assert _results(report)["artifact.final_receipt"]["status"] == "fail"


@pytest.mark.unit
def test_stale_receipt_and_each_stale_scope_are_reported_independently():
    snapshot = _healthy_snapshot()
    snapshot["receipt"]["completed_at"] = (NOW - timedelta(hours=36)).isoformat()
    stale_scope = snapshot["scope_heads"][17]["scope_id"]
    snapshot["scope_heads"][17]["last_complete_at"] = (
        NOW - timedelta(hours=36, seconds=1)
    ).isoformat()

    report, _ = _run(snapshot)
    results = _results(report)

    assert results["artifact.final_receipt"]["status"] == "fail"
    assert results["freshness.per_scope"]["status"] == "fail"
    per_scope = {
        item["scope_id"]: item
        for item in results["freshness.per_scope"]["details"]["scopes"]
    }
    assert per_scope[stale_scope]["status"] == "fail"
    assert per_scope[KNOWN_SCOPE]["status"] == "ok"


@pytest.mark.unit
def test_active_lease_is_a_separate_hard_failure():
    snapshot = _healthy_snapshot()
    snapshot["active_leases"] = [
        {"scope_id": KNOWN_SCOPE, "owner_id": "dag_ingest_espn/run/1"}
    ]

    report, _ = _run(snapshot)

    assert _results(report)["leases.zero_active"]["status"] == "fail"


@pytest.mark.unit
def test_registry_drift_is_not_masked_by_exact_target():
    snapshot = _healthy_snapshot()
    snapshot["registry"]["configured_signature"] = "d" * 64

    report, _ = _run(snapshot)
    results = _results(report)

    assert results["registry.frozen"]["status"] == "fail"
    assert results["target.exact_181"]["status"] == "ok"


@pytest.mark.unit
@pytest.mark.parametrize("count", [180, 182])
def test_target_must_be_exactly_181_unique_scopes(count):
    snapshot = _healthy_snapshot()
    snapshot["target"]["scope_ids"] = _scope_ids(count)

    report, _ = _run(snapshot)

    assert _results(report)["target.exact_181"]["status"] == "fail"


@pytest.mark.unit
def test_duplicate_target_scope_does_not_satisfy_exact_181():
    snapshot = _healthy_snapshot()
    snapshot["target"]["scope_ids"][-1] = snapshot["target"]["scope_ids"][0]

    report, _ = _run(snapshot)

    assert _results(report)["target.exact_181"]["status"] == "fail"


@pytest.mark.unit
@pytest.mark.parametrize("mutation", ["hash", "membership", "order"])
def test_target_hash_is_derived_from_the_canonical_scope_identity(mutation):
    snapshot = _healthy_snapshot()
    if mutation == "hash":
        snapshot["target"]["target_scope_sha256"] = "f" * 64
    elif mutation == "membership":
        scopes = snapshot["target"]["scope_ids"]
        scopes[0] = "999999:2026"
        scopes.sort()
    else:
        snapshot["target"]["scope_ids"].reverse()

    report, _ = _run(snapshot)

    assert _results(report)["target.exact_181"]["status"] == "fail"


@pytest.mark.unit
def test_mixed_or_unverified_physical_heads_fail_closed():
    for change in (
        {"parser_version": "espn-native-parser-v2"},
        {"runtime_version": "espn-native-runtime-v3"},
        {"physical_verified": False},
    ):
        snapshot = _healthy_snapshot()
        snapshot["scope_heads"][37].update(change)
        report, _ = _run(snapshot)
        assert _results(report)["heads.physical_versions"]["status"] == "fail"


@pytest.mark.unit
@pytest.mark.parametrize("state", ["planned", "failed", "skipped", "quarantined"])
def test_nonterminal_or_failed_dispositions_are_invalid(state):
    snapshot = _healthy_snapshot()
    known = next(
        scope
        for scope in snapshot["dispositions"]["scopes"]
        if scope["scope_id"] == KNOWN_SCOPE
    )
    known["events"][0]["entities"]["lineup"]["state"] = state

    report, _ = _run(snapshot)

    assert _results(report)["qualification.dispositions"]["status"] == "fail"


@pytest.mark.unit
def test_known_schedule_ids_without_event_entity_dispositions_fail_closed():
    snapshot = _healthy_snapshot()
    known = next(
        scope
        for scope in snapshot["dispositions"]["scopes"]
        if scope["scope_id"] == KNOWN_SCOPE
    )
    known["events"] = []

    report, _ = _run(snapshot)
    results = _results(report)

    assert results["qualification.dispositions"]["status"] == "fail"
    assert results["events.leagues_cup_five"]["status"] == "ok"


@pytest.mark.unit
def test_failed_raw_disposition_cannot_hide_behind_terminal_event_rows():
    snapshot = _healthy_snapshot()
    snapshot["dispositions"]["scopes"][0]["raw_evidence"][0]["state"] = "failed"

    report, _ = _run(snapshot)

    assert _results(report)["qualification.dispositions"]["status"] == "fail"


@pytest.mark.unit
def test_all_five_known_events_must_exist_in_exact_scope():
    snapshot = _healthy_snapshot()
    snapshot["known_events"]["event_ids"].pop()

    report, _ = _run(snapshot)

    assert _results(report)["events.leagues_cup_five"]["status"] == "fail"


@pytest.mark.unit
@pytest.mark.parametrize(
    "url",
    [
        "http://0.0.0.0:8086/health",
        "http://127.0.0.1:8080/health",
        "http://localhost:8086/health",
        "https://127.0.0.1:8086/health",
    ],
)
def test_ui_probe_requires_exact_loopback_ip_port_and_path(url):
    snapshot = _healthy_snapshot()
    snapshot["ui_health"]["url"] = url

    report, _ = _run(snapshot)

    assert _results(report)["runtime.ui_health"]["status"] == "fail"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status_code", 503),
        ("metadatabase", {"status": "unhealthy"}),
        ("scheduler", {"status": "unhealthy"}),
        ("triggerer", {"status": "unhealthy"}),
        ("dag_processor", {"status": "unhealthy"}),
    ],
)
def test_http_or_required_component_health_failure_is_not_green(field, value):
    snapshot = _healthy_snapshot()
    if field == "status_code":
        snapshot["ui_health"][field] = value
    else:
        snapshot["ui_health"]["body"][field] = value

    report, _ = _run(snapshot)

    assert _results(report)["runtime.ui_health"]["status"] == "fail"


@pytest.mark.unit
def test_optional_airflow_components_may_report_null_status():
    snapshot = _healthy_snapshot()
    snapshot["ui_health"]["body"]["triggerer"] = {"status": None}
    snapshot["ui_health"]["body"]["dag_processor"] = {"status": None}

    report, _ = _run(snapshot)

    assert _results(report)["runtime.ui_health"]["status"] == "ok"


@pytest.mark.unit
def test_exact_metadb_container_name_and_health_are_required():
    for change in (
        {"name": "espn-airflow-metadb"},
        {"status": "exited"},
        {"health": "unhealthy"},
    ):
        snapshot = _healthy_snapshot()
        snapshot["container"].update(change)
        report, _ = _run(snapshot)
        assert _results(report)["runtime.metadb_container"]["status"] == "fail"


@pytest.mark.unit
def test_compact6_requires_exact_public_inventory_and_three_way_parity():
    snapshot = _healthy_snapshot()
    snapshot["layout"] = {
        "layout_mode": "compact6",
        "inventory": [
            *_inventory(COMPACT6_PUBLIC_OBJECTS, "bronze"),
            *_inventory(COMPACT6_INTERNAL_REQUIRED_OBJECTS, "espn_internal"),
        ],
        "serving_relation": "iceberg.bronze.espn_schedule",
        "serving_readable": True,
        "parity": {"schedule": True, "lineup": True, "matchsheet": True},
    }

    report, _ = _run(snapshot)

    result = _results(report)["serving.layout_parity"]
    assert result["status"] == "ok"
    assert result["details"]["public_object_count"] == 6

    snapshot["layout"]["parity"]["lineup"] = False
    report, _ = _run(snapshot)
    assert _results(report)["serving.layout_parity"]["status"] == "fail"


@pytest.mark.unit
@pytest.mark.parametrize("mode", [None, "", "legacy13", "mixed"])
def test_unknown_layout_is_hard_unknown_or_failure_never_green(mode):
    snapshot = _healthy_snapshot()
    snapshot["layout"]["layout_mode"] = mode

    report, _ = _run(snapshot)

    assert _results(report)["serving.layout_parity"]["status"] in {
        "unknown",
        "fail",
    }


@pytest.mark.unit
def test_mixed_layout_inventory_fails_closed():
    snapshot = _healthy_snapshot()
    snapshot["layout"]["inventory"] = _inventory(COMPACT6_PUBLIC_OBJECTS, "bronze")

    report, _ = _run(snapshot)

    assert _results(report)["serving.layout_parity"]["status"] == "fail"


@pytest.mark.unit
def test_reader_exception_does_not_short_circuit_other_independent_results():
    report, readers = _run(
        errors={"read_container": RuntimeError("docker unavailable")}
    )
    results = _results(report)

    assert readers.calls == list(READ_METHODS)
    assert len(results) == 14
    assert results["runtime.metadb_container"]["status"] == "unknown"
    assert results["runtime.ui_health"]["status"] == "ok"
    assert results["leases.zero_active"]["status"] == "ok"
    assert report["status"] == "fail"


@pytest.mark.unit
def test_probe_calls_only_declared_readers_and_contains_no_writer_path():
    class ReadOnlyBoundary(InjectedReaders):
        def __getattr__(self, name):
            if any(token in name for token in ("write", "store", "migrate", "ddl")):
                raise AssertionError(f"writer boundary touched: {name}")
            raise AttributeError(name)

    readers = ReadOnlyBoundary()
    run_probe(readers, observed_at=NOW)

    assert readers.calls == list(READ_METHODS)
    source = Path("scripts/espn_rollout_probe_v1.py").read_text(encoding="utf-8")
    for forbidden in (
        "check_36h_freshness_and_alerts",
        ".migrate(",
        "CREATE TABLE",
        "ALTER TABLE",
        "DROP TABLE",
        "INSERT INTO",
        "UPDATE ",
        "DELETE FROM",
    ):
        assert forbidden not in source


@pytest.mark.unit
def test_cli_reads_snapshot_from_stdin_and_only_emits_versioned_json(
    monkeypatch, capsys
):
    monkeypatch.setattr("sys.stdin", StringIO(json.dumps(_healthy_snapshot())))

    exit_code = main(["--snapshot", "-", "--observed-at", NOW.isoformat()])

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["kind"] == "espn-rollout-probe-v1"
    assert output["status"] == "ok"


@pytest.mark.unit
def test_versioned_probe_can_run_directly_from_release_root():
    completed = subprocess.run(
        [sys.executable, "scripts/espn_rollout_probe_v1.py", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "--snapshot" in completed.stdout
