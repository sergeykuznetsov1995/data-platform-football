from __future__ import annotations

import importlib
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Mapping

import pytest


def _load_module():
    sys.modules.pop("dag_backup_whoscored_storage", None)
    return importlib.import_module("dag_backup_whoscored_storage")


def test_backup_dag_is_twice_daily_serial_and_fail_closed() -> None:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
    from utils.config import SCHEDULES

    BashOperator._instances.clear()
    PythonOperator._instances.clear()
    mod = _load_module()
    bash_tasks = {task.task_id: task for task in BashOperator._instances}
    python_tasks = {task.task_id: task for task in PythonOperator._instances}
    tasks = {**bash_tasks, **python_tasks}

    assert mod.dag.schedule == "0 3,15 * * *"
    assert mod.dag.schedule == SCHEDULES["dag_backup_whoscored_storage"]
    assert mod.dag._dag_kwargs["max_active_runs"] == 1
    assert mod.dag._dag_kwargs["dagrun_timeout"] == timedelta(hours=10)
    assert mod.dag._dag_kwargs["is_paused_upon_creation"] is True
    assert set(tasks) == {
        "validate_whoscored_backup_schedule_contract",
        "validate_whoscored_backup_recovery_contract",
        "validate_whoscored_backup_config",
        "inventory_whoscored_raw",
        "backup_whoscored_raw",
        "verify_whoscored_backup",
        "inventory_whoscored_ops",
        "backup_whoscored_ops",
        "verify_whoscored_ops_backup",
        "cleanup_whoscored_backup_local_inventories",
        "propagate_whoscored_backup_status",
    }
    assert (
        "WHOSCORED_BACKUP_DESTINATION_URI is required"
        in tasks["validate_whoscored_backup_config"].bash_command
    )
    assert (
        "provider-verified object-lock retention is required"
        in tasks["validate_whoscored_backup_config"].bash_command
    )
    preflight = tasks["validate_whoscored_backup_config"].bash_command
    assert "whoscored_raw_backup.py preflight" in preflight
    assert '--source-uri "$WHOSCORED_RAW_STORE_URI"' in preflight
    assert '--source-uri "$WHOSCORED_OPS_STORE_URI"' in preflight
    assert '--destination-uri "$WHOSCORED_BACKUP_DESTINATION_URI"' in preflight
    assert '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"' in preflight
    assert preflight.startswith("set -euo pipefail; ")
    for task in tasks.values():
        assert task._init_kwargs["pool"] == mod.BACKUP_POOL
        assert task._init_kwargs["do_xcom_push"] is False
    recovery = tasks["validate_whoscored_backup_recovery_contract"]
    assert recovery._init_kwargs["python_callable"] is (
        mod.validate_whoscored_backup_recovery_contract
    )
    schedule_contract = tasks["validate_whoscored_backup_schedule_contract"]
    assert schedule_contract._init_kwargs["python_callable"] is (
        mod.validate_whoscored_backup_schedule_contract
    )
    assert schedule_contract.downstream_task_ids == {
        "validate_whoscored_backup_config",
        "validate_whoscored_backup_recovery_contract",
    }
    assert recovery.downstream_task_ids == {"propagate_whoscored_backup_status"}
    assert "validate_whoscored_backup_config" not in recovery.downstream_task_ids
    cleanup = tasks["cleanup_whoscored_backup_local_inventories"]
    assert cleanup._init_kwargs["trigger_rule"] == "all_done"
    assert "-mtime" in cleanup.bash_command
    terminal_gate = tasks["propagate_whoscored_backup_status"]
    assert terminal_gate._init_kwargs["python_callable"] is (
        mod.validate_whoscored_backup_completion
    )
    assert terminal_gate._init_kwargs["trigger_rule"] == ("none_failed_min_one_success")
    assert terminal_gate.upstream_task_ids == {
        "verify_whoscored_backup",
        "verify_whoscored_ops_backup",
        "cleanup_whoscored_backup_local_inventories",
        "validate_whoscored_backup_recovery_contract",
    }


def test_backup_and_verification_use_the_same_inventory() -> None:
    from airflow.operators.bash import BashOperator

    BashOperator._instances.clear()
    mod = _load_module()
    tasks = {task.task_id: task for task in BashOperator._instances}

    assert mod.RAW_INVENTORY in tasks["inventory_whoscored_raw"].bash_command
    assert mod.RAW_INVENTORY in tasks["backup_whoscored_raw"].bash_command
    assert mod.RAW_INVENTORY in tasks["verify_whoscored_backup"].bash_command
    assert mod.OPS_INVENTORY in tasks["inventory_whoscored_ops"].bash_command
    assert mod.OPS_INVENTORY in tasks["backup_whoscored_ops"].bash_command
    assert mod.OPS_INVENTORY in tasks["verify_whoscored_ops_backup"].bash_command
    assert "$WHOSCORED_OPS_STORE_URI" in tasks["inventory_whoscored_ops"].bash_command
    assert "--allow-empty" not in tasks["inventory_whoscored_ops"].bash_command
    assert "--allow-empty" not in tasks["inventory_whoscored_raw"].bash_command
    assert "--apply" in tasks["backup_whoscored_raw"].bash_command
    assert "verify-backup" in tasks["verify_whoscored_backup"].bash_command
    for task_id in (
        "inventory_whoscored_raw",
        "backup_whoscored_raw",
        "verify_whoscored_backup",
        "inventory_whoscored_ops",
        "backup_whoscored_ops",
        "verify_whoscored_ops_backup",
    ):
        assert (
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"' in tasks[task_id].bash_command
        )


def test_backup_preflight_propagates_the_first_probe_failure(tmp_path) -> None:
    from airflow.operators.bash import BashOperator
    from jinja2 import Environment

    BashOperator._instances.clear()
    mod = _load_module()
    task = next(
        item
        for item in BashOperator._instances
        if item.task_id == "validate_whoscored_backup_config"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python"
    fake_python.write_text("#!/bin/sh\nexit 23\n", encoding="utf-8")
    fake_python.chmod(0o700)
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "WHOSCORED_BACKUP_DESTINATION_URI": "s3://off-host/backups",
        "WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE": "object-lock",
        "WHOSCORED_RAW_STORE_URI": "s3://primary/raw",
        "WHOSCORED_OPS_STORE_URI": "s3://primary/ops",
    }

    jinja = Environment()
    jinja.filters.update(mod.dag._dag_kwargs["user_defined_filters"])
    rendered_command = jinja.from_string(task.bash_command).render(
        run_id="manual__preflight-test"
    )
    result = subprocess.run(
        ["bash", "-c", rendered_command],
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 23, result.stderr


def test_backup_run_directory_binds_long_run_id_without_truncation_collision() -> None:
    from jinja2 import Environment

    mod = _load_module()
    jinja = Environment()
    jinja.filters.update(mod.dag._dag_kwargs["user_defined_filters"])
    prefix = "manual__" + "q" * 300
    first = jinja.from_string(mod.RUN_DIR).render(run_id=prefix + "__one")
    second = jinja.from_string(mod.RUN_DIR).render(run_id=prefix + "__two")

    assert first != second
    assert len(first.rsplit("/", 1)[-1]) <= 120
    assert len(second.rsplit("/", 1)[-1]) <= 120


def _restore_evidence(now: datetime) -> dict[str, object]:
    started = now - timedelta(hours=2)
    completed = now - timedelta(hours=1)
    inventory_created = now - timedelta(hours=3)

    def source(uri: str, restore_uri: str, objects: int) -> dict[str, object]:
        return {
            "source_uri": uri,
            "restore_uri": restore_uri,
            "inventory_key": (
                "backup-inventories/20260722T080000000000Z-"
                + hashlib.sha256(uri.encode("utf-8")).hexdigest()[:16]
                + "-"
                + "a" * 64
                + ".json"
            ),
            "inventory_sha256": "a" * 64,
            "objects_sha256": "b" * 64,
            "snapshot_started_at": inventory_created.isoformat(),
            "snapshot_completed_at": (
                inventory_created + timedelta(minutes=5)
            ).isoformat(),
            "object_count": objects,
            "total_bytes": objects * 100,
            "expected_objects": objects,
            "copied_objects": objects,
            "already_present_objects": 0,
            "checked_objects": objects,
            "missing": [],
            "corrupt": [],
            "marker_present": True,
            "marker_valid": True,
            "restore_passed": True,
            "restored_inventory_object_count": objects,
            "restored_inventory_total_bytes": objects * 100,
            "restored_inventory_objects_sha256": "b" * 64,
            "exact_tree_match": True,
        }

    return {
        "schema_version": 2,
        "status": "passed",
        "started_at": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "completed_at": completed.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rpo_hours": 24,
        "rto_hours": 24,
        "backup_destination_uri": "s3://off-host-backup/whoscored",
        "runtime_release": _runtime_release(),
        "sources": [
            source(
                "s3://football/raw/whoscored",
                "s3://recovery-raw/whoscored",
                3,
            ),
            source(
                "s3://football/ops/whoscored",
                "s3://recovery-ops/whoscored",
                2,
            ),
        ],
    }


def _runtime_release() -> dict[str, str]:
    return {
        "parser_version": "whoscored-parser-v8",
        "manifest_sha256": "c" * 64,
        "code_tree_sha256": "d" * 64,
    }


def _seal_evidence(document: dict[str, object]) -> None:
    document.pop("off_host_receipt", None)
    proof = json.dumps(
        document,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    digest = hashlib.sha256(proof).hexdigest()
    completed = datetime.strptime(str(document["completed_at"]), "%Y-%m-%dT%H:%M:%SZ")
    document["off_host_receipt"] = {
        "key": (
            "restore-drill-receipts/v2/"
            f"{completed.strftime('%Y%m%dT%H%M%SZ')}-{digest}.json"
        ),
        "sha256": digest,
    }


def _off_host_loader(document: dict[str, object]):
    proof = {
        name: value for name, value in document.items() if name != "off_host_receipt"
    }
    receipt = document["off_host_receipt"]
    assert isinstance(receipt, dict)

    def load(key: str):
        assert key == receipt["key"]
        return proof

    return load


def _write_evidence(path: Path, document: dict[str, object]) -> None:
    _seal_evidence(document)
    path.write_text(
        json.dumps(document, sort_keys=True, separators=(",", ":")),
        encoding="ascii",
    )
    path.chmod(0o600)


def test_restore_drill_evidence_proves_recent_raw_and_ops_empty_target_restore(
    tmp_path: Path,
) -> None:
    mod = _load_module()
    now = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    evidence = tmp_path / "restore-drill.json"
    document = _restore_evidence(now)
    _write_evidence(evidence, document)

    result = mod.validate_restore_drill_evidence(
        evidence,
        raw_store_uri="s3://football/raw/whoscored",
        ops_store_uri="s3://football/ops/whoscored",
        backup_destination_uri="s3://off-host-backup/whoscored",
        expected_runtime_release=_runtime_release(),
        now=now,
        off_host_receipt_loader=_off_host_loader(document),
    )

    assert result == {
        "status": "passed",
        "rpo_hours": 24,
        "rto_hours": 24,
        "duration_seconds": 3600,
        "evidence_age_seconds": 3600,
        "runtime_release": _runtime_release(),
        "source_uris": [
            "s3://football/ops/whoscored",
            "s3://football/raw/whoscored",
        ],
        "off_host_receipt_key": document["off_host_receipt"]["key"],
        "off_host_receipt_sha256": document["off_host_receipt"]["sha256"],
    }


def test_restore_drill_evidence_can_require_live_backup_revalidation(
    tmp_path: Path,
) -> None:
    mod = _load_module()
    now = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    evidence = tmp_path / "restore-drill.json"
    document = _restore_evidence(now)
    _write_evidence(evidence, document)
    calls: list[tuple[Mapping[str, object], datetime]] = []

    def revalidate(value, checked_at):
        calls.append((value, checked_at))
        return {"status": "passed", "checked_at": "live"}

    result = mod.validate_restore_drill_evidence(
        evidence,
        raw_store_uri="s3://football/raw/whoscored",
        ops_store_uri="s3://football/ops/whoscored",
        backup_destination_uri="s3://off-host-backup/whoscored",
        expected_runtime_release=_runtime_release(),
        now=now,
        off_host_receipt_loader=_off_host_loader(document),
        backup_revalidator=revalidate,
    )

    assert result["live_backup"] == {"status": "passed", "checked_at": "live"}
    assert calls == [(document, now)]

    with pytest.raises(RuntimeError, match="live off-host backup revalidation failed"):
        mod.validate_restore_drill_evidence(
            evidence,
            raw_store_uri="s3://football/raw/whoscored",
            ops_store_uri="s3://football/ops/whoscored",
            backup_destination_uri="s3://off-host-backup/whoscored",
            expected_runtime_release=_runtime_release(),
            now=now,
            off_host_receipt_loader=_off_host_loader(document),
            backup_revalidator=lambda *_args: (_ for _ in ()).throw(
                RuntimeError("deleted object")
            ),
        )


def test_restore_drill_rejects_missing_or_different_off_host_receipt(
    tmp_path: Path,
) -> None:
    mod = _load_module()
    now = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    document = _restore_evidence(now)
    evidence = tmp_path / "restore-drill.json"
    _write_evidence(evidence, document)
    remote = _off_host_loader(document)(document["off_host_receipt"]["key"])
    different = {**remote, "status": "failed"}

    with pytest.raises(RuntimeError, match="differs from local evidence"):
        mod.validate_restore_drill_evidence(
            evidence,
            raw_store_uri="s3://football/raw/whoscored",
            ops_store_uri="s3://football/ops/whoscored",
            backup_destination_uri="s3://off-host-backup/whoscored",
            expected_runtime_release=_runtime_release(),
            now=now,
            off_host_receipt_loader=lambda _key: different,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("wrong-source", "admitted raw and ops"),
        ("slow-restore", "24h RTO"),
        ("stale-evidence", "stale or from the future"),
        ("release-drift", "different runtime release"),
        ("missing-marker", "incomplete"),
        ("nonempty-target", "empty-target"),
    ),
)
def test_restore_drill_evidence_rejects_unproven_recovery(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    mod = _load_module()
    now = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    document = _restore_evidence(now)
    sources = document["sources"]
    assert isinstance(sources, list)
    raw = sources[0]
    assert isinstance(raw, dict)
    if mutation == "wrong-source":
        raw["source_uri"] = "s3://warehouse/raw/whoscored"
        raw["inventory_key"] = (
            "backup-inventories/20260722T080000000000Z-"
            + hashlib.sha256(raw["source_uri"].encode("utf-8")).hexdigest()[:16]
            + "-"
            + "a" * 64
            + ".json"
        )
    elif mutation == "slow-restore":
        document["started_at"] = (now - timedelta(hours=26)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    elif mutation == "stale-evidence":
        document["completed_at"] = (now - timedelta(hours=25)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        document["started_at"] = (now - timedelta(hours=26)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    elif mutation == "release-drift":
        release = document["runtime_release"]
        assert isinstance(release, dict)
        release["code_tree_sha256"] = "e" * 64
    elif mutation == "missing-marker":
        raw["marker_present"] = False
    else:
        raw["already_present_objects"] = 1
        raw["copied_objects"] = 2
    evidence = tmp_path / "restore-drill.json"
    _write_evidence(evidence, document)

    with pytest.raises(RuntimeError, match=message):
        mod.validate_restore_drill_evidence(
            evidence,
            raw_store_uri="s3://football/raw/whoscored",
            ops_store_uri="s3://football/ops/whoscored",
            backup_destination_uri="s3://off-host-backup/whoscored",
            expected_runtime_release=_runtime_release(),
            now=now,
            off_host_receipt_loader=_off_host_loader(document),
        )


@pytest.mark.parametrize(
    "restore_uri",
    (
        "s3://football/raw/whoscored/rehearsal",
        "s3://football/raw",
        "s3://off-host-backup/whoscored/rehearsal",
    ),
)
def test_restore_drill_rejects_equal_or_nested_protected_prefixes(
    tmp_path: Path,
    restore_uri: str,
) -> None:
    mod = _load_module()
    now = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    document = _restore_evidence(now)
    sources = document["sources"]
    assert isinstance(sources, list)
    raw = sources[0]
    assert isinstance(raw, dict)
    raw["restore_uri"] = restore_uri
    evidence = tmp_path / "restore-drill.json"
    _write_evidence(evidence, document)

    with pytest.raises(RuntimeError, match="roots are not distinct"):
        mod.validate_restore_drill_evidence(
            evidence,
            raw_store_uri="s3://football/raw/whoscored",
            ops_store_uri="s3://football/ops/whoscored",
            backup_destination_uri="s3://off-host-backup/whoscored",
            expected_runtime_release=_runtime_release(),
            now=now,
            off_host_receipt_loader=_off_host_loader(document),
        )


def test_restore_drill_rejects_backup_destination_nested_with_source(
    tmp_path: Path,
) -> None:
    mod = _load_module()
    now = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    evidence = tmp_path / "restore-drill.json"
    document = _restore_evidence(now)
    document["backup_destination_uri"] = "s3://football/raw"
    _write_evidence(evidence, document)

    with pytest.raises(RuntimeError, match="destination must be distinct"):
        mod.validate_restore_drill_evidence(
            evidence,
            raw_store_uri="s3://football/raw/whoscored",
            ops_store_uri="s3://football/ops/whoscored",
            backup_destination_uri="s3://football/raw",
            expected_runtime_release=_runtime_release(),
            now=now,
            off_host_receipt_loader=_off_host_loader(document),
        )


def test_restore_drill_rejects_symlinked_evidence(tmp_path: Path) -> None:
    mod = _load_module()
    now = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    evidence = tmp_path / "restore-drill.json"
    link = tmp_path / "restore-drill-link.json"
    document = _restore_evidence(now)
    _write_evidence(evidence, document)
    link.symlink_to(evidence)

    with pytest.raises(RuntimeError, match="unavailable"):
        mod.validate_restore_drill_evidence(
            link,
            raw_store_uri="s3://football/raw/whoscored",
            ops_store_uri="s3://football/ops/whoscored",
            backup_destination_uri="s3://off-host-backup/whoscored",
            expected_runtime_release=_runtime_release(),
            now=now,
            off_host_receipt_loader=_off_host_loader(document),
        )


def test_airflow_backup_contract_requires_fixed_slo_and_evidence_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = _load_module()
    from scripts import whoscored_raw_backup as raw_backup

    now = datetime.now(timezone.utc).replace(microsecond=0)
    evidence = tmp_path / "restore-drill.json"
    document = _restore_evidence(now)
    _write_evidence(evidence, document)
    monkeypatch.setattr(
        raw_backup,
        "open_store",
        lambda *_args, **_kwargs: SimpleNamespace(
            _read_json=_off_host_loader(document)
        ),
    )
    monkeypatch.setattr(mod, "RESTORE_DRILL_EVIDENCE_PATH", str(evidence))
    monkeypatch.setattr(mod, "_current_runtime_release", _runtime_release)
    for name, value in {
        "WHOSCORED_BACKUP_RPO_HOURS": "24",
        "WHOSCORED_BACKUP_RTO_HOURS": "24",
        "WHOSCORED_BACKUP_RESTORE_DRILL_MAX_AGE_HOURS": "24",
        "WHOSCORED_BACKUP_RESTORE_DRILL_EVIDENCE_PATH": str(evidence),
        "WHOSCORED_RAW_STORE_URI": "s3://football/raw/whoscored",
        "WHOSCORED_OPS_STORE_URI": "s3://football/ops/whoscored",
        "WHOSCORED_BACKUP_DESTINATION_URI": "s3://off-host-backup/whoscored",
    }.items():
        monkeypatch.setenv(name, value)

    assert mod.validate_whoscored_backup_recovery_contract()["status"] == "passed"
    monkeypatch.setattr(
        raw_backup,
        "revalidate_restore_drill_backup",
        lambda **_kwargs: {"status": "passed", "checked_at": "live"},
    )
    assert mod.validate_whoscored_backup_recovery_contract(full_revalidation=True)[
        "live_backup"
    ] == {"status": "passed", "checked_at": "live"}

    monkeypatch.setenv("WHOSCORED_BACKUP_RTO_HOURS", "25")
    with pytest.raises(RuntimeError, match="exactly 24"):
        mod.validate_whoscored_backup_recovery_contract()


def test_scheduled_backup_rejects_late_start_even_with_valid_drill(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = _load_module()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    evidence = tmp_path / "restore-drill.json"
    _write_evidence(evidence, _restore_evidence(now))
    monkeypatch.setattr(mod, "RESTORE_DRILL_EVIDENCE_PATH", str(evidence))
    monkeypatch.setattr(mod, "_current_runtime_release", _runtime_release)
    for name, value in {
        "WHOSCORED_BACKUP_RPO_HOURS": "24",
        "WHOSCORED_BACKUP_RTO_HOURS": "24",
        "WHOSCORED_BACKUP_RESTORE_DRILL_MAX_AGE_HOURS": "24",
        "WHOSCORED_BACKUP_RESTORE_DRILL_EVIDENCE_PATH": str(evidence),
        "WHOSCORED_RAW_STORE_URI": "s3://football/raw/whoscored",
        "WHOSCORED_OPS_STORE_URI": "s3://football/ops/whoscored",
        "WHOSCORED_BACKUP_DESTINATION_URI": "s3://off-host-backup/whoscored",
    }.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(RuntimeError, match="15-minute start window"):
        mod.validate_whoscored_backup_schedule_contract(
            dag_run=SimpleNamespace(run_type="scheduled"),
            data_interval_end=now - timedelta(minutes=16),
        )


def test_terminal_gate_binds_nonempty_raw_and_ops_recovery_points(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = _load_module()
    from scripts import whoscored_raw_backup as raw_backup

    now = datetime(2026, 7, 22, 12, tzinfo=timezone.utc)
    inventories = {
        "raw.json": {
            "source_uri": "s3://football/raw/whoscored",
            "snapshot_started_at": "2026-07-22T10:00:00+00:00",
            "inventory_sha256": "a" * 64,
            "objects_sha256": "b" * 64,
            "object_count": 3,
            "total_bytes": 300,
        },
        "ops.json": {
            "source_uri": "s3://football/ops/whoscored",
            "snapshot_started_at": "2026-07-22T10:05:00+00:00",
            "inventory_sha256": "c" * 64,
            "objects_sha256": "d" * 64,
            "object_count": 2,
            "total_bytes": 200,
        },
    }
    monkeypatch.setattr(
        raw_backup,
        "load_inventory",
        lambda path: inventories[path.name],
    )
    monkeypatch.setenv("WHOSCORED_RAW_STORE_URI", "s3://football/raw/whoscored")
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", "s3://football/ops/whoscored")

    result = mod.validate_whoscored_backup_completion(
        raw_inventory_path="raw.json",
        ops_inventory_path="ops.json",
        validation_now=now,
        data_interval_end=now - timedelta(hours=2),
    )

    assert result["status"] == "passed"
    assert set(result["inventories"]) == {"raw", "ops"}

    inventories["ops.json"]["object_count"] = 0
    with pytest.raises(RuntimeError, match="ops backup inventory must be non-empty"):
        mod.validate_whoscored_backup_completion(
            raw_inventory_path="raw.json",
            ops_inventory_path="ops.json",
            validation_now=now,
            data_interval_end=now - timedelta(hours=2),
        )


@pytest.mark.parametrize(
    ("validation_now", "interval_end", "message"),
    (
        (
            datetime(2026, 7, 22, 12),
            datetime(2026, 7, 22, 10, tzinfo=timezone.utc),
            "clock must be timezone-aware",
        ),
        (
            datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
            datetime(2026, 7, 22, 10),
            "timezone-aware interval end",
        ),
    ),
)
def test_terminal_gate_requires_timezone_aware_deadline(
    monkeypatch: pytest.MonkeyPatch,
    validation_now: datetime,
    interval_end: datetime,
    message: str,
) -> None:
    mod = _load_module()
    monkeypatch.setenv("WHOSCORED_RAW_STORE_URI", "s3://football/raw/whoscored")
    monkeypatch.setenv("WHOSCORED_OPS_STORE_URI", "s3://football/ops/whoscored")

    with pytest.raises(RuntimeError, match=message):
        mod.validate_whoscored_backup_completion(
            raw_inventory_path="raw.json",
            ops_inventory_path="ops.json",
            validation_now=validation_now,
            data_interval_end=interval_end,
        )
