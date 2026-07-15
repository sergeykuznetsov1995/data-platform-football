from __future__ import annotations

import importlib
import os
import subprocess
import sys


def _load_module():
    sys.modules.pop("dag_backup_whoscored_storage", None)
    return importlib.import_module("dag_backup_whoscored_storage")


def test_backup_dag_is_daily_serial_and_fail_closed() -> None:
    from airflow.operators.bash import BashOperator

    BashOperator._instances.clear()
    mod = _load_module()
    tasks = {task.task_id: task for task in BashOperator._instances}

    assert mod.dag.schedule == "0 3 * * *"
    assert mod.dag._dag_kwargs["max_active_runs"] == 1
    assert set(tasks) == {
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
    assert "WHOSCORED_BACKUP_DESTINATION_URI is required" in tasks[
        "validate_whoscored_backup_config"
    ].bash_command
    assert "immutable backup retention is required" in tasks[
        "validate_whoscored_backup_config"
    ].bash_command
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
    cleanup = tasks["cleanup_whoscored_backup_local_inventories"]
    assert cleanup._init_kwargs["trigger_rule"] == "all_done"
    assert "-mtime" in cleanup.bash_command
    terminal_gate = tasks["propagate_whoscored_backup_status"]
    assert terminal_gate._init_kwargs["trigger_rule"] == (
        "none_failed_min_one_success"
    )
    assert terminal_gate.upstream_task_ids == {
        "verify_whoscored_backup",
        "verify_whoscored_ops_backup",
        "cleanup_whoscored_backup_local_inventories",
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
    assert '$WHOSCORED_OPS_STORE_URI' in tasks[
        "inventory_whoscored_ops"
    ].bash_command
    assert "--allow-empty" in tasks["inventory_whoscored_ops"].bash_command
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
        assert '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"' in tasks[
            task_id
        ].bash_command


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
