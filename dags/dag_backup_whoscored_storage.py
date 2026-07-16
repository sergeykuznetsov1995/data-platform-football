"""Daily immutable off-host backup and verification for WhoScored raw data."""

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

from __future__ import annotations

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = "/opt/airflow" if _whoscored_production else _whoscored_source.rsplit("/dags/", 1)[0]
if _whoscored_production:
    if getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None) != 2:
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None) != _whoscored_root:
    _whoscored_anchor_path = (
        _whoscored_root + "/docker/images/airflow/whoscored_runtime_startup.py"
    )
    _whoscored_anchor_globals = {
        "__builtins__": __builtins__,
        "sys": _whoscored_bootstrap_sys,
        "_WHOSCORED_RUNTIME_ROOT": _whoscored_root,
        "_WHOSCORED_REQUIRE_FULL_ATTESTATION": False,
    }
    with open(_whoscored_anchor_path, "rb") as _whoscored_anchor_handle:
        _whoscored_anchor_source = _whoscored_anchor_handle.read()
    exec(
        compile(_whoscored_anchor_source, _whoscored_anchor_path, "exec"),
        _whoscored_anchor_globals,
    )
_WHOSCORED_RUNTIME_CONTRACT = (
    _whoscored_bootstrap_sys._load_whoscored_runtime_contract(_whoscored_root)
)

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from dags.scripts.whoscored_identity import stable_safe_token
from utils.config import DAG_TAGS, SCHEDULES
from utils.default_args import SCRAPER_ARGS


BACKUP_POOL = "whoscored_storage_pool"
RUN_DIR = (
    "/opt/airflow/logs/whoscored_backup/"
    "{{ run_id | stable_safe_token }}"
)
RAW_INVENTORY = f"{RUN_DIR}/raw-inventory.json"
OPS_INVENTORY = f"{RUN_DIR}/ops-inventory.json"

_ENV = {
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
}


with DAG(
    dag_id="dag_backup_whoscored_storage",
    default_args={**SCRAPER_ARGS, "retries": 2, "retry_delay": timedelta(minutes=10)},
    description="Inventory, copy and read-back verify immutable WhoScored raw objects",
    schedule=SCHEDULES.get("dag_backup_whoscored_storage", "0 3 * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    user_defined_filters={"stable_safe_token": stable_safe_token},
    tags=DAG_TAGS.get("whoscored", ["whoscored", "storage"]),
) as dag:
    preflight = BashOperator(
        task_id="validate_whoscored_backup_config",
        bash_command=(
            "set -euo pipefail; "
            'test -n "${WHOSCORED_BACKUP_DESTINATION_URI:-}" || '
            "{ echo 'WHOSCORED_BACKUP_DESTINATION_URI is required' >&2; exit 2; }; "
            'case "${WHOSCORED_BACKUP_DESTINATION_RETENTION_MODE:-}" in '
            "(object-lock|versioned-worm) ;; (*) echo 'immutable backup retention "
            "is required' >&2; exit 2;; esac; "
            "python /opt/airflow/scripts/whoscored_raw_backup.py preflight "
            '--source-uri "$WHOSCORED_RAW_STORE_URI" '
            '--destination-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"; '
            "python /opt/airflow/scripts/whoscored_raw_backup.py preflight "
            '--source-uri "$WHOSCORED_OPS_STORE_URI" '
            '--destination-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"; '
            f"mkdir -p {RUN_DIR}"
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=5),
    )
    inventory = BashOperator(
        task_id="inventory_whoscored_raw",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py inventory "
            '--store-uri "$WHOSCORED_RAW_STORE_URI" '
            f"--output {RAW_INVENTORY} "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=4),
    )
    backup = BashOperator(
        task_id="backup_whoscored_raw",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py backup "
            '--source-uri "$WHOSCORED_RAW_STORE_URI" '
            '--destination-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            f"--inventory {RAW_INVENTORY} --apply "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=6),
    )
    verify = BashOperator(
        task_id="verify_whoscored_backup",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py verify-backup "
            '--store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            f"--inventory {RAW_INVENTORY} "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=6),
    )
    ops_inventory = BashOperator(
        task_id="inventory_whoscored_ops",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py inventory "
            '--store-uri "$WHOSCORED_OPS_STORE_URI" '
            f"--output {OPS_INVENTORY} --allow-empty "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=4),
    )
    ops_backup = BashOperator(
        task_id="backup_whoscored_ops",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py backup "
            '--source-uri "$WHOSCORED_OPS_STORE_URI" '
            '--destination-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            f"--inventory {OPS_INVENTORY} --apply "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=6),
    )
    ops_verify = BashOperator(
        task_id="verify_whoscored_ops_backup",
        bash_command=(
            "python /opt/airflow/scripts/whoscored_raw_backup.py verify-backup "
            '--store-uri "$WHOSCORED_BACKUP_DESTINATION_URI" '
            f"--inventory {OPS_INVENTORY} "
            '--workers "${WHOSCORED_BACKUP_WORKERS:-16}"'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        execution_timeout=timedelta(hours=6),
    )
    cleanup = BashOperator(
        task_id="cleanup_whoscored_backup_local_inventories",
        bash_command=(
            'days="${WHOSCORED_BACKUP_LOCAL_RETENTION_DAYS:-14}"; '
            'case "$days" in (*[!0-9]*|"") echo "invalid retention" >&2; exit 2;; esac; '
            'find /opt/airflow/logs/whoscored_backup -mindepth 1 -maxdepth 1 '
            '-type d -mtime "+$days" -exec rm -rf -- {} +'
        ),
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=10),
    )
    terminal_gate = BashOperator(
        task_id="propagate_whoscored_backup_status",
        bash_command="true",
        env=_ENV,
        append_env=True,
        pool=BACKUP_POOL,
        do_xcom_push=False,
        # cleanup is intentionally all_done, but it must never hide a failed
        # inventory/copy/verify task by becoming the DAG's only successful
        # leaf. A failed or upstream_failed verify/cleanup propagates to this
        # terminal leaf and therefore to the DagRun.
        trigger_rule="none_failed_min_one_success",
        execution_timeout=timedelta(minutes=5),
    )

    preflight >> inventory >> backup >> verify
    preflight >> ops_inventory >> ops_backup >> ops_verify
    verify >> cleanup
    ops_verify >> cleanup
    verify >> terminal_gate
    ops_verify >> terminal_gate
    cleanup >> terminal_gate
