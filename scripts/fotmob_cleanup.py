#!/usr/bin/env python3
"""Plan and safely execute FotMob staging cleanup / inventory compaction.

``plan`` is read-only and records exact objects plus their metadata. ``execute``
accepts only the byte-for-byte reviewed plan (SHA-256 confirmation), rechecks
every target, and requires recent evidence that FotMob writers are paused.
There is no wildcard DROP path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

sys.dont_write_bytecode = True

try:  # package import in tests / ``python -m``
    from scripts.fotmob_acceptance import QueryClient, TrinoQueryClient, connect_from_env, load_trino_env
    from scripts import fotmob_runtime as runtime_binding
except ModuleNotFoundError:  # direct ``python scripts/fotmob_cleanup.py``
    from fotmob_acceptance import QueryClient, TrinoQueryClient, connect_from_env, load_trino_env
    import fotmob_runtime as runtime_binding


STAGING_RE = re.compile(r"^fotmob_[a-z0-9_]+__stg_[a-z0-9_]{1,128}$")
INVENTORY = "fotmob_field_inventory"
INVENTORY_KEYS = (
    "target_type",
    "competition_id",
    "source_season_key",
    "json_path",
    "disposition",
)
PAUSED_DAGS = {
    "dag_orchestrate_fotmob",
    "dag_ingest_fotmob",
    "dag_trigger_fotmob_daily",
    "dag_refresh_fotmob",
    "dag_backfill_fotmob",
    "dag_collect_fotmob_players",
}
# Dynamic catalog rows and structural-decision evidence are rollback inputs,
# never cleanup targets. Keep their current views protected as well.
PRESERVED_DYNAMIC_CATALOG_EVIDENCE = frozenset(
    {
        "fotmob_competitions",
        "fotmob_competitions_current",
        "fotmob_competition_scope_observations",
        "fotmob_competition_scope_observations_current",
    }
)
CONFIRM_EXECUTE = "EXECUTE_REVIEWED_FOTMOB_CLEANUP"


class CleanupError(RuntimeError):
    pass


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _now() -> str:
    return _now_dt().isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2, default=str)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
        temporary = Path(stream.name)
    temporary.replace(path)
    directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _identifier(value: str) -> str:
    if not value.replace("_", "").isalnum():
        raise CleanupError(f"unsafe SQL identifier: {value!r}")
    return '"' + value + '"'


def _qualified(catalog: str, schema: str, table: str) -> str:
    return ".".join(_identifier(value) for value in (catalog, schema, table))


def _qualified_snapshots(catalog: str, schema: str, table: str) -> str:
    # Iceberg metadata tables contain '$' inside the quoted table identifier.
    _identifier(catalog)
    _identifier(schema)
    _identifier(table)
    return f'{_identifier(catalog)}.{_identifier(schema)}."{table}$snapshots"'


def _scalar(client: QueryClient, sql: str) -> Any:
    rows = client.query(sql)
    if len(rows) != 1 or len(rows[0]) != 1:
        raise CleanupError("metadata query did not return one scalar")
    return rows[0][0]


def _timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _columns(
    client: QueryClient, *, catalog: str, schema: str, table: str
) -> list[str]:
    rows = client.query(
        f"""-- cleanup:columns:{table}
        SELECT column_name
        FROM {_qualified(catalog, 'information_schema', 'columns')}
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
        """
    )
    output = [str(row[0]) for row in rows if len(row) == 1]
    if not output:
        raise CleanupError(f"{table}: no columns found")
    return output


def _table_state(
    client: QueryClient, *, catalog: str, schema: str, table: str
) -> dict[str, Any]:
    count = int(
        _scalar(
            client,
            f"-- cleanup:count:{table}\nSELECT COUNT(*) FROM {_qualified(catalog, schema, table)}",
        )
    )
    snapshots = client.query(
        f"""-- cleanup:snapshot:{table}
        SELECT snapshot_id, committed_at
        FROM {_qualified_snapshots(catalog, schema, table)}
        ORDER BY committed_at DESC, snapshot_id DESC
        LIMIT 1
        """,
    )
    if len(snapshots) != 1 or len(snapshots[0]) != 2:
        raise CleanupError(f"{table}: latest Iceberg snapshot is unavailable")
    snapshot_id, committed_at = snapshots[0]
    if snapshot_id is None or committed_at is None:
        raise CleanupError(f"{table}: latest Iceberg snapshot is incomplete")
    return {
        "row_count": count,
        "snapshot_id": str(snapshot_id),
        "last_snapshot_at": _timestamp(committed_at).isoformat(),
    }


def _inventory_shape(
    client: QueryClient, *, catalog: str, schema: str
) -> tuple[int, int]:
    shape_rows = client.query(
        f"""-- cleanup:inventory-shape
        SELECT COUNT(*), COUNT(*) FILTER (WHERE key_rn = 1)
        FROM (
            SELECT ROW_NUMBER() OVER (
                PARTITION BY {', '.join(_identifier(key) for key in INVENTORY_KEYS)}
                ORDER BY _ingested_at DESC, _target_batch_id DESC
            ) key_rn
            FROM {_qualified(catalog, schema, INVENTORY)}
        ) ranked
        """
    )
    if len(shape_rows) != 1 or len(shape_rows[0]) != 2:
        raise CleanupError("field inventory shape query did not return two scalars")
    return int(shape_rows[0][0]), int(shape_rows[0][1])


def build_plan(
    client: QueryClient,
    *,
    catalog: str,
    schema: str,
    older_than_hours: int,
    clock: Callable[[], datetime] = _now_dt,
    isolated_stack: bool = False,
) -> dict[str, Any]:
    if older_than_hours < 1:
        raise CleanupError("--older-than-hours must be at least 1")
    now = clock().astimezone(timezone.utc)
    cutoff = now - timedelta(hours=older_than_hours)
    rows = client.query(
        f"""-- cleanup:list-staging
        SELECT table_name
        FROM {_qualified(catalog, 'information_schema', 'tables')}
        WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'
          AND table_name LIKE 'fotmob%__stg_%'
        ORDER BY table_name
        """
    )
    staging: list[dict[str, Any]] = []
    rejected: list[dict[str, str]] = []
    for row in rows:
        if len(row) != 1:
            raise CleanupError("staging inventory returned a non-scalar row")
        table = str(row[0])
        if not STAGING_RE.fullmatch(table):
            rejected.append({"table": table, "reason": "name_not_owned_by_fotmob_writer"})
            continue
        state = _table_state(client, catalog=catalog, schema=schema, table=table)
        if _timestamp(state["last_snapshot_at"]) > cutoff:
            rejected.append({"table": table, "reason": "newer_than_cutoff"})
            continue
        staging.append(
            {
                "table": table,
                "qualified_table": f"{catalog}.{schema}.{table}",
                **state,
                "action": "drop_table",
            }
        )

    if isolated_stack:
        return {
            "schema_version": "fotmob-cleanup-plan-v1",
            "mode": "isolated_staging_only",
            "generated_at": now.isoformat(),
            "expires_at": (now + timedelta(hours=24)).isoformat(),
            "older_than_hours": older_than_hours,
            "catalog": catalog,
            "schema": schema,
            "dry_run": True,
            "staging_targets": staging,
            "rejected_candidates": rejected,
            "dynamic_catalog_evidence_action": "retain",
            "dynamic_catalog_evidence_objects": sorted(PRESERVED_DYNAMIC_CATALOG_EVIDENCE),
            "inventory_compaction": {"source_table": INVENTORY, "action": "retain"},
        }

    inventory_columns = _columns(
        client, catalog=catalog, schema=schema, table=INVENTORY
    )
    required_inventory_columns = {
        *INVENTORY_KEYS,
        "_ingested_at",
        "_target_batch_id",
    }
    missing_keys = sorted(required_inventory_columns - set(inventory_columns))
    if missing_keys:
        raise CleanupError(f"field inventory misses key columns: {missing_keys!r}")
    source_rows, distinct_rows = _inventory_shape(
        client, catalog=catalog, schema=schema
    )
    inventory_state = _table_state(
        client, catalog=catalog, schema=schema, table=INVENTORY
    )
    if source_rows != inventory_state["row_count"]:
        raise CleanupError("field inventory count changed during planning")
    token = hashlib.sha256(
        f"{now.isoformat()}:{catalog}:{schema}:{source_rows}:{distinct_rows}".encode()
    ).hexdigest()[:12]
    shadow = f"{INVENTORY}__compact_{token}"
    backup = f"{INVENTORY}__backup_{token}"
    return {
        "schema_version": "fotmob-cleanup-plan-v1",
        "generated_at": now.isoformat(),
        "expires_at": (now + timedelta(hours=24)).isoformat(),
        "catalog": catalog,
        "schema": schema,
        "dry_run": True,
        "staging_targets": staging,
        "rejected_candidates": rejected,
        "dynamic_catalog_evidence_action": "retain",
        "dynamic_catalog_evidence_objects": sorted(
            PRESERVED_DYNAMIC_CATALOG_EVIDENCE
        ),
        "inventory_compaction": {
            "source_table": INVENTORY,
            "source_rows": int(source_rows),
            "distinct_rows": int(distinct_rows),
            "duplicate_rows": int(source_rows) - int(distinct_rows),
            "snapshot_id": inventory_state["snapshot_id"],
            "last_snapshot_at": inventory_state["last_snapshot_at"],
            "columns": inventory_columns,
            "natural_key": list(INVENTORY_KEYS),
            "order_columns": ["_ingested_at", "_target_batch_id"],
            "shadow_table": shadow,
            "backup_table": backup,
            "action": (
                "shadow_swap" if int(source_rows) > int(distinct_rows) else "none"
            ),
        },
    }


def _validate_pause_evidence(
    path: Path,
    *,
    clock: Callable[[], datetime],
    catalog: str,
    schema: str,
    project: str,
    release_sha: str,
) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CleanupError(f"invalid pause evidence: {exc}") from exc
    if payload.get("passed") is not True:
        raise CleanupError("pause evidence is not green")
    observed = set(payload.get("paused") or payload.get("writers_paused") or ())
    if observed != PAUSED_DAGS:
        raise CleanupError("pause evidence does not cover every FotMob writer")
    states = payload.get("pause_states")
    if not isinstance(states, Mapping) or set(states) != PAUSED_DAGS:
        raise CleanupError("pause evidence misses exact DAG pause states")
    if any(value not in (True, "True", "true", "1", 1) for value in states.values()):
        raise CleanupError("pause evidence contains an unpaused writer")
    if payload.get("running_runs") or payload.get("queued_runs"):
        raise CleanupError("pause evidence contains active writer runs")
    expected = {
        "catalog": catalog,
        "schema": schema,
        "project": project,
        "git_sha": release_sha,
    }
    mismatches = {
        key: {"expected": value, "observed": payload.get(key)}
        for key, value in expected.items()
        if payload.get(key) != value
    }
    if mismatches:
        raise CleanupError(f"pause evidence stack identity mismatch: {mismatches!r}")
    generated = _timestamp(payload.get("generated_at"))
    now = clock().astimezone(timezone.utc)
    if generated > now + timedelta(minutes=5):
        raise CleanupError("pause evidence timestamp is in the future")
    if now - generated > timedelta(hours=1):
        raise CleanupError("pause evidence is older than one hour")
    shared = payload.get("shared_consumer_pause")
    expected_pause_states = dict(
        runtime_binding.DESTRUCTIVE_SHARED_PAUSE_STATES
    )
    container_id = (
        str(shared.get("shared_scheduler_container_id") or "")
        if isinstance(shared, Mapping)
        else ""
    )
    if (
        not isinstance(shared, Mapping)
        or re.fullmatch(r"[0-9a-f]{64}", container_id) is None
        or shared.get("schedule_owner") != "isolated"
        or shared.get("pause_states_after") != expected_pause_states
        or shared.get("active_runs") != []
        or shared.get("active_task_instances") != []
        or shared.get("atomic_metadata_transaction") is not True
    ):
        raise CleanupError("pause evidence shared_consumer_pause is incomplete")
    return payload


def _quiesce_isolated_scheduler(
    args: argparse.Namespace,
    *,
    shared_consumer_pause: Mapping[str, Any] | None = None,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    try:
        from scripts import fotmob_rollback as runtime
    except ModuleNotFoundError:  # direct ``python scripts/fotmob_cleanup.py``
        import fotmob_rollback as runtime

    if not args.env_file or not args.deployment_report:
        raise CleanupError("execute requires --env-file and --deployment-report")
    context = runtime._deployment_context(args)
    if context["git_sha"] != args.release_sha:
        raise CleanupError("live deployment report SHA differs from --release-sha")
    live_identity = runtime.validate_live_deployment(
        args, require_running=False, run=run
    )
    writer_state: Mapping[str, Any] = {
        "pause_states": {dag_id: True for dag_id in runtime.DAGS},
        "active_runs": {},
        "active_task_instances": [],
        "atomic_metadata_snapshot": True,
    }
    environment = runtime._compose_environment(args)
    base = runtime._compose_base(args)
    if live_identity["scheduler_running"]:
        writer_state = runtime.inspect_writer_state(args, run=run)
        try:
            runtime.require_writers_stopped(writer_state)
        except runtime.RollbackError as exc:
            raise CleanupError(f"live writer quiescence failed: {exc}") from exc
        deployed_sha = runtime._container_deploy_sha(args, run=run)
        if deployed_sha != args.release_sha:
            raise CleanupError("live scheduler SHA differs from --release-sha")
        run(
            (*base, "stop", "airflow-scheduler"),
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )
        live_identity = runtime.validate_live_deployment(
            args, require_running=False, run=run
        )
    if live_identity["scheduler_running"]:
        raise CleanupError("isolated scheduler is still running after stop")
    shared_readback = None
    if shared_consumer_pause is not None:
        expected_id = str(
            shared_consumer_pause.get("shared_scheduler_container_id") or ""
        )
        try:
            shared_readback = runtime.inspect_shared_consumer_pause(
                args, expected_container_id=expected_id, run=run
            )
        except runtime.RollbackError as exc:
            raise CleanupError(f"shared consumer live quiescence failed: {exc}") from exc
    return {
        "project": args.project,
        "git_sha": args.release_sha,
        "pause_states": writer_state["pause_states"],
        "active_runs": writer_state["active_runs"],
        "scheduler_stopped": True,
        "live_deployment": live_identity,
        "shared_consumer_readback": shared_readback,
    }


def _validate_plan_shape(
    plan: Mapping[str, Any], *, clock: Callable[[], datetime], isolated_stack: bool = False
) -> None:
    if plan.get("schema_version") != "fotmob-cleanup-plan-v1":
        raise CleanupError("unsupported cleanup plan schema")
    now = clock().astimezone(timezone.utc)
    generated_at = _timestamp(plan.get("generated_at"))
    expires_at = _timestamp(plan.get("expires_at"))
    if generated_at > now + timedelta(minutes=5):
        raise CleanupError("cleanup plan timestamp is in the future")
    if expires_at < now:
        raise CleanupError("cleanup plan has expired")
    if expires_at - generated_at > timedelta(hours=24):
        raise CleanupError("cleanup plan lifetime exceeds 24 hours")
    catalog, schema = str(plan.get("catalog", "")), str(plan.get("schema", ""))
    _identifier(catalog)
    _identifier(schema)
    if plan.get("dry_run") is not True:
        raise CleanupError("cleanup plan must originate from a dry-run plan")
    if plan.get("dynamic_catalog_evidence_action") != "retain" or plan.get(
        "dynamic_catalog_evidence_objects"
    ) != sorted(PRESERVED_DYNAMIC_CATALOG_EVIDENCE):
        raise CleanupError("cleanup plan does not preserve dynamic catalog/evidence")
    targets = plan.get("staging_targets")
    if not isinstance(targets, list):
        raise CleanupError("staging_targets must be a list")
    names: set[str] = set()
    for target in targets:
        if not isinstance(target, Mapping):
            raise CleanupError("invalid staging target")
        name = str(target.get("table"))
        if not STAGING_RE.fullmatch(name) or name in names:
            raise CleanupError(f"invalid or duplicate explicit staging target: {name!r}")
        if target.get("action") != "drop_table":
            raise CleanupError(f"{name}: unsupported cleanup action")
        if target.get("qualified_table") != f"{catalog}.{schema}.{name}":
            raise CleanupError(f"{name}: qualified identity does not match plan stack")
        if int(target.get("row_count", -1)) < 0 or not str(
            target.get("snapshot_id", "")
        ):
            raise CleanupError(f"{name}: invalid reviewed table metadata")
        names.add(name)
    if names & PRESERVED_DYNAMIC_CATALOG_EVIDENCE:
        raise CleanupError("cleanup plan targets preserved dynamic catalog/evidence")
    inventory = plan.get("inventory_compaction")
    if not isinstance(inventory, Mapping):
        raise CleanupError("inventory_compaction must be an object")
    if inventory.get("source_table") != INVENTORY:
        raise CleanupError("inventory plan does not target the canonical table")
    if inventory.get("source_table") in PRESERVED_DYNAMIC_CATALOG_EVIDENCE:
        raise CleanupError("cleanup plan targets preserved dynamic catalog/evidence")
    if isolated_stack:
        if plan.get("mode") != "isolated_staging_only" or dict(inventory) != {
            "source_table": INVENTORY, "action": "retain"
        }:
            raise CleanupError("isolated cleanup accepts staging-only plans; inventory must be retained")
        hours = plan.get("older_than_hours")
        if type(hours) is not int or hours < 1:
            raise CleanupError("isolated plan misses a valid reviewed age threshold")
        cutoff = generated_at - timedelta(hours=hours)
        for target in targets:
            if _timestamp(target.get("last_snapshot_at")) > cutoff:
                raise CleanupError("staging target is newer than reviewed age cutoff")
        return
    if plan.get("mode") == "isolated_staging_only":
        raise CleanupError("isolated staging plan requires --isolated-stack")
    action = inventory.get("action")
    if action not in {"none", "shadow_swap"}:
        raise CleanupError("inventory plan contains an unsupported action")
    source_rows = int(inventory.get("source_rows", -1))
    distinct_rows = int(inventory.get("distinct_rows", -1))
    if source_rows < 0 or distinct_rows < 0 or distinct_rows > source_rows:
        raise CleanupError("inventory plan contains invalid row counts")
    if action == "none" and source_rows != distinct_rows:
        raise CleanupError("inventory plan skips compaction despite duplicate rows")
    if action == "shadow_swap" and source_rows <= distinct_rows:
        raise CleanupError("inventory plan requests compaction without duplicate rows")
    if int(inventory.get("duplicate_rows", source_rows - distinct_rows)) != (
        source_rows - distinct_rows
    ):
        raise CleanupError("inventory plan has an inconsistent duplicate count")
    if not str(inventory.get("snapshot_id", "")):
        raise CleanupError("inventory plan misses exact snapshot identity")
    _timestamp(inventory.get("last_snapshot_at"))
    columns = inventory.get("columns")
    if not isinstance(columns, list) or not columns or len(columns) != len(set(columns)):
        raise CleanupError("inventory plan has invalid columns")
    if not {*INVENTORY_KEYS, "_ingested_at", "_target_batch_id"}.issubset(columns):
        raise CleanupError("inventory plan misses required columns")
    if inventory.get("natural_key") != list(INVENTORY_KEYS):
        raise CleanupError("inventory plan natural key differs from writer contract")
    if inventory.get("order_columns") != ["_ingested_at", "_target_batch_id"]:
        raise CleanupError("inventory plan ordering differs from writer contract")
    if action == "shadow_swap":
        if not re.fullmatch(
            r"fotmob_field_inventory__compact_[0-9a-f]{12}",
            str(inventory.get("shadow_table", "")),
        ):
            raise CleanupError("inventory plan has an invalid shadow name")
        if not re.fullmatch(
            r"fotmob_field_inventory__backup_[0-9a-f]{12}",
            str(inventory.get("backup_table", "")),
        ):
            raise CleanupError("inventory plan has an invalid backup name")
        if str(inventory["shadow_table"]).rsplit("_", 1)[-1] != str(
            inventory["backup_table"]
        ).rsplit("_", 1)[-1]:
            raise CleanupError("inventory shadow and backup tokens differ")


def _table_exists(
    client: QueryClient, *, catalog: str, schema: str, table: str
) -> bool:
    rows = client.query(
        f"""-- cleanup:table-exists:{table}
        SELECT COUNT(*)
        FROM {_qualified(catalog, 'information_schema', 'tables')}
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        """
    )
    if len(rows) != 1 or len(rows[0]) != 1:
        raise CleanupError(f"{table}: existence query did not return one scalar")
    return int(rows[0][0]) == 1


def _swap_name_state(
    client: QueryClient,
    *,
    catalog: str,
    schema: str,
    source: str,
    shadow: str,
    backup: str,
) -> set[str]:
    rows = client.query(
        f"""-- cleanup:swap-name-state
        SELECT table_name FROM {_qualified(catalog, 'information_schema', 'tables')}
        WHERE table_schema = '{schema}'
          AND table_name IN ('{source}', '{shadow}', '{backup}')
        """
    )
    observed = {str(row[0]) for row in rows if len(row) == 1}
    if len(observed) != len(rows) or observed - {source, shadow, backup}:
        raise CleanupError("inventory swap name-state query returned invalid rows")
    return observed


def _reviewed_inventory_state(spec: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "row_count": int(spec["source_rows"]),
        "snapshot_id": str(spec["snapshot_id"]),
        "last_snapshot_at": _timestamp(spec["last_snapshot_at"]).isoformat(),
    }


def _require_reviewed_inventory_source(
    client: QueryClient,
    *,
    catalog: str,
    schema: str,
    table: str,
    spec: Mapping[str, Any],
) -> None:
    if _table_state(client, catalog=catalog, schema=schema, table=table) != (
        _reviewed_inventory_state(spec)
    ):
        raise CleanupError(f"{table}: table is not the reviewed inventory source")
    if _columns(client, catalog=catalog, schema=schema, table=table) != [
        str(value) for value in spec["columns"]
    ]:
        raise CleanupError(f"{table}: schema differs from the reviewed inventory")


def _require_reviewed_compaction(
    client: QueryClient,
    *,
    catalog: str,
    schema: str,
    compact_table: str,
    original_table: str,
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    """Prove a table is the exact deterministic dedup of the reviewed source."""

    _require_reviewed_inventory_source(
        client,
        catalog=catalog,
        schema=schema,
        table=original_table,
        spec=spec,
    )
    columns = [str(value) for value in spec["columns"]]
    if _columns(client, catalog=catalog, schema=schema, table=compact_table) != columns:
        raise CleanupError(f"{compact_table}: compacted schema differs from plan")
    compact_state = _table_state(
        client, catalog=catalog, schema=schema, table=compact_table
    )
    if compact_state["row_count"] != int(spec["distinct_rows"]):
        raise CleanupError(f"{compact_table}: compacted row count differs from plan")
    quoted_columns = ", ".join(_identifier(column) for column in columns)
    keys = ", ".join(_identifier(key) for key in INVENTORY_KEYS)
    difference_count = int(
        _scalar(
            client,
            f"""-- cleanup:inventory-compaction-diff:{compact_table}:{original_table}
            SELECT COUNT(*) FROM (
                (
                    SELECT {quoted_columns}
                    FROM {_qualified(catalog, schema, compact_table)}
                    EXCEPT
                    SELECT {quoted_columns} FROM (
                        SELECT source_row.*,
                               ROW_NUMBER() OVER (
                                   PARTITION BY {keys}
                                   ORDER BY _ingested_at DESC, _target_batch_id DESC
                               ) AS _cleanup_rn
                        FROM {_qualified(catalog, schema, original_table)} source_row
                    ) expected WHERE _cleanup_rn = 1
                )
                UNION ALL
                (
                    SELECT {quoted_columns} FROM (
                        SELECT source_row.*,
                               ROW_NUMBER() OVER (
                                   PARTITION BY {keys}
                                   ORDER BY _ingested_at DESC, _target_batch_id DESC
                               ) AS _cleanup_rn
                        FROM {_qualified(catalog, schema, original_table)} source_row
                    ) expected WHERE _cleanup_rn = 1
                    EXCEPT
                    SELECT {quoted_columns}
                    FROM {_qualified(catalog, schema, compact_table)}
                )
            ) differences""",
        )
    )
    if difference_count:
        raise CleanupError(
            f"{compact_table}: contents are not the reviewed deterministic compaction"
        )
    return compact_state


def _reconcile_inventory_swap_names(
    client: QueryClient,
    *,
    catalog: str,
    schema: str,
    source: str,
    shadow: str,
    backup: str,
    spec: Mapping[str, Any],
) -> bool:
    """Restore the reviewed canonical source after ambiguous swap DDL.

    Trino can commit a rename and lose the HTTP response. Every operation is
    therefore followed by a fresh name-state query instead of trusting the
    client exception. The function is also run before preflight, making the
    exact reviewed plan restart-safe after process/container interruption.
    """

    changed = False
    last_state: set[str] = set()
    for _ in range(6):
        state = _swap_name_state(
            client,
            catalog=catalog,
            schema=schema,
            source=source,
            shadow=shadow,
            backup=backup,
        )
        last_state = state
        if backup in state:
            if source in state and shadow in state:
                raise CleanupError(
                    "inventory swap has all canonical/shadow/backup names; "
                    "automatic recovery is ambiguous"
                )
            if source in state:
                _require_reviewed_compaction(
                    client,
                    catalog=catalog,
                    schema=schema,
                    compact_table=source,
                    original_table=backup,
                    spec=spec,
                )
                try:
                    client.query(
                        f"ALTER TABLE {_qualified(catalog, schema, source)} "
                        f"RENAME TO {_identifier(shadow)}"
                    )
                except Exception:
                    pass
                changed = True
                continue
            if shadow in state:
                _require_reviewed_compaction(
                    client,
                    catalog=catalog,
                    schema=schema,
                    compact_table=shadow,
                    original_table=backup,
                    spec=spec,
                )
            else:
                _require_reviewed_inventory_source(
                    client,
                    catalog=catalog,
                    schema=schema,
                    table=backup,
                    spec=spec,
                )
            try:
                client.query(
                    f"ALTER TABLE {_qualified(catalog, schema, backup)} "
                    f"RENAME TO {_identifier(source)}"
                )
            except Exception:
                pass
            changed = True
            continue
        if source not in state:
            raise CleanupError(
                "canonical inventory table is absent and no reviewed backup exists"
            )
        if shadow in state:
            _require_reviewed_inventory_source(
                client,
                catalog=catalog,
                schema=schema,
                table=source,
                spec=spec,
            )
            _require_reviewed_compaction(
                client,
                catalog=catalog,
                schema=schema,
                compact_table=shadow,
                original_table=source,
                spec=spec,
            )
            try:
                client.query(f"DROP TABLE {_qualified(catalog, schema, shadow)}")
            except Exception:
                pass
            changed = True
            continue
        return changed
    raise CleanupError(
        f"inventory swap recovery did not converge; observed names={sorted(last_state)!r}"
    )


def _execute_inventory_swap(
    client: QueryClient,
    *,
    catalog: str,
    schema: str,
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    if spec.get("action") == "none":
        return {"action": "none", "rows": int(spec["source_rows"])}
    source = str(spec["source_table"])
    shadow = str(spec["shadow_table"])
    backup = str(spec["backup_table"])
    if source != INVENTORY:
        raise CleanupError("inventory source is not the canonical table")
    if not re.fullmatch(r"fotmob_field_inventory__compact_[0-9a-f]{12}", shadow):
        raise CleanupError("invalid inventory shadow name")
    if not re.fullmatch(r"fotmob_field_inventory__backup_[0-9a-f]{12}", backup):
        raise CleanupError("invalid inventory backup name")
    _reconcile_inventory_swap_names(
        client,
        catalog=catalog,
        schema=schema,
        source=source,
        shadow=shadow,
        backup=backup,
        spec=spec,
    )
    columns = [str(value) for value in spec.get("columns") or ()]
    required_columns = {*INVENTORY_KEYS, "_ingested_at", "_target_batch_id"}
    if not columns or not required_columns.issubset(columns):
        raise CleanupError("inventory plan has invalid columns")
    current_columns = _columns(client, catalog=catalog, schema=schema, table=source)
    if columns != current_columns:
        raise CleanupError("inventory schema changed after plan creation")
    existing = _swap_name_state(
        client,
        catalog=catalog,
        schema=schema,
        source=source,
        shadow=shadow,
        backup=backup,
    )
    if existing != {source}:
        raise CleanupError("planned shadow or backup table already exists")
    quoted_columns = ", ".join(_identifier(column) for column in columns)
    keys = ", ".join(_identifier(key) for key in INVENTORY_KEYS)
    client.query(
        f"""CREATE TABLE {_qualified(catalog, schema, shadow)}
        WITH (partitioning = ARRAY['target_type']) AS
        SELECT {quoted_columns}
        FROM (
            SELECT source_row.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY {keys}
                       ORDER BY _ingested_at DESC, _target_batch_id DESC
                   ) AS _cleanup_rn
            FROM {_qualified(catalog, schema, source)} source_row
        ) ranked
        WHERE _cleanup_rn = 1
        """
    )
    shadow_rows = int(
        _scalar(
            client,
            f"SELECT COUNT(*) FROM {_qualified(catalog, schema, shadow)}",
        )
    )
    if shadow_rows != int(spec["distinct_rows"]):
        raise CleanupError(
            f"inventory shadow count mismatch: {shadow_rows}!={spec['distinct_rows']}"
        )
    duplicate_groups = int(
        _scalar(
            client,
            f"""SELECT COUNT(*) FROM (
                SELECT {keys} FROM {_qualified(catalog, schema, shadow)}
                GROUP BY {keys} HAVING COUNT(*) > 1
            ) dups""",
        )
    )
    if duplicate_groups:
        raise CleanupError("inventory shadow still contains duplicate keys")
    try:
        client.query(
            f"ALTER TABLE {_qualified(catalog, schema, source)} "
            f"RENAME TO {_identifier(backup)}"
        )
        client.query(
            f"ALTER TABLE {_qualified(catalog, schema, shadow)} "
            f"RENAME TO {_identifier(source)}"
        )
        final_rows = int(
            _scalar(
                client,
                f"SELECT COUNT(*) FROM {_qualified(catalog, schema, source)}",
            )
        )
        if final_rows != shadow_rows:
            raise CleanupError("inventory count changed during swap")
    except Exception as original:
        try:
            _reconcile_inventory_swap_names(
                client,
                catalog=catalog,
                schema=schema,
                source=source,
                shadow=shadow,
                backup=backup,
                spec=spec,
            )
        except Exception as exc:
            raise CleanupError(
                f"inventory swap failed and automatic restore was incomplete: {exc}"
            ) from original
        raise CleanupError("inventory swap failed; reviewed source restored") from original
    promoted_state = _require_reviewed_compaction(
        client,
        catalog=catalog,
        schema=schema,
        compact_table=source,
        original_table=backup,
        spec=spec,
    )
    return {
        "action": "shadow_swap",
        "source_rows": int(spec["source_rows"]),
        "rows": final_rows,
        "duplicates_removed": int(spec["source_rows"]) - final_rows,
        "backup_table": backup,
        "promoted_state": promoted_state,
        "backup_dropped_after_validation": False,
    }


def _finalize_inventory_swap(
    client: QueryClient,
    *,
    catalog: str,
    schema: str,
    spec: Mapping[str, Any],
    promoted_state: Mapping[str, Any],
) -> dict[str, Any]:
    """Drop the reviewed backup only after a durable pending journal exists."""

    source = str(spec["source_table"])
    shadow = str(spec["shadow_table"])
    backup = str(spec["backup_table"])
    expected_promoted = {
        "row_count": int(promoted_state.get("row_count", -1)),
        "snapshot_id": str(promoted_state.get("snapshot_id", "")),
        "last_snapshot_at": _timestamp(
            promoted_state.get("last_snapshot_at")
        ).isoformat(),
    }
    if expected_promoted["row_count"] != int(spec["distinct_rows"]) or not (
        expected_promoted["snapshot_id"]
    ):
        raise CleanupError("pending journal has invalid promoted inventory identity")
    names = _swap_name_state(
        client,
        catalog=catalog,
        schema=schema,
        source=source,
        shadow=shadow,
        backup=backup,
    )
    current = _table_state(client, catalog=catalog, schema=schema, table=source)
    if current != expected_promoted:
        raise CleanupError("promoted inventory differs from the durable journal")
    if names == {source, backup}:
        verified = _require_reviewed_compaction(
            client,
            catalog=catalog,
            schema=schema,
            compact_table=source,
            original_table=backup,
            spec=spec,
        )
        if verified != expected_promoted:
            raise CleanupError("promoted inventory identity changed before finalize")
        try:
            client.query(f"DROP TABLE {_qualified(catalog, schema, backup)}")
        except Exception:
            final_names = _swap_name_state(
                client,
                catalog=catalog,
                schema=schema,
                source=source,
                shadow=shadow,
                backup=backup,
            )
            if final_names != {source}:
                raise
    elif names != {source}:
        raise CleanupError(
            f"pending inventory finalize has unsafe names: {sorted(names)!r}"
        )
    final_names = _swap_name_state(
        client,
        catalog=catalog,
        schema=schema,
        source=source,
        shadow=shadow,
        backup=backup,
    )
    if final_names != {source}:
        raise CleanupError("inventory backup DROP was not committed")
    return {
        "backup_dropped_after_validation": True,
        "promoted_state": expected_promoted,
        "resumed_after_durable_journal": names == {source},
    }


def execute_plan(
    client: QueryClient,
    plan: Mapping[str, Any],
    *,
    clock: Callable[[], datetime] = _now_dt,
) -> dict[str, Any]:
    _validate_plan_shape(plan, clock=clock)
    catalog, schema = str(plan["catalog"]), str(plan["schema"])
    inventory_spec = plan["inventory_compaction"]
    if inventory_spec.get("action") == "shadow_swap":
        _reconcile_inventory_swap_names(
            client,
            catalog=catalog,
            schema=schema,
            source=INVENTORY,
            shadow=str(inventory_spec["shadow_table"]),
            backup=str(inventory_spec["backup_table"]),
            spec=inventory_spec,
        )
    # Complete all metadata preflight before the first mutation.  A later
    # target drift must not leave an earlier reviewed target already dropped.
    preflight: list[tuple[str, int, bool]] = []
    for target in plan["staging_targets"]:
        name = str(target["table"])
        if not _table_exists(client, catalog=catalog, schema=schema, table=name):
            preflight.append((name, int(target["row_count"]), True))
            continue
        current = _table_state(client, catalog=catalog, schema=schema, table=name)
        if current != {
            "row_count": int(target["row_count"]),
            "snapshot_id": str(target["snapshot_id"]),
            "last_snapshot_at": _timestamp(target["last_snapshot_at"]).isoformat(),
        }:
            raise CleanupError(f"{name}: metadata changed after plan review")
        preflight.append((name, int(target["row_count"]), False))
    source_rows, distinct_rows = _inventory_shape(
        client, catalog=catalog, schema=schema
    )
    inventory_state = _table_state(
        client, catalog=catalog, schema=schema, table=INVENTORY
    )
    if (
        source_rows != int(inventory_spec["source_rows"])
        or distinct_rows != int(inventory_spec.get("distinct_rows", source_rows))
        or inventory_state["snapshot_id"] != str(inventory_spec["snapshot_id"])
        or inventory_state["last_snapshot_at"]
        != _timestamp(inventory_spec["last_snapshot_at"]).isoformat()
    ):
        raise CleanupError("field inventory metadata changed after plan review")

    dropped: list[dict[str, Any]] = []
    for name, row_count, already_absent in preflight:
        if not already_absent:
            client.query(f"DROP TABLE {_qualified(catalog, schema, name)}")
        dropped.append(
            {
                "table": name,
                "row_count": row_count,
                "already_absent": already_absent,
            }
        )
    inventory = _execute_inventory_swap(
        client,
        catalog=catalog,
        schema=schema,
        spec=inventory_spec,
    )
    requires_finalize = inventory.get("action") == "shadow_swap"
    return {
        "schema_version": "fotmob-cleanup-execution-v1",
        "generated_at": clock().astimezone(timezone.utc).isoformat(),
        "passed": not requires_finalize,
        "phase": (
            "inventory_promoted_backup_retained" if requires_finalize else "complete"
        ),
        "dropped_staging": dropped,
        "inventory_compaction": inventory,
        "dynamic_catalog_evidence_action": "retain",
        "dynamic_catalog_evidence_objects": sorted(
            PRESERVED_DYNAMIC_CATALOG_EVIDENCE
        ),
    }


def _load_pending_journal(
    path: Path,
    *,
    plan_sha256: str,
    plan: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("schema_version") != "fotmob-cleanup-execution-v1" or (
        payload.get("phase") != "inventory_promoted_backup_retained"
    ):
        return None
    if payload.get("passed") is not False or payload.get("plan_sha256") != plan_sha256:
        raise CleanupError("pending cleanup journal does not match reviewed plan")
    inventory = payload.get("inventory_compaction")
    spec = plan["inventory_compaction"]
    if not isinstance(inventory, Mapping) or inventory.get("action") != "shadow_swap":
        raise CleanupError("pending cleanup journal has invalid inventory state")
    if inventory.get("backup_table") != spec.get("backup_table") or not isinstance(
        inventory.get("promoted_state"), Mapping
    ):
        raise CleanupError("pending cleanup journal identity differs from plan")
    return payload


ISOLATED_RUNTIME_ROOTS = (
    "dags", "scrapers", "scripts", "configs/medallion", "configs/fotmob"
)
ISOLATED_BINDING_KEYS = (
    "FOTMOB_ISOLATED_STACK", "ALERT_ENV", "FOTMOB_WRITER_LOCK",
    "FOTMOB_DEPLOYMENT_REPORT_PATH", "FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
    "FBREF_CONTROL_DB_URI", "CONTROL_DB_URI", *sorted(runtime_binding.TRINO_ENV_KEYS),
)


def _binding_digest(env: Mapping[str, str]) -> str:
    return hashlib.sha256(json.dumps(
        {key: env.get(key, "") for key in ISOLATED_BINDING_KEYS}, sort_keys=True
    ).encode()).hexdigest()


def _require_isolated_environment(env: Mapping[str, str]) -> None:
    from urllib.parse import urlsplit
    if env.get("FOTMOB_ISOLATED_STACK") != "1" or env.get("ALERT_ENV") != "fotmob-isolated":
        raise CleanupError("scheduler is not the explicit isolated FotMob stack")
    if any(env.get(key, "").strip() for key in (
        "FOTMOB_DEPLOYMENT_REPORT_PATH", "FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH"
    )):
        raise CleanupError("isolated cleanup requires a ceremony-free scheduler")
    if env.get("FOTMOB_WRITER_LOCK", "1").strip().casefold() in {"0", "false", "no"}:
        raise CleanupError("isolated cleanup cannot disable the writer lock")
    if not env.get("TRINO_HOST", "").strip():
        raise CleanupError("isolated scheduler has no Trino binding")
    uri = env.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "").strip()
    parsed = urlsplit(uri)
    if (parsed.scheme not in {"postgresql+psycopg2", "postgresql+psycopg", "postgresql", "postgres"}
        or parsed.hostname != "fotmob-airflow-metadb" or parsed.port not in {None, 5432}
        or parsed.path != "/airflow"):
        raise CleanupError("isolated scheduler must use its own metadata DB")


def _attest_isolated_host(args: argparse.Namespace, *, run: Callable[..., Any]) -> dict[str, Any]:
    container = runtime_binding._inspect_container(args.scheduler_container_id, run=run)
    labels = (container.get("Config") or {}).get("Labels") or {}
    if (container.get("Id") != args.scheduler_container_id
        or labels.get("com.docker.compose.project") != args.project
        or labels.get("com.docker.compose.service") != "airflow-scheduler"
        or container.get("Name") != "/fotmob-airflow-scheduler"
        or not (container.get("State") or {}).get("Running")):
        raise CleanupError("live isolated scheduler container identity mismatch")
    env = runtime_binding._parsed_environment(container)
    _require_isolated_environment(env)
    root = args.release_root.resolve()
    mounts = container.get("Mounts") or ()
    for relative in ISOLATED_RUNTIME_ROOTS:
        destination = "/opt/airflow/" + relative
        matches = [item for item in mounts if item.get("Destination") == destination]
        if (len(matches) != 1 or matches[0].get("Type") != "bind"
            or matches[0].get("RW") is not False
            or Path(str(matches[0].get("Source"))).resolve() != root / relative):
            raise CleanupError(f"isolated readonly runtime mount mismatch: {destination}")
    # Reject overlays of tracked code. The external .airflowignore is the sole
    # admitted nested file bind in the ceremony-free recipe.
    for item in mounts:
        destination = str(item.get("Destination", ""))
        if destination == "/opt/airflow/dags/.airflowignore":
            if item.get("Type") != "bind" or item.get("RW") is not False:
                raise CleanupError("isolated DAG ignore mount must be readonly")
            continue
        if any(destination.startswith("/opt/airflow/" + relative + "/")
               for relative in ISOLATED_RUNTIME_ROOTS):
            raise CleanupError("isolated runtime contains an unexpected nested mount")
    def git(*arguments: str) -> str:
        return run(("git", "-C", str(root), *arguments), check=True,
                   capture_output=True, text=True).stdout
    if git("rev-parse", "HEAD").strip() != args.release_sha:
        raise CleanupError("isolated runtime SHA differs from --release-sha")
    if git("status", "--porcelain").strip():
        raise CleanupError("isolated runtime checkout is dirty")
    ignored = git("ls-files", "--others", "--ignored", "--exclude-standard", "--", *ISOLATED_RUNTIME_ROOTS)
    if any(line and not runtime_binding._is_generated_bytecode_path(line)
           for line in ignored.splitlines()):
        raise CleanupError("isolated runtime contains ignored untracked code")
    manifest = {}
    for line in git("ls-tree", "-r", "HEAD", "--", *ISOLATED_RUNTIME_ROOTS).splitlines():
        metadata, relative = line.split("\t", 1)
        mode, kind, blob = metadata.split()
        path = root / relative
        if mode not in {"100644", "100755"} or kind != "blob" or path.is_symlink():
            raise CleanupError("isolated runtime contains unsupported tracked objects")
        content = path.read_bytes()
        if hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest() != blob:
            raise CleanupError("isolated runtime bytes differ from reviewed Git SHA")
        manifest["/opt/airflow/" + relative] = hashlib.sha256(content).hexdigest()
    if "/opt/airflow/scripts/fotmob_cleanup.py" not in manifest:
        raise CleanupError("isolated release does not contain cleanup implementation")
    return {
        "scheduler_container_id": args.scheduler_container_id,
        "git_sha": args.release_sha, "manifest": manifest,
        "binding_digest": _binding_digest(env),
        "hostname": str((container.get("Config") or {}).get("Hostname", "")),
    }


def _attest_isolated_process(identity: Mapping[str, Any]) -> None:
    import socket
    _require_isolated_environment(os.environ)
    if not identity.get("hostname") or socket.gethostname() != identity["hostname"]:
        raise CleanupError("cleanup process is not in the attested scheduler")
    if _binding_digest(os.environ) != identity["binding_digest"]:
        raise CleanupError("isolated scheduler Trino or lock binding changed")
    for path, expected in identity["manifest"].items():
        if hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected:
            raise CleanupError("container-visible runtime bytes differ from reviewed SHA")


def _isolated_activity(client: QueryClient) -> dict[str, Any]:
    from airflow.models import DagRun, TaskInstance
    from airflow.settings import Session
    from sqlalchemy import text
    session = Session()
    try:
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        runs = session.query(DagRun.dag_id).filter(
            DagRun.dag_id.like("%fotmob%"), DagRun.state.in_(("queued", "running"))
        ).all()
        tasks = session.query(TaskInstance.dag_id).filter(
            TaskInstance.dag_id.like("%fotmob%"),
            TaskInstance.state.in_(("queued", "running", "restarting", "scheduled", "deferred", "up_for_retry", "up_for_reschedule"))
        ).all()
        session.commit()
    finally:
        session.close()
    if runs or tasks:
        raise CleanupError("isolated scheduler has active FotMob runs or tasks")
    writes = client.query("""-- cleanup:active-writes
        SELECT query_id FROM system.runtime.queries
        WHERE state NOT IN ('FINISHED', 'FAILED')
          AND regexp_like(lower(query), '\\b(insert|create|drop|alter|merge|delete)\\b')
          AND lower(query) LIKE '%fotmob%'
          AND query NOT LIKE '%cleanup:active-writes%'
    """)
    if writes:
        raise CleanupError("Trino has active FotMob writes")
    nodes = _isolated_trino_nodes(client)
    return {"active_runs": [], "active_task_instances": [], "active_writes": [],
            "trino_nodes": nodes, "scheduler_bound_client": True}


def _isolated_trino_nodes(client: QueryClient) -> list[list[str]]:
    nodes = client.query("SELECT node_id, http_uri FROM system.runtime.nodes ORDER BY node_id")
    if not nodes:
        raise CleanupError("isolated scheduler Trino node identity is unavailable")
    if any(len(row) != 2 or not all(isinstance(value, str) and value for value in row)
           for row in nodes) or len({row[0] for row in nodes}) != len(nodes):
        raise CleanupError("isolated scheduler Trino node identity is invalid")
    return [list(row) for row in nodes]


class _LeaseQueryClient:
    """Keep the PostgreSQL authority live while a Trino request is running."""

    def __init__(self, client: Any, lease: Any):
        self.client, self.lease = client, lease

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        self.lease.check()
        cursor = self.client.connection.cursor()
        try:
            with self.lease.watch(cursor.cancel):
                cursor.execute(sql)
                result = list(cursor.fetchall())
            self.lease.check()
            return result
        finally:
            cursor.close()


def execute_isolated_plan(client: QueryClient, plan: Mapping[str, Any], *,
                          lease: Any, clock: Callable[[], datetime] = _now_dt,
                          before_mutation: Callable[[], None] | None = None) -> dict[str, Any]:
    _validate_plan_shape(plan, clock=clock, isolated_stack=True)
    lease.check()
    catalog, schema = str(plan["catalog"]), str(plan["schema"])
    preflight = []
    for target in plan["staging_targets"]:
        name = str(target["table"])
        absent = not _table_exists(client, catalog=catalog, schema=schema, table=name)
        if not absent:
            current = _table_state(client, catalog=catalog, schema=schema, table=name)
            expected = {"row_count": int(target["row_count"]), "snapshot_id": str(target["snapshot_id"]),
                        "last_snapshot_at": _timestamp(target["last_snapshot_at"]).isoformat()}
            if current != expected:
                raise CleanupError(f"{name}: metadata changed after plan review")
        preflight.append((target, absent))
    lease.check()
    dropped = []
    for target, absent in preflight:
        lease.check()
        if not absent:
            if before_mutation is not None:
                before_mutation()
            lease.check()
            client.query(f"DROP TABLE {_qualified(catalog, schema, str(target['table']))}")
        dropped.append({"table": target["table"], "row_count": int(target["row_count"]),
                        "already_absent": absent})
    lease.check()
    return {"schema_version": "fotmob-cleanup-execution-v1", "mode": "isolated_staging_only",
            "generated_at": clock().astimezone(timezone.utc).isoformat(), "passed": True,
            "phase": "complete", "dropped_staging": dropped,
            "inventory_compaction": {"source_table": INVENTORY, "action": "retain"},
            "dynamic_catalog_evidence_action": "retain",
            "dynamic_catalog_evidence_objects": sorted(PRESERVED_DYNAMIC_CATALOG_EVIDENCE)}


def _connect_isolated_from_env(*, catalog: str, schema: str) -> QueryClient:
    """No hidden POST replay; every query/cancellation has bounded network IO."""
    import trino.dbapi
    from trino.auth import BasicAuthentication

    host = os.environ.get("TRINO_HOST", "").strip()
    if not host:
        raise CleanupError("TRINO_HOST is required")
    password = os.environ.get("TRINO_PASSWORD", "")
    user = os.environ.get("TRINO_USER", "airflow").strip()
    options: dict[str, Any] = {
        "host": host, "port": int(os.environ.get("TRINO_PORT", "8443")),
        "user": user, "catalog": catalog, "schema": schema,
        "http_scheme": os.environ.get("TRINO_HTTP_SCHEME", "https").strip(),
        "verify": os.environ.get("TRINO_TLS_VERIFY", "true").lower() not in {"0", "false", "no"},
        "max_attempts": 1, "request_timeout": (3.0, 5.0),
    }
    if password:
        options["auth"] = BasicAuthentication(user, password)
    return TrinoQueryClient(trino.dbapi.connect(**options))


def _run_isolated_request(request: Mapping[str, Any]) -> dict[str, Any]:
    """Runs only through Docker exec in the attested live scheduler."""
    _attest_isolated_process(request["identity"])
    client = _connect_isolated_from_env(catalog=request["catalog"], schema=request["schema"])
    try:
        if request["command"] == "plan":
            report = build_plan(client, catalog=request["catalog"], schema=request["schema"],
                                older_than_hours=request["older_than_hours"], isolated_stack=True,
                                clock=_now_dt)
            report["data_plane_identity"] = _isolated_trino_nodes(client)
            report["passed"] = True
        elif request["command"] == "execute":
            plan_bytes = request["plan_text"].encode("utf-8")
            if hashlib.sha256(plan_bytes).hexdigest() != request["plan_sha256"]:
                raise CleanupError("scheduler received a different reviewed plan SHA-256")
            plan = json.loads(plan_bytes)
            if plan.get("catalog") != request["catalog"] or plan.get("schema") != request["schema"]:
                raise CleanupError("scheduler plan catalog/schema mismatch")
            expected_runtime = {"scheduler_container_id": request["identity"]["scheduler_container_id"],
                                "git_sha": request["identity"]["git_sha"],
                                "binding_digest": request["identity"]["binding_digest"],
                                "runtime_bytes_verified": True, "scheduler_bound_client": True}
            if plan.get("isolated_runtime") != expected_runtime:
                raise CleanupError("reviewed plan belongs to a different isolated runtime")
            from dags.scripts import run_fotmob_scraper as runner
            with runner._writer_lock() as lease:
                if not lease or not callable(getattr(lease, "check", None)):
                    raise CleanupError("isolated cleanup requires a live writer lease")
                lease.check()
                _attest_isolated_process(request["identity"])
                guarded = _LeaseQueryClient(client, lease)
                before = _isolated_activity(guarded)
                if not before.get("trino_nodes") or before["trino_nodes"] != plan.get("data_plane_identity"):
                    raise CleanupError("reviewed plan Trino nodes differ from live scheduler binding")
                report = execute_isolated_plan(
                    guarded, plan, lease=lease, clock=_now_dt,
                    before_mutation=lambda: _attest_isolated_process(request["identity"]),
                )
                _attest_isolated_process(request["identity"])
                report["writer_quiescence_before"] = before
                report["writer_quiescence_after"] = _isolated_activity(guarded)
                lease.check()
                report["plan_sha256"] = request["plan_sha256"]
        else:
            raise CleanupError("unsupported isolated cleanup request")
        report["isolated_runtime"] = {
            "scheduler_container_id": request["identity"]["scheduler_container_id"],
            "git_sha": request["identity"]["git_sha"], "runtime_bytes_verified": True,
            "binding_digest": request["identity"]["binding_digest"],
            "scheduler_bound_client": True,
        }
        return report
    finally:
        client.close()


def _isolated_request(args: argparse.Namespace, *, run: Callable[..., Any]) -> dict[str, Any]:
    if args.pause_evidence or args.deployment_report or args.trino_env_file:
        raise CleanupError("isolated cleanup uses scheduler bindings, without ceremony or host Trino overrides")
    if args.project != "fotmob-airflow" or not args.release_root:
        raise CleanupError("isolated cleanup requires fotmob-airflow and --release-root")
    if not re.fullmatch(r"[0-9a-f]{64}", args.scheduler_container_id):
        raise CleanupError("isolated cleanup requires exact --scheduler-container-id")
    if not re.fullmatch(r"[0-9a-f]{40}", args.release_sha):
        raise CleanupError("isolated cleanup requires exact --release-sha")
    request = {"command": args.command, "catalog": args.catalog, "schema": args.schema,
               "older_than_hours": args.older_than_hours}
    if args.command == "execute":
        if not args.plan or args.confirm != CONFIRM_EXECUTE:
            raise CleanupError("isolated execute requires --plan and reviewed confirmation")
        plan_bytes = args.plan.read_bytes()
        actual_sha = hashlib.sha256(plan_bytes).hexdigest()
        if actual_sha != args.plan_sha256.lower():
            raise CleanupError(f"reviewed plan SHA-256 mismatch: expected {actual_sha}")
        plan = json.loads(plan_bytes)
        if plan.get("catalog") != args.catalog or plan.get("schema") != args.schema:
            raise CleanupError("CLI catalog/schema do not match reviewed plan")
        _validate_plan_shape(plan, clock=_now_dt, isolated_stack=True)
        request.update(plan_text=plan_bytes.decode("utf-8"), plan_sha256=actual_sha)
    request["identity"] = _attest_isolated_host(args, run=run)
    # Verify the exact runtime bytes before importing its cleanup module. The
    # imported implementation repeats this proof under the same writer lease.
    code = """import hashlib,json,sys
sys.dont_write_bytecode = True
sys.pycache_prefix = '/dev/null/fotmob-cleanup'
r=json.load(sys.stdin)
for p,h in r['identity']['manifest'].items():
    if hashlib.sha256(open(p,'rb').read()).hexdigest()!=h:
        raise RuntimeError('container runtime bytes mismatch before cleanup import')
sys.path.insert(0,'/opt/airflow')
from scripts.fotmob_cleanup import _run_isolated_request
print('FOTMOB_ISOLATED_CLEANUP_JSON='+json.dumps(_run_isolated_request(r),sort_keys=True,default=str))
"""
    output = run(("docker", "exec", "-i", args.scheduler_container_id, "python", "-c", code),
                 input=json.dumps(request), check=True, capture_output=True, text=True).stdout
    marker = "FOTMOB_ISOLATED_CLEANUP_JSON="
    reports = [json.loads(line[len(marker):]) for line in output.splitlines() if line.startswith(marker)]
    if len(reports) != 1 or not isinstance(reports[0], dict):
        raise CleanupError("isolated scheduler did not return one cleanup report")
    if _attest_isolated_host(args, run=run) != request["identity"]:
        raise CleanupError("isolated scheduler identity changed during cleanup")
    return reports[0]


def build_parser() -> argparse.ArgumentParser:
    default_compose = Path(__file__).resolve().parents[1] / "deploy/fotmob/airflow.compose.yaml"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "execute"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--catalog", default="iceberg")
    parser.add_argument("--schema", default="bronze")
    parser.add_argument("--project", default="fotmob-airflow")
    parser.add_argument("--compose-file", type=Path, default=default_compose)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--deployment-report", type=Path)
    parser.add_argument("--isolated-stack", action="store_true", help="Scheduler-bound, ceremony-free staging cleanup only")
    parser.add_argument("--scheduler-container-id", default="", help="Exact live isolated scheduler 64-hex container ID")
    parser.add_argument("--release-root", type=Path, help="Exact readonly mounted Git release tree")
    parser.add_argument(
        "--release-sha",
        default="",
        help="Exact deployed 40-hex SHA recorded in pause evidence",
    )
    parser.add_argument("--older-than-hours", type=int, default=24)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--plan-sha256", default="")
    parser.add_argument("--pause-evidence", type=Path)
    parser.add_argument("--confirm", default="")
    parser.add_argument(
        "--trino-env-file",
        type=Path,
        help="Optional Compose-style env file; only TRINO_* keys are loaded",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    client_factory: Callable[..., QueryClient] = connect_from_env,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> int:
    args = build_parser().parse_args(argv)
    client: QueryClient | None = None
    pending_report: dict[str, Any] | None = None
    try:
        if args.isolated_stack:
            report = _isolated_request(args, run=run)
        elif args.command == "plan":
            if args.trino_env_file:
                load_trino_env(args.trino_env_file)
            client = client_factory(catalog=args.catalog, schema=args.schema)
            report = build_plan(
                client,
                catalog=args.catalog,
                schema=args.schema,
                older_than_hours=args.older_than_hours,
            )
        else:
            if not args.plan or not args.pause_evidence:
                raise CleanupError("execute requires --plan and --pause-evidence")
            if args.confirm != CONFIRM_EXECUTE:
                raise CleanupError(f"execute requires --confirm {CONFIRM_EXECUTE}")
            release_sha = args.release_sha.strip().lower()
            if not re.fullmatch(r"[0-9a-f]{40}", release_sha):
                raise CleanupError("execute requires --release-sha as full 40-hex SHA")
            args.release_sha = release_sha
            try:
                plan_bytes = args.plan.read_bytes()
            except OSError as exc:
                raise CleanupError(f"cannot read cleanup plan: {exc}") from exc
            actual_sha = hashlib.sha256(plan_bytes).hexdigest()
            if args.plan_sha256.lower() != actual_sha:
                raise CleanupError(
                    f"reviewed plan SHA-256 mismatch: expected {actual_sha}"
                )
            pause_evidence = _validate_pause_evidence(
                args.pause_evidence,
                clock=_now_dt,
                catalog=args.catalog,
                schema=args.schema,
                project=args.project,
                release_sha=release_sha,
            )
            try:
                plan = json.loads(plan_bytes)
            except json.JSONDecodeError as exc:
                raise CleanupError(f"invalid cleanup plan JSON: {exc}") from exc
            if plan.get("catalog") != args.catalog or plan.get("schema") != args.schema:
                raise CleanupError("CLI catalog/schema do not match reviewed plan")
            _validate_plan_shape(plan, clock=_now_dt)
            quiescence = _quiesce_isolated_scheduler(
                args,
                shared_consumer_pause=pause_evidence["shared_consumer_pause"],
                run=run,
            )
            if not args.trino_env_file:
                raise CleanupError("execute requires --trino-env-file")
            try:
                runtime_binding.load_host_trino_environment(args.trino_env_file)
            except runtime_binding.RuntimeBindingError as exc:
                raise CleanupError(str(exc)) from exc
            client = client_factory(catalog=args.catalog, schema=args.schema)
            try:
                from scripts import fotmob_rollback as runtime
            except ModuleNotFoundError:
                import fotmob_rollback as runtime

            context = runtime._deployment_context(args)
            try:
                marker_before = runtime_binding.validate_data_plane_marker(
                    client, context
                )
                publication_before = (
                    runtime_binding.assert_no_active_fotmob_publication(
                        context, run=run
                    )
                )
            except runtime_binding.RuntimeBindingError as exc:
                raise CleanupError(str(exc)) from exc
            pending_report = _load_pending_journal(
                args.output, plan_sha256=actual_sha, plan=plan
            )
            if pending_report is None:
                report = execute_plan(client, plan)
                report["plan_sha256"] = actual_sha
                report["writer_quiescence_before"] = quiescence
                report["data_plane_before"] = marker_before
                report["publication_quiescence_before"] = publication_before
                if report["phase"] == "inventory_promoted_backup_retained":
                    pending_report = report
                    # This atomic journal is the commit point: backup removal
                    # is forbidden until the promoted snapshot identity is
                    # durably recoverable after process/host interruption.
                    _atomic_json(args.output, pending_report)
            else:
                report = pending_report
                for target in plan["staging_targets"]:
                    if _table_exists(
                        client,
                        catalog=args.catalog,
                        schema=args.schema,
                        table=str(target["table"]),
                    ):
                        raise CleanupError(
                            "pending cleanup journal conflicts with a recreated staging table"
                        )
                report["resumed_from_pending_journal"] = True
                report["writer_quiescence_before"] = quiescence
                report["data_plane_before"] = marker_before
                report["publication_quiescence_before"] = publication_before
            if report["phase"] == "inventory_promoted_backup_retained":
                finalization = _finalize_inventory_swap(
                    client,
                    catalog=args.catalog,
                    schema=args.schema,
                    spec=plan["inventory_compaction"],
                    promoted_state=report["inventory_compaction"]["promoted_state"],
                )
                report["inventory_compaction"].update(finalization)
            try:
                marker_after = runtime_binding.validate_data_plane_marker(client, context)
            except runtime_binding.RuntimeBindingError as exc:
                raise CleanupError(str(exc)) from exc
            post_quiescence = _quiesce_isolated_scheduler(
                args,
                shared_consumer_pause=pause_evidence["shared_consumer_pause"],
                run=run,
            )
            try:
                publication_after = (
                    runtime_binding.assert_no_active_fotmob_publication(
                        context, run=run
                    )
                )
            except runtime_binding.RuntimeBindingError as exc:
                raise CleanupError(str(exc)) from exc
            report["passed"] = True
            report["phase"] = "complete"
            report["data_plane_after"] = marker_after
            report["writer_quiescence_after"] = post_quiescence
            report["publication_quiescence_after"] = publication_after
            pending_report = None
    except Exception as exc:
        if pending_report is not None:
            report = pending_report
            report["passed"] = False
            report["phase"] = "inventory_promoted_backup_retained"
            report["finalize_error"] = f"{type(exc).__name__}: {exc}"
        else:
            report = {
                "schema_version": "fotmob-cleanup-error-v1",
                "generated_at": _now(),
                "passed": False,
                "command": args.command,
                "error": f"{type(exc).__name__}: {exc}",
            }
    finally:
        if client is not None:
            client.close()
    _atomic_json(args.output, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if report.get("passed") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
