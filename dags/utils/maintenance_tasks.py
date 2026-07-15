"""
Iceberg Maintenance Tasks
=========================

Periodic maintenance for Iceberg tables: expire stale snapshots and remove
orphan files. Without this, delete-then-insert DAGs (e.g. dag_ingest_whoscored)
accumulate thousands of metadata snapshots — `whoscored_events` reached 12K+
files / 26 GB metadata for 49 MB of data before the first sweep.

Trino DOES NOT expose Iceberg table properties like
`write.metadata.delete-after-commit.enabled` or
`write.metadata.previous-versions-max` via SET PROPERTIES /
extra_properties (Iceberg connector blocks `write.metadata.*` keys), so the
only way to keep the warehouse healthy is periodic sweeps from this module.

IMPORTANT (#266): `expire_snapshots` / `remove_orphan_files` reject any
`retention_threshold` shorter than Trino's configured minimum
(`iceberg.expire_snapshots_min_retention` / `..._remove_orphan_files...`,
both default 7d) with `INVALID_PROCEDURE_ARGUMENT`. The daily high-churn DAG
asks for '3d', so without lowering that minimum every expire silently fails
and the sweep becomes a no-op. We lower it PER SESSION (`SET SESSION ...`) at
connection time — scoped to this connection only, no Trino restart, no global
config change. High-churn tables (e.g. `fotmob_match_details`) also commit
hundreds of snapshots per run, so the per-session floor must be well under the
requested threshold.

Uses `_get_trino_connection()` from `silver_tasks` (lightweight `import trino`,
avoids heavy `scrapers/__init__.py`).
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, List, Mapping, Optional, Tuple

import trino as trino_lib

from utils.silver_tasks import _get_trino_connection

logger = logging.getLogger(__name__)

DEFAULT_SCHEMAS: Tuple[str, ...] = ("bronze", "silver", "gold")
DEFAULT_RETENTION = "7d"

# Per-session floor for the expire/orphan retention guards. Set well below any
# `retention_threshold` this module uses (down to '3d' for daily high-churn) so
# the requested threshold is always honored. See #266 — without this the daily
# sweep's '3d' is rejected by Trino's 7d default and every expire no-ops.
SESSION_MIN_RETENTION = "1h"

# High-churn tables — daily DAGs do delete-then-insert, so even a 7-day
# retention leaves >14 stale snapshots between weekly sweeps. Run a separate
# daily DAG with retention='3d' against this allowlist.
HIGH_CHURN_BRONZE: Tuple[str, ...] = (
    "whoscored_events",
    "whoscored_lineups",
    "whoscored_missing_players",
    "whoscored_schedule",
    # V2 logical commits/snapshots are append-only, but still create at least
    # one Iceberg snapshot per daily run.  Keep their metadata bounded along
    # with the payload tables.  The table filter is applied after SHOW TABLES,
    # so additive rollout is safe before every manifest table exists.
    "whoscored_match_ingest_manifest",
    "whoscored_preview_ingest_manifest",
    "whoscored_profile_ingest_manifest",
    "whoscored_player_profile_versions",
    "fbref_match_events",
    "fbref_match_player_stats",
    "fbref_match_team_stats",
    "fbref_lineups",
    "understat_shots",
    "understat_player_match_stats",
    "understat_players",
    "understat_schedule",
    "understat_team_match_stats",
    "matchhistory_results",  # #307: was matchhistory_games (legacy table dropped)
    # #266: daily fotmob/sofascore/espn writers were never on the list and
    # bloated to multi-GB metadata (fotmob_match_details hit 7.2G / 154M data).
    "fotmob_match_details",
    "fotmob_player_details",
    "fotmob_player_stats",
    "sofascore_player_ratings",
    "sofascore_player_universe",
    "sofascore_player_profile",
    "sofascore_player_season_stats",
    "sofascore_event_player_stats",
    "sofascore_match_stats",
    "espn_lineup",
    "espn_matchsheet",
)

FBREF_GENERIC_STAGE_BASES: Tuple[str, ...] = (
    "fbref_page_manifest",
    "fbref_table_inventory",
    "fbref_table_cells",
)
FBREF_PUBLICATION_STAGE_BASES: Tuple[str, ...] = ("fbref_target_scope",)
_FBREF_GENERIC_SEMANTIC_COLUMNS: Mapping[str, Tuple[str, ...]] = {
    "fbref_page_manifest": (
        "target_id", "canonical_url", "page_kind", "content_hash",
        "parser_version", "parse_status", "persist_status",
        "validation_status", "table_count", "cell_count", "errors_json",
    ),
    "fbref_table_inventory": (
        "target_id", "page_kind", "content_hash", "parser_version",
        "table_instance_id", "source_table_id", "table_id",
        "source_location", "source_ordinal", "availability",
        "schema_signature", "content_signature", "duplicate_of", "caption",
        "row_count", "reason",
    ),
    "fbref_table_cells": (
        "target_id", "page_kind", "content_hash", "parser_version",
        "table_instance_id", "table_id", "row_id", "source_row_index",
        "cell_id", "cell_index", "data_stat", "raw_header_path",
        "raw_value", "entity_ids",
    ),
}
FBREF_STAGE_MIN_AGE = timedelta(hours=24)
_FBREF_ANY_STAGE_RE = re.compile(r"^fbref_[a-z0-9_]+__stg_[a-z0-9_]+$")
# Typed writers own this exact suffix contract. Unlike generic stages, its
# digest is intentionally opaque, so the janitor reports it but cannot infer a
# logical refresh or prove it redundant.
_FBREF_TYPED_STAGE_RE = re.compile(
    r"^(?P<base>fbref_[a-z0-9_]+)__stg_fbref_"
    r"[0-9a-f]{16}_[0-9a-f]{12}$"
)
_FBREF_STAGE_RE = re.compile(
    r"^(?P<base>fbref_page_manifest|fbref_table_inventory|fbref_table_cells)"
    r"__stg_lr_(?P<refresh>[0-9a-f]{32})_[ctm]$"
)
_FBREF_LEGACY_STAGE_RE = re.compile(
    r"^(?P<base>fbref_page_manifest|fbref_table_inventory|fbref_table_cells)"
    r"__stg_(?P<a>[0-9a-f]{8})_(?P<b>[0-9a-f]{4})_"
    r"(?P<c>[0-9a-f]{4})_(?P<d>[0-9a-f]{4})_"
    r"(?P<e>[0-9a-f]{12})_[0-9a-f]{12}_[ctm]$"
)


def _fbref_stage_identity(table: str) -> tuple[str, str] | None:
    """Return ``(live_table, logical_refresh_id)`` for allowlisted names."""

    matched = _FBREF_STAGE_RE.fullmatch(table)
    if matched:
        return matched.group("base"), str(uuid.UUID(hex=matched.group("refresh")))
    legacy = _FBREF_LEGACY_STAGE_RE.fullmatch(table)
    if legacy:
        refresh = "-".join(
            legacy.group(name) for name in ("a", "b", "c", "d", "e")
        )
        return legacy.group("base"), str(uuid.UUID(refresh))
    return None


def _fbref_stage_family(table: str) -> tuple[str, str | None]:
    """Classify every syntactically valid FBref stage without trusting it."""

    base, separator, _suffix = table.partition("__stg_")
    if not separator or not _FBREF_ANY_STAGE_RE.fullmatch(table):
        return "unknown", None
    if base in FBREF_GENERIC_STAGE_BASES:
        return "generic", base
    typed = _FBREF_TYPED_STAGE_RE.fullmatch(table)
    if typed:
        return "typed", typed.group("base")
    if base in FBREF_PUBLICATION_STAGE_BASES:
        return "publication_scope", base
    return "unknown", base


def _fetch_scalar(conn, sql: str):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        return None if not rows else rows[0][0]
    finally:
        cursor.close()


def _fbref_stage_created_at(conn, table: str) -> datetime:
    value = _fetch_scalar(
        conn,
        "SELECT max(committed_at) "
        f'FROM iceberg.bronze."{table}$snapshots"',
    )
    if not isinstance(value, datetime):
        raise RuntimeError(f"stage {table} has no Iceberg snapshot timestamp")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _fbref_stage_row_count(conn, table: str) -> int:
    return int(
        _fetch_scalar(
            conn, f'SELECT count(*) FROM iceberg.bronze."{table}"'
        )
        or 0
    )


def _fbref_stage_owner_evidence(
    conn, table: str, *, column: str = "run_id"
) -> dict[str, Any]:
    if column not in {"run_id", "_batch_id", "control_run_id"}:
        raise ValueError(f"unsupported FBref stage owner column: {column}")
    cursor = conn.cursor()
    try:
        cursor.execute(
            f'SELECT "{column}", count(*) FROM '
            f'iceberg.bronze."{table}" GROUP BY "{column}"'
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
    return {
        "row_count": sum(int(row[1] or 0) for row in rows),
        "null_owner_rows": sum(
            int(row[1] or 0) for row in rows if row[0] is None
        ),
        "run_ids": tuple(
            sorted(str(row[0]) for row in rows if row[0] is not None)
        ),
    }


def _terminal_processing_runs(
    run_ids: tuple[str, ...],
    run_lookup: Callable[[str], Mapping[str, Any] | None],
) -> tuple[bool, list[dict[str, Any]]]:
    """Validate stage row owners independently from the source observation.

    A replay intentionally writes source raw under the replay processing run,
    so matching these ids to the logical refresh's source run would reject a
    healthy retained stage.  Every id must instead resolve to its own terminal
    control run.
    """

    evidence: list[dict[str, Any]] = []
    valid = True
    for run_id in run_ids:
        try:
            normalized = str(uuid.UUID(run_id))
            run = run_lookup(normalized)
        except (AttributeError, TypeError, ValueError):
            run = None
            normalized = run_id
        status = str((run or {}).get("status") or "").casefold()
        terminal = status in {"succeeded", "failed", "cancelled"}
        evidence.append(
            {
                "run_id": normalized,
                "status": status or None,
                "terminal": terminal,
                "known": run is not None,
            }
        )
        valid = valid and run is not None and terminal
    return valid, evidence


def _fbref_stage_semantic_delta(conn, stage: str, live: str) -> int:
    columns = _FBREF_GENERIC_SEMANTIC_COLUMNS.get(live)
    if not columns:
        raise RuntimeError(f"no semantic column contract for {live}")
    projection = ", ".join(f'"{column}"' for column in columns)
    value = _fetch_scalar(
        conn,
        "SELECT count(*) FROM ("
        f'SELECT {projection} FROM iceberg.bronze."{stage}" '
        "EXCEPT "
        f'SELECT {projection} FROM iceberg.bronze."{live}"'
        ") AS missing_from_live",
    )
    return int(value or 0)


def _drop_fbref_stage(conn, table: str) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute(f'DROP TABLE iceberg.bronze."{table}"')
        cursor.fetchall()
    finally:
        cursor.close()


def janitor_fbref_generic_stages(
    *,
    conn=None,
    owner_lookup: Callable[[str], Mapping[str, Any] | None],
    run_lookup: Callable[[str], Mapping[str, Any] | None],
    before_drop: Callable[[str, str], None] | None = None,
    apply: bool = False,
    now: datetime | None = None,
    min_age: timedelta = FBREF_STAGE_MIN_AGE,
) -> dict[str, Any]:
    """Audit or remove only redundant, terminal, attributable FBref stages.

    ``owner_lookup`` resolves the source observation while ``run_lookup``
    resolves the (possibly different) replay/processing run stored in stage
    rows.  Both callbacks keep Trino policy independently testable.  Only the
    three generic families have a semantic equality contract; all other FBref
    stages are inventoried with recovery evidence and remain protected.
    """

    if min_age < FBREF_STAGE_MIN_AGE:
        raise ValueError("FBref stage janitor min_age must be at least 24 hours")
    if apply and before_drop is None:
        raise ValueError("apply mode requires a destructive before_drop fence")
    own_connection = conn is None
    connection = conn or _connect()
    observed_at = now or datetime.now(timezone.utc)
    if observed_at.tzinfo is None:
        raise ValueError("janitor now must be timezone-aware")
    observed_at = observed_at.astimezone(timezone.utc)
    decisions: list[dict[str, Any]] = []
    try:
        stage_names = sorted(
            table
            for table in _list_tables(connection, "bronze")
            if table.startswith("fbref_") and "__stg_" in table
        )
        for stage in stage_names:
            decision: dict[str, Any] = {
                "stage": stage,
                "action": "protected",
                "reason": "unknown",
            }
            family, family_live = _fbref_stage_family(stage)
            decision["stage_family"] = family
            if family_live is not None:
                decision["live_table"] = family_live
            if family != "generic":
                try:
                    created_at = _fbref_stage_created_at(connection, stage)
                    decision["created_at"] = created_at.isoformat()
                    decision["stage_row_count"] = _fbref_stage_row_count(
                        connection, stage
                    )
                    owner_column = {
                        "typed": "_batch_id",
                        "publication_scope": "control_run_id",
                    }.get(family)
                    if owner_column is not None:
                        owner_evidence = _fbref_stage_owner_evidence(
                            connection, stage, column=owner_column
                        )
                        stage_run_ids = tuple(owner_evidence["run_ids"])
                        decision["stage_run_ids"] = list(stage_run_ids)
                        decision["null_processing_owner_rows"] = int(
                            owner_evidence["null_owner_rows"]
                        )
                        owners_valid, processing_runs = _terminal_processing_runs(
                            stage_run_ids, run_lookup
                        )
                        decision["processing_runs"] = processing_runs
                        decision["processing_runs_terminal"] = owners_valid
                    if family == "typed":
                        decision["reason"] = (
                            "typed_stage_requires_recovery_review"
                        )
                        decision["recovery_action"] = (
                            "inspect retained rows and the processing run; "
                            "typed semantic equivalence is not proven"
                        )
                    elif family == "publication_scope":
                        decision["reason"] = (
                            "publication_scope_stage_requires_recovery_review"
                        )
                        decision["recovery_action"] = (
                            "compare scope_hash/count with the immutable live "
                            "generation before manual cleanup"
                        )
                    else:
                        decision["reason"] = "unsupported_fbref_stage"
                        decision["recovery_action"] = (
                            "identify the writer and prove recovery or "
                            "redundancy before manual cleanup"
                        )
                except Exception as exc:
                    decision.update(
                        reason="inspection_failed",
                        error_class=type(exc).__name__,
                        error=str(exc)[:500],
                    )
                decisions.append(decision)
                continue
            identity = _fbref_stage_identity(stage)
            if identity is None:
                decision["reason"] = "unrecognized_generic_stage"
                decisions.append(decision)
                continue
            live, logical_refresh_id = identity
            decision.update(
                live_table=live, logical_refresh_id=logical_refresh_id
            )
            try:
                created_at = _fbref_stage_created_at(connection, stage)
                decision["created_at"] = created_at.isoformat()
                decision["stage_row_count"] = _fbref_stage_row_count(
                    connection, stage
                )
                if observed_at - created_at < min_age:
                    decision["reason"] = "younger_than_min_age"
                    decisions.append(decision)
                    continue

                owner = owner_lookup(logical_refresh_id)
                if not owner:
                    decision["reason"] = "unknown_control_owner"
                    decisions.append(decision)
                    continue
                decision["owner_run_id"] = str(owner.get("run_id") or "")
                active_flags = (
                    "active_fetch_lease",
                    "active_budget_reservation",
                    "active_observation_processing",
                    "active_publication_lock",
                )
                terminal = bool(
                    owner.get(
                        "terminal",
                        str(owner.get("run_status") or "")
                        in {"succeeded", "failed", "cancelled"},
                    )
                )
                if not terminal:
                    decision["reason"] = "owner_run_not_terminal"
                    decisions.append(decision)
                    continue
                active = [name for name in active_flags if bool(owner.get(name))]
                if active:
                    decision["reason"] = "active_control_state"
                    decision["active_flags"] = active
                    decisions.append(decision)
                    continue

                owner_evidence = _fbref_stage_owner_evidence(
                    connection, stage, column="run_id"
                )
                stage_run_ids = tuple(owner_evidence["run_ids"])
                decision["stage_run_ids"] = list(stage_run_ids)
                null_owner_rows = int(owner_evidence["null_owner_rows"])
                decision["null_processing_owner_rows"] = null_owner_rows
                if (
                    int(owner_evidence["row_count"])
                    != int(decision["stage_row_count"])
                    or (
                        int(decision["stage_row_count"]) > 0
                        and (null_owner_rows > 0 or not stage_run_ids)
                    )
                ):
                    decision["reason"] = "processing_owner_missing"
                    decisions.append(decision)
                    continue
                owners_valid, processing_runs = _terminal_processing_runs(
                    stage_run_ids, run_lookup
                )
                decision["processing_runs"] = processing_runs
                if not owners_valid:
                    decision["reason"] = "processing_run_not_terminal"
                    decisions.append(decision)
                    continue
                semantic_delta = _fbref_stage_semantic_delta(
                    connection, stage, live
                )
                decision["semantic_delta_rows"] = semantic_delta
                if semantic_delta:
                    decision["reason"] = "semantic_delta_present"
                    decisions.append(decision)
                    continue

                if not apply:
                    decision.update(action="eligible", reason="redundant_terminal")
                    decisions.append(decision)
                    continue

                # Re-check mutable control evidence and semantic equality as
                # close as possible to DROP. The publication lock held by the
                # maintenance wrapper fences new Bronze publication as well.
                owner_recheck = owner_lookup(logical_refresh_id)
                owner_evidence_recheck = _fbref_stage_owner_evidence(
                    connection, stage, column="run_id"
                )
                rechecked_ids = tuple(owner_evidence_recheck["run_ids"])
                rechecked_valid, _ = _terminal_processing_runs(
                    rechecked_ids, run_lookup
                )
                if (
                    owner_recheck != owner
                    or owner_evidence_recheck != owner_evidence
                    or rechecked_ids != stage_run_ids
                    or not rechecked_valid
                    or _fbref_stage_semantic_delta(connection, stage, live)
                ):
                    decision["reason"] = "eligibility_changed_before_drop"
                    decisions.append(decision)
                    continue
                assert before_drop is not None
                before_drop(stage, logical_refresh_id)
                _drop_fbref_stage(connection, stage)
                decision.update(action="dropped", reason="redundant_terminal")
                decisions.append(decision)
            except Exception as exc:
                decision.update(
                    action="protected",
                    reason="inspection_failed",
                    error_class=type(exc).__name__,
                    error=str(exc)[:500],
                )
                decisions.append(decision)
    finally:
        if own_connection:
            try:
                connection.close()
            except Exception:
                pass

    counts = {
        action: sum(item["action"] == action for item in decisions)
        for action in ("protected", "eligible", "dropped")
    }
    attention_reasons = {
        "unrecognized_generic_stage",
        "unsupported_fbref_stage",
        "typed_stage_requires_recovery_review",
        "publication_scope_stage_requires_recovery_review",
        "unknown_control_owner",
        "owner_run_not_terminal",
        "processing_owner_missing",
        "processing_run_not_terminal",
        "semantic_delta_present",
        "eligibility_changed_before_drop",
        "inspection_failed",
    }
    attention_required = sum(
        item["reason"] in attention_reasons for item in decisions
    )
    return {
        "mode": "apply" if apply else "audit",
        "minimum_age_hours": int(min_age.total_seconds() // 3600),
        "stage_count": len(decisions),
        **{f"{key}_count": value for key, value in counts.items()},
        "attention_required_count": attention_required,
        "decisions": decisions,
    }


def maintain_fbref_generic_stages(*, mode: str | None = None) -> dict[str, Any]:
    """Run the janitor under a zero-budget FBref publication generation."""

    from scrapers.fbref.control import ControlStore

    normalized_mode = str(
        mode or os.environ.get("FBREF_STAGE_JANITOR_MODE", "audit")
    ).strip().casefold()
    if normalized_mode not in {"audit", "apply"}:
        raise ValueError("FBREF_STAGE_JANITOR_MODE must be audit or apply")
    control = ControlStore.from_env()
    run_id = control.create_run(
        "maintenance",
        request_limit=0,
        byte_limit=0,
        metadata={"dag_id": "dag_iceberg_maintenance_daily", "network": False},
    )
    control.start_run(run_id)
    acquired = False
    succeeded = False
    try:
        control.acquire_publication_lock(
            run_id,
            dag_id="dag_iceberg_maintenance_daily",
            ttl_seconds=60 * 60,
        )
        acquired = True

        def destructive_fence(stage: str, logical_refresh_id: str) -> None:
            # Renew and assert from the database clock directly before every
            # DROP.  The stage/refresh values are logged so an operator can
            # correlate a failed fence without relaxing the fail-closed rule.
            logger.info(
                "Fencing FBref stage drop: stage=%s refresh=%s",
                stage,
                logical_refresh_id,
            )
            control.renew_publication_lock(
                run_id,
                source="fbref",
                ttl_seconds=60 * 60,
            )
            control.assert_publication_lock_owner(run_id, source="fbref")

        result = janitor_fbref_generic_stages(
            owner_lookup=control.get_observation_cleanup_evidence,
            run_lookup=control.get_run,
            before_drop=destructive_fence,
            apply=normalized_mode == "apply",
        )
        attention_required = int(
            result.get("attention_required_count") or 0
        )
        audit_only_backlog = (
            int(result.get("eligible_count") or 0)
            if normalized_mode == "audit"
            else 0
        )
        if attention_required or audit_only_backlog:
            raise RuntimeError(
                "FBref stage janitor retained stale stages requiring recovery: "
                f"attention={attention_required}, "
                f"audit_only_eligible={audit_only_backlog}"
            )
        succeeded = True
        return {**result, "control_run_id": run_id}
    finally:
        try:
            if acquired:
                control.release_publication_lock(run_id)
        finally:
            control.finish_run(run_id, succeeded=succeeded)


def _set_session_min_retention(conn) -> None:
    """Lower the expire/orphan retention floor for THIS session only (#266).

    Trino rejects `retention_threshold` shorter than the configured minimum
    (default 7d). Scoped `SET SESSION` lets the daily DAG's '3d' (and the
    aggressive cleanup of churn tables) actually run, without a Trino restart
    or a global config change.
    """
    cur = conn.cursor()
    try:
        for prop in (
            "iceberg.expire_snapshots_min_retention",
            "iceberg.remove_orphan_files_min_retention",
        ):
            cur.execute(f"SET SESSION {prop} = '{SESSION_MIN_RETENTION}'")
            cur.fetchall()
    finally:
        cur.close()


def _connect():
    """Open a Trino connection with the session retention floor lowered."""
    conn = _get_trino_connection()
    _set_session_min_retention(conn)
    return conn


def _row_to_stats(cursor) -> dict:
    """Convert one-row procedure output to {col_name: value}.

    Trino's `EXECUTE remove_orphan_files` returns either:
      - a single row with named columns (scanned_files_count, deleted_files_count, ...)
      - or, depending on procedure, a list of (name, value) pairs.
    Read via `cursor.description` to handle both shapes safely.
    """
    rows = cursor.fetchall()
    if not rows:
        return {}
    cols = [d[0] for d in (cursor.description or [])]
    # Shape A: single row, multi-column
    if len(rows) == 1 and len(cols) == len(rows[0]) and len(cols) > 1:
        return {cols[i]: rows[0][i] for i in range(len(cols))}
    # Shape B: list of (name, value) pairs (legacy / different procedures)
    if all(len(r) == 2 for r in rows):
        return {r[0]: r[1] for r in rows}
    # Fallback — return as-is dict by column zero
    return {f"row_{i}": r for i, r in enumerate(rows)}


def _list_tables(conn, schema: str) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"SHOW TABLES FROM iceberg.{schema}")
        rows = cur.fetchall()
    finally:
        cur.close()
    return [r[0] for r in rows]


def _exec_alter(conn, sql: str) -> dict:
    """Execute ALTER TABLE ... EXECUTE ... and return parsed stats."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return _row_to_stats(cur)
    finally:
        cur.close()


def _maintain_one(conn, fq: str, retention_threshold: str) -> dict:
    """Run expire_snapshots + remove_orphan_files on a single table.

    Returns parsed stats from remove_orphan_files (deleted_files_count etc.).
    """
    _exec_alter(
        conn,
        f"ALTER TABLE {fq} EXECUTE expire_snapshots(retention_threshold => '{retention_threshold}')",
    )
    return _exec_alter(
        conn,
        f"ALTER TABLE {fq} EXECUTE remove_orphan_files(retention_threshold => '{retention_threshold}')",
    )


def maintain_iceberg_tables(
    schemas: Tuple[str, ...] = DEFAULT_SCHEMAS,
    retention_threshold: str = DEFAULT_RETENTION,
    table_filter: Optional[Iterable[str]] = None,
) -> dict:
    """Run expire_snapshots + remove_orphan_files on every table in `schemas`.

    Args:
        schemas: which Iceberg schemas to walk (default bronze/silver/gold).
        retention_threshold: '7d' for weekly, '3d' for daily high-churn.
            Requires `iceberg.{expire-snapshots,remove-orphan-files}.min-retention`
            in `configs/trino/catalog/iceberg.properties` to allow it.
        table_filter: if set, only tables whose short name is in this set
            are processed (used by the daily high-churn DAG).
    """
    conn = _connect()
    total_tables = 0
    total_deleted = 0
    total_scanned = 0
    failures: List[Tuple[str, str]] = []
    filter_set = set(table_filter) if table_filter else None

    for schema in schemas:
        try:
            tables = _list_tables(conn, schema)
        except Exception as e:
            logger.error("Failed to list tables in iceberg.%s: %s", schema, e)
            failures.append((f"iceberg.{schema}", str(e)[:300]))
            # Trino may have dropped the connection — re-open for next schema.
            try:
                conn.close()
            except Exception:
                pass
            conn = _connect()
            continue

        for tn in tables:
            if filter_set is not None and tn not in filter_set:
                continue
            fq = f"iceberg.{schema}.{tn}"
            total_tables += 1
            try:
                stats = _maintain_one(conn, fq, retention_threshold)
                deleted = int(stats.get("deleted_files_count", 0) or 0)
                scanned = int(stats.get("scanned_files_count", 0) or 0)
                total_deleted += deleted
                total_scanned += scanned
                if deleted > 0:
                    logger.info("%s: scanned=%d deleted=%d", fq, scanned, deleted)
            except trino_lib.exceptions.TrinoConnectionError as e:
                logger.warning("Connection lost on %s, reconnecting: %s", fq, e)
                failures.append((fq, f"connection: {e}"[:300]))
                try:
                    conn.close()
                except Exception:
                    pass
                conn = _connect()
            except Exception as e:
                logger.error("Maintenance failed on %s: %s", fq, e)
                failures.append((fq, str(e)[:300]))

    logger.info(
        "Iceberg maintenance done: tables=%d scanned=%d deleted=%d failures=%d",
        total_tables, total_scanned, total_deleted, len(failures),
    )
    for fq, err in failures:
        logger.warning("  FAIL %s: %s", fq, err)

    try:
        conn.close()
    except Exception:
        pass

    # #266: a systemic misconfiguration (e.g. min-retention floor above the
    # requested threshold) makes EVERY per-table expire fail while the task
    # still returns "success". Raise when nothing could be processed so the
    # sweep can no longer no-op silently.
    if total_tables > 0 and len(failures) >= total_tables:
        raise RuntimeError(
            f"Iceberg maintenance failed on all {total_tables} tables "
            f"(first error: {failures[0][1]})"
        )

    return {
        "tables_processed": total_tables,
        "files_scanned": total_scanned,
        "files_deleted": total_deleted,
        "failures": failures,
    }
