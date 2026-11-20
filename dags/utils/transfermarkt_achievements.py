"""Append-only achievement writer with per-recipient physical receipts."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from types import SimpleNamespace
from typing import Any, Mapping

import pandas as pd

from dags.utils import transfermarkt_current_write as current
from dags.utils.transfermarkt_approval import load_standing_policy
from dags.utils.transfermarkt_current_state import _sql
from scrapers.transfermarkt.achievement_observations import (
    ACHIEVEMENT_OBSERVATION_COLUMNS,
    ACHIEVEMENT_OBSERVATION_KEY,
    AchievementExtraction,
    validate_achievement_extraction,
)
from scrapers.transfermarkt.writer import writer_deadline, writer_lock

ACHIEVEMENT_TABLE = "iceberg.bronze.transfermarkt_achievement_observations"
ACHIEVEMENT_RECEIPTS_TABLE = "iceberg.ops.transfermarkt_achievement_receipts_v1"
ACHIEVEMENT_DAG_ID = "dag_ingest_transfermarkt_achievements"
DEFAULT_POLICY_PATH = "/opt/airflow/dags/configs/transfermarkt/achievements_standing_approval_policy.json"
TIMESTAMP_COLUMNS = frozenset({"captured_at", "observed_at", "extracted_at", "_ingested_at"})
ACHIEVEMENT_WRITER_COLUMNS = (*ACHIEVEMENT_OBSERVATION_COLUMNS, "_source", "_entity_type", "_batch_id", "_ingested_at")
RECEIPT_COLUMNS = (
    "receipt_id", "target_table", "recipient_type", "recipient_id",
    "assessment_manifest_id", "universe_version", "run_id", "cycle_id",
    "source_capture_id", "source_body_hash", "parser_revision", "schema_revision",
    "natural_key_columns_json", "detail_rows", "empty_rows", "row_count",
    "status_counts_json", "plan_sha256", "physical_hash", "committed_batch", "committed_at",
)
RECEIPT_COUNT_COLUMNS = frozenset({"detail_rows", "empty_rows", "row_count"})
RECEIPT_ID_STAMP = datetime(1970, 1, 1, tzinfo=timezone.utc)


def achievement_ddl() -> tuple[str, str]:
    business = ",\n    ".join(
        f"{column} timestamp(6)" if column in TIMESTAMP_COLUMNS else f"{column} varchar"
        for column in ACHIEVEMENT_WRITER_COLUMNS
    )
    receipt = ",\n    ".join(
        f"{column} timestamp(6)" if column == "committed_at"
        else f"{column} bigint" if column in RECEIPT_COUNT_COLUMNS
        else f"{column} varchar"
        for column in RECEIPT_COLUMNS
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {ACHIEVEMENT_TABLE} (\n    {business}\n) WITH (format = 'PARQUET')",
        f"CREATE TABLE IF NOT EXISTS {ACHIEVEMENT_RECEIPTS_TABLE} (\n    {receipt}\n) WITH (format = 'PARQUET')",
    )


def validate_achievement_write_policy(policy) -> None:
    if policy.dag_id != ACHIEVEMENT_DAG_ID:
        raise current.CurrentWriteError("achievement policy has the wrong DAG identity")
    allowed = set(policy.allowed_write_tables)
    missing = tuple(table for table in (ACHIEVEMENT_TABLE, ACHIEVEMENT_RECEIPTS_TABLE) if table not in allowed)
    if missing:
        raise current.CurrentWriteError("achievement policy does not authorize: " + ", ".join(missing))


def _authorize(preflight: Mapping[str, Any]) -> None:
    path = os.environ.get("TM_ACHIEVEMENTS_POLICY_PATH", DEFAULT_POLICY_PATH)
    validate_achievement_write_policy(load_standing_policy(path))
    current._authorize(preflight)


def _batch_id(extraction: AchievementExtraction) -> str:
    return "tm-achievements-batch-" + hashlib.sha256(extraction.run_id.encode()).hexdigest()[:32]


def achievement_rows(extraction: AchievementExtraction) -> tuple[dict[str, Any], ...]:
    errors = validate_achievement_extraction(extraction)
    if errors:
        raise current.CurrentWriteError("achievement structural DQ failed: " + ", ".join(errors))
    batch = _batch_id(extraction)
    rendered = []
    for source_row in extraction.rows:
        row = dict(source_row)
        row.update({
            "_source": "transfermarkt", "_entity_type": "achievement",
            "_batch_id": batch, "_ingested_at": extraction.extracted_at,
        })
        rendered.append({column: row[column] for column in ACHIEVEMENT_WRITER_COLUMNS})
    keys = [tuple(row[column] for column in ACHIEVEMENT_OBSERVATION_KEY) for row in rendered]
    if len(set(keys)) != len(keys):
        raise current.CurrentWriteError("achievement extraction has duplicate physical keys")
    return tuple(rendered)


def _frame(extraction: AchievementExtraction) -> pd.DataFrame:
    frame = pd.DataFrame(achievement_rows(extraction), columns=ACHIEVEMENT_WRITER_COLUMNS)
    for column in TIMESTAMP_COLUMNS:
        frame[column] = pd.to_datetime(frame[column], utc=True).dt.tz_localize(None)
    return frame


def _canonical_cell(column: str, value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if column in TIMESTAMP_COLUMNS:
        stamp = pd.Timestamp(value)
        if stamp.tzinfo is not None:
            stamp = stamp.tz_convert("UTC").tz_localize(None)
        return stamp.isoformat(timespec="microseconds")
    return current._cell(column, value)


def _canonical_row(values: Mapping[str, Any]) -> dict[str, Any]:
    return {column: _canonical_cell(column, values[column]) for column in ACHIEVEMENT_WRITER_COLUMNS}


def _read_existing(connection, extraction: AchievementExtraction) -> pd.DataFrame:
    predicate = " AND ".join(f"{column} = ?" for column in ("recipient_type", "recipient_id", "source_capture_id"))
    params = [extraction.recipient.recipient_type, extraction.recipient.recipient_id, extraction.raw_capture_id]
    try:
        frame = current.run._query_dataframe(
            connection,
            f'SELECT {", ".join(ACHIEVEMENT_WRITER_COLUMNS)} FROM {ACHIEVEMENT_TABLE} WHERE {predicate}',
            params,
        )
    except Exception as exc:
        if not current._missing_table(exc):
            raise
        frame = pd.DataFrame(columns=list(ACHIEVEMENT_WRITER_COLUMNS))
    return frame.sort_values(list(ACHIEVEMENT_OBSERVATION_KEY), kind="stable").reset_index(drop=True)


def _verify_existing(existing: pd.DataFrame, extraction: AchievementExtraction) -> None:
    expected = [_canonical_row(row) for row in achievement_rows(extraction)]
    actual = [
        _canonical_row(dict(zip(existing.columns, row)))
        for row in existing.itertuples(index=False, name=None)
    ]
    if len(actual) != len(expected):
        raise current.CurrentWriteError("achievement physical readback has missing or extra capture rows")
    expected_by_key = {tuple(row[column] for column in ACHIEVEMENT_OBSERVATION_KEY): row for row in expected}
    actual_by_key = {tuple(row[column] for column in ACHIEVEMENT_OBSERVATION_KEY): row for row in actual}
    if len(actual_by_key) != len(actual) or actual_by_key != expected_by_key:
        raise current.CurrentWriteError("achievement physical readback differs from the immutable capture plan")


def _receipt_values(
    extraction: AchievementExtraction,
    physical_hash: str,
    committed_at: datetime,
) -> dict[str, Any]:
    if not isinstance(committed_at, datetime) or committed_at.tzinfo is None or committed_at.utcoffset() is None:
        raise current.CurrentWriteError("committed_at must be timezone-aware")
    plan = extraction.plan_sha256()
    status_counts: dict[str, int] = {}
    for row in extraction.rows:
        status = str(row["observation_status"])
        status_counts[status] = status_counts.get(status, 0) + 1
    first = extraction.rows[0]
    return {
        "receipt_id": "tm-achievement-receipt-" + hashlib.sha256(plan.encode()).hexdigest()[:32],
        "target_table": ACHIEVEMENT_TABLE,
        "recipient_type": extraction.recipient.recipient_type,
        "recipient_id": extraction.recipient.recipient_id,
        "assessment_manifest_id": extraction.assessment_manifest_id,
        "universe_version": extraction.universe_version,
        "run_id": extraction.run_id,
        "cycle_id": extraction.cycle_id,
        "source_capture_id": extraction.raw_capture_id,
        "source_body_hash": extraction.source_body_hash,
        "parser_revision": first["parser_revision"],
        "schema_revision": first["schema_revision"],
        "natural_key_columns_json": json.dumps(ACHIEVEMENT_OBSERVATION_KEY, separators=(",", ":")),
        "detail_rows": status_counts.get("observed_present", 0),
        "empty_rows": status_counts.get("observed_empty", 0),
        "row_count": len(extraction.rows),
        "status_counts_json": json.dumps(status_counts, sort_keys=True, separators=(",", ":")),
        "plan_sha256": plan,
        "physical_hash": physical_hash,
        "committed_batch": _batch_id(extraction),
        "committed_at": committed_at.astimezone(timezone.utc).replace(tzinfo=None),
    }


def _receipt_id(extraction: AchievementExtraction) -> str:
    return _receipt_values(extraction, "", RECEIPT_ID_STAMP)["receipt_id"]


def _stored_receipt(connection, receipt_id: str) -> pd.DataFrame:
    return current.run._query_dataframe(
        connection,
        f'SELECT {", ".join(RECEIPT_COLUMNS)} FROM {ACHIEVEMENT_RECEIPTS_TABLE} WHERE receipt_id = ?',
        [receipt_id],
    )


def _receipt_row(row: Mapping[str, Any]) -> dict[str, Any]:
    values = {}
    for column in RECEIPT_COLUMNS:
        value = row[column]
        if column == "committed_at":
            if value is None or pd.isna(value):
                raise current.CurrentWriteError("achievement receipt has no committed_at")
            values[column] = pd.Timestamp(value).to_pydatetime()
        elif value is None or pd.isna(value):
            values[column] = None
        elif column in RECEIPT_COUNT_COLUMNS:
            values[column] = int(value)
        else:
            values[column] = str(value)
    return values


def _require_receipt(actual: Mapping[str, Any], expected: Mapping[str, Any], stage: str) -> None:
    changed = []
    for column in RECEIPT_COLUMNS:
        left, right = actual[column], expected[column]
        if column == "committed_at":
            left = pd.Timestamp(left).to_pydatetime().replace(tzinfo=timezone.utc)
            right = pd.Timestamp(right).to_pydatetime().replace(tzinfo=timezone.utc)
        elif column in RECEIPT_COUNT_COLUMNS:
            left, right = int(left), int(right)
        if left != right:
            changed.append(column)
    if changed:
        raise current.CurrentWriteError("achievement receipt " + stage + " differs: " + ", ".join(changed))


def _merge_receipt(connection, values: Mapping[str, Any]) -> None:
    columns = ", ".join(RECEIPT_COLUMNS)
    literals = ", ".join(
        _sql(value.replace(tzinfo=timezone.utc)) if column == "committed_at" else _sql(value)
        for column, value in values.items()
    )
    updates = ", ".join(f"{column} = s.{column}" for column in RECEIPT_COLUMNS if column != "receipt_id")
    current.run._execute_cursor(
        connection,
        f"MERGE INTO {ACHIEVEMENT_RECEIPTS_TABLE} t USING (VALUES ({literals})) s({columns}) "
        "ON t.receipt_id = s.receipt_id "
        f"WHEN MATCHED THEN UPDATE SET {updates} "
        f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ("
        + ", ".join(f"s.{column}" for column in RECEIPT_COLUMNS) + ")",
    )


def _return_receipt(values: Mapping[str, Any], physical: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(values)
    result.update({
        "verified": True, "receipt_readback": True, "physical": physical,
        "physical_hash": physical["physical_hash"],
        "natural_key_columns": list(ACHIEVEMENT_OBSERVATION_KEY),
        "status_counts": json.loads(values["status_counts_json"]),
        "committed_at": values["committed_at"].replace(tzinfo=timezone.utc).isoformat(),
    })
    return result


def write_achievement_observations(
    scraper,
    extraction: AchievementExtraction,
    preflight: Mapping[str, Any],
    *,
    committed_at: datetime | None = None,
) -> dict[str, Any]:
    _authorize(preflight)
    if not isinstance(extraction, AchievementExtraction):
        raise current.CurrentWriteError("extraction must be AchievementExtraction")
    if committed_at is None:
        committed_at = datetime.now(timezone.utc)
    frame = _frame(extraction)
    output = SimpleNamespace(
        table_name="transfermarkt_achievement_observations", key="achievements",
        is_legacy=False, replace_keys=(),
    )
    with writer_deadline(getattr(scraper, "_current_deadline_monotonic", None)), writer_lock():
        _authorize(preflight)
        connection = scraper._bronze_connection()
        try:
            for ddl in achievement_ddl():
                current.run._execute_cursor(connection, ddl)
            existing = _read_existing(connection, extraction)
            _verify_existing(existing, extraction) if len(existing) else None
            stored = _stored_receipt(connection, _receipt_id(extraction))
            if len(stored) > 1:
                raise current.CurrentWriteError("multiple receipts exist for one achievement plan")
            if not len(existing):
                scraper._iceberg_writer.write_dataframe(
                    df=frame, database="bronze", table="transfermarkt_achievement_observations",
                    merge_keys=list(ACHIEVEMENT_OBSERVATION_KEY), add_metadata=False, source="transfermarkt",
                )
            physical = current._read_back(scraper, output, frame, extraction.run_id)
            existing = _read_existing(connection, extraction)
            _verify_existing(existing, extraction)
            _authorize(preflight)
            if len(stored):
                stored_values = _receipt_row(stored.iloc[0])
                expected = _receipt_values(
                    extraction, physical["physical_hash"],
                    stored_values["committed_at"].replace(tzinfo=timezone.utc),
                )
                _require_receipt(stored_values, expected, "stored")
                return _return_receipt(stored_values, physical)
            values = _receipt_values(extraction, physical["physical_hash"], committed_at)
            _merge_receipt(connection, values)
            readback = _stored_receipt(connection, values["receipt_id"])
            if len(readback) != 1:
                raise current.CurrentWriteError("achievement receipt readback is missing")
            _require_receipt(_receipt_row(readback.iloc[0]), values, "readback")
            return _return_receipt(values, physical)
        finally:
            connection.close()
