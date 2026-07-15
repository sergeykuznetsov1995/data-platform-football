"""Opt-in destructive-path smoke test against a real Trino/Iceberg catalog."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
import uuid

import pytest


def _execute(conn, sql: str):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        cursor.close()


@pytest.mark.integration
def test_generic_stage_apply_drops_only_redundant_real_iceberg_stage(
    monkeypatch,
):
    if os.environ.get("FBREF_TEST_TRINO_APPLY") != "1":
        pytest.skip("set FBREF_TEST_TRINO_APPLY=1 for destructive Trino smoke")

    import utils.maintenance_tasks as maintenance

    refresh_id = str(uuid.uuid4())
    stage = (
        "fbref_table_cells__stg_lr_"
        f"{uuid.UUID(refresh_id).hex}_c"
    )
    qualified = f'iceberg.bronze."{stage}"'
    source_run_id = str(uuid.uuid4())
    replay_run_id = str(uuid.uuid4())
    fenced: list[tuple[str, str]] = []
    conn = maintenance._connect()
    try:
        tables = {
            str(row[0])
            for row in _execute(conn, "SHOW TABLES FROM iceberg.bronze")
        }
        if "fbref_table_cells" not in tables:
            pytest.skip("real catalog has no fbref_table_cells live contract")
        _execute(
            conn,
            f"CREATE TABLE {qualified} AS "
            "SELECT * FROM iceberg.bronze.fbref_table_cells WHERE false",
        )
        if int(
            _execute(
                conn,
                "SELECT count(*) FROM iceberg.bronze.fbref_table_cells",
            )[0][0]
            or 0
        ) == 0:
            pytest.skip("real fbref_table_cells contract has no row to prove")
        columns = [
            str(row[0])
            for row in _execute(
                conn, "DESCRIBE iceberg.bronze.fbref_table_cells"
            )
        ]
        projection = ", ".join(
            f"'{replay_run_id}' AS \"run_id\""
            if column == "run_id"
            else f'"{column}"'
            for column in columns
        )
        _execute(
            conn,
            f"INSERT INTO {qualified} SELECT {projection} "
            "FROM iceberg.bronze.fbref_table_cells LIMIT 1",
        )
        # Scope this destructive smoke to the isolated table created above;
        # a developer's catalog may contain unrelated retained recovery stages.
        monkeypatch.setattr(
            maintenance, "_list_tables", lambda *_args: [stage]
        )

        result = maintenance.janitor_fbref_generic_stages(
            conn=conn,
            owner_lookup=lambda logical_refresh_id: {
                "run_id": source_run_id,
                "run_status": "succeeded",
                "terminal": True,
                "active_fetch_lease": False,
                "active_budget_reservation": False,
                "active_observation_processing": False,
            }
            if logical_refresh_id == refresh_id
            else None,
            run_lookup=lambda run_id: {
                "run_id": run_id,
                "status": "succeeded",
            }
            if run_id == replay_run_id
            else None,
            before_drop=lambda table, logical_refresh_id: fenced.append(
                (table, logical_refresh_id)
            ),
            apply=True,
            # Preserve the production 24h floor while making this freshly
            # created, isolated stage old enough in the test's observation.
            now=datetime.now(timezone.utc) + timedelta(hours=25),
        )

        decision = next(
            item for item in result["decisions"] if item["stage"] == stage
        )
        assert decision["action"] == "dropped"
        assert decision["owner_run_id"] == source_run_id
        assert decision["stage_run_ids"] == [replay_run_id]
        assert decision["semantic_delta_rows"] == 0
        assert fenced == [(stage, refresh_id)]
        assert stage not in {
            str(row[0])
            for row in _execute(conn, "SHOW TABLES FROM iceberg.bronze")
        }
    finally:
        try:
            _execute(conn, f"DROP TABLE IF EXISTS {qualified}")
        finally:
            conn.close()
