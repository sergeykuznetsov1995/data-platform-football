"""Opt-in Trino/Iceberg smoke: a repeated tombstone replace keeps one row (#1557).

``insert_dataframe_atomic(single_statement_replace=True)`` removes the old rows
with one MERGE matching tombstones ``IS NOT DISTINCT FROM`` on every column.
With dynamic filtering on, a filter built from a join column drops the target
rows whose value is NULL, so a row with any NULL column was never deleted and
every repeat appended a copy.  Sandbox tables get unique names and are dropped.
The base ``TrinoTableManager`` still has dynamic filtering on and must fail
(strict xfail: the bug reproduced); ``EspnTrinoTableManager`` must pass.

Run: ``ESPN_TEST_TRINO_APPLY=1 TRINO_HOST=… TRINO_PORT=… TRINO_PASSWORD=…``.
"""

from __future__ import annotations

import os
import uuid

import pandas as pd
import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(
    params=[
        pytest.param(
            "base",
            marks=pytest.mark.xfail(
                strict=True,
                raises=AssertionError,
                reason="#1557: dynamic filter loses NULL rows",
            ),
        ),
        "espn",
    ]
)
def manager(request):
    if os.environ.get("ESPN_TEST_TRINO_APPLY") != "1":
        pytest.skip("set ESPN_TEST_TRINO_APPLY=1 for the destructive Trino smoke")
    from scrapers.base.trino_manager import TrinoTableManager
    from scrapers.espn.trino_manager import EspnTrinoTableManager

    factory = {"base": TrinoTableManager, "espn": EspnTrinoTableManager}
    trino = factory[request.param](host=os.environ.get("TRINO_HOST", "trino"))
    try:
        yield trino
    finally:
        trino.close()


def _count(trino, qualified: str) -> int:
    return int(trino._execute(f"SELECT count(*) FROM {qualified}", fetch=True)[0][0])


def test_repeated_replace_of_rows_with_nulls_keeps_one_generation(manager):
    """Replace one live ``espn_match`` partition twice in a sandbox copy."""
    table = f"t1557_nulls_{uuid.uuid4().hex}"
    qualified = f"iceberg.bronze.{table}"
    try:
        # A partitioned copy of the live table: the planner sees the live data
        # volume and statistics, as the production MERGE does.
        manager._execute(
            f"CREATE TABLE {qualified} WITH (partitioning = "
            "ARRAY['competition_slug', 'season_year']) AS SELECT * FROM "
            "iceberg.bronze.espn_match"
        )
        partition = manager._execute(
            f"SELECT competition_slug, season_year FROM {qualified} "
            "WHERE referee IS NULL GROUP BY 1, 2 ORDER BY count(*) DESC, 1, 2 "
            "LIMIT 1",
            fetch=True,
        )
        if not partition:
            pytest.skip("live espn_match has no row with a NULL column")
        slug, year = partition[0]
        where = f"competition_slug = '{slug}' AND season_year = {int(year)}"
        described = manager._execute(f"DESCRIBE {qualified}", fetch=True)
        columns = [str(row[0]) for row in described]
        rows = manager._execute(
            f"SELECT {', '.join(columns)} FROM (SELECT *, row_number() OVER "
            "(PARTITION BY event_id ORDER BY _ingested_at DESC) AS rn "
            f"FROM {qualified} WHERE {where}) WHERE rn = 1",
            fetch=True,
        )
        frame = pd.DataFrame([list(row) for row in rows], columns=columns)
        ids = ", ".join(str(int(value)) for value in sorted(frame["event_id"]))
        for attempt in range(3):
            inserted = manager.insert_dataframe_atomic(
                "bronze",
                table,
                frame,
                delete_filter=f"{where} AND event_id IN ({ids})",
                staging_id=f"b{uuid.uuid4().hex}",
                single_statement_replace=True,
                target_column_types={str(r[0]): str(r[1]) for r in described},
            )
            assert inserted == len(frame), attempt

            assert _count(manager, f"{qualified} WHERE {where}") == len(
                frame
            ), attempt
    finally:
        manager._execute(f"DROP TABLE IF EXISTS {qualified}")


def test_repeated_espn_batch_keeps_one_generation_in_all_four_tables(
    manager, monkeypatch
):
    import scrapers.espn.bronze_writer as bronze_writer
    from scrapers.espn.bronze_schema import TABLES
    from tests.unit.scrapers.test_espn_bronze_writer import _batch, _match

    sandbox = f"t1557_espn_{uuid.uuid4().hex}"
    monkeypatch.setattr(bronze_writer, "BRONZE_DATABASE", sandbox)
    batch = _batch(_match(1, "aa"), _match(2, "aa"))
    created: list[str] = []
    try:
        manager._execute(f"CREATE SCHEMA iceberg.{sandbox}")
        for table in TABLES:
            qualified = f"iceberg.{sandbox}.{table}"
            # A partitioned copy of the live table: the planner sees the
            # live data volume and statistics, as the production MERGE does.
            manager._execute(
                f"CREATE TABLE {qualified} WITH (partitioning = "
                "ARRAY['competition_slug', 'season_year']) AS SELECT * FROM "
                f"iceberg.bronze.{table}"
            )
            created.append(qualified)

        receipts = [
            bronze_writer.write_tournament_batch(batch, trino=manager)
            for _ in range(3)
        ]

        last = receipts[-1]
        assert {r.rows_per_table == last.rows_per_table for r in receipts} == {True}
        for table, expected in last.rows_per_table.items():
            qualified = f"iceberg.{sandbox}.{table}"
            scope = f"{qualified} WHERE competition_slug = 'eng.1' AND season_year = 2020"
            assert _count(manager, scope) == expected, table
            batches = manager._execute(
                f"SELECT DISTINCT _batch_id FROM {scope}", fetch=True
            )
            assert [row[0] for row in batches] == [last.batch_id], table
    finally:
        for qualified in created:
            manager._execute(f"DROP TABLE IF EXISTS {qualified}")
        manager._execute(f"DROP SCHEMA IF EXISTS iceberg.{sandbox}")
