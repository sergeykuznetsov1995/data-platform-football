"""DDL of the ESPN bronze tables of the new contour (#1503)."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from scrapers.espn.bronze_rows import (
    BatchStamp,
    RawRef,
    event_rows,
    lineup_rows,
    match_row,
    team_stats_rows,
)
from scrapers.espn.bronze_schema import (
    LINEAGE_FIELDS,
    NATURAL_KEYS,
    PARTITION_SPEC,
    TABLES,
    ensure_bronze_tables,
)
from scrapers.espn.parser_contracts import LineupRow, MatchEventRow, MatchsheetRow
from tests.unit.scrapers.test_espn_probes import _parse

RAW = RawRef(
    "s3://raw/blobs/sha256/ab/ab.json.gz", "ab" * 32, datetime(2026, 9, 25, 10)
)
STAMP = BatchStamp("0" * 32, datetime(2026, 9, 25, 11))


@pytest.mark.unit
def test_four_tables_with_their_names() -> None:
    assert list(TABLES) == [
        "espn_match",
        "espn_match_lineup",
        "espn_team_stats",
        "espn_match_events",
    ]
    assert set(NATURAL_KEYS) == set(TABLES)


@pytest.mark.unit
def test_partitions_scope_and_lineage_are_shared() -> None:
    assert PARTITION_SPEC == [
        ("competition_slug", "identity"),
        ("season_year", "identity"),
    ]
    lineage = pa.schema(list(LINEAGE_FIELDS))
    for schema in TABLES.values():
        assert schema.names[:3] == ["competition_slug", "season_year", "event_id"]
        tail = pa.schema(list(schema)[-len(LINEAGE_FIELDS) :])
        assert tail.equals(lineage)
        assert len(set(schema.names)) == len(schema.names)


@pytest.mark.unit
def test_no_legacy_or_bloated_columns() -> None:
    forbidden = {
        "espn_lineup",
        "roster",
        "extra_json",
        "statistics_json",
        "scope_id",
        "generation_id",
        "league",
        "season",
        "game",
    }
    assert "espn_lineup" not in TABLES
    for schema in TABLES.values():
        assert not forbidden & set(schema.names)
        for field in schema:
            assert not pa.types.is_null(field.type)


@pytest.mark.unit
def test_every_contract_field_has_a_home_or_a_reason() -> None:
    """No dead column: every data column comes from a parser contract field."""
    consumed_elsewhere = {
        "scope_id",
        "competition_id",
        "source_season_year",
        "is_home",
        "statistics_json",
        "stat_map_version",
        "league",
        "season",
        "game",
        "parser_version",
        "extra_json",
        "roster",
        # Match facts of the matchsheet rows live on espn_match.
        "score",
        "venue_id",
        "venue",
        "attendance",
        "referee",
        "score_h1",
        "score_h2",
        "score_et",
        "shootout_score",
        "aggregate_score",
        "advance",
        "leg",
    }
    for contract, table in (
        (LineupRow, "espn_match_lineup"),
        (MatchsheetRow, "espn_team_stats"),
        (MatchEventRow, "espn_match_events"),
    ):
        missing = (
            set(contract.__slots__) - set(TABLES[table].names) - consumed_elsewhere
        )
        assert missing == set(), (table, missing)


@pytest.mark.unit
def test_ensure_bronze_tables_creates_each_table_with_partitions() -> None:
    writer = MagicMock()

    ensure_bronze_tables(writer)

    calls = writer.create_table_if_not_exists.call_args_list
    assert [call.args[:2] for call in calls] == [("bronze", name) for name in TABLES]
    for call in calls:
        assert call.args[2] is TABLES[call.args[1]]
        assert call.kwargs["partition_spec"] == PARTITION_SPEC


@pytest.mark.unit
def test_ensure_bronze_tables_emits_partitioned_ddl() -> None:
    with patch.dict("sys.modules", {"trino": MagicMock(), "trino.dbapi": MagicMock()}):
        from scrapers.base.iceberg_writer import IcebergWriter
        from scrapers.base.trino_manager import TrinoTableManager

    writer = IcebergWriter()
    manager = MagicMock()
    manager.table_exists.return_value = False
    manager.arrow_schema_to_trino.side_effect = lambda schema: (
        TrinoTableManager.arrow_schema_to_trino(manager, schema)
    )
    writer._trino_manager = manager

    ensure_bronze_tables(writer)

    created = manager.create_iceberg_table.call_args_list
    assert len(created) == 4
    for call in created:
        assert call.kwargs["partition_columns"] == ["competition_slug", "season_year"]
        assert call.kwargs["schema"] == "bronze"
    match_columns = created[0].kwargs["columns"]
    assert match_columns["event_id"] == "BIGINT"
    assert match_columns["kickoff"] == "TIMESTAMP"
    assert match_columns["played_final"] == "BOOLEAN"


@pytest.mark.unit
@pytest.mark.parametrize(
    "name", ["summary_eng1_2020.json", "summary_jpn1_2026_ten_starters.json"]
)
def test_row_functions_match_the_ddl_exactly(name: str) -> None:
    """Contract: column set and types of each row function == the table DDL."""
    event, summary = _parse(name)
    for table, build in (
        ("espn_match", lambda: [match_row(event, summary, raw=RAW, stamp=STAMP)]),
        (
            "espn_match_lineup",
            lambda: lineup_rows(event, summary, raw=RAW, stamp=STAMP),
        ),
        (
            "espn_team_stats",
            lambda: team_stats_rows(event, summary, raw=RAW, stamp=STAMP),
        ),
        ("espn_match_events", lambda: event_rows(event, summary, raw=RAW, stamp=STAMP)),
    ):
        rows = build()
        assert rows, table
        for row in rows:
            assert list(row) == TABLES[table].names, table
        # Raises on any value that does not fit the column type.
        pa.Table.from_pylist(rows, schema=TABLES[table])


@pytest.mark.unit
def test_match_row_without_summary_matches_the_ddl() -> None:
    event, _ = _parse("summary_eng1_2020.json")
    row = match_row(event, None, raw=RAW, stamp=STAMP)
    assert list(row) == TABLES["espn_match"].names
    pa.Table.from_pylist([row], schema=TABLES["espn_match"])
