from collections import Counter
from datetime import datetime, timedelta
from unittest.mock import Mock

import duckdb
import pandas as pd
import pytest

from scrapers.base.sql_validator import validate_catalog_qualified_name
from scrapers.base.trino_manager import SQL_BYTE_BUDGET, TrinoAmbiguousWriteError, TrinoError
from scrapers.espn.bronze_schema import (
    BRONZE_DATABASE, EVENTS_TABLE, LINEAGE_FIELDS, LINEUP_TABLE, TABLES, TEAM_STATS_TABLE,
)
from scrapers.espn.bronze_writer import WRITE_ORDER, delete_filter, write_ready_batches
from scrapers.espn.ready_replace import replace_ready_frame
from scrapers.espn.trino_manager import EspnTrinoTableManager
from tests.unit.scrapers.test_espn_bronze_writer import _match
from tests.unit.scrapers.test_espn_grouped_writer import _edition


pytestmark = pytest.mark.unit


@pytest.fixture
def manager():
    # Exercise the source manager's real scalar and schema formatters offline.
    source = object.__new__(EspnTrinoTableManager)
    source.catalog = "iceberg"
    source._execute_replacement = Mock()
    source.insert_dataframe_atomic = Mock(side_effect=AssertionError("unexpected stage"))
    return source


@pytest.fixture
def db(manager):
    with duckdb.connect() as conn:
        conn.execute("ATTACH ':memory:' AS iceberg")
        conn.execute(f"CREATE SCHEMA iceberg.{BRONZE_DATABASE}")
        conn.execute(f"USE iceberg.{BRONZE_DATABASE}")
        for table, schema in TABLES.items():
            types = manager.arrow_schema_to_trino(schema)
            fields = ", ".join(f'"{name}" {types[name]}' for name in schema.names)
            conn.execute(f"CREATE TABLE iceberg.{BRONZE_DATABASE}.{table} ({fields})")
        manager._execute_replacement.side_effect = conn.execute
        yield conn


_SLUG = "coupé's"
_KEYS = ((_SLUG, 2020, 7), (_SLUG, 2021, 7))
_SCOPE = " OR ".join(f"({delete_filter(slug, year, [event])})" for slug, year, event in _KEYS)


def _stats_frame(*rows):
    return pd.DataFrame(rows, columns=TABLES[TEAM_STATS_TABLE].names, dtype=object)


def _publish_stats(manager, frame):
    return replace_ready_frame(
        manager, TEAM_STATS_TABLE, frame, match_keys=_KEYS,
        delete_filter=_SCOPE, staging_id="bphysical",
    )


def test_merge_replaces_all_children_for_selected_matches(manager, db):
    table = f"iceberg.{BRONZE_DATABASE}.{TEAM_STATS_TABLE}"
    sibling_keys = [(_SLUG, 2020, 8), ("sibling", 2021, 7)]
    for slug, year, event in [*_KEYS, *sibling_keys]:
        db.execute(
            f"INSERT INTO {table} (competition_slug, season_year, event_id, team_id) "
            "VALUES (?, ?, ?, 1), (?, ?, ?, 2)",
            [slug, year, event] * 2,
        )

    stamp = datetime(2020, 6, 5, 4, 3, 2, 123456)
    frame = _stats_frame({
        "competition_slug": _SLUG, "season_year": 2020, "event_id": 7,
        "team_id": 3, "raw_uri": "Pelé's ⚽", "_ingested_at": stamp,
    })
    assert _publish_stats(manager, frame) == 1
    keys = "competition_slug, season_year, event_id, team_id"
    survivors = [(*key, team) for key in sibling_keys for team in (1, 2)]
    assert sorted(db.execute(f"SELECT {keys} FROM {table}").fetchall()) == sorted(
        [*survivors, (*_KEYS[0], 3)]
    )
    assert db.execute(
        f"SELECT raw_uri, parser_version, _ingested_at FROM {table} WHERE team_id = 3"
    ).fetchone() == ("Pelé's ⚽", None, stamp)

    assert _publish_stats(manager, _stats_frame()) == 0
    assert sorted(db.execute(f"SELECT {keys} FROM {table}").fetchall()) == sorted(survivors)
    assert manager._execute_replacement.call_count == 2
    manager.insert_dataframe_atomic.assert_not_called()


def test_grouped_writer_uses_one_merge_per_table_and_keeps_edition_stamps(manager, db):
    manager.execute_query = lambda sql, params=None: db.execute(sql, params).fetchall()
    receipts = write_ready_batches(
        [_edition(year, _match(7, "aa")) for year in (2020, 2021)],
        trino=manager, replace_frame=replace_ready_frame,
    )
    assert len({receipt.batch_id for receipt in receipts.values()}) == 2
    calls = manager._execute_replacement.call_args_list
    assert len(calls) == len(WRITE_ORDER)
    for table, call in zip(WRITE_ORDER, calls):
        target = validate_catalog_qualified_name(manager.catalog, BRONZE_DATABASE, table)
        assert call.args[0].startswith(f"MERGE INTO {target} ")
        stored = db.execute(
            f"SELECT season_year, _batch_id, count(*) FROM {target} GROUP BY 1, 2"
        ).fetchall()
        assert sorted(stored) == sorted(
            (year, receipt.batch_id, receipt.rows_per_table[table])
            for (_, year), receipt in receipts.items() if receipt.rows_per_table[table]
        )
    manager.insert_dataframe_atomic.assert_not_called()


def test_merge_byte_limit_includes_complete_utf8_statement(manager):
    frame = _stats_frame({
        "competition_slug": _SLUG, "season_year": 2020, "event_id": 7, "raw_uri": "⚽'",
    })
    assert _publish_stats(manager, frame) == 1
    sample = manager._execute_replacement.call_args.args[0]
    frame.loc[0, "raw_uri"] += "x" * (SQL_BYTE_BUDGET - len(sample.encode("utf-8")))
    manager._execute_replacement.reset_mock()

    assert _publish_stats(manager, frame) == 1
    manager._execute_replacement.assert_called_once()
    assert len(manager._execute_replacement.call_args.args[0].encode("utf-8")) == SQL_BYTE_BUDGET
    manager.insert_dataframe_atomic.assert_not_called()

    frame.loc[0, "raw_uri"] += "x"
    manager.insert_dataframe_atomic.side_effect = None
    manager.insert_dataframe_atomic.return_value = 1
    assert _publish_stats(manager, frame) == 1
    manager._execute_replacement.assert_called_once()
    manager.insert_dataframe_atomic.assert_called_once_with(
        BRONZE_DATABASE, TEAM_STATS_TABLE, frame, delete_filter=_SCOPE,
        staging_id="bphysical", single_statement_replace=True,
        target_column_types=manager.arrow_schema_to_trino(TABLES[TEAM_STATS_TABLE]),
    )


def test_ambiguous_direct_submission_propagates_without_staging(manager):
    failure = RuntimeError("ambiguous commit")
    manager._execute_replacement.side_effect = failure
    with pytest.raises(RuntimeError) as error:
        _publish_stats(manager, _stats_frame())
    assert error.value is failure
    manager._execute_replacement.assert_called_once()
    manager.insert_dataframe_atomic.assert_not_called()


def _child_frame(table, rows):
    columns = TABLES[table].names
    return pd.DataFrame(rows, columns=columns, dtype=object)


def _publish_child(manager, table, frame, keys=_KEYS):
    return replace_ready_frame(
        manager, table, frame, match_keys=iter(keys), delete_filter=_SCOPE, staging_id="bphysical",
    )


def _execute_stage_in_duckdb(db, manager, table):
    # DuckDB unnests a list of structs into one struct column. Adapt only the
    # Trino row-array expansion, executing the VALUES, projection and DML as emitted.
    types = manager.arrow_schema_to_trino(TABLES[table])
    children = TABLES[table].names[3:-len(LINEAGE_FIELDS)]
    expansion = (
        ' CROSS JOIN UNNEST(CAST(json_parse(g."_payload") AS ARRAY(ROW('
        + ", ".join(types[name] for name in children)
        + ')))) AS c(' + ", ".join(f'"{name}"' for name in children) + ')'
    )
    extracts = ", ".join(
        f"""CAST(json_extract_string(item.value, '$[{index}]') AS {types[name]}) AS "{name}" """
        for index, name in enumerate(children)
    )
    staged_counts = []

    def execute(sql, fetch=False, *, replay_safe=True):
        assert len(sql.encode("utf-8")) <= SQL_BYTE_BUDGET
        if sql.startswith(("CREATE TABLE", "INSERT INTO")):
            assert not replay_safe
            assert sql.endswith(expansion)
            sql = sql[:-len(expansion)] + (
                f' CROSS JOIN LATERAL (SELECT {extracts} FROM json_each(g."_payload") AS item) AS c'
            )
        result = db.execute(sql)
        if fetch:
            rows = result.fetchall()
            staged_counts.append(rows[0][0])
            return rows

    manager._execute = Mock(side_effect=execute)
    return staged_counts


@pytest.mark.parametrize("table", [EVENTS_TABLE, LINEUP_TABLE])
def test_large_child_stage_packs_actual_rows_and_replaces_full_scope(manager, db, table):
    staged_counts = _execute_stage_in_duckdb(db, manager, table)
    target = validate_catalog_qualified_name(manager.catalog, BRONZE_DATABASE, table)
    stamp = datetime(2020, 6, 5, 4, 3, 2, 123456)
    lineage = {
        "_source": "espn", "_ingested_at": stamp, "_batch_id": "initial batch",
        "_source_fetched_at": stamp, "raw_uri": "Pelé's", "raw_sha256": "digest",
        "parser_version": "v1",
    }
    key_cols = ("competition_slug", "season_year", "event_id")
    selected = [*_KEYS, (_SLUG, 2020, 9), (_SLUG, 2020, 10)]
    siblings = [(_SLUG, 2020, 8), ("sibling", 2021, 7)]
    for key in [*selected, *siblings]:
        db.execute(
            f"INSERT INTO {target} (competition_slug, season_year, event_id) "
            "VALUES (?, ?, ?), (?, ?, ?)", [*key, *key],
        )

    base = dict.fromkeys(TABLES[table].names)
    base.update(zip(key_cols, selected[0]))
    base.update(lineage)
    wide = "⚽'" * 4200
    if table == EVENTS_TABLE:
        wide_field = "text"
        base.update(event_key="tied", sequence=2**53 + 1, scoring_play=False,
                    athlete_ids='["7","9"]', clock_value=1.2345678901234567)
    else:
        wide_field = "player"
        base.update(athlete_id=2**53 + 1, starter=False, subbed_in=True,
                    substitutions_json='[{"id":"7"}]', appearances=1.2345678901234567)
    dense = {**base, wide_field: wide}
    rows = [dense] * 31
    # Retained children may differ in any single lineage component, even with
    # identical natural keys. Also keep separate an edition and an event sibling.
    for name, value in lineage.items():
        changed = value + timedelta(microseconds=1) if isinstance(value, datetime) else value + "-old"
        rows.append({**base, name: changed})
    rows.extend({**base, **dict(zip(key_cols, key))} for key in selected[1:3])
    frame = _child_frame(table, rows)

    assert _publish_child(manager, table, frame, [*selected, selected[0]]) == len(frame)
    assert staged_counts == [len(frame)]
    sqls = [call.args[0] for call in manager._execute.call_args_list]
    assert [sql.split()[0] for sql in sqls] == ["CREATE", "INSERT", "INSERT", "SELECT", "DROP"]
    stage = sqls[3].removeprefix("SELECT count(*) FROM ")
    assert sqls[0].startswith(f"CREATE TABLE {stage} AS SELECT ")
    assert all(sql.startswith(f"INSERT INTO {stage} SELECT ") for sql in sqls[1:3])
    assert sqls[4] == f"DROP TABLE IF EXISTS {stage}"
    assert all(f"FROM {target} " not in sql for sql in sqls[:4])
    assert all(sql.count("initial batch") < 12 for sql in sqls[:3])
    manager._execute_replacement.assert_called_once()
    merge = manager._execute_replacement.call_args.args[0]
    assert len(merge.encode("utf-8")) <= SQL_BYTE_BUDGET
    assert f"FROM {stage}) AS s(" in merge
    assert merge.count("(TRUE, ") == len(selected)
    types = manager.arrow_schema_to_trino(TABLES[table])
    for slug, year in {key[:2] for key in selected}:
        slug_sql = manager._format_sql_value(slug, types["competition_slug"])
        year_sql = manager._format_sql_value(year, types["season_year"])
        assert f'(t."competition_slug" = {slug_sql} AND t."season_year" = {year_sql})' in merge
    expected = Counter(frame.itertuples(index=False, name=None))
    for key in siblings:
        expected[(*key, *(None for _ in TABLES[table].names[3:]))] += 2
    assert Counter(db.execute(f"SELECT * FROM {target}").fetchall()) == expected
    manager.insert_dataframe_atomic.assert_not_called()


def _two_stage_writes():
    row = {"competition_slug": _SLUG, "season_year": 2020, "event_id": 7, "text": "x" * 155_000}
    return _child_frame(EVENTS_TABLE, [row, row])


@pytest.mark.parametrize("phase", ["CREATE", "INSERT", "MERGE"])
def test_uncertain_large_write_is_never_replayed_or_cleaned_up(manager, phase):
    failure = TrinoAmbiguousWriteError("uncertain commit")

    def execute(sql, fetch=False, *, replay_safe=True):
        if sql.startswith(phase):
            assert not replay_safe
            raise failure
        if fetch:
            return [(2,)]

    manager._execute = Mock(side_effect=execute)
    if phase == "MERGE":
        manager._execute_replacement.side_effect = failure
    with pytest.raises(TrinoAmbiguousWriteError) as error:
        _publish_child(manager, EVENTS_TABLE, _two_stage_writes())
    assert error.value is failure
    actions = [call.args[0].split()[0] for call in manager._execute.call_args_list]
    assert actions == {
        "CREATE": ["CREATE"], "INSERT": ["CREATE", "INSERT"], "MERGE": ["CREATE", "INSERT", "SELECT"],
    }[phase]
    assert manager._execute_replacement.call_count == int(phase == "MERGE")
    manager.insert_dataframe_atomic.assert_not_called()


def test_stage_count_and_committed_outcome_survive_cleanup_failure(manager):
    frame = _two_stage_writes()
    counts = iter([1, 2])

    def execute(sql, fetch=False, **kwargs):
        if fetch:
            return [(next(counts),)]
        if sql.startswith("DROP"):
            raise RuntimeError("drop failed")

    manager._execute = Mock(side_effect=execute)
    with pytest.raises(TrinoError, match="staged 1, expected 2"):
        _publish_child(manager, EVENTS_TABLE, frame)
    manager._execute_replacement.assert_not_called()
    assert _publish_child(manager, EVENTS_TABLE, frame) == 2
    manager._execute_replacement.assert_called_once()
    sqls = [call.args[0] for call in manager._execute.call_args_list]
    stages = [sql.split(" AS SELECT", 1)[0] for sql in sqls if sql.startswith("CREATE")]
    assert len(set(stages)) == 2
    assert sum(sql.startswith("DROP TABLE IF EXISTS ") for sql in sqls) == 2


def test_unbeatable_late_child_row_fails_before_ctas_or_target(manager):
    frame = _two_stage_writes()
    frame.loc[1, "text"] = "⚽'" * (SQL_BYTE_BUDGET // 5)
    manager._execute = Mock()
    with pytest.raises(ValueError, match="single child row exceeds"):
        _publish_child(manager, EVENTS_TABLE, frame)
    manager._execute.assert_not_called()
    manager._execute_replacement.assert_not_called()
    manager.insert_dataframe_atomic.assert_not_called()


def test_first_ctas_can_use_the_entire_sql_budget(manager):
    manager._execute = Mock(return_value=[(2,)])
    frame = _two_stage_writes()
    assert _publish_child(manager, EVENTS_TABLE, frame) == 2
    first = manager._execute.call_args_list[0].args[0]
    frame.loc[0, "text"] += "x" * (SQL_BYTE_BUDGET - len(first.encode("utf-8")))
    manager._execute.reset_mock()
    assert _publish_child(manager, EVENTS_TABLE, frame) == 2
    first = manager._execute.call_args_list[0].args[0]
    assert first.startswith("CREATE TABLE ")
    assert len(first.encode("utf-8")) == SQL_BYTE_BUDGET


def test_invalid_late_scalar_is_rejected_before_writing(manager):
    frame = _two_stage_writes()
    frame = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
    frame.loc[2, "clock_value"] = float("inf")
    manager._execute = Mock()
    with pytest.raises(ValueError):
        _publish_child(manager, EVENTS_TABLE, frame)
    manager._execute.assert_not_called()
    manager._execute_replacement.assert_not_called()
