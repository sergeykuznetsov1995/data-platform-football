"""Migration SQL exercised against local in-memory tables; no production I/O."""

import re

import duckdb
import pytest

from scrapers.understat.migrate_1440 import BACKUP, COPY, TABLE, ScheduleMigration


class Query:
    def __init__(self):
        self.connection = duckdb.connect()
        self.connection.execute("ATTACH ':memory:' AS iceberg")
        self.connection.execute("CREATE SCHEMA iceberg.bronze")
        self.definition = '''(
            league varchar, season varchar, game_id bigint,
            forecast_home_win varchar,
            forecast_draw varchar,
            forecast_away_win varchar,
            _batch_id varchar, other varchar
        )'''
        self.connection.execute(f"CREATE TABLE {TABLE} {self.definition}")
        self.connection.execute(f"INSERT INTO {TABLE} VALUES "
                                "('EPL', '2526', 1, '0.25', '0', '1', 'batch', NULL), "
                                "('EPL', '2526', 2, NULL, NULL, NULL, 'batch', 'kept')")
        self.calls = []
        self.corrupt = False

    def execute_query(self, sql):
        self.calls.append(sql)
        if sql.startswith("SHOW CREATE TABLE"):
            return [(f"CREATE TABLE {TABLE} {self.definition}\nWITH (\n"
                     " format = 'PARQUET',\n partitioning = ARRAY['league', 'season'],\n"
                     " location = 's3://warehouse/old'\n)",)]
        if sql.startswith("SHOW COLUMNS"):
            return [(row[0], row[1].lower()) for row in self.connection.execute(
                sql.replace("SHOW COLUMNS FROM", "DESCRIBE")
            ).fetchall()]
        if sql.startswith("ALTER TABLE"):
            sql = re.sub(r"RENAME TO [\w.]+", lambda m: "RENAME TO " + m[0].rsplit('.', 1)[-1], sql)
        if sql.startswith("CREATE TABLE"):
            sql = sql.split("WITH (")[0]
        sql = sql.replace("iceberg.information_schema.tables", "information_schema.tables")
        result = self.connection.execute(sql).fetchall()
        if self.corrupt and sql.startswith("INSERT INTO"):
            self.connection.execute(f"UPDATE {COPY} SET other = 'corrupt' WHERE game_id = 1")
        return result


def test_migration_dry_run_is_read_only_and_preserves_partition_properties():
    query = Query()
    ddl = ScheduleMigration(query).run()
    assert "partitioning = ARRAY['league', 'season']" in ddl
    assert "location" not in ddl
    assert "forecast_draw double" in ddl
    assert not any(sql.startswith(("CREATE", "INSERT", "ALTER", "DROP")) for sql in query.calls)


def test_migration_apply_and_rollback_preserve_keys_nulls_values_and_batch():
    query = Query()
    migration = ScheduleMigration(query)
    original = query.connection.execute(f"SELECT * FROM {TABLE} ORDER BY game_id").fetchall()
    migration.run("apply", publication_paused=True)
    rows = query.connection.execute(f"SELECT * FROM {TABLE} ORDER BY game_id").fetchall()
    assert rows == [('EPL', '2526', 1, 0.25, 0.0, 1.0, 'batch', None),
                    ('EPL', '2526', 2, None, None, None, 'batch', 'kept')]
    assert query.connection.execute(f"SELECT * FROM {BACKUP} ORDER BY game_id").fetchall() == original
    migration.run("rollback", publication_paused=True)
    assert query.connection.execute(f"SELECT * FROM {TABLE} ORDER BY game_id").fetchall() == original


@pytest.mark.parametrize("failure", ["invalid", "duplicate", "null_key", "mismatch", "retained"])
def test_migration_blocks_unsafe_switch(failure):
    query = Query()
    if failure == "invalid":
        query.connection.execute(f"UPDATE {TABLE} SET forecast_draw = 'bad'")
    elif failure == "duplicate":
        query.connection.execute(f"INSERT INTO {TABLE} SELECT * FROM {TABLE}")
    elif failure == "null_key":
        query.connection.execute(f"UPDATE {TABLE} SET game_id = NULL")
    elif failure == "mismatch":
        query.corrupt = True
    else:
        query.connection.execute(f"CREATE TABLE {BACKUP} (retained bigint)")
    with pytest.raises((RuntimeError, duckdb.ConversionException)):
        ScheduleMigration(query).run("apply", publication_paused=True)
    assert not any(sql.startswith("ALTER") for sql in query.calls)


def test_rollback_refuses_to_discard_new_publications():
    query = Query()
    migration = ScheduleMigration(query)
    migration.run("apply", publication_paused=True)
    query.connection.execute(f"UPDATE {TABLE} SET _batch_id = 'new-batch'")
    query.calls.clear()
    with pytest.raises(RuntimeError, match="data mismatch"):
        migration.run("rollback", publication_paused=True)
    assert not any(sql.startswith("ALTER") for sql in query.calls)


@pytest.mark.parametrize("mode", ["apply", "rollback"])
def test_mutating_modes_require_explicit_publication_pause(mode):
    query = Query()
    with pytest.raises(ValueError, match="Pause"):
        ScheduleMigration(query).run(mode)
    assert query.calls == []
