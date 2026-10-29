"""Offline-testable schedule migration. Run as ``python -m ...migrate_1440``.

The operator must pause all Understat publications before apply/rollback and keep
that pause through verification. Renames are not atomic as a pair: if interrupted,
restore the retained backup name before resuming. No table is dropped. Dry-run
only reads; bootstrap is a separate runner command: --bootstrap-manifest.
"""

from __future__ import annotations

import argparse
import re

from .contracts import FORECAST_COLUMNS


TABLE = "iceberg.bronze.understat_schedule"
COPY = TABLE + "_1440_double"
BACKUP = TABLE + "_1440_backup"
KEY = '"league", "season", "game_id"'


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


class ScheduleMigration:
    def __init__(self, query):
        self.query = query

    def _rows(self, sql):
        return self.query.execute_query(sql) or []

    def _count(self, sql):
        return int(self._rows(sql)[0][0])

    def _columns(self, table):
        return [(row[0], row[1]) for row in self._rows(f"SHOW COLUMNS FROM {table}")]

    def _projection(self, columns):
        return ", ".join(
            f"CAST({_quote(name)} AS double) AS {_quote(name)}"
            if name in FORECAST_COLUMNS else _quote(name)
            for name, _ in columns
        )

    def _check_keys(self, table):
        if self._count(
            f'SELECT count(*) FROM {table} WHERE "league" IS NULL '
            'OR "season" IS NULL OR "game_id" IS NULL'
        ) or self._rows(
            f"SELECT {KEY} FROM {table} GROUP BY {KEY} HAVING count(*) <> 1 LIMIT 1"
        ):
            raise RuntimeError(f"Invalid natural keys in {table}")

    def _verify(self, old, new, columns):
        expected = [(name, "double" if name in FORECAST_COLUMNS else kind)
                    for name, kind in columns]
        if self._columns(new) != expected:
            raise RuntimeError("Migration schema mismatch")
        self._check_keys(old)
        self._check_keys(new)
        projection = self._projection(columns)
        names = ", ".join(_quote(name) for name, _ in columns)
        # EXCEPT ALL compares every field, NULL and metadata included, both ways.
        for left, right in (
            (f"SELECT {projection} FROM {old}", f"SELECT {names} FROM {new}"),
            (f"SELECT {names} FROM {new}", f"SELECT {projection} FROM {old}"),
        ):
            if self._count(f"SELECT count(*) FROM (({left}) EXCEPT ALL ({right})) AS delta"):
                raise RuntimeError("Migration data mismatch; refusing switch")

    def _copy_ddl(self):
        ddl = self._rows(f"SHOW CREATE TABLE {TABLE}")[0][0]
        ddl, count = re.subn(r"\ACREATE TABLE\s+[^\s(]+", f"CREATE TABLE {COPY}", ddl)
        if count != 1:
            raise RuntimeError("Unsupported SHOW CREATE TABLE format")
        for column in FORECAST_COLUMNS:
            ddl, count = re.subn(
                rf'(?m)^(\s*"?{column}"?\s+)varchar(?:\(\d+\))?(\s*[,\n])',
                r'\1double\2', ddl,
            )
            if count != 1:
                raise RuntimeError(f"Expected one varchar declaration for {column}")
        # A copy needs its own physical location; retain format and partitioning.
        ddl = re.sub(r"(?m)^\s*location\s*=\s*'(?:[^']|'')*'\s*,?\s*$", "", ddl)
        ddl = re.sub(r",\s*\)", "\n)", ddl)
        if re.search(r"\blocation\s*=", ddl, re.I):
            raise RuntimeError("Unsupported location property; refusing shared storage")
        return ddl

    def run(self, mode="dry-run", *, publication_paused=False):
        if mode not in {"dry-run", "apply", "rollback"}:
            raise ValueError("Unknown migration mode")
        if mode != "dry-run" and not publication_paused:
            raise ValueError("Pause Understat publications and pass --publication-paused")
        if mode == "rollback":
            columns = self._columns(BACKUP)
            self._verify(BACKUP, TABLE, columns)
            self._require_absent(COPY)
            self._rows(f"ALTER TABLE {TABLE} RENAME TO {COPY}")
            self._rows(f"ALTER TABLE {BACKUP} RENAME TO {TABLE}")
            return
        columns = self._columns(TABLE)
        types = dict(columns)
        if not {"league", "season", "game_id", "_batch_id"} <= types.keys():
            raise RuntimeError("Missing schedule keys or batch identity")
        if any(types.get(name) != "varchar" for name in FORECAST_COLUMNS):
            raise RuntimeError("Expected legacy varchar forecasts")
        self._check_keys(TABLE)
        # Force CAST evaluation even in dry-run; bad values fail rather than NULL.
        projection = self._projection(columns)
        self._rows(f"SELECT {projection} FROM {TABLE}")
        ddl = self._copy_ddl()
        self._require_absent(COPY)
        self._require_absent(BACKUP)
        if mode == "dry-run":
            return ddl
        self._rows(ddl)
        names = ", ".join(_quote(name) for name, _ in columns)
        self._rows(f"INSERT INTO {COPY} ({names}) SELECT {projection} FROM {TABLE}")
        self._verify(TABLE, COPY, columns)
        self._rows(f"ALTER TABLE {TABLE} RENAME TO {BACKUP}")
        self._rows(f"ALTER TABLE {COPY} RENAME TO {TABLE}")

    def _require_absent(self, table):
        name = table.rsplit(".", 1)[1]
        rows = self._rows(
            "SELECT table_name FROM iceberg.information_schema.tables "
            f"WHERE table_schema = 'bronze' AND table_name = '{name}'"
        )
        if rows:
            raise RuntimeError(f"Retained migration table exists: {table}; inspect before retry")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("dry-run", "apply", "rollback"), default="dry-run")
    parser.add_argument("--publication-paused", action="store_true")
    args = parser.parse_args(argv)
    from scrapers.base.iceberg_writer import IcebergWriter

    result = ScheduleMigration(IcebergWriter()._get_trino_manager()).run(
        args.mode, publication_paused=args.publication_paused,
    )
    if result:
        print(result)


if __name__ == "__main__":
    main()
