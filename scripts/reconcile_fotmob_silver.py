"""Prepare read-only #2021 count SQL; never connects to Trino.

First print discovery SQL. Feed its complete results as --scopes JSON to
materialize one query per tournament/season and bounded entity bucket. Run the
queries only through the admitted night runner with its 2GB limit. Equal counts
are evidence, not proof of key/category correctness or source completeness.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'dags'))
from dags.utils import fotmob_silver_build as build  # noqa: E402

SCOPED = build.MATCH_TABLES + build.SEASON_CONTEXT_TABLES + build.SEASON_GLOBAL_TABLES
SEASONED = build.MATCH_TABLES + build.SEASON_CONTEXT_TABLES + ('fotmob_competition_seasons',)


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _validate(schema: str, slot: str) -> None:
    if not re.fullmatch(r'[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*', schema) or slot not in ('a', 'b'):
        raise ValueError('expected catalog.schema and slot a or b')


def discovery(schema: str, slot: str) -> dict[str, str]:
    """Union Bronze and slot keys, retaining removed Silver partitions for comparison."""
    _validate(schema, slot)
    result = {}
    for table in SCOPED:
        view = build.CALENDAR_VIEW if table in build.MATCH_TABLES else build.SIGNAL_SOURCES[table][0]
        season = 'source_season_key' if table in SEASONED else "''"
        result[table] = (
            f'SELECT DISTINCT TRY_CAST(competition_id AS BIGINT) AS competition_id, {season} AS source_season_key '
            f'FROM iceberg.bronze.{view} WHERE TRY_CAST(competition_id AS BIGINT) IS NOT NULL'
            + (" AND source_season_key IS NOT NULL AND source_season_key <> ''" if table in SEASONED else '')
            + f'\nUNION\nSELECT DISTINCT competition_id, {season} FROM {schema}.{table}_{slot}'
        )
    return result


def materialize(schema: str, slot: str, scopes: dict, sql_dir: Path = ROOT / 'dags/sql/silver') -> list[dict]:
    """scopes is the complete {table: [[competition_id, season], ...]} discovery result."""
    _validate(schema, slot)
    if set(scopes) != set(SCOPED):
        raise ValueError('discovery must include every scoped table, including empty results')
    units = []
    for table in build.ALL_TABLES:
        items = scopes.get(table)
        if items is None:
            items = [(None, bucket) for bucket in range(build.DEFAULT_BUCKETS.get(table, 1))]
        seen = set()
        for comp, key in items:
            if table in SCOPED:
                if type(comp) is not int or not isinstance(key, str) or (table in SEASONED and not key):
                    raise ValueError('invalid discovered key')
                if table not in SEASONED and key != '':
                    raise ValueError('competition-only table must have an empty season')
                comp_scope = f'competition_id = {comp}'
                output_scope = comp_scope + (f' AND source_season_key = {_literal(key)}' if table in SEASONED else '')
                # Keep the whole tournament calendar and payloads: the selected
                # child season can depend on a card stored under its parent.
                sql = build.render_scoped_sql(table, sql_dir, calendar_scope=comp_scope,
                    payload_scope=f'competition_id = {_literal(str(comp))}',
                    season_scope=comp_scope if table in build.SEASON_GLOBAL_TABLES else output_scope)
            else:
                buckets = build.DEFAULT_BUCKETS.get(table, 1)
                output_scope = f'mod({build.BUCKET_KEY[table]}, {buckets}) = {key}' if buckets > 1 else 'TRUE'
                sql = build.render_scoped_sql(table, sql_dir, entity_scope=output_scope)
            identity = json.dumps([table, comp, key], ensure_ascii=False)
            if identity in seen:
                raise ValueError('duplicate discovered key')
            seen.add(identity)
            units.append({'id': identity, 'table': table, 'sql': (
                'SELECT\n  (SELECT count(*) FROM (\n' + sql + f'\n) expected WHERE {output_scope}) AS expected_count,\n'
                f'  (SELECT count(*) FROM {schema}.{table}_{slot} WHERE {output_scope}) AS actual_count'
            )})
    return units


def classify(expected_ids: list[str], results: dict, before: dict, after: dict) -> str:
    """Fail closed on missing/changed guards; caller captures source+reader+build state.

    Guard equality is a drift detector, not an immutable source snapshot. Source
    signals must include every dependency; slot writes must be quiescent.
    """
    required = {'sources', 'reader_state', 'build_state'}
    if (set(before) != required or set(after) != required
            or any(not before[k] or not after[k] for k in required) or before != after):
        return 'inconclusive_drift_or_missing_guards'
    if len(expected_ids) != len(set(expected_ids)) or set(results) != set(expected_ids) or not expected_ids:
        return 'incomplete'
    for counts in results.values():
        if not isinstance(counts, (list, tuple)) or len(counts) != 2 or any(type(n) is not int or n < 0 for n in counts):
            return 'incomplete'
    return 'equal_counts' if all(a == b for a, b in results.values()) else 'differences_require_explanation'


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--schema', default='iceberg.silver')
    parser.add_argument('--slot', choices=('a', 'b'), required=True)
    parser.add_argument('--scopes', type=Path, help='complete JSON discovery results; omit to print discovery SQL')
    args = parser.parse_args(argv)
    plan = {'schema': args.schema, 'slot': args.slot, 'tables': list(build.ALL_TABLES),
            'execution': 'disabled; SQL preparation only',
            'required_guards': ['sources', 'reader_state', 'build_state']}
    if args.scopes is None:
        plan['discovery'] = discovery(args.schema, args.slot)
    else:
        plan['units'] = materialize(args.schema, args.slot, json.loads(args.scopes.read_text()))
    print(json.dumps(plan, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
