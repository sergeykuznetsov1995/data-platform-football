"""Gold schema-drift audit (#765).

Finds gold tables whose LIVE Iceberg schema has drifted from the schema their
SQL would produce — the failure mode behind #765, where `gold.dim_venue` stayed
on a stale 7-column schema after #719/#750 grew the SQL to 11 columns.

For every ``dags/sql/gold/*.sql[.j2]`` (excluding ``*_empty.sql`` fallbacks):

  expected schema  = the columns Trino reports for the runner's CTAS envelope
                     ``SELECT *, CURRENT_TIMESTAMP AS _silver_created_at
                       FROM ( <rendered SELECT> ) LIMIT 0``
                     (read off ``cursor.description`` — no row scan, no parsing).
  actual schema    = ``DESCRIBE iceberg.gold.<table>``.
  drift            = a column present in one set but not the other.

The SQL is rendered with the SAME dispatch ``run_gold_transform`` uses
(``utils.dim_loaders.render_<dim>_sql`` for config-driven dims;
``utils.gold_tasks._render_source_priority`` for source-priority fact ``.sql.j2``;
plain read for pure ``.sql``), so the expected columns are byte-identical to what
the production runner emits — including the appended ``_silver_created_at``.

Runs ONLY inside the airflow container (needs Trino, like
``audit_silver_charter.py --schema``). Exits 1 if any table has drift.

Usage:
    python scripts/audit_gold_drift.py                 # all gold tables
    python scripts/audit_gold_drift.py --table dim_venue
    python scripts/audit_gold_drift.py --types         # also flag type mismatches (WARN)
"""
from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

GOLD_SQL_DIR = Path(__file__).resolve().parent.parent / 'dags' / 'sql' / 'gold'

# Fallback files (`*_empty.sql`) are typed 0-row contracts for an EXISTING gold
# table when its Silver source is absent — not standalone tables. Excluded from
# the audit (same rationale as audit_silver_charter.py #369).
FALLBACK_SUFFIX = '_empty'

# Config-driven dims render via dedicated renderers in utils.dim_loaders — mirrors
# the _RENDERERS registry in dags/dag_transform_fbref_gold.py. Every OTHER .sql.j2
# is a source-priority fact template rendered via gold_tasks._render_source_priority.
INLINE_DIM_RENDERERS = {
    'dim_competition': 'render_dim_competition_sql',
    'dim_season':      'render_dim_season_sql',
    'dim_venue':       'render_dim_venue_sql',
    'dim_team':        'render_dim_team_sql',
    'dim_player':      'render_dim_player_sql',
    'dim_match':       'render_dim_match_sql',
}


def _stem(path: Path) -> str:
    return path.name.replace('.sql.j2', '').replace('.sql', '')


def gold_sql_files() -> list[Path]:
    """Gold SELECT files to audit — fallback ``*_empty.sql`` excluded."""
    files = sorted(GOLD_SQL_DIR.glob('*.sql')) + sorted(GOLD_SQL_DIR.glob('*.sql.j2'))
    return [f for f in files if not _stem(f).endswith(FALLBACK_SUFFIX)]


def render_gold_sql(path: Path) -> str:
    """Return the rendered SELECT for one gold file, mirroring run_gold_transform."""
    table = _stem(path)
    if not path.name.endswith('.sql.j2'):
        return path.read_text(encoding='utf-8')

    if table in INLINE_DIM_RENDERERS:
        from utils import dim_loaders
        render_fn = getattr(dim_loaders, INLINE_DIM_RENDERERS[table])
        with tempfile.NamedTemporaryFile(
            mode='w', suffix=f'_{table}.sql', delete=False, encoding='utf-8',
        ) as fh:
            out_path = fh.name
        render_fn(str(path), out_path)
        return Path(out_path).read_text(encoding='utf-8')

    # Source-priority fact template (#437).
    from utils.gold_tasks import _render_source_priority
    return Path(_render_source_priority(str(path), table)).read_text(encoding='utf-8')


def expected_schema(cur, path: Path) -> dict[str, str]:
    """Columns the runner's CTAS would emit, via a zero-row probe of its envelope.

    Reads them off ``cursor.description`` (Trino plans + returns the result
    schema for ``LIMIT 0`` without scanning rows). Faithful to add_timestamp=True
    (the gold default); if a template already projects ``_silver_created_at`` the
    appended copy would raise DUPLICATE_COLUMN_NAME, so we retry without the wrap.
    """
    select_sql = render_gold_sql(path).strip()
    if select_sql.endswith(';'):
        select_sql = select_sql[:-1].rstrip()

    for append_ts in (True, False):
        if append_ts:
            probe = (
                "SELECT *, CURRENT_TIMESTAMP AS _silver_created_at\n"
                f"FROM (\n{select_sql}\n) LIMIT 0"
            )
        else:
            probe = f"SELECT * FROM (\n{select_sql}\n) LIMIT 0"
        try:
            cur.execute(probe)
            cur.fetchall()  # consume (empty) result set — project rule
            return {d[0]: d[1] for d in cur.description}
        except Exception as e:
            if append_ts and 'DUPLICATE_COLUMN' in str(e).upper():
                continue
            raise


def actual_schema(cur, table: str) -> dict[str, str] | None:
    """Live gold schema via DESCRIBE, or None if the table does not exist."""
    try:
        cur.execute(f'DESCRIBE iceberg.gold.{table}')
        rows = cur.fetchall()
    except Exception as e:
        msg = str(e)
        if 'TABLE_NOT_FOUND' in msg or 'does not exist' in msg:
            return None
        raise
    return {r[0]: r[1] for r in rows}


def _base_type(t: str) -> str:
    """Strip parameters so 'varchar(10)' == 'varchar', 'decimal(5,2)' == 'decimal'."""
    return t.split('(', 1)[0].strip().lower()


def diff(expected: dict[str, str], actual: dict[str, str], check_types: bool) -> list[tuple]:
    """Findings: (severity, kind, detail). ERROR = column drift; WARN = type mismatch."""
    findings: list[tuple] = []
    exp, act = set(expected), set(actual)
    for c in sorted(exp - act):
        findings.append(('ERROR', 'MISSING', f'{c} ({expected[c]}) — in SQL, absent in live table'))
    for c in sorted(act - exp):
        findings.append(('ERROR', 'EXTRA', f'{c} ({actual[c]}) — in live table, absent in SQL'))
    if check_types:
        for c in sorted(exp & act):
            if _base_type(expected[c]) != _base_type(actual[c]):
                findings.append(('WARN', 'TYPE', f'{c}: live {actual[c]} vs SQL {expected[c]}'))
    return findings


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--table', default=None, help='audit only this gold table stem')
    p.add_argument('--types', action='store_true', help='also flag type mismatches (WARN)')
    args = p.parse_args()

    sys.path.insert(0, '/opt/airflow/dags')
    from utils.silver_tasks import _get_trino_connection  # lazy: needs container

    files = gold_sql_files()
    if args.table:
        files = [f for f in files if _stem(f) == args.table]
        if not files:
            sys.exit(f'no gold SQL file for table {args.table!r}')

    conn = _get_trino_connection()
    cur = conn.cursor()

    results: list[tuple[str, str, list[tuple]]] = []  # (table, verdict, findings)
    drift_tables: list[str] = []
    for f in sorted(files, key=_stem):
        table = _stem(f)
        try:
            expected = expected_schema(cur, f)
        except Exception as e:
            # Could not derive expected schema (e.g. an upstream silver table is
            # absent). Report, don't crash — the audit covers what it can compare.
            results.append((table, 'SKIP', [('WARN', 'NO_EXPECTED',
                                             f'{type(e).__name__}: {str(e)[:160]}')]))
            continue
        actual = actual_schema(cur, table)
        if actual is None:
            results.append((table, 'NO_TABLE', [('ERROR', 'NO_TABLE',
                                                 'live gold table does not exist')]))
            drift_tables.append(table)
            continue
        findings = diff(expected, actual, args.types)
        verdict = 'DRIFT' if any(sev == 'ERROR' for sev, *_ in findings) else 'OK'
        if verdict == 'DRIFT':
            drift_tables.append(table)
        results.append((table, verdict, findings))

    _print_report(results, drift_tables)
    sys.exit(1 if drift_tables else 0)


def _print_report(results: list[tuple[str, str, list[tuple]]], drift_tables: list[str]) -> None:
    icon = {'OK': 'OK   ', 'DRIFT': 'DRIFT', 'NO_TABLE': 'GONE ', 'SKIP': 'SKIP '}
    for table, verdict, findings in results:
        print(f'{icon.get(verdict, verdict):5} {table}', file=sys.stderr)
        for sev, kind, detail in findings:
            print(f'        [{sev}] {kind}: {detail}', file=sys.stderr)
    n = len(results)
    ok = sum(1 for _, v, _ in results if v == 'OK')
    print(f'\nGold tables scanned: {n} · OK: {ok} · drifted: {len(drift_tables)}',
          file=sys.stderr)
    if drift_tables:
        print('Drifted tables (re-materialize via the gold runner — #741 auto-heals):',
              file=sys.stderr)
        for t in sorted(drift_tables):
            print(f'  - {t}', file=sys.stderr)


if __name__ == '__main__':
    main()
