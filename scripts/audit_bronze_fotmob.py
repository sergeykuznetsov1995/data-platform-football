"""
audit_bronze_fotmob.py — one-shot DQ audit of all 9 Bronze fotmob tables.

Run inside the Airflow container:

    docker compose exec -T airflow-webserver python /opt/airflow/scripts/audit_bronze_fotmob.py

Outputs Markdown to stdout. Redirect on host:

    docker compose exec -T airflow-webserver python /opt/airflow/scripts/audit_bronze_fotmob.py \
        > docs/fotmob_bronze_dq_audit_$(date +%F).md

Per table, reports: row count, freshness, per-column NULL %, fully-NULL columns,
PK-uniqueness check, (league, season) coverage, JSON sanity for the *_json columns.
"""
from __future__ import annotations

import datetime as dt
import sys
from typing import Dict, List, Tuple

sys.path.insert(0, '/opt/airflow/dags')
from utils.silver_tasks import _get_trino_connection  # noqa: E402


# Hypothesised PKs — empirically verified by this script.
PK_CANDIDATES: Dict[str, List[str]] = {
    'fotmob_schedule':         ['match_id', 'league', 'season'],
    'fotmob_team_stats':       ['team_id', 'league', 'season'],
    'fotmob_player_stats':     ['participant_id', 'stat_name', 'league', 'season'],
    'fotmob_team_leaderboards': ['team_id', 'stat_name', 'league', 'season'],
    'fotmob_transfers':        ['player_id', 'transfer_date', 'to_club_id', 'league', 'season'],
    'fotmob_team_profile':     ['team_id', 'league', 'season'],
    'fotmob_team_squad':       ['team_id', 'player_id', 'league', 'season'],
    'fotmob_match_details':    ['match_id', 'league', 'season'],
    'fotmob_player_details':   ['player_id', 'league', 'season'],
}

TABLES = list(PK_CANDIDATES.keys())


def fetch_columns(cur, table: str) -> List[Tuple[str, str]]:
    cur.execute(
        "SELECT column_name, data_type "
        "FROM iceberg.information_schema.columns "
        f"WHERE table_schema='bronze' AND table_name='{table}' "
        "ORDER BY ordinal_position"
    )
    return [(r[0], r[1]) for r in cur.fetchall()]


def null_profile(cur, table: str, cols: List[Tuple[str, str]]) -> Tuple[int, List[Tuple[str, str, int, float]]]:
    """Returns (total_rows, [(col, type, null_count, null_pct), ...] sorted by null_pct desc)."""
    if not cols:
        return 0, []
    select_parts = ['count(*) AS total'] + [f'count("{c}") AS nn_{i}' for i, (c, _) in enumerate(cols)]
    cur.execute(f"SELECT {', '.join(select_parts)} FROM iceberg.bronze.{table}")
    row = cur.fetchone()
    total = row[0]
    out = []
    for i, (c, t) in enumerate(cols):
        non_null = row[i + 1]
        null_count = total - non_null
        null_pct = (null_count / total) if total else 0.0
        out.append((c, t, null_count, null_pct))
    out.sort(key=lambda x: (-x[3], x[0]))
    return total, out


def pk_check(cur, table: str, pk: List[str]) -> Tuple[int, int, int]:
    """Returns (total_rows, distinct_keys, duplicate_groups). 0 dup_groups = clean."""
    pk_expr = ', '.join('"' + c + '"' for c in pk)
    where_clause = ' AND '.join('"' + c + '" IS NOT NULL' for c in pk)
    cur.execute(
        f"SELECT count(*), count(DISTINCT ({pk_expr})) "
        f"FROM iceberg.bronze.{table} WHERE {where_clause}"
    )
    total, distinct = cur.fetchone()
    cur.execute(
        f"SELECT count(*) FROM ("
        f"  SELECT {pk_expr} FROM iceberg.bronze.{table} WHERE {where_clause} "
        f"  GROUP BY {pk_expr} HAVING count(*) > 1"
        f") d"
    )
    dup_groups = cur.fetchone()[0]
    return total, distinct, dup_groups


def coverage(cur, table: str) -> List[Tuple[str, int, int]]:
    cur.execute(
        f"SELECT league, season, count(*) FROM iceberg.bronze.{table} "
        f"GROUP BY 1, 2 ORDER BY 1, 2"
    )
    return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def freshness(cur, table: str) -> Tuple[str, float]:
    cur.execute(
        f"SELECT CAST(max(_ingested_at) AS varchar), "
        f"       date_diff('second', max(_ingested_at), current_timestamp AT TIME ZONE 'UTC') "
        f"FROM iceberg.bronze.{table}"
    )
    max_ts, age_sec = cur.fetchone()
    if max_ts is None or age_sec is None:
        return 'NULL', float('inf')
    return max_ts, age_sec / 3600.0


def json_sanity(cur, table: str, cols: List[Tuple[str, str]]) -> List[Tuple[str, int, int]]:
    """For columns ending in _json: (col, empty_count, empty_pct_x100)."""
    json_cols = [c for c, _ in cols if c.endswith('_json')]
    if not json_cols:
        return []
    select_parts = ['count(*) AS total'] + [
        f"sum(CASE WHEN \"{c}\" IS NULL OR \"{c}\" = '' OR \"{c}\" = '{{}}' OR \"{c}\" = 'null' THEN 1 ELSE 0 END) AS empty_{i}"
        for i, c in enumerate(json_cols)
    ]
    cur.execute(f"SELECT {', '.join(select_parts)} FROM iceberg.bronze.{table}")
    row = cur.fetchone()
    total = row[0]
    return [
        (json_cols[i], row[i + 1] or 0, round(((row[i + 1] or 0) / total) * 100, 1) if total else 0.0)
        for i in range(len(json_cols))
    ]


def fmt_pct(p: float) -> str:
    return f"{p * 100:5.1f}%"


def emit(table: str, cur) -> None:
    cols = fetch_columns(cur, table)
    total, null_rows = null_profile(cur, table, cols)
    pk = PK_CANDIDATES[table]
    pk_total, pk_distinct, pk_dup_groups = pk_check(cur, table, pk)
    cov = coverage(cur, table)
    max_ts, age_h = freshness(cur, table)
    json_rows = json_sanity(cur, table, cols)

    print(f"## `iceberg.bronze.{table}`")
    print()
    print(f"- **Rows:** `{total}`  |  **Columns:** `{len(cols)}`")
    print(f"- **Freshness:** `_ingested_at` max = `{max_ts}` (age `{age_h:.1f}h`)")
    print(f"- **PK hypothesis:** `{tuple(pk)}` → total non-null `{pk_total}`, "
          f"distinct `{pk_distinct}`, **dup groups `{pk_dup_groups}`** "
          f"{'✅' if pk_dup_groups == 0 else '⚠'}")
    print()

    fully = [(c, t) for (c, t, n, p) in null_rows if p == 1.0]
    if fully:
        print(f"### Fully-NULL columns ({len(fully)})")
        for c, t in fully:
            print(f"- `{c}` ({t})")
        print()

    # Top NULL columns (exclude 0% NULL noise)
    noisy = [(c, t, n, p) for (c, t, n, p) in null_rows if p > 0]
    if noisy:
        print(f"### Per-column NULL rate (columns with NULLs, top {min(15, len(noisy))})")
        print()
        print("| Column | Type | NULL count | NULL % |")
        print("|---|---|---:|---:|")
        for c, t, n, p in noisy[:15]:
            flag = ' 🔴' if p == 1.0 else (' ⚠' if p >= 0.95 else '')
            print(f"| `{c}`{flag} | {t} | {n} | {fmt_pct(p)} |")
        print()

    print(f"### Coverage `(league, season)`")
    print()
    print("| league | season | rows |")
    print("|---|---:|---:|")
    for lg, sn, n in cov:
        print(f"| `{lg}` | {sn} | {n} |")
    print()

    if json_rows:
        print(f"### JSON sanity (`_json` columns)")
        print()
        print("| Column | Empty/NULL/`'{}'`/`'null'` | % |")
        print("|---|---:|---:|")
        for c, n, p in json_rows:
            flag = ' 🔴' if p >= 95 else (' ⚠' if p >= 50 else '')
            print(f"| `{c}`{flag} | {n} | {p:.1f}% |")
        print()

    print('---')
    print()


def main() -> int:
    now = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    print(f"# Bronze fotmob — DQ audit ({now})")
    print()
    print("Source: `scripts/audit_bronze_fotmob.py`. Run inside `airflow-webserver`. "
          "Connects via `dags.utils.silver_tasks._get_trino_connection`.")
    print()
    print(f"**Tables scanned:** {len(TABLES)}  "
          f"(`{', '.join(TABLES)}`)")
    print()
    print("Legend: 🔴 = column ≥95% NULL/empty (likely systematic gap), "
          "⚠ = column 50–95% empty or PK has duplicates.")
    print()

    conn = _get_trino_connection()
    cur = conn.cursor()
    try:
        for t in TABLES:
            try:
                emit(t, cur)
            except Exception as exc:
                print(f"## `iceberg.bronze.{t}` — **AUDIT ERROR**")
                print()
                print(f"```\n{exc}\n```")
                print()
                print('---')
                print()
    finally:
        cur.close()
        conn.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
