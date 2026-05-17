"""Bronze column-quality audit: ALL_NULL / CONSTANT / HIGH_NULL / ALL_EMPTY_STR.

Scans every column in every `iceberg.bronze.*` table and flags:
  - ERROR  ALL_NULL       — non_null = 0 (not in EXPECTED_NULL allowlist)
  - ERROR  ALL_EMPTY_STR  — varchar col where every non-NULL value is ''
  - WARN   HIGH_NULL      — null_rate > 0.95 (not allowlist, not 1.0)
  - WARN   CONSTANT       — distinct = 1 (not a partition column)
  - INFO   EXPECTED_NULL  — col matches feedback_bronze_expected_null_columns.md

Usage (inside airflow-webserver container):
    python /opt/airflow/scripts/audit_bronze_columns.py --output /tmp/audit.md

Uses `import trino` directly to skip the heavy scrapers/__init__.py
(nodriver + selenium ~1.5GB RAM).
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, '/opt/airflow/dags')
from utils.silver_tasks import _get_trino_connection

# ---- Allowlist (verbatim from feedback_bronze_expected_null_columns.md) ----

EXPECTED_NULL: dict[str, set[str]] = {
    'fbref_keeper_keeper_adv': {
        # 23 "advanced keeper" cols restricted by FBref Feb 2026
        'goals_fk', 'goals_ck', 'goals_og', 'psxg', 'psxg/sot', 'psxg+/-', '/90',
        'cmp', 'att', 'cmp%', 'att (gk)', 'thr', 'launch%', 'avglen',
        'opp', 'stp', 'stp%', '#opa', '#opa/90', 'avgdist',
        'pkatt', 'pka', 'pksv',
        # Merged-header duplicate columns (FBref renders the same metric twice
        # under different table headers, parser keeps both — `_1` suffix is the
        # second occurrence and is always empty because FBref only fills the first).
        'att_1', 'avglen_1', 'launch%_1',
    },
    'fbref_team_misc': {'pkwon', 'pkcon'},
    'fbref_player_misc': {'pkwon', 'pkcon'},
    'fbref_match_player_stats': {'pkwon', 'pkcon'},
    'matchhistory_games': {
        # 31 dropped bookmakers (Betfair, William Hill, 1xBet, VC, IW, SJ, SO, LB, GB, SB, BS)
        'bfh', 'bfd', 'bfa', 'whh', 'whd', 'wha', '1xbh', '1xbd', '1xba',
        'vch', 'vcd', 'vca', 'iwh', 'iwd', 'iwa', 'sjh', 'sjd', 'sja',
        'soh', 'sod', 'soa', 'lbh', 'lbd', 'lba', 'gbh', 'gbd', 'gba',
        'sbh', 'sbd', 'sba', 'bsh',  # bsd/bsa removed at some point — verify
    },
    'espn_matchsheet': {'capacity'},
    'sofifa_team_ratings': {
        # sofifa.com removed these data-col cells from team page (FC 26, verified 2026-05-14).
        # soccerdata still requests them via showCol[]=... but sofifa silently ignores.
        'build_up_speed', 'build_up_dribbling', 'build_up_passing', 'build_up_positioning',
        'chance_creation_crossing', 'chance_creation_passing',
        'chance_creation_shooting', 'chance_creation_positioning',
        'defence_aggression', 'defence_pressure', 'defence_team_width',
        'defence_defender_line', 'defence_domestic_prestige',
        'international_prestige',
        'whole_team_average_age',  # renamed to starting_xi_average_age upstream
    },
    'whoscored_schedule': {
        'aggregate_winner_field', 'extra_result_field',
        'home_extratime_score', 'away_extratime_score',
        'home_penalty_score', 'away_penalty_score',
        'stage',
    },
    'whoscored_season_stages': {'stage'},
    'whoscored_events': {
        # event-conditional cols (≥97% NULL by design)
        'goal_mouth_y', 'goal_mouth_z', 'blocked_x', 'blocked_y',
        'is_shot', 'is_goal', 'card_type',
        'related_event_id', 'related_player_id',
        'end_x', 'end_y',
    },
}

# Columns that hold exactly 1 distinct value by design — should NOT trigger
# CONSTANT WARN. Most cases: each FBref stat-table contains exactly one stat_type
# (DAG materialises one table per stat_type), and helper columns like `matches`
# (link text), `pos` (always 'GK' in keeper tables) are constants for the same
# reason.
EXPECTED_CONSTANT: dict[str, set[str]] = {
    'fbref_player_stats': {'stat_type', 'matches'},
    'fbref_player_shooting': {'stat_type', 'matches'},
    'fbref_player_playingtime': {'stat_type', 'matches'},
    'fbref_player_misc': {'stat_type', 'matches'},
    'fbref_team_stats': {'stat_type'},
    'fbref_team_shooting': {'stat_type'},
    # min% and mn/mp are denormalised aggregates FBref renders as a column header
    # but populates with the team season constant (1 distinct value across all rows).
    'fbref_team_playingtime': {'stat_type', 'min%', 'mn/mp'},
    'fbref_team_misc': {'stat_type'},
    'fbref_keeper_keeper': {'stat_type', 'pos', 'matches'},
    'fbref_keeper_keeper_adv': {'stat_type', 'pos', 'matches'},
    # notes is sparse-by-design (HIGH_NULL) but the few populated rows happen to
    # share the same text ("Award Decided on Pens"), triggering CONSTANT too.
    'fbref_schedule': {'notes'},
}

# Internal metadata cols — we skip them from CONSTANT-checks (they are constant
# by design — _source='fbref' for all rows), but still NULL-check them.
META_COLS = {'_source', '_entity_type', '_ingested_at', '_batch_id'}

# Source prefix → group label for the report
SOURCE_GROUPS = [
    ('fbref_', 'FBref'),
    ('fotmob_', 'FotMob'),
    ('sofascore_', 'Sofascore'),
    ('sofifa_', 'SoFIFA'),
    ('understat_', 'Understat'),
    ('whoscored_', 'WhoScored'),
    ('espn_', 'ESPN'),
    ('clubelo_', 'ClubElo'),
    ('matchhistory_', 'MatchHistory'),
]


def source_of(table: str) -> str:
    for prefix, label in SOURCE_GROUPS:
        if table.startswith(prefix):
            return label
    return 'Other'


# ---- Helpers ----

_PARTITIONING_RE = re.compile(r"partitioning\s*=\s*ARRAY\[(.*?)\]", re.IGNORECASE | re.DOTALL)


def get_partition_cols(cur, table: str) -> set[str]:
    cur.execute(f"SHOW CREATE TABLE iceberg.bronze.{table}")
    ddl = cur.fetchall()[0][0]
    m = _PARTITIONING_RE.search(ddl)
    if not m:
        return set()
    return {x.strip().strip("'\"") for x in m.group(1).split(',') if x.strip()}


def describe(cur, table: str) -> list[tuple[str, str]]:
    cur.execute(f"DESCRIBE iceberg.bronze.{table}")
    return [(r[0], r[1]) for r in cur.fetchall()]


def safe_alias(col: str, idx: int) -> str:
    """Produce a SQL-safe alias for arbitrary column name."""
    base = re.sub(r'[^a-zA-Z0-9_]', '_', col)
    if not base or not base[0].isalpha() and base[0] != '_':
        base = f"c{idx}_{base}"
    return base


def is_varchar(typ: str) -> bool:
    return typ.startswith('varchar')


def is_skip_distinct(typ: str) -> bool:
    """Skip distinct count on types where it's expensive or unsupported."""
    return typ.startswith('timestamp') or typ.startswith('row(') or typ.startswith('array(') or typ.startswith('map(')


# ---- Audit ----

def audit_table(cur, table: str) -> tuple[int, list[dict]]:
    """Return (total_rows, findings) for one table."""
    cols = describe(cur, table)
    if not cols:
        return 0, []
    try:
        partition_cols = get_partition_cols(cur, table)
    except Exception as e:
        print(f"  ! get_partition_cols({table}) failed: {e}", file=sys.stderr)
        partition_cols = set()

    # Build big SELECT
    select_parts = ['count(*) AS "_total"']
    plan: list[tuple[str, str, str, bool]] = []  # (col, type, alias, do_es)
    for idx, (col, typ) in enumerate(cols):
        alias = safe_alias(col, idx)
        # non-null count (always)
        select_parts.append(f'count("{col}") AS "{alias}__nn"')
        # distinct count (skip expensive types)
        if not is_skip_distinct(typ):
            select_parts.append(f'count(distinct "{col}") AS "{alias}__d"')
        # empty-string count for varchar
        do_es = is_varchar(typ)
        if do_es:
            select_parts.append(
                f'sum(CASE WHEN "{col}" = \'\' THEN 1 ELSE 0 END) AS "{alias}__es"'
            )
        plan.append((col, typ, alias, do_es))

    sql = f"SELECT {', '.join(select_parts)} FROM iceberg.bronze.{table}"
    cur.execute(sql)
    desc = [d[0] for d in cur.description]
    row = cur.fetchall()[0]
    res = dict(zip(desc, row))

    total = int(res.get('_total', 0) or 0)
    if total == 0:
        return 0, [{'table': table, 'col': '*', 'sev': 'INFO', 'detail': 'table is empty'}]

    findings: list[dict] = []
    allow_for_table = EXPECTED_NULL.get(table, set())
    allow_constant = EXPECTED_CONSTANT.get(table, set())

    for col, typ, alias, do_es in plan:
        nn = int(res.get(f'{alias}__nn', 0) or 0)
        nulls = total - nn
        null_rate = nulls / total if total else 0.0
        in_allowlist = col in allow_for_table
        is_meta = col in META_COLS

        # NULL classification
        if null_rate == 1.0:
            if in_allowlist:
                findings.append({
                    'table': table, 'col': col, 'sev': 'INFO',
                    'detail': f'EXPECTED_NULL (100% NULL by allowlist, {typ})',
                })
            else:
                findings.append({
                    'table': table, 'col': col, 'sev': 'ERROR',
                    'detail': f'ALL_NULL — 0 of {total} non-NULL ({typ})',
                })
        elif null_rate > 0.95 and not in_allowlist:
            findings.append({
                'table': table, 'col': col, 'sev': 'WARN',
                'detail': f'HIGH_NULL — null_rate={null_rate:.1%} ({nn}/{total} non-NULL, {typ})',
            })

        # Empty-string classification (varchar only)
        if do_es and nn > 0:
            es = int(res.get(f'{alias}__es', 0) or 0)
            if es == nn:
                findings.append({
                    'table': table, 'col': col, 'sev': 'ERROR',
                    'detail': f"ALL_EMPTY_STR — all {nn} non-NULL values are '' ({typ})",
                })

        # Constant classification (skip meta cols + partition cols + small tables)
        if total >= 10 and not is_meta and col not in partition_cols and not is_skip_distinct(typ):
            d = res.get(f'{alias}__d')
            if d is not None:
                d = int(d)
                if d == 1 and nn > 0:
                    if col in allow_constant:
                        findings.append({
                            'table': table, 'col': col, 'sev': 'INFO',
                            'detail': f'EXPECTED_CONSTANT (1 distinct by design across {nn} non-NULL rows, {typ})',
                        })
                    else:
                        findings.append({
                            'table': table, 'col': col, 'sev': 'WARN',
                            'detail': f'CONSTANT — only 1 distinct value across {nn} non-NULL rows ({typ})',
                        })

    return total, findings


# ---- Report ----

SEV_ORDER = {'ERROR': 0, 'WARN': 1, 'INFO': 2}


def render_report(per_table: dict[str, tuple[int, list[dict]]], output: Path) -> None:
    total_tables = len(per_table)
    total_cols_scanned = sum(len(set(f['col'] for f in fs)) for _, fs in per_table.values())
    err_findings = [f for _, fs in per_table.values() for f in fs if f['sev'] == 'ERROR']
    warn_findings = [f for _, fs in per_table.values() for f in fs if f['sev'] == 'WARN']
    info_findings = [f for _, fs in per_table.values() for f in fs if f['sev'] == 'INFO']
    err_tables = len({f['table'] for f in err_findings})
    warn_tables = len({f['table'] for f in warn_findings})

    today = datetime.utcnow().strftime('%Y-%m-%d')
    lines: list[str] = [
        f"# Bronze column quality audit — {today}",
        "",
        "Сканирование всех `iceberg.bronze.*` столбцов на predefined-классы мусора:",
        "**ALL_NULL** (100% NULL не в allowlist), **ALL_EMPTY_STR** (varchar где все non-NULL = `''`),",
        "**HIGH_NULL** (null_rate > 95% не в allowlist), **CONSTANT** (1 distinct value на не-partition колонке).",
        "",
        "## Summary",
        "",
        f"- Tables scanned: **{total_tables}**",
        f"- ERROR findings: **{len(err_findings)}** in **{err_tables}** table(s)",
        f"- WARN findings: **{len(warn_findings)}** in **{warn_tables}** table(s)",
        f"- INFO (allowlist hits): **{len(info_findings)}**",
        "",
    ]

    # Per-source findings (ERROR + WARN)
    lines.append("## Findings by source (ERROR + WARN)")
    lines.append("")
    by_source: dict[str, list[dict]] = defaultdict(list)
    for table, (_, fs) in per_table.items():
        for f in fs:
            if f['sev'] in ('ERROR', 'WARN'):
                by_source[source_of(table)].append(f)

    if not by_source:
        lines.append("✅ Чистый bronze — никаких ERROR/WARN.")
        lines.append("")
    else:
        for source in sorted(by_source.keys()):
            findings = sorted(by_source[source], key=lambda f: (SEV_ORDER[f['sev']], f['table'], f['col']))
            lines.append(f"### {source} ({len(findings)})")
            lines.append("")
            lines.append("| Table | Column | Severity | Detail |")
            lines.append("|---|---|---|---|")
            for f in findings:
                lines.append(f"| `{f['table']}` | `{f['col']}` | **{f['sev']}** | {f['detail']} |")
            lines.append("")

    # Per-table summary (totals + finding counts)
    lines.append("## Per-table summary")
    lines.append("")
    lines.append("| Table | Rows | ERROR | WARN | INFO |")
    lines.append("|---|---:|---:|---:|---:|")
    for table in sorted(per_table.keys()):
        total, fs = per_table[table]
        e = sum(1 for f in fs if f['sev'] == 'ERROR')
        w = sum(1 for f in fs if f['sev'] == 'WARN')
        i = sum(1 for f in fs if f['sev'] == 'INFO')
        lines.append(f"| `{table}` | {total} | {e} | {w} | {i} |")
    lines.append("")

    # Allowlist hits section
    lines.append("## Allowlist hits (expected 100% NULL)")
    lines.append("")
    if not info_findings:
        lines.append("Нет allowlist-hit'ов (значит ни одна expected-NULL колонка реально не 100% NULL — проверить!).")
        lines.append("")
    else:
        lines.append("| Table | Column | Note |")
        lines.append("|---|---|---|")
        for f in sorted(info_findings, key=lambda f: (f['table'], f['col'])):
            if f['col'] == '*':
                continue
            lines.append(f"| `{f['table']}` | `{f['col']}` | {f['detail']} |")
        lines.append("")

    # Tables that audit could not scan (DESCRIBE failed, etc.)
    lines.append("## Empty tables")
    lines.append("")
    empties = [t for t, (n, _) in per_table.items() if n == 0]
    if empties:
        for t in empties:
            lines.append(f"- `{t}`")
        lines.append("")
    else:
        lines.append("(none)")
        lines.append("")

    output.write_text('\n'.join(lines), encoding='utf-8')


# ---- Main ----

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output', default=f'/tmp/bronze_column_audit_{datetime.utcnow():%Y-%m-%d}.md')
    args = p.parse_args()

    conn = _get_trino_connection()
    cur = conn.cursor()
    cur.execute('SHOW TABLES FROM iceberg.bronze')
    tables = sorted(r[0] for r in cur.fetchall())
    print(f"Scanning {len(tables)} bronze tables...", file=sys.stderr)

    per_table: dict[str, tuple[int, list[dict]]] = {}
    for t in tables:
        try:
            total, findings = audit_table(cur, t)
            print(f"  {t}: rows={total} findings={len(findings)}", file=sys.stderr)
            per_table[t] = (total, findings)
        except Exception as e:
            print(f"  ! {t}: SCAN FAILED — {type(e).__name__}: {e}", file=sys.stderr)
            per_table[t] = (-1, [{
                'table': t, 'col': '*', 'sev': 'ERROR',
                'detail': f'audit script failed: {type(e).__name__}: {e}',
            }])

    output = Path(args.output)
    render_report(per_table, output)
    print(f"\nReport written to: {output}", file=sys.stderr)


if __name__ == '__main__':
    main()
