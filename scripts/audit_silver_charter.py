"""Silver Charter compliance audit.

Checks every `dags/sql/silver/*.sql[.j2]` against the Silver Layer Charter
(docs/decisions/silver-charter.md) and classifies each table.

Two layers:
  Layer A (static, NO Trino) — regex over the SQL files. Runs on the host.
    R1 ROLLUP          ERROR  — GROUP BY + aggregate on a *_season table (grain ↑).
    R2 SILVER_ON_SILVER WARN  — reads iceberg.silver.<non-xref> (cross-table dependency).
    R3 XREF_PREDICATE  ERROR  — xref_* JOIN missing (league AND season).
    R4 CROSS_SOURCE    WARN   — ≥2 distinct source prefixes referenced (xref excepted).
    R5 NAMING          WARN   — file name not {source}_{entity}_{grain}.
    R6 NO_DDL          ERROR  — file contains CREATE TABLE / INSERT.
  Layer B (schema, Trino DESCRIBE; only with --schema) — runs inside the airflow container.
    S1 presence of _silver_created_at / league / season.
    S2 season is varchar (not bigint).
    S3 bare *_id naming drift (no _raw / _canonical suffix).

Verdicts come from findings + the SANCTIONED registry (kept in sync with charter §7):
    COMPLIANT  — no findings.
    REVIEW     — only WARN findings, not in registry.
    EXCEPTION  — rule-violating but sanctioned (feeds a live Gold block).
    VIOLATOR   — sanctioned for Gold migration (issue filed).
    INVESTIGATE— flagged for manual follow-up.

Usage:
    python scripts/audit_silver_charter.py                       # Layer A, all files
    python scripts/audit_silver_charter.py --table sofascore_team_match
    python scripts/audit_silver_charter.py --output /tmp/silver_audit.md
    python scripts/audit_silver_charter.py --schema              # + Layer B (needs Trino)
    python scripts/audit_silver_charter.py --check               # gate: exit 1 on unsanctioned ERROR
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

SILVER_SQL_DIR = Path(__file__).resolve().parent.parent / 'dags' / 'sql' / 'silver'

# Fallback files (`*_empty.sql`) materialize an *existing* Silver table with a
# typed 0-row spine when its Bronze source is absent (e.g. SoFIFA Cloudflare
# freeze — issue #180). They are not standalone tables, so they are excluded
# from the per-table audit (else they show up as phantom "dead stub" tables —
# the false positive that prompted issue #369).
FALLBACK_SUFFIX = '_empty'


def silver_sql_files() -> list[Path]:
    """Silver SELECT files to audit — fallback `*_empty.sql` excluded (#369)."""
    files = sorted(SILVER_SQL_DIR.glob('*.sql')) + sorted(SILVER_SQL_DIR.glob('*.sql.j2'))
    return [f for f in files
            if not f.name.replace('.sql.j2', '').replace('.sql', '').endswith(FALLBACK_SUFFIX)]

# Source prefixes (order matters — longest unambiguous first is not needed, all distinct).
SOURCE_PREFIXES = (
    'fbref', 'fotmob', 'understat', 'whoscored', 'sofascore',
    'espn', 'matchhistory', 'transfermarkt', 'capology', 'sofifa', 'clubelo',
)

# Grain / entity-suffix tokens accepted by the naming standard (R5).
GRAIN_TOKENS = (
    'match', 'season', 'profile', 'events', 'history',
    # sanctioned legacy aliases (suffix drift noted in charter §3)
    'aggregate', 'odds', 'salaries', 'players', 'transfers', 'lineup',
    'enriched', 'unavailable', 'spadl', 'ratings', 'stats',
)

# Sanctioned registry — keep in sync with docs/decisions/silver-charter.md §7.
SANCTIONED: dict[str, tuple[str, str]] = {
    # #370 team-wave DONE: the 4 team-season rollups moved to gold.*_team_season
    # (built by dag_transform_fbref_gold) and their silver SQL was deleted, so
    # they no longer exist to scan. fbref_team_season_profile stays in Silver but
    # is now COMPLIANT (season-from-season conform, not a match→season rollup —
    # R1 detector refined to require a match/event source).
    'whoscored_player_season_aggregate': ('EXCEPTION', 'player season-rollup feeding Gold; migration #370 PR2'),
    'fotmob_player_season_profile': ('EXCEPTION', 'PIVOT of season-grain Bronze; reclassify in #370 PR2'),
    # sofascore_team_match: resolved #367 — cross-entity minutes/assists rollup
    # moved out; now a clean single-source conform (PIVOT match_stats + schedule). COMPLIANT.
    # #382 DONE: the 2 cross-source E3/E4 facts (match_cards, match_substitutions)
    # were folded into gold.fct_card / gold.fct_substitution (assembly reads
    # bronze+xref directly) and their silver SQL was deleted — no longer scanned.
    # sofifa_player_profile_empty: resolved #369 — it is the empty fallback for
    # silver.sofifa_player_profile (issue #180), not a standalone table. Now
    # excluded from the scan (see FALLBACK_SUFFIX), so no registry entry needed.
}

# Tables whose `season` is legitimately stored year-start (bigint `2024`),
# NOT the default varchar slug `'2425'`. Sanctioned because they join to
# `xref_team` / `xref_match` in the source's native year-start format; the
# slug↔year-start conversion happens at the Gold boundary, not in Silver.
# Charter §4/§7. For these, S2 is a WARN (visible) instead of an ERROR.
# Full unification onto slug is tracked under the cross-source identity epic.
SEASON_YEAR_START_OK: set[str] = {
    'fbref_keeper_profile', 'fbref_match_enriched', 'fbref_match_events',
    'fbref_match_lineups', 'fbref_player_match_stats',
    'fbref_player_season_profile', 'fbref_team_season_profile',
    'fotmob_keeper_profile', 'fotmob_match_referee',
    'fotmob_player_market_value_history', 'fotmob_player_profile',
    'fotmob_player_season_profile', 'matchhistory_match_odds',
}

_COMMENT_LINE = re.compile(r'--[^\n]*')
_COMMENT_BLOCK = re.compile(r'/\*.*?\*/', re.DOTALL)
_AGG = re.compile(r'\b(SUM|AVG|MIN|MAX|COUNT)\s*\(', re.IGNORECASE)
_GROUP_BY = re.compile(r'\bGROUP\s+BY\b', re.IGNORECASE)
# A reference to a finer-grain (match / event) source. R1 only fires when a
# season-grain table actually rolls UP from one of these (charter §2: "grain
# change upward, season from match"). A season-grain table built from
# already-season-grain Bronze (dedup / PIVOT / intra-season player→team agg)
# is conform, not a rollup — issue #370.
_MATCH_GRAIN_SRC = re.compile(r'iceberg\.(?:bronze|silver)\.[a-z0-9_]*(?:_match|_events)',
                              re.IGNORECASE)
_DDL = re.compile(r'\b(CREATE\s+TABLE|CREATE\s+OR\s+REPLACE|INSERT\s+INTO)\b', re.IGNORECASE)
_SILVER_REF = re.compile(r'iceberg\.silver\.([a-z0-9_]+)', re.IGNORECASE)
_ANY_REF = re.compile(r'iceberg\.(?:bronze|silver)\.([a-z0-9_]+)', re.IGNORECASE)
_XREF_JOIN = re.compile(r'iceberg\.silver\.(xref_[a-z]+)', re.IGNORECASE)


def strip_sql(text: str) -> str:
    """Remove comments so doc prose ('we PIVOT', 'GROUP BY ...') is not analysed."""
    text = _COMMENT_BLOCK.sub(' ', text)
    text = _COMMENT_LINE.sub(' ', text)
    return text


def source_of(name: str) -> str | None:
    for p in SOURCE_PREFIXES:
        if name.startswith(p):
            return p
    return None


def grain_of(stem: str) -> str:
    """Coarse grain from the table name: season > match > other."""
    if 'season' in stem:
        return 'season'
    if 'match' in stem:
        return 'match'
    return 'other'


def audit_file(path: Path) -> list[dict]:
    """Layer A static findings for one SQL file."""
    stem = path.name.replace('.sql.j2', '').replace('.sql', '')
    raw = path.read_text(encoding='utf-8')
    sql = strip_sql(raw)
    findings: list[dict] = []
    is_xref = stem.startswith('xref_')

    # R6 NO_DDL — pure SELECT expected.
    if _DDL.search(sql):
        findings.append({'rule': 'R6', 'sev': 'ERROR', 'detail': 'file contains DDL (CREATE/INSERT)'})

    # R1 ROLLUP — GROUP BY + aggregate on a season-grain table that ALSO reads a
    # finer-grain (match / event) source = grain change upward. A season-grain
    # table built from already-season-grain Bronze (dedup / PIVOT / intra-season
    # player→team aggregation) is conform, NOT a rollup — charter §2 (#370).
    if (grain_of(stem) == 'season' and _GROUP_BY.search(sql) and _AGG.search(sql)
            and _MATCH_GRAIN_SRC.search(sql)):
        findings.append({'rule': 'R1', 'sev': 'ERROR',
                         'detail': 'season-grain rollup (GROUP BY + aggregate over a '
                                   'match/event source) — fact aggregation'})

    # R2 SILVER_ON_SILVER — reads another silver table (non-xref, non-self).
    silver_refs = sorted({m for m in _SILVER_REF.findall(sql)
                          if not m.startswith('xref_') and m != stem})
    if silver_refs:
        findings.append({'rule': 'R2', 'sev': 'WARN',
                         'detail': f'reads silver.{{{", ".join(silver_refs)}}}'})

    # R3 XREF_PREDICATE — each xref_* reference must carry league AND season nearby.
    # xref is loaded either via JOIN ... ON (predicate after) or a CTE
    # `SELECT ..., league, season FROM xref_*` (predicate before, in the SELECT list),
    # so the window is two-sided. xref_* files reference xref by definition — skip them.
    if not is_xref:
        for m in _XREF_JOIN.finditer(sql):
            window = sql[max(0, m.start() - 300):m.end() + 600].lower()
            if 'league' not in window or 'season' not in window:
                findings.append({'rule': 'R3', 'sev': 'ERROR',
                                 'detail': f'{m.group(1)} reference missing (league AND season) predicate'})
                break

    # R4 CROSS_SOURCE — ≥2 distinct source prefixes among bronze/silver refs (xref excepted).
    if not is_xref:
        srcs = {s for ref in _ANY_REF.findall(sql)
                if not ref.startswith('xref_') and (s := source_of(ref))}
        if len(srcs) >= 2:
            findings.append({'rule': 'R4', 'sev': 'WARN',
                             'detail': f'cross-source refs: {", ".join(sorted(srcs))}'})

    # R5 NAMING — {source}_{...}_{grain-token}.
    if not is_xref:
        src = source_of(stem)
        if src is None:
            findings.append({'rule': 'R5', 'sev': 'WARN', 'detail': 'unknown source prefix'})
        elif not any(tok in stem for tok in GRAIN_TOKENS):
            findings.append({'rule': 'R5', 'sev': 'WARN', 'detail': 'no recognized grain suffix'})

    return findings


def verdict_of(stem: str, findings: list[dict]) -> tuple[str, str]:
    if stem in SANCTIONED:
        return SANCTIONED[stem]
    if not findings:
        return ('COMPLIANT', '')
    if any(f['sev'] == 'ERROR' for f in findings):
        return ('VIOLATOR', 'unsanctioned ERROR finding — review')
    return ('REVIEW', 'WARN-only findings')


def has_unsanctioned_error(stem: str, findings: list[dict]) -> bool:
    """Gate condition (#372): an ERROR finding on a table not in SANCTIONED.

    This is exactly the case ``verdict_of`` labels VIOLATOR with reason
    'unsanctioned ERROR finding'. Sanctioned tables (EXCEPTION/VIOLATOR/
    INVESTIGATE registry entries) never trip the gate.
    """
    return stem not in SANCTIONED and any(f['sev'] == 'ERROR' for f in findings)


# ---- Layer B (schema, optional) --------------------------------------------

def audit_schema(cur, table: str) -> list[dict]:
    cur.execute(f'DESCRIBE iceberg.silver.{table}')
    cols = {r[0]: r[1] for r in cur.fetchall()}
    findings: list[dict] = []
    for required in ('_silver_created_at', 'league', 'season'):
        if required not in cols:
            findings.append({'rule': 'S1', 'sev': 'ERROR', 'detail': f'missing column {required}'})
    if 'season' in cols and not cols['season'].startswith('varchar'):
        if table in SEASON_YEAR_START_OK:
            findings.append({'rule': 'S2', 'sev': 'WARN',
                             'detail': f"season is {cols['season']} (sanctioned year-start, "
                                       "slug↔year-start converted at Gold boundary — charter §4/§7)"})
        else:
            findings.append({'rule': 'S2', 'sev': 'ERROR',
                             'detail': f"season is {cols['season']}, expected varchar slug"})
    for col in cols:
        if col.endswith('_id') and not (col.endswith('_id_raw') or col.endswith('_id_canonical')):
            findings.append({'rule': 'S3', 'sev': 'WARN',
                             'detail': f'bare id column {col} (use _raw / _canonical)'})
    return findings


# ---- Report ----------------------------------------------------------------

def render(per_table: dict[str, list[dict]], output: Path) -> None:
    n = len(per_table)
    errs = sum(1 for fs in per_table.values() for f in fs if f['sev'] == 'ERROR')
    warns = sum(1 for fs in per_table.values() for f in fs if f['sev'] == 'WARN')
    by_verdict: dict[str, int] = {}
    rows = []
    for stem in sorted(per_table):
        fs = per_table[stem]
        v, reason = verdict_of(stem, fs)
        by_verdict[v] = by_verdict.get(v, 0) + 1
        ruleset = ', '.join(sorted({f['rule'] for f in fs})) or '—'
        rows.append((stem, v, ruleset, reason))

    L = [
        f'# Silver Charter Audit — {datetime.now(timezone.utc):%Y-%m-%d}',
        '',
        f'Tables scanned: **{n}** · ERROR findings: **{errs}** · WARN findings: **{warns}**',
        '',
        'Verdicts: ' + ' · '.join(f'{k}={v}' for k, v in sorted(by_verdict.items())),
        '',
        '> Reference: `docs/decisions/silver-charter.md`. EXCEPTION/VIOLATOR/INVESTIGATE',
        '> verdicts come from the sanctioned registry (charter §7).',
        '',
        '## Per-table verdict',
        '',
        '| Table | Verdict | Rules | Note |',
        '|---|---|---|---|',
    ]
    icon = {'COMPLIANT': '✅', 'EXCEPTION': '⚠️', 'VIOLATOR': '❌',
            'REVIEW': '🔍', 'INVESTIGATE': '🔍'}
    for stem, v, ruleset, reason in rows:
        L.append(f'| `{stem}` | {icon.get(v, "")} {v} | {ruleset} | {reason} |')

    L += ['', '## Findings by rule', '', '| Table | Rule | Severity | Detail |', '|---|---|---|---|']
    for stem in sorted(per_table):
        for f in per_table[stem]:
            L.append(f"| `{stem}` | {f['rule']} | {f['sev']} | {f['detail']} |")

    output.write_text('\n'.join(L) + '\n', encoding='utf-8')


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--output', default=f'/tmp/silver_charter_audit_{datetime.now(timezone.utc):%Y-%m-%d}.md')
    p.add_argument('--table', default=None, help='audit only this table stem')
    p.add_argument('--schema', action='store_true', help='also run Layer B (needs Trino)')
    p.add_argument('--check', action='store_true',
                   help='gate mode: Layer A only, exit 1 on unsanctioned ERROR (#372)')
    args = p.parse_args()

    files = silver_sql_files()
    if args.table:
        files = [f for f in files if f.name.replace('.sql.j2', '').replace('.sql', '') == args.table]
        if not files:
            sys.exit(f'no silver SQL file for table {args.table!r}')

    per_table: dict[str, list[dict]] = {}
    for f in files:
        stem = f.name.replace('.sql.j2', '').replace('.sql', '')
        per_table[stem] = audit_file(f)

    # --check: Layer A gate (no Trino, no report file). Exit 1 on unsanctioned ERROR.
    if args.check:
        offenders = {stem: fs for stem, fs in per_table.items()
                     if has_unsanctioned_error(stem, fs)}
        if offenders:
            print('Silver Charter gate FAILED — unsanctioned ERROR finding(s):',
                  file=sys.stderr)
            for stem in sorted(offenders):
                for f in offenders[stem]:
                    if f['sev'] == 'ERROR':
                        print(f'  {stem}: [{f["rule"]}] {f["detail"]}', file=sys.stderr)
            print('\nFix the SQL, or sanction the table in the registry '
                  '(charter §7) if intentional.', file=sys.stderr)
            sys.exit(1)
        print(f'Silver Charter gate OK — {len(per_table)} files, no unsanctioned ERROR.',
              file=sys.stderr)
        sys.exit(0)

    if args.schema:
        sys.path.insert(0, '/opt/airflow/dags')
        from utils.silver_tasks import _get_trino_connection  # lazy: needs container
        cur = _get_trino_connection().cursor()
        for stem in per_table:
            try:
                per_table[stem] += audit_schema(cur, stem)
            except Exception as e:
                per_table[stem].append({'rule': 'S?', 'sev': 'WARN',
                                        'detail': f'schema check skipped: {type(e).__name__}'})

    out = Path(args.output)
    render(per_table, out)
    # Console summary for quick CLI feedback.
    for stem in sorted(per_table):
        v, _ = verdict_of(stem, per_table[stem])
        rules = ','.join(sorted({f['rule'] for f in per_table[stem]})) or '-'
        print(f'{v:12} {stem:42} [{rules}]', file=sys.stderr)
    print(f'\nReport: {out}', file=sys.stderr)


if __name__ == '__main__':
    main()
