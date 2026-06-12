#!/usr/bin/env python3
"""Before/after parity check for the season-slug unification (#404).

The unification only changes the *representation* of ``season`` (year-start
bigint ``2024`` → slug varchar ``'2425'``). It must NOT change any row counts or
metric totals. This script snapshots a fixed set of metrics from Silver + Gold,
then diffs two snapshots and fails loudly on any non-zero delta.

Usage (read-only; run BEFORE the cutover, then again AFTER):

    python scripts/parity_season_unify.py snapshot --out /tmp/before.json
    # ... deploy #404 (DROP + rebuild Silver → Gold) ...
    python scripts/parity_season_unify.py snapshot --out /tmp/after.json
    python scripts/parity_season_unify.py diff /tmp/before.json /tmp/after.json

Connection: same env contract as ``silver_tasks._get_trino_connection``
(TRINO_HOST / TRINO_PORT / TRINO_USER / TRINO_PASSWORD). Run on the host or via
``make shell-trino`` env.

Invariant asserted by ``diff``: for every metric, before == after. The ONLY
thing allowed to change is the literal season *values* — never the cardinality
(``season_distinct``) nor any count/sum.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import trino as trino_lib


# Per-table metrics. Every entry yields `<name>__rows` (COUNT(*)) and, when the
# table has a season column, `<name>__season_distinct` (COUNT(DISTINCT season)).
# `sums` are extra semantic invariants (SUM of a numeric column) — same data,
# only season representation changed, so these must be byte-identical.
TABLES: dict[str, dict] = {
    # ---- Silver (the 13 tables flipped to slug) ----
    'silver.fbref_player_match_stats':   {'sums': ['goals', 'assists', 'minutes', 'shots']},
    'silver.fbref_match_enriched':       {'sums': ['home_score', 'away_score']},
    'silver.fbref_match_events':         {'sums': []},
    'silver.fbref_match_lineups':        {'sums': []},
    'silver.fbref_keeper_profile':       {'sums': ['saves', 'goals_against']},
    'silver.fbref_player_season_profile': {'sums': ['goals', 'assists']},
    'silver.fbref_team_season_profile':  {'sums': []},
    'silver.fotmob_keeper_profile':      {'sums': []},
    'silver.fotmob_match_referee':       {'sums': []},
    'silver.fotmob_player_market_value_history': {'sums': ['market_value_eur']},
    'silver.fotmob_player_profile':      {'sums': []},
    'silver.fotmob_player_season_profile': {'sums': []},
    'silver.matchhistory_match_odds':    {'sums': []},
    # ---- xref (value-only change for fbref/fotmob/matchhistory branches) ----
    'silver.xref_team':                  {'sums': []},
    'silver.xref_match':                 {'sums': []},
    'silver.xref_manager':               {'sums': []},
    'silver.xref_referee':               {'sums': []},
    # ---- Gold dims + base facts ----
    'gold.dim_match':                    {'sums': ['total_goals']},
    'gold.dim_team':                     {'sums': []},
    'gold.dim_player':                   {'sums': []},
    'gold.dim_manager':                  {'sums': []},
    'gold.fct_match_odds':               {'sums': []},
    # ---- Gold facts ----
    'gold.fct_player_match':             {'sums': ['goals', 'assists', 'minutes']},
    'gold.fct_player_season_stats':      {'sums': ['goals']},
    'gold.fct_keeper_season_stats':      {'sums': []},
    'gold.fct_team_season_stats':        {'sums': []},
    'gold.fct_team_match':               {'sums': ['goals_for', 'goals_against']},
    # fct_card / fct_goal / fct_substitution dropped in #448
    # (superseded by gold.fct_match_timeline)
    'gold.fct_lineup':                   {'sums': []},
    'gold.fct_player_unavailable':       {'sums': []},
    'gold.fct_player_market_value':      {'sums': ['market_value_eur']},
    # ---- Gold dims without a season column (cardinality-only sanity) ----
    'gold.dim_referee':                  {'sums': [], 'no_season': True},
}


def _connect():
    host = os.environ.get('TRINO_HOST', 'localhost')
    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')
    if password:
        port = int(os.environ.get('TRINO_PORT', 8443))
        return trino_lib.dbapi.connect(
            host=host, port=port, user=user, catalog='iceberg',
            http_scheme='https',
            auth=trino_lib.auth.BasicAuthentication(user, password),
            verify=False,
        )
    port = int(os.environ.get('TRINO_PORT', 8080))
    return trino_lib.dbapi.connect(host=host, port=port, user=user, catalog='iceberg')


def _scalar(cur, sql: str):
    cur.execute(sql)
    row = cur.fetchall()
    return row[0][0] if row else None


def snapshot(out_path: str) -> None:
    conn = _connect()
    cur = conn.cursor()
    metrics: dict[str, object] = {}
    for table, cfg in TABLES.items():
        fq = f'iceberg.{table}'
        try:
            metrics[f'{table}__rows'] = _scalar(cur, f'SELECT COUNT(*) FROM {fq}')
            if not cfg.get('no_season'):
                metrics[f'{table}__season_distinct'] = _scalar(
                    cur, f'SELECT COUNT(DISTINCT season) FROM {fq}')
            for col in cfg.get('sums', []):
                metrics[f'{table}__sum_{col}'] = _scalar(
                    cur, f'SELECT CAST(COALESCE(SUM({col}), 0) AS double) FROM {fq}')
        except Exception as exc:  # missing table during a partial deploy → record it
            metrics[f'{table}__error'] = str(exc)[:200]
    conn.close()
    with open(out_path, 'w', encoding='utf-8') as fh:
        json.dump(metrics, fh, indent=2, sort_keys=True, default=str)
    print(f'wrote {len(metrics)} metrics → {out_path}')


def diff(before_path: str, after_path: str) -> int:
    with open(before_path, encoding='utf-8') as fh:
        before = json.load(fh)
    with open(after_path, encoding='utf-8') as fh:
        after = json.load(fh)

    keys = sorted(set(before) | set(after))
    deltas: list[str] = []
    for k in keys:
        b, a = before.get(k), after.get(k)
        if k.endswith('__error'):
            deltas.append(f'  ERROR present: {k} = {a or b}')
            continue
        if b != a:
            deltas.append(f'  CHANGED {k}: before={b!r} after={a!r}')

    if deltas:
        print('PARITY FAIL — season unification changed data:')
        print('\n'.join(deltas))
        return 1
    print(f'PARITY OK — {len(keys)} metrics identical before/after '
          '(only season representation changed).')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='Season-slug unification parity check (#404)')
    sub = parser.add_subparsers(dest='cmd', required=True)
    snap = sub.add_parser('snapshot', help='snapshot metrics to JSON')
    snap.add_argument('--out', required=True)
    df = sub.add_parser('diff', help='diff two snapshots')
    df.add_argument('before')
    df.add_argument('after')
    args = parser.parse_args()

    if args.cmd == 'snapshot':
        snapshot(args.out)
        return 0
    return diff(args.before, args.after)


if __name__ == '__main__':
    sys.exit(main())
