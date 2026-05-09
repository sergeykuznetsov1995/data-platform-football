#!/usr/bin/env python3
"""Post-scrape verification for E3.5 ESPN backfill.

Acceptance criteria (from task brief):
 - Per-season lineup: >=350 distinct games / 380 (~92%+) coverage. Rows ~11K-14K.
 - Per-season schedule: 380 rows (no duplicates).
 - 2425 after redo: should be ~same as 2526 (~13950 rows / ~352 games).

Usage (in container):
    docker compose exec -T airflow-scheduler python /opt/airflow/scripts/verify_espn_e3_5.py
"""

import sys
import warnings

warnings.filterwarnings('ignore')

from dags.utils.silver_tasks import _get_trino_connection  # noqa: E402

EXPECTED_SEASONS = ['2122', '2223', '2324', '2425', '2526']
SCHEDULE_TARGET = 380
LINEUP_GAMES_MIN = 350
LINEUP_ROWS_MIN = 11000


def main() -> int:
    conn = _get_trino_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT season, COUNT(*) AS rows, COUNT(DISTINCT game) AS games "
        "FROM iceberg.bronze.espn_lineup GROUP BY 1 ORDER BY 1"
    )
    lineup = {s: (r, g) for s, r, g in cur.fetchall()}

    cur.execute(
        "SELECT season, COUNT(*) AS rows "
        "FROM iceberg.bronze.espn_schedule GROUP BY 1 ORDER BY 1"
    )
    schedule = {s: r for s, r in cur.fetchall()}

    cur.execute(
        "SELECT season, COUNT(*) - COUNT(DISTINCT game||'|'||player||'|'||team) AS dup_count "
        "FROM iceberg.bronze.espn_lineup GROUP BY 1 ORDER BY 1"
    )
    dup_counts = {s: d for s, d in cur.fetchall()}

    cur.close()
    conn.close()

    print('=' * 78)
    print('ESPN bronze post-scrape verification')
    print('=' * 78)
    print(f"{'season':<8}{'sched':>8}{'lineup':>10}{'games':>8}{'dups':>8}  status")
    print('-' * 78)

    failures = []
    for s in EXPECTED_SEASONS:
        sched = schedule.get(s, 0)
        rows, games = lineup.get(s, (0, 0))
        dups = dup_counts.get(s, 0)

        ok = True
        notes = []
        if s in ('2122', '2223', '2324', '2425', '2526'):
            if sched != SCHEDULE_TARGET:
                ok = False
                notes.append(f"sched={sched}!={SCHEDULE_TARGET}")
        if s in ('2122', '2223', '2324', '2425', '2526'):
            if games < LINEUP_GAMES_MIN:
                ok = False
                notes.append(f"games={games}<{LINEUP_GAMES_MIN}")
            if rows < LINEUP_ROWS_MIN:
                ok = False
                notes.append(f"rows={rows}<{LINEUP_ROWS_MIN}")
        if dups > 0:
            ok = False
            notes.append(f"dup_lineup_rows={dups}")

        status = 'OK' if ok else 'FAIL ' + ' '.join(notes)
        if not ok:
            failures.append(s)
        print(f"{s:<8}{sched:>8}{rows:>10}{games:>8}{dups:>8}  {status}")

    print('-' * 78)
    if failures:
        print(f"FAILED seasons: {failures}")
        return 1
    print('All seasons pass acceptance criteria.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
