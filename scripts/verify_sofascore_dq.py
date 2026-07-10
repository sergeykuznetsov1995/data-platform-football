#!/usr/bin/env python3
"""Post-backfill DQ acceptance for the SofaScore top-5 match_capture backfill (issue #897).

Проверяет, что артефакт бага токенов сезона (#888/#896 — ``--season`` сдвигал партицию
на +1 сезон) не просочился в bronze: метка ``season`` каждой per-match записи должна
совпадать с реальным сезоном её матча в ``sofascore_schedule``.

Read-only. Ключ сверки — SofaScore event id, который лежит как ``match_id`` (varchar) в
четырёх per-match таблицах и как ``game_id`` (bigint) в ``schedule``/``venue``. Джойн идёт
БЕЗ предиката по season (иначе сдвинутые строки молча выпадают, как в продовых Silver-JOIN),
затем ``t.season`` сравнивается со ``schedule.season``.

Проверки:
 1. schedule-целостность: ни один ``game_id`` не под >1 season; нет пересечения game_id
    между сезонами одной лиги.
 2. per-match mislabel (player_ratings / event_player_stats / match_stats / event_shotmap):
    t.season должен равняться schedule.season по тому же event id. Ожидание: 0.
 3. orphan match_id: per-match match_id, отсутствующий в schedule той же лиги.
 4. venue vs schedule: переподтверждение 0 mislabel по venue.

Usage (в контейнере):
    docker compose exec -T airflow-scheduler python /opt/airflow/scripts/verify_sofascore_dq.py

Exit 1 при любом mislabel / дубле партиции / пересечении сезонов.
"""

import sys
import warnings

warnings.filterwarnings('ignore')

from dags.utils.silver_tasks import _get_trino_connection  # noqa: E402

EXPECTED_LEAGUES = (
    'ENG-Premier League', 'ESP-La Liga', 'FRA-Ligue 1',
    'GER-Bundesliga', 'ITA-Serie A',
)
PER_MATCH_TABLES = [
    'sofascore_player_ratings',
    'sofascore_event_player_stats',
    'sofascore_match_stats',
    'sofascore_event_shotmap',
]
_TOP5_SQL = "(" + ", ".join(f"'{lg}'" for lg in EXPECTED_LEAGUES) + ")"


def main() -> int:
    conn = _get_trino_connection()
    cur = conn.cursor()

    def scalar(sql):
        cur.execute(sql)
        return cur.fetchall()

    failures = []
    print('=' * 78)
    print('SofaScore top-5 match_capture — DQ acceptance (#897)')
    print('=' * 78)

    # 1. schedule integrity ---------------------------------------------------
    multi = scalar(f"""
        SELECT COUNT(*) FROM (
          SELECT league, game_id FROM iceberg.bronze.sofascore_schedule
          WHERE league IN {_TOP5_SQL}
          GROUP BY 1, 2 HAVING COUNT(DISTINCT season) > 1)""")[0][0]
    overlap = scalar(f"""
        WITH s AS (SELECT DISTINCT league, season, game_id
                   FROM iceberg.bronze.sofascore_schedule WHERE league IN {_TOP5_SQL})
        SELECT COUNT(*) FROM s a JOIN s b
          ON a.league = b.league AND a.game_id = b.game_id AND a.season < b.season""")[0][0]
    print(f"\n[schedule] game_id под >1 season : {multi}")
    print(f"[schedule] пересечений game_id между сезонами : {overlap}")
    if multi:
        failures.append(f"schedule: {multi} game_id под >1 season")
    if overlap:
        failures.append(f"schedule: {overlap} пересечений game_id между сезонами")

    # 2. per-match mislabel + 3. orphan --------------------------------------
    print(f"\n{'table':32s}{'mislabel':>10s}{'orphan':>10s}  status")
    print('-' * 78)
    for t in PER_MATCH_TABLES:
        mislabel = scalar(f"""
            WITH t AS (SELECT DISTINCT league, season, match_id
                       FROM iceberg.bronze.{t} WHERE league IN {_TOP5_SQL}),
                 s AS (SELECT DISTINCT league, season, CAST(game_id AS varchar) gid
                       FROM iceberg.bronze.sofascore_schedule WHERE league IN {_TOP5_SQL})
            SELECT COUNT(*) FROM t JOIN s
              ON s.gid = t.match_id AND s.league = t.league
            WHERE t.season <> s.season""")[0][0]
        orphan = scalar(f"""
            WITH t AS (SELECT DISTINCT league, match_id
                       FROM iceberg.bronze.{t} WHERE league IN {_TOP5_SQL}),
                 s AS (SELECT DISTINCT league, CAST(game_id AS varchar) gid
                       FROM iceberg.bronze.sofascore_schedule WHERE league IN {_TOP5_SQL})
            SELECT COUNT(*) FROM t LEFT JOIN s
              ON s.gid = t.match_id AND s.league = t.league
            WHERE s.gid IS NULL""")[0][0]
        ok = mislabel == 0
        if not ok:
            failures.append(f"{t}: {mislabel} mislabel")
        print(f"{t:32s}{mislabel:>10d}{orphan:>10d}  {'OK' if ok else 'FAIL'}")

    # 4. venue vs schedule ----------------------------------------------------
    venue_mislabel = scalar(f"""
        WITH v AS (SELECT DISTINCT league, season, game_id
                   FROM iceberg.bronze.sofascore_venue WHERE league IN {_TOP5_SQL}),
             s AS (SELECT DISTINCT league, season, game_id
                   FROM iceberg.bronze.sofascore_schedule WHERE league IN {_TOP5_SQL})
        SELECT COUNT(*) FROM v JOIN s
          ON s.game_id = v.game_id AND s.league = v.league
        WHERE v.season <> s.season""")[0][0]
    print(f"\n[venue] mislabel vs schedule : {venue_mislabel}  "
          f"{'OK' if venue_mislabel == 0 else 'FAIL'}")
    if venue_mislabel:
        failures.append(f"venue: {venue_mislabel} mislabel")

    cur.close()
    conn.close()

    print('-' * 78)
    if failures:
        print(f"FAILED: {len(failures)} проблем(ы):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("All checks pass — 0 mislabel across all 5 bronze tables.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
