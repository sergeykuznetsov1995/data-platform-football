"""R5 repair driver — пересборка xref → silver → gold после бэкфила SofaScore.

Контекст: docs/research/R5_medallion_loss_review.md (HIGH-1). Запускается
ВНУТРИ контейнера airflow-scheduler (все пути/коннекты контейнерные):

    python dags/scripts/r5_repair_transforms.py --phase gold-fast
    python dags/scripts/r5_repair_transforms.py --phase xref
    python dags/scripts/r5_repair_transforms.py --phase silver
    python dags/scripts/r5_repair_transforms.py --phase gold-full

Фазы:
  gold-fast — только fct_shot + fct_player_match (код-фиксы R5: is_sot,
              rating-fallback). Не требует свежего bronze/xref.
  xref      — xref_team/referee/manager/match (SQL) + xref_player (python
              resolver, rebuild → replace_league как в dag_transform_xref).
  silver    — canonical-embedding таблицы затронутых источников:
              sofascore_player_ratings, sofascore_shots, understat_shots.
  gold-full — все gold-таблицы, читающие xref_player(sofascore)/shots:
              fct_player_match(+audit), fct_player_season_stats(+audit),
              fct_match_rating, fct_shot(+audit).

Порядок при полном ремонте: скрейп player_capture (сезоны 2017, 2024) →
xref → silver → gold-full. gold-fast можно гнать в любой момент.
"""
import argparse
import logging
import sys

sys.path.insert(0, '/opt/airflow/dags')
sys.path.insert(0, '/opt/airflow')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('r5_repair')

PARTS = ['league', 'season']


class _FakeTI:
    """Заглушка Airflow TaskInstance для DAG-callables вне Airflow."""

    def xcom_push(self, key=None, value=None):
        logger.info("xcom_push(%s) suppressed (standalone run)", key)


def phase_gold_fast():
    from utils.gold_tasks import run_gold_transform
    for sql, table in [
        ('dags/sql/gold/fct_shot.sql', 'fct_shot'),
        ('dags/sql/gold/fct_player_match.sql.j2', 'fct_player_match'),
    ]:
        r = run_gold_transform(sql_file=sql, table_name=table, partition_columns=PARTS)
        logger.info("%s: %s rows", table, r.get('rows'))


def phase_xref():
    import dag_transform_xref as x
    from utils.silver_tasks import run_silver_transform

    ti = _FakeTI()
    x._run_xref_team(ti=ti)
    x._run_xref_referee(ti=ti)
    x._run_xref_manager(ti=ti)
    for _task, sql, table in x.PURE_SQL_XREF_TRANSFORMS:
        r = run_silver_transform(sql_file=sql, table_name=table, schema='silver')
        logger.info("%s: %s rows", table, r.get('rows'))
    x._run_xref_player(ti=ti)


def phase_silver():
    from utils.silver_tasks import run_silver_transform
    for sql, table in [
        ('dags/sql/silver/sofascore_player_ratings.sql', 'sofascore_player_ratings'),
        ('dags/sql/silver/sofascore_shots.sql', 'sofascore_shots'),
        ('dags/sql/silver/understat_shots.sql', 'understat_shots'),
    ]:
        r = run_silver_transform(sql_file=sql, table_name=table, schema='silver')
        logger.info("%s: %s rows", table, r.get('rows'))


def phase_gold_full():
    from utils.gold_tasks import run_gold_transform
    for sql, table in [
        ('dags/sql/gold/fct_player_match.sql.j2', 'fct_player_match'),
        ('dags/sql/gold/fct_player_match_audit.sql', 'fct_player_match_audit'),
        ('dags/sql/gold/fct_player_season_stats.sql.j2', 'fct_player_season_stats'),
        ('dags/sql/gold/fct_player_season_stats_audit.sql', 'fct_player_season_stats_audit'),
        ('dags/sql/gold/fct_match_rating.sql', 'fct_match_rating'),
        ('dags/sql/gold/fct_shot.sql', 'fct_shot'),
        ('dags/sql/gold/fct_shot_audit.sql', 'fct_shot_audit'),
    ]:
        r = run_gold_transform(sql_file=sql, table_name=table, partition_columns=PARTS)
        logger.info("%s: %s rows", table, r.get('rows'))


PHASES = {
    'gold-fast': phase_gold_fast,
    'xref': phase_xref,
    'silver': phase_silver,
    'gold-full': phase_gold_full,
}

if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--phase', required=True, choices=sorted(PHASES))
    args = p.parse_args()
    PHASES[args.phase]()
    logger.info("phase %s OK", args.phase)
