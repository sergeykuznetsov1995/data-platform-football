"""R5 repair driver — пересборка xref → silver → gold после бэкфила SofaScore.

Контекст: docs/research/R5_medallion_loss_review.md (HIGH-1). Запускается
ВНУТРИ контейнера airflow-scheduler (все пути/коннекты контейнерные):

    python dags/scripts/r5_repair_transforms.py --phase xref \
        --fbref-control-run-id <validated-source-run-uuid>

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
from types import SimpleNamespace
import uuid

sys.path.insert(0, '/opt/airflow/dags')
sys.path.insert(0, '/opt/airflow')

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('r5_repair')

PARTS = ['league', 'season']


class _FakeTI:
    """Заглушка Airflow TaskInstance для DAG-callables вне Airflow."""

    def xcom_push(self, key=None, value=None):
        logger.info("xcom_push(%s) suppressed (standalone run)", key)


def phase_gold_fast(fbref_control_run_id):
    from utils.gold_tasks import run_gold_transform
    for sql, table in [
        ('dags/sql/gold/fct_shot.sql', 'fct_shot'),
        ('dags/sql/gold/fct_player_match.sql.j2', 'fct_player_match'),
    ]:
        r = run_gold_transform(
            sql_file=sql,
            table_name=table,
            partition_columns=PARTS,
            fbref_control_run_id=fbref_control_run_id,
        )
        logger.info("%s: %s rows", table, r.get('rows'))


def phase_xref(fbref_control_run_id):
    import dag_transform_xref as x
    from utils.silver_tasks import run_silver_transform

    ti = _FakeTI()
    context = {
        'ti': ti,
        'dag_run': SimpleNamespace(
            conf={'fbref_control_run_id': fbref_control_run_id}
        ),
    }
    x._run_xref_team(**context)
    x._run_xref_referee(**context)
    x._run_xref_manager(**context)
    for _task, sql, table in x.PURE_SQL_XREF_TRANSFORMS:
        r = run_silver_transform(
            sql_file=sql,
            table_name=table,
            schema='silver',
            fbref_control_run_id=fbref_control_run_id,
        )
        logger.info("%s: %s rows", table, r.get('rows'))
    x._run_xref_player(**context)
    x._validate_xref(**context)


def phase_silver(fbref_control_run_id):
    from utils.silver_tasks import run_silver_transform
    for sql, table in [
        ('dags/sql/silver/sofascore_player_ratings.sql', 'sofascore_player_ratings'),
        ('dags/sql/silver/sofascore_shots.sql', 'sofascore_shots'),
        ('dags/sql/silver/understat_shots.sql', 'understat_shots'),
    ]:
        r = run_silver_transform(
            sql_file=sql,
            table_name=table,
            schema='silver',
            fbref_control_run_id=fbref_control_run_id,
        )
        logger.info("%s: %s rows", table, r.get('rows'))


def phase_gold_full(fbref_control_run_id):
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
        r = run_gold_transform(
            sql_file=sql,
            table_name=table,
            partition_columns=PARTS,
            fbref_control_run_id=fbref_control_run_id,
        )
        logger.info("%s: %s rows", table, r.get('rows'))


PHASES = {
    'gold-fast': phase_gold_fast,
    'xref': phase_xref,
    'silver': phase_silver,
    'gold-full': phase_gold_full,
}


def run_locked_phase(phase, fbref_control_run_id):
    """Validate the scope and fence all global CTAS repair writes."""

    from scrapers.fbref.control import ControlStore
    from utils.fbref_pipeline_tasks import (
        FBREF_PUBLICATION_LOCK_TTL_SECONDS,
        validate_fbref_publication_scope,
    )

    validate_fbref_publication_scope(
        control_run_id=fbref_control_run_id
    )
    control = ControlStore.from_env()
    control.migrate()
    repair_run_id = str(uuid.uuid4())
    control.create_run(
        'repair',
        run_id=repair_run_id,
        request_limit=0,
        byte_limit=0,
        metadata={
            'phase': phase,
            'fbref_control_run_id': fbref_control_run_id,
        },
    )
    control.start_run(repair_run_id)
    try:
        control.acquire_publication_lock(
            repair_run_id,
            dag_id='r5_repair_transforms',
            ttl_seconds=FBREF_PUBLICATION_LOCK_TTL_SECONDS,
        )
        PHASES[phase](fbref_control_run_id)
        control.release_publication_lock(repair_run_id)
    except BaseException:
        try:
            control.release_publication_lock(repair_run_id)
        except Exception:
            logger.exception("repair publication lock cleanup failed")
        control.finish_run(repair_run_id, succeeded=False)
        raise
    control.finish_run(repair_run_id, succeeded=True)

if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--phase', required=True, choices=sorted(PHASES))
    p.add_argument(
        '--fbref-control-run-id',
        required=True,
        help='UUID of the validated immutable FBref publication scope',
    )
    args = p.parse_args()
    try:
        control_run_id = str(uuid.UUID(args.fbref_control_run_id.strip()))
    except (AttributeError, TypeError, ValueError) as exc:
        p.error(f'--fbref-control-run-id must be a UUID: {exc}')
    run_locked_phase(args.phase, control_run_id)
    logger.info("phase %s OK", args.phase)
