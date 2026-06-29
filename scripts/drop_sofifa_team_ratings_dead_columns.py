#!/usr/bin/env python3
"""
One-shot drop of 15 dead 100%-NULL columns from bronze.sofifa_team_ratings
(issue #601, followup из write-only audit #476).

Background: EA убрала из FC 26 командные тактики (build-up / chance-creation /
defence sliders), international & domestic prestige и переименовала
``whole_team_average_age`` → ``starting_xi_average_age``. sofifa.com больше не
рендерит соответствующие ``<td data-col=...>`` ячейки, а upstream soccerdata всё
ещё запрашивал их через ``&showCol[]=`` → 15 колонок 100% NULL (подтверждено
audit_bronze_columns.py, live 2026-06-16). Парсер починен в
``scrapers/sofifa/flaresolverr_reader.read_team_ratings`` (теперь скрейпит только
8 живых колонок), но колонки остаются в Iceberg-схеме как остаток — этот скрипт
выпиливает их физически.

Оставляем 8 живых рейтингов + identity + meta:
    league, team, team_id, overall, attack, midfield, defence, transfer_budget,
    club_worth, players, starting_xi_average_age, fifa_edition (partition),
    version_id, update, _source, _entity_type, _ingested_at, _batch_id

Почему CTAS+rename, а не ``ALTER TABLE ... DROP COLUMN``:
    На Iceberg-таблицах, прошедших несколько раундов ADD COLUMN, ``DROP COLUMN``
    падает с ``ICEBERG_COMMIT_ERROR: ... incompatible types in respective
    positions``. Рабочий обход — пересобрать таблицу через CTAS без мёртвых
    колонок, затем DROP + RENAME. Партиционирование (fifa_edition) ОБЯЗАНО
    переноситься явно, иначе ``replace_partitions`` в run_sofifa_scraper.py
    сломается.

Скрипт schema-driven: список оставляемых колонок строится из
information_schema.columns (в исходном порядке) за вычетом DROP-набора —
не нужно хардкодить полную схему и она устойчива к будущим ADD COLUMN.

ВАЖНО перед запуском: поставить dag_ingest_sofifa на паузу (иначе INSERT race
во время swap'а).

Run inside the airflow container (так он видит Trino в docker-сети):

    docker compose exec airflow-webserver \
        python /opt/airflow/scripts/drop_sofifa_team_ratings_dead_columns.py

Флаги:
    --dry-run   только показать план (keep/drop колонки, без записи)
"""

import argparse
import logging
import os
import sys
import warnings

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('drop_sofifa_team_ratings_dead')

# table -> dead columns to drop (lower-case, как в Iceberg)
DEAD_COLUMNS: dict[str, set[str]] = {
    'sofifa_team_ratings': {
        'build_up_speed', 'build_up_dribbling', 'build_up_passing',
        'build_up_positioning',
        'chance_creation_crossing', 'chance_creation_passing',
        'chance_creation_shooting', 'chance_creation_positioning',
        'defence_aggression', 'defence_pressure', 'defence_team_width',
        'defence_defender_line', 'defence_domestic_prestige',
        'international_prestige',
        'whole_team_average_age',
    },
}

# table -> partition columns (must survive the CTAS rebuild verbatim)
PARTITION_COLS: dict[str, list[str]] = {
    'sofifa_team_ratings': ['fifa_edition'],
}


def get_conn():
    import trino
    password = os.environ.get('TRINO_PASSWORD', '')
    user = os.environ.get('TRINO_USER', 'airflow')
    kw = dict(
        host=os.environ.get('TRINO_HOST', 'trino'),
        port=int(os.environ.get('TRINO_PORT', 8443)),
        user=user,
        catalog='iceberg',
    )
    if password:
        kw.update(
            http_scheme='https',
            auth=trino.auth.BasicAuthentication(user, password),
            verify=False,
        )
    return trino.dbapi.connect(**kw)


def execute(c, sql):
    logger.info(f"EXEC: {sql[:140]}{'…' if len(sql) > 140 else ''}")
    c.execute(sql)
    return c.fetchall()


def ordered_columns(c, table: str) -> list[str]:
    """Все колонки таблицы в порядке ordinal_position."""
    rows = execute(c, f"""
        SELECT column_name
        FROM iceberg.information_schema.columns
        WHERE table_schema = 'bronze' AND table_name = '{table}'
        ORDER BY ordinal_position
    """)
    return [r[0] for r in rows]


def rebuild_table(c, table: str, dead: set[str], part_cols: list[str],
                  dry_run: bool) -> None:
    cols = ordered_columns(c, table)
    if not cols:
        logger.error(f"{table}: таблица не найдена / пустая схема — пропуск")
        return

    present_dead = [col for col in cols if col in dead]
    missing_dead = sorted(dead - set(cols))
    if missing_dead:
        logger.info(
            f"{table}: колонки уже отсутствуют (нечего дропать): {missing_dead}"
        )
    if not present_dead:
        logger.info(f"{table}: мёртвых колонок нет — таблица уже чистая, пропуск")
        return

    keep = [col for col in cols if col not in dead]
    for pcol in part_cols:
        if pcol not in keep:
            logger.error(
                f"{table}: partition-колонка '{pcol}' отсутствует в keep-наборе "
                f"— отказ (CTAS потеряет партиционирование)"
            )
            sys.exit(2)

    logger.info(f"{table}: drop={present_dead}")
    logger.info(f"{table}: keep ({len(keep)} cols)={keep}")

    if dry_run:
        logger.info(f"{table}: --dry-run, изменений нет")
        return

    fqtn = f'iceberg.bronze.{table}'
    staging = f'iceberg.bronze.{table}_drop_staging'
    keep_select = ', '.join(f'"{col}"' for col in keep)
    part_array = ', '.join(f"'{p}'" for p in part_cols)

    rows = execute(c, f'SELECT COUNT(*) FROM {fqtn}')
    before = rows[0][0]
    logger.info(f"{table}: before rows={before}")

    try:
        execute(c, f'DROP TABLE IF EXISTS {staging}')
    except Exception as e:
        logger.warning(f"{table}: не удалось дропнуть прежний staging: {e}")

    execute(c, f"""
        CREATE TABLE {staging}
        WITH (partitioning = ARRAY[{part_array}])
        AS
        SELECT {keep_select}
        FROM {fqtn}
    """)

    rows = execute(c, f'SELECT COUNT(*) FROM {staging}')
    after = rows[0][0]
    logger.info(f"{table}: staging rows={after}")

    if after != before:
        logger.error(
            f"{table}: row count mismatch (before={before} after={after}) — "
            f"abort, staging оставлен для ручной проверки"
        )
        sys.exit(3)

    execute(c, f'DROP TABLE {fqtn}')
    execute(c, f'ALTER TABLE {staging} RENAME TO bronze.{table}')

    final_cols = ordered_columns(c, table)
    leftover = sorted(set(final_cols) & dead)
    if leftover:
        logger.error(f"{table}: колонки всё ещё присутствуют: {leftover}")
        sys.exit(4)
    logger.info(
        f"{table}: готово — dropped {len(present_dead)} колонок, "
        f"{after} строк сохранено"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true',
                        help='только показать keep/drop план, без записи')
    args = parser.parse_args()

    conn = get_conn()
    c = conn.cursor()
    for table, dead in DEAD_COLUMNS.items():
        rebuild_table(c, table, dead, PARTITION_COLS[table], args.dry_run)


if __name__ == '__main__':
    main()
