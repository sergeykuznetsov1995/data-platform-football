#!/usr/bin/env python3
"""
One-shot drop of the UTF-8 BOM artifact column from matchhistory Bronze (issue #309,
followup из bronze-аудита #282).

Background: football-data.co.uk отдаёт CSV с UTF-8 BOM (байты EF BB BF) на первом
заголовке. ``requests`` декодирует тело как latin-1, поэтому BOM превращается в три
символа '\xef\xbb\xbf', и первая колонка ``Div`` сохранялась как артефактная
``ï»¿div`` рядом с нормальной ``div``. Скрапер починен (scrapers/matchhistory/
scraper.py::_standardize_columns срезает BOM из заголовков), этот скрипт убирает уже
накопленную колонку из live-таблицы. Нормальная ``div`` сохраняется.

  bronze.matchhistory_results — '\xef\xbb\xbfdiv'   (latin-1 mojibake form)

Ни ``div``, ни ``ï»¿div`` не используются в Silver/Gold — дроп безопасен.

Почему CTAS+rename, а не ``ALTER TABLE ... DROP COLUMN``:
    На Iceberg-таблицах, прошедших несколько раундов ADD COLUMN, ``DROP COLUMN``
    падает с ``ICEBERG_COMMIT_ERROR: ... incompatible types in respective
    positions`` (position-aware reads расходятся с current schema). Рабочий обход —
    пересобрать таблицу через CTAS без артефактной колонки, затем DROP + RENAME.
    Партиционирование (league, season) ОБЯЗАНО переноситься явно, иначе
    ``replace_partitions`` в run_matchhistory_scraper.py сломается.

Скрипт schema-driven: список оставляемых колонок строится из
information_schema.columns (в исходном порядке) за вычетом DROP-набора —
не нужно хардкодить полную схему и она устойчива к будущим ADD COLUMN.

ВАЖНО перед запуском: поставить dag_ingest_matchhistory на паузу (иначе INSERT race
во время swap'а).

Run inside the airflow container (так он видит Trino в docker-сети):

    docker compose exec airflow-webserver \
        python /opt/airflow/scripts/drop_matchhistory_bom_column.py

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
logger = logging.getLogger('drop_matchhistory_bom')

# table -> artifact columns to drop (lower-case, как в Iceberg). BOM surfaces as the
# latin-1 mojibake '\xef\xbb\xbf' prefix (verified live 2026-06-06, codepoints
# 0xef 0xbb 0xbf 'd' 'i' 'v'). The single-char '﻿' form covered for safety.
DEAD_COLUMNS: dict[str, set[str]] = {
    'matchhistory_results': {'\xef\xbb\xbfdiv', '﻿div'},
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


def rebuild_table(c, table: str, dead: set[str], dry_run: bool) -> None:
    cols = ordered_columns(c, table)
    if not cols:
        logger.error(f"{table}: таблица не найдена / пустая схема — пропуск")
        return

    present_dead = [col for col in cols if col in dead]
    missing_dead = sorted(dead - set(cols))
    if missing_dead:
        logger.info(
            f"{table}: колонки уже отсутствуют (нечего дропать): {missing_dead!r}"
        )
    if not present_dead:
        logger.info(f"{table}: артефактных колонок нет — таблица уже чистая, пропуск")
        return

    keep = [col for col in cols if col not in dead]
    for pcol in ('league', 'season'):
        if pcol not in keep:
            logger.error(
                f"{table}: partition-колонка '{pcol}' отсутствует в keep-наборе "
                f"— отказ (CTAS потеряет партиционирование)"
            )
            sys.exit(2)

    logger.info(f"{table}: drop={present_dead!r}")
    logger.info(f"{table}: keep ({len(keep)} cols)={keep}")

    if dry_run:
        logger.info(f"{table}: --dry-run, изменений нет")
        return

    fqtn = f'iceberg.bronze.{table}'
    staging = f'iceberg.bronze.{table}_drop_staging'
    keep_select = ', '.join(f'"{col}"' for col in keep)

    rows = execute(c, f'SELECT COUNT(*) FROM {fqtn}')
    before = rows[0][0]
    logger.info(f"{table}: before rows={before}")

    try:
        execute(c, f'DROP TABLE IF EXISTS {staging}')
    except Exception as e:
        logger.warning(f"{table}: не удалось дропнуть прежний staging: {e}")

    execute(c, f"""
        CREATE TABLE {staging}
        WITH (partitioning = ARRAY['league', 'season'])
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
        logger.error(f"{table}: колонки всё ещё присутствуют: {leftover!r}")
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
        rebuild_table(c, table, dead, args.dry_run)


if __name__ == '__main__':
    main()
