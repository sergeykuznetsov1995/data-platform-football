"""Юнит-тест ключа дедупа FotMob-трансферов (issue #1149, PR #1158).

Исполняет ``dags/sql/silver/fotmob_transfers.sql`` в in-memory DuckDB
(текстовая подстановка имён таблиц — TRY_CAST/SUBSTR/LPAD/MOD совместимы).

Баг под тестом (подтверждён живьём 2026-08-11 запросом к
``iceberg.bronze.fotmob_transfer_events_current``): источник отдаёт один и тот же
переход дважды с таймстемпами в минутах друг от друга, поэтому дедуп по СЫРОЙ строке
``transfer_date`` считал их разными записями. Обе доезжали до Silver и роняли
DQ-правило ``no_duplicates``, у которого грейн — DATE.

Фикстура зеркалит живой случай: NED-Eredivisie, player_id 874482, два наблюдения
одного перехода 2026-08-04 с таймстемпами 09:12 и 09:31.

Тест намеренно двусторонний: сначала проверяется, что фикстура ВОСПРОИЗВОДИТ баг на
прежнем ключе (иначе тест вакуумный), потом — что текущий ключ его чинит.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


duckdb = pytest.importorskip("duckdb")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TRANSFERS_SQL = PROJECT_ROOT / "dags" / "sql" / "silver" / "fotmob_transfers.sql"

# competition_id 57 = NED-Eredivisie в живом league_map
LEAGUE_MAP_VALUES = "(57, 'NED-Eredivisie')"

OLD_DEDUP_KEY = (
    "PARTITION BY player_id, from_club_id, to_club_id, transfer_date, "
    "league, season_year"
)
NEW_DEDUP_KEY = (
    "PARTITION BY player_id, from_club_id, to_club_id, transfer_date_parsed, "
    "league, season_year"
)


def _render(*, dedup_key: str | None = None) -> str:
    sql = TRANSFERS_SQL.read_text(encoding="utf-8")
    sql = sql.replace("{{ fotmob_league_map_values_sql }}", LEAGUE_MAP_VALUES)
    sql = sql.replace("iceberg.bronze.", "")
    if dedup_key is not None:
        sql = re.sub(r"PARTITION BY [^\n]*season_year", dedup_key, sql, count=1)
    return sql


def _connect():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE fotmob_transfer_events_current (
            player_id BIGINT, player_name VARCHAR, transfer_date VARCHAR,
            from_club_id BIGINT, from_club_full_name VARCHAR,
            to_club_id BIGINT, to_club_full_name VARCHAR,
            position_label VARCHAR, position_key VARCHAR,
            fee_value DOUBLE, market_value DOUBLE, on_loan BOOLEAN,
            transfer_type_key VARCHAR, transfer_type_text VARCHAR,
            competition_id BIGINT, _observed_at TIMESTAMP, _target_batch_id VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE fotmob_transfers (
            player_id BIGINT, player_name VARCHAR, transfer_date VARCHAR,
            from_club_id BIGINT, from_club_full_name VARCHAR,
            to_club_id BIGINT, to_club_full_name VARCHAR,
            position_label VARCHAR, position_key VARCHAR,
            fee_value DOUBLE, market_value VARCHAR, on_loan BOOLEAN,
            transfer_type_key VARCHAR, transfer_type_text VARCHAR,
            league VARCHAR, _ingested_at TIMESTAMP, _batch_id VARCHAR
        )
        """
    )
    # один переход, два наблюдения одного дня с разными таймстемпами
    for stamp, batch in (
        ("2026-08-04T09:12:00Z", "b1"),
        ("2026-08-04T09:31:00Z", "b2"),
    ):
        con.execute(
            """
            INSERT INTO fotmob_transfer_events_current VALUES
            (874482, 'Test Player', ?, 100, 'From FC', 200, 'To FC',
             'Midfielder', 'midfielder', 1000000.0, 2000000.0, false,
             'transfer', 'Transfer', 57, TIMESTAMP '2026-08-04 10:00:00', ?)
            """,
            [stamp, batch],
        )
    return con


def test_fixture_reproduces_the_duplicate_on_the_old_raw_string_key():
    """Страховка от вакуумного теста: на прежнем ключе дубль обязан выживать."""
    con = _connect()
    rows = con.execute(_render(dedup_key=OLD_DEDUP_KEY)).fetchall()
    assert len(rows) == 2, (
        "фикстура не воспроизводит баг: на дедупе по сырой строке должны "
        f"выжить обе записи, выжило {len(rows)}"
    )


def test_current_key_collapses_same_day_timestamps_to_one_row():
    con = _connect()
    rows = con.execute(_render()).fetchall()
    assert len(rows) == 1, (
        f"дедуп по распарсенной дате должен оставить одну запись, осталось {len(rows)}"
    )


def test_dedup_key_is_pinned_to_the_parsed_date():
    """Ключ дедупа обязан совпадать с грейном DQ-правила no_duplicates (DATE)."""
    sql = TRANSFERS_SQL.read_text(encoding="utf-8")
    partition = re.search(r"PARTITION BY [^\n]*season_year", sql)
    assert partition is not None
    assert "transfer_date_parsed" in partition.group(0)
    assert not re.search(r"\btransfer_date\b(?!_parsed)", partition.group(0))
