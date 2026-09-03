"""Общий на весь источник потолок запросов FotMob.

``RateLimiter`` — token bucket в памяти процесса, поэтому суммарная нагрузка на
источник равнялась потолку одного процесса лишь потому, что замок писателя
исключал одновременную работу волны контура и юнита кампании. Как только они
работают параллельно, потолок обязан стать общим — иначе к источнику уходит
сумма локальных капов.

Общее состояние живёт в контрольной БД (та же метабаза изолята, что держит
замок писателя): одна строка на источник, один атомарный ``UPDATE`` на попытку.
Локальный лимитер при этом сохраняется и остаётся первым барьером — он режет
99 % попыток без единого обращения к базе.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional


logger = logging.getLogger(__name__)

SHARED_RATE_LIMIT_TABLE = "fotmob_shared_rate_limit"


class PgSharedRateLimiter:
    """Token bucket на одну строку в Postgres: одна попытка — один запрос."""

    def __init__(
        self,
        dsn: str,
        *,
        key: str = "fotmob",
        rpm: float = 60.0,
        burst: float = 4.0,
        connection: Optional[Any] = None,
    ) -> None:
        self.key = key
        self.rpm = float(rpm)
        self.burst = float(burst)
        self._lock = threading.Lock()
        if connection is None:
            import psycopg2

            connection = psycopg2.connect(dsn)
        self._connection = connection
        self._connection.autocommit = True
        self._bootstrap()

    def _bootstrap(self) -> None:
        """Создать строку потолка и подтянуть в неё текущие константы кода.

        ``DO UPDATE`` обязателен: без него первая созданная строка навсегда
        зафиксировала бы потолок, и правка константы в коде ничего бы не
        меняла. Наполнение (``tokens``/``updated_at``) при этом не трогается —
        иначе каждый старт процесса дарил бы источнику полное ведро.
        """

        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SHARED_RATE_LIMIT_TABLE} (
                    key TEXT PRIMARY KEY,
                    tokens DOUBLE PRECISION NOT NULL,
                    capacity DOUBLE PRECISION NOT NULL,
                    rate_per_second DOUBLE PRECISION NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            cursor.execute(
                f"""
                INSERT INTO {SHARED_RATE_LIMIT_TABLE}
                    (key, tokens, capacity, rate_per_second, updated_at)
                VALUES (%s, %s, %s, %s, clock_timestamp())
                ON CONFLICT (key) DO UPDATE
                    SET capacity = EXCLUDED.capacity,
                        rate_per_second = EXCLUDED.rate_per_second
                """,
                (self.key, self.burst, self.burst, self.rpm / 60.0),
            )

    def try_take(self) -> bool:
        """Взять один токен, если он есть. Без сна и без второго запроса."""

        refill = (
            "LEAST(r.capacity, r.tokens + EXTRACT(EPOCH FROM "
            "(clock_timestamp() - r.updated_at)) * r.rate_per_second)"
        )
        with self._lock, self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE {SHARED_RATE_LIMIT_TABLE} r
                   SET tokens = {refill} - 1,
                       updated_at = clock_timestamp()
                 WHERE r.key = %s
                   AND {refill} >= 1
                RETURNING tokens
                """,
                (self.key,),
            )
            return cursor.fetchone() is not None

    def close(self) -> None:
        self._connection.close()


class CompositeRateLimiter:
    """Локальный кап процесса И общий потолок источника — оба обязаны дать токен.

    Порядок «локальный → общий → возврат» держит два свойства: база не видит
    попыток, зарубленных локальным капом, и ни один токен не сгорает впустую —
    после выката узкое место почти всегда общий потолок, и без возврата
    локальные токены терялись бы штатно.
    """

    def __init__(self, local, shared, *, sleep=time.sleep) -> None:
        self.local = local
        self.shared = shared
        self._sleep = sleep

    def _wait_out(self, started: float, timeout: Optional[float]) -> bool:
        # Транспорт крутит цикл шагом 0,25 с и между шагами проверяет отмену,
        # поэтому одно обращение обязано укладываться в свой timeout.
        if timeout is not None:
            remaining = timeout - (time.monotonic() - started)
            if remaining > 0:
                self._sleep(remaining)
        return False

    def acquire(self, timeout: Optional[float] = None) -> bool:
        started = time.monotonic()
        if not self.local.try_acquire():
            return self._wait_out(started, timeout)
        if self.shared.try_take():
            return True
        self.local.refund()
        return self._wait_out(started, timeout)

    def try_acquire(self) -> bool:
        if not self.local.try_acquire():
            return False
        if self.shared.try_take():
            return True
        self.local.refund()
        return False

    @property
    def available_tokens(self) -> float:
        return self.local.available_tokens
