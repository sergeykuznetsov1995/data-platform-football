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
import math
import threading
import time
from typing import Any, Optional

from scrapers.utils.rate_limiter import RateLimiter


logger = logging.getLogger(__name__)

SHARED_RATE_LIMIT_TABLE = "fotmob_shared_rate_limit"
FOTMOB_SHARED_RPM = 60.0
FOTMOB_SHARED_BURST = 4.0


class RefundableRateLimiter(RateLimiter):
    """Локальный token bucket, умеющий вернуть невостребованный токен.

    Возврат нужен, когда за локальным капом стоит второй барьер (общий потолок
    источника): без него токен, взятый локально и не пропущенный общим, сгорал
    бы, и локальный кап медленно душил бы процесс.

    Метод живёт здесь, а не в самом ``RateLimiter``:
    ``scrapers/utils/rate_limiter.py`` входит в пломбу
    ``scrapers/whoscored/runtime_contract.lock``, и любая правка этого файла
    требует перештамповки пломбы, то есть сдвигает согласованный
    ``lock_sha256`` чужого источника.
    """

    def refund(self) -> None:
        with self._lock:
            self._refill()
            self._tokens = min(self.config.burst_size, self._tokens + 1)


class PgSharedRateLimiter:
    """Token bucket на одну строку в Postgres: одна попытка — один запрос."""

    def __init__(
        self,
        dsn: str,
        *,
        key: str = "fotmob",
        rpm: float = FOTMOB_SHARED_RPM,
        burst: float = FOTMOB_SHARED_BURST,
        connection: Optional[Any] = None,
        connection_factory=None,
    ) -> None:
        self.key = key
        self.rpm = float(rpm)
        self.burst = float(burst)
        if not all(math.isfinite(value) and value > 0 for value in (self.rpm, self.burst)):
            raise ValueError("FotMob shared limits must be finite and positive")
        self._lock = threading.Lock()
        if connection_factory is None:
            def connection_factory():
                import psycopg2

                return psycopg2.connect(
                    dsn, connect_timeout=2,
                    options="-c statement_timeout=1000 -c lock_timeout=1000",
                    keepalives=1, keepalives_idle=1, keepalives_interval=1,
                    keepalives_count=3, tcp_user_timeout=2000,
                )
        self._connect = connection_factory
        self._connection = connection if connection is not None else self._connect()
        self._connection.autocommit = True
        self._bootstrap()

    def _query(self, sql, params=None, *, fetch=False):
        import psycopg2

        with self._lock:
            for attempt in range(2):
                try:
                    if self._connection is None:
                        self._connection = self._connect()
                        self._connection.autocommit = True
                    with self._connection.cursor() as cursor:
                        cursor.execute(sql, params)
                        return cursor.fetchone() if fetch else None
                except (psycopg2.OperationalError, psycopg2.InterfaceError):
                    if self._connection is not None:
                        try:
                            self._connection.close()
                        except Exception:
                            pass
                    self._connection = None
                    if attempt:
                        raise

    def _bootstrap(self) -> None:
        # Additive migration preserves the existing bucket and any active pause.
        self._query(f"""
            CREATE TABLE IF NOT EXISTS {SHARED_RATE_LIMIT_TABLE} (
                key TEXT PRIMARY KEY,
                tokens DOUBLE PRECISION NOT NULL,
                capacity DOUBLE PRECISION NOT NULL,
                rate_per_second DOUBLE PRECISION NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
        """)
        self._query(f"""
            ALTER TABLE {SHARED_RATE_LIMIT_TABLE}
                ADD COLUMN IF NOT EXISTS blocked_until TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS stop_reason TEXT,
                ADD COLUMN IF NOT EXISTS policy_version INTEGER
        """)
        self._query(f"""
            INSERT INTO {SHARED_RATE_LIMIT_TABLE}
                (key, tokens, capacity, rate_per_second, updated_at, policy_version)
            VALUES (%s, %s, %s, %s, clock_timestamp(), 1)
            ON CONFLICT (key) DO NOTHING
        """, (self.key, self.burst, self.burst, self.rpm / 60.0))
        if (self.rpm, self.burst) == (FOTMOB_SHARED_RPM, FOTMOB_SHARED_BURST):
            # Only legacy rows are migrated, once, to the source policy.
            # Keep tokens, refill timestamp and any active pause untouched.
            self._query(f"""
                UPDATE {SHARED_RATE_LIMIT_TABLE}
                   SET capacity = %s, rate_per_second = %s, policy_version = 1
                 WHERE key = %s AND policy_version IS NULL
            """, (self.burst, self.rpm / 60.0, self.key))
        policy = self._query(f"""
            SELECT capacity, rate_per_second FROM {SHARED_RATE_LIMIT_TABLE}
             WHERE key = %s
        """, (self.key,), fetch=True)
        if policy != (self.burst, self.rpm / 60.0):
            raise ValueError("FotMob shared rate policy mismatch")

    def pause(self, until, reason: str) -> None:
        self._query(f"""
            UPDATE {SHARED_RATE_LIMIT_TABLE}
               SET blocked_until = GREATEST(blocked_until, %s),
                   stop_reason = CASE WHEN stop_reason = 'ban'
                                       AND blocked_until > clock_timestamp()
                                      THEN 'ban' ELSE %s END
             WHERE key = %s
        """, (until, reason, self.key))

    def pause_state(self):
        return self._query(f"""
            SELECT blocked_until, stop_reason FROM {SHARED_RATE_LIMIT_TABLE}
             WHERE key = %s AND blocked_until > clock_timestamp()
        """, (self.key,), fetch=True)

    def try_take(self) -> bool:
        import psycopg2

        refill = (
            "LEAST(r.capacity, r.tokens + EXTRACT(EPOCH FROM "
            "(clock_timestamp() - r.updated_at)) * r.rate_per_second)"
        )
        try:
            return self._query(f"""
                UPDATE {SHARED_RATE_LIMIT_TABLE} r
                   SET tokens = {refill} - 1,
                       updated_at = clock_timestamp()
                 WHERE r.key = %s
                   AND (r.blocked_until IS NULL OR r.blocked_until <= clock_timestamp())
                   AND {refill} >= 1
                RETURNING tokens
            """, (self.key,), fetch=True) is not None
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            return False

    def close(self) -> None:
        if self._connection is not None:
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
        admitted = False
        try:
            admitted = self.shared.try_take()
            if admitted:
                return True
        finally:
            if not admitted:
                self.local.refund()
        return self._wait_out(started, timeout)

    def try_acquire(self) -> bool:
        if not self.local.try_acquire():
            return False
        admitted = False
        try:
            admitted = self.shared.try_take()
            if admitted:
                return True
        finally:
            if not admitted:
                self.local.refund()
        return False

    @property
    def available_tokens(self) -> float:
        return self.local.available_tokens
