"""Общий потолок запросов к FotMob (В1, #1242).

Пока волна контура и юнит кампании не могли работать одновременно, суммарная
нагрузка на источник равнялась потолку одного процесса. Теперь она держится
явно — одной строкой в контрольной БД.
"""

import time
from datetime import datetime, timedelta, timezone

import psycopg2

import pytest

from scrapers.fotmob.shared_rate_limiter import (
    SHARED_RATE_LIMIT_TABLE,
    CompositeRateLimiter,
    PgSharedRateLimiter,
    RefundableRateLimiter,
)


class _FakeCursor:
    def __init__(self, connection):
        self._connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, sql, params=None):
        sql = " ".join(sql.split())
        self._connection.executed.append((sql, params))
        self.sql = sql
        if self._connection.broken:
            raise psycopg2.OperationalError("connection lost")
        if sql.startswith("INSERT") and self._connection.policy is None:
            self._connection.policy = (params[2], params[3])
        if "SET blocked_until = GREATEST" in sql:
            until, reason, _key = params
            old = self._connection.pause
            self._connection.pause = (max(until, old[0]) if old else until,
                                      "ban" if old and old[1] == "ban" else reason)

    def fetchone(self):
        if self.sql.startswith("SELECT capacity"):
            return self._connection.policy
        if self.sql.startswith("SELECT blocked_until"):
            state = self._connection.pause
            return state if state and state[0] > datetime.now(timezone.utc) else None
        if self._connection.pause and self._connection.pause[0] > datetime.now(timezone.utc):
            return None
        return self._connection.rows.pop(0)


class _FakeConnection:
    def __init__(self, rows=None):
        self.policy = None
        self.pause = None
        self.broken = False
        self.autocommit = False
        self.closed = False
        self.executed = []
        self.rows = list(rows or [])

    def cursor(self):
        return _FakeCursor(self)

    def close(self):
        self.closed = True


def _limiter(rows=None, **kwargs):
    connection = _FakeConnection(rows)
    limiter = PgSharedRateLimiter(
        "postgresql://ignored", connection=connection, **kwargs
    )
    return limiter, connection


@pytest.mark.unit
def test_bootstrap_creates_the_row_and_carries_current_constants():
    limiter, connection = _limiter(rpm=60.0, burst=4.0)

    statements = [sql for sql, _params in connection.executed]
    assert len(statements) == 4
    assert statements[0].startswith(
        f"CREATE TABLE IF NOT EXISTS {SHARED_RATE_LIMIT_TABLE}"
    )
    insert, params = connection.executed[2]
    assert insert.startswith(f"INSERT INTO {SHARED_RATE_LIMIT_TABLE}")
    assert "ON CONFLICT (key) DO NOTHING" in insert
    assert "EXCLUDED.capacity" not in insert
    assert "ADD COLUMN IF NOT EXISTS blocked_until" in statements[1]
    assert "tokens =" not in statements[1]
    assert params == ("fotmob", 4.0, 4.0, 1.0)
    assert connection.autocommit is True
    assert limiter.key == "fotmob"


@pytest.mark.unit
def test_take_is_one_atomic_statement_and_reads_its_result():
    limiter, connection = _limiter(rows=[(3.0,), None])
    connection.executed.clear()

    assert limiter.try_take() is True
    assert limiter.try_take() is False

    statements = [sql for sql, _params in connection.executed]
    assert len(statements) == 2
    for sql, params in connection.executed:
        assert sql.startswith(f"UPDATE {SHARED_RATE_LIMIT_TABLE} r")
        assert "clock_timestamp()" in sql
        assert "RETURNING tokens" in sql
        assert params == ("fotmob",)


class _StubShared:
    def __init__(self, answers):
        self._answers = list(answers)
        self.calls = 0

    def try_take(self):
        self.calls += 1
        return self._answers.pop(0) if len(self._answers) > 1 else self._answers[0]


@pytest.mark.unit
def test_local_refusal_never_asks_the_database():
    local = RefundableRateLimiter(max_requests=1, window_seconds=600, burst_size=1)
    assert local.try_acquire() is True
    shared = _StubShared([True])
    slept = []
    composite = CompositeRateLimiter(local, shared, sleep=slept.append)

    assert composite.acquire(timeout=0.25) is False

    assert shared.calls == 0
    assert slept and slept[0] <= 0.25


@pytest.mark.unit
def test_shared_refusal_returns_the_local_token():
    local = RefundableRateLimiter(max_requests=60, window_seconds=600, burst_size=4)
    shared = _StubShared([False])
    slept = []
    composite = CompositeRateLimiter(local, shared, sleep=slept.append)
    before = local.available_tokens

    started = time.monotonic()
    assert composite.acquire(timeout=0.25) is False
    elapsed = time.monotonic() - started

    assert shared.calls == 1
    # Токен вернулся: без возврата локальный кап медленно душил бы процесс,
    # ведь после выката узкое место почти всегда общий потолок.
    assert local.available_tokens == pytest.approx(before, abs=1e-3)
    # Сон учтён в бюджете одного обращения — транспорт крутит цикл шагом 0,25 с
    # и между шагами проверяет отмену.
    assert slept and sum(slept) <= 0.25
    assert elapsed < 0.25


@pytest.mark.unit
def test_both_barriers_pass_gives_a_token():
    local = RefundableRateLimiter(max_requests=60, window_seconds=600, burst_size=4)
    shared = _StubShared([True])
    composite = CompositeRateLimiter(local, shared, sleep=lambda _delay: None)
    before = local.available_tokens

    assert composite.acquire(timeout=0.25) is True

    assert shared.calls == 1
    assert local.available_tokens == pytest.approx(before - 1, abs=1e-3)


@pytest.mark.unit
def test_refund_never_exceeds_the_burst():
    local = RefundableRateLimiter(max_requests=60, window_seconds=600, burst_size=2)

    local.refund()
    local.refund()

    assert local.available_tokens == pytest.approx(2.0, abs=1e-3)


@pytest.mark.unit
def test_sealed_base_limiter_is_untouched():
    """scrapers/utils/rate_limiter.py входит в пломбу WhoScored.

    Правка этого файла сдвигает согласованный lock_sha256 чужого источника и
    валит его профильный CI на КАЖДОМ PR, поэтому возврат токена живёт в
    подклассе, а не в самом RateLimiter.
    """

    from scrapers.utils.rate_limiter import RateLimiter

    assert not hasattr(RateLimiter, "refund")
    assert issubclass(RefundableRateLimiter, RateLimiter)


@pytest.mark.unit
def test_disabled_ceiling_leaves_a_bare_local_limiter(monkeypatch):
    import importlib
    import sys

    sys.modules.pop("dags.scripts.run_fotmob_scraper", None)
    mod = importlib.import_module("dags.scripts.run_fotmob_scraper")
    monkeypatch.setenv(mod.FOTMOB_SHARED_RPM_ENV, "0")
    local = RefundableRateLimiter(max_requests=45, window_seconds=60, burst_size=4)

    assert mod._with_shared_rpm_ceiling(local, workers=4) is local


@pytest.mark.unit
def test_default_ceiling_is_todays_source_load(monkeypatch):
    import importlib
    import sys

    sys.modules.pop("dags.scripts.run_fotmob_scraper", None)
    mod = importlib.import_module("dags.scripts.run_fotmob_scraper")
    monkeypatch.delenv(mod.FOTMOB_SHARED_RPM_ENV, raising=False)

    assert mod._shared_rpm_ceiling() == 60.0
    assert mod._shared_rpm_ceiling({mod.FOTMOB_SHARED_RPM_ENV: "45"}) == 45.0


@pytest.mark.unit
def test_ceiling_wraps_the_local_limiter_with_the_control_db(monkeypatch):
    import importlib
    import sys

    sys.modules.pop("dags.scripts.run_fotmob_scraper", None)
    mod = importlib.import_module("dags.scripts.run_fotmob_scraper")
    monkeypatch.delenv(mod.FOTMOB_SHARED_RPM_ENV, raising=False)
    monkeypatch.setattr(
        "scrapers.fbref.control.store.resolve_control_db_uri",
        lambda env=None: "postgresql://airflow@metadb:5432/airflow",
    )
    seen = {}

    class _Connection(_FakeConnection):
        def __init__(self):
            super().__init__()

    def connect(dsn, **kwargs):
        seen["dsn"] = dsn
        seen["options"] = kwargs
        return _Connection()

    monkeypatch.setattr(psycopg2, "connect", connect)
    local = RefundableRateLimiter(max_requests=60, window_seconds=60, burst_size=4)

    composite = mod._with_shared_rpm_ceiling(local, workers=4)

    assert isinstance(composite, CompositeRateLimiter)
    assert composite.local is local
    assert composite.shared.rpm == 60.0
    assert composite.shared.burst == 4.0
    assert seen["dsn"] == "postgresql://airflow@metadb:5432/airflow"
    assert seen["options"]["connect_timeout"] == 2
    assert seen["options"]["tcp_user_timeout"] == 2000
    assert "statement_timeout=1000" in seen["options"]["options"]
    assert "lock_timeout=1000" in seen["options"]["options"]


def test_shared_pause_blocks_another_client_and_never_shortens():
    first, connection = _limiter(rows=[(3.0,)])
    until = datetime.now(timezone.utc) + timedelta(hours=2)
    first.pause(until, "retry_after")
    second = PgSharedRateLimiter("ignored", connection=connection)
    second.pause(until - timedelta(hours=1), "retry_after")
    assert second.pause_state() == (until, "retry_after")
    assert second.try_take() is False
    sql = connection.executed[-1][0]
    assert "r.blocked_until <= clock_timestamp()" in sql
    assert connection.rows == [(3.0,)]


def test_reconnect_uses_existing_bucket_and_preserves_pause():
    limiter, broken = _limiter()
    replacement = _FakeConnection(rows=[(2.0,)])
    replacement.policy = broken.policy
    until = datetime.now(timezone.utc) + timedelta(minutes=15)
    replacement.pause = (until, "ban")
    limiter._connect = lambda: replacement
    broken.broken = True
    assert limiter.try_take() is False
    assert broken.closed
    assert limiter.pause_state() == (until, "ban")
    replacement.pause = None
    assert limiter.try_take() is True
    assert all(not sql.startswith("INSERT") for sql, _ in replacement.executed)


def test_metadb_outage_refunds_local_token_and_never_admits():
    limiter, connection = _limiter()
    connection.broken = True
    def unavailable():
        raise psycopg2.OperationalError("offline")
    limiter._connect = unavailable
    local = RefundableRateLimiter(max_requests=60, window_seconds=60, burst_size=4)
    composite = CompositeRateLimiter(local, limiter, sleep=lambda _: None)
    assert composite.acquire(timeout=0.01) is False
    assert local.available_tokens == pytest.approx(4.0)


def test_incompatible_policy_does_not_overwrite_bucket_or_pause():
    limiter, connection = _limiter(rows=[(0.5,)])
    until = datetime.now(timezone.utc) + timedelta(minutes=15)
    limiter.pause(until, "ban")
    with pytest.raises(ValueError, match="policy mismatch"):
        PgSharedRateLimiter("ignored", connection=connection, burst=8)
    assert connection.policy == (4.0, 1.0)
    assert connection.rows == [(0.5,)]
    assert connection.pause == (until, "ban")


@pytest.mark.parametrize("workers", [1, 4, 16])
def test_worker_count_does_not_change_shared_policy(monkeypatch, workers):
    from dags.scripts import run_fotmob_scraper as runner
    monkeypatch.delenv(runner.FOTMOB_SHARED_RPM_ENV, raising=False)
    monkeypatch.setattr("scrapers.fbref.control.store.resolve_control_db_uri", lambda env: "ignored")
    connection = _FakeConnection()
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **kw: connection)
    local = RefundableRateLimiter(max_requests=60, window_seconds=60, burst_size=workers)
    shared = runner._with_shared_rpm_ceiling(local, workers=workers).shared
    assert (shared.rpm, shared.burst) == (60.0, 4.0)
