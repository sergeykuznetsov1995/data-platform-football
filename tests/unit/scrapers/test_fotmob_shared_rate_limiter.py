"""Общий потолок запросов к FotMob (В1, #1242).

Пока волна контура и юнит кампании не могли работать одновременно, суммарная
нагрузка на источник равнялась потолку одного процесса. Теперь она держится
явно — одной строкой в контрольной БД.
"""

import time

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
        self._connection.executed.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self._connection.rows.pop(0)


class _FakeConnection:
    def __init__(self, rows=None):
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
    assert len(statements) == 2
    assert statements[0].startswith(
        f"CREATE TABLE IF NOT EXISTS {SHARED_RATE_LIMIT_TABLE}"
    )
    insert, params = connection.executed[1]
    assert insert.startswith(f"INSERT INTO {SHARED_RATE_LIMIT_TABLE}")
    # Без DO UPDATE первая созданная строка навсегда зафиксировала бы потолок,
    # и правка константы в коде ничего бы не меняла.
    assert "ON CONFLICT (key) DO UPDATE" in insert
    assert "capacity = EXCLUDED.capacity" in insert
    assert "rate_per_second = EXCLUDED.rate_per_second" in insert
    # Наполнение ведра при этом не трогается: иначе каждый старт процесса
    # дарил бы источнику полное ведро.
    assert "tokens = EXCLUDED.tokens" not in insert
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

    def connect(dsn):
        seen["dsn"] = dsn
        return _Connection()

    monkeypatch.setitem(
        sys.modules, "psycopg2", type(sys)("psycopg2")
    )
    sys.modules["psycopg2"].connect = connect
    local = RefundableRateLimiter(max_requests=60, window_seconds=60, burst_size=4)

    composite = mod._with_shared_rpm_ceiling(local, workers=4)

    assert isinstance(composite, CompositeRateLimiter)
    assert composite.local is local
    assert composite.shared.rpm == 60.0
    assert composite.shared.burst == 4.0
    assert seen["dsn"] == "postgresql://airflow@metadb:5432/airflow"
