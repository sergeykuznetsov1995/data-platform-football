"""Общая обвязка тестов deploy/sofascore (#1245).

Заглушка метабазы больше не отвечает файлами на подстроки SQL: она держит sqlite-базу со
схемой метабазы Airflow и ИСПОЛНЯЕТ запросы скрипта. Так тест проверяет сам SQL (выбор
отслеживаемого прогона, пять чисел, доказательство учёта), а не то, что в строке есть
нужные слова. Булевы запросы (`SELECT is_paused`, `is_active=true`) остаются на файлах:
sqlite отдаёт 1/0 вместо t/f, и подделка представления ничего бы не доказала.

Ломатель тупика исполняется ПО-НАСТОЯЩЕМУ: `docker exec … python -` кормит стабу текст
deploy/sofascore/drain_breaker.py, а тонкий шим модуля airflow кладёт ORM-вызовы на ту же
sqlite-базу. Последствия для планировщика (validate → upstream_failed, finalize, закрытие
прогона) стаб применяет из `$STATE/after_breaker.sql` — их считает планировщик, не ломатель.
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
import textwrap


SCHEMA = """
CREATE TABLE dag_run (dag_id TEXT, run_id TEXT, state TEXT, start_date TEXT);
CREATE TABLE task_instance (
  dag_id TEXT, run_id TEXT, task_id TEXT, map_index INTEGER,
  state TEXT, pool TEXT, try_number INTEGER, start_date TEXT
);
CREATE TABLE import_error (id INTEGER);
CREATE TABLE xcom (dag_id TEXT, run_id TEXT, task_id TEXT, map_index INTEGER, key TEXT, value TEXT);
"""

# psql -At: колонки через '|', NULL — пустая строка, командный тег не печатается.
SQLRUN = '''\
import sqlite3
import sys

db, sql = sys.argv[1], sys.argv[2]
script = len(sys.argv) > 3 and sys.argv[3] == "script"
con = sqlite3.connect(db)
# В бою xcom.value — bytea, и скрипт читает его через convert_from(value,'UTF8').
# В стенде значение лежит текстом, функция — тождественная.
con.create_function("convert_from", 2, lambda v, enc: v if v is None or isinstance(v, str) else bytes(v).decode(enc))
try:
    if script:
        con.executescript(sql)
        rows = []
    else:
        rows = con.execute(sql).fetchall()
except sqlite3.Error as exc:
    sys.stderr.write("sqlite: %s\\nSQL: %s\\n" % (exc, sql))
    sys.exit(1)
con.commit()
for row in rows:
    print("|".join("" if value is None else str(value) for value in row))
'''

# Шим ORM: ровно то, чем пользуется drain_breaker.py, поверх тех же sqlite-таблиц.
# with_row_locks на sqlite — no-op (SELECT … FOR UPDATE там нет): гонку блокировок этот
# стенд не воспроизводит, её проверяет разовый прогон на PostgreSQL (см. PR #1245).
BREAKER_HOST = '''\
import sqlite3
import sys
import types

DB = sys.argv[1]
CON = sqlite3.connect(DB)


class _Col:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return (self.name, "=", other)

    def __ge__(self, other):
        return (self.name, ">=", other)


class _Row:
    def __init__(self, table, keys, data):
        self._table = table
        self._keys = keys
        self.__dict__.update(data)

    def set_state(self, state, session=None):
        where = " AND ".join("%s = ?" % k for k in self._keys)
        CON.execute(
            "UPDATE %s SET state = ? WHERE %s" % (self._table, where),
            [state] + [getattr(self, k) for k in self._keys],
        )
        self.state = state
        return True


class _Query:
    def __init__(self, model, conds=()):
        self.model, self.conds = model, conds

    def filter(self, *conds):
        return _Query(self.model, tuple(self.conds) + tuple(conds))

    def _rows(self):
        where = " AND ".join("%s %s ?" % (n, op) for n, op, _ in self.conds)
        sql = "SELECT * FROM %s" % self.model.__table__
        if where:
            sql += " WHERE " + where
        cur = CON.execute(sql, [v for _, _, v in self.conds])
        cols = [d[0] for d in cur.description]
        return [_Row(self.model.__table__, self.model.__keys__, dict(zip(cols, r))) for r in cur.fetchall()]

    def one_or_none(self):
        rows = self._rows()
        return rows[0] if rows else None

    def all(self):
        return self._rows()


class _Session:
    def query(self, model):
        return _Query(model)

    def commit(self):
        CON.commit()

    def close(self):
        CON.close()


class DagRun:
    __table__ = "dag_run"
    __keys__ = ("dag_id", "run_id")
    dag_id, run_id = _Col("dag_id"), _Col("run_id")


class TaskInstance:
    __table__ = "task_instance"
    __keys__ = ("dag_id", "run_id", "task_id", "map_index")
    dag_id, run_id = _Col("dag_id"), _Col("run_id")
    task_id, map_index = _Col("task_id"), _Col("map_index")


airflow = types.ModuleType("airflow")
settings = types.ModuleType("airflow.settings")
settings.Session = _Session
models = types.ModuleType("airflow.models")
models.DagRun, models.TaskInstance = DagRun, TaskInstance
utils = types.ModuleType("airflow.utils")
sa = types.ModuleType("airflow.utils.sqlalchemy")
sa.with_row_locks = lambda query, session=None, **kwargs: query
airflow.settings, airflow.models, airflow.utils = settings, models, utils
utils.sqlalchemy = sa
sys.modules.update({
    "airflow": airflow, "airflow.settings": settings, "airflow.models": models,
    "airflow.utils": utils, "airflow.utils.sqlalchemy": sa,
})

source = sys.stdin.read()
sys.argv = ["-"] + sys.argv[2:]
exec(compile(source, "drain_breaker.py", "exec"), {"__name__": "__main__", "__builtins__": __builtins__})
'''


def write_metadb_stub(state_dir: Path) -> None:
    """Кладёт sqlite-базу со схемой метабазы и два помощника заглушки docker."""
    (state_dir / "sqlrun.py").write_text(SQLRUN, encoding="utf-8")
    (state_dir / "breaker_host.py").write_text(BREAKER_HOST, encoding="utf-8")
    con = sqlite3.connect(state_dir / "metadb.sqlite")
    con.executescript(SCHEMA)
    con.commit()
    con.close()


def seed(state_dir: Path, sql: str) -> None:
    con = sqlite3.connect(state_dir / "metadb.sqlite")
    con.executescript(textwrap.dedent(sql))
    con.commit()
    con.close()


def rows(state_dir: Path, sql: str) -> list[tuple]:
    con = sqlite3.connect(state_dir / "metadb.sqlite")
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


HIST = "dag_backfill_sofascore_all_mens"


def world(
    state_dir: Path,
    *,
    run_id: str = "scheduled__2026-09-05T02:15:41.446584+00:00",
    scope_state: str | None = "up_for_retry",
    scope_pool: str = "sofascore_history_pool",
    scope_try: int = 1,
    scope_map: int = 0,
    dag_run_state: str = "running",
    validate_state: str | None = None,
    validate_map: int = -1,
    finalize_state: str | None = None,
    propagate_state: str | None = None,
    plan_kind: str | None = "capture",
    extra_scope: tuple[int, str | None, int] | None = None,
) -> str:
    """Срез прогона истории. По умолчанию — ночь 05.09: скоуп упал, повтор ждёт слота.

    ``scope_state=None`` — NULL-состояние (после ручного clear или сброса orphan TI).
    ``extra_scope`` — второй mapped-скоуп (batch>1), кортеж (map_index, state, try_number).
    """
    plan = ""
    if plan_kind == "capture":
        plan = (
            '[{"SOFASCORE_CAMPAIGN_ACTION": "capture", '
            '"SOFASCORE_EXPECTED_CAMPAIGN_ID": "camp1", '
            '"SOFASCORE_SCOPE_KEY": "camp1:937:78750"}]'
        )
    elif plan_kind == "metadata":
        plan = (
            '[{"SOFASCORE_CAMPAIGN_ACTION": "metadata", '
            '"SOFASCORE_EXPECTED_CAMPAIGN_ID": "camp1", '
            '"SOFASCORE_METADATA_WAVE": "2024"}]'
        )
    con = sqlite3.connect(state_dir / "metadb.sqlite")
    con.execute(
        "INSERT INTO dag_run (dag_id, run_id, state, start_date) VALUES (?,?,?,?)",
        (HIST, run_id, dag_run_state, "2026-09-05T03:27:55"),
    )
    con.execute(
        "INSERT INTO task_instance (dag_id, run_id, task_id, map_index, state, pool, try_number, start_date)"
        " VALUES (?,?,?,?,?,?,?,?)",
        (HIST, run_id, "run_historical_scope", scope_map, scope_state, scope_pool, scope_try, "2026-09-05T03:28:20"),
    )
    if extra_scope is not None:
        con.execute(
            "INSERT INTO task_instance (dag_id, run_id, task_id, map_index, state, pool, try_number, start_date)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (HIST, run_id, "run_historical_scope", extra_scope[0], extra_scope[1],
             scope_pool, extra_scope[2], "2026-09-05T03:28:20"),
        )
    con.execute(
        "INSERT INTO task_instance (dag_id, run_id, task_id, map_index, state, pool, try_number, start_date)"
        " VALUES (?,?,?,?,?,?,?,?)",
        (HIST, run_id, "validate_historical_scope", validate_map, validate_state, "default_pool", 0, None),
    )
    for task_id, state in (
        ("finalize_historical_run", finalize_state),
        ("propagate_historical_status", propagate_state),
    ):
        con.execute(
            "INSERT INTO task_instance (dag_id, run_id, task_id, map_index, state, pool, try_number, start_date)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (HIST, run_id, task_id, -1, state, "default_pool", 0, None),
        )
    if plan:
        con.execute(
            "INSERT INTO xcom (dag_id, run_id, task_id, map_index, key, value) VALUES (?,?,?,?,?,?)",
            (HIST, run_id, "plan_historical_batch", -1, "return_value", plan),
        )
    con.commit()
    con.close()
    return run_id
