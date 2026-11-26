"""Orchestration contract of ``dag_espn_current`` (#1504, wave log #1505).

The DAG file lives in ``deploy/espn/dags/`` (the espn-airflow DAG folder,
#1507), so it is imported by path under the Airflow stubs of this package.
"""

from __future__ import annotations

import importlib.util
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

DAG_FILE = Path(__file__).resolve().parents[3] / "deploy" / "espn" / "dags" / "dag_espn_current.py"


@pytest.fixture
def dag_module():
    from airflow.operators.python import PythonOperator

    PythonOperator._instances.clear()
    spec = importlib.util.spec_from_file_location("dag_espn_current_under_test", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _task(task_id: str):
    from airflow.operators.python import PythonOperator

    return next(task for task in PythonOperator._instances if task.task_id == task_id)


@pytest.mark.unit
def test_four_waves_a_day_paused_on_creation(dag_module) -> None:
    kwargs = dag_module.dag._dag_kwargs
    assert kwargs["dag_id"] == "dag_espn_current"
    assert kwargs["schedule"] == "0 0,6,12,18 * * *"
    assert kwargs["max_active_runs"] == 1
    assert kwargs["max_active_tasks"] == 3
    assert kwargs["catchup"] is False
    assert kwargs["is_paused_upon_creation"] is True
    assert kwargs["dagrun_timeout"] == timedelta(minutes=55)


@pytest.mark.unit
def test_topology_retries_and_three_concurrent_groups(dag_module) -> None:
    prepare, plan = _task("prepare"), _task("plan_wave")
    run, summary = _task("run_tournament"), _task("wave_summary")

    assert prepare.downstream_task_ids == {"plan_wave"}
    assert plan.downstream_task_ids == {"run_tournament"}
    assert run.downstream_task_ids == {"wave_summary"}
    for task in (prepare, plan, run):
        assert task._init_kwargs["retries"] == 2, task.task_id
    assert run.is_mapped is True
    assert run._expand_kwargs["op_kwargs"].operator is plan
    assert run._init_kwargs["retry_delay"] == timedelta(minutes=3)
    assert run._init_kwargs["max_active_tis_per_dag"] == 4
    assert run._init_kwargs["execution_timeout"] == timedelta(minutes=20)
    assert plan._init_kwargs["pool"] == run._init_kwargs["pool"] == "espn_live"
    assert summary._init_kwargs["trigger_rule"] == "all_done"
    assert summary._init_kwargs["retries"] == 0


class _TI:
    def __init__(self, task_id, state, map_index=-1, outcome=None):
        self.task_id, self.state, self.map_index, self.outcome = task_id, state, map_index, outcome

    def xcom_push(self, *, key, value):
        assert (self.task_id, key) == ("run_tournament", "outcome")
        self.outcome = value


class _SummaryTI:
    def __init__(self, instances):
        self.instances = instances

    def xcom_pull(self, *, task_ids, map_indexes=None, key="return_value"):
        assert (task_ids, key) in {("plan_wave", "return_value"), ("run_tournament", "outcome")}
        matches = [i for i in self.instances if i.task_id == task_ids and i.outcome is not None]
        if map_indexes is None and matches and matches[0].map_index >= 0:
            return [i.outcome for i in sorted(matches, key=lambda i: i.map_index)]
        index = -1 if map_indexes is None else map_indexes
        return next((i.outcome for i in matches if i.map_index == index), None)


def _outcome(slug, state="green", error=None):
    return {"slug": slug, "season_year": 2026, "state": state, "matches": 1,
            "dispositions": {}, "first_error": error}


def _context(instances):
    dag_run = SimpleNamespace(get_task_instances=lambda: instances, start_date=None)
    return {"dag_run": dag_run, "ti": _SummaryTI(instances), "run_id": "scheduled__x"}


class _LogConn:
    """Trino connection of the wave log: records the statements."""

    def __init__(self) -> None:
        self.sql: list[str] = []

    def cursor(self):
        conn = self

        class _Cursor:
            def execute(self, sql):
                conn.sql.append(sql)

            def fetchall(self):
                return []

            def close(self):
                pass

        return _Cursor()


@pytest.fixture(autouse=True)
def log_conn(dag_module, monkeypatch):
    conn = _LogConn()
    monkeypatch.setattr(dag_module, "_trino", lambda: SimpleNamespace(connection=conn))
    return conn


@pytest.mark.unit
def test_wave_summary_tolerates_one_red_tournament_in_five(dag_module) -> None:
    instances = [_TI("plan_wave", "success")] + [
        _TI("run_tournament", "success", i, _outcome(f"x.{i}")) for i in range(4)
    ] + [_TI("run_tournament", "failed", 4, _outcome("x.4", "red", "RuntimeError: boom"))]

    result = dag_module.wave_summary(**_context(instances))

    assert (result["tournaments"], result["red_tournaments"], result["red"]) == (5, 1, False)
    assert "x.4:2026 -> red -> RuntimeError: boom" in result["table"]


@pytest.mark.unit
@pytest.mark.parametrize("state", ["failed", "upstream_failed"])
def test_wave_summary_is_red_when_the_plan_failed(dag_module, state) -> None:
    from airflow.exceptions import AirflowException

    instances = [_TI("plan_wave", state), _TI("run_tournament", "upstream_failed", -1)]

    with pytest.raises(AirflowException, match=f"plan_wave: {state}"):
        dag_module.wave_summary(**_context(instances))


@pytest.mark.unit
def test_prepare_creates_the_wave_log_table(dag_module, log_conn, monkeypatch) -> None:
    from scrapers.espn import bronze_schema, trino_manager

    writer = object()
    ensured_writers = []
    monkeypatch.setattr(trino_manager, "EspnIcebergWriter", lambda: writer)
    monkeypatch.setattr(bronze_schema, "ensure_bronze_tables", ensured_writers.append)

    dag_module.prepare()

    assert ensured_writers == [writer]
    assert any(
        "CREATE TABLE IF NOT EXISTS iceberg.ops.espn_wave_tournament_v1" in sql
        for sql in log_conn.sql
    )


@pytest.mark.unit
def test_wave_summary_writes_the_log_before_a_red_wave_fails(dag_module, log_conn) -> None:
    from airflow.exceptions import AirflowException

    instances = [_TI("plan_wave", "success")] + [
        _TI("run_tournament", "failed", i, _outcome(f"x.{i}", "red", "RuntimeError: boom"))
        for i in range(2)
    ] + [_TI("run_tournament", "success", 2, _outcome("x.2"))]

    with pytest.raises(AirflowException, match="ESPN wave red"):
        dag_module.wave_summary(**_context(instances))

    (insert,) = [sql for sql in log_conn.sql if sql.startswith("INSERT")]
    assert insert.startswith("INSERT INTO iceberg.ops.espn_wave_tournament_v1 (run_id, ")
    assert "'scheduled__x'" in insert
    assert "'(wave)', NULL, 'red', 3, '2 of 3 tournaments red (> 20%)'" in insert
    assert "'x.0', 2026, 'red', 1, 'RuntimeError: boom'" in insert
    assert "'x.2', 2026, 'green', 1, NULL" in insert


@pytest.mark.unit
def test_wave_summary_writes_the_log_of_a_green_wave(dag_module, log_conn) -> None:
    instances = [_TI("plan_wave", "success"), _TI("run_tournament", "success", 0, _outcome("x.0"))]

    dag_module.wave_summary(**_context(instances))

    (insert,) = [sql for sql in log_conn.sql if sql.startswith("INSERT")]
    assert "'(wave)', NULL, 'green', 1, NULL" in insert


@pytest.mark.unit
def test_trino_is_the_espn_manager_without_dynamic_filtering(
    dag_module, monkeypatch
) -> None:
    """#1557: the tombstone MERGE needs a connection without dynamic filtering."""
    from scrapers.espn.trino_manager import EspnTrinoTableManager

    monkeypatch.undo()  # the autouse log_conn stub replaces _trino
    assert type(dag_module._trino()) is EspnTrinoTableManager


@pytest.fixture
def planned(dag_module, monkeypatch):
    """``plan_wave`` of the DAG with the wave logic stubbed: records its kwargs."""
    import scrapers.espn.denominator as denominator
    from scrapers.espn import editions_store, wave

    seen: dict = {}

    def plan_wave(**kwargs):
        seen.update(kwargs)
        return SimpleNamespace(works=(), out_of_season=("cup",))

    monkeypatch.setattr(wave, "plan_wave", plan_wave)
    monkeypatch.setattr(wave, "live_rows", lambda denominator: [])
    monkeypatch.setattr(wave, "finish_requests", lambda *args, **kwargs: None)
    monkeypatch.setattr(denominator, "load_denominator", lambda: None)
    monkeypatch.setattr(editions_store, "default_state_path", lambda: None)
    monkeypatch.setattr(dag_module, "_client", lambda: None)
    return seen


def _plan_context(**conf):
    return {"dag_run": SimpleNamespace(conf=conf), "run_id": "manual__x"}


def _work(slug, ids=(), year=2026, error=None):
    from scrapers.espn.wave import TournamentWork

    return TournamentWork(slug, year, tuple(ids), 1, slug, str(year),
                          date(year, 1, 1), date(year, 12, 31), (), error=error)


def _plan_members(dag_module, monkeypatch, members):
    from scrapers.espn import wave

    monkeypatch.setattr(wave, "plan_wave", lambda **kw: SimpleNamespace(works=iter(members)))
    return dag_module.plan_wave(**_plan_context())


@pytest.mark.unit
def test_plan_packs_all_editions_under_both_deadlines(dag_module, planned, monkeypatch):
    sizes = [41] + [4] * 58 + [2] + [0] * 18
    members = [_work(f"league.{i // 2}", range(size), 2025 + i % 2,
                     error="missing day" if i == 77 else None)
               for i, size in enumerate(sizes)]
    # A repeated id in an edition occupies one slot. The same ids in its peer season
    # still occupy their own slots.
    members[1] = _work(members[1].slug, [0, 1, 2, 3, 0], members[1].season_year)
    plan = _plan_members(dag_module, monkeypatch, members)
    assert plan == dag_module.plan_wave(**_plan_context())
    assert len(plan) == 7
    assert plan[0] == {"works": [members[0].to_xcom()]}
    flat = []
    total = 0
    for group in plan:
        assert set(group) == {"works"}
        rows = group["works"]
        weight = sum(len(set(row["event_ids"])) for row in rows)
        assert len(rows) <= 20 and (weight <= 40 or len(rows) == 1)
        flat.extend(rows)
        total += weight
    assert total == 275 and len(flat) == 78
    by_key = {(row["slug"], row["season_year"]): row for row in flat}
    assert by_key == {(w.slug, w.season_year): w.to_xcom() for w in members}


@pytest.mark.unit
def test_duplicate_plan_keys_fail_before_mapping(dag_module, planned, monkeypatch):
    from airflow.exceptions import AirflowFailException

    with pytest.raises(AirflowFailException, match="duplicate"):
        _plan_members(dag_module, monkeypatch, [_work("Eng.1"), _work("eng.1")])


@pytest.mark.unit
def test_grouped_call_and_summary_preserve_proved_siblings(dag_module, planned, monkeypatch):
    from airflow.exceptions import AirflowFailException

    members = [_work(f"x.{i}", error="missing day" if i == 4 else None) for i in range(5)]
    plan = _plan_members(dag_module, monkeypatch, members)
    task = _TI("run_tournament", "failed", 0)
    monkeypatch.setattr(dag_module, "_client", lambda: SimpleNamespace(flush=lambda: None))
    with pytest.raises(AirflowFailException, match="missing day"):
        dag_module.run_tournament(**plan[0], ti=task, run_id="scheduled__x")

    rows = task.outcome["outcomes"]
    assert [(row["slug"], row["state"]) for row in rows] == [
        (w.slug, "red" if w.error else "green") for w in members
    ]
    result = dag_module.wave_summary(**_context([_TI("plan_wave", "success", outcome=plan), task]))
    assert (result["tournaments"], result["red_tournaments"], result["red"]) == (5, 1, False)
    assert "missing day" in result["table"][-1]


@pytest.mark.unit
def test_new_singleton_passes_managers_and_mapped_identity(dag_module, planned, monkeypatch):
    from scrapers.espn import grouped_wave, wave

    member = _work("eng.1")
    kwargs, = _plan_members(dag_module, monkeypatch, [member])
    client = object()
    monkeypatch.setattr(dag_module, "_client", lambda: client)
    task = _TI("run_tournament", "success", 3)
    outcome = wave.TournamentOutcome(member.slug, member.season_year, wave.GREEN, 0, {})

    def run(members, **deps):
        assert members == [member]
        assert deps["conn"] is deps["trino"].connection
        assert deps["client"] is client
        assert deps["child_trino_factory"] is dag_module._trino
        assert deps["child_trino_factory"]() is not deps["trino"]
        assert (deps["run_id"], deps["task_id"]) == ("run", "run_tournament[3]")
        return [outcome]

    monkeypatch.setattr(grouped_wave, "run_tournament_group", run)
    result = dag_module.run_tournament(**kwargs, ti=task, run_id="run")
    assert result == task.outcome == {"outcomes": [outcome.as_dict()]}
    monkeypatch.setattr(wave, "run_tournament", lambda item, **deps: outcome)
    assert dag_module.run_tournament(work=member.to_xcom(), ti=task) == task.outcome == outcome.as_dict()


@pytest.mark.unit
def test_systemic_group_failure_reports_every_member(dag_module, monkeypatch):
    task = _TI("run_tournament", "failed", 2)

    def unavailable():
        raise OSError("primary unavailable")

    monkeypatch.setattr(dag_module, "_trino", unavailable)
    with pytest.raises(OSError, match="primary unavailable"):
        dag_module.run_tournament(works=[_work(s).to_xcom() for s in ("a", "b")], ti=task)
    assert [(o["slug"], o["state"], o["first_error"]) for o in task.outcome["outcomes"]] == [
        (s, "red", "OSError: primary unavailable") for s in ("a", "b")
    ]


@pytest.mark.unit
@pytest.mark.parametrize("state", ["failed", "skipped", "removed"])
def test_summary_recovers_missing_group_and_partial_xcom(dag_module, log_conn, state):
    from airflow.exceptions import AirflowException

    plan = [
        {"works": [_work(s).to_xcom() for s in ("a", "b")]},
        {"works": [_work(s).to_xcom() for s in ("c", "d", "e")]},
        {"work": _work("f").to_xcom()},
    ]
    instances = [
        _TI("plan_wave", "success", outcome=plan),
        _TI("run_tournament", state, 0),
        _TI("run_tournament", "failed", 1, {"outcomes": [
            _outcome("c"), _outcome("e"), _outcome("e"), _outcome("wrong"),
        ]}),
        _TI("run_tournament", "failed", 2, _outcome("f")),
    ]
    with pytest.raises(AirflowException, match="5 of 6 tournaments red"):
        dag_module.wave_summary(**_context(instances))
    insert, = [sql for sql in log_conn.sql if sql.startswith("INSERT")]
    for slug in ("a", "b"):
        assert f"'{slug}', 2026, 'red', 0, 'task {state}" in insert
    assert "'c', 2026, 'green', 1" in insert and "'wrong'" not in insert
    for slug in ("d", "e"):
        assert f"'{slug}', 2026, 'red', 0, 'task failed" in insert
    assert "'f', 2026, 'red', 1, 'task failed'" in insert


@pytest.mark.unit
def test_zero_map_placeholder_has_no_editions(dag_module):
    instances = [_TI("plan_wave", "success", outcome=[]), _TI("run_tournament", "skipped")]
    assert dag_module.wave_summary(**_context(instances))["tournaments"] == 0


@pytest.mark.unit
def test_missing_map_without_plan_keeps_visible_failure(dag_module):
    instances = [_TI("plan_wave", "success"), _TI("run_tournament", "failed", 0)]
    instances.extend(_TI("run_tournament", "success", i, _outcome(str(i))) for i in range(1, 5))
    result = dag_module.wave_summary(**_context(instances))
    assert result["red_tournaments"] == 1 and result["tournaments"] == 5
    assert any("run_tournament[0]=failed" in line for line in result["table"])


@pytest.mark.unit
def test_plan_wave_passes_the_catch_up_window_of_the_conf(dag_module, planned) -> None:
    """#1508: conf {"since", "until"} of a manual run reaches the wave plan."""
    from datetime import date

    dag_module.plan_wave(**_plan_context(since="2026-08-13", until="2026-08-15"))

    assert (planned["since"], planned["until"]) == (date(2026, 8, 13), date(2026, 8, 15))


@pytest.mark.unit
def test_plan_wave_without_conf_plans_the_usual_wave(dag_module, planned) -> None:
    dag_module.plan_wave(**_plan_context())
    assert (planned["since"], planned["until"]) == (None, None)
    planned.clear()
    from datetime import datetime, timezone
    dag_module.plan_wave(dag_run=SimpleNamespace(), run_id="scheduled__x",
                         data_interval_end=datetime(2026, 11, 25, 6, tzinfo=timezone.utc))
    assert (planned["since"], planned["until"]) == (None, None)


@pytest.mark.unit
def test_midnight_plan_records_which_targets_were_outside_the_calendar(dag_module, planned):
    from datetime import datetime, timezone
    from scrapers.espn.freshness_source import CORE_SCOPE_KEY

    pushed = {}
    boundary = datetime(2026, 11, 25, tzinfo=timezone.utc)
    task = SimpleNamespace(xcom_push=lambda *, key, value: pushed.update({key: value}))
    dag_module.plan_wave(run_id="scheduled__2026-11-24T18:00:00+00:00",
                         dag_run=SimpleNamespace(), ti=task, data_interval_end=boundary)
    assert planned["check_core"]
    assert pushed == {CORE_SCOPE_KEY: {"day": planned["now"].date().isoformat(),
                                       "out_of_season": ["cup"]}}


@pytest.mark.unit
@pytest.mark.parametrize(
    "conf",
    [
        {"since": "2026-08-13"},
        {"since": "2026-08-13", "until": "2026-08-27"},  # 15 days
        {"since": "2026-08-15", "until": "2026-08-13"},
        {"since": "2999-01-01", "until": "2999-01-02"},
        {"since": "13.08.2026", "until": "2026-08-15"},
        {"since": "", "until": ""},
        {"since": 0, "until": False},
        {"since": None, "until": "2026-08-15"},
    ],
)
def test_plan_wave_fails_without_retry_on_a_bad_window(dag_module, planned, conf) -> None:
    from airflow.exceptions import AirflowFailException

    with pytest.raises(AirflowFailException, match="catch-up window"):
        dag_module.plan_wave(**_plan_context(**conf))
    assert planned == {}


@pytest.mark.unit
def test_wave_row_of_a_catch_up_run_shows_its_window(dag_module, log_conn) -> None:
    instances = [_TI("plan_wave", "success"), _TI("run_tournament", "success", 0, _outcome("x.0"))]
    context = _context(instances)
    context["dag_run"].conf = {"since": "2026-08-13", "until": "2026-08-15"}

    dag_module.wave_summary(**context)

    (insert,) = [sql for sql in log_conn.sql if sql.startswith("INSERT")]
    assert "'(wave)', NULL, 'green', 1, 'window 2026-08-13..2026-08-15'" in insert
