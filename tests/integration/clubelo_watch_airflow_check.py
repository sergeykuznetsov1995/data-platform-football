"""Explicit offline check with real Airflow and a disposable SQLite metabase.

Run as a script in an isolated container, never in production. No source,
Telegram or warehouse requests; task states are supplied to exercise DagRun
leaf-state evaluation and real ShortCircuit skip propagation.
"""
from datetime import timedelta

from airflow.models import DagBag
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils import db, timezone
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType


def main():
    db.initdb()
    bag = DagBag(dag_folder='/workspace/dags/dag_ingest_clubelo.py', include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags['dag_ingest_clubelo']
    assert {task.task_id for task in dag.leaves} == {'validate_data', 'scrape_history', 'finish_watch'}
    monday = timezone.datetime(2026, 10, 12, 0, 30)
    gate = dag.get_task('gate_watch')
    assert gate.python_callable(dag_run=type('Run', (), {'run_type': DagRunType.SCHEDULED})(),
                                data_interval_end=monday, params={})
    assert not gate.python_callable(dag_run=type('Run', (), {'run_type': DagRunType.MANUAL})(),
                                    data_interval_end=monday, params={})
    # A failed/timed-out optional check is softened by its report leaf;
    # a failed daily remains failed through the independent validate leaf.
    for index, (validate_state, expected) in enumerate([
        (TaskInstanceState.SUCCESS, DagRunState.SUCCESS),
        (TaskInstanceState.FAILED, DagRunState.FAILED),
        (TaskInstanceState.UPSTREAM_FAILED, DagRunState.FAILED),
    ]):
        logical_date = monday + timedelta(days=index)
        with create_session() as session:
            run = dag.create_dagrun(
                run_id=f'manual__clubelo_watch_check_{index}', execution_date=logical_date,
                data_interval=(logical_date - timedelta(hours=4), logical_date),
                start_date=timezone.utcnow(), state=DagRunState.RUNNING,
                run_type=DagRunType.MANUAL, external_trigger=True, session=session,
            )
            for ti in run.get_task_instances(session=session):
                state = TaskInstanceState.SUCCESS
                if ti.task_id in ('gate_history', 'scrape_history'):
                    state = TaskInstanceState.SKIPPED
                if ti.task_id == 'check_watch':
                    state = TaskInstanceState.FAILED
                if ti.task_id == 'validate_data':
                    state = validate_state
                ti.set_state(state, session=session)
            run.update_state(session=session)
            assert run.state == expected, (validate_state, run.state, expected)
            report = dag.get_task('finish_watch').python_callable('/tmp/nonexistent_watch.json', dag_run=run)
            assert report['status'] == 'error'
    # Real ShortCircuitOperator should skip only the optional check, allowing
    # finish_watch(all_done) while leaving all daily/history tasks untouched.
    with create_session() as session:
        date = monday + timedelta(days=4)
        run = dag.create_dagrun(
            run_id='manual__clubelo_watch_skip_check', execution_date=date,
            data_interval=(date - timedelta(hours=4), date), start_date=timezone.utcnow(),
            state=DagRunState.RUNNING, run_type=DagRunType.MANUAL, external_trigger=True,
            session=session,
        )
        session.commit()
        # TaskInstance uses an execution copy: without it op_kwargs/context
        # are mistaken for template-time dependencies and recurse into task.
        gate = gate.prepare_for_execution()
        gate.execute({'dag_run': run, 'params': {}, 'data_interval_end': date,
                      'task': gate, 'ti': run.get_task_instance('gate_watch', session=session)})
        session.expire_all()
        states = {ti.task_id: ti.state for ti in run.get_task_instances(session=session)}
        assert states['check_watch'] == TaskInstanceState.SKIPPED
        assert all(state is None for task, state in states.items() if task != 'check_watch'), states
    # Real trigger-rule evaluation: failed (including blocked) daily forbids
    # starting the optional source transport. Success permits it.
    for index, daily_state in enumerate((TaskInstanceState.FAILED, TaskInstanceState.SUCCESS)):
        with create_session() as session:
            date = monday + timedelta(days=5 + index)
            run = dag.create_dagrun(
                run_id=f'manual__clubelo_watch_dependency_{index}', execution_date=date,
                data_interval=(date - timedelta(hours=4), date), start_date=timezone.utcnow(),
                state=DagRunState.RUNNING, run_type=DagRunType.MANUAL, external_trigger=True,
                session=session,
            )
            run.get_task_instance('gate_watch', session=session).set_state(TaskInstanceState.SUCCESS, session=session)
            run.get_task_instance('scrape_daily', session=session).set_state(daily_state, session=session)
            # Airflow sessions disable autoflush; the dependency queries use
            # a SQL state filter, so fixture states must be flushed first.
            session.flush()
            ti = run.get_task_instance('check_watch', session=session)
            ti.task = dag.get_task('check_watch')
            statuses = list(TriggerRuleDep().get_dep_statuses(
                ti=ti, session=session, dep_context=DepContext(flag_upstream_failed=True)))
            assert all(status.passed for status in statuses) == (daily_state == TaskInstanceState.SUCCESS), statuses
    print('ClubElo real Airflow: import, timetable gate, skip propagation, daily-failure guard and 3 DagRun states passed')


if __name__ == '__main__':
    main()
