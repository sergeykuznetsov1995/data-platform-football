"""Изоляция скоупов батча против НАСТОЯЩЕГО Airflow 2.11.2 (#1248 ступень 1).

Самая дорогая мина ступени: `validate_historical_scope` раскладывается по тому же плану,
что и `run_historical_scope`, но живёт ВНЕ mapped-группы. С дефолтным `all_success` один
упавший `run[1]` делал `upstream_failed` ВСЕ три validate, и `finalize` записывал в
failures.json два оплаченных и успешно собранных скоупа. Юнит-тест видит только
`trigger_rule` у оператора; здесь настоящие раскрытие карт, `DagRun.update_state()`,
validate и finalize.

Запуск (pytest в venv Airflow нет):
    /tmp/whoscored-ci-airflow-2.11.2.6mn32H/bin/python -m unittest \\
        tests.airflow_real.test_sofascore_batch_isolation -v
"""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from tests.airflow_real.test_sofascore_drain_breaker import (
    POOL,
    ROOT,
    _bind_orm,
    _campaign_files,
)



class BatchIsolationOnRealAirflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = Path(tempfile.mkdtemp(prefix="sofascore-batch-isolation-"))
        campaign = cls.tmp / "all-men"
        campaign.mkdir()
        (campaign / "results").mkdir()
        snapshot, policy = _campaign_files(campaign, seasons=3)
        cls.state_path = campaign / "state.json"
        cls.failures_path = campaign / "failures.json"
        cls.env = {
            **os.environ,
            "AIRFLOW_HOME": str(cls.tmp / "airflow"),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__UNIT_TEST_MODE": "False",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{cls.tmp}/airflow.db",
            "AIRFLOW__CORE__DAGS_FOLDER": str(ROOT / "dags"),
            "PYTHONPATH": f"{ROOT}:{ROOT / 'dags'}",
            "SOFASCORE_ALL_MENS_SNAPSHOT": str(snapshot),
            "SOFASCORE_ALL_MENS_POLICY": str(policy),
            "SOFASCORE_ALL_MENS_STATE": str(cls.state_path),
            "SOFASCORE_ALL_MENS_RESULT_DIR": str(campaign / "results"),
            "SOFASCORE_PROXY_BUDGET_ARTIFACT": str(
                ROOT / "configs" / "sofascore" / "workload_policy.json"
            ),
            "SOFASCORE_HISTORY_POOL": POOL,
            # Ровно боевая ступень 1: три скоупа в прогоне, три задачи разом.
            "SOFASCORE_HISTORY_BATCH_SIZE": "3",
            "SOFASCORE_HISTORY_MAX_ACTIVE_TASKS": "3",
        }
        os.environ.update(cls.env)
        (cls.tmp / "airflow").mkdir()
        _bind_orm(cls.env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"])
        subprocess.run(
            [str(Path(sys.executable).with_name("airflow")), "db", "migrate"],
            env=cls.env, check=True, capture_output=True,
        )
        spec = importlib.util.spec_from_file_location(
            "dag_backfill_sofascore_all_mens_batch",
            ROOT / "dags" / "dag_backfill_sofascore_all_mens.py",
        )
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)
        cls.dag = cls.module.dag

        from airflow.models.pool import Pool
        from airflow.utils.session import create_session

        with create_session() as session:
            session.add(Pool(pool=POOL, slots=3, description="history", include_deferred=False))
        cls.dag.sync_to_db()

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _advance(self, dag_run) -> None:
        from airflow.utils.session import create_session

        with create_session() as session:
            dag_run.dag = self.dag
            schedulable, _ = dag_run.update_state(session=session, execute_callbacks=False)
            if schedulable:
                dag_run.schedule_tis(schedulable, session=session)
            session.commit()
        dag_run.refresh_from_db()

    def _run_task(self, dag_run, task_id: str, map_index: int = -1) -> None:
        ti = dag_run.get_task_instance(task_id, map_index=map_index)
        ti.refresh_from_task(self.dag.get_task(task_id))
        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=False)

    def _state(self, dag_run, task_id: str, map_index: int = -1):
        ti = dag_run.get_task_instance(task_id, map_index=map_index)
        return None if ti is None else ti.state

    def test_a_failed_scope_does_not_cancel_the_accounting_of_its_neighbours(self) -> None:
        from airflow.utils.state import DagRunState
        from airflow.utils.types import DagRunType
        from datetime import datetime, timedelta, timezone
        from airflow.utils.session import create_session

        start = datetime(2026, 9, 5, 3, 27, tzinfo=timezone.utc)
        dag_run = self.dag.create_dagrun(
            run_id="real-batch-isolation",
            execution_date=start,
            data_interval=(start, start + timedelta(minutes=1)),
            start_date=start,
            state=DagRunState.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )
        self._run_task(dag_run, "plan_historical_batch")
        self._advance(dag_run)
        planned = dag_run.get_task_instance("plan_historical_batch").xcom_pull(
            task_ids="plan_historical_batch"
        )
        self.assertEqual(len(planned), 3, planned)

        # Соседи собрались и оплачены, средний скоуп упал и результата не оставил.
        for index in (0, 2):
            result = Path(planned[index]["SOFASCORE_SCOPE_RESULT_PATH"])
            result.parent.mkdir(parents=True, exist_ok=True)
            result.write_text(json.dumps({
                "campaign_id": planned[index]["SOFASCORE_EXPECTED_CAMPAIGN_ID"],
                "snapshot_id": planned[index]["SOFASCORE_EXPECTED_SNAPSHOT_ID"],
                "tournament_id": int(planned[index]["SOFASCORE_TOURNAMENT_ID"]),
                "source_season_id": int(planned[index]["SOFASCORE_SOURCE_SEASON_ID"]),
                "status": "success",
            }), encoding="utf-8")
        with create_session() as session:
            for index, scope_state in ((0, "success"), (1, "failed"), (2, "success")):
                ti = dag_run.get_task_instance(
                    "run_historical_scope", map_index=index, session=session
                )
                ti.state = scope_state
                ti.pool = POOL
                session.merge(ti)
            session.commit()
        self._advance(dag_run)

        # Ни один validate не погашен соседом: планировщик поставил в очередь все три,
        # включая соседей упавшего скоупа (с all_success их бы не было вовсе).
        for index in (0, 1, 2):
            self.assertEqual(
                self._state(dag_run, "validate_historical_scope", index), "scheduled", index
            )
        for index in (0, 2):
            self._run_task(dag_run, "validate_historical_scope", map_index=index)
        with self.assertRaises(Exception):
            self._run_task(dag_run, "validate_historical_scope", map_index=1)
        self._advance(dag_run)

        completed = json.loads(self.state_path.read_text(encoding="utf-8"))["completed"]
        self.assertEqual(
            sorted(completed),
            sorted(planned[index]["SOFASCORE_SCOPE_KEY"] for index in (0, 2)),
            completed,
        )

        self._run_task(dag_run, "finalize_historical_run")
        failures = json.loads(self.failures_path.read_text(encoding="utf-8"))
        self.assertEqual(
            list(failures["attempts"]), [planned[1]["SOFASCORE_SCOPE_KEY"]], failures
        )
