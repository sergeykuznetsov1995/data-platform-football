"""Players accounting with real Airflow, SQLite and synthetic scope results.

Run with an Airflow Python: python -m unittest
tests.airflow_real.test_sofascore_players_lane -v
Only the planner output and capture task states are supplied by the test.
Mapping, scheduling, validation, failure accounting and the watcher are real.
No capture command, browser, source, Trino or S3 client is executed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
DAG_ID = "dag_players_sofascore_all_mens"
POOL = "sofascore_players_pool"


class PlayersLaneOnRealAirflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.directory = tempfile.TemporaryDirectory(prefix="sofascore-players-real-")
        cls.addClassCleanup(cls.directory.cleanup)
        cls.tmp = Path(cls.directory.name)
        cls.state_path = cls.tmp / "players-state.json"
        cls.failures_path = cls.tmp / "players-failures.json"
        cls.history_path = cls.tmp / "state.json"
        cls.history_path.write_text('{"synthetic_history": true}', encoding="utf-8")
        cls.history_bytes = cls.history_path.read_bytes()
        cls.run_number = 0
        environment = {
            key: value for key, value in os.environ.items()
            if not key.startswith("SOFASCORE_")
        }
        environment.update({
            "AIRFLOW_HOME": str(cls.tmp / "airflow"),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__UNIT_TEST_MODE": "False",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{cls.tmp}/airflow.db",
            "AIRFLOW__CORE__DAGS_FOLDER": str(cls.tmp / "empty-dags"),
            "AIRFLOW__CORE__PLUGINS_FOLDER": str(cls.tmp / "empty-plugins"),
            "PYTHONPATH": os.pathsep.join((
                str(ROOT), str(ROOT / "dags"), os.environ.get("PYTHONPATH", ""),
            )),
            "SOFASCORE_ALL_MENS_SNAPSHOT": str(cls.tmp / "snapshot.json"),
            "SOFASCORE_ALL_MENS_POLICY": str(cls.tmp / "policy.json"),
            "SOFASCORE_ALL_MENS_STATE": str(cls.history_path),
            "SOFASCORE_PLAYERS_STATE": str(cls.state_path),
            "SOFASCORE_PLAYERS_RESULT_DIR": str(cls.tmp / "results"),
            "SOFASCORE_PROXY_BUDGET_ARTIFACT": str(cls.tmp / "budget.json"),
            "SOFASCORE_PLAYERS_BATCH_SIZE": "3",
            "SOFASCORE_PLAYERS_MAX_ACTIVE_TASKS": "1",
        })
        cls.env_patch = patch.dict(os.environ, environment, clear=True)
        cls.env_patch.start()
        cls.addClassCleanup(cls.env_patch.stop)
        original_path = sys.path[:]
        cls.addClassCleanup(lambda: sys.path.__setitem__(slice(None), original_path))
        sys.path[:0] = [str(ROOT), str(ROOT / "dags")]
        (cls.tmp / "airflow").mkdir()
        (cls.tmp / "empty-dags").mkdir()
        (cls.tmp / "empty-plugins").mkdir()

        from airflow import settings
        from airflow.configuration import conf

        if not conf.has_section("database"):
            conf.add_section("database")
        conn = environment["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
        conf.set("database", "sql_alchemy_conn", conn)
        settings.SQL_ALCHEMY_CONN = conn
        settings.configure_orm()
        cls.addClassCleanup(settings.dispose_orm)
        subprocess.run(
            [sys.executable, "-m", "airflow", "db", "migrate"],
            env=environment, check=True, capture_output=True,
        )

        from airflow.models import DagBag
        from airflow.models.pool import Pool
        from airflow.utils.session import create_session

        cls.bag = DagBag(
            dag_folder=str(ROOT / "dags" / f"{DAG_ID}.py"),
            include_examples=False, safe_mode=False,
        )
        if cls.bag.import_errors or set(cls.bag.dag_ids) != {DAG_ID}:
            raise AssertionError((cls.bag.import_errors, cls.bag.dag_ids))
        cls.dag = cls.bag.dags[DAG_ID]
        with create_session() as session:
            session.add(Pool(
                pool=POOL, slots=1, description="synthetic players test",
                include_deferred=False,
            ))
        cls.dag.sync_to_db()

    def setUp(self) -> None:
        self.state_path.unlink(missing_ok=True)
        self.failures_path.unlink(missing_ok=True)

    def tearDown(self) -> None:
        self.assertEqual(self.history_path.read_bytes(), self.history_bytes)

    def test_import_and_serial_contract(self) -> None:
        from airflow.models.dag import DagModel
        from airflow.models.mappedoperator import MappedOperator
        from airflow.utils.session import create_session

        self.assertEqual(self.bag.import_errors, {})
        self.assertEqual(self.dag.dag_id, DAG_ID)
        edges = {
            "plan_players_batch": set(),
            "run_players_scope": {"plan_players_batch"},
            "validate_players_scope": {"plan_players_batch", "run_players_scope"},
            "finalize_players_run": {"plan_players_batch", "validate_players_scope"},
            "propagate_players_status": {"finalize_players_run"},
        }
        self.assertEqual(set(self.dag.task_ids), set(edges))
        for task_id, upstream in edges.items():
            self.assertEqual(self.dag.get_task(task_id).upstream_task_ids, upstream)
        self.assertTrue(self.dag.is_paused_upon_creation)
        with create_session() as session:
            self.assertTrue(session.get(DagModel, DAG_ID).is_paused)
        self.assertFalse(self.dag.catchup)
        self.assertFalse(self.dag.render_template_as_native_obj)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.max_active_tasks, 1)
        self.assertEqual(self.dag.dagrun_timeout, timedelta(hours=4, minutes=30))
        self.assertEqual(self.dag.schedule_interval, "0 7,12,17,22 * * *")
        run = self.dag.get_task("run_players_scope")
        self.assertIsInstance(run, MappedOperator)
        self.assertEqual(run.pool, POOL)
        self.assertEqual(run.pool_slots, 1)
        self.assertEqual(run.partial_kwargs["max_active_tis_per_dag"], 1)
        self.assertEqual(run.partial_kwargs["execution_timeout"], timedelta(minutes=40))
        self.assertEqual(run.partial_kwargs["retries"], 1)
        self.assertEqual(run.partial_kwargs["retry_delay"], timedelta(minutes=2))
        validate = self.dag.get_task("validate_players_scope")
        self.assertIsInstance(validate, MappedOperator)
        for task_id in (
            "validate_players_scope", "finalize_players_run", "propagate_players_status",
        ):
            self.assertEqual(self.dag.get_task(task_id).trigger_rule, "all_done")
        self.assertEqual(validate.partial_kwargs["retries"], 0)
        for task_id in ("plan_players_batch", "finalize_players_run", "propagate_players_status"):
            self.assertEqual(self.dag.get_task(task_id).retries, 0)

    def _run_task(self, dag_run, task_id: str, map_index: int = -1) -> None:
        # Never execute the capture BashOperator, even if a caller is mistaken.
        self.assertNotEqual(task_id, "run_players_scope")
        ti = dag_run.get_task_instance(task_id, map_index=map_index)
        self.assertIsNotNone(ti)
        ti.refresh_from_task(self.dag.get_task(task_id))
        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=False)

    def _advance(self, dag_run) -> None:
        from airflow.utils.session import create_session

        with create_session() as session:
            dag_run.dag = self.dag
            ready, _ = dag_run.update_state(session=session, execute_callbacks=False)
            if ready:
                dag_run.schedule_tis(ready, session=session)
        dag_run.refresh_from_db()

    def _state(self, dag_run, task_id: str, index: int = -1):
        ti = dag_run.get_task_instance(task_id, map_index=index)
        return None if ti is None else ti.state

    def _batch(self, outcomes):
        from airflow.utils.session import create_session
        from airflow.utils.state import DagRunState
        from airflow.utils.types import DagRunType

        type(self).run_number += 1
        number = self.run_number
        start = datetime(2026, 9, 5, tzinfo=timezone.utc) + timedelta(days=number)
        run_id = f"synthetic-players-{number}"
        planned = []
        for index, outcome in enumerate(outcomes):
            season = 1725 - index
            output = self.tmp / "results" / run_id / str(index)
            output.mkdir(parents=True)
            environment = {
                "SOFASCORE_CAMPAIGN_ACTION": "players",
                "SOFASCORE_SCOPE_KEY": f"c-1:17:{season}",
                "SOFASCORE_EXPECTED_CAMPAIGN_ID": "c-1",
                "SOFASCORE_SCOPE_RUN_ID": f"{run_id}--17-{season}",
                "SOFASCORE_SCOPE_RESULT_PATH": str(output / "result.json"),
                "SOFASCORE_SCOPE_OUTPUT_DIR": str(output),
            }
            result = {
                "campaign_id": "c-1", "snapshot_id": "a" * 64,
                "tournament_id": 17, "source_season_id": season,
                "run_id": environment["SOFASCORE_SCOPE_RUN_ID"],
                "status": "success",
            }
            if outcome == "run_failed":
                result.update(status="failed", errors=["synthetic capture failure"])
            elif outcome == "deferred":
                result.update(
                    status="deferred", deferral_reason="PlayerEvidenceNotReady: matches pending",
                    source_request_count=0,
                )
            (output / "result.json").write_text(json.dumps(result), encoding="utf-8")
            if outcome in {"healthy", "validation_failed"}:
                (output / "players.json").write_text(json.dumps({
                    "universe_players": 2 if outcome == "healthy" else 0,
                }), encoding="utf-8")
            planned.append(environment)

        dag_run = self.dag.create_dagrun(
            run_id=run_id, execution_date=start,
            data_interval=(start, start + timedelta(minutes=1)), start_date=start,
            state=DagRunState.RUNNING, run_type=DagRunType.MANUAL,
        )
        with patch.object(
            self.dag.get_task("plan_players_batch"), "python_callable",
            lambda **context: planned,
        ):
            self._run_task(dag_run, "plan_players_batch")
        self._advance(dag_run)
        actual_plan = dag_run.get_task_instance("plan_players_batch").xcom_pull(
            task_ids="plan_players_batch"
        )
        self.assertEqual(actual_plan, planned)
        with create_session() as session:
            for index, outcome in enumerate(outcomes):
                ti = dag_run.get_task_instance("run_players_scope", map_index=index, session=session)
                self.assertIsNotNone(ti, "scheduler must expand the real mapped capture task")
                ti.state = "failed" if outcome == "run_failed" else "success"
                session.merge(ti)
        self._advance(dag_run)
        for index in range(len(outcomes)):
            self.assertEqual(self._state(dag_run, "validate_players_scope", index), "scheduled")
        return dag_run, planned

    def test_failed_capture_and_validation_do_not_cancel_healthy_scope(self) -> None:
        from airflow.exceptions import AirflowException

        dag_run, planned = self._batch(["healthy", "run_failed", "validation_failed"])
        self._run_task(dag_run, "validate_players_scope", 0)
        for index, message in ((1, "did not finish successfully"), (2, "no player universe")):
            with self.assertRaisesRegex(AirflowException, message):
                self._run_task(dag_run, "validate_players_scope", index)
            self.assertEqual(self._state(dag_run, "validate_players_scope", index), "failed")
        self._advance(dag_run)
        self.assertEqual(self._state(dag_run, "finalize_players_run"), "scheduled")
        self._run_task(dag_run, "finalize_players_run")
        completed = json.loads(self.state_path.read_text(encoding="utf-8"))["completed"]
        self.assertEqual(set(completed), {planned[0]["SOFASCORE_SCOPE_KEY"]})
        failures = json.loads(self.failures_path.read_text(encoding="utf-8"))
        self.assertEqual(set(failures["attempts"]), {
            planned[1]["SOFASCORE_SCOPE_KEY"], planned[2]["SOFASCORE_SCOPE_KEY"],
        })
        self._advance(dag_run)
        with self.assertRaisesRegex(AirflowException, "SofaScore players attempt failed"):
            self._run_task(dag_run, "propagate_players_status")
        self._advance(dag_run)
        self.assertEqual(dag_run.state, "failed")

    def test_deferred_zero_source_scope_is_green_but_not_completed(self) -> None:
        dag_run, planned = self._batch(["healthy", "deferred"])
        for index in range(2):
            self._run_task(dag_run, "validate_players_scope", index)
        deferred = dag_run.get_task_instance("validate_players_scope", map_index=1).xcom_pull(
            task_ids="validate_players_scope", map_indexes=1,
        )
        self.assertEqual(deferred["status"], "deferred")
        self._advance(dag_run)
        self._run_task(dag_run, "finalize_players_run")
        completed = json.loads(self.state_path.read_text(encoding="utf-8"))["completed"]
        self.assertEqual(set(completed), {planned[0]["SOFASCORE_SCOPE_KEY"]})
        failures = json.loads(self.failures_path.read_text(encoding="utf-8"))
        self.assertEqual(set(failures["attempts"]), {planned[1]["SOFASCORE_SCOPE_KEY"]})
        self._advance(dag_run)
        self._run_task(dag_run, "propagate_players_status")
        self._advance(dag_run)
        self.assertEqual(dag_run.state, "success")


if __name__ == "__main__":
    unittest.main()
