"""Ломатель тупика шага drain против НАСТОЯЩЕГО Airflow 2.11.2 (#1245).

Юнит-стенд `tests/unit/deploy` кладёт ORM-вызовы ломателя на sqlite-шим: он проверяет
выбор и отказы, но не то, что будет делать планировщик после `failed`. Здесь всё
настоящее: миграция метабазы, модуль `dags/dag_backfill_sofascore_all_mens.py`, реальные
`plan_historical_batch` / `finalize_historical_run` / `validate_historical_scope`,
раскрытие mapped-задач и `DagRun.update_state()`.

Проверяется цепочка ночи 05.09 целиком: скоуп упал → повтор припаркован в осушённом пуле →
ломатель ставит `failed` → validate-плейсхолдер получает `upstream_failed` → finalize пишет
отказ в `failures.json` → cooldown → propagate → DagRun закрыт. Плюс успешный сценарий
(зачёт через `mark_completed`), успешный скоуп с провалившейся валидацией, идемпотентность
и отказы ломателя.

Запуск (pytest в venv Airflow нет):
    /tmp/whoscored-ci-airflow-2.11.2.6mn32H/bin/python -m unittest \\
        tests.airflow_real.test_sofascore_drain_breaker -v

ОГРАНИЧЕНИЕ: на sqlite `with_row_locks` — no-op, гонка «ломатель против планировщика»
здесь не воспроизводится. Её проверяет разовый прогон на PostgreSQL (см. описание PR #1245).
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
DAG_ID = "dag_backfill_sofascore_all_mens"
POOL = "sofascore_history_pool"
BREAKER = ROOT / "deploy" / "sofascore" / "drain_breaker.py"


def _campaign_files(root: Path, seasons: int = 1) -> tuple[Path, Path]:
    """Снимок кампании и политика, связанные так же, как в бою (одна пара digest-ов).

    ``seasons`` > 1 даёт снимок, из которого планировщик набирает батч (#1248 ступень 1).
    """

    sys.path[:0] = [str(ROOT), str(ROOT / "dags")]
    from scrapers.sofascore.all_mens_campaign import (  # noqa: E402
        _snapshot_digest,
        campaign_policy_id,
        candidate_ids_digest,
    )

    tournaments = [{
        "unique_tournament_id": 17,
        "capture_key": "SS-17",
        "metadata_status": "ready",
        "seasons": [{
            "source_season_id": 1725 - offset,
            "canonical_season": "%d%d" % (25 - offset, 26 - offset),
            "start_year": 2025 - offset,
            "season_format": "split_year",
            "team_count": 20,
            "metadata_status": "ready",
            "team_count_evidence": {"count": 20, "endpoint": "/teams"},
        } for offset in range(seasons)],
    }]
    policy = {
        "schema_version": 1,
        "candidate_count": 1,
        "candidate_ids_sha256": candidate_ids_digest(tournaments),
    }
    document = {
        "schema_version": 1,
        "candidate_count": 1,
        "policy_id": campaign_policy_id(policy),
        "campaign_id": hashlib.sha256(json.dumps({
            "policy_id": campaign_policy_id(policy),
            "candidate_ids_sha256": policy["candidate_ids_sha256"],
        }, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "tournaments": tournaments,
    }
    document["snapshot_id"] = _snapshot_digest(document)
    snapshot_path = root / "snapshot.json"
    policy_path = root / "all_mens_campaign.json"
    snapshot_path.write_text(json.dumps(document), encoding="utf-8")
    policy_path.write_text(json.dumps(policy), encoding="utf-8")
    return snapshot_path, policy_path


def _bind_orm(sql_alchemy_conn: str) -> None:
    """Привязать ORM Airflow к нужной базе.

    Настройки Airflow процессные: соседний модуль airflow_real (batch isolation) поднимает
    свою базу и свой ORM, и без явной привязки `create_session` пришёл бы в чужой — а после
    его tearDownClass и вовсе в удалённый — файл.
    """

    from airflow import settings
    from airflow.configuration import conf

    if not conf.has_section("database"):
        conf.add_section("database")
    conf.set("database", "sql_alchemy_conn", sql_alchemy_conn)
    settings.SQL_ALCHEMY_CONN = sql_alchemy_conn
    settings.configure_orm()


class DrainBreakerOnRealAirflow(unittest.TestCase):
    tmp: Path
    _runs = 0

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = Path(tempfile.mkdtemp(prefix="sofascore-drain-breaker-"))
        campaign = cls.tmp / "all-men"
        campaign.mkdir()
        (campaign / "results").mkdir()
        snapshot, policy = _campaign_files(campaign)
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
            "SOFASCORE_PROXY_BUDGET_ARTIFACT": str(ROOT / "configs" / "sofascore" / "workload_policy.json"),
            "SOFASCORE_HISTORY_POOL": POOL,
        }
        os.environ.update(cls.env)
        (cls.tmp / "airflow").mkdir()
        _bind_orm(cls.env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"])
        subprocess.run(
            [str(Path(sys.executable).with_name("airflow")), "db", "migrate"],
            env=cls.env, check=True, capture_output=True,
        )
        spec = importlib.util.spec_from_file_location(
            "dag_backfill_sofascore_all_mens_real",
            ROOT / "dags" / "dag_backfill_sofascore_all_mens.py",
        )
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)
        cls.dag = cls.module.dag
        # Охлаждение между прогонами ждёт настоящие 60 с: в тесте оно нулевое, а сенсор
        # опрашивается на месте (mode='poke'), а не через up_for_reschedule.
        from datetime import timedelta

        cls.module.ACTIVE_COOLDOWN = timedelta(seconds=0)
        cls.module.IDLE_COOLDOWN = timedelta(seconds=0)
        cooldown = cls.dag.get_task("wait_before_next_continuous_run")
        cooldown.mode = "poke"
        cooldown.poke_interval = 0

        from airflow.models.pool import Pool
        from airflow.utils.session import create_session

        with create_session() as session:
            # default_pool заводит `airflow db migrate`; полосе истории нужен свой.
            session.add(Pool(pool=POOL, slots=1, description="history", include_deferred=False))
        cls.dag.sync_to_db()

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def setUp(self) -> None:
        # Учёт кампании — общий файл: зачёт из одного теста менял бы план следующего.
        self.state_path.unlink(missing_ok=True)
        self.failures_path.unlink(missing_ok=True)

    # --- помощники ---------------------------------------------------------------------
    def _new_run(self, run_id: str):
        from airflow.utils.state import DagRunState
        from airflow.utils.types import DagRunType
        from datetime import datetime, timedelta, timezone

        # Свой интервал на каждый прогон: (dag_id, execution_date) уникальны в метабазе.
        DrainBreakerOnRealAirflow._runs += 1
        start = datetime(2026, 9, 5, 3, 27, tzinfo=timezone.utc) + timedelta(
            hours=DrainBreakerOnRealAirflow._runs
        )
        return self.dag.create_dagrun(
            run_id=run_id,
            execution_date=start,
            data_interval=(start, start + timedelta(minutes=1)),
            start_date=start,
            state=DagRunState.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

    def _run_task(self, dag_run, task_id: str, map_index: int = -1) -> None:
        ti = dag_run.get_task_instance(task_id, map_index=map_index)
        ti.refresh_from_task(self.dag.get_task(task_id))
        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=False)

    def _advance(self, dag_run) -> None:
        """Один шаг планировщика: раскрыть карты, посчитать зависимости, запланировать."""
        from airflow.utils.session import create_session

        with create_session() as session:
            dag_run.dag = self.dag
            # Ровно то, что делает планировщик: update_state раскрывает карты, считает
            # зависимости и решает судьбу прогона, schedule_tis ставит готовые в scheduled.
            schedulable, _ = dag_run.update_state(session=session, execute_callbacks=False)
            if schedulable:
                dag_run.schedule_tis(schedulable, session=session)
            session.commit()
        dag_run.refresh_from_db()

    def _state(self, dag_run, task_id: str, map_index: int = -1):
        ti = dag_run.get_task_instance(task_id, map_index=map_index)
        return None if ti is None else ti.state

    def _park_retry(self, dag_run) -> None:
        """Первая попытка скоупа упала, повтор ждёт слота в осушённом пуле (ночь 05.09)."""
        from airflow.utils.session import create_session

        with create_session() as session:
            ti = dag_run.get_task_instance("run_historical_scope", map_index=0, session=session)
            ti.state = "up_for_retry"
            ti.try_number = 1
            ti.pool = POOL
            session.merge(ti)
            session.commit()

    def _break(self, run_id: str, pool: str = POOL) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(BREAKER), DAG_ID, run_id, pool],
            env=self.env, capture_output=True, text=True,
        )

    def _plan_and_expand(self, run_id: str):
        dag_run = self._new_run(run_id)
        self._run_task(dag_run, "plan_historical_batch")
        self._advance(dag_run)
        self.assertEqual(self._state(dag_run, "run_historical_scope", 0), "scheduled")
        return dag_run

    # --- сценарии ----------------------------------------------------------------------
    def test_a_parked_retry_is_failed_and_the_run_finishes_with_a_recorded_refusal(self) -> None:
        dag_run = self._plan_and_expand("real-failed-scope")
        self._park_retry(dag_run)

        proc = self._break(dag_run.run_id)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("-> failed", proc.stdout)
        dag_run.refresh_from_db()
        self.assertEqual(self._state(dag_run, "run_historical_scope", 0), "failed")

        # Дальше — настоящий планировщик. С all_done (#1248 ступень 1) validate своего
        # скоупа больше не гасится соседями по батчу: он доходит до собственной проверки,
        # не находит результата и падает сам; finalize стартует по all_done и пишет отказ
        # в failures.json.
        self._advance(dag_run)
        self.assertEqual(self._state(dag_run, "validate_historical_scope", 0), "scheduled")
        with self.assertRaises(Exception):
            self._run_task(dag_run, "validate_historical_scope", map_index=0)
        dag_run.refresh_from_db()
        self.assertEqual(self._state(dag_run, "validate_historical_scope", 0), "failed")
        self._advance(dag_run)
        self._run_task(dag_run, "finalize_historical_run")
        failures = json.loads(self.failures_path.read_text(encoding="utf-8"))
        self.assertEqual(
            [entry["count"] for entry in failures["attempts"].values()], [1], failures
        )

        self._advance(dag_run)
        self._run_task(dag_run, "wait_before_next_continuous_run")
        self._advance(dag_run)
        with self.assertRaises(Exception):
            self._run_task(dag_run, "propagate_historical_status")
        self._advance(dag_run)
        self.assertEqual(dag_run.state, "failed")

    def test_a_successful_scope_is_counted_and_the_run_succeeds(self) -> None:
        dag_run = self._plan_and_expand("real-success-scope")
        planned = dag_run.get_task_instance("plan_historical_batch").xcom_pull(
            task_ids="plan_historical_batch"
        )
        scope_key = planned[0]["SOFASCORE_SCOPE_KEY"]
        result = Path(planned[0]["SOFASCORE_SCOPE_RESULT_PATH"])
        result.parent.mkdir(parents=True, exist_ok=True)
        result.write_text(json.dumps({
            "campaign_id": planned[0]["SOFASCORE_EXPECTED_CAMPAIGN_ID"],
            "snapshot_id": planned[0]["SOFASCORE_EXPECTED_SNAPSHOT_ID"],
            "tournament_id": int(planned[0]["SOFASCORE_TOURNAMENT_ID"]),
            "source_season_id": int(planned[0]["SOFASCORE_SOURCE_SEASON_ID"]),
            "status": "success",
        }), encoding="utf-8")

        from airflow.utils.session import create_session

        with create_session() as session:
            ti = dag_run.get_task_instance("run_historical_scope", map_index=0, session=session)
            ti.state = "success"
            session.merge(ti)
            session.commit()
        # Ломателю здесь делать нечего: скоуп терминален, состояние вне протокола.
        proc = self._break(dag_run.run_id)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("состояние вне протокола", proc.stdout)

        self._advance(dag_run)
        self._run_task(dag_run, "validate_historical_scope", map_index=0)
        completed = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.assertIn(scope_key, completed["completed"])
        self._advance(dag_run)
        self._run_task(dag_run, "finalize_historical_run")
        self._advance(dag_run)
        self._run_task(dag_run, "wait_before_next_continuous_run")
        self._advance(dag_run)
        self._run_task(dag_run, "propagate_historical_status")
        self._advance(dag_run)
        self.assertEqual(dag_run.state, "success")

    def test_a_successful_scope_with_a_failed_validation_is_refused_by_finalize(self) -> None:
        """Скоуп отработал, а валидация упала (результат нечитаем): скоуп не засчитан и не
        отказан — раньше он возвращался следующим прогоном и покупался заново без конца.
        Отказ пишет finalize по all_done, и именно на это опирается ACCOUNTED=t."""
        from airflow.utils.session import create_session

        dag_run = self._plan_and_expand("real-bad-validation")
        with create_session() as session:
            ti = dag_run.get_task_instance("run_historical_scope", map_index=0, session=session)
            ti.state = "success"
            session.merge(ti)
            session.commit()
        self._advance(dag_run)
        with self.assertRaises(Exception):
            self._run_task(dag_run, "validate_historical_scope", map_index=0)
        self._advance(dag_run)
        self._run_task(dag_run, "finalize_historical_run")

        failures = json.loads(self.failures_path.read_text(encoding="utf-8"))
        self.assertEqual(
            [entry["count"] for entry in failures["attempts"].values()], [1], failures
        )
        self.assertFalse(self.state_path.exists(), "зачёта быть не должно")

    def test_the_breaker_is_idempotent_and_refuses_what_is_not_its_business(self) -> None:
        dag_run = self._plan_and_expand("real-refusals")
        self._park_retry(dag_run)

        first = self._break(dag_run.run_id)
        self.assertIn("-> failed", first.stdout)
        # Второй вызов ничего не меняет: скоуп уже терминален.
        second = self._break(dag_run.run_id)
        self.assertIn("состояние вне протокола", second.stdout)
        dag_run.refresh_from_db()
        self.assertEqual(self._state(dag_run, "run_historical_scope", 0), "failed")

        # Чужой пул — отказ, состояние не трогаем.
        self._park_retry(dag_run)
        other = self._break(dag_run.run_id, pool="default_pool")
        self.assertIn("отказ — скоуп в пуле", other.stdout)
        dag_run.refresh_from_db()
        self.assertEqual(self._state(dag_run, "run_historical_scope", 0), "up_for_retry")

        # NULL-состояние — тоже отказ.
        from airflow.utils.session import create_session

        with create_session() as session:
            ti = dag_run.get_task_instance("run_historical_scope", map_index=0, session=session)
            ti.state = None
            session.merge(ti)
            session.commit()
        null_state = self._break(dag_run.run_id)
        self.assertIn("состояние вне протокола", null_state.stdout)

        # Прогона нет — отказ без исключений.
        missing = self._break("no-such-run")
        self.assertEqual(missing.returncode, 0, missing.stderr)
        self.assertIn("отказ — прогона", missing.stdout)

    def test_the_breaker_fails_only_the_parked_scopes_of_a_batch(self) -> None:
        """Батч (#1248 ступень 1): припаркованные скоупы гасим, работающий — нет.

        Пятое число deploy.sh считает любой scheduled/up_for_retry в осушённом пуле,
        поэтому ломатель обязан погасить ровно это множество — иначе прогон не закроется
        и ночь уйдёт без доставки."""
        from airflow.models import TaskInstance
        from airflow.utils.session import create_session

        dag_run = self._plan_and_expand("real-batch")
        with create_session() as session:
            running = dag_run.get_task_instance(
                "run_historical_scope", map_index=0, session=session
            )
            running.state = "running"
            running.try_number = 1
            running.pool = POOL
            session.merge(running)
            for map_index, state in ((1, "scheduled"), (2, "up_for_retry")):
                twin = TaskInstance(
                    self.dag.get_task("run_historical_scope"),
                    run_id=dag_run.run_id,
                    map_index=map_index,
                )
                twin.state = state
                twin.pool = POOL
                twin.try_number = 1
                session.merge(twin)
            session.commit()

        proc = self._break(dag_run.run_id)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertNotIn("batch>1 не поддержан", proc.stdout)
        dag_run.refresh_from_db()
        self.assertEqual(self._state(dag_run, "run_historical_scope", 0), "running")
        self.assertEqual(self._state(dag_run, "run_historical_scope", 1), "failed")
        self.assertEqual(self._state(dag_run, "run_historical_scope", 2), "failed")

    def test_the_placeholder_row_is_never_touched(self) -> None:
        """map_index = -1 — не скоуп, а NULL-плейсхолдер нераскрытой карты."""
        from airflow.utils.session import create_session

        # Прогон только создан: карта ещё не раскрыта, есть один NULL-плейсхолдер map -1.
        dag_run = self._new_run("real-placeholder")
        with create_session() as session:
            placeholder = dag_run.get_task_instance(
                "run_historical_scope", map_index=-1, session=session
            )
            placeholder.pool = POOL
            placeholder.state = "up_for_retry"
            session.merge(placeholder)
            session.commit()

        proc = self._break(dag_run.run_id)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("mapped-скоупов 0", proc.stdout)
        dag_run.refresh_from_db()
        self.assertEqual(self._state(dag_run, "run_historical_scope", -1), "up_for_retry")


if __name__ == "__main__":
    unittest.main()
