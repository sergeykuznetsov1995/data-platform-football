"""Unit tests for ``dags/dag_ingest_sofascore.py``.

Covers:
- #751 Bronze freshness gate (match-grain tables, WARNING).
- #782 folded-in per-player capture: the Saturday/manual gate
  (``_gate_player_capture``), the gated player tasks, and the player freshness
  check (ERROR), migrated from the former ``dag_ingest_sofascore_players``.

Airflow is not installed on the host; ``tests/unit/dags/conftest.py`` installs
stub ``airflow`` modules into ``sys.modules`` so the DAG module body (operators
+ ``>>`` wiring) executes and can be asserted on.
"""

from __future__ import annotations

import importlib
import sys
from datetime import datetime
from types import SimpleNamespace

import pytest
from unittest.mock import MagicMock


def _reload_dag_module():
    """Force a fresh import of the SofaScore ingest DAG module."""
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

    BashOperator._instances.clear()
    PythonOperator._instances.clear()

    sys.modules.pop("dag_ingest_sofascore", None)
    sys.modules.pop("dags.dag_ingest_sofascore", None)

    return importlib.import_module("dag_ingest_sofascore")


@pytest.fixture
def dag_module(real_medallion_config_dir):
    return _reload_dag_module()


def _python_task(task_id):
    from airflow.operators.python import PythonOperator

    for t in PythonOperator._instances:
        if t.task_id == task_id:
            return t
    return None


def _bash_task(task_id):
    from airflow.operators.bash import BashOperator

    for t in BashOperator._instances:
        if t.task_id == task_id:
            return t
    return None


def _patch_active_catalog(monkeypatch, competition_ids, source_seasons=None):
    """Replace the read-only registry consumer for one DAG import."""
    from scrapers.sofascore.catalog import SofaScoreCatalog

    configured = source_seasons or {}

    class _Catalog:
        @staticmethod
        def enabled_competition_ids():
            return tuple(competition_ids)

        @staticmethod
        def competition(competition_id):
            default = "2026" if competition_id.startswith("INT-") else "2526"
            seasons = configured.get(competition_id, (default,))
            return SimpleNamespace(
                seasons=tuple(
                    SimpleNamespace(
                        canonical_season=season,
                        activatable=season is not None,
                    )
                    for season in seasons
                )
            )

    monkeypatch.setattr(
        SofaScoreCatalog,
        "load",
        classmethod(lambda cls, path=None: _Catalog()),
    )


class TestSignedWorkloadTopology:
    def test_phase_plans_gate_every_paid_capture(self, dag_module):
        season_plan = _bash_task("prepare_sofascore_season_plan")
        target_plan = _bash_task("prepare_sofascore_target_plan")
        player_plan = _bash_task("prepare_sofascore_player_plan")
        assert season_plan is not None and target_plan is not None
        assert player_plan is not None
        assert "--phase season" in season_plan.bash_command
        assert "--phase targets" in target_plan.bash_command
        assert "--phase players" in player_plan.bash_command
        assert all(
            "--allow-inactive-season" not in task.bash_command
            for task in (season_plan, target_plan, player_plan)
        )
        for league, schedule in dag_module.schedule_tasks.items():
            assert "--workload-plan" in schedule.bash_command
        for league, capture in dag_module.match_capture_tasks.items():
            assert "prepare_sofascore_target_plan" in capture.bash_command
        for capture in dag_module.player_capture_tasks.values():
            assert "prepare_sofascore_player_plan" in capture.bash_command

    def test_player_plan_is_after_all_matches_and_gate(self, dag_module):
        gate = _python_task("gate_player_capture")
        player_plan = _bash_task("prepare_sofascore_player_plan")
        assert gate is not None and player_plan is not None
        assert set(gate.upstream_task_ids) == {
            task.task_id for task in dag_module.match_capture_tasks.values()
        }
        assert player_plan.upstream_task_ids == {"gate_player_capture"}
        # #946 4d: the per-league rotation gate now sits between the signed plan
        # and each capture.
        assert player_plan.downstream_task_ids == {
            dag_module._player_rotation_gate_task_id(league)
            for league in dag_module.SOFASCORE_LEAGUES
        }

    def test_single_proxy_lease_is_serialized_without_manual_pool(self, dag_module):
        assert dag_module.dag._dag_kwargs["max_active_tasks"] == 1


class TestBronzeFreshnessGate:
    """#751: a ``validate_bronze_freshness`` task must exist, wired after
    canonical manifest DQ, alerting on stale ``bronze.sofascore_*`` ingestion."""

    def test_dag_module_imports_and_exposes_callable(self, dag_module):
        assert hasattr(dag_module, "validate_bronze_freshness")

    def test_manifest_dq_is_between_validation_and_freshness(self, dag_module):
        validate = _python_task("validate_data")
        manifest_dq = _python_task("run_sofascore_dq")
        fresh = _python_task("validate_bronze_freshness")
        assert validate is not None and manifest_dq is not None and fresh is not None
        assert manifest_dq.python_callable is dag_module.run_sofascore_dq
        assert fresh.python_callable is dag_module.validate_bronze_freshness
        assert manifest_dq.upstream_task_ids == {"validate_data"}
        assert fresh.upstream_task_ids == {"run_sofascore_dq"}

    def test_manifest_dq_failure_propagates_for_tournament_leg(
        self, dag_module, monkeypatch
    ):
        def load(path, logger):
            if "int_world_cup" in path and "match_capture" in path:
                return {"endpoint_completeness": 0.8, "errors": []}
            return {"endpoint_completeness": 1.0, "errors": []}

        monkeypatch.setattr(dag_module, "_load_result", load)
        with pytest.raises(Exception, match="canonical endpoint completeness failed"):
            dag_module.run_sofascore_dq()

    def test_freshness_checks_cover_match_grain_tables_as_warning(
        self,
        dag_module,
        monkeypatch,
    ):
        captured = {}

        def fake_run_checks(checks, raise_on_error=True):
            captured["checks"] = checks
            captured["raise_on_error"] = raise_on_error
            return MagicMock()

        import utils.alerts as al
        import utils.data_quality as dq

        monkeypatch.setattr(dq, "run_checks", fake_run_checks)
        monkeypatch.setattr(
            al,
            "telegram_dq_summary",
            lambda *a, **k: captured.setdefault("telegram", True),
        )
        # #842: freshness now reads the match_capture result file to detect a
        # skip-existing no-op — pin it to "no result" so the checks always run.
        monkeypatch.setattr(dag_module, "_load_result", lambda *a, **k: {})

        dag_module.validate_bronze_freshness()

        checks = captured["checks"]
        freshness = [c for c in checks if c.kind == "freshness"]
        assert {c.params["table"] for c in freshness} == {
            "bronze.sofascore_schedule",
            "bronze.sofascore_league_table",
            "bronze.sofascore_match_stats",
            "bronze.sofascore_event_player_stats",
            "bronze.sofascore_player_ratings",
        }
        assert all(c.params["ts_col"] == "_ingested_at" for c in freshness)
        # #711: a label-coverage guard on match_stats flags a partial /statistics
        # capture (group + numeric values present, stat name/key empty) that the
        # row-count floors in validate_data miss.
        coverage = [c for c in checks if c.kind == "coverage"]
        assert len(coverage) == 1
        assert coverage[0].params["table"] == "bronze.sofascore_match_stats"
        assert len(checks) == len(freshness) + len(coverage)
        # WARNING-only gate must not hard-fail the DAG.
        assert all(c.severity == "WARNING" for c in checks)
        assert captured["raise_on_error"] is False
        assert captured.get("telegram") is True


class TestValidateDataIncrementalNoop:
    """#842 incremental match_capture: a clean skip-existing no-op run (all
    resolved matches already in bronze) reports 0 captured rows by design —
    the capture row-floors must not WARN; schedule/table freshness still runs.
    A genuinely-low non-noop run still WARNs (incl. the new venue floor)."""

    SCHEDULE_OK = {
        "schedule_rows": 381,
        "league_table_rows": 20,
        "tables": [],
        "errors": [],
    }

    @staticmethod
    def _patch_results(dag_module, monkeypatch, schedule_result, capture_result):
        # Tournament-leg paths (#920 Phase 2) resolve to the runner's healthy
        # out-of-window marker, so these club-batch tests stay leg-agnostic
        # (a missing leg file now WARNs — see TestValidateDataTournamentLegs).
        skipped = {"skipped": "out_of_window", "errors": [], "tables": []}

        def _fake_load(path, logger):
            if path == dag_module.MATCH_CAPTURE_RESULT_PATH:
                return capture_result
            if path == dag_module.SCHEDULE_RESULT_PATH:
                return schedule_result
            return dict(skipped)

        monkeypatch.setattr(dag_module, "_load_result", _fake_load)

    @staticmethod
    def _noop_capture():
        return {
            "rows": 0,
            "eps_rows": 0,
            "match_stats_rows": 0,
            "shotmap_rows": 0,
            "venue_rows": 0,
            "matches_total": 380,
            "matches_skipped_existing": 380,
            "fallback": False,
            "errors": [],
            "tables": [],
        }

    def test_noop_run_skips_capture_row_floors(self, dag_module, monkeypatch):
        self._patch_results(
            dag_module, monkeypatch, dict(self.SCHEDULE_OK), self._noop_capture()
        )
        validation = dag_module.validate_data()
        assert validation["status"] == "success"
        assert validation["warnings"] == []

    def test_zero_rows_without_skip_still_warns(self, dag_module, monkeypatch):
        capture = self._noop_capture()
        capture["matches_skipped_existing"] = 0  # nothing skipped → real gap
        self._patch_results(dag_module, monkeypatch, dict(self.SCHEDULE_OK), capture)
        validation = dag_module.validate_data()
        warned = " ".join(validation["warnings"])
        for table in (
            "player_ratings",
            "shotmap",
            "event_player_stats",
            "match_stats",
            "venue",
        ):
            assert table in warned

    def test_venue_floor_warns_when_low(self, dag_module, monkeypatch):
        capture = {
            "rows": 25000,
            "matches_with_ratings": 380,
            "eps_rows": 12000,
            "match_stats_rows": 34000,
            "shotmap_rows": 9500,
            "venue_rows": 5,
            "venue_matches": 5,
            "matches_total": 380,
            "matches_skipped_existing": 0,
            "fallback": False,
            "errors": [],
            "tables": [],
        }
        self._patch_results(dag_module, monkeypatch, dict(self.SCHEDULE_OK), capture)
        validation = dag_module.validate_data()
        assert any("venue" in w for w in validation["warnings"])
        assert len(validation["warnings"]) == 1

    def test_noop_still_checks_schedule_and_standings_freshness(
        self,
        dag_module,
        monkeypatch,
    ):
        captured = {}
        import utils.alerts as al
        import utils.data_quality as dq

        def fake_run_checks(checks, **kwargs):
            captured["freshness"] = {
                check.params["table"] for check in checks if check.kind == "freshness"
            }
            captured["coverage"] = {
                check.params["table"] for check in checks if check.kind == "coverage"
            }
            return MagicMock()

        monkeypatch.setattr(dq, "run_checks", fake_run_checks)
        monkeypatch.setattr(
            al,
            "telegram_dq_summary",
            lambda *a, **k: captured.setdefault("telegram", True),
        )
        monkeypatch.setattr(
            dag_module, "_load_result", lambda *a, **k: self._noop_capture()
        )
        dag_module.validate_bronze_freshness()
        assert captured["freshness"] == {
            "bronze.sofascore_schedule",
            "bronze.sofascore_league_table",
        }
        assert captured["coverage"] == {"bronze.sofascore_match_stats"}
        assert captured.get("telegram") is True

    def test_freshness_runs_on_fallback_capture(self, dag_module, monkeypatch):
        """A failed/fallback capture is NOT a no-op — the stall alert must
        still fire even when every match was 'skipped'."""
        capture = self._noop_capture()
        capture["fallback"] = True
        captured = {}
        import utils.alerts as al
        import utils.data_quality as dq

        def fake_run_checks(checks, raise_on_error=True):
            captured["ran"] = True
            return MagicMock()

        monkeypatch.setattr(dq, "run_checks", fake_run_checks)
        monkeypatch.setattr(
            al,
            "telegram_dq_summary",
            lambda *a, **k: captured.setdefault("telegram", True),
        )
        monkeypatch.setattr(dag_module, "_load_result", lambda *a, **k: capture)
        dag_module.validate_bronze_freshness()
        assert captured.get("ran") is True


class TestValidateDataTournamentLegs:
    """#920 Phase 2: the Phase-1 fan-out's per-tournament result files are now
    observed by validate_data. Missing/failed files hard-fail; only the
    explicit out-of-window marker stays silent. Club floors are derived from
    competitions.yaml and must equal the historical literals for
    CLUB_LEAGUES == ['ENG-Premier League']."""

    SCHEDULE_OK = {
        "schedule_rows": 381,
        "league_table_rows": 20,
        "tables": [],
        "errors": [],
    }
    CAPTURE_OK = {
        "rows": 25000,
        "matches_with_ratings": 380,
        "eps_rows": 12000,
        "match_stats_rows": 34000,
        "shotmap_rows": 9500,
        "venue_rows": 380,
        "matches_total": 380,
        "matches_skipped_existing": 0,
        "fallback": False,
        "errors": [],
        "tables": [],
    }

    def _patch(self, dag_module, monkeypatch, t_schedule, t_capture):
        wc_sched = "/tmp/sofascore_result_int_world_cup.json"
        wc_capture = "/tmp/sofascore_match_capture_result_int_world_cup.json"

        def _fake_load(path, logger):
            if path == dag_module.MATCH_CAPTURE_RESULT_PATH:
                return dict(self.CAPTURE_OK)
            if path == dag_module.SCHEDULE_RESULT_PATH:
                return dict(self.SCHEDULE_OK)
            if path == wc_sched:
                return t_schedule
            if path == wc_capture:
                return t_capture
            return {}

        monkeypatch.setattr(dag_module, "_load_result", _fake_load)

    def test_club_floors_equal_legacy_literals(self, dag_module):
        # The pre-#920-Phase-2 inline literals, now derived — exact match for
        # the current single-club scope (no silent recalibration).
        assert dag_module._summed_club_floors() == {
            "schedule_rows": 100,
            "league_table_rows": 10,
            "player_ratings_rows": 300,
            "shotmap_rows": 300,
            "event_player_stats_rows": 10_000,
            "match_stats_rows": 10_000,
            "venue_rows": 300,
        }

    def test_out_of_window_marker_is_silent(self, dag_module, monkeypatch):
        skipped = {"skipped": "out_of_window", "errors": [], "tables": []}
        self._patch(dag_module, monkeypatch, skipped, dict(skipped))
        validation = dag_module.validate_data()
        assert validation["status"] == "success"
        assert validation["warnings"] == []

    def test_missing_leg_files_fail(self, dag_module, monkeypatch):
        # The runner ALWAYS writes its output file (out-of-window runs write
        # the 'skipped' marker) — a missing file means the runner died
        # before writing and must not pass silently (review hardening).
        self._patch(dag_module, monkeypatch, {}, {})
        with pytest.raises(Exception, match="schedule result file.*missing"):
            dag_module.validate_data()

    def test_failed_tournament_leg_hard_fails(
        self, dag_module, monkeypatch
    ):
        # WC floors: schedule 100*104//380=27, league_table 10*48//20=24.
        t_sched = {
            "schedule_rows": 5,
            "league_table_rows": 0,
            "tables": [],
            "errors": [],
        }
        t_capture = {
            "rows": 3,
            "shotmap_rows": 0,
            "eps_rows": 0,
            "match_stats_rows": 0,
            "venue_rows": 0,
            "matches_total": 10,
            "matches_skipped_existing": 0,
            "fallback": False,
            "errors": ["camoufox timeout"],
            "tables": [],
        }
        self._patch(dag_module, monkeypatch, t_sched, t_capture)
        with pytest.raises(Exception, match="match capture returned fallback/errors"):
            dag_module.validate_data()

    def test_healthy_tournament_leg_no_warnings(self, dag_module, monkeypatch):
        t_sched = {
            "schedule_rows": 104,
            "league_table_rows": 48,
            "tables": [],
            "errors": [],
        }
        t_capture = {
            "rows": 2600,
            "shotmap_rows": 2600,
            "eps_rows": 2800,
            "match_stats_rows": 9000,
            "venue_rows": 104,
            "matches_total": 104,
            "matches_skipped_existing": 0,
            "fallback": False,
            "errors": [],
            "tables": [],
        }
        self._patch(dag_module, monkeypatch, t_sched, t_capture)
        validation = dag_module.validate_data()
        assert validation["warnings"] == []


class TestRunScopedResultProvenance:
    def test_result_paths_are_unique_per_dag_run(self, dag_module):
        first = dag_module._result_path(
            dag_module.SCHEDULE_RESULT_PATH, {"run_id": "scheduled__one"}
        )
        second = dag_module._result_path(
            dag_module.SCHEDULE_RESULT_PATH, {"run_id": "scheduled__two"}
        )

        assert first != second
        assert first.startswith("/tmp/sofascore/")
        assert first.endswith("/sofascore_result.json")

    def test_failed_secondary_producer_is_rejected_before_file_read(
        self, dag_module, monkeypatch
    ):
        class DagRun:
            run_id = "scheduled__failed-secondary"

            @staticmethod
            def get_task_instance(task_id):
                state = (
                    "failed"
                    if task_id == "scrape_match_capture_int_world_cup"
                    else "success"
                )
                return SimpleNamespace(state=state)

        monkeypatch.setattr(
            dag_module,
            "_load_result",
            lambda *args, **kwargs: pytest.fail("failed producer result was read"),
        )
        with pytest.raises(
            Exception, match="scrape_match_capture_int_world_cup=failed"
        ):
            dag_module.validate_data(dag_run=DagRun(), run_id=DagRun.run_id)

    def test_current_run_never_reads_legacy_stale_file(
        self, dag_module, monkeypatch
    ):
        requested = []

        def load(path, logger):
            requested.append(path)
            if path == dag_module.SCHEDULE_RESULT_PATH:
                return TestValidateDataTournamentLegs.SCHEDULE_OK
            if path == dag_module.MATCH_CAPTURE_RESULT_PATH:
                return TestValidateDataTournamentLegs.CAPTURE_OK
            return {}

        monkeypatch.setattr(dag_module, "_load_result", load)
        with pytest.raises(Exception, match="Schedule results file"):
            dag_module.validate_data(run_id="scheduled__new")
        assert requested
        assert requested[0] != dag_module.SCHEDULE_RESULT_PATH


class TestPlayerCaptureGate:
    """The player capture runs on Saturday master-runs or when forced."""

    def test_gate_callable_exposed(self, dag_module):
        assert hasattr(dag_module, "_gate_player_capture")

    def test_gate_task_exists_as_shortcircuit(self, dag_module):
        gate = _python_task("gate_player_capture")
        assert gate is not None
        assert gate.python_callable is dag_module._gate_player_capture

    def test_run_players_param_forces_capture(self, dag_module):
        assert dag_module._gate_player_capture(params={"run_players": True}) is True

    def test_saturday_scheduled_run_triggers_capture(self, dag_module):
        # 2024-01-06 is a Saturday (weekday()==5).
        assert (
            dag_module._gate_player_capture(
                params={},
                dag_run=SimpleNamespace(external_trigger=False),
                logical_date=datetime(2024, 1, 6),
            )
            is True
        )

    def test_uses_interval_end_for_cron_run_day(self, dag_module):
        # Airflow cron logical_date is the interval start; the Saturday run can
        # therefore carry a Friday logical date.
        assert (
            dag_module._gate_player_capture(
                params={},
                dag_run=SimpleNamespace(external_trigger=False),
                logical_date=datetime(2024, 1, 5),
                data_interval_end=datetime(2024, 1, 6),
            )
            is True
        )

    def test_weekday_scheduled_run_skips_capture(self, dag_module):
        # 2024-01-01 is a Monday (weekday()==0).
        assert (
            dag_module._gate_player_capture(
                params={},
                dag_run=SimpleNamespace(external_trigger=False),
                logical_date=datetime(2024, 1, 1),
            )
            is False
        )

    def test_saturday_master_trigger_runs_weekly_capture(self, dag_module):
        assert (
            dag_module._gate_player_capture(
                params={},
                dag_run=SimpleNamespace(external_trigger=True),
                logical_date=datetime(2024, 1, 6),
            )
            is True
        )

    def test_master_uses_actual_start_not_interval_start_day(self, dag_module):
        # Stable master boundary wins over both prior-interval logical_date and
        # a delayed child start on Sunday.
        assert (
            dag_module._gate_player_capture(
                params={},
                dag_run=SimpleNamespace(
                    external_trigger=True,
                    start_date=datetime(2024, 1, 7, 1),
                    conf={"master_data_interval_end": "2024-01-06T14:00:00+00:00"},
                ),
                logical_date=datetime(2024, 1, 5, 14),
            )
            is True
        )


class TestPlayerRotationGate:
    """#946 4d: on the Saturday the weekly gate lets through, each club league
    is captured once every ``SOFASCORE_PLAYER_ROTATION_MODULUS`` weeks."""

    SATURDAY = datetime(2026, 1, 10)  # ISO week 2
    CLUB_SCOPE = [
        "ENG-Premier League",
        "ESP-La Liga",
        "ITA-Serie A",
        "GER-Bundesliga",
        "FRA-Ligue 1",
        "NED-Eredivisie",
        "POR-Primeira Liga",
        "BEL-Pro League",
        "TUR-Super Lig",
        "SCO-Premiership",
        "AUT-Bundesliga",
        "SUI-Super League",
    ]

    @pytest.fixture
    def rotation_env(self, monkeypatch):
        monkeypatch.setenv("SOFASCORE_PLAYER_ROTATION_MODULUS", "4")
        monkeypatch.setenv("SOFASCORE_PLAYER_ROTATION_MIN_LEAGUES", "10")

    def _wide_scope(self, dag_module, monkeypatch):
        """Simulate an onboarded 12-club scope without re-importing the DAG."""
        leagues = self.CLUB_SCOPE + ["INT-World Cup"]
        monkeypatch.setattr(dag_module, "SOFASCORE_LEAGUES", leagues)
        monkeypatch.setattr(dag_module, "CLUB_LEAGUES", list(self.CLUB_SCOPE))
        monkeypatch.setattr(dag_module, "TOURNAMENT_LEAGUES", ["INT-World Cup"])
        return leagues

    def test_gates_sit_between_the_signed_plan_and_each_capture(self, dag_module):
        for league, capture in dag_module.player_capture_tasks.items():
            gate = _python_task(dag_module._player_rotation_gate_task_id(league))
            assert gate is not None
            assert gate.python_callable is dag_module._gate_player_rotation
            assert gate.op_kwargs == {"league": league}
            assert gate.upstream_task_ids == {"prepare_sofascore_player_plan"}
            assert capture.task_id in gate.downstream_task_ids

    def test_skip_must_not_cascade_into_the_validators(self, dag_module):
        from airflow.operators.python import ShortCircuitOperator

        for league in dag_module.SOFASCORE_LEAGUES:
            gate = _python_task(dag_module._player_rotation_gate_task_id(league))
            assert isinstance(gate, ShortCircuitOperator)
            # True (the Airflow default) would skip validate_player_data too.
            assert gate._init_kwargs["ignore_downstream_trigger_rules"] is False

    def test_todays_two_league_scope_keeps_its_weekly_cadence(
        self, dag_module, rotation_env
    ):
        # Regression: below the threshold the rotation is a no-op — EPL and the
        # World Cup still run on every Saturday.
        for week in range(4):
            boundary = datetime(2026, 1, 10 + 7 * week)
            due = dag_module._due_player_leagues(
                {
                    "params": {},
                    "dag_run": SimpleNamespace(external_trigger=False),
                    "data_interval_end": boundary,
                }
            )
            assert due == set(dag_module.SOFASCORE_LEAGUES)

    def test_wide_scope_runs_one_cohort_per_week_and_covers_all_in_modulus(
        self, dag_module, monkeypatch, rotation_env
    ):
        from dags.scripts.prepare_sofascore_workload import player_rotation_due

        leagues = self._wide_scope(dag_module, monkeypatch)
        seen = {league: 0 for league in leagues}
        for week in range(4):
            boundary = datetime(2026, 1, 10 + 7 * week)
            context = {
                "params": {},
                "dag_run": SimpleNamespace(external_trigger=False),
                "data_interval_end": boundary,
            }
            due = dag_module._due_player_leagues(context)
            expected = {
                league
                for league in leagues
                if player_rotation_due(
                    league,
                    rotation_date=boundary.date(),
                    club_league_count=len(self.CLUB_SCOPE),
                    is_tournament=league == "INT-World Cup",
                )
            }
            assert due == expected
            assert due != set(leagues)  # the whole point: a proper subset
            assert "INT-World Cup" in due  # cups are never rotated out
            for league in due:
                seen[league] += 1
                assert (
                    dag_module._gate_player_rotation(league=league, **context) is True
                )
            for league in set(leagues) - due:
                assert (
                    dag_module._gate_player_rotation(league=league, **context) is False
                )
        assert all(seen[league] == 1 for league in self.CLUB_SCOPE)
        assert seen["INT-World Cup"] == 4

    def test_forced_run_captures_every_league(
        self, dag_module, monkeypatch, rotation_env
    ):
        leagues = self._wide_scope(dag_module, monkeypatch)
        context = {
            "params": {"run_players": True},
            "dag_run": SimpleNamespace(external_trigger=True),
            "data_interval_end": self.SATURDAY,
        }
        assert dag_module._due_player_leagues(context) == set(leagues)

    def test_plan_task_signs_only_the_due_cohort(self, dag_module):
        plan = _bash_task("prepare_sofascore_player_plan")
        # The plan is fed the SAME boundary the gate resolves
        # (`_resolve_player_run_boundary`): the master data_interval_end from
        # dag_run.conf, falling back to this run's own interval end — not an
        # independent template that only coincides within one ISO week.
        assert (
            "--players-rotation-date "
            "\"{{ dag_run.conf.get('master_data_interval_end') "
            "or (data_interval_end | ds) }}\""
        ) in plan.bash_command
        assert "master_data_interval_end" in plan.bash_command
        assert "{% if params.run_players %}--players-force{% endif %}" in (
            plan.bash_command
        )
        # Matches and league tables are not rotated.
        for task_id in ("prepare_sofascore_season_plan", "prepare_sofascore_target_plan"):
            assert "--players-rotation-date" not in _bash_task(task_id).bash_command


class TestPlayerCaptureTasks:
    """#782: the player capture task + validation wiring folded in from the
    former ``dag_ingest_sofascore_players``."""

    def test_capture_task_runs_player_capture_entity(self, dag_module):
        task = _bash_task("scrape_player_capture")
        assert task is not None
        assert "--entity player_capture" in task.bash_command
        assert f'--league "{dag_module.CLUB_LEAGUES[0]}"' in task.bash_command
        # A fallback/403 must remain a failed producer.
        assert "exit 0" not in task.bash_command
        assert task.env["SOFASCORE_DAG_RUN_ID"] == "{{ run_id }}"

    def test_player_validate_then_freshness_wired(self, dag_module):
        # The Bash→Python edge isn't modelled by the stub (BashOperator.__rshift__
        # is a no-op); assert the Python→Python edge the stub tracks.
        validate = _python_task("validate_player_data")
        fresh = _python_task("validate_player_freshness")
        assert validate is not None and fresh is not None
        assert "validate_player_data" in fresh.upstream_task_ids

    def test_player_freshness_checks_cover_player_tables_as_error(
        self,
        dag_module,
        monkeypatch,
    ):
        captured = {}

        def fake_run_checks(checks, raise_on_error=True):
            captured["checks"] = checks
            captured["raise_on_error"] = raise_on_error
            return MagicMock(errors=[])

        import utils.alerts as al
        import utils.data_quality as dq

        monkeypatch.setattr(dq, "run_checks", fake_run_checks)
        monkeypatch.setattr(
            al,
            "telegram_dq_summary",
            lambda *a, **k: captured.setdefault("telegram", True),
        )

        dag_module.validate_player_freshness()

        checks = captured["checks"]
        assert {c.params["table"] for c in checks} == {
            "bronze.sofascore_player_profile",
            "bronze.sofascore_player_season_stats",
        }
        assert all(c.kind == "freshness" for c in checks)
        assert all(c.severity == "ERROR" for c in checks)
        # Weekly cadence → 8-day window before alerting.
        assert all(c.params["max_age_hours"] == 192 for c in checks)
        # raise_on_error stays False; the function re-raises manually so the
        # Telegram summary lands before the hard-fail.
        assert captured["raise_on_error"] is False
        assert captured.get("telegram") is True

    def test_player_freshness_stale_tables_raises(
        self,
        dag_module,
        monkeypatch,
    ):
        from airflow.exceptions import AirflowException

        captured = {}
        stale = SimpleNamespace(
            name="freshness[bronze.sofascore_player_profile._ingested_at<192h]",
            details="age 300h > 192h",
            error=None,
        )

        def fake_run_checks(checks, raise_on_error=True):
            return MagicMock(errors=[stale])

        import utils.alerts as al
        import utils.data_quality as dq

        monkeypatch.setattr(dq, "run_checks", fake_run_checks)
        monkeypatch.setattr(
            al,
            "telegram_dq_summary",
            lambda *a, **k: captured.setdefault("telegram", True),
        )

        with pytest.raises(AirflowException):
            dag_module.validate_player_freshness()

        # Telegram must fire before the hard-fail.
        assert captured.get("telegram") is True


class TestSeasonParam:
    """#711 (epic #708): the season must be a UI Param defaulting to
    CURRENT_SEASON so the master-triggered daily run ingests the current
    season unchanged, while a "Trigger DAG w/ config" override can backfill a
    past season."""

    def test_season_param_default_is_current_season(self, dag_module):
        from utils.config import CURRENT_SEASON

        season_param = dag_module.dag._dag_kwargs["params"]["season"]
        # conftest's _Param stub stores the default (real Param also exposes it).
        assert season_param.default == CURRENT_SEASON


class TestProxyEfficientCadence:
    def test_source_has_no_duplicate_schedule(self, dag_module):
        assert dag_module.dag._dag_kwargs["schedule"] is None


class TestRegistryActivation:
    """Discovery is broad; only explicit registry activation shapes the DAG."""

    def test_bootstrap_scope_preserves_epl_and_world_cup(self, dag_module):
        assert set(dag_module.SOFASCORE_LEAGUES) == {
            "ENG-Premier League",
            "INT-World Cup",
        }
        assert dag_module.CLUB_LEAGUES == ["ENG-Premier League"]
        assert dag_module.TOURNAMENT_LEAGUES == ["INT-World Cup"]

    @pytest.mark.parametrize(
        "task_id",
        [
            "scrape_sofascore_data_uefa_champions_league",
            "scrape_match_capture_uefa_champions_league",
            "scrape_sofascore_data_rus_premier_league",
            "scrape_match_capture_rus_premier_league",
            "scrape_sofascore_data_int_africa_cup_of_nations",
            "scrape_match_capture_int_africa_cup_of_nations",
        ],
    )
    def test_discovered_but_disabled_competitions_create_no_tasks(
        self,
        dag_module,
        task_id,
    ):
        assert _bash_task(task_id) is None

    def test_activating_second_club_adds_dedicated_capture_without_renaming_epl(
        self,
        monkeypatch,
        real_medallion_config_dir,
    ):
        _patch_active_catalog(
            monkeypatch,
            (
                "ESP-La Liga",
                "INT-World Cup",
                "ENG-Premier League",
            ),
        )
        _reload_dag_module()

        schedule = _bash_task("scrape_sofascore_data")
        assert '--league "ENG-Premier League"' in schedule.bash_command
        la_liga_schedule = _bash_task("scrape_sofascore_data_esp_la_liga")
        assert '--league "ESP-La Liga"' in la_liga_schedule.bash_command

        epl = _bash_task("scrape_match_capture")
        assert '--league "ENG-Premier League"' in epl.bash_command
        assert "$SOFASCORE_RESULT_DIR/sofascore_match_capture_result.json" in (
            epl.bash_command
        )

        la_liga = _bash_task("scrape_match_capture_esp_la_liga")
        assert la_liga is not None
        assert '--league "ESP-La Liga"' in la_liga.bash_command
        assert (
            "$SOFASCORE_RESULT_DIR/sofascore_match_capture_result_esp_la_liga.json"
            in la_liga.bash_command
        )
        la_liga_players = _bash_task("scrape_player_capture_esp_la_liga")
        assert la_liga_players is not None
        assert '--league "ESP-La Liga"' in la_liga_players.bash_command

    def test_secondary_club_capture_is_validated_without_inflating_epl_floor(
        self,
        monkeypatch,
        real_medallion_config_dir,
    ):
        _patch_active_catalog(
            monkeypatch,
            (
                "ESP-La Liga",
                "INT-World Cup",
                "ENG-Premier League",
            ),
        )
        module = _reload_dag_module()
        schedule = {
            "schedule_rows": 200,
            "league_table_rows": 20,
            "tables": [],
            "errors": [],
        }
        primary = {
            "rows": 300,
            "shotmap_rows": 300,
            "eps_rows": 10_000,
            "match_stats_rows": 10_000,
            "venue_rows": 300,
            "fallback": False,
            "errors": [],
            "tables": [],
        }

        def load(path, logger):
            if path == module.SCHEDULE_RESULT_PATH:
                return schedule
            if path == module.MATCH_CAPTURE_RESULT_PATH:
                return primary
            if "int_world_cup" in path:
                return {"skipped": True}
            if "esp_la_liga" in path:
                return {}
            raise AssertionError(path)

        monkeypatch.setattr(module, "_load_result", load)
        with pytest.raises(Exception, match="ESP-La Liga: schedule result file"):
            module.validate_data()

    def test_empty_active_scope_fails_closed(self, monkeypatch):
        _patch_active_catalog(monkeypatch, ())
        with pytest.raises(Exception, match="no usable active tournaments"):
            _reload_dag_module()

    def test_missing_or_malformed_registry_fails_closed(self, monkeypatch):
        from scrapers.sofascore.catalog import CatalogError, SofaScoreCatalog

        def _fail(cls, path=None):
            raise CatalogError("cannot parse tournaments.json")

        monkeypatch.setattr(SofaScoreCatalog, "load", classmethod(_fail))
        with pytest.raises(
            Exception,
            match="registry is missing or invalid: cannot parse",
        ):
            _reload_dag_module()

    def test_active_canonical_stub_fails_before_tasks_are_built(
        self,
        monkeypatch,
        real_medallion_config_dir,
    ):
        _patch_active_catalog(
            monkeypatch,
            (
                "ENG-Premier League",
                "UEFA-Champions League",
            ),
        )
        with pytest.raises(Exception, match="no canonical seasons configured"):
            _reload_dag_module()

    def test_active_tournament_without_source_season_fails_before_paid_access(
        self,
        monkeypatch,
        real_medallion_config_dir,
    ):
        _patch_active_catalog(
            monkeypatch,
            ("ENG-Premier League", "INT-World Cup"),
            source_seasons={"INT-World Cup": ()},
        )
        with pytest.raises(Exception, match="no activatable SofaScore seasons"):
            _reload_dag_module()

    def test_stale_source_season_cannot_activate_current_club_run(
        self,
        monkeypatch,
        real_medallion_config_dir,
    ):
        _patch_active_catalog(
            monkeypatch,
            ("ENG-Premier League", "INT-World Cup"),
            source_seasons={"ENG-Premier League": ("2425",)},
        )
        with pytest.raises(Exception, match="scheduled season.*is missing"):
            _reload_dag_module()


class TestSeasonRenderedFromParams:
    """#711: split-year club backfills keep the UI season parameter."""

    @pytest.mark.parametrize(
        "task_id",
        [
            "scrape_sofascore_data",
            "scrape_match_capture",
            "scrape_player_capture",
        ],
    )
    def test_scrape_task_renders_season_from_params(self, dag_module, task_id):
        task = _bash_task(task_id)
        assert task is not None, f"missing task {task_id}"
        # f-string collapses {{{{ }}}} -> {{ }}, so the literal Jinja tag
        # survives into the rendered bash_command.
        assert "--season {{ params.season }}" in task.bash_command, (
            f"{task_id} does not render season from params"
        )


class TestTournamentFanOut:
    """#920 Phase 1: club leagues stay batched in the original tasks
    (task_id/output/Jinja-season unchanged — regression guard for the
    'клубные лиги не должны измениться' acceptance criterion); each
    single-year tournament (e.g. INT-World Cup) gets its own dedicated
    task with its configured calendar year instead of the club UI parameter."""

    def test_club_schedule_task_excludes_tournament_leagues(self, dag_module):
        task = _bash_task("scrape_sofascore_data")
        assert task is not None
        assert '--league "ENG-Premier League"' in task.bash_command
        assert "INT-World Cup" not in task.bash_command

    def test_club_schedule_task_output_is_run_scoped(self, dag_module):
        task = _bash_task("scrape_sofascore_data")
        assert "$SOFASCORE_RESULT_DIR/sofascore_result.json" in task.bash_command
        assert task.env["SOFASCORE_DAG_RUN_ID"] == "{{ run_id }}"

    def test_tournament_schedule_task_exists_dedicated(self, dag_module):
        task = _bash_task("scrape_sofascore_data_int_world_cup")
        assert task is not None
        assert '--league "INT-World Cup"' in task.bash_command
        assert "--season 2026" in task.bash_command
        assert "--season {{ params.season }}" not in task.bash_command
        assert (
            "$SOFASCORE_RESULT_DIR/sofascore_result_int_world_cup.json"
            in task.bash_command
        )

    def test_club_match_capture_task_id_and_run_scoped_output(self, dag_module):
        task = _bash_task("scrape_match_capture")
        assert task is not None
        assert '--league "ENG-Premier League"' in task.bash_command
        assert (
            "$SOFASCORE_RESULT_DIR/sofascore_match_capture_result.json"
            in task.bash_command
        )
        assert "int_world_cup" not in task.bash_command

    def test_tournament_match_capture_task_exists_dedicated(self, dag_module):
        task = _bash_task("scrape_match_capture_int_world_cup")
        assert task is not None
        assert '--league "INT-World Cup"' in task.bash_command
        assert "--season 2026" in task.bash_command
        assert "--season {{ params.season }}" not in task.bash_command
        assert (
            "$SOFASCORE_RESULT_DIR/sofascore_match_capture_result_int_world_cup.json"
            in task.bash_command
        )

    def test_tournament_player_capture_uses_calendar_year(self, dag_module):
        task = _bash_task("scrape_player_capture_int_world_cup")
        assert task is not None
        assert '--league "INT-World Cup"' in task.bash_command
        assert "--season 2026" in task.bash_command
        assert "--season {{ params.season }}" not in task.bash_command

    def test_tournament_tasks_import_outside_active_date_window(
        self,
        monkeypatch,
        real_medallion_config_dir,
    ):
        import utils.medallion_config as medallion

        monkeypatch.setattr(medallion, "get_active_season", lambda league: None)
        module = _reload_dag_module()

        assert module.dag is not None
        task = _bash_task("scrape_sofascore_data_int_world_cup")
        assert task is not None
        assert "--season 2026" in task.bash_command

    def test_global_leagues_does_not_override_registry_activation(
        self,
        monkeypatch,
        real_medallion_config_dir,
    ):
        import utils.config as config

        monkeypatch.setattr(config, "LEAGUES", ["ENG-Premier League"])
        _reload_dag_module()

        # SofaScore no longer inherits this process-wide list. Its registry
        # still has World Cup active, while the other sources keep seeing the
        # monkeypatched global value.
        assert _bash_task("scrape_sofascore_data_int_world_cup") is not None
        assert _bash_task("scrape_match_capture_int_world_cup") is not None
        assert _bash_task("scrape_sofascore_data") is not None
        assert _bash_task("scrape_match_capture") is not None


class TestValidatePlayerData:
    def _write(self, dag_module, monkeypatch, tmp_path, result):
        monkeypatch.setattr(
            dag_module,
            "_load_result",
            lambda path, logger: (
                result
                if path == dag_module.PLAYER_CAPTURE_RESULT_PATH
                else {"skipped": "out_of_window"}
            ),
        )

    def test_low_rows_without_fallback_warns_but_succeeds(
        self,
        dag_module,
        monkeypatch,
        tmp_path,
    ):
        self._write(
            dag_module,
            monkeypatch,
            tmp_path,
            {
                "rows": 10,
                "profile_players": 10,
                "fallback": False,
                "tables": ["t"],
                "errors": [],
            },
        )
        out = dag_module.validate_player_data()
        assert out["status"] == "success"  # low rows are WARN-only, not failed
        assert any("Low player_profile" in w for w in out["warnings"])

    def test_fallback_with_some_rows_hard_fails(
        self,
        dag_module,
        monkeypatch,
        tmp_path,
    ):
        self._write(
            dag_module,
            monkeypatch,
            tmp_path,
            {
                "rows": 10,
                "profile_players": 10,
                "fallback": True,
                "tables": ["t"],
                "errors": ["R0_2B_FALLBACK: http_403"],
            },
        )
        with pytest.raises(Exception, match="skipped/fallback/errors"):
            dag_module.validate_player_data()

    def test_zero_rows_with_errors_raises(
        self,
        dag_module,
        monkeypatch,
        tmp_path,
    ):
        self._write(
            dag_module,
            monkeypatch,
            tmp_path,
            {
                "rows": 0,
                "profile_players": 0,
                "fallback": True,
                "tables": [],
                "errors": ["R0_2B_FALLBACK: http_403"],
            },
        )
        with pytest.raises(Exception):
            dag_module.validate_player_data()

    def test_low_season_stats_warns_but_succeeds(
        self,
        dag_module,
        monkeypatch,
        tmp_path,
    ):
        # Full profile coverage, but few exact season aggregates are exposed.
        # rows (#751 PR3b) → WARN-only, the run still succeeds.
        self._write(
            dag_module,
            monkeypatch,
            tmp_path,
            {
                "rows": 520,
                "profile_players": 520,
                "season_stats_rows": 12,
                "season_stats_players": 12,
                "fallback": False,
                "tables": ["t"],
                "errors": [],
            },
        )
        out = dag_module.validate_player_data()
        assert out["status"] == "success"
        assert out["summary"]["player_season_stats_rows"] == 12
        assert any("player_season_stats" in w for w in out["warnings"])

    def test_full_season_stats_no_warning(
        self,
        dag_module,
        monkeypatch,
        tmp_path,
    ):
        self._write(
            dag_module,
            monkeypatch,
            tmp_path,
            {
                "rows": 520,
                "profile_players": 520,
                "season_stats_rows": 500,
                "season_stats_players": 500,
                "fallback": False,
                "tables": ["t"],
                "errors": [],
            },
        )
        out = dag_module.validate_player_data()
        assert out["status"] == "success"
        assert not any("player_season_stats" in w for w in out["warnings"])


class TestValidatePlayerDataUnderRotation:
    """#946 4d: a league outside this week's cohort produces no result file —
    the validator must require its ``skipped`` state instead of a file."""

    SATURDAY = datetime(2026, 1, 10)

    def _context(self, dag_module, monkeypatch, states):
        monkeypatch.setenv("SOFASCORE_PLAYER_ROTATION_MODULUS", "2")
        monkeypatch.setenv("SOFASCORE_PLAYER_ROTATION_MIN_LEAGUES", "1")

        class DagRun:
            external_trigger = False
            run_id = "scheduled__rotation"
            conf: dict = {}

            @staticmethod
            def get_task_instance(task_id):
                return SimpleNamespace(state=states.get(task_id, "success"))

        return {
            "params": {},
            "dag_run": DagRun(),
            "run_id": DagRun.run_id,
            "data_interval_end": self.SATURDAY,
        }

    def test_skipped_league_needs_no_result_file(
        self, dag_module, monkeypatch
    ):
        # modulus=2 with min_leagues=1 forces a real split on the shipped scope.
        context = self._context(dag_module, monkeypatch, {})
        due = dag_module._due_player_leagues(context)
        assert due != set(dag_module.SOFASCORE_LEAGUES)
        skipped = [lg for lg in dag_module.SOFASCORE_LEAGUES if lg not in due]
        states = {
            dag_module._player_capture_task_id(league): "skipped" for league in skipped
        }
        context = self._context(dag_module, monkeypatch, states)

        def load(path, logger):
            for league in skipped:
                assert dag_module._league_slug(league) not in path
            return {
                "rows": 520,
                "profile_players": 520,
                "season_stats_rows": 500,
                "season_stats_players": 500,
                "players_total": 520,
                "fallback": False,
                "tables": ["t"],
                "errors": [],
            }

        monkeypatch.setattr(dag_module, "_load_result", load)
        out = dag_module.validate_player_data(**context)
        assert out["status"] == "success"
        assert out["summary"]["rotation_skipped"] == sorted(skipped)

    def test_non_due_league_that_did_not_skip_fails_loudly(
        self, dag_module, monkeypatch
    ):
        context = self._context(dag_module, monkeypatch, {})
        due = dag_module._due_player_leagues(context)
        skipped = [lg for lg in dag_module.SOFASCORE_LEAGUES if lg not in due]
        states = {
            dag_module._player_capture_task_id(league): "failed" for league in skipped
        }
        monkeypatch.setattr(dag_module, "_load_result", lambda path, logger: {})
        with pytest.raises(Exception, match="rotation cohort did not skip"):
            dag_module.validate_player_data(**self._context(dag_module, monkeypatch, states))

    def test_due_league_without_a_result_file_still_fails(
        self, dag_module, monkeypatch
    ):
        # The rotation must not weaken the guard for the leagues that DID run.
        context = {
            "params": {"run_players": True},
            "dag_run": SimpleNamespace(
                external_trigger=False,
                conf={},
                get_task_instance=lambda task_id: SimpleNamespace(state="success"),
            ),
            "run_id": "scheduled__forced",
            "data_interval_end": self.SATURDAY,
        }
        monkeypatch.setattr(dag_module, "_load_result", lambda path, logger: {})
        with pytest.raises(Exception, match="missing or unreadable"):
            dag_module.validate_player_data(**context)
