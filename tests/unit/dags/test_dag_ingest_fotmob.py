"""Unit tests for the source-native FotMob ingestion DAG.

The host uses lightweight Airflow stubs. Tests cover environment inheritance,
exact-scope params, one schedule owner, run-specific reports, the dedicated
HTTP pool and fail-closed native validation.
"""

from __future__ import annotations

import importlib
import hashlib
import sys

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _reload_dag_module():
    """Force a fresh import of the FotMob DAG module so each test sees
    a clean ``BashOperator._instances`` list (the stubbed BashOperator
    registers every constructed instance globally for inspection)."""
    from pathlib import Path

    from airflow.operators.bash import BashOperator  # stub

    BashOperator._instances.clear()

    # #920 Phase 1: the DAG module now calls is_single_year_competition() at
    # import time to build its task graph — point CONFIG_DIR at the real
    # shipped configs/medallion (on the host, it otherwise defaults to
    # /opt/airflow/configs/medallion, which only exists in the container).
    # CONFIG_DIR is resolved once at medallion_config import time, so patch
    # the module attribute directly (mirrors
    # tests/unit/sql/test_dim_competition_render.py).
    from utils import medallion_config

    medallion_config.CONFIG_DIR = (
        Path(__file__).resolve().parents[3] / "configs" / "medallion"
    )
    medallion_config.reset_cache()

    # Drop cached module so DAG body re-executes (and re-creates operators)
    sys.modules.pop("dag_ingest_fotmob", None)
    sys.modules.pop("dags.dag_ingest_fotmob", None)

    # The DAG file lives in /root/data_platform/dags/ which is on sys.path
    # via the conftest.
    return importlib.import_module("dag_ingest_fotmob")


def _daily_report(mod):
    planned = [
        f"{competition_id}=selected-{competition_id}"
        for competition_id in mod.FOTMOB_DAILY_COMPETITION_IDS
    ]
    operations = [
        {
            "entity": "competition_catalog",
            "status": "success",
            "errors": [],
            "retryable": [],
            "terminal": [],
            "counts": {"competitions": 555},
        },
        *[
            {
                "entity": "scope_completion",
                "status": "success",
                "errors": [],
                "retryable": [],
                "terminal": [],
                "metadata": {"scope": scope},
            }
            for scope in planned
        ],
        *[
            {
                "entity": "competition_completion",
                "status": "success",
                "errors": [],
                "retryable": [],
                "terminal": [],
                "metadata": {"competition_id": competition_id},
            }
            for competition_id in mod.FOTMOB_DAILY_COMPETITION_IDS
        ],
    ]
    return {
        "run_id": "run-daily",
        "mode": "daily",
        "status": "success",
        "complete": True,
        "operations": operations,
        "transport": {
            "attempts": 7_268,
            "direct_bytes": 193 * 1024 * 1024,
            "proxy_bytes": 0,
        },
        "budget": {
            "requests": 7_268,
            "max_requests": mod.FOTMOB_DAILY_MAX_REQUESTS,
            "direct_bytes": 193 * 1024 * 1024,
            "max_direct_bytes": mod.FOTMOB_DAILY_MAX_DIRECT_MIB * 1024 * 1024,
            "proxy_bytes": 0,
            "max_proxy_bytes": 0,
        },
        "errors": [],
        "selection": {
            "daily_contract": mod.FOTMOB_DAILY_CONTRACT_SCHEMA,
            "competition_scope": {
                "schema": mod.FOTMOB_DAILY_CONTRACT_SCHEMA,
                "scope_file": mod.FOTMOB_DAILY_SCOPE_FILE,
                "scope_sha256": mod.FOTMOB_DAILY_SCOPE_SHA256,
                "scope_count": mod.FOTMOB_DAILY_SCOPE_COUNT,
                "competition_ids": list(mod.FOTMOB_DAILY_COMPETITION_IDS),
                "competition_ids_sha256": (mod.FOTMOB_DAILY_COMPETITION_IDS_SHA256),
                "competition_count": mod.FOTMOB_DAILY_COMPETITION_COUNT,
            },
            "entities": sorted(mod.FOTMOB_DAILY_ENTITIES),
            "explicit_scopes": [],
            "competition_limit": 0,
            "season_limit": 0,
            "scope_plan_signature": "fmplan1-" + "a" * 64,
            "planned_scopes": planned,
            "completed_scopes": list(planned),
            "completed_transfer_competition_ids": list(
                mod.FOTMOB_DAILY_COMPETITION_IDS
            ),
            "requests_per_minute": mod.FOTMOB_DAILY_REQUESTS_PER_MINUTE,
        },
    }


def _source_refresh_report(mod):
    contract = mod._source_refresh_contract()
    outcomes = [
        {
            **target,
            "status": "not_available" if index == 0 else "success",
        }
        for index, target in enumerate(contract["targets"])
    ]
    return {
        "run_id": "source-refresh-run",
        "mode": "backfill",
        "status": "success",
        "complete": True,
        "operations": [
            {
                "entity": "player_snapshots",
                "status": "success",
                "attempted": 7,
                "succeeded": 6,
                "skipped": 0,
                "not_available": 1,
                "errors": [],
                "retryable": [],
                "terminal": [],
            },
            {
                "entity": "player_source_refresh_contract",
                "status": "success",
                "attempted": 7,
                "succeeded": 7,
                "errors": [],
                "retryable": [],
                "terminal": [],
                "counts": {"terminal_targets": 7},
                "metadata": {
                    "profile": contract["profile"],
                    "targets_sha256": contract["sha256"],
                    "target_outcomes": outcomes,
                },
            },
            {
                "entity": "commit_flush",
                "status": "success",
                "errors": [],
                "retryable": [],
                "terminal": [],
            },
            {
                "entity": "current_views",
                "status": "success",
                "errors": [],
                "retryable": [],
                "terminal": [],
            },
        ],
        "transport": {"attempts": 8, "direct_bytes": 1024, "proxy_bytes": 0},
        "budget": {
            "requests": 8,
            "max_requests": mod.PLAYER_SOURCE_REFRESH_MAX_REQUESTS,
            "direct_bytes": 1024,
            "max_direct_bytes": (
                mod.PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB * 1024 * 1024
            ),
            "proxy_bytes": 0,
            "max_proxy_bytes": 0,
        },
        "errors": [],
        "rows": {},
        "tables": [],
        "selection": {
            "profile": contract["profile"],
            "entities": ["players"],
            "explicit_scopes": [],
            "competition_limit": 0,
            "season_limit": 0,
            "scope_plan_signature": contract["plan_signature"],
            "planned_scopes": [],
            "completed_scopes": [],
            "completed_transfer_competition_ids": [],
            "requests_per_minute": 30,
            "source_refresh": {
                key: contract[key]
                for key in (
                    "profile",
                    "artifact",
                    "sha256",
                    "target_count",
                    "targets",
                    "plan_signature",
                )
            },
            "target_outcomes": outcomes,
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestFotmobBashOperatorEnv:
    """Regression coverage for the append_env=True fix."""

    @pytest.mark.unit
    def test_dag_loads_without_errors(self):
        """The DAG module should import cleanly under the stubbed Airflow."""
        mod = _reload_dag_module()

        # validate_data is a plain function — it should still be exported
        assert hasattr(mod, "validate_data")
        assert callable(mod.validate_data)

    @pytest.mark.unit
    def test_scrape_task_has_append_env_true(self):
        """``scrape_fotmob_data`` BashOperator MUST set append_env=True.

        Without append_env, the explicit ``env`` dict completely replaces
        the parent process environment, dropping TRINO_PASSWORD,
        FLARESOLVERR_URL, etc. and breaking the scraper.
        """
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        scrape_tasks = [
            op for op in BashOperator._instances if op.task_id == "scrape_fotmob_data"
        ]
        assert len(scrape_tasks) == 1, (
            f"Expected exactly one scrape_fotmob_data BashOperator, "
            f"found {len(scrape_tasks)}: "
            f"{[op.task_id for op in BashOperator._instances]}"
        )
        task = scrape_tasks[0]

        assert task.append_env is True, (
            "scrape_fotmob_data BashOperator must set append_env=True so "
            "the parent Airflow env (TRINO_PASSWORD, PYTHONPATH, ...) "
            "leaks through to the subprocess."
        )

    @pytest.mark.unit
    def test_scrape_task_env_still_overrides_pythonpath(self):
        """append_env=True doesn't mean we lose explicit overrides — the
        ``env`` dict should still contain the expected PYTHONPATH/PATH/HOME
        keys, otherwise the subprocess won't find scrapers/."""
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        task = next(
            op for op in BashOperator._instances if op.task_id == "scrape_fotmob_data"
        )

        assert task.env is not None, "explicit env dict must be set"
        assert "PYTHONPATH" in task.env
        assert "/opt/airflow" in task.env["PYTHONPATH"]


class TestFotmobNativeParams:
    """Native runs use exact FotMob scopes instead of calculated years."""

    @pytest.mark.unit
    def test_mode_and_scope_are_ui_configurable_params(self):
        mod = _reload_dag_module()

        from airflow.models.param import Param  # stub

        params = mod.dag._dag_kwargs["params"]
        assert isinstance(params["mode"], Param)
        assert params["mode"].default == "daily"
        assert isinstance(params["scope"], Param)
        assert params["scope"].default == ""
        assert params["daily_contract"].default == ""
        assert params["competition_scope_file"].default == ""
        assert params["requests_per_minute"].default == 30
        assert params["source_refresh_profile"].default == ""
        assert params["source_refresh_targets_sha256"].default == ""
        assert params["source_refresh_target_count"].default == 0
        assert "season" not in params

    @pytest.mark.unit
    def test_scrape_command_renders_native_mode_and_exact_scope(self):
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        task = next(
            op for op in BashOperator._instances if op.task_id == "scrape_fotmob_data"
        )

        assert '--mode "{{ params.mode }}"' in task.bash_command
        assert '--scope "{{ params.scope }}"' in task.bash_command
        assert '--daily-contract "{{ params.daily_contract }}"' in task.bash_command
        assert "--competition-scope-file" in task.bash_command
        assert '--requests-per-minute "{{ params.requests_per_minute }}"' in (
            task.bash_command
        )
        assert "--requests-per-minute 30" not in task.bash_command
        assert '--max-proxy-mib "{{ params.max_proxy_mib }}"' in task.bash_command
        assert '--source-refresh-profile "{{ params.source_refresh_profile }}"' in (
            task.bash_command
        )
        assert (
            "--source-refresh-targets-sha256 "
            '"{{ params.source_refresh_targets_sha256 }}"'
        ) in task.bash_command
        assert '--next-build-id ""' in task.bash_command
        assert "--season " not in task.bash_command
        assert "--leagues " not in task.bash_command


class TestDynamicDiscoveryDag:
    """One native task dynamically discovers club and tournament identities."""

    @pytest.mark.unit
    def test_one_native_task_has_run_specific_output_and_http_pool(self):
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        task = next(
            op for op in BashOperator._instances if op.task_id == "scrape_fotmob_data"
        )
        assert "{{ ts_nodash }}_{{ ti.try_number }}" in task.bash_command
        generation = "{{ dag_run.conf['fotmob_publication']['generation_id'] }}"
        assert f'--publication-generation-id "{generation}"' in task.bash_command
        for argument in (
            "--publication-schema",
            "--publication-source",
            "--publication-owner",
            "--publication-data-interval-start",
            "--publication-data-interval-end",
            "--publication-runtime-fingerprint",
        ):
            assert argument in task.bash_command
        assert "--run-id" not in task.bash_command
        assert "guarded-run" not in task.bash_command
        assert "python dags/scripts/run_fotmob_scraper.py" in task.bash_command
        assert "/tmp/fotmob_result_" in task.bash_command
        assert task._init_kwargs["pool"] == "fotmob_http_pool"
        assert task._init_kwargs["execution_timeout"].total_seconds() == 8 * 3600
        assert task._init_kwargs["retries"] == 0

    @pytest.mark.unit
    def test_no_hardcoded_tournament_fanout(self):
        _reload_dag_module()

        from airflow.operators.bash import BashOperator  # stub

        assert [op.task_id for op in BashOperator._instances] == ["scrape_fotmob_data"]

    @pytest.mark.unit
    def test_master_is_single_schedule_owner(self):
        mod = _reload_dag_module()

        assert mod.dag._dag_kwargs["schedule"] is None


class TestNativeValidation:
    @pytest.mark.unit
    def test_source_refresh_accepts_exact_seven_terminal_targets_without_catalog(
        self, tmp_path
    ):
        import json

        mod = _reload_dag_module()
        report = tmp_path / "source-refresh.json"
        report.write_text(json.dumps(_source_refresh_report(mod)), encoding="utf-8")

        summary = mod.validate_data(str(report))

        assert summary["selection"]["profile"] == (mod.PLAYER_SOURCE_REFRESH_PROFILE)
        assert len(summary["selection"]["target_outcomes"]) == 7
        assert summary["selection"]["explicit_scope_count"] == 0

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("mutation", "message"),
        [
            (
                lambda payload: payload["selection"].__setitem__(
                    "profile", "unreviewed-profile"
                ),
                "profile mismatch",
            ),
            (
                lambda payload: payload["selection"]["source_refresh"].__setitem__(
                    "sha256", "0" * 64
                ),
                "artifact binding mismatch",
            ),
            (
                lambda payload: payload["selection"]["source_refresh"].__setitem__(
                    "target_count", 8
                ),
                "artifact binding mismatch",
            ),
            (
                lambda payload: payload["selection"]["target_outcomes"][0].__setitem__(
                    "player_id", 999999
                ),
                "exactly seven targets",
            ),
            (
                lambda payload: payload["operations"][0].__setitem__("skipped", 1),
                "seven terminal player outcomes",
            ),
            (
                lambda payload: payload["budget"].__setitem__("max_requests", 63),
                "transport budget mismatch",
            ),
            (
                lambda payload: payload["selection"].__setitem__(
                    "planned_scopes", ["47=2026/2027"]
                ),
                "planner surface is not empty",
            ),
        ],
    )
    def test_source_refresh_rejects_any_widening_or_missing_terminal_proof(
        self, tmp_path, mutation, message
    ):
        import json

        from airflow.exceptions import AirflowException

        mod = _reload_dag_module()
        payload = _source_refresh_report(mod)
        mutation(payload)
        report = tmp_path / "source-refresh-mutated.json"
        report.write_text(json.dumps(payload), encoding="utf-8")

        with pytest.raises(AirflowException, match=message):
            mod.validate_data(str(report))

    @pytest.mark.unit
    def test_daily_requires_exact_cohort_and_complete_scope_and_transfer_sets(
        self, tmp_path
    ):
        import json

        mod = _reload_dag_module()
        report = tmp_path / "daily.json"
        payload = _daily_report(mod)
        report.write_text(json.dumps(payload), encoding="utf-8")

        summary = mod.validate_data(str(report))

        assert summary["selection"]["daily_contract"] == "fotmob-daily-v1"
        assert summary["selection"]["competition_scope"]["competition_count"] == 21
        assert (
            summary["selection"]["completed_scopes"]
            == summary["selection"]["planned_scopes"]
        )

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("mutation", "message"),
        [
            (
                lambda payload: payload["selection"]["competition_scope"][
                    "competition_ids"
                ].__setitem__(0, 999999),
                "competition scope mismatch",
            ),
            (
                lambda payload: payload["selection"]["completed_scopes"].pop(),
                "completed scopes differ",
            ),
            (
                lambda payload: payload["selection"][
                    "completed_transfer_competition_ids"
                ].pop(),
                "transfer completions differ",
            ),
            (
                lambda payload: payload["selection"]["entities"].pop(),
                "entity set mismatch",
            ),
            (
                lambda payload: payload["budget"].__setitem__("max_requests", 2_000),
                "transport budget mismatch",
            ),
            (
                lambda payload: payload["budget"].__setitem__("proxy_bytes", 1),
                "direct-only invariant",
            ),
        ],
    )
    def test_daily_rejects_partial_or_mutated_contract(
        self, tmp_path, mutation, message
    ):
        import json

        from airflow.exceptions import AirflowException

        mod = _reload_dag_module()
        payload = _daily_report(mod)
        mutation(payload)
        report = tmp_path / "daily-mutated.json"
        report.write_text(json.dumps(payload), encoding="utf-8")

        with pytest.raises(AirflowException, match=message):
            mod.validate_data(str(report))

    @pytest.mark.unit
    def test_legacy_or_missing_mode_is_rejected(self, tmp_path):
        import json

        from airflow.exceptions import AirflowException

        mod = _reload_dag_module()
        report = tmp_path / "report.json"
        report.write_text(
            json.dumps({"status": "success", "rows": {"schedule": 1}}),
            encoding="utf-8",
        )

        with pytest.raises(AirflowException, match="native mode is required"):
            mod.validate_data(str(report))

    @pytest.mark.unit
    def test_incomplete_native_report_fails(self, tmp_path):
        import json

        from airflow.exceptions import AirflowException

        mod = _reload_dag_module()
        report = tmp_path / "report.json"
        report.write_text(
            json.dumps(
                {
                    "mode": "backfill",
                    "status": "incomplete",
                    "complete": False,
                    "operations": [],
                    "transport": {"proxy_bytes": 0},
                    "budget": {"requests": 1, "max_requests": 2000},
                    "errors": ["schema drift"],
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(AirflowException, match="Incomplete FotMob"):
            mod.validate_data(str(report))

    @pytest.mark.unit
    def test_complete_direct_native_report_passes(self, tmp_path):
        import json

        mod = _reload_dag_module()
        report = tmp_path / "report.json"
        report.write_text(
            json.dumps(
                {
                    "run_id": "run-1",
                    "mode": "backfill",
                    "status": "success",
                    "complete": True,
                    "operations": [
                        {
                            "entity": "competition_catalog",
                            "status": "review_required",
                            "errors": [],
                            "retryable": [],
                            "terminal": [],
                            "counts": {"competitions": 10},
                        }
                    ],
                    "transport": {
                        "attempts": 1,
                        "direct_bytes": 100,
                        "proxy_bytes": 0,
                    },
                    "budget": {
                        "requests": 1,
                        "max_requests": 2000,
                        "direct_bytes": 100,
                        "max_direct_bytes": 1024,
                        "proxy_bytes": 0,
                        "max_proxy_bytes": 0,
                    },
                    "errors": [],
                    "rows": {"competition_catalog": 10},
                    "tables": ["iceberg.bronze.fotmob_competitions"],
                    "selection": {
                        "entities": ["leaderboards", "season"],
                        "explicit_scopes": ["47=2025/2026"],
                        "competition_limit": 0,
                        "season_limit": 0,
                        "scope_plan_signature": "fmplan1-" + "a" * 64,
                    },
                }
            ),
            encoding="utf-8",
        )

        validation = mod.validate_data(str(report))

        assert validation["status"] == "success"
        assert validation["transport"]["proxy_bytes"] == 0
        assert validation["selection"] == {
            "entities": ["leaderboards", "season"],
            "explicit_scope_count": 1,
            "explicit_scope_sha256": hashlib.sha256(b"47=2025/2026\n").hexdigest(),
            "scope_plan_signature": "fmplan1-" + "a" * 64,
            "competition_limit": 0,
            "season_limit": 0,
        }

    @pytest.mark.unit
    def test_native_report_requires_bounded_exact_selection_evidence(self, tmp_path):
        import json

        from airflow.exceptions import AirflowException

        mod = _reload_dag_module()
        report = tmp_path / "report.json"
        report.write_text(
            json.dumps(
                {
                    "mode": "backfill",
                    "status": "success",
                    "complete": True,
                    "operations": [
                        {
                            "entity": "competition_catalog",
                            "counts": {"competitions": 1},
                        }
                    ],
                    "transport": {
                        "attempts": 1,
                        "direct_bytes": 1,
                        "proxy_bytes": 0,
                    },
                    "budget": {
                        "requests": 1,
                        "max_requests": 2,
                        "direct_bytes": 1,
                        "max_direct_bytes": 2,
                        "proxy_bytes": 0,
                        "max_proxy_bytes": 0,
                    },
                    "errors": [],
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(AirflowException, match="selection evidence"):
            mod.validate_data(str(report))


class TestSilverDependency:
    @pytest.mark.unit
    def test_ingest_waits_for_silver_before_master_can_start_xref(self):
        mod = _reload_dag_module()

        assert mod.trigger_silver._init_kwargs["wait_for_completion"] is True
        assert mod.trigger_silver._init_kwargs["poke_interval"] == 30
        assert mod.trigger_silver._init_kwargs["allowed_states"] == ["success"]
        assert mod.trigger_silver._init_kwargs["failed_states"] == ["failed"]
        assert mod.trigger_silver._init_kwargs["reset_dag_run"] is False
        assert mod.trigger_silver._init_kwargs["logical_date"] == (
            "{{ logical_date.isoformat() }}"
        )
        assert mod.seal_publication.upstream_task_ids == {"trigger_silver_transform"}
        assert mod.finalize_publication._init_kwargs["trigger_rule"] == "all_done"
