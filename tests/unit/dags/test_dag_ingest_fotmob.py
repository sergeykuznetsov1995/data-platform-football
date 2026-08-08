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
            "scope_plan_signature": "fmplan1-98c9a8f98ba8eaa14bfc8232b9667682e11e4fce27e120eee5ea9572b66e0385",
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
        assert "--scope" not in task.bash_command
        assert task.env["FOTMOB_SCOPE_JSON"] == "{{ params.scope | tojson }}"
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
        assert "/tmp/fotmob_result_{{ ts_nodash }}.json" in task.bash_command
        assert "ti.try_number" not in task.bash_command
        assert '/usr/bin/rm -f -- "/tmp/fotmob_result_{{ ts_nodash }}.json"' in (
            task.bash_command
        )
        generation = (
            "{{ (dag_run.conf.get('fotmob_publication') or {})"
            ".get('generation_id', '') }}"
        )
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
    def test_automatic_catalog_uses_real_acceptance_gate(self, tmp_path):
        import json
        from datetime import datetime, timezone

        from airflow.exceptions import AirflowException
        from scrapers.fotmob.catalog_contract import build_catalog_contract

        mod = _reload_dag_module()
        now = datetime.now(timezone.utc)
        contract = build_catalog_contract(
            catalog_batch_id="catalog-batch",
            catalog_content_hash="a" * 64,
            classifier_version="fotmob-men-v1",
            parser_version="fotmob-native-v2",
            entities=["season"],
            entity_policy={},
            included_ids=[47],
            scopes=[(47, "2025/2026")],
        ).as_dict()
        payload = {
            "run_id": "automatic-run",
            "mode": "refresh",
            "status": "success",
            "complete": True,
            "operations": [
                {
                    "entity": "competition_catalog",
                    "status": "success",
                    "errors": [],
                    "retryable": [],
                    "terminal": [],
                    "counts": {"competitions": 2},
                }
            ],
            "transport": {"attempts": 1, "direct_bytes": 1, "proxy_bytes": 0},
            "budget": {
                "requests": 1,
                "max_requests": 2,
                "direct_bytes": 1,
                "max_direct_bytes": 2,
                "proxy_bytes": 0,
                "max_proxy_bytes": 0,
            },
            "errors": [],
            "selection": {
                "entities": ["season"],
                "explicit_scopes": [],
                "competition_limit": 0,
                "season_limit": 0,
                "scope_lane": "current",
                "scope_plan_signature": contract["plan_signature"],
                "catalog_contract": contract,
                "catalog_ids": [47, 88],
                "catalog_decisions": [
                    {
                        "competition_id": 47,
                        "catalog_name": "Premier League",
                        "profile_name": "Premier League",
                        "source_gender": "male",
                        "source_age_group": "adult",
                        "source_type": "league",
                        "probe_status": "success",
                        "decision": "included",
                        "reason": "structurally confirmed adult men's competition",
                        "policy_rule": "include_structural_male_adult",
                        "classifier_version": "fotmob-men-v1",
                        "profile_target_key": "leagues?id=47",
                        "profile_content_hash": "b" * 64,
                    },
                    {
                        "competition_id": 88,
                        "catalog_name": "Women's League",
                        "profile_name": "Women's League",
                        "source_gender": "female",
                        "source_age_group": "adult",
                        "source_type": "league",
                        "probe_status": "success",
                        "decision": "excluded",
                        "reason": "women/female competition",
                        "policy_rule": "exclude_female",
                        "classifier_version": "fotmob-men-v1",
                        "profile_target_key": "leagues?id=88",
                        "profile_content_hash": "c" * 64,
                    },
                ],
                "planned_scopes": ["47=2025/2026"],
                "completed_scopes": ["47=2025/2026"],
                "scope_attempts": [
                    {
                        "competition_id": 47,
                        "source_season_key": "2025/2026",
                        "plan_signature": contract["plan_signature"],
                        "attempt_count": 1,
                        "last_attempt_at": now.isoformat(),
                        "next_retry_at": None,
                        "outcome": "success",
                        "reason": "scope completion committed",
                        "attempt_identities": ["automatic-run:47=2025/2026"],
                    }
                ],
                "completed_transfer_competition_ids": [],
                "transfer_plan_signature": None,
                "deferrals": [],
            },
        }
        report = tmp_path / "automatic.json"
        report.write_text(json.dumps(payload), encoding="utf-8")

        assert mod.validate_data(str(report))["status"] == "success"

        payload["selection"]["scope_plan_signature"] = "fmplan1-" + "0" * 64
        report.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(AirflowException, match="automatic catalog evidence failed"):
            mod.validate_data(str(report))

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
    def test_daily_evidence_validator_accepts_spaced_source_season(self):
        mod = _reload_dag_module()
        payload = _daily_report(mod)
        original = payload["selection"]["planned_scopes"][0]
        spaced = "42=2025 Apertura"
        payload["selection"]["planned_scopes"][0] = spaced
        payload["selection"]["completed_scopes"][0] = spaced
        for operation in payload["operations"]:
            metadata = operation.get("metadata") or {}
            if metadata.get("scope") == original:
                metadata["scope"] = spaced

        violations, summary = mod._validate_daily_selection(
            result=payload,
            selection=payload["selection"],
            entities=sorted(mod.FOTMOB_DAILY_ENTITIES),
            raw_scopes=[],
            budget=payload["budget"],
        )

        assert violations == []
        assert summary["planned_scopes"][0] == spaced

    @pytest.mark.unit
    def test_replay_evidence_validator_accepts_spaced_source_season(
        self, tmp_path, monkeypatch
    ):
        import json

        from scrapers.fotmob.planner import deterministic_plan_signature

        mod = _reload_dag_module()
        scope = "230=2025 Apertura"
        scope_sha256 = hashlib.sha256(f"{scope}\n".encode("utf-8")).hexdigest()
        contract = {
            "sha256": "a" * 64,
            "target_count": 1,
            "player_ids": [123],
            "targets": [
                {
                    "competition_id": 230,
                    "source_season_key": "2025 Apertura",
                    "team_id": 9,
                    "player_id": 123,
                }
            ],
        }
        monkeypatch.setattr(mod, "_source_refresh_contract", lambda: contract)
        monkeypatch.setattr(mod, "FOTMOB_DAILY_SCOPE_COUNT", 1)
        monkeypatch.setattr(mod, "FOTMOB_DAILY_SCOPE_SHA256", scope_sha256)
        generation_id = "11111111-1111-4111-8111-111111111111"
        payload = {
            "run_id": generation_id,
            "mode": "replay",
            "status": "incomplete",
            "complete": False,
            "errors": ["missing raw player input"],
            "selection": {
                "entities": mod.ISSUE_930_REPLAY_ENTITIES,
                "explicit_scopes": [scope],
                "competition_limit": 0,
                "season_limit": 0,
                "scope_plan_signature": deterministic_plan_signature(
                    mod.ISSUE_930_REPLAY_ENTITIES,
                    policy={
                        "match_policy": "finished_only",
                        "leaderboard_policy": "all_advertised",
                        "team_policy": "global_observed_snapshot",
                        "player_policy": "global_observed_snapshot",
                    },
                ),
                "planned_scopes": [scope],
                "completed_scopes": [],
                "replay_missing_player_inputs": {
                    "schema": mod.REPLAY_MISSING_INPUT_SCHEMA,
                    "failure_class": "missing_player_raw_inputs_only",
                    "missing_player_ids": [123],
                    "affected_scopes": [scope],
                },
            },
            "transport": {"attempts": 0, "direct_bytes": 0, "proxy_bytes": 0},
            "budget": {
                "requests": 0,
                "max_requests": 2_000,
                "direct_bytes": 0,
                "max_direct_bytes": 256 * 1024 * 1024,
                "proxy_bytes": 0,
                "max_proxy_bytes": 0,
            },
        }
        result_path = tmp_path / "replay.json"
        result_path.write_text(json.dumps(payload), encoding="utf-8")
        task_states = {
            "validate_publication_writer_fence": ("success", 1),
            "scrape_fotmob_data": ("failed", 1),
        }
        dag_run = type(
            "ReplayDagRun",
            (),
            {
                "conf": {"fotmob_publication": {"generation_id": generation_id}},
                "run_id": "issue930_replay_a1__" + generation_id.replace("-", ""),
                "get_task_instance": lambda _self, task_id: type(
                    "TaskInstance",
                    (),
                    {"state": task_states[task_id][0], "try_number": task_states[task_id][1]},
                )(),
            },
        )()

        proof = mod.prove_replay_missing_player_inputs(
            str(result_path),
            dag_run=dag_run,
            ti=type(
                "TaskInstance",
                (),
                {"task_id": mod.REPLAY_MISSING_INPUT_PROOF_TASK_ID, "try_number": 1},
            )(),
        )

        assert proof["scope_count"] == 1
        assert proof["scope_sha256"] == scope_sha256

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
                        "scope_plan_signature": "fmplan1-98c9a8f98ba8eaa14bfc8232b9667682e11e4fce27e120eee5ea9572b66e0385",
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
            "scope_plan_signature": "fmplan1-98c9a8f98ba8eaa14bfc8232b9667682e11e4fce27e120eee5ea9572b66e0385",
            "competition_limit": 0,
            "season_limit": 0,
        }

    @pytest.mark.unit
    def test_evidence_validator_accepts_spaced_exact_source_season(self, tmp_path):
        import json

        mod = _reload_dag_module()
        report = tmp_path / "report.json"
        payload = {
            "run_id": "run-1",
            "mode": "backfill",
            "status": "success",
            "complete": True,
            "operations": [
                {
                    "entity": "competition_catalog",
                    "status": "success",
                    "errors": [],
                    "retryable": [],
                    "terminal": [],
                    "counts": {"competitions": 1},
                }
            ],
            "transport": {"attempts": 1, "direct_bytes": 1, "proxy_bytes": 0},
            "budget": {
                "requests": 1,
                "max_requests": 2,
                "direct_bytes": 1,
                "max_direct_bytes": 2,
                "proxy_bytes": 0,
                "max_proxy_bytes": 0,
            },
            "errors": [],
            "selection": {
                "entities": ["season"],
                "explicit_scopes": ["230=2025 Apertura"],
                "competition_limit": 0,
                "season_limit": 0,
                "scope_plan_signature": "fmplan1-"
                "98c9a8f98ba8eaa14bfc8232b9667682e11e4fce27e120eee5ea9572b66e0385",
            },
        }
        report.write_text(json.dumps(payload), encoding="utf-8")

        assert mod.validate_data(str(report))["selection"]["explicit_scope_count"] == 1

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
    @pytest.mark.parametrize(
        ("tables", "expected"),
        [
            ([], False),
            (["iceberg.bronze.fotmob_competition_scope_observations"], False),
            (["iceberg.bronze.fotmob_catalog_batches"], False),
            (["iceberg.bronze.fotmob_ingest_manifest"], False),
            (["iceberg.bronze.fotmob_matches"], True),
            (["iceberg.bronze.fotmob_transfer_events"], True),
        ],
    )
    def test_silver_gate_reads_validated_committed_inputs(self, tables, expected):
        mod = _reload_dag_module()

        class _TI:
            def xcom_pull(self, *, task_ids):
                assert task_ids == "validate_data"
                return {"bronze_inputs_changed": tables}

        assert mod._should_transform(ti=_TI()) is expected

    @pytest.mark.unit
    def test_validation_normalizes_changed_bronze_inputs(self, tmp_path):
        import json

        mod = _reload_dag_module()
        payload = _daily_report(mod)
        payload["tables"] = [
            "iceberg.bronze.fotmob_matches",
            " iceberg.bronze.fotmob_matches ",
            "iceberg.bronze.fotmob_competition_scope_observations",
        ]
        report = tmp_path / "result.json"
        report.write_text(json.dumps(payload), encoding="utf-8")

        assert mod.validate_data(str(report))["bronze_inputs_changed"] == [
            "iceberg.bronze.fotmob_competition_scope_observations",
            "iceberg.bronze.fotmob_matches",
        ]

    @pytest.mark.unit
    def test_existing_silver_input_set_is_complete_and_native(self):
        mod = _reload_dag_module()

        assert mod.FOTMOB_SILVER_BRONZE_INPUTS == frozenset(
            {
                "iceberg.bronze.fotmob_competition_seasons",
                "iceberg.bronze.fotmob_season_teams",
                "iceberg.bronze.fotmob_matches",
                "iceberg.bronze.fotmob_match_payloads",
                "iceberg.bronze.fotmob_standings",
                "iceberg.bronze.fotmob_leaderboards",
                "iceberg.bronze.fotmob_squad_snapshots",
                "iceberg.bronze.fotmob_player_snapshots",
                "iceberg.bronze.fotmob_team_snapshots",
                "iceberg.bronze.fotmob_transfer_events",
            }
        )

    @pytest.mark.unit
    def test_ingest_waits_for_silver_before_master_can_start_xref(self):
        from airflow.operators.python import PythonOperator

        PythonOperator._instances.clear()
        mod = _reload_dag_module()

        assert mod.trigger_silver._init_kwargs["wait_for_completion"] is True
        assert mod.trigger_silver._init_kwargs["poke_interval"] == 30
        assert mod.trigger_silver._init_kwargs["allowed_states"] == ["success"]
        assert mod.trigger_silver._init_kwargs["failed_states"] == ["failed"]
        assert mod.trigger_silver._init_kwargs["reset_dag_run"] is False
        assert mod.trigger_silver._init_kwargs["logical_date"] == (
            "{{ logical_date.isoformat() }}"
        )
        silver_triggers = [
            task
            for task in PythonOperator._instances
            if task.task_id == "trigger_silver_transform"
        ]
        assert silver_triggers == [mod.trigger_silver]
        assert mod.transform_gate._init_kwargs["ignore_downstream_trigger_rules"] is False
        assert mod.transform_gate._init_kwargs.get("op_kwargs", {}) == {}
        bronze_candidate = next(
            task
            for task in PythonOperator._instances
            if task.task_id == "record_bronze_only_publication_candidate"
        )
        assert bronze_candidate.python_callable is (
            mod.record_fotmob_bronze_only_candidate
        )
        assert bronze_candidate.upstream_task_ids == {"validate_data"}
        assert bronze_candidate._init_kwargs["op_kwargs"] == {
            "validation_task_id": "validate_data",
            "silver_input_tables": sorted(mod.FOTMOB_SILVER_BRONZE_INPUTS),
        }
        assert mod.seal_publication.upstream_task_ids == {
            "record_bronze_only_publication_candidate",
            "trigger_silver_transform",
        }
        assert mod.seal_publication._init_kwargs["trigger_rule"] == (
            "none_failed_min_one_success"
        )
        # The Silver child is synchronous and failed DQ is explicitly a failed
        # state, so the seal cannot publish readiness after DQ failure.
        assert mod.trigger_silver._init_kwargs["failed_states"] == ["failed"]
        assert mod.finalize_publication._init_kwargs["trigger_rule"] == "all_done"
