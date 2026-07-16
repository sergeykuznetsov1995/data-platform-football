from __future__ import annotations

import ast
import importlib.util
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[3]
MODULE_NAME = "_test_run_fbref_canary"
SPEC = importlib.util.spec_from_file_location(
    MODULE_NAME,
    ROOT / "scripts" / "research" / "run_fbref_canary.py",
)
assert SPEC is not None and SPEC.loader is not None
canary = importlib.util.module_from_spec(SPEC)
sys.modules[MODULE_NAME] = canary
SPEC.loader.exec_module(canary)

pytestmark = pytest.mark.unit
NOW = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)


class FakeControl:
    def __init__(self, events: list[str], *, finish_raises: bool = False) -> None:
        self.events = events
        self.finish_raises = finish_raises
        self.cohort = []
        self.forced_due_target = None

    def upsert_frontier_target(self, target) -> None:
        self.events.append("force_due")
        self.forced_due_target = target
        source_target = canary.competition_index_target()
        assert target.target_id == source_target.target_id
        assert target.canonical_url == source_target.canonical_url
        assert target.source_ids == source_target.source_ids
        assert target.page_kind == "competition_index"
        assert target.next_fetch_at is not None
        assert target.next_fetch_at.tzinfo is not None

    def create_run_cohort(self, run_id, cohort) -> int:
        self.events.append("cohort")
        self.cohort = list(cohort)
        assert len(self.cohort) == 1
        assert self.cohort[0].ordinal == 0
        assert self.cohort[0].target_id == "fbref:competition_index:all"
        assert self.cohort[0].logical_refresh_id == (
            canary.make_logical_refresh_id(run_id, self.cohort[0].target_id)
        )
        return 1

    def finish_run(self, run_id, *, succeeded: bool) -> None:
        self.events.append(f"finish:{succeeded}")
        if self.finish_raises:
            raise RuntimeError("postgresql://user:secret@db/control")


def _wave(**overrides):
    values = {
        "cohort_size": 1,
        "claimed": 1,
        "fetched": 1,
        "recovered_from_raw": 0,
        "parsed": 0,
        "seeded": 0,
        "skipped_ineligible": 0,
        "requests": 21,
        "wire_bytes": 1_024,
        "decoded_html_bytes": 900,
        "browser_document_bytes": 100,
        "browser_asset_bytes": 24,
        "browser_bootstraps": 1,
        "failures": [],
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class FakePipeline:
    def __init__(
        self,
        *,
        fail_stage: str | None = None,
        finish_raises: bool = False,
    ) -> None:
        self.events: list[str] = []
        self.fail_stage = fail_stage
        self.control = FakeControl(self.events, finish_raises=finish_raises)
        self.settings = None
        self.page_kinds = []

    def _fail(self, stage: str) -> None:
        if self.fail_stage == stage:
            raise RuntimeError("postgresql://user:secret@db/control")

    def initialize_run(self, *, airflow_run_id, dag_id, settings):
        self.events.append("initialize")
        self.settings = settings
        self._fail("initialize")
        return canary.make_control_run_id(airflow_run_id, dag_id=dag_id)

    def seed_competition_index(self) -> str:
        self.events.append("seed")
        self._fail("seed")
        return "fbref:competition_index:all"

    def fetch_wave(self, run_id, *, worker_id, page_kinds, settings):
        self.events.append("fetch")
        self._fail("fetch")
        assert run_id in worker_id
        assert settings is self.settings
        self.page_kinds.append(tuple(page_kinds))
        return _wave()

    def parse_wave(self, run_id, *, page_kinds, settings):
        self.events.append("parse")
        self._fail("parse")
        assert run_id
        assert settings is self.settings
        self.page_kinds.append(tuple(page_kinds))
        return _wave(
            cohort_size=0,
            claimed=0,
            fetched=0,
            parsed=1,
            requests=0,
            seeded=3,
            browser_bootstraps=0,
        )

    def validate_and_finish(self, run_id):
        self.events.append("validate")
        self._fail("validate")
        self.control.finish_run(run_id, succeeded=True)
        return {
            "requests_used": 21,
            "bytes_used": 1_148,
            "budget_exceeded": False,
            "target_counts": {
                "succeeded": 1,
                "postgresql://user:secret@db/control": 99,
            },
            "dataset_validation_counts": {"succeeded": 1},
            "traffic_by_page_kind": {
                "competition_index": {
                    "page_kind": "competition_index",
                    "network_attempts": 1,
                    "warm_http_successes": 1,
                    "failed_network_attempts": 0,
                    "unclassified_failures": 0,
                    "classified_retries": 0,
                    "duplicate_fetch_violations": 0,
                    "warm_http_success_rate": 1.0,
                    "p50_latency_ms": 123.5,
                    "p95_latency_ms": 130.25,
                    "p50_http_wire_bytes": 2_048.0,
                    "p95_http_wire_bytes": 2_304.0,
                    "p50_provider_billed_bytes": 4_096.0,
                    "p95_provider_billed_bytes": 4_352.0,
                    "http_wire_bytes": 2_048,
                    "decoded_html_bytes": 1_800,
                    "compressed_raw_bytes": 700,
                    "provider_billed_bytes": 4_096,
                    "metadata": {"secret": "must-not-escape"},
                    "control_db_uri": "postgresql://user:secret@db/control",
                },
                "postgresql://user:secret@db/control": {
                    "network_attempts": 999
                },
            },
            "traffic_totals": {
                "network_attempts": 1,
                "warm_http_successes": 1,
                "failed_network_attempts": 0,
                "unclassified_failures": 0,
                "classified_retries": 0,
                "duplicate_fetch_violations": 0,
                "warm_http_success_rate": 1.0,
                "unclassified_failure_rate": 0.0,
                "control_db_uri": "postgresql://user:secret@db/control",
            },
            "session_metrics": {
                "sessions": 1,
                "max_bootstraps_per_session": 1,
                "browser_bootstrap_requests": 7,
                "browser_document_bytes": 1_024,
                "browser_asset_bytes": 2_048,
                "http_requests": 1,
                "http_wire_bytes": 2_048,
                "decoded_html_bytes": 1_800,
                "compressed_raw_bytes": 700,
                "provider_billed_bytes": 4_096,
                "metadata": {"secret": "must-not-escape"},
            },
            "competition_coverage": [
                {
                    "crawl_state": "eligible",
                    "lifecycle_state": "active",
                    "count": 128,
                    "control_db_uri": "postgresql://user:secret@db/control",
                },
                {
                    "crawl_state": "skipped_female",
                    "lifecycle_state": "active",
                    "count": 24,
                },
            ],
            "table_availability": {
                "available": 1,
                "empty": 2,
                "postgresql://user:secret@db/control": 99,
            },
            "sentinel_coverage": {
                "Premier League": {
                    "published": True,
                    "competition_id": "9",
                    "gender": "male",
                    "eligibility": "eligible",
                    "metadata": {"secret": "must-not-escape"},
                },
                "Champions League": {
                    "published": False,
                    "competition_id": None,
                    "gender": None,
                    "eligibility": None,
                },
                "postgresql://user:secret@db/control": {
                    "published": True
                },
            },
            "female_downstream_targets": 0,
            "unknown_gender_downstream_targets": 0,
            "metadata": {"secret": "must-not-escape"},
            "control_db_uri": "postgresql://user:secret@db/control",
        }


def _config(tmp_path: Path, **overrides):
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("proxy.example:8080:user:password\n", encoding="utf-8")
    values = {
        "logical_run_label": canary.generate_logical_run_label(
            "issue-923",
            now=NOW,
            nonce=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        ),
        "proxy_file": proxy_file,
        "request_limit": 25,
        "byte_limit_mb": 25,
    }
    values.update(overrides)
    return canary.CanaryConfig(**values)


def test_canary_executes_one_index_lifecycle_with_fixed_bounds(tmp_path) -> None:
    pipeline = FakePipeline()

    result = canary.run_canary(
        _config(tmp_path), pipeline=pipeline, clock=lambda: NOW
    )

    assert pipeline.events == [
        "initialize",
        "seed",
        "force_due",
        "cohort",
        "fetch",
        "parse",
        "validate",
        "finish:True",
    ]
    assert pipeline.page_kinds == [
        ("competition_index",),
        ("competition_index",),
    ]
    assert pipeline.settings.run_type == "current"
    assert pipeline.settings.request_limit == 25
    assert pipeline.settings.byte_limit == 25 * canary.MIB
    assert pipeline.settings.shard_size == 1
    assert pipeline.settings.target_request_reservation == 2
    assert pipeline.settings.proxy_file == str(tmp_path / "proxies.txt")
    assert pipeline.control.forced_due_target.next_fetch_at == NOW
    assert result["status"] == "succeeded"
    assert result["scope"] == {
        "page_kinds": ["competition_index"],
        "shard_size": 1,
        "parse_mode": "offline_raw_manifest",
    }
    assert result["silver_triggered"] is False


def test_success_json_whitelists_summary_and_proxy_secrets(tmp_path) -> None:
    config = _config(tmp_path)

    result = canary.run_canary(config, pipeline=FakePipeline())
    rendered = json.dumps(result, sort_keys=True)
    validation = result["validation"]

    assert "secret" not in rendered
    assert "password" not in rendered
    assert str(config.proxy_file) not in rendered
    assert "control_db_uri" not in rendered
    assert "metadata" not in rendered
    assert validation["traffic_by_page_kind"] == {
        "competition_index": {
            "network_attempts": 1,
            "warm_http_successes": 1,
            "failed_network_attempts": 0,
            "unclassified_failures": 0,
            "classified_retries": 0,
            "duplicate_fetch_violations": 0,
            "warm_http_success_rate": 1.0,
            "p50_latency_ms": 123.5,
            "p95_latency_ms": 130.25,
            "p50_http_wire_bytes": 2_048.0,
            "p95_http_wire_bytes": 2_304.0,
            "p50_provider_billed_bytes": 4_096.0,
            "p95_provider_billed_bytes": 4_352.0,
            "http_wire_bytes": 2_048,
            "decoded_html_bytes": 1_800,
            "compressed_raw_bytes": 700,
            "provider_billed_bytes": 4_096,
        }
    }
    assert validation["traffic_totals"] == {
        "network_attempts": 1,
        "warm_http_successes": 1,
        "failed_network_attempts": 0,
        "unclassified_failures": 0,
        "classified_retries": 0,
        "duplicate_fetch_violations": 0,
        "warm_http_success_rate": 1.0,
        "unclassified_failure_rate": 0.0,
    }
    assert validation["session_metrics"] == {
        "sessions": 1,
        "max_bootstraps_per_session": 1,
        "browser_bootstrap_requests": 7,
        "browser_document_bytes": 1_024,
        "browser_asset_bytes": 2_048,
        "http_requests": 1,
        "http_wire_bytes": 2_048,
        "decoded_html_bytes": 1_800,
        "compressed_raw_bytes": 700,
        "provider_billed_bytes": 4_096,
    }
    assert validation["competition_coverage"] == [
        {
            "crawl_state": "eligible",
            "lifecycle_state": "active",
            "count": 128,
        },
        {
            "crawl_state": "skipped_female",
            "lifecycle_state": "active",
            "count": 24,
        },
    ]
    assert validation["table_availability"] == {"available": 1, "empty": 2}
    assert validation["sentinel_coverage"] == {
        "Premier League": {
            "published": True,
            "competition_id": "9",
            "gender": "male",
            "eligibility": "eligible",
        },
        "Champions League": {
            "published": False,
            "competition_id": None,
            "gender": None,
            "eligibility": None,
        },
    }
    assert validation["female_downstream_targets"] == 0
    assert validation["unknown_gender_downstream_targets"] == 0


@pytest.mark.parametrize(
    "stage,expected_events",
    [
        ("initialize", ["initialize", "finish:False"]),
        ("seed", ["initialize", "seed", "finish:False"]),
        (
            "fetch",
            [
                "initialize",
                "seed",
                "force_due",
                "cohort",
                "fetch",
                "finish:False",
            ],
        ),
        (
            "parse",
            [
                "initialize",
                "seed",
                "force_due",
                "cohort",
                "fetch",
                "parse",
                "finish:False",
            ],
        ),
        (
            "validate",
            [
                "initialize",
                "seed",
                "force_due",
                "cohort",
                "fetch",
                "parse",
                "validate",
                "finish:False",
            ],
        ),
    ],
)
def test_every_runtime_failure_best_effort_finishes_failed(
    tmp_path, stage, expected_events
) -> None:
    pipeline = FakePipeline(fail_stage=stage)

    with pytest.raises(canary.CanaryExecutionError) as raised:
        canary.run_canary(_config(tmp_path), pipeline=pipeline)

    assert pipeline.events == expected_events
    assert raised.value.failure_finish_attempted is True
    assert raised.value.error_class == "RuntimeError"
    assert "secret" not in json.dumps(raised.value.as_dict())
    assert "secret" not in str(raised.value)


def test_finish_failure_never_masks_or_leaks_original_error(tmp_path) -> None:
    pipeline = FakePipeline(fail_stage="fetch", finish_raises=True)

    with pytest.raises(canary.CanaryExecutionError) as raised:
        canary.run_canary(_config(tmp_path), pipeline=pipeline)

    assert raised.value.stage == "fetch_competition_index"
    assert raised.value.error_class == "RuntimeError"
    assert raised.value.failure_finish_attempted is True
    assert pipeline.events[-1] == "finish:False"
    assert "secret" not in json.dumps(raised.value.as_dict())


def test_generated_logical_labels_are_unique_and_traceable() -> None:
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)

    first = canary.generate_logical_run_label("issue-923", now=now)
    second = canary.generate_logical_run_label("issue-923", now=now)

    assert first != second
    assert first.startswith("issue-923-20260711T120000Z-")
    assert len(first.rsplit("-", 1)[-1]) == 32


@pytest.mark.parametrize(
    "field,value",
    [
        ("request_limit", 20),
        ("request_limit", 21),
        ("request_limit", 26),
        ("byte_limit_mb", 5),
        ("byte_limit_mb", 26),
    ],
)
def test_programmatic_config_rejects_unsafe_budget_bounds(
    tmp_path, field, value
) -> None:
    with pytest.raises(canary.CanaryConfigurationError):
        _config(tmp_path, **{field: value})


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--request-limit", "20"),
        ("--request-limit", "21"),
        ("--request-limit", "26"),
        ("--byte-limit-mb", "5"),
        ("--byte-limit-mb", "26"),
    ],
)
def test_cli_rejects_unsafe_budget_bounds(tmp_path, flag, value) -> None:
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("proxy\n", encoding="utf-8")

    with pytest.raises(SystemExit) as raised:
        canary.build_cli_parser().parse_args(
            ["--proxy-file", str(proxy_file), flag, value]
        )

    assert raised.value.code == 2


def test_cli_requires_an_explicit_proxy_file() -> None:
    with pytest.raises(SystemExit) as raised:
        canary.build_cli_parser().parse_args([])

    assert raised.value.code == 2


def test_main_returns_redacted_json_on_runtime_failure(tmp_path, capsys) -> None:
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("proxy.example:8080:user:password\n", encoding="utf-8")

    exit_code = canary.main(
        ["--proxy-file", str(proxy_file), "--run-label", "issue-923"],
        pipeline=FakePipeline(fail_stage="fetch"),
    )

    rendered = capsys.readouterr().out
    payload = json.loads(rendered)
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["stage"] == "fetch_competition_index"
    assert payload["failure_finish_attempted"] is True
    assert "secret" not in rendered
    assert "password" not in rendered
    assert str(proxy_file) not in rendered


def test_script_has_no_airflow_silver_or_dag_trigger_imports() -> None:
    source = (
        ROOT / "scripts" / "research" / "run_fbref_canary.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")

    assert not any(name == "airflow" or name.startswith("airflow.") for name in imported)
    assert "TriggerDagRunOperator" not in source
    assert "dag_transform_fbref_silver" not in source
    assert "https://fbref.com/en/comps/" not in source


def test_live_wrapper_enforces_separate_kernel_quotas() -> None:
    source = (
        ROOT / "scripts" / "research" / "run_fbref_canary_guarded.sh"
    ).read_text(encoding="utf-8")

    assert 'RUN_LIVE_FBREF_CANARY:-}" != "1"' in source
    assert "INGRESS_LIMIT_BYTES=$((18 * 1024 * 1024))" in source
    assert "EGRESS_LIMIT_BYTES=$((4 * 1024 * 1024))" in source
    assert source.count("-m quota --quota") == 2
    assert "--request-limit \"$REQUEST_LIMIT\"" in source
    assert "--byte-limit-mb \"$BYTE_LIMIT_MB\"" in source
    assert '"$REPO_ROOT/scripts/compose.sh"' in source
    assert "docker compose" not in source
    assert "--volume" not in source
    assert 'docker cp "$PROXY_FILE"' in source
    assert "COMPOSE_PID=$!" in source
