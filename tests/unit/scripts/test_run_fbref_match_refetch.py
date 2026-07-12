from __future__ import annotations

import ast
import importlib.util
import itertools
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[3]
MODULE_NAME = "_test_run_fbref_match_refetch"
SPEC = importlib.util.spec_from_file_location(
    MODULE_NAME,
    ROOT / "scripts" / "research" / "run_fbref_match_refetch.py",
)
assert SPEC is not None and SPEC.loader is not None
refetch = importlib.util.module_from_spec(SPEC)
sys.modules[MODULE_NAME] = refetch
SPEC.loader.exec_module(refetch)

pytestmark = pytest.mark.unit
NOW = datetime(2026, 7, 12, 12, 0, tzinfo=timezone.utc)
MATCH_IDS = ("9c4f2bcd", "0a1b2c3d", "deadbeef")


def _target_payload(match_id: str = "9c4f2bcd", **overrides) -> dict:
    values = {
        "match_id": match_id,
        "url": f"https://fbref.com/en/matches/{match_id}/Some-Match",
        "competition_id": "9",
        "season_id": "2023-2024",
    }
    values.update(overrides)
    return values


_targets_file_counter = itertools.count()


def _write_targets(tmp_path: Path, payload: object) -> Path:
    # A distinct file per call: _config() builds its default targets file while
    # evaluating its own body, which would otherwise overwrite the file an
    # override argument had just written.
    targets_file = tmp_path / f"targets_{next(_targets_file_counter)}.json"
    targets_file.write_text(json.dumps(payload), encoding="utf-8")
    return targets_file


def _config(tmp_path: Path, **overrides):
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("proxy.example:8080:user:password\n", encoding="utf-8")
    values = {
        "logical_run_label": refetch.generate_logical_run_label(
            "issue-934",
            now=NOW,
            nonce=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        ),
        "proxy_file": proxy_file,
        "targets_file": _write_targets(
            tmp_path,
            [_target_payload(match_id) for match_id in MATCH_IDS],
        ),
        "request_limit": 25,
        "byte_limit_mb": 25,
    }
    values.update(overrides)
    return refetch.RefetchConfig(**values)


def _targets(config):
    return refetch.load_refetch_targets(config.targets_file)


class FakeRawStore:
    def __init__(
        self,
        available_target_ids: frozenset[str] | set[str] = frozenset(),
        *,
        bridge_raises: bool = False,
    ) -> None:
        self.available_target_ids = set(available_target_ids)
        self.bridge_raises = bridge_raises
        self.bridge_calls: list[tuple[str, str]] = []
        self.imported_refresh_ids: set[str] = set()

    def import_fetch_from_available_raw(
        self, target, *, logical_refresh_id, attempt_id=None
    ):
        if self.bridge_raises:
            raise RuntimeError("postgresql://user:secret@db/control")
        assert target.page_kind == "match"
        self.bridge_calls.append((target.target_id, logical_refresh_id))
        if target.target_id in self.available_target_ids:
            self.imported_refresh_ids.add(logical_refresh_id)
            return SimpleNamespace(
                target_id=target.target_id,
                logical_refresh_id=logical_refresh_id,
            )
        return None


class FakeControl:
    def __init__(self, events: list[str], *, finish_raises: bool = False) -> None:
        self.events = events
        self.finish_raises = finish_raises
        self.frontier = []
        self.cohort = []
        self.summary_target_counts = None

    def upsert_frontier_target(self, target) -> None:
        self.events.append("force_due")
        self.frontier.append(target)
        assert target.page_kind == "match"
        assert target.refresh_policy == "historical_once"
        assert target.next_fetch_at is not None
        assert target.next_fetch_at.tzinfo is not None

    def create_run_cohort(self, run_id, cohort) -> int:
        self.events.append("cohort")
        self.cohort = list(cohort)
        for ordinal, member in enumerate(self.cohort):
            assert member.ordinal == ordinal
            assert member.logical_refresh_id == (
                refetch.make_logical_refresh_id(run_id, member.target_id)
            )
        return len(self.cohort)

    def get_run_summary(self, run_id, **kwargs):
        if self.summary_target_counts is None:
            raise RuntimeError("postgresql://user:secret@db/control")
        return {"target_counts": dict(self.summary_target_counts)}

    def finish_run(self, run_id, *, succeeded: bool) -> None:
        self.events.append(f"finish:{succeeded}")
        if self.finish_raises:
            raise RuntimeError("postgresql://user:secret@db/control")


def _wave(**overrides):
    values = {
        "cohort_size": 0,
        "claimed": 0,
        "fetched": 0,
        "recovered_from_raw": 0,
        "parsed": 0,
        "seeded": 0,
        "skipped_ineligible": 0,
        "requests": 0,
        "wire_bytes": 0,
        "decoded_html_bytes": 0,
        "browser_document_bytes": 0,
        "browser_asset_bytes": 0,
        "browser_bootstraps": 0,
        "failures": [],
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class FakePipeline:
    def __init__(
        self,
        *,
        raw_store: FakeRawStore | None = None,
        fail_stage: str | None = None,
        finish_raises: bool = False,
    ) -> None:
        self.events: list[str] = []
        self.fail_stage = fail_stage
        self.control = FakeControl(self.events, finish_raises=finish_raises)
        self.raw_store = raw_store if raw_store is not None else FakeRawStore()
        self.settings = None
        self.page_kinds = []
        self.fetch_calls = 0

    def _fail(self, stage: str) -> None:
        if self.fail_stage == stage:
            raise RuntimeError("postgresql://user:secret@db/control")

    def initialize_run(self, *, airflow_run_id, dag_id, settings):
        self.events.append("initialize")
        self.settings = settings
        self._fail("initialize")
        return refetch.make_control_run_id(airflow_run_id, dag_id=dag_id)

    def fetch_wave(self, run_id, *, worker_id, page_kinds, settings):
        self.events.append("fetch")
        self.fetch_calls += 1
        self._fail("fetch")
        assert run_id in worker_id
        assert settings is self.settings
        self.page_kinds.append(tuple(page_kinds))
        cohort = self.control.cohort
        # Production clamps the wave before it claims: one clearance bootstrap
        # is reserved up front and every target reserves both HTTP attempts, so
        # a 25-request budget can only ever claim two targets — recovering a
        # target from raw does not buy back a claim slot.
        request_capacity = max(
            0,
            (settings.request_limit - settings.bootstrap_request_reservation)
            // refetch.MAX_TARGET_HTTP_ATTEMPTS,
        )
        byte_capacity = settings.byte_limit // settings.request_reservation_bytes
        claim_limit = min(
            len(cohort), settings.shard_size, request_capacity, byte_capacity
        )
        claimed_members = cohort[:claim_limit]
        recovered = sum(
            1
            for member in claimed_members
            if member.logical_refresh_id in self.raw_store.imported_refresh_ids
        )
        fetched = len(claimed_members) - recovered
        return _wave(
            cohort_size=len(cohort),
            claimed=len(claimed_members),
            fetched=fetched,
            recovered_from_raw=recovered,
            requests=0 if fetched == 0 else 20 + fetched,
            wire_bytes=2_048 * fetched,
            decoded_html_bytes=1_900 * fetched,
            browser_bootstraps=0 if fetched == 0 else 1,
        )

    def parse_wave(self, run_id, *, page_kinds, settings):
        self.events.append("parse")
        self._fail("parse")
        assert run_id
        assert settings is self.settings
        self.page_kinds.append(tuple(page_kinds))
        return _wave(parsed=len(self.control.cohort))

    def validate_and_finish(self, run_id):
        self.events.append("validate")
        self._fail("validate")
        self.control.finish_run(run_id, succeeded=True)
        return {
            "requests_used": 24,
            "bytes_used": 9_000_000,
            "budget_exceeded": False,
            "target_counts": {
                "succeeded": 3,
                "postgresql://user:secret@db/control": 99,
            },
            "dataset_validation_counts": {"succeeded": 12},
            "traffic_by_page_kind": {
                "match": {
                    "network_attempts": 2,
                    "warm_http_successes": 2,
                    "failed_network_attempts": 0,
                    "unclassified_failures": 0,
                    "classified_retries": 0,
                    "duplicate_fetch_violations": 0,
                    "warm_http_success_rate": 1.0,
                    "metadata": {"secret": "must-not-escape"},
                    "control_db_uri": "postgresql://user:secret@db/control",
                }
            },
            "traffic_totals": {
                "network_attempts": 2,
                "warm_http_successes": 2,
                "warm_http_success_rate": 1.0,
                "unclassified_failure_rate": 0.0,
                "control_db_uri": "postgresql://user:secret@db/control",
            },
            "session_metrics": {
                "sessions": 1,
                "max_bootstraps_per_session": 1,
                "metadata": {"secret": "must-not-escape"},
            },
            "sentinel_coverage": {
                "Premier League": {
                    "published": True,
                    "competition_id": "9",
                    "gender": "male",
                    "eligibility": "eligible",
                }
            },
            "metadata": {"secret": "must-not-escape"},
            "control_db_uri": "postgresql://user:secret@db/control",
        }


def test_refetch_executes_bounded_lifecycle_with_raw_bridge(tmp_path) -> None:
    config = _config(tmp_path)
    targets = _targets(config)
    pipeline = FakePipeline(
        raw_store=FakeRawStore(available_target_ids={"fbref:match:9c4f2bcd"})
    )

    result = refetch.run_refetch(
        config, targets, pipeline=pipeline, clock=lambda: NOW
    )

    assert pipeline.events == [
        "initialize",
        "force_due",
        "force_due",
        "force_due",
        "cohort",
        "fetch",
        "parse",
        "validate",
        "finish:True",
    ]
    assert pipeline.page_kinds == [("match",), ("match",)]
    assert pipeline.fetch_calls == 1
    assert pipeline.settings.run_type == "backfill"
    assert pipeline.settings.request_limit == 25
    assert pipeline.settings.byte_limit == 25 * refetch.MIB
    assert pipeline.settings.shard_size == 3
    assert pipeline.settings.target_request_reservation == 2
    assert pipeline.settings.proxy_file == str(tmp_path / "proxies.txt")
    assert result["status"] == "succeeded"
    assert result["scope"] == {
        "page_kinds": ["match"],
        "shard_size": 3,
        "run_type": "backfill",
        "parse_mode": "offline_raw_manifest",
    }
    # A 25-request budget reserves one clearance bootstrap (20) and both HTTP
    # attempts per target, so the wave claims two of the three targets and the
    # remainder stays pending for the next invocation.
    assert result["counts"] == {
        "targets": 3,
        "recovered_from_raw": 1,
        "fetched": 1,
        "failed": 0,
        "pending": 1,
    }
    assert result["fetch"]["recovered_from_raw"] == 1
    assert result["fetch"]["fetched"] == 1
    assert result["silver_triggered"] is False


def test_frontier_upserts_are_historical_once_and_due_now(tmp_path) -> None:
    config = _config(tmp_path)
    targets = _targets(config)
    pipeline = FakePipeline()

    refetch.run_refetch(config, targets, pipeline=pipeline, clock=lambda: NOW)

    assert len(pipeline.control.frontier) == 3
    for target, forced in zip(targets, pipeline.control.frontier):
        assert forced.target_id == target.page_target.target_id
        assert forced.canonical_url == target.page_target.canonical_url
        assert forced.source_ids == target.page_target.source_ids
        assert forced.refresh_policy == "historical_once"
        assert forced.next_fetch_at == NOW


def test_bridge_probes_every_cohort_member_before_the_wave(tmp_path) -> None:
    config = _config(tmp_path)
    targets = _targets(config)
    raw_store = FakeRawStore(available_target_ids={"fbref:match:deadbeef"})
    pipeline = FakePipeline(raw_store=raw_store)

    result = refetch.run_refetch(config, targets, pipeline=pipeline)

    run_id = result["control_run_id"]
    assert raw_store.bridge_calls == [
        (
            target.page_target.target_id,
            refetch.make_logical_refresh_id(
                run_id, target.page_target.target_id
            ),
        )
        for target in targets
    ]
    # Raw exists for the third target, which this budget cannot claim yet, so
    # the bridge still probed it but the wave fetched the first two.
    assert result["counts"]["recovered_from_raw"] == 0
    assert result["counts"]["fetched"] == 2
    assert result["counts"]["pending"] == 1


def test_single_fetch_wave_even_when_every_target_is_recovered(tmp_path) -> None:
    match_ids = tuple(f"{index:08x}" for index in range(5))
    config = _config(
        tmp_path,
        targets_file=_write_targets(
            tmp_path, [_target_payload(match_id) for match_id in match_ids]
        ),
    )
    targets = _targets(config)
    pipeline = FakePipeline(
        raw_store=FakeRawStore(
            available_target_ids={
                f"fbref:match:{match_id}" for match_id in match_ids
            }
        )
    )

    result = refetch.run_refetch(config, targets, pipeline=pipeline)

    # Recovering from raw costs no network, but it does not buy back a claim
    # slot: the wave is clamped before it knows which targets are recoverable.
    assert pipeline.fetch_calls == 1
    assert result["counts"] == {
        "targets": 5,
        "recovered_from_raw": 2,
        "fetched": 0,
        "failed": 0,
        "pending": 3,
    }
    assert result["fetch"]["requests"] == 0


def test_success_json_whitelists_summary_and_proxy_secrets(tmp_path) -> None:
    config = _config(tmp_path)

    result = refetch.run_refetch(config, _targets(config), pipeline=FakePipeline())
    rendered = json.dumps(result, sort_keys=True)

    assert "secret" not in rendered
    assert "password" not in rendered
    assert str(config.proxy_file) not in rendered
    assert str(config.targets_file) not in rendered
    assert "control_db_uri" not in rendered
    assert "metadata" not in rendered
    assert result["validation"]["target_counts"] == {"succeeded": 3}
    assert result["validation"]["traffic_by_page_kind"].keys() == {"match"}


def test_targets_build_page_targets_with_all_three_source_ids(tmp_path) -> None:
    targets_file = _write_targets(
        tmp_path,
        [
            _target_payload(
                "9C4F2BCD",
                url="https://www.fbref.com/en/matches/9C4F2BCD/Some-Match/",
            )
        ],
    )

    (target,) = refetch.load_refetch_targets(targets_file)

    assert target.match_id == "9c4f2bcd"
    assert target.page_target.page_kind == "match"
    assert target.page_target.target_id == "fbref:match:9c4f2bcd"
    assert target.page_target.canonical_url == (
        "https://fbref.com/en/matches/9c4f2bcd"
    )
    assert target.page_target.source_ids == {
        "match_id": "9c4f2bcd",
        "competition_id": "9",
        "season_id": "2023-2024",
    }


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {},
        "not-a-list",
        [_target_payload(match_id=f"{index:08x}") for index in range(26)],
        ["not-an-object"],
        [_target_payload(match_id="")],
        [_target_payload(competition_id="")],
        [_target_payload(season_id=" ")],
        [{key: value for key, value in _target_payload().items() if key != "url"}],
        [_target_payload(url="https://example.com/en/matches/9c4f2bcd/X")],
        [_target_payload(url="https://fbref.com/en/comps/9/Premier-League")],
        [_target_payload(url="https://fbref.com/en/matches/0a1b2c3d/Other")],
        [_target_payload("9c4f2bcd"), _target_payload("9c4f2bcd")],
    ],
)
def test_load_targets_rejects_unsafe_target_lists(tmp_path, payload) -> None:
    targets_file = _write_targets(tmp_path, payload)

    with pytest.raises(refetch.RefetchConfigurationError):
        refetch.load_refetch_targets(targets_file)


@pytest.mark.parametrize(
    "field,value",
    [
        ("request_limit", 20),
        ("request_limit", 21),
        ("request_limit", 26),
        ("byte_limit_mb", 6),
        ("byte_limit_mb", 26),
    ],
)
def test_programmatic_config_rejects_unsafe_budget_bounds(
    tmp_path, field, value
) -> None:
    with pytest.raises(refetch.RefetchConfigurationError):
        _config(tmp_path, **{field: value})


def test_config_rejects_relative_missing_or_empty_input_files(tmp_path) -> None:
    empty_file = tmp_path / "empty.txt"
    empty_file.write_text("", encoding="utf-8")

    for overrides in (
        {"proxy_file": Path("proxies.txt")},
        {"proxy_file": tmp_path / "missing.txt"},
        {"proxy_file": empty_file},
        {"targets_file": Path("targets.json")},
        {"targets_file": tmp_path / "missing.json"},
        {"targets_file": empty_file},
    ):
        with pytest.raises(refetch.RefetchConfigurationError):
            _config(tmp_path, **overrides)


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--request-limit", "20"),
        ("--request-limit", "21"),
        ("--request-limit", "26"),
        ("--byte-limit-mb", "6"),
        ("--byte-limit-mb", "26"),
    ],
)
def test_cli_rejects_unsafe_budget_bounds(tmp_path, flag, value) -> None:
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("proxy\n", encoding="utf-8")
    targets_file = _write_targets(tmp_path, [_target_payload()])

    with pytest.raises(SystemExit) as raised:
        refetch.build_cli_parser().parse_args(
            [
                "--proxy-file",
                str(proxy_file),
                "--targets-file",
                str(targets_file),
                flag,
                value,
            ]
        )

    assert raised.value.code == 2


def test_cli_requires_explicit_proxy_and_targets_files(tmp_path) -> None:
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("proxy\n", encoding="utf-8")

    with pytest.raises(SystemExit) as raised:
        refetch.build_cli_parser().parse_args(["--proxy-file", str(proxy_file)])

    assert raised.value.code == 2


@pytest.mark.parametrize(
    "stage,expected_stage,expected_events",
    [
        ("initialize", "initialize", ["initialize", "finish:False"]),
        (
            "fetch",
            "fetch_matches",
            [
                "initialize",
                "force_due",
                "force_due",
                "force_due",
                "cohort",
                "fetch",
                "finish:False",
            ],
        ),
        (
            "parse",
            "offline_parse_matches",
            [
                "initialize",
                "force_due",
                "force_due",
                "force_due",
                "cohort",
                "fetch",
                "parse",
                "finish:False",
            ],
        ),
        (
            "validate",
            "validate",
            [
                "initialize",
                "force_due",
                "force_due",
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
    tmp_path, stage, expected_stage, expected_events
) -> None:
    config = _config(tmp_path)
    pipeline = FakePipeline(fail_stage=stage)

    with pytest.raises(refetch.RefetchExecutionError) as raised:
        refetch.run_refetch(config, _targets(config), pipeline=pipeline)

    assert pipeline.events == expected_events
    assert raised.value.stage == expected_stage
    assert raised.value.error_class == "RuntimeError"
    assert raised.value.failure_finish_attempted is True
    assert "secret" not in json.dumps(raised.value.as_dict())
    assert "secret" not in str(raised.value)


def test_bridge_failure_reports_recover_stage_without_leaking(tmp_path) -> None:
    config = _config(tmp_path)
    pipeline = FakePipeline(raw_store=FakeRawStore(bridge_raises=True))

    with pytest.raises(refetch.RefetchExecutionError) as raised:
        refetch.run_refetch(config, _targets(config), pipeline=pipeline)

    assert raised.value.stage == "recover_from_raw"
    assert raised.value.error_class == "RuntimeError"
    assert pipeline.events[-1] == "finish:False"
    assert "secret" not in json.dumps(raised.value.as_dict())


def test_finish_failure_never_masks_or_leaks_original_error(tmp_path) -> None:
    config = _config(tmp_path)
    pipeline = FakePipeline(fail_stage="fetch", finish_raises=True)

    with pytest.raises(refetch.RefetchExecutionError) as raised:
        refetch.run_refetch(config, _targets(config), pipeline=pipeline)

    assert raised.value.stage == "fetch_matches"
    assert raised.value.error_class == "RuntimeError"
    assert raised.value.failure_finish_attempted is True
    assert pipeline.events[-1] == "finish:False"
    assert "secret" not in json.dumps(raised.value.as_dict())


def test_untouched_cohort_after_fetch_failure_hints_registry_missing(
    tmp_path,
) -> None:
    config = _config(tmp_path)
    pipeline = FakePipeline(fail_stage="fetch")
    pipeline.control.summary_target_counts = {"pending": 3}

    with pytest.raises(refetch.RefetchExecutionError) as raised:
        refetch.run_refetch(config, _targets(config), pipeline=pipeline)

    assert raised.value.hint == "registry_missing"
    assert raised.value.as_dict()["hint"] == "registry_missing"


def test_attempted_cohort_or_summary_failure_yields_no_hint(tmp_path) -> None:
    config = _config(tmp_path)
    attempted = FakePipeline(fail_stage="fetch")
    attempted.control.summary_target_counts = {"pending": 1, "retry": 2}
    unavailable = FakePipeline(fail_stage="fetch")

    for pipeline in (attempted, unavailable):
        with pytest.raises(refetch.RefetchExecutionError) as raised:
            refetch.run_refetch(config, _targets(config), pipeline=pipeline)
        assert raised.value.hint is None
        assert "secret" not in json.dumps(raised.value.as_dict())


def test_main_returns_redacted_json_on_runtime_failure(tmp_path, capsys) -> None:
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("proxy.example:8080:user:password\n", encoding="utf-8")
    targets_file = _write_targets(
        tmp_path, [_target_payload(match_id) for match_id in MATCH_IDS]
    )

    exit_code = refetch.main(
        [
            "--proxy-file",
            str(proxy_file),
            "--targets-file",
            str(targets_file),
            "--run-label",
            "issue-934",
        ],
        pipeline=FakePipeline(fail_stage="fetch"),
    )

    rendered = capsys.readouterr().out
    payload = json.loads(rendered)
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert payload["stage"] == "fetch_matches"
    assert payload["failure_finish_attempted"] is True
    assert payload["silver_triggered"] is False
    assert "secret" not in rendered
    assert "password" not in rendered
    assert str(proxy_file) not in rendered


def test_main_returns_configuration_failure_for_bad_targets(
    tmp_path, capsys
) -> None:
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("proxy\n", encoding="utf-8")
    targets_file = _write_targets(tmp_path, [])

    exit_code = refetch.main(
        ["--proxy-file", str(proxy_file), "--targets-file", str(targets_file)]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 2
    assert payload == {
        "status": "failed",
        "stage": "configuration",
        "error_class": "RefetchConfigurationError",
        "failure_finish_attempted": False,
        "silver_triggered": False,
    }


def test_script_has_no_airflow_silver_or_dag_trigger_imports() -> None:
    source = (
        ROOT / "scripts" / "research" / "run_fbref_match_refetch.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")

    assert not any(
        name == "airflow" or name.startswith("airflow.") for name in imported
    )
    assert "TriggerDagRunOperator" not in source
    assert "dag_transform_fbref_silver" not in source
    assert "https://fbref.com/" not in source
