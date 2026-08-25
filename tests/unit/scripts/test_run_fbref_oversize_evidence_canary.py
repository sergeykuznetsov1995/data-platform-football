from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.research import run_fbref_oversize_evidence_canary as canary


TARGETS = (
    "fbref:season_stats:569:2025-2026:playingtime",
    "fbref:season_stats:569:2025-2026:standard",
)


class _Control:
    def __init__(self):
        self.events = []

    def acquire_publication_lock(self, run_id, *, dag_id, ttl_seconds):
        self.events.append(("lock", run_id, dag_id, ttl_seconds))

    def finish_run(self, run_id, *, succeeded):
        self.events.append(("finish", run_id, succeeded))

    def release_publication_lock(self, run_id):
        self.events.append(("release", run_id))


class _Pipeline:
    def __init__(self, *, failure=None):
        self.control = _Control()
        self.failure = failure
        self.seeded = None
        self.initialized = None
        self.fetch = None

    def initialize_acceptance_run(self, **kwargs):
        self.initialized = kwargs
        return canary.make_control_run_id(
            kwargs["airflow_run_id"], dag_id=kwargs["dag_id"]
        )

    def seed_acceptance_cohort(self, run_id, target_ids, **kwargs):
        self.seeded = (run_id, tuple(target_ids), kwargs)
        return {"target_ids": list(target_ids), "cohort_size": len(target_ids)}

    def fetch_wave(self, run_id, **kwargs):
        self.fetch = (run_id, kwargs)
        if self.failure is not None:
            raise self.failure
        return SimpleNamespace(requests=len(TARGETS))


def _config(tmp_path: Path) -> canary.CanaryConfig:
    proxy = tmp_path / "proxy.txt"
    proxy.write_text("http://proxy.invalid:8080\n", encoding="utf-8")
    return canary.CanaryConfig(
        logical_run_label="oversize-evidence-test",
        proxy_file=proxy,
        reviewed_source_run_id="94838bac-786a-5d59-99e4-f6a2b3f7971e",
        reviewed_terminal_snapshot_sha256="a" * 64,
        target_ids=TARGETS,
    )


def test_run_installs_only_explicit_nonpublishing_cohort(tmp_path):
    pipeline = _Pipeline()
    config = _config(tmp_path)

    result = canary.run_canary(config, pipeline=pipeline)

    assert pipeline.initialized["dag_id"] == canary.CANARY_DAG_ID
    assert pipeline.initialized["execution_metadata"] == {
        "reviewed_source_run_id": config.reviewed_source_run_id,
        "reviewed_terminal_snapshot_sha256": (
            config.reviewed_terminal_snapshot_sha256
        ),
    }
    _run_id, target_ids, seed_kwargs = pipeline.seeded
    assert target_ids == TARGETS
    assert seed_kwargs["required_page_kinds"] == ("season_stats",)
    assert seed_kwargs["required_routes"] == ("playingtime", "standard")
    assert set(seed_kwargs["coverage_slots"].values()) == set(TARGETS)
    assert pipeline.fetch[1]["page_kinds"] == ("season_stats",)
    assert pipeline.fetch[1]["settings"].request_limit == 100
    assert pipeline.fetch[1]["settings"].byte_limit == 50 * 1024 * 1024
    assert pipeline.fetch[1]["settings"].shard_size == 25
    assert pipeline.control.events[-2][0:2] == ("finish", result["run_id"])
    assert pipeline.control.events[-2][2] is True
    assert pipeline.control.events[-1] == ("release", result["run_id"])
    assert result["publication_eligible"] is False
    assert result["target_ids"] == list(TARGETS)


def test_oversize_failure_stays_red_and_closes_control_run(tmp_path):
    pipeline = _Pipeline(failure=RuntimeError("response_too_large evidence"))

    with pytest.raises(canary.CanaryExecutionError) as caught:
        canary.run_canary(_config(tmp_path), pipeline=pipeline)

    assert caught.value.stage == "fetch"
    assert "response_too_large evidence" not in str(caught.value)
    assert pipeline.control.events[-2][0] == "finish"
    assert pipeline.control.events[-2][2] is False
    assert pipeline.control.events[-1][0] == "release"


@pytest.mark.parametrize(
    "overrides",
    [
        {"target_ids": ()},
        {"target_ids": (TARGETS[0], TARGETS[0])},
        {"target_ids": ("fbref:match:not-allowed",)},
        {"reviewed_terminal_snapshot_sha256": "not-a-sha"},
        {"reviewed_source_run_id": "not-a-uuid"},
    ],
)
def test_config_rejects_non_exact_or_unreviewed_inputs(tmp_path, overrides):
    values = {
        "logical_run_label": "oversize-evidence-test",
        "proxy_file": tmp_path / "proxy.txt",
        "reviewed_source_run_id": "94838bac-786a-5d59-99e4-f6a2b3f7971e",
        "reviewed_terminal_snapshot_sha256": "a" * 64,
        "target_ids": TARGETS,
    }
    values["proxy_file"].write_text("proxy\n", encoding="utf-8")
    values.update(overrides)

    with pytest.raises(canary.CanaryConfigurationError):
        canary.CanaryConfig(**values)
