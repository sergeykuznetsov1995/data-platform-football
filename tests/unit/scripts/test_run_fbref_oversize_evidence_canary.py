from pathlib import Path

import pytest

from scrapers.fbref import pipeline as canary
from scrapers.fbref.pipeline import WaveResult


TARGETS = (
    "fbref:season_stats:569:2025-2026:playingtime",
    "fbref:season_stats:569:2025-2026:standard",
)
SOURCE_RUN_ID = "94838bac-786a-5d59-99e4-f6a2b3f7971e"
SNAPSHOT_SHA256 = "a" * 64


class _Control:
    def __init__(self, *, release_failure=None):
        self.events = []
        self.release_failure = release_failure

    def acquire_publication_lock(self, run_id, *, dag_id, ttl_seconds):
        self.events.append(("lock", run_id, dag_id, ttl_seconds))

    def finish_run(self, run_id, *, succeeded):
        self.events.append(("finish", run_id, succeeded))

    def release_publication_lock(self, run_id):
        self.events.append(("release", run_id))
        if self.release_failure is not None:
            raise self.release_failure


def _exact_wave(targets=TARGETS):
    count = len(targets)
    return WaveResult(
        cohort_size=count,
        claimed=count,
        fetched=count,
        requests=count,
        wire_bytes=count * 100,
        decoded_html_bytes=count * 200,
    )


class _Pipeline:
    def __init__(self, *, failure=None, wave=None, release_failure=None):
        self.control = _Control(release_failure=release_failure)
        self.failure = failure
        self.wave = wave if wave is not None else _exact_wave()
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
        return self.wave


def _authority(targets=TARGETS) -> canary.OversizeEvidenceAuthority:
    return canary.OversizeEvidenceAuthority(
        review_state="REVIEWED",
        source_run_id=SOURCE_RUN_ID,
        terminal_snapshot_sha256=SNAPSHOT_SHA256,
        target_ids=targets,
    )


def _config(tmp_path: Path) -> canary.OversizeEvidenceConfig:
    proxy = tmp_path / "proxy.txt"
    proxy.write_text("http://proxy.invalid:8080\n", encoding="utf-8")
    return canary.OversizeEvidenceConfig(
        logical_run_label="oversize-evidence-test",
        proxy_file=proxy,
    )


def test_unreviewed_baked_authority_stops_before_pipeline_or_traffic(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        canary,
        "OVERSIZE_EVIDENCE_AUTHORITY",
        canary.OversizeEvidenceAuthority(
            review_state="UNREVIEWED",
            source_run_id="00000000-0000-0000-0000-000000000000",
            terminal_snapshot_sha256="0" * 64,
            target_ids=canary.OVERSIZE_EVIDENCE_TARGET_IDS,
        ),
    )
    pipeline = _Pipeline()

    with pytest.raises(canary.OversizeEvidenceConfigurationError, match="unreviewed"):
        canary.run_oversize_evidence_canary(_config(tmp_path), pipeline=pipeline)

    assert pipeline.initialized is None
    assert pipeline.seeded is None
    assert pipeline.fetch is None
    assert pipeline.control.events == []


def test_checked_in_authority_is_the_terminal_reviewed_snapshot() -> None:
    assert canary.OVERSIZE_EVIDENCE_AUTHORITY == canary.OversizeEvidenceAuthority(
        review_state="REVIEWED",
        source_run_id=SOURCE_RUN_ID,
        terminal_snapshot_sha256=(
            "b114e1139c50857b2985ead5ef2f72083660fc75cc9d1e9466874959a77bd543"
        ),
        target_ids=canary.OVERSIZE_EVIDENCE_TARGET_IDS,
    )


def test_run_installs_only_baked_explicit_nonpublishing_cohort(tmp_path, monkeypatch):
    authority = _authority()
    monkeypatch.setattr(canary, "OVERSIZE_EVIDENCE_AUTHORITY", authority)
    pipeline = _Pipeline()
    config = _config(tmp_path)

    result = canary.run_oversize_evidence_canary(config, pipeline=pipeline)

    assert pipeline.initialized["dag_id"] == canary.CANARY_DAG_ID
    assert pipeline.initialized["execution_metadata"] == {
        "reviewed_source_run_id": authority.source_run_id,
        "reviewed_terminal_snapshot_sha256": (authority.terminal_snapshot_sha256),
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
    assert pipeline.control.events[-2] == ("release", result["run_id"])
    assert pipeline.control.events[-1] == ("finish", result["run_id"], True)
    assert result["publication_eligible"] is False
    assert result["target_ids"] == list(TARGETS)
    assert result["wave"] == pipeline.wave.as_dict()


def test_fetch_exception_stays_red_and_closes_control_run(tmp_path, monkeypatch):
    monkeypatch.setattr(canary, "OVERSIZE_EVIDENCE_AUTHORITY", _authority())
    pipeline = _Pipeline(failure=RuntimeError("response_too_large evidence"))

    with pytest.raises(canary.OversizeEvidenceExecutionError) as caught:
        canary.run_oversize_evidence_canary(_config(tmp_path), pipeline=pipeline)

    assert caught.value.stage == "fetch"
    assert "response_too_large evidence" not in str(caught.value)
    assert pipeline.control.events[-2][0] == "finish"
    assert pipeline.control.events[-2][2] is False
    assert pipeline.control.events[-1][0] == "release"


@pytest.mark.parametrize("oversized", [1, 4])
def test_returned_oversize_wave_stays_red_and_closes_control_run(
    tmp_path, monkeypatch, oversized
):
    targets = tuple(canary.OVERSIZE_EVIDENCE_TARGET_IDS[:oversized])
    authority = _authority(targets)
    monkeypatch.setattr(canary, "OVERSIZE_EVIDENCE_AUTHORITY", authority)
    pipeline = _Pipeline(
        wave=WaveResult(
            cohort_size=oversized,
            claimed=oversized,
            fetched=0,
            requests=oversized,
            terminal_oversized_pages=oversized,
        )
    )

    with pytest.raises(canary.OversizeEvidenceExecutionError) as caught:
        canary.run_oversize_evidence_canary(_config(tmp_path), pipeline=pipeline)

    assert caught.value.stage == "validate_fetch_result"
    assert pipeline.control.events[-2][0:3] == (
        "finish",
        caught.value.run_id,
        False,
    )
    assert pipeline.control.events[-1] == ("release", caught.value.run_id)


@pytest.mark.parametrize("oversized", [1, 4])
def test_returned_oversize_wave_exits_nonzero(tmp_path, monkeypatch, oversized):
    targets = tuple(canary.OVERSIZE_EVIDENCE_TARGET_IDS[:oversized])
    monkeypatch.setattr(canary, "OVERSIZE_EVIDENCE_AUTHORITY", _authority(targets))
    pipeline = _Pipeline(
        wave=WaveResult(
            cohort_size=oversized,
            claimed=oversized,
            requests=oversized,
            terminal_oversized_pages=oversized,
        )
    )
    with pytest.raises(canary.OversizeEvidenceExecutionError):
        canary.run_oversize_evidence_canary(_config(tmp_path), pipeline=pipeline)

    assert pipeline.control.events[-2][2] is False


@pytest.mark.parametrize(
    "wave",
    [
        WaveResult(cohort_size=2, claimed=1, fetched=1, requests=1),
        WaveResult(cohort_size=2, claimed=2, fetched=1, requests=2),
        WaveResult(
            cohort_size=2,
            claimed=2,
            fetched=2,
            requests=2,
            budget_exhausted=True,
        ),
        WaveResult(
            cohort_size=2,
            claimed=2,
            fetched=2,
            requests=2,
            failures=["redacted"],
        ),
        WaveResult(
            cohort_size=2,
            claimed=2,
            fetched=2,
            requests=2,
            recovered_from_raw=1,
        ),
        WaveResult(cohort_size=2, claimed=2, fetched=2, requests=3),
    ],
)
def test_any_inexact_returned_wave_stays_red(tmp_path, monkeypatch, wave):
    monkeypatch.setattr(canary, "OVERSIZE_EVIDENCE_AUTHORITY", _authority())
    pipeline = _Pipeline(wave=wave)

    with pytest.raises(canary.OversizeEvidenceExecutionError) as caught:
        canary.run_oversize_evidence_canary(_config(tmp_path), pipeline=pipeline)

    assert caught.value.stage == "validate_fetch_result"
    assert pipeline.control.events[-2][2] is False


@pytest.mark.parametrize(
    "overrides",
    [
        {"logical_run_label": ""},
        {"logical_run_label": "spaces are invalid"},
        {"proxy_file": Path("relative-proxy.txt")},
    ],
)
def test_config_rejects_invalid_operator_inputs(tmp_path, overrides):
    values = {
        "logical_run_label": "oversize-evidence-test",
        "proxy_file": tmp_path / "proxy.txt",
    }
    values["proxy_file"].write_text("proxy\n", encoding="utf-8")
    values.update(overrides)

    with pytest.raises(canary.OversizeEvidenceConfigurationError):
        canary.OversizeEvidenceConfig(**values)


def test_release_failure_never_commits_a_succeeded_run(tmp_path, monkeypatch):
    monkeypatch.setattr(canary, "OVERSIZE_EVIDENCE_AUTHORITY", _authority())
    pipeline = _Pipeline(release_failure=RuntimeError("release failed"))

    with pytest.raises(canary.OversizeEvidenceExecutionError) as caught:
        canary.run_oversize_evidence_canary(_config(tmp_path), pipeline=pipeline)

    assert caught.value.stage == "release_publication_lock"
    assert not any(
        event[0] == "finish" and event[2] is True for event in pipeline.control.events
    )
    assert ("finish", caught.value.run_id, False) in pipeline.control.events
