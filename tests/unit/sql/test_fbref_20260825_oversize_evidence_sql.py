"""Static fail-closed contract for provisional FBref oversize diagnostics."""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_DIR = PROJECT_ROOT / "docs" / "operations" / "sql"
REMEDIATION_FILE = (
    SQL_DIR / "fbref_20260825_reanimate_exact_oversize_evidence.sql"
)
GATE_FILE = SQL_DIR / "fbref_20260825_oversize_evidence_canary_gate.sql"
FETCHER_FILE = PROJECT_ROOT / "scrapers" / "fbref" / "fetcher.py"
RUNNER_FILE = (
    PROJECT_ROOT
    / "scripts"
    / "research"
    / "run_fbref_oversize_evidence_canary.py"
)
PROVISIONAL_TARGET_IDS = {
    "fbref:season_stats:6:2022:playingtime",
    "fbref:season_stats:678:2021:playingtime",
    "fbref:season_stats:569:2025-2026:playingtime",
    "fbref:season_stats:569:2025-2026:standard",
}

pytestmark = pytest.mark.unit


def _raw(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _sql(path: Path) -> str:
    uncommented = "\n".join(
        line
        for line in _raw(path).splitlines()
        if not line.lstrip().startswith("--")
    )
    return re.sub(r"\s+", " ", uncommented).strip().lower()


def _literal_target_ids(path: Path) -> set[str]:
    return set(re.findall(r"'(?P<id>fbref:season_stats:[^']+)'", _raw(path)))


def test_operator_order_is_explicitly_provisional_and_non_executable() -> None:
    raw = _raw(REMEDIATION_FILE)
    fetcher = _raw(FETCHER_FILE)

    assert "fbref-camoufox-metered-warm-http-v10" in raw
    assert "fbref-camoufox-metered-warm-http-v10" in fetcher
    assert "PROVISIONAL — PROD1 IS NOT TERMINAL" in raw
    assert "DO NOT EXECUTE" in raw
    assert "reviewed_source_run_id" in raw
    assert "reviewed_terminal_snapshot_sha256" in raw
    assert "replace only after the source run is terminal" in raw.lower()
    assert "94838bac-786a-5d59-99e4-f6a2b3f7971e" in raw
    assert "read-only terminal snapshot" in raw.lower()
    assert "git diff --check" in raw
    assert "do not infer or install a new\n-- decoded-body cap" in raw.lower()


def test_placeholder_guard_refuses_unreviewed_source_snapshot() -> None:
    sql = _sql(REMEDIATION_FILE)

    assert r"\set on_error_stop on" in sql
    assert r"\if :{?reviewed_source_run_id}" in sql
    assert r"\if :{?reviewed_terminal_snapshot_sha256}" in sql
    assert "source run id is required after terminal review" in sql
    assert "terminal snapshot sha256 is required after terminal review" in sql
    assert "begin;" in sql
    assert "commit;" in sql


def test_source_run_and_terminal_oversize_set_are_executable_guards() -> None:
    sql = _sql(REMEDIATION_FILE)

    assert "from fbref_control.crawl_run" in sql
    assert "from fbref_control.run_target" in sql
    assert "fbref_control.fetch_attempt" in sql
    assert "run.run_id = :'reviewed_source_run_id'::uuid" in sql
    assert "run.finished_at is not null" in sql
    assert "run.status in ('succeeded', 'failed', 'cancelled')" in sql
    assert "target.status = 'failed'" in sql
    assert "attempt.status = 'failed'" in sql
    assert "attempt.error_class = 'response_too_large'" in sql
    assert "attempt.http_status = 200" in sql
    assert "attempt.http_request_count = 1" in sql
    assert "attempt.logical_refresh_id = target.logical_refresh_id" in sql
    assert (
        "select * from fbref_20260825_oversize_evidence_expected except "
        "select * from fbref_20260825_source_terminal_oversize"
    ) in sql
    assert (
        "select * from fbref_20260825_source_terminal_oversize except "
        "select * from fbref_20260825_oversize_evidence_expected"
    ) in sql
    assert "select count(distinct target_id)" in sql
    assert "duplicate source target" in sql
    assert "encode(sha256(convert_to(" in sql
    assert "computed_snapshot_sha256" in sql


def test_provisional_expected_set_is_exactly_the_observed_four() -> None:
    ids = _literal_target_ids(REMEDIATION_FILE)

    assert ids == PROVISIONAL_TARGET_IDS
    assert len(ids) == 4
    raw = _raw(REMEDIATION_FILE)
    assert "expected_count <> 4" not in raw
    assert "exact-three" not in raw.lower()
    assert "all three" not in raw.lower()
    assert "either target" not in raw.lower()


def test_transaction_serializes_frontier_and_rejects_writer_state() -> None:
    sql = _sql(REMEDIATION_FILE)

    assert "pg_advisory_xact_lock" in sql
    assert "lock table fbref_control.crawl_run in share mode" in sql
    assert "lock table fbref_control.page_frontier in share row exclusive mode" in sql
    assert "lock table fbref_control.run_target" in sql
    assert "fbref_control.fetch_attempt in share mode" in sql
    assert "status in ('pending', 'running')" in sql
    assert "frontier.state = 'leased'" in sql
    assert "frontier.claim_token is not null" in sql
    assert "frontier.lease_run_id is not null" in sql
    assert "frontier.lease_refresh_id is not null" in sql
    assert "frontier.lease_expires_at is not null" in sql
    assert "for update of frontier" in sql
    assert "raise exception" in sql


def test_frontier_selection_is_exact_and_preserves_history_fields() -> None:
    sql = _sql(REMEDIATION_FILE)

    for predicate in (
        "frontier.source = 'fbref'",
        "frontier.page_kind = 'season_stats'",
        "frontier.refresh_policy = 'daily'",
        "frontier.state = 'dead'",
        "frontier.last_http_status = 200",
        "frontier.last_fetched_at is null",
        "frontier.last_error_class = 'response_too_large'",
    ):
        assert predicate in sql
    assert "selected_count <> expected_count" in sql
    assert "updated_count <> expected_count" in sql
    assert "from fbref_20260825_oversize_evidence_selected as selected" in sql
    assert "returning frontier.target_id" in sql

    update_targets = re.findall(
        r"\bupdate (fbref_control\.[a-z0-9_]+)", sql
    )
    assert update_targets == ["fbref_control.page_frontier"]
    update = sql[sql.index("update fbref_control.page_frontier") :]
    set_clause = update[update.index(" set ") : update.index(" from ")]
    assert set(re.findall(r"\b([a-z_]+) =", set_clause)) == {
        "state",
        "updated_at",
    }
    for preserved in (
        "last_content_hash",
        "last_etag",
        "last_modified",
        "last_fetched_at",
        "last_http_status",
        "lease_epoch",
        "source_ids",
    ):
        assert f"{preserved} =" not in set_clause


def test_exact_cohort_mechanism_does_not_rely_on_due_time() -> None:
    raw = _raw(REMEDIATION_FILE)
    sql = _sql(REMEDIATION_FILE)

    assert "create_explicit_run_cohort" in raw
    assert "seed_acceptance_cohort" in raw
    assert "immutable exact cohort" in raw.lower()
    assert "publication_eligible=false" in raw
    assert "100 requests / 50 MiB / shard 25" in raw
    update = sql[sql.index("update fbref_control.page_frontier") :]
    assert "next_fetch_at =" not in update
    assert "$prospective_exact_cohort_proof$" in sql
    assert "unexpected eligible target" in sql


def test_purpose_built_runner_is_fetch_only_and_physically_nonpublishing() -> None:
    runner = _raw(RUNNER_FILE)
    raw = _raw(REMEDIATION_FILE)

    assert "PipelineSettings.acceptance" in runner
    assert "initialize_acceptance_run" in runner
    assert "seed_acceptance_cohort" in runner
    assert "fetch_wave" in runner
    assert "publication_eligible" in runner
    assert "parse_wave" not in runner
    assert "validate_and_finish" not in runner
    assert "trigger" not in runner.lower()
    assert "run_fbref_oversize_evidence_canary.py" in raw
    assert "--reviewed-source-run-id" in raw
    assert "--reviewed-terminal-snapshot-sha256" in raw
    assert raw.count("--target-id fbref:season_stats:") == 4


def test_post_run_gate_is_separate_read_only_and_fail_closed() -> None:
    raw = _raw(GATE_FILE)
    sql = _sql(GATE_FILE)

    assert r"\set on_error_stop on" in sql
    assert "begin transaction read only" in sql
    assert "from fbref_control.crawl_run" in sql
    assert "fbref_control.run_target" in sql
    assert "fbref_control.fetch_attempt" in sql
    assert "metadata ->> 'execution_mode' = 'acceptance_nonpublishing'" in sql
    assert "metadata ->> 'publication_eligible' = 'false'" in sql
    assert "metadata ->> 'shard_size' = '25'" in sql
    assert "request_limit = 100" in sql
    assert "byte_limit = 52428800" in sql
    assert "from fbref_control.publication_lock" in sql
    assert "publication_lock.released_at is null" in sql
    assert "attempt_count = 1" in sql
    assert "intended_attempt_count = 1" in sql
    assert "request_count = 1" in sql
    assert "http_3xx_count = 0" in sql
    assert "foreign logical refresh" in raw.lower()
    assert "response_too_large is diagnostic red" in raw.lower()
    assert "NO-GO" in raw
    assert "raise exception" in sql


def test_post_run_gate_derives_targets_from_reviewed_source_run() -> None:
    sql = _sql(GATE_FILE)

    assert "run.run_id = :'reviewed_source_run_id'::uuid" in sql
    assert "source_target.target_id" in sql
    assert "source_attempt.error_class = 'response_too_large'" in sql
    assert "expected except select target_id from actual" in sql
    assert "actual except select target_id from expected" in sql
    assert not _literal_target_ids(GATE_FILE)
