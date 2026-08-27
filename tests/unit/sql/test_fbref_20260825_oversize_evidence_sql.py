"""Static fail-closed contract for provisional FBref oversize diagnostics."""

from __future__ import annotations

import hashlib
import inspect
import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_DIR = PROJECT_ROOT / "docs" / "operations" / "sql"
REMEDIATION_FILE = SQL_DIR / "fbref_20260825_reanimate_exact_oversize_evidence.sql"
GATE_FILE = SQL_DIR / "fbref_20260825_oversize_evidence_canary_gate.sql"
FETCHER_FILE = PROJECT_ROOT / "scrapers" / "fbref" / "fetcher.py"
PIPELINE_FILE = PROJECT_ROOT / "scrapers" / "fbref" / "pipeline.py"
POSTGRES_REGRESSION_FILE = (
    PROJECT_ROOT
    / "tests"
    / "integration"
    / "sql"
    / "verify_fbref_oversize_evidence_gate_pg.sh"
)
TERMINAL_TARGET_IDS = {
    "fbref:season_stats:6:2022:playingtime",
    "fbref:season_stats:678:2021:playingtime",
    "fbref:season_stats:569:2025-2026:playingtime",
    "fbref:season_stats:569:2025-2026:standard",
}
DIAGNOSTIC_TARGET_IDS = {
    "fbref:season_stats:569:2025-2026:playingtime",
    "fbref:season_stats:569:2025-2026:standard",
}
DEMOTED_TARGET_IDS = TERMINAL_TARGET_IDS - DIAGNOSTIC_TARGET_IDS
TERMINAL_SOURCE_RUN_ID = "94838bac-786a-5d59-99e4-f6a2b3f7971e"
TERMINAL_SNAPSHOT_SHA256 = (
    "b114e1139c50857b2985ead5ef2f72083660fc75cc9d1e9466874959a77bd543"
)

pytestmark = pytest.mark.unit


def _raw(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _sql(path: Path) -> str:
    uncommented = "\n".join(
        line for line in _raw(path).splitlines() if not line.lstrip().startswith("--")
    )
    return re.sub(r"\s+", " ", uncommented).strip().lower()


def _literal_target_ids(path: Path) -> set[str]:
    return set(re.findall(r"'(?P<id>fbref:season_stats:[^']+)'", _raw(path)))


def _expected_snapshot_rows() -> list[tuple[str, ...]]:
    pattern = re.compile(
        r"\(\s*'(?P<target_id>fbref:season_stats:[^']+)'\s*,\s*"
        r"'(?P<canonical_url>[^']+)'\s*,\s*"
        r"'(?P<target_status>[^']+)'\s*,\s*"
        r"'(?P<attempt_status>[^']+)'\s*,\s*"
        r"'(?P<error_class>[^']+)'\s*,\s*"
        r"(?P<http_status>\d+)\s*,\s*"
        r"(?P<http_request_count>\d+)\s*,\s*"
        r"'(?P<error_message>[^']+)'\s*\)",
        re.DOTALL,
    )
    return [match.groups() for match in pattern.finditer(_raw(REMEDIATION_FILE))]


def test_operator_order_marks_terminal_candidate_no_go_pending_review() -> None:
    raw = _raw(REMEDIATION_FILE)
    fetcher = _raw(FETCHER_FILE)

    assert "fbref-camoufox-metered-warm-http-v10" in raw
    assert "fbref-camoufox-metered-warm-http-v10" in fetcher
    assert "TERMINAL AUTHORITY BAKED" in raw
    assert "CANDIDATE NO-GO UNTIL INDEPENDENT REVIEW" in raw
    assert "DO NOT EXECUTE" in raw
    assert "fbref_oversize_baked_source_run_id" in raw
    assert "fbref_oversize_baked_snapshot_sha256" in raw
    assert TERMINAL_SOURCE_RUN_ID in raw
    assert TERMINAL_SNAPSHOT_SHA256 in raw
    assert "saved terminal snapshot" in raw.lower()
    assert "git diff --check" in raw
    assert "8 mib decoded-body cap" in raw.lower()
    assert "9 mib target reservation" in raw.lower()
    assert "8 mib+1" in raw.lower()


@pytest.mark.parametrize("path", [REMEDIATION_FILE, GATE_FILE])
def test_baked_reviewed_authority_and_unreviewed_guard_are_not_operator_inputs(
    path,
) -> None:
    raw = _raw(path)
    sql = _sql(path)

    assert r"\set on_error_stop on" in sql
    assert r"\set fbref_oversize_authority_state reviewed" in sql
    assert (r"\set fbref_oversize_baked_source_run_id " + TERMINAL_SOURCE_RUN_ID) in sql
    assert (
        r"\set fbref_oversize_baked_snapshot_sha256 " + TERMINAL_SNAPSHOT_SHA256
    ) in sql
    assert "fbref oversize authority is unreviewed" in sql
    assert ":'fbref_oversize_authority_state' = 'reviewed'" in sql
    assert ":'fbref_oversize_baked_source_run_id'::uuid" in sql
    assert ":'fbref_oversize_baked_snapshot_sha256'" in sql
    assert r"\if :{?reviewed_source_run_id}" not in sql
    assert r"\if :{?reviewed_terminal_snapshot_sha256}" not in sql
    assert "--set=reviewed_source_run_id" not in raw
    assert "--set=reviewed_terminal_snapshot_sha256" not in raw


def test_source_run_and_terminal_oversize_set_are_executable_guards() -> None:
    sql = _sql(REMEDIATION_FILE)

    assert "from fbref_control.crawl_run" in sql
    assert "from fbref_control.run_target" in sql
    assert "fbref_control.fetch_attempt" in sql
    assert "run.run_id = :'fbref_oversize_baked_source_run_id'::uuid" in sql
    assert "run.finished_at is not null" in sql
    assert "run.status in ('succeeded', 'failed', 'cancelled')" in sql
    assert "target.status = 'failed'" in sql
    assert "attempt.status = 'failed'" in sql
    assert "attempt.error_class = 'response_too_large'" in sql
    assert "attempt.http_status = 200" in sql
    assert "attempt.http_request_count = 1" in sql
    assert "attempt.logical_refresh_id = target.logical_refresh_id" in sql
    assert (
        "select * from fbref_20260825_oversize_source_expected except "
        "select * from fbref_20260825_source_terminal_oversize"
    ) in sql
    assert (
        "select * from fbref_20260825_source_terminal_oversize except "
        "select * from fbref_20260825_oversize_source_expected"
    ) in sql
    assert "select count(distinct target_id)" in sql
    assert "duplicate source target" in sql
    assert "encode(sha256(convert_to(" in sql
    assert "computed_snapshot_sha256" in sql


def test_expected_set_is_exactly_the_terminal_four() -> None:
    ids = _literal_target_ids(REMEDIATION_FILE)

    assert ids == TERMINAL_TARGET_IDS
    assert len(ids) == 4
    raw = _raw(REMEDIATION_FILE)
    assert "expected_count <> 4" in raw
    assert "exact-three" not in raw.lower()
    assert "all three" not in raw.lower()
    assert "either target" not in raw.lower()


def test_terminal_authority_and_diagnostic_cohort_are_distinct_exact_sets() -> (
    None
):
    remediation = _sql(REMEDIATION_FILE)
    gate = _sql(GATE_FILE)

    assert "fbref_20260825_oversize_source_expected" in remediation
    assert "fbref_20260825_oversize_diagnostic_expected" in remediation
    assert "fbref_20260825_oversize_diagnostic_expected" in gate
    assert "source_count <> 4" in remediation
    assert "diagnostic_count <> 2" in remediation
    for target_id in TERMINAL_TARGET_IDS:
        assert target_id in remediation
    for target_id in DIAGNOSTIC_TARGET_IDS:
        assert target_id in gate
    for target_id in DEMOTED_TARGET_IDS:
        assert (
            target_id
            not in gate.split("diagnostic_expected", 1)[1].split(")", 1)[0]
        )


def test_baked_terminal_rows_reproduce_reviewed_snapshot_digest() -> None:
    rows = _expected_snapshot_rows()
    serialized = "".join(
        "\t".join(row) + "\n" for row in sorted(rows, key=lambda row: row[0])
    ).encode()

    assert len(rows) == 4
    assert hashlib.sha256(serialized).hexdigest() == TERMINAL_SNAPSHOT_SHA256
    standard = next(row for row in rows if row[0].endswith(":569:2025-2026:standard"))
    assert standard[1] == "https://fbref.com/en/comps/569/stats/Copa-del-Rey-Stats"
    assert standard[7].endswith("/en/comps/569/stats/Copa-del-Rey-Stats")


def test_terminal_runner_cohort_matches_remediation_exactly() -> None:
    from scrapers.fbref import pipeline

    assert set(pipeline.OVERSIZE_EVIDENCE_TARGET_IDS) == TERMINAL_TARGET_IDS
    assert (
        set(pipeline.OVERSIZE_EVIDENCE_DIAGNOSTIC_TARGET_IDS)
        == DIAGNOSTIC_TARGET_IDS
    )


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

    update_targets = re.findall(r"\bupdate (fbref_control\.[a-z0-9_]+)", sql)
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


def test_requeue_requires_scope_remediation_and_excludes_false_currents() -> (
    None
):
    raw = _raw(REMEDIATION_FILE)
    sql = _sql(REMEDIATION_FILE)

    assert "current-season remediation acceptance" in raw.lower()
    assert "fbref_20260825_oversize_demoted_expected" in sql
    for predicate in (
        "season.present",
        "season.lifecycle_state = 'present'",
        "not season.is_current",
        "frontier.refresh_policy = 'daily'",
        "frontier.state = 'quarantined'",
        "frontier.next_fetch_at is null",
        "frontier.last_error_class = 'scopequarantined'",
        "frontier.last_error_message = 'noncurrent_season'",
    ):
        assert predicate in sql
    assert "scope remediation proof mismatch" in sql
    update = sql[sql.index("update fbref_control.page_frontier") :]
    for target_id in DEMOTED_TARGET_IDS:
        assert target_id not in update


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
    from scrapers.fbref import pipeline

    runner = inspect.getsource(pipeline.run_oversize_evidence_canary)
    profile = inspect.getsource(pipeline._oversize_evidence_settings)
    factory = inspect.getsource(
        pipeline._build_oversize_evidence_live_pipeline
    )
    authority = inspect.getsource(
        pipeline._validate_oversize_evidence_authority
    )
    raw = _raw(REMEDIATION_FILE)

    assert "_oversize_evidence_settings" in runner
    assert "PipelineSettings.acceptance" in profile
    assert "validate_fbref_proxy_meter" in factory
    assert "persistent_http_session=True" in factory
    assert "initialize_acceptance_run" in runner
    assert "seed_acceptance_cohort" in runner
    assert "fetch_wave" in runner
    assert "publication_eligible" in runner
    assert "parse_wave" not in runner
    assert "validate_and_finish" not in runner
    assert "trigger" not in runner.lower()
    assert "from scrapers.fbref.pipeline import OversizeEvidenceConfig" in raw
    assert "run_oversize_evidence_canary" in raw
    assert "/opt/airflow/scripts" not in raw
    assert "OVERSIZE_EVIDENCE_AUTHORITY" in authority
    assert 'review_state="REVIEWED"' in authority
    assert "_validate_oversize_evidence_authority" in runner
    assert "_validate_oversize_evidence_wave_result" in runner
    assert "--reviewed-source-run-id" not in raw
    assert "--reviewed-terminal-snapshot-sha256" not in raw
    assert "--target-id" not in raw
    assert hashlib.sha256(PIPELINE_FILE.read_bytes()).hexdigest() in raw
    settings_file = PROJECT_ROOT / "scrapers" / "fbref" / "settings.py"
    assert hashlib.sha256(settings_file.read_bytes()).hexdigest() in raw
    assert "DEFAULT_SEASON_STATS_HTTP_BODY_LIMIT_BYTES == 8388608" in raw
    assert "DEFAULT_REQUEST_RESERVATION_BYTES == 9437184" in raw


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
    assert "all_attempts as" in sql
    assert "join fbref_control.fetch_attempt as attempt" in sql
    assert "attempt.run_id = run.run_id" in sql
    assert "from fbref_20260825_oversize_diagnostic_expected" in sql
    assert "sum(all_attempts.http_request_count)" in sql
    assert "clearance_session_page_accounting" in sql
    assert "clearance_session_tail_reservation" in sql
    assert (
        "page.requests_used <> page.http_requests + page.browser_bootstrap_requests"
        in sql
    )
    assert "run.requests_used = traffic_totals.page_request_count" in sql
    assert "traffic_totals.http_request_count = (" in sql
    assert "traffic_totals.page_request_count <= run.request_limit" in sql
    assert "registered_target_id is not null" in sql
    assert "expected_target_id is not null" in sql
    assert (
        "all_attempts.attempt_logical_refresh_id = "
        "all_attempts.intended_logical_refresh_id"
    ) in sql
    assert "http_status_history = array[200]::integer[]" in sql
    assert "'all_attempts'" in sql
    assert "foreign logical refresh" in raw.lower()
    assert "response_too_large is diagnostic red" in raw.lower()
    assert "NO-GO" in raw
    assert "raise exception" in sql


def test_browser_traffic_is_conserved_without_being_misclassified_as_extra_http() -> (
    None
):
    sql = _sql(GATE_FILE)

    assert "sum(page.browser_bootstrap_requests)" in sql
    assert "sum(session.browser_bootstrap_requests)" in sql
    assert "sum(page.http_requests)" in sql
    assert "sum(session.http_requests)" in sql
    assert "page.attempt_id = all_attempts.attempt_id" in sql
    assert "session.tail_status <> 'settled'" in sql
    assert "session.tail_budget_requests_used <> 0" in sql
    assert "traffic_totals.browser_request_count <= 20" in sql
    assert "traffic_totals.page_request_count <= 22" in sql
    assert "session_totals.session_count = 1" in sql
    assert "metadata ->> 'browser_request_limit' = '20'" in sql
    assert "metadata ->> 'browser_solve_limit' = '1'" in sql
    assert "metadata ->> 'provider_dag_id' = 'dag_accept_fbref_bronze'" in sql
    assert "metadata ->> 'provider_task_id' = 'oversize_evidence_fetch'" in sql
    assert "metadata ->> 'provider_scope' = :'airflow_run_id'" in sql
    assert "metadata ->> 'provider_run_id' = run.run_id::text" in sql
    assert "metadata ->> 'provider_byte_limit' = '39321600'" in sql
    assert "run.bytes_used <= 39321600" in sql
    assert (
        "session.browser_document_bytes + session.browser_asset_bytes + "
        "session.browser_unobserved_bytes > 4194304"
    ) in sql
    assert "run.requests_used = attempt_totals.request_count" not in sql


def test_persistent_ledger_gate_matches_store_reconciliation_contract() -> (
    None
):
    sql = _sql(GATE_FILE)
    pg = _raw(POSTGRES_REGRESSION_FILE)

    for predicate in (
        "run.requests_reserved = 0",
        "run.bytes_reserved = 0",
        "session.provider_billed_bytes is null",
        "session.tail_page_provider_bytes is null",
        "session.tail_authoritative_provider_bytes is null",
        "session.tail_page_provider_bytes <> session.page_provider_bytes",
        "session.tail_authoritative_provider_bytes <> session.provider_billed_bytes",
        "page.attempt_reservation_id <> page.reservation_id",
        "budget.logical_refresh_id <> budget.page_logical_refresh_id",
        "budget.tail_logical_refresh_id <> budget.tail_session_id",
        "budget.tail_bytes_reserved <> 9437184",
        "session.browser_document_bytes <> session.page_browser_document_bytes",
        "session.browser_asset_bytes <> session.page_browser_asset_bytes",
        "session.browser_unobserved_bytes <> session.page_browser_unobserved_bytes",
        "session.http_wire_bytes <> session.page_http_wire_bytes",
        "session.decoded_html_bytes <> session.page_decoded_html_bytes",
        "session.compressed_raw_bytes <> session.page_compressed_raw_bytes",
        "page.evidence_sha256 <> page.recomputed_evidence_sha256",
        "session.tail_settlement_sha256 <> session.recomputed_tail_settlement_sha256",
    ):
        assert predicate in sql
    for label in (
        "browser-profile-metadata-mismatch",
        "provider-profile-metadata-mismatch",
        "null-session-provider-bytes",
        "contradictory-tail-receipt",
        "outstanding-run-reservation",
        "attempt-reservation-mismatch",
        "reservation-logical-refresh-mismatch",
        "target-reservation-size-mismatch",
        "session-byte-accounting-mismatch",
        "page-evidence-digest-mismatch",
        "tail-evidence-digest-mismatch",
        "browser-byte-cap-overrun",
    ):
        assert label in pg


def test_both_sql_artifacts_require_the_comp569_pair_to_remain_current() -> (
    None
):
    remediation = _sql(REMEDIATION_FILE)
    gate = _sql(GATE_FILE)
    pg = _raw(POSTGRES_REGRESSION_FILE)

    for sql in (remediation, gate):
        assert "diagnostic_current_expected" in sql
        assert "diagnostic_current_actual" in sql
        assert "season.competition_id = expected.competition_id" in sql
        assert "season.season_id = expected.season_id" in sql
        assert "season.is_current" in sql
    assert "genuine-current-mismatch" in pg
    assert "browser-reservation-overrun" in pg


def test_success_gate_proves_full_raw_and_page_accounting_are_lossless() -> (
    None
):
    sql = _sql(GATE_FILE)
    pg = _raw(POSTGRES_REGRESSION_FILE)

    assert "attempt.content_hash" in sql
    assert "attempt.raw_manifest_key" in sql
    assert "attempt.compressed_bytes" in sql
    assert "page.attempt_decoded_bytes <> page.decoded_html_bytes" in sql
    assert "page.attempt_compressed_bytes <> page.compressed_raw_bytes" in sql
    assert "page.attempt_wire_bytes <> page.http_wire_bytes" in sql
    assert (
        "page.attempt_provider_billed_bytes <> page.provider_billed_bytes"
        in sql
    )
    assert "page.decoded_html_bytes > 8388608" in sql
    assert "all_attempts.content_hash ~ '^[0-9a-f]{64}$'" in sql
    assert (
        "'manifests/fetches/' || all_attempts.attempt_logical_refresh_id::text"
        in sql
    )
    assert (
        "all_attempts.transport_version = 'fbref-camoufox-metered-warm-http-v10'"
        in sql
    )
    assert "page.attempt_session_version <> page.session_id::text" in sql
    assert "raw-decoded-loss-mismatch" in pg
    for label in (
        "malformed-raw-content-hash",
        "foreign-raw-manifest-key",
        "foreign-fetcher-version",
        "foreign-attempt-session",
        "null-attempt-transport",
    ):
        assert label in pg


def test_post_run_gate_binds_and_recomputes_reviewed_source_snapshot() -> None:
    sql = _sql(GATE_FILE)

    assert "run.run_id = :'fbref_oversize_baked_source_run_id'::uuid" in sql
    assert "source_target.target_id" in sql
    assert "source_attempt.error_class = 'response_too_large'" in sql
    assert "source_snapshot_digest" in sql
    assert "encode(sha256(convert_to(" in sql
    assert "snapshot_sha256 = :'fbref_oversize_baked_snapshot_sha256'" in sql
    assert (
        "metadata ->> 'reviewed_source_run_id' = :'fbref_oversize_baked_source_run_id'"
    ) in sql
    assert (
        "metadata ->> 'reviewed_terminal_snapshot_sha256' = "
        ":'fbref_oversize_baked_snapshot_sha256'"
    ) in sql
    assert "from fbref_20260825_oversize_diagnostic_expected" in sql
    assert "select target_id from diagnostic_targets" in sql
    assert _literal_target_ids(GATE_FILE) == TERMINAL_TARGET_IDS


def test_post_run_gate_requires_succeeded_run_exactly() -> None:
    sql = _sql(GATE_FILE)

    assert "run.status = 'succeeded'" in sql
    assert "run.status in ('succeeded', 'failed')" not in sql


def test_no_non_deliverable_script_is_required_by_the_runbook() -> None:
    raw = _raw(REMEDIATION_FILE)

    assert "run_fbref_oversize_evidence_canary.py" not in raw
    assert "verify_fbref_oversize_evidence_gate_pg.sh" not in raw
    assert "/opt/airflow/scripts" not in raw


def test_disposable_postgres_regression_covers_false_pass_cases() -> None:
    raw = _raw(POSTGRES_REGRESSION_FILE)

    assert "postgres:16-alpine" in raw
    assert "fbref_20260825_oversize_evidence_canary_gate.sql" in raw
    assert "unreviewed-authority" in raw
    assert "reviewed-pass" in raw
    assert "run-accounting-mismatch" in raw
    assert "realistic-browser-bootstrap" in raw
    assert "browser-accounting-mismatch" in raw
    assert "raw-decoded-loss-mismatch" in raw
    assert "malformed-raw-content-hash" in raw
    assert "foreign-raw-manifest-key" in raw
    assert "foreign-fetcher-version" in raw
    assert "foreign-attempt-session" in raw
    assert "null-attempt-transport" in raw
    assert "genuine-current-mismatch" in raw
    assert "browser-reservation-overrun" in raw
    assert "browser-profile-metadata-mismatch" in raw
    assert "provider-profile-metadata-mismatch" in raw
    assert "null-session-provider-bytes" in raw
    assert "contradictory-tail-receipt" in raw
    assert "outstanding-run-reservation" in raw
    assert "attempt-reservation-mismatch" in raw
    assert "reservation-logical-refresh-mismatch" in raw
    assert "target-reservation-size-mismatch" in raw
    assert "session-byte-accounting-mismatch" in raw
    assert "page-evidence-digest-mismatch" in raw
    assert "tail-evidence-digest-mismatch" in raw
    assert "demoted-scope-mismatch" in raw
    assert "orphan-extra-attempt" in raw
    assert "failed-run" in raw
    assert "provenance-mismatch" in raw
    assert "source-digest-mismatch" in raw
    assert "NO-GO" in raw
    assert "PASS: exact FBref oversize diagnostic" in raw
