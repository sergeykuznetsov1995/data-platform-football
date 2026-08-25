"""Static safety contract for the bounded FBref 2026-08-25 remediations.

These tests intentionally inspect executable SQL rather than exercising a live
database.  The production files are one-shot operator tools, so their safety
boundary must be reviewable without granting a test process write access to the
control plane.
"""

from __future__ import annotations

import hashlib
import inspect
import re
from pathlib import Path

import pytest

from scrapers.fbref.control.store import ControlStore, _FRONTIER_SCOPE_CTE


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_DIR = PROJECT_ROOT / "docs" / "operations" / "sql"
PIPELINE_FILE = PROJECT_ROOT / "scrapers" / "fbref" / "pipeline.py"
LARGE_PAGES_SQL = SQL_DIR / "fbref_20260825_reanimate_large_pages.sql"
FALSE_SEASONS_SQL = SQL_DIR / "fbref_20260825_reopen_false_season_quarantines.sql"
REDIRECT_ALIASES_SQL = (
    SQL_DIR / "fbref_20260825_repoint_current_season_redirect_aliases.sql"
)
REDIRECT_CANARY_GATE_SQL = (
    SQL_DIR / "fbref_20260825_redirect_alias_canary_gate.sql"
)

LARGE_SELECTED = "fbref_20260825_large_pages_selected"
FALSE_SEASONS_SELECTED = "fbref_20260825_false_seasons_selected"
EXPECTED_FALSE_SEASON_IDS = {
    "fbref:season:15:2025-2026",
    "fbref:season:16:2025-2026",
    "fbref:season:20:2025-2026",
    "fbref:season:34:2025-2026",
}
EXPECTED_FALSE_SEASON_ID_LIST = [
    "fbref:season:15:2025-2026",
    "fbref:season:16:2025-2026",
    "fbref:season:20:2025-2026",
    "fbref:season:34:2025-2026",
]
EXPECTED_REDIRECT_TARGET_IDS = {
    "fbref:season:33:2026-2027",
    "fbref:season:59:2026-2027",
}
EXPECTED_REDIRECT_OLD_URLS = {
    "https://fbref.com/en/comps/33/2-bundesliga-stats",
    "https://fbref.com/en/comps/59/3-liga-stats",
}
EXPECTED_REDIRECT_OBSERVED_LOCATIONS = {
    "https://fbref.com/en/comps/33/2/",
    "https://fbref.com/en/comps/59/3/",
}
EXPECTED_REDIRECT_CANONICAL_URLS = {
    "https://fbref.com/en/comps/33/2",
    "https://fbref.com/en/comps/59/3",
}

pytestmark = pytest.mark.unit


def _executable_sql(path: Path) -> str:
    """Return lower-cased SQL with line comments removed."""

    sql = path.read_text(encoding="utf-8")
    uncommented = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    return uncommented.lower()


def _normalized(path: Path) -> str:
    return re.sub(r"\s+", " ", _executable_sql(path)).strip()


def _selection_clause(path: Path, selected_table: str) -> str:
    sql = _normalized(path)
    start = sql.index(f"create temp table {selected_table}")
    end = sql.index("do $selection_guard$", start)
    return sql[start:end]


def _update_clause(path: Path) -> str:
    sql = _normalized(path)
    start = sql.index("update fbref_control.page_frontier as frontier")
    end = sql.index("returning frontier.target_id", start)
    return sql[start : end + len("returning frontier.target_id")]


def _update_set_clause(path: Path, selected_table: str) -> str:
    update = _update_clause(path)
    match = re.search(
        rf" set (.*?) from {re.escape(selected_table)} as selected", update
    )
    assert match is not None
    return match.group(1)


def _assigned_columns(set_clause: str) -> set[str]:
    return set(re.findall(r"\b([a-z_]+)\s*=", set_clause))


@pytest.mark.parametrize("path", [LARGE_PAGES_SQL, FALSE_SEASONS_SQL])
def test_common_transaction_and_concurrency_guards(path: Path) -> None:
    sql = _normalized(path)

    assert sql.startswith("begin;")
    assert sql.endswith("commit;")
    assert "pg_advisory_xact_lock" in sql
    assert "lock table fbref_control.crawl_run in share mode" in sql
    assert "from fbref_control.crawl_run" in sql
    assert re.search(r"status\s+in\s*\(\s*'pending'\s*,\s*'running'\s*\)", sql)
    assert "raise exception" in sql
    assert "for update" in sql


@pytest.mark.parametrize("path", [LARGE_PAGES_SQL, FALSE_SEASONS_SQL])
def test_common_mutation_surface_is_closed_and_frontier_only(path: Path) -> None:
    sql = _executable_sql(path)

    update_targets = re.findall(r"\bupdate\s+([a-z0-9_.]+)", sql)
    assert update_targets == ["fbref_control.page_frontier"]
    for forbidden in (
        r"\binsert\s+into\b",
        r"\bdelete\s+from\b",
        r"\bmerge\s+into\b",
        r"\btruncate\b",
        r"\balter\s+table\b",
        r"\bdrop\s+table\b",
        r"\bcreate\s+(?:or\s+replace\s+)?(?:function|procedure|trigger)\b",
        r"\bexecute\b",
        r"\bcopy\b",
        r"\bcall\b",
        r"\bgrant\b",
        r"\brevoke\b",
        r"\brefresh\s+materialized\s+view\b",
        r"\breindex\b",
        r"\bvacuum\b",
        r"\bcluster\b",
        r"\bcomment\s+on\b",
        r"\bsecurity\s+label\b",
        r"\bimport\s+foreign\s+schema\b",
    ):
        assert not re.search(forbidden, sql)

    create_tables = re.findall(r"\bcreate\s+((?:temp(?:orary)?\s+)?table)\b", sql)
    assert create_tables == ["temp table", "temp table"]
    assert not re.search(r"\bcreate\s+(?!temp(?:orary)?\s+table\b)", sql)


def test_large_page_update_changes_only_scheduling_and_error_fields() -> None:
    assignments = _assigned_columns(_update_set_clause(LARGE_PAGES_SQL, LARGE_SELECTED))

    assert assignments == {
        "state",
        "next_fetch_at",
        "retry_after",
        "last_error_class",
        "last_error_message",
        "updated_at",
    }


def test_false_season_update_clears_only_stale_http_validators_in_addition() -> None:
    set_clause = _update_set_clause(FALSE_SEASONS_SQL, FALSE_SEASONS_SELECTED)
    assignments = _assigned_columns(set_clause)

    assert assignments == {
        "state",
        "next_fetch_at",
        "retry_after",
        "last_error_class",
        "last_error_message",
        "last_etag",
        "last_modified",
        "updated_at",
    }
    assert "last_etag = null" in set_clause
    assert "last_modified = null" in set_clause
    assert "last_content_hash" not in set_clause


def test_large_page_update_never_clears_http_validators_or_content_hash() -> None:
    set_clause = _update_set_clause(LARGE_PAGES_SQL, LARGE_SELECTED)

    assert "last_etag" not in set_clause
    assert "last_modified" not in set_clause
    assert "last_content_hash" not in set_clause


@pytest.mark.parametrize("path", [LARGE_PAGES_SQL, FALSE_SEASONS_SQL])
def test_common_update_returns_only_exact_affected_ids(path: Path) -> None:
    sql = _normalized(path)

    assert "returning frontier.target_id" in _update_clause(path)
    assert re.search(
        r"select target_id from fbref_20260825_.*?_updated order by target_id;",
        sql,
    )


def test_large_page_remediation_is_exactly_bounded_to_eligible_rows() -> None:
    sql = _normalized(LARGE_PAGES_SQL)
    selection = _selection_clause(LARGE_PAGES_SQL, LARGE_SELECTED)
    update = _update_clause(LARGE_PAGES_SQL)

    for clause in (selection, update):
        assert "frontier.source = 'fbref'" in clause
        assert "frontier.page_kind = 'season_stats'" in clause
        assert "frontier.state = 'dead'" in clause
        assert "frontier.last_error_class = 'response_too_large'" in clause
    assert "limit 26" in selection
    assert "for update" in selection
    assert re.search(r"selected_count\s*<\s*1\s+or\s+selected_count\s*>\s*25", sql)
    assert "updated_count <> selected_count" in sql


def test_false_season_remediation_requires_the_exact_four_quarantines() -> None:
    sql = _normalized(FALSE_SEASONS_SQL)
    selection = _selection_clause(FALSE_SEASONS_SQL, FALSE_SEASONS_SELECTED)
    update = _update_clause(FALSE_SEASONS_SQL)

    for clause in (selection, update):
        ids = re.findall(r"'(fbref:season:[^']+)'", clause)
        assert ids == EXPECTED_FALSE_SEASON_ID_LIST
        assert set(ids) == EXPECTED_FALSE_SEASON_IDS
        assert "frontier.source = 'fbref'" in clause
        assert "frontier.page_kind = 'season'" in clause
        assert "frontier.state = 'quarantined'" in clause
        assert "frontier.last_error_class = 'parsecontractquarantined'" in clause
        assert "frontier.last_error_message = 'schedule_season_mismatch'" in clause
    assert "for update" in selection
    assert re.search(r"selected_count\s*<>\s*4", sql)
    assert "updated_count <> 4" in sql


def test_redirect_alias_remediation_has_transaction_and_writer_guards() -> None:
    sql = _normalized(REDIRECT_ALIASES_SQL)

    assert sql.startswith("begin;")
    assert sql.endswith("commit;")
    assert "pg_advisory_xact_lock" in sql
    assert "lock table fbref_control.crawl_run in share mode" in sql
    assert "from fbref_control.crawl_run" in sql
    assert re.search(r"status\s+in\s*\(\s*'pending'\s*,\s*'running'\s*\)", sql)
    assert "raise exception" in sql
    assert sql.count("for update") >= 2


def test_redirect_alias_remediation_orders_code_sql_then_strict_canary() -> None:
    sql = REDIRECT_ALIASES_SQL.read_text(encoding="utf-8")
    pipeline = PIPELINE_FILE.read_text(encoding="utf-8")
    mapping_version = "fbref-season-install-redirects-20260825-v1"

    assert "DO NOT run this as a data-only fix" in sql
    assert "scrapers/fbref/pipeline.py must be deployed first" in sql
    assert mapping_version in sql
    assert mapping_version in pipeline
    deploy = sql.index("1. Deploy the code")
    runtime_probe = sql.index("2. Run this read-only probe")
    apply_sql = sql.index("3. Apply this SQL")
    canary = sql.index("4. Trigger the standard bounded 100/50/25 canary")
    assert deploy < runtime_probe < apply_sql < canary
    assert "HTTP 200 in exactly one target" in sql
    assert "Any 3xx is acceptance NO-GO" in sql
    assert "keep FBref ingestion paused" in sql
    assert "run_target ordinals 0 and 1" in sql


def test_redirect_alias_remediation_binds_to_the_deployed_cohort_policy() -> None:
    sql = _normalized(REDIRECT_ALIASES_SQL)
    raw = REDIRECT_ALIASES_SQL.read_text(encoding="utf-8").lower()
    source = inspect.getsource(ControlStore.create_due_run_cohort)
    policy_sha256 = hashlib.sha256(source.encode()).hexdigest()

    assert policy_sha256 in raw
    assert "inspect.getsource(controlstore.create_due_run_cohort)" in raw
    assert re.sub(r"\s+", " ", _FRONTIER_SCOPE_CTE.lower()).strip() in sql
    prospective_start = sql.index("do $prospective_cohort_guard$")
    prospective = sql[
        prospective_start :
        sql.index("$prospective_cohort_guard$;", prospective_start)
    ]
    for policy_fragment in (
        "frontier.page_kind = 'competition_index' then 0",
        "frontier.refresh_policy <> 'historical_once'",
        "coalesce(scope.has_current_season, false) then 2",
        "scope.scope_count > 0",
        "not coalesce(scope.has_female, false)",
        "not coalesce(scope.has_unknown, true)",
        "not coalesce(scope.inactive_competition, true)",
        "not coalesce(scope.invalid_season, true)",
        "for update of frontier skip locked",
    ):
        assert policy_fragment in prospective
    ordering = prospective[prospective.rindex("order by eligible.admission_tier") :]
    assert ordering.index("eligible.admission_tier") < ordering.index(
        "eligible.due_at"
    ) < ordering.index("frontier.priority desc") < ordering.index(
        "frontier.created_at"
    ) < ordering.index("frontier.target_id")
    assert "limit 2" in ordering


def test_redirect_alias_preflight_stabilizes_pre_live_dag_side_effects() -> None:
    sql = _normalized(REDIRECT_ALIASES_SQL)

    assert "lock table fbref_control.page_frontier" in sql
    assert "fbref_control.frontier_provenance" in sql
    assert "fbref_control.competition_registry" in sql
    assert "fbref_control.season_registry" in sql
    assert "fbref_control.season_alias" in sql
    lease_guard = sql[
        sql.index("do $lease_zero_guard$") :
        sql.index(
            "$lease_zero_guard$;",
            sql.index("do $lease_zero_guard$"),
        )
    ]
    assert "frontier.state = 'leased'" in lease_guard
    assert "raise exception" in lease_guard
    seed_guard = sql[
        sql.index("do $competition_index_seed_guard$") :
        sql.index("$competition_index_seed_guard$;", sql.index("do $competition_index_seed_guard$"))
    ]
    assert "fbref:competition_index:all" in seed_guard
    assert "https://fbref.com/en/comps" in seed_guard
    assert "refresh_policy = 'daily'" in seed_guard
    assert "priority = 100" in seed_guard
    recovery_guard = sql[
        sql.index("do $recovery_zero_guard$") :
        sql.index("$recovery_zero_guard$;", sql.index("do $recovery_zero_guard$"))
    ]
    assert "source_run.run_type = 'current'" in recovery_guard
    assert "attempt.status = 'succeeded'" in recovery_guard
    assert "attempt.raw_manifest_key is not null" in recovery_guard
    assert "observed.parser_version = 'fbref-page-document-v4'" in recovery_guard
    assert "observed.typed_parser_version = 'fbref-typed-bronze-v4'" in recovery_guard
    assert "observed.stateful_parser_version = 'fbref-discovery-parser-v6'" in recovery_guard
    assert "raise exception" in recovery_guard


def test_redirect_alias_remediation_requires_exact_prospective_ordinals() -> None:
    sql = _normalized(REDIRECT_ALIASES_SQL)
    guard = sql[
        sql.index("do $prospective_cohort_guard$") :
        sql.index("$prospective_cohort_guard$;", sql.index("do $prospective_cohort_guard$"))
    ]

    assert "prospective_target_ids := array_append" in guard
    assert (
        "order by eligible.admission_tier, eligible.due_at, "
        "frontier.priority desc, frontier.created_at, frontier.target_id"
    ) in guard
    assert "array[ 'fbref:season:59:2026-2027', 'fbref:season:33:2026-2027' ]" in guard
    assert "raise exception" in guard
    assert sql.index("do $prospective_cohort_guard$") < sql.index(
        "update fbref_control.season_registry"
    )


def test_redirect_alias_remediation_is_the_exact_two_url_mapping() -> None:
    sql = _executable_sql(REDIRECT_ALIASES_SQL)
    normalized = _normalized(REDIRECT_ALIASES_SQL)

    assert set(re.findall(r"'(fbref:season:[^']+)'", sql)) == (
        EXPECTED_REDIRECT_TARGET_IDS
    )
    assert set(re.findall(r"'(https://fbref.com/en/comps/[^']+-stats)'", sql)) == (
        EXPECTED_REDIRECT_OLD_URLS
    )
    assert set(re.findall(r"'(https://fbref.com/en/comps/\d+/\d+/)'", sql)) == (
        EXPECTED_REDIRECT_OBSERVED_LOCATIONS
    )
    assert set(re.findall(r"'(https://fbref.com/en/comps/\d+/\d+)'", sql)) == (
        EXPECTED_REDIRECT_CANONICAL_URLS
    )
    assert "selected_registry_count <> 2" in sql
    assert "selected_frontier_count <> 2" in sql
    assert "updated_registry_count <> 2" in sql
    assert "updated_frontier_count <> 2" in sql
    exact_location_token = re.compile(
        r"\('location=' \|\| mapping\.observed_location\) = any \(\s*"
        r"string_to_array\(frontier\.last_error_message, ','\)\s*\)"
    )
    assert len(exact_location_token.findall(normalized)) == 2
    assert "strpos(" not in normalized
    expected = "https://fbref.com/en/comps/33/2/"
    exact_evidence = f"body_bytes=0,location={expected},server=cloudflare"
    suffix_near_miss = f"body_bytes=0,location={expected}evil,server=cloudflare"
    def token_matches(evidence: str) -> bool:
        return f"location={expected}" in evidence.split(",")

    assert token_matches(exact_evidence)
    assert not token_matches(suffix_near_miss)
    assert normalized.count(
        "frontier.next_fetch_at < clock_timestamp()"
    ) == 2


def test_redirect_alias_remediation_updates_only_registry_and_frontier() -> None:
    sql = _executable_sql(REDIRECT_ALIASES_SQL)
    update_targets = re.findall(r"(?m)^\s*update\s+([a-z0-9_.]+)", sql)

    assert update_targets == [
        "fbref_control.season_registry",
        "fbref_control.page_frontier",
    ]
    for forbidden in (
        r"\binsert\s+into\b",
        r"\bdelete\s+from\b",
        r"\bmerge\s+into\b",
        r"\btruncate\b",
        r"\balter\s+table\b",
        r"\bdrop\s+table\b",
        r"\bexecute\b",
        r"\bcopy\b",
        r"\bcall\b",
    ):
        assert not re.search(forbidden, sql)


def test_redirect_alias_frontier_update_preserves_history_and_raw_identity() -> None:
    sql = _normalized(REDIRECT_ALIASES_SQL)
    start = sql.index("update fbref_control.page_frontier as frontier")
    end = sql.index("returning frontier.target_id", start)
    update = sql[start:end]
    set_clause = update[update.index(" set ") : update.index(" from ")]

    assert _assigned_columns(set_clause) == {
        "canonical_url",
        "state",
        "retry_after",
        "last_error_class",
        "last_error_message",
        "last_etag",
        "last_modified",
        "updated_at",
    }
    assert "state = 'queued'" in set_clause
    assert "next_fetch_at" not in set_clause
    assert "last_etag = null" in set_clause
    assert "last_modified = null" in set_clause
    for preserved in (
        "last_content_hash",
        "last_fetched_at",
        "last_http_status",
    ):
        assert preserved not in set_clause


def test_redirect_alias_canary_gate_is_read_only_executable_and_fail_closed() -> None:
    sql = _normalized(REDIRECT_CANARY_GATE_SQL)
    raw = REDIRECT_CANARY_GATE_SQL.read_text(encoding="utf-8").lower()

    assert sql.startswith("\\set on_error_stop on")
    assert "begin transaction read only;" in sql
    assert ":{?airflow_run_id}" in raw
    assert "\\gset fbref_redirect_gate_" in raw
    assert "\\if :fbref_redirect_gate_passed" in raw
    assert raw.count("raise exception") == 2
    assert "\\quit" not in raw
    assert "'fbref:season:59:2026-2027', 0::bigint" in sql
    assert "'fbref:season:33:2026-2027', 1::bigint" in sql
    assert "run_gate as ( select count(*) = 1" in sql
    assert "status = 'succeeded'" in sql
    assert "request_limit = 100" in sql
    assert "byte_limit = 52428800" in sql
    assert "metadata ->> 'execution_mode' = 'canary_nonpublishing'" in sql
    assert "metadata ->> 'publication_eligible' = 'false'" in sql
    assert "metadata ->> 'shard_size' = '25'" in sql
    assert "a.ordinal is not distinct from e.ordinal" in sql
    assert "a.attempt_count = 1" in sql
    assert "a.success_count = 1" in sql
    assert "a.request_count = 1" in sql
    assert "a.http_200_count = 1" in sql
    assert "a.http_3xx_count = 0" in sql
    assert "a.http_status_history_count = 1" in sql
    assert "a.exact_200_history_count = 1" in sql
    for forbidden in (
        r"\bcreate\b",
        r"\balter\b",
        r"\bdrop\b",
        r"\binsert\b",
        r"\bupdate\b",
        r"\bdelete\b",
        r"\bmerge\b",
        r"\bcall\b",
    ):
        assert not re.search(forbidden, _executable_sql(REDIRECT_CANARY_GATE_SQL))


def test_redirect_alias_canary_gate_rejects_cross_identity_extra_attempts() -> None:
    """A second run/target attempt must fail even when its refresh id differs.

    ``fetch_attempt`` has no composite foreign key to ``run_target``.  The gate
    must therefore count every attempt for the selected run and target, then
    independently prove that the sole attempt has the target's intended
    logical-refresh identity.  This represents one valid 200 attempt plus one
    otherwise-valid extra attempt whose logical-refresh id is mismatched.
    """

    sql = _normalized(REDIRECT_CANARY_GATE_SQL)
    actual = sql[sql.index("actual as (") : sql.index("run_gate as (")]

    join = actual[actual.index("left join fbref_control.fetch_attempt") : actual.index("where target.target_id")]
    assert "attempt.run_id = target.run_id" in join
    assert "attempt.target_id = target.target_id" in join
    assert "attempt.logical_refresh_id = target.logical_refresh_id" not in join

    # These totals include both the intended attempt and the mismatched extra
    # attempt/request, so either duplicate prevents acceptance.
    assert "count(attempt.attempt_id)::bigint as attempt_count" in actual
    assert "coalesce(sum(attempt.http_request_count), 0)::bigint as request_count" in actual

    # The intended identity and all acceptance properties are proven
    # independently, rather than inferred from an identity-filtered join.
    for identity_count, expected in (
        ("intended_attempt_count", 1),
        ("intended_success_count", 1),
        ("intended_request_count", 1),
        ("intended_http_200_count", 1),
        ("intended_http_3xx_count", 0),
        ("intended_http_status_history_count", 1),
        ("intended_exact_200_history_count", 1),
        ("intended_history_3xx_count", 0),
    ):
        assert identity_count in actual
        assert f"a.{identity_count} = {expected}" in sql
