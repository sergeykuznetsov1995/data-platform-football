from pathlib import Path
import re


SQL = (
    Path(__file__).parents[3]
    / "docs"
    / "operations"
    / "sql"
    / "fbref_production_acceptance.sql"
).read_text(encoding="utf-8")
CANARY_SQL = (
    Path(__file__).parents[3]
    / "docs"
    / "operations"
    / "sql"
    / "fbref_canary_acceptance.sql"
).read_text(encoding="utf-8")
CONTROL_SQL = (
    Path(__file__).parents[3]
    / "docs"
    / "operations"
    / "sql"
    / "fbref_control_dataset_acceptance.sql"
).read_text(encoding="utf-8")
READINESS = (
    Path(__file__).parents[3]
    / "FBREF_PRODUCTION_READINESS_2026-07-11.md"
).read_text(encoding="utf-8")


def _without_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


def test_acceptance_sql_is_read_only_and_parameterized():
    executable = _without_comments(SQL)

    assert ":control_run_id" in executable
    assert ":expected_current_male_competitions" in executable
    assert len(re.findall(r"\bPASS\b", executable, flags=re.IGNORECASE)) >= 4
    assert not re.search(
        r"\b(CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|MERGE|CALL)\b",
        executable,
        flags=re.IGNORECASE,
    )


def test_acceptance_sql_covers_scope_generic_typed_silver_and_staging():
    for relation in (
        "fbref_target_scope",
        "fbref_page_manifest",
        "fbref_table_inventory",
        "fbref_table_cells",
        "fbref_dataset_availability",
        "fbref_match_enriched",
        "fbref_player_match_stats",
        "information_schema.tables",
    ):
        assert relation in SQL
    assert "eligible_male" in SQL
    assert "outside_scope_rows" in SQL
    assert "__stg_" in SQL


def test_acceptance_sql_enumerates_every_production_typed_dataset():
    from scrapers.fbref.typed_bronze import (
        MATCH_AVAILABILITY_TABLE,
        MATCH_DATASET_TABLES,
        SEASON_DATASET_TABLES,
    )

    typed_tables = {
        "fbref_schedule",
        MATCH_AVAILABILITY_TABLE,
        *MATCH_DATASET_TABLES.values(),
        *SEASON_DATASET_TABLES.values(),
    }

    missing = sorted(table for table in typed_tables if table not in SQL)
    assert missing == []

    assert sorted(
        dataset
        for dataset in {
            "schedule",
            *MATCH_DATASET_TABLES,
            *SEASON_DATASET_TABLES,
        }
        if f"('{dataset}'," not in SQL
    ) == []


def test_canary_sql_is_read_only_and_has_exact_profile_scope():
    executable = _without_comments(CANARY_SQL)

    assert ":control_run_id" in executable
    assert executable.count(";") == 5
    assert not re.search(
        r"\b(CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|MERGE|CALL)\b",
        executable,
        flags=re.IGNORECASE,
    )
    for relation in (
        "fbref_page_manifest",
        "fbref_table_inventory",
        "fbref_table_cells",
        "fbref_dataset_availability",
        "information_schema.tables",
    ):
        assert relation in CANARY_SQL
    assert "fbref_target_scope" not in executable
    assert "outside_scope_rows" not in executable
    assert "iceberg.silver" not in executable


def test_canary_sql_enumerates_every_typed_dataset_and_exact_batch():
    from scrapers.fbref.typed_bronze import (
        MATCH_AVAILABILITY_TABLE,
        MATCH_DATASET_TABLES,
        SEASON_DATASET_TABLES,
    )

    typed_tables = {
        "fbref_schedule",
        MATCH_AVAILABILITY_TABLE,
        *MATCH_DATASET_TABLES.values(),
        *SEASON_DATASET_TABLES.values(),
    }
    assert sorted(table for table in typed_tables if table not in CANARY_SQL) == []
    datasets = {
        "schedule",
        "dataset_availability",
        *MATCH_DATASET_TABLES,
        *SEASON_DATASET_TABLES,
    }
    assert sorted(
        dataset for dataset in datasets if f"('{dataset}'," not in CANARY_SQL
    ) == []
    generator = CANARY_SQL.split("-- 3.", 1)[1].split("-- 4.", 1)[0]
    assert "WHERE _batch_id =" in generator
    assert "CAST(:control_run_id AS varchar)" in generator
    assert "fbref_target_scope" not in generator


def test_scope_and_dataset_gates_are_derived_not_hardcoded():
    assert "active_male_competitions" in SQL
    assert "complete_current_competitions = active_male_competitions" in SQL
    assert (
        "active_male_competitions =\n"
        "             CAST(:expected_current_male_competitions AS bigint)"
    ) in SQL
    assert "eligibility_flag_mismatches = 0" in SQL
    assert "duplicate_scope_rows = 0" in SQL
    assert "match_dataset_matrix" in SQL
    assert "season_dataset_matrix" in SQL
    assert "availability_matches" in SQL
    assert "expected_matches" in SQL
    assert "missing_matches" in SQL
    assert "117" not in _without_comments(SQL)


def test_generated_scope_audit_includes_events_lineups_and_availability():
    generated = SQL.split("-- 3b.", 1)[1].split("-- 3c.", 1)[0]
    for table in (
        "fbref_match_events",
        "fbref_lineups",
        "fbref_shot_events",
        "fbref_dataset_availability",
    ):
        assert table in generated
    assert "outside_scope_rows" in generated


def test_optional_typed_tables_never_break_trino_planning_when_absent():
    from scrapers.fbref.typed_bronze import (
        MATCH_DATASET_TABLES,
        SEASON_DATASET_TABLES,
    )

    executable = _without_comments(SQL)
    optional_tables = {
        *MATCH_DATASET_TABLES.values(),
        *SEASON_DATASET_TABLES.values(),
    }
    assert sorted(
        table
        for table in optional_tables
        if f"FROM iceberg.bronze.{table}" in executable
    ) == []
    assert "VERIFY_POSTGRES_EXPLICIT_EMPTY" in SQL
    assert "VERIFY_POSTGRES_TABLE_REQUIREMENT" in SQL
    assert "match_signal.available_rows" in SQL
    assert "PASS_EXPLICIT_EMPTY_ALLOWED" not in SQL
    assert "physical_requirement = 'required'" in SQL


def test_policy_skipped_and_direct_match_seasons_are_explicitly_exempt():
    from scrapers.fbref.constants import UNAVAILABLE_SEASON_STAT_ROUTES

    for route in UNAVAILABLE_SEASON_STAT_ROUTES:
        assert f"'{route}', 'policy_exempt'" in SQL
    assert "PASS_POLICY_EXEMPT" in SQL
    assert "PASS_DIRECT_MATCH_ONLY" in SQL
    assert "VERIFY_POSTGRES_DISCOVERED_ROUTE_MANIFEST" in SQL


def test_postgres_companion_proves_explicit_empty_season_manifests():
    executable = _without_comments(CONTROL_SQL)

    assert ":control_run_id" in executable
    assert ":expected_run_type" in executable
    assert ":expected_request_limit" in executable
    assert ":expected_byte_limit_mb" in executable
    assert "run_type = CAST(:expected_run_type AS text)" in executable
    assert (
        "request_limit = CAST(:expected_request_limit AS integer)"
        in executable
    )
    assert (
        "CAST(:expected_byte_limit_mb AS bigint) * 1048576"
        in executable
    )
    assert "observation_processing" in CONTROL_SQL
    assert "dataset_manifest" in CONTROL_SQL
    assert "typed:__complete__" in CONTROL_SQL
    assert "'available', 'empty', 'restricted', 'not_applicable'" in CONTROL_SQL
    assert "direct_match_only" in CONTROL_SQL
    assert "discovered_requirements" in CONTROL_SQL
    assert "supported_season_route_frontier" in CONTROL_SQL
    assert "processing_status = 'succeeded'" in CONTROL_SQL
    assert "dataset_requires_materialized_table" in CONTROL_SQL
    assert "metadata ? 'raw_baseline'" in CONTROL_SQL
    assert "metadata ? 'raw_fetch_attempt_snapshot'" in CONTROL_SQL
    assert "frontier.page_kind IN ('season', 'season_stats')" in CONTROL_SQL
    assert "frontier.page_kind = 'season' THEN 'standard'" in CONTROL_SQL
    assert "match_control_manifest_matrix" in CONTROL_SQL
    assert "backfill/200/100" in CONTROL_SQL
    assert "reviewed_live_transport" in CONTROL_SQL
    assert "fbref-camoufox-metered-warm-http-v6" in CONTROL_SQL
    assert "successful_warm_http_attempts > 0" in CONTROL_SQL
    assert "attempt.session_version = session.session_id::text" in CONTROL_SQL
    assert "status IN ('closed', 'failed', 'expired')" in CONTROL_SQL
    assert "terminal_sessions = clearance_sessions" in CONTROL_SQL
    assert "closed_sessions" not in CONTROL_SQL
    assert "linked_successful_warm_http_attempts > 0" in CONTROL_SQL
    assert "unlinked_successful_warm_http_attempts = 0" in CONTROL_SQL
    assert "session.browser_bootstrap_attempts > 0" in CONTROL_SQL
    assert "session.browser_bootstrap_requests > 0" in CONTROL_SQL
    assert "session.http_requests > 0" in CONTROL_SQL
    assert "all_fetch_attempts = 0" in CONTROL_SQL
    assert "clearance_sessions = 0" in CONTROL_SQL
    assert not re.search(
        r"\b(CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|MERGE|CALL)\b",
        executable,
        flags=re.IGNORECASE,
    )


def test_postgres_dataset_matrices_use_only_exact_run_fetches():
    dataset_matrices = CONTROL_SQL.split("FROM route_targets;", 1)[1]
    season_matrix, match_matrix = dataset_matrices.split(
        "-- Prove every eligible match identity", 1
    )

    for matrix in (season_matrix, match_matrix):
        assert "WITH selected_run AS" in matrix
        assert "END AS evidence_run_id" in matrix
        assert "attempt.run_id = selected_run.evidence_run_id" in matrix
        assert "attempt.run_id = CAST(:control_run_id AS uuid)" not in matrix
        assert "attempt.status = 'succeeded'" in matrix
        assert "attempt.raw_manifest_key IS NOT NULL" in matrix
        assert "JOIN run_fetches AS run_fetch" in matrix
        assert "run_fetch.target_id = frontier.target_id" in matrix
        assert (
            "processing.logical_refresh_id = run_fetch.logical_refresh_id"
            in matrix
        )
        assert "processing.content_hash = run_fetch.content_hash" in matrix
        assert (
            "processing.parser_version = 'fbref-page-document-v3'" in matrix
        )
        assert (
            "processing.typed_parser_version = 'fbref-typed-bronze-v3'"
            in matrix
        )
        assert (
            "processing.stateful_parser_version = "
            "'fbref-discovery-parser-v6'" in matrix
        )
        assert "latest_observation" not in matrix
        assert "DISTINCT ON (processing.target_id)" not in matrix
        assert not re.search(r"\bAS fetch\b", matrix, flags=re.IGNORECASE)

    assert (
        "observation.logical_refresh_id = requirement.logical_refresh_id"
        in season_matrix
    )
    assert (
        "observation.logical_refresh_id = direct.logical_refresh_id"
        in match_matrix
    )
    assert match_matrix.count("('match_") == 6
    assert "('shot_events')" in match_matrix
    assert "('lineups')" in match_matrix


def test_postgres_replay_uses_audited_source_run_evidence():
    control_gate = CONTROL_SQL.split("-- Prove that a live run", 1)[0]
    dataset_matrices = CONTROL_SQL.split("FROM route_targets;", 1)[1]

    assert "audited_control_run_id IS NOT NULL" in control_gate
    assert "audited_control_run_id = run_id" in control_gate
    assert "audited_control_run_id <> run_id" in control_gate
    assert "evidence_run.status = 'succeeded'" in control_gate
    assert "evidence_run.run_type IN ('current', 'backfill')" in control_gate
    assert "evidence_run.request_limit = 200" in control_gate
    assert "evidence_run.byte_limit = 100 * 1048576" in control_gate
    assert "->> 'run_type' = run_type" in control_gate
    assert "->> 'zero_delta_required'" in control_gate
    assert "run_type = 'replay' THEN 'true' ELSE 'false'" in control_gate
    assert (
        "metadata -> 'raw_audit' ->> 'processing_control_run_id'\n"
        "             = run_id::text"
    ) in control_gate

    for matrix in dataset_matrices.split(
        "-- Prove every eligible match identity", 1
    ):
        assert "WHEN run.run_type = 'replay'" in matrix
        assert "->> 'audited_control_run_id'" in matrix
        assert "WHEN run.run_type <> 'replay' THEN run.run_id" in matrix
        assert "attempt.run_id = selected_run.evidence_run_id" in matrix


def test_match_matrix_anti_joins_exact_expected_match_ids():
    match_matrix = SQL.split("-- 3c.", 1)[1].split("-- 3d.", 1)[0]

    assert "scheduled_match_ids" in match_matrix
    assert "expected_match_evidence" in match_matrix
    assert "availability.match_id = expected.match_id" in match_matrix
    assert "missing_match_ids" in match_matrix
    assert "extra_match_ids" in match_matrix
    assert "missing_matches, 0) = 0" in match_matrix
    assert "extras.extra_matches, 0) = 0" in match_matrix
    assert (
        "expected_matches, 0) = 0\n"
        "         AND coalesce(availability.availability_matches, 0) = 0"
    ) in match_matrix
    assert "VERIFY_POSTGRES_DIRECT_MATCH_IDENTITIES" in match_matrix


def test_silver_freshness_uses_oldest_row_not_one_fresh_row():
    silver = SQL.split("-- 4. Silver", 1)[1].split(
        "-- 5. Production", 1
    )[0]
    assert _without_comments(silver).count("min(_silver_created_at)") == 2
    assert "max(_silver_created_at)" not in silver
    assert "null_freshness = 0" in silver


def test_profile_specific_acceptance_scripts_are_routed_explicitly():
    header = "\n".join(SQL.splitlines()[:8])
    assert "only for the publishing 200-request / 100-MiB" in header
    assert "use fbref_canary_acceptance.sql" in header
    assert "`fbref_canary_acceptance.sql`" in READINESS
    assert "`fbref_production_acceptance.sql`" in READINESS
    assert "`fbref_control_dataset_acceptance.sql`" in READINESS
    assert "bindings `replay`, `0`, and `0`" in READINESS
