"""Static contract for the two-engine FBref male completeness gate."""

from pathlib import Path
import re

import pytest

from scrapers.fbref.bronze import (
    FBrefGenericBronzeWriter,
    GenericPagePersistItem,
)
from scrapers.fbref.page_document import parse_page_document


SQL_PATH = Path("docs/operations/sql/fbref_male_completeness_control.sql")


@pytest.mark.unit
def test_both_engine_statements_parse_in_their_declared_dialects():
    sqlglot = pytest.importorskip("sqlglot")
    sql = SQL_PATH.read_text(encoding="utf-8")
    marker = "-- ENGINE: Trino"
    postgres, trino = sql.split(marker, maxsplit=1)
    run_id = "00000000-0000-0000-0000-000000000001"
    postgres = postgres.replace(
        ":accepted_control_run_ids_json", f"'[\"{run_id}\"]'"
    ).replace(":publication_control_run_id", f"'{run_id}'")
    replacements = {
        ":accepted_control_run_ids_json": "'[\"run-one\"]'",
        ":expected_scope_ids_json": (
            '\'[{"source_competition_id":"9","source_season_id":"2025-2026"}]\''
        ),
        ":dead_match_ids_json": "'[]'",
        ":publication_control_run_id": "'run-one'",
        ":accepted_run_count": "1",
        ":accepted_run_keys_md5": "'d41d8cd98f00b204e9800998ecf8427e'",
        ":expected_scope_count": "1",
        ":expected_scope_distinct_count": "1",
        ":expected_scope_keys_md5": "'d41d8cd98f00b204e9800998ecf8427e'",
        ":dead_match_count": "0",
        ":dead_match_keys_md5": "'d41d8cd98f00b204e9800998ecf8427e'",
    }
    for placeholder in sorted(replacements, key=len, reverse=True):
        trino = trino.replace(placeholder, replacements[placeholder])

    assert sqlglot.parse_one(postgres, read="postgres") is not None
    assert sqlglot.parse_one(trino, read="trino") is not None


@pytest.mark.unit
def test_gate_crosses_active_male_registry_with_both_required_seasons():
    sql = SQL_PATH.read_text(encoding="utf-8")
    lowered = sql.casefold()

    assert "2025-2026" in sql
    assert "2026-2027" in sql
    assert "cross join required_seasons" in lowered
    assert "competition.gender = 'male'" in lowered
    assert "competition.crawl_state = 'active'" in lowered
    assert "competition.lifecycle_state in ('present', 'missing_once')" in lowered
    assert "competition.present" in lowered
    assert "expected_current_male_competitions" not in lowered
    assert "expected_scope_ids_json" in lowered
    assert "expected_scope_count" in lowered
    assert "expected_scope_distinct_count" in lowered
    assert "expected_scope_keys_md5" in lowered
    assert ":expected_scope_ids_json" in lowered
    assert ":expected_scope_count" in lowered
    assert ":expected_scope_keys_md5" in lowered


@pytest.mark.unit
def test_gate_deduplicates_schedule_match_keys_and_emits_competition_plus_total():
    sql = SQL_PATH.read_text(encoding="utf-8").casefold()

    assert "regexp_extract(schedule.match_url, '/matches/([^/]+)'" in sql
    assert "row_number() over" in sql
    assert "partition by source_competition_id, source_season_id, match_id" in sql
    assert "schedule_rank = 1" in sql
    assert "grouping sets" in sql
    assert "total" in sql
    assert "current_run_matches" in sql
    assert "iceberg.bronze.fbref_page_manifest" in sql
    assert "accepted_control_run_ids_json" in sql
    assert "publication_control_run_id" in sql
    assert "manifest_rank = 1" in sql


@pytest.mark.unit
def test_gate_requires_all_eight_dataset_decisions_and_direct_or_empty_proof():
    sql = SQL_PATH.read_text(encoding="utf-8").casefold()
    datasets = (
        "shot_events",
        "match_events",
        "lineups",
        "match_team_stats",
        "match_managers",
        "match_officials",
        "match_keeper_stats",
        "match_player_stats",
    )

    assert all(f"('{dataset}'" in sql for dataset in datasets)
    assert "availability_decisions = 8" in sql
    assert "current_run_match_rows = 1" in sql
    assert "availability = 'available'" in sql
    assert "typed_rows > 0" in sql
    assert re.search(
        r"availability\s+not\s+in\s*\(\s*'available',\s*'empty',"
        r"\s*'restricted',\s*'not_applicable'\s*\)",
        sql,
    )
    assert re.search(
        r"availability\s+in\s*\('empty', 'restricted', 'not_applicable'\)",
        sql,
    )
    assert "nullif(trim(availability.reason), '') is not null" in sql
    assert all(f"fbref_{dataset}" in sql for dataset in datasets)
    assert "manifest.parser_version = observation.typed_parser_version" in sql
    assert "_batch_id = manifest.run_id" in sql
    assert "manifest.parse_status <> 'success'" in sql
    assert "manifest.persist_status <> 'success'" in sql
    assert "manifest.validation_status <> 'success'" in sql


@pytest.mark.unit
def test_current_run_manifest_cte_accepts_the_writer_installed_status_values():
    duckdb = pytest.importorskip("duckdb")
    sql = SQL_PATH.read_text(encoding="utf-8")
    cte = re.search(
        r"\), current_run_matches AS \((.*?)\n\), availability_ranked AS",
        sql,
        flags=re.DOTALL,
    )
    assert cte is not None
    statement = cte.group(1)
    page = parse_page_document(
        "<table><tr><td>ok</td></tr></table>",
        target_id="fbref:match:m1",
        page_kind="match",
        source_ids={"match_id": "m1"},
    )
    manifest = FBrefGenericBronzeWriter._validate_page_batch(
        (
            GenericPagePersistItem(
                page=page,
                canonical_url="https://example.invalid/m1",
                run_id="run-one",
                staging_identity="refresh-one",
            ),
        )
    )[0].manifest
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE fbref_page_manifest (
            target_id VARCHAR, page_kind VARCHAR, run_id VARCHAR,
            parse_status VARCHAR, persist_status VARCHAR,
            validation_status VARCHAR, content_hash VARCHAR,
            parser_version VARCHAR, persisted_at TIMESTAMP
        )
        """
    )
    connection.execute(
        "INSERT INTO fbref_page_manifest VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            manifest["target_id"],
            manifest["page_kind"],
            "run-one",
            manifest["parse_status"],
            manifest["persist_status"],
            manifest["validation_status"],
            manifest["content_hash"],
            manifest["parser_version"],
            "2026-08-08 00:00:00",
        ),
    )

    rows = connection.execute(
        "WITH accepted_run_bridge(run_id) AS (VALUES ('run-one')), "
        "page_manifest_ranked AS ("
        "SELECT *, row_number() OVER (PARTITION BY target_id "
        "ORDER BY persisted_at DESC, content_hash DESC, parser_version DESC, "
        "run_id DESC) AS manifest_rank FROM fbref_page_manifest), "
        f"current_run_matches AS ({statement}) "
        "SELECT match_id, current_run_match_rows, "
        "invalid_current_run_match_rows FROM current_run_matches"
    ).fetchall()

    assert rows == [("m1", 1, 0)]


@pytest.mark.unit
def test_omitted_latest_run_cannot_reuse_an_older_accepted_manifest():
    duckdb = pytest.importorskip("duckdb")
    sql = SQL_PATH.read_text(encoding="utf-8")
    cte = re.search(
        r"\), current_run_matches AS \((.*?)\n\), availability_ranked AS",
        sql,
        flags=re.DOTALL,
    )
    assert cte is not None
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE page_manifest_ranked (
            target_id VARCHAR, page_kind VARCHAR, run_id VARCHAR,
            parse_status VARCHAR, persist_status VARCHAR,
            validation_status VARCHAR, manifest_rank BIGINT
        );
        CREATE TABLE accepted_run_bridge (run_id VARCHAR);
        INSERT INTO accepted_run_bridge VALUES ('run-one');
        INSERT INTO page_manifest_ranked VALUES
            ('fbref:match:m1', 'match', 'run-one',
             'success', 'success', 'success', 2),
            ('fbref:match:m1', 'match', 'run-two',
             'success', 'success', 'success', 1);
        """
    )

    evidence = connection.execute(
        f"WITH current_run_matches AS ({cte.group(1)}) "
        "SELECT run_id, current_run_match_rows, accepted_manifest_rows "
        "FROM current_run_matches"
    ).fetchone()

    assert evidence == ("run-two", 1, 0)


@pytest.mark.unit
def test_missing_entire_competition_fails_the_real_publication_set_reconciliation():
    duckdb = pytest.importorskip("duckdb")
    sql = SQL_PATH.read_text(encoding="utf-8")
    cte = re.search(
        r"\), publication_scope_evidence AS \((.*?)\n\), selected_run_scope_base AS",
        sql,
        flags=re.DOTALL,
    )
    assert cte is not None
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE expected_scope_bridge (
            source_competition_id VARCHAR, source_season_id VARCHAR
        );
        CREATE TABLE published_scope (
            source_competition_id VARCHAR, source_season_id VARCHAR
        );
        INSERT INTO expected_scope_bridge VALUES
            ('9', '2025-2026'), ('9', '2026-2027'),
            ('12', '2025-2026'), ('12', '2026-2027');
        INSERT INTO published_scope VALUES
            ('9', '2025-2026'), ('9', '2026-2027');
        """
    )

    evidence = connection.execute(
        f"WITH publication_scope_evidence AS ({cte.group(1)}) "
        "SELECT missing_expected_scope_rows, unexpected_published_scope_rows "
        "FROM publication_scope_evidence"
    ).fetchone()

    assert evidence == (2, 0)


@pytest.mark.unit
def test_selected_run_scope_evidence_accepts_two_complete_runs_and_rejects_omission():
    duckdb = pytest.importorskip("duckdb")
    sql = SQL_PATH.read_text(encoding="utf-8")
    cte = re.search(
        r"\), selected_run_scope_evidence AS \((.*?)\n\), schedule_base AS",
        sql,
        flags=re.DOTALL,
    )
    assert cte is not None
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE accepted_run_bridge (run_id VARCHAR);
        CREATE TABLE expected_scope_bridge (
            source_competition_id VARCHAR, source_season_id VARCHAR
        );
        CREATE TABLE selected_run_scope (
            run_id VARCHAR, source_competition_id VARCHAR,
            source_season_id VARCHAR
        );
        INSERT INTO accepted_run_bridge VALUES ('run-one'), ('run-two');
        INSERT INTO expected_scope_bridge VALUES
            ('9', '2025-2026'), ('9', '2026-2027');
        INSERT INTO selected_run_scope VALUES
            ('run-one', '9', '2025-2026'),
            ('run-one', '9', '2026-2027'),
            ('run-two', '9', '2025-2026'),
            ('run-two', '9', '2026-2027');
        """
    )
    statement = (
        f"WITH selected_run_scope_evidence AS ({cte.group(1)}) "
        "SELECT missing_selected_run_scope_rows, "
        "unexpected_selected_run_scope_rows "
        "FROM selected_run_scope_evidence"
    )

    assert connection.execute(statement).fetchone() == (0, 0)
    connection.execute("DELETE FROM selected_run_scope WHERE run_id = 'run-two'")
    assert connection.execute(statement).fetchone() == (2, 0)


@pytest.mark.unit
def test_accepted_run_set_evidence_rejects_duplicate_run_ids():
    duckdb = pytest.importorskip("duckdb")
    sql = SQL_PATH.read_text(encoding="utf-8")
    cte = re.search(
        r"\), accepted_run_set_evidence AS \((.*?)\n\), expected_scope_bridge AS",
        sql,
        flags=re.DOTALL,
    )
    assert cte is not None
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE accepted_run_bridge (run_id VARCHAR)")
    connection.execute(
        "INSERT INTO accepted_run_bridge VALUES ('run-one'), ('run-one')"
    )

    evidence = connection.execute(
        f"WITH accepted_run_set_evidence AS ({cte.group(1)}) "
        "SELECT accepted_run_count, accepted_run_distinct_count "
        "FROM accepted_run_set_evidence"
    ).fetchone()

    assert evidence == (2, 1)


@pytest.mark.unit
def test_dead_match_bridge_is_bound_from_the_postgres_control_result():
    sql = SQL_PATH.read_text(encoding="utf-8").casefold()

    assert "dead_match_ids_json" in sql
    assert "dead_match_count" in sql
    assert "dead_match_keys_md5" in sql
    assert ":dead_match_ids_json" in sql
    assert ":dead_match_count" in sql
    assert ":dead_match_keys_md5" in sql
    assert "dead_bridge_rows = 1" in sql
    assert "current_run_match_rows = 0" in sql
    assert "dead_bridge_rows = 0" in sql


@pytest.mark.unit
def test_dead_match_classifier_accepts_only_a_clean_control_bridge_row():
    duckdb = pytest.importorskip("duckdb")
    sql = SQL_PATH.read_text(encoding="utf-8")
    cte = re.search(
        r"\), classified_match_proof AS \((.*?)\n\), per_scope AS",
        sql,
        flags=re.DOTALL,
    )
    assert cte is not None
    connection = duckdb.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE match_proof (
            match_id VARCHAR,
            dead_bridge_rows BIGINT,
            current_run_match_rows BIGINT,
            invalid_current_run_match_rows BIGINT,
            accepted_manifest_rows BIGINT,
            accepted_schedule_rows BIGINT,
            availability_decisions BIGINT,
            invalid_dataset_decisions BIGINT,
            directly_materialized_datasets BIGINT,
            explicitly_empty_datasets BIGINT
        )
        """
    )
    connection.executemany(
        "INSERT INTO match_proof VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            ("durable", 0, 1, 0, 1, 1, 8, 0, 5, 3),
            ("dead", 1, 0, 1, 0, 1, 0, 8, 0, 0),
            ("ambiguous", 1, 1, 0, 1, 1, 8, 0, 5, 3),
            ("stale", 0, 1, 0, 0, 1, 8, 0, 5, 3),
        ),
    )

    rows = connection.execute(
        f"WITH classified_match_proof AS ({cte.group(1)}) "
        "SELECT match_id, trino_proof FROM classified_match_proof "
        "ORDER BY match_id"
    ).fetchall()

    assert rows == [
        ("ambiguous", "unproved"),
        ("dead", "dead"),
        ("durable", "durable"),
        ("stale", "unproved"),
    ]


@pytest.mark.unit
def test_companion_control_query_proves_success_or_auditable_dead_target():
    sql = SQL_PATH.read_text(encoding="utf-8").casefold()

    assert "engine: postgresql" in sql
    assert "engine: trino" in sql
    assert "fbref_control.observation_processing" in sql
    assert "fbref_control.dataset_manifest" in sql
    assert "observation.frontier_state = 'dead'" in sql
    assert "nullif(trim(observation.last_error_class), '') is not null" in sql
    assert "control_proof in ('durable', 'dead')" in sql
    assert "frontier.last_content_hash" in sql
    assert "attempt.content_hash" in sql
    assert "latest_global_target" in sql
    assert "target.run_id = accepted.control_run_id" in sql
    assert re.search(
        r"accepted(?:_set)?\.accepted_run_count\s*=\s*"
        r"accepted(?:_set)?\.accepted_run_distinct_count",
        sql,
    )
    assert "every returned verdict must be pass" in sql
    assert sql.count("group by grouping sets") == 2
