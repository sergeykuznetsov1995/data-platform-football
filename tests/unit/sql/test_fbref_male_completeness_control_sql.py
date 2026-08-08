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


@pytest.mark.unit
def test_gate_deduplicates_schedule_match_keys_and_emits_competition_plus_total():
    sql = SQL_PATH.read_text(encoding="utf-8").casefold()

    assert "regexp_extract(match_url, '/matches/([^/]+)'" in sql
    assert "row_number() over" in sql
    assert "partition by source_competition_id, source_season_id, match_id" in sql
    assert "schedule_rank = 1" in sql
    assert "grouping sets" in sql
    assert "total" in sql
    assert "current_run_matches" in sql
    assert "iceberg.bronze.fbref_page_manifest" in sql
    assert "manifest.run_id = cast(:control_run_id as varchar)" in sql


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
    assert "_batch_id = cast(:control_run_id as varchar)" in sql
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
    statement = cte.group(1).replace(
        "iceberg.bronze.fbref_page_manifest", "fbref_page_manifest"
    ).replace("CAST(:control_run_id AS varchar)", "'run-one'")
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
            validation_status VARCHAR
        )
        """
    )
    connection.execute(
        "INSERT INTO fbref_page_manifest VALUES (?, ?, ?, ?, ?, ?)",
        (
            manifest["target_id"],
            manifest["page_kind"],
            "run-one",
            manifest["parse_status"],
            manifest["persist_status"],
            manifest["validation_status"],
        ),
    )

    rows = connection.execute(
        f"WITH current_run_matches AS ({statement}) "
        "SELECT match_id, current_run_match_rows, "
        "invalid_current_run_match_rows FROM current_run_matches"
    ).fetchall()

    assert rows == [("m1", 1, 0)]


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
            availability_decisions BIGINT,
            invalid_dataset_decisions BIGINT,
            directly_materialized_datasets BIGINT,
            explicitly_empty_datasets BIGINT
        )
        """
    )
    connection.executemany(
        "INSERT INTO match_proof VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            ("durable", 0, 1, 0, 8, 0, 5, 3),
            ("dead", 1, 0, 1, 0, 8, 0, 0),
            ("ambiguous", 1, 1, 0, 8, 0, 5, 3),
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
    assert "every returned verdict must be pass" in sql
    assert sql.count("group by grouping sets") == 2
