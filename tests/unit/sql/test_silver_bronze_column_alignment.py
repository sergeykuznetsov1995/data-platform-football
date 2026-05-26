"""Assert that every Bronze column referenced by Silver SQL exists in the
snapshot fixture — catches `time` vs `minutes` / `xgchain` vs `xg_chain` /
`yellow_card` vs `yellow_cards` / etc. mismatches offline, before they hit
live Trino during DAG deployment.

This test is a direct followup to PR #50, where six column-name mismatches
between Silver SQL and Bronze schema were caught only at runtime. Issue #71.

Schema source of truth
----------------------
``tests/fixtures/bronze_schemas.json`` — committed snapshot updated via
``scripts/snapshot_bronze_schemas.py`` (idempotent; run inside the Airflow
container or from host with a Trino HTTPS port-forward).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Set

import pytest

# Same-directory helper — make sure it's importable without installing it
# as a package. ``conftest.py`` could also do this; keeping it local for now.
import sys

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from _bronze_alignment import (  # noqa: E402
    collect_bronze_refs,
    list_silver_sql_paths,
    load_silver_sql,
)

sqlglot = pytest.importorskip("sqlglot")  # parser dependency

_FIXTURE_PATH = Path(__file__).resolve().parents[2] / "fixtures" / "bronze_schemas.json"


@pytest.fixture(scope="module")
def fixture() -> dict:
    if not _FIXTURE_PATH.exists():
        pytest.fail(
            f"Bronze schema fixture missing: {_FIXTURE_PATH}. "
            "Run `python scripts/snapshot_bronze_schemas.py` "
            "(inside the airflow-webserver container, or from host with TRINO_* env vars set)."
        )
    return json.loads(_FIXTURE_PATH.read_text())


@pytest.fixture(scope="module")
def bronze_columns(fixture) -> Dict[str, Set[str]]:
    """``{bronze.table_name: set(column_names)}`` from the fixture."""
    return {
        name: set(meta["columns"].keys())
        for name, meta in fixture["tables"].items()
    }


@pytest.fixture(scope="module")
def known_missing(fixture) -> Set[str]:
    """Bronze tables the snapshot script saw as TABLE_NOT_FOUND in Trino.

    Silver SQL referencing only known-missing tables is skipped (xfail-like)
    so deprecated upstream sources (e.g. ``fbref_shot_events``) don't fail
    the suite. A Silver SQL touching at least one *present* Bronze table is
    still validated against those.
    """
    return set(fixture.get("missing_tables", []))


@pytest.mark.unit
@pytest.mark.parametrize(
    "sql_path",
    list_silver_sql_paths(),
    ids=lambda p: p.name,
)
def test_silver_sql_columns_exist_in_bronze(
    sql_path: Path,
    bronze_columns: Dict[str, Set[str]],
    known_missing: Set[str],
):
    sql = load_silver_sql(sql_path)
    refs = collect_bronze_refs(sql)

    if not refs:
        pytest.skip(f"{sql_path.name} references no Bronze tables (silver-only consumer)")

    # Tables referenced but not in fixture & not in known_missing: fixture stale.
    untracked = set(refs.keys()) - {n.removeprefix("bronze.") for n in bronze_columns} - known_missing
    assert not untracked, (
        f"{sql_path.name} references Bronze tables not in fixture: {sorted(untracked)}. "
        f"Run `scripts/snapshot_bronze_schemas.py` to refresh the snapshot."
    )

    failures = []
    for bronze_table, referenced_cols in sorted(refs.items()):
        if bronze_table in known_missing:
            continue  # upstream table deprecated — skip
        known_cols = bronze_columns[f"bronze.{bronze_table}"]
        missing_cols = referenced_cols - known_cols
        if missing_cols:
            failures.append((bronze_table, sorted(missing_cols)))

    assert not failures, (
        f"{sql_path.name}: Silver SQL references columns not present in Bronze schema:\n"
        + "\n".join(f"  bronze.{t}: {cols}" for t, cols in failures)
    )


@pytest.mark.unit
def test_extractor_smoke_detects_unknown_column(bronze_columns):
    """Positive control for the extractor itself.

    Synthesize a Silver SQL that references a column that does not exist in
    the bronze fixture. The extractor must surface it; otherwise the main
    test above can silently pass even with bugs.
    """
    fake_sql = """
    SELECT nonexistent_col, game_id
    FROM iceberg.bronze.understat_player_match_stats
    """
    refs = collect_bronze_refs(fake_sql)
    assert "understat_player_match_stats" in refs
    assert "nonexistent_col" in refs["understat_player_match_stats"]
    assert "nonexistent_col" not in bronze_columns["bronze.understat_player_match_stats"]


@pytest.mark.unit
def test_extractor_skips_window_function_aliases():
    """``ROW_NUMBER() OVER (...) AS rn`` introduced inside an inner SELECT
    must not be reported as a Bronze column reference even when the outer
    SELECT filters on it via passthrough CTE.

    Regression guard for the original walker — see _bronze_alignment.py
    docstring.
    """
    sql = """
    WITH dedup AS (
        SELECT *
        FROM (
            SELECT b.*, ROW_NUMBER() OVER (PARTITION BY id) AS rn
            FROM iceberg.bronze.understat_player_match_stats b
        )
        WHERE rn = 1
    )
    SELECT game_id FROM dedup
    """
    refs = collect_bronze_refs(sql)
    assert "understat_player_match_stats" in refs
    assert "rn" not in refs["understat_player_match_stats"], (
        "Window-function alias 'rn' must not be attributed to bronze"
    )
    assert "game_id" in refs["understat_player_match_stats"]
