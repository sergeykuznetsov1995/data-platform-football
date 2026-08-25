"""Static safety contract for the bounded FBref 2026-08-25 remediations.

These tests intentionally inspect executable SQL rather than exercising a live
database.  The production files are one-shot operator tools, so their safety
boundary must be reviewable without granting a test process write access to the
control plane.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_DIR = PROJECT_ROOT / "docs" / "operations" / "sql"
LARGE_PAGES_SQL = SQL_DIR / "fbref_20260825_reanimate_large_pages.sql"
FALSE_SEASONS_SQL = SQL_DIR / "fbref_20260825_reopen_false_season_quarantines.sql"

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
