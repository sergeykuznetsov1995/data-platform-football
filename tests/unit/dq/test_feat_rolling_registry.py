"""Unit tests for feat_* rolling-column registries (#186) — sync between
``gold_tasks.FEAT_*_ROLLING_COLS`` and the corresponding SQL files.

Why
---
``validate_gold_quality()`` materialises ``CHECK.point_in_time(...)`` checks
per registry column. If a registry name drifts away from the SQL output
column, the DQ check silently passes (no rows match) and data leakage goes
undetected. These tests fail loudly on drift.

Replaces the narrower ``test_e6_rolling_registry.py`` (removed in PR #185 as
orphaned when the registries were deleted in 583e645). Restored here covering
all SIX feat_* registries, not just the two E6 tables.

Coverage
--------
* Each registry column appears as a whole-word identifier in the main SQL,
  and — for tables that ship an ``_empty`` fallback — in that fallback too.
* Registries have no duplicates (would inflate validate_gold_quality count),
  no empty entries, and are lowercase (Trino case-folds identifiers).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = REPO_ROOT / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from utils import gold_tasks  # noqa: E402

pytestmark = pytest.mark.unit

SQL_DIR = REPO_ROOT / "dags" / "sql" / "gold"


# ---------------------------------------------------------------------------
# Registry → SQL-file mapping.  ``empty`` is the _empty fallback SQL filename
# or None for tables that have no fallback variant.
# ---------------------------------------------------------------------------
REGISTRIES = {
    "FEAT_TEAM_FORM_ROLLING_COLS": {
        "cols": gold_tasks.FEAT_TEAM_FORM_ROLLING_COLS,
        "main": "feat_team_form.sql",
        "empty": None,
    },
    "FEAT_TEAM_H2H_ROLLING_COLS": {
        "cols": gold_tasks.FEAT_TEAM_H2H_ROLLING_COLS,
        "main": "feat_team_h2h.sql",
        "empty": None,
    },
    "FEAT_TEAM_XG_FORM_ROLLING_COLS": {
        "cols": gold_tasks.FEAT_TEAM_XG_FORM_ROLLING_COLS,
        "main": "feat_team_xg_form.sql",
        "empty": "feat_team_xg_form_empty.sql",
    },
    "FEAT_PLAYER_FORM_ROLLING_COLS": {
        "cols": gold_tasks.FEAT_PLAYER_FORM_ROLLING_COLS,
        "main": "feat_player_form.sql",
        "empty": None,
    },
    "FEAT_REFEREE_BIAS_ROLLING_COLS": {
        "cols": gold_tasks.FEAT_REFEREE_BIAS_ROLLING_COLS,
        "main": "feat_referee_bias.sql",
        "empty": "feat_referee_bias_empty.sql",
    },
    "FEAT_TEAM_EVENT_STYLE_ROLLING_COLS": {
        "cols": gold_tasks.FEAT_TEAM_EVENT_STYLE_ROLLING_COLS,
        "main": "feat_team_event_style.sql",
        "empty": "feat_team_event_style_empty.sql",
    },
}


def _strip_comments(sql: str) -> str:
    """Drop -- line + /* */ block comments before identifier search."""
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql


def _read(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Sync tests — registry vs SQL
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", REGISTRIES.keys())
def test_all_cols_present_in_main_sql(name):
    """Every registry column must appear in its main SQL — else the
    point_in_time mask check targets a non-existent column (silent pass)."""
    spec = REGISTRIES[name]
    body = _strip_comments(_read(spec["main"]))
    for col in spec["cols"]:
        pat = r"\b" + re.escape(col) + r"\b"
        assert re.search(pat, body), (
            f"{spec['main']} missing rolling col {col!r} ({name})"
        )


@pytest.mark.parametrize(
    "name",
    [n for n, s in REGISTRIES.items() if s["empty"] is not None],
)
def test_all_cols_present_in_empty_fallback(name):
    """Columns must also exist in the _empty fallback SQL — the fallback is
    materialised when an optional Silver dependency is absent, and the DQ
    check runs against whichever variant shipped."""
    spec = REGISTRIES[name]
    body = _strip_comments(_read(spec["empty"]))
    for col in spec["cols"]:
        pat = r"\b" + re.escape(col) + r"\b"
        assert re.search(pat, body), (
            f"{spec['empty']} missing rolling col {col!r} ({name})"
        )


# ---------------------------------------------------------------------------
# Registry hygiene
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", REGISTRIES.keys())
def test_registry_no_duplicates(name):
    registry = REGISTRIES[name]["cols"]
    dupes = [c for c in registry if registry.count(c) > 1]
    assert len(registry) == len(set(registry)), f"{name} duplicates: {dupes}"


@pytest.mark.parametrize("name", REGISTRIES.keys())
def test_registry_non_empty(name):
    registry = REGISTRIES[name]["cols"]
    assert len(registry) > 0, f"{name} is empty"
    for col in registry:
        assert col, f"{name} contains empty/None entry"


@pytest.mark.parametrize("name", REGISTRIES.keys())
def test_registry_lowercase(name):
    """Trino case-folds; an uppercase registry entry would make the CHECK SQL
    reference a column under a different case → false-pass."""
    for col in REGISTRIES[name]["cols"]:
        assert col == col.lower(), f"{name} entry {col!r} has uppercase chars"
