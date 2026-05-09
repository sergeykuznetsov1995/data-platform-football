"""Unit tests for E6 rolling-column registries (W5a) — sync between
``gold_tasks.FEAT_*_ROLLING_COLS`` and the corresponding SQL files.

Why
---
``validate_gold_quality()`` materialises ``CHECK.point_in_time(...)``
checks per registry column. If a registry name drifts away from the
SQL output column, the DQ check silently passes (no rows match) and
data leakage goes undetected. These tests fail loudly on drift.

Coverage
--------
* Each registry column appears as a whole-word identifier in BOTH the
  main SQL and the empty-fallback SQL.
* Registry has no duplicates (would inflate validate_gold_quality count).
* Registry names are lowercase (Trino case-folds identifiers).
* Registry name patterns match the documented suffix conventions
  (``_l5_avg`` / ``_l10`` / ``_l5_*`` / ``_l20``).
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


# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------

FEAT_REFEREE_BIAS_SQL       = REPO_ROOT / "dags" / "sql" / "gold" / "feat_referee_bias.sql"
FEAT_REFEREE_BIAS_EMPTY     = REPO_ROOT / "dags" / "sql" / "gold" / "feat_referee_bias_empty.sql"
FEAT_TEAM_EVENT_STYLE_SQL   = REPO_ROOT / "dags" / "sql" / "gold" / "feat_team_event_style.sql"
FEAT_TEAM_EVENT_STYLE_EMPTY = REPO_ROOT / "dags" / "sql" / "gold" / "feat_team_event_style_empty.sql"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    """Drop -- line + /* */ block comments before identifier search."""
    sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql


# ---------------------------------------------------------------------------
# Sync tests — registry vs SQL
# ---------------------------------------------------------------------------

class TestRefereeBiasRegistryColumnsInSQL:
    """Every column in FEAT_REFEREE_BIAS_ROLLING_COLS must appear in BOTH SQLs."""

    def test_all_cols_present_in_main_sql(self):
        body = _strip_comments(_read(FEAT_REFEREE_BIAS_SQL))
        for col in gold_tasks.FEAT_REFEREE_BIAS_ROLLING_COLS:
            pat = r"\b" + re.escape(col) + r"\b"
            assert re.search(pat, body), (
                f"feat_referee_bias.sql missing rolling col {col!r}"
            )

    def test_all_cols_present_in_empty_fallback(self):
        body = _strip_comments(_read(FEAT_REFEREE_BIAS_EMPTY))
        for col in gold_tasks.FEAT_REFEREE_BIAS_ROLLING_COLS:
            pat = r"\b" + re.escape(col) + r"\b"
            assert re.search(pat, body), (
                f"feat_referee_bias_empty.sql missing rolling col {col!r}"
            )


class TestTeamEventStyleRegistryColumnsInSQL:
    """Every column in FEAT_TEAM_EVENT_STYLE_ROLLING_COLS must appear in BOTH SQLs."""

    def test_all_cols_present_in_main_sql(self):
        body = _strip_comments(_read(FEAT_TEAM_EVENT_STYLE_SQL))
        for col in gold_tasks.FEAT_TEAM_EVENT_STYLE_ROLLING_COLS:
            pat = r"\b" + re.escape(col) + r"\b"
            assert re.search(pat, body), (
                f"feat_team_event_style.sql missing rolling col {col!r}"
            )

    def test_all_cols_present_in_empty_fallback(self):
        body = _strip_comments(_read(FEAT_TEAM_EVENT_STYLE_EMPTY))
        for col in gold_tasks.FEAT_TEAM_EVENT_STYLE_ROLLING_COLS:
            pat = r"\b" + re.escape(col) + r"\b"
            assert re.search(pat, body), (
                f"feat_team_event_style_empty.sql missing rolling col {col!r}"
            )


# ---------------------------------------------------------------------------
# Registry hygiene
# ---------------------------------------------------------------------------

class TestRegistryHygiene:
    """Registries must be sane — no dupes, lowercase, expected sizes."""

    REGISTRIES = {
        "FEAT_REFEREE_BIAS_ROLLING_COLS": gold_tasks.FEAT_REFEREE_BIAS_ROLLING_COLS,
        "FEAT_TEAM_EVENT_STYLE_ROLLING_COLS": gold_tasks.FEAT_TEAM_EVENT_STYLE_ROLLING_COLS,
    }

    def test_registry_no_duplicates(self):
        for name, registry in self.REGISTRIES.items():
            assert len(registry) == len(set(registry)), (
                f"{name} contains duplicates: "
                f"{[c for c in registry if registry.count(c) > 1]}"
            )

    def test_registry_lowercase(self):
        """Trino case-folds; uppercase reg → CHECK SQL refers to a column
        that actually exists with a different case → false-pass.
        """
        for name, registry in self.REGISTRIES.items():
            for col in registry:
                assert col == col.lower(), (
                    f"{name} entry {col!r} contains uppercase characters"
                )

    def test_registry_no_empty_strings(self):
        for name, registry in self.REGISTRIES.items():
            for col in registry:
                assert col, f"{name} contains empty/None entry"

    def test_referee_bias_registry_has_six_cols(self):
        assert len(gold_tasks.FEAT_REFEREE_BIAS_ROLLING_COLS) == 6

    def test_team_event_style_registry_has_ten_cols(self):
        assert len(gold_tasks.FEAT_TEAM_EVENT_STYLE_ROLLING_COLS) == 10


# ---------------------------------------------------------------------------
# Naming convention
# ---------------------------------------------------------------------------

class TestNamingConventions:
    """Roll-window suffix conventions documented in feat_*.sql headers."""

    def test_referee_bias_uses_l10_or_rate_suffix(self):
        """All referee_bias entries match the ``_l10`` window or ``_rate_l10``."""
        valid_re = re.compile(r"_l10$")
        for col in gold_tasks.FEAT_REFEREE_BIAS_ROLLING_COLS:
            assert valid_re.search(col), (
                f"referee_bias col {col!r} doesn't end with _l10 — "
                "naming convention drift"
            )

    def test_team_event_style_uses_l5_avg_suffix(self):
        """All team_event_style entries end with ``_l5_avg``."""
        for col in gold_tasks.FEAT_TEAM_EVENT_STYLE_ROLLING_COLS:
            assert col.endswith("_l5_avg"), (
                f"team_event_style col {col!r} doesn't end with _l5_avg — "
                "naming convention drift"
            )
