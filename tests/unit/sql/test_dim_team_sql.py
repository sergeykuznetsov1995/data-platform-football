"""
Unit tests for ``dags/sql/gold/dim_team.sql.j2`` — star-schema grain (#425).

dim_team is one row per CLUB: spine = silver.xref_team (source='fbref',
GROUP BY canonical_id collapses the per-(league, season) rows), attributes
(team_name / country / short_name) from team_aliases.yaml via the
``{{ team_meta_values_sql }}`` placeholder.

Same pattern as ``test_xref_team_sql.py``: regex/keyword sanity over the
raw template text (plus a render pass via dim_loaders against a fixture
team_aliases.yaml).
"""

from __future__ import annotations

import re
import sys
import textwrap
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_team.sql.j2"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


def _read_sql() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _strip_comments(sql: str) -> str:
    """Remove ``-- ...`` lines for assertions about executable SQL only."""
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


pytestmark = pytest.mark.unit


class TestDimTeamStarStructure:
    """Regex sanity over the dim_team template post-#425 redesign."""

    def test_reads_from_silver_xref_team(self):
        sql = _strip_comments(_read_sql())
        assert "iceberg.silver.xref_team" in sql

    def test_no_legacy_entity_xref_in_executable_sql(self):
        sql = _strip_comments(_read_sql())
        assert "entity_xref" not in sql

    def test_fbref_source_filter(self):
        sql = _strip_comments(_read_sql())
        assert re.search(r"source\s*=\s*'fbref'", sql, re.IGNORECASE)

    def test_grain_is_club_group_by_canonical_only(self):
        """GROUP BY collapses per-(league, season) xref rows to one per club —
        league/season must NOT be part of the grouping key anymore."""
        sql = _strip_comments(_read_sql())
        m = re.search(r"GROUP\s+BY\s+([^\n;]+)", sql, re.IGNORECASE)
        assert m, "dim_team template is missing a GROUP BY clause"
        group_by = m.group(1).lower()
        assert "canonical_id" in group_by
        assert "season" not in group_by
        assert "league" not in group_by

    def test_design_columns_present(self):
        sql = _strip_comments(_read_sql())
        for col in ("team_id", "team_name", "country", "short_name"):
            assert re.search(rf"\b{col}\b", sql, re.IGNORECASE), (
                f"dim_team template must emit {col!r}"
            )

    def test_meta_placeholder_present(self):
        """Attributes come from team_aliases.yaml via the VALUES placeholder."""
        assert "{{ team_meta_values_sql }}" in _read_sql()

    def test_pure_select_no_create_table(self):
        sql = _strip_comments(_read_sql())
        assert "CREATE TABLE" not in sql.upper()
        assert "INSERT INTO" not in sql.upper()


class TestDimTeamRender:
    """render_dim_team_sql fills the placeholder from team_aliases.yaml."""

    _TEAM_ALIASES_YAML = textwrap.dedent("""\
        teams:
          - canonical_name: "X United"
            canonical_id: "x_united"
            country: "England"
            short_name: "X Utd"
            aliases:
              _generic: ["X United"]
            competition_scope: ["ENG-Premier League"]
        """)

    def test_renders_team_meta_tuples(self, monkeypatch, tmp_path):
        pytest.importorskip("yaml")
        (tmp_path / "team_aliases.yaml").write_text(self._TEAM_ALIASES_YAML)

        # Patch the module attribute (NOT env + reload): monkeypatch restores
        # it on teardown, so later tests keep the real config.
        from utils import medallion_config
        monkeypatch.setattr(medallion_config, "CONFIG_DIR", tmp_path)
        medallion_config.reset_cache()

        from utils import dim_loaders
        out_path = tmp_path / "dim_team_rendered.sql"
        dim_loaders.render_dim_team_sql(str(SQL_PATH), str(out_path))
        rendered = out_path.read_text()
        medallion_config.reset_cache()

        assert "('x_united', 'X United', 'England', 'X Utd')" in rendered
        # The standalone placeholder must be gone from the VALUES block.
        assert not re.search(
            r"^\s*\{\{\s*team_meta_values_sql\s*\}\}\s*$",
            rendered, flags=re.MULTILINE,
        )
