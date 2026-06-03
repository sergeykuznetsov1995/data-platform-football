"""
Unit tests for ``dags/sql/silver/xref_referee.sql.j2`` — structural / logical (#143).

Strategy mirrors ``test_xref_team_sql.py``: the pure-SQL ``xref_referee.sql``
became a Jinja template with a single ``{{ referee_aliases_values_sql }}``
placeholder (issue #143 — curated cross-source identity, no fuzzy). We verify:

1. **Structural invariants** of the raw template (2 sources, aliases CTE,
   league predicate, confidence CASE name_alias/orphan, orphan prefixes,
   NFD diacritic fold, pure SELECT, Bronze-only reads).
2. **Render-time correctness**: medallion_config hydrates the template into
   stable Trino SQL (no leftover standalone placeholder, real VALUES embedded,
   both Bronze tables referenced).

No DuckDB / sqlglot — regex over the template is enough.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_PATH = PROJECT_ROOT / "dags" / "sql" / "silver" / "xref_referee.sql.j2"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

os.environ.setdefault(
    "MEDALLION_CONFIG_DIR", str(PROJECT_ROOT / "configs" / "medallion")
)


def _read_template() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


pytestmark = pytest.mark.unit


class TestXrefRefereeTemplateStructure:
    """Regex/keyword sanity over the raw ``xref_referee.sql.j2``."""

    def test_template_uses_aliases_placeholder(self):
        assert "{{ referee_aliases_values_sql }}" in _read_template(), (
            "xref_referee.sql.j2 must declare the {{ referee_aliases_values_sql }} "
            "placeholder consumed by medallion_config.render_sql_template()"
        )

    def test_three_sources_fbref_matchhistory_fotmob(self):
        sql = _read_template().lower()
        assert "'fbref'" in sql and "'matchhistory'" in sql and "'fotmob'" in sql

    def test_fotmob_source_branch(self):
        """FotMob (issue #270): 'fotmob' AS source reading match_facts_json."""
        sql = _read_template().lower()
        assert re.search(r"'fotmob'\s+as\s+source", sql)
        assert "iceberg.bronze.fotmob_match_details" in sql
        assert "$.infobox.referee.text" in sql

    def test_no_other_sources(self):
        sql = _read_template().lower()
        for forbidden in ["'understat'", "'whoscored'", "'sofascore'",
                          "'clubelo'", "'espn'"]:
            pattern = re.compile(re.escape(forbidden) + r"\s+as\s+source", re.I)
            assert not pattern.search(sql), (
                f"{forbidden} must not be a source in xref_referee — only "
                "fbref + matchhistory + fotmob carry referee data (issue #270)"
            )

    def test_has_aliases_cte(self):
        assert re.search(r"\baliases\s+as\s*\(", _read_template(), re.I)

    def test_league_predicate_in_join(self):
        """Issue #148 league guard must be wired into the alias JOIN."""
        assert re.search(r"a\.league\s*=\s*rt\.league", _read_template(), re.I)

    def test_confidence_is_name_alias_or_orphan(self):
        sql = _read_template().lower()
        assert "'name_alias'" in sql and "'orphan'" in sql
        assert "'name_normalize'" not in sql, (
            "old pure-SQL confidence label must be gone after the #143 refactor"
        )

    def test_orphan_prefixes(self):
        sql = _read_template().lower()
        assert "'fb_ref_'" in sql and "'mh_ref_'" in sql
        assert "'fm_ref_'" in sql  # FotMob orphan fallback (issue #270)

    def test_diacritic_fold_idiom(self):
        r"""NFD + \p{Mn} strip — same fold as xref_team/xref_manager (issue #215)."""
        sql = _read_template()
        assert "NFD" in sql
        assert r"\p{Mn}" in sql

    def test_match_score_null(self):
        assert re.search(r"CAST\(NULL AS double\)", _read_template(), re.I)

    def test_pure_select_no_create_table(self):
        """No DDL in the executable SQL (header comment may mention CTAS)."""
        code = "\n".join(
            line for line in _read_template().splitlines()
            if not line.lstrip().startswith("--")
        ).lower()
        assert "create table" not in code and "create or replace" not in code

    def test_reads_bronze_only(self):
        sql = _read_template().lower()
        assert "iceberg.bronze.fbref_schedule" in sql
        assert "iceberg.bronze.matchhistory_games" in sql
        assert "iceberg.gold." not in sql


class TestXrefRefereeRender:
    """Render the template via the real shipped referee_aliases.yaml."""

    def _render(self) -> str:
        from utils import medallion_config as mc
        mc.reset_cache()
        return mc.render_sql_template(
            SQL_PATH,
            referee_aliases_values_sql=mc.get_referee_alias_sql_values(
                with_canonical_id=True, with_league=True
            ),
        )

    def test_no_leftover_standalone_placeholder(self):
        rendered = self._render()
        assert not re.search(r"^[ \t]*\{\{\s*\w+\s*\}\}[ \t]*$", rendered, re.M)

    def test_embeds_real_alias_values(self):
        rendered = self._render()
        assert "ref_michael_oliver" in rendered
        assert "'M Oliver'" in rendered  # MatchHistory initial form merges to canonical

    def test_references_both_bronze_tables(self):
        rendered = self._render()
        assert "iceberg.bronze.fbref_schedule" in rendered
        assert "iceberg.bronze.matchhistory_games" in rendered
