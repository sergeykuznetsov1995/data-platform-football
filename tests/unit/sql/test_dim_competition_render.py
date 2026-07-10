"""
Unit tests for ``utils.dim_loaders.render_dim_competition_sql`` (issue #425).

Verifies the renderer that hydrates the ``dim_competition.sql.j2`` template
with VALUES tuples sourced from ``configs/medallion/competitions.yaml``.

Strategy:
  * Build a temp competitions.yaml with 3 leagues (one in-scope, two stubs)
    mimicking the real shape.
  * Point ``MEDALLION_CONFIG_DIR`` at it + reload ``utils.medallion_config``
    (CONFIG_DIR is resolved at import time) + reset the YAML lru_cache.
  * Invoke ``render_dim_competition_sql`` with the real .sql.j2 template
    so any change to the template's ``AS t(...)`` clause is also exercised.
  * Assert: one tuple per competition (stubs included — a dictionary lists
    what exists), PK = league slug verbatim, 4 fields per tuple, apostrophe
    escaping.

This is a Python-level test — the rendered SQL is NOT actually executed
against Trino. The corresponding end-to-end check lives in
``tests/integration/test_e2_dims_smoke.py``.
"""

from __future__ import annotations

import re
import sys
import textwrap
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_competition.sql.j2"

# ``utils.dim_loaders`` lives under ``dags/utils/`` — the dags/ folder is on
# sys.path inside the Airflow container but not on the host. Add it here so
# the test runs without depending on tests/unit/dags/conftest.py.
_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))

EXPECTED_COLUMNS = ["league", "competition_name", "country", "tier", "competition_format", "is_international"]

_COMPETITIONS_YAML_CONTENT = textwrap.dedent("""\
    competitions:
      - id: "ENG-Premier League"
        name: "English Premier League"
        country: "England"
        tier: 1
        seasons:
          - id: 2425
            format: "league_round_robin"
            team_count: 20
            start: "2024-08-16"
            end: "2025-05-25"
        sources:
          primary: ["fbref"]
          fallback: []
        in_scope: true

      - id: "ESP-La Liga"
        name: "Spanish La Liga"
        country: "Spain"
        tier: 1
        seasons: []
        sources:
          primary: []
          fallback: []
        in_scope: false
        notes: "stub"

      - id: "FRA-Ligue 1"
        name: "Ligue 1 d'Uber Eats"
        country: "France"
        tier: 1
        seasons: []
        sources:
          primary: []
          fallback: []
        in_scope: false
        notes: "stub with apostrophe in name (escaping check)"
    """)


@pytest.fixture
def rendered_sql(monkeypatch, tmp_path) -> str:
    """Run ``render_dim_competition_sql`` against a fixture competitions.yaml."""
    pytest.importorskip("yaml")
    (tmp_path / "competitions.yaml").write_text(_COMPETITIONS_YAML_CONTENT)

    # Patch the module attribute (NOT env + reload): monkeypatch restores it
    # on teardown, so later tests in the same session keep the real config.
    from utils import medallion_config
    monkeypatch.setattr(medallion_config, "CONFIG_DIR", tmp_path)
    medallion_config.reset_cache()

    from utils import dim_loaders

    out_path = tmp_path / "dim_competition_rendered.sql"
    returned = dim_loaders.render_dim_competition_sql(
        str(TEMPLATE_PATH), str(out_path)
    )
    assert returned == str(out_path), "renderer must echo the out_path"
    yield out_path.read_text()

    medallion_config.reset_cache()


@pytest.mark.unit
class TestDimCompetitionRender:
    def test_rendered_sql_is_non_empty(self, rendered_sql):
        assert rendered_sql.strip(), "rendered SQL must be non-empty"

    def test_template_placeholder_replaced(self, rendered_sql):
        """The standalone ``{{ rows }}`` placeholder must be substituted
        (the comment header still mentions it — that's intentional)."""
        assert not re.search(r'^\s*\{\{ rows \}\}\s*$', rendered_sql,
                             flags=re.MULTILINE), (
            "active {{ rows }} placeholder must be substituted"
        )

    def test_one_tuple_per_competition_stubs_included(self, rendered_sql):
        """One VALUES tuple per YAML entry — 3 for the fixture. Stubs are
        included: dim_competition is a dictionary, not an ingest log."""
        tuple_starts = re.findall(
            r"\(\s*'(?:ENG-Premier League|ESP-La Liga|FRA-Ligue 1)'",
            rendered_sql,
        )
        assert len(tuple_starts) == 3, (
            f"expected 3 VALUES tuples, found {len(tuple_starts)}"
        )

    def test_league_pk_is_slug_verbatim(self, rendered_sql):
        """PK ``league`` carries the competition slug verbatim — the same
        value as the ``league`` column on every fact (no mapping needed)."""
        assert "('ENG-Premier League', 'English Premier League', 'England', 1, 'league_round_robin', false)" \
            in rendered_sql

    def test_six_fields_per_tuple(self, rendered_sql):
        rows = re.findall(
            r"\(\s*'(?:ENG|ESP|FRA)[^)]*\)",
            rendered_sql,
        )
        assert len(rows) == 3
        for row in rows:
            # The FRA row contains an escaped apostrophe ('') — strip doubled
            # quotes before the naive comma split.
            fields = row.replace("''", "").split(",")
            assert len(fields) == 6, (
                f"expected 6 fields per row, got {len(fields)}: {row!r}"
            )

    def test_apostrophe_escaped(self, rendered_sql):
        """Names with apostrophes must be SQL-escaped (' -> '')."""
        assert "Ligue 1 d''Uber Eats" in rendered_sql

    def test_all_columns_declared(self, rendered_sql):
        for col in EXPECTED_COLUMNS:
            assert col in rendered_sql, (
                f"expected column declaration '{col}' missing from rendered SQL"
            )
