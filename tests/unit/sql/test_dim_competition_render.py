"""
Unit tests for ``utils.dim_loaders.render_dim_competition_sql`` (E2 — 2026-05).

Verifies the renderer that hydrates the ``dim_competition.sql.j2`` template
with VALUES tuples sourced from ``scrapers/sources/leagues.yaml:metadata``.

Strategy:
  * Build a temp YAML with 5 leagues mimicking the real shape.
  * monkeypatch ``LEAGUES_YAML`` (module-level Path constant) to point at it.
  * Invoke ``render_dim_competition_sql`` with the real .sql.j2 template
    so any change to the template's ``AS t(...)`` clause is also exercised.
  * Assert the rendered SQL has 5 VALUES tuples, deterministic slugs,
    constant ``competition_source='config'`` / ``competition_version='v1'``,
    and that all 13 declared columns receive a value per row.

This is a Python-level test — the rendered SQL is NOT actually executed
against Trino/DuckDB. The corresponding end-to-end check lives in
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

# 13 columns declared in the template's `AS t(...)` clause — kept in lockstep
# with dim_competition.sql.j2:25-37. If this list changes, the template
# changed too and this test should be updated alongside.
EXPECTED_COLUMNS = [
    "competition_id",
    "competition_name",
    "country",
    "competition_level",
    "n_teams",
    "matches_per_season",
    "fbref_id",
    "whoscored_id",
    "sofascore_id",
    "espn_id",
    "competition_canonical",
    "competition_source",
    "competition_version",
]


# ---------------------------------------------------------------------------
# Fixture: temp leagues.yaml
# ---------------------------------------------------------------------------

# Five fixture leagues. Each carries every key the renderer references:
#   country, level, teams, matches_per_season,
#   fbref_id, whoscored_id, sofascore_id, espn_id
_LEAGUES_YAML_CONTENT = textwrap.dedent("""\
    metadata:
      ENG-Premier League:
        country: England
        level: 1
        teams: 20
        matches_per_season: 380
        fbref_id: 9
        whoscored_id: 2
        sofascore_id: 17
        espn_id: eng.1
      ESP-La Liga:
        country: Spain
        level: 1
        teams: 20
        matches_per_season: 380
        fbref_id: 12
        whoscored_id: 4
        sofascore_id: 8
        espn_id: esp.1
      GER-Bundesliga:
        country: Germany
        level: 1
        teams: 18
        matches_per_season: 306
        fbref_id: 20
        whoscored_id: 3
        sofascore_id: 35
        espn_id: ger.1
      ITA-Serie A:
        country: Italy
        level: 1
        teams: 20
        matches_per_season: 380
        fbref_id: 11
        whoscored_id: 5
        sofascore_id: 23
        espn_id: ita.1
      FRA-Ligue 1:
        country: France
        level: 1
        teams: 18
        matches_per_season: 306
        fbref_id: 13
        whoscored_id: 22
        sofascore_id: 34
        espn_id: fra.1
""")


@pytest.fixture
def fixture_leagues_yaml(tmp_path) -> Path:
    p = tmp_path / "leagues.yaml"
    p.write_text(_LEAGUES_YAML_CONTENT)
    return p


@pytest.fixture
def rendered_sql(monkeypatch, fixture_leagues_yaml, tmp_path) -> str:
    """Run ``render_dim_competition_sql`` against the fixture YAML."""
    pytest.importorskip("yaml")  # PyYAML
    from utils import dim_loaders

    monkeypatch.setattr(dim_loaders, "LEAGUES_YAML", fixture_leagues_yaml)

    out_path = tmp_path / "dim_competition_rendered.sql"
    returned = dim_loaders.render_dim_competition_sql(
        str(TEMPLATE_PATH), str(out_path)
    )
    assert returned == str(out_path), "renderer must echo the out_path"
    return out_path.read_text()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestDimCompetitionRender:
    def test_rendered_sql_is_non_empty(self, rendered_sql):
        """Sanity: render writes a non-empty file."""
        assert rendered_sql.strip(), "rendered SQL must be non-empty"

    def test_template_placeholder_replaced(self, rendered_sql):
        """The literal ``{{ rows }}`` placeholder must be substituted in the
        active VALUES block (the comment header still mentions it — that's
        intentional and tested in test_comment_placeholder_preserved)."""
        # No standalone-on-its-own-line placeholder must remain
        assert not re.search(r'^\s*\{\{ rows \}\}\s*$', rendered_sql,
                             flags=re.MULTILINE), (
            "active {{ rows }} placeholder must be substituted"
        )

    def test_five_values_tuples_rendered(self, rendered_sql):
        """One VALUES tuple per league entry → 5 tuples for the fixture."""
        # Each rendered row begins with `('<slug>',` — count those.
        # Slugs are 'eng_*', 'esp_*', 'ger_*', 'ita_*', 'fra_*'
        tuple_starts = re.findall(
            r"\(\s*'(?:eng_|esp_|ger_|ita_|fra_)[a-z0-9_]+'",
            rendered_sql,
        )
        assert len(tuple_starts) == 5, (
            f"expected 5 VALUES tuples, found {len(tuple_starts)}: "
            f"{tuple_starts}"
        )

    def test_competition_id_slugs_deterministic(self, rendered_sql):
        """Slugs match ``_slug()`` semantics (lowercase + non-alnum→'_')."""
        expected_slugs = {
            "eng_premier_league",
            "esp_la_liga",
            "ger_bundesliga",
            "ita_serie_a",
            "fra_ligue_1",
        }
        for slug in expected_slugs:
            assert f"'{slug}'" in rendered_sql, (
                f"expected slug {slug!r} not found in rendered SQL"
            )

    def test_constant_source_and_version_literals(self, rendered_sql):
        """``competition_source='config'`` and ``competition_version='v1'``
        appear in every VALUES tuple (5 leagues -> 5 occurrences each).

        Counts only the active SQL body (comments stripped) — the .sql.j2
        header references the literals in narrative form too.
        """
        # Strip BOTH full-line and trailing ``--`` comments so we count only
        # active SQL literals (header narrative + column-declaration trailing
        # comments both reference 'config' / 'v1').
        body_lines = []
        for line in rendered_sql.splitlines():
            if line.lstrip().startswith("--"):
                continue
            # Trailing inline comment: ``column,       -- 'config'``
            if "--" in line:
                line = line.split("--", 1)[0]
            body_lines.append(line)
        body = "\n".join(body_lines)
        config_count = body.count("'config'")
        v1_count = body.count("'v1'")
        assert config_count == 5, (
            f"expected 5 'config' literals (one per row), got {config_count}"
        )
        assert v1_count == 5, (
            f"expected 5 'v1' literals (one per row), got {v1_count}"
        )

    def test_all_13_columns_populated(self, rendered_sql):
        """All 13 columns declared in ``AS t(...)`` are present in the
        template, and each rendered VALUES tuple has 13 fields."""
        # 1) The column list in the template's AS t(...) is unchanged
        for col in EXPECTED_COLUMNS:
            assert col in rendered_sql, (
                f"expected column declaration '{col}' missing from rendered SQL"
            )

        # 2) Every VALUES tuple has 13 comma-separated fields. Easiest
        # heuristic: extract substrings between matching parens that begin
        # with a slug literal, then count top-level commas.
        slug_tuple_re = re.compile(
            r"\(\s*('(?:eng_|esp_|ger_|ita_|fra_)[a-z0-9_]+'.*?)\)",
            re.DOTALL,
        )
        rows = slug_tuple_re.findall(rendered_sql)
        assert len(rows) == 5, f"expected 5 tuples, got {len(rows)}"

        for row in rows:
            # Top-level comma split — the fixture has no nested parens or
            # commas inside string literals so simple split is safe.
            fields = [f.strip() for f in row.split(",")]
            assert len(fields) == 13, (
                f"expected 13 fields per row, got {len(fields)}: {row!r}"
            )

    def test_canonical_equals_competition_name(self, rendered_sql):
        """Per ``_slug`` contract: competition_canonical == competition_name."""
        # Each row contains, e.g., "'ENG-Premier League', 'England', ... 'ENG-Premier League', 'config', 'v1'"
        # i.e. the league name appears at least twice (name + canonical).
        for league_name in [
            "ENG-Premier League",
            "ESP-La Liga",
            "GER-Bundesliga",
            "ITA-Serie A",
            "FRA-Ligue 1",
        ]:
            # Two occurrences per row (name + canonical) inside that row's tuple
            assert rendered_sql.count(f"'{league_name}'") >= 2, (
                f"league {league_name!r} should appear ≥2× per row "
                f"(name + canonical); got {rendered_sql.count(f'{league_name!r}')}"
            )
