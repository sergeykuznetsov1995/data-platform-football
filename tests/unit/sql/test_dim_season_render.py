"""
Unit tests for ``utils.dim_loaders.render_dim_season_sql`` (issue #425).

Verifies the renderer that produces dim_season VALUES tuples from the union
of ``seasons`` across in-scope competitions in
``configs/medallion/competitions.yaml``.

Strategy:
  * Build a temp competitions.yaml with two in-scope leagues sharing season
    slug '2425' (different windows — dedup must take min(start)/max(end)),
    plus an out-of-scope stub whose seasons must be EXCLUDED.
  * Point ``MEDALLION_CONFIG_DIR`` at it + reload ``utils.medallion_config``.
  * Freeze ``date.today()`` inside dim_loaders to 2025-06-15 — the summer
    gap AFTER the '2425' season ended and BEFORE '2526' starts. is_current
    must still flag exactly one row ('2425' — the latest started season);
    a naive BETWEEN would flag zero.
  * Assert dedup, exclusion, leading-zero slug formatting and date literals.
"""

from __future__ import annotations

import datetime as _dt
import importlib
import re
import sys
import textwrap
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_season.sql.j2"

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


# ---------------------------------------------------------------------------
# Fixed-clock helper
# ---------------------------------------------------------------------------

class _FixedDate(_dt.date):
    """``date`` subclass whose ``.today()`` returns a fixed value (summer gap).

    Subclassing instead of monkeypatching avoids C-level "cannot set
    attribute" issues on the built-in ``date`` type. ``fromisoformat`` is
    inherited, so the renderer's date parsing keeps working.
    """

    @classmethod
    def today(cls):
        return _dt.date(2025, 6, 15)


_COMPETITIONS_YAML_CONTENT = textwrap.dedent("""\
    competitions:
      - id: "ENG-Premier League"
        name: "English Premier League"
        country: "England"
        tier: 1
        seasons:
          - id: 2324
            format: "league_round_robin"
            team_count: 20
            start: "2023-08-11"
            end: "2024-05-19"
          - id: 2425
            format: "league_round_robin"
            team_count: 20
            start: "2024-08-16"
            end: "2025-05-25"
          - id: 2526
            format: "league_round_robin"
            team_count: 20
            start: "2025-08-15"
            end: "2026-05-24"
        sources:
          primary: ["fbref"]
          fallback: []
        in_scope: true

      - id: "ESP-La Liga"
        name: "Spanish La Liga"
        country: "Spain"
        tier: 1
        seasons:
          # Same slug as ENG '2425' but a WIDER window — dedup must merge
          # into one row with min(start)=2024-08-10, max(end)=2025-06-01.
          - id: 2425
            format: "league_round_robin"
            team_count: 20
            start: "2024-08-10"
            end: "2025-06-01"
        sources:
          primary: ["fbref"]
          fallback: []
        in_scope: true

      - id: "GER-Bundesliga"
        name: "German Bundesliga"
        country: "Germany"
        tier: 1
        seasons:
          # Out-of-scope league — its season must NOT appear in dim_season.
          - id: 1920
            format: "league_round_robin"
            team_count: 18
            start: "2019-08-16"
            end: "2020-06-27"
        sources:
          primary: []
          fallback: []
        in_scope: false
        notes: "stub"
    """)


@pytest.fixture
def rendered_sql(monkeypatch, tmp_path) -> str:
    pytest.importorskip("yaml")
    (tmp_path / "competitions.yaml").write_text(_COMPETITIONS_YAML_CONTENT)
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    from utils import medallion_config
    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    from utils import dim_loaders
    monkeypatch.setattr(dim_loaders, "date", _FixedDate)

    out_path = tmp_path / "dim_season_rendered.sql"
    returned = dim_loaders.render_dim_season_sql(str(TEMPLATE_PATH), str(out_path))
    assert returned == str(out_path), "renderer must echo the out_path"
    yield out_path.read_text()

    medallion_config.reset_cache()


def _tuples(rendered_sql: str) -> dict:
    """Parse rendered VALUES rows into {slug: (name, start, end, is_current)}."""
    rows = re.findall(
        r"\('(\d{4})', '([\d-]+)', DATE '([\d-]+)', DATE '([\d-]+)', (true|false)\)",
        rendered_sql,
    )
    return {slug: (name, start, end, cur) for slug, name, start, end, cur in rows}


@pytest.mark.unit
class TestDimSeasonRender:
    def test_template_placeholder_replaced(self, rendered_sql):
        assert not re.search(r'^\s*\{\{ rows \}\}\s*$', rendered_sql,
                             flags=re.MULTILINE)

    def test_one_row_per_distinct_slug_in_scope_only(self, rendered_sql):
        """3 distinct slugs from in-scope leagues; the stub's '1920' excluded."""
        t = _tuples(rendered_sql)
        assert set(t) == {"2324", "2425", "2526"}

    def test_shared_slug_deduped_min_start_max_end(self, rendered_sql):
        """ENG + ESP both carry '2425' → one row, widest covering window."""
        _, start, end, _ = _tuples(rendered_sql)["2425"]
        assert start == "2024-08-10", "dedup must take the EARLIEST start"
        assert end == "2025-06-01", "dedup must take the LATEST end"

    def test_season_name_derived_from_slug(self, rendered_sql):
        t = _tuples(rendered_sql)
        assert t["2324"][0] == "2023-24"
        assert t["2425"][0] == "2024-25"
        assert t["2526"][0] == "2025-26"

    def test_is_current_exactly_one_row_summer_gap(self, rendered_sql):
        """Frozen today=2025-06-15 sits BETWEEN seasons. The latest started
        slug ('2425') must be current; '2526' has not started yet."""
        t = _tuples(rendered_sql)
        assert t["2425"][3] == "true"
        assert t["2324"][3] == "false"
        assert t["2526"][3] == "false"
        flags = [v[3] for v in t.values()]
        assert flags.count("true") == 1, "exactly one is_current row"

    def test_columns_declared(self, rendered_sql):
        for col in ["season", "season_name", "start_date", "end_date",
                    "is_current"]:
            assert col in rendered_sql


@pytest.mark.unit
def test_leading_zero_slug_formatting(monkeypatch, tmp_path):
    """YAML season id is an int — id 203 must render as slug '0203', not '203'."""
    pytest.importorskip("yaml")
    yaml_content = textwrap.dedent("""\
        competitions:
          - id: "XXX-Test League"
            name: "Test League"
            country: "Testland"
            tier: 1
            seasons:
              - id: 203
                format: "league_round_robin"
                team_count: 20
                start: "2002-08-10"
                end: "2003-05-20"
            sources:
              primary: []
              fallback: []
            in_scope: true
        """)
    (tmp_path / "competitions.yaml").write_text(yaml_content)
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    from utils import medallion_config
    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    from utils import dim_loaders
    out_path = tmp_path / "out.sql"
    dim_loaders.render_dim_season_sql(str(TEMPLATE_PATH), str(out_path))
    rendered = out_path.read_text()
    medallion_config.reset_cache()

    assert "('0203', '2002-03'" in rendered
