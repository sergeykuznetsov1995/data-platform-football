"""
Unit tests for ``utils.dim_loaders.render_dim_season_sql`` (E2 — 2026-05).

Verifies the renderer that produces the dim_season VALUES tuples from a
fixed window of 5 seasons ending at ``utils.config.CURRENT_SEASON``.

Strategy:
  * monkeypatch ``dim_loaders.date.today`` (via the ``date`` symbol bound
    in the module) to a deterministic 2024-09-15 — well inside the 2024-25
    season window (Aug 1 2024 → Jul 31 2025).
  * monkeypatch ``utils.config.CURRENT_SEASON`` to 2024 so the helper
    ``_seasons_window`` deterministically yields [2020..2024] (5 entries).
  * Invoke ``render_dim_season_sql`` with the real .sql.j2 template.
  * Assert: 5 rows, 2024-25 has is_current=true, valid_from/valid_to
    match Aug 1 / Jul 31, season_canonical/source/version literals match.
"""

from __future__ import annotations

import datetime as _dt
import re
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE_PATH = PROJECT_ROOT / "dags" / "sql" / "gold" / "dim_season.sql.j2"

# ``utils.dim_loaders`` lives under ``dags/utils/`` — add to sys.path so
# the test runs without depending on tests/unit/dags/conftest.py.
_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))


# ---------------------------------------------------------------------------
# Fixed-clock helper
# ---------------------------------------------------------------------------

class _FixedDate(_dt.date):
    """``date`` subclass whose ``.today()`` returns a fixed value.

    Subclassing instead of monkeypatching avoids C-level "cannot set
    attribute" issues on the built-in ``date`` type.
    """

    @classmethod
    def today(cls):
        return _dt.date(2024, 9, 15)


@pytest.fixture
def patched_renderer(monkeypatch):
    """Patch CURRENT_SEASON + date.today() inside ``dim_loaders``.

    Returns the imported (patched) module so tests can call its renderer
    directly.
    """
    pytest.importorskip("yaml")
    from utils import dim_loaders

    # _seasons_window reads CURRENT_SEASON via a deferred import —
    # ``from utils.config import CURRENT_SEASON`` happens INSIDE
    # ``render_dim_season_sql``. Patch the value at the source module so
    # the next import sees 2024.
    import utils.config as _cfg
    monkeypatch.setattr(_cfg, "CURRENT_SEASON", 2024, raising=False)

    # Patch ``date`` symbol in dim_loaders so date.today() returns 2024-09-15.
    monkeypatch.setattr(dim_loaders, "date", _FixedDate)

    return dim_loaders


@pytest.fixture
def rendered_sql(patched_renderer, tmp_path) -> str:
    out_path = tmp_path / "dim_season_rendered.sql"
    returned = patched_renderer.render_dim_season_sql(
        str(TEMPLATE_PATH), str(out_path)
    )
    assert returned == str(out_path), "renderer must echo the out_path"
    return out_path.read_text()


# ---------------------------------------------------------------------------
# Helpers — extract VALUES tuples from rendered SQL
# ---------------------------------------------------------------------------

def _extract_value_tuples(sql_text: str):
    """Return the list of dicts, one per VALUES tuple, keyed by column name.

    The template's column order (from ``AS t(...)``) is:
        season_id, season_start_year, season_end_year, season_label,
        valid_from, valid_to, is_current, season_canonical,
        season_source, season_version
    """
    cols = [
        "season_id",
        "season_start_year",
        "season_end_year",
        "season_label",
        "valid_from",
        "valid_to",
        "is_current",
        "season_canonical",
        "season_source",
        "season_version",
    ]

    # Each tuple begins with `('YYYY-YY',` — match those.
    tuple_re = re.compile(
        r"\(\s*('\d{4}-\d{2}',[^)]+)\)",
        re.DOTALL,
    )
    rows = tuple_re.findall(sql_text)
    parsed = []
    for body in rows:
        # Top-level comma split is safe — none of the values contain commas
        # inside quotes for this template.
        fields = [f.strip() for f in body.split(",")]
        assert len(fields) == len(cols), (
            f"expected {len(cols)} fields per VALUES tuple, got {len(fields)}: {body!r}"
        )
        parsed.append(dict(zip(cols, fields)))
    return parsed


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestDimSeasonRender:
    def test_five_rows_rendered(self, rendered_sql):
        """``_seasons_window(2024, n=5)`` → [2020..2024] = 5 rows."""
        rows = _extract_value_tuples(rendered_sql)
        assert len(rows) == 5, f"expected 5 rows, got {len(rows)}"

    def test_season_ids_match_window(self, rendered_sql):
        """Expected season_ids: 2020-21, 2021-22, 2022-23, 2023-24, 2024-25."""
        rows = _extract_value_tuples(rendered_sql)
        season_ids = [r["season_id"].strip("'") for r in rows]
        assert sorted(season_ids) == [
            "2020-21",
            "2021-22",
            "2022-23",
            "2023-24",
            "2024-25",
        ], f"unexpected season_ids: {season_ids}"

    def test_only_current_season_is_flagged(self, rendered_sql):
        """Only the 2024-25 row carries is_current=true; the other 4 are false.

        With today() pinned to 2024-09-15, the only window containing today
        is 2024-08-01 .. 2025-07-31, i.e. the '2024-25' season.
        """
        rows = _extract_value_tuples(rendered_sql)
        current = [r for r in rows if r["is_current"].lower() == "true"]
        non_current = [r for r in rows if r["is_current"].lower() == "false"]

        assert len(current) == 1, (
            f"expected exactly 1 row with is_current=true, got {len(current)}: "
            f"{[r['season_id'] for r in current]}"
        )
        assert current[0]["season_id"] == "'2024-25'", (
            f"is_current=true must be the 2024-25 row, got "
            f"{current[0]['season_id']!r}"
        )
        assert len(non_current) == 4

    def test_valid_from_and_valid_to_anchors(self, rendered_sql):
        """Current season anchors: valid_from='2024-08-01', valid_to='2025-07-31'."""
        rows = _extract_value_tuples(rendered_sql)
        current = next(r for r in rows if r["season_id"] == "'2024-25'")
        # The renderer emits ``DATE 'YYYY-MM-DD'`` literals
        assert current["valid_from"] == "DATE '2024-08-01'", (
            f"valid_from must be DATE '2024-08-01', got {current['valid_from']!r}"
        )
        assert current["valid_to"] == "DATE '2025-07-31'", (
            f"valid_to must be DATE '2025-07-31', got {current['valid_to']!r}"
        )

    def test_canonical_equals_season_id(self, rendered_sql):
        """``season_canonical = season_id`` for every row."""
        rows = _extract_value_tuples(rendered_sql)
        for r in rows:
            assert r["season_canonical"] == r["season_id"], (
                f"season_canonical {r['season_canonical']!r} != "
                f"season_id {r['season_id']!r}"
            )

    def test_constant_source_and_version_literals(self, rendered_sql):
        """``season_source='config'`` + ``season_version='v1'`` per row."""
        rows = _extract_value_tuples(rendered_sql)
        for r in rows:
            assert r["season_source"] == "'config'", (
                f"season_source must be 'config', got {r['season_source']!r}"
            )
            assert r["season_version"] == "'v1'", (
                f"season_version must be 'v1', got {r['season_version']!r}"
            )
