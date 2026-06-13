"""Behaviour-preservation + config tests for source_priority.yaml (#437).

The Gold match facts ``fct_team_match`` / ``fct_player_match`` moved their
cross-source ``COALESCE`` columns out of inline SQL into
``configs/medallion/source_priority.yaml``, rendered into ``.sql.j2`` templates
by ``medallion_config.get_source_priority_exprs`` / ``render_fact_sql``.

These tests lock two things:

1. **Behaviour preservation (DoD: 0-row diff).** The rendered ``.sql.j2`` must
   equal the pre-#437 hand-written SQL once ``--`` comments are stripped and all
   whitespace collapsed — i.e. byte-identical *executable* SQL, so the
   materialised fact is provably unchanged. The pre-#437 SQL is committed as a
   golden fixture under ``tests/unit/fixtures/source_priority/``.
2. **The config is the single source of truth for priority.** The emitter
   renders ``COALESCE`` args in the configured order (with the optional wrap),
   and no multi-source ``COALESCE`` is left inline in the templates.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]

_DAGS_DIR = PROJECT_ROOT / "dags"
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))
os.environ.setdefault(
    "MEDALLION_CONFIG_DIR", str(PROJECT_ROOT / "configs" / "medallion")
)

GOLD_SQL = PROJECT_ROOT / "dags" / "sql" / "gold"
# Golden fixtures live under tests/unit/fixtures/ (parents[1] == tests/unit).
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "source_priority"

# (table_name, .sql.j2 template, pre-#437 golden snapshot)
CASES = [
    ("fct_team_match",
     GOLD_SQL / "fct_team_match.sql.j2",
     FIXTURES / "fct_team_match.pre437.sql"),
    ("fct_player_match",
     GOLD_SQL / "fct_player_match.sql.j2",
     FIXTURES / "fct_player_match.pre437.sql"),
]
_IDS = [c[0] for c in CASES]

pytestmark = pytest.mark.unit


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines()
        if not line.lstrip().startswith("--")
    )


def _normalize(sql: str) -> str:
    """Executable-SQL equivalence key: drop ``--`` comments, collapse all
    whitespace (incl. newlines) to single spaces. Two SQL strings with the same
    key produce identical Trino output."""
    return re.sub(r"\s+", " ", _strip_comments(sql)).strip()


@pytest.mark.parametrize("table,template,golden", CASES, ids=_IDS)
def test_rendered_template_matches_pre437_sql(table, template, golden):
    """DoD — behaviour identical. Rendered .sql.j2 == pre-#437 SQL (normalized)."""
    from utils.medallion_config import render_fact_sql

    rendered = render_fact_sql(template, table)
    assert _normalize(rendered) == _normalize(golden.read_text(encoding="utf-8")), (
        f"{table}: rendered .sql.j2 diverges from the pre-#437 SQL — the refactor "
        f"is NOT behaviour-preserving (would change materialised rows)."
    )


@pytest.mark.parametrize("table,template,golden", CASES, ids=_IDS)
def test_render_leaves_no_placeholder(table, template, golden):
    """Every {{ m_* }} placeholder must resolve (no missing config metric)."""
    from utils.medallion_config import render_fact_sql

    rendered = render_fact_sql(template, table)
    assert "{{" not in rendered and "}}" not in rendered, (
        f"{table}: unresolved placeholder left in rendered SQL"
    )


@pytest.mark.parametrize("table,template,golden", CASES, ids=_IDS)
def test_no_inline_multi_source_coalesce_in_template(table, template, golden):
    """A 2+-arg COALESCE is a cross-source merge and MUST be a placeholder, not
    inline — otherwise its priority lives outside source_priority.yaml. The PK
    bridge fallback COALESCE(xmf.match_id_canonical, fb.match_id) is the one
    documented exception (resolves match_id, not a metric)."""
    raw = _strip_comments(template.read_text(encoding="utf-8"))
    offenders = [
        m.group(0)
        for m in re.finditer(r"COALESCE\s*\([^)]*,[^)]*\)", raw, re.IGNORECASE)
        if "match_id_canonical" not in m.group(0)
    ]
    assert not offenders, (
        f"{table}: inline multi-source COALESCE left in template (move it to "
        f"source_priority.yaml): {offenders}"
    )


def test_emitter_preserves_priority_order_and_wrap():
    """The emitter renders COALESCE args in configured order + applies wrap —
    this is what encodes 'who we trust first per metric'."""
    from utils.medallion_config import get_source_priority_exprs

    tm = get_source_priority_exprs("fct_team_match")
    assert tm["m_xg"] == (
        "ROUND(COALESCE(us.xg, fm.expected_goals, ss.expected_goals), 4) AS xg,"
    )
    assert tm["m_passes"] == "COALESCE(ss.total_passes, ws.pass_total) AS passes,"

    pm = get_source_priority_exprs("fct_player_match")
    assert pm["m_goals"] == (
        "CAST(COALESCE(fb.goals, ss.goals, ws.goals, us.goals) AS BIGINT) AS goals,"
    )
    # A computed per-source fallback expression must survive verbatim.
    assert pm["m_penalty_attempts"] == (
        "CAST(COALESCE(fb.penalty_attempts, ss.penalties_missed + ss.penalty_goals) "
        "AS BIGINT) AS penalty_attempts,"
    )


def test_reordering_sources_changes_priority():
    """Sanity that order matters: the first configured source is the first
    COALESCE arg (so changing priority in the YAML changes the SQL)."""
    from utils.medallion_config import get_source_priority_exprs

    tm = get_source_priority_exprs("fct_team_match")
    inside = re.search(r"COALESCE\(([^)]*)\)", tm["m_xg"]).group(1)
    args = [a.strip() for a in inside.split(",")]
    assert args[0] == "us.xg", "fct_team_match.xg must keep Understat first (RX2)"


def test_unknown_table_raises():
    from utils.medallion_config import (
        MedallionConfigError,
        get_source_priority_exprs,
    )

    with pytest.raises(MedallionConfigError):
        get_source_priority_exprs("fct_does_not_exist")
