"""Guard: the slug→year-start season conversion is gone from Gold (#404).

After #404 every Silver/xref table emits the slug ('2425'), so Gold never needs
to convert a slug back to a bigint year-start. The bronze→slug direction
(``format('%02d%02d', …)`` / ``LPAD(CAST(MOD(season,…)))``) is still legitimate in
the handful of Gold facts that read bronze year-start events directly
(fct_match_timeline whoscored branch, dim_manager; the fct_card/fct_goal/
fct_substitution fbref branches were dropped in #448) — so we only forbid the
slug→year idiom, which has no remaining valid use.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

GOLD_DIR = Path(__file__).resolve().parents[3] / "dags" / "sql" / "gold"

# slug→year-start: `2000 + CAST(SUBSTR(season,1,2) AS BIGINT)` (and TRY_CAST/substr
# variants). Whitespace-tolerant, case-insensitive.
_SLUG_TO_YEAR = re.compile(
    r"2000\s*\+\s*(?:CAST|TRY_CAST)\s*\(\s*(?:SUBSTR|substr)\s*\(\s*[a-z0-9_.]*season",
    re.IGNORECASE,
)


def _strip_sql_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
    text = re.sub(r"--[^\n]*", " ", text)
    return text


@pytest.mark.unit
@pytest.mark.parametrize(
    "sql_path",
    sorted(GOLD_DIR.glob("*.sql")) + sorted(GOLD_DIR.glob("*.sql.j2")),
    ids=lambda p: p.name,
)
def test_no_slug_to_year_conversion_in_gold(sql_path: Path):
    body = _strip_sql_comments(sql_path.read_text(encoding="utf-8"))
    assert not _SLUG_TO_YEAR.search(body), (
        f"{sql_path.name} still converts season slug → year-start "
        "(`2000 + CAST(SUBSTR(season…))`). After #404 season is slug end-to-end; "
        "JOIN/emit it directly."
    )
