"""Regression guard for #803 — TM Silver canonical_id scoped to latest season.

The xref_player resolver produces false matches on the thin historical FBref
spine (one canonical_id → many TM players), which collapsed into 24963 duplicate
(canonical_id, mv_date) rows in transfermarkt_market_value_history once the full
10-season Bronze backfill (#793) landed. The fix scopes the canonical join in
each TM Silver SQL to the latest (current) season only; history keeps
canonical_id=NULL until xref is historized (#788).

These are static-content guards (no Trino) — they fail if someone drops the
season scope and reintroduces the historical fan-out.
"""

import re
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).resolve().parents[3] / 'dags' / 'sql' / 'silver'

# The three Silver tables whose canonical_id is bridged via silver.xref_player.
# (coaches uses manager_aliases, not xref_player — excluded.)
TM_CANONICAL_SQL = [
    'transfermarkt_players.sql',
    'transfermarkt_market_value_history.sql',
    'transfermarkt_transfers.sql.j2',
]


@pytest.mark.unit
@pytest.mark.parametrize('sql_file', TM_CANONICAL_SQL)
def test_canonical_join_scoped_to_latest_season(sql_file):
    """xp CTE must restrict canonical to season = (SELECT max(season) ...)."""
    text = (SQL_DIR / sql_file).read_text(encoding='utf-8')

    # Still filters orphans (pre-existing behaviour must remain).
    assert "confidence <> 'orphan'" in text, (
        f"{sql_file}: lost the confidence<>'orphan' filter"
    )

    # #803: canonical scoped to the latest season via a max(season) subquery.
    # Whitespace-insensitive match for `season = ( SELECT max(season) ...`.
    scope = re.search(
        r"season\s*=\s*\(\s*SELECT\s+max\(\s*season\s*\)",
        text,
        re.IGNORECASE,
    )
    assert scope is not None, (
        f"{sql_file}: canonical join is NOT scoped to the latest season — "
        f"#803 fan-out (24963 dup canonical_id/mv_date) will return"
    )
