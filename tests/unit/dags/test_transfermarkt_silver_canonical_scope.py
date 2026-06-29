"""Guard for TM Silver canonical_id scoping (#803 → #788).

Background: the xref_player resolver used to false-match on the thin historical
FBref spine (one canonical_id → many TM players). #803 worked around it by
scoping the canonical join in every TM Silver SQL to the latest season.

#788 historizes canonical for the per-season tables (``players``, ``transfers``):
the resolver now demotes distinct TM player_ids that false-match onto one
canonical to tm_<id> orphans (see ``_dedup_canonical_per_season``), and each
JOIN carries a season predicate, so per-season canonical matches without
fan-out.

``market_value_history`` is the exception and STAYS scoped to the latest season:
Bronze writes a player's FULL career MV graph into EVERY season snapshot, so one
(player_id, mv_date) repeats across all season partitions (~×3.18). Removing the
scope re-introduces duplicate (canonical_id, mv_date) rows — not a resolver
fan-out, but a Bronze snapshot-semantics artifact. Proper historization needs a
(player_id, mv_date) dedup → tracked as a followup.

These are static-content guards (no Trino).
"""

import re
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).resolve().parents[3] / 'dags' / 'sql' / 'silver'

# #788: per-season tables — canonical historized across all seasons (no scope).
TM_HISTORIZED_SQL = [
    'transfermarkt_players.sql',
    'transfermarkt_transfers.sql.j2',
]
# market_value_history — stays latest-season-scoped (Bronze snapshot dup).
TM_SCOPED_SQL = [
    'transfermarkt_market_value_history.sql',
]
TM_CANONICAL_SQL = TM_HISTORIZED_SQL + TM_SCOPED_SQL

_MAX_SEASON_RE = re.compile(
    r"season\s*=\s*\(\s*SELECT\s+max\(\s*season\s*\)", re.IGNORECASE
)


@pytest.mark.unit
@pytest.mark.parametrize('sql_file', TM_CANONICAL_SQL)
def test_canonical_filters_orphans(sql_file):
    """Every TM xp CTE must keep filtering orphans (demoted false matches out)."""
    text = (SQL_DIR / sql_file).read_text(encoding='utf-8')
    assert "confidence <> 'orphan'" in text, (
        f"{sql_file}: lost the confidence<>'orphan' filter"
    )


@pytest.mark.unit
@pytest.mark.parametrize('sql_file', TM_HISTORIZED_SQL)
def test_historized_not_scoped_to_latest_season(sql_file):
    """#788: players/transfers must NOT scope canonical to the latest season."""
    text = (SQL_DIR / sql_file).read_text(encoding='utf-8')
    assert _MAX_SEASON_RE.search(text) is None, (
        f"{sql_file}: canonical is still scoped to the latest season — #788 "
        f"historizes it across all seasons (remove the max(season) subquery; "
        f"fan-out is prevented by the resolver demote + season JOIN)"
    )


@pytest.mark.unit
@pytest.mark.parametrize('sql_file', TM_SCOPED_SQL)
def test_market_value_history_scoped_to_latest_season(sql_file):
    """market_value_history MUST stay latest-season-scoped — Bronze repeats the
    full MV graph in every season snapshot, so an unscoped join fans out
    duplicate (canonical_id, mv_date) rows. See module docstring / #788."""
    text = (SQL_DIR / sql_file).read_text(encoding='utf-8')
    assert _MAX_SEASON_RE.search(text) is not None, (
        f"{sql_file}: lost the latest-season scope — duplicate "
        f"(canonical_id, mv_date) rows will return (Bronze snapshot ×3.18)"
    )


@pytest.mark.unit
@pytest.mark.parametrize('sql_file', TM_CANONICAL_SQL)
def test_canonical_join_carries_season_predicate(sql_file):
    """The xp JOIN must match on season — without it, multi-season canonical
    rows fan-out across seasons."""
    text = (SQL_DIR / sql_file).read_text(encoding='utf-8')
    predicate = re.search(r"xp\.season\s*=\s*b\.season", text, re.IGNORECASE)
    assert predicate is not None, (
        f"{sql_file}: canonical JOIN lost the `xp.season = b.season` predicate — "
        f"per-season canonical would fan-out across seasons"
    )
