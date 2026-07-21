"""
Unit tests for Silver dedup hardening (issue #464).

Five Silver files read Bronze WITHOUT a reliable dedup before aggregation/JOIN.
Today bronze scrapers run replace_partitions=True (full-state), so there are no
dupes — but a replace→append regression (precedent: ClubElo #283) or a re-ingest
would silently produce ×2 dupes that corrupt sums / score / odds.

Fix = one defensive pattern: ROW_NUMBER over the natural key
``ORDER BY _ingested_at DESC, _batch_id DESC`` + ``WHERE rn = 1`` BEFORE any
Bronze aggregation (mirrors fbref_match_events / understat_player_season /
gold.fct_shot).

Two test kinds (mirrors issue #463 test_fbref_season_profile_dedup):
  * Executable DuckDB tests — files whose SQL is DuckDB-compatible
    (understat shots penalty aggr, fotmob long→wide pivot). They prove the
    BEHAVIOUR: a duplicate snapshot no longer doubles/poisons the output.
  * Regex sanity — all six files must carry the dedup window with the
    deterministic tiebreaker. Covers the files too costly to execute whole
    (matchhistory: 200+ cols + gold.dim_match; sofascore: 2-arg IF / 60 cols).

CAVEAT (memory #426): DuckDB tolerates same-level SELECT-alias inside OVER()
that Trino rejects (COLUMN_NOT_FOUND). Run EXPLAIN (TYPE VALIDATE) against live
Trino as a separate verification step.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from scrapers.fotmob.constants import render_fotmob_sql


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SILVER_DIR = PROJECT_ROOT / "dags" / "sql" / "silver"

UNDERSTAT_PM_SQL = SILVER_DIR / "understat_player_match_aggregate.sql"
SOFASCORE_PS_SQL = SILVER_DIR / "sofascore_player_season_aggregate.sql"
SOFASCORE_TM_SQL = SILVER_DIR / "sofascore_team_match.sql"
MATCHHISTORY_ODDS_SQL = SILVER_DIR / "matchhistory_match_odds.sql"
FOTMOB_PLAYER_SQL = SILVER_DIR / "fotmob_player_season_profile.sql"
FOTMOB_KEEPER_SQL = SILVER_DIR / "fotmob_keeper_profile.sql"

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Trino → DuckDB translation (table-name substitution only)
# ---------------------------------------------------------------------------

_ICEBERG_TO_LOCAL = {
    "iceberg.bronze.understat_player_match_stats": "understat_player_match_stats",
    "iceberg.bronze.understat_shots":              "understat_shots",
    # native fotmob bronze (#930 cutover: player_season_profile, keeper_profile)
    "iceberg.bronze.fotmob_competition_seasons_current": "fotmob_competition_seasons_current",
    "iceberg.bronze.fotmob_season_teams_current":        "fotmob_season_teams_current",
    "iceberg.bronze.fotmob_squad_snapshots_current":     "fotmob_squad_snapshots_current",
    "iceberg.bronze.fotmob_player_snapshots_current":    "fotmob_player_snapshots_current",
    "iceberg.bronze.fotmob_leaderboards_current":        "fotmob_leaderboards_current",
}


def _translate(sql: str) -> str:
    for k, v in _ICEBERG_TO_LOCAL.items():
        sql = sql.replace(k, v)
    return sql


def _read(path: Path) -> str:
    return render_fotmob_sql(path.read_text(encoding="utf-8"))


def _strip_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


def _ddl(table: str, coltypes: dict[str, str]) -> str:
    defs = ", ".join(f'"{c}" {t}' for c, t in coltypes.items())
    return f"CREATE TABLE {table} ({defs})"


def _insert(con, table: str, **values):
    cols = ", ".join(f'"{c}"' for c in values)
    placeholders = ", ".join("?" for _ in values)
    con.execute(
        f"INSERT INTO {table} ({cols}) VALUES ({placeholders})",
        list(values.values()),
    )


T1 = "2026-01-01 00:00:00"
T2 = "2026-02-01 00:00:00"


# ---------------------------------------------------------------------------
# understat_player_match_aggregate — shot_penalty_aggr must dedup understat_shots
# ---------------------------------------------------------------------------

_UNDERSTAT_PM_COLS = {
    "game_id": "BIGINT", "player_id": "BIGINT", "player": "VARCHAR",
    "team_id": "BIGINT", "position": "VARCHAR", "minutes": "BIGINT",
    "goals": "BIGINT", "own_goals": "BIGINT", "shots": "BIGINT",
    "yellow_cards": "BIGINT", "red_cards": "BIGINT", "assists": "BIGINT",
    "key_passes": "BIGINT", "xg": "DOUBLE", "xa": "DOUBLE",
    "xg_chain": "DOUBLE", "xg_buildup": "DOUBLE", "league": "VARCHAR",
    "season": "VARCHAR", "_ingested_at": "TIMESTAMP", "_batch_id": "VARCHAR",
}

_UNDERSTAT_SHOTS_COLS = {
    "shot_id": "BIGINT", "game_id": "BIGINT", "player_id": "BIGINT",
    "xg": "DOUBLE", "result": "VARCHAR", "situation": "VARCHAR",
    "league": "VARCHAR", "season": "VARCHAR",
    "_ingested_at": "TIMESTAMP", "_batch_id": "VARCHAR",
}


@pytest.fixture()
def duck_understat():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute(_ddl("understat_player_match_stats", _UNDERSTAT_PM_COLS))
    con.execute(_ddl("understat_shots", _UNDERSTAT_SHOTS_COLS))
    yield con
    con.close()


def _run(con, sql_path: Path):
    sql = _translate(_read(sql_path))
    return con.execute(f"SELECT * FROM (\n{sql}\n)").fetch_df()


class TestUnderstatShotPenaltyDedup:
    """A re-scraped (duplicate) penalty shot must not double penalty_xg /
    penalty_goals, which feed non_penalty_xg / non_penalty_goals."""

    def _seed_player_match(self, con):
        _insert(con, "understat_player_match_stats",
                game_id=1, player_id=10, player="Test Player", team_id=99,
                position="F", minutes=90, goals=5, own_goals=0, shots=8,
                yellow_cards=0, red_cards=0, assists=1, key_passes=2,
                xg=3.0, xa=0.5, xg_chain=3.5, xg_buildup=1.0,
                league="ENG-Premier League", season="2425",
                _ingested_at=T1, _batch_id="b1")

    def test_duplicate_penalty_shot_not_double_counted(self, duck_understat):
        # Arrange: one penalty shot ingested TWICE (append-mode regression)
        self._seed_player_match(duck_understat)
        for batch, ts in [("b1", T1), ("b2", T2)]:
            _insert(duck_understat, "understat_shots",
                    shot_id=100, game_id=1, player_id=10, xg=0.7612,
                    result="Goal", situation=None,
                    league="ENG-Premier League", season="2425",
                    _ingested_at=ts, _batch_id=batch)

        # Act
        df = _run(duck_understat, UNDERSTAT_PM_SQL)

        # Assert: penalty counted ONCE → np_goals = 5-1 = 4, np_xg = 3.0-0.7612
        assert len(df) == 1
        row = df.iloc[0]
        assert row["non_penalty_goals"] == 4, (
            "duplicate penalty shot double-counted → penalty_goals doubled"
        )
        assert row["non_penalty_xg"] == pytest.approx(3.0 - 0.7612, abs=1e-4), (
            "duplicate penalty shot doubled penalty_xg"
        )

    def test_single_penalty_shot_baseline(self, duck_understat):
        # Control: one shot, one ingest → identical result (no over-dedup)
        self._seed_player_match(duck_understat)
        _insert(duck_understat, "understat_shots",
                shot_id=100, game_id=1, player_id=10, xg=0.7612,
                result="Goal", situation=None,
                league="ENG-Premier League", season="2425",
                _ingested_at=T1, _batch_id="b1")

        df = _run(duck_understat, UNDERSTAT_PM_SQL)

        assert df.iloc[0]["non_penalty_goals"] == 4
        assert df.iloc[0]["non_penalty_xg"] == pytest.approx(3.0 - 0.7612, abs=1e-4)


# ---------------------------------------------------------------------------
# fotmob profiles — stats_pivoted must take the LATEST snapshot, not MAX
# ---------------------------------------------------------------------------

# --- native fotmob bronze (#930 cutover): минимальные схемы под scope-каркас ---

_FOTMOB_NATIVE_SEASONS_COLS = {
    "competition_id": "VARCHAR", "source_season_key": "VARCHAR",
    "is_selected": "BOOLEAN", "is_latest": "BOOLEAN",
}

_FOTMOB_NATIVE_SEASON_TEAMS_COLS = {
    "competition_id": "BIGINT", "source_season_key": "VARCHAR",
    "team_id": "BIGINT", "team_name": "VARCHAR",
}

_FOTMOB_NATIVE_SQUAD_COLS = {
    "team_id": "VARCHAR", "member_id": "VARCHAR", "member_type": "VARCHAR",
}

_FOTMOB_NATIVE_SNAPSHOTS_COLS = {
    "player_id": "VARCHAR", "name": "VARCHAR", "position_description": "VARCHAR",
    "is_coach": "BOOLEAN", "primary_team_id": "BIGINT",
    "primary_team_name": "VARCHAR",
    "_observed_at": "TIMESTAMP", "_target_batch_id": "VARCHAR",
    "_ingested_at": "TIMESTAMP",
}

_FOTMOB_NATIVE_LEADERBOARDS_COLS = {
    "competition_id": "BIGINT", "source_season_key": "VARCHAR",
    "participant_type": "VARCHAR", "participant_id": "BIGINT",
    "team_id": "BIGINT", "stat_name": "VARCHAR", "stat_value": "DOUBLE",
    "matches_played": "BIGINT",
    "_observed_at": "TIMESTAMP", "_target_batch_id": "VARCHAR",
    "_ingested_at": "TIMESTAMP",
}


@pytest.fixture()
def duck_fotmob():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    # native bronze (#930): player_season_profile / keeper_profile читают *_current
    con.execute(_ddl("fotmob_competition_seasons_current", _FOTMOB_NATIVE_SEASONS_COLS))
    con.execute(_ddl("fotmob_season_teams_current", _FOTMOB_NATIVE_SEASON_TEAMS_COLS))
    con.execute(_ddl("fotmob_squad_snapshots_current", _FOTMOB_NATIVE_SQUAD_COLS))
    con.execute(_ddl("fotmob_player_snapshots_current", _FOTMOB_NATIVE_SNAPSHOTS_COLS))
    con.execute(_ddl("fotmob_leaderboards_current", _FOTMOB_NATIVE_LEADERBOARDS_COLS))
    yield con
    con.close()


def _seed_fotmob_native_stat(con, stat_name, old_value, new_value):
    """Native (#930): same stat re-scraped into fotmob_leaderboards_current.

    _current-view даёт обе строки (его natural key содержит team_id/rank/
    top_list_index — мельче silver-ключа дедупа), поэтому silver обязан сам
    выбрать последний снапшот по (_observed_at, _target_batch_id)."""
    for ts, batch, val in [(T1, "b1", old_value), (T2, "b2", new_value)]:
        _insert(con, "fotmob_leaderboards_current",
                competition_id=47, source_season_key="2024/2025",
                participant_type="player", participant_id=1, team_id=99,
                stat_name=stat_name, stat_value=val, matches_played=10,
                _observed_at=ts, _target_batch_id=batch, _ingested_at=ts)


class TestFotmobPivotLatestSnapshot:
    """For a non-monotonic metric (rating, save_percentage) re-scraped with a
    LOWER corrected value, MAX(stat_value) would return the stale historical
    high; the dedup must surface the latest snapshot instead."""

    def test_player_rating_takes_latest_not_max(self, duck_fotmob):
        # Arrange (native #930): season-ось + глобальный снапшот игрока;
        # членство в (лиге, сезоне) придёт из лидербордов (lb_player_scope).
        _insert(duck_fotmob, "fotmob_competition_seasons_current",
                competition_id="47", source_season_key="2024/2025",
                is_selected=True, is_latest=True)
        _insert(duck_fotmob, "fotmob_player_snapshots_current",
                player_id="1", name="Out Field", position_description="forward",
                is_coach=False, primary_team_id=99, primary_team_name="Test FC",
                _observed_at=T1, _target_batch_id="b1", _ingested_at=T1)
        # rating corrected DOWN 7.5 → 6.0 on re-scrape
        _seed_fotmob_native_stat(duck_fotmob, "rating", old_value=7.5, new_value=6.0)

        # Act
        df = _run(duck_fotmob, FOTMOB_PLAYER_SQL)

        # Assert
        assert len(df) == 1
        assert df.iloc[0]["fotmob_rating"] == pytest.approx(6.0), (
            "MAX(stat_value) over snapshots returned stale high instead of latest"
        )
        # season-слаг обязан прийти из season_axis каркаса ('2024/2025' → '2425')
        assert df.iloc[0]["season"] == "2425"
        assert df.iloc[0]["league"] == "ENG-Premier League"

    def test_keeper_save_percentage_takes_latest_not_max(self, duck_fotmob):
        # Arrange (native #930): season-ось + глобальный снапшот вратаря;
        # членство в (лиге, сезоне) придёт из лидербордов (lb_player_scope).
        _insert(duck_fotmob, "fotmob_competition_seasons_current",
                competition_id="47", source_season_key="2024/2025",
                is_selected=True, is_latest=True)
        _insert(duck_fotmob, "fotmob_player_snapshots_current",
                player_id="1", name="The Keeper", position_description="keeper",
                is_coach=False, primary_team_id=99, primary_team_name="Test FC",
                _observed_at=T1, _target_batch_id="b1", _ingested_at=T1)
        # save% corrected DOWN 80.0 → 72.0 on re-scrape
        _seed_fotmob_native_stat(duck_fotmob, "_save_percentage",
                                 old_value=80.0, new_value=72.0)

        # Act
        df = _run(duck_fotmob, FOTMOB_KEEPER_SQL)

        # Assert
        assert len(df) == 1
        assert df.iloc[0]["save_percentage"] == pytest.approx(72.0), (
            "MAX(stat_value) over snapshots returned stale high instead of latest"
        )
        # season-слаг обязан прийти из season_axis каркаса ('2024/2025' → '2425')
        assert df.iloc[0]["season"] == "2425"
        assert df.iloc[0]["league"] == "ENG-Premier League"


# ---------------------------------------------------------------------------
# Regex sanity — every file carries a dedup window with the deterministic
# tiebreaker; the files too costly to execute are covered structurally.
# ---------------------------------------------------------------------------

_TIEBREAKER = re.compile(r"_ingested_at\s+DESC\s*,\s*_batch_id\s+DESC", re.IGNORECASE)
# native *_current (#930): _batch_id не существует — детерминированный
# тайбрейкер там (_observed_at DESC, _target_batch_id DESC)
_TIEBREAKER_NATIVE = re.compile(
    r"_observed_at\s+DESC\s*,\s*(?:\w+\.)?_target_batch_id\s+DESC", re.IGNORECASE)


def _windows(sql: str) -> list[tuple[str, str]]:
    """Return (partition_by, order_by) for each ROW_NUMBER window."""
    body = _strip_comments(sql)
    return re.findall(
        r"ROW_NUMBER\(\)\s*OVER\s*\(\s*PARTITION BY\s+(.+?)\s+ORDER BY\s+(.+?)\)",
        body, re.DOTALL | re.IGNORECASE)


class TestUnderstatShotsDedupStructure:
    def test_shots_deduped_before_aggregation(self):
        body = _strip_comments(_read(UNDERSTAT_PM_SQL))
        # shot_penalty_aggr must read from a deduped CTE, not raw bronze
        assert re.search(r"shot_penalty_aggr\s+AS\s*\(.*?FROM\s+shots_dedup",
                         body, re.DOTALL | re.IGNORECASE), (
            "shot_penalty_aggr must aggregate the deduped shots CTE, not "
            "iceberg.bronze.understat_shots directly (#464)"
        )
        shot_windows = [(p, o) for p, o in _windows(_read(UNDERSTAT_PM_SQL))
                        if "shot_id" in p.lower()]
        assert shot_windows, "no PARTITION BY shot_id dedup window (#464)"
        for _, order in shot_windows:
            assert _TIEBREAKER.search(order), (
                f"shots dedup lacks deterministic tiebreaker (#464): {order!r}"
            )


class TestSofascoreSeasonCanonicalDedup:
    def test_canonical_rn_window_present(self):
        body = _strip_comments(_read(SOFASCORE_PS_SQL))
        canonical_windows = [(p, o) for p, o in _windows(_read(SOFASCORE_PS_SQL))
                             if "canonical_id" in p.lower()]
        assert canonical_windows, (
            "sofascore_player_season needs a ROW_NUMBER PARTITION BY "
            "canonical_id, league, season after the xref_player JOIN (#464)"
        )
        assert re.search(r"WHERE\s+canonical_rn\s*=\s*1", body, re.IGNORECASE), (
            "canonical dedup window not filtered with WHERE canonical_rn = 1"
        )


class TestSofascoreTeamScheduleDedup:
    def test_schedule_dim_not_select_distinct(self):
        body = _strip_comments(_read(SOFASCORE_TM_SQL))
        # schedule_dim must dedup by game_id, not rely on SELECT DISTINCT
        assert not re.search(
            r"schedule_dim\s+AS\s*\(\s*SELECT\s+DISTINCT", body, re.IGNORECASE), (
            "schedule_dim still uses SELECT DISTINCT — two snapshots of one "
            "game_id with different scores (live 0:0 vs final) both survive (#464)"
        )
        game_windows = [(p, o) for p, o in _windows(_read(SOFASCORE_TM_SQL))
                        if re.search(r"\bgame_id\b", p, re.IGNORECASE)]
        assert game_windows, "no PARTITION BY game_id dedup window in schedule_dim (#464)"
        for _, order in game_windows:
            assert _TIEBREAKER.search(order), (
                f"schedule_dim dedup lacks deterministic tiebreaker (#464): {order!r}"
            )


class TestMatchhistoryOddsDedup:
    def test_mh_deduped_by_match_key(self):
        windows = _windows(_read(MATCHHISTORY_ODDS_SQL))
        match_windows = [
            (p, o) for p, o in windows
            if "home_team" in p.lower() and "away_team" in p.lower()
        ]
        assert match_windows, (
            "matchhistory_match_odds CTE `mh` reads bronze without dedup — needs "
            "ROW_NUMBER PARTITION BY (league, match_date, home_team, away_team, "
            "season) (#464)"
        )
        for _, order in match_windows:
            assert _TIEBREAKER.search(order), (
                f"matchhistory dedup lacks deterministic tiebreaker (#464): {order!r}"
            )


class TestFotmobPivotDedupStructure:
    @pytest.mark.parametrize("path", [FOTMOB_PLAYER_SQL, FOTMOB_KEEPER_SQL],
                             ids=["player", "keeper"])
    def test_stats_deduped_before_pivot(self, path):
        body = _strip_comments(_read(path))
        assert re.search(r"stats_pivoted\s+AS\s*\(.*?FROM\s+stats_dedup",
                         body, re.DOTALL | re.IGNORECASE), (
            f"{path.name}: stats_pivoted must MAX-pivot the deduped CTE, not "
            f"iceberg.bronze.fotmob_player_stats directly (#464)"
        )
        stat_windows = [(p, o) for p, o in _windows(_read(path))
                        if "stat_name" in p.lower()]
        assert stat_windows, f"{path.name}: no PARTITION BY ... stat_name dedup window (#464)"
        for _, order in stat_windows:
            # legacy bronze: (_ingested_at, _batch_id); native *_current (#930):
            # (_observed_at, _target_batch_id) — _batch_id там не существует.
            assert _TIEBREAKER.search(order) or _TIEBREAKER_NATIVE.search(order), (
                f"{path.name}: stats dedup lacks deterministic tiebreaker (#464): {order!r}"
            )
