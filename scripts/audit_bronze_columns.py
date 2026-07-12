"""Bronze column-quality audit: ALL_NULL / CONSTANT / HIGH_NULL / ALL_EMPTY_STR.

Scans every column in every `iceberg.bronze.*` table and flags:
  - ERROR  ALL_NULL       — non_null = 0 (not in EXPECTED_NULL allowlist)
  - ERROR  ALL_EMPTY_STR  — varchar col where every non-NULL value is ''
  - WARN   HIGH_NULL      — null_rate > 0.95 (not allowlist, not 1.0)
  - WARN   CONSTANT       — distinct = 1 (not a partition column)
  - INFO   EXPECTED_NULL  — col matches feedback_bronze_expected_null_columns.md

Usage (inside airflow-webserver container):
    python /opt/airflow/scripts/audit_bronze_columns.py --output /tmp/audit.md

Uses the lightweight Silver Trino helper and never imports scraper runtimes.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, '/opt/airflow/dags')
from utils.silver_tasks import _get_trino_connection

# ---- Allowlist (verbatim from feedback_bronze_expected_null_columns.md) ----

EXPECTED_NULL: dict[str, set[str]] = {
    # fbref_keeper_keeper_adv removed in #606 — scrape stopped, table dropped.
    'fbref_team_misc': {'pkwon', 'pkcon'},
    'fbref_player_misc': {'pkwon', 'pkcon'},
    'fbref_match_player_stats': {'pkwon', 'pkcon'},
    'fbref_schedule': {
        # FBref schedule "Notes" column is upstream-sparse — only filled for rare
        # match states ("Match Awarded", "Abandoned", ...). 100% NULL across all
        # currently-ingested rows; not a parser drop (column maps straight from the
        # raw HTML table). Verified 2026-06-03 (#276).
        'notes',
    },
    # matchhistory_games removed (#307: legacy table dropped). matchhistory_results
    # has 0 all-NULL columns (verified 2026-06-04, #282) → no allowlist needed.
    'espn_matchsheet': {'capacity'},
    # sofifa_team_ratings: the 15 dead FC-26 columns are no longer scraped
    # (flaresolverr_reader.read_team_ratings trimmed to 8 live cols) and were
    # physically dropped from Bronze via drop_sofifa_team_ratings_dead_columns.py
    # (#601). No all-NULL columns remain, so no allowlist entry is needed.
    'whoscored_schedule': {
        'aggregate_winner_field', 'extra_result_field',
        'home_extratime_score', 'away_extratime_score',
        'home_penalty_score', 'away_penalty_score',
        'stage',
        # serialized nested betting-odds column — soccerdata's WhoScored
        # read_schedule never populates `bets` for in-scope leagues (100% NULL
        # across all 2280 rows, verified 2026-06-04 #278). Not a parser drop;
        # the sibling `incidents` nested column does carry data.
        'bets',
    },
    'whoscored_season_stages': {'stage'},
    'whoscored_events': {
        # event-conditional cols (≥97% NULL by design)
        'goal_mouth_y', 'goal_mouth_z', 'blocked_x', 'blocked_y',
        'is_shot', 'is_goal', 'card_type',
        'related_event_id', 'related_player_id',
        'end_x', 'end_y',
    },
    'sofascore_event_player_stats': {
        # is_home/captain/substitute/position_specific are now populated
        # from /lineups — forward-filled for new matches (#301) and
        # back-filled for the historical 15189 rows (#337) — so they are
        # no longer expected-NULL.
        # auto-flatten artifacts: `ratingVersions` is a nested
        # {original, alternative} object _coerce_scalar cannot reduce to a
        # scalar; `statisticsType` is an always-empty key. Both 100% NULL.
        'rating_versions', 'statistics_type',
    },
    'sofascore_event_shotmap': {
        # Per-shot payload omits teamId/team.id and reversedPeriodCount/period
        # for in-scope leagues — 100% NULL across all 9543 rows (verified
        # 2026-06-04 #280). Shot side is still recoverable via `is_home`;
        # period is simply not surfaced per shot by SofaScore's shotmap block.
        'team_id', 'period',
    },
    'sofascore_player_season_stats': {
        # auto-flatten always-empty `statisticsType` key (100% NULL, 533 rows).
        'statistics_type',
    },
    # --- FotMob (#281) -------------------------------------------------------
    # Originally 14 columns were 100% NULL live (verified 2026-06-04) in two
    # classes: (a) upstream-missing — scraper emits the field but FotMob returns
    # None for every in-scope row; (b) dead-legacy drift — the current scraper no
    # longer emits the column, it survived only as an Iceberg schema remnant.
    # The 10 dead-legacy columns were physically dropped via CTAS-rebuild (#304,
    # scripts/drop_fotmob_dead_columns.py), so only the (a) upstream-missing
    # entries remain allowlisted below.
    'fotmob_team_stats': {
        # (a) upstream-missing: read_team_season_stats writes team.get('form'),
        # but FotMob's league table omits `form` for in-scope leagues.
        'form',
    },
    'fotmob_team_leaderboards': {
        # (a) upstream-shape: read_team_leaderboards maps item.get('TeamName')
        # (scraper.py:716), but team-leaderboard items carry the team name in
        # `ParticipantName` (-> participant_name) instead, so team_name is NULL.
        'team_name',
    },
    'fotmob_transfers': {
        # (a) upstream-missing: read_transfers derives fee_text from
        # fee.fallback/fee.text (scraper.py:775); FotMob supplies only the
        # numeric `fee_value` for in-scope transfers, so fee_text is NULL.
        'fee_text',
    },
    'fotmob_player_details': {
        # (a) upstream-missing: read_player_details maps d.get('nextMatch')
        # (scraper.py:988); the player __NEXT_DATA__ payload omits `nextMatch`
        # for every squad player, so the column is 100% NULL.
        'next_match_json',
    },
    # NB: no 'capology_player_salaries' entry — #320 allowlisted adjusted_total_*
    # while only the in-progress season was materialised, but #321 backfilled
    # completed seasons (which DO carry adjusted_total_*), so those cols are no
    # longer all-NULL. Removing the stale entry (resolves #319).
}

# Columns that hold exactly 1 distinct value by design — should NOT trigger
# CONSTANT WARN. Most cases: each FBref stat-table contains exactly one stat_type
# (DAG materialises one table per stat_type), and helper columns like `matches`
# (link text), `pos` (always 'GK' in keeper tables) are constants for the same
# reason.
EXPECTED_CONSTANT: dict[str, set[str]] = {
    'fbref_player_stats': {'stat_type', 'matches'},
    'fbref_player_shooting': {'stat_type', 'matches'},
    'fbref_player_playingtime': {'stat_type', 'matches'},
    'fbref_player_misc': {'stat_type', 'matches'},
    'fbref_team_stats': {'stat_type'},
    'fbref_team_shooting': {'stat_type'},
    # min% and mn/mp are denormalised aggregates FBref renders as a column header
    # but populates with the team season constant (1 distinct value across all rows).
    'fbref_team_playingtime': {'stat_type', 'min%', 'mn/mp'},
    'fbref_team_misc': {'stat_type'},
    'fbref_keeper_keeper': {'stat_type', 'pos', 'matches'},
    # notes is sparse-by-design (HIGH_NULL) but the few populated rows happen to
    # share the same text ("Award Decided on Pens"), triggering CONSTANT too.
    'fbref_schedule': {'notes'},
    # div = football-data division code (E0); CONSTANT т.к. в scope только EPL (#309).
    'matchhistory_results': {'div'},
}

# Internal metadata cols — we skip them from CONSTANT-checks (they are constant
# by design — _source='fbref' for all rows), but still NULL-check them.
META_COLS = {'_source', '_entity_type', '_ingested_at', '_batch_id'}

# Per-source parser "contract": minimal REQUIRED tables + columns each source's
# scraper is expected to emit into bronze. Authored by reading the scraper class
# without importing optional browser runtimes. Semantics: listed columns are the
# minimal required set; extra
# live columns are NOT errors. Used by the `--source` contract-diff mode.
# #274 seeds espn only; other sources filled in #276-#286.
EXPECTED_TABLES: dict[str, dict[str, set[str]]] = {
    'espn': {
        # Reliably-produced set (verified vs live bronze 2026-06-03). The
        # _standardize_schedule renames (home_score->home_goals, venue, attendance)
        # are conditional on raw soccerdata columns that ESPN never supplies, so
        # those targets never materialise — contract lists what actually lands.
        'espn_schedule': {
            'league', 'season', 'game', 'match_date', 'home_team', 'away_team',
            'game_id', 'league_id',
            *META_COLS,
        },
        # Producer: ESPNScraper.read_lineup (dags/scripts/run_espn_scraper.py
        # lineup branch). Extra stat columns also land, but only these are
        # required.
        'espn_lineup': {
            'league', 'season', 'game', 'team', 'player', 'position',
            'formation_place', 'sub_in', 'sub_out',
            *META_COLS,
        },
        # Producer: ESPNScraper.read_matchsheet (run_espn_scraper.py matchsheet
        # branch; soccerdata read_matchsheet — #298). One row per (game, team) with
        # venue + ~35 team stat columns; only identity + venue are required
        # (extra stat columns are NOT errors; `capacity` is 100%-NULL and lives
        # in EXPECTED_NULL, so it is not listed here).
        'espn_matchsheet': {
            'league', 'season', 'game', 'team', 'is_home', 'venue', 'attendance',
            *META_COLS,
        },
        # NOTE: espn_standings is NOT in the contract — soccerdata's ESPN reader
        # has no read_standings, so the table is never materialised. The dead
        # scrape path was removed in #354. Listing it here would be a permanent
        # false-positive.
    },
    'fbref': {
        # Minimal required set per table — identity keys + a few core metrics
        # (FBref schemas are wide & volatile; extra live columns are NOT errors,
        # and per-column non-NULL coverage is enforced separately by audit_table).
        # Verified vs live bronze 2026-06-03. fbref_shot_events is intentionally
        # absent (see EXPECTED_ABSENT) — FBref Feb-2026 shot-data restriction.
        # --- match-level ---
        'fbref_schedule': {
            'league', 'season', 'date', 'home', 'away', 'score', *META_COLS,
        },
        'fbref_shot_events': {  # listed for completeness; gated by EXPECTED_ABSENT
            'league', 'season', 'match_id', 'minute', 'player', 'squad', *META_COLS,
        },
        'fbref_match_events': {
            'league', 'season', 'match_id', 'minute', 'event_type', 'player', 'team',
            *META_COLS,
        },
        'fbref_lineups': {
            'league', 'season', 'match_id', 'team', 'player', 'is_starter', 'position',
            *META_COLS,
        },
        'fbref_match_team_stats': {
            'league', 'season', 'match_id', 'home_team', 'away_team', 'home_shots',
            'away_shots', *META_COLS,
        },
        'fbref_match_player_stats': {
            'league', 'season', 'match_id', 'player', 'team', 'min', 'gls', *META_COLS,
        },
        'fbref_match_managers': {
            'league', 'season', 'match_id', 'side', 'team', 'manager_name', *META_COLS,
        },
        # --- season player ---
        'fbref_player_stats': {
            'league', 'season', 'player', 'squad', 'mp', 'min', 'gls', *META_COLS,
        },
        'fbref_player_shooting': {
            'league', 'season', 'player', 'squad', 'sh', 'sot', 'gls', *META_COLS,
        },
        'fbref_player_playingtime': {
            'league', 'season', 'player', 'squad', 'mp', 'min', 'starts', *META_COLS,
        },
        'fbref_player_misc': {
            'league', 'season', 'player', 'squad', 'crdy', 'crdr', 'fls', *META_COLS,
        },
        # --- season team ---
        'fbref_team_stats': {
            'league', 'season', 'squad', 'team_id', 'mp', 'gls', 'poss', *META_COLS,
        },
        'fbref_team_shooting': {
            'league', 'season', 'squad', 'team_id', 'sh', 'sot', 'gls', *META_COLS,
        },
        'fbref_team_playingtime': {
            'league', 'season', 'squad', 'team_id', 'mp', 'min', 'starts', *META_COLS,
        },
        'fbref_team_misc': {
            'league', 'season', 'squad', 'team_id', 'crdy', 'crdr', 'fls', *META_COLS,
        },
        # --- keeper ---
        'fbref_keeper_keeper': {
            'league', 'season', 'player', 'squad', 'mp', 'ga', 'saves', *META_COLS,
        },
        # keeper_adv removed in #606 — scrape stopped, table dropped.
    },
    'understat': {
        # soccerdata reader output (UnderstatScraper). 5 tables, all partitioned
        # ['league', 'season']. Minimal required set = identity keys + core metrics
        # + META_COLS; extra live columns are NOT errors. Column names verified vs
        # live bronze snapshot (tests/fixtures/bronze_schemas.json, 2026-06-02).
        # NOTE: bronze.understat_players carries ×10 row dups upstream (deduped in
        # Silver via ROW_NUMBER) — not a coverage failure (per-column non-NULL is
        # unaffected by row dups).
        'understat_schedule': {
            'league', 'season', 'game', 'game_id', 'date',
            'home_team', 'away_team', 'home_goals', 'away_goals',
            'home_xg', 'away_xg', *META_COLS,
        },
        'understat_shots': {
            'league', 'season', 'game_id', 'shot_id', 'minute',
            'player', 'player_id', 'team', 'team_id', 'xg', 'result', *META_COLS,
        },
        'understat_players': {
            'league', 'season', 'player', 'player_id', 'team', 'position',
            'matches', 'minutes', 'goals', 'assists', 'shots', 'xg', 'xa', *META_COLS,
        },
        'understat_team_match_stats': {
            'league', 'season', 'game_id', 'date', 'home_team', 'away_team',
            'home_goals', 'away_goals', 'home_xg', 'away_xg', *META_COLS,
        },
        'understat_player_match_stats': {
            'league', 'season', 'game_id', 'player', 'player_id', 'team',
            'minutes', 'goals', 'assists', 'shots', 'xg', 'xa', *META_COLS,
        },
    },
    'whoscored': {
        # soccerdata WhoScored reader + FlareSolverr events fetcher. 4 tables,
        # all partitioned ['league', 'season']. Minimal required = identity keys +
        # core + META_COLS; extra live cols are NOT errors; EXPECTED_NULL cols
        # (extratime/penalty/stage/bets on schedule; event-conditional on events)
        # are excluded here. Verified vs live bronze 2026-06-04 (#278): all 4
        # tables materialise (schedule 2280, events 709937, missing_players 21874,
        # season_stages 6 rows) — 0 missing tables/columns.
        'whoscored_schedule': {
            'league', 'season', 'game', 'game_id', 'date',
            'home_team', 'away_team', 'home_team_id', 'away_team_id',
            'home_score', 'away_score', *META_COLS,
        },
        'whoscored_events': {
            'league', 'season', 'game', 'game_id', 'minute', 'second', 'period',
            'type', 'outcome_type', 'team', 'team_id', 'player', 'player_id',
            'x', 'y', *META_COLS,
        },
        'whoscored_missing_players': {
            'league', 'season', 'game', 'game_id', 'team',
            'player', 'player_id', 'reason', 'status', *META_COLS,
        },
        'whoscored_season_stages': {  # cup/league stage metadata (6 rows live)
            'league', 'season', 'stage_id', *META_COLS,
        },
    },
    'sofascore': {
        # soccerdata Sofascore reader (schedule + league_table) + cherry-pick
        # JSON API endpoints (ratings/shotmap/event+season player stats/profile/
        # match_stats). 8 tables, all partitioned ['league', 'season']. Minimal
        # required = identity keys + core metrics + META_COLS; extra live cols
        # (the wide Opta stat catalogues on *_player_*_stats) are NOT errors.
        # Verified vs live bronze 2026-06-04 (#280): all 8 materialise + non-empty
        # (schedule 380, league_table 20, ratings/event_player_stats 15189,
        # event_shotmap 9543, match_stats 47188, player_season_stats/profile 533).
        'sofascore_schedule': {
            'league', 'season', 'game_id', 'date',
            'home_team', 'away_team', 'home_score', 'away_score',
            *META_COLS,
        },
        'sofascore_league_table': {
            'league', 'season', 'team', 'mp', 'w', 'd', 'l',
            'gf', 'ga', 'gd', 'pts', 'group', *META_COLS,
        },
        'sofascore_player_ratings': {
            'league', 'season', 'match_id', 'player_id',
            'team_side', 'rating', *META_COLS,
        },
        'sofascore_player_season_stats': {
            'league', 'season', 'player_id', 'team_id', 'team_name',
            'appearances', 'goals', 'assists', 'minutes_played', 'rating',
            *META_COLS,
        },
        'sofascore_player_profile': {
            'league', 'season', 'player_id', 'name', 'position',
            *META_COLS,
        },
        'sofascore_event_shotmap': {
            # `period` + `team_id` are 100% NULL upstream (see EXPECTED_NULL),
            # so they are excluded from the required set.
            'league', 'season', 'match_id', 'shot_id', 'player_id',
            'minute', 'outcome', 'x', 'y', 'xg', *META_COLS,
        },
        'sofascore_event_player_stats': {
            'league', 'season', 'match_id', 'player_id',
            'minutes_played', 'rating', 'position', *META_COLS,
        },
        'sofascore_match_stats': {
            'league', 'season', 'match_id', 'period',
            'stat_key', 'stat_name', 'home_value', 'away_value', *META_COLS,
        },
    },
    'fotmob': {
        # FotMob scraper (scrapers/fotmob/scraper.py, scrape_all). 9 tables, all
        # partitioned ['league', 'season']. Minimal required = identity keys +
        # core metrics + META_COLS; extra live cols are NOT errors; 14 100%-NULL
        # cols (10 dead-legacy + 4 upstream-missing) live in EXPECTED_NULL and are
        # excluded here. Verified vs live bronze 2026-06-04 (#281): all 9
        # materialise + non-empty (schedule 760, team_stats 40, player_stats 20227,
        # team_profile 20, team_squad 607, team_leaderboards 574, transfers 100,
        # match_details 380, player_details 607 rows) — 0 missing tables/columns.
        'fotmob_schedule': {
            'league', 'season', 'match_id', 'date',
            'home_team', 'away_team', 'home_team_id', 'away_team_id',
            *META_COLS,
        },
        'fotmob_team_stats': {
            'league', 'season', 'team_id', 'team_name',
            'position', 'played', 'points', 'group', *META_COLS,
        },
        'fotmob_player_stats': {
            'league', 'season', 'participant_id', 'participant_name', 'team_id',
            'stat_name', 'stat_category_header', 'stat_value', 'minutes_played',
            *META_COLS,
        },
        'fotmob_team_profile': {
            'league', 'season', 'team_id', 'team_name',
            'short_name', 'overview_season', *META_COLS,
        },
        'fotmob_team_squad': {
            'league', 'season', 'team_id', 'player_id', 'player_name',
            'role', 'shirt_number', *META_COLS,
        },
        'fotmob_team_leaderboards': {
            'league', 'season', 'team_id', 'participant_name',
            'stat_name', 'stat_category_group', 'stat_value', *META_COLS,
        },
        'fotmob_transfers': {
            'league', 'season', 'player_id', 'player_name',
            'transfer_date', 'to_club_id', 'from_club', *META_COLS,
        },
        'fotmob_match_details': {
            'league', 'season', 'match_id', 'home_team', 'away_team',
            'lineup_json', 'events_json', *META_COLS,
        },
        'fotmob_player_details': {
            'league', 'season', 'player_id', 'name',
            'birth_date', 'primary_team_id', *META_COLS,
        },
    },
    'clubelo': {
        # ClubElo scraper (scrapers/clubelo/scraper.py). 2 tables. Verified vs
        # live bronze 2026-06-04 (#283). Identity col is `team` (NOT `club`).
        # `rank`/`league` are upstream-sparse but present (not all-NULL), so they
        # are intentionally NOT in the required set — extra live cols are not
        # errors. clubelo_ratings is the daily snapshot (partition rating_date);
        # the heavy ratings_historical table is produced weekly by
        # dag_ingest_clubelo_full with replace_partitions=['rating_date'] —
        # never daily APPEND (2026-05-04 HDFS overflow).
        'clubelo_ratings': {
            'team', 'country', 'level', 'elo', 'from', 'to', 'rating_date',
            *META_COLS,
        },
        'clubelo_ratings_historical': {
            'team', 'country', 'level', 'elo', 'from', 'to', 'rating_date',
            *META_COLS,
        },
    },
    'matchhistory': {
        # football-data.co.uk CSV scraper, COLUMN_MAPPING (scrapers/matchhistory/scraper.py).
        # 1 table, partitioned ['league','season']. Минимальный контракт = identity +
        # core match stats + META_COLS. Широкие odds-колонки (odds_home_b365, maxh, ...) —
        # extra live cols, НЕ ошибки. 0 all-NULL колонок live (verified 2026-06-04, #282).
        # #307 RESOLVED: silver мигрирован на matchhistory_results, legacy
        # matchhistory_games дропнут — дрейфа больше нет.
        'matchhistory_results': {
            'league', 'season', 'match_date', 'home_team', 'away_team',
            'home_goals', 'away_goals', 'result',
            'home_goals_ht', 'away_goals_ht', 'result_ht', 'referee',
            'home_shots', 'away_shots', 'home_shots_on_target', 'away_shots_on_target',
            'home_fouls', 'away_fouls', 'home_corners', 'away_corners',
            'home_yellow', 'away_yellow', 'home_red', 'away_red',
            *META_COLS,
        },
    },
    'sofifa': {
        # SoFIFA scraper (scrapers/sofifa/scraper.py — soccerdata reader +
        # FlareSolverr override). 6 tables (sofifa_leagues unpartitioned, the
        # other 5 partitioned ['fifa_edition']). Minimal required = identity keys
        # + core ratings + META_COLS; extra live cols are NOT errors; the 15
        # dead FC-26 sofifa_team_ratings cols were dropped from the parser and
        # Bronze (#601). FlareSolverr v3.4.6 (Chromium 142) clears the sofifa.com
        # Turnstile — ingest works (the earlier #180 CF freeze is resolved). All
        # 6 tables materialise + non-empty (verified live 2026-06-05, #284):
        # FC 26 / ENG-Premier League — player_ratings 546, players 546,
        # team_ratings 20, teams 20, versions 852, leagues 1 — 0 missing
        # tables/columns, 0 all-NULL outside the allowlist.
        'sofifa_players': {
            'fifa_edition', 'player', 'player_id', 'team', 'league', *META_COLS,
        },
        'sofifa_teams': {
            'fifa_edition', 'team', 'team_id', 'league', *META_COLS,
        },
        'sofifa_player_ratings': {
            'fifa_edition', 'player', 'player_id', 'position',
            'overallrating', 'potential', 'pace', 'shooting', 'passing',
            'dribbling', 'defending', 'physical', *META_COLS,
        },
        # build_up_*/chance_creation_*/defence_*/...prestige/whole_team_average_age
        # (15 cols) were removed by FC 26 upstream; the parser no longer scrapes
        # them and they were dropped from Bronze (#601).
        'sofifa_team_ratings': {
            'fifa_edition', 'team', 'team_id', 'league',
            'overall', 'attack', 'midfield', 'defence', *META_COLS,
        },
        'sofifa_versions': {  # FIFA-edition lookup (soccerdata read_versions)
            'version_id', 'fifa_edition', 'update', *META_COLS,
        },
        # league -> sofifa league_id lookup (soccerdata read_leagues).
        # Unpartitioned reference table; replace-on-`league` keeps it idempotent.
        'sofifa_leagues': {
            'league', 'league_id', *META_COLS,
        },
    },
    'transfermarkt': {
        # 3 tables, ENG-PL MVP only (TM_LEAGUE_MAP). Verified live 2026-06-05
        # (#285): players 555, market_value_history 2121, transfers 750 rows on
        # ('ENG-Premier League','2526'). 0 all-NULL columns -> no EXPECTED_NULL.
        # Minimal required set (identity keys + core metrics); extra live columns
        # are NOT errors. Sparse-by-design (NOT all-NULL): transfers.fee_eur
        # 176/750 (free transfers), transfers.market_value_eur 435/750.
        'transfermarkt_players': {
            'league', 'season', 'player_id', 'name', 'position', 'nationality',
            'market_value_eur', 'current_club_id', 'current_club_name', *META_COLS,
        },
        'transfermarkt_market_value_history': {
            'league', 'season', 'player_id', 'mv_date', 'value_eur', 'club_name',
            *META_COLS,
        },
        'transfermarkt_transfers': {
            'league', 'season', 'player_id', 'transfer_date', 'from_club_id',
            'to_club_id', 'fee_text', 'is_upcoming', *META_COLS,
        },
    },
    'capology': {
        # 4 APL data products (#321), all from the same anti-bot-free tls path,
        # partition (league, season[, currency]). All 3 currencies inline.
        # Verified live 2026-06-05 across seasons 2324/2425/2526. Backfill of
        # completed seasons populates adjusted_total_* (resolves #319) so the
        # salaries adjusted_total_* cols are no longer all-NULL — hence no
        # EXPECTED_NULL entry for capology.
        'capology_player_salaries': {
            'league', 'season', 'currency', 'player_slug', 'player_name',
            'club_slug', 'club_name', 'country_code', 'age', 'position',
            'status', 'active', 'loan', 'verified',
            'weekly_gross_gbp', 'weekly_gross_eur', 'weekly_gross_usd',
            'annual_gross_gbp', 'annual_gross_eur', 'annual_gross_usd',
            'weekly_net_gbp', 'weekly_net_eur', 'weekly_net_usd',
            'annual_net_gbp', 'annual_net_eur', 'annual_net_usd',
            'bonus_gross_gbp', 'bonus_gross_eur', 'bonus_gross_usd',
            'bonus_net_gbp', 'bonus_net_eur', 'bonus_net_usd',
            'total_gross_gbp', 'total_gross_eur', 'total_gross_usd',
            'total_net_gbp', 'total_net_eur', 'total_net_usd',
            'adjusted_total_gross_gbp', 'adjusted_total_gross_eur', 'adjusted_total_gross_usd',
            'adjusted_total_net_gbp', 'adjusted_total_net_eur', 'adjusted_total_net_usd',
            *META_COLS,
        },
        # Club-level wage totals. Positional split d/f/k/m is Pro-locked
        # upstream → intentionally not ingested (so NOT in the contract).
        'capology_team_payrolls': {
            'league', 'season', 'club_slug', 'club_name', 'club_code',
            'weekly_gross_gbp', 'weekly_gross_eur', 'weekly_gross_usd',
            'weekly_net_gbp', 'weekly_net_eur', 'weekly_net_usd',
            'annual_gross_gbp', 'annual_gross_eur', 'annual_gross_usd',
            'annual_net_gbp', 'annual_net_eur', 'annual_net_usd',
            'bonus_gross_gbp', 'bonus_gross_eur', 'bonus_gross_usd',
            'bonus_net_gbp', 'bonus_net_eur', 'bonus_net_usd',
            'total_gross_gbp', 'total_gross_eur', 'total_gross_usd',
            'total_net_gbp', 'total_net_eur', 'total_net_usd',
            'adjusted_total_gross_gbp', 'adjusted_total_gross_eur', 'adjusted_total_gross_usd',
            'adjusted_total_net_gbp', 'adjusted_total_net_eur', 'adjusted_total_net_usd',
            *META_COLS,
        },
        # Player-level contracts: signed/expiration ISO dates + years +
        # salary + full contract_total value.
        'capology_contract_extensions': {
            'league', 'season', 'player_slug', 'player_name',
            'club_slug', 'club_name', 'signed', 'expiration', 'years',
            'weekly_gross_gbp', 'weekly_gross_eur', 'weekly_gross_usd',
            'weekly_net_gbp', 'weekly_net_eur', 'weekly_net_usd',
            'annual_gross_gbp', 'annual_gross_eur', 'annual_gross_usd',
            'annual_net_gbp', 'annual_net_eur', 'annual_net_usd',
            'bonus_gross_gbp', 'bonus_gross_eur', 'bonus_gross_usd',
            'bonus_net_gbp', 'bonus_net_eur', 'bonus_net_usd',
            'total_gross_gbp', 'total_gross_eur', 'total_gross_usd',
            'total_net_gbp', 'total_net_eur', 'total_net_usd',
            'adjusted_total_gross_gbp', 'adjusted_total_gross_eur', 'adjusted_total_gross_usd',
            'adjusted_total_net_gbp', 'adjusted_total_net_eur', 'adjusted_total_net_usd',
            'contract_total_gross_gbp', 'contract_total_gross_eur', 'contract_total_gross_usd',
            'contract_total_net_gbp', 'contract_total_net_eur', 'contract_total_net_usd',
            *META_COLS,
        },
        # Club-level transfer-window net spend (balances can be negative).
        'capology_transfer_window': {
            'league', 'season', 'club_slug', 'club_name', 'club_code',
            'players', 'age', 'foreign',
            'income_gbp', 'income_eur', 'income_usd',
            'expense_gbp', 'expense_eur', 'expense_usd',
            'balance_gbp', 'balance_eur', 'balance_usd',
            'adjbalance_gbp', 'adjbalance_eur', 'adjbalance_usd',
            *META_COLS,
        },
    },
}

# Tables a source's contract names but that are intentionally NOT materialised
# (upstream restriction). Absent / empty == PASS, surfaced as "expected absent (OK)"
# instead of a missing-table failure.
EXPECTED_ABSENT: dict[str, set[str]] = {
    'fbref': {'fbref_shot_events'},  # FBref Feb-2026 shot-data restriction (#276)
}

# Source prefix → group label for the report
SOURCE_GROUPS = [
    ('fbref_', 'FBref'),
    ('fotmob_', 'FotMob'),
    ('sofascore_', 'Sofascore'),
    ('sofifa_', 'SoFIFA'),
    ('understat_', 'Understat'),
    ('whoscored_', 'WhoScored'),
    ('espn_', 'ESPN'),
    ('clubelo_', 'ClubElo'),
    ('matchhistory_', 'MatchHistory'),
]


# CLI slug -> live bronze table prefix. Slug == key in EXPECTED_TABLES.
# (SOURCE_GROUPS is label-oriented for the full-scan report; --source needs a slug.)
SOURCE_PREFIXES: dict[str, str] = {
    'fbref': 'fbref_',
    'understat': 'understat_',
    'whoscored': 'whoscored_',
    'espn': 'espn_',
    'sofascore': 'sofascore_',
    'fotmob': 'fotmob_',
    'matchhistory': 'matchhistory_',
    'clubelo': 'clubelo_',
    'sofifa': 'sofifa_',
    'transfermarkt': 'transfermarkt_',
    'capology': 'capology_',
}


def source_of(table: str) -> str:
    for prefix, label in SOURCE_GROUPS:
        if table.startswith(prefix):
            return label
    return 'Other'


# ---- Helpers ----

_PARTITIONING_RE = re.compile(r"partitioning\s*=\s*ARRAY\[(.*?)\]", re.IGNORECASE | re.DOTALL)


def get_partition_cols(cur, table: str) -> set[str]:
    cur.execute(f"SHOW CREATE TABLE iceberg.bronze.{table}")
    ddl = cur.fetchall()[0][0]
    m = _PARTITIONING_RE.search(ddl)
    if not m:
        return set()
    return {x.strip().strip("'\"") for x in m.group(1).split(',') if x.strip()}


def describe(cur, table: str) -> list[tuple[str, str]]:
    cur.execute(f"DESCRIBE iceberg.bronze.{table}")
    return [(r[0], r[1]) for r in cur.fetchall()]


def safe_alias(col: str, idx: int) -> str:
    """Produce a SQL-safe alias for arbitrary column name."""
    base = re.sub(r'[^a-zA-Z0-9_]', '_', col)
    if not base or not base[0].isalpha() and base[0] != '_':
        base = f"c{idx}_{base}"
    return base


def is_varchar(typ: str) -> bool:
    return typ.startswith('varchar')


def is_skip_distinct(typ: str) -> bool:
    """Skip distinct count on types where it's expensive or unsupported."""
    return typ.startswith('timestamp') or typ.startswith('row(') or typ.startswith('array(') or typ.startswith('map(')


# ---- Audit ----

def audit_table(cur, table: str) -> tuple[int, list[dict]]:
    """Return (total_rows, findings) for one table."""
    cols = describe(cur, table)
    if not cols:
        return 0, []
    try:
        partition_cols = get_partition_cols(cur, table)
    except Exception as e:
        print(f"  ! get_partition_cols({table}) failed: {e}", file=sys.stderr)
        partition_cols = set()

    # Build big SELECT
    select_parts = ['count(*) AS "_total"']
    plan: list[tuple[str, str, str, bool]] = []  # (col, type, alias, do_es)
    for idx, (col, typ) in enumerate(cols):
        alias = safe_alias(col, idx)
        # non-null count (always)
        select_parts.append(f'count("{col}") AS "{alias}__nn"')
        # distinct count (skip expensive types)
        if not is_skip_distinct(typ):
            select_parts.append(f'count(distinct "{col}") AS "{alias}__d"')
        # empty-string count for varchar
        do_es = is_varchar(typ)
        if do_es:
            select_parts.append(
                f'sum(CASE WHEN "{col}" = \'\' THEN 1 ELSE 0 END) AS "{alias}__es"'
            )
        plan.append((col, typ, alias, do_es))

    sql = f"SELECT {', '.join(select_parts)} FROM iceberg.bronze.{table}"
    cur.execute(sql)
    desc = [d[0] for d in cur.description]
    row = cur.fetchall()[0]
    res = dict(zip(desc, row))

    total = int(res.get('_total', 0) or 0)
    if total == 0:
        return 0, [{'table': table, 'col': '*', 'sev': 'INFO', 'detail': 'table is empty'}]

    findings: list[dict] = []
    allow_for_table = EXPECTED_NULL.get(table, set())
    allow_constant = EXPECTED_CONSTANT.get(table, set())

    for col, typ, alias, do_es in plan:
        nn = int(res.get(f'{alias}__nn', 0) or 0)
        nulls = total - nn
        null_rate = nulls / total if total else 0.0
        in_allowlist = col in allow_for_table
        is_meta = col in META_COLS

        # NULL classification
        if null_rate == 1.0:
            if in_allowlist:
                findings.append({
                    'table': table, 'col': col, 'sev': 'INFO',
                    'detail': f'EXPECTED_NULL (100% NULL by allowlist, {typ})',
                })
            else:
                findings.append({
                    'table': table, 'col': col, 'sev': 'ERROR',
                    'detail': f'ALL_NULL — 0 of {total} non-NULL ({typ})',
                })
        elif null_rate > 0.95 and not in_allowlist:
            findings.append({
                'table': table, 'col': col, 'sev': 'WARN',
                'detail': f'HIGH_NULL — null_rate={null_rate:.1%} ({nn}/{total} non-NULL, {typ})',
            })

        # Empty-string classification (varchar only)
        if do_es and nn > 0:
            es = int(res.get(f'{alias}__es', 0) or 0)
            if es == nn:
                findings.append({
                    'table': table, 'col': col, 'sev': 'ERROR',
                    'detail': f"ALL_EMPTY_STR — all {nn} non-NULL values are '' ({typ})",
                })

        # Constant classification (skip meta cols + partition cols + small tables)
        if total >= 10 and not is_meta and col not in partition_cols and not is_skip_distinct(typ):
            d = res.get(f'{alias}__d')
            if d is not None:
                d = int(d)
                if d == 1 and nn > 0:
                    if col in allow_constant:
                        findings.append({
                            'table': table, 'col': col, 'sev': 'INFO',
                            'detail': f'EXPECTED_CONSTANT (1 distinct by design across {nn} non-NULL rows, {typ})',
                        })
                    else:
                        findings.append({
                            'table': table, 'col': col, 'sev': 'WARN',
                            'detail': f'CONSTANT — only 1 distinct value across {nn} non-NULL rows ({typ})',
                        })

    return total, findings


# ---- Contract diff (--source mode) ----

def diff_contract(
    cur,
    source: str,
    live_tables: set[str],
    per_table: dict[str, tuple[int, list[dict]]],
) -> dict[str, list]:
    """Diff EXPECTED_TABLES[source] against live bronze.

    Returns four categories:
      - missing_tables: [(table, reason)]   — absent / empty / scan-failed
      - expected_absent: [(table, reason)]  — in EXPECTED_ABSENT, absent/empty == PASS
      - missing_columns: [(table, col)]     — contract col not in live DESCRIBE
      - all_null_columns: [(table, col, detail)] — ALL_NULL findings (allowlist-aware,
        reused from audit_table; spans ALL {source}_* tables, not only contract cols)
    """
    contract = EXPECTED_TABLES.get(source, {})
    absent_ok = EXPECTED_ABSENT.get(source, set())
    missing_tables: list[tuple[str, str]] = []
    expected_absent: list[tuple[str, str]] = []
    missing_columns: list[tuple[str, str]] = []
    all_null_columns: list[tuple[str, str, str]] = []

    # (a) missing tables / (b) missing columns — driven by the contract
    for table in sorted(contract):
        expected_cols = contract[table]
        in_absent_ok = table in absent_ok
        if table not in live_tables:
            if in_absent_ok:
                expected_absent.append((table, 'absent — expected (upstream restriction)'))
            else:
                missing_tables.append((table, 'absent from bronze'))
            continue
        total = per_table.get(table, (0, []))[0]
        if total == -1:
            missing_tables.append((table, 'audit scan failed'))
            continue
        if total == 0:
            if in_absent_ok:
                expected_absent.append((table, 'present but empty — expected'))
            else:
                missing_tables.append((table, 'present but empty (0 rows)'))
            continue
        live_cols = {c.lower() for c, _ in describe(cur, table)}
        for col in sorted(expected_cols):
            if col.lower() not in live_cols:
                missing_columns.append((table, col))

    # (c) all-NULL columns — reuse audit_table findings across all live {source}_*
    for table in sorted(per_table):
        _, findings = per_table[table]
        for f in findings:
            if f['sev'] == 'ERROR' and f['detail'].startswith('ALL_NULL'):
                all_null_columns.append((table, f['col'], f['detail']))

    return {
        'missing_tables': missing_tables,
        'expected_absent': expected_absent,
        'missing_columns': missing_columns,
        'all_null_columns': all_null_columns,
    }


# ---- Report ----

SEV_ORDER = {'ERROR': 0, 'WARN': 1, 'INFO': 2}


def render_report(per_table: dict[str, tuple[int, list[dict]]], output: Path) -> None:
    total_tables = len(per_table)
    err_findings = [f for _, fs in per_table.values() for f in fs if f['sev'] == 'ERROR']
    warn_findings = [f for _, fs in per_table.values() for f in fs if f['sev'] == 'WARN']
    info_findings = [f for _, fs in per_table.values() for f in fs if f['sev'] == 'INFO']
    err_tables = len({f['table'] for f in err_findings})
    warn_tables = len({f['table'] for f in warn_findings})

    today = datetime.utcnow().strftime('%Y-%m-%d')
    lines: list[str] = [
        f"# Bronze column quality audit — {today}",
        "",
        "Сканирование всех `iceberg.bronze.*` столбцов на predefined-классы мусора:",
        "**ALL_NULL** (100% NULL не в allowlist), **ALL_EMPTY_STR** (varchar где все non-NULL = `''`),",
        "**HIGH_NULL** (null_rate > 95% не в allowlist), **CONSTANT** (1 distinct value на не-partition колонке).",
        "",
        "## Summary",
        "",
        f"- Tables scanned: **{total_tables}**",
        f"- ERROR findings: **{len(err_findings)}** in **{err_tables}** table(s)",
        f"- WARN findings: **{len(warn_findings)}** in **{warn_tables}** table(s)",
        f"- INFO (allowlist hits): **{len(info_findings)}**",
        "",
    ]

    # Per-source findings (ERROR + WARN)
    lines.append("## Findings by source (ERROR + WARN)")
    lines.append("")
    by_source: dict[str, list[dict]] = defaultdict(list)
    for table, (_, fs) in per_table.items():
        for f in fs:
            if f['sev'] in ('ERROR', 'WARN'):
                by_source[source_of(table)].append(f)

    if not by_source:
        lines.append("✅ Чистый bronze — никаких ERROR/WARN.")
        lines.append("")
    else:
        for source in sorted(by_source.keys()):
            findings = sorted(by_source[source], key=lambda f: (SEV_ORDER[f['sev']], f['table'], f['col']))
            lines.append(f"### {source} ({len(findings)})")
            lines.append("")
            lines.append("| Table | Column | Severity | Detail |")
            lines.append("|---|---|---|---|")
            for f in findings:
                lines.append(f"| `{f['table']}` | `{f['col']}` | **{f['sev']}** | {f['detail']} |")
            lines.append("")

    # Per-table summary (totals + finding counts)
    lines.append("## Per-table summary")
    lines.append("")
    lines.append("| Table | Rows | ERROR | WARN | INFO |")
    lines.append("|---|---:|---:|---:|---:|")
    for table in sorted(per_table.keys()):
        total, fs = per_table[table]
        e = sum(1 for f in fs if f['sev'] == 'ERROR')
        w = sum(1 for f in fs if f['sev'] == 'WARN')
        i = sum(1 for f in fs if f['sev'] == 'INFO')
        lines.append(f"| `{table}` | {total} | {e} | {w} | {i} |")
    lines.append("")

    # Allowlist hits section
    lines.append("## Allowlist hits (expected 100% NULL)")
    lines.append("")
    if not info_findings:
        lines.append("Нет allowlist-hit'ов (значит ни одна expected-NULL колонка реально не 100% NULL — проверить!).")
        lines.append("")
    else:
        lines.append("| Table | Column | Note |")
        lines.append("|---|---|---|")
        for f in sorted(info_findings, key=lambda f: (f['table'], f['col'])):
            if f['col'] == '*':
                continue
            lines.append(f"| `{f['table']}` | `{f['col']}` | {f['detail']} |")
        lines.append("")

    # Tables that audit could not scan (DESCRIBE failed, etc.)
    lines.append("## Empty tables")
    lines.append("")
    empties = [t for t, (n, _) in per_table.items() if n == 0]
    if empties:
        for t in empties:
            lines.append(f"- `{t}`")
        lines.append("")
    else:
        lines.append("(none)")
        lines.append("")

    output.write_text('\n'.join(lines), encoding='utf-8')


def render_source_report(
    source: str,
    diff: dict[str, list],
    per_table: dict[str, tuple[int, list[dict]]],
    output: Path,
) -> None:
    """Per-source contract report: missing tables / columns / all-NULL columns."""
    missing_tables = diff['missing_tables']
    expected_absent = diff.get('expected_absent', [])
    missing_columns = diff['missing_columns']
    all_null_columns = diff['all_null_columns']
    contract = EXPECTED_TABLES.get(source, {})

    today = datetime.utcnow().strftime('%Y-%m-%d')
    lines: list[str] = [
        f"# Bronze contract audit — {source} — {today}",
        "",
        "Дифф live `iceberg.bronze.{source}_*` против контракта парсера "
        "(`EXPECTED_TABLES`).",
        "**missing table** (нет/пустая), **missing column** (ожидаемой колонки нет "
        "в DESCRIBE), **all-NULL column** (100% NULL не в allowlist).",
        "",
        "## Summary",
        "",
        f"- Contract tables: **{len(contract)}**",
        f"- Missing tables: **{len(missing_tables)}**",
        f"- Expected absent (OK): **{len(expected_absent)}**",
        f"- Missing columns: **{len(missing_columns)}**",
        f"- All-NULL columns: **{len(all_null_columns)}**",
        "",
    ]

    if not contract:
        lines.append(
            f"⚠️ Контракт для `{source}` пуст (`EXPECTED_TABLES['{source}']` "
            "не заполнен) — наполняется в per-source issue #276-#286."
        )
        lines.append("")

    # 1. Missing tables
    lines.append("## Missing tables")
    lines.append("")
    if missing_tables:
        lines.append("| Table | Reason |")
        lines.append("|---|---|")
        for table, reason in missing_tables:
            lines.append(f"| `{table}` | {reason} |")
    else:
        lines.append("(none)")
    lines.append("")

    # 1b. Expected absent (intentionally not materialised — NOT a failure)
    lines.append("## Expected absent (OK)")
    lines.append("")
    if expected_absent:
        lines.append("| Table | Reason |")
        lines.append("|---|---|")
        for table, reason in expected_absent:
            lines.append(f"| `{table}` | {reason} |")
    else:
        lines.append("(none)")
    lines.append("")

    # 2. Missing columns
    lines.append("## Missing columns")
    lines.append("")
    if missing_columns:
        lines.append("| Table | Column |")
        lines.append("|---|---|")
        for table, col in missing_columns:
            lines.append(f"| `{table}` | `{col}` |")
    else:
        lines.append("(none)")
    lines.append("")

    # 3. All-NULL columns
    lines.append("## All-NULL columns")
    lines.append("")
    if all_null_columns:
        lines.append("| Table | Column | Detail |")
        lines.append("|---|---|---|")
        for table, col, detail in all_null_columns:
            lines.append(f"| `{table}` | `{col}` | {detail} |")
    else:
        lines.append("(none)")
    lines.append("")

    # Context: all live {source}_* tables scanned
    lines.append("## Live tables scanned (context)")
    lines.append("")
    if per_table:
        lines.append("| Table | Rows | ERROR | WARN |")
        lines.append("|---|---:|---:|---:|")
        for table in sorted(per_table):
            total, fs = per_table[table]
            e = sum(1 for f in fs if f['sev'] == 'ERROR')
            w = sum(1 for f in fs if f['sev'] == 'WARN')
            lines.append(f"| `{table}` | {total} | {e} | {w} |")
    else:
        lines.append("(none — нет живых таблиц с этим префиксом)")
    lines.append("")

    output.write_text('\n'.join(lines), encoding='utf-8')


# ---- Main ----

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output', default=f'/tmp/bronze_column_audit_{datetime.utcnow():%Y-%m-%d}.md')
    p.add_argument('--source', default=None,
                   help='slug (espn, fbref, ...) — scan only {source}_* tables and '
                        'diff against the parser contract (EXPECTED_TABLES)')
    args = p.parse_args()

    source: Optional[str] = None
    if args.source:
        source = args.source.lower()
        if source not in SOURCE_PREFIXES:
            sys.exit(f"unknown source '{source}'; known: {sorted(SOURCE_PREFIXES)}")

    conn = _get_trino_connection()
    cur = conn.cursor()
    cur.execute('SHOW TABLES FROM iceberg.bronze')
    all_tables = sorted(r[0] for r in cur.fetchall())

    if source:
        prefix = SOURCE_PREFIXES[source]
        tables = [t for t in all_tables if t.startswith(prefix)]
    else:
        tables = all_tables
    print(f"Scanning {len(tables)} bronze tables...", file=sys.stderr)

    per_table: dict[str, tuple[int, list[dict]]] = {}
    for t in tables:
        try:
            total, findings = audit_table(cur, t)
            print(f"  {t}: rows={total} findings={len(findings)}", file=sys.stderr)
            per_table[t] = (total, findings)
        except Exception as e:
            print(f"  ! {t}: SCAN FAILED — {type(e).__name__}: {e}", file=sys.stderr)
            per_table[t] = (-1, [{
                'table': t, 'col': '*', 'sev': 'ERROR',
                'detail': f'audit script failed: {type(e).__name__}: {e}',
            }])

    output = Path(args.output)
    if source:
        diff = diff_contract(cur, source, set(all_tables), per_table)
        render_source_report(source, diff, per_table, output)
    else:
        render_report(per_table, output)
    print(f"\nReport written to: {output}", file=sys.stderr)


if __name__ == '__main__':
    main()
