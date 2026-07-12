"""Focused SQL contract tests for the Transfermarkt native-v2 shadow."""
from __future__ import annotations

from pathlib import Path
from datetime import date

import pytest


sqlglot = pytest.importorskip('sqlglot')
duckdb = pytest.importorskip('duckdb')


ROOT = Path(__file__).resolve().parents[3]
SILVER = ROOT / 'dags' / 'sql' / 'silver'
GOLD = ROOT / 'dags' / 'sql' / 'gold'


def _text(path: Path) -> str:
    return path.read_text(encoding='utf-8')


def _duckdb_sql(path: Path) -> str:
    rendered = sqlglot.transpile(_text(path), read='trino', write='duckdb')[0]
    return rendered.replace('iceberg.bronze.', 'bronze.').replace(
        'iceberg.silver.', 'silver.',
    ).replace('iceberg.gold.', 'gold.')


def test_global_facts_drop_scrape_league_and_season():
    mv = _text(SILVER / 'transfermarkt_market_value_points_v2.sql')
    transfers = _text(SILVER / 'transfermarkt_transfer_events_v2.sql.j2')
    select_mv = mv.split('FROM (', 1)[0]
    select_transfer = transfers.split('FROM dedup b', 1)[0].rsplit('SELECT', 1)[-1]
    assert 'b.league' not in select_mv and 'b.season' not in select_mv
    assert 'b.league' not in select_transfer
    assert 'event_season' in select_transfer


def test_global_xref_has_source_and_inverse_conflict_guards():
    sql = _text(SILVER / 'transfermarkt_player_xref_global_v2.sql')
    assert 'canonical_candidate_count' in sql
    assert 'source_player_count' in sql
    assert "'source_conflict'" in sql
    assert "'canonical_conflict'" in sql
    assert "THEN s.canonical_candidate" in sql


def test_global_xref_conflict_behavior_executes():
    con = duckdb.connect(':memory:')
    con.execute('CREATE SCHEMA bronze')
    con.execute('CREATE SCHEMA silver')
    for table in (
        'transfermarkt_squad_memberships',
        'transfermarkt_player_attribute_observations',
        'transfermarkt_market_value_points',
        'transfermarkt_transfer_events',
    ):
        con.execute(f'CREATE TABLE bronze.{table} (player_id varchar)')
    con.execute('''
        CREATE TABLE silver.xref_player (
            canonical_id varchar, source varchar, source_id varchar,
            league varchar, season varchar, confidence varchar
        )
    ''')
    players = ['resolved', 'source_conflict', 'inverse_a', 'inverse_b', 'orphan']
    con.executemany(
        'INSERT INTO bronze.transfermarkt_squad_memberships VALUES (?)',
        [(p,) for p in players],
    )
    con.executemany(
        'INSERT INTO silver.xref_player VALUES (?,?,?,?,?,?)',
        [
            ('fb_res', 'transfermarkt', 'resolved', 'ENG', '2425', 'exact'),
            ('fb_res', 'transfermarkt', 'resolved', 'ENG', '2526', 'exact'),
            ('fb_a', 'transfermarkt', 'source_conflict', 'ENG', '2425', 'exact'),
            ('fb_b', 'transfermarkt', 'source_conflict', 'ENG', '2526', 'exact'),
            ('fb_shared', 'transfermarkt', 'inverse_a', 'ENG', '2526', 'exact'),
            ('fb_shared', 'transfermarkt', 'inverse_b', 'ENG', '2526', 'exact'),
            ('tm_orphan', 'transfermarkt', 'orphan', 'ENG', '2526', 'orphan'),
        ],
    )
    rows = con.execute(
        _duckdb_sql(SILVER / 'transfermarkt_player_xref_global_v2.sql'),
    ).fetchall()
    names = [c[0] for c in con.description]
    by_id = {dict(zip(names, row))['player_id']: dict(zip(names, row)) for row in rows}
    assert by_id['resolved']['canonical_id'] == 'fb_res'
    assert by_id['resolved']['resolution_status'] == 'resolved'
    assert by_id['source_conflict']['canonical_id'] is None
    assert by_id['source_conflict']['resolution_status'] == 'source_conflict'
    assert by_id['inverse_a']['resolution_status'] == 'canonical_conflict'
    assert by_id['inverse_b']['resolution_status'] == 'canonical_conflict'
    assert by_id['orphan']['resolution_status'] == 'orphan'


def test_attribute_selector_resolves_fields_independently_without_contract_carry():
    sql = _text(SILVER / 'transfermarkt_player_attributes_v2.sql')
    for field in ('name', 'dob', 'height_cm', 'foot', 'nationality'):
        assert f'FILTER (WHERE {field} IS NOT NULL)' in sql
    assert 'l.contract_until' in sql
    assert 'COALESCE(l.contract_until' not in sql
    assert 'transfermarkt_market_value_points_v2' in sql
    assert 'COALESCE(mv.value_eur, l.market_value_eur)' in sql
    assert 'latest_scope_season' in sql
    assert 'CASE WHEN season = latest_scope_season THEN 0 ELSE 1 END' in sql


def test_late_historical_backfill_does_not_replace_current_contract_scope():
    con = duckdb.connect(':memory:')
    con.execute('CREATE SCHEMA silver')
    con.execute('''CREATE TABLE silver.transfermarkt_player_attribute_observations_v2 (
        player_id varchar, player_slug varchar, name varchar, position varchar,
        dob date, age integer, height_cm integer, foot varchar,
        nationality varchar, contract_until date, market_value_eur bigint,
        club_id varchar, club_name varchar, observed_at timestamp,
        _bronze_ingested_at timestamp, _batch_id varchar, league varchar,
        season varchar
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_market_value_points_v2 (
        player_id varchar, value_eur bigint, mv_date date,
        _bronze_ingested_at timestamp
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_player_xref_global_v2 (
        player_id varchar, canonical_id varchar, resolution_status varchar
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_player_contract_observations_v2 (
        competition_id varchar, edition_id varchar, team_id varchar,
        player_id varchar, contract_until date, observed_at timestamp,
        applicability_status varchar, _bronze_ingested_at timestamp
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_competition_editions_v2 (
        competition_id varchar, edition_id varchar, end_date date
    )''')
    con.executemany(
        'INSERT INTO silver.transfermarkt_player_attribute_observations_v2 '
        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        [
            (
                'p1', 'player', 'Player', 'Forward', date(1997, 1, 1), 29,
                180, 'right', 'England', date(2029, 6, 30), 10_000_000,
                'A', 'Current Club', '2026-06-01', '2026-06-01', 'current',
                'ENG', '2526',
            ),
            (
                'p1', 'player', 'Player', 'Forward', date(1997, 1, 1), 30,
                180, 'right', 'England', None, 8_000_000,
                'B', 'Historical Club', '2026-07-10', '2026-07-10', 'backfill',
                'ENG', '2425',
            ),
        ],
    )
    con.execute(
        "INSERT INTO silver.transfermarkt_player_xref_global_v2 "
        "VALUES ('p1', 'canonical_p1', 'resolved')"
    )
    con.executemany(
        'INSERT INTO silver.transfermarkt_competition_editions_v2 VALUES (?,?,?)',
        [
            ('GB1', '2025', date(2026, 6, 30)),
            ('GB1', '2024', date(2025, 6, 30)),
        ],
    )
    con.executemany(
        'INSERT INTO silver.transfermarkt_player_contract_observations_v2 '
        'VALUES (?,?,?,?,?,?,?,?)',
        [
            (
                'GB1', '2025', 'A', 'p1', date(2029, 6, 30),
                '2026-06-01', 'applicable', '2026-06-01',
            ),
            (
                'GB1', '2024', 'B', 'p1', None,
                '2026-07-10', 'applicable', '2026-07-10',
            ),
        ],
    )

    row = con.execute(
        _duckdb_sql(SILVER / 'transfermarkt_player_attributes_v2.sql'),
    ).fetchone()
    names = [column[0] for column in con.description]
    result = dict(zip(names, row))

    assert result['contract_until'] == date(2029, 6, 30)
    assert result['age'] == 29
    assert result['observed_club_id'] == 'A'
    assert result['observed_season'] == '2526'


def test_event_season_normalization_and_undated_upcoming_preservation():
    sql = _text(SILVER / 'transfermarkt_transfer_events_v2.sql.j2')
    assert "REPLACE(b.event_season, '/', '')" in sql
    assert 'WHEN b.transfer_date IS NOT NULL' in sql
    assert 'AND transfer_date IS NOT NULL' not in sql
    assert 'WHERE player_id IS NOT NULL' in sql
    assert 'transfer_id IS NOT NULL' in sql


def test_multi_club_assignment_uses_transfer_on_or_before_season_end():
    sql = _text(SILVER / 'transfermarkt_player_team_season_assignment_v2.sql')
    assert 'transfermarkt_competition_editions_v2' in sql
    assert 'e.transfer_date <= bounds.edition_end_date' in sql
    assert "'-06-30'" in sql
    assert "season_format = 'single_year'" in sql
    assert "'-12-31'" in sql
    assert 'FROM member_counts m' in sql
    assert 'membership_target_count = 1' in sql
    assert "THEN 'latest_transfer'" in sql
    assert "ELSE 'ambiguous'" in sql


def test_multi_club_assignment_ignores_post_season_transfer_when_executed():
    con = duckdb.connect(':memory:')
    con.execute('CREATE SCHEMA silver')
    con.execute('''CREATE TABLE silver.transfermarkt_squad_memberships_v2 (
        competition_id varchar, edition_id varchar, league varchar,
        season varchar, player_id varchar, club_id varchar, club_name varchar
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_transfer_events_v2 (
        player_id varchar, event_season varchar, transfer_date date,
        transfer_id varchar, to_club_id varchar,
        source_competition_id varchar
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_competition_editions_v2 (
        competition_id varchar, edition_id varchar, end_date date,
        season_format varchar, canonical_season varchar
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_player_xref_global_v2 (
        player_id varchar, canonical_id varchar, resolution_status varchar
    )''')
    con.execute('''CREATE TABLE silver.xref_team (
        source_id varchar, league varchar, season varchar, canonical_id varchar,
        source varchar, confidence varchar
    )''')
    memberships = [
        ('GB1', '2025', 'ENG', '2526', 'single', 'A', 'Club A'),
        ('GB1', '2025', 'ENG', '2526', 'multi', 'B', 'Club B'),
        ('GB1', '2025', 'ENG', '2526', 'multi', 'C', 'Club C'),
        ('GB1', '2025', 'ENG', '2526', 'amb', 'D', 'Club D'),
        ('GB1', '2025', 'ENG', '2526', 'amb', 'E', 'Club E'),
        ('GB1', '2025', 'ENG', '2526', 'multi_out', 'F', 'Club F'),
        ('GB1', '2025', 'ENG', '2526', 'multi_out', 'G', 'Club G'),
        ('FIWC', '2026', 'WORLD', '2026', 'single_year', 'W1', 'Team 1'),
        ('FIWC', '2026', 'WORLD', '2026', 'single_year', 'W2', 'Team 2'),
    ]
    con.executemany(
        'INSERT INTO silver.transfermarkt_squad_memberships_v2 '
        'VALUES (?,?,?,?,?,?,?)',
        memberships,
    )
    con.executemany(
        'INSERT INTO silver.transfermarkt_competition_editions_v2 '
        'VALUES (?,?,?,?,?)',
        [
            ('GB1', '2025', None, 'split_year', '2526'),
            ('FIWC', '2026', None, 'single_year', '2026'),
        ],
    )
    con.executemany(
        'INSERT INTO silver.transfermarkt_transfer_events_v2 '
        'VALUES (?,?,?,?,?,?)',
        [
            ('multi', '2526', date(2026, 1, 2), 'in_season', 'C', 'GB1'),
            ('multi', '2526', date(2026, 7, 2), 'after_end', 'B', 'GB1'),
            ('multi_out', '2526', date(2025, 8, 1), 'joined_g', 'G', 'GB1'),
            # The absolute latest in-season move leaves the membership scope.
            # It must make the assignment ambiguous, not resurrect Club G.
            (
                'multi_out', '2526', date(2026, 5, 1), 'left_league',
                'FOREIGN', 'GB1',
            ),
            (
                'single_year', '2026', date(2026, 12, 20), 'world_cup',
                'W2', 'FIWC',
            ),
            (
                'single_year', '2026', date(2027, 1, 2), 'after_world_cup',
                'W1', 'FIWC',
            ),
        ],
    )
    rows = con.execute(
        _duckdb_sql(SILVER / 'transfermarkt_player_team_season_assignment_v2.sql'),
    ).fetchall()
    names = [c[0] for c in con.description]
    by_player = {dict(zip(names, r))['player_id']: dict(zip(names, r)) for r in rows}
    assert by_player['single']['club_id'] == 'A'
    assert by_player['single']['assignment_status'] == 'single_membership'
    assert by_player['multi']['club_id'] == 'C'
    assert by_player['multi']['assignment_status'] == 'latest_transfer'
    assert by_player['amb']['club_id'] is None
    assert by_player['amb']['assignment_status'] == 'ambiguous'
    assert by_player['multi_out']['club_id'] is None
    assert by_player['multi_out']['assignment_status'] == 'ambiguous'
    assert by_player['single_year']['club_id'] == 'W2'
    assert by_player['single_year']['assignment_status'] == 'latest_transfer'
    assert by_player['single_year']['latest_transfer_date'] == date(2026, 12, 20)


def test_team_season_value_uses_exact_edition_end_and_single_year_fallback():
    sql = _text(GOLD / 'transfermarkt_team_season_market_value_v2.sql')
    assert 'transfermarkt_market_value_points_v2' in sql
    assert 'transfermarkt_competition_editions_v2' in sql
    assert 'mv.mv_date <= bounds.edition_end_date' in sql
    assert 'bounds.competition_id = a.competition_id' in sql
    assert 'bounds.edition_id = a.edition_id' in sql
    assert "season_format = 'single_year'" in sql
    assert "'-12-31'" in sql
    assert 'SUBSTR(a.season' not in sql
    assert "'-06-30'" not in sql
    assert 'ORDER BY mv.mv_date DESC' in sql
    assert "assignment_status <> 'ambiguous'" in sql
    assert 'ambiguous_players_excluded' in sql


def test_team_season_value_cutoff_executes():
    con = duckdb.connect(':memory:')
    con.execute('CREATE SCHEMA silver')
    con.execute('''CREATE TABLE silver.transfermarkt_player_team_season_assignment_v2 (
        competition_id varchar, edition_id varchar, player_id varchar,
        team_id varchar, club_id varchar, club_name varchar,
        assignment_status varchar, league varchar, season varchar
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_competition_editions_v2 (
        competition_id varchar, edition_id varchar, end_date date,
        season_format varchar, canonical_season varchar
    )''')
    con.execute('''CREATE TABLE silver.transfermarkt_market_value_points_v2 (
        player_id varchar, mv_date date, value_eur bigint,
        _bronze_ingested_at timestamp
    )''')
    con.executemany(
        'INSERT INTO silver.transfermarkt_player_team_season_assignment_v2 '
        'VALUES (?,?,?,?,?,?,?,?,?)',
        [
            (
                'GB1', '2025', 'p1', 'team_a', 'A', 'Club A',
                'single_membership', 'ENG', '2526',
            ),
            ('GB1', '2025', 'p2', None, None, None, 'ambiguous', 'ENG', '2526'),
            (
                'FIWC', '2026', 'wc1', 'world_team', 'W1', 'World Team',
                'single_membership', 'WORLD', '2026',
            ),
        ],
    )
    con.executemany(
        'INSERT INTO silver.transfermarkt_competition_editions_v2 '
        'VALUES (?,?,?,?,?)',
        [
            ('GB1', '2025', date(2026, 5, 24), 'split_year', '2526'),
            ('FIWC', '2026', date(2026, 7, 19), 'single_year', '2026'),
        ],
    )
    con.executemany(
        'INSERT INTO silver.transfermarkt_market_value_points_v2 VALUES (?,?,?,?)',
        [
            ('p1', date(2026, 5, 24), 10_000_000, '2026-05-24'),
            ('p1', date(2026, 5, 25), 20_000_000, '2026-05-25'),
            ('wc1', date(2026, 6, 1), 30_000_000, '2026-06-01'),
            ('wc1', date(2026, 7, 18), 40_000_000, '2026-07-18'),
            ('wc1', date(2026, 7, 20), 50_000_000, '2026-07-20'),
        ],
    )
    rows = con.execute(
        _duckdb_sql(GOLD / 'transfermarkt_team_season_market_value_v2.sql'),
    ).fetchall()
    names = [c[0] for c in con.description]
    by_scope = {
        (result['league'], result['season']): result
        for result in (dict(zip(names, row)) for row in rows)
    }
    assert by_scope[('ENG', '2526')]['squad_market_value_eur'] == 10_000_000
    assert by_scope[('ENG', '2526')]['ambiguous_players_excluded'] == 1
    assert by_scope[('WORLD', '2026')]['squad_market_value_eur'] == 40_000_000


def test_transfer_gold_v2_primary_key_and_upcoming_contract():
    sql = _text(GOLD / 'fct_transfer_v2.sql')
    assert 't.transfer_id' in sql
    assert 't.event_season' in sql
    assert 't.is_upcoming' in sql
    assert 'WHERE t.transfer_date IS NOT NULL' not in sql
    assert 't.league' not in sql and 't.season' not in sql


def test_manager_v2_rejects_many_coach_ids_per_canonical():
    sql = _text(GOLD / 'dim_manager_v2.sql')
    assert 'COUNT(*) OVER (PARTITION BY canonical_id)' in sql
    assert 'coach_ids_per_canonical = 1' in sql
    assert 'transfermarkt_coach_profiles_v2' in sql
    assert 'transfermarkt_coach_stints_v2' not in sql
