"""Native-v2 DQ contract for canonical ``gold.fct_transfer``."""

from __future__ import annotations

import inspect
import sys
from pathlib import Path

import duckdb
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = PROJECT_ROOT / 'dags'
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from utils import gold_tasks  # noqa: E402


pytestmark = pytest.mark.unit


def _transfer_checks():
    return gold_tasks._fct_transfer_quality_checks()


def test_transfer_has_pk_and_semantic_identity_uniqueness_guards():
    checks = [
        check for check in _transfer_checks()
        if check.kind == 'no_duplicates'
        and check.params['table'] == 'gold.fct_transfer'
    ]

    by_name = {check.name: check for check in checks}
    assert set(by_name) == {
        'no_duplicates[gold.fct_transfer(transfer_id)]',
        'semantic_duplicates[fct_transfer_event_identity]',
    }
    assert by_name[
        'no_duplicates[gold.fct_transfer(transfer_id)]'
    ].params['pk'] == ['transfer_id']
    assert by_name[
        'semantic_duplicates[fct_transfer_event_identity]'
    ].params['pk'] == [
        'player_id', 'transfer_date', 'event_season',
        'from_team_id', 'to_team_id',
    ]


def test_semantic_guard_catches_bootstrap_and_runtime_ids_for_same_event():
    from utils import data_quality

    con = duckdb.connect(':memory:')
    con.execute("ATTACH ':memory:' AS iceberg")
    con.execute('CREATE SCHEMA iceberg.gold')
    con.execute('''
        CREATE TABLE iceberg.gold.fct_transfer (
            transfer_id VARCHAR,
            player_id VARCHAR,
            transfer_date DATE,
            event_season VARCHAR,
            from_team_id VARCHAR,
            to_team_id VARCHAR
        )
    ''')
    con.execute("""
        INSERT INTO iceberg.gold.fct_transfer VALUES
        ('bootstrap_hash', 'p1', DATE '2025-07-01', '2526', 'a', 'b'),
        ('ceapi_source_id', 'p1', DATE '2025-07-01', '2526', 'a', 'b')
    """)
    check = next(
        item for item in _transfer_checks()
        if item.name == 'semantic_duplicates[fct_transfer_event_identity]'
    )
    try:
        result = data_quality._run_no_duplicates(con, check)
        assert result['passed'] is False
        assert result['value'] == 1
    finally:
        con.close()


def test_upcoming_events_do_not_require_date_or_club_sides_globally():
    checks = [
        check for check in _transfer_checks()
        if check.kind == 'no_nulls'
        and check.params['table'] == 'gold.fct_transfer'
    ]

    assert len(checks) == 1
    required = set(checks[0].params['cols'])
    assert required == {'transfer_id', 'player_id', 'is_loan', 'is_upcoming'}
    assert required.isdisjoint({
        'transfer_date', 'event_season', 'from_team_id', 'to_team_id',
    })
    assert checks[0].params['where'] is None


def test_transfer_uses_nullable_event_season_fk_not_scrape_partition_fks():
    checks = gold_tasks._star_gate_league_season_fk_checks()
    transfer_fks = [
        check for check in checks
        if check.kind == 'ref_integrity'
        and check.params['child'] == 'gold.fct_transfer'
    ]

    assert 'gold.fct_transfer' not in gold_tasks._STAR_FACT_TABLES
    assert len(transfer_fks) == 1
    event_season = transfer_fks[0]
    assert event_season.params['key'] == 'event_season'
    assert event_season.params['parent'] == 'gold.dim_season'
    assert event_season.params['parent_key'] == 'season'
    assert 'MIN(season)' in event_season.params['where']
    assert 'MAX(season)' in event_season.params['where']
    assert event_season.severity == 'ERROR'


def test_historical_event_season_outside_dim_window_is_valid():
    """Global career history predating dim_season must not fail the FK."""
    from utils import data_quality

    con = duckdb.connect(':memory:')
    con.execute("ATTACH ':memory:' AS iceberg")
    con.execute('CREATE SCHEMA iceberg.gold')
    con.execute('CREATE TABLE iceberg.gold.dim_season (season VARCHAR)')
    con.execute(
        "INSERT INTO iceberg.gold.dim_season VALUES ('1617'), ('2526')",
    )
    con.execute(
        'CREATE TABLE iceberg.gold.fct_transfer (event_season VARCHAR)',
    )
    con.execute(
        "INSERT INTO iceberg.gold.fct_transfer VALUES ('1011'), ('1617')",
    )

    check = next(
        check for check in gold_tasks._star_gate_league_season_fk_checks()
        if check.kind == 'ref_integrity'
        and check.params['child'] == 'gold.fct_transfer'
    )
    try:
        result = data_quality._run_ref_integrity(con, check)
        assert result['passed'] is True

        # An unknown season inside the configured window is still an error.
        con.execute("INSERT INTO iceberg.gold.fct_transfer VALUES ('2020')")
        result = data_quality._run_ref_integrity(con, check)
        assert result['passed'] is False
        assert result['value'] == 1
    finally:
        con.close()


def test_out_of_window_seasons_are_reported_without_becoming_a_gate():
    checks = gold_tasks._star_gate_league_season_fk_checks()
    observability = [
        check for check in checks
        if check.name == (
            'star_observability[fct_transfer.event_season out-of-window]'
        )
    ]

    assert len(observability) == 1
    assert observability[0].kind == 'row_count'
    assert observability[0].params['min_rows'] == 0
    assert observability[0].params['max_rows'] is None
    assert observability[0].severity == 'WARNING'


def test_existing_transfer_fk_warning_thresholds_are_unchanged():
    checks = [
        check for check in _transfer_checks()
        if check.kind == 'ref_integrity'
    ]
    by_key = {check.params['key']: check for check in checks}

    assert set(by_key) == {'player_id', 'from_team_id', 'to_team_id'}
    assert by_key['player_id'].params['warn_rate'] == 0.55
    assert by_key['from_team_id'].params['warn_rate'] == 0.90
    assert by_key['to_team_id'].params['warn_rate'] == 0.90
    assert all(check.params['error_rate'] is None for check in checks)
    assert all(check.severity == 'WARNING' for check in checks)


def test_transfer_row_floor_remains_500():
    source = inspect.getsource(gold_tasks.validate_gold_row_counts)
    assert "CHECK.row_count('gold.fct_transfer',  min_rows=500)" in source
