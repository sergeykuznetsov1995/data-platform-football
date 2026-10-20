"""Offline regression of global career selection and physical observation writes."""
from datetime import datetime, timedelta, timezone
from dataclasses import replace
import re

import pytest

from dags.scripts import run_transfermarkt_scraper as run
from dags.utils.transfermarkt_career_debt import career_debt_ctes
from dags.utils import transfermarkt_bronze_dq as dq
from dags.utils import transfermarkt_scope_state as state
from tests.unit.utils.test_transfermarkt_current_write import (
    scraper, snapshot, club, _table, current, NOW,
)

pytestmark = pytest.mark.unit


def select(scraper, endpoint='transfers', *, cycle='next', league='ENG-Premier League'):
    return run._select_player_ids(scraper, run.ENTITY_SPECS[endpoint], league, 2026,
        500, 0, 'current', cycle, True, legacy_materialization_required=False)


@pytest.mark.parametrize('endpoint', ['transfers', 'market_value_history'])
def test_roster_debt_1200_is_paid_only_on_commit_across_run_keys(scraper, monkeypatch, endpoint):
    roster = [str(i) for i in range(1, 1201)]
    monkeypatch.setattr(scraper, '_resolve_player_ids_from_bronze', lambda *a, **k: roster)
    spec = run.ENTITY_SPECS[endpoint]
    for cycle, expected in enumerate((700, 200, 0)):
        ids, _, _, _, coverage = select(scraper, endpoint, cycle=str(cycle))
        assert coverage['pending'] == len(roster) - cycle * 500
        assert len(ids) == min(500, coverage['pending'])
        rows = [('success', 1, None, None) for _ in ids]
        # Until the actual checkpoint of the committed data, selection alone
        # must not decrease the outstanding debt.
        result = {'roster_coverage': coverage}
        assert run._commit_checkpoint_or_pending(scraper, spec, ids, rows,
            str(cycle), 'ENG-Premier League', 2026) == 'success'
        run._commit_career_coverage(result, ids, rows)
        assert coverage['pending'] == expected
    # The same global player keys in another scope are already paid.
    ids, hits, _, _, coverage = select(scraper, endpoint, cycle='new-day', league='ESP-La Liga')
    assert ids == [] and hits == 1200 and coverage['pending'] == 0


@pytest.mark.parametrize('status', ['success', 'authoritative_empty', 'failed', 'schema_error', 'retry_exhausted'])
@pytest.mark.parametrize('age,changed', [(1, False), (8, False), (1, True)])
def test_ttl_signals_and_failed_keys(scraper, monkeypatch, status, age, changed):
    monkeypatch.setattr(scraper, '_resolve_player_ids_from_bronze', lambda *a, **k: ['1'])
    at = datetime.now(timezone.utc) - timedelta(days=age)
    spec = run.ENTITY_SPECS['transfers']
    assert run._persist_fetch_state(scraper, spec, ['1'], [(status, 1, None, None)],
        'old', committed_at_by_id={'1': at})
    conn = scraper.test_db.sql
    if status not in {'success', 'authoritative_empty'}:
        # MemoryCursor's MERGE adapter uses captured_at for every inserted row.
        conn.execute('UPDATE transfermarkt_fetch_state SET last_success_at = NULL')
    conn.execute('CREATE TABLE transfermarkt_current_signals_v1 '
        '(entity_id text, entity text, measurement_kind text, first_detected_at text)')
    # A cold observation from a second scope is never a new global change.
    conn.execute('INSERT INTO transfermarkt_current_signals_v1 VALUES (?, ?, ?, ?)',
        ('1', 'player', 'change' if changed else 'cold', datetime.now(timezone.utc).isoformat()))
    ids, _, _, _, coverage = select(scraper)
    successful = status in {'success', 'authoritative_empty'}
    assert ids == ([] if successful and age < 7 and not changed else ['1'])
    assert coverage['pending'] == (0 if successful else 1)
    if successful and age >= 7:
        assert coverage['refresh_pending'] == 0  # admitted refresh, not initial debt


def test_checkpoint_outage_recovers_globally_without_reselecting(scraper, monkeypatch):
    monkeypatch.setattr(scraper, '_resolve_player_ids_from_bronze', lambda *a, **k: ['1'])
    spec = run.ENTITY_SPECS['transfers']
    ids, _, _, _, coverage = select(scraper)
    assert coverage['pending'] == 1
    # Failed data/manifest commit never calls the post-commit checkpoint.
    assert select(scraper)[0] == ['1']
    original = run._persist_fetch_state
    monkeypatch.setattr(run, '_persist_fetch_state', lambda *a, **k: False)
    assert run._commit_checkpoint_or_pending(scraper, spec, ids, [('success', 1, None, None)],
        'committed', 'ENG-Premier League', 2026) == 'committed_checkpoint_pending'
    monkeypatch.setattr(run, '_persist_fetch_state', original)
    run._reconcile_pending_fetch_state(scraper)
    assert select(scraper, league='ESP-La Liga')[0] == []


def test_old_parser_errors_cannot_be_hidden_by_incompatible_bootstrap(scraper, monkeypatch):
    spec = run.ENTITY_SPECS['market_value_history']
    monkeypatch.setattr(scraper, '_resolve_player_ids_from_bronze', lambda *a, **k: ['1'])
    run._ensure_fetch_state(scraper.test_db)
    db = scraper.test_db.sql
    for status in ('failed', 'schema_error', 'retry_exhausted'):
        db.execute('INSERT INTO transfermarkt_fetch_state (endpoint, source_id, parser_version, schema_version, status) '
            'VALUES (?, ?, ?, ?, ?)', ('market_value_points', '1', 'old-' + status, 'old', status))
    db.execute('CREATE TABLE transfermarkt_market_value_points '
        '(player_id text, mv_date text, _ingested_at text, _batch_id text, parser_revision text, schema_revision text)')
    db.execute('INSERT INTO transfermarkt_market_value_points VALUES '
        "('1', '2020-01-01', '2026-10-01', 'bootstrap', 'old', 'old')")
    db.execute('CREATE TABLE transfermarkt_market_value_history '
        '(player_id text, league text, season text, mv_date text, _batch_id text)')
    db.execute('INSERT INTO transfermarkt_market_value_history VALUES '
        "('1', 'ENG-Premier League', '2627', '2020-01-01', 'bootstrap')")
    derived = run._load_data_derived_state(scraper, spec.state_endpoint, 'ENG-Premier League', 2026, ['1'])
    assert derived == {}
    assert select(scraper, 'market_value_history')[0] == ['1']


def test_scope_writer_merges_unchanged_attributes_but_preserves_a_b_a(scraper):
    spec = run._spec_for_write_mode(run.ENTITY_SPECS['players'], 'native-only')
    for index, market in enumerate((1000000, 1000000, 2000000, 1000000)):
        shot = snapshot(changed=('10', '20'))
        shot = replace(shot, clubs={key: club(key, value.rows[0]['player_id'],
            at=NOW + timedelta(days=index), market=market if key == '10' else 1000000)
            for key, value in shot.clubs.items()})
        scope = current._scope(scraper, shot)
        scraper._batch_id = 'batch-' + str(index)
        frames = current._roster_frames(scraper, shot, scope, 'cycle-' + str(index))
        frames.pop('legacy_players')
        result = {'outputs': {}, 'tables': []}
        frames = run._carry_forward_observed_at(scraper, spec, frames, result)
        from scrapers.transfermarkt.first_seen import prepare_frames
        frames = prepare_frames(scraper, spec.outputs, frames)
        run._save_frames(scraper, spec, frames, False, result)
        attrs = _table(scraper, 'transfermarkt_player_attribute_observations')
        assert len(attrs) == (2, 2, 3, 4)[index]
    assert attrs[attrs.player_id == '1'].market_value_eur.astype(int).tolist() == [1000000, 2000000, 1000000]


def test_dq_counts_unique_players_missing_either_endpoint(scraper):
    db = scraper.test_db.sql
    run._ensure_fetch_state(scraper.test_db)
    db.execute('CREATE TABLE transfermarkt_squad_memberships (competition_id text, edition_id text, player_id text)')
    db.executemany('INSERT INTO transfermarkt_squad_memberships VALUES (?, ?, ?)',
        [('GB1', '2026', '1'), ('ES1', '2026', '1'), ('GB1', '2026', '2')])
    db.execute('CREATE TABLE transfermarkt_scope_manifest_v2 (competition_id text, edition_id text, '
        'entity_manifest_json text, committed_at text, status text)')
    # Execute the actual query through a small Trino-to-SQLite adapter.
    query = dq.build_career_debt_sql()
    query = re.sub(r'iceberg\.(?:ops|bronze)\.', '', query)
    query = query.replace('TRY_CAST', 'CAST').replace('COUNT_IF(pending IS NULL)', 'SUM(pending IS NULL)')
    db.create_function('json_extract_scalar', 2, lambda value, path: 0)
    for comp in ('GB1', 'ES1'):
        db.execute('INSERT INTO transfermarkt_scope_manifest_v2 VALUES (?, ?, ?, ?, ?)',
            (comp, '2026', '{}', '2026-10-20', state.SCOPE_COMPLETION_STATUS))
    assert db.execute(query).fetchone() == (2, 0, 2, 2)
    for endpoint in ('transfers', 'market_value_history'):
        assert run._persist_fetch_state(scraper, run.ENTITY_SPECS[endpoint], ['1'],
            [('authoritative_empty', 0, None, None)], 'commit')
    assert db.execute(query).fetchone() == (1, 0, 2, 2)
    # Failed refresh does not make a previously successful player initial debt.
    assert 'last_success_at IS NOT NULL' in career_debt_ctes()


def test_manifest_counts_player_once_across_endpoints():
    item = {'roster_size': 2, 'selected': 0, 'pending': 1,
        'policy_version': 'player-endpoint-v1', 'debt_player_ids': ['1'], 'refresh_pending': 0}
    state.ScopeManifest._validate_roster_coverage(None, {
        'roster_coverage': {'transfers': dict(item), 'market_value_history': dict(item)},
        'career_fetches_pending': 1})
    with pytest.raises(state.ScopeManifestError):
        state.ScopeManifest._validate_roster_coverage(None, {
            'roster_coverage': {'transfers': dict(item, pending=0)}, 'career_fetches_pending': 0})


@pytest.mark.parametrize('mode', ['dual', 'native-only'])
@pytest.mark.parametrize('empty', [False, True])
@pytest.mark.parametrize('force', [False, True])
def test_current_writer_reuses_global_full_receipt_in_another_scope(scraper, mode, empty, force):
    from tests.unit.utils.test_transfermarkt_current_write import http
    payload = {'list': [] if empty else [
        {'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': '€1m'}]}
    preflight = {'write_mode': mode, 'revision': 7}
    scraper._current_career_signal_generations = {'1': 'scope-a-generation'}
    http(scraper, [payload])
    first = current.fetch_current_career(scraper, 'market_value_points', ['1'],
        {'competition_id': 'GB1', 'edition_id': '2026'}, preflight, 'scope-a', decoded_body_soft_stop_bytes=100000)
    assert first['verified']
    stamp = _table(scraper, 'transfermarkt_fetch_state').iloc[0].last_success_at
    scraper._current_career_signal_generations = {'1': 'scope-b-generation'}
    scraper._current_career_force_ids = {'1'} if force else set()
    factory = http(scraper, [payload] if force else [])
    second = current.fetch_current_career(scraper, 'market_value_points', ['1'],
        {'competition_id': 'ES1', 'edition_id': '2026'}, preflight, 'scope-b', decoded_body_soft_stop_bytes=100000)
    assert second['verified'] and second['processed_player_ids'] == ['1']
    if force:
        assert len(factory.clients[0].get_calls) == 1
        assert 'cached_capture_ids' not in second
    else:
        assert factory.clients == []
        assert second['cached_capture_ids'].keys() == {'1'}
        assert _table(scraper, 'transfermarkt_fetch_state').iloc[0].last_success_at == stamp
