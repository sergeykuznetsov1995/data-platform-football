"""Physical key registry and crash/replay contracts; no source I/O."""
from datetime import date, datetime, timedelta, timezone
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from scrapers.transfermarkt import first_seen as seen
from scrapers.transfermarkt.write_intents import save_intent, unpack_frames, pending_intents
from tests.unit.utils.test_transfermarkt_current_write import MemoryDB
from tests.unit.utils.test_transfermarkt_current_write import scraper as _physical_scraper_fixture

scraper = _physical_scraper_fixture

pytestmark = pytest.mark.unit
NOW = datetime(2026, 10, 12, 12, tzinfo=timezone.utc)


@pytest.fixture
def backend(monkeypatch, tmp_path):
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.setenv('TM_WRITE_INTENT_DIR', str(tmp_path / 'intents'))
    db = MemoryDB()
    return SimpleNamespace(_bronze_connection=lambda: db, db=db)


def bundle(table, at=NOW):
    key = {
        'transfermarkt_market_value_points': {'player_id': '1', 'mv_date': date(2026, 1, 1)},
        'transfermarkt_transfer_events': {'transfer_id': 'transfer-1'},
        'transfermarkt_squad_memberships': {'competition_id': 'GB1', 'edition_id': '2026', 'club_id': '10', 'player_id': '1'},
        'transfermarkt_coach_stints': {'club_id': '10', 'coach_id': '7', 'appointed_date': date(2025, 1, 1), 'left_date': None},
    }[table]
    return pd.DataFrame([{**key, 'fetched_at': at, 'source_url': 'https://www.transfermarkt.com/proof',
                          'source_body_hash': 'a' * 64, 'cycle_id': 'cycle', '_batch_id': 'original'}])


def enrich(backend, table, frame):
    return seen.prepare_frames(backend, [SimpleNamespace(key='native', table_name=table, is_legacy=False)], {'native': frame})['native']


@pytest.mark.parametrize('table', sorted(seen.KEYS))
def test_all_four_tables_refresh_preserves_first_raw_observation(backend, table):
    first = enrich(backend, table, bundle(table))
    later = enrich(backend, table, bundle(table, NOW + timedelta(days=1)))
    assert first.first_seen_at.tolist() == later.first_seen_at.tolist() == [pd.Timestamp(NOW)]
    assert later.fetched_at[0] == NOW + timedelta(days=1)
    assert backend.db.sql.execute('SELECT COUNT(*) FROM transfermarkt_first_seen_v1').fetchone()[0] == 1
    assert not any('UPDATE ' in statement or 'DELETE ' in statement for statement in backend.db.events)


def test_fact_deleted_and_restored_cannot_reset_first_observation(backend):
    table = 'transfermarkt_market_value_points'
    enrich(backend, table, bundle(table))
    # No Bronze row is required to recover the original clock.
    restored = enrich(backend, table, bundle(table, NOW + timedelta(days=7)))
    assert restored.first_seen_at[0] == NOW


def test_durable_intent_contains_clock_and_replay_is_identical_after_49_hours(backend):
    table = 'transfermarkt_market_value_points'
    frame = enrich(backend, table, bundle(table))
    identity = {'entity': 'mv', 'scope': 'GB1/2026'}
    path = save_intent(identity, {'native': frame})
    contents = path.read_bytes()
    replay = unpack_frames(pending_intents(identity)[0][1]['frames'])['native']
    pd.testing.assert_frame_equal(enrich(backend, table, replay), frame)
    assert path.read_bytes() == contents


def test_old_captured_physical_frames_and_old_intents_are_not_enriched(backend):
    table = 'transfermarkt_market_value_points'
    frame = bundle(table)
    frame.attrs['tm_original_capture_refs'] = [{'player_id': '1', 'batch_id': 'old'}]
    pd.testing.assert_frame_equal(enrich(backend, table, frame), frame)
    assert 'first_seen_at' not in frame


def test_new_registry_requires_original_raw_lineage(backend):
    frame = bundle('transfermarkt_market_value_points').drop(columns='source_body_hash')
    with pytest.raises(RuntimeError, match='source lineage'):
        enrich(backend, 'transfermarkt_market_value_points', frame)
    assert backend.db.sql.execute('SELECT COUNT(*) FROM transfermarkt_first_seen_v1').fetchone()[0] == 0


def test_registry_collision_duplicates_and_clock_override_fail_closed(backend):
    table = 'transfermarkt_market_value_points'
    frame = enrich(backend, table, bundle(table))
    bad = frame.copy()
    bad['first_seen_at'] = NOW + timedelta(hours=1)
    with pytest.raises(RuntimeError, match='immutable first_seen_at'):
        enrich(backend, table, bad)
    backend.db.sql.execute('INSERT INTO transfermarkt_first_seen_v1 SELECT * FROM transfermarkt_first_seen_v1')
    with pytest.raises(RuntimeError, match='duplicate fact keys'):
        enrich(backend, table, frame)


def test_registry_commit_before_interrupted_readback_is_recovered(backend):
    table = 'transfermarkt_squad_memberships'
    frame = bundle(table)
    enrich(backend, table, frame)
    backend.db.fail_sql = 'SELECT key_hash, key_json, first_seen_at'
    with pytest.raises(RuntimeError, match='late physical SQL'):
        enrich(backend, table, bundle(table, NOW + timedelta(hours=1)))
    backend.db.fail_sql = None
    assert enrich(backend, table, bundle(table, NOW + timedelta(hours=2))).first_seen_at[0] == NOW


def test_registration_cannot_mutate_bronze_before_durable_intent(backend):
    table = 'transfermarkt_market_value_points'
    backend.db.sql.execute(f'CREATE TABLE {table}(player_id text, mv_date text)')
    backend.db.sql.execute(f"INSERT INTO {table} VALUES ('old', '2000-01-01')")
    enrich(backend, table, bundle(table))
    assert {row[1] for row in backend.db.sql.execute(f'PRAGMA table_info({table})')} == {'player_id', 'mv_date'}
    assert not any('ALTER TABLE iceberg.bronze' in sql for sql in backend.db.events)


def test_date_key_has_same_identity_across_dataframe_and_sql_scalar_types():
    table = 'transfermarkt_coach_stints'
    row = bundle(table).to_dict('records')[0]
    converted = {**row, 'appointed_date': pd.Timestamp(row['appointed_date']), 'left_date': pd.NaT}
    assert seen.fact_key(table, row) == seen.fact_key(table, converted)


def test_old_replay_inherits_known_clock_without_changing_journal_projection(backend):
    table = 'transfermarkt_market_value_points'
    original = bundle(table)
    output = SimpleNamespace(key='native', table_name=table, is_legacy=False)
    assert seen.inherit_registered_clocks(backend, output, original) is original
    enrich(backend, table, original)
    replay = seen.inherit_registered_clocks(backend, output, original)
    assert replay.first_seen_at[0] == NOW
    pd.testing.assert_frame_equal(replay.drop(columns='first_seen_at'), original)
    assert 'first_seen_at' not in original
    assert backend.db.sql.execute('SELECT COUNT(*) FROM transfermarkt_first_seen_v1').fetchone()[0] == 1


@pytest.mark.parametrize('name', ['standing_approval_policy.json', 'standing_backfill_policy.json'])
def test_additive_policy_hash_preserves_old_captures_without_waiving_caps(name):
    from dags.scripts.run_transfermarkt_scope_cycle import standing_policy_for_hash
    from dags.utils.transfermarkt_approval import load_standing_policy

    policy = load_standing_policy(Path('dags/configs/transfermarkt') / name)
    assert seen.REGISTRY in policy.allowed_write_tables
    prior = replace(policy, allowed_write_tables=tuple(table for table in policy.allowed_write_tables if table != seen.REGISTRY))
    restored = standing_policy_for_hash(policy, prior.policy_hash)
    assert restored == prior
    changed = replace(policy, paid_proxy=replace(policy.paid_proxy, request_limit=policy.paid_proxy.request_limit + 1))
    assert standing_policy_for_hash(changed, prior.policy_hash) is None


def test_old_scope_write_preserves_registered_clock_in_physical_bronze(scraper):
    from dags.scripts import run_transfermarkt_scraper as run

    table = 'transfermarkt_market_value_points'
    output = run.OutputSpec('native', table, replace_keys=('player_id',))
    spec = run.EntitySpec('market_value_history', 'player_id', 'players', 'market_value_points', '', '', (output,))
    original = bundle(table)
    scraper.test_db.sql.execute(f'CREATE TABLE {table}(' + ', '.join(column + ' TEXT' for column in original.columns) + ')')
    scraper.test_db.sql.execute(f"INSERT INTO {table}(player_id,mv_date) VALUES ('old', '2000-01-01')")
    scraper.test_db.sql.execute('CREATE TABLE transfermarkt_career_snapshot_anchors_v1(table_name text, capture_times_json text)')
    scraper.test_db.sql.execute('CREATE TABLE transfermarkt_career_capture_refs_v1(table_name text, refs_json text)')
    enriched = seen.prepare_frames(scraper, spec.outputs, {'native': original})['native']
    results = {'outputs': {}, 'tables': []}
    run._save_frames(scraper, spec, {'native': enriched}, True, results)
    initial = scraper.test_db.sql.execute(f"SELECT first_seen_at FROM {table} WHERE player_id='1'").fetchone()[0]
    run._save_frames(scraper, spec, {'native': original}, True, results)
    assert scraper.test_db.sql.execute(f"SELECT first_seen_at FROM {table} WHERE player_id='1'").fetchone()[0] == initial
    assert scraper.test_db.sql.execute(f"SELECT first_seen_at FROM {table} WHERE player_id='old'").fetchone()[0] is None
    assert 'first_seen_at' not in original


def test_first_seen_schema_and_physical_writer_preserve_microseconds(backend):
    from scrapers.transfermarkt.writer import TransfermarktTrinoManager, TransfermarktIcebergWriter

    stamp = NOW + timedelta(microseconds=123456)
    frame = enrich(backend, 'transfermarkt_market_value_points', bundle('transfermarkt_market_value_points', stamp))
    arrow = TransfermarktIcebergWriter()._pandas_to_arrow(frame)
    assert TransfermarktTrinoManager().arrow_schema_to_trino(arrow.schema)['first_seen_at'] == 'TIMESTAMP(6)'
    assert arrow.to_pandas().first_seen_at[0] == stamp.replace(tzinfo=None)
