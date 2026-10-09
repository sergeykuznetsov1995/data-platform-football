"""Physical current write integration using an in-memory SQL Bronze backend."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3

import pandas as pd
import pytest

from dags.utils import transfermarkt_current_write as current
from scrapers.transfermarkt import scraper as tm
from scrapers.transfermarkt.client import TransfermarktHttpClient, CurrentPortionDeadlineExceeded
from scrapers.transfermarkt.current_capture import ClubRosterSnapshot, FullRosterSnapshot
from scrapers.transfermarkt.raw_store import RawResponseStore
from scrapers.transfermarkt.registry import deterministic_scope_id
from scrapers.base.iceberg_writer import IcebergWriter
from tests.unit.scrapers.test_transfermarkt_traffic import _ClientFactory, _FakeResp, _NoWaitLimiter, _manager

pytestmark = pytest.mark.unit
NOW = datetime(2026, 10, 8, tzinfo=timezone.utc)
PREFLIGHT = {'write_mode': 'dual', 'revision': 7}
SCOPE = {'competition_id': 'GB1', 'edition_id': '2026'}


class MemoryDB:
    def __init__(self):
        self.sql = sqlite3.connect(':memory:')
        self.events = []
        self.corrupt_table = None
        self.corrupt_time = False
        self.fail_sql = None

    def cursor(self):
        return MemoryCursor(self)

    def close(self):
        pass


class MemoryCursor:
    def __init__(self, db):
        self.db = db
        self.cur = db.sql.cursor()
        self.description = None

    def execute(self, sql, params=()):
        self.db.events.append(sql)
        if self.db.fail_sql and self.db.fail_sql in sql:
            raise RuntimeError('injected late physical SQL failure')
        sql = re.sub(r'iceberg\.(?:bronze|ops)\.', '', sql).strip()
        sql = re.sub(r"CURRENT_TIMESTAMP\s*-\s*INTERVAL '([0-9]+)' DAY", r"DATE(CURRENT_TIMESTAMP, '-\1 days')", sql)
        try:
            if sql.startswith('CREATE SCHEMA') or sql.startswith('ALTER TABLE'):
                return
            if sql.startswith('MERGE INTO'):
                found = re.search(r'MERGE INTO (\w+).*?USING\s*\(\s*VALUES\s*(.*?)\)\s*s\((.*?)\)\s*ON', sql, re.S)
                assert found, sql
                table, values, columns = found.groups()
                if table in {'transfermarkt_dual_write_manifest_v2', 'transfermarkt_native_write_manifest_v2'}:
                    predicate = sql.split('ON', 1)[1].split('WHEN MATCHED', 1)[0]
                    keys = re.findall(r't\.(\w+)\s*=\s*s\.\1\b', predicate)
                    assert keys, sql
                    self.cur.execute(f'CREATE TEMP TABLE manifest_source AS SELECT * FROM {table} WHERE 0')
                    self.cur.execute(f'INSERT INTO manifest_source ({columns}) VALUES {values}')
                    self.cur.execute(f'DELETE FROM {table} WHERE EXISTS (SELECT 1 FROM manifest_source s WHERE '
                                     + ' AND '.join(f'{table}.{key} = s.{key}' for key in keys) + ')')
                    self.cur.execute(f'INSERT INTO {table} ({columns}) SELECT {columns} FROM manifest_source')
                    self.cur.execute('DROP TABLE manifest_source')
                else:
                    self.cur.execute(f'INSERT INTO {table} ({columns}) VALUES {values}')
                if table == 'transfermarkt_fetch_state':
                    self.cur.execute(f"UPDATE {table} SET last_success_at = CURRENT_TIMESTAMP WHERE status IN ('success', 'authoritative_empty')")
            else:
                sql = re.sub(r'\s+WITH \(format =.*', '', sql, flags=re.S)
                self.cur.execute(sql, tuple(params))
            self.description = self.cur.description
        except sqlite3.OperationalError as exc:
            if 'no such table' in str(exc):
                raise RuntimeError('TABLE_NOT_FOUND: ' + str(exc)) from exc
            raise

    def fetchall(self):
        rows = self.cur.fetchall()
        columns = [col[0] for col in self.cur.description or []]
        return [tuple(datetime.fromisoformat(value).date() if value and column in {'appointed_date', 'left_date', 'dob', 'mv_date'} else value
                      for column, value in zip(columns, row)) for row in rows]

    def close(self):
        self.cur.close()


class MemoryWriter(IcebergWriter):
    def __init__(self, db):
        super().__init__()
        self.db = db

    def _write_to_iceberg(self, df, database, table, partition_spec, mode='append', delete_filter=None, merge_keys=None, bulk_arrow=False):
        # Exercise the production metadata/default and Arrow conversion paths;
        # only the final storage engine is replaced with local SQL.
        df = self._pandas_to_arrow(df).to_pandas()
        cur = self.db.sql.cursor()
        columns = list(df.columns)
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table} ({", ".join(col + " TEXT" for col in columns)})')
        if delete_filter:
            self.db.events.append('DELETE ' + table)
            cur.execute(f'DELETE FROM {table} WHERE {delete_filter}')
        values = [tuple(current._cell(col, value) for col, value in zip(columns, row)) for row in df.itertuples(index=False, name=None)]
        for row in values:
            if merge_keys:
                idx = [columns.index(key) for key in merge_keys]
                cur.execute(f'DELETE FROM {table} WHERE ' + ' AND '.join(key + ' = ?' for key in merge_keys), tuple(row[i] for i in idx))
            cur.execute(f'INSERT INTO {table} ({", ".join(columns)}) VALUES ({", ".join("?" for _ in columns)})', row)
        if self.db.corrupt_table == table:
            cur.execute(f"UPDATE {table} SET source_body_hash = 'wrong-hash'")
        if self.db.corrupt_time and table == 'transfermarkt_squad_memberships':
            cur.execute(f"UPDATE {table} SET fetched_at = '2026-10-08T02:00:00.000000' WHERE club_id = '10'")
        return 'iceberg.' + database + '.' + table


@pytest.fixture
def scraper(monkeypatch, tmp_path):
    monkeypatch.delenv('TRANSFERMARKT_RAW_STORE_URI', raising=False)
    monkeypatch.setenv('TRANSFERMARKT_REQUIRE_RAW_STORE', 'false')
    monkeypatch.setenv('TM_PENDING_CHECKPOINT_DIR', str(tmp_path / 'checkpoints'))
    instance = tm.TransfermarktScraper(proxy_control_url=None, canonical_season='2627')
    db = MemoryDB()
    instance._bronze_connection = lambda: db
    instance._iceberg_writer = MemoryWriter(db)
    instance._batch_id = 'test-batch-A'
    def count(database, table, deletion, key):
        try:
            return db.sql.execute(f'SELECT COUNT(DISTINCT {key}) FROM {table} WHERE {deletion}').fetchone()[0]
        except sqlite3.OperationalError as exc:
            if 'no such table' in str(exc):
                return 0
            raise
    instance._count_existing_partition = count
    def authorize(mode, revision):
        if revision != 7:
            raise RuntimeError('persisted reader revision drift')
        return {'write_mode': mode, 'expected_revision': revision}
    monkeypatch.setattr(current.run, '_authorize_write_mode', authorize)
    instance.test_db = db
    instance.test_store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    return instance


def club(club_id, player_id, *, at=NOW, market=1000000):
    row = {'club_id': club_id, 'club_slug': 'club-' + club_id, 'current_club_name': 'Club ' + club_id,
        'player_id': player_id, 'player_slug': 'player-' + player_id, 'name': 'Player ' + player_id,
        'market_value_eur': market, 'position': 'Striker', 'dob': '2000-01-01', 'age': 26,
        'height_cm': 180, 'foot': 'right', 'nationality': 'France', 'contract_until': '2027-06-30'}
    return ClubRosterSnapshot(club_id, (row,), 'raw-' + club_id, at,
        f'https://www.transfermarkt.com/club-{club_id}/kader/verein/{club_id}/saison_id/2026/plus/1',
        'a' * 64, 'baseline-' + club_id, 'b' * 64)


def snapshot(*, changed=('10',), players=('1', '2')):
    return FullRosterSnapshot(deterministic_scope_id('GB1', '2026'), 'GB1', '2026', 2026,
        ('10', '20'), {'10': club('10', players[0], at=NOW + timedelta(hours=1)),
                       '20': club('20', players[1])}, changed)


def _table(scraper, table):
    return pd.read_sql_query('SELECT * FROM ' + table, scraper.test_db.sql)


def test_selective_roster_physically_keeps_full_fields_and_old_lineage(scraper):
    receipt = current.write_current_roster(scraper, snapshot(), PREFLIGHT, 'current-1')
    assert receipt['verified'] and receipt['business_entity'] == 'roster'
    assert receipt['full_roster_club_ids'] == ['10', '20']
    assert receipt['retained_baseline_inputs'] == {'20': 'baseline-20'}
    memberships = _table(scraper, 'transfermarkt_squad_memberships')
    assert set(memberships['club_id']) == {'10', '20'}
    assert memberships.loc[memberships.club_id == '20', 'fetched_at'].iloc[0] == '2026-10-08T00:00:00.000000'
    attrs = _table(scraper, 'transfermarkt_player_attribute_observations')
    assert attrs['club_id'].tolist() == ['10']
    legacy = _table(scraper, 'transfermarkt_players')
    assert set(legacy['player_id']) == {'1', '2'}
    for column in ('position', 'dob', 'age', 'height_cm', 'foot', 'nationality', 'contract_until', 'market_value_eur'):
        assert legacy[column].notna().all(), column
    assert len(receipt['manifests']) == 3
    assert all(item['status'] == 'success' for item in receipt['manifests'])
    assert all(output.get('physical_hash') for output in receipt['outputs'].values())


def test_multi_club_memberships_remain_two_physical_rows(scraper):
    current.write_current_roster(scraper, snapshot(players=('1', '1')), PREFLIGHT, 'current-1')
    memberships = _table(scraper, 'transfermarkt_squad_memberships')
    assert memberships.player_id.tolist() == ['1', '1']
    assert set(_table(scraper, 'transfermarkt_players')['current_club_id']) == {'10', '20'}


def test_participant_removal_updates_memberships_without_new_attribute_observation(scraper):
    baseline = snapshot(changed=('10', '20'), players=('1', '1'))
    initial = current.write_current_roster(scraper, baseline, PREFLIGHT, 'initial')
    attrs = _table(scraper, 'transfermarkt_player_attribute_observations').copy()
    retained = replace(baseline.clubs['10'], bronze_manifest=initial['bronze_manifest'])
    reduced = replace(baseline, expected_team_ids=('10',), clubs={'10': retained}, changed_club_ids=())
    scraper._current_membership_changed = True
    receipt = current.write_current_roster(scraper, reduced, PREFLIGHT, 'removed-participant')
    assert receipt['verified'] is True
    assert set(_table(scraper, 'transfermarkt_squad_memberships')['club_id']) == {'10'}
    assert set(_table(scraper, 'transfermarkt_player_contract_observations')['team_id']) == {'10'}
    assert set(_table(scraper, 'transfermarkt_players')['current_club_id']) == {'10'}
    pd.testing.assert_frame_equal(attrs, _table(scraper, 'transfermarkt_player_attribute_observations'))
    assert 'attribute_observations' not in receipt['outputs']


def test_membership_change_flag_does_not_bypass_existing_replacement_guard(scraper):
    baseline = snapshot(changed=('10', '20'))
    initial = current.write_current_roster(scraper, baseline, PREFLIGHT, 'initial')
    retained = replace(baseline.clubs['10'], bronze_manifest=initial['bronze_manifest'])
    reduced = replace(baseline, expected_team_ids=('10',), clubs={'10': retained}, changed_club_ids=())
    with pytest.raises(current.CurrentWriteError, match='updated clubs'):
        current.write_current_roster(scraper, reduced, PREFLIGHT, 'no-delta-proof')
    scraper._current_membership_changed = True
    with pytest.raises(Exception, match='guard|ratio|50|90|completeness'):
        current.write_current_roster(scraper, reduced, PREFLIGHT, 'guarded-removal')
    assert set(_table(scraper, 'transfermarkt_squad_memberships')['club_id']) == {'10', '20'}


def test_repeat_changed_club_carries_stamp_and_merges_natural_key(scraper):
    current.write_current_roster(scraper, snapshot(), PREFLIGHT, 'current-1')
    first = _table(scraper, 'transfermarkt_player_attribute_observations').iloc[0]['observed_at']
    scraper._batch_id = 'test-batch-B'
    proof = current.write_current_roster(scraper, snapshot(), PREFLIGHT, 'current-2')
    attrs = _table(scraper, 'transfermarkt_player_attribute_observations')
    assert len(attrs) == 1
    assert attrs.iloc[0]['observed_at'] == first
    assert attrs.iloc[0]['_batch_id'] == proof['outputs']['attribute_observations']['batch_id']


def test_revision_drift_stops_before_first_delete(scraper):
    with pytest.raises(RuntimeError, match='revision drift'):
        current.write_current_roster(scraper, snapshot(), {'write_mode': 'dual', 'revision': 8}, 'current-1')
    assert scraper.test_db.events == []


def test_all_output_guards_run_before_first_delete(scraper):
    baseline = current._roster_frames(scraper, snapshot(), scraper._resolve_scope('GB1', '2026'), 'baseline')
    extra = pd.concat([baseline['contract_observations']] * 3, ignore_index=True)
    extra['player_id'] = ['1', '2', '3', '4', '5', '6']
    scraper.save_to_iceberg(extra, 'transfermarkt_player_contract_observations')
    scraper.test_db.events.clear()
    with pytest.raises(Exception, match='90%'):
        current.write_current_roster(scraper, snapshot(), PREFLIGHT, 'current-1')
    assert not any(event.startswith('DELETE ') for event in scraper.test_db.events)
    assert len(_table(scraper, 'transfermarkt_player_contract_observations')) == 6


def test_lineage_readback_corruption_never_acknowledges(scraper):
    scraper.test_db.corrupt_table = 'transfermarkt_squad_memberships'
    with pytest.raises(current.CurrentWriteError, match='physical business/lineage mismatch'):
        current.write_current_roster(scraper, snapshot(), PREFLIGHT, 'current-1')
    assert len(_table(scraper, 'transfermarkt_players')) == 2
    assert not any('MERGE INTO iceberg.ops' in sql for sql in scraper.test_db.events)


def test_late_manifest_sql_failure_leaves_no_verified_receipt(scraper):
    scraper.test_db.fail_sql = "'player_contract_observations'"
    with pytest.raises(RuntimeError, match='late physical SQL failure'):
        current.write_current_roster(scraper, snapshot(), PREFLIGHT, 'current-1')
    assert len(_table(scraper, 'transfermarkt_squad_memberships')) == 2
    assert len(_table(scraper, 'transfermarkt_players')) == 2


def test_native_only_writes_no_legacy_surface(scraper):
    proof = current.write_current_roster(scraper, snapshot(), {'write_mode': 'native-only', 'revision': 7}, 'current-1')
    assert proof['verified'] and proof['write_mode'] == 'native-only'
    tables = {row[0] for row in scraper.test_db.sql.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert 'transfermarkt_players' not in tables
    assert len(proof['manifests'][0]['rows']) == 3


def http(scraper, payloads):
    factory = _ClientFactory([_FakeResp(json.dumps(payload).encode()) for payload in payloads])
    scraper._http_client = TransfermarktHttpClient(proxy_manager=_manager(3), raw_store=scraper.test_store,
        require_raw_store=True, client_factory=factory, rate_limiter=_NoWaitLimiter(), sleep_fn=lambda _: None)
    return factory


def test_career_soft_stop_commits_exact_player_and_defers_remaining_age(scraper):
    factory = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': '€1m'}]}])
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], SCOPE, PREFLIGHT, 'current-1', decoded_body_soft_stop_bytes=1)
    assert proof['verified']
    assert proof['career_window']['processed_player_ids'] == ['1']
    assert proof['career_window']['deferred_player_ids'] == ['2']
    assert len(factory.clients[0].get_calls) == 1
    assert _table(scraper, 'transfermarkt_market_value_points').player_id.tolist() == ['1']
    assert _table(scraper, 'transfermarkt_fetch_state').source_id.tolist() == ['1']


def test_career_typed_empty_physically_clears_global_player_key(scraper):
    http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': '€1m'}]}])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'current-1', decoded_body_soft_stop_bytes=100000)
    scraper._batch_id = 'empty-batch'
    http(scraper, [{'list': []}])
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'current-2', decoded_body_soft_stop_bytes=100000)
    assert proof['verified']
    assert _table(scraper, 'transfermarkt_market_value_points').empty
    assert _table(scraper, 'transfermarkt_market_value_history').empty
    assert _table(scraper, 'transfermarkt_fetch_state').iloc[-1]['status'] == 'authoritative_empty'


def test_same_day_source_timestamp_corruption_rejected(scraper):
    scraper.test_db.corrupt_time = True
    with pytest.raises(current.CurrentWriteError, match='physical business/lineage mismatch'):
        current.write_current_roster(scraper, snapshot(), PREFLIGHT, 'current-1')


def test_late_fetch_state_failure_cannot_acknowledge_career(scraper):
    http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': '€1m'}]}])
    scraper.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_fetch_state'
    with pytest.raises(current.CurrentWriteError, match='fetch-state commit failed'):
        current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'current-1', decoded_body_soft_stop_bytes=100000)
    assert len(_table(scraper, 'transfermarkt_market_value_points')) == 1


def test_career_deadline_after_one_full_player_keeps_exact_remainder(scraper):
    factory = _ClientFactory([_FakeResp(json.dumps({'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000}]}).encode()), CurrentPortionDeadlineExceeded('tail')])
    scraper._http_client = TransfermarktHttpClient(proxy_manager=_manager(3), raw_store=scraper.test_store,
        require_raw_store=True, client_factory=factory, rate_limiter=_NoWaitLimiter(), sleep_fn=lambda _: None)
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1', '2', '3'], SCOPE, PREFLIGHT, 'current-1', decoded_body_soft_stop_bytes=100000)
    assert proof['processed_player_ids'] == ['1']
    assert proof['deferred_player_ids'] == ['2', '3']
    assert _table(scraper, 'transfermarkt_fetch_state').source_id.tolist() == ['1']


def _coach_http(scraper, *, stop=False):
    from tests.unit.scrapers.test_transfermarkt_scraper import _history_html, _history_row, _coach_profile_html
    history = _history_html(_history_row('coach', '7', 'Coach', 'Jul 1, 2026', '-'))
    profile = _coach_profile_html('Coach', 'Jan 1, 1970 (56)', 'France')
    replies = [_FakeResp(history.encode()), _FakeResp(profile.encode())]
    if stop:
        replies.append(CurrentPortionDeadlineExceeded('tail'))
    factory = _ClientFactory(replies)
    scraper._http_client = TransfermarktHttpClient(proxy_manager=_manager(3), raw_store=scraper.test_store,
        require_raw_store=True, client_factory=factory, rate_limiter=_NoWaitLimiter(), sleep_fn=lambda _: None)
    return factory


def test_coaches_complete_club_before_deadline_preserve_remainder(scraper):
    current.write_current_roster(scraper, snapshot(), PREFLIGHT, 'roster-1')
    factory = _coach_http(scraper, stop=True)
    proof = current.fetch_current_coaches(scraper, SCOPE, PREFLIGHT, 'coaches-1')
    assert proof['verified'] and proof['processed_club_ids'] == ['10']
    assert proof['deferred_club_ids'] == ['20']
    assert _table(scraper, 'transfermarkt_coach_stints').club_id.tolist() == ['10']
    assert _table(scraper, 'transfermarkt_coach_profiles').nationality.tolist() == ['France']
    assert _table(scraper, 'transfermarkt_coaches').current_club_id.tolist() == ['10']
    assert len(factory.clients[0].get_calls) == 3
    assert _table(scraper, 'transfermarkt_fetch_state').source_id.tolist() == ['10']


def test_all_cached_coaches_use_old_receipts_and_full_physical_rows(scraper):
    current.write_current_roster(scraper, snapshot(), PREFLIGHT, 'roster-1')
    _coach_http(scraper)
    first = current.fetch_current_coaches(scraper, SCOPE, PREFLIGHT, 'coaches-1', clubs=['10'])
    assert first['verified']
    old_stints = _table(scraper, 'transfermarkt_coach_stints').copy()
    factory = http(scraper, [])
    cached = current.fetch_current_coaches(scraper, SCOPE, PREFLIGHT, 'coaches-2', clubs=['10'])
    assert cached['verified'] and cached['processed_club_ids'] == []
    assert cached['cached_club_ids'] == ['10']
    assert cached['cached_receipts'][0]['cached'] is True
    assert factory.clients == []
    pd.testing.assert_frame_equal(old_stints, _table(scraper, 'transfermarkt_coach_stints'))


def test_all_empty_roster_does_not_clear_old_populated_data(scraper):
    current.write_current_roster(scraper, snapshot(), PREFLIGHT, 'roster-1')
    clubs = {}
    for club_id, prior in snapshot().clubs.items():
        clubs[club_id] = replace(prior, rows=(), applicability_status='authoritative_empty',
            authoritative_empty_proof={'kind': 'typed_fetch_state', 'status': 'authoritative_empty',
                                       'raw_capture_id': prior.raw_capture_id, 'source_body_hash': prior.source_body_hash})
    empty = replace(snapshot(), clubs=clubs, changed_club_ids=('10', '20'))
    with pytest.raises(current.CurrentWriteError, match='all-empty'):
        current.write_current_roster(scraper, empty, PREFLIGHT, 'roster-2')
    assert len(_table(scraper, 'transfermarkt_squad_memberships')) == 2


def test_typed_empty_club_counts_as_covered_without_new_attribute_observation(scraper):
    original = snapshot(changed=('10', '20'), players=('1', '1'))
    first = current.write_current_roster(scraper, original, PREFLIGHT, 'roster-1')
    scraper._batch_id = 'empty-one-club'
    prior = original.clubs['10']
    empty = replace(prior, rows=(), applicability_status='authoritative_empty',
        authoritative_empty_proof={'kind': 'typed_fetch_state', 'status': 'authoritative_empty',
                                  'raw_capture_id': prior.raw_capture_id, 'source_body_hash': prior.source_body_hash})
    new = replace(original, clubs={'10': empty, '20': original.clubs['20']}, changed_club_ids=('10',))
    proof = current.write_current_roster(scraper, new, PREFLIGHT, 'roster-2')
    assert proof['verified'] and proof['authoritative_empty_club_ids'] == ['10']
    assert _table(scraper, 'transfermarkt_squad_memberships').club_id.tolist() == ['20']
    assert _table(scraper, 'transfermarkt_player_contract_observations').team_id.tolist() == ['20']
    assert set(_table(scraper, 'transfermarkt_player_attribute_observations')._batch_id) == {first['outputs']['attribute_observations']['batch_id']}
    assert 'attribute_observations' not in proof['outputs']


def test_full_transfer_career_keeps_source_season_and_clears_exact_global_key(scraper):
    payload = {'transfers': [{'id': 'source-1', 'date': 'Jul 1, 2020', 'season': '20/21',
        'from': {'clubName': 'A', 'href': '/verein/1/'}, 'to': {'clubName': 'B', 'href': '/verein/2/'},
        'fee': '€1.00m', 'marketValue': '€2.00m', 'upcoming': False}]}
    http(scraper, [payload])
    first = current.fetch_current_career(scraper, 'transfer_events', ['1'], SCOPE, PREFLIGHT, 'current-1', decoded_body_soft_stop_bytes=100000)
    assert first['verified']
    assert _table(scraper, 'transfermarkt_transfer_events').event_season.tolist() == ['2021']
    assert _table(scraper, 'transfermarkt_transfers').season.tolist() == ['2627']
    scraper._batch_id = 'empty-transfer'
    http(scraper, [{'transfers': []}])
    proof = current.fetch_current_career(scraper, 'transfer_events', ['1'], SCOPE, PREFLIGHT, 'current-2', decoded_body_soft_stop_bytes=100000)
    assert proof['verified']
    assert _table(scraper, 'transfermarkt_transfer_events').empty
    assert _table(scraper, 'transfermarkt_transfers').empty


def test_real_runner_wrapper_does_not_expand_request_budget_per_player(scraper):
    from dags.scripts import run_transfermarkt_current as lane
    from scrapers.transfermarkt.models import PRODUCTION_ENTITY_BUDGETS, TrafficBudgetExceeded
    factory = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000}]}])
    consumed = PRODUCTION_ENTITY_BUDGETS['market_value_history']['requests'] - 1
    lane._install_entity_limits(scraper, {'traffic_used_by_entity': {'market_value_points': {'requests': consumed}}})
    with pytest.raises(TrafficBudgetExceeded, match='entity budget exhausted'):
        current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], SCOPE, PREFLIGHT, 'current-1', decoded_body_soft_stop_bytes=100000)
    assert len(factory.clients[0].get_calls) == 1
    assert not any(sql.startswith('DELETE ') for sql in scraper.test_db.events)


def test_soft_stop_is_absolute_client_offset_without_double_offset(scraper):
    payload = {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000}]}
    factory = _ClientFactory([_FakeResp(b'prefix' * 20), _FakeResp(json.dumps(payload).encode())])
    scraper._http_client = TransfermarktHttpClient(proxy_manager=_manager(3), raw_store=scraper.test_store,
        require_raw_store=True, client_factory=factory, rate_limiter=_NoWaitLimiter(), sleep_fn=lambda _: None)
    scraper._http_client.fetch('https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1', as_json=False, label='listing')
    start = scraper._http_client.get_traffic_stats()['decoded_response_body_bytes']
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], SCOPE, PREFLIGHT, 'current-1', decoded_body_soft_stop_bytes=start + len(json.dumps(payload).encode()) * 3 // 4)
    assert proof['processed_player_ids'] == ['1'] and proof['deferred_player_ids'] == ['2']
    assert len(factory.clients[0].get_calls) == 2


def committed(snapshot_value, proof):
    return replace(snapshot_value, changed_club_ids=(), clubs={club_id: replace(value, bronze_manifest=proof['bronze_manifest'])
        if club_id in snapshot_value.changed_club_ids else value for club_id, value in snapshot_value.clubs.items()})


def test_roster_write_intent_replay_keeps_batch_and_attribute_keys(scraper):
    original = snapshot(changed=('10', '20'))
    scraper._current_roster_write_at = NOW
    first = current.write_current_roster(scraper, original, PREFLIGHT, 'lost-ack-cycle')
    second = current.write_current_roster(scraper, original, PREFLIGHT, 'lost-ack-cycle')
    assert first['manifest_cycle_id'] == second['manifest_cycle_id']
    assert first['outputs']['attribute_observations']['batch_id'] == second['outputs']['attribute_observations']['batch_id']
    physical = _table(scraper, 'transfermarkt_player_attribute_observations')
    assert len(physical) == len(original.rows)
    assert current.verify_current_roster_baseline(scraper, committed(original, second), [second], PREFLIGHT)['verified']


def test_two_roster_bundles_same_cycle_have_distinct_batches_and_surviving_manifests(scraper):
    first_snapshot = snapshot(changed=('10', '20'))
    first = current.write_current_roster(scraper, first_snapshot, PREFLIGHT, 'same-cycle')
    old = committed(first_snapshot, first)
    new = replace(old, changed_club_ids=('10',), clubs={**dict(old.clubs), '10': club('10', '1', at=NOW + timedelta(hours=2), market=2000000)})
    second = current.write_current_roster(scraper, new, PREFLIGHT, 'same-cycle')
    assert first['manifest_cycle_id'] != second['manifest_cycle_id']
    assert first['outputs']['attribute_observations']['batch_id'] != second['outputs']['attribute_observations']['batch_id']
    physical = _table(scraper, 'transfermarkt_player_attribute_observations')
    assert set(physical.club_id) == {'10', '20'}
    verified = current.verify_current_roster_baseline(scraper, committed(new, second), [first, second], PREFLIGHT)
    assert verified['verified']
    manifests = _table(scraper, 'transfermarkt_dual_write_manifest_v2')
    assert len(set(manifests.cycle_id)) == 2


def test_baseline_verifier_rejects_tampered_recovery_and_ops_receipts(scraper):
    original = snapshot(changed=('10', '20'))
    receipt = current.write_current_roster(scraper, original, PREFLIGHT, 'same-cycle')
    old = committed(original, receipt)
    assert current.verify_current_roster_baseline(scraper, old, [receipt], PREFLIGHT)['verified']
    changed_rows = dict(old.clubs['20'].rows[0])
    changed_rows['market_value_eur'] = 999999
    fake = replace(old, clubs={**dict(old.clubs), '20': replace(old.clubs['20'], rows=(changed_rows,))})
    with pytest.raises(current.CurrentWriteError, match='business/lineage differs'):
        current.verify_current_roster_baseline(scraper, fake, [receipt], PREFLIGHT)
    scraper.test_db.sql.execute("UPDATE transfermarkt_dual_write_manifest_v2 SET native_hash = 'bad' WHERE entity = 'squad_memberships'")
    with pytest.raises(current.CurrentWriteError, match='manifest identity differs'):
        current.verify_current_roster_baseline(scraper, old, [receipt], PREFLIGHT)


def test_baseline_verifier_detects_old_source_timestamp_changed_within_day(scraper):
    original = snapshot(changed=('10', '20'))
    receipt = current.write_current_roster(scraper, original, PREFLIGHT, 'same-cycle')
    scraper.test_db.sql.execute("UPDATE transfermarkt_player_contract_observations SET fetched_at = '2026-10-08T02:00:00.000000' WHERE team_id = '20'")
    with pytest.raises(current.CurrentWriteError, match='physical checksum differs'):
        current.verify_current_roster_baseline(scraper, committed(original, receipt), [receipt], PREFLIGHT)


def test_json_roundtrip_restores_date_arrow_schema_nullable_numbers_and_nulls(scraper):
    import pyarrow as pa
    from datetime import date
    from scrapers.base.iceberg_writer import IcebergWriter
    original = snapshot(changed=('10', '20'))
    player = dict(original.clubs['20'].rows[0])
    player.update(dob=None, contract_until=None, age=None, height_cm=None, market_value_eur=None)
    original = replace(original, clubs={**dict(original.clubs), '20': replace(original.clubs['20'], rows=(player,))})
    resolved = scraper._resolve_scope('GB1', '2026')
    cold_frames = current._roster_frames(scraper, original, resolved, 'same-cycle')
    proof = current.write_current_roster(scraper, original, PREFLIGHT, 'same-cycle')
    warm = FullRosterSnapshot.from_mapping(json.loads(json.dumps(committed(original, proof).as_dict(), default=lambda value: value.isoformat())))
    warm = replace(warm, changed_club_ids=('10', '20'))
    warm_frames = current._roster_frames(scraper, warm, resolved, 'same-cycle')
    converter = IcebergWriter()
    for key in cold_frames:
        cold = converter._pandas_to_arrow(cold_frames[key])
        replayed = converter._pandas_to_arrow(warm_frames[key])
        assert cold.schema == replayed.schema, key
        if 'dob' in replayed.schema.names:
            assert pa.types.is_date32(replayed.schema.field('dob').type)
            assert isinstance(warm_frames[key].iloc[0]['dob'], date)
        if 'contract_until' in replayed.schema.names:
            assert pa.types.is_date32(replayed.schema.field('contract_until').type)
        if 'age' in replayed.schema.names:
            assert pa.types.is_int64(replayed.schema.field('age').type)
    assert pd.isna(warm_frames['legacy_players'].iloc[1]['dob'])
    assert current.write_current_roster(scraper, warm, PREFLIGHT, 'same-cycle')['verified']


def test_retained_membership_and_contract_keep_original_observation_stamp(scraper):
    first_snapshot = snapshot(changed=('10', '20'))
    proof = current.write_current_roster(scraper, first_snapshot, PREFLIGHT, 'same-cycle')
    before_memberships = _table(scraper, 'transfermarkt_squad_memberships')
    before_contracts = _table(scraper, 'transfermarkt_player_contract_observations')
    old = committed(first_snapshot, proof)
    next_snapshot = replace(old, changed_club_ids=('10',), clubs={**dict(old.clubs), '10': club('10', '1', at=NOW + timedelta(hours=2))})
    current.write_current_roster(scraper, next_snapshot, PREFLIGHT, 'next-cycle')
    assert _table(scraper, 'transfermarkt_squad_memberships').query("club_id == '20'").observed_at.iloc[0] == before_memberships.query("club_id == '20'").observed_at.iloc[0]
    assert _table(scraper, 'transfermarkt_player_contract_observations').query("team_id == '20'").observed_at.iloc[0] == before_contracts.query("team_id == '20'").observed_at.iloc[0]


def test_native_only_warm_baseline_manifest_identity_verified(scraper):
    original = snapshot(changed=('10', '20'))
    native_preflight = {'write_mode': 'native-only', 'revision': 7}
    proof = current.write_current_roster(scraper, original, native_preflight, 'same-cycle')
    assert current.verify_current_roster_baseline(scraper, committed(original, proof), [proof], native_preflight)['verified']


def test_national_team_contracts_remain_typed_not_applicable(scraper, monkeypatch):
    from scrapers.transfermarkt.registry import TeamType
    resolve = scraper._resolve_scope
    def national(*args):
        scope = resolve(*args)
        return {**scope, 'record': replace(scope['record'], team_type=TeamType.NATIONAL_TEAM)}
    monkeypatch.setattr(scraper, '_resolve_scope', national)
    original = snapshot(changed=('10', '20'))
    proof = current.write_current_roster(scraper, original, PREFLIGHT, 'same-cycle')
    contract = next(row for item in proof['manifests'] for row in item['rows'] if row['entity'] == 'player_contract_observations')
    assert contract['applicability_status'] == 'not_applicable'
    assert proof['outputs']['contract_observations']['rows'] == 0
    assert current.verify_current_roster_baseline(scraper, committed(original, proof), [proof], PREFLIGHT)['verified']


def test_typed_empty_coach_history_is_cached_without_invented_fresh_stamps(scraper):
    from tests.unit.scrapers.test_transfermarkt_scraper import _EMPTY_STAFF_PAGE
    current.write_current_roster(scraper, snapshot(), PREFLIGHT, 'roster-1')
    factory = _ClientFactory([_FakeResp(_EMPTY_STAFF_PAGE.encode())])
    scraper._http_client = TransfermarktHttpClient(proxy_manager=_manager(3), raw_store=scraper.test_store,
        require_raw_store=True, client_factory=factory, rate_limiter=_NoWaitLimiter(), sleep_fn=lambda _: None)
    first = current.fetch_current_coaches(scraper, SCOPE, PREFLIGHT, 'coaches-1', clubs=['10'])
    assert first['verified'] and first['processed_club_ids'] == ['10']
    old = _table(scraper, 'transfermarkt_fetch_state')
    factory = http(scraper, [])
    cached = current.fetch_current_coaches(scraper, SCOPE, PREFLIGHT, 'coaches-2', clubs=['10'])
    assert cached['verified'] and cached['cached_club_ids'] == ['10']
    assert factory.clients == []
    pd.testing.assert_frame_equal(old, _table(scraper, 'transfermarkt_fetch_state'))


def _complete_scope_fixture(scraper, tmp_path):
    from dags.scripts import run_transfermarkt_scope_cycle as cycle
    from dags.utils.transfermarkt_scope_state import ScopeManifest, EntityEvidence, ddl_statements
    from tests.unit.dags.test_transfermarkt_scope_state import _manifest
    mode = {'write_mode': 'native-only', 'revision': 7}
    snap = snapshot(changed=('10', '20'), players=('1', '1'))
    proofs = {'players': current.write_current_roster(scraper, snap, mode, 'old-parent')}
    http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000}]}])
    proofs['market_value_history'] = current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, mode, 'old-parent', decoded_body_soft_stop_bytes=100000)
    http(scraper, [{'transfers': [{'id': 'source-1', 'date': 'Jul 1, 2020', 'season': '20/21',
        'from': {'clubName': 'A', 'href': '/verein/1/'}, 'to': {'clubName': 'B', 'href': '/verein/2/'},
        'fee': '€1.00m', 'marketValue': '€2.00m', 'upcoming': False}]}])
    proofs['transfers'] = current.fetch_current_career(scraper, 'transfer_events', ['1'], SCOPE, mode, 'old-parent', decoded_body_soft_stop_bytes=100000)
    from tests.unit.scrapers.test_transfermarkt_scraper import _history_html, _history_row, _coach_profile_html
    history = _history_html(_history_row('coach', '7', 'Coach', 'Jul 1, 2026', '-'))
    factory = _ClientFactory([_FakeResp(history.encode()), _FakeResp(history.encode()), _FakeResp(_coach_profile_html('Coach', 'Jan 1, 1970', 'France').encode())])
    scraper._http_client = TransfermarktHttpClient(proxy_manager=_manager(3), raw_store=scraper.test_store,
        require_raw_store=True, client_factory=factory, rate_limiter=_NoWaitLimiter(), sleep_fn=lambda _: None)
    resolved = scraper._resolve_scope('GB1', '2026')
    memberships = current._roster_frames(scraper, snap, resolved, 'old-parent')['memberships']
    coach_bundle = scraper.read_coach_data('GB1', 2026, memberships=memberships)
    coach_bundle.pop('legacy_coaches')
    spec = current.run._spec_for_write_mode(current.run.ENTITY_SPECS['coaches'], 'native-only')
    proofs['coaches'], _ = current._commit(scraper, spec, coach_bundle, resolved, 'native-only', 7, 'old-parent')
    child = 'old-complete-child'
    scraper.test_db.sql.execute("UPDATE transfermarkt_native_write_manifest_v2 SET cycle_id = ?", (child,))
    entities = []
    sources = {}
    for parser, proof in proofs.items():
        native = dict(proof['manifests'][0])
        native['cycle_id'] = child
        result = {'entity': parser, 'run_key': child, 'cycle_ledger_key': child, 'competition_id': 'GB1',
            'edition_id': '2026', 'canonical_season': '2627', 'registry_snapshot_id': 'registry-1',
            'scope_id': snap.scope_id, 'errors': [], 'fallback': False, 'native_write_complete': True,
            'write_mode': 'native-only', 'native_write_manifest_complete': True, 'native_write_manifest': native,
            'outputs': {key: {'rows': value['rows'], 'table': value['table']} for key, value in proof['outputs'].items()}}
        for key, name in cycle.ENTITY_OUTPUTS[parser]:
            row = next(row for row in native['rows'] if row['entity'] == name)
            entities.append(EntityEvidence(name, 'ok', row['native_rows'], proof['outputs'][key]['rows'],
                row['native_rows'], row['native_hash'], row['native_hash'], 'passed', 0, 0, 0, 0, 0, 0, 0))
        path = tmp_path / (parser + '-result.json')
        path.write_text(json.dumps(result))
        sources[parser] = {'result_path': str(path), 'result_sha256': current.hashlib.sha256(path.read_bytes()).hexdigest()}
    base = _manifest('GB1:2026')
    dq = dict(base.dq_evidence)
    dq['scope_capture'] = {**dq['scope_capture'], 'scope_id': snap.scope_id, 'expected_team_ids': ['10', '20'],
                           'observed_team_ids': ['10', '20'], 'endpoint_status_by_team': {'10': 'ok', '20': 'ok'}}
    dq['roster_coverage'] = {name: {'roster_size': 1, 'selected': 1, 'pending': 0} for name in ('market_value_history', 'transfers')}
    manifest = replace(base, scope_id=snap.scope_id, child_cycle_id=child, canonical_competition_id=resolved['compatibility_league'],
                       canonical_season='2627', entities=tuple(entities), dq_evidence=dq)
    manifest.validate(cycle.EXPECTED_ENTITIES)
    for ddl in ddl_statements():
        current.run._execute_cursor(scraper.test_db, ddl)
    values = manifest.as_dict()
    entity_json = json.dumps({'entities': values.pop('entities'), 'dq_evidence': values.pop('dq_evidence')})
    values.update(entity_manifest_json=entity_json, manifest_digest=manifest.digest, status='complete', committed_at='2026-10-08T04:00:00')
    columns = list(values)
    scraper.test_db.sql.execute(f'INSERT INTO transfermarkt_scope_manifest_v2 ({", ".join(columns)}) VALUES ({", ".join("?" for _ in columns)})', tuple(values.values()))
    entry = {'scope_manifest': manifest.as_dict(), 'snapshot': snap.as_dict(), 'scope_sources': sources}
    return manifest, entry, mode


def test_previous_complete_scope_bridge_rehashes_all_seven_outputs_read_only(scraper, tmp_path):
    manifest, entry, mode = _complete_scope_fixture(scraper, tmp_path)
    scraper.test_db.events.clear()
    proof = current.verify_current_complete_scope(scraper, manifest, entry, mode)
    assert proof['verified'] and proof['bronze_manifest'] == manifest.digest
    assert proof['committed_at'] == '2026-10-08T04:00:00+00:00'
    assert len(proof['outputs']) == 7
    assert not any(sql.startswith(('MERGE', 'DELETE', 'CREATE', 'INSERT', 'ALTER')) for sql in scraper.test_db.events)
    scraper.test_db.sql.execute("UPDATE transfermarkt_market_value_points SET value_eur = '9999'")
    with pytest.raises(Exception, match='live Bronze batch changed'):
        current.verify_current_complete_scope(scraper, manifest, entry, mode)


def test_complete_scope_bridge_rejects_changed_original_result_and_ops_payload(scraper, tmp_path):
    manifest, entry, mode = _complete_scope_fixture(scraper, tmp_path)
    source = entry['scope_sources']['players']
    from pathlib import Path
    path = Path(source['result_path'])
    original = path.read_bytes()
    path.write_bytes(original + b' ')
    with pytest.raises(current.CurrentWriteError, match='result file changed'):
        current.verify_current_complete_scope(scraper, manifest, entry, mode)
    path.write_bytes(original)
    scraper.test_db.sql.execute("UPDATE transfermarkt_scope_manifest_v2 SET canonical_season = '2526'")
    with pytest.raises(current.CurrentWriteError, match='ops payload differs'):
        current.verify_current_complete_scope(scraper, manifest, entry, mode)


def test_current_sql_bound_rechecks_each_real_trino_http_poll_preserving_auth():
    import requests
    import trino.dbapi
    from dags.scripts.run_transfermarkt_current import CurrentPortionError
    clock = [0.0]
    calls = []
    session = requests.Session()
    session.auth = ('fixture-user', 'fixture-password')
    def request(method, url, **kwargs):
        calls.append((method, kwargs['timeout']))
        clock[0] += 4
        response = requests.Response()
        response.status_code = 200
        response.headers['Content-Type'] = 'application/json'
        response._content = json.dumps({'id': 'fixture-query', 'infoUri': 'http://offline.invalid/info',
            'nextUri': 'http://offline.invalid/next', 'stats': {}, 'warnings': []}).encode()
        return response
    session.request = request
    connection = trino.dbapi.Connection('offline.invalid', http_session=session, request_timeout=30, max_attempts=1)
    current.bind_current_connection_deadline(connection, 40, lambda: clock[0])
    with pytest.raises(CurrentPortionError, match='another Trino query poll'):
        connection.cursor().execute('SELECT 1').fetchall()
    assert calls == [('POST', 10), ('GET', 6), ('GET', 2)]
    assert session.auth == ('fixture-user', 'fixture-password')


def test_current_iceberg_manager_retries_scoped_and_restored(scraper):
    from scrapers.base.trino_manager import TrinoTableManager
    from types import SimpleNamespace
    manager = TrinoTableManager(host='offline.invalid')
    created = []
    def factory():
        connection = SimpleNamespace(request_timeout=30, max_attempts=3)
        created.append(connection)
        return connection
    manager._create_connection = factory
    scraper._iceberg_writer._trino_manager = manager
    scraper._current_deadline_monotonic = 100
    scraper._current_clock = lambda: 0
    original = manager._create_connection
    with current._current_writer_deadline(scraper):
        assert manager._CONNECT_RETRIES == manager._COMMIT_RETRIES == 1
        assert manager._create_connection().max_attempts == 1
        assert created[0].request_timeout == 30
    assert manager._create_connection is original
    assert manager._CONNECT_RETRIES == 7 and manager._COMMIT_RETRIES == 5
