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
from scrapers.transfermarkt import writer as tm_writer
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
        self.next_snapshot_id = 1
        self.snapshots = {}

    def commit_snapshot(self, table):
        snapshot = self.next_snapshot_id
        self.next_snapshot_id += 1
        prior = self.snapshots.get(table, [])
        self.sql.execute(f'CREATE TABLE {table}__snapshot_{snapshot} AS SELECT * FROM {table}')
        self.snapshots.setdefault(table, []).append((snapshot, prior[-1][0] if prior else None,
            datetime.now(timezone.utc).replace(tzinfo=None).isoformat()))

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
        if '$snapshots' in sql:
            target = re.search(r'FROM "?(\w+)\$snapshots', sql).group(1)
            if not self.db.sql.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (target,)).fetchone():
                raise RuntimeError('TABLE_NOT_FOUND: ' + target)
            self.cur.execute('DROP TABLE IF EXISTS snapshot_metadata')
            self.cur.execute('CREATE TEMP TABLE snapshot_metadata(snapshot_id bigint, parent_id bigint, committed_at text)')
            self.cur.executemany('INSERT INTO snapshot_metadata VALUES (?, ?, ?)', self.db.snapshots.get(target, []))
            sql = re.sub(r'"?\w+\$snapshots"?', 'snapshot_metadata', sql)
            self.cur.execute(sql, tuple(params))
            self.description = self.cur.description
            return
        if 'CROSS JOIN UNNEST(CAST(JSON_PARSE(a.capture_times_json)' in sql:
            placeholders = ', '.join('?' for _ in params[1:])
            self.cur.execute('SELECT j.key source_id, MAX(j.value) FROM transfermarkt_career_snapshot_anchors_v1 a '
                f'JOIN json_each(a.capture_times_json) j WHERE a.table_name=? AND j.key IN ({placeholders}) GROUP BY j.key', tuple(params))
            self.description = self.cur.description
            return
        sql = re.sub(r'(\w+) FOR VERSION AS OF (\d+)', r'\1__snapshot_\2', sql)

        sql = re.sub(r"TIMESTAMP '([^']*)'", r"'\1'", sql)
        sql = re.sub(r"CURRENT_TIMESTAMP\s*-\s*INTERVAL '([0-9]+)' DAY", r"DATE(CURRENT_TIMESTAMP, '-\1 days')", sql)
        try:
            if sql.startswith('CREATE SCHEMA') or sql.startswith('ALTER TABLE'):
                return
            if sql.startswith('MERGE INTO'):
                found = re.search(r'MERGE INTO (\w+).*?USING\s*\(\s*VALUES\s*(.*?)\)\s*s\((.*?)\)\s*ON', sql, re.S)
                assert found, sql
                table, values, columns = found.groups()
                if table == 'transfermarkt_fetch_state':
                    self.cur.execute(f'CREATE TEMP TABLE fetch_source ({columns})')
                    self.cur.execute(f'INSERT INTO fetch_source VALUES {values}')
                    self.cur.execute(f'DELETE FROM {table} WHERE EXISTS (SELECT 1 FROM fetch_source s WHERE '
                        f'{table}.endpoint=s.endpoint AND {table}.source_id=s.source_id '
                        f'AND {table}.parser_version=s.parser_version AND {table}.schema_version=s.schema_version '
                        f'AND ({table}.last_attempt_at IS NULL OR s.captured_at >= {table}.last_attempt_at))')
                    target_cols = columns.replace(', captured_at', '').replace(',\n    captured_at', '')
                    self.cur.execute(f'INSERT INTO {table} ({target_cols}, first_attempt_at, last_attempt_at, last_success_at) '
                        f'SELECT {target_cols}, captured_at, captured_at, captured_at FROM fetch_source s WHERE NOT EXISTS '
                        f'(SELECT 1 FROM {table} t WHERE t.endpoint=s.endpoint AND t.source_id=s.source_id '
                        f'AND t.parser_version=s.parser_version AND t.schema_version=s.schema_version)')
                    self.cur.execute('DROP TABLE fetch_source')
                elif table in {'transfermarkt_dual_write_manifest_v2', 'transfermarkt_native_write_manifest_v2', 'transfermarkt_career_capture_refs_v1', 'transfermarkt_career_snapshot_anchors_v1', 'transfermarkt_proxy_ledger_events_v1'}:
                    predicate = sql.split('ON', 1)[1].split('WHEN MATCHED', 1)[0]
                    keys = re.findall(r't\.(\w+)\s*=\s*s\.\1\b', predicate)
                    assert keys, sql
                    self.cur.execute(f'CREATE TEMP TABLE manifest_source AS SELECT * FROM {table} WHERE 0')
                    self.cur.execute(f'INSERT INTO manifest_source ({columns}) VALUES {values}')
                    match = ' AND '.join(f'{table}.{key} = s.{key}' for key in keys)
                    if 'WHEN MATCHED' in sql:
                        self.cur.execute(f'DELETE FROM {table} WHERE EXISTS (SELECT 1 FROM manifest_source s WHERE ' + match + ')')
                        self.cur.execute(f'INSERT INTO {table} ({columns}) SELECT {columns} FROM manifest_source')
                    else:
                        source_cols = ', '.join('s.' + col.strip() for col in columns.split(','))
                        self.cur.execute(f'INSERT INTO {table} ({columns}) SELECT {source_cols} FROM manifest_source s WHERE NOT EXISTS '
                            f'(SELECT 1 FROM {table} WHERE ' + match + ')')
                    self.cur.execute('DROP TABLE manifest_source')
                else:
                    self.cur.execute(f'INSERT INTO {table} ({columns}) VALUES {values}')

            else:
                sql = re.sub(r'\s+WITH \(format =.*', '', sql, flags=re.S)
                self.cur.execute(sql, tuple(params))
                delete = re.match(r'DELETE FROM (transfermarkt_\w+)', sql)
                if delete and delete.group(1) in tm_writer.CAREER_TABLES:
                    self.db.commit_snapshot(delete.group(1))
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

    def write_dataframe(self, **kwargs):
        return super().write_dataframe(**{**kwargs, 'add_metadata': False})

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
        if table in tm_writer.CAREER_TABLES:
            self.db.commit_snapshot(table)
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


def test_late_fetch_state_failure_is_durable_without_paid_retry(scraper):
    factory = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': '€1m'}]}])
    scraper.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_fetch_state'
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'current-1', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and proof['checkpoint_status'] == 'committed_checkpoint_pending'
    assert len(_table(scraper, 'transfermarkt_market_value_points')) == 1
    _, pending = current.run._load_pending_checkpoint('market_value_points', 'GB1', 2026)
    assert pending and pending['rows'][0]['source_id'] == '1'
    original_at = pending['rows'][0]['committed_at']
    scraper.test_db.fail_sql = None
    current.run._reconcile_pending_fetch_state(scraper)
    rows = scraper.test_db.sql.execute('SELECT last_success_at FROM transfermarkt_fetch_state').fetchall()
    assert pd.Timestamp(rows[0][0]).tz_localize('UTC') == pd.Timestamp(original_at)
    assert len(factory.clients) == 1


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


def test_career_manifest_failure_replays_original_bundle_without_http(scraper):
    factory = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': '€1m'}]}])
    scraper.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_dual_write_manifest_v2'
    with pytest.raises(RuntimeError, match='injected late'):
        current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], SCOPE, PREFLIGHT, 'original-cycle', decoded_body_soft_stop_bytes=1)
    original_rows = _table(scraper, 'transfermarkt_market_value_points').copy()
    scraper.test_db.fail_sql = None
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], SCOPE, PREFLIGHT, 'new-cycle', decoded_body_soft_stop_bytes=1)
    assert proof['reconciled_without_http'] and proof['cycle_id'] == 'original-cycle'
    assert proof['processed_player_ids'] == ['1'] and proof['deferred_player_ids'] == ['2']
    pd.testing.assert_frame_equal(original_rows, _table(scraper, 'transfermarkt_market_value_points'))
    assert sum(len(client.get_calls) for client in factory.clients) == 1


def test_cached_native_career_is_byte_for_byte_unchanged_during_legacy_hydration(scraper):
    http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': '€1m'}]}])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'initial', decoded_body_soft_stop_bytes=100000)
    original = _table(scraper, 'transfermarkt_market_value_points').copy()
    # Original physical rows can be read with SQL's object/date types.
    cached = {'market_value_points': current.run._query_dataframe(scraper.test_db,
        'SELECT * FROM iceberg.bronze.transfermarkt_market_value_points', [])}
    http(scraper, [{'list': [{'datum_mw': 'Jan 2, 2025', 'y': 2000000, 'verein': 'New', 'age': '21', 'mw': '€2m'}]}])
    fresh = scraper.read_market_value_points(league='GB1', season=2026, player_ids=['2'])
    spec = current.run.ENTITY_SPECS[current.run.ENTITY_MV_HISTORY]
    frames = current.run._merge_career_cache_frames(scraper, spec, {'market_value_points': fresh}, cached, 'GB1', 2026)
    scraper._batch_id = 'second-unit'
    frames = current.run._align_batch_ids(scraper, frames)
    results = {'outputs': {}, 'tables': []}
    current.run._save_frames(scraper, spec, frames, False, results)
    actual = _table(scraper, 'transfermarkt_market_value_points')
    pd.testing.assert_frame_equal(original.reset_index(drop=True), actual[actual.player_id == '1'].reset_index(drop=True))
    assert actual[actual.player_id == '2']._batch_id.tolist() == ['second-unit']
    assert results['outputs']['market_value_points']['captured_rows'] == 1
    manifest = current.run._persist_dual_write_manifest(scraper, spec, frames, results, 'second-cycle', 'GB1', 2026)
    assert manifest['status'] == 'success'
    refs = scraper.test_db.sql.execute("SELECT refs_json FROM transfermarkt_career_capture_refs_v1 WHERE cycle_id='second-cycle'").fetchone()[0]
    assert ['1', original.iloc[0]._batch_id, 1] in json.loads(refs)


def test_history_proof_survives_fresher_current_career_replacement(scraper):
    from scrapers.transfermarkt.career_refs import physical_predicate, apply_physical_refs
    from dags.utils.transfermarkt_native_v2 import PARITY_BY_NAME
    http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'History', 'age': '20', 'mw': '€1m'}]}])
    history = current.fetch_current_career(scraper, 'market_value_points', ['1'],
        {'competition_id': 'GB1', 'edition_id': '2025'}, PREFLIGHT, 'history-cycle', decoded_body_soft_stop_bytes=100000)
    original = _table(scraper, 'transfermarkt_market_value_points').copy()
    history_row = history['manifests'][0]['rows'][0]
    assert history_row['native_snapshot_id'] > 0 and history_row['legacy_snapshot_id'] > 0
    http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 9000000, 'verein': 'Current', 'age': '21', 'mw': '€9m'}]}])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'current-cycle', decoded_body_soft_stop_bytes=100000)
    live = _table(scraper, 'transfermarkt_market_value_points')
    assert live._batch_id.tolist() != original._batch_id.tolist()
    pair = PARITY_BY_NAME['market_value_points']
    cursor = scraper.test_db.cursor()
    capture = physical_predicate(cursor, history['manifest_cycle_id'], pair.name, pair.native_table)
    projection = pair._projection(table=pair.native_table, columns=original.columns, batch_id=history_row['native_batch_id'])
    projection = apply_physical_refs(projection, table=pair.native_table, batch=history_row['native_batch_id'], predicate=capture)
    assert f"FOR VERSION AS OF {history_row['native_snapshot_id']}" in projection
    retained = current.run._query_dataframe(scraper.test_db, projection, [])
    assert current._rows(retained) == current._rows(original)
    # Both sides of historical compatibility are read from original snapshots.
    legacy_sql, native_sql = pair.queries(legacy_batch_id=history_row['legacy_batch_id'], native_batch_id=history_row['native_batch_id'])
    for sql in (legacy_sql, native_sql):
        sql = apply_physical_refs(sql, table=pair.native_table, batch=history_row['native_batch_id'], predicate=capture)
        assert current.run._execute_cursor(scraper.test_db, sql, fetch=True) == [(0,)]


def test_pending_history_ops_reconcile_after_current_supersedes_without_older_overwrite(scraper):
    old_scope = {'competition_id': 'GB1', 'edition_id': '2025'}
    first_factory = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'History', 'age': '20', 'mw': '€1m'}]}])
    scraper.test_db.fail_sql = 'MERGE INTO iceberg.ops.transfermarkt_dual_write_manifest_v2'
    with pytest.raises(RuntimeError, match='injected late'):
        current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT, 'history-original', decoded_body_soft_stop_bytes=100000)
    scraper.test_db.fail_sql = None
    latest_factory = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 9000000, 'verein': 'Current', 'age': '21', 'mw': '€9m'}]}])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'current-latest', decoded_body_soft_stop_bytes=100000)
    latest = _table(scraper, 'transfermarkt_market_value_points').copy()
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT, 'history-recovery', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and proof['reconciled_without_http'] and proof['cycle_id'] == 'history-original'
    pd.testing.assert_frame_equal(latest, _table(scraper, 'transfermarkt_market_value_points'))
    assert sum(len(client.get_calls) for client in first_factory.clients) == 1
    assert sum(len(client.get_calls) for client in latest_factory.clients) == 1

@pytest.mark.parametrize('new_response', [
    {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 9000000, 'verein': 'Current', 'age': '21', 'mw': 'EUR9m'}]},
    {'list': []},
])
def test_before_receipt_crash_recovers_anchored_original_after_newer_current(scraper, new_response):
    from scrapers.transfermarkt.write_intents import pending_intents
    old_scope = {'competition_id': 'GB1', 'edition_id': '2025'}
    first = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'History', 'age': '20', 'mw': 'EUR1m'}]}])
    scraper.test_db.fail_sql = 'CREATE TABLE IF NOT EXISTS iceberg.ops.transfermarkt_career_capture_refs_v1'
    with pytest.raises(RuntimeError, match='injected late'):
        current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT, 'old-original', decoded_body_soft_stop_bytes=100000)
    original = _table(scraper, 'transfermarkt_market_value_points').copy()
    assert scraper.test_db.sql.execute('SELECT COUNT(*) FROM transfermarkt_career_snapshot_anchors_v1').fetchone()[0] == 2
    scraper.test_db.fail_sql = None
    latest = http(scraper, [new_response])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'fresh-current', decoded_body_soft_stop_bytes=100000)
    fresh_rows = _table(scraper, 'transfermarkt_market_value_points').copy()
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT, 'old-recovery', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and proof['reconciled_without_http'] and proof['cycle_id'] == 'old-original'
    pd.testing.assert_frame_equal(fresh_rows, _table(scraper, 'transfermarkt_market_value_points'))
    row = proof['manifests'][0]['rows'][0]
    snap = current.run._query_dataframe(scraper.test_db,
        f"SELECT * FROM iceberg.bronze.transfermarkt_market_value_points FOR VERSION AS OF {row['native_snapshot_id']}", [])
    assert current._rows(snap) == current._rows(original)
    assert sum(len(client.get_calls) for client in first.clients) == 1
    assert sum(len(client.get_calls) for client in latest.clients) == 1
    if not new_response['list']:
        assert scraper.test_db.sql.execute("SELECT status FROM transfermarkt_fetch_state WHERE source_id='1'").fetchone()[0] == 'authoritative_empty'


def test_native_only_cached_career_reads_original_lineage_without_legacy_tables(scraper):
    http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Current', 'age': '20', 'mw': 'EUR1m'}]}])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE,
        {'write_mode': 'native-only', 'revision': 7}, 'native-cache', decoded_body_soft_stop_bytes=100000)
    spec = current.run._spec_for_write_mode(current.run.ENTITY_SPECS[current.run.ENTITY_MV_HISTORY], 'native-only')
    before = current.run._query_dataframe(scraper.test_db, 'SELECT * FROM iceberg.bronze.transfermarkt_market_value_points', [])
    scraper.test_db.events.clear()
    cached = current.run._load_cached_career_frames(scraper, spec, ['1'])['market_value_points']
    assert current._rows(cached) == current._rows(before)
    assert not any('dual_write_manifest' in sql or 'transfermarkt_market_value_history' in sql for sql in scraper.test_db.events)


def test_failed_uncommitted_old_intent_cannot_resurrect_new_successful_empty(scraper):
    from scrapers.transfermarkt.writer import guard_frames, StaleTransfermarktWrite
    import pandas as pd
    http(scraper, [{'list': []}])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'empty-clock', decoded_body_soft_stop_bytes=100000)
    old = pd.DataFrame({'player_id': ['1'], 'fetched_at': [datetime(2020, 1, 1, tzinfo=timezone.utc)], '_batch_id': ['old']})
    with pytest.raises(StaleTransfermarktWrite):
        guard_frames(scraper, current.run.ENTITY_SPECS[current.run.ENTITY_MV_HISTORY].outputs, {'market_value_points': old, 'legacy_market_value_history': old})
    assert not scraper.test_db.sql.execute("SELECT 1 FROM sqlite_master WHERE name='transfermarkt_market_value_points'").fetchall()

@pytest.mark.parametrize('existing', [False, True])
def test_old_typed_empty_reconciles_without_deleting_fresher_current(scraper, existing):
    old_scope = {'competition_id': 'GB1', 'edition_id': '2025'}
    value = {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': 'EUR1m'}]}
    if existing:
        http(scraper, [value])
        current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'seed', decoded_body_soft_stop_bytes=100000)
    original_empty = http(scraper, [{'list': []}])
    scraper.test_db.fail_sql = 'CREATE TABLE IF NOT EXISTS iceberg.ops.transfermarkt_career_capture_refs_v1'
    with pytest.raises(RuntimeError, match='injected late'):
        current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT, 'old-empty', decoded_body_soft_stop_bytes=100000)
    scraper.test_db.fail_sql = None
    newest = http(scraper, [value])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'new-full', decoded_body_soft_stop_bytes=100000)
    live = _table(scraper, 'transfermarkt_market_value_points').copy()
    receipt = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT, 'replay-empty', decoded_body_soft_stop_bytes=100000)
    assert receipt['verified'] and receipt['reconciled_without_http']
    pd.testing.assert_frame_equal(live, _table(scraper, 'transfermarkt_market_value_points'))
    row = receipt['manifests'][0]['rows'][0]
    assert row['physical_refs'] == [['1', row['native_batch_id'], 0]]
    assert row['empty_capture_refs'][0]['status'] == 'authoritative_empty'
    assert row['empty_capture_refs'][0]['raw_attempts']
    assert sum(len(c.get_calls) for c in original_empty.clients) == 1
    assert sum(len(c.get_calls) for c in newest.clients) == 1


def test_two_partial_captures_share_resume_cycle_but_not_immutable_ref_unit(scraper):
    from scrapers.transfermarkt.career_refs import physical_predicate
    value = {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': 'EUR1m'}]}
    rows = []
    for player in ('1', '2'):
        http(scraper, [value])
        receipt = current.fetch_current_career(scraper, 'market_value_points', [player], SCOPE, PREFLIGHT, 'stable-cycle', decoded_body_soft_stop_bytes=100000)
        rows.append((receipt['manifest_cycle_id'], receipt['manifests'][0]['rows'][0]))
    assert rows[0][1]['capture_unit_id'] != rows[1][1]['capture_unit_id']
    assert rows[0][1]['native_batch_id'] != rows[1][1]['native_batch_id']
    for cycle_id, row in rows:
        capture = physical_predicate(scraper.test_db.cursor(), cycle_id, row['entity'], row['native_table'], batch_id=row['native_batch_id'])
        assert capture.refs == row['physical_refs']


def test_mixed_bundle_recovery_checks_second_original_empty_delete_snapshot(scraper):
    full = {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': 'EUR1m'}]}
    http(scraper, [full])
    current.fetch_current_career(scraper, 'market_value_points', ['2'], SCOPE, PREFLIGHT, 'seed-player2', decoded_body_soft_stop_bytes=100000)
    original = http(scraper, [full, {'list': []}])
    old_scope = {'competition_id': 'GB1', 'edition_id': '2025'}
    scraper.test_db.fail_sql = 'CREATE TABLE IF NOT EXISTS iceberg.ops.transfermarkt_career_capture_refs_v1'
    with pytest.raises(RuntimeError, match='injected late'):
        current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], old_scope, PREFLIGHT, 'old-mixed', decoded_body_soft_stop_bytes=100000)
    scraper.test_db.fail_sql = None
    latest = http(scraper, [full])
    current.fetch_current_career(scraper, 'market_value_points', ['2'], SCOPE, PREFLIGHT, 'new-player2', decoded_body_soft_stop_bytes=100000)
    current_rows = _table(scraper, 'transfermarkt_market_value_points').copy()
    receipt = current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], old_scope, PREFLIGHT, 'replay-mixed', decoded_body_soft_stop_bytes=100000)
    assert receipt['verified'] and receipt['reconciled_without_http']
    pd.testing.assert_frame_equal(current_rows, _table(scraper, 'transfermarkt_market_value_points'))
    row = receipt['manifests'][0]['rows'][0]
    assert {item[0]: item[2] for item in row['physical_refs']} == {'1': 1, '2': 0}
    assert sum(len(c.get_calls) for c in original.clients) == 2
    assert sum(len(c.get_calls) for c in latest.clients) == 1


def test_refs_allow_two_immutable_batches_for_same_history_child_entity(scraper):
    from scrapers.transfermarkt.career_refs import persist_capture_refs, physical_predicate
    full = {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': 'EUR1m'}]}
    units = []
    for player in ('1', '2'):
        http(scraper, [full])
        current.fetch_current_career(scraper, 'market_value_points', [player], SCOPE, PREFLIGHT, 'current-' + player, decoded_body_soft_stop_bytes=100000)
        frame = current.run._query_dataframe(scraper.test_db,
            'SELECT * FROM iceberg.bronze.transfermarkt_market_value_points WHERE player_id=?', [player])
        proof = persist_capture_refs(scraper.test_db, 'stable-history-child', 'market_value_points', 'transfermarkt_market_value_points', frame)
        units.append((frame._batch_id.iloc[0], proof))
    assert len({proof['capture_unit_id'] for _, proof in units}) == 2
    for batch, proof in units:
        predicate = physical_predicate(scraper.test_db.cursor(), 'stable-history-child', 'market_value_points', 'transfermarkt_market_value_points', batch_id=batch)
        assert predicate.refs == proof['physical_refs']


def test_two_empty_players_in_same_current_cycle_get_distinct_original_units(scraper):
    proofs = []
    for player in ('1', '2'):
        http(scraper, [{'list': []}])
        receipt = current.fetch_current_career(scraper, 'market_value_points', [player], SCOPE, PREFLIGHT, 'same-portion', decoded_body_soft_stop_bytes=100000)
        proofs.append(receipt['manifests'][0]['rows'][0])
    assert proofs[0]['native_batch_id'] != proofs[1]['native_batch_id']
    assert proofs[0]['capture_unit_id'] != proofs[1]['capture_unit_id']
    assert proofs[0]['empty_capture_refs'][0]['player_id'] == '1'
    assert proofs[1]['empty_capture_refs'][0]['player_id'] == '2'
    assert all(len(proof['empty_capture_refs'][0]['raw_attempts']) == 1 for proof in proofs)


def test_uncommitted_old_journal_does_not_claim_newer_snapshot_as_original(scraper, monkeypatch):
    from scrapers.transfermarkt.writer import StaleTransfermarktWrite
    from scrapers.transfermarkt.write_intents import pending_intents
    full = {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': 'EUR1m'}]}
    original = http(scraper, [full])
    old_scope = {'competition_id': 'GB1', 'edition_id': '2025'}
    store = scraper._iceberg_writer
    write = store._write_to_iceberg
    with monkeypatch.context() as patch:
        patch.setattr(store, '_write_to_iceberg', lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('before Bronze commit')))
        with pytest.raises(RuntimeError, match='before Bronze'):
            current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT, 'never-committed', decoded_body_soft_stop_bytes=100000)
    latest = http(scraper, [full])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'newer-committed', decoded_body_soft_stop_bytes=100000)
    live = _table(scraper, 'transfermarkt_market_value_points').copy()
    with pytest.raises(StaleTransfermarktWrite):
        current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT, 'unproven-replay', decoded_body_soft_stop_bytes=100000)
    pd.testing.assert_frame_equal(live, _table(scraper, 'transfermarkt_market_value_points'))
    assert pending_intents({'kind': 'current', 'entity': 'market_value_history'})
    assert sum(len(c.get_calls) for c in original.clients) == 1
    assert sum(len(c.get_calls) for c in latest.clients) == 1


def test_original_empty_proof_checksum_cannot_be_changed_under_same_unit(scraper):
    from scrapers.transfermarkt.career_refs import physical_predicate
    http(scraper, [{'list': []}])
    receipt = current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'empty-proof', decoded_body_soft_stop_bytes=100000)
    row = receipt['manifests'][0]['rows'][0]
    scraper.test_db.sql.execute("UPDATE transfermarkt_career_capture_refs_v1 SET empty_proof_json = REPLACE(empty_proof_json, 'authoritative_empty', 'success')")
    with pytest.raises(RuntimeError, match='original proof checksum'):
        physical_predicate(scraper.test_db.cursor(), receipt['manifest_cycle_id'], row['entity'], row['native_table'], batch_id=row['native_batch_id'])


def _execute_career_loss_query(scraper):
    """Execute generated DQ with only Trino JSON syntax adapted to SQLite."""
    from dags.utils.transfermarkt_bronze_dq import build_career_write_loss_sql
    sql = build_career_write_loss_sql('iceberg.bronze.transfermarkt_market_value_points')
    sql = re.sub(r'iceberg\.(?:bronze|ops)\.', '', sql)
    sql = sql.replace('CROSS JOIN UNNEST(CAST(JSON_PARSE(refs_json) AS array(json))) AS refs(ref)', 'CROSS JOIN json_each(refs_json) ref')
    sql = sql.replace('JSON_EXTRACT_SCALAR(ref,', 'JSON_EXTRACT_SCALAR(ref.value,')
    # Timestamps are normalized UTC ISO text in this physical test warehouse.
    clock_start = sql.index('COALESCE(TRY(CAST(from_iso8601_timestamp(')
    clock_end = sql.index(' capture_clock', clock_start)
    sql = sql[:clock_start] + '''COALESCE(json_extract(r.capture_times_json, '$."' || JSON_EXTRACT_SCALAR(ref.value, '$[0]') || '"'), r.committed_at)''' + sql[clock_end:]
    scraper.test_db.sql.create_function('JSON_EXTRACT_SCALAR', 2,
        lambda body, path: json.loads(body)[int(path[2:-1])])
    return scraper.test_db.sql.execute(sql).fetchone()[0]


def test_full_loss_dq_orders_original_source_clocks_after_late_history_reconciliation(scraper):
    full = {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': 'EUR1m'}]}
    old_scope = {'competition_id': 'GB1', 'edition_id': '2025'}
    http(scraper, [full])
    scraper.test_db.fail_sql = 'CREATE TABLE IF NOT EXISTS iceberg.ops.transfermarkt_career_capture_refs_v1'
    with pytest.raises(RuntimeError, match='injected late'):
        current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT, 'old-dq', decoded_body_soft_stop_bytes=100000)
    scraper.test_db.fail_sql = None
    http(scraper, [full])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'new-dq', decoded_body_soft_stop_bytes=100000)
    current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT, 'old-dq-reconcile', decoded_body_soft_stop_bytes=100000)
    assert _execute_career_loss_query(scraper) == 0
    scraper.test_db.sql.execute("DELETE FROM transfermarkt_market_value_points WHERE player_id='1'")
    assert _execute_career_loss_query(scraper) == 1


def test_full_loss_dq_detects_rows_left_under_any_batch_after_authoritative_empty(scraper):
    full = {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': 'EUR1m'}]}
    http(scraper, [full])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'before-empty-dq', decoded_body_soft_stop_bytes=100000)
    stored = _table(scraper, 'transfermarkt_market_value_points').copy()
    http(scraper, [{'list': []}])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT, 'empty-dq', decoded_body_soft_stop_bytes=100000)
    assert _execute_career_loss_query(scraper) == 0
    stored.to_sql('transfermarkt_market_value_points', scraper.test_db.sql, if_exists='append', index=False)
    assert _execute_career_loss_query(scraper) == 1


def _partial_dual_then_newer(scraper, monkeypatch, *, old_players=('1',), newer_players=('1',), newer_empty=False, newer_mode='dual'):
    old_scope = {'competition_id': 'GB1', 'edition_id': '2025'}
    old = {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Old', 'age': '20', 'mw': 'EUR1m'}]}
    fresh = {'list': []} if newer_empty else {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 9000000, 'verein': 'Fresh', 'age': '21', 'mw': 'EUR9m'}]}
    source = http(scraper, [old for _ in old_players])
    write = scraper._iceberg_writer._write_to_iceberg
    def interrupted(*args, **kwargs):
        if kwargs.get('table', args[2] if len(args) > 2 else None) == 'transfermarkt_market_value_history':
            raise RuntimeError('cut before legacy')
        return write(*args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(scraper._iceberg_writer, '_write_to_iceberg', interrupted)
        with pytest.raises(RuntimeError, match='cut before legacy'):
            current.fetch_current_career(scraper, 'market_value_points', list(old_players), old_scope, PREFLIGHT,
                'old-partial', decoded_body_soft_stop_bytes=100000)
    newest = http(scraper, [fresh for _ in newer_players])
    current.fetch_current_career(scraper, 'market_value_points', list(newer_players), SCOPE, {**PREFLIGHT, 'write_mode': newer_mode},
        'new-complete', decoded_body_soft_stop_bytes=100000)
    return old_scope, source, newest


def test_partial_dual_terminal_retirement_preserves_fresh_bytes_and_allows_unrelated_same_scope(scraper, monkeypatch):
    from scrapers.transfermarkt.write_intents import pending_intents, retired_intents
    old_scope, source, newest = _partial_dual_then_newer(scraper, monkeypatch)
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    legacy = _table(scraper, 'transfermarkt_market_value_history').copy()
    failure = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-delivery', decoded_body_soft_stop_bytes=100000)
    assert failure['status'] == 'superseded_partial_write' and failure['verified'] is False
    assert failure['original_native_snapshot_id'] > 0 and failure['original_legacy_snapshot_id'] is None
    assert failure['career_window']['processed_player_ids'] == [] and failure['career_window']['deferred_player_ids'] == ['1']
    assert failure['original_window']['processed_player_ids'] == ['1']
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    pd.testing.assert_frame_equal(legacy, _table(scraper, 'transfermarkt_market_value_history'))
    identity = {'kind': 'current', 'scope_id': scraper._resolve_scope('GB1', '2025')['scope_id'], 'entity': 'market_value_history'}
    assert pending_intents(identity) == []
    retired = retired_intents(identity)
    assert len(retired) == 1 and retired[0][0].exists()  # Never delete original paid prefix.
    events = list(scraper.test_db.events)
    assert current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-delivery', decoded_body_soft_stop_bytes=100000) == failure
    assert not any('MERGE INTO iceberg.bronze.' in event for event in scraper.test_db.events[len(events):])
    later = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 2000000, 'verein': 'Other', 'age': '20', 'mw': 'EUR2m'}]}])
    proof = current.fetch_current_career(scraper, 'market_value_points', ['2'], old_scope, PREFLIGHT,
        'next-unrelated', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and proof['processed_player_ids'] == ['2']
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points').query("player_id == '1'").reset_index(drop=True))
    pd.testing.assert_frame_equal(legacy, _table(scraper, 'transfermarkt_market_value_history').query("player_id == '1'").reset_index(drop=True))
    assert [sum(len(c.get_calls) for c in f.clients) for f in (source, newest, later)] == [1, 1, 1]
    assert not scraper.test_db.sql.execute("SELECT 1 FROM transfermarkt_dual_write_manifest_v2 WHERE cycle_id LIKE 'old-partial:%'").fetchall()


def test_next_current_job_reuses_superseding_capture_without_native_restamp_or_paid_retry(scraper, monkeypatch):
    old_scope, source, newest = _partial_dual_then_newer(scraper, monkeypatch)
    failure = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-delivery', decoded_body_soft_stop_bytes=100000)
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    latest_legacy = _table(scraper, 'transfermarkt_market_value_history').copy()
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'next-cached-job', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and proof['status'] == 'verified_superseding_cache' and proof['reconciled_without_http']
    assert proof['cached_capture_ids']['1'] == failure['superseding_players']['1']['raw']['capture_id']
    assert proof['cycle_id'] == 'next-cached-job' and proof['manifest_cycle_id'] != failure['original_cycle_id']
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    actual_legacy = _table(scraper, 'transfermarkt_market_value_history')
    pd.testing.assert_frame_equal(latest_legacy, actual_legacy[actual_legacy.season == '2627'].reset_index(drop=True))
    assert actual_legacy[actual_legacy.season == '2526'].value_eur.tolist() == ['9000000']
    assert [sum(len(c.get_calls) for c in f.clients) for f in (source, newest)] == [1, 1]


def test_partial_mixed_capture_without_newer_proof_for_every_player_stays_visible(scraper, monkeypatch):
    from scrapers.transfermarkt.writer import StaleTransfermarktWrite
    from scrapers.transfermarkt.write_intents import pending_intents
    old_scope, _, _ = _partial_dual_then_newer(scraper, monkeypatch, old_players=('1', '2'))
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    with pytest.raises(StaleTransfermarktWrite):
        current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], old_scope, PREFLIGHT,
            'unsafe-mixed', decoded_body_soft_stop_bytes=100000)
    assert pending_intents({'kind': 'current', 'entity': 'market_value_history'})
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))

@pytest.mark.parametrize('damage', ['manifest', 'archive', 'raw'])
def test_terminal_retirement_rejects_corrupt_or_unsuccessful_newer_capture(scraper, monkeypatch, damage):
    from scrapers.transfermarkt.write_intents import _root, pending_intents
    old_scope, source, newer = _partial_dual_then_newer(scraper, monkeypatch)
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    if damage == 'manifest':
        scraper.test_db.sql.execute("UPDATE transfermarkt_dual_write_manifest_v2 SET status='parity_mismatch'")
    elif damage == 'archive':
        archive = next((_root() / 'completed').glob('*.json'))
        archive.write_text(archive.read_text().replace('"verified":true', '"verified":false'))
    else:
        raw = scraper._http_client.get_raw_attempt_records()[0]
        record = scraper.test_store.load_capture(raw['capture_id'])[1]
        blob = scraper.test_store.root + '/' + record.blob_key
        from pathlib import Path
        Path(blob).write_bytes(b'corrupt gzip raw')
    with pytest.raises(Exception):
        current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
            'unsafe-recovery', decoded_body_soft_stop_bytes=100000)
    assert pending_intents({'kind': 'current', 'entity': 'market_value_history'})
    assert not list((_root() / 'resolutions').glob('*.json'))
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    assert [sum(len(c.get_calls) for c in f.clients) for f in (source, newer)] == [1, 1]


def test_newer_native_without_complete_legacy_does_not_retire_old_partial_job(scraper, monkeypatch):
    from scrapers.transfermarkt.writer import StaleTransfermarktWrite
    from scrapers.transfermarkt.write_intents import pending_intents
    old_scope, _, _ = _partial_dual_then_newer(scraper, monkeypatch)
    scraper.test_db.sql.execute('DELETE FROM transfermarkt_dual_write_manifest_v2')
    with pytest.raises(RuntimeError, match='genuine successful dual manifest'):
        current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
            'not-complete-dual', decoded_body_soft_stop_bytes=100000)
    assert pending_intents({'kind': 'current', 'entity': 'market_value_history'})


def test_typed_empty_newer_full_capture_retires_original_native_partial(scraper, monkeypatch):
    old_scope, old, new = _partial_dual_then_newer(scraper, monkeypatch, newer_empty=True)
    result = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'empty-superseding', decoded_body_soft_stop_bytes=100000)
    assert result['status'] == 'superseded_partial_write' and not result['verified']
    assert result['original_legacy_snapshot_id'] is None
    assert _table(scraper, 'transfermarkt_market_value_points').empty
    assert [sum(len(c.get_calls) for c in f.clients) for f in (old, new)] == [1, 1]


def _collector_for_backend(scraper, old_scope, cycle_id, ids, *, signals=None):
    from dags.scripts import run_transfermarkt_current as collector
    from types import SimpleNamespace
    from unittest.mock import Mock
    target = scraper._resolve_scope(old_scope['competition_id'], old_scope['edition_id'])
    scope = collector._Scope.__new__(collector._Scope)
    scope.scraper, scope.scope, scope.preflight, scope.cycle_id = scraper, old_scope, PREFLIGHT, cycle_id
    scope.target = SimpleNamespace(scope_id=target['scope_id'])
    scope.cursor = SimpleNamespace(generation='generation')
    scope.resume = {'careers': {'market_value_points': ids, 'transfer_events': []}}
    scope.data = {'player_values': {'1': {'market_value_present': True, 'market_value_eur': 1000000}}}
    scope.signals, scope.snapshot = signals or {}, None
    scope.persist, scope.admission, scope._require_reconciled_signals = Mock(), Mock(), Mock()
    scope.acknowledge = Mock(side_effect=AssertionError('terminal/mismatched job must never acknowledge'))
    scope.career_writer = current.fetch_current_career
    return scope


def test_collector_terminal_failure_is_not_proof_and_cached_raw_mismatch_remains_pending(scraper, monkeypatch):
    from dags.scripts import run_transfermarkt_current as collector
    from dags.utils.transfermarkt_current_state import SignalObservation, SignalState
    scope_id = scraper._resolve_scope('GB1', '2025')['scope_id']
    observation = SignalObservation(scope_id, 'player', '1', 'a' * 64, NOW, 'test-version',
        'signal-raw', NOW, 'https://tmapi.transfermarkt.technology/player', 'b' * 64)
    signal = SignalState(observation)
    scraper._current_career_signal_generations = {'1': collector.semantic_signature([
        scope_id, 'market_value_points', '1', observation.signature, signal.first_detected_at.isoformat(), ''])}
    old_scope, _, newer = _partial_dual_then_newer(scraper, monkeypatch)
    from scrapers.transfermarkt.write_intents import pending_intents
    pending = pending_intents({'kind': 'current', 'entity': 'market_value_history'})[0][1]
    old_capture = pending['evidence']['raw_attempts'][0]['capture_id']
    for ddl in collector.build_current_state_tables():
        current.run._execute_cursor(scraper.test_db, ddl)
    signal_sql = []
    monkeypatch.setattr(collector, '_sql', lambda _scraper, sql: signal_sql.append(sql))
    scope = _collector_for_backend(scraper, old_scope, 'collector-retirement', ['1', '2'], signals={'player:1': signal})
    scope.careers()
    assert len(scope.data['career_job_failures']) == 1
    assert scope.resume['careers']['market_value_points'] == ['1', '2']
    assert scope.signals['player:1'].status == 'failed'
    assert scope.signals['player:1'].result.startswith('superseded_partial_write:')
    assert not scope.data.get('career_receipts') and not scope.data.get('career_proofs')
    scope.acknowledge.assert_not_called()
    # Delivery windows can shrink without changing the original terminal job.
    failure = dict(scope.data['career_job_failures'])
    scope.resume['careers']['market_value_points'] = ['1']
    scope.careers()
    assert scope.data['career_job_failures'] == failure
    assert scope.resume['careers']['market_value_points'] == ['1']
    scope.resume['careers']['market_value_points'] = ['1', '2']
    # A prior caller's old successful outcome must not override cached proof's
    # exact newer capture identity during tmapi-vs-CEAPI validation.
    scraper.get_fetch_outcomes = lambda: {'market_value_points': {'1': {'raw_capture_id': old_capture}}}
    scope.cycle_id = 'collector-cached'
    scope.careers()
    assert scope.resume['careers']['market_value_points'] == ['1', '2']
    assert scope.data['career_receipts'][-1]['collector_window']['source_mismatch_ids'] == ['1']
    assert scope.signals['player:1'].status == 'failed'
    scope.acknowledge.assert_not_called()
    assert sum(len(c.get_calls) for c in newer.clients) == 1


def test_terminal_resolution_checksum_failure_cannot_hide_original_paid_intent(scraper, monkeypatch):
    from scrapers.transfermarkt.write_intents import _root, pending_intents
    old_scope, _, _ = _partial_dual_then_newer(scraper, monkeypatch)
    current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-corruption', decoded_body_soft_stop_bytes=100000)
    resolution = next((_root() / 'resolutions').glob('*.json'))
    resolution.write_text(resolution.read_text().replace('superseded_partial_write', 'complete'))
    with pytest.raises(RuntimeError, match='checksum'):
        pending_intents({'kind': 'current', 'entity': 'market_value_history'})
    assert list(_root().glob('*.json'))  # Original journal survives retirement.


def test_archive_after_49_hours_can_retire_failure_but_cannot_refresh_new_current_cache(scraper, monkeypatch):
    old_scope, old, newer = _partial_dual_then_newer(scraper, monkeypatch)
    now = scraper._http_client._time()
    monkeypatch.setattr(scraper._http_client, '_time', lambda: now + 49 * 3600)
    failure = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'late-retirement', decoded_body_soft_stop_bytes=100000)
    assert failure['status'] == 'superseded_partial_write' and failure['verified'] is False
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    legacy = _table(scraper, 'transfermarkt_market_value_history').copy()
    deferred = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'new-overdue-job', decoded_body_soft_stop_bytes=100000)
    assert deferred['status'] == 'supersession_deferred' and not deferred['verified']
    assert deferred['blocking_reasons'] == {'1': 'career_raw_cache_expired'}
    assert deferred['career_window']['processed_player_ids'] == []
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    pd.testing.assert_frame_equal(legacy, _table(scraper, 'transfermarkt_market_value_history'))
    assert [sum(len(c.get_calls) for c in f.clients) for f in (old, newer)] == [1, 1]


def test_retired_old_observation_does_not_rebuy_when_newer_cache_generation_differs(scraper, monkeypatch):
    scraper._current_career_signal_generations = {'1': 'original-generation'}
    old_scope, old, newer = _partial_dual_then_newer(scraper, monkeypatch)
    current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-generation', decoded_body_soft_stop_bytes=100000)
    # The archived superseding capture belongs to the original recorded
    # generation. The caller declares a new generation; the ordinary new
    # capture path is allowed, but the archive must not satisfy it.
    scraper._current_career_signal_generations = {'1': 'new-generation'}
    latest = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 11000000, 'verein': 'Newer', 'age': '21', 'mw': 'EUR11m'}]}])
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'new-observed-job', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and proof.get('status') != 'verified_superseding_cache'
    assert [sum(len(c.get_calls) for c in f.clients) for f in (old, newer, latest)] == [1, 1, 1]


def test_retired_job_uses_latest_verified_successor_after_another_current_update(scraper, monkeypatch):
    old_scope, _, _ = _partial_dual_then_newer(scraper, monkeypatch)
    current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-first', decoded_body_soft_stop_bytes=100000)
    next_source = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 12000000, 'verein': 'Newest', 'age': '21', 'mw': 'EUR12m'}]}])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT,
        'newer-successor', decoded_body_soft_stop_bytes=100000)
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'reuse-latest-successor', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and proof['status'] == 'verified_superseding_cache'
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    assert _table(scraper, 'transfermarkt_market_value_history').query("season == '2526'").value_eur.tolist() == ['12000000']
    assert sum(len(c.get_calls) for c in next_source.clients) == 1


def test_overdue_retired_player_does_not_block_unrelated_same_scope_in_exact_window(scraper, monkeypatch):
    old_scope, _, _ = _partial_dual_then_newer(scraper, monkeypatch)
    current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-before-overdue', decoded_body_soft_stop_bytes=100000)
    later = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 2000000, 'verein': 'Other', 'age': '20', 'mw': 'EUR2m'}]}])
    now = scraper._http_client._time()
    monkeypatch.setattr(scraper._http_client, '_time', lambda: now + 49 * 3600)
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], old_scope, PREFLIGHT,
        'overdue-plus-unrelated', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and proof['processed_player_ids'] == ['2'] and proof['deferred_player_ids'] == ['1']
    assert proof['career_window']['requested_player_ids'] == ['1', '2']
    assert sum(len(c.get_calls) for c in later.clients) == 1


def test_crash_after_completed_archive_before_finish_replays_original_clock_and_repairs_index(scraper, monkeypatch):
    from scrapers.transfermarkt import write_intents
    full = {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Club', 'age': '20', 'mw': 'EUR1m'}]}
    factory = http(scraper, [full])
    with monkeypatch.context() as patch:
        patch.setattr(write_intents, 'finish_intent', lambda _: (_ for _ in ()).throw(RuntimeError('cut before acknowledgement')))
        with pytest.raises(RuntimeError, match='cut before acknowledgement'):
            current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT,
                'archive-original', decoded_body_soft_stop_bytes=100000)
    archive = next((write_intents._root() / 'completed').glob('*.json'))
    initial = write_intents._read_record(archive)['receipt']['committed_at']
    # Model the earlier crash boundary after the primary archive's fsync,
    # before the by-unit pointer's durable acknowledgement.
    for pointer in (write_intents._root() / 'completed/by-unit').glob('*.json'):
        pointer.unlink()
    rows = _table(scraper, 'transfermarkt_market_value_points').copy()
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT,
        'archive-replay', decoded_body_soft_stop_bytes=100000)
    assert proof['reconciled_without_http'] and proof['committed_at'] == initial
    assert list((write_intents._root() / 'completed/by-unit').glob('*.json'))
    assert write_intents.pending_intents({'kind': 'current'}) == []
    pd.testing.assert_frame_equal(rows, _table(scraper, 'transfermarkt_market_value_points'))
    assert sum(len(c.get_calls) for c in factory.clients) == 1


def test_cached_completed_job_reconciles_after_49_hours_without_new_write_or_cache_freshness(scraper, monkeypatch):
    from scrapers.transfermarkt import write_intents
    old_scope, _, newest = _partial_dual_then_newer(scraper, monkeypatch)
    current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'cached-retirement', decoded_body_soft_stop_bytes=100000)
    with monkeypatch.context() as patch:
        patch.setattr(write_intents, 'finish_intent', lambda _: (_ for _ in ()).throw(RuntimeError('cut cached acknowledgement')))
        with pytest.raises(RuntimeError, match='cut cached acknowledgement'):
            current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
                'cached-original-job', decoded_body_soft_stop_bytes=100000)
    path, _ = write_intents.pending_intents({'kind': 'current'})[0]
    archive = write_intents.completed_intent(path)
    for pointer in (write_intents._root() / 'completed/by-unit').glob('*.json'):
        if write_intents._read_record(pointer)['intent_sha256'] == path.stem:
            pointer.unlink()
    monkeypatch.setattr(scraper._http_client, '_time', lambda: pd.to_datetime(
        archive['journal']['evidence']['captured_at_by_id']['1'], utc=True).timestamp() + 49 * 3600)
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    legacy = _table(scraper, 'transfermarkt_market_value_history').copy()
    events = len(scraper.test_db.events)
    replay = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'cached-restart', decoded_body_soft_stop_bytes=100000)
    assert replay['verified'] and replay['reconciled_without_http']
    assert replay['cycle_id'] == 'cached-original-job' and replay['committed_at'] == archive['receipt']['committed_at']
    assert not write_intents.pending_intents({'kind': 'current'})
    assert not any('MERGE INTO iceberg.bronze.' in event for event in scraper.test_db.events[events:])
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    pd.testing.assert_frame_equal(legacy, _table(scraper, 'transfermarkt_market_value_history'))
    assert sum(len(c.get_calls) for c in newest.clients) == 1
    fresh_job = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'new-job-after-49h', decoded_body_soft_stop_bytes=100000)
    assert not fresh_job['verified'] and fresh_job['status'] == 'supersession_deferred'
    assert sum(len(c.get_calls) for c in newest.clients) == 1


@pytest.mark.parametrize('newer_empty', [False, True])
def test_retirement_uses_selected_live_players_after_another_bundle_player_changes(scraper, monkeypatch, newer_empty):
    old_scope, _, newest = _partial_dual_then_newer(scraper, monkeypatch,
        newer_players=('1', '2'), newer_empty=newer_empty)
    latest = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 12000000,
        'verein': 'Player two newer', 'age': '22', 'mw': 'EUR12m'}]}])
    current.fetch_current_career(scraper, 'market_value_points', ['2'], SCOPE, PREFLIGHT,
        'p2-later-complete', decoded_body_soft_stop_bytes=100000)
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    legacy = _table(scraper, 'transfermarkt_market_value_history').copy()
    failure = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-p1-after-p2-changed', decoded_body_soft_stop_bytes=100000)
    assert failure['status'] == 'superseded_partial_write' and not failure['verified']
    assert failure['retired_player_ids'] == ['1'] and failure['original_legacy_snapshot_id'] is None
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    pd.testing.assert_frame_equal(legacy, _table(scraper, 'transfermarkt_market_value_history'))
    assert sum(len(c.get_calls) for c in newest.clients) == 2
    assert sum(len(c.get_calls) for c in latest.clients) == 1
    if not newer_empty:
        proof = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
            'cached-p1-after-p2-changed', decoded_body_soft_stop_bytes=100000)
        assert proof['verified'] and proof['status'] == 'verified_superseding_cache'
        pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
        assert sum(len(c.get_calls) for c in latest.clients) == 1


def test_pending_cached_job_keeps_original_source_and_retires_when_newer_current_supersedes_it(scraper, monkeypatch):
    from scrapers.transfermarkt import write_intents
    old_scope, _, _ = _partial_dual_then_newer(scraper, monkeypatch)
    current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-first-job', decoded_body_soft_stop_bytes=100000)
    write = scraper._iceberg_writer._write_to_iceberg
    def interrupted(*args, **kwargs):
        if kwargs.get('table', args[2] if len(args) > 2 else None) == 'transfermarkt_market_value_history':
            raise RuntimeError('cut cached legacy')
        return write(*args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(scraper._iceberg_writer, '_write_to_iceberg', interrupted)
        with pytest.raises(RuntimeError, match='cut cached legacy'):
            current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
                'cached-partial-original', decoded_body_soft_stop_bytes=100000)
    path, journal = write_intents.pending_intents({'kind': 'current'})[0]
    original_body = path.read_bytes()
    latest = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 12000000,
        'verein': 'Latest', 'age': '22', 'mw': 'EUR12m'}]}])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT,
        'latest-after-cached-partial', decoded_body_soft_stop_bytes=100000)
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    legacy = _table(scraper, 'transfermarkt_market_value_history').copy()
    failure = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-cached-partial', decoded_body_soft_stop_bytes=100000)
    assert not failure['verified'] and failure['status'] == 'superseded_partial_write'
    assert failure['original_cycle_id'] == 'cached-partial-original'
    assert failure['intent_sha256'] == path.stem and path.read_bytes() == original_body
    assert failure['original_raw']['1']['capture_id'] == journal['evidence']['cache_sources'][0]['capture_id']
    assert failure['original_legacy_snapshot_id'] is None and failure['original_native_snapshot_id'] > 0
    assert write_intents.completed_intent(path) is None
    assert not write_intents.pending_intents({'kind': 'current'})
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    pd.testing.assert_frame_equal(legacy, _table(scraper, 'transfermarkt_market_value_history'))
    assert sum(len(c.get_calls) for c in latest.clients) == 1


def test_current_transfer_partial_dual_can_retire_with_real_newer_transfer_capture(scraper, monkeypatch):
    from scrapers.transfermarkt.write_intents import pending_intents
    old_scope = {'competition_id': 'GB1', 'edition_id': '2025'}
    old = {'transfers': [{'date': 'Sep 1, 2025', 'season': '25/26', 'upcoming': False,
        'from': {'clubName': 'Old', 'href': '/old/verein/10'}, 'to': {'clubName': 'Next', 'href': '/next/verein/20'}, 'fee': 'free transfer', 'marketValue': 'EUR1m'}]}
    fresh = {'transfers': [{**old['transfers'][0], 'marketValue': 'EUR9m'}]}
    source = http(scraper, [old])
    writer = scraper._iceberg_writer._write_to_iceberg
    def interrupt(*args, **kwargs):
        if kwargs.get('table', args[2] if len(args) > 2 else None) == 'transfermarkt_transfers':
            raise RuntimeError('cut transfer legacy')
        return writer(*args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(scraper._iceberg_writer, '_write_to_iceberg', interrupt)
        with pytest.raises(RuntimeError, match='cut transfer legacy'):
            current.fetch_current_career(scraper, 'transfer_events', ['1'], old_scope, PREFLIGHT,
                'old-transfer', decoded_body_soft_stop_bytes=100000)
    new = http(scraper, [fresh])
    current.fetch_current_career(scraper, 'transfer_events', ['1'], SCOPE, PREFLIGHT,
        'fresh-transfer', decoded_body_soft_stop_bytes=100000)
    rows = _table(scraper, 'transfermarkt_transfer_events').copy()
    proof = current.fetch_current_career(scraper, 'transfer_events', ['1'], old_scope, PREFLIGHT,
        'retire-transfer', decoded_body_soft_stop_bytes=100000)
    assert proof['status'] == 'superseded_partial_write' and not proof['verified']
    assert proof['original_legacy_snapshot_id'] is None
    assert pending_intents({'kind': 'current', 'entity': 'transfers'}) == []
    pd.testing.assert_frame_equal(rows, _table(scraper, 'transfermarkt_transfer_events'))
    assert [sum(len(c.get_calls) for c in f.clients) for f in (source, new)] == [1, 1]


def test_multiunit_cached_archive_restart_preserves_each_native_batch_and_unblocks_next_player(scraper, monkeypatch):
    from scrapers.transfermarkt import write_intents
    old_scope, _, _ = _partial_dual_then_newer(scraper, monkeypatch, old_players=('1', '2'))
    http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 10000000, 'verein': 'Two', 'age': '21', 'mw': 'EUR10m'}]}])
    current.fetch_current_career(scraper, 'market_value_points', ['2'], SCOPE, PREFLIGHT,
        'separate-p2-complete', decoded_body_soft_stop_bytes=100000)
    failure = current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], old_scope, PREFLIGHT,
        'retire-separate-units', decoded_body_soft_stop_bytes=100000)
    assert not failure['verified'] and failure['retired_player_ids'] == ['1', '2']
    with monkeypatch.context() as patch:
        patch.setattr(write_intents, 'finish_intent', lambda _: (_ for _ in ()).throw(RuntimeError('cut multiunit acknowledgement')))
        with pytest.raises(RuntimeError, match='cut multiunit acknowledgement'):
            current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], old_scope, PREFLIGHT,
                'multiunit-cached', decoded_body_soft_stop_bytes=100000)
    path, _ = write_intents.pending_intents({'kind': 'current'})[0]
    archived = write_intents.completed_intent(path)
    archived_native = write_intents.unpack_frames(archived['frames'])['market_value_points']
    assert archived_native._batch_id.nunique() == 2
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    legacy = _table(scraper, 'transfermarkt_market_value_history').copy()
    calls = sum(len(client.get_calls) for client in scraper._http_client._client_factory.clients)
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], old_scope, PREFLIGHT,
        'multiunit-restart', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and proof['cycle_id'] == 'multiunit-cached'
    assert proof['committed_at'] == archived['receipt']['committed_at']
    assert not write_intents.pending_intents({'kind': 'current'})
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    pd.testing.assert_frame_equal(legacy, _table(scraper, 'transfermarkt_market_value_history'))
    assert sum(len(client.get_calls) for client in scraper._http_client._client_factory.clients) == calls
    future = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 3000000, 'verein': 'Three', 'age': '21', 'mw': 'EUR3m'}]}])
    result = current.fetch_current_career(scraper, 'market_value_points', ['3'], old_scope, PREFLIGHT,
        'multiunit-unrelated', decoded_body_soft_stop_bytes=100000)
    assert result['verified'] and result['processed_player_ids'] == ['3']
    assert sum(len(client.get_calls) for client in future.clients) == 1
    actual = _table(scraper, 'transfermarkt_market_value_points')
    pd.testing.assert_frame_equal(native, actual[actual.player_id.isin(['1', '2'])].reset_index(drop=True))


def _empty_partial_then_current(scraper, monkeypatch, fault, *, later_empty=False):
    from scrapers.transfermarkt import write_intents
    old_scope = {'competition_id': 'GB1', 'edition_id': '2025'}
    full = {'list': [{'datum_mw': 'Jan 1, 2025', 'y': 1000000, 'verein': 'Old', 'age': '20', 'mw': 'EUR1m'}]}
    http(scraper, [full])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'empty-seed', decoded_body_soft_stop_bytes=100000)
    http(scraper, [{'list': []}])
    execute = current.run._execute_cursor
    table = 'transfermarkt_market_value_points' if fault == 'never_committed' else 'transfermarkt_market_value_history'
    def cut(connection, sql, *args, **kwargs):
        if sql.startswith('DELETE FROM iceberg.bronze.' + table + ' '):
            raise RuntimeError('cut empty DELETE')
        return execute(connection, sql, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(current.run, '_execute_cursor', cut)
        if fault == 'before_receipt':
            patch.setattr(write_intents, 'record_empty_commit', lambda *_: (_ for _ in ()).throw(RuntimeError('cut empty DELETE')))
        with pytest.raises(RuntimeError, match='cut empty DELETE'):
            current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
                'original-empty-partial', decoded_body_soft_stop_bytes=100000)
    path, journal = write_intents.pending_intents({'kind': 'current'})[0]
    if fault == 'missing_raw':
        raw = journal['evidence']['raw_attempts'][0]
        record = scraper.test_store.load_capture(raw['capture_id'])[1]
        from pathlib import Path
        Path(scraper.test_store.root + '/' + record.blob_key).unlink()
    native_receipt = write_intents.read_empty_commit(path, 'transfermarkt_market_value_points', ['1'])
    assert (native_receipt is None) == (fault in {'never_committed', 'before_receipt'})
    assert write_intents.read_empty_commit(path, 'transfermarkt_market_value_history', ['1']) is None
    full['list'][0]['y'] = 9000000
    if later_empty:
        full = {'list': []}
    future_source = http(scraper, [full])
    current.fetch_current_career(scraper, 'market_value_points', ['1'], SCOPE, PREFLIGHT,
        'current-after-empty-partial', decoded_body_soft_stop_bytes=100000)
    return old_scope, path, journal, native_receipt, future_source


@pytest.mark.parametrize('fault', ['never_committed', 'before_receipt', 'after_native'])
def test_later_foreign_empty_delete_cannot_supply_missing_original_empty_commit(scraper, monkeypatch, fault):
    from scrapers.transfermarkt import write_intents
    old_scope, path, _, native_receipt, source = _empty_partial_then_current(scraper, monkeypatch, fault, later_empty=True)
    assert _table(scraper, 'transfermarkt_market_value_points').empty
    assert _table(scraper, 'transfermarkt_market_value_history').empty
    if fault == 'after_native':
        failure = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
            'original-empty-retired-after-foreign-empty', decoded_body_soft_stop_bytes=100000)
        assert failure['status'] == 'superseded_partial_write' and not failure['verified']
        assert failure['original_native_snapshot_id'] == native_receipt['snapshot_id']
        assert failure['original_legacy_snapshot_id'] is None
    else:
        with pytest.raises(Exception):
            current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
                'cannot-borrow-foreign-empty-snapshot', decoded_body_soft_stop_bytes=100000)
        assert write_intents.pending_intents({'kind': 'current'})
        assert write_intents.resolution_for(path) is None
    assert sum(len(client.get_calls) for client in source.clients) == 1


def test_native_empty_delete_partial_can_retire_without_claiming_old_legacy_success(scraper, monkeypatch):
    from scrapers.transfermarkt import write_intents
    old_scope, path, journal, native_receipt, source = _empty_partial_then_current(scraper, monkeypatch, 'after_native')
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    legacy = _table(scraper, 'transfermarkt_market_value_history').copy()
    body = path.read_bytes()
    failure = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-empty-partial', decoded_body_soft_stop_bytes=100000)
    assert failure['status'] == 'superseded_partial_write' and not failure['verified']
    assert failure['original_native_snapshot_id'] == native_receipt['snapshot_id']
    assert failure['original_legacy_snapshot_id'] is None and path.read_bytes() == body
    assert failure['original_raw']['1']['capture_id'] == journal['evidence']['raw_attempts'][0]['capture_id']
    assert not write_intents.pending_intents({'kind': 'current'})
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    pd.testing.assert_frame_equal(legacy, _table(scraper, 'transfermarkt_market_value_history'))
    assert sum(len(client.get_calls) for client in source.clients) == 1
    future = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 3000000, 'verein': 'Two', 'age': '21', 'mw': 'EUR3m'}]}])
    proof = current.fetch_current_career(scraper, 'market_value_points', ['2'], old_scope, PREFLIGHT,
        'unrelated-after-empty-partial', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and sum(len(client.get_calls) for client in future.clients) == 1


@pytest.mark.parametrize('fault', ['never_committed', 'before_receipt', 'missing_raw'])
def test_empty_partial_without_exact_original_delete_proof_fails_closed(scraper, monkeypatch, fault):
    from scrapers.transfermarkt import write_intents
    old_scope, path, _, _, source = _empty_partial_then_current(scraper, monkeypatch, fault)
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    body = path.read_bytes()
    with pytest.raises(Exception):
        current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
            'cannot-prove-empty-partial', decoded_body_soft_stop_bytes=100000)
    assert path.read_bytes() == body and write_intents.pending_intents({'kind': 'current'})
    assert write_intents.resolution_for(path) is None
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    assert sum(len(client.get_calls) for client in source.clients) == 1


def test_cached_players_can_use_distinct_current_successors_after_one_shared_bundle_splits(scraper, monkeypatch):
    old_scope, _, _ = _partial_dual_then_newer(scraper, monkeypatch, old_players=('1', '2'), newer_players=('1', '2'))
    current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], old_scope, PREFLIGHT,
        'retire-shared-two', decoded_body_soft_stop_bytes=100000)
    latest = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 12000000, 'verein': 'Later two', 'age': '22', 'mw': 'EUR12m'}]}])
    current.fetch_current_career(scraper, 'market_value_points', ['2'], SCOPE, PREFLIGHT,
        'only-p2-later', decoded_body_soft_stop_bytes=100000)
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    legacy = _table(scraper, 'transfermarkt_market_value_history').copy()
    proof = current.fetch_current_career(scraper, 'market_value_points', ['1', '2'], old_scope, PREFLIGHT,
        'cached-split-successors', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and proof['processed_player_ids'] == ['1', '2']
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    actual = _table(scraper, 'transfermarkt_market_value_history')
    pd.testing.assert_frame_equal(legacy, actual[actual.season == '2627'].reset_index(drop=True))
    cached = actual[actual.season == '2526']
    assert dict(zip(cached.player_id, cached.value_eur.astype(int))) == {'1': 9000000, '2': 12000000}
    assert sum(len(client.get_calls) for client in latest.clients) == 1


def test_genuine_native_only_successor_retires_dual_failure_and_allows_independent_current(scraper, monkeypatch):
    from scrapers.transfermarkt import write_intents
    old_scope, _, source = _partial_dual_then_newer(scraper, monkeypatch, newer_mode='native-only')
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    failure = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
        'retire-via-native-only', decoded_body_soft_stop_bytes=100000)
    assert not failure['verified'] and failure['status'] == 'superseded_partial_write'
    assert failure['original_legacy_snapshot_id'] is None
    assert not write_intents.pending_intents({'kind': 'current'})
    pd.testing.assert_frame_equal(native, _table(scraper, 'transfermarkt_market_value_points'))
    assert sum(len(client.get_calls) for client in source.clients) == 1
    future = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 3000000, 'verein': 'Two', 'age': '21', 'mw': 'EUR3m'}]}])
    proof = current.fetch_current_career(scraper, 'market_value_points', ['2'], old_scope, PREFLIGHT,
        'native-only-unrelated', decoded_body_soft_stop_bytes=100000)
    assert proof['verified'] and sum(len(client.get_calls) for client in future.clients) == 1


def _native_history_portion(scraper, monkeypatch, tmp_path, player, value, mode='native-only'):
    import contextlib
    import scrapers.transfermarkt
    source = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': value,
        'verein': 'History', 'age': '21', 'mw': 'EUR9m'}]}])
    scraper._batch_id = 'history-capture-' + player
    with monkeypatch.context() as patch:
        patch.setattr(scrapers.transfermarkt, 'TransfermarktScraper', lambda **_: contextlib.nullcontext(scraper))
        patch.setattr(current.run, '_select_player_ids', lambda *_args, **_kwargs:
            ([player], 0, 0, [], {'roster_size': 1, 'selected': 1, 'pending': 0}))
        result = current.run._run_entity(current.run.ENTITY_SPECS['market_value_history'], ['GB1'], 2026,
            1, str(tmp_path / ('history-result-' + player + '.json')), refresh_mode='history',
            run_key='stable-history-child', write_mode=mode, expected_reader_revision=7)
    assert result == 0
    exported = json.loads((tmp_path / ('history-result-' + player + '.json')).read_text())
    assert exported['career_capture_archive_status'] == 'complete'
    return source, exported


@pytest.mark.parametrize('mode', ['native-only', 'dual'])
@pytest.mark.parametrize('damage', [None, 'missing_latest_archive', 'corrupt_latest_attestation', 'corrupt_latest_manifest', 'missing_latest_raw'])
def test_native_history_archive_reconciles_old_current_after_stable_child_manifest_changes(scraper, monkeypatch, tmp_path, damage, mode):
    from scrapers.transfermarkt import write_intents, superseded
    old_scope, _, _ = _partial_dual_then_newer(scraper, monkeypatch)
    manifest_key = 'native_write_manifest' if mode == 'native-only' else 'batch_manifest'
    attestation_key = 'native_manifest_attestation' if mode == 'native-only' else 'dual_manifest_attestation'
    _, first = _native_history_portion(scraper, monkeypatch, tmp_path, '1', 9000000, mode)
    first_row, = first[manifest_key]['rows']
    first_archive = write_intents.completed_capture('stable-history-child', 'market_value_points', first_row['native_batch_id'])
    _, second = _native_history_portion(scraper, monkeypatch, tmp_path, '2', 10000000, mode)
    second_row, = second[manifest_key]['rows']
    second_archive = write_intents.completed_capture('stable-history-child', 'market_value_points', second_row['native_batch_id'])
    assert first_row['native_batch_id'] != second_row['native_batch_id']
    actual_old_attestation = first_archive['receipt'][attestation_key]
    assert actual_old_attestation['proof']['row'][actual_old_attestation['proof']['fields'].index('native_batch_id')] == first_row['native_batch_id']
    native = _table(scraper, 'transfermarkt_market_value_points').copy()
    if damage == 'missing_latest_archive':
        (write_intents._root() / 'completed' / (second_archive['intent_sha256'] + '.json')).unlink()
    elif damage == 'corrupt_latest_attestation':
        changed = json.loads(json.dumps(second_archive))
        changed['receipt'][attestation_key]['sha256'] = '0' * 64
        archive_path = write_intents._root() / 'completed' / (second_archive['intent_sha256'] + '.json')
        archive_path.unlink()
        # A valid outer checksum cannot turn a bad actual-row attestation into proof.
        write_intents._immutable_record(archive_path, changed)
    elif damage == 'corrupt_latest_manifest':
        table = 'transfermarkt_native_write_manifest_v2' if mode == 'native-only' else 'transfermarkt_dual_write_manifest_v2'
        scraper.test_db.sql.execute(f"UPDATE {table} SET status='parity_mismatch' WHERE cycle_id='stable-history-child'")
    elif damage == 'missing_latest_raw':
        raw = second_archive['journal']['evidence']['raw_attempts'][0]
        record = scraper.test_store.load_capture(raw['capture_id'])[1]
        from pathlib import Path
        Path(scraper.test_store.root + '/' + record.blob_key).unlink()
    if damage:
        with pytest.raises(Exception):
            superseded.verify_complete(scraper, current.run.ENTITY_SPECS['market_value_history'], first_archive, players=['1'])
        with pytest.raises(Exception):
            current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
                'cannot-retire-unproven-native-history', decoded_body_soft_stop_bytes=100000)
        assert write_intents.pending_intents({'kind': 'current'})
    else:
        proof = superseded.verify_complete(scraper, current.run.ENTITY_SPECS['market_value_history'], first_archive, players=['1'])
        assert proof['archive']['receipt'][attestation_key] == actual_old_attestation
        failure = current.fetch_current_career(scraper, 'market_value_points', ['1'], old_scope, PREFLIGHT,
            'retire-via-original-native-history', decoded_body_soft_stop_bytes=100000)
        assert not failure['verified'] and failure['original_legacy_snapshot_id'] is None
        assert failure['superseding_players']['1']['unit']['native_batch_id'] == first_row['native_batch_id']
        assert not write_intents.pending_intents({'kind': 'current'})
        future = http(scraper, [{'list': [{'datum_mw': 'Jan 1, 2025', 'y': 3000000, 'verein': 'Three', 'age': '21', 'mw': 'EUR3m'}]}])
        result = current.fetch_current_career(scraper, 'market_value_points', ['3'], old_scope, PREFLIGHT,
            'unrelated-after-native-history', decoded_body_soft_stop_bytes=100000)
        assert result['verified'] and sum(len(client.get_calls) for client in future.clients) == 1
    actual = _table(scraper, 'transfermarkt_market_value_points')
    pd.testing.assert_frame_equal(native, actual[actual.player_id.isin(['1', '2'])].reset_index(drop=True))
