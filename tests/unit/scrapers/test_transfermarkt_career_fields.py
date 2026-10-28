"""Offline regressions from saved CEAPI responses for #1408."""
import copy
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from scrapers.transfermarkt import scraper as tm
from scrapers.transfermarkt.writer import TransfermarktIcebergWriter, TransfermarktTrinoManager


@pytest.fixture
def payload():
    # Selected, otherwise unmodified rows from the lead's raw-store evidence.
    path = Path(__file__).parents[2] / 'fixtures/transfermarkt/ceapi-career-fields.json'
    return json.loads(path.read_text())


def test_removed_value_and_point_context_survive_native_and_legacy(payload):
    rows = tm._parse_mv_history(payload['market_value'], '124543')
    assert rows[1]['value_eur'] is None
    assert rows[0]['value_eur'] == 50000
    assert rows[0]['is_value_removed'] is False
    assert rows[0]['club_id'] == '75'
    assert rows[1]['is_value_removed'] is True
    assert rows[1]['club_id'] == '2646'
    assert {row['mv_last_change'] for row in rows} == {date(2026, 1, 8)}
    frame = tm._with_metadata(rows, tm.MARKET_VALUE_POINT_COLUMNS,
                              entity_type='market_value_points', batch_id='b')
    assert pd.isna(frame.iloc[1].value_eur)
    assert frame.iloc[1].is_value_removed
    assert frame.iloc[1].club_id == '2646'
    legacy = tm.materialize_legacy_market_value_history(frame, 'GB1', '2026', 'split_year')
    assert pd.isna(legacy.iloc[1].value_eur)
    assert list(legacy.columns) == tm.LEGACY_MV_COLUMNS + tm._METADATA_COLUMNS


def test_transfer_fee_and_club_context_survive_native_and_legacy(payload):
    rows = tm._parse_transfers(payload['transfers'], '1')
    assert rows[0]['fee_text'] == 'Loan fee: €500K'
    assert rows[0]['fee_eur'] == 500000
    assert rows[0]['is_loan_end'] is False
    assert rows[1]['fee_text'] == 'End of loan €300K'
    assert rows[1]['fee_eur'] == 300000
    assert rows[1]['is_loan_end'] is True
    assert rows[2]['from_is_special'] is False
    assert rows[2]['to_is_special'] is True
    assert rows[2]['to_club_id'] == '515'
    assert rows[2]['to_country_id'] is None
    for row, raw in zip(rows, payload['transfers']['transfers']):
        for side in ('from', 'to'):
            assert row[f'{side}_country_flag'] == raw[side]['countryFlag']
            if raw[side]['countryFlag']:
                assert row[f'{side}_country_id'] == raw[side]['countryFlag'].split('/')[-1].split('.')[0]
    frame = tm._with_metadata(rows, tm.TRANSFER_EVENT_COLUMNS,
                              entity_type='transfer_events', batch_id='b')
    assert frame.iloc[1].is_loan_end
    assert frame.iloc[2].to_is_special
    assert frame.iloc[0].from_country_id == rows[0]['from_country_id']
    legacy = tm.materialize_legacy_transfers(frame, 'GB1', '2026', 'split_year')
    assert legacy.iloc[0].fee_text == 'Loan fee: €500K'
    assert list(legacy.columns) == tm.LEGACY_TRANSFER_COLUMNS + tm._METADATA_COLUMNS
    changed = copy.deepcopy(payload['transfers'])
    for row in changed['transfers']:
        row['fee'] = 'free transfer'
        row['from']['isSpecial'] = True
        row['from']['countryFlag'] = None
    assert [row['transfer_id'] for row in tm._parse_transfers(changed, '1')] == [row['transfer_id'] for row in rows]


@pytest.mark.parametrize('raw,expected', [(None, None), ('€0', False), ('?', False), ('', False)])
def test_zero_or_missing_value_does_not_mean_removed(raw, expected):
    row, = tm._parse_mv_history({'list': [{'x': 1425250800000, 'y': 0, 'mw': raw}]}, '1')
    assert row['value_eur'] == 0
    assert row['is_value_removed'] is expected
    assert row['club_id'] is row['mv_last_change'] is None


@pytest.mark.parametrize('entity,columns,booleans', [
    ('market_value_points', tm.MARKET_VALUE_POINT_COLUMNS, ('is_value_removed',)),
    ('transfer_events', tm.TRANSFER_EVENT_COLUMNS, ('from_is_special', 'to_is_special', 'is_loan_end')),
])
def test_first_all_null_batch_has_nullable_schema(entity, columns, booleans):
    frame = tm._with_metadata([{'player_id': '1'}], columns, entity_type=entity, batch_id='b')
    types = TransfermarktTrinoManager().arrow_schema_to_trino(
        TransfermarktIcebergWriter()._pandas_to_arrow(frame).schema)
    assert all(types[column] == 'BOOLEAN' and pd.isna(frame.iloc[0][column]) for column in booleans)
    if entity == 'market_value_points':
        assert types['mv_last_change'] == 'DATE'
        assert types['value_eur'] == 'BIGINT'


def test_missing_transfer_context_is_not_inferred():
    row, = tm._parse_transfers({'transfers': [{'from': {}, 'to': {}, 'fee': None}]}, '1')
    assert all(row[column] is None for column in (
        'from_is_special', 'to_is_special', 'from_country_id', 'to_country_id',
        'from_country_flag', 'to_country_flag', 'is_loan_end', 'fee_text', 'fee_eur'))


@pytest.mark.parametrize('raw', ['', None, 'https://img/flagge/verysmall/x.png',
                                  'https://img/flagge/verysmall/26.svg'])
def test_image_ids_require_observed_numeric_png_path(raw):
    assert tm._image_id(raw, 'flagge/verysmall') is None


def test_cache_and_lineage_use_same_revisions():
    from dags.scripts import run_transfermarkt_scraper as run
    assert run.PARSER_VERSION == tm.PARSER_REVISION
    assert run.SCHEMA_VERSION == tm.SCHEMA_REVISION
    assert tm.TransfermarktScraper.get_fetch_outcomes_envelope(
        type('Scraper', (), {'get_fetch_outcomes': lambda self: {}})()
    )['schema_version'] == tm.SCHEMA_REVISION


@pytest.mark.parametrize('kind', ['current', 'scope'])
@pytest.mark.parametrize('entity,key,columns', [
    ('market_value_history', 'market_value_points', tm.MARKET_VALUE_POINT_COLUMNS),
    ('transfers', 'transfer_events', tm.TRANSFER_EVENT_COLUMNS),
])
@pytest.mark.parametrize('incompatible', ['column', 'parser_revision', 'schema_revision'])
def test_pending_replay_refuses_old_contract_and_preserves_journal(
    tmp_path, monkeypatch, kind, entity, key, columns, incompatible,
):
    from scrapers.transfermarkt.write_intents import save_intent, pending_intents
    monkeypatch.setenv('TM_WRITE_INTENT_DIR', str(tmp_path))
    identity = {'kind': kind, 'entity': entity}
    frame = tm._with_metadata([{'player_id': '1', 'parser_revision': tm.PARSER_REVISION,
                               'schema_revision': tm.SCHEMA_REVISION}], columns,
                              entity_type=key, batch_id='b')
    path = save_intent(identity, {key: frame})
    assert len(pending_intents(identity)) == 1
    path.unlink()
    if incompatible == 'column':
        frame = frame.drop(columns=['is_value_removed' if key == 'market_value_points' else 'is_loan_end'])
    else:
        frame[incompatible] = 'old'
    path = save_intent(identity, {key: frame})
    original = path.read_bytes()
    with pytest.raises(RuntimeError, match='career replay parser/schema contract differs'):
        pending_intents(identity)
    assert path.read_bytes() == original


@pytest.mark.parametrize('entity,key,columns', [
    ('market_value_history', 'market_value_points', tm.MARKET_VALUE_POINT_COLUMNS),
    ('transfers', 'transfer_events', tm.TRANSFER_EVENT_COLUMNS),
])
def test_mixed_pending_replay_keeps_cached_lineage(tmp_path, monkeypatch, entity, key, columns):
    from dags.scripts import run_transfermarkt_scraper as run
    from scrapers.transfermarkt.write_intents import save_intent, pending_intents, unpack_frames
    monkeypatch.setenv('TM_WRITE_INTENT_DIR', str(tmp_path))
    fresh = tm._with_metadata([{'player_id': 'new', 'parser_revision': tm.PARSER_REVISION,
                               'schema_revision': tm.SCHEMA_REVISION}], columns,
                              entity_type=key, batch_id='new-batch')
    cached = tm._with_metadata([{'player_id': 'old', 'parser_revision': 'v2',
                                'schema_revision': '2'}], columns,
                               entity_type=key, batch_id='old-batch')
    if entity == 'transfers':
        fresh['transfer_id'] = 'new-transfer'
        cached['transfer_id'] = 'old-transfer'
    spec = run.ENTITY_SPECS[entity]
    frames = run._merge_career_cache_frames(tm, spec, {key: fresh}, {key: cached}, 'GB1', 2026)
    identity = {'kind': 'scope', 'entity': entity}
    save_intent(identity, frames)
    restored = unpack_frames(pending_intents(identity)[0][1]['frames'])[key]
    assert restored.loc[restored.player_id == 'old', 'parser_revision'].item() == 'v2'
    assert restored.attrs['tm_original_capture_refs'] == [{'player_id': 'old', 'batch_id': 'old-batch'}]
    restored.loc[restored.player_id == 'new', 'parser_revision'] = 'old'
    for path, _payload in pending_intents(identity):
        path.unlink()
    save_intent(identity, {key: restored})
    with pytest.raises(RuntimeError, match='career replay parser/schema contract differs'):
        pending_intents(identity)


@pytest.mark.parametrize('changed', [None, 'existing', 'missing_nonnull', 'missing_required'])
def test_cached_guard_checks_original_projection_before_schema_evolution(changed):
    from unittest.mock import MagicMock
    from dags.scripts import run_transfermarkt_scraper as run
    from scrapers.transfermarkt import writer
    from scrapers.transfermarkt.revisions import CAREER_NULLABLE_ADDITIONS
    frame = tm._with_metadata([{'player_id': 'old', 'value_eur': 100,
                               'parser_revision': 'v2', 'schema_revision': '2'}],
                              tm.MARKET_VALUE_POINT_COLUMNS,
                              entity_type='market_value_points', batch_id='old-batch')
    frame.attrs['tm_original_capture_refs'] = [{'player_id': 'old', 'batch_id': 'old-batch'}]
    # A fresh row brings new populated columns into the same write bundle.
    fresh = frame.copy()
    fresh['player_id'] = 'new'
    fresh['club_id'] = '75'
    physical = [column for column in frame.columns if column not in CAREER_NULLABLE_ADDITIONS]
    if changed == 'missing_nonnull':
        frame['club_id'] = '75'
    if changed == 'missing_required':
        physical.remove('value_eur')
    rows = frame[physical].copy()
    if changed == 'existing':
        rows['value_eur'] = 999
    scraper = MagicMock()
    cursor = scraper._bronze_connection.return_value.cursor.return_value
    def execute(sql, params=None):
        assert sql.startswith('SELECT * FROM ')
        cursor.description = [(column,) for column in physical]
        cursor.fetchall.return_value = list(rows.itertuples(index=False, name=None))
    cursor.execute.side_effect = execute
    mixed = pd.concat([frame, fresh], ignore_index=True)
    mixed.attrs.update(frame.attrs)
    outputs = run.ENTITY_SPECS[run.ENTITY_MV_HISTORY].outputs
    if changed:
        with pytest.raises(writer.StaleTransfermarktWrite):
            writer.guard_cached_frames(scraper, outputs, {'market_value_points': mixed})
    else:
        writer.guard_cached_frames(scraper, outputs, {'market_value_points': mixed})
