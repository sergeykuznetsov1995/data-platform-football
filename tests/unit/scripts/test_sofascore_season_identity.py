"""#1577: Copa Paraguay has two native seasons under canonical 2026."""
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dags.scripts import prepare_sofascore_workload as planner
from dags.scripts import run_sofascore_scraper as runner
from dags.utils.sofascore_dq import validate_season_alignment
from scrapers.sofascore.pipeline import build_event_spec


@pytest.mark.parametrize('source_season_id,expected_mismatches', [(95576, 4), (98037, 0)])
def test_retained_copa_paraguay_events_keep_native_season(source_season_id, expected_mismatches):
    records = json.loads((Path(__file__).parents[2] / 'fixtures' /
                         'sofascore_season_95576_events.json').read_text())
    rows = []
    for record in records:
        event = record['payload']['event']
        spec = build_event_spec(source_tournament_id=13614,
                                source_season_id=source_season_id,
                                target_id=event['id'], endpoint='event',
                                freshness_key='final', paid_proxy=False)
        for row in spec.parsers['events'](record['payload']):
            row.update(source_season_id=str(source_season_id), season='2026')
            assert row['season_id'] == 98037
            rows.append(row)
    # Append-only schedule probes could resolve the same two events twice.
    report = validate_season_alignment(rows + rows,
        expected_source_season_id=source_season_id, expected_canonical_season='2026')
    assert report.metrics['season.mismatches'] == expected_mismatches
    if expected_mismatches:
        with pytest.raises(Exception, match='season_mismatch: 4 rows'):
            report.require()
    else:
        report.require()


@pytest.mark.parametrize('probe', ['capture', 'targets', 'deadlines'])
def test_all_bronze_target_probes_bind_native_season(monkeypatch, probe):
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.fetchall.return_value = [('16478404', 123)] if probe == 'deadlines' else [('16478404',)]
    module = runner if probe == 'capture' else planner
    monkeypatch.setattr(module, '_trino_connect', lambda: connection)
    if probe == 'capture':
        assert runner._resolve_match_ids_from_bronze('SS-13614', '2026', None,
                                                     source_season_id=98037) == ['16478404']
    elif probe == 'targets':
        assert planner._finished_match_ids('SS-13614', '2026', 98037) == {'16478404'}
    else:
        assert planner._finished_match_deadlines('SS-13614', '2026', 98037) == {'16478404': 123}
    sql, params = cursor.execute.call_args.args
    assert 'CAST(season_id AS bigint) = ?' in sql
    assert params == ('SS-13614', '2026', 98037)
    assert connection.close.called
