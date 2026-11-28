from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyarrow import fs

from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from scrapers.transfermarkt.raw_store import RawResponseStore
from scrapers.transfermarkt.referee_capture import (
    RefereeCaptureError,
    capture_referee_directory,
    capture_referee_profiles,
    referee_directory_url,
    referee_profile_url,
    referee_scopes,
)
from scrapers.transfermarkt.registry import deterministic_scope_id
from tests.unit.dags.test_transfermarkt_scope_planner import _competition, _edition, _joined_row

pytestmark = pytest.mark.unit
FIXTURES = Path(__file__).resolve().parents[2] / 'fixtures' / 'transfermarkt' / 'referees'
DIRECTORY = (FIXTURES / 'directory_compact.html').read_text()
DETAILED = (FIXTURES / 'directory_detailed.html').read_text()
PROFILE = (FIXTURES / 'profile.html').read_text()
NOW = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)


class FakeClient:
    def __init__(self, root, responses, *, scope_id, omit_raw=False):
        self._raw_store = RawResponseStore(fs.LocalFileSystem(), str(root))
        self.responses = responses
        self.scope_id = scope_id
        self.omit_raw = omit_raw
        self.calls = []

    def fetch(self, url, *, as_json=False, label='endpoint', **kwargs):
        self.calls.append((url, label))
        html = self.responses[url]
        raw = self._raw_store.store_attempt(
            url=url, body=html.encode(), status_code=200, headers={},
            fetched_at=NOW.isoformat(), cycle_id='cycle-test', scope_id=self.scope_id,
            endpoint=label, attempt=0,
        )
        return FetchOutcome(
            status=FetchStatus.OK, value=html, status_code=200,
            raw_capture_id=None if self.omit_raw else raw.capture_id,
            raw_body_hash=raw.content_hash, raw_fetched_at=raw.fetched_at,
        )


def scope(*, current=True, route='wettbewerb', competition='XY1', edition='2025'):
    return {
        'scope_id': deterministic_scope_id(competition, edition),
        'competition_id': competition,
        'edition_id': edition,
        'slug': 'example-league',
        'current': current,
        'route': route,
        'competition_source_url': (
            f'https://www.transfermarkt.com/example-league/startseite/{route}/{competition}'
        ),
    }


def registry_row(value):
    competition = replace(
        _competition(value['competition_id']),
        slug=value['slug'], source_url=value['competition_source_url'],
        registry_snapshot_id='registry-test',
    )
    edition = replace(
        _edition(value['competition_id'], value['edition_id'], current=value['current']),
        registry_snapshot_id='registry-test',
    )
    return _joined_row(competition, edition)


def test_directory_urls_use_registry_identity_route_and_page():
    value = scope()
    assert referee_directory_url(value) == (
        'https://www.transfermarkt.com/example-league/schiedsrichter/'
        'wettbewerb/XY1/saison_id/2025/plus/1'
    )
    assert referee_directory_url(value, page=2).endswith('/plus/1/page/2')
    cup = scope(route='pokalwettbewerb')
    assert '/pokalwettbewerb/XY1/' in referee_directory_url(cup)
    assert referee_profile_url('00101').endswith('/-/profil/schiedsrichter/101')


def test_registry_universe_separates_current_and_history_and_keeps_route():
    current = registry_row(scope())
    old = registry_row(scope(current=False, edition='2024'))
    current_scopes = referee_scopes([current, old])
    history_scopes = referee_scopes([current, old], historical=True)
    assert [row['edition_id'] for row in current_scopes] == ['2025']
    assert [row['edition_id'] for row in history_scopes] == ['2024']
    assert current_scopes[0]['registry_snapshot_id'] == 'registry-test'
    assert current_scopes[0]['route'] == 'wettbewerb'


def test_two_page_capture_reconciles_metrics_and_preserves_raw_lineage(tmp_path):
    value = scope()
    first_url = referee_directory_url(value)
    second_url = referee_directory_url(value, page=2)
    first = DIRECTORY.replace(
        '</tbody>',
        '</tbody><div class="naechste-seite"><a href="/example-league/schiedsrichter/'
        'wettbewerb/XY1/saison_id/2025/plus/1/page/2">next</a></div>',
    )
    client = FakeClient(tmp_path, {first_url: first, second_url: DETAILED}, scope_id=value['scope_id'])
    bundle = capture_referee_directory(
        client, value, run_id='run-test', universe_version='registry-test', extracted_at=NOW,
    )
    assert client.calls == [(first_url, 'referee_directory'), (second_url, 'referee_directory')]
    assert bundle.expected_directory_pages == 2
    assert len(bundle.directory_referees) == 2
    assert len(bundle.aggregates) == 14
    assert bundle.profiles == ()
    assert len(bundle.raw_capture_ids) == 2
    for row in bundle.aggregates:
        assert row['observed_at'] == NOW.isoformat()
        assert row['captured_at'] == NOW.isoformat()
        assert row['raw_capture_id'] in bundle.raw_capture_ids
        assert row['source_body_hash'] and row['source_url'] in {first_url, second_url}
        assert row['scope_id'] == value['scope_id'] and row['cycle_id'] == 'cycle-test'
        assert row['run_id'] == 'run-test' and row['universe_version'] == 'registry-test'
        assert row['parser_revision'] == 'referees-v2'


def test_next_page_cannot_turn_directory_into_match_discovery(tmp_path):
    value = scope()
    url = referee_directory_url(value)
    bad = DIRECTORY.replace(
        '</tbody>',
        '</tbody><div class="naechste-seite"><a href="/spielbericht/index/spielbericht/1">next</a></div>',
    )
    client = FakeClient(tmp_path, {url: bad}, scope_id=value['scope_id'])
    with pytest.raises(RefereeCaptureError, match='route_forbidden'):
        capture_referee_directory(
            client, value, run_id='run-test', universe_version='registry-test', extracted_at=NOW,
        )
    assert len(client.calls) == 1


def test_missing_raw_identity_fails_before_any_output(tmp_path):
    value = scope()
    url = referee_directory_url(value)
    client = FakeClient(tmp_path, {url: DIRECTORY}, scope_id=value['scope_id'], omit_raw=True)
    with pytest.raises(RefereeCaptureError, match='raw_lineage'):
        capture_referee_directory(
            client, value, run_id='run-test', universe_version='registry-test', extracted_at=NOW,
        )


def test_profile_capture_uses_only_profile_routes_and_deduplicates_ids(tmp_path):
    value = scope()
    url = referee_profile_url('101')
    client = FakeClient(tmp_path, {url: PROFILE}, scope_id=value['scope_id'])
    bundle = capture_referee_profiles(
        client, ['101', '101'], scope=value,
        run_id='run-test', universe_version='registry-test', extracted_at=NOW,
    )
    assert len(bundle.profiles) == 1
    assert bundle.aggregates == ()
    assert bundle.profiles[0]['referee_id'] == '101'
    assert bundle.profiles[0]['raw_capture_id'] in bundle.raw_capture_ids
    assert client.calls == [(url, 'referee_profile')]
