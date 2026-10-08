"""Deadline admission and generation-isolated cache use real client/raw paths."""
from datetime import datetime, timezone

import pytest

from scrapers.transfermarkt.client import (
    CurrentPortionDeadlineExceeded, ProxyFilterLeaseProvider, TransfermarktHttpClient,
)
from scrapers.transfermarkt.models import LeaseTrafficSnapshot, ProxyLease, SharedTrafficLedger, TrafficMeterError
from scrapers.transfermarkt.raw_store import RawResponseStore
from tests.unit.scrapers.test_transfermarkt_traffic import _ClientFactory, _FakeResp, _manager
from tests.unit.scrapers.test_transfermarkt_proxy_transport_v2 import (
    _ControlClient, _ControlResponse, _FakeLeaseProvider, _metadata,
)

pytestmark = pytest.mark.unit
URL = 'https://www.transfermarkt.com/team/kader/verein/1/saison_id/2025/plus/1'


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def read(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class Provider(_FakeLeaseProvider):
    def __init__(self, clock, *, permit_duration=0):
        super().__init__([])
        self.clock = clock
        self.permit_duration = permit_duration
        self.waits = []

    def acquire_request_permit(self, *, metadata, request_id, max_wait_seconds=65):
        self.waits.append(max_wait_seconds)
        self.clock.now += self.permit_duration
        return 'permit'


def client(clock, responses, *, deadline=100, provider=None, **kwargs):
    factory = _ClientFactory(responses)
    created = TransfermarktHttpClient(
        proxy_manager=None if provider else _manager(3),
        lease_provider=provider, traffic_ledger=SharedTrafficLedger(),
        lease_metadata=_metadata(), client_factory=factory,
        monotonic_fn=clock.read, time_fn=clock.read,
        sleep_fn=clock.sleep, random_fn=lambda: 0,
        request_deadline_monotonic=deadline, **kwargs,
    )
    return created, factory


def test_deadline_prevents_any_new_io_or_false_attempt():
    clock = Clock()
    instance, factory = client(clock, [], deadline=35)
    with pytest.raises(CurrentPortionDeadlineExceeded):
        instance.fetch(URL, as_json=False)
    assert factory.calls == []
    assert instance.get_traffic_stats()['requests'] == 0
    assert instance.get_raw_attempt_records() == ()


def test_http_timeout_clamped_and_unexpired_success_accounted():
    instance, factory = client(Clock(), [_FakeResp(b'ok')], deadline=40)
    assert instance.fetch(URL, as_json=False).value == 'ok'
    assert factory.clients[0].get_calls[0][1]['timeout'] == 10
    assert instance.get_traffic_stats()['requests'] == 1


def test_permit_wait_clamped_then_http_rechecked_and_lease_settled():
    clock = Clock()
    provider = Provider(clock, permit_duration=65)
    instance, factory = client(clock, [], provider=provider)
    # Model a last accounting delta reported only during final lease close.
    original_close = provider.close
    def close(lease):
        provider.current = LeaseTrafficSnapshot(up_bytes=7, down_bytes=11)
        return original_close(lease)
    provider.close = close
    with pytest.raises(CurrentPortionDeadlineExceeded):
        instance.fetch(URL, as_json=False)
    assert provider.waits == [65]
    assert factory.clients[0].get_calls == []
    assert factory.clients[0].closed
    assert provider.closed == ['lease-1']
    stats = instance.get_traffic_stats()
    assert stats['requests'] == 0
    assert stats['provider_metered_bytes'] == 18
    assert instance.get_raw_attempt_records() == ()


def test_permit_short_remaining_budget():
    clock = Clock()
    provider = Provider(clock)
    instance, _ = client(clock, [_FakeResp(b'ok')], deadline=50, provider=provider)
    assert instance.fetch(URL, as_json=False).is_success
    assert provider.waits == [15]
    instance.close()
    assert provider.closed == ['lease-1']


def test_backoff_cannot_start_next_attempt_into_tail():
    clock = Clock()
    instance, factory = client(clock, [_FakeResp(b'bad', status=502)], deadline=35.2)
    with pytest.raises(CurrentPortionDeadlineExceeded):
        instance.fetch(URL, as_json=False)
    assert clock.sleeps == []
    assert instance.get_traffic_stats()['requests'] == 1
    assert factory.clients[0].closed


def test_rate_limiter_wait_bounded_without_request():
    clock = Clock()
    class Limiter:
        def acquire(self, *, timeout):
            assert timeout == 15
            clock.now += timeout
            return False
    instance, factory = client(clock, [], deadline=50, rate_limiter=Limiter())
    with pytest.raises(CurrentPortionDeadlineExceeded):
        instance.fetch(URL, as_json=False)
    assert factory.clients[0].get_calls == []
    assert factory.clients[0].closed


def test_real_control_permit_exhaustion_is_graceful_and_timeout_clamped():
    clock = Clock()
    control = _ControlClient([_ControlResponse(429, {'code': 'request_permit_pending', 'retry_after_seconds': 5})])
    provider = ProxyFilterLeaseProvider(
        'http://control.invalid', control_client=control, control_token='x' * 32,
        request_deadline_monotonic=40, monotonic_fn=clock.read,
        time_fn=clock.read, sleep_fn=clock.sleep,
    )
    with pytest.raises(CurrentPortionDeadlineExceeded):
        provider.acquire_request_permit(metadata=_metadata(), request_id='request', max_wait_seconds=2)
    assert control.calls[0][2]['timeout'] == 2
    assert clock.sleeps == []


def test_lease_settlement_uses_tail_and_never_starts_after_hard_deadline():
    clock = Clock()
    control = _ControlClient([_ControlResponse(200, {'up_bytes': 7, 'down_bytes': 11, 'closed': True})])
    provider = ProxyFilterLeaseProvider(
        'http://control.invalid', control_client=control, control_token='x' * 32,
        request_deadline_monotonic=1, monotonic_fn=clock.read,
        time_fn=clock.read, sleep_fn=clock.sleep,
    )
    lease = ProxyLease('lease', 'token', 'http://proxy.invalid:8000', 100, 200)
    assert provider.close(lease).provider_bytes == 18
    assert control.calls[0][2]['timeout'] == 1
    clock.now = 1
    with pytest.raises(TrafficMeterError, match='settlement'):
        provider.stats(lease)
    assert len(control.calls) == 1


def test_generation_raw_roundtrip_same_cycle_retry_and_fresh_next_cycle(tmp_path, monkeypatch):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    cache = {}
    clock = Clock()
    clock.now = datetime.now(timezone.utc).timestamp()
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-A')
    first, _ = client(clock, [_FakeResp(b'old')], deadline=None, cache=cache, raw_store=store, require_raw_store=True)
    args = dict(as_json=False, label='listing', context={'scope': 'GB1:2025'}, cache_key=URL, cache_ttl_seconds=86400)
    old = first.fetch(URL, cache_generation='signal-day-A', **args)
    assert cache[URL]['cache_generation'] == 'signal-day-A'
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-B')
    retry, retry_factory = client(clock, [], deadline=None, cache=cache, raw_store=store, require_raw_store=True)
    replay = retry.fetch(URL, cache_generation='signal-day-A', **args)
    assert replay.cache_hit and replay.raw_fetched_at == old.raw_fetched_at
    assert retry_factory.calls == []
    assert retry.get_raw_attempt_records() == ()
    assert len(retry.get_cache_source_records()) == 1
    # A stale signal is rejected before even reading its obsolete raw evidence.
    cache[URL]['outcome']['raw_capture_id'] = 'unavailable-old-generation'
    fresh, fresh_factory = client(clock, [_FakeResp(b'new')], deadline=None, cache=cache, raw_store=store, require_raw_store=True)
    result = fresh.fetch(URL, cache_generation='signal-day-B', **args)
    assert not result.cache_hit and result.value == 'new'
    assert len(fresh_factory.clients[0].get_calls) == 1
    assert result.raw_capture_id != old.raw_capture_id


def test_changed_club_generation_invalidates_otherwise_verified_48h_squad(tmp_path, monkeypatch):
    store = RawResponseStore.from_uri((tmp_path / 'raw').as_uri())
    cache = {}
    clock = Clock()
    clock.now = datetime.now(timezone.utc).timestamp()
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-A')
    args = dict(as_json=False, label='squad', context={'scope': 'BRC:2025'}, cache_key=URL, cache_ttl_seconds=86400)
    old, _ = client(clock, [_FakeResp(b'old')], deadline=None, cache=cache, raw_store=store, require_raw_store=True, resume_squad_cache=True)
    old.fetch(URL, cache_generation='club-signature-A', **args)
    monkeypatch.setenv('TM_CHILD_CYCLE_ID', 'child-B')
    clock.now += 25 * 3600
    resumed, factory = client(clock, [], deadline=None, cache=cache, raw_store=store, require_raw_store=True, resume_squad_cache=True)
    assert resumed.fetch(URL, cache_generation='club-signature-A', **args).cache_hit
    assert factory.calls == []
    changed, factory = client(clock, [_FakeResp(b'changed')], deadline=None, cache=cache, raw_store=store, require_raw_store=True, resume_squad_cache=True)
    result = changed.fetch(URL, cache_generation='club-signature-B', **args)
    assert not result.cache_hit and result.value == 'changed'
    assert len(factory.clients[0].get_calls) == 1


@pytest.mark.parametrize('generation', ['', ' ', 123])
def test_invalid_generation_refuses_before_io(generation):
    instance, factory = client(Clock(), [])
    with pytest.raises(ValueError):
        instance.fetch(URL, as_json=False, cache_generation=generation)
    assert factory.calls == []


def test_legacy_default_has_original_timeout_and_backoff():
    clock = Clock()
    instance, factory = client(clock, [_FakeResp(b'bad', status=502), _FakeResp(b'ok')], deadline=None)
    assert instance.fetch(URL, as_json=False).value == 'ok'
    assert factory.clients[0].get_calls[0][1]['timeout'] == 12
    assert clock.sleeps == [0.5]
