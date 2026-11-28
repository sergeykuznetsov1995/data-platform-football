"""The client's part of a source stop: the gateway's one probe is remembered and answered once.

A refusal of the probe is not answered here; the ordinary site feedback reports it and
the gateway holds the source again. No socket is opened (control and transport are stubs)."""

from __future__ import annotations

from dataclasses import replace

import pytest

from scrapers.transfermarkt.client import ProxyFilterLeaseProvider, TransfermarktHttpClient
from scrapers.transfermarkt.models import (
    FetchStatus,
    LeaseTrafficSnapshot,
    ProxyLease,
    SharedTrafficLedger,
    TrafficMeterError,
)

pytestmark = pytest.mark.unit

CONTROL_TOKEN = 'c' * 32
URL = 'https://www.transfermarkt.com/x'


class _Response:
    def __init__(self, body, status=200):
        self.content = body
        self.status_code = status
        self.headers = {'Content-Length': str(len(body))}

    @property
    def text(self):
        return self.content.decode('utf-8')


class _TlsFactory:
    def __init__(self, responses):
        self.client = _TlsClient(responses)

    def __call__(self, **kwargs):
        return self.client


class _TlsClient:
    def __init__(self, responses):
        self.responses = list(responses)

    def get(self, url, **kwargs):
        return self.responses.pop(0)

    def close(self):
        pass


class _ProbingProvider:
    """A stream lease provider whose permits may be the gateway's one probe."""

    def __init__(self, probe):
        self.probe = probe
        self.probe_permit_id = None
        self.passed = 0
        self.current = LeaseTrafficSnapshot()

    def acquire(self, *, max_bytes, ttl_seconds, metadata):
        return ProxyLease(lease_id='lease-1', token='token-1', proxy_url='http://proxy_filter:8900',
                          max_bytes=max_bytes, expires_at=9_999_999_999, stream_id='current-0')

    def stats(self, lease):
        return self.current

    def close(self, lease):
        return replace(self.current, closed=True)

    def acquire_request_permit(self, *, metadata, request_id):
        self.probe_permit_id = 'f' * 64 if self.probe else None
        return self.probe_permit_id or 'e' * 64

    def report_probe_passed(self):
        self.passed += 1

    @staticmethod
    def authenticated_proxy_url(lease):
        return f'http://lease:{lease.token}@proxy_filter:8900'


def _fetch(provider, responses, **kwargs):
    factory = _TlsFactory(responses)
    client = TransfermarktHttpClient(
        lease_provider=provider, client_factory=factory,
        traffic_ledger=SharedTrafficLedger(hard_provider_bytes=10**6, soft_provider_bytes=9 * 10**5),
        sleep_fn=lambda _: None)
    try:
        return client.fetch(URL, as_json=False, **kwargs)
    finally:
        client.close()


def test_a_successful_probe_is_answered_to_the_gateway_exactly_once():
    provider = _ProbingProvider(probe=True)
    assert _fetch(provider, [_Response(b'<html>page</html>')]).status is FetchStatus.OK
    assert provider.passed == 1 and provider.probe_permit_id is None


def test_an_ordinary_request_and_a_refused_probe_are_not_answered_as_passed():
    provider = _ProbingProvider(probe=False)
    assert _fetch(provider, [_Response(b'<html>page</html>')]).status is FetchStatus.OK
    assert provider.passed == 0 and provider.probe_permit_id is None
    provider = _ProbingProvider(probe=True)
    outcome = _fetch(provider, [_Response(b'blocked', status=403)], max_attempts=1)
    assert outcome.status is FetchStatus.BLOCKED and provider.passed == 0
    assert provider.probe_permit_id == 'f' * 64   # the refusal is the site feedback's to report


def test_an_unreachable_gateway_feedback_costs_time_not_the_page(caplog):
    provider = _ProbingProvider(probe=True)

    def fail():
        raise RuntimeError('control plane down')

    provider.report_probe_passed = fail
    assert _fetch(provider, [_Response(b'<html>page</html>')]).status is FetchStatus.OK
    assert 'could not report the source probe' in caplog.text
    assert provider.probe_permit_id is None   # not retried with the next request's permit


# ---- the provider's control call ----

class _ControlResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _ControlClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


def _provider_with_probe_permit(*rest):
    control = _ControlClient([
        _ControlResponse(201, dict(id='lease', token='lease-secret', proxy_url='http://gateway:8900',
                                   max_bytes=1000, expires_at=1000, stream_id='current-0')),
        _ControlResponse(200, dict(granted=True, permit_id='a' * 64, permit_token='p' * 32, probe=True)),
        _ControlResponse(200, dict(consumed=True, permit_id='a' * 64)),
        *rest,
    ])
    provider = ProxyFilterLeaseProvider('http://gateway:8899', control_client=control,
                                       control_token=CONTROL_TOKEN)
    metadata = dict(dag_id='dag_ingest_transfermarkt', run_id='run', task_id='task', canonical_url=URL)
    assert provider.acquire(max_bytes=1000, ttl_seconds=60, metadata=metadata).stream_id == 'current-0'
    provider.acquire_request_permit(metadata=metadata, request_id='probe')
    return provider, control, metadata


def test_probe_grant_is_bound_to_its_consumed_permit_and_reported_once():
    provider, control, metadata = _provider_with_probe_permit(_ControlResponse(200, dict(recorded=True)))
    assert provider.probe_permit_id == 'a' * 64
    provider.report_probe_passed()
    method, url, kwargs = control.calls[-1]
    assert (method, url) == ('POST', 'http://gateway:8899/v1/transfermarkt/request-permits/probe-ok')
    assert kwargs['json'] == dict(dag_id=metadata['dag_id'], run_id=metadata['run_id'], request_id='probe',
                                  lease_id='lease', lease_token='lease-secret', permit_id='a' * 64)


def test_gateway_without_source_stops_answers_404_and_nothing_is_raised():
    provider, _control, _metadata = _provider_with_probe_permit(
        _ControlResponse(404, dict(error='unknown control endpoint')))
    assert provider.report_probe_passed() is None
    provider, _control, _metadata = _provider_with_probe_permit(_ControlResponse(200, dict(recorded=False)))
    with pytest.raises(TrafficMeterError, match='not durably recorded'):
        provider.report_probe_passed()


def test_ordinary_grant_is_not_a_probe():
    provider, control, metadata = _provider_with_probe_permit()
    control.responses = [_ControlResponse(200, dict(granted=True, permit_id='b' * 64, permit_token='p' * 32)),
                         _ControlResponse(200, dict(consumed=True, permit_id='b' * 64))]
    provider.acquire_request_permit(metadata=metadata, request_id='plain')
    assert provider.probe_permit_id is None
