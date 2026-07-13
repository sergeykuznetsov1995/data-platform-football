"""Transfermarkt proxy-only transport, budgets, and traffic telemetry."""

from unittest.mock import MagicMock

import pytest

from scrapers.transfermarkt import (
    FetchStatus,
    ProxyRequiredError,
    TrafficBudgetExceeded,
    TransfermarktScraper,
)
from scrapers.transfermarkt.client import (
    DEFAULT_HEADERS,
    TransfermarktHttpClient,
    redact_sensitive,
)
from scrapers.utils.proxy_manager import ProxyManager


_UNSET = object()


class _FakeResp:
    def __init__(
        self,
        body: bytes,
        status: int = 200,
        json_value=_UNSET,
        headers=None,
    ):
        self.content = body
        self.status_code = status
        self._json_value = json_value
        self.headers = headers or {}

    @property
    def text(self):
        return self.content.decode('utf-8', errors='replace')

    def json(self):
        if isinstance(self._json_value, Exception):
            raise self._json_value
        if self._json_value is _UNSET:
            raise ValueError('no json configured')
        return self._json_value


class _FakeTlsClient:
    def __init__(self, responses):
        self._responses = responses
        self.get_calls = []
        self.closed = False

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def close(self):
        self.closed = True


class _ClientFactory:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.clients = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        client = _FakeTlsClient(self.responses)
        self.clients.append(client)
        return client


class _NoWaitLimiter:
    def __init__(self):
        self.calls = 0

    def acquire(self):
        self.calls += 1
        return True


def _manager(count=2):
    manager = ProxyManager(
        rotation_strategy='round_robin', cooldown_seconds=0,
    )
    for idx in range(count):
        manager.add_proxy(
            host=f'proxy-{idx}.invalid', port=8000 + idx,
            username=f'user{idx}', password=f'secret{idx}',
        )
    return manager


@pytest.fixture
def scraper():
    return TransfermarktScraper(leagues=['ENG-Premier League'], seasons=[2025])


@pytest.mark.unit
def test_zero_traffic_shape(scraper):
    stats = scraper.get_traffic_stats()
    assert stats['proxy_response_bytes'] == 0
    assert stats['proxy_response_mb'] == 0.0
    assert stats['decoded_response_body_bytes'] == 0
    assert stats['request_attempts'] == 0
    assert stats['retries'] == 0
    assert stats['status_counts'] == {}
    assert stats['budget_exhausted'] is False
    assert stats['circuit_state'] == 'closed'
    assert stats['top_traffic_urls'] == []


@pytest.mark.unit
def test_missing_proxy_fails_before_constructing_tls_client():
    factory = MagicMock()
    client = TransfermarktHttpClient(
        client_factory=factory, rate_limiter=_NoWaitLimiter(),
    )

    with pytest.raises(ProxyRequiredError, match='requires a residential proxy'):
        client.fetch('https://www.transfermarkt.us/a', as_json=False)

    factory.assert_not_called()


@pytest.mark.unit
def test_sticky_client_full_chrome133_headers_and_scalar_timeout():
    factory = _ClientFactory([
        _FakeResp(b'<html>a</html>'),
        _FakeResp(b'<html>b</html>'),
    ])
    limiter = _NoWaitLimiter()
    client = TransfermarktHttpClient(
        proxy='http://user:secret@proxy.invalid:8000',
        client_factory=factory,
        rate_limiter=limiter,
    )

    first = client.fetch('https://www.transfermarkt.us/a', as_json=False)
    second = client.fetch('https://www.transfermarkt.us/b', as_json=False)

    assert first.status == second.status == FetchStatus.OK
    assert len(factory.calls) == 1
    assert factory.calls[0]['client_identifier'] == 'chrome_133'
    assert factory.calls[0]['headers'] == dict(DEFAULT_HEADERS)
    assert 'Chrome/133.0.0.0' in factory.calls[0]['headers']['User-Agent']
    assert factory.calls[0]['headers']['Sec-CH-UA'].count('133') == 2
    assert [call[1] for call in factory.clients[0].get_calls] == [
        {'timeout': 12.0}, {'timeout': 12.0},
    ]
    assert limiter.calls == 2


@pytest.mark.unit
def test_403_rotates_proxy_once_and_records_retry():
    manager = _manager(2)
    factory = _ClientFactory([
        _FakeResp(b'blocked', status=403),
        _FakeResp(b'ok', status=200),
    ])
    client = TransfermarktHttpClient(
        proxy_manager=manager,
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
        sleep_fn=lambda _: None,
        random_fn=lambda: 0,
    )

    outcome = client.fetch('https://www.transfermarkt.us/a', as_json=False)

    assert outcome.status == FetchStatus.OK
    assert outcome.attempts == 2
    assert len(factory.calls) == 2
    assert factory.calls[0]['proxy'] != factory.calls[1]['proxy']
    stats = client.get_traffic_stats()
    assert stats['request_attempts'] == 2
    assert stats['retries'] == 1
    assert stats['status_counts'] == {'200': 1, '403': 1}
    proxies = manager._proxies
    assert sum(proxy.success_count for proxy in proxies) == 1
    assert sum(proxy.failure_count for proxy in proxies) == 1


@pytest.mark.unit
def test_plain_json_decode_error_is_schema_error_without_retry_or_proxy_ban():
    manager = _manager(2)
    factory = _ClientFactory([
        _FakeResp(b'not json', json_value=ValueError('invalid payload')),
    ])
    client = TransfermarktHttpClient(
        proxy_manager=manager,
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )

    outcome = client.fetch('https://www.transfermarkt.us/a', as_json=True)

    assert outcome.status == FetchStatus.SCHEMA_ERROR
    assert outcome.attempts == 1
    assert len(factory.calls) == 1
    assert sum(proxy.success_count for proxy in manager._proxies) == 1
    assert not any(proxy.is_banned for proxy in manager._proxies)


@pytest.mark.unit
def test_validator_exception_is_nonretryable_schema_error_and_proxy_success():
    manager = _manager(2)
    factory = _ClientFactory([
        _FakeResp(b'{"list":[]}', json_value={'list': []}),
    ])
    client = TransfermarktHttpClient(
        proxy_manager=manager,
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )

    def broken_validator(value):
        raise RuntimeError(
            'validator bug via http://private:secret@proxy.invalid:1'
        )

    outcome = client.fetch(
        'https://www.transfermarkt.us/a',
        as_json=True,
        validator=broken_validator,
    )

    assert outcome.status == FetchStatus.SCHEMA_ERROR
    assert outcome.attempts == 1
    assert 'private' not in outcome.error
    assert 'secret' not in outcome.error
    assert client.get_traffic_stats()['retries'] == 0
    assert sum(proxy.success_count for proxy in manager._proxies) == 1
    assert sum(proxy.failure_count for proxy in manager._proxies) == 0


@pytest.mark.unit
def test_tls_client_constructor_failure_is_redacted_and_accounted():
    manager = _manager(2)

    def broken_factory(**kwargs):
        raise RuntimeError(f"TLS init failed for {kwargs['proxy']}")

    client = TransfermarktHttpClient(
        proxy_manager=manager,
        client_factory=broken_factory,
        rate_limiter=_NoWaitLimiter(),
    )

    outcome = client.fetch(
        'https://www.transfermarkt.us/a', as_json=False, max_attempts=1,
    )

    assert outcome.status == FetchStatus.RETRY_EXHAUSTED
    assert outcome.attempts == 1
    assert 'user0' not in outcome.error and 'secret0' not in outcome.error
    stats = client.get_traffic_stats()
    assert stats['request_attempts'] == 1
    assert stats['failed_attempts'] == 1
    assert stats['status_counts'] == {'0': 1}
    assert sum(proxy.failure_count for proxy in manager._proxies) == 1


@pytest.mark.unit
def test_tls_constructor_failure_retries_on_different_proxy():
    manager = _manager(2)
    calls = []
    successful_client = _FakeTlsClient([_FakeResp(b'ok')])

    def flaky_factory(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise RuntimeError(f"TLS init failed for {kwargs['proxy']}")
        return successful_client

    client = TransfermarktHttpClient(
        proxy_manager=manager,
        client_factory=flaky_factory,
        rate_limiter=_NoWaitLimiter(),
        sleep_fn=lambda _: None,
    )

    outcome = client.fetch(
        'https://www.transfermarkt.us/a', as_json=False,
    )

    assert outcome.status == FetchStatus.OK
    assert outcome.attempts == 2
    assert calls[0]['proxy'] != calls[1]['proxy']
    stats = client.get_traffic_stats()
    assert stats['failed_attempts'] == 1
    assert stats['successful_attempts'] == 1
    assert stats['retries'] == 1


@pytest.mark.unit
def test_invalid_json_cloudflare_challenge_rotates_at_most_twice():
    manager = _manager(2)
    challenge = b'<html><title>Just a moment...</title></html>'
    factory = _ClientFactory([
        _FakeResp(challenge, json_value=ValueError('json')),
        _FakeResp(challenge, json_value=ValueError('json')),
    ])
    client = TransfermarktHttpClient(
        proxy_manager=manager,
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
        sleep_fn=lambda _: None,
    )

    outcome = client.fetch('https://www.transfermarkt.us/a', as_json=True)

    assert outcome.status == FetchStatus.BLOCKED
    assert outcome.attempts == 2
    assert len(factory.calls) == 2


@pytest.mark.unit
def test_404_is_failure_not_authoritative_empty():
    manager = _manager(1)
    factory = _ClientFactory([_FakeResp(b'not found', status=404)])
    client = TransfermarktHttpClient(
        proxy_manager=manager,
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )

    outcome = client.fetch(
        'https://www.transfermarkt.us/missing', as_json=True,
    )

    assert outcome.status == FetchStatus.RETRY_EXHAUSTED
    assert outcome.status_code == 404
    assert 'not an authoritative empty' in outcome.error
    assert outcome.attempts == 1
    assert manager._proxies[0].success_count == 1
    assert manager._proxies[0].failure_count == 0
    assert client.get_traffic_stats()['failed_attempts'] == 1


@pytest.mark.unit
def test_terminal_failure_is_avoided_on_next_logical_fetch():
    manager = _manager(2)
    first_proxy = manager._proxies[0]
    manager.get_proxy = MagicMock(return_value=first_proxy)
    factory = _ClientFactory([
        _FakeResp(b'bad gateway', status=500),
        _FakeResp(b'ok', status=200),
    ])
    client = TransfermarktHttpClient(
        proxy_manager=manager,
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )

    failed = client.fetch(
        'https://www.transfermarkt.us/first', as_json=False, max_attempts=1,
    )
    succeeded = client.fetch(
        'https://www.transfermarkt.us/second', as_json=False, max_attempts=1,
    )

    assert failed.status == FetchStatus.RETRY_EXHAUSTED
    assert succeeded.status == FetchStatus.OK
    assert factory.calls[0]['proxy'] != factory.calls[1]['proxy']


@pytest.mark.unit
def test_failed_single_explicit_proxy_is_never_reused():
    factory = _ClientFactory([_FakeResp(b'bad gateway', status=500)])
    client = TransfermarktHttpClient(
        proxy='http://user:secret@proxy.invalid:8000',
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )

    failed = client.fetch(
        'https://www.transfermarkt.us/first', as_json=False, max_attempts=1,
    )
    with pytest.raises(ProxyRequiredError, match='requires a residential proxy'):
        client.fetch(
            'https://www.transfermarkt.us/second', as_json=False,
            max_attempts=1,
        )

    assert failed.status == FetchStatus.RETRY_EXHAUSTED
    assert len(factory.calls) == 1


@pytest.mark.unit
def test_parser_schema_failures_open_five_endpoint_circuit():
    factory = _ClientFactory([
        *[_FakeResp(b'<table class="items"></table>') for _ in range(5)],
        _FakeResp(b'should not be fetched'),
    ])
    client = TransfermarktHttpClient(
        proxy='http://user:secret@proxy.invalid:8000',
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )
    def validator(value):
        return 'missing player links'

    outcomes = [
        client.fetch(
            f'https://www.transfermarkt.us/{idx}',
            as_json=False,
            max_attempts=1,
            validator=validator,
        )
        for idx in range(5)
    ]
    blocked = client.fetch(
        'https://www.transfermarkt.us/6', as_json=False, validator=validator,
    )

    assert all(outcome.status == FetchStatus.SCHEMA_ERROR for outcome in outcomes)
    assert blocked.status == FetchStatus.RETRY_EXHAUSTED
    assert 'circuit is open' in blocked.error
    assert client.get_traffic_stats()['circuit_state'] == 'open'
    assert sum(len(item.get_calls) for item in factory.clients) == 5


@pytest.mark.unit
def test_decoded_body_budget_raises_immediately_and_reports_state():
    factory = _ClientFactory([_FakeResp(b'abcd')])
    client = TransfermarktHttpClient(
        proxy='http://proxy.invalid:8000',
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )
    client.set_decoded_body_budget(3)

    with pytest.raises(TrafficBudgetExceeded, match='decoded-body budget'):
        client.fetch('https://www.transfermarkt.us/a', as_json=False)

    stats = client.get_traffic_stats()
    assert stats['decoded_response_body_bytes'] == 4
    assert stats['budget_exhausted'] is True
    assert sum(len(item.get_calls) for item in factory.clients) == 1


@pytest.mark.unit
def test_exact_decoded_cap_blocks_next_request_before_io_no_n_plus_one():
    factory = _ClientFactory([_FakeResp(b'abc'), _FakeResp(b'not-fetched')])
    client = TransfermarktHttpClient(
        proxy='http://proxy.invalid:8000',
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )
    client.set_decoded_body_budget(3)

    assert client.fetch(
        'https://www.transfermarkt.us/at-cap', as_json=False,
    ).status == FetchStatus.OK
    with pytest.raises(TrafficBudgetExceeded, match='3/3 bytes'):
        client.fetch('https://www.transfermarkt.us/n-plus-one', as_json=False)

    assert client.get_traffic_stats()['decoded_response_body_bytes'] == 3
    assert sum(len(item.get_calls) for item in factory.clients) == 1


@pytest.mark.unit
def test_request_attempt_cap_blocks_n_plus_one_before_io():
    factory = _ClientFactory([_FakeResp(b'ok'), _FakeResp(b'not-fetched')])
    client = TransfermarktHttpClient(
        proxy='http://proxy.invalid:8000',
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )
    client.begin_request_scope(request_attempt_budget=1)

    assert client.fetch(
        'https://www.transfermarkt.us/at-cap', as_json=False,
    ).status == FetchStatus.OK
    with pytest.raises(TrafficBudgetExceeded, match='1/1'):
        client.fetch('https://www.transfermarkt.us/n-plus-one', as_json=False)

    assert sum(len(item.get_calls) for item in factory.clients) == 1


@pytest.mark.unit
def test_cycle_budget_survives_operation_scope_reset():
    factory = _ClientFactory([_FakeResp(b'abc'), _FakeResp(b'def')])
    client = TransfermarktHttpClient(
        proxy='http://proxy.invalid:8000',
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )
    client.set_cycle_decoded_body_budget(5)
    client.set_decoded_body_budget(4)
    assert client.fetch(
        'https://www.transfermarkt.us/phase-one', as_json=False,
    ).status == FetchStatus.OK

    client.set_decoded_body_budget(4)
    with pytest.raises(TrafficBudgetExceeded, match='scope=cycle'):
        client.fetch('https://www.transfermarkt.us/phase-two', as_json=False)

    stats = client.get_traffic_stats()
    assert stats['decoded_response_body_bytes'] == 6
    assert stats['cycle_decoded_body_budget_bytes'] == 5
    assert sum(len(item.get_calls) for item in factory.clients) == 2


@pytest.mark.unit
def test_dead_soft_budget_api_is_absent():
    client = TransfermarktHttpClient(
        proxy='http://proxy.invalid:8000', rate_limiter=_NoWaitLimiter(),
    )

    assert not hasattr(client, 'begin_budget_scope')
    assert not hasattr(client, 'set_expected_requests')


@pytest.mark.unit
def test_proxy_credentials_are_redacted_from_transport_error():
    factory = _ClientFactory([
        RuntimeError('failed via http://private-user:private-pass@proxy.invalid:1'),
    ])
    client = TransfermarktHttpClient(
        proxy='http://private-user:private-pass@proxy.invalid:1',
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )

    outcome = client.fetch(
        'https://www.transfermarkt.us/a', as_json=False, max_attempts=1,
    )

    assert outcome.status == FetchStatus.RETRY_EXHAUSTED
    assert 'private-user' not in outcome.error
    assert 'private-pass' not in outcome.error
    assert '****:****@proxy.invalid:1' in outcome.error
    rendered_stats = str(client.get_traffic_stats())
    assert 'private-user' not in rendered_stats
    assert 'private-pass' not in rendered_stats


@pytest.mark.unit
def test_socks_proxy_credentials_are_redacted():
    rendered = redact_sensitive(
        'failed socks5://private-user:private-pass@proxy.invalid:1080'
    )

    assert rendered == 'failed socks5://****:****@proxy.invalid:1080'


@pytest.mark.unit
def test_tm_init_skips_large_pool_prevalidation(tmp_path, monkeypatch):
    proxy_file = tmp_path / 'proxies.txt'
    proxy_file.write_text('\n'.join(
        f'proxy-{idx}.invalid:{8000 + idx}:user{idx}:pass{idx}'
        for idx in range(11)
    ))
    validate = MagicMock(side_effect=AssertionError('must not validate'))
    monkeypatch.setattr(ProxyManager, 'validate_proxies', validate)

    scraper = TransfermarktScraper(proxy_file=str(proxy_file))

    assert scraper._proxy_manager.total_count == 11
    assert scraper._proxy_manager.config.rotation_strategy == 'random'
    validate.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize('requested,expected', [(1000, 12), (0, 1), (-5, 1)])
def test_rate_limit_override_is_clamped_to_safe_range(requested, expected):
    scraper = TransfermarktScraper(rate_limit=requested)

    assert scraper._rate_limiter.config.max_requests == expected
    assert scraper._rate_limiter.config.burst_size == 1


@pytest.mark.unit
def test_200_empty_list_is_authoritative_valid_empty_fetch_outcome(monkeypatch):
    scraper = TransfermarktScraper(proxy='http://user:pass@proxy.invalid:1')
    factory = _ClientFactory([
        _FakeResp(b'{"list":[]}', json_value={'list': []}),
    ])
    scraper._http_client = TransfermarktHttpClient(
        proxy='http://user:pass@proxy.invalid:1',
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )

    frame = scraper.read_market_value_points(
        'ENG-Premier League', 2025, player_ids=['42'],
    )

    assert frame.empty
    outcome = scraper.get_fetch_outcomes()['market_value_points']['42']
    assert outcome['status'] == 'valid_empty'
    assert outcome['status_code'] == 200
    assert outcome['row_count'] == 0
    assert outcome['payload_hash']


@pytest.mark.unit
def test_career_404_cannot_be_negative_cached_or_deleted():
    scraper = TransfermarktScraper(proxy='http://user:pass@proxy.invalid:1')
    factory = _ClientFactory([_FakeResp(b'not found', status=404)])
    scraper._http_client = TransfermarktHttpClient(
        proxy='http://user:pass@proxy.invalid:1',
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
    )

    frame = scraper.read_market_value_points(
        'ENG-Premier League', 2025, player_ids=['404-player'],
    )

    assert frame.empty
    outcome = scraper.get_fetch_outcomes()[
        'market_value_points'
    ]['404-player']
    assert outcome['status'] == 'retry_exhausted'
    assert outcome['status_code'] == 404
    assert outcome['row_count'] == 0


@pytest.mark.unit
def test_the_breaker_reopens_after_the_source_has_had_time_to_recover():
    # The source fails in waves. A breaker that never reopens turns a wave into
    # the end of the entity: every later page is refused untried, and a run that
    # already paid for hundreds of pages dies holding them.
    clock = {'now': 1_000.0}
    manager = _manager(2)
    factory = _ClientFactory([_FakeResp(b'boom', status=502)] * 5 + [
        _FakeResp(b'<html>back</html>'),
    ])
    client = TransfermarktHttpClient(
        proxy_manager=manager,
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
        sleep_fn=lambda _: None,
        random_fn=lambda: 0,
        time_fn=lambda: clock['now'],
        circuit_failures=5,
        circuit_reset_seconds=120.0,
    )

    for _ in range(5):
        client.fetch('https://www.transfermarkt.com/a', as_json=False,
                     max_attempts=1)
    assert client.get_traffic_stats()['circuit_state'] == 'open'

    still_open = client.fetch('https://www.transfermarkt.com/b', as_json=False)
    assert still_open.status == FetchStatus.RETRY_EXHAUSTED
    assert len(factory.calls) == 5  # refused without touching the network

    clock['now'] += 121.0
    recovered = client.fetch('https://www.transfermarkt.com/b', as_json=False,
                             max_attempts=1)

    assert recovered.status == FetchStatus.OK
    assert client.get_traffic_stats()['circuit_state'] == 'closed'


@pytest.mark.unit
def test_a_failed_probe_reopens_the_breaker_instead_of_starting_a_storm():
    clock = {'now': 1_000.0}
    manager = _manager(2)
    factory = _ClientFactory([_FakeResp(b'boom', status=502)] * 7)
    client = TransfermarktHttpClient(
        proxy_manager=manager,
        client_factory=factory,
        rate_limiter=_NoWaitLimiter(),
        sleep_fn=lambda _: None,
        random_fn=lambda: 0,
        time_fn=lambda: clock['now'],
        circuit_failures=5,
        circuit_reset_seconds=120.0,
    )
    for _ in range(5):
        client.fetch('https://www.transfermarkt.com/a', as_json=False,
                     max_attempts=1)

    clock['now'] += 121.0
    probe = client.fetch('https://www.transfermarkt.com/b', as_json=False,
                         max_attempts=1)
    assert probe.status == FetchStatus.RETRY_EXHAUSTED
    assert len(factory.calls) == 6  # the probe, and only the probe

    after = client.fetch('https://www.transfermarkt.com/c', as_json=False)
    assert after.status == FetchStatus.RETRY_EXHAUSTED
    assert len(factory.calls) == 6  # shut again, at the cost of one request
