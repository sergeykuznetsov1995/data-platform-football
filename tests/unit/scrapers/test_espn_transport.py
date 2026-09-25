"""Frozen, network-free contracts for the native ESPN HTTP boundary."""

from __future__ import annotations

import gzip
import io
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests
from pyarrow import fs

from scrapers.espn.gate import TransportGate, load_transport_policy
from scrapers.espn.raw_store import EspnRawStore, RawTargetCorrupt
from scrapers.espn.transport import (
    AllOriginsBlocked,
    AmbientProxyError,
    DirectTransportError,
    EndpointType,
    EspnHttpClient,
    HttpStatusError,
    OriginBlocked,
    ResponseTooLarge,
    RetryExhausted,
    canonicalize_target,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "espn" / "probes"
WEB = "https://site.web.api.espn.com"
SITE = "https://site.api.espn.com"


class FakeResponse:
    def __init__(self, status: int, body: bytes = b"", headers=None):
        self.status_code = status
        self.headers = headers or {}
        self.raw = io.BytesIO(body)
        self.closed = False

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}
        self.proxies = {"https": "http://forbidden.invalid"}
        self.trust_env = True

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class PartialTimeoutRaw:
    def __init__(self, chunk: bytes):
        self.chunk = chunk
        self.calls = 0

    def read(self, _size):
        self.calls += 1
        if self.calls == 1:
            return self.chunk
        raise requests.Timeout("secret=must-not-leak")


def _clear_proxy_env(monkeypatch):
    for name in (
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ):
        monkeypatch.delenv(name, raising=False)


class GateClock:
    def __init__(self):
        self.now = datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc)
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)


def _gate(tmp_path, clock=None, lane="live"):
    clock = clock or GateClock()
    gate = TransportGate(
        load_transport_policy(),
        tmp_path / "gate" / "gate.json",
        lane,
        step_ceiling=0,
        utcnow_fn=clock,
        sleep_fn=clock.sleep,
    )
    gate.clock = clock
    return gate


def _client(monkeypatch, tmp_path, responses, gate=None, **kwargs):
    _clear_proxy_env(monkeypatch)
    store = EspnRawStore.from_uri(tmp_path.as_uri())
    session = FakeSession(responses)
    sleeps = []
    client = EspnHttpClient(
        store,
        gate=gate or _gate(tmp_path),
        session=session,
        sleep_fn=sleeps.append,
        monotonic_fn=lambda: 0.0,
        utcnow_fn=lambda: datetime(2026, 7, 31, tzinfo=timezone.utc),
        **kwargs,
    )
    return client, session, sleeps, store


@pytest.mark.unit
def test_non_positive_bounds_are_rejected_and_pace_lives_in_the_gate(
    monkeypatch, tmp_path
):
    client, _, _, _ = _client(monkeypatch, tmp_path, [], max_attempts=6)
    assert client.max_attempts == 6 and client.lane == "live"
    for name in ("rate_per_minute", "burst", "request_permit", "budget"):
        assert not hasattr(client, name)
    with pytest.raises(ValueError, match="positive"):
        _client(monkeypatch, tmp_path, [], max_attempts=0)

@pytest.mark.unit
def test_success_is_raw_first_measured_and_cached(monkeypatch, tmp_path):
    body = b'{"events":[{"id":"1"}]}'
    encoded = gzip.compress(body, mtime=0)
    client, session, _, store = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(200, encoded, {"Content-Encoding": "gzip"})],
    )

    result = client.fetch_json(
        "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard",
        EndpointType.SCOREBOARD,
    )

    assert result.json_data == {"events": [{"id": "1"}]}
    assert result.body == body
    assert result.attempts == 1 and not result.cache_hit
    assert result.direct_bytes == len(encoded) and result.proxy_bytes == 0
    assert result.raw_uri.endswith(".json.gz") and result.content_hash
    client.flush()
    assert store.load(result.target)[0] == body
    assert session.trust_env is False and session.proxies == {}
    assert session.calls[0][1]["timeout"] == (5.0, 20.0)
    assert session.calls[0][1]["allow_redirects"] is False
    ledger = client.ledger[-1]
    assert ledger.endpoint is EndpointType.SCOREBOARD
    assert ledger.url_fingerprint == result.target.url_fingerprint
    assert ledger.status == 200 and ledger.disposition == "success"
    assert ledger.proxy_bytes == 0 and ledger.raw_uri == result.raw_uri

    cached = client.fetch_json(
        result.target.canonical_url,
        EndpointType.SCOREBOARD,
    )
    assert cached.cache_hit and cached.attempts == cached.direct_bytes == 0
    assert len(session.calls) == 1


@pytest.mark.unit
def test_replay_has_zero_network_calls_and_corrupt_cache_refetches(
    monkeypatch, tmp_path
):
    url = "https://site.api.espn.com/apis/site/v2/sports/soccer/summary?event=9"
    client, session, _, store = _client(
        monkeypatch, tmp_path, [FakeResponse(200, b'{"header":{}}')]
    )
    target = canonicalize_target(url)
    record = store.store(target, EndpointType.SUMMARY, b'{"old":true}')

    replayed = client.replay_json(url, EndpointType.SUMMARY)
    assert replayed.cache_hit and replayed.attempts == 0
    assert len(session.calls) == 0

    store._write_bytes(record.blob_key, b"not-gzip")
    with pytest.raises(RawTargetCorrupt):
        store.load(target)
    fetched = client.fetch_json(
        url, EndpointType.SUMMARY, force_refresh=False
    )
    assert fetched.json_data == {"header": {}}
    assert len(session.calls) == 1
    client.flush()
    assert store.load(target)[0] == b'{"header":{}}'


@pytest.mark.unit
def test_force_refresh_daily_get_ignores_existing_mutable_target_alias(
    monkeypatch, tmp_path
):
    url = "https://site.api.espn.com/apis/site/v2/sports/soccer/summary?event=9"
    client, session, _, store = _client(
        monkeypatch, tmp_path, [FakeResponse(200, b'{"fresh":true}')]
    )
    store.store(canonicalize_target(url), EndpointType.SUMMARY, b'{"stale":true}')

    fetched = client.fetch_json(
        url,
        EndpointType.SUMMARY,
        force_refresh=True,
    )

    assert fetched.json_data == {"fresh": True}
    assert fetched.cache_hit is False
    assert len(session.calls) == 1


@pytest.mark.unit
@pytest.mark.parametrize("status", [408, 425, 429, 500, 503])
def test_retryable_statuses_honor_retry_after(monkeypatch, tmp_path, status):
    client, session, sleeps, _ = _client(
        monkeypatch,
        tmp_path,
        [
            FakeResponse(status, b"busy", {"Retry-After": "3"}),
            FakeResponse(200, b'{"ok":true}'),
        ],
    )
    result = client.fetch_json(
        "https://site.api.espn.com/apis/site/v2/sports/soccer/summary?event=10",
        "Summary",
    )
    assert result.attempts == 2
    assert sleeps == [3.0]
    assert len(session.calls) == 2


@pytest.mark.unit
def test_timeout_retries_but_nonretryable_4xx_fails_once(monkeypatch, tmp_path):
    timeout_client, timeout_session, _, _ = _client(
        monkeypatch,
        tmp_path / "timeout",
        [requests.Timeout("late"), FakeResponse(200, b'{"ok":true}')],
    )
    result = timeout_client.fetch_json(
        "https://site.api.espn.com/apis/site/v2/sports/soccer/leagues",
        "catalog",
    )
    assert result.attempts == 2 and len(timeout_session.calls) == 2

    terminal, session, _, _ = _client(
        monkeypatch,
        tmp_path / "terminal",
        [FakeResponse(404, b"missing"), FakeResponse(200, b"{}")],
    )
    with pytest.raises(HttpStatusError) as exc_info:
        terminal.fetch_json(
            "https://site.api.espn.com/apis/site/v2/sports/soccer/leagues",
            "catalog",
        )
    assert exc_info.value.status == 404
    assert len(session.calls) == 1

    connection, connection_session, _, _ = _client(
        monkeypatch,
        tmp_path / "connection",
        [requests.ConnectionError("offline"), FakeResponse(200, b"{}")],
    )
    with pytest.raises(DirectTransportError):
        connection.fetch_json(
            "https://site.api.espn.com/apis/site/v2/sports/soccer/leagues",
            "catalog",
        )
    assert len(connection_session.calls) == 1


@pytest.mark.unit
def test_pace_is_the_gate_step_s0_one_request_per_second(monkeypatch, tmp_path):
    gate = _gate(tmp_path)
    client, session, sleeps, _ = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(200, b"{}") for _ in range(5)],
        gate=gate,
    )

    for index in range(5):
        client.fetch_json(
            f"https://site.api.espn.com/catalog?page={index}",
            "catalog",
            force_refresh=True,
        )

    assert len(session.calls) == 5
    assert sleeps == []
    assert gate.clock.sleeps == [pytest.approx(1.0)] * 4

@pytest.mark.unit
def test_each_real_attempt_takes_one_gate_permit(monkeypatch, tmp_path):
    gate = _gate(tmp_path)
    client, session, _, _ = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(503), FakeResponse(200, b"{}")],
        gate=gate,
    )

    client.fetch_json(
        "https://site.api.espn.com/apis/site/v2/sports/soccer/leagues",
        "catalog",
        force_refresh=True,
    )

    assert gate.snapshot()["daily"]["live"]["requests"] == 2
    assert len(session.calls) == 2
    assert client.ledger[-1].origin_attempts == ((WEB, 503), (WEB, 200))

@pytest.mark.unit
def test_oversize_response_fails_closed(monkeypatch, tmp_path):
    oversize, _, _, _ = _client(
        monkeypatch,
        tmp_path / "large",
        [FakeResponse(200, b"12345")],
        response_cap_bytes=4,
    )
    with pytest.raises(ResponseTooLarge):
        oversize.fetch_json(
            "https://site.api.espn.com/apis/site/v2/sports/soccer/leagues",
            "catalog",
        )

@pytest.mark.unit
def test_ambient_proxy_and_non_https_are_rejected(monkeypatch, tmp_path):
    _clear_proxy_env(monkeypatch)
    monkeypatch.setenv("https_proxy", "http://proxy.invalid")
    with pytest.raises(AmbientProxyError):
        EspnHttpClient(
            EspnRawStore.from_uri(tmp_path.as_uri()),
            gate=_gate(tmp_path),
            session=FakeSession([]),
        )

    _clear_proxy_env(monkeypatch)
    client = EspnHttpClient(
        EspnRawStore.from_uri(tmp_path.as_uri()),
        gate=_gate(tmp_path),
        session=FakeSession([]),
    )
    with pytest.raises(ValueError, match="HTTPS"):
        client.fetch_json("http://site.api.espn.com/summary?event=1", "summary")


@pytest.mark.unit
def test_exact_defaults(monkeypatch, tmp_path):
    client, _, _, _ = _client(monkeypatch, tmp_path, [])
    assert client.connect_timeout == 5.0
    assert client.read_timeout == 20.0
    assert client.response_cap_bytes == 16 * 1024 * 1024
    assert client.max_attempts == 4
    assert client.session.headers["User-Agent"] == (
        "data-platform-football/espn-native-v2"
    )
    assert client.session.headers["Accept-Encoding"] == "gzip, deflate"

@pytest.mark.unit
def test_raw_store_requires_configuration_and_content_addresses(monkeypatch, tmp_path):
    monkeypatch.delenv("ESPN_RAW_STORE_URI", raising=False)
    with pytest.raises(Exception, match="ESPN_RAW_STORE_URI"):
        EspnRawStore.from_env()

    store = EspnRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("https://site.api.espn.com/catalog")
    first = store.store(target, EndpointType.CATALOG, b'{"a":1}')
    second = store.store(target, EndpointType.CATALOG, b'{"a":1}')
    assert first.content_hash == second.content_hash
    assert first.blob_key == second.blob_key
    assert (
        json.loads(store._read_bytes(store._alias_key(target.url_fingerprint)))[
            "content_hash"
        ]
        == first.content_hash
    )


@pytest.mark.unit
def test_exact_blob_read_ignores_moved_alias_and_rejects_uri_hash_drift(tmp_path):
    store = EspnRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target("https://site.api.espn.com/summary?event=9")
    first = store.store(target, EndpointType.SUMMARY, b'{"version":1}')
    store.store(target, EndpointType.SUMMARY, b'{"version":2}')

    assert store.load(target)[0] == b'{"version":2}'
    assert store.load_exact(first.raw_uri, first.content_hash) == b'{"version":1}'
    with pytest.raises(RawTargetCorrupt, match="URI"):
        store.load_exact(first.raw_uri, "0" * 64)


@pytest.mark.unit
def test_partial_read_timeout_charges_wire_bytes_and_retries(monkeypatch, tmp_path):
    partial = FakeResponse(200)
    partial.raw = PartialTimeoutRaw(b"123")
    gate = _gate(tmp_path)
    client, session, _, _ = _client(
        monkeypatch,
        tmp_path,
        [partial, FakeResponse(200, b'{"ok":true}')],
        gate=gate,
    )
    result = client.fetch_json("https://site.api.espn.com/catalog", "catalog")
    assert result.attempts == 2
    assert result.direct_bytes == 3 + len(b'{"ok":true}')
    assert gate.snapshot()["daily"]["live"]["bytes"] == result.direct_bytes
    assert len(session.calls) == 2


@pytest.mark.unit
def test_retryable_status_is_classified_before_bad_body(monkeypatch, tmp_path):
    client, _, sleeps, store = _client(
        monkeypatch,
        tmp_path,
        [
            FakeResponse(
                503,
                b"not-gzip",
                {"Content-Encoding": "gzip", "Retry-After": "4"},
            ),
            FakeResponse(200, b'{"ok":true}'),
        ],
    )
    result = client.fetch_json("https://site.api.espn.com/catalog", "catalog")
    assert result.attempts == 2 and sleeps == [4.0]
    client.flush()
    assert store.load(result.target)[0] == b'{"ok":true}'


@pytest.mark.unit
@pytest.mark.parametrize(
    ("retry_after", "expected"),
    [
        ("120", 60.0),
        ("nan", 1.0),
        ("inf", 1.0),
        ("-1", 1.0),
        ("+2", 1.0),
        ("01", 1.0),
        ("Thu, 31 Jul 2026 00:02:00 GMT", 60.0),
        ("Wed, 30 Jul 2026 00:00:00 GMT", 1.0),
    ],
)
def test_retry_after_is_canonical_finite_and_capped(
    monkeypatch, tmp_path, retry_after, expected
):
    client, _, sleeps, _ = _client(
        monkeypatch,
        tmp_path,
        [
            FakeResponse(429, b"", {"Retry-After": retry_after}),
            FakeResponse(200, b"{}"),
        ],
    )
    client.fetch_json("https://site.api.espn.com/catalog", "catalog")
    assert sleeps == [expected]


@pytest.mark.unit
@pytest.mark.parametrize(
    "encoded",
    [
        gzip.compress(b"{}", mtime=0)[:-2],
        gzip.compress(b"{}", mtime=0) + b"trailing",
        gzip.compress(b"{}", mtime=0) + gzip.compress(b"{}", mtime=0),
    ],
)
def test_gzip_requires_one_complete_member(monkeypatch, tmp_path, encoded):
    client, _, _, store = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(200, encoded, {"Content-Encoding": "gzip"})],
        max_attempts=1,
    )
    target = canonicalize_target("https://site.api.espn.com/catalog")
    with pytest.raises(DirectTransportError):
        client.fetch_json(target.canonical_url, "catalog")
    assert not store.has_target(target)


@pytest.mark.unit
def test_secrets_never_reach_alias_ledger_exception_or_repr(monkeypatch, tmp_path):
    secret = "TOP-SECRET-123"
    url = f"https://site.api.espn.com/catalog?apikey={secret}&event=7"
    client, _, _, store = _client(monkeypatch, tmp_path, [FakeResponse(200, b"{}")])
    result = client.fetch_json(url, "catalog")
    client.flush()
    alias = store._read_bytes(store._alias_key(result.target.url_fingerprint))
    combined = alias + repr(result).encode() + repr(client.ledger).encode()
    assert secret.encode() not in combined

    with pytest.raises(ValueError) as exc_info:
        canonicalize_target(f"https://user:{secret}@site.api.espn.com/catalog")
    assert secret not in str(exc_info.value) and secret not in repr(exc_info.value)


@pytest.mark.unit
def test_corrupt_alias_is_cache_miss_and_nonlocal_filesystem_is_supported(
    monkeypatch, tmp_path
):
    url = "https://site.api.espn.com/catalog"
    client, session, _, store = _client(
        monkeypatch, tmp_path, [FakeResponse(200, b'{"new":true}')]
    )
    target = canonicalize_target(url)
    store.store(target, EndpointType.CATALOG, b'{"old":true}')
    store._write_bytes(store._alias_key(target.url_fingerprint), b"not-json")
    assert client.fetch_json(url, "catalog").json_data == {"new": True}
    assert len(session.calls) == 1

    remote = EspnRawStore(
        fs._MockFileSystem(), "bucket/espn", uri_prefix="s3://bucket/espn"
    )
    record = remote.store(target, EndpointType.CATALOG, b"{}")
    assert remote.load(target)[0] == b"{}"
    assert record.raw_uri.startswith("s3://bucket/espn/")


@pytest.mark.unit
def test_off_domain_https_and_mixed_case_proxy_are_rejected(monkeypatch, tmp_path):
    _clear_proxy_env(monkeypatch)
    with pytest.raises(ValueError):
        canonicalize_target("https://example.com/catalog?token=secret")
    monkeypatch.setenv("HtTp_PrOxY", "http://proxy.invalid")
    with pytest.raises(AmbientProxyError):
        EspnHttpClient(
            EspnRawStore.from_uri(tmp_path.as_uri()),
            gate=_gate(tmp_path),
            session=FakeSession([]),
        )


def _akamai_403():
    body = (FIXTURES / "site_api_403_akamai.body").read_bytes()
    return FakeResponse(403, body, {"Content-Type": "text/html"})


SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary"


@pytest.mark.unit
def test_site_api_url_goes_to_web_api_primary_keeping_logical_identity(
    monkeypatch, tmp_path
):
    body = b'{"header":{}}'
    encoded = gzip.compress(body, mtime=0)
    client, session, _, store = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(200, encoded, {"Content-Encoding": "gzip"})],
    )
    target = canonicalize_target(SUMMARY, {"event": 740880})
    result = client.fetch_json(SUMMARY, EndpointType.SUMMARY, {"event": 740880})
    client.flush()

    assert session.calls[0][0] == target.canonical_url.replace(SITE, WEB, 1)
    assert result.target.url_fingerprint == target.url_fingerprint
    assert result.transport_origin == WEB
    assert result.attempts == 1
    stored_body, record = store.load(target)
    assert stored_body == body and record.transport_origin == WEB


@pytest.mark.unit
def test_direct_web_api_request_is_allowed(monkeypatch, tmp_path):
    url = "https://site.web.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary"
    client, session, _, _ = _client(monkeypatch, tmp_path, [FakeResponse(200, b"{}")])
    result = client.fetch_json(url, EndpointType.SUMMARY, {"event": 9})
    assert result.transport_origin == WEB
    assert session.calls[0][0].startswith(WEB + "/")
    replayed = client.replay_json(url, EndpointType.SUMMARY, {"event": 9})
    assert replayed.cache_hit and len(session.calls) == 1


@pytest.mark.unit
def test_akamai_403_defers_request_without_retry(monkeypatch, tmp_path):
    gate = _gate(tmp_path)
    client, session, sleeps, store = _client(
        monkeypatch,
        tmp_path,
        [_akamai_403(), FakeResponse(200, b"{}")],
        gate=gate,
    )
    with pytest.raises(OriginBlocked) as exc_info:
        client.fetch_json(SUMMARY, EndpointType.SUMMARY, {"event": 9})

    # The reserve starts closed, so the whole site cluster is blocked.
    assert isinstance(exc_info.value, AllOriginsBlocked)
    entry = exc_info.value.ledger_entry
    assert entry.disposition == "blocked_deferred"
    assert (entry.status, entry.attempts, entry.host) == (
        403,
        1,
        "site.web.api.espn.com",
    )
    assert entry.origin_attempts == ((WEB, 403),)
    assert len(session.calls) == 1 and sleeps == []
    assert gate.snapshot()["daily"]["live"]["requests"] == 1
    client.flush()
    assert not store.has_target(canonicalize_target(SUMMARY, {"event": 9}))

    with pytest.raises(AllOriginsBlocked) as blocked:
        client.fetch_json(SUMMARY, EndpointType.SUMMARY, {"event": 10})
    assert blocked.value.ledger_entry.attempts == 0
    assert len(session.calls) == 1


@pytest.mark.unit
def test_primary_403_moves_next_requests_to_open_reserve_one_permit_each(
    monkeypatch, tmp_path
):
    """Criterion 1 at the client: no second attempt, the reserve serves next."""
    gate = _gate(tmp_path)
    gate.choose_origin("site")
    gate.clock.now += timedelta(days=1)  # the reserve's daily probe is due
    client, session, _, _ = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(200, b"{}"), _akamai_403()]
        + [FakeResponse(200, b"{}") for _ in range(3)],
        gate=gate,
    )
    probe = client.fetch_json(SUMMARY, EndpointType.SUMMARY, {"event": 0})
    assert probe.transport_origin == SITE  # the reserve's probe answered 200

    with pytest.raises(OriginBlocked) as exc_info:
        client.fetch_json(SUMMARY, EndpointType.SUMMARY, {"event": 1})
    assert not isinstance(exc_info.value, AllOriginsBlocked)
    assert exc_info.value.ledger_entry.attempts == 1

    before = gate.snapshot()["daily"]["live"]["requests"]
    for event in (2, 3, 4):
        result = client.fetch_json(SUMMARY, EndpointType.SUMMARY, {"event": event})
        assert result.attempts == 1 and result.transport_origin == SITE
    assert gate.snapshot()["daily"]["live"]["requests"] - before == 3
    assert [call[0].split("/apis")[0] for call in session.calls] == [
        SITE,
        WEB,
        SITE,
        SITE,
        SITE,
    ]

@pytest.mark.unit
def test_retryable_failure_retries_same_origin_with_new_permit(
    monkeypatch, tmp_path
):
    gate = _gate(tmp_path)
    client, session, sleeps, _ = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(502), requests.Timeout("late"), FakeResponse(200, b"{}")],
        gate=gate,
    )
    result = client.fetch_json(SUMMARY, EndpointType.SUMMARY, {"event": 9})
    assert result.attempts == 3
    assert [call[0].split("/apis")[0] for call in session.calls] == [WEB] * 3
    assert sleeps == [1.0, 2.0]
    assert client.ledger[-1].origin_attempts == ((WEB, 502), (WEB, None), (WEB, 200))
    assert gate.snapshot()["daily"]["live"]["requests"] == 3


@pytest.mark.unit
def test_retryable_failure_stays_bounded(monkeypatch, tmp_path):
    client, session, _, _ = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(503) for _ in range(5)],
    )
    with pytest.raises(RetryExhausted) as exc_info:
        client.fetch_json(SUMMARY, EndpointType.SUMMARY, {"event": 9})
    assert exc_info.value.ledger_entry.attempts == 4
    assert exc_info.value.ledger_entry.disposition == "retry_exhausted"
    assert len(session.calls) == 4


@pytest.mark.unit
def test_ledger_records_address_status_bytes_encoding_latency_step_lane(
    monkeypatch, tmp_path
):
    """Criterion 4: every request leaves one ledger entry with its address."""
    body = b'{"header":{"id":"740880"}}'
    encoded = gzip.compress(body, mtime=0)
    client, _, _, _ = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(200, encoded, {"Content-Encoding": "gzip"})],
    )
    client.fetch_json(SUMMARY, EndpointType.SUMMARY, {"event": 740880})
    entry = client.ledger[-1]
    assert entry.host == "site.web.api.espn.com"
    assert entry.transport_origin == WEB
    assert entry.status == 200 and entry.attempts == 1
    assert entry.direct_bytes == len(encoded)
    assert entry.content_encoding == "gzip"
    assert entry.latency_ms == 0.0
    assert (entry.step, entry.lane) == (0, "live")
    assert entry.requested_at == "2026-07-31T00:00:00+00:00"
    assert entry.disposition == "success"


@pytest.mark.unit
def test_uncompressed_body_over_100kb_warns_and_is_journaled(
    monkeypatch, tmp_path, caplog
):
    body = b'{"pad":"' + b"x" * 102400 + b'"}'
    client, _, _, _ = _client(monkeypatch, tmp_path, [FakeResponse(200, body)])
    with caplog.at_level(logging.WARNING, logger="scrapers.espn.transport"):
        client.fetch_json(SUMMARY, EndpointType.SUMMARY, {"event": 9})
    assert "uncompressed" in caplog.text
    entry = client.ledger[-1]
    assert entry.content_encoding == "identity" and entry.direct_bytes > 102400


@pytest.mark.unit
def test_unknown_espn_host_has_no_transport_cluster(monkeypatch, tmp_path):
    client, session, _, _ = _client(monkeypatch, tmp_path, [FakeResponse(200, b"{}")])
    with pytest.raises(ValueError, match="cluster"):
        client.fetch_json("https://www.espn.com/soccer/", "catalog")
    assert session.calls == []


@pytest.mark.unit
def test_legacy_raw_alias_cache_hit_stays_network_free_and_origin_is_additive(
    monkeypatch, tmp_path
):
    url = "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary"
    target = canonicalize_target(url, {"event": 9})
    client, session, _, store = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(403), FakeResponse(200, b'{"new":true}')],
    )
    record = store.store(target, EndpointType.SUMMARY, b'{"old":true}')
    alias_key = store._alias_key(target.url_fingerprint)
    legacy_alias = json.loads(store._read_bytes(alias_key))
    assert "transport_origin" not in legacy_alias

    result = client.fetch_json(
        url,
        EndpointType.SUMMARY,
        {"event": 9},
    )

    assert result.json_data == {"old": True}
    assert result.cache_hit and result.attempts == 0
    assert result.transport_origin == "https://site.api.espn.com"
    assert client.ledger[-1].transport_origin == "https://site.api.espn.com"
    assert store.load(target)[1].content_hash == record.content_hash
    assert session.calls == []


@pytest.mark.unit
def test_transport_origin_is_validated_serialized_and_secret_free(tmp_path):
    store = EspnRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target(
        "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary",
        {"event": 9},
    )
    record = store.store(
        target,
        EndpointType.SUMMARY,
        b"{}",
        transport_origin="https://site.web.api.espn.com",
    )
    alias = json.loads(store._read_bytes(store._alias_key(target.url_fingerprint)))
    assert record.transport_origin == "https://site.web.api.espn.com"
    assert alias["transport_origin"] == "https://site.web.api.espn.com"
    assert store.load(target)[1].transport_origin == record.transport_origin

    secret = "ORIGIN-SECRET-123"
    invalid = (
        "https://example.com",
        "http://site.web.api.espn.com",
        "https://site.web.api.espn.com/path",
        f"https://user:{secret}@site.web.api.espn.com",
    )
    for origin in invalid:
        with pytest.raises(ValueError) as exc_info:
            store.store(
                target,
                EndpointType.SUMMARY,
                b"{}",
                transport_origin=origin,
            )
        assert secret not in str(exc_info.value)
        assert secret not in repr(exc_info.value)


@pytest.mark.unit
def test_raw_alias_versions_reject_mixed_provenance_schema(tmp_path):
    store = EspnRawStore.from_uri(tmp_path.as_uri())
    target = canonicalize_target(
        "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary",
        {"event": 9},
    )
    legacy = store.store(target, EndpointType.SUMMARY, b"{}")
    alias_key = store._alias_key(target.url_fingerprint)
    legacy_payload = json.loads(store._read_bytes(alias_key))
    assert legacy.manifest_version == "espn-raw-v1"
    assert "transport_origin" not in legacy_payload

    legacy_payload["transport_origin"] = "https://site.api.espn.com"
    store._write_bytes(alias_key, (json.dumps(legacy_payload) + "\n").encode())
    with pytest.raises(RawTargetCorrupt, match="identity"):
        store.load(target)

    current = store.store(
        target,
        EndpointType.SUMMARY,
        b"{}",
        transport_origin="https://site.api.espn.com",
    )
    current_payload = json.loads(store._read_bytes(alias_key))
    assert current.manifest_version == "espn-raw-v2"
    current_payload.pop("transport_origin")
    store._write_bytes(alias_key, (json.dumps(current_payload) + "\n").encode())
    with pytest.raises(RawTargetCorrupt, match="identity"):
        store.load(target)
