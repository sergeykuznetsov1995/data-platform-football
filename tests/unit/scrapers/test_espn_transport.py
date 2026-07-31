"""Frozen, network-free contracts for the native ESPN HTTP boundary."""

from __future__ import annotations

import gzip
import io
import json
from datetime import datetime, timezone

import pytest
import requests
from pyarrow import fs

from scrapers.espn.raw_store import EspnRawStore, RawTargetCorrupt
from scrapers.espn.transport import (
    AmbientProxyError,
    BudgetExceeded,
    CircuitOpen,
    DirectTransportError,
    EndpointType,
    EspnHttpClient,
    HttpStatusError,
    ResponseTooLarge,
    RetryExhausted,
    TaskBudget,
    canonicalize_target,
)


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


def _client(monkeypatch, tmp_path, responses, **kwargs):
    _clear_proxy_env(monkeypatch)
    store = EspnRawStore.from_uri(tmp_path.as_uri())
    session = FakeSession(responses)
    sleeps = []
    client = EspnHttpClient(
        store,
        session=session,
        sleep_fn=sleeps.append,
        monotonic_fn=lambda: 0.0,
        utcnow_fn=lambda: datetime(2026, 7, 31, tzinfo=timezone.utc),
        **kwargs,
    )
    return client, session, sleeps, store


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
        competition_id=700,
    )

    assert result.json_data == {"events": [{"id": "1"}]}
    assert result.body == body
    assert result.attempts == 1 and not result.cache_hit
    assert result.direct_bytes == len(encoded) and result.proxy_bytes == 0
    assert result.raw_uri.endswith(".json.gz") and result.content_hash
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
        competition_id=700,
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

    replayed = client.replay_json(url, EndpointType.SUMMARY, event_id=9)
    assert replayed.cache_hit and replayed.attempts == 0
    assert len(session.calls) == 0

    store._write_bytes(record.blob_key, b"not-gzip")
    with pytest.raises(RawTargetCorrupt):
        store.load(target)
    fetched = client.fetch_json(
        url, EndpointType.SUMMARY, event_id=9, force_refresh=False
    )
    assert fetched.json_data == {"header": {}}
    assert len(session.calls) == 1
    assert store.load(target)[0] == b'{"header":{}}'


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
        event_id=10,
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
def test_rate_limit_is_thirty_per_minute_with_burst_four(monkeypatch, tmp_path):
    client, session, sleeps, _ = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(200, b"{}") for _ in range(5)],
    )

    for index in range(5):
        client.fetch_json(
            f"https://site.api.espn.com/catalog?page={index}",
            "catalog",
            force_refresh=True,
        )

    assert len(session.calls) == 5
    assert sleeps == [2.0]


@pytest.mark.unit
def test_oversize_circuit_and_budgets_fail_closed(monkeypatch, tmp_path):
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

    failures = [FakeResponse(503, b"x") for _ in range(5)]
    circuit, session, _, _ = _client(
        monkeypatch,
        tmp_path / "circuit",
        failures,
        max_attempts=1,
    )
    for _ in range(5):
        with pytest.raises(RetryExhausted):
            circuit.fetch_json(
                "https://site.api.espn.com/apis/site/v2/sports/soccer/leagues",
                "catalog",
                force_refresh=True,
            )
    with pytest.raises(CircuitOpen):
        circuit.fetch_json(
            "https://site.api.espn.com/apis/site/v2/sports/soccer/leagues",
            "catalog",
            force_refresh=True,
        )
    assert len(session.calls) == 5

    budgeted, budget_session, _, _ = _client(
        monkeypatch,
        tmp_path / "budget",
        [FakeResponse(200, b"{}")],
        budget=TaskBudget(max_requests=0),
    )
    with pytest.raises(BudgetExceeded):
        budgeted.fetch_json(
            "https://site.api.espn.com/apis/site/v2/sports/soccer/leagues",
            "catalog",
        )
    assert budget_session.calls == []


@pytest.mark.unit
def test_ambient_proxy_and_non_https_are_rejected(monkeypatch, tmp_path):
    _clear_proxy_env(monkeypatch)
    monkeypatch.setenv("https_proxy", "http://proxy.invalid")
    with pytest.raises(AmbientProxyError):
        EspnHttpClient(
            EspnRawStore.from_uri(tmp_path.as_uri()), session=FakeSession([])
        )

    _clear_proxy_env(monkeypatch)
    client = EspnHttpClient(
        EspnRawStore.from_uri(tmp_path.as_uri()), session=FakeSession([])
    )
    with pytest.raises(ValueError, match="HTTPS"):
        client.fetch_json("http://site.api.espn.com/summary?event=1", "summary")


@pytest.mark.unit
def test_exact_defaults_and_unique_competition_event_budgets(monkeypatch, tmp_path):
    budget = TaskBudget(max_competitions=1, max_summary_events=1)
    client, session, _, _ = _client(
        monkeypatch,
        tmp_path,
        [FakeResponse(200, b"{}"), FakeResponse(200, b"{}")],
        budget=budget,
    )
    assert client.connect_timeout == 5.0
    assert client.read_timeout == 20.0
    assert client.response_cap_bytes == 16 * 1024 * 1024
    assert client.rate_per_minute == 30
    assert client.burst == 4
    assert client.max_attempts == 4
    assert budget.max_competitions == 1
    assert budget.max_summary_events == 1

    client.fetch_json(
        "https://site.api.espn.com/summary?event=1",
        "summary",
        competition_id=7,
        event_id=1,
    )
    with pytest.raises(BudgetExceeded):
        client.fetch_json(
            "https://site.api.espn.com/summary?event=2",
            "summary",
            competition_id=8,
            event_id=2,
        )
    assert len(session.calls) == 1


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
def test_byte_budget_reserves_before_network_and_never_overruns(monkeypatch, tmp_path):
    exhausted, exhausted_session, _, _ = _client(
        monkeypatch,
        tmp_path / "empty",
        [FakeResponse(200, b"{}")],
        budget=TaskBudget(max_bytes=0),
    )
    with pytest.raises(BudgetExceeded):
        exhausted.fetch_json("https://site.api.espn.com/catalog", "catalog")
    assert exhausted_session.calls == []

    framed, _, _, _ = _client(
        monkeypatch,
        tmp_path / "framed",
        [FakeResponse(200, b"12", {"Content-Length": "2"})],
        budget=TaskBudget(max_bytes=1),
    )
    with pytest.raises(BudgetExceeded):
        framed.fetch_json("https://site.api.espn.com/catalog", "catalog")
    assert framed.budget.bytes_used <= 1

    unframed, _, _, _ = _client(
        monkeypatch,
        tmp_path / "unframed",
        [FakeResponse(200, b"12")],
        budget=TaskBudget(max_bytes=1),
    )
    with pytest.raises(BudgetExceeded):
        unframed.fetch_json("https://site.api.espn.com/catalog", "catalog")
    assert unframed.budget.bytes_used <= 1


@pytest.mark.unit
def test_partial_read_timeout_charges_wire_bytes_and_retries(monkeypatch, tmp_path):
    partial = FakeResponse(200)
    partial.raw = PartialTimeoutRaw(b"123")
    client, session, _, _ = _client(
        monkeypatch,
        tmp_path,
        [partial, FakeResponse(200, b'{"ok":true}')],
        budget=TaskBudget(max_bytes=32),
    )
    result = client.fetch_json("https://site.api.espn.com/catalog", "catalog")
    assert result.attempts == 2
    assert result.direct_bytes == 3 + len(b'{"ok":true}')
    assert client.budget.bytes_used == result.direct_bytes
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
            EspnRawStore.from_uri(tmp_path.as_uri()), session=FakeSession([])
        )
