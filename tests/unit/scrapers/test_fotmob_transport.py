import gzip
import io
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
import requests

from scrapers.fotmob.raw_store import FotMobRawStore
from scrapers.fotmob.transport import (
    FetchOutcome,
    FotMobTransport,
    _parse_retry_after,
    canonicalize_target,
)


class FakeResponse:
    def __init__(self, status, body=b"", headers=None, url=None):
        self.status_code = status
        self.raw = io.BytesIO(body)
        self.headers = headers or {}
        self.url = url
        self.closed = False

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}
        self.proxies = {"https": "http://must-be-cleared.invalid"}
        self.trust_env = True
        self.calls = []
        self._lock = threading.Lock()

    def get(self, url, **kwargs):
        with self._lock:
            self.calls.append((url, kwargs))
            response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        if response.url is None:
            response.url = url
        return response


def _store(tmp_path):
    return FotMobRawStore.from_uri(tmp_path.as_uri())


def _transport(responses, *, raw_store=None, **kwargs):
    session = FakeSession(responses)
    transport = FotMobTransport(
        raw_store,
        session=session,
        sleep_fn=lambda _delay: None,
        jitter_fn=lambda _low, _high: 0.0,
        **kwargs,
    )
    return transport, session


def test_canonical_target_sorts_query_and_enforces_https_allowlist():
    first = canonicalize_target(
        "leagues",
        [("season", "2025/2026"), ("id", 47), ("id", 42), ("skip", None)],
    )
    second = canonicalize_target(
        "https://www.fotmob.com/api/data/leagues?id=42",
        {"id": 47, "season": "2025/2026"},
    )
    assert first == second
    assert first.canonical_url == (
        "https://www.fotmob.com/api/data/leagues?id=42&id=47&season=2025%2F2026"
    )
    data_host = canonicalize_target(
        "https://data.fotmob.com/stats/47/season/2025/goals.json"
    )
    assert data_host.canonical_url.startswith("https://data.fotmob.com/")

    for bad in (
        "http://www.fotmob.com/api/data/leagues",
        "https://example.com/api/data/leagues",
        "https://user@www.fotmob.com/api/data/leagues",
        "https://www.fotmob.com:444/api/data/leagues",
        "",
    ):
        with pytest.raises(ValueError):
            canonicalize_target(bad)


def test_cancel_interrupts_worker_retry_wait_cooperatively():
    class NotifyingSession(FakeSession):
        def __init__(self):
            super().__init__([requests.ConnectionError("down")])
            self.called = threading.Event()

        def get(self, url, **kwargs):
            self.called.set()
            return super().get(url, **kwargs)

        def close(self):
            return None

    session = NotifyingSession()
    transport = FotMobTransport(
        session=session,
        max_attempts=4,
        backoff_base=60,
        jitter_fn=lambda _low, _high: 0.0,
    )

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(transport.fetch_json, "allLeagues")
        assert session.called.wait(1)
        transport.cancel()
        with pytest.raises(RuntimeError, match="cancelled"):
            future.result(timeout=1)


def test_direct_success_measures_wire_and_decoded_bytes_and_commits_raw(tmp_path):
    decoded = b'{"details":{"name":"Premier League"}}'
    encoded = gzip.compress(decoded, mtime=0)
    response = FakeResponse(
        200,
        encoded,
        {
            "Content-Encoding": "gzip",
            "ETag": '"v1"',
            "Last-Modified": "Fri, 10 Jul 2026 12:00:00 GMT",
        },
    )
    transport, session = _transport([response], raw_store=_store(tmp_path))

    result = transport.fetch_json("leagues", {"season": "2025/2026", "id": 47})

    assert result.outcome == FetchOutcome.SUCCESS
    assert result.status == "success"
    assert result.ok and result.data == {"details": {"name": "Premier League"}}
    assert result.body == decoded
    assert result.encoded_bytes == result.direct_bytes == len(encoded)
    assert result.decoded_bytes == len(decoded)
    assert result.proxy_bytes == 0
    assert result.etag == '"v1"'
    assert result.raw_uri and result.raw_uri.endswith(".json.gz")
    assert result.content_hash and result.fetched_at
    assert session.trust_env is False
    assert session.proxies == {}
    assert session.headers["Accept-Encoding"] == "gzip, deflate"
    assert session.calls[0][0].endswith("id=47&season=2025%2F2026")
    assert session.calls[0][1]["allow_redirects"] is False
    assert response.closed

    stats = transport.snapshot_stats()
    assert stats.logical_targets == stats.attempts == 1
    assert stats.status_counts == {"200": 1}
    assert stats.outcome_counts == {"success": 1}
    assert stats.direct_bytes == stats.encoded_bytes == len(encoded)
    assert stats.decoded_bytes == len(decoded)
    assert stats.proxy_bytes == 0


def test_etag_and_last_modified_304_replay_cached_body(tmp_path):
    store = _store(tmp_path)
    target = canonicalize_target("allLeagues")
    body = b'{"countries":[{"name":"England"}]}'
    stored = store.store(
        target,
        body,
        fetched_at="2026-07-10T12:00:00+00:00",
        etag='"catalog-v7"',
        last_modified="Fri, 10 Jul 2026 12:00:00 GMT",
    )
    transport, session = _transport(
        [FakeResponse(304)],
        raw_store=store,
        max_attempts=2,
    )

    result = transport.fetch_json("allLeagues")

    assert result.outcome == FetchOutcome.NOT_MODIFIED
    assert result.http_status == 304
    assert result.cache_hit and not result.stale
    assert result.body == body
    assert result.raw_uri == stored.raw_uri
    assert result.fetched_at == stored.fetched_at
    assert result.direct_bytes == 0
    request_headers = session.calls[0][1]["headers"]
    assert request_headers == {
        "If-None-Match": '"catalog-v7"',
        "If-Modified-Since": "Fri, 10 Jul 2026 12:00:00 GMT",
    }
    _, refreshed = store.load(target)
    assert refreshed.fetched_at == stored.fetched_at
    assert refreshed.validated_at > refreshed.fetched_at
    stats = transport.snapshot_stats()
    assert stats.cache_hits == stats.not_modified == 1
    assert stats.decoded_bytes == len(body)


def test_rotated_url_neither_inherits_validators_nor_closes_on_304(tmp_path):
    """Мина A1 (урок FBref 05.08): кэш-валидатор не переезжает на новый URL.

    Ротация Next.js build id меняет физический адрес карточки игрока. Если бы
    ETag/Last-Modified достались новому адресу, источник ответил бы 304 «не
    изменилось» на страницу, которую никогда не качали, и цель закрылась бы
    успехом без единого 200.
    """

    store = _store(tmp_path)
    old_url = "https://www.fotmob.com/_next/data/build-1/players/10.json"
    new_url = "https://www.fotmob.com/_next/data/build-2/players/10.json"
    body = b'{"pageProps":{"data":{"id":10}}}'
    store.store(
        canonicalize_target(old_url),
        body,
        fetched_at="2026-08-18T12:00:00+00:00",
        etag='"player-10-v7"',
        last_modified="Mon, 18 Aug 2026 12:00:00 GMT",
    )

    transport, session = _transport([FakeResponse(304)], raw_store=store)
    result = transport.fetch_json(new_url)

    # Валидаторы прежнего адреса на новый не уехали...
    assert session.calls[0][1]["headers"] == {}
    # ...а 304 без собственного сырья не закрывает цель успехом.
    assert result.outcome == FetchOutcome.TERMINAL_FAILURE
    assert result.http_status == 304
    assert not result.cache_hit
    assert result.body is None
    assert "304 without a valid raw body" in result.error

    # Контроль: у прежнего адреса механизм валидаторов работает — иначе первая
    # половина теста была бы зелёной по пустой причине.
    same_url_transport, same_url_session = _transport(
        [FakeResponse(304)], raw_store=store
    )
    replay = same_url_transport.fetch_json(old_url)
    assert same_url_session.calls[0][1]["headers"] == {
        "If-None-Match": '"player-10-v7"',
        "If-Modified-Since": "Mon, 18 Aug 2026 12:00:00 GMT",
    }
    assert replay.outcome == FetchOutcome.NOT_MODIFIED
    assert replay.body == body


@pytest.mark.parametrize("value", ["Infinity", "-Infinity", "NaN"])
def test_parse_retry_after_ignores_non_finite_values(value):
    now = datetime(2026, 8, 18, 12, tzinfo=timezone.utc)
    assert _parse_retry_after(value, now) is None
    transport, _session = _transport([], backoff_base=2)
    assert transport._retry_delay(2, value) == 4.0


@pytest.mark.parametrize("value", ["Infinity", "-Infinity", "NaN"])
def test_retry_after_duration_ignores_non_finite_values(value):
    assert FotMobTransport._retry_after_duration(value) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2.5", 2.5),
        ("0", 0.0),
        ("-3", 0.0),
        ("Tue, 18 Aug 2026 12:00:03 GMT", 3.0),
        ("Tue, 18 Aug 2026 11:59:59 GMT", 0.0),
    ],
)
def test_parse_retry_after_preserves_finite_values_and_http_dates(value, expected):
    now = datetime(2026, 8, 18, 12, tzinfo=timezone.utc)
    assert _parse_retry_after(value, now) == expected


def test_retry_after_retryable_statuses_then_success_are_counted(monkeypatch):
    import scrapers.fotmob.transport as module
    class Clock(datetime):
        current = datetime(2026, 10, 26, tzinfo=timezone.utc)
        @classmethod
        def now(cls, tz=None):
            return cls.current
    monkeypatch.setattr(module, "datetime", Clock)
    delays = []
    session = FakeSession(
        [
            FakeResponse(429, b"rate", {"Retry-After": "3"}),
            FakeResponse(503, b"busy"),
            FakeResponse(200, b'{"ok":true}'),
        ]
    )
    def sleep(seconds):
        delays.append(seconds)
        Clock.current += timedelta(seconds=seconds)
    transport = FotMobTransport(
        session=session,
        max_attempts=4,
        sleep_fn=sleep,
        jitter_fn=lambda _low, _high: 0.0,
    )

    result = transport.fetch_json("allLeagues")

    assert result.outcome == FetchOutcome.SUCCESS
    assert result.attempts == 3 and result.retries == 2
    assert delays == [3.0, 2.0]
    assert result.direct_bytes == len(b"rate") + len(b"busy") + len(b'{"ok":true}')
    stats = transport.snapshot_stats()
    assert stats.attempts == 3 and stats.retries == 2
    assert stats.status_counts == {"429": 1, "503": 1, "200": 1}


def test_exhausted_429_preserves_final_retry_after_for_caller():
    transport, _session = _transport(
        [FakeResponse(429, b"rate", {"Retry-After": "7200"})],
        max_attempts=1,
    )

    result = transport.fetch_json("leagues", {"id": 47})

    assert result.outcome == FetchOutcome.RETRYABLE_FAILURE
    assert result.retry_after == timedelta(hours=2)


def test_exhausted_retryable_response_replays_valid_stale_cache(tmp_path):
    store = _store(tmp_path)
    target = canonicalize_target("leagues", {"id": 42, "season": "2025/2026"})
    body = b'{"details":{"name":"Champions League"}}'
    store.store(target, body, etag='"ucl-v1"')
    transport, _ = _transport(
        [FakeResponse(503, b"down"), FakeResponse(503, b"still down")],
        raw_store=store,
        max_attempts=2,
    )

    result = transport.fetch_json(
        "leagues",
        {"season": "2025/2026", "id": 42},
    )

    assert result.outcome == FetchOutcome.STALE_REPLAY
    assert result.ok and result.cache_hit and result.stale
    assert not result.terminal
    assert result.http_status == 503
    assert result.json_data["details"]["name"] == "Champions League"
    assert result.attempts == 2 and result.retries == 1
    assert "retryable HTTP 503" in result.error
    stats = transport.snapshot_stats()
    assert stats.stale_replays == stats.cache_hits == 1


@pytest.mark.parametrize("status", [204, 404])
def test_terminal_not_available_never_replays_old_cache(tmp_path, status):
    store = _store(tmp_path)
    target = canonicalize_target("playerData", {"id": 999})
    store.store(target, b'{"old":true}', etag='"old"')
    transport, session = _transport(
        [FakeResponse(status, b"not found")],
        raw_store=store,
        max_attempts=4,
    )

    result = transport.fetch_json("playerData", {"id": 999})

    assert result.outcome == FetchOutcome.NOT_AVAILABLE
    assert result.terminal and not result.cache_hit and not result.stale
    assert result.json_data is None and result.body is None
    assert result.attempts == 1 and result.retries == 0
    assert len(session.calls) == 1


def test_null_json_body_is_terminal_not_available(tmp_path):
    # FotMob answers 200 with a literal ``null`` body for dead catalog
    # entries (e.g. placeholder/promo competitions). That carries no data
    # for any parser and must degrade like a 204/404, not crash downstream
    # into a false schema_drift.
    transport, session = _transport(
        [FakeResponse(200, b"null")],
        raw_store=_store(tmp_path),
    )

    result = transport.fetch_json("leagues", {"id": 285})

    assert result.outcome == FetchOutcome.NOT_AVAILABLE
    assert result.terminal and not result.ok
    assert result.json_data is None and result.body is None
    assert "null JSON body" in result.error
    assert len(session.calls) == 1


def test_revalidated_null_cache_stays_not_available(tmp_path):
    store = _store(tmp_path)
    target = canonicalize_target("leagues", {"id": 285})
    store.store(target, b"null", etag='"dead-v1"')
    transport, _session = _transport(
        [FakeResponse(304, b"", {"ETag": '"dead-v1"'})],
        raw_store=store,
    )

    result = transport.fetch_json("leagues", {"id": 285})

    assert result.outcome == FetchOutcome.NOT_AVAILABLE
    assert result.terminal and not result.ok and not result.cache_hit
    assert result.json_data is None and result.body is None


def test_stale_replay_of_null_cache_preserves_stale_provenance(tmp_path):
    store = _store(tmp_path)
    target = canonicalize_target("leagues", {"id": 285})
    store.store(target, b"null", etag='"dead-v1"')
    transport, _session = _transport(
        [FakeResponse(503, b"down"), FakeResponse(503, b"still down")],
        raw_store=store,
        max_attempts=2,
    )

    result = transport.fetch_json("leagues", {"id": 285})

    assert result.outcome == FetchOutcome.NOT_AVAILABLE
    assert not result.terminal and not result.ok
    assert result.cache_hit and result.stale
    assert result.http_status == 503
    assert result.json_data is None and result.body is None


def test_terminal_status_is_not_retried_and_retryable_can_disable_stale(tmp_path):
    store = _store(tmp_path)
    target = canonicalize_target("leagues", {"id": 289})
    store.store(target, b'{"name":"AFCON"}')
    terminal, terminal_session = _transport(
        [FakeResponse(403, b"forbidden"), FakeResponse(200, b"{}")],
        raw_store=store,
        max_attempts=4,
    )
    result = terminal.fetch_json("leagues", {"id": 289})
    assert result.outcome == FetchOutcome.TERMINAL_FAILURE
    assert result.terminal and result.attempts == 1
    assert len(terminal_session.calls) == 1

    retryable, _ = _transport(
        [FakeResponse(500, b"broken")],
        raw_store=store,
        max_attempts=1,
    )
    result = retryable.fetch_json(
        "leagues",
        {"id": 289},
        allow_stale_on_error=False,
    )
    assert result.outcome == FetchOutcome.RETRYABLE_FAILURE
    assert not result.terminal and not result.cache_hit


def test_network_errors_retry_and_stale_age_can_reject_replay(tmp_path):
    store = _store(tmp_path)
    target = canonicalize_target("leagues", {"id": 63})
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    store.store(target, b'{"name":"RPL"}', fetched_at=old)
    error = requests.ConnectionError("offline")
    transport, _ = _transport(
        [error, requests.Timeout("still offline")],
        raw_store=store,
        max_attempts=2,
        max_stale_seconds=60,
    )

    result = transport.fetch_json("leagues", {"id": 63})

    assert result.outcome == FetchOutcome.RETRYABLE_FAILURE
    assert result.attempts == 2 and result.retries == 1
    assert result.direct_bytes == 0
    assert transport.snapshot_stats().status_counts == {"exception": 2}


def test_invalid_fresh_json_uses_cache_but_corrupt_cache_is_not_replayed(tmp_path):
    store = _store(tmp_path)
    target = canonicalize_target("leagues", {"id": 47})
    body = b'{"valid":true}'
    record = store.store(target, body)
    transport, _ = _transport(
        [FakeResponse(200, b"not-json")],
        raw_store=store,
        max_attempts=1,
    )
    result = transport.fetch_json("leagues", {"id": 47})
    assert result.outcome == FetchOutcome.STALE_REPLAY
    assert result.body == body
    assert "invalid JSON response" in result.error

    store._write_bytes(record.blob_key, b"corrupt")
    transport, session = _transport(
        [FakeResponse(503, b"down")],
        raw_store=store,
        max_attempts=1,
    )
    result = transport.fetch_json("leagues", {"id": 47})
    assert result.outcome == FetchOutcome.RETRYABLE_FAILURE
    assert not result.cache_hit
    assert "retryable HTTP 503" in result.error
    assert session.calls[0][1]["headers"] == {}


def test_offline_replay_and_thread_safe_aggregate_stats(tmp_path):
    store = _store(tmp_path)
    target = canonicalize_target("allLeagues")
    store.store(target, b'{"count":555}')
    replay = FotMobTransport(store).replay_json("allLeagues")
    assert replay.outcome == FetchOutcome.SUCCESS
    assert replay.cache_hit and replay.attempts == 0 and replay.direct_bytes == 0

    count = 32
    responses = [FakeResponse(200, b'{"ok":true}') for _ in range(count)]
    transport, _ = _transport(responses, max_attempts=1)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(lambda _: transport.fetch_json("allLeagues"), range(count))
        )
    assert all(result.outcome == FetchOutcome.SUCCESS for result in results)
    stats = transport.snapshot_stats()
    assert stats.logical_targets == count
    assert stats.attempts == count
    assert stats.status_counts == {"200": count}
    assert stats.outcome_counts == {"success": count}
    assert stats.decoded_bytes == count * len(b'{"ok":true}')


def test_offline_player_replay_uses_recorded_target_key_not_current_build(tmp_path):
    store = _store(tmp_path)
    historical = canonicalize_target(
        "https://www.fotmob.com/_next/data/build-from-original-run/players/10.json"
    )
    body = b'{"pageProps":{"data":{"id":10,"name":"Ten"}}}'
    stored = store.store(historical, body)
    transport = FotMobTransport(store)

    replay = transport.replay_target_key(historical.target_key)

    assert replay.outcome == FetchOutcome.SUCCESS
    assert replay.url == historical.canonical_url
    assert replay.target_key == historical.target_key
    assert replay.content_hash == stored.content_hash
    assert replay.body == body
    assert replay.cache_hit and replay.attempts == 0
    assert replay.direct_bytes == replay.proxy_bytes == 0
    missing = transport.replay_target_key("a" * 64)
    assert missing.outcome == FetchOutcome.TERMINAL_FAILURE
    assert "target-key replay failed" in missing.error


def test_cached_alias_replays_exact_selected_season_without_network(tmp_path):
    store = _store(tmp_path)
    selected = canonicalize_target("leagues", {"id": 47})
    exact = canonicalize_target("leagues", {"id": 47, "season": "2025/2026"})
    body = b'{"details":{"selectedSeason":"2025/2026"}}'
    store.store(selected, body)
    transport = FotMobTransport(store)

    transport.alias_cached_json(selected.canonical_url, exact.canonical_url)
    replay = transport.replay_json(exact.canonical_url)

    assert replay.ok and replay.cache_hit
    assert replay.attempts == replay.direct_bytes == replay.proxy_bytes == 0
    assert replay.body == body


def test_document_fetch_reuses_direct_only_accounting_without_json_validation():
    html = b'<script id="__NEXT_DATA__">{"buildId":"build-123"}</script>'
    encoded = gzip.compress(html, mtime=0)
    transport, session = _transport(
        [FakeResponse(200, encoded, {"Content-Encoding": "gzip"})]
    )

    result = transport.fetch_document()

    assert result.ok and result.body == html and result.json_data is None
    assert result.direct_bytes == len(encoded) and result.proxy_bytes == 0
    assert session.trust_env is False and session.proxies == {}
    stats = transport.snapshot_stats()
    assert stats.logical_targets == stats.attempts == 1
    assert stats.direct_bytes == len(encoded)
    assert stats.decoded_bytes == len(html)


@pytest.mark.parametrize("document", [False, True])
@pytest.mark.parametrize("status", [429, 503])
def test_long_retry_after_defers_without_early_retry_and_blocks_second_client(document, status):
    from scrapers.fotmob.shared_rate_limiter import CompositeRateLimiter, RefundableRateLimiter
    from tests.unit.scrapers.test_fotmob_shared_rate_limiter import _limiter
    shared, connection = _limiter(rows=[(3.0,)] * 8)
    def client(responses):
        local = RefundableRateLimiter(max_requests=60, window_seconds=60, burst_size=4)
        return _transport(responses, rate_limiter=CompositeRateLimiter(local, shared), max_retry_delay=60)
    first, session = client([FakeResponse(status, b"busy", {"Retry-After": "7200"}), FakeResponse(200, b'{}')])
    fetch = first.fetch_document if document else first.fetch_json
    result = fetch("https://www.fotmob.com/")
    assert result.outcome == FetchOutcome.RETRYABLE_FAILURE
    assert result.retry_after == timedelta(hours=2)
    assert result.blocked_until is not None
    assert len(session.calls) == 1
    second_shared = type(shared)("ignored", connection=connection)
    second, other_session = client([FakeResponse(200, b'{}')])
    second.rate_limiter.shared = second_shared
    other = second.fetch_json("allLeagues")
    assert other.attempts == 0 and other.stop_reason == "retry_after"
    assert not other_session.calls


def test_three_distinct_403_stop_wave_and_shared_client():
    from scrapers.fotmob.shared_rate_limiter import CompositeRateLimiter, RefundableRateLimiter
    from tests.unit.scrapers.test_fotmob_shared_rate_limiter import _limiter
    shared, connection = _limiter(rows=[(3.0,)] * 10)
    transport, session = _transport([FakeResponse(403)] * 4)
    transport.rate_limiter = CompositeRateLimiter(
        RefundableRateLimiter(max_requests=600, window_seconds=60, burst_size=10), shared)
    transport.fetch_json("leagues", {"id": 1})
    transport.fetch_json("leagues", {"id": 1})
    transport.fetch_document("https://www.fotmob.com/")
    assert transport.stop_reason is None
    transport.fetch_json("leagues", {"id": 2})
    blocked = transport.fetch_json("leagues", {"id": 3})
    assert blocked.stop_reason == "ban" and blocked.attempts == 0
    assert timedelta(minutes=14) < blocked.retry_after <= timedelta(minutes=15)
    assert len(session.calls) == 4
    other, session2 = _transport([FakeResponse(200, b'{}')])
    other.rate_limiter = CompositeRateLimiter(
        RefundableRateLimiter(max_requests=60, window_seconds=60, burst_size=4),
        type(shared)("ignored", connection=connection))
    assert other.fetch_document().stop_reason == "ban"
    assert session2.calls == []


@pytest.mark.parametrize("reason", ["retry_after", "ban"])
@pytest.mark.parametrize("document", [False, True])
def test_failed_pause_write_is_republished_after_reconnect(reason, document):
    import psycopg2
    from scrapers.fotmob.shared_rate_limiter import CompositeRateLimiter, RefundableRateLimiter
    from tests.unit.scrapers.test_fotmob_shared_rate_limiter import _limiter

    shared, broken = _limiter(rows=[(3.0,)])
    recovered_shared, recovered = _limiter(rows=[(3.0,)])
    first, first_session = _transport([FakeResponse(200, b'{}')])
    second, second_session = _transport([FakeResponse(200, b'{}')])
    for transport, limiter in [(first, shared), (second, recovered_shared)]:
        transport.rate_limiter = CompositeRateLimiter(
            RefundableRateLimiter(max_requests=60, window_seconds=60, burst_size=4),
            limiter,
        )

    def unavailable():
        raise psycopg2.OperationalError("metadb unavailable")

    broken.broken = True
    shared._connect = unavailable
    with pytest.raises(psycopg2.OperationalError):
        first._pause(7200, reason)
    until = first.blocked_until
    assert shared._connection is None
    assert recovered.pause is None

    shared._connect = lambda: recovered
    fetch = first.fetch_document if document else first.fetch_json
    assert fetch("https://www.fotmob.com/").stop_reason == reason
    assert recovered.pause == (until, reason)
    other = second.fetch_json("allLeagues")
    assert other.stop_reason == reason and other.blocked_until == until
    assert other.attempts == 0
    assert first_session.calls == second_session.calls == []


def test_non_403_response_resets_ban_series():
    transport, session = _transport([FakeResponse(s, b'{}') for s in [403, 403, 404, 403, 403, 200]])
    for target in range(6):
        transport.fetch_json("leagues", {"id": target})
    assert transport.stop_reason is None
    assert len(session.calls) == 6


@pytest.mark.parametrize("document", [False, True])
def test_expired_pause_still_requires_rate_limit_admission(monkeypatch, document):
    transport, session = _transport([FakeResponse(200, b'{}')])
    class Limiter:
        calls = 0
        def acquire(self, **kwargs):
            self.calls += 1
            return True
    limiter = Limiter()
    transport.rate_limiter = limiter
    # Pause is seen inside acquire, and expires before the next loop.
    states = iter([None, (datetime.now(timezone.utc), "retry_after"), None, None, None])
    monkeypatch.setattr(transport, "_pause_state", lambda: next(states, None))
    fetch = transport.fetch_document if document else transport.fetch_json
    assert fetch("https://www.fotmob.com/").ok
    assert len(session.calls) == limiter.calls == 1


@pytest.mark.parametrize("status", [200, 404])
def test_non_403_headers_reset_series_even_when_body_read_fails(status):
    class BrokenBody:
        def read(self, **kwargs):
            raise requests.ConnectionError("body interrupted")
    response = FakeResponse(status)
    response.raw = BrokenBody()
    transport, session = _transport([FakeResponse(403), FakeResponse(403), response, FakeResponse(403)], max_attempts=1)
    for target in range(4):
        transport.fetch_json("leagues", {"id": target})
    assert transport.stop_reason is None
    assert transport.snapshot_stats().attempts == 4
    assert len(session.calls) == 4
