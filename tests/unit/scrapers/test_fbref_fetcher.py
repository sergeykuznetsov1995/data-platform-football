import sys
from types import ModuleType
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from scrapers.fbref.fetcher import (
    DEFAULT_BROWSER_BYTE_LIMIT,
    DEFAULT_BROWSER_REQUEST_LIMIT,
    FBrefFetcher,
    FetchError,
    MAX_HTML_BYTES,
    MAX_TARGET_HTTP_ATTEMPTS,
)
from scrapers.fbref.settings import (
    DEFAULT_HTTP_WIRE_OVERHEAD_RESERVATION_BYTES,
    DEFAULT_REQUEST_RESERVATION_BYTES,
)


def _fetcher(response, *, max_bytes=2 * 1024 * 1024):
    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    session = MagicMock()
    responses = iter(response if isinstance(response, list) else [response])

    def get(*_args, **kwargs):
        try:
            current = next(responses)
        except StopIteration:
            current = response[-1] if isinstance(response, list) else response
        callback = kwargs.get("content_callback")
        if callback is not None:
            chunks = getattr(current, "stream_chunks", None) or [current.content]
            for chunk in chunks:
                if callback(chunk) == 0xFFFFFFFF:
                    error = RuntimeError("curl: (23) write callback aborted")
                    error.response = current
                    raise error
        return current

    session.get.side_effect = get
    fetcher._http_session = session
    fetcher._transport = None
    fetcher._bootstrap_stats = {
        "real_bytes_downloaded": 150,
        "real_requests_count": 3,
        "browser_bootstrap_attempts": 1,
        "budget_unobserved_bytes": 77,
        "real_bytes_by_resource_type": {"document": 100, "script": 50},
    }
    fetcher.max_html_bytes = max_bytes
    fetcher.max_target_http_attempts = MAX_TARGET_HTTP_ATTEMPTS
    fetcher.status_retry_delay_seconds = 3.0
    fetcher._sleep = MagicMock()
    return fetcher


def _response(
    status=200,
    body=b"<html><table></table></html>",
    *,
    headers=None,
    wire_size=0,
    stream_chunks=None,
):
    return SimpleNamespace(
        status_code=status,
        content=body,
        headers=headers
        or {"content-type": "text/html", "etag": '"v1"'},
        request=SimpleNamespace(headers={}),
        wire_size=wire_size,
        stream_chunks=stream_chunks,
    )


def test_clearance_bootstrap_consumes_transport_delta_not_cumulative_stats(
    monkeypatch,
):
    transport = MagicMock()
    transport.fetch.return_value = "<html><body>source</body></html>"
    transport.traffic_delta.return_value = {
        "real_bytes_downloaded": 150,
        "real_requests_count": 3,
        "browser_bootstrap_attempts": 1,
        "budget_unobserved_bytes": 7,
        "real_bytes_by_resource_type": {"document": 100, "script": 50},
    }
    transport.traffic_stats.side_effect = AssertionError(
        "cumulative traffic must not be billed by FBrefFetcher"
    )
    clearance = {
        "cookies": {"cf_clearance": "test"},
        "user_agent": "test-agent",
        "proxy": None,
    }
    transport.get_clearance.return_value = clearance
    session = MagicMock()
    create_session = MagicMock(return_value=session)
    monkeypatch.setattr(FBrefFetcher, "_create_http_session", create_session)
    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    fetcher.bootstrap_url = "https://fbref.com/en/"
    fetcher._http_session = None
    fetcher._transport = transport

    fetcher._ensure_clearance()

    transport.traffic_delta.assert_called_once_with()
    transport.traffic_stats.assert_not_called()
    calls = [call[0] for call in transport.method_calls]
    assert calls.index("close") < calls.index("traffic_delta")
    create_session.assert_called_once_with(clearance)
    assert fetcher._http_session is session
    assert fetcher._bootstrap_stats == transport.traffic_delta.return_value


def test_clearance_closes_browser_before_counting_background_traffic(
    monkeypatch,
):
    events = []
    final_stats = {
        "real_bytes_downloaded": 350,
        "real_requests_count": 4,
        "browser_bootstrap_attempts": 1,
        "budget_unobserved_bytes": 100,
        "real_bytes_by_resource_type": {"document": 100, "script": 250},
    }
    transport = MagicMock()
    transport.fetch.return_value = "<html><body>source</body></html>"
    transport.get_clearance.return_value = {
        "cookies": {"cf_clearance": "test"},
        "user_agent": "test-agent",
        "proxy": None,
    }
    transport.close.side_effect = lambda: events.append("closed")

    def final_delta():
        assert events == ["closed"]
        events.append("accounted")
        return final_stats

    transport.traffic_delta.side_effect = final_delta
    session = MagicMock()
    monkeypatch.setattr(
        FBrefFetcher,
        "_create_http_session",
        MagicMock(return_value=session),
    )
    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    fetcher.bootstrap_url = "https://fbref.com/en/"
    fetcher._http_session = None
    fetcher._transport = transport

    fetcher._ensure_clearance()

    assert events == ["closed", "accounted"]
    assert fetcher._http_session is session
    assert fetcher._bootstrap_stats == final_stats


def test_clearance_traffic_export_failure_charges_full_reserved_ceiling():
    transport = MagicMock()
    transport.fetch.return_value = "<html><body>source</body></html>"
    transport.traffic_delta.side_effect = RuntimeError("metrics unavailable")
    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    fetcher.bootstrap_url = "https://fbref.com/en/"
    fetcher._http_session = None
    fetcher._transport = transport
    fetcher._max_browser_requests = 80
    fetcher._max_browser_bytes = 16 * 1024 * 1024

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "clearance_failed"
    assert raised.value.browser_requests == 80
    assert raised.value.browser_bootstrap_attempts == 4
    assert raised.value.browser_unobserved_bytes == 16 * 1024 * 1024


def test_clearance_export_exception_keeps_observed_traffic():
    transport = MagicMock()
    transport.fetch.return_value = "<html><body>source</body></html>"
    transport.traffic_delta.return_value = {
        "real_bytes_downloaded": 150,
        "real_requests_count": 3,
        "browser_bootstrap_attempts": 1,
        "budget_unobserved_bytes": 7,
        "real_bytes_by_resource_type": {"document": 100, "script": 50},
    }
    transport.get_clearance.side_effect = RuntimeError("export unavailable")
    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    fetcher.bootstrap_url = "https://fbref.com/en/"
    fetcher._http_session = None
    fetcher._transport = transport

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "clearance_export_failed"
    assert raised.value.browser_requests == 3
    assert raised.value.browser_document_bytes == 100
    assert raised.value.browser_asset_bytes == 50
    assert raised.value.browser_unobserved_bytes == 7


def test_target_fetch_uses_warm_http_bytes_and_emits_bootstrap_once(monkeypatch):
    monkeypatch.setattr(
        "scrapers.fbref.fetcher._response_wire_size", lambda _response: 42
    )
    fetcher = _fetcher(_response())

    first = fetcher.fetch("https://fbref.com/en/comps", page_kind="competition_index")
    second = fetcher.fetch("https://fbref.com/en/comps", page_kind="competition_index")

    assert first.body.startswith(b"<html>")
    assert first.http_wire_bytes == 42
    assert first.browser_document_bytes == 100
    assert first.browser_asset_bytes == 50
    assert first.browser_requests == 3
    assert first.browser_bootstrap_attempts == 1
    assert first.browser_unobserved_bytes == 77
    assert second.browser_document_bytes == 0
    assert second.browser_bootstrap_attempts == 0
    assert second.browser_unobserved_bytes == 0
    assert fetcher._http_session.get.call_count == 2
    assert fetcher._http_session.get.call_args.kwargs["allow_redirects"] is False
    assert callable(
        fetcher._http_session.get.call_args.kwargs["content_callback"]
    )


def test_conditional_304_has_no_body_and_needs_no_page_validation(monkeypatch):
    monkeypatch.setattr(
        "scrapers.fbref.fetcher._response_wire_size", lambda _response: 12
    )
    fetcher = _fetcher(_response(status=304, body=b""))

    result = fetcher.fetch(
        "https://fbref.com/en/comps",
        page_kind="competition_index",
        etag='"v1"',
    )

    assert result.status_code == 304
    assert result.body == b""
    assert fetcher._http_session.get.call_args.kwargs["headers"] == {
        "If-None-Match": '"v1"'
    }


def test_raw_contract_and_response_ceiling_fail_closed(monkeypatch):
    monkeypatch.setattr(
        "scrapers.fbref.fetcher._response_wire_size", lambda _response: 99
    )
    invalid = _fetcher(_response(body=b"not an html document"))
    with pytest.raises(FetchError, match="not_html_document") as caught:
        invalid.fetch("https://fbref.com/x", page_kind="season")
    assert caught.value.error_class == "raw_contract_not_html_document"

    too_large = _fetcher(_response(body=b"x" * 11), max_bytes=10)
    with pytest.raises(FetchError, match="exceeded") as caught:
        too_large.fetch("https://fbref.com/x", page_kind="season")
    assert caught.value.error_class == "response_too_large"


def test_http_200_cloudflare_challenge_poison_is_session_scoped(monkeypatch):
    monkeypatch.setattr(
        "scrapers.fbref.fetcher._response_wire_size", lambda _response: 42
    )
    fetcher = _fetcher(
        _response(body=b"<html><body>Just a moment...</body></html>")
    )

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("https://fbref.com/en/comps", page_kind="competition_index")

    assert caught.value.error_class == "raw_contract_cloudflare_challenge"
    assert caught.value.http_status == 200


def test_warm_transport_error_has_structured_session_classification():
    fetcher = _fetcher(_response())
    fetcher._http_session.get.side_effect = RuntimeError(
        "Connection reset by proxy"
    )

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("https://fbref.com/en/comps", page_kind="competition_index")

    assert caught.value.error_class == "warm_session_connection"
    assert caught.value.http_requests == 1


def test_unknown_warm_error_remains_target_scoped_http_exception():
    fetcher = _fetcher(_response())
    fetcher._http_session.get.side_effect = RuntimeError("decoder exploded")

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("https://fbref.com/en/comps", page_kind="competition_index")

    assert caught.value.error_class == "http_exception"


def test_streaming_ceiling_aborts_on_oversized_chunk_before_buffering_rest(
    monkeypatch,
):
    monkeypatch.setattr(
        "scrapers.fbref.fetcher._response_wire_size",
        lambda response: response.wire_size,
    )
    response = _response(
        body=b"unused",
        wire_size=11,
        stream_chunks=[b"123456", b"78901", b"never-read"],
    )
    fetcher = _fetcher(response, max_bytes=10)

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("https://fbref.com/x", page_kind="season")

    error = caught.value
    assert error.error_class == "response_too_large"
    assert error.target_requests == 1
    assert error.http_requests == 1
    assert error.http_status_history == (200,)
    assert error.wire_bytes == 11
    assert fetcher._http_session.get.call_count == 1


def test_streaming_ceiling_is_cumulative_across_status_retry_attempts(
    monkeypatch,
):
    monkeypatch.setattr(
        "scrapers.fbref.fetcher._response_wire_size",
        lambda response: response.wire_size,
    )
    fetcher = _fetcher(
        [
            _response(
                status=500,
                body=b"unused",
                wire_size=6,
                stream_chunks=[b"123456"],
            ),
            _response(
                status=200,
                body=b"unused",
                wire_size=5,
                stream_chunks=[b"78901"],
            ),
        ],
        max_bytes=10,
    )

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("https://fbref.com/x", page_kind="season")

    error = caught.value
    assert error.error_class == "response_too_large"
    assert error.target_requests == 2
    assert error.http_requests == 2
    assert error.http_status_history == (500, 200)
    assert error.browser_bootstrap_attempts == 1
    assert error.wire_bytes == 11
    assert fetcher._http_session.get.call_count == 2
    fetcher._sleep.assert_called_once_with(3.0)


def test_match_without_summary_tables_is_valid_raw_evidence(monkeypatch):
    monkeypatch.setattr(
        "scrapers.fbref.fetcher._response_wire_size", lambda _response: 20
    )
    body = b"<html><body>Match awarded; statistics not available</body></html>"
    fetcher = _fetcher(_response(body=body))

    result = fetcher.fetch("https://fbref.com/en/matches/abcdef12", page_kind="match")

    assert result.body == body


def test_constructor_passes_hard_browser_budget(monkeypatch):
    transport = MagicMock()
    constructor = MagicMock(return_value=transport)
    monkeypatch.setattr(
        "scrapers.fbref.fetcher.CamoufoxFbrefTransport", constructor
    )

    fetcher = FBrefFetcher()

    assert constructor.call_args.kwargs["max_network_requests"] == (
        DEFAULT_BROWSER_REQUEST_LIMIT
    )
    assert constructor.call_args.kwargs["max_network_bytes"] == (
        DEFAULT_BROWSER_BYTE_LIMIT
    )
    fetcher.close()


def test_bootstrap_provider_exception_is_session_scoped_fetch_error():
    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    transport = MagicMock()
    transport.fetch.side_effect = RuntimeError("fbref_proxy_pool_unavailable")
    transport.traffic_delta.return_value = {
        "real_requests_count": 0,
        "real_bytes_downloaded": 0,
    }
    fetcher._http_session = None
    fetcher._transport = transport
    fetcher.bootstrap_url = "https://fbref.com/en/"

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "clearance_failed"
    assert "RuntimeError" in str(raised.value)


def test_reset_clearance_drops_session_transport_and_metered_lease():
    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    old_transport = MagicMock()
    old_session = MagicMock()
    new_transport = MagicMock()
    close_lease = MagicMock()
    fetcher._transport = old_transport
    fetcher._http_session = old_session
    fetcher._bootstrap_stats = {"old": True}
    fetcher._clearance = {"old": True}
    fetcher._close_provider_lease = close_lease
    fetcher._create_transport = MagicMock(return_value=new_transport)

    fetcher.reset_clearance()

    old_session.close.assert_called_once_with()
    old_transport.close.assert_called_once_with()
    close_lease.assert_called_once_with()
    assert fetcher._transport is new_transport
    assert fetcher._bootstrap_stats is None
    assert fetcher._clearance is None


def test_target_and_bootstrap_have_independent_byte_reservations():
    assert (
        MAX_HTML_BYTES + DEFAULT_HTTP_WIRE_OVERHEAD_RESERVATION_BYTES
        <= DEFAULT_REQUEST_RESERVATION_BYTES
    )
    assert DEFAULT_BROWSER_BYTE_LIMIT == 4 * 1024 * 1024


def test_warm_session_reuses_explicit_proxy_auth_and_ignores_environment(
    monkeypatch,
):
    created = []

    class FakeSession:
        def __init__(self, **kwargs):
            created.append(kwargs)
            self.cookies = {}
            self.headers = {}

    curl_module = ModuleType("curl_cffi")
    requests_module = ModuleType("curl_cffi.requests")
    requests_module.Session = FakeSession
    curl_module.requests = requests_module
    monkeypatch.setitem(sys.modules, "curl_cffi", curl_module)
    monkeypatch.setitem(sys.modules, "curl_cffi.requests", requests_module)

    session = FBrefFetcher._create_http_session({
        "cookies": {"cf_clearance": "cookie-value"},
        "user_agent": "Mozilla/5.0 Firefox/135",
        "proxy": {
            "server": "http://proxy.example:8080",
            "username": "proxy-user",
            "password": "proxy-password",
        },
    })

    assert created == [{
        "impersonate": "firefox135",
        "proxy": "http://proxy.example:8080",
        "proxy_auth": ("proxy-user", "proxy-password"),
        "trust_env": False,
        "retry": 0,
    }]
    assert "proxy-user" not in created[0]["proxy"]
    assert "proxy-password" not in created[0]["proxy"]
    assert session.cookies == {"cf_clearance": "cookie-value"}
    assert session.headers["User-Agent"] == "Mozilla/5.0 Firefox/135"


def test_retryable_500_retries_once_and_accounts_both_requests(monkeypatch):
    monkeypatch.setattr(
        "scrapers.fbref.fetcher._response_wire_size",
        lambda response: response.wire_size,
    )
    fetcher = _fetcher([
        _response(status=500, body=b"temporary", wire_size=111),
        _response(status=200, wire_size=222),
    ])

    result = fetcher.fetch(
        "https://fbref.com/en/comps",
        page_kind="competition_index",
    )

    assert result.status_code == 200
    assert result.http_requests == 2
    assert result.http_status_history == (500, 200)
    assert result.http_wire_bytes == 333
    assert result.browser_requests == 3
    assert fetcher._http_session.get.call_count == 2
    fetcher._sleep.assert_called_once_with(3.0)


def test_repeated_500_persists_redacted_hash_evidence_and_exact_counts(
    monkeypatch,
):
    monkeypatch.setattr(
        "scrapers.fbref.fetcher._response_wire_size",
        lambda response: response.wire_size,
    )
    headers = {
        "content-type": "text/plain",
        "server": "provider-edge",
        "set-cookie": "session=secret-token",
        "proxy-authenticate": "Basic realm=secret-token",
    }
    fetcher = _fetcher([
        _response(status=500, body=b"first secret-token", wire_size=101),
        _response(
            status=500,
            body=b"second secret-token",
            headers=headers,
            wire_size=202,
        ),
    ])

    with pytest.raises(FetchError) as caught:
        fetcher.fetch(
            "https://fbref.com/en/comps",
            page_kind="competition_index",
        )

    error = caught.value
    assert error.error_class == "http_status"
    assert error.http_status == 500
    assert error.target_requests == 2
    assert error.http_requests == 2
    assert error.http_status_history == (500, 500)
    assert error.browser_bootstrap_attempts == 1
    assert error.target_request_made is True
    assert error.wire_bytes == 303
    assert "attempts=2" in str(error)
    assert "status_history=500,500" in str(error)
    assert "body_bytes=19" in str(error)
    assert "body_sha256=" in str(error)
    assert "server=provider-edge" in str(error)
    assert "secret-token" not in str(error)
    assert "set_cookie" not in str(error)
    assert "proxy_authenticate" not in str(error)
    assert fetcher._http_session.get.call_count == 2
    fetcher._sleep.assert_called_once_with(3.0)


def test_non_retryable_status_remains_one_accounted_request(monkeypatch):
    monkeypatch.setattr(
        "scrapers.fbref.fetcher._response_wire_size",
        lambda response: response.wire_size,
    )
    fetcher = _fetcher(_response(status=403, body=b"forbidden", wire_size=90))

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("https://fbref.com/en/comps", page_kind="competition_index")

    assert caught.value.target_requests == 1
    assert caught.value.http_requests == 1
    assert caught.value.http_status_history == (403,)
    assert caught.value.wire_bytes == 90
    assert fetcher._http_session.get.call_count == 1
    fetcher._sleep.assert_not_called()
