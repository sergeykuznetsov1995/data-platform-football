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
from scrapers.fbref.proxy_lease import FBrefProxyLeaseError
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

    assert fetcher.ensure_clearance() is True
    assert fetcher.ensure_clearance() is False

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
    create_http_session = MagicMock(return_value=session)
    monkeypatch.setattr(
        FBrefFetcher,
        "_create_http_session",
        create_http_session,
    )
    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    fetcher.bootstrap_url = "https://fbref.com/en/"
    fetcher._http_session = None
    fetcher._transport = transport

    fetcher._ensure_clearance()

    assert events == ["closed", "accounted"]
    assert fetcher._http_session is session
    assert fetcher._bootstrap_stats == final_stats


def test_late_hard_policy_callback_rejects_exported_clearance(monkeypatch):
    transport = MagicMock()
    transport.fetch.return_value = "<html><body>source</body></html>"
    transport.get_clearance.return_value = {
        "cookies": {"cf_clearance": "test"},
        "user_agent": "test-agent",
        "proxy": None,
    }
    transport.traffic_delta.return_value = {
        "real_bytes_downloaded": 0,
        "real_requests_count": 1,
        "budget_unobserved_bytes": 589824,
        "network_policy_failed": True,
        "network_policy_failure": "unexpected_websocket_handshake",
    }
    create_http_session = MagicMock()
    monkeypatch.setattr(
        FBrefFetcher,
        "_create_http_session",
        create_http_session,
    )
    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    fetcher.bootstrap_url = "https://fbref.com/en/"
    fetcher._http_session = None
    fetcher._transport = transport

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "hard_transport_policy"
    assert "unexpected_websocket_handshake" in str(raised.value)
    create_http_session.assert_not_called()


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

    assert raised.value.error_class == "hard_transport_policy"
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


@pytest.mark.parametrize(
    "message",
    [
        # Real libcurl prose seen in production; none of it contains a
        # substring the text classifier looks for.
        "Failed to perform, curl: (7) Could not connect to proxy",
        "Proxy CONNECT aborted",
        "Failed to perform, curl: (56) Recv failure",
    ],
)
def test_dead_proxy_is_a_session_failure_not_a_target_verdict(message):
    """#1122: the transport never reached the source, so the page was not judged."""
    exceptions = pytest.importorskip("curl_cffi.requests.exceptions")
    fetcher = _fetcher(_response())
    fetcher._http_session.get.side_effect = exceptions.ProxyError(message)

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("https://fbref.com/en/comps", page_kind="competition_index")

    assert caught.value.error_class == "warm_session_connection"


def test_transport_timeout_is_typed_even_when_the_prose_is_silent():
    exceptions = pytest.importorskip("curl_cffi.requests.exceptions")
    fetcher = _fetcher(_response())
    fetcher._http_session.get.side_effect = exceptions.Timeout(
        "Failed to perform, curl: (28)"
    )

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("https://fbref.com/en/comps", page_kind="competition_index")

    assert caught.value.error_class == "warm_session_timeout"


def test_a_source_verdict_outranks_the_exception_type():
    """A proxy error carrying 403 stays forbidden: the source did answer."""
    exceptions = pytest.importorskip("curl_cffi.requests.exceptions")
    fetcher = _fetcher(_response())
    error = exceptions.ProxyError("Could not connect to proxy")
    error.response = SimpleNamespace(status_code=403)
    fetcher._http_session.get.side_effect = error

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("https://fbref.com/en/comps", page_kind="competition_index")

    assert caught.value.error_class == "warm_session_forbidden"


def test_proxy_prose_about_rate_limiting_outranks_the_exception_type():
    """A proxy that reports 429 after CONNECT has no response object: the
    sentence is its only evidence, so the text must stay the primary signal."""
    exceptions = pytest.importorskip("curl_cffi.requests.exceptions")
    fetcher = _fetcher(_response())
    fetcher._http_session.get.side_effect = exceptions.ProxyError(
        "Received HTTP code 429 from proxy after CONNECT"
    )

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("https://fbref.com/en/comps", page_kind="competition_index")

    assert caught.value.error_class == "warm_session_rate_limit"


@pytest.mark.parametrize(
    "name",
    ["SSLError", "DNSError", "ConnectTimeout", "ReadTimeout"],
)
def test_transport_subclasses_are_session_failures_too(name):
    """Subclasses ride along by isinstance — pin that radius explicitly."""
    exceptions = pytest.importorskip("curl_cffi.requests.exceptions")
    error_type = getattr(exceptions, name, None)
    if error_type is None:
        pytest.skip(f"curl_cffi does not expose {name}")
    fetcher = _fetcher(_response())
    fetcher._http_session.get.side_effect = error_type("opaque libcurl prose")

    with pytest.raises(FetchError) as caught:
        fetcher.fetch("https://fbref.com/en/comps", page_kind="competition_index")

    assert caught.value.error_class.startswith("warm_session_")


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
    assert constructor.call_args.kwargs["headless"] == "virtual"
    assert constructor.call_args.kwargs["humanize"] is True
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


def test_failed_geoip_is_a_non_refreshable_hard_transport_error():
    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    transport = MagicMock()
    transport.fetch.return_value = None
    transport.traffic_delta.return_value = {
        "real_requests_count": 1,
        "real_bytes_downloaded": 0,
        "geoip_lookup_failed": True,
    }
    fetcher._http_session = None
    fetcher._transport = transport
    fetcher.bootstrap_url = "https://fbref.com/en/"

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "hard_transport_policy"
    assert "geoip_lookup_failed" in str(raised.value)


def test_unreachable_geoip_exit_is_a_re_solvable_clearance_failure():
    """A dead exit must not spend the whole wave the way a policy breach does.

    ``hard_transport_policy`` stops the wave before a new lease can spend, which
    is right for an answered geo-policy breach and wrong for an exit that never
    connected: the warm HTTP path already re-solves the identical ProxyError.
    Emitting ``clearance_failed`` puts the failure back on the session path,
    where the existing guards bound the cost (2 re-solves per target, 3 targets
    per wave).  Measured 17-18.08: 6 waves died this way, one of them after
    collecting 404 pages (#1188).
    """

    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    transport = MagicMock()
    transport.fetch.return_value = None
    transport.traffic_delta.return_value = {
        "real_requests_count": 1,
        "real_bytes_downloaded": 0,
        "geoip_lookup_failed": True,
        "geoip_transport_failure": True,
    }
    fetcher._http_session = None
    fetcher._transport = transport
    fetcher.bootstrap_url = "https://fbref.com/en/"

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "clearance_failed"
    assert "hard transport policy" not in str(raised.value)


def _fetcher_with_dead_exit_and_failing_drain(**stats_overrides):
    """A paid fetcher whose exit died and whose lease refuses to close."""

    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    transport = MagicMock()
    transport.fetch.return_value = None
    delta = {
        "real_requests_count": 1,
        "real_bytes_downloaded": 0,
        "geoip_lookup_failed": True,
        "geoip_transport_failure": True,
    }
    delta.update(stats_overrides)
    transport.traffic_delta.return_value = delta
    lease_client = MagicMock()
    lease_client.wait_drained.side_effect = FBrefProxyLeaseError(
        "FBref paid proxy drain found terminal accounting state"
    )
    fetcher._http_session = None
    fetcher._transport = transport
    fetcher.bootstrap_url = "https://fbref.com/en/"
    fetcher._lease_client = lease_client
    fetcher._provider_lease = SimpleNamespace(
        lease_id="lease-1",
        max_bytes=16 * 1024 * 1024,
    )
    fetcher._provider_context = {}
    fetcher._provider_bootstrap_max_bytes = 0
    fetcher._provider_bootstrap_spent_bytes = 0
    fetcher._provider_lease_observed_bytes = 0
    return fetcher


def test_unreachable_exit_re_solves_even_when_its_lease_will_not_drain():
    """An exit that never connected cannot have spent the ledger it won't close.

    This used to be pinned the other way: the dead exit left the lease
    unaccounted, ``wait_drained`` raised, and ``browser_provider_drain_failed``
    re-imposed ``hard_transport_policy``, discarding the wave and everything it
    had already collected.  Measured spend in that state is 352-415 bytes of a
    16 MiB lease, and the owner's call on 19.08 was to treat it as a session
    miss.  The wave now re-solves on a fresh proxy under the existing guards
    (2 re-solves per target, 3 targets per wave) (#1188).
    """

    fetcher = _fetcher_with_dead_exit_and_failing_drain()

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "clearance_failed"
    assert "browser_provider_drain_failed" not in str(raised.value)
    assert "hard transport policy" not in str(raised.value)


def test_an_exit_answering_from_a_bad_country_is_condemned_before_the_drain():
    """A lookup that failed *with* a connection is a policy breach, as before.

    The verdict here does not come from the drain guard at all -- it is already
    ``geoip_lookup_failed`` by the time the lease is asked to close -- and the
    assertion names that reason on purpose.  A mutation that widened the drain
    exemption to this case would leave the test green, so pinning
    ``hard_transport_policy`` alone would be pinning nothing.
    """

    fetcher = _fetcher_with_dead_exit_and_failing_drain(
        geoip_transport_failure=False,
    )

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "hard_transport_policy"
    assert "geoip_lookup_failed" in str(raised.value)
    assert "browser_provider_drain_failed" not in str(raised.value)


def test_a_drain_failure_after_real_traffic_still_ends_the_wave():
    """No geo-IP verdict at all: the lease simply refused to close its books."""

    fetcher = _fetcher_with_dead_exit_and_failing_drain(
        geoip_lookup_failed=False,
        geoip_transport_failure=False,
        real_bytes_downloaded=4096,
    )

    with pytest.raises(FetchError) as raised:
        fetcher._ensure_clearance()

    assert raised.value.error_class == "hard_transport_policy"
    assert "browser_provider_drain_failed" in str(raised.value)


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
        "user_agent": "Mozilla/5.0 Firefox/152",
        "browser_headers": {
            "accept": "browser-accept",
            "accept-language": "en-DE,en;q=0.9",
            "accept-encoding": "gzip, deflate, br, zstd",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
        },
        "proxy": {
            "server": "http://proxy.example:8080",
            "username": "proxy-user",
            "password": "proxy-password",
        },
    })

    assert created == [{
        "impersonate": "firefox147",
        "proxy": "http://proxy.example:8080",
        "proxy_auth": ("proxy-user", "proxy-password"),
        "trust_env": False,
        "retry": 0,
    }]
    assert "proxy-user" not in created[0]["proxy"]
    assert "proxy-password" not in created[0]["proxy"]
    assert session.cookies == {"cf_clearance": "cookie-value"}
    assert session.headers["User-Agent"] == "Mozilla/5.0 Firefox/152"
    assert session.headers["Accept"] == "browser-accept"
    assert session.headers["Accept-Language"] == "en-DE,en;q=0.9"
    assert session.headers["Accept-Encoding"] == "gzip, deflate, br, zstd"
    assert session.headers["Sec-Fetch-Dest"] == "document"
    assert session.headers["Sec-Fetch-Mode"] == "navigate"
    assert session.headers["Sec-Fetch-Site"] == "none"


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


def test_lease_drain_failure_names_itself_before_ending_the_wave(caplog):
    """A silent drain failure is indistinguishable from a policy breach.

    ``browser_provider_drain_failed`` stops the wave, and the swallowed
    exception is the only evidence of what the meter actually refused.  The
    geo-IP path proved the cost of leaving that blind: a full day of red runs
    whose cause only became visible once the type reached the log (#1188).
    """

    import logging

    fetcher = FBrefFetcher.__new__(FBrefFetcher)
    transport = MagicMock()
    transport.fetch.return_value = None
    transport.traffic_delta.return_value = {
        "real_requests_count": 1,
        "real_bytes_downloaded": 0,
    }
    fetcher._http_session = None
    fetcher._transport = transport
    fetcher.bootstrap_url = "https://fbref.com/en/"
    fetcher._lease_client = MagicMock()
    fetcher._wait_and_observe_provider = MagicMock(
        side_effect=RuntimeError("meter rejected drain: counters not final")
    )
    fetcher._provider_bootstrap_max_bytes = 0
    fetcher._provider_bootstrap_spent_bytes = 0
    fetcher._provider_lease = None
    fetcher._provider_lease_observed_bytes = 0

    with caplog.at_level(logging.WARNING, logger="scrapers.fbref.fetcher"):
        with pytest.raises(FetchError) as raised:
            fetcher._ensure_clearance()

    assert raised.value.error_class == "hard_transport_policy"
    assert "browser_provider_drain_failed" in str(raised.value)
    assert "RuntimeError" in caplog.text
    assert "counters not final" in caplog.text
