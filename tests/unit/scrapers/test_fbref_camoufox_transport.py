"""Network-free tests for the single FBref Camoufox clearance transport."""

import base64
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, call as mock_call

import pytest

from scrapers.fbref.camoufox_fetch import (
    BROWSER_REQUEST_FIXED_OVERHEAD_BYTES,
    BROWSER_UNDECLARED_BODY_RESERVATION_BYTES,
    CamoufoxFbrefTransport,
    FIREFOX_METERED_NETWORK_PREFS,
    GEOIP_BYTE_RESERVATION_BYTES,
    GEOIP_LOOKUP_URL,
    GEOIP_REQUEST_RESERVATION,
    UNEXPECTED_BROWSER_NETWORK_RESERVATION_BYTES,
    _navigation_error_type,
    is_cloudflare_blocked,
    resolve_geoip_without_redirects,
    should_block_request,
)


def _install_camoufox_mocks(monkeypatch, camoufox):
    modules = __import__("sys").modules
    monkeypatch.setitem(
        modules,
        "camoufox.sync_api",
        MagicMock(Camoufox=camoufox),
    )
    monkeypatch.setitem(
        modules,
        "camoufox.addons",
        MagicMock(DefaultAddons=(SimpleNamespace(name="UBO"),)),
    )


@pytest.mark.unit
@pytest.mark.parametrize("resource_type", ["image", "media", "font", "stylesheet"])
def test_blocks_nonessential_assets(resource_type):
    assert should_block_request(resource_type, "https://fbref.com/x") is True


@pytest.mark.unit
def test_never_blocks_turnstile_assets():
    assert should_block_request(
        "stylesheet", "https://challenges.cloudflare.com/turnstile/x.css"
    ) is False


@pytest.mark.unit
def test_blocks_known_autocomplete_payload():
    assert should_block_request(
        "xhr", "https://fbref.com/short/inc/players_search_list.csv"
    ) is True


@pytest.mark.unit
@pytest.mark.parametrize(
    "html",
    ["", "<html>Just a moment...</html>", "<html>cf_chl_opt</html>"],
)
def test_cloudflare_shell_detection(html):
    assert is_cloudflare_blocked(html) is True


@pytest.mark.unit
def test_real_table_page_is_not_cloudflare_shell():
    assert is_cloudflare_blocked("<html><table id='stats'></table></html>") is False


@pytest.mark.unit
def test_budget_arguments_must_be_positive():
    with pytest.raises(ValueError, match="max_network_requests"):
        CamoufoxFbrefTransport(max_network_requests=0)
    with pytest.raises(ValueError, match="max_network_bytes"):
        CamoufoxFbrefTransport(max_network_bytes=0)


@pytest.mark.unit
def test_metered_browser_disables_every_pre_route_browser_transport():
    assert FIREFOX_METERED_NETWORK_PREFS == {
        "media.peerconnection.enabled": False,
        "network.webtransport.enabled": False,
        "network.http.http3.enable": False,
        "network.http.redirection-limit": 0,
        "network.proxy.failover_direct": False,
        "network.http.speculative-parallel-limit": 0,
        "network.preconnect": False,
        "network.early-hints.enabled": False,
        "network.early-hints.preconnect.enabled": False,
        "network.prefetch-next": False,
        "network.dns.disablePrefetch": True,
        "network.predictor.enabled": False,
    }


class _Route:
    def __init__(self, resource_type="document", url="https://fbref.com/en/"):
        self.request = MagicMock(resource_type=resource_type, url=url)
        self.aborted = 0
        self.continued = 0

    def abort(self):
        self.aborted += 1

    def continue_(self):
        self.continued += 1


class _Response:
    def __init__(self, request, headers, status=200):
        self.request = request
        self.headers = headers
        self.status = status


class _StatusFailsResponse:
    def __init__(self, request, headers):
        self.request = request
        self.headers = headers

    @property
    def status(self):
        raise RuntimeError("response detached")


class _ContinueFailsRoute(_Route):
    def continue_(self):
        self.continued += 1
        raise RuntimeError("continue failed after request handoff")


@pytest.mark.unit
def test_intentionally_aborted_duplicate_urls_do_not_latch_unrouted_failure():
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * UNEXPECTED_BROWSER_NETWORK_RESERVATION_BYTES,
    )
    url = "https://fbref.com/static/badge.png"
    first = _Route(resource_type="image", url=url)
    second = _Route(resource_type="image", url=url)

    transport._maybe_block(first)
    transport._maybe_block(second)

    # Firefox can re-wrap both Request objects before requestfailed. URL queue
    # multiplicity must still consume exactly the two intentional aborts.
    for _ in range(2):
        failed = MagicMock(resource_type="image", url=url)
        failed.failure = "NS_BINDING_ABORTED"
        transport._on_request_failed(failed)

    stats = transport.traffic_stats()
    assert first.aborted == second.aborted == 1
    assert first.continued == second.continued == 0
    assert stats["real_requests_count"] == 0
    assert stats["budget_unobserved_bytes"] == 0
    assert stats["network_policy_failed"] is False
    assert transport._intentional_abort_request_urls == {}
    assert transport._intentional_abort_ids_by_url == {}


@pytest.mark.unit
def test_unknown_request_failure_latches_and_charges_once():
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * UNEXPECTED_BROWSER_NETWORK_RESERVATION_BYTES,
    )
    failed = MagicMock(
        resource_type="document",
        url="https://fbref.com/unrouted",
    )
    failed.failure = "NS_ERROR_NET_RESET"

    transport._on_request_failed(failed)

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 1
    assert stats["budget_unobserved_bytes"] == (
        UNEXPECTED_BROWSER_NETWORK_RESERVATION_BYTES
    )
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "unrouted_http_failure"


@pytest.mark.unit
def test_duplicate_routed_failure_stays_settled_for_late_response():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * admission,
    )
    route = _Route(url="https://fbref.com/en/")
    route.request._impl_obj._guid = "request@bootstrap"
    route.request.sizes.side_effect = RuntimeError("sizes unavailable")
    transport._navigation_source_url = "https://fbref.com/en/"
    transport._maybe_block(route)

    route.request.failure = "NS_ERROR_PROXY_BAD_GATEWAY"
    transport._on_request_failed(route.request)
    duplicate = MagicMock(
        resource_type="document",
        url=route.request.url,
        is_navigation_request=True,
    )
    duplicate.failure = "NS_ERROR_PROXY_BAD_GATEWAY"
    duplicate._impl_obj._guid = "request@bootstrap"
    transport._on_request_failed(duplicate)

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 1
    assert stats["unobserved_reserved_bytes"] == admission
    assert stats["network_policy_failed"] is False
    assert len(transport._settled_failed_request_urls) == 1

    late = MagicMock(
        resource_type="document",
        url=route.request.url,
        is_navigation_request=True,
    )
    late._impl_obj._guid = "request@bootstrap"
    transport._on_response(
        _Response(
            late,
            {"content-length": "0", "location": "/next"},
            status=302,
        )
    )

    assert transport._pending_manual_redirect_url == "https://fbref.com/next"
    assert transport.traffic_stats()["network_policy_failed"] is False
    assert transport._settled_failed_request_urls == {}
    assert transport._settled_failed_request_guids == {}
    assert transport._late_response_request_guids == {"request@bootstrap"}


@pytest.mark.unit
def test_routed_finish_before_response_accepts_only_that_late_response():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * admission,
    )
    route = _Route(url="https://fbref.com/en/")
    route.request._impl_obj._guid = "request@bootstrap"
    route.request.sizes.return_value = {
        "responseBodySize": 100,
        "responseHeadersSize": 20,
        "requestBodySize": 0,
        "requestHeadersSize": 10,
    }
    transport._navigation_source_url = "https://fbref.com/en/"
    transport._maybe_block(route)

    # This is the order observed from the real Firefox canary: the admitted
    # request settles, then its response callback is dispatched.
    transport._on_request_finished(route.request)
    late = MagicMock(
        resource_type="document",
        url=route.request.url,
        is_navigation_request=True,
    )
    late._impl_obj._guid = "request@bootstrap"
    transport._on_response(_Response(late, {}, status=403))

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 1
    assert stats["real_bytes_downloaded"] == 130
    assert stats["network_policy_failed"] is False
    assert transport._settled_finished_request_urls == {}
    assert transport._settled_finished_request_guids == {}
    assert transport._late_response_request_guids == {"request@bootstrap"}


@pytest.mark.unit
def test_duplicate_response_after_normal_finish_is_accepted_once():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * admission,
    )
    route = _Route(url="https://fbref.com/en/")
    route.request._impl_obj._guid = "request@bootstrap"
    route.request.sizes.return_value = {
        "responseBodySize": 100,
        "responseHeadersSize": 20,
        "requestBodySize": 0,
        "requestHeadersSize": 10,
    }
    transport._maybe_block(route)
    transport._on_response(
        _Response(route.request, {"content-length": "100"}, status=403)
    )
    transport._on_request_finished(route.request)

    duplicate = MagicMock(resource_type="document", url=route.request.url)
    duplicate._impl_obj._guid = "request@bootstrap"
    transport._on_response(_Response(duplicate, {}, status=403))
    transport._on_request_finished(duplicate)
    transport._on_response(_Response(duplicate, {}, status=403))
    duplicate.failure = "NS_ERROR_NET_RESET"
    transport._on_request_failed(duplicate)

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 1
    assert stats["real_bytes_downloaded"] == 130
    assert stats["network_policy_failed"] is False
    assert transport._settled_finished_request_urls == {}


@pytest.mark.unit
def test_late_oversized_response_trips_byte_cap_after_unknown_finish():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * admission,
    )
    route = _Route(url="https://fbref.com/en/")
    route.request._impl_obj._guid = "request@bootstrap"
    route.request.sizes.side_effect = RuntimeError("sizes unavailable")
    transport._maybe_block(route)
    transport._on_request_finished(route.request)

    late = MagicMock(resource_type="document", url=route.request.url)
    late._impl_obj._guid = "request@bootstrap"
    transport._on_response(_Response(late, {}, status=200))
    assert transport.traffic_stats()["byte_budget_exhausted"] is False

    declared_body = BROWSER_UNDECLARED_BODY_RESERVATION_BYTES + 1
    transport._on_response(
        _Response(
            late,
            {"content-length": str(declared_body)},
            status=200,
        )
    )

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"] == (
        "late_declared_body_exceeds_settled_reservation:"
        f"{BROWSER_REQUEST_FIXED_OVERHEAD_BYTES + declared_body}>{admission}"
    )
    assert stats["unobserved_reserved_bytes"] == (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES + declared_body
    )


@pytest.mark.unit
def test_late_oversized_response_tops_up_successful_size_accounting():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * admission,
    )
    route = _Route(url="https://fbref.com/en/")
    route.request._impl_obj._guid = "request@bootstrap"
    route.request.sizes.return_value = {
        "responseBodySize": 100,
        "responseHeadersSize": 20,
        "requestBodySize": 0,
        "requestHeadersSize": 10,
    }
    transport._maybe_block(route)
    transport._on_request_finished(route.request)

    late = MagicMock(resource_type="document", url=route.request.url)
    late._impl_obj._guid = "request@bootstrap"
    declared_body = BROWSER_UNDECLARED_BODY_RESERVATION_BYTES + 1
    desired = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES + declared_body
    transport._on_response(
        _Response(
            late,
            {"content-length": str(declared_body)},
            status=200,
        )
    )

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is True
    assert stats["real_bytes_downloaded"] == 130
    assert stats["unobserved_reserved_bytes"] == desired - 130
    assert stats["budget_bytes_consumed"] == desired


@pytest.mark.unit
def test_settled_marker_retains_wrapper_until_session_cleanup():
    transport = CamoufoxFbrefTransport(max_network_requests=5)
    route = _Route(url="https://fbref.com/en/")
    route.request._impl_obj._guid = "request@bootstrap"
    route.request.sizes.side_effect = RuntimeError("sizes unavailable")
    transport._maybe_block(route)
    transport._on_request_finished(route.request)

    key = id(route.request)
    assert transport._settled_finished_request_objects[key] is route.request

    transport._clear_request_callback_tracking()

    assert transport._settled_finished_request_objects == {}


@pytest.mark.unit
def test_no_guid_exact_wrapper_accepts_repeated_late_callbacks():
    transport = CamoufoxFbrefTransport(max_network_requests=5)
    route = _Route(url="https://fbref.com/en/")
    route.request.sizes.side_effect = RuntimeError("sizes unavailable")
    transport._maybe_block(route)
    transport._on_request_finished(route.request)

    response = _Response(route.request, {}, status=403)
    transport._on_response(response)
    transport._on_request_finished(route.request)
    transport._on_response(response)

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 1
    assert stats["network_policy_failed"] is False


@pytest.mark.unit
def test_settled_finish_does_not_hide_same_url_response_with_different_guid():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * admission,
    )
    admitted = _Route(url="https://fbref.com/en/")
    admitted.request._impl_obj._guid = "request@admitted"
    admitted.request.sizes.return_value = {
        "responseBodySize": 100,
        "responseHeadersSize": 20,
        "requestBodySize": 0,
        "requestHeadersSize": 10,
    }
    transport._maybe_block(admitted)
    transport._on_request_finished(admitted.request)

    escaped = MagicMock(
        resource_type="document",
        url=admitted.request.url,
    )
    escaped._impl_obj._guid = "request@escaped"
    transport._on_response(_Response(escaped, {}, status=403))

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 2
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "unrouted_http_response"


@pytest.mark.unit
def test_settled_finish_does_not_hide_same_guid_response_with_different_url():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * admission,
    )
    admitted = _Route(url="https://fbref.com/en/")
    admitted.request._impl_obj._guid = "request@admitted"
    admitted.request.sizes.return_value = {
        "responseBodySize": 100,
        "responseHeadersSize": 20,
        "requestBodySize": 0,
        "requestHeadersSize": 10,
    }
    transport._maybe_block(admitted)
    transport._on_request_finished(admitted.request)

    escaped = MagicMock(
        resource_type="document",
        url="https://fbref.com/en/other",
    )
    escaped._impl_obj._guid = "request@admitted"
    transport._on_response(_Response(escaped, {}, status=403))

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 2
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "unrouted_http_response"


@pytest.mark.unit
def test_no_guid_settled_finish_does_not_hide_same_url_other_wrapper():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * admission,
    )
    admitted = _Route(url="https://fbref.com/en/")
    admitted.request.sizes.return_value = {
        "responseBodySize": 100,
        "responseHeadersSize": 20,
        "requestBodySize": 0,
        "requestHeadersSize": 10,
    }
    transport._maybe_block(admitted)
    transport._on_request_finished(admitted.request)

    escaped = MagicMock(resource_type="document", url=admitted.request.url)
    transport._on_response(_Response(escaped, {}, status=403))

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 2
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "unrouted_http_response"


@pytest.mark.unit
def test_proxy_bad_gateway_is_a_rotatable_network_failure():
    assert _navigation_error_type(
        RuntimeError("Page.goto: NS_ERROR_PROXY_BAD_GATEWAY")
    ) == "network"

    callback = MagicMock()
    transport = CamoufoxFbrefTransport(
        geoip=False,
        proxy_result_callback=callback,
    )
    transport._page = MagicMock()
    transport._page.goto.side_effect = [
        RuntimeError("Page.goto: NS_ERROR_PROXY_BAD_GATEWAY"),
        None,
    ]
    transport._restart = MagicMock()  # type: ignore[method-assign]
    transport._solve_current_page = MagicMock(  # type: ignore[method-assign]
        return_value="<html><table></table></html>"
    )

    assert transport.fetch("https://fbref.com/en/") == (
        "<html><table></table></html>"
    )
    assert transport._page.goto.call_count == 2
    transport._restart.assert_called_once_with()
    assert callback.call_args_list == [
        mock_call(False, "network"),
        mock_call(True, None),
    ]


@pytest.mark.unit
def test_settled_failure_does_not_hide_same_url_with_a_different_guid():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * admission,
    )
    admitted = _Route(url="https://fbref.com/admitted")
    admitted.request._impl_obj._guid = "request@admitted"
    admitted.request.sizes.side_effect = RuntimeError("sizes unavailable")
    transport._maybe_block(admitted)
    admitted.request.failure = "NS_ERROR_PROXY_BAD_GATEWAY"
    transport._on_request_failed(admitted.request)

    escaped = MagicMock(
        resource_type="document",
        url=admitted.request.url,
    )
    escaped._impl_obj._guid = "request@escaped"
    escaped.failure = "NS_ERROR_NET_RESET"
    transport._on_request_failed(escaped)

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 2
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "unrouted_http_failure"


@pytest.mark.unit
def test_settled_failure_does_not_hide_same_url_response_with_different_guid():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * admission,
    )
    admitted = _Route(url="https://fbref.com/admitted")
    admitted.request._impl_obj._guid = "request@admitted"
    admitted.request.sizes.side_effect = RuntimeError("sizes unavailable")
    transport._maybe_block(admitted)
    admitted.request.failure = "NS_ERROR_PROXY_BAD_GATEWAY"
    transport._on_request_failed(admitted.request)

    escaped = MagicMock(
        resource_type="document",
        url=admitted.request.url,
    )
    escaped._impl_obj._guid = "request@escaped"
    transport._on_response(
        _Response(escaped, {"content-length": "0"}, status=200)
    )

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 2
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "unrouted_http_response"


@pytest.mark.unit
def test_request_budget_aborts_before_network():
    transport = CamoufoxFbrefTransport(max_network_requests=2)
    routes = [_Route(), _Route(), _Route()]

    for route in routes:
        transport._maybe_block(route)

    assert [route.continued for route in routes] == [1, 1, 0]
    assert routes[-1].aborted == 1
    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 2
    assert stats["budget_blocked_count"] == 1


@pytest.mark.unit
def test_byte_budget_aborts_subsequent_request():
    transport = CamoufoxFbrefTransport(max_network_bytes=100)
    transport._bytes_total = 100
    route = _Route()

    transport._maybe_block(route)

    assert route.aborted == 1
    assert route.continued == 0


@pytest.mark.unit
def test_continue_exception_keeps_unknown_reservation_charged():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    ceiling = BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=(2 * (overhead + ceiling)) - 1
    )
    failed = _ContinueFailsRoute()
    failed.request.sizes.side_effect = RuntimeError("sizes unavailable")

    transport._maybe_block(failed)
    # A later Playwright failure callback must not double-charge or release it.
    transport._on_request_failed(failed.request)
    next_route = _Route(url="https://fbref.com/next")
    transport._maybe_block(next_route)

    stats = transport.traffic_stats()
    assert failed.continued == 1
    assert failed.aborted == 1
    assert stats["unobserved_reserved_bytes"] == overhead + ceiling
    assert stats["inflight_reserved_bytes"] == 0
    assert next_route.aborted == 1
    assert next_route.continued == 0


@pytest.mark.unit
def test_browser_aborts_before_network_when_body_ceiling_cannot_fit():
    cap = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES + 100
    transport = CamoufoxFbrefTransport(max_network_bytes=cap)
    route = _Route()
    transport._maybe_block(route)

    stats = transport.traffic_stats()
    assert route.continued == 0
    assert route.aborted == 1
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"] == "request_admission_exceeds_byte_cap"
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["budget_unobserved_bytes"] == 0
    assert stats["budget_blocked_count"] == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"content-length": "not-a-number"},
        {"content-length": "10", "transfer-encoding": "chunked"},
    ],
)
def test_undeclared_body_reserves_the_ceiling_and_settles_to_observed(headers):
    """Cloudflare/HTTP-2 responses carry no usable Content-Length; the guard
    must reserve a ceiling instead of killing the clearance session."""
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    ceiling = BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=overhead + ceiling + 1_000
    )
    route = _Route()
    transport._maybe_block(route)

    transport._on_response(_Response(route.request, headers))

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is False
    assert stats["inflight_reserved_bytes"] == overhead + ceiling

    route.request.sizes.return_value = {
        "responseBodySize": 40_000,
        "responseHeadersSize": 1_000,
        "requestBodySize": 0,
        "requestHeadersSize": 500,
    }
    transport._on_request_finished(route.request)

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is False
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["real_bytes_downloaded"] == 41_500


@pytest.mark.unit
def test_undeclared_body_that_outgrows_its_reservation_aborts_at_completion():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    ceiling = BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=overhead + ceiling + 1_000
    )
    route = _Route()
    transport._maybe_block(route)
    transport._on_response(_Response(route.request, {}))

    oversized = overhead + ceiling + 1
    route.request.sizes.return_value = {
        "responseBodySize": oversized,
        "responseHeadersSize": 0,
        "requestBodySize": 0,
        "requestHeadersSize": 0,
    }
    transport._on_request_finished(route.request)

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"].startswith(
        "completed_size_exceeded_reservation:"
    )


@pytest.mark.unit
def test_browser_parallel_requests_cannot_overbook_body_ceilings():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    admission = overhead + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=2 * admission
    )
    first = _Route(url="https://fbref.com/one")
    second = _Route(url="https://fbref.com/two")
    third = _Route(url="https://fbref.com/three")
    transport._maybe_block(first)
    transport._maybe_block(second)
    transport._maybe_block(third)

    stats = transport.traffic_stats()
    assert first.continued == second.continued == 1
    assert third.continued == 0
    assert third.aborted == 1
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"] == "request_admission_exceeds_byte_cap"
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["unobserved_reserved_bytes"] == 2 * admission


@pytest.mark.unit
def test_browser_releases_declared_reservation_after_finish_and_failure():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    admission = overhead + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=(2 * admission) + 1000
    )
    finished = _Route(url="https://fbref.com/finished")
    failed = _Route(url="https://fbref.com/failed")
    finished.request.sizes.return_value = {
        "responseBodySize": 100,
        "responseHeadersSize": 20,
        "requestBodySize": 0,
        "requestHeadersSize": 10,
    }
    failed.request.sizes.side_effect = RuntimeError("sizes unavailable")
    transport._maybe_block(finished)
    transport._maybe_block(failed)
    transport._on_response(
        _Response(finished.request, {"content-length": "100"})
    )
    transport._on_response(
        _Response(failed.request, {"content-length": "200"})
    )

    transport._on_request_finished(finished.request)
    assert transport.traffic_stats()["inflight_reserved_bytes"] == (
        overhead + 200
    )
    transport._on_request_failed(failed.request)

    stats = transport.traffic_stats()
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["real_bytes_downloaded"] == 130
    assert stats["unobserved_reserved_bytes"] == overhead + 200
    assert stats["byte_budget_exhausted"] is False


@pytest.mark.unit
def test_unrouted_response_latches_policy_and_conservative_charge():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    admission = overhead + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=3 * admission,
        max_network_requests=5,
    )
    route = _Route()
    transport._maybe_block(route)
    transport._on_response(
        _Response(route.request, {"content-length": "50"})
    )

    hop_request = MagicMock(
        resource_type="document", url="https://fbref.com/en/?redirected=1"
    )
    hop_request.redirected_from = route.request
    transport._on_response(
        _Response(hop_request, {"content-length": "60"})
    )
    terminal_rewrap = MagicMock(
        resource_type="document", url=hop_request.url
    )
    terminal_rewrap.failure = "NS_ERROR_NET_RESET"
    transport._on_request_failed(terminal_rewrap)

    stats = transport.traffic_stats()
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "unrouted_http_response"
    assert stats["inflight_reserved_bytes"] == overhead + 50
    assert stats["unobserved_reserved_bytes"] == admission
    assert stats["real_requests_count"] == 2
    assert stats["completed_requests_count"] == 0


@pytest.mark.unit
def test_same_url_response_with_different_guid_is_unrouted():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * admission,
    )
    admitted = _Route(url="https://fbref.com/en/")
    admitted.request._impl_obj._guid = "request@admitted"
    transport._maybe_block(admitted)
    escaped = MagicMock(
        resource_type="document",
        url=admitted.request.url,
    )
    escaped._impl_obj._guid = "request@escaped"

    transport._on_response(
        _Response(escaped, {"content-length": "0"}, status=200)
    )

    stats = transport.traffic_stats()
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "unrouted_http_response"
    assert stats["real_requests_count"] == 2


@pytest.mark.unit
def test_unrouted_response_diagnostic_is_useful_and_redacted(caplog):
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * UNEXPECTED_BROWSER_NETWORK_RESERVATION_BYTES,
    )
    transport._navigation_source_url = "https://fbref.com/en/"
    request = MagicMock(
        resource_type="document",
        url="https://fbref.com/en/?challenge_secret=do-not-log",
        is_navigation_request=True,
        redirected_from=None,
    )
    request._impl_obj._guid = "request@escaped"

    with caplog.at_level("ERROR"):
        transport._on_response(
            _Response(
                request,
                {"location": "/next?token=do-not-log"},
                status=302,
            )
        )

    assert "response missed route admission" in caplog.text
    assert "status=302" in caplog.text
    assert "resource=document" in caplog.text
    assert "same_guid_routed_failure=False" in caplog.text
    assert "same_guid_late_response=False" in caplog.text
    assert "location_present=True" in caplog.text
    assert "challenge_secret" not in caplog.text
    assert "do-not-log" not in caplog.text
    assert transport.traffic_stats()["network_policy_failed"] is True


@pytest.mark.unit
def test_unrouted_response_stays_fail_closed_when_diagnostic_raises(caplog):
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * UNEXPECTED_BROWSER_NETWORK_RESERVATION_BYTES,
    )
    transport._log_unrouted_response = MagicMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("secret-url-token")
    )
    request = MagicMock(
        resource_type="document",
        url="https://fbref.com/en/escaped",
    )

    with caplog.at_level("ERROR"):
        transport._on_response(
            _Response(request, {"content-length": "0"}, status=200)
        )

    stats = transport.traffic_stats()
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "unrouted_http_response"
    assert stats["real_requests_count"] == 1
    assert "secret-url-token" not in caplog.text


@pytest.mark.unit
def test_unrouted_request_completion_latches_and_charges_once():
    transport = CamoufoxFbrefTransport(
        max_network_requests=5,
        max_network_bytes=3 * UNEXPECTED_BROWSER_NETWORK_RESERVATION_BYTES,
    )
    completed = MagicMock(
        resource_type="document",
        url="https://fbref.com/unrouted-complete",
    )

    transport._on_request_finished(completed)

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 1
    assert stats["completed_requests_count"] == 0
    assert stats["budget_unobserved_bytes"] == (
        UNEXPECTED_BROWSER_NETWORK_RESERVATION_BYTES
    )
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "unrouted_http_completion"


@pytest.mark.unit
def test_rewrapped_request_reuses_its_route_reservation():
    """Firefox can deliver the response with a different Request wrapper for
    the request route() already reserved; it must not be charged twice."""
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    admission = overhead + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=(4 * admission) + 1000,
        max_network_requests=5,
    )
    route = _Route()
    route.request._impl_obj._guid = "request@rewrapped"
    transport._maybe_block(route)

    rewrapped = MagicMock(resource_type="document", url=route.request.url)
    rewrapped._impl_obj._guid = "request@rewrapped"
    rewrapped.redirected_from = None
    transport._on_response(_Response(rewrapped, {"content-length": "50"}))

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is False
    assert stats["real_requests_count"] == 1  # one slot, not two
    assert stats["inflight_reserved_bytes"] == overhead + 50

    rewrapped.sizes.return_value = {
        "responseBodySize": 50,
        "responseHeadersSize": 10,
        "requestBodySize": 0,
        "requestHeadersSize": 10,
    }
    transport._on_request_finished(rewrapped)

    stats = transport.traffic_stats()
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["unobserved_reserved_bytes"] == 0
    assert stats["real_bytes_downloaded"] == 70
    assert transport._inflight_request_guids == {}
    assert transport._inflight_ids_by_guid == {}


@pytest.mark.unit
def test_late_callbacks_after_byte_abort_are_not_counted_as_unrouted():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(max_network_bytes=admission)
    admitted = _Route(url="https://fbref.com/admitted")
    blocked = _Route(url="https://fbref.com/blocked")
    transport._maybe_block(admitted)
    transport._maybe_block(blocked)

    transport._on_response(
        _Response(admitted.request, {"content-length": "10"})
    )
    failed_rewrap = MagicMock(
        resource_type="document", url=admitted.request.url
    )
    failed_rewrap.failure = "NS_BINDING_ABORTED"
    transport._on_request_failed(failed_rewrap)

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 1
    assert stats["network_policy_failed"] is False
    assert stats["byte_budget_exhausted"] is True
    assert stats["unobserved_reserved_bytes"] == admission


@pytest.mark.unit
def test_failed_request_unknown_bytes_keep_full_reservation_charged():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    admission = overhead + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=overhead + 100 + admission - 1
    )
    failed = _Route(url="https://fbref.com/failed")
    failed.request.sizes.side_effect = RuntimeError("sizes unavailable")
    transport._maybe_block(failed)
    transport._on_response(
        _Response(failed.request, {"content-length": "100"})
    )

    transport._on_request_failed(failed.request)
    next_route = _Route(url="https://fbref.com/next")
    transport._maybe_block(next_route)

    stats = transport.traffic_stats()
    assert stats["unobserved_reserved_bytes"] == overhead + 100
    assert stats["inflight_reserved_bytes"] == 0
    assert next_route.aborted == 1
    assert next_route.continued == 0


@pytest.mark.unit
def test_failed_request_uses_observed_sizes_when_available():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    admission = overhead + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    transport = CamoufoxFbrefTransport(max_network_bytes=2 * admission)
    failed = _Route(url="https://fbref.com/failed")
    failed.request.sizes.return_value = {
        "responseBodySize": 10,
        "responseHeadersSize": 10,
        "requestBodySize": 0,
        "requestHeadersSize": 10,
    }
    transport._maybe_block(failed)

    transport._on_request_failed(failed.request)

    stats = transport.traffic_stats()
    assert stats["real_bytes_downloaded"] == 30
    assert stats["unobserved_reserved_bytes"] == 0


@pytest.mark.unit
def test_teardown_conservatively_charges_still_inflight_reservations():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    admission = overhead + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    transport = CamoufoxFbrefTransport(max_network_bytes=admission + 100)
    route = _Route()
    transport._maybe_block(route)
    transport._on_response(
        _Response(route.request, {"content-length": "100"})
    )

    transport._stop()

    stats = transport.traffic_stats()
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["unobserved_reserved_bytes"] == overhead + 100
    assert stats["budget_bytes_consumed"] == overhead + 100


@pytest.mark.unit
def test_close_cancellation_settles_admitted_request_without_policy_latch():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(max_network_bytes=2 * admission)
    route = _Route()
    route.request.sizes.side_effect = RuntimeError("sizes unavailable")
    transport._maybe_block(route)
    cm = MagicMock()

    def cancel_admitted(*_args):
        rewrapped = MagicMock(
            resource_type="document", url=route.request.url
        )
        rewrapped.failure = "NS_BINDING_ABORTED"
        rewrapped.sizes.side_effect = RuntimeError("sizes unavailable")
        transport._on_request_failed(rewrapped)

    cm.__exit__.side_effect = cancel_admitted
    transport._cm = cm

    transport.close()

    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 1
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["unobserved_reserved_bytes"] == admission
    assert stats["network_policy_failed"] is False


@pytest.mark.unit
def test_teardown_failure_propagates_kills_and_retains_cleanup_handle():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(max_network_bytes=2 * admission)
    route = _Route()
    transport._maybe_block(route)
    cm = MagicMock()
    cm.__exit__.side_effect = RuntimeError("browser close failed")
    transport._cm = cm
    transport._browser = MagicMock()
    transport._context = MagicMock()
    transport._page = MagicMock()
    transport._kill_browser_processes = MagicMock()  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="browser close failed"):
        transport.close()

    # The active page cannot be reused, while the Camoufox manager remains for
    # an explicit cleanup retry. Unknown in-flight bytes are permanently spent.
    assert transport._cm is cm
    assert transport._browser is None
    assert transport._context is None
    assert transport._page is None
    transport._kill_browser_processes.assert_called_once_with(
        "teardown", None
    )
    assert transport.traffic_stats()["budget_unobserved_bytes"] == admission

    with pytest.raises(RuntimeError, match="reuse disabled"):
        transport._start()

    # A later successful physical cleanup does not hide the failed boundary.
    cm.__exit__.side_effect = None
    with pytest.raises(RuntimeError, match="browser close failed"):
        transport.close()
    assert cm.__exit__.call_count == 2
    assert transport._cm is None


@pytest.mark.unit
@pytest.mark.parametrize("cleanup", ["stop", "abort"])
def test_inflight_cleanup_clears_repeated_url_indexes(cleanup):
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    admission = overhead + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    url = "https://fbref.com/en/repeated"
    transport = CamoufoxFbrefTransport(max_network_bytes=2 * admission)
    first = _Route(url=url)
    second = _Route(url=url)
    transport._maybe_block(first)
    transport._maybe_block(second)

    assert len(transport._inflight_ids_by_url[url]) == 2
    if cleanup == "stop":
        transport._stop()
    else:
        transport._abort_session_for_byte_budget("test_cleanup")

    assert transport._inflight_byte_reservations == {}
    assert transport._inflight_request_urls == {}
    assert transport._inflight_ids_by_url == {}
    stats = transport.traffic_stats()
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["unobserved_reserved_bytes"] == 2 * admission


@pytest.mark.unit
def test_browser_rejects_request_before_network_when_ceiling_does_not_fit():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    transport = CamoufoxFbrefTransport(max_network_bytes=overhead + 9)
    route = _Route()
    transport._maybe_block(route)

    transport._on_response(
        _Response(route.request, {"content-length": "10"})
    )

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is True
    assert route.continued == 0
    assert route.aborted == 1
    assert stats["byte_budget_failure"] == "request_admission_exceeds_byte_cap"


@pytest.mark.unit
def test_finished_request_accounts_rx_and_tx_bytes():
    transport = CamoufoxFbrefTransport()
    route = _Route(resource_type="document")
    request = route.request
    request.sizes.return_value = {
        "responseBodySize": 1000,
        "responseHeadersSize": 100,
        "requestBodySize": 10,
        "requestHeadersSize": 40,
    }

    transport._maybe_block(route)
    transport._on_request_finished(request)

    stats = transport.traffic_stats()
    assert stats["real_bytes_downloaded"] == 1150
    assert stats["real_requests_count"] == 1
    assert stats["real_bytes_by_resource_type"]["document"] == 1150


@pytest.mark.unit
def test_bootstrap_navigation_attempts_count_failed_rotation_exactly():
    transport = CamoufoxFbrefTransport()
    page = MagicMock()
    page.goto.side_effect = [RuntimeError("ERR_CONNECTION_RESET"), None]
    transport._page = page
    transport._lifecycle_action = MagicMock(return_value=True)
    transport._solve_current_page = MagicMock(
        return_value="<html><table></table></html>"
    )

    html = transport.fetch("https://fbref.com/en/")

    assert html is not None
    assert page.goto.call_count == 2
    assert transport.traffic_stats()["browser_bootstrap_attempts"] == 2


@pytest.mark.unit
def test_clearance_exports_cookie_fingerprint_and_bound_proxy():
    transport = CamoufoxFbrefTransport()
    transport._proxy = {
        "server": "http://proxy.example:8000",
        "username": "user",
        "password": "secret",
    }
    page = MagicMock()
    page.context.cookies.return_value = [
        {"name": "cf_clearance", "value": "token"},
        {"name": "other", "value": "value"},
    ]
    page.evaluate.return_value = "Mozilla/5.0 Firefox/152.0"
    transport._page = page
    route = _Route()
    route.request.all_headers.return_value = {
        "accept": "browser-accept",
        "accept-language": "en-DE,en;q=0.9",
        "accept-encoding": "gzip, deflate, br, zstd",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
    }
    transport._maybe_block(route)

    clearance = transport.get_clearance()

    assert clearance["cookies"]["cf_clearance"] == "token"
    assert clearance["user_agent"] == "Mozilla/5.0 Firefox/152.0"
    assert clearance["browser_headers"] == {
        "accept": "browser-accept",
        "accept-language": "en-DE,en;q=0.9",
        "accept-encoding": "gzip, deflate, br, zstd",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
    }
    assert clearance["proxy"] == transport._proxy


@pytest.mark.unit
def test_clearance_requires_cf_cookie():
    transport = CamoufoxFbrefTransport()
    page = MagicMock()
    page.context.cookies.return_value = [{"name": "other", "value": "value"}]
    transport._page = page

    assert transport.get_clearance() is None


@pytest.mark.unit
def test_restart_drops_previous_session_navigation_headers(monkeypatch):
    page = MagicMock()
    context = MagicMock()
    context.new_page.return_value = page
    browser = MagicMock()
    browser.new_context.return_value = context
    camoufox = MagicMock()
    camoufox.return_value.__enter__.return_value = browser
    _install_camoufox_mocks(monkeypatch, camoufox)
    transport = CamoufoxFbrefTransport(
        proxy={"server": "http://proxy.example:8000"},
        geoip=False,
    )

    transport._start()
    old_route = _Route()
    old_route.request.all_headers.return_value = {
        "accept": "old-accept",
        "accept-language": "old-language",
        "accept-encoding": "old-encoding",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
    }
    transport._maybe_block(old_route)
    transport._restart()
    page.context.cookies.return_value = [
        {"name": "cf_clearance", "value": "new-token"},
    ]
    page.evaluate.return_value = "Mozilla/5.0 Firefox/152.0"

    assert transport._browser_navigation_headers == {}
    assert transport.get_clearance() is None


class _FakeChild:
    def __init__(self, name: str) -> None:
        self._name = name
        self.killed = False

    def name(self) -> str:
        return self._name

    def kill(self) -> None:
        self.killed = True


def test_hung_browser_start_kills_only_its_own_browser_processes(monkeypatch):
    """Camoufox's launch never times out on a dead exit IP, so the watchdog
    kills the browser it spawned — that is what makes the launch raise and the
    caller rotate onto a fresh proxy."""
    import psutil

    transport = CamoufoxFbrefTransport(proxy={"server": "http://p:1"})
    browser = _FakeChild("camoufox-bin")
    driver = _FakeChild("node")  # playwright's driver — must survive
    unrelated = _FakeChild("postgres")

    process = MagicMock()
    process.children.return_value = [browser, driver, unrelated]
    monkeypatch.setattr(psutil, "Process", lambda: process)

    transport._kill_browser_processes()

    assert browser.killed
    # Killing playwright's driver severs the connection every later session in
    # this process needs: production then failed every navigation instantly.
    assert not driver.killed
    assert not unrelated.killed
    assert transport.traffic_stats()["browser_watchdog_kills"] == 1


def test_browser_start_watchdog_is_disarmed_once_the_browser_is_up(monkeypatch):
    """A healthy start must not leave a timer that kills a working session."""
    timers = []

    class _Timer:
        def __init__(self, interval, function, args=()):
            self.interval = interval
            self.function = function
            self.args = args
            self.cancelled = False
            self.daemon = False
            timers.append(self)

        def start(self):
            pass

        def cancel(self):
            self.cancelled = True

    monkeypatch.setattr(
        "scrapers.fbref.camoufox_fetch.threading.Timer", _Timer
    )
    page = MagicMock()
    page.evaluate.return_value = True
    context = MagicMock()
    context.new_page.return_value = page
    browser = MagicMock()
    browser.new_context.return_value = context
    camoufox = MagicMock()
    camoufox.return_value.__enter__.return_value = browser
    _install_camoufox_mocks(monkeypatch, camoufox)

    transport = CamoufoxFbrefTransport(
        proxy={"server": "http://p:1"}, geoip=False
    )
    transport._start()

    assert len(timers) == 1
    assert timers[0].cancelled is True
    assert timers[0].interval == transport.BROWSER_START_TIMEOUT_S


def test_proxy_authorization_header_requires_explicit_lease_client_flag():
    transport = CamoufoxFbrefTransport(
        proxy={
            "server": "http://untrusted-proxy:8900",
            "username": "lease",
            "password": "secret-token",
        },
        geoip=False,
    )
    transport._proxy = transport._proxy_provider()

    assert transport._proxy_authorization_header() is None


@pytest.mark.parametrize(
    "proxy",
    [
        {},
        {"username": "lease", "password": "secret-token"},
        {
            "server": "",
            "username": "lease",
            "password": "secret-token",
        },
        {
            "server": "http://fbref_proxy_filter:8900",
            "username": "other",
            "password": "token",
        },
        {
            "server": "http://fbref_proxy_filter:8900",
            "username": "lease",
            "password": "",
        },
        {
            "server": "http://fbref_proxy_filter:8900",
            "username": "lease",
            "password": "токен",
        },
        {
            "server": "http://fbref_proxy_filter:8900",
            "username": "lease",
            "password": "x" * 513,
        },
    ],
)
def test_preemptive_proxy_auth_rejects_bad_lease_before_network(proxy):
    resolver = MagicMock(return_value="203.0.113.9")
    transport = CamoufoxFbrefTransport(
        proxy=proxy,
        max_network_requests=10,
        geoip_resolver=resolver,
        geoip_database_check=lambda: None,
        preemptive_proxy_auth=True,
    )

    with pytest.raises(
        RuntimeError, match="paid proxy credential is invalid"
    ) as raised:
        transport._start()

    resolver.assert_not_called()
    secret = str(proxy.get("password") or "")
    if secret:
        assert secret not in str(raised.value)
    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 0
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "invalid_proxy_credential"


def test_geoip_resolver_is_one_bounded_attempt_without_redirects(monkeypatch):
    import requests

    response = MagicMock(
        status_code=200,
        headers={"content-length": "12"},
    )
    response.raw.read.return_value = b"203.0.113.9\n"
    session = MagicMock()
    session.get.return_value = response
    monkeypatch.setattr(requests, "Session", lambda: session)

    result = resolve_geoip_without_redirects({
        "server": "http://fbref_proxy_filter:8900",
        "username": "lease",
        "password": "lease-token",
    })

    assert result == "203.0.113.9"
    session.get.assert_called_once()
    call = session.get.call_args
    assert call.args == (GEOIP_LOOKUP_URL,)
    assert call.kwargs["allow_redirects"] is False
    assert call.kwargs["stream"] is True
    assert call.kwargs["headers"]["Connection"] == "close"
    assert isinstance(call.kwargs["timeout"], tuple)
    response.raw.read.assert_called_once_with(65, decode_content=True)
    response.close.assert_called_once_with()
    assert session.trust_env is False
    assert session.mount.call_count == 2
    session.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("status", "headers", "payload", "message"),
    [
        (302, {"location": "https://ipinfo.io/ip"}, b"", "non-200"),
        (200, {"content-length": "65"}, b"1.1.1.1", "oversized"),
        (200, {}, b"x" * 65, "oversized"),
        (200, {}, b"not-an-ip", "invalid"),
    ],
)
def test_geoip_resolver_fails_closed_after_its_only_attempt(
    monkeypatch, status, headers, payload, message
):
    import requests

    response = MagicMock(status_code=status, headers=headers)
    response.raw.read.return_value = payload
    session = MagicMock()
    session.get.return_value = response
    monkeypatch.setattr(requests, "Session", lambda: session)

    with pytest.raises(RuntimeError, match=message):
        resolve_geoip_without_redirects({
            "server": "http://fbref_proxy_filter:8900",
            "username": "lease",
            "password": "lease-token",
        })

    session.get.assert_called_once()
    response.close.assert_called_once_with()
    session.close.assert_called_once_with()


def test_geoip_admission_refuses_before_resolver_or_camoufox():
    resolver = MagicMock(return_value="203.0.113.9")
    transport = CamoufoxFbrefTransport(
        proxy={"server": "http://p:1"},
        max_network_requests=GEOIP_REQUEST_RESERVATION,
        geoip_resolver=resolver,
        geoip_database_check=lambda: None,
    )
    transport._network_requests_started = GEOIP_REQUEST_RESERVATION

    with pytest.raises(RuntimeError, match="geo-IP request admission"):
        transport._start()

    resolver.assert_not_called()
    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == GEOIP_REQUEST_RESERVATION
    assert stats["request_budget_exhausted"] is True
    assert stats["budget_blocked_count"] == 1


def test_failed_geoip_attempt_still_spends_its_admitted_slot():
    resolver = MagicMock(side_effect=RuntimeError("lookup failed"))
    transport = CamoufoxFbrefTransport(
        proxy={"server": "http://p:1"},
        max_network_requests=2,
        geoip_resolver=resolver,
        geoip_database_check=lambda: None,
    )

    with pytest.raises(RuntimeError, match="lookup failed"):
        transport._start()

    resolver.assert_called_once()
    assert transport.traffic_stats()["real_requests_count"] == 1
    assert transport.traffic_stats()["budget_unobserved_bytes"] == (
        GEOIP_BYTE_RESERVATION_BYTES
    )


def test_public_fetch_never_retries_a_failed_geoip_or_acquires_new_lease():
    resolver = MagicMock(side_effect=RuntimeError("lookup failed"))
    provider = MagicMock(return_value={"server": "http://p:1"})
    transport = CamoufoxFbrefTransport(
        proxy_provider=provider,
        max_network_requests=20,
        max_network_bytes=20 * GEOIP_BYTE_RESERVATION_BYTES,
        geoip_resolver=resolver,
        geoip_database_check=lambda: None,
    )

    assert transport.fetch("https://fbref.com/en/") is None
    assert transport.fetch("https://fbref.com/en/") is None

    resolver.assert_called_once_with({"server": "http://p:1"})
    provider.assert_called_once_with()
    stats = transport.traffic_stats()
    assert stats["browser_start_attempts"] == 1
    assert stats["real_requests_count"] == 1
    assert stats["geoip_lookup_failed"] is True


def test_geoip_byte_admission_refuses_before_socket():
    resolver = MagicMock(return_value="203.0.113.9")
    transport = CamoufoxFbrefTransport(
        proxy={"server": "http://p:1"},
        max_network_requests=20,
        max_network_bytes=GEOIP_BYTE_RESERVATION_BYTES - 1,
        geoip_resolver=resolver,
        geoip_database_check=lambda: None,
    )

    with pytest.raises(RuntimeError, match="byte admission"):
        transport._start()

    resolver.assert_not_called()
    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 0
    assert stats["budget_unobserved_bytes"] == 0
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"] == "geoip_admission_exceeds_byte_cap"


def test_bounded_geoip_has_no_direct_fallback():
    resolver = MagicMock(return_value="203.0.113.9")
    transport = CamoufoxFbrefTransport(
        max_network_requests=20,
        geoip_resolver=resolver,
        geoip_database_check=lambda: None,
    )

    with pytest.raises(RuntimeError, match="paid lease proxy"):
        transport._start()

    resolver.assert_not_called()
    assert transport.traffic_stats()["real_requests_count"] == 0


def test_each_start_preadmits_geoip_and_disables_automatic_redirects(
    monkeypatch,
):
    page = MagicMock()
    page.evaluate.return_value = True
    context = MagicMock()
    context.new_page.return_value = page
    browser = MagicMock()
    browser.new_context.return_value = context
    camoufox = MagicMock()
    camoufox.return_value.__enter__.return_value = browser
    _install_camoufox_mocks(monkeypatch, camoufox)
    resolver = MagicMock(return_value="203.0.113.9")
    transport = CamoufoxFbrefTransport(
        proxy={
            "server": "http://fbref_proxy_filter:8900",
            "username": "lease",
            "password": "lease-token",
        },
        max_network_requests=10,
        geoip_resolver=resolver,
        geoip_database_check=lambda: None,
        preemptive_proxy_auth=True,
    )

    transport._start()
    transport._stop()
    transport._start()

    assert resolver.call_count == 2
    assert transport.traffic_stats()["real_requests_count"] == (
        2 * GEOIP_REQUEST_RESERVATION
    )
    assert transport.traffic_stats()["budget_unobserved_bytes"] == (
        2 * GEOIP_BYTE_RESERVATION_BYTES
    )
    assert camoufox.call_count == 2
    for call in camoufox.call_args_list:
        assert call.kwargs["geoip"] == "203.0.113.9"
        assert str(call.kwargs["executable_path"]) == (
            "/opt/fbref-camoufox/camoufox-bin"
        )
        assert call.kwargs["ff_version"] == 152
        assert call.kwargs["i_know_what_im_doing"] is True
        assert [addon.name for addon in call.kwargs["exclude_addons"]] == [
            "UBO"
        ]
        assert call.kwargs["firefox_user_prefs"] == (
            FIREFOX_METERED_NETWORK_PREFS
        )
    expected_proxy_authorization = "Basic " + base64.b64encode(
        b"lease:lease-token"
    ).decode("ascii")
    assert all(
        call.kwargs == {
            "service_workers": "block",
            "extra_http_headers": {
                "Proxy-Authorization": expected_proxy_authorization,
            },
        }
        for call in browser.new_context.call_args_list
    )
    assert "lease-token" not in repr(browser.new_context.call_args_list)
    context.add_init_script.assert_not_called()
    assert context.new_page.call_count == 2
    assert context.route.call_count == 2
    assert context.route_web_socket.call_count == 2
    assert context.on.call_count == 6
    page.on.assert_not_called()
    page.route_web_socket.assert_not_called()
    websocket = MagicMock()
    context.route_web_socket.call_args.args[1](websocket)
    websocket.close.assert_called_once_with(
        code=1008, reason="FBref websocket traffic is disabled"
    )


def test_managed_shell_is_counted_before_turnstile_iframe(monkeypatch):
    transport = CamoufoxFbrefTransport(geoip=False)
    transport.CF_SOLVE_TIMEOUT_S = 1
    transport.POLL_INTERVAL_S = 0
    transport._page = MagicMock()
    transport._page.evaluate.side_effect = [True, False]
    transport._page.frames = []
    transport._click_turnstile = MagicMock(return_value=False)
    clock = MagicMock(side_effect=[0, 0, 2])
    monkeypatch.setattr("scrapers.fbref.camoufox_fetch.time.time", clock)
    monkeypatch.setattr(
        "scrapers.fbref.camoufox_fetch.time.sleep", MagicMock()
    )

    assert transport._solve_current_page() is None
    assert transport._last_solve_failure == "cloudflare"
    assert transport.cf_challenge_attempts == 1
    assert transport.cf_challenges_failed == 1


def test_popup_and_subresource_share_context_admission(monkeypatch):
    page = MagicMock()
    page.evaluate.return_value = True
    context = MagicMock()
    context.new_page.return_value = page
    browser = MagicMock()
    browser.new_context.return_value = context
    camoufox = MagicMock()
    camoufox.return_value.__enter__.return_value = browser
    _install_camoufox_mocks(monkeypatch, camoufox)
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        proxy={
            "server": "http://fbref_proxy_filter:8900",
            "username": "lease",
            "password": "lease-token",
        },
        max_network_requests=3,
        max_network_bytes=(
            GEOIP_BYTE_RESERVATION_BYTES + (2 * admission) + 1000
        ),
        geoip_resolver=lambda _proxy: "203.0.113.9",
        geoip_database_check=lambda: None,
    )
    transport._start()
    route_handler = context.route.call_args.args[1]
    handlers = {
        call.args[0]: call.args[1]
        for call in context.on.call_args_list
    }

    popup = _Route(url="https://fbref.com/popup")
    popup.request.sizes.return_value = {
        "responseBodySize": 10,
        "responseHeadersSize": 20,
        "requestBodySize": 0,
        "requestHeadersSize": 30,
    }
    route_handler(popup)
    handlers["response"](_Response(popup.request, {"content-length": "10"}))
    handlers["requestfinished"](popup.request)

    popup_script = _Route(
        resource_type="script",
        url="https://fbref.com/popup.js",
    )
    popup_script.request.sizes.return_value = {
        "responseBodySize": 10,
        "responseHeadersSize": 20,
        "requestBodySize": 0,
        "requestHeadersSize": 30,
    }
    route_handler(popup_script)
    handlers["response"](
        _Response(popup_script.request, {"content-length": "10"})
    )
    handlers["requestfinished"](popup_script.request)

    beyond_profile = _Route(url="https://fbref.com/second-popup")
    route_handler(beyond_profile)
    websocket = MagicMock()
    context.route_web_socket.call_args.args[1](websocket)

    assert popup.continued == 1
    assert popup_script.continued == 1
    assert beyond_profile.continued == 0
    assert beyond_profile.aborted == 1
    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 4
    assert stats["completed_requests_count"] == 2
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["network_policy_failed"] is True
    assert stats["network_policy_failure"] == "unexpected_websocket_handshake"
    assert stats["budget_unobserved_bytes"] == (
        GEOIP_BYTE_RESERVATION_BYTES + admission
    )
    websocket.close.assert_called_once_with(
        code=1008, reason="FBref websocket traffic is disabled"
    )


def test_first_request_beyond_profile_is_aborted_before_network():
    transport = CamoufoxFbrefTransport(
        max_network_requests=1,
        geoip_resolver=lambda _proxy: "203.0.113.9",
        geoip_database_check=lambda: None,
    )
    # One startup/geo-IP request was admitted before its socket was opened.
    transport._network_requests_started = GEOIP_REQUEST_RESERVATION
    route = _Route()

    transport._maybe_block(route)

    assert route.continued == 0
    assert route.aborted == 1
    assert transport.traffic_stats()["real_requests_count"] == 1
    assert transport.traffic_stats()["request_budget_exhausted"] is True


def test_blocked_native_redirect_does_not_rotate_proxy():
    transport = CamoufoxFbrefTransport(geoip=False)
    transport._page = MagicMock()
    transport._page.goto.side_effect = RuntimeError("NS_ERROR_REDIRECT_LOOP")
    transport._solve_current_page = MagicMock()  # type: ignore[method-assign]
    transport._restart = MagicMock()  # type: ignore[method-assign]

    assert transport.fetch("https://fbref.com/en/") is None

    transport._page.goto.assert_called_once()
    transport._solve_current_page.assert_not_called()
    transport._restart.assert_not_called()
    assert transport._last_solve_failure == "redirect_blocked"
    assert transport.traffic_stats()["redirect_blocked"] is True


def test_redirect_callback_with_generic_goto_error_does_not_rotate_proxy():
    transport = CamoufoxFbrefTransport(geoip=False)
    transport._page = MagicMock()

    def failed_goto(*_args, **_kwargs):
        transport._redirect_blocked = True
        raise RuntimeError("Target closed")

    transport._page.goto.side_effect = failed_goto
    transport._solve_current_page = MagicMock()  # type: ignore[method-assign]
    transport._restart = MagicMock()  # type: ignore[method-assign]

    assert transport.fetch("https://fbref.com/en/") is None

    transport._page.goto.assert_called_once()
    transport._solve_current_page.assert_not_called()
    transport._restart.assert_not_called()
    assert transport._last_solve_failure == "redirect_blocked"


@pytest.mark.unit
def test_one_safe_server_redirect_is_reissued_through_route_admission():
    admission = (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
        + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
    )
    transport = CamoufoxFbrefTransport(
        geoip=False,
        max_network_requests=2,
        max_network_bytes=2 * admission,
    )
    transport._page = MagicMock()
    seen_routes = []

    def goto(current_url, **_kwargs):
        route = _Route(url=current_url)
        route.request.is_navigation_request.return_value = True
        route.request.sizes.return_value = {
            "responseBodySize": 0,
            "responseHeadersSize": 300,
            "requestBodySize": 0,
            "requestHeadersSize": 200,
        }
        seen_routes.append(route)
        transport._maybe_block(route)
        if route.aborted:
            raise RuntimeError("NS_BINDING_ABORTED")
        if current_url == "https://fbref.com/en/":
            transport._on_response(
                _Response(
                    route.request,
                    {"content-length": "0", "location": "/en/redirected"},
                    status=302,
                )
            )
            route.request.failure = "NS_ERROR_REDIRECT_LOOP"
            transport._on_request_failed(route.request)
            raise RuntimeError("Target closed after redirect callback")
        transport._on_response(
            _Response(route.request, {"content-length": "0"}, status=200)
        )
        transport._on_request_finished(route.request)

    transport._page.goto.side_effect = goto

    transport._navigate_with_one_manual_redirect("https://fbref.com/en/")

    assert [call.args[0] for call in transport._page.goto.call_args_list] == [
        "https://fbref.com/en/",
        "https://fbref.com/en/redirected",
    ]
    assert all(route.continued == 1 for route in seen_routes)
    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 2
    assert stats["completed_requests_count"] == 1
    assert stats["network_policy_failed"] is False
    assert stats["request_budget_exhausted"] is False
    assert stats["redirect_blocked"] is False
    assert stats["browser_navigation_attempts"] == 2


@pytest.mark.unit
@pytest.mark.parametrize(
    "location",
    [
        "http://fbref.com/en/other",
        "https://evil.example/en/other",
        "https://user:secret@fbref.com/en/other",
        "https://fbref.com:444/en/other",
    ],
)
def test_unsafe_server_redirect_is_never_reissued(location):
    transport = CamoufoxFbrefTransport(max_network_requests=2)
    route = _Route()
    route.request.is_navigation_request.return_value = True
    transport._navigation_source_url = "https://fbref.com/en/"
    transport._maybe_block(route)

    transport._on_response(
        _Response(
            route.request,
            {"content-length": "0", "location": location},
            status=302,
        )
    )

    assert transport._pending_manual_redirect_url is None


@pytest.mark.unit
def test_unknown_redirect_status_is_never_reissued():
    transport = CamoufoxFbrefTransport(max_network_requests=2)
    route = _Route()
    route.request.is_navigation_request.return_value = True
    transport._navigation_source_url = "https://fbref.com/en/"
    transport._maybe_block(route)

    transport._on_response(
        _StatusFailsResponse(
            route.request,
            {"content-length": "0", "location": "/next"},
        )
    )

    assert transport._pending_manual_redirect_url is None


@pytest.mark.unit
def test_late_redirect_response_after_failure_is_not_a_routing_bypass():
    transport = CamoufoxFbrefTransport(max_network_requests=2)
    route = _Route()
    route.request.is_navigation_request.return_value = True
    route.request.failure = "NS_ERROR_REDIRECT_LOOP"
    route.request.sizes.return_value = {
        "responseBodySize": 0,
        "responseHeadersSize": 300,
        "requestBodySize": 0,
        "requestHeadersSize": 200,
    }
    route.request._impl_obj._guid = "request@late-redirect"
    transport._navigation_source_url = "https://fbref.com/en/"
    transport._maybe_block(route)
    transport._on_request_failed(route.request)

    rewrapped = MagicMock(resource_type="document", url=route.request.url)
    rewrapped._impl_obj._guid = "request@late-redirect"
    rewrapped.is_navigation_request.return_value = True
    transport._on_response(
        _Response(
            rewrapped,
            {"content-length": "0", "location": "/next"},
            status=302,
        )
    )

    stats = transport.traffic_stats()
    assert stats["network_policy_failed"] is False
    assert stats["real_requests_count"] == 1
    assert transport._pending_manual_redirect_url == "https://fbref.com/next"
    assert transport._settled_failed_request_urls == {}
    assert transport._settled_failed_ids_by_url == {}
    assert transport._settled_failed_request_guids == {}


@pytest.mark.unit
def test_manual_redirect_second_request_still_obeys_request_cap():
    transport = CamoufoxFbrefTransport(
        geoip=False,
        max_network_requests=1,
    )
    transport._page = MagicMock()
    seen_routes = []

    def goto(current_url, **_kwargs):
        route = _Route(url=current_url)
        route.request.is_navigation_request.return_value = True
        route.request.sizes.return_value = {
            "responseBodySize": 0,
            "responseHeadersSize": 300,
            "requestBodySize": 0,
            "requestHeadersSize": 200,
        }
        seen_routes.append(route)
        transport._maybe_block(route)
        if route.aborted:
            raise RuntimeError("NS_BINDING_ABORTED")
        transport._on_response(
            _Response(
                route.request,
                {"content-length": "0", "location": "/next"},
                status=302,
            )
        )
        route.request.failure = "NS_ERROR_REDIRECT_LOOP"
        transport._on_request_failed(route.request)
        raise RuntimeError("NS_ERROR_REDIRECT_LOOP")

    transport._page.goto.side_effect = goto

    with pytest.raises(RuntimeError, match="NS_BINDING_ABORTED"):
        transport._navigate_with_one_manual_redirect("https://fbref.com/en/")

    assert len(seen_routes) == 2
    assert seen_routes[0].continued == 1
    assert seen_routes[1].continued == 0
    assert seen_routes[1].aborted == 1
    stats = transport.traffic_stats()
    assert stats["real_requests_count"] == 1
    assert stats["request_budget_exhausted"] is True


def test_redirect_seen_while_solve_raises_never_rotates_proxy():
    transport = CamoufoxFbrefTransport(geoip=False)
    transport._page = MagicMock()

    def failed_solve():
        transport._redirect_blocked = True
        raise RuntimeError("Target closed after NS_ERROR_REDIRECT_LOOP")

    transport._solve_current_page = MagicMock(  # type: ignore[method-assign]
        side_effect=failed_solve
    )
    transport._restart = MagicMock()  # type: ignore[method-assign]

    assert transport.fetch("https://fbref.com/en/") is None

    transport._page.goto.assert_called_once()
    transport._solve_current_page.assert_called_once_with()
    transport._restart.assert_not_called()
    assert transport._last_solve_failure == "redirect_blocked"


def test_request_cap_inside_goto_rejects_partial_page_and_stops_browser():
    transport = CamoufoxFbrefTransport(
        geoip=False,
        max_network_requests=1,
    )
    transport._page = MagicMock()
    transport._cm = object()
    transport._network_requests_started = 1
    blocked = _Route(url="https://fbref.com/popup")
    transport._page.goto.side_effect = lambda *_args, **_kwargs: (
        transport._maybe_block(blocked)
    )
    transport._solve_current_page = MagicMock(  # type: ignore[method-assign]
        return_value="<html><table></table></html>"
    )
    transport._restart = MagicMock()  # type: ignore[method-assign]
    transport._stop = MagicMock()  # type: ignore[method-assign]

    assert transport.fetch("https://fbref.com/en/") is None

    assert blocked.continued == 0
    assert blocked.aborted == 1
    transport._solve_current_page.assert_not_called()
    transport._restart.assert_not_called()
    transport._stop.assert_called_once_with()


def test_byte_cap_during_solve_rejects_valid_partial_html_and_stops_browser():
    transport = CamoufoxFbrefTransport(
        geoip=False,
        max_network_bytes=2 * (
            BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
            + BROWSER_UNDECLARED_BODY_RESERVATION_BYTES
        ),
    )
    transport._page = MagicMock()
    transport._cm = object()

    def solve_then_abort():
        transport._abort_session_for_byte_budget("test_during_solve")
        return "<html><table></table></html>"

    transport._solve_current_page = MagicMock(  # type: ignore[method-assign]
        side_effect=solve_then_abort
    )
    transport._restart = MagicMock()  # type: ignore[method-assign]
    transport._stop = MagicMock()  # type: ignore[method-assign]

    assert transport.fetch("https://fbref.com/en/") is None

    transport._page.goto.assert_called_once()
    transport._solve_current_page.assert_called_once_with()
    transport._restart.assert_not_called()
    transport._stop.assert_called_once_with()


@pytest.mark.parametrize("valid_page", [False, True])
def test_subresource_redirect_failure_never_rotates_proxy(valid_page):
    transport = CamoufoxFbrefTransport(geoip=False)
    transport._page = MagicMock()
    failed = MagicMock()
    failed.failure = "NS_ERROR_REDIRECT_LOOP"
    failed.sizes.return_value = {
        "responseBodySize": 0,
        "responseHeadersSize": 0,
        "requestBodySize": 0,
        "requestHeadersSize": 0,
    }
    transport._page.goto.side_effect = lambda *_args, **_kwargs: (
        transport._on_request_failed(failed)
    )
    html = "<html><table></table></html>" if valid_page else None
    transport._solve_current_page = MagicMock(  # type: ignore[method-assign]
        return_value=html
    )
    transport._restart = MagicMock()  # type: ignore[method-assign]

    assert transport.fetch("https://fbref.com/en/") is None

    transport._page.goto.assert_called_once()
    transport._solve_current_page.assert_not_called()
    transport._restart.assert_not_called()
    assert transport._last_solve_failure == "redirect_blocked"


def test_byte_budget_abort_never_closes_the_browser_from_a_callback():
    """Playwright's sync API deadlocks when the browser is torn down inside an
    event callback: the callback waits for the close, the close waits for the
    callback. The cap must only raise its flag there — closing is the fetch
    loop's job, back on the main thread."""
    transport = CamoufoxFbrefTransport(max_network_bytes=100)
    stopped = []
    transport._stop = lambda: stopped.append(True)  # type: ignore[method-assign]
    transport._cm = object()

    transport._abort_session_for_byte_budget("declared_content_length_exceeds_cap:999")

    assert stopped == []
    assert transport._byte_budget_exhausted is True
    assert transport.traffic_stats()["byte_budget_failure"] == (
        "declared_content_length_exceeds_cap:999"
    )


def test_fetch_closes_the_session_after_the_budget_aborted_it():
    transport = CamoufoxFbrefTransport(max_network_bytes=100)
    stopped = []
    transport._stop = lambda: stopped.append(True)  # type: ignore[method-assign]
    transport._cm = object()
    transport._page = MagicMock()
    transport._page.goto.side_effect = lambda *a, **k: (
        transport._abort_session_for_byte_budget("undeclared_body_exceeds_cap:1")
    )

    assert transport.fetch("https://fbref.com/en/") is None
    assert stopped == [True]


def test_a_wedged_navigation_is_killed_by_the_attempt_deadline(monkeypatch):
    """Playwright's nav timeout lives in its driver: when a stalled exit IP
    wedges the driver connection, goto never returns and its own timeout never
    fires. The attempt deadline kills the browser, which raises out of goto and
    puts the fetch back on the rotation path instead of hanging the wave."""
    timers = []

    class _Timer:
        def __init__(self, interval, function, args=()):
            self.interval = interval
            self.function = function
            self.args = args
            self.daemon = False
            self.cancelled = False
            timers.append(self)

        def start(self):
            # A wedged call is what the timer exists for: fire immediately.
            self.function(*self.args)

        def cancel(self):
            self.cancelled = True

    monkeypatch.setattr(
        "scrapers.fbref.camoufox_fetch.threading.Timer", _Timer
    )
    transport = CamoufoxFbrefTransport(proxy={"server": "http://p:1"})
    killed = []
    monkeypatch.setattr(
        transport, "_kill_browser_processes",
        lambda phase="start", timeout_s=None: killed.append(phase),
    )
    transport._page = MagicMock()
    transport._page.goto.side_effect = RuntimeError("Target closed")
    transport._restart = MagicMock()  # type: ignore[method-assign]

    assert transport.fetch("https://fbref.com/en/") is None
    assert killed and killed[0] == "navigation"
    assert timers[0].interval == transport.BROWSER_ATTEMPT_TIMEOUT_S
    assert transport._restart.call_count == transport.MAX_PROXY_ROTATIONS


def test_a_wedged_solve_is_killed_by_the_attempt_deadline(monkeypatch):
    """The solve loop checks its own deadline between polls — but a wedged
    evaluate() blocks before the check. The killed browser must surface as a
    failed attempt, not as an exception out of fetch."""
    transport = CamoufoxFbrefTransport(proxy={"server": "http://p:1"})
    transport.BROWSER_ATTEMPT_TIMEOUT_S = 0.01
    watchdog_fired = threading.Event()
    killed_phases = []

    def kill_browser(phase="start", timeout_s=None):
        killed_phases.append((phase, timeout_s))
        watchdog_fired.set()

    def wedged_solve():
        assert watchdog_fired.wait(1), "real watchdog Timer did not fire"
        watchdog_fired.clear()
        raise RuntimeError("Target closed")

    monkeypatch.setattr(transport, "_kill_browser_processes", kill_browser)
    transport._page = MagicMock()
    transport._solve_current_page = MagicMock(  # type: ignore[method-assign]
        side_effect=wedged_solve
    )
    transport._restart = MagicMock()  # type: ignore[method-assign]

    assert transport.fetch("https://fbref.com/en/") is None
    assert killed_phases == [
        ("challenge solve", transport.BROWSER_ATTEMPT_TIMEOUT_S)
    ] * (transport.MAX_PROXY_ROTATIONS + 1)
    assert transport._restart.call_count == transport.MAX_PROXY_ROTATIONS


def test_traffic_is_billed_once_per_bootstrap_not_once_per_target():
    """The counters are cumulative for the session. Charging the running totals
    made every later target of a wave pay again for the same browser traffic:
    one wave billed 140 requests for ~20 real ones and exhausted the run's
    budget on traffic that never crossed the proxy."""
    transport = CamoufoxFbrefTransport(proxy={"server": "http://p:1"})
    transport._bytes_total = 1_000_000
    transport._requests_count = 19
    transport._bytes_by_type["document"] = 700_000

    first = transport.traffic_delta()
    assert first["real_bytes_downloaded"] == 1_000_000
    assert first["real_requests_count"] == 19
    assert first["real_bytes_by_resource_type"]["document"] == 700_000

    # Nothing new crossed the wire: the next target must be charged nothing.
    second = transport.traffic_delta()
    assert second["real_bytes_downloaded"] == 0
    assert second["real_requests_count"] == 0
    assert second["real_bytes_by_resource_type"]["document"] == 0

    # A second solve is charged exactly its own cost.
    transport._bytes_total += 1_100_000
    transport._requests_count += 21
    third = transport.traffic_delta()
    assert third["real_bytes_downloaded"] == 1_100_000
    assert third["real_requests_count"] == 21


def test_traffic_delta_never_bills_a_falling_reservation_gauge_negative():
    transport = CamoufoxFbrefTransport(proxy={"server": "http://p:1"})
    transport._inflight_reserved_bytes = 1_000

    first = transport.traffic_delta()
    assert first["inflight_reserved_bytes"] == 1_000

    transport._inflight_reserved_bytes = 0
    second = transport.traffic_delta()
    assert second["inflight_reserved_bytes"] == 0
    assert second["budget_unobserved_bytes"] == 0
    assert second["budget_bytes_consumed"] == 0

    # The baseline follows the gauge down, so a later reservation is charged
    # normally instead of being hidden behind the old high-water mark.
    transport._inflight_reserved_bytes = 250
    third = transport.traffic_delta()
    assert third["inflight_reserved_bytes"] == 250
    assert third["budget_unobserved_bytes"] == 250
