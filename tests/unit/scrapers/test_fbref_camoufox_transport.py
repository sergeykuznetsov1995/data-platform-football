"""Network-free tests for the single FBref Camoufox clearance transport."""

from unittest.mock import MagicMock

import pytest

from scrapers.fbref.camoufox_fetch import (
    BROWSER_REQUEST_FIXED_OVERHEAD_BYTES,
    CamoufoxFbrefTransport,
    is_cloudflare_blocked,
    should_block_request,
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
    def __init__(self, request, headers):
        self.request = request
        self.headers = headers


class _ContinueFailsRoute(_Route):
    def continue_(self):
        self.continued += 1
        raise RuntimeError("continue failed after request handoff")


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
    transport = CamoufoxFbrefTransport(
        max_network_bytes=(2 * overhead) - 1
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
    assert stats["unobserved_reserved_bytes"] == overhead
    assert stats["inflight_reserved_bytes"] == 0
    assert next_route.aborted == 1
    assert next_route.continued == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    ("headers", "reason"),
    [
        ({}, "missing_or_invalid_content_length"),
        ({"content-length": "not-a-number"}, "missing_or_invalid_content_length"),
        (
            {"content-length": "10", "transfer-encoding": "chunked"},
            "chunked_content_length",
        ),
    ],
)
def test_browser_aborts_whole_session_on_unbounded_response(headers, reason):
    cap = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES + 100
    transport = CamoufoxFbrefTransport(max_network_bytes=cap)
    route = _Route()
    transport._maybe_block(route)

    transport._on_response(_Response(route.request, headers))

    stats = transport.traffic_stats()
    assert route.continued == 1
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"] == reason
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["budget_unobserved_bytes"] == (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    )
    assert stats["budget_blocked_count"] == 1


@pytest.mark.unit
def test_browser_parallel_declared_lengths_cannot_overbook_cap():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=(2 * overhead) + 100
    )
    first = _Route(url="https://fbref.com/one")
    second = _Route(url="https://fbref.com/two")
    transport._maybe_block(first)
    transport._maybe_block(second)

    transport._on_response(
        _Response(first.request, {"content-length": "60"})
    )
    assert transport.traffic_stats()["inflight_reserved_bytes"] == (
        2 * overhead + 60
    )

    transport._on_response(
        _Response(second.request, {"content-length": "41"})
    )

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"] == (
        "declared_content_length_exceeds_cap:41"
    )
    assert stats["inflight_reserved_bytes"] == 0


@pytest.mark.unit
def test_browser_releases_declared_reservation_after_finish_and_failure():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=(2 * overhead) + 1000
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
def test_redirect_hop_response_is_adopted_not_fatal():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=(3 * overhead) + 1000,
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

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is False
    assert stats["inflight_reserved_bytes"] == (2 * overhead) + 50 + 60
    # The hop consumes a request slot exactly like a routed request.
    assert stats["real_requests_count"] == 2
    assert stats["completed_requests_count"] == 0


@pytest.mark.unit
def test_untracked_response_without_redirect_chain_stays_fatal():
    transport = CamoufoxFbrefTransport(
        max_network_bytes=BROWSER_REQUEST_FIXED_OVERHEAD_BYTES + 100
    )
    orphan = MagicMock(resource_type="document", url="https://fbref.com/en/")
    orphan.redirected_from = None

    transport._on_response(_Response(orphan, {"content-length": "10"}))

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"] == "untracked_response"


@pytest.mark.unit
def test_redirect_hop_over_request_cap_aborts_session():
    transport = CamoufoxFbrefTransport(
        max_network_bytes=10 * BROWSER_REQUEST_FIXED_OVERHEAD_BYTES,
        max_network_requests=1,
    )
    route = _Route()
    transport._maybe_block(route)

    hop_request = MagicMock(
        resource_type="document", url="https://fbref.com/en/?redirected=1"
    )
    hop_request.redirected_from = route.request
    transport._on_response(
        _Response(hop_request, {"content-length": "60"})
    )

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"] == "redirect_hop_over_request_cap"


@pytest.mark.unit
def test_redirect_hop_over_byte_cap_aborts_session():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=(2 * overhead) - 1,
        max_network_requests=5,
    )
    route = _Route()
    transport._maybe_block(route)

    hop_request = MagicMock(
        resource_type="document", url="https://fbref.com/en/?redirected=1"
    )
    hop_request.redirected_from = route.request
    transport._on_response(
        _Response(hop_request, {"content-length": "60"})
    )

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"] == "redirect_hop_over_byte_cap"


@pytest.mark.unit
def test_redirect_hop_reservation_settles_on_finish():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=(3 * overhead) + 1000,
        max_network_requests=5,
    )
    route = _Route()
    transport._maybe_block(route)
    transport._on_response(
        _Response(route.request, {"content-length": "50"})
    )
    route.request.sizes.return_value = {
        "responseBodySize": 50,
        "responseHeadersSize": 10,
        "requestBodySize": 0,
        "requestHeadersSize": 10,
    }
    transport._on_request_finished(route.request)

    hop_request = MagicMock(
        resource_type="document", url="https://fbref.com/en/?redirected=1"
    )
    hop_request.redirected_from = route.request
    hop_request.sizes.return_value = {
        "responseBodySize": 60,
        "responseHeadersSize": 10,
        "requestBodySize": 0,
        "requestHeadersSize": 10,
    }
    transport._on_response(
        _Response(hop_request, {"content-length": "60"})
    )
    transport._on_request_finished(hop_request)

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is False
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["real_bytes_downloaded"] == 70 + 80
    assert stats["real_requests_count"] == 2
    assert stats["completed_requests_count"] == 2


@pytest.mark.unit
def test_failed_request_unknown_bytes_keep_full_reservation_charged():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=(2 * overhead) + 99
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
    transport = CamoufoxFbrefTransport(max_network_bytes=2 * overhead)
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
    transport = CamoufoxFbrefTransport(max_network_bytes=overhead + 100)
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
def test_browser_rejects_single_declared_body_larger_than_remaining_cap():
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    transport = CamoufoxFbrefTransport(max_network_bytes=overhead + 9)
    route = _Route()
    transport._maybe_block(route)

    transport._on_response(
        _Response(route.request, {"content-length": "10"})
    )

    stats = transport.traffic_stats()
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"] == (
        "declared_content_length_exceeds_cap:10"
    )


@pytest.mark.unit
def test_finished_request_accounts_rx_and_tx_bytes():
    transport = CamoufoxFbrefTransport()
    request = MagicMock(resource_type="document")
    request.sizes.return_value = {
        "responseBodySize": 1000,
        "responseHeadersSize": 100,
        "requestBodySize": 10,
        "requestHeadersSize": 40,
    }

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
    page.evaluate.return_value = "Mozilla/5.0 Firefox/135.0"
    transport._page = page

    clearance = transport.get_clearance()

    assert clearance["cookies"]["cf_clearance"] == "token"
    assert clearance["user_agent"] == "Mozilla/5.0 Firefox/135.0"
    assert clearance["proxy"] == transport._proxy


@pytest.mark.unit
def test_clearance_requires_cf_cookie():
    transport = CamoufoxFbrefTransport()
    page = MagicMock()
    page.context.cookies.return_value = [{"name": "other", "value": "value"}]
    transport._page = page

    assert transport.get_clearance() is None
