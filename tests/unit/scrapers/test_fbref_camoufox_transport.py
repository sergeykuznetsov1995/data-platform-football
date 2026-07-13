"""Network-free tests for the single FBref Camoufox clearance transport."""

from unittest.mock import MagicMock

import pytest

from scrapers.fbref.camoufox_fetch import (
    BROWSER_REQUEST_FIXED_OVERHEAD_BYTES,
    BROWSER_UNDECLARED_BODY_RESERVATION_BYTES,
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
    "headers",
    [
        {},
        {"content-length": "not-a-number"},
        {"content-length": "10", "transfer-encoding": "chunked"},
    ],
)
def test_browser_aborts_whole_session_when_undeclared_body_cannot_fit(headers):
    cap = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES + 100
    transport = CamoufoxFbrefTransport(max_network_bytes=cap)
    route = _Route()
    transport._maybe_block(route)

    transport._on_response(_Response(route.request, headers))

    stats = transport.traffic_stats()
    assert route.continued == 1
    assert stats["byte_budget_exhausted"] is True
    assert stats["byte_budget_failure"] == (
        f"undeclared_body_exceeds_cap:{BROWSER_UNDECLARED_BODY_RESERVATION_BYTES}"
    )
    assert stats["inflight_reserved_bytes"] == 0
    assert stats["budget_unobserved_bytes"] == (
        BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    )
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
def test_rewrapped_request_reuses_its_route_reservation():
    """Firefox can deliver the response with a different Request wrapper for
    the request route() already reserved; it must not be charged twice."""
    overhead = BROWSER_REQUEST_FIXED_OVERHEAD_BYTES
    transport = CamoufoxFbrefTransport(
        max_network_bytes=(4 * overhead) + 1000,
        max_network_requests=5,
    )
    route = _Route()
    transport._maybe_block(route)

    rewrapped = MagicMock(resource_type="document", url=route.request.url)
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


@pytest.mark.unit
def test_unrouted_request_over_request_cap_aborts_session():
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
    assert stats["byte_budget_failure"] == "unrouted_request_over_request_cap"


@pytest.mark.unit
def test_unrouted_request_over_byte_cap_aborts_session():
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
    assert stats["byte_budget_failure"] == "unrouted_request_over_byte_cap"


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
    driver = _FakeChild("node")
    unrelated = _FakeChild("postgres")

    process = MagicMock()
    process.children.return_value = [browser, driver, unrelated]
    monkeypatch.setattr(psutil, "Process", lambda: process)

    transport._kill_browser_processes()

    assert browser.killed and driver.killed
    assert not unrelated.killed
    assert transport.traffic_stats()["browser_start_timeouts"] == 1


def test_browser_start_watchdog_is_disarmed_once_the_browser_is_up(monkeypatch):
    """A healthy start must not leave a timer that kills a working session."""
    timers = []

    class _Timer:
        def __init__(self, interval, function):
            self.interval = interval
            self.function = function
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
    browser = MagicMock()
    browser.new_page.return_value = page
    camoufox = MagicMock()
    camoufox.return_value.__enter__.return_value = browser
    monkeypatch.setitem(
        __import__("sys").modules,
        "camoufox.sync_api",
        MagicMock(Camoufox=camoufox),
    )

    transport = CamoufoxFbrefTransport(proxy={"server": "http://p:1"})
    transport._start()

    assert len(timers) == 1
    assert timers[0].cancelled is True
    assert timers[0].interval == transport.BROWSER_START_TIMEOUT_S


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
