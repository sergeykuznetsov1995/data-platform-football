"""Unit tests for the FBref Camoufox transport (Turnstile solver, resource
blocking, byte accounting, proxy rotation) and the FBrefBrowserMixin camoufox
fetch path (validation, proxy health, curl_cffi fast-path) — all with faked
Playwright pages / mocked transports, no real browser. The live end-to-end
fetch is covered by a sandbox e2e run."""

from collections import Counter
from unittest.mock import MagicMock

import pytest

from scrapers.fbref import camoufox_fetch as cf
from scrapers.fbref.camoufox_fetch import (
    CamoufoxFbrefTransport,
    is_cloudflare_blocked,
    should_block_request,
)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

class TestShouldBlockRequest:
    @pytest.mark.unit
    @pytest.mark.parametrize("rtype", ["image", "media", "font", "stylesheet"])
    def test_blocks_non_essential_static(self, rtype):
        assert should_block_request(rtype, "https://fbref.com/x.png") is True

    @pytest.mark.unit
    @pytest.mark.parametrize("rtype", ["document", "script", "xhr", "fetch"])
    def test_passes_essential(self, rtype):
        assert should_block_request(rtype, "https://fbref.com/en/comps/9/") is False

    @pytest.mark.unit
    def test_blocks_analytics_regardless_of_type(self):
        assert should_block_request(
            "script", "https://www.google-analytics.com/analytics.js") is True

    @pytest.mark.unit
    def test_always_passes_cloudflare_assets(self):
        # Even a CSS/font asset served by the challenge iframe must pass, or the
        # bypass is starved.
        assert should_block_request(
            "stylesheet", "https://challenges.cloudflare.com/turnstile/v0/x.css"
        ) is False
        assert should_block_request(
            "script", "https://fbref.com/cdn-cgi/challenge-platform/h/b/x.js"
        ) is False

    @pytest.mark.unit
    def test_blocks_fbref_autocomplete_cache(self):
        # ~1.65 MB search_list.csv — top per-URL consumer in the #616 audit.
        assert should_block_request(
            "xhr", "https://fbref.com/short/inc/players_search_list.csv"
        ) is True

    @pytest.mark.unit
    def test_block_scripts_flag(self):
        url = "https://fbref.com/static/js/sr.min.js"
        # Off by default — scripts pass.
        assert should_block_request("script", url) is False
        # FBREF_CAMOUFOX_BLOCK_SCRIPTS=1 → first-party scripts blocked...
        assert should_block_request("script", url, block_scripts=True) is True
        # ...but Cloudflare challenge JS and documents still pass.
        assert should_block_request(
            "script", "https://challenges.cloudflare.com/turnstile/v0/x.js",
            block_scripts=True,
        ) is False
        assert should_block_request("document", url, block_scripts=True) is False


class TestIsCloudflareBlocked:
    @pytest.mark.unit
    def test_empty_is_blocked(self):
        assert is_cloudflare_blocked("") is True

    @pytest.mark.unit
    @pytest.mark.parametrize("marker", [
        "Just a moment...", "cf_chl_opt", "Checking your browser",
    ])
    def test_markers_flag_challenge(self, marker):
        assert is_cloudflare_blocked(f"<html>{marker}</html>") is True

    @pytest.mark.unit
    def test_real_page_not_blocked(self):
        assert is_cloudflare_blocked(
            "<html><body><table id='sched'></table></body></html>",
            title="2025-2026 Premier League Scores & Fixtures | FBref",
        ) is False


# ---------------------------------------------------------------------------
# Fake Playwright page
# ---------------------------------------------------------------------------

class _FakeMouse:
    def __init__(self):
        self.clicks = []

    def click(self, x, y):
        self.clicks.append((x, y))


class _FakeFrame:
    def __init__(self, url, box=None):
        self.url = url
        self._box = box

    def locator(self, _sel):
        frame = self

        class _Loc:
            def bounding_box(self_inner):
                return frame._box
        return _Loc()


class _FakeContext:
    def __init__(self, cookies=None):
        self._cookies = cookies or []

    def cookies(self, _urls=None):
        return self._cookies


class _FakePage:
    """Configurable fake: ``table_after`` polls return has_table=True; frames
    expose a Turnstile iframe with a bounding box for the click path."""

    def __init__(self, table_after=1, content_html=None, frames=None,
                 title="Just a moment..."):
        self._table_after = table_after
        self._content = content_html or (
            "<html><body><table id='sched'></table></body></html>")
        self.frames = frames if frames is not None else [
            _FakeFrame("https://challenges.cloudflare.com/turnstile/v0/x",
                       box={"x": 352, "y": 304, "width": 300, "height": 65})
        ]
        self._title = title
        self.mouse = _FakeMouse()
        self._eval_calls = 0
        self.goto_calls = []
        self.context = _FakeContext()

    def title(self):
        return self._title

    def evaluate(self, js):
        if "querySelector('table')" in js:
            self._eval_calls += 1
            if self._table_after is None:  # never shows a table
                return False
            return self._eval_calls >= self._table_after
        if "navigator.userAgent" in js:
            return ("Mozilla/5.0 (X11; Linux x86_64; rv:135.0) "
                    "Gecko/20100101 Firefox/135.0")
        return None  # FBREF_UNCOMMENT_TABLES_JS etc.

    def content(self):
        return self._content

    def goto(self, url, **_kw):
        self.goto_calls.append(url)

    def route(self, *_a, **_k):
        pass

    def on(self, *_a, **_k):
        pass


def _transport(monkeypatch, page):
    """Build a transport with _start patched to install a fake page."""
    t = CamoufoxFbrefTransport(proxy=None)
    t.POLL_INTERVAL_S = 0.0
    t.CF_SOLVE_TIMEOUT_S = 1.0
    monkeypatch.setattr(cf.time, "sleep", lambda *_: None)

    def fake_start():
        t._page = page
    monkeypatch.setattr(t, "_start", fake_start)
    monkeypatch.setattr(t, "_restart", fake_start)
    return t


# ---------------------------------------------------------------------------
# Turnstile solve
# ---------------------------------------------------------------------------

class TestSolveCurrentPage:
    @pytest.mark.unit
    def test_returns_html_when_table_appears(self, monkeypatch):
        page = _FakePage(table_after=1)
        t = _transport(monkeypatch, page)
        t._page = page
        html = t._solve_current_page()
        assert html is not None and "<table" in html

    @pytest.mark.unit
    def test_clicks_checkbox_before_solving(self, monkeypatch):
        # Table only appears after 3 polls → the solver must click the
        # Turnstile checkbox (after CLICK_AFTER_POLLS) at least once.
        page = _FakePage(table_after=3)
        t = _transport(monkeypatch, page)
        t._page = page
        t._solve_current_page()
        assert page.mouse.clicks, "expected a Turnstile checkbox click"
        cx, cy = page.mouse.clicks[0]
        assert cx == 352 + 30 and cy == 304 + 65 / 2

    @pytest.mark.unit
    def test_timeout_returns_none(self, monkeypatch):
        page = _FakePage(table_after=None)  # never shows a table
        t = _transport(monkeypatch, page)
        t._page = page
        assert t._solve_current_page() is None

    @pytest.mark.unit
    def test_never_returns_challenge_shell(self, monkeypatch):
        # A table is 'present' but the content is still the CF challenge → must
        # NOT be accepted as solved (guards the empty-title false positive).
        page = _FakePage(table_after=1,
                         content_html="<html>Just a moment...<table></table></html>")
        t = _transport(monkeypatch, page)
        t._page = page
        assert t._solve_current_page() is None


# ---------------------------------------------------------------------------
# fetch() orchestration
# ---------------------------------------------------------------------------

class TestFetch:
    @pytest.mark.unit
    def test_success_first_attempt(self, monkeypatch):
        page = _FakePage(table_after=1)
        t = _transport(monkeypatch, page)
        html = t.fetch("https://fbref.com/x")
        assert html is not None and "<table" in html
        # The fake page shows a Turnstile iframe → one attempt, one pass.
        assert t.cf_challenge_attempts == 1
        assert t.cf_challenges_passed == 1
        assert page.goto_calls == ["https://fbref.com/x"]

    @pytest.mark.unit
    def test_rotates_on_goto_failure(self, monkeypatch):
        page = _FakePage(table_after=1)

        calls = {"n": 0}
        orig_goto = page.goto

        def flaky_goto(url, **kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("net::ERR_PROXY_CONNECTION_FAILED")
            orig_goto(url, **kw)
        page.goto = flaky_goto

        t = _transport(monkeypatch, page)
        proxy_result = MagicMock()
        t._proxy_result_callback = proxy_result
        html = t.fetch("https://fbref.com/x")
        assert html is not None
        # A goto failure is a nav problem, NOT a CF challenge stat — only the
        # second attempt saw (and passed) the challenge.
        assert t.cf_challenges_failed == 0
        assert t.cf_challenge_attempts == 1
        assert t.cf_challenges_passed == 1
        assert proxy_result.call_args_list == [
            ((False, "timeout"), {}),
            ((True, None), {}),
        ]

    @pytest.mark.unit
    def test_browser_crash_does_not_immediately_ban_proxy(self, monkeypatch):
        page = _FakePage(table_after=1)
        calls = {"n": 0}
        orig_goto = page.goto

        def flaky_goto(url, **kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("Target page, context or browser closed")
            orig_goto(url, **kw)

        page.goto = flaky_goto
        t = _transport(monkeypatch, page)
        proxy_result = MagicMock()
        t._proxy_result_callback = proxy_result

        assert t.fetch("https://fbref.com/x") is not None
        assert proxy_result.call_args_list == [
            ((True, None), {}),
        ]

    @pytest.mark.unit
    def test_start_crash_is_bounded_and_not_charged_to_proxy(
        self, monkeypatch
    ):
        page = _FakePage(table_after=1)
        t = CamoufoxFbrefTransport(proxy=None)
        t.POLL_INTERVAL_S = 0.0
        t.CF_SOLVE_TIMEOUT_S = 1.0
        monkeypatch.setattr(cf.time, "sleep", lambda *_: None)
        calls = {"n": 0}

        def flaky_start():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("browser process closed")
            t._page = page

        monkeypatch.setattr(t, "_start", flaky_start)
        proxy_result = MagicMock()
        t._proxy_result_callback = proxy_result

        assert t.fetch("https://fbref.com/x") is not None
        assert calls["n"] == 2
        assert proxy_result.call_args_list == [
            ((True, None), {}),
        ]

    @pytest.mark.unit
    def test_no_table_without_challenge_does_not_penalize_proxy(
        self, monkeypatch
    ):
        page = _FakePage(table_after=None, frames=[])
        t = _transport(monkeypatch, page)
        proxy_result = MagicMock()
        t._proxy_result_callback = proxy_result

        assert t.fetch("https://fbref.com/x") is None
        assert proxy_result.call_count == 0

    @pytest.mark.unit
    def test_returns_none_after_exhausting_rotations(self, monkeypatch):
        page = _FakePage(table_after=None)  # never solves
        t = _transport(monkeypatch, page)
        proxy_result = MagicMock()
        t._proxy_result_callback = proxy_result
        html = t.fetch("https://fbref.com/x")
        assert html is None
        # 1 initial + MAX_PROXY_ROTATIONS retries all failed to solve.
        assert t.cf_challenges_failed == t.MAX_PROXY_ROTATIONS + 1
        assert proxy_result.call_count == t.MAX_PROXY_ROTATIONS + 1
        proxy_result.assert_called_with(False, "cloudflare")


# ---------------------------------------------------------------------------
# byte accounting
# ---------------------------------------------------------------------------

class TestByteAccounting:
    @pytest.mark.unit
    def test_request_finished_tallies_sizes(self):
        t = CamoufoxFbrefTransport(proxy=None)

        class _Req:
            resource_type = "document"

            def sizes(self):
                return {"responseBodySize": 1000, "responseHeadersSize": 100,
                        "requestBodySize": 10, "requestHeadersSize": 40}

        t._on_request_finished(_Req())
        stats = t.traffic_stats()
        assert stats["real_bytes_downloaded"] == 1150
        assert stats["real_requests_count"] == 1
        assert stats["real_bytes_by_resource_type"]["document"] == 1150

    @pytest.mark.unit
    def test_maybe_block_aborts_and_counts(self):
        t = CamoufoxFbrefTransport(proxy=None)
        aborted = {"n": 0}
        continued = {"n": 0}

        class _Route:
            class request:
                resource_type = "image"
                url = "https://fbref.com/logo.png"

            def abort(self_inner):
                aborted["n"] += 1

            def continue_(self_inner):
                continued["n"] += 1

        t._maybe_block(_Route())
        assert aborted["n"] == 1 and continued["n"] == 0
        assert t._blocked_count == 1


# ---------------------------------------------------------------------------
# CF counter semantics (#B5): attempts only when a challenge shell is seen
# ---------------------------------------------------------------------------

class TestChallengeCounterSemantics:
    @pytest.mark.unit
    def test_warm_page_is_not_a_challenge_attempt(self, monkeypatch):
        # No CF iframe, real content immediately → warm page, no CF stats.
        page = _FakePage(table_after=1, frames=[])
        t = _transport(monkeypatch, page)
        html = t.fetch("https://fbref.com/x")
        assert html is not None
        assert t.cf_challenge_attempts == 0
        assert t.cf_challenges_passed == 0
        assert t.cf_challenges_failed == 0

    @pytest.mark.unit
    def test_shell_without_iframe_still_counts_attempt(self, monkeypatch):
        # Content shows a table but is still the CF shell (no iframe) — each
        # solve attempt must be counted and failed.
        page = _FakePage(
            table_after=1, frames=[],
            content_html="<html>Just a moment...<table></table></html>")
        t = _transport(monkeypatch, page)
        assert t.fetch("https://fbref.com/x") is None
        assert t.cf_challenge_attempts == t.MAX_PROXY_ROTATIONS + 1
        assert t.cf_challenges_failed == t.MAX_PROXY_ROTATIONS + 1
        assert t.cf_challenges_passed == 0


# ---------------------------------------------------------------------------
# Page-count restart (Firefox memory cap)
# ---------------------------------------------------------------------------

class TestPageLimitRestart:
    @pytest.mark.unit
    def test_restarts_after_page_limit(self, monkeypatch):
        page = _FakePage(table_after=1, frames=[])
        t = _transport(monkeypatch, page)
        t._max_pages_per_session = 2
        restarts = {"n": 0}

        def counting_restart():
            restarts["n"] += 1
            t._page = page
            t._pages_this_session = 0  # mirrors real _restart → _start
        monkeypatch.setattr(t, "_restart", counting_restart)

        for i in range(5):
            assert t.fetch(f"https://fbref.com/{i}") is not None
        # Limit 2 → restart before pages 3 and 5.
        assert restarts["n"] == 2

    @pytest.mark.unit
    def test_default_limit_from_env(self, monkeypatch):
        monkeypatch.setenv("FBREF_CAMOUFOX_MAX_PAGES", "37")
        t = CamoufoxFbrefTransport(proxy=None)
        assert t._max_pages_per_session == 37
        monkeypatch.delenv("FBREF_CAMOUFOX_MAX_PAGES")
        t2 = CamoufoxFbrefTransport(proxy=None)
        assert t2._max_pages_per_session == 200


# ---------------------------------------------------------------------------
# get_clearance (curl_cffi fast-path export)
# ---------------------------------------------------------------------------

class TestGetClearance:
    @pytest.mark.unit
    def test_exports_cookies_ua_proxy(self, monkeypatch):
        page = _FakePage(table_after=1)
        page.context = _FakeContext([
            {"name": "cf_clearance", "value": "tok123"},
            {"name": "sid", "value": "s1"},
        ])
        t = _transport(monkeypatch, page)
        t._page = page
        t._proxy = {"server": "http://p.example.io:10000",
                    "username": "u", "password": "pw"}
        clearance = t.get_clearance()
        assert clearance["cookies"] == {"cf_clearance": "tok123", "sid": "s1"}
        assert "Firefox" in clearance["user_agent"]
        assert clearance["proxy"]["server"] == "http://p.example.io:10000"

    @pytest.mark.unit
    def test_none_without_cf_clearance(self, monkeypatch):
        page = _FakePage(table_after=1)
        page.context = _FakeContext([{"name": "sid", "value": "s1"}])
        t = _transport(monkeypatch, page)
        t._page = page
        assert t.get_clearance() is None

    @pytest.mark.unit
    def test_none_without_page(self):
        t = CamoufoxFbrefTransport(proxy=None)
        assert t.get_clearance() is None


# ---------------------------------------------------------------------------
# FBrefBrowserMixin camoufox path: validation, proxy health, HTTP fast-path
# ---------------------------------------------------------------------------

_SEASON_HTML = ('<html><body><table id="stats_standard">'
                + 'x' * 6000 + '</table></body></html>')
_MATCH_OK_HTML = (
    '<html><body>'
    '<table id="stats_18bb7c10_summary"><tr><td>x</td></tr></table>'
    '<table id="stats_b8fd03ef_summary"><tr><td>y</td></tr></table>'
    + 'x' * 6000 + '</body></html>')
# Lineup table parsed fine, but no stats_*_summary — the ~5% truncated load.
_MATCH_TRUNCATED_HTML = (
    '<html><body><table id="lineup_a"><tr><td>x</td></tr></table>'
    + 'x' * 6000 + '</body></html>')
_MATCH_ONE_TEAM_HTML = (
    '<html><body>'
    '<table id="stats_18bb7c10_summary"><tr><td>x</td></tr></table>'
    + 'x' * 6000 + '</body></html>')


def _make_camoufox_host(transport_html=_MATCH_OK_HTML):
    """FBrefScraper stub wired for the camoufox fetch path only."""
    from scrapers.fbref.scraper import FBrefScraper

    s = FBrefScraper.__new__(FBrefScraper)
    s.fbref_transport = 'camoufox'
    s._page_cache = {}
    s._rate_limiter = MagicMock()
    s._proxy_manager = MagicMock()
    s._current_proxy_obj = MagicMock()
    s._consecutive_fetch_failures = 0
    s._last_validation_failure = None
    s._nodriver_browser = None
    s._http_session = None
    s._http_cookies_time = None
    s._http_request_count = 0
    s._http_proxy_minted = None
    s._http_consecutive_fallbacks = 0
    s.HTTP_MAX_FALLBACKS_BEFORE_REMINT = 2
    s.HTTP_COOKIE_TTL_MINUTES = 25
    s.HTTP_MAX_REQUESTS = 150
    s._real_traffic_base_bytes = 0
    s._real_traffic_base_requests = 0
    s._real_traffic_base_bytes_by_rtype = Counter()
    s._real_traffic_base_requests_by_rtype = Counter()
    s._cf_challenge_attempts_base = 0
    s._cf_challenges_passed_base = 0
    s._cf_challenges_failed_base = 0
    s._stats = {'failures': 0, 'successes': 0}
    s._track_download = MagicMock()
    s._manage_cache_size = MagicMock()

    transport = MagicMock()
    transport.fetch.return_value = transport_html
    transport.traffic_stats.return_value = {
        'real_bytes_downloaded': 100,
        'real_requests_count': 2,
        'real_bytes_by_resource_type': {'document': 100},
        'cf_challenge_attempts': 1,
        'cf_challenges_passed': 1,
        'cf_challenges_failed': 0,
        'blocked_count': 0,
    }
    transport.get_clearance.return_value = None
    s._camoufox_transport = transport
    s._get_camoufox_transport = MagicMock(return_value=transport)
    return s, transport


class TestCamoufoxPathValidation:
    """#A1: the camoufox path must run the same page validation as nodriver."""

    @pytest.mark.unit
    def test_truncated_match_page_is_failure(self):
        s, _ = _make_camoufox_host(_MATCH_TRUNCATED_HTML)
        html = s._fetch_page_camoufox(
            'https://fbref.com/en/matches/x', page_type='match')
        assert html is None
        assert s._stats['failures'] == 1
        assert s._last_validation_failure == 'no_match_summary'
        # A page-contract failure is not a dead proxy. Per-attempt proxy health
        # is emitted by CamoufoxFbrefTransport before this validator runs.
        s._proxy_manager.record_result.assert_not_called()

    @pytest.mark.unit
    def test_valid_match_page_succeeds(self):
        s, _ = _make_camoufox_host(_MATCH_OK_HTML)
        html = s._fetch_page_camoufox(
            'https://fbref.com/en/matches/x', page_type='match')
        assert html == _MATCH_OK_HTML
        assert s._stats['successes'] == 1
        assert s._last_validation_failure is None
        s._proxy_manager.record_result.assert_not_called()

    @pytest.mark.unit
    def test_one_team_summary_is_partial_not_a_tombstone_signal(self):
        s, _ = _make_camoufox_host(_MATCH_ONE_TEAM_HTML)

        assert s._fetch_page_camoufox(
            'https://fbref.com/en/matches/x', page_type='match'
        ) is None
        assert s._last_validation_failure == 'incomplete_match_summaries'

    @pytest.mark.unit
    def test_no_html_is_failure_without_double_recording_proxy(self):
        s, _ = _make_camoufox_host(None)
        html = s._fetch_page_camoufox('https://fbref.com/x', page_type='other')
        assert html is None
        assert s._stats['failures'] == 1
        s._proxy_manager.record_result.assert_not_called()


class TestCamoufoxHttpFastPath:
    """Camoufox parity with the nodriver curl_cffi fast-path (#624)."""

    @pytest.mark.unit
    def test_mints_http_session_after_transport_success(self):
        s, transport = _make_camoufox_host(_SEASON_HTML)
        transport.get_clearance.return_value = {
            'cookies': {'cf_clearance': 'tok'},
            'user_agent': 'UA-Firefox',
            'proxy': {'server': 'http://p.example.io:1',
                      'username': 'u', 'password': 'pw'},
        }
        session = MagicMock()
        s._create_http_session = MagicMock(return_value=session)

        html = s._fetch_page_camoufox('https://fbref.com/x', page_type='other')

        assert html == _SEASON_HTML
        kwargs = s._create_http_session.call_args.kwargs
        assert kwargs['impersonate'] == 'firefox135'
        assert kwargs['user_agent'] == 'UA-Firefox'
        assert kwargs['proxy_url'] == 'http://u:pw@p.example.io:1'
        assert s._http_session is session
        assert s._http_proxy_minted == 'p.example.io:1'

    @pytest.mark.unit
    def test_uses_http_fast_path_when_session_live(self):
        s, transport = _make_camoufox_host()
        s._http_session = MagicMock()
        s._http_cookies_expired = MagicMock(return_value=False)
        s._fetch_page_http = MagicMock(return_value=_SEASON_HTML)

        html = s._fetch_page_camoufox('https://fbref.com/x', page_type='other')

        assert html == _SEASON_HTML
        transport.fetch.assert_not_called()
        assert s._stats['http_fetch_ok'] == 1
        # An HTTP-path result says nothing about the browser's current proxy.
        s._proxy_manager.record_result.assert_not_called()

    @pytest.mark.unit
    def test_fallback_drops_session_at_threshold_and_remints(self):
        s, transport = _make_camoufox_host(_SEASON_HTML)
        s._http_session = MagicMock()
        s._http_cookies_expired = MagicMock(return_value=False)
        s._fetch_page_http = MagicMock(return_value=None)
        s._http_consecutive_fallbacks = 1  # one miss away from threshold (2)

        html = s._fetch_page_camoufox('https://fbref.com/x', page_type='other')

        assert html == _SEASON_HTML  # camoufox fallback succeeded
        assert s._stats['http_fetch_fallback'] == 1
        assert s._http_consecutive_fallbacks == 0
        # Session dropped at threshold, then the transport success re-mints.
        transport.get_clearance.assert_called_once()

    @pytest.mark.unit
    def test_expired_session_is_dropped_and_reminted(self):
        s, transport = _make_camoufox_host(_SEASON_HTML)
        stale = MagicMock()
        s._http_session = stale
        s._http_cookies_expired = MagicMock(return_value=True)

        html = s._fetch_page_camoufox('https://fbref.com/x', page_type='other')

        assert html == _SEASON_HTML
        stale.close.assert_called_once()
        transport.fetch.assert_called_once()
        transport.get_clearance.assert_called_once()
        assert s._stats['http_session_expired'] == 1

    @pytest.mark.unit
    def test_truncated_http_match_falls_back_to_browser(self):
        s, transport = _make_camoufox_host(_MATCH_OK_HTML)
        s._http_session = MagicMock()
        s._http_cookies_expired = MagicMock(return_value=False)
        s._fetch_page_http = MagicMock(return_value=_MATCH_TRUNCATED_HTML)
        s._record_http_diag = MagicMock()

        html = s._fetch_page_camoufox(
            'https://fbref.com/en/matches/x', page_type='match')

        assert html == _MATCH_OK_HTML
        assert s._stats.get('http_fetch_ok', 0) == 0
        assert s._stats['http_fetch_fallback'] == 1
        transport.fetch.assert_called_once()
        assert s._record_http_diag.call_args.kwargs['reason'] == (
            'page_validation_no_match_summary'
        )


class TestMergeCamoufoxTraffic:
    """#B5: transport counters merge ON TOP of the accumulated base instead
    of overwriting run totals."""

    @pytest.mark.unit
    def test_accumulates_on_top_of_base(self):
        s, transport = _make_camoufox_host()
        s._real_traffic_base_bytes = 1000
        s._real_traffic_base_requests = 10
        s._real_traffic_base_bytes_by_rtype = Counter({'document': 1000})
        s._cf_challenge_attempts_base = 5

        s._merge_camoufox_traffic(transport)

        assert s._stats['real_bytes_downloaded'] == 1100
        assert s._stats['real_requests_count'] == 12
        assert s._stats['real_bytes_by_resource_type']['document'] == 1100
        assert s._stats['cf_challenge_attempts'] == 6


class TestPlaywrightProxyToUrl:
    @pytest.mark.unit
    def test_with_credentials(self):
        from scrapers.fbref.browser_manager import FBrefBrowserMixin
        assert FBrefBrowserMixin._playwright_proxy_to_url(
            {'server': 'http://h:1', 'username': 'u', 'password': 'p'}
        ) == 'http://u:p@h:1'

    @pytest.mark.unit
    def test_without_credentials(self):
        from scrapers.fbref.browser_manager import FBrefBrowserMixin
        assert FBrefBrowserMixin._playwright_proxy_to_url(
            {'server': 'http://h:1'}) == 'http://h:1'

    @pytest.mark.unit
    def test_none_proxy(self):
        from scrapers.fbref.browser_manager import FBrefBrowserMixin
        assert FBrefBrowserMixin._playwright_proxy_to_url(None) is None
