"""Unit tests for the FBref Camoufox transport (Turnstile solver, resource
blocking, byte accounting, proxy rotation) — all with a faked Playwright page,
no real browser. The live end-to-end fetch is covered by a sandbox e2e run."""

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

    def title(self):
        return self._title

    def evaluate(self, js):
        if "querySelector('table')" in js:
            self._eval_calls += 1
            if self._table_after is None:  # never shows a table
                return False
            return self._eval_calls >= self._table_after
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
                raise RuntimeError("dead proxy")
            orig_goto(url, **kw)
        page.goto = flaky_goto

        t = _transport(monkeypatch, page)
        html = t.fetch("https://fbref.com/x")
        assert html is not None
        assert t.cf_challenges_failed == 1  # first goto failed
        assert t.cf_challenges_passed == 1  # second solved

    @pytest.mark.unit
    def test_returns_none_after_exhausting_rotations(self, monkeypatch):
        page = _FakePage(table_after=None)  # never solves
        t = _transport(monkeypatch, page)
        html = t.fetch("https://fbref.com/x")
        assert html is None
        # 1 initial + MAX_PROXY_ROTATIONS retries all failed to solve.
        assert t.cf_challenges_failed == t.MAX_PROXY_ROTATIONS + 1


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
