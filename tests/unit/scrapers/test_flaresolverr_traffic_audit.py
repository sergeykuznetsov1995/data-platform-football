"""
Unit tests for FlareSolverrClient traffic-audit instrumentation (issue #616).

WhoScored + SoFIFA fetch through FlareSolverr/Camoufox, which exposes no CDP
Network events, so we instrument the single chokepoint every path shares —
``FlareSolverrClient`` — counting:

* ``fs_response_bytes`` — bytes FlareSolverr returns to us (rendered HTML +
  JSON envelope). This is a LOWER BOUND on residential-proxy traffic, NOT the
  proxy MB itself: Camoufox downloads images/CSS/JS/XHR through the proxy and
  returns only the rendered HTML. Naming says ``fs_response_*`` (not
  ``proxy_*``) so we never repeat the #131 mistake of passing payload off as
  proxy traffic.
* ``requests`` + per-URL breakdown (host+path, query dropped).
* ``sessions_created`` — each FlareSolverr session = a fresh CF cold-start,
  the real traffic driver (SoFIFA rotates every 4 requests, WhoScored 8–10).
* ``cf_challenge_failures`` — raised CF / Turnstile failures.
"""
import json

import pytest
from unittest.mock import MagicMock


def _fs_response(payload: dict, *, ok: bool = True, status_code: int = 200):
    """Build a fake ``requests.Response`` carrying a FlareSolverr JSON body."""
    body = json.dumps(payload).encode("utf-8")
    resp = MagicMock()
    resp.ok = ok
    resp.status_code = status_code
    resp.content = body
    resp.text = body.decode("utf-8")
    resp.json.return_value = payload
    return resp


def _ok_solution(html: str) -> dict:
    """FlareSolverr success envelope for a ``request.get`` returning ``html``."""
    return {
        "status": "ok",
        "solution": {
            "response": html,
            "status": 200,
            "cookies": [],
            "userAgent": "x",
        },
    }


def _client_with_posts(responses):
    """FlareSolverrClient whose ``session.post`` yields ``responses`` in order."""
    from scrapers.base.flaresolverr_client import FlareSolverrClient

    client = FlareSolverrClient(url="http://flaresolverr:8191")
    fake_session = MagicMock()
    fake_session.post.side_effect = list(responses)
    client.session = fake_session  # property setter
    return client


class TestFlareSolverrTrafficCountersInit:
    """All new issue-#616 counters must be zero-initialised."""

    @pytest.mark.unit
    def test_init_zero_counters(self):
        from scrapers.base.flaresolverr_client import FlareSolverrClient

        client = FlareSolverrClient()

        assert client._fs_response_bytes == 0
        assert client._requests == 0
        assert dict(client._bytes_by_url) == {}
        assert dict(client._requests_by_url) == {}
        assert client._sessions_created == 0
        assert client._cf_challenge_failures == 0


class TestGetAccumulates:
    """A successful ``get()`` must count payload bytes + one request, attributed
    to the normalised request URL."""

    @pytest.mark.unit
    def test_get_counts_bytes_and_request(self):
        html = "<html>" + "x" * 1000 + "</html>"
        payload = _ok_solution(html)
        client = _client_with_posts([_fs_response(payload)])

        out = client.get("https://www.whoscored.com/Matches/123/Live", "sess-1")

        assert out["html"] == html
        assert client._requests == 1
        # Total = full FlareSolverr JSON envelope bytes returned to us.
        assert client._fs_response_bytes == len(json.dumps(payload).encode())
        # Per-URL attributed under host+path.
        assert dict(client._bytes_by_url) == {
            "www.whoscored.com/Matches/123/Live": client._fs_response_bytes
        }
        assert dict(client._requests_by_url) == {
            "www.whoscored.com/Matches/123/Live": 1
        }

    @pytest.mark.unit
    def test_multiple_gets_accumulate(self):
        p1 = _ok_solution("<html>1</html>")
        p2 = _ok_solution("<html>22</html>")
        client = _client_with_posts([_fs_response(p1), _fs_response(p2)])

        client.get("https://sofifa.com/player/1/?r=240002&set=true", "s")
        client.get("https://sofifa.com/player/2/?r=240002&set=true", "s")

        assert client._requests == 2
        assert client._fs_response_bytes == (
            len(json.dumps(p1).encode()) + len(json.dumps(p2).encode())
        )


class TestPerUrlTracking:
    """Per-URL counter groups by host+path (query dropped) so cache-busting
    params collapse and the breakdown stays bounded."""

    @pytest.mark.unit
    def test_url_key_strips_query(self):
        client = _client_with_posts([_fs_response(_ok_solution("<html>x</html>"))])

        client.get("https://sofifa.com/player/7/?r=240002&set=true&hl=en-US", "s")

        assert list(client._bytes_by_url.keys()) == ["sofifa.com/player/7/"]

    @pytest.mark.unit
    def test_same_endpoint_collapses(self):
        # WhoScored's tournaments data endpoint uses a cache-busting ?d= param;
        # both calls must collapse to one key.
        posts = [_fs_response(_ok_solution(f"<html>{i}</html>")) for i in range(2)]
        client = _client_with_posts(posts)

        client.get("https://www.whoscored.com/t/1/data/?d=111", "s")
        client.get("https://www.whoscored.com/t/1/data/?d=222", "s")

        assert list(client._bytes_by_url.keys()) == ["www.whoscored.com/t/1/data/"]
        assert client._requests_by_url["www.whoscored.com/t/1/data/"] == 2


class TestSessionAndCfCounters:
    """``create_session`` ≈ a CF cold-start; CF failures must be counted and
    must NOT be booked as successful requests. Counters accumulate across a
    session rotation on one reused client."""

    @pytest.mark.unit
    def test_create_session_increments(self):
        client = _client_with_posts([_fs_response({"status": "ok", "session": "s1"})])

        client.create_session("s1", proxy_url="http://u:p@host:1")

        assert client._sessions_created == 1

    @pytest.mark.unit
    def test_cf_failure_increments_and_not_counted_as_request(self):
        from scrapers.base.flaresolverr_client import FlareSolverrCFChallengeFailed

        payload = {
            "status": "error",
            "message": "Error: Cloudflare challenge detected, timeout",
        }
        client = _client_with_posts([_fs_response(payload)])

        with pytest.raises(FlareSolverrCFChallengeFailed):
            client.get("https://www.whoscored.com/Matches/9/Live", "s")

        assert client._cf_challenge_failures == 1
        assert client._requests == 0
        assert dict(client._bytes_by_url) == {}

    @pytest.mark.unit
    def test_counters_survive_session_recreate(self):
        # One client reused across a rotation (destroy + create) — totals
        # accumulate, mirroring the readers' ``self._fs_client`` lifecycle.
        posts = [
            _fs_response(_ok_solution("<html>a</html>")),     # get 1
            _fs_response({"status": "ok"}),                    # destroy_session
            _fs_response({"status": "ok", "session": "s2"}),   # create_session
            _fs_response(_ok_solution("<html>bb</html>")),     # get 2
        ]
        client = _client_with_posts(posts)

        client.get("https://sofifa.com/player/1/", "s1")
        client.destroy_session("s1")
        client.create_session("s2")
        client.get("https://sofifa.com/player/2/", "s2")

        assert client._requests == 2
        assert client._sessions_created == 1
        assert len(client._bytes_by_url) == 2


class TestGetTrafficStatsShape:
    """``get_traffic_stats()`` surfaces all audit fields as a sorted, capped
    top-N list — the shape the bench harness / run scripts consume."""

    @pytest.mark.unit
    def test_stats_shape_and_top_sorted(self):
        from scrapers.base.flaresolverr_client import FlareSolverrClient

        client = FlareSolverrClient()
        client._fs_response_bytes = 3_000_000
        client._requests = 5
        client._sessions_created = 2
        client._cf_challenge_failures = 1
        client._bytes_by_url["www.whoscored.com/Matches/1/Live"] = 2_000_000
        client._requests_by_url["www.whoscored.com/Matches/1/Live"] = 1
        client._bytes_by_url["sofifa.com/player/9/"] = 1_000_000
        client._requests_by_url["sofifa.com/player/9/"] = 4

        stats = client.get_traffic_stats()

        assert stats["fs_response_bytes"] == 3_000_000
        assert stats["fs_response_mb"] == round(3_000_000 / 1024 / 1024, 4)
        assert stats["requests"] == 5
        assert stats["sessions_created"] == 2
        assert stats["cf_challenge_failures"] == 1
        urls = stats["top_traffic_urls"]
        assert [u["url"] for u in urls] == [
            "www.whoscored.com/Matches/1/Live",
            "sofifa.com/player/9/",
        ]
        assert urls[0]["bytes"] == 2_000_000
        assert urls[1]["requests"] == 4

    @pytest.mark.unit
    def test_top_traffic_urls_capped(self):
        from scrapers.base.flaresolverr_client import FlareSolverrClient

        client = FlareSolverrClient()
        for i in range(40):
            client._bytes_by_url[f"host{i}.com/p"] = i + 1
            client._requests_by_url[f"host{i}.com/p"] = 1

        stats = client.get_traffic_stats()

        assert len(stats["top_traffic_urls"]) == FlareSolverrClient._TOP_URLS_N
        assert stats["top_traffic_urls"][0]["url"] == "host39.com/p"


class TestScraperTrafficSurface:
    """SoFIFA exposes its FlareSolverr client counters to its runner/bench."""

    @pytest.mark.unit
    def test_sofifa_returns_reader_client_stats(self):
        from types import SimpleNamespace
        from scrapers.sofifa.scraper import SoFIFAScraper

        reader = SimpleNamespace(
            _fs_client=SimpleNamespace(
                get_traffic_stats=lambda: {"requests": 545, "sessions_created": 136}
            )
        )

        out = SoFIFAScraper.get_traffic_stats(SimpleNamespace(_reader=reader))

        assert out == {"requests": 545, "sessions_created": 136}

    @pytest.mark.unit
    def test_sofifa_no_reader_returns_empty(self):
        from types import SimpleNamespace
        from scrapers.sofifa.scraper import SoFIFAScraper

        out = SoFIFAScraper.get_traffic_stats(SimpleNamespace(_reader=None))

        assert out == {}
