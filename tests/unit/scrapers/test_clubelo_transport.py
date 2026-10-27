"""ClubElo transport (#1462): white list, pace, retries, block stop, wire bytes."""

from __future__ import annotations

import gzip

import pytest
import requests

from scrapers.clubelo.transport import (
    ClubEloBlocked,
    ClubEloFetchError,
    ClubEloTransport,
    check_path,
    get_source,
)
from tests.unit.scrapers.clubelo_fakes import (
    FIXTURE_DIR,
    FakeResponse,
    FakeSession,
    fixture_response,
    redirect_response,
)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _transport(session, clock=None, **kw):
    clock = clock or Clock()
    return ClubEloTransport(session, sleep=clock.sleep, clock=clock.time, **kw)


@pytest.mark.parametrize("path", ["/Ranking", "/Results", "/login/", "/riverplate", "/santos-fc_2"])
def test_white_list_accepts(path):
    assert check_path(path) == path


@pytest.mark.parametrize(
    "path", ["/Arsenal/Results", "/2026-09-22/Ranking", "/", "", "Ranking", "/a?b=1", "/Fixtures/", "/..",
             "/.", "/...", "/Arsenal%2FResults", "/%2e%2e", "/a\\b", "/a#b"]
)
def test_white_list_rejects_before_any_request(path):
    session = FakeSession({})
    with pytest.raises(ValueError):
        _transport(session).get(path)
    assert session.calls == []


def test_get_source_html_only():
    assert get_source() == "html"
    with pytest.raises(NotImplementedError):
        get_source("api")
    with pytest.raises(ValueError):
        get_source("csv")


def test_gzip_page_counts_wire_bytes_and_decodes():
    wire = (FIXTURE_DIR / "club_riverplate.html.gz").read_bytes()
    session = FakeSession({"/riverplate": fixture_response("club_riverplate.html.gz")})
    transport = _transport(session)
    page = transport.get("/riverplate")
    assert page.status == 200
    assert page.wire_bytes == len(wire) == 48980
    assert page.body_gz == wire and page.gzip_by == "wire"
    assert page.body == gzip.decompress(wire)
    assert transport.wire_bytes == 48980 and transport.requests == 1
    call = session.calls[0]
    assert call["allow_redirects"] is False and call["stream"] is True
    assert call["timeout"] == 30.0
    assert call["headers"] == {"Accept-Encoding": "gzip"}
    assert session.answers["/riverplate"].raw.decode_flags == [False]


def test_identity_body_is_gzipped_by_us():
    session = FakeSession({"/login/": FakeResponse(200, b"<html>x</html>")})
    page = _transport(session).get("/login/")
    assert page.gzip_by == "us" and page.content_encoding == "identity"
    assert gzip.decompress(page.body_gz) == b"<html>x</html>"
    assert page.wire_bytes == len(b"<html>x</html>")


def test_redirect_is_returned_not_followed():
    session = FakeSession({"/lsapi-2483": redirect_response()})
    page = _transport(session).get("/lsapi-2483")
    assert page.status == 302 and page.location == "/"
    assert len(session.calls) == 1


def test_one_request_per_interval():
    clock = Clock()
    session = FakeSession({"/login/": [FakeResponse(200, b"a"), FakeResponse(200, b"b")]})
    transport = _transport(session, clock, min_interval=1.0)
    transport.get("/login/")
    clock.now += 0.25
    transport.get("/login/")
    assert clock.sleeps == [pytest.approx(0.75)]


def test_5xx_is_retried_twice_with_pause():
    clock = Clock()
    session = FakeSession(
        {"/riverplate": [FakeResponse(500, b"err"), FakeResponse(502, b"err"),
                         fixture_response("club_riverplate.html.gz")]}
    )
    transport = _transport(session, clock, retry_pause=30.0)
    page = transport.get("/riverplate")
    assert page.status == 200
    assert transport.requests == 3
    assert clock.sleeps.count(30.0) == 2


def test_500_after_all_retries_raises_fetch_error():
    session = FakeSession({"/riverplate": [FakeResponse(500, b"Server Error (500)")] * 3})
    transport = _transport(session)
    with pytest.raises(ClubEloFetchError, match="HTTP 500"):
        transport.get("/riverplate")
    assert transport.requests == 3


def test_network_error_is_retried_then_raises():
    err = requests.ConnectionError("reset")
    session = FakeSession({"/riverplate": [err, err, err]})
    transport = _transport(session)
    with pytest.raises(ClubEloFetchError, match="ConnectionError"):
        transport.get("/riverplate")
    assert transport.requests == 3


@pytest.mark.parametrize(
    "response",
    [FakeResponse(403, b"forbidden"), FakeResponse(429, b"slow down"),
     FakeResponse(200, b"challenge", {"cf-mitigated": "challenge"})],
)
def test_block_stops_without_retry(response):
    session = FakeSession({"/riverplate": [response]})
    transport = _transport(session)
    with pytest.raises(ClubEloBlocked):
        transport.get("/riverplate")
    assert transport.requests == 1


def test_404_is_returned_without_retry():
    session = FakeSession({"/nope": [FakeResponse(404, b"no")]})
    transport = _transport(session)
    assert transport.get("/nope").status == 404
    assert transport.requests == 1


def test_unexpected_encoding_is_a_fetch_error():
    session = FakeSession({"/login/": FakeResponse(200, b"xx", {"content-encoding": "br"})})
    with pytest.raises(ClubEloFetchError, match="content-encoding"):
        _transport(session).get("/login/")


def test_truncated_gzip_is_retried_then_a_fetch_error():
    wire = (FIXTURE_DIR / "club_riverplate.html.gz").read_bytes()[:1000]
    session = FakeSession({"/riverplate": [FakeResponse(200, wire, {"content-encoding": "gzip"})
                                           for _ in range(3)]})
    transport = _transport(session)
    with pytest.raises(ClubEloFetchError, match="broken gzip"):
        transport.get("/riverplate")
    assert transport.requests == 3 and transport.wire_bytes == 3000


@pytest.fixture
def bootstrap(monkeypatch):
    from scrapers.base import flaresolverr_client
    from scrapers.clubelo import proxy_lease

    events = []
    proxy = "http://lease:secret-lease@clubelo_proxy_filter:8900"

    class Lease:
        proxy_url = proxy

        def acquire(self):
            events.append("acquire")
            return self

        def close(self):
            events.append("release")

    class Solver:
        solution = {"status": 200, "html": "DOM MUST NOT BECOME RAW",
                    "userAgent": "browser-UA", "cookies": [
                        {"name": "cf_clearance", "value": "clear", "domain": ".clubelo.com"},
                        {"name": "__cf_bm", "value": "bm"}]}

        def __init__(self, **kwargs):
            pass

        def create_session(self, session_id, proxy_url):
            events.append(("create", proxy_url))

        def get(self, url, session_id):
            events.append(("solve", url))
            return self.solution

        def destroy_session(self, session_id):
            events.append("destroy")

        def close(self):
            events.append("solver-close")

    monkeypatch.setattr(proxy_lease, "ClubEloLease", Lease)
    monkeypatch.setattr(flaresolverr_client, "FlareSolverrClient", Solver)
    return events, Lease, Solver


def _challenge_session(answers):
    session = FakeSession(answers)
    session.cookies = requests.cookies.RequestsCookieJar()
    session.headers = {}
    session.proxies = {}
    session.close = lambda: None
    return session


def _challenge():
    return FakeResponse(403, b"challenge", {"cf-mitigated": "challenge"})


def test_challenge_solver_then_http_raw_same_lease(bootstrap):
    events, Lease, _ = bootstrap
    session = _challenge_session({"/Results": [_challenge(), FakeResponse(200, b"HTTP RAW")],
                                  "/Arsenal": FakeResponse(200, b"CLUB RAW")})
    original_get = session.get
    proxies = []

    def get(url, **kwargs):
        proxies.append(dict(session.proxies))
        return original_get(url, **kwargs)

    session.get = get
    transport = _transport(session, retries=0)
    page = transport.get("/Results")
    with transport:
        assert page.body == b"HTTP RAW" and gzip.decompress(page.body_gz) == b"HTTP RAW"
        assert transport.get("/Arsenal").body == b"CLUB RAW"
        assert session.trust_env is False
        assert session.headers["User-Agent"] == "browser-UA"
        assert session.cookies.get("cf_clearance", domain="clubelo.com") == "clear"
        assert session.cookies.get("__cf_bm", domain="clubelo.com") == "bm"
    transport.close()
    assert proxies == [{}, {"https": Lease.proxy_url}, {"https": Lease.proxy_url}]
    assert events == ["acquire", ("create", Lease.proxy_url),
                      ("solve", "https://clubelo.com/Ranking"),
                      "destroy", "solver-close", "release"]
    assert all(response.closed for response in session.answers.values()
               if isinstance(response, FakeResponse))


def test_clean_http_never_bootstraps(bootstrap):
    events, _, _ = bootstrap
    session = _challenge_session({"/Ranking": FakeResponse(200, b"raw")})
    with _transport(session) as transport:
        assert transport.get("/Ranking").body == b"raw"
        assert session.proxies == {} and session.trust_env is False
    assert events == []


def test_repeat_challenge_stops_and_releases(bootstrap):
    events, _, _ = bootstrap
    session = _challenge_session({"/Ranking": [_challenge(), _challenge()]})
    with pytest.raises(ClubEloBlocked), _transport(session) as transport:
        transport.get("/Ranking")
    assert events.count("acquire") == events.count("release") == 1
    assert len(session.calls) == 2


@pytest.mark.parametrize("solution", [None, {}, {"status": 403},
    {"status": 200, "cookies": [], "userAgent": "UA"},
    {"status": 200, "cookies": [{"name": "x", "value": "v"}], "userAgent": "UA"},
    {"status": 200, "cookies": [{"name": "cf_clearance", "value": "v", "domain": "evil.com"}], "userAgent": "UA"},
    {"status": 200, "cookies": [{"name": "cf_clearance", "value": "v"}], "userAgent": "UA\r\ninjected"},
    {"status": 200, "cookies": [{"name": "cf_clearance", "value": "v\ninjected"}], "userAgent": "UA"},
])
def test_invalid_solver_response_fails_closed(bootstrap, solution):
    events, _, Solver = bootstrap
    Solver.solution = solution
    session = _challenge_session({"/Ranking": _challenge()})
    with pytest.raises(ClubEloBlocked, match="lease or solver failed"), _transport(session) as transport:
        transport.get("/Ranking")
    assert events.count("release") == events.count("destroy") == 1
    assert len(session.calls) == 1


@pytest.mark.parametrize("failure", ["acquire", "create_session", "get"])
def test_bootstrap_error_is_redacted_and_cleaned(bootstrap, monkeypatch, failure):
    events, Lease, Solver = bootstrap

    def fail(*args, **kwargs):
        raise requests.ConnectionError("secret-lease password control-token")

    monkeypatch.setattr(Lease if failure == "acquire" else Solver, failure, fail)
    with pytest.raises(ClubEloBlocked) as caught:
        with _transport(_challenge_session({"/Ranking": _challenge()})) as transport:
            transport.get("/Ranking")
    assert "secret" not in str(caught.value) and "password" not in str(caught.value)
    assert events.count("release") == 1


def test_proxied_network_error_does_not_leak_credentials(bootstrap, caplog):
    session = _challenge_session({"/Ranking": [_challenge(), requests.exceptions.ProxyError("secret-lease")]})
    with pytest.raises(ClubEloFetchError) as caught, _transport(session, retries=0) as transport:
        transport.get("/Ranking")
    assert "secret-lease" not in str(caught.value) + caplog.text
