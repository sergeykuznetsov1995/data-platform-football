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
