"""ClubElo transport (#1462): white list, pace, retries, block stop, wire bytes.

#2246 adds bootstrap observability: every Cloudflare bootstrap attempt —
success, solver failure or lease refusal — emits exactly one JSON record on
the ``clubelo_bootstrap_attempt`` prefix. These tests parse the records as
JSON (never substring-grepping for structure) and prove the bounded allowlist
and the redaction of cookies, userAgent, proxy credentials and secrets.
"""

from __future__ import annotations

import builtins
import gzip
import json

import pytest
import requests

from scrapers.clubelo.transport import (
    BOOTSTRAP_LOG_PREFIX,
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


@pytest.mark.parametrize(
    "path", ["/Ranking", "/Results", "/login/", "/riverplate", "/santos-fc_2", "/Porto B"]
)
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


def test_space_slug_is_sent_quoted_and_page_path_stays_logical():
    session = FakeSession({"/Porto%20B": FakeResponse(200, b"<html>x</html>")})
    transport = _transport(session)
    page = transport.get("/Porto B")
    assert check_path("/Porto B") == "/Porto B"
    assert [call["path"] for call in session.calls] == ["/Porto%20B"]
    assert page.path == "/Porto B" and page.body == b"<html>x</html>"


def test_non_ascii_slug_is_utf8_quoted():
    session = FakeSession({"/K%C3%B8benhavn": FakeResponse(200, b"<html>x</html>")})
    page = _transport(session).get("/København")
    assert [call["path"] for call in session.calls] == ["/K%C3%B8benhavn"]
    assert page.path == "/København"


@pytest.mark.parametrize("path", [
    "/Porto%20B",        # input percent-escape: never decoded or accepted
    "/Porto B/extra",    # a space does not re-open a second segment
    "/Porto B?x=1",
    "/Porto B#frag",
    "/Porto B%x",
    "/Porto B\\x",
    "/Porto\tB", "/Porto\nB", "/Porto\rB", "/Porto\vB", "/Porto\fB",
    "/Porto\xa0B",       # NBSP and other unicode whitespace stay forbidden
    "/Porto\x00B", "/Porto\x7fB",  # control characters and DEL
    "/ ",
])
def test_space_slug_rejects_forbidden_variants_before_any_request(path):
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
            self.config = kwargs

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


_ATTEMPT_KEYS = frozenset({
    "schema", "event", "attempt", "outcome", "retry", "lease_id",
    "upstream_fingerprint", "latency_ms", "failed_validation_predicate",
})


def _attempt_records(caplog):
    """Parse every #2246 JSON attempt record; each record is the whole allowlist."""
    records = []
    for record in caplog.records:
        message = record.getMessage()
        if not message.startswith(BOOTSTRAP_LOG_PREFIX + " "):
            continue
        data = json.loads(message[len(BOOTSTRAP_LOG_PREFIX) + 1:])
        assert set(data) == _ATTEMPT_KEYS
        assert data["schema"] == 1 and data["event"] == BOOTSTRAP_LOG_PREFIX
        assert isinstance(data["attempt"], int) and data["attempt"] >= 1
        assert isinstance(data["retry"], bool)
        assert isinstance(data["latency_ms"], int) and data["latency_ms"] >= 0
        records.append(data)
    return records


@pytest.fixture
def identified_lease(bootstrap):
    """The bootstrap lease double carrying a lease id and a stage-1 fingerprint."""
    _events, Lease, _Solver = bootstrap
    Lease.lease_id = "lease-7741"
    Lease.upstream_fingerprint = "0123456789abcdef"
    return bootstrap


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
    with pytest.raises(ClubEloBlocked, match=r"challenge not solved: .*\(attempt 3/3\)"), \
            _transport(session) as transport:
        transport.get("/Ranking")
    assert events.count("acquire") == events.count("release") == events.count("destroy") == 3
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
    assert events.count("release") == (1 if failure == "acquire" else 3)


def test_proxied_network_error_does_not_leak_credentials(bootstrap, caplog):
    session = _challenge_session({"/Ranking": [_challenge(), requests.exceptions.ProxyError("secret-lease")]})
    with pytest.raises(ClubEloFetchError) as caught, _transport(session, retries=0) as transport:
        transport.get("/Ranking")
    assert "secret-lease" not in str(caught.value) + caplog.text


def test_challenge_solver_has_ninety_seconds_with_http_margin(bootstrap):
    session = _challenge_session({"/Ranking": [_challenge(), FakeResponse(200, b"RAW")]})
    with _transport(session) as transport:
        transport.get("/Ranking")
        config = transport._solver.config
        assert config["default_max_timeout_ms"] == 90000
        assert config["default_timeout"] >= config["default_max_timeout_ms"] / 1000 + 30


def test_dead_tunnel_retries_with_a_fresh_lease(bootstrap, caplog):
    from scrapers.base.flaresolverr_client import FlareSolverrError

    events, Lease, Solver = bootstrap
    answers = [FlareSolverrError("HTTP 500: net::ERR_TUNNEL_CONNECTION_FAILED lease:secret-lease"),
               Solver.solution]
    original_get = Solver.get

    def get(self, url, session_id):
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            events.append(("solve", url))
            raise answer
        return original_get(self, url, session_id)

    Solver.get = get
    clock = Clock()
    session = _challenge_session({"/Ranking": [_challenge(), FakeResponse(200, b"RAW")]})
    with _transport(session, clock=clock) as transport:
        assert transport.get("/Ranking").body == b"RAW"
        assert session.proxies == {"https": Lease.proxy_url}
    assert events == ["acquire", ("create", Lease.proxy_url), ("solve", "https://clubelo.com/Ranking"),
                      "destroy", "solver-close", "release",
                      "acquire", ("create", Lease.proxy_url), ("solve", "https://clubelo.com/Ranking"),
                      "destroy", "solver-close", "release"]
    assert 10.0 in clock.sleeps
    assert "attempt 1/3 failed: proxy tunnel failed" in caplog.text
    assert "secret-lease" not in caplog.text


def test_lease_failure_is_named_and_not_retried(bootstrap, monkeypatch):
    events, Lease, _ = bootstrap

    def refused(self):
        events.append("acquire")
        raise ValueError("ClubElo gateway refused the lease")

    monkeypatch.setattr(Lease, "acquire", refused)
    with pytest.raises(ClubEloBlocked, match=r"lease: ClubElo gateway refused the lease \(attempt 1/3\)"):
        with _transport(_challenge_session({"/Ranking": _challenge()})) as transport:
            transport.get("/Ranking")
    assert events == ["acquire", "release"]


@pytest.mark.parametrize("error, reason", [
    ("FlareSolverrTimeout", "solver timeout or unreachable"),
    ("FlareSolverrCFChallengeFailed", "challenge not solved: solver gave up"),
    ("FlareSolverrError", "solver error"),
    ("RuntimeError", r"solver error \(RuntimeError\)"),
    ("ValueError", r"solver error \(ValueError\)"),
])
def test_solver_failure_reason_is_named(bootstrap, monkeypatch, error, reason):
    from scrapers.base import flaresolverr_client

    events, _, Solver = bootstrap
    exc_type = getattr(flaresolverr_client, error, None) or getattr(builtins, error)

    def fail(self, url, session_id):
        raise exc_type("secret-lease password")

    monkeypatch.setattr(Solver, "get", fail)
    with pytest.raises(ClubEloBlocked, match=reason + r" \(attempt 3/3\)") as caught:
        with _transport(_challenge_session({"/Ranking": _challenge()})) as transport:
            transport.get("/Ranking")
    assert "secret" not in str(caught.value)
    assert events.count("acquire") == events.count("release") == 3


def test_success_emits_exactly_one_clearance_accepted_record(identified_lease, caplog):
    _events, _Lease, _Solver = identified_lease
    session = _challenge_session({"/Ranking": [_challenge(), FakeResponse(200, b"RAW")]})
    with _transport(session) as transport:
        transport.get("/Ranking")
    records = _attempt_records(caplog)
    assert len(records) == 1
    record = records[0]
    assert set(record) == set(_ATTEMPT_KEYS)  # bounded allowlist, no extra keys
    assert record["schema"] == 1 and record["event"] == BOOTSTRAP_LOG_PREFIX
    assert record["attempt"] == 1
    assert record["outcome"] == "clearance_accepted"
    assert record["retry"] is False
    assert record["lease_id"] == "lease-7741"
    assert record["upstream_fingerprint"] == "0123456789abcdef"
    assert record["latency_ms"] >= 0
    assert record["failed_validation_predicate"] is None


def test_tunnel_then_success_records_one_record_per_attempt(identified_lease, caplog):
    from scrapers.base.flaresolverr_client import FlareSolverrError

    _events, _Lease, Solver = identified_lease
    original_get = Solver.get
    seen = []

    def get(self, url, session_id):
        seen.append(url)
        if len(seen) == 1:
            raise FlareSolverrError(
                "HTTP 500: net::ERR_TUNNEL_CONNECTION_FAILED lease:secret-lease")
        return original_get(self, url, session_id)

    Solver.get = get
    session = _challenge_session({"/Ranking": [_challenge(), FakeResponse(200, b"RAW")]})
    with _transport(session) as transport:
        assert transport.get("/Ranking").body == b"RAW"
    records = _attempt_records(caplog)
    assert [(r["attempt"], r["outcome"], r["retry"]) for r in records] == [
        (1, "tunnel", True), (2, "clearance_accepted", False)]
    assert all(r["lease_id"] == "lease-7741" for r in records)
    assert all(r["upstream_fingerprint"] == "0123456789abcdef" for r in records)
    assert all(r["latency_ms"] >= 0 for r in records)
    assert all(r["failed_validation_predicate"] is None for r in records)
    assert "ERR_TUNNEL_CONNECTION_FAILED" not in caplog.text
    assert "secret-lease" not in caplog.text


@pytest.mark.parametrize("error, outcome", [
    ("FlareSolverrTimeout", "solver_timeout"),
    ("FlareSolverrCFChallengeFailed", "challenge_not_solved"),
    ("FlareSolverrError", "solver_error"),
    ("RuntimeError", "solver_error"),
    ("ValueError", "solver_error"),
])
def test_three_solver_failures_emit_exactly_three_records(
        bootstrap, monkeypatch, caplog, error, outcome):
    from scrapers.base import flaresolverr_client

    _events, _Lease, Solver = bootstrap
    exc_type = getattr(flaresolverr_client, error, None) or getattr(builtins, error)

    def fail(self, url, session_id):
        raise exc_type("SENTINEL-SOLVER-SECRET")

    monkeypatch.setattr(Solver, "get", fail)
    session = _challenge_session({"/Ranking": _challenge()})
    with pytest.raises(ClubEloBlocked), _transport(session) as transport:
        transport.get("/Ranking")
    records = _attempt_records(caplog)
    assert len(records) == 3  # exactly one per attempt: never zero, never six
    assert [(r["attempt"], r["outcome"], r["retry"]) for r in records] == [
        (1, outcome, True), (2, outcome, True), (3, outcome, True)]
    assert all(r["lease_id"] is None for r in records)
    assert all(r["upstream_fingerprint"] is None for r in records)
    assert all(r["latency_ms"] >= 0 for r in records)
    assert all(r["failed_validation_predicate"] is None for r in records)
    assert "SENTINEL-SOLVER-SECRET" not in caplog.text


@pytest.mark.parametrize("solution, predicate", [
    (None, "solution_not_dict"),
    ({}, "status_not_200"),
    ({"status": 403}, "status_not_200"),
    ({"status": 200}, "user_agent_not_string"),
    ({"status": 200, "userAgent": "  "}, "user_agent_blank"),
    ({"status": 200, "userAgent": "SENTINEL-UA\r\ninjected"}, "user_agent_not_printable_ascii"),
    ({"status": 200, "userAgent": "SENTINEL-UA"}, "cookies_not_list"),
    ({"status": 200, "userAgent": "SENTINEL-UA", "cookies": ("cf_clearance",)},
     "cookies_not_list"),
    ({"status": 200, "userAgent": "SENTINEL-UA", "cookies": []}, "cookies_empty"),
    ({"status": 200, "userAgent": "SENTINEL-UA", "cookies": ["SENTINEL-COOKIE"]},
     "cookie_not_mapping"),
    ({"status": 200, "userAgent": "SENTINEL-UA",
      "cookies": [{"name": 42, "value": "SENTINEL-VALUE"}]}, "cookie_name_not_string"),
    ({"status": 200, "userAgent": "SENTINEL-UA",
      "cookies": [{"name": "", "value": "SENTINEL-VALUE"}]}, "cookie_name_empty"),
    ({"status": 200, "userAgent": "SENTINEL-UA", "cookies": [{"name": "cf_clearance"}]},
     "cookie_value_not_string"),
    ({"status": 200, "userAgent": "SENTINEL-UA",
      "cookies": [{"name": "cf_clearance", "value": ""}]}, "cookie_value_empty"),
    ({"status": 200, "userAgent": "SENTINEL-UA",
      "cookies": [{"name": "cf_clearance", "value": "SENTINEL-VALUE",
                   "domain": "SENTINEL-evil.com"}]}, "cookie_domain_not_clubelo"),
    ({"status": 200, "userAgent": "SENTINEL-UA",
      "cookies": [{"name": "cf_clearance", "value": "SENTINEL-VALUE",
                   "path": "/Ranking"}]}, "cookie_path_not_root"),
    ({"status": 200, "userAgent": "SENTINEL-UA",
      "cookies": [{"name": "cf_clearance", "value": "SENTINEL-VALUE,COMMA"}]},
     "cookie_characters_forbidden"),
    ({"status": 200, "userAgent": "SENTINEL-UA",
      "cookies": [{"name": "__cf_bm", "value": "SENTINEL-VALUE"}]},
     "missing_cf_clearance"),
])
def test_invalid_clearance_records_the_exact_failed_predicate(
        bootstrap, caplog, solution, predicate):
    _events, _Lease, Solver = bootstrap
    Solver.solution = solution
    session = _challenge_session({"/Ranking": _challenge()})
    with pytest.raises(ClubEloBlocked, match=r"challenge not solved"), \
            _transport(session) as transport:
        transport.get("/Ranking")
    records = _attempt_records(caplog)
    assert len(records) == 3
    assert all(r["outcome"] == "invalid_clearance" for r in records)
    assert all(r["retry"] is True for r in records)
    assert [r["failed_validation_predicate"] for r in records] == [predicate] * 3
    for sentinel in ("SENTINEL-UA", "SENTINEL-VALUE", "SENTINEL-evil", "SENTINEL-COOKIE"):
        assert sentinel not in caplog.text


def test_lease_refusal_emits_one_non_retryable_record(bootstrap, monkeypatch, caplog):
    _events, Lease, _Solver = bootstrap

    def refused(self):
        raise ValueError("ClubElo gateway refused the lease")

    monkeypatch.setattr(Lease, "acquire", refused)
    session = _challenge_session({"/Ranking": _challenge()})
    with pytest.raises(ClubEloBlocked,
                       match=r"lease: ClubElo gateway refused the lease \(attempt 1/3\)"):
        with _transport(session) as transport:
            transport.get("/Ranking")
    records = _attempt_records(caplog)
    assert len(records) == 1
    record = records[0]
    assert (record["attempt"], record["outcome"], record["retry"]) == \
        (1, "lease_refusal", False)
    assert record["lease_id"] is None
    assert record["upstream_fingerprint"] is None
    assert record["latency_ms"] >= 0
    assert record["failed_validation_predicate"] is None


def test_gateway_lease_error_is_one_lease_error_record(bootstrap, monkeypatch, caplog):
    _events, Lease, _Solver = bootstrap

    def broken(self):
        raise requests.ConnectionError("SENTINEL-CONTROL-TOKEN")

    monkeypatch.setattr(Lease, "acquire", broken)
    session = _challenge_session({"/Ranking": _challenge()})
    with pytest.raises(ClubEloBlocked) as caught:
        with _transport(session) as transport:
            transport.get("/Ranking")
    records = _attempt_records(caplog)
    assert len(records) == 1
    assert records[0]["outcome"] == "lease_error"
    assert records[0]["retry"] is False
    assert "SENTINEL-CONTROL-TOKEN" not in str(caught.value) + caplog.text


def test_attempt_records_redact_solution_and_proxy_secrets(identified_lease, caplog):
    _events, _Lease, Solver = identified_lease
    Solver.solution = {
        "status": 200, "html": "SENTINEL-HTML-BODY",
        "userAgent": "SENTINEL-UA/9.9",
        "cookies": [{"name": "cf_clearance", "value": "SENTINEL-CLEARANCE-VALUE"},
                    {"name": "__cf_bm", "value": "SENTINEL-BM-VALUE",
                     "domain": ".clubelo.com"}],
    }
    session = _challenge_session({"/Ranking": [_challenge(), FakeResponse(200, b"RAW")]})
    with _transport(session) as transport:
        transport.get("/Ranking")
    records = _attempt_records(caplog)
    assert [r["outcome"] for r in records] == ["clearance_accepted"]
    raw = caplog.text + json.dumps(records)
    for sentinel in ("SENTINEL-HTML-BODY", "SENTINEL-UA/9.9", "SENTINEL-CLEARANCE-VALUE",
                     "SENTINEL-BM-VALUE", "secret-lease", "clubelo_proxy_filter:8900"):
        assert sentinel not in raw
    for forbidden in ("html", "cookies", "userAgent", "user_agent", "proxy_url", "token",
                      "exception", "error", "message"):
        assert forbidden not in _ATTEMPT_KEYS


def test_attempt_latency_uses_the_injected_monotonic_clock(identified_lease, caplog):
    _events, _Lease, Solver = identified_lease
    clock = Clock()
    original_get = Solver.get

    def get(self, url, session_id):
        clock.now += 0.25
        return original_get(self, url, session_id)

    Solver.get = get
    session = _challenge_session({"/Ranking": [_challenge(), FakeResponse(200, b"RAW")]})
    with _transport(session, clock=clock) as transport:
        transport.get("/Ranking")
    records = _attempt_records(caplog)
    assert records[0]["latency_ms"] == 250


@pytest.mark.parametrize("fingerprint", [
    "NOT-A-FINGERPRINT", "ABCDEF0123456789", "0123456789abcde", "0123456789abcdef0",
    42, None,
])
def test_attempt_record_sanitizes_non_fingerprint_values(bootstrap, caplog, fingerprint):
    _events, Lease, _Solver = bootstrap
    Lease.lease_id = "lease-7741"
    Lease.upstream_fingerprint = fingerprint
    session = _challenge_session({"/Ranking": [_challenge(), FakeResponse(200, b"RAW")]})
    with _transport(session) as transport:
        transport.get("/Ranking")
    records = _attempt_records(caplog)
    assert len(records) == 1
    assert records[0]["upstream_fingerprint"] is None
    assert records[0]["lease_id"] == "lease-7741"
