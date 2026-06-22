"""Unit tests for the SofaScore Camoufox capture transport pure helpers (#757).

The browser orchestration (``SofascoreCamoufoxCapture.capture_event``) is live
integration — covered by ``scripts/research/probe_sofascore_capture.py``. Here we
test the pure response-classification / selection logic that decides what counts
as real data, which has no browser dependency.
"""
import pytest

from scrapers.sofascore.camoufox_capture import (
    event_url,
    extract_events,
    finished_event_ids,
    is_challenge,
    is_data_api_url,
    normalize_event,
    parse_proxy_line,
    response_path,
    select_event_endpoints,
)

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------- #
#  response_path                                                              #
# --------------------------------------------------------------------------- #
def test_response_path_strips_host_and_query():
    # Arrange
    url = "https://www.sofascore.com/api/v1/event/123/lineups?foo=bar"
    # Act / Assert
    assert response_path(url) == "/api/v1/event/123/lineups"


def test_response_path_handles_api_host():
    assert response_path("https://api.sofascore.com/api/v1/unique-tournament/17/seasons") == \
        "/api/v1/unique-tournament/17/seasons"


# --------------------------------------------------------------------------- #
#  is_data_api_url                                                            #
# --------------------------------------------------------------------------- #
def test_is_data_api_url_true_for_same_origin_data():
    assert is_data_api_url("https://www.sofascore.com/api/v1/event/123/statistics") is True


def test_is_data_api_url_true_for_public_api_host():
    # host-agnostic: the public host counts too
    assert is_data_api_url("https://api.sofascore.com/api/v1/unique-tournament/17/seasons") is True


@pytest.mark.parametrize("url", [
    "https://www.sofascore.com/api/v1/team/4724/image",
    "https://www.sofascore.com/api/v1/country/US/flag",
    "https://www.sofascore.com/api/v1/odds/provider/760/logo",
    "https://www.sofascore.com/api/v1/event/123/jersey/home/player/clean",
])
def test_is_data_api_url_false_for_binary_endpoints(url):
    assert is_data_api_url(url) is False


def test_is_data_api_url_false_for_non_sofascore():
    assert is_data_api_url("https://challenges.cloudflare.com/api/v1/foo") is False


def test_is_data_api_url_false_without_api_path():
    assert is_data_api_url("https://www.sofascore.com/football/match/x/y") is False


# --------------------------------------------------------------------------- #
#  is_challenge                                                               #
# --------------------------------------------------------------------------- #
def test_is_challenge_true_for_challenge_body():
    assert is_challenge({"error": {"code": 403, "reason": "challenge"}}) is True


def test_is_challenge_false_for_real_data():
    assert is_challenge({"home": {"players": []}, "away": {}}) is False


def test_is_challenge_false_for_non_dict():
    assert is_challenge(None) is False
    assert is_challenge("nope") is False


# --------------------------------------------------------------------------- #
#  event_url                                                                  #
# --------------------------------------------------------------------------- #
def test_event_url_builds_www_event_path():
    assert event_url(15186878) == "https://www.sofascore.com/event/15186878"


# --------------------------------------------------------------------------- #
#  select_event_endpoints                                                     #
# --------------------------------------------------------------------------- #
def test_select_event_endpoints_returns_only_real_json():
    # Arrange — a mixed buffer like a real capture: lineups real, statistics
    # challenge-403, shotmap missing, event real, incidents non-200.
    eid = 999
    buffer = {
        f"/api/v1/event/{eid}": {"status": 200, "json": {"event": {"id": eid}}, "challenge": False},
        f"/api/v1/event/{eid}/lineups": {"status": 200, "json": {"home": {}, "away": {}}, "challenge": False},
        f"/api/v1/event/{eid}/statistics": {"status": 403, "json": {"error": {"reason": "challenge"}}, "challenge": True},
        f"/api/v1/event/{eid}/incidents": {"status": 304, "json": None, "challenge": None},
        # shotmap absent from buffer entirely
    }
    # Act
    out = select_event_endpoints(buffer, eid)
    # Assert — only the two real endpoints, keyed by entity name
    assert set(out) == {"event", "lineups"}
    assert out["lineups"] == {"home": {}, "away": {}}
    assert out["event"] == {"event": {"id": eid}}


def test_select_event_endpoints_empty_when_all_challenged():
    eid = 1
    buffer = {
        f"/api/v1/event/{eid}/lineups": {"status": 403, "json": {"error": {"reason": "challenge"}}, "challenge": True},
    }
    assert select_event_endpoints(buffer, eid) == {}


# --------------------------------------------------------------------------- #
#  parse_proxy_line                                                           #
# --------------------------------------------------------------------------- #
def test_parse_proxy_line_valid():
    assert parse_proxy_line("1.2.3.4:10000:user:pass") == {
        "server": "http://1.2.3.4:10000", "username": "user", "password": "pass",
    }


def test_parse_proxy_line_password_with_colons():
    out = parse_proxy_line("host:8080:u:p:a:s:s")
    assert out["password"] == "p:a:s:s"
    assert out["server"] == "http://host:8080"


@pytest.mark.parametrize("line", ["", "   ", "# comment", "host:port:only-three"])
def test_parse_proxy_line_rejects_bad_lines(line):
    assert parse_proxy_line(line) is None


# --------------------------------------------------------------------------- #
#  events parsing (schedule + event-id resolution)                           #
# --------------------------------------------------------------------------- #
def _events_buffer():
    """A capture buffer with one events/last page (finished + scheduled) plus a
    challenged page and an unrelated entry — mirrors a real capture."""
    return {
        "/api/v1/unique-tournament/17/season/76986/events/last/0": {
            "status": 200, "challenge": False, "json": {"events": [
                {"id": 1, "status": {"type": "finished"},
                 "homeTeam": {"name": "A"}, "awayTeam": {"name": "B"},
                 "homeScore": {"current": 2}, "awayScore": {"current": 1},
                 "startTimestamp": 1719000000},
                {"id": 2, "status": {"type": "notstarted"},
                 "homeTeam": {"name": "C"}, "awayTeam": {"name": "D"}},
            ]},
        },
        # duplicate id 1 from the next page — must dedupe
        "/api/v1/unique-tournament/17/season/76986/events/next/0": {
            "status": 200, "challenge": False, "json": {"events": [
                {"id": 1, "status": {"type": "finished"}, "homeTeam": {"name": "A"}},
                {"id": 3, "status": {"type": "inprogress"}},
            ]},
        },
        # challenged events page — must be ignored
        "/api/v1/unique-tournament/99/season/1/events/last/0": {
            "status": 403, "challenge": True, "json": {"error": {"reason": "challenge"}},
        },
        # unrelated entry
        "/api/v1/event/1/lineups": {"status": 200, "challenge": False, "json": {"home": {}}},
    }


def test_extract_events_dedupes_across_pages_and_skips_challenged():
    events = extract_events(_events_buffer())
    ids = sorted(e["id"] for e in events)
    assert ids == [1, 2, 3]  # deduped, challenged page skipped


def test_finished_event_ids_only_finished():
    events = extract_events(_events_buffer())
    assert finished_event_ids(events) == ["1"]


def test_normalize_event_flattens_row():
    ev = {
        "id": 15186878, "status": {"type": "finished"},
        "homeTeam": {"name": "USA"}, "awayTeam": {"name": "Australia"},
        "homeScore": {"current": 0}, "awayScore": {"current": 2},
        "startTimestamp": 1719000000,
    }
    assert normalize_event(ev) == {
        "event_id": "15186878", "status": "finished", "start_timestamp": 1719000000,
        "home_team": "USA", "away_team": "Australia", "home_score": 0, "away_score": 2,
    }
