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
    extract_player_from_next_data,
    finished_event_ids,
    is_challenge,
    is_data_api_url,
    merge_capture,
    normalize_event,
    parse_proxy_line,
    player_url,
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


# --------------------------------------------------------------------------- #
#  extract_tournament_events (ut_id-filtered schedule capture — #757 B1)      #
# --------------------------------------------------------------------------- #
def _tournament_buffer():
    """A league-page capture buffer mirroring the #757 B0 spike: the target
    ut=17 schedule arrives via /events/{round,last}, AND a FEATURED other
    tournament (ut=16) fires its own /events/last|next that must be ignored."""
    return {
        "/api/v1/unique-tournament/17/season/96668/events/round/1": {
            "status": 200, "challenge": False, "json": {"events": [
                {"id": 101, "status": {"type": "finished"}},
                {"id": 102, "status": {"type": "notstarted"}},
            ]}},
        # last page (finished) — id 101 overlaps round/1 and must dedupe.
        "/api/v1/unique-tournament/17/season/96668/events/last/0": {
            "status": 200, "challenge": False, "json": {"events": [
                {"id": 101, "status": {"type": "finished"}},
                {"id": 103, "status": {"type": "finished"}},
            ]}},
        # FEATURED other tournament — MUST be excluded (the B0 wrong-ut bug).
        "/api/v1/unique-tournament/16/season/58210/events/last/0": {
            "status": 200, "challenge": False, "json": {"events": [
                {"id": 901, "status": {"type": "finished"}},
            ]}},
        # challenged target path — ignored.
        "/api/v1/unique-tournament/17/season/96668/events/next/0": {
            "status": 403, "challenge": True, "json": {"error": {"reason": "challenge"}}},
    }


def test_extract_tournament_events_filters_by_ut_id_and_dedupes():
    from scrapers.sofascore.camoufox_capture import extract_tournament_events
    events = extract_tournament_events(_tournament_buffer(), 17)
    ids = sorted(e["id"] for e in events)
    # 101 deduped across round+last; 901 (ut=16 featured) excluded; challenged out.
    assert ids == [101, 102, 103]


def test_extract_tournament_events_feeds_finished_event_ids():
    from scrapers.sofascore.camoufox_capture import extract_tournament_events
    events = extract_tournament_events(_tournament_buffer(), 17)
    assert finished_event_ids(events) == ["101", "103"]


def test_extract_tournament_events_empty_for_absent_ut():
    from scrapers.sofascore.camoufox_capture import extract_tournament_events
    assert extract_tournament_events(_tournament_buffer(), 8) == []


# --------------------------------------------------------------------------- #
#  capture_event retry (lineups reliability — #757)                           #
# --------------------------------------------------------------------------- #
class TestCaptureEventRetry:
    """capture_event re-navigates when a required endpoint (default lineups)
    is missing — the live runner saw ~1/2 lineup misses without retry (a tab
    XHR or its body-read races). We patch _navigate/_click_tabs so no browser
    is needed and drive the buffer state per attempt.
    """

    def _cap(self):
        from unittest.mock import MagicMock
        from scrapers.sofascore.camoufox_capture import SofascoreCamoufoxCapture
        cap = SofascoreCamoufoxCapture(proxy=None)
        cap._page = MagicMock()  # only wait_for_timeout is touched
        return cap

    def test_retries_until_lineups_present(self):
        # Arrange — attempt 1 captures only the event; attempt 2 adds lineups.
        cap = self._cap()
        eid = 555
        states = [
            {f"/api/v1/event/{eid}": {"status": 200, "json": {"event": 1}, "challenge": False}},
            {f"/api/v1/event/{eid}": {"status": 200, "json": {"event": 1}, "challenge": False},
             f"/api/v1/event/{eid}/lineups": {"status": 200, "json": {"home": {}, "away": {}}, "challenge": False}},
        ]
        calls = {"n": 0}

        def fake_navigate(url, extra_settle_ms=0):
            cap._buffer = states[calls["n"]]
            calls["n"] += 1

        cap._navigate = fake_navigate
        cap._click_tabs = lambda *a, **k: None

        # Act
        result = cap.capture_event(eid, required=("lineups",), max_attempts=3)

        # Assert — retried exactly once, lineups recovered.
        assert calls["n"] == 2
        assert "lineups" in result
        assert "event" in result

    def test_stops_after_max_attempts_when_lineups_never_arrive(self):
        # Arrange — lineups never show up; event always does.
        cap = self._cap()
        eid = 777
        calls = {"n": 0}

        def fake_navigate(url, extra_settle_ms=0):
            cap._buffer = {
                f"/api/v1/event/{eid}": {"status": 200, "json": {"e": 1}, "challenge": False},
            }
            calls["n"] += 1

        cap._navigate = fake_navigate
        cap._click_tabs = lambda *a, **k: None

        # Act
        result = cap.capture_event(eid, required=("lineups",), max_attempts=3)

        # Assert — exhausted attempts, returns whatever was captured (no raise).
        assert calls["n"] == 3
        assert "lineups" not in result
        assert "event" in result

    def test_no_retry_when_required_empty(self):
        # Arrange — required=() means "take one pass, don't retry".
        cap = self._cap()
        eid = 999
        calls = {"n": 0}

        def fake_navigate(url, extra_settle_ms=0):
            cap._buffer = {
                f"/api/v1/event/{eid}": {"status": 200, "json": {"e": 1}, "challenge": False},
            }
            calls["n"] += 1

        cap._navigate = fake_navigate
        cap._click_tabs = lambda *a, **k: None

        # Act
        cap.capture_event(eid, required=(), max_attempts=3)

        # Assert — single navigation, no retry loop.
        assert calls["n"] == 1


# --------------------------------------------------------------------------- #
#  merge_capture — 304/no-body must not clobber a good capture (#751 PR2)      #
# --------------------------------------------------------------------------- #
def test_merge_capture_takes_new_when_no_existing():
    new = {"status": 200, "json": {"a": 1}, "challenge": False}
    assert merge_capture(None, new) is new


def test_merge_capture_keeps_good_json_over_later_none():
    # Arrange — a good 200+JSON already captured, then a 304/no-body arrives.
    good = {"status": 200, "json": {"statistics": []}, "challenge": False}
    empty = {"status": 304, "json": None, "challenge": None}

    # Act + Assert — the good record survives the 304 clobber.
    assert merge_capture(good, empty) is good


def test_merge_capture_upgrades_none_to_good_json():
    # Arrange — a body-read race stored json=None first, then the real body.
    raced = {"status": 200, "json": None, "challenge": None}
    good = {"status": 200, "json": {"shotmap": []}, "challenge": False}

    # Act + Assert — the real body replaces the raced empty.
    assert merge_capture(raced, good) is good


def test_merge_capture_replaces_good_with_newer_good():
    old = {"status": 200, "json": {"v": 1}, "challenge": False}
    new = {"status": 200, "json": {"v": 2}, "challenge": False}
    assert merge_capture(old, new) is new


# --------------------------------------------------------------------------- #
#  _on_response wiring + capture_event accumulation across retries (#751 PR2)  #
# --------------------------------------------------------------------------- #
class _FakeResp:
    """Minimal Playwright Response stand-in: _on_response touches url/status/json."""

    def __init__(self, url, status, json_obj, raise_json=False):
        self.url = url
        self.status = status
        self._json = json_obj
        self._raise = raise_json

    def json(self):
        if self._raise:
            raise RuntimeError("body unavailable")
        return self._json


def _cap_no_browser():
    from scrapers.sofascore.camoufox_capture import SofascoreCamoufoxCapture
    cap = SofascoreCamoufoxCapture(proxy=None)
    cap._buffer = {}
    return cap


def test_on_response_does_not_clobber_good_capture_with_304():
    # Arrange — a good statistics body lands, then a 304 re-fetch of the same URL.
    cap = _cap_no_browser()
    url = "https://www.sofascore.com/api/v1/event/42/statistics"
    cap._on_response(_FakeResp(url, 200, {"statistics": [{"period": "ALL"}]}))
    cap._on_response(_FakeResp(url, 304, None, raise_json=True))

    # Assert — the buffer still holds the good JSON, not the empty 304.
    rec = cap._buffer["/api/v1/event/42/statistics"]
    assert rec["json"] == {"statistics": [{"period": "ALL"}]}
    assert rec["status"] == 200


def test_click_tabs_clicks_statistics_by_testid():
    # The Statistics tab must be clicked by its stable data-testid (fires both
    # /statistics and /shotmap); get_by_text grabbed a non-tab node (#751 PR2).
    from unittest.mock import MagicMock
    cap = _cap_no_browser()
    cap._page = MagicMock()
    cap._page.evaluate.return_value = True

    cap._click_tabs(("Statistics",))

    # evaluate invoked with the tab-statistics testid; text fallback NOT used.
    assert cap._page.evaluate.call_args[0][1] == "tab-statistics"
    cap._page.get_by_text.assert_not_called()


def test_click_tabs_falls_back_to_text_without_testid():
    # A tab with no known testid (Player statistics) uses the get_by_text path.
    from unittest.mock import MagicMock
    cap = _cap_no_browser()
    cap._page = MagicMock()

    cap._click_tabs(("Player statistics",))

    cap._page.get_by_text.assert_called_once()
    assert cap._page.get_by_text.call_args[0][0] == "Player statistics"


def test_capture_event_accumulates_statistics_across_retry():
    """The retry that recovers a missing `lineups` must NOT lose a `statistics`
    captured on an earlier pass (re-nav serves it as a 304/no-body). Pre-PR2
    this regressed because the buffer was reset per attempt — RED then."""
    from unittest.mock import MagicMock
    cap = _cap_no_browser()
    cap._page = MagicMock()  # only wait_for_timeout is touched
    eid = 42
    stats_url = f"https://www.sofascore.com/api/v1/event/{eid}/statistics"
    lineups_url = f"https://www.sofascore.com/api/v1/event/{eid}/lineups"
    calls = {"n": 0}

    def fake_navigate(url, extra_settle_ms=0):
        # Attempt 1: statistics arrives, lineups does NOT.
        # Attempt 2: lineups arrives; statistics re-served as a 304/no-body.
        if calls["n"] == 0:
            cap._on_response(_FakeResp(stats_url, 200, {"statistics": [{"period": "ALL"}]}))
        else:
            cap._on_response(_FakeResp(lineups_url, 200, {"home": {}, "away": {}}))
            cap._on_response(_FakeResp(stats_url, 304, None, raise_json=True))
        calls["n"] += 1

    cap._navigate = fake_navigate
    cap._click_tabs = lambda *a, **k: None

    # Act — require lineups so attempt 1 (no lineups) forces a retry.
    result = cap.capture_event(eid, required=("lineups",), max_attempts=3)

    # Assert — retried once, BOTH endpoints present (statistics survived).
    assert calls["n"] == 2
    assert "lineups" in result
    assert "statistics" in result
    assert result["statistics"] == {"statistics": [{"period": "ALL"}]}


# --------------------------------------------------------------------------- #
#  Per-player helpers (#751 PR3) — profile snapshot                           #
#  Shapes mirror the live probe (scripts/research/probe_sofascore_player.py).  #
# --------------------------------------------------------------------------- #
def test_player_url_uses_dummy_slug():
    assert player_url(1416535) == "https://www.sofascore.com/player/x/1416535"


def _next_data(pid=1416535):
    # bio at props.pageProps.player with the fields _flatten_player_profile reads.
    return {
        "props": {"pageProps": {"player": {
            "id": pid, "name": "Charalampos Kostoulas",
            "slug": "kostoulas-charalampos", "position": "F",
            "height": 185, "preferredFoot": "Right",
            "dateOfBirthTimestamp": 1180483200,
            "team": {"id": 30, "name": "Brighton & Hove Albion"},
        }}}
    }


def test_extract_player_from_next_data_digs_pageprops_player():
    player = extract_player_from_next_data(_next_data(1416535), 1416535)
    assert player is not None
    assert player["name"] == "Charalampos Kostoulas"
    assert player["height"] == 185


def test_extract_player_from_next_data_matches_str_or_int_id():
    assert extract_player_from_next_data(_next_data(1416535), "1416535") is not None


def test_extract_player_from_next_data_none_on_id_mismatch():
    # A wrong-page SSR (different player) must not yield a mismatched row.
    assert extract_player_from_next_data(_next_data(999), 1416535) is None


@pytest.mark.parametrize("nd", [None, {}, {"props": {}}, {"props": {"pageProps": {}}}])
def test_extract_player_from_next_data_none_on_missing(nd):
    assert extract_player_from_next_data(nd, 1416535) is None
