"""Unit tests for the SofaScore Camoufox capture transport pure helpers (#757).

The browser orchestration (``SofascoreCamoufoxCapture.capture_event``) is live
integration — covered by ``scripts/research/probe_sofascore_capture.py``. Here we
test the pure response-classification / selection logic that decides what counts
as real data, which has no browser dependency.
"""
import json

import pytest

from scrapers.sofascore.camoufox_capture import (
    event_url,
    extract_events,
    extract_player_from_next_data,
    fetch_names_for_tabs,
    finished_event_ids,
    is_challenge,
    is_data_api_url,
    merge_capture,
    normalize_event,
    parse_proxy_line,
    player_url,
    response_path,
    select_event_endpoints,
    should_block_request,
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
    # #840: auto-passthrough — source-key names, nested objects flatten with a
    # path prefix (homeTeam.name -> home_team_name). Silver renames/derives.
    ev = {
        "id": 15186878, "status": {"type": "finished"},
        "homeTeam": {"name": "USA"}, "awayTeam": {"name": "Australia"},
        "homeScore": {"current": 0}, "awayScore": {"current": 2},
        "startTimestamp": 1719000000,
        "roundInfo": {"round": 7},
    }
    row = normalize_event(ev)
    assert row["game_id"] == 15186878
    assert row["start_timestamp"] == 1719000000       # raw epoch (was `date`)
    assert row["home_team_name"] == "USA"
    assert row["away_team_name"] == "Australia"
    assert row["home_score_current"] == 0
    assert row["away_score_current"] == 2
    assert row["round_info_round"] == 7
    assert row["status_type"] == "finished"           # passthrough bonus
    # Old renamed/derived names + dropped soccerdata placeholders are gone.
    for dead in ("date", "home_team", "away_team", "home_score", "away_score",
                 "round", "week", "game"):
        assert dead not in row


def test_normalize_event_missing_round_and_scores():
    # A not-started fixture: no roundInfo, scores absent → those columns absent
    # (#840: absent source key -> absent column, not a None placeholder).
    ev = {
        "id": 999, "status": {"type": "notstarted"},
        "homeTeam": {"name": "Foo"}, "awayTeam": {"name": "Bar"},
        "startTimestamp": 1720000000,
    }
    row = normalize_event(ev)
    assert row["game_id"] == 999
    assert row["home_team_name"] == "Foo"
    assert row["start_timestamp"] == 1720000000
    assert "round_info_round" not in row
    assert "home_score_current" not in row and "away_score_current" not in row


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
#  In-page fetch (#842) — fetch_names_for_tabs + fetch_event                   #
# --------------------------------------------------------------------------- #
def test_fetch_names_for_tabs_all_event_tabs():
    names = fetch_names_for_tabs(
        ("Lineups", "Statistics", "Player statistics", "Shotmap"))
    assert names == ("event", "lineups", "statistics", "shotmap")


def test_fetch_names_for_tabs_lineups_only_and_unknown():
    # event + lineups always ride along; unknown tab labels are ignored.
    assert fetch_names_for_tabs(("Lineups",)) == ("event", "lineups")
    assert fetch_names_for_tabs(("Season",)) == ("event", "lineups")
    assert fetch_names_for_tabs(()) == ("event", "lineups")


class TestFetchEvent:
    """fetch_event (#842): same-origin in-page fetch of the per-event JSON
    endpoints on the already-navigated page — no re-navigation. Bodies are
    parsed in Python (no settle-window race) and selected via
    select_event_endpoints; a challenged/failed endpoint stays missing so the
    caller can fall back to a full capture_event navigation."""

    def _cap_with_page(self, responder):
        from unittest.mock import MagicMock
        cap = _cap_no_browser()
        cap._page = MagicMock()
        cap._page.evaluate.side_effect = lambda js, path=None: responder(path)
        return cap

    def test_fetches_named_endpoints_and_selects_json(self):
        eid = 42
        bodies = {
            f"/api/v1/event/{eid}": {"event": {"id": eid}},
            f"/api/v1/event/{eid}/lineups": {"home": {}, "away": {}},
            f"/api/v1/event/{eid}/statistics": {"statistics": []},
            f"/api/v1/event/{eid}/shotmap": {"shotmap": []},
        }
        cap = self._cap_with_page(
            lambda path: {"status": 200, "body": json.dumps(bodies[path])})

        result = cap.fetch_event(
            eid, names=("event", "lineups", "statistics", "shotmap"),
            fetch_wait_ms=0,
        )

        assert set(result) == {"event", "lineups", "statistics", "shotmap"}
        assert result["lineups"] == {"home": {}, "away": {}}
        # No navigation happened — only in-page evaluate calls.
        cap._page.goto.assert_not_called()

    def test_challenged_endpoint_stays_missing(self):
        # Clearance expired mid-session: the API starts answering 403 challenge.
        eid = 7

        def responder(path):
            if path.endswith("/lineups"):
                return {"status": 403,
                        "body": json.dumps({"error": {"reason": "challenge"}})}
            return {"status": 200, "body": json.dumps({"ok": 1})}

        cap = self._cap_with_page(responder)
        result = cap.fetch_event(eid, names=("event", "lineups"), fetch_wait_ms=0)

        assert "lineups" not in result   # caller will fall back to a full nav
        assert "event" in result

    def test_evaluate_error_yields_missing_not_raise(self):
        def responder(path):
            raise RuntimeError("page gone")

        cap = self._cap_with_page(responder)
        result = cap.fetch_event(9, names=("event", "lineups"), fetch_wait_ms=0)
        assert result == {}

    def test_non_json_body_stays_missing(self):
        cap = self._cap_with_page(
            lambda path: {"status": 200, "body": "<html>cf challenge</html>"})
        result = cap.fetch_event(5, names=("lineups",), fetch_wait_ms=0)
        assert result == {}


class TestFetchPlayer:
    """fetch_player (#842): in-page fetch of the player bio + season stats —
    no navigation. /api/v1/player/{id} SSRs the identical `player` object into
    __NEXT_DATA__ (live-probed 2026-07-02), and the target (ut, season_id) is
    resolved from /statistics/seasons in Python instead of the picker."""

    _PID = "924378"
    _PLAYER = {"id": 924378, "name": "Sepp van den Berg", "height": 192}
    _SEASONS = {"uniqueTournamentSeasons": [{
        "uniqueTournament": {"id": 17},
        "seasons": [{"year": "25/26", "id": 76986},
                    {"year": "24/25", "id": 61627}],
    }]}
    _OVERALL = {"statistics": {"rating": 7.1, "appearances": 30}}

    def _cap_with_page(self, responder):
        from unittest.mock import MagicMock
        cap = _cap_no_browser()
        cap._page = MagicMock()
        cap._page.evaluate.side_effect = lambda js, path=None: responder(path)
        return cap

    def _responder(self, path):
        bodies = {
            f"/api/v1/player/{self._PID}": {"player": self._PLAYER},
            f"/api/v1/player/{self._PID}/statistics/seasons": self._SEASONS,
            (f"/api/v1/player/{self._PID}/unique-tournament/17"
             f"/season/76986/statistics/overall"): self._OVERALL,
        }
        return {"status": 200, "body": json.dumps(bodies[path])}

    def test_profile_and_season_stats_fetched(self):
        cap = self._cap_with_page(self._responder)
        out = cap.fetch_player(self._PID, target_ut=17, target_year="25/26",
                               fetch_wait_ms=0)
        assert out["profile"] == self._PLAYER
        # seasons list + the EXACT (ut=17, sid=76986) overall — resolved in
        # Python, no Season-tab picker involved.
        assert set(out["season_buffer"]) == {
            f"/api/v1/player/{self._PID}/statistics/seasons",
            (f"/api/v1/player/{self._PID}/unique-tournament/17"
             f"/season/76986/statistics/overall"),
        }
        # The overall record carries real JSON, ready for
        # select_player_season_stats downstream.
        overall = out["season_buffer"][
            (f"/api/v1/player/{self._PID}/unique-tournament/17"
             f"/season/76986/statistics/overall")]
        assert overall["json"] == self._OVERALL

    def test_challenged_bio_returns_profile_none(self):
        cap = self._cap_with_page(lambda path: {
            "status": 403,
            "body": json.dumps({"error": {"reason": "challenge"}}),
        })
        out = cap.fetch_player(self._PID, target_ut=17, target_year="25/26",
                               fetch_wait_ms=0)
        assert out["profile"] is None       # caller falls back to a full nav
        assert out["season_buffer"] == {}

    def test_no_target_ut_fetches_bio_only(self):
        cap = self._cap_with_page(self._responder)
        out = cap.fetch_player(self._PID, fetch_wait_ms=0)
        assert out["profile"] == self._PLAYER
        assert out["season_buffer"] == {}
        assert cap._page.evaluate.call_count == 1

    def test_unresolved_season_skips_overall_fetch(self):
        # The player never played the target competition/season — the seasons
        # list has no (ut, year) match, so no overall fetch fires.
        cap = self._cap_with_page(self._responder)
        out = cap.fetch_player(self._PID, target_ut=17, target_year="19/20",
                               fetch_wait_ms=0)
        assert out["profile"] == self._PLAYER
        assert set(out["season_buffer"]) == {
            f"/api/v1/player/{self._PID}/statistics/seasons"}
        assert cap._page.evaluate.call_count == 2


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


# --------------------------------------------------------------------------- #
#  Per-player SEASON STATS helpers (#751 PR3b) — season-tab picker selection   #
#  Shapes mirror the live probe (scripts/research/probe_sofascore_player.py,   #
#  FACT 2/4b): the player page captures /statistics/seasons (year→id map) plus #
#  one /unique-tournament/{ut}/season/{sid}/statistics/overall per tab/picker. #
# --------------------------------------------------------------------------- #
def test_season_short_to_label_maps_four_digit():
    from scrapers.sofascore.camoufox_capture import season_short_to_label
    assert season_short_to_label("2526") == "25/26"
    assert season_short_to_label("2122") == "21/22"


def test_season_short_to_label_passthrough_non_four_digit():
    from scrapers.sofascore.camoufox_capture import season_short_to_label
    # already a label / unknown token — returned unchanged
    assert season_short_to_label("25/26") == "25/26"
    assert season_short_to_label("2026") == "20/26"  # 4-digit is always split


def _seasons_payload():
    """/statistics/seasons shape (probe FACT 2): per-unique-tournament year→id."""
    return {
        "uniqueTournamentSeasons": [
            {"uniqueTournament": {"id": 17, "name": "Premier League"},
             "seasons": [{"year": "25/26", "id": 76986},
                         {"year": "24/25", "id": 61627}]},
            {"uniqueTournament": {"id": 16, "name": "World Cup"},
             "seasons": [{"year": "2026", "id": 58210}]},
        ]
    }


def _player_season_buffer(pid=839981):
    """A transferred player's season-tab capture: the default tab fired the
    World Cup (ut=16) overall, then the picker selected EPL (ut=17) overall.
    Plus the /statistics/seasons map and a challenged stray to be ignored."""
    return {
        f"/api/v1/player/{pid}/statistics/seasons": {
            "status": 200, "challenge": False, "json": _seasons_payload()},
        f"/api/v1/player/{pid}/unique-tournament/16/season/58210/statistics/overall": {
            "status": 200, "challenge": False,
            "json": {"team": {"id": 9, "name": "Brazil"},
                     "statistics": {"rating": 7.1, "totalGoals": 1}}},
        f"/api/v1/player/{pid}/unique-tournament/17/season/76986/statistics/overall": {
            "status": 200, "challenge": False,
            "json": {"team": {"id": 30, "name": "West Ham"},
                     "statistics": {"rating": 6.8, "totalGoals": 4}}},
        # challenged stray for ANOTHER player — must never be selected.
        "/api/v1/player/111/unique-tournament/17/season/76986/statistics/overall": {
            "status": 403, "challenge": True, "json": {"error": {"reason": "challenge"}}},
    }


def test_extract_player_seasons_map_builds_ut_year_to_id():
    from scrapers.sofascore.camoufox_capture import extract_player_seasons_map
    m = extract_player_seasons_map(_player_season_buffer(839981), 839981)
    assert m[17]["25/26"] == 76986
    assert m[17]["24/25"] == 61627
    assert m[16]["2026"] == 58210


def test_extract_player_seasons_map_empty_when_not_captured():
    from scrapers.sofascore.camoufox_capture import extract_player_seasons_map
    assert extract_player_seasons_map({}, 839981) == {}


# --------------------------------------------------------------------------- #
#  league standings parsers — live-proven shapes (EPL 24/25, #779)            #
#  Fixtures mirror the REAL capture taken 2026-06-23 (sid 61627): the         #
#  /standings/total 'total' block + the /unique-tournament/{ut}/seasons map.  #
# --------------------------------------------------------------------------- #
def _tournament_seasons_payload():
    """Real /unique-tournament/17/seasons response (trimmed, #779). NOTE: this
    endpoint does NOT fire on the standings landing — only when the matches
    widget is nudged — so read_league_table falls back to the events."""
    return {"seasons": [
        {"year": "26/27", "id": 96668},
        {"year": "25/26", "id": 76986},
        {"year": "24/25", "id": 61627},
        {"year": "23/24", "id": 52186},
    ]}


def _standings_buffer(sid=61627):
    """Real EPL 24/25 standings/total capture (#779), trimmed to champion,
    runner-up, bottom. The 'home' block is challenged → must be skipped."""
    return {
        f"/api/v1/unique-tournament/17/season/{sid}/standings/total": {
            "status": 200, "challenge": False, "json": {"standings": [
                {"type": "total", "rows": [
                    {"team": {"name": "Liverpool FC", "id": 44, "shortName": "Liverpool"},
                     "matches": 38, "wins": 25, "draws": 9, "losses": 4,
                     "scoresFor": 86, "scoresAgainst": 41, "points": 84, "position": 1},
                    {"team": {"name": "Arsenal", "id": 42, "shortName": "Arsenal"},
                     "matches": 38, "wins": 20, "draws": 14, "losses": 4,
                     "scoresFor": 69, "scoresAgainst": 34, "points": 74, "position": 2},
                    {"team": {"name": "Southampton", "id": 45, "shortName": "Southampton"},
                     "matches": 38, "wins": 2, "draws": 6, "losses": 30,
                     "scoresFor": 26, "scoresAgainst": 86, "points": 12, "position": 20},
                ]},
            ]}},
        f"/api/v1/unique-tournament/17/season/{sid}/standings/home": {
            "status": 403, "challenge": True,
            "json": {"error": {"reason": "challenge"}}},
    }


def test_extract_tournament_seasons_map_real_epl_payload():
    from scrapers.sofascore.camoufox_capture import extract_tournament_seasons_map
    buf = {"/api/v1/unique-tournament/17/seasons": {
        "status": 200, "challenge": False, "json": _tournament_seasons_payload()}}
    m = extract_tournament_seasons_map(buf, 17)
    assert m["24/25"] == 61627
    assert m["25/26"] == 76986


def test_extract_tournament_seasons_map_empty_when_absent():
    from scrapers.sofascore.camoufox_capture import extract_tournament_seasons_map
    # live FACT (#779): /seasons does not fire on the standings landing.
    assert extract_tournament_seasons_map({}, 17) == {}


def test_extract_tournament_standings_returns_total_block_rows():
    from scrapers.sofascore.camoufox_capture import extract_tournament_standings
    rows = extract_tournament_standings(_standings_buffer(), 17, 61627)
    assert [r["team"]["name"] for r in rows] == [
        "Liverpool FC", "Arsenal", "Southampton"]


def test_extract_tournament_standings_empty_for_wrong_sid():
    from scrapers.sofascore.camoufox_capture import extract_tournament_standings
    # off-season guard: the page may serve a DIFFERENT sid's standings.
    assert extract_tournament_standings(_standings_buffer(sid=96668), 17, 61627) == []


def test_normalize_standing_flattens_real_liverpool_row():
    from scrapers.sofascore.camoufox_capture import (
        extract_tournament_standings, normalize_standing)
    rows = extract_tournament_standings(_standings_buffer(), 17, 61627)
    assert normalize_standing(rows[0]) == {
        "team": "Liverpool FC", "mp": 38, "w": 25, "d": 9, "l": 4,
        "gf": 86, "ga": 41, "gd": 45, "pts": 84}


def test_normalize_standing_invariants_hold_on_all_rows():
    from scrapers.sofascore.camoufox_capture import (
        extract_tournament_standings, normalize_standing)
    rows = [normalize_standing(r)
            for r in extract_tournament_standings(_standings_buffer(), 17, 61627)]
    # gd is derived (the API row has none); pts = 3W + D for every real row.
    assert all(r["gd"] == r["gf"] - r["ga"] for r in rows)
    assert all(r["pts"] == 3 * r["w"] + r["d"] for r in rows)


def test_normalize_standing_gd_none_when_scores_missing():
    from scrapers.sofascore.camoufox_capture import normalize_standing
    out = normalize_standing({"team": {"name": "X"}, "points": 0})
    assert out["gd"] is None and out["gf"] is None


def test_extract_player_seasons_map_empty_on_challenge():
    from scrapers.sofascore.camoufox_capture import extract_player_seasons_map
    buf = {"/api/v1/player/5/statistics/seasons": {
        "status": 403, "challenge": True, "json": {"error": {"reason": "challenge"}}}}
    assert extract_player_seasons_map(buf, 5) == {}


def test_select_player_season_stats_picks_target_ut_with_season_guard():
    from scrapers.sofascore.camoufox_capture import select_player_season_stats
    # target = EPL (ut=17), season 76986 — must NOT return the default World Cup tab.
    sel = select_player_season_stats(
        _player_season_buffer(839981), 839981, target_ut=17, target_season_id=76986)
    assert sel is not None
    ut, sid, payload = sel
    assert (ut, sid) == (17, 76986)
    assert payload["team"]["name"] == "West Ham"


def test_select_player_season_stats_none_when_target_ut_absent():
    from scrapers.sofascore.camoufox_capture import select_player_season_stats
    # La Liga (ut=8) was never captured (picker missed) → None, not a wrong-comp row.
    assert select_player_season_stats(
        _player_season_buffer(839981), 839981, target_ut=8, target_season_id=99) is None


def test_select_player_season_stats_season_guard_rejects_other_season():
    from scrapers.sofascore.camoufox_capture import select_player_season_stats
    # ut=17 present but only season 76986 — guarding on a different sid → None.
    assert select_player_season_stats(
        _player_season_buffer(839981), 839981, target_ut=17, target_season_id=61627) is None


def test_select_player_season_stats_takes_latest_without_guard():
    from scrapers.sofascore.camoufox_capture import select_player_season_stats
    pid = 5
    buf = {
        f"/api/v1/player/{pid}/unique-tournament/17/season/61627/statistics/overall": {
            "status": 200, "challenge": False, "json": {"statistics": {"rating": 1}}},
        f"/api/v1/player/{pid}/unique-tournament/17/season/76986/statistics/overall": {
            "status": 200, "challenge": False, "json": {"statistics": {"rating": 2}}},
    }
    # No season-guard → deterministic: the most recent season_id (76986).
    ut, sid, _ = select_player_season_stats(buf, pid, target_ut=17)
    assert (ut, sid) == (17, 76986)


def test_select_player_season_stats_ignores_other_players_and_challenges():
    from scrapers.sofascore.camoufox_capture import select_player_season_stats
    # Only the challenged stray for player 111 matches ut/season; must be skipped.
    buf = {
        "/api/v1/player/111/unique-tournament/17/season/76986/statistics/overall": {
            "status": 403, "challenge": True, "json": {"error": {"reason": "challenge"}}},
    }
    assert select_player_season_stats(buf, 839981, target_ut=17) is None


# --------------------------------------------------------------------------- #
#  capture_player season buffer (#751 PR3b) — Season tab + picker in ONE nav   #
#  Browser steps are patched (no Firefox); we assert the buffer FILTERING that #
#  hands the caller only the season-stats + /statistics/seasons paths.         #
# --------------------------------------------------------------------------- #
class TestCapturePlayerSeasonBuffer:
    def _cap(self):
        from unittest.mock import MagicMock
        from scrapers.sofascore.camoufox_capture import SofascoreCamoufoxCapture
        cap = SofascoreCamoufoxCapture(proxy=None)
        cap._page = MagicMock()  # only wait_for_timeout is touched
        return cap

    def test_returns_profile_and_season_buffer_subset(self):
        cap = self._cap()
        pid = 839981
        cap._navigate = lambda *a, **k: None
        cap._extract_player_next_data = lambda p: {"id": int(p), "name": "Paqueta"}
        cap._click_tabs = lambda *a, **k: None

        def fake_drive(label):
            # Simulate the tab + picker firing the season XHRs (+ unrelated noise).
            cap._buffer = {
                f"/api/v1/player/{pid}/statistics/seasons":
                    {"status": 200, "json": {"x": 1}, "challenge": False},
                f"/api/v1/player/{pid}/unique-tournament/17/season/76986/statistics/overall":
                    {"status": 200, "json": {"s": 1}, "challenge": False},
                # unrelated capture (events) — must be filtered OUT.
                f"/api/v1/player/{pid}/events/last/0":
                    {"status": 200, "json": {"events": []}, "challenge": False},
            }

        cap._drive_season_picker = fake_drive

        out = cap.capture_player(pid, season_picker_label="Premier League")

        assert out["profile"]["name"] == "Paqueta"
        assert set(out["season_buffer"]) == {
            f"/api/v1/player/{pid}/statistics/seasons",
            f"/api/v1/player/{pid}/unique-tournament/17/season/76986/statistics/overall",
        }

    def test_profile_only_when_no_picker_label(self):
        # Backward-compatible PR3 path: no picker label → no season capture.
        cap = self._cap()
        cap._navigate = lambda *a, **k: None
        cap._extract_player_next_data = lambda p: {"id": int(p)}
        out = cap.capture_player(7, season_picker_label=None)
        assert out["season_buffer"] == {}
        assert out["profile"] == {"id": 7}


# --------------------------------------------------------------------------- #
#  paginate_tournament_season (#824)                                          #
# --------------------------------------------------------------------------- #
class TestPaginateTournamentSeason:
    """paginate_tournament_season drives an in-page fetch of a SPECIFIC season's
    ``/events/last/{page}`` so a historical-season backfill is not empty (#824).
    We fake ``_page`` so no browser is needed and assert the URL sequence + the
    loop-termination logic (no-next / empty page / max_pages)."""

    class _RecordingPage:
        def __init__(self, results):
            self._results = list(results)
            self.paths = []

        def evaluate(self, js, arg=None):
            self.paths.append(arg)
            return self._results.pop(0) if self._results else {
                "ok": False, "count": 0, "more": False}

        def wait_for_timeout(self, ms):
            pass

    def _cap(self, results):
        from scrapers.sofascore.camoufox_capture import SofascoreCamoufoxCapture
        cap = SofascoreCamoufoxCapture(proxy=None)
        cap._page = self._RecordingPage(results)
        cap._buffer = {"sentinel": 1}
        return cap

    def test_pages_until_no_next_then_stops(self):
        cap = self._cap([
            {"ok": True, "count": 10, "more": True},
            {"ok": True, "count": 10, "more": True},
            {"ok": True, "count": 5, "more": False},   # last page
        ])
        out = cap.paginate_tournament_season(17, 12345)
        # Exactly 3 fetches, paged 0..2 with the season-scoped path.
        assert cap._page.paths == [
            "/api/v1/unique-tournament/17/season/12345/events/last/0",
            "/api/v1/unique-tournament/17/season/12345/events/last/1",
            "/api/v1/unique-tournament/17/season/12345/events/last/2",
        ]
        # Returns the buffer snapshot (a copy of self._buffer).
        assert out == {"sentinel": 1}

    def test_stops_on_empty_events_page(self):
        cap = self._cap([
            {"ok": True, "count": 10, "more": True},
            {"ok": True, "count": 0, "more": True},    # empty → stop even if more
        ])
        cap.paginate_tournament_season(17, 12345)
        assert len(cap._page.paths) == 2

    def test_stops_on_failed_page(self):
        cap = self._cap([
            {"ok": True, "count": 10, "more": True},
            {"ok": False, "count": 0, "more": False},  # fetch !ok → stop
        ])
        cap.paginate_tournament_season(17, 12345)
        assert len(cap._page.paths) == 2

    def test_respects_max_pages_cap(self):
        # Every page says there is more — the cap is the only stop.
        cap = self._cap([{"ok": True, "count": 10, "more": True}] * 10)
        cap.paginate_tournament_season(17, 12345, max_pages=3)
        assert len(cap._page.paths) == 3

    def test_evaluate_exception_breaks_gracefully(self):
        from scrapers.sofascore.camoufox_capture import SofascoreCamoufoxCapture

        class _BoomPage:
            def __init__(self):
                self.paths = []

            def evaluate(self, js, arg=None):
                self.paths.append(arg)
                raise RuntimeError("page closed")

            def wait_for_timeout(self, ms):
                pass

        cap = SofascoreCamoufoxCapture(proxy=None)
        cap._page = _BoomPage()
        cap._buffer = {"x": 1}
        # Must not raise; returns whatever buffer was captured.
        out = cap.paginate_tournament_season(17, 999)
        assert out == {"x": 1}
        assert len(cap._page.paths) == 1


# --------------------------------------------------------------------------- #
#  should_block_request (#842 — cut proxy bytes via route-abort)              #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("resource_type", ["image", "media", "font", "stylesheet"])
def test_should_block_request_blocks_static_resource_types(resource_type):
    # Non-essential static that the SPA doesn't need to fire its /api/v1 XHRs.
    assert should_block_request(resource_type, "https://www.sofascore.com/static/x") is True


@pytest.mark.parametrize("url", [
    "https://www.google-analytics.com/collect",
    "https://stats.g.doubleclick.net/j/collect",
    "https://static.hotjar.com/c/hotjar-123.js",
    "https://connect.facebook.net/en_US/fbevents.js",
    "https://cloudflareinsights.com/cdn-cgi/rum",
])
def test_should_block_request_blocks_analytics_hosts(url):
    # resource_type=script, but a tracking host — block by URL substring.
    assert should_block_request("script", url) is True


@pytest.mark.parametrize("resource_type", [
    "document", "script", "xhr", "fetch", "websocket",
])
def test_should_block_request_allows_essential_types_on_first_party(resource_type):
    # The SPA's own JS/HTML/XHR must load so it can fire the data endpoints.
    assert should_block_request(resource_type, "https://www.sofascore.com/event/123") is False


@pytest.mark.parametrize("url", [
    "https://www.sofascore.com/api/v1/event/123/lineups",
    "https://api.sofascore.com/api/v1/event/123/statistics",
])
def test_should_block_request_never_blocks_data_endpoints(url):
    # The 5 payloads we came for — never block even if mislabelled.
    assert should_block_request("xhr", url) is False


def test_should_block_request_blocks_crest_image_under_api_path():
    # Crests live under /api/v1/.../image (excluded from is_data_api_url) and
    # arrive as resource_type=image → block them to save bytes.
    url = "https://www.sofascore.com/api/v1/team/4724/image"
    assert should_block_request("image", url) is True


def test_should_block_request_never_blocks_turnstile_script():
    # Cloudflare Turnstile/challenge assets are required for the bypass.
    url = "https://challenges.cloudflare.com/turnstile/v0/api.js"
    assert should_block_request("script", url) is False


@pytest.mark.parametrize("resource_type,url", [
    ("stylesheet", "https://challenges.cloudflare.com/cdn-cgi/challenge-platform/h/b/style.css"),
    ("font", "https://challenges.cloudflare.com/turnstile/v0/g/font.woff2"),
    ("image", "https://challenges.cloudflare.com/cdn-cgi/challenge-platform/x.png"),
])
def test_should_block_request_never_blocks_cloudflare_challenge_assets(resource_type, url):
    # The challenge iframe pulls its OWN css/font/img — the bare type block would
    # otherwise abort them and starve the bypass (#842 evasion review).
    assert should_block_request(resource_type, url) is False


# --------------------------------------------------------------------------- #
#  _maybe_block / _on_request_finished (route + byte accounting, #842)        #
# --------------------------------------------------------------------------- #
class _FakeRequest:
    def __init__(self, resource_type, url, *, sizes=None, sizes_raises=False):
        self.resource_type = resource_type
        self.url = url
        self._sizes = sizes or {}
        self._sizes_raises = sizes_raises

    def sizes(self):
        if self._sizes_raises:
            raise RuntimeError("sizes unavailable")
        return self._sizes


class _FakeRoute:
    def __init__(self, resource_type, url, *, abort_raises=False):
        self.request = _FakeRequest(resource_type, url)
        self._abort_raises = abort_raises
        self.aborted = False
        self.continued = False

    def abort(self):
        if self._abort_raises:
            raise RuntimeError("Route is already handled")
        self.aborted = True

    def continue_(self):
        self.continued = True


def _cap():
    from scrapers.sofascore.camoufox_capture import SofascoreCamoufoxCapture
    return SofascoreCamoufoxCapture(proxy=None)


class TestMaybeBlock:
    def test_aborts_blockable_and_counts(self):
        cap = _cap()
        route = _FakeRoute("image", "https://www.sofascore.com/static/x.png")
        cap._maybe_block(route)
        assert route.aborted is True
        assert route.continued is False
        assert cap._blocked_count == 1

    def test_continues_essential(self):
        cap = _cap()
        route = _FakeRoute("xhr", "https://www.sofascore.com/api/v1/event/1/lineups")
        cap._maybe_block(route)
        assert route.continued is True
        assert route.aborted is False
        assert cap._blocked_count == 0

    def test_abort_error_falls_back_to_continue(self):
        # A routing error must never stall the navigation.
        cap = _cap()
        route = _FakeRoute("image", "https://www.sofascore.com/static/x.png", abort_raises=True)
        cap._maybe_block(route)  # must not raise
        assert route.continued is True


class TestOnRequestFinished:
    def test_sums_all_four_size_fields(self):
        cap = _cap()
        req = _FakeRequest("script", "https://x/app.js", sizes={
            "requestBodySize": 1, "requestHeadersSize": 2,
            "responseBodySize": 100, "responseHeadersSize": 10,
        })
        cap._on_request_finished(req)
        assert cap._bytes_total == 113
        assert cap._bytes_by_type["script"] == 113

    def test_sizes_exception_is_swallowed(self):
        cap = _cap()
        req = _FakeRequest("script", "https://x/app.js", sizes_raises=True)
        cap._on_request_finished(req)  # must not raise
        assert cap._bytes_total == 0
