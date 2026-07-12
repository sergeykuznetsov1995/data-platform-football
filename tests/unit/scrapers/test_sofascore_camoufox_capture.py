"""Unit tests for the production exact-JSON Camoufox transport."""
import json
from unittest.mock import MagicMock

import pytest

from scrapers.sofascore.camoufox_capture import (
    SofascoreCamoufoxCapture,
    is_challenge,
    is_data_api_url,
    merge_capture,
    normalize_event,
    response_path,
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
    assert row['home_team'] == row['home_team_name']
    assert row['away_team'] == row['away_team_name']
    assert row['home_score'] == row['home_score_current']
    assert row['away_score'] == row['away_score_current']
    for dead in ("date", "round", "week", "game"):
        assert dead not in row


def test_normalize_event_pins_season_year_to_str():
    # #913: clubs carry season.year as '25/26' (str), single-year cups as an
    # INT (2026) — bronze season_year is varchar; a mixed batch breaks the
    # Arrow write (same class as the #840 match_stats home/away fix).
    club = normalize_event({"id": 1, "season": {"year": "25/26"}})
    cup = normalize_event({"id": 2, "season": {"year": 2026}})
    assert club["season_year"] == "25/26"
    assert cup["season_year"] == "2026"


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


def test_merge_capture_keeps_good_over_later_challenge_json():
    good = {"status": 200, "json": {"events": [1]}, "challenge": False}
    challenged = {
        "status": 403,
        "json": {"error": {"reason": "challenge"}},
        "challenge": True,
    }
    assert merge_capture(good, challenged) is good


def test_merge_capture_keeps_terminal_404_over_later_transport_error():
    terminal = {"status": 404, "json": None, "challenge": False}
    retryable = {"status": 503, "json": None, "challenge": False}
    assert merge_capture(terminal, retryable) is terminal


# --------------------------------------------------------------------------- #
#  _on_response wiring                                                       #
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


# --------------------------------------------------------------------------- #
#  league standings parsers — live-proven shapes (EPL 24/25, #779)            #
#  Fixtures mirror the REAL capture taken 2026-06-23 (sid 61627): the         #
#  /standings/total 'total' block.                                            #
# --------------------------------------------------------------------------- #
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
        "gf": 86, "ga": 41, "gd": 45, "pts": 84, "group": "__total__"}


def _wc_group_standings_buffer(ut=16, sid=58210):
    """A group tournament fires one 'total' block PER GROUP (#913)."""
    def row(name, pts):
        return {"team": {"name": name}, "matches": 3, "wins": pts // 3,
                "draws": pts % 3, "losses": 3 - pts // 3 - pts % 3,
                "scoresFor": 5, "scoresAgainst": 2, "points": pts}
    return {
        f"/api/v1/unique-tournament/{ut}/season/{sid}/standings/total": {
            "status": 200, "challenge": False, "json": {"standings": [
                {"type": "total", "name": "Group A",
                 "rows": [row("Mexico", 9), row("South Africa", 4)]},
                {"type": "total", "name": "Group B",
                 "rows": [row("Canada", 7), row("Qatar", 1)]},
            ]}},
    }


def test_extract_tournament_standings_collects_all_group_blocks():
    # WC regression (#913): only Group A of 12 survived — the extractor took
    # the FIRST 'total' block. All group blocks must be collected.
    from scrapers.sofascore.camoufox_capture import extract_tournament_standings
    rows = extract_tournament_standings(_wc_group_standings_buffer(), 16, 58210)
    assert [r["team"]["name"] for r in rows] == [
        "Mexico", "South Africa", "Canada", "Qatar"]
    assert [r["group"] for r in rows] == [
        "Group A", "Group A", "Group B", "Group B"]


def test_normalize_standing_carries_group_from_block():
    from scrapers.sofascore.camoufox_capture import (
        extract_tournament_standings, normalize_standing)
    rows = extract_tournament_standings(_wc_group_standings_buffer(), 16, 58210)
    out = [normalize_standing(r) for r in rows]
    assert out[0]["group"] == "Group A" and out[-1]["group"] == "Group B"


def test_extract_tournament_standings_single_league_block_has_no_group():
    # A club league (one 'total' block) must NOT get its block name as group.
    from scrapers.sofascore.camoufox_capture import (
        extract_tournament_standings, normalize_standing)
    rows = extract_tournament_standings(_standings_buffer(), 17, 61627)
    assert all(normalize_standing(r)["group"] == "__total__" for r in rows)


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


@pytest.mark.parametrize("url", [
    "https://ublockorigin.github.io/uAssets/filters/filters.txt",
    "https://cdn.jsdelivr.net/gh/uBlockOrigin/uAssets/filters/privacy.txt",
    "https://raw.githubusercontent.com/uBlockOrigin/uAssets/master/filters.txt",
    "https://publicsuffix.org/list/public_suffix_list.dat",
])
def test_should_block_browser_extension_updates_before_rate_limiting(url):
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

    def test_abort_error_never_falls_open_to_continue(self):
        cap = _cap()
        route = _FakeRoute("image", "https://www.sofascore.com/static/x.png", abort_raises=True)
        cap._maybe_block(route)  # must not raise
        assert route.continued is False
        assert cap._blocked_count == 1

    def test_paces_document_script_and_challenge_asset_once_each(self):
        calls = []
        cap = SofascoreCamoufoxCapture(
            request_limiter=lambda: calls.append("paced") or True
        )
        routes = [
            _FakeRoute("document", "https://www.sofascore.com/event/1"),
            _FakeRoute("script", "https://www.sofascore.com/app.js"),
            _FakeRoute(
                "script",
                "https://challenges.cloudflare.com/turnstile/v0/api.js",
            ),
        ]

        for route in routes:
            cap._maybe_block(route)

        assert calls == ["paced"] * 3
        assert all(route.continued for route in routes)
        assert cap._source_request_count == 3

    def test_limiter_error_aborts_instead_of_continuing(self):
        cap = SofascoreCamoufoxCapture(
            request_limiter=lambda: (_ for _ in ()).throw(RuntimeError("stop"))
        )
        route = _FakeRoute("script", "https://www.sofascore.com/app.js")

        cap._maybe_block(route)

        assert route.aborted is True
        assert route.continued is False
        assert cap._source_request_count == 0

    def test_route_validation_exception_aborts_instead_of_continuing(
        self, monkeypatch
    ):
        import scrapers.sofascore.camoufox_capture as module

        cap = _cap()
        monkeypatch.setattr(
            module,
            "should_block_request",
            lambda *_args: (_ for _ in ()).throw(RuntimeError("invalid route")),
        )
        route = _FakeRoute("script", "https://www.sofascore.com/app.js")

        cap._maybe_block(route)

        assert route.aborted is True
        assert route.continued is False

    def test_exact_mode_allows_only_current_authorized_api_path(self):
        cap = _cap()
        cap._exact_only = True
        cap._allowed_api_path = "/api/v1/event/1/lineups"
        allowed = _FakeRoute(
            "xhr", "https://www.sofascore.com/api/v1/event/1/lineups"
        )
        unrelated = _FakeRoute(
            "xhr", "https://www.sofascore.com/api/v1/event/2/lineups"
        )

        cap._maybe_block(allowed)
        cap._maybe_block(unrelated)

        assert allowed.continued is True
        assert unrelated.aborted is True

    def test_exact_mode_allows_required_cloudflare_challenge_asset(self):
        cap = _cap()
        cap._exact_only = True
        route = _FakeRoute(
            "script",
            "https://challenges.cloudflare.com/cdn-cgi/challenge-platform/x.js",
        )

        cap._maybe_block(route)

        assert route.continued is True
        assert route.aborted is False

    def test_exit_probe_url_is_open_only_during_explicit_probe(self):
        cap = _cap()
        cap._exact_only = True
        url = "https://api.ipify.org/?format=json"
        blocked = _FakeRoute("xhr", url)
        cap._maybe_block(blocked)
        assert blocked.aborted is True

        cap._allowed_external_url = url
        allowed = _FakeRoute("xhr", url)
        cap._maybe_block(allowed)
        assert allowed.continued is True


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


# --------------------------------------------------------------------------- #
#  __enter__ cleanup on failed start (#879)                                   #
# --------------------------------------------------------------------------- #
class TestEnterCleanupOnFailedStart:
    """A failed Camoufox start must tear the half-open manager down and
    re-raise — otherwise the sync-playwright loop stays armed in the thread and
    the NEXT session in the same process dies with "Sync API inside the asyncio
    loop" (#879, live-hit on the ESP-2016/GER-2022 backfill units)."""

    def _enter_with_fake_camoufox(self, cm):
        import sys
        from unittest.mock import MagicMock, patch

        from scrapers.sofascore.camoufox_capture import SofascoreCamoufoxCapture

        cap = SofascoreCamoufoxCapture(proxy={"server": "http://exit:1"})
        fake_sync_api = MagicMock()
        fake_sync_api.Camoufox.return_value = cm
        with patch.dict(sys.modules, {
            "camoufox": MagicMock(sync_api=fake_sync_api),
            "camoufox.sync_api": fake_sync_api,
        }):
            with pytest.raises(RuntimeError):
                cap.__enter__()
        return cap

    def test_failed_cm_enter_is_torn_down_and_reraised(self):
        from unittest.mock import MagicMock

        cm = MagicMock()
        cm.__enter__.side_effect = RuntimeError("Failed to connect to proxy")

        cap = self._enter_with_fake_camoufox(cm)

        assert cm.__exit__.called
        assert cap._cm is None  # our __exit__ stays an idempotent no-op

    def test_failed_new_page_is_torn_down_and_reraised(self):
        from unittest.mock import MagicMock

        cm = MagicMock()
        cm.__enter__.return_value.new_page.side_effect = RuntimeError(
            "tab crashed")

        cap = self._enter_with_fake_camoufox(cm)

        assert cm.__exit__.called
        assert cap._cm is None

    def test_teardown_failure_does_not_mask_original_error(self):
        from unittest.mock import MagicMock

        cm = MagicMock()
        cm.__enter__.side_effect = RuntimeError("Failed to connect to proxy")
        cm.__exit__.side_effect = RuntimeError("teardown also broke")

        cap = self._enter_with_fake_camoufox(cm)  # raises the ORIGINAL error

        assert cap._cm is None


# --------------------------------------------------------------------------- #
#  fetch_api_json (#879)                                                      #
# --------------------------------------------------------------------------- #
class TestFetchApiJson:
    """Generic in-page fetch of an arbitrary /api/v1 path. Returns the
    buffer-shaped record (status/json/challenge) and merges it into the live
    buffer; None only on a transport failure — callers use that distinction to
    separate 'legitimately empty' from 'worth retrying' (#879)."""

    def _cap_with_page(
        self,
        *,
        status=200,
        body=b"{}",
        headers=None,
        request_raises=None,
    ):
        from unittest.mock import MagicMock

        cap = _cap()
        cap._page = MagicMock()
        response = MagicMock()
        response.status = status
        response.body.return_value = body
        response.headers = headers or {"content-type": "application/json"}
        if request_raises is not None:
            cap._page.goto.side_effect = request_raises
        else:
            cap._page.goto.return_value = response
        return cap

    def test_returns_rec_and_merges_into_buffer(self):
        path = "/api/v1/unique-tournament/8/seasons"
        body = json.dumps({"seasons": [{"year": "24/25", "id": 61643}]})
        cap = self._cap_with_page(body=body.encode())

        rec = cap.fetch_api_json(path)

        cap._page.goto.assert_called_once_with(
            f"https://www.sofascore.com{path}",
            wait_until="commit",
            timeout=cap._nav_timeout_ms,
        )
        assert cap._navigation_count == 1
        assert rec["status"] == 200
        assert rec["challenge"] is False
        assert rec["json"]["seasons"][0]["id"] == 61643
        assert cap._buffer[path]["json"] == rec["json"]

    def test_returns_none_when_request_raises(self):
        cap = self._cap_with_page(request_raises=RuntimeError("page gone"))

        assert cap.fetch_api_json("/api/v1/unique-tournament/8/seasons") is None

    def test_request_failure_does_not_add_endpoint_navigation(self):
        path = "/api/v1/event/7"
        cap = self._cap_with_page(request_raises=RuntimeError("network error"))
        cap._exact_only = True

        rec = cap.fetch_api_json(path)

        assert rec is None
        cap._page.goto.assert_called_once()
        assert cap._navigation_count == 1
        assert cap._allowed_api_path is None

    def test_non_json_body_keeps_json_none(self):
        raw = b"<html>blocked</html>"
        cap = self._cap_with_page(status=403, body=raw)

        rec = cap.fetch_api_json("/api/v1/unique-tournament/8/seasons")

        assert rec["status"] == 403
        assert rec["json"] is None
        assert rec["body"] == raw

    def test_challenge_body_is_flagged(self):
        body = json.dumps({"error": {"code": 403, "reason": "challenge"}})
        cap = self._cap_with_page(body=body.encode())

        rec = cap.fetch_api_json("/api/v1/unique-tournament/8/seasons")

        assert rec["challenge"] is True

    def test_preserves_exact_non_text_response_bytes(self):
        raw = b"\x00\xff\r\nnot-json"
        cap = self._cap_with_page(body=raw)

        rec = cap.fetch_api_json("/api/v1/event/7")

        assert rec["body"] == raw
        assert rec["json"] is None


class TestExactWarmAndCanaryExitProbe:
    def test_warm_stops_background_work_then_enables_lockdown(self):
        cap = _cap()
        cap._page = MagicMock()
        navigations = []
        cap._navigate_exact_origin = navigations.append

        cap.warm_exact_json("https://www.sofascore.com/event/1")

        assert navigations == ["https://www.sofascore.com/"]
        assert cap._exact_only is True
        assert cap._allowed_api_path is None
        assert cap._allowed_external_url is None

    def test_warm_blocks_all_passive_api_but_allows_document(self):
        cap = _cap()
        cap._page = MagicMock()
        passive = _FakeRoute(
            "xhr", "https://www.sofascore.com/api/v1/event/999"
        )
        document = _FakeRoute(
            "document", "https://www.sofascore.com/"
        )

        def navigate(_url):
            cap._maybe_block(passive)
            cap._maybe_block(document)

        cap._navigate_exact_origin = navigate
        cap.warm_exact_json("https://www.sofascore.com/event/1")

        assert passive.aborted is True
        assert passive.continued is False
        assert document.continued is True
        assert cap._source_request_count == 1
        assert cap._exact_only is True

    def test_exact_warm_uses_one_proven_html_anchor(self):
        cap = _cap()
        cap._page = MagicMock()
        response = MagicMock()
        response.url = "https://www.sofascore.com/"
        response.status = 200
        cap._page.goto.side_effect = lambda *args, **kwargs: (
            setattr(
                cap,
                "_source_request_count",
                cap._source_request_count + 1,
            )
            or response
        )
        cap._page.evaluate.return_value = "https://www.sofascore.com"

        cap.warm_exact_json("https://www.sofascore.com/event/1")

        cap._page.goto.assert_called_once_with(
            "https://www.sofascore.com/",
            wait_until="domcontentloaded",
            timeout=cap._nav_timeout_ms,
        )
        cap._page.evaluate.assert_called_once_with("() => location.origin")
        assert cap._navigation_count == 1
        assert cap._source_request_count == 1

    @pytest.mark.parametrize(
        "response_url",
        [
            "https://api.sofascore.com/",
            "https://other.example/",
            "https://www.sofascore.com/?redirected=1",
            "https://www.sofascore.com/#fragment",
            "https://www.sofascore.com/football",
        ],
    )
    def test_exact_warm_rejects_cross_origin_query_or_fragment(self, response_url):
        cap = _cap()
        cap._page = MagicMock()
        response = MagicMock(status=200, url=response_url)

        def navigate(*_args, **_kwargs):
            cap._source_request_count += 1
            return response

        cap._page.goto.side_effect = navigate
        cap._page.evaluate.return_value = "https://www.sofascore.com"

        with pytest.raises(RuntimeError, match="anchor_url_ok=False"):
            cap.warm_exact_json("https://www.sofascore.com/event/1")

        assert cap._navigation_count == 1

    def test_exact_warm_rejects_redirect_or_asset_extra_request(self):
        cap = _cap()
        cap._page = MagicMock()
        response = MagicMock(
            status=200,
            url="https://www.sofascore.com/",
        )

        def navigate(*_args, **_kwargs):
            # A redirect or an allowed challenge asset would both cross the
            # route-level source gate and make this delta greater than one.
            cap._source_request_count += 2
            return response

        cap._page.goto.side_effect = navigate
        cap._page.evaluate.return_value = "https://www.sofascore.com"

        with pytest.raises(RuntimeError, match="source_request_delta=2"):
            cap.warm_exact_json("https://www.sofascore.com/event/1")

        assert cap._navigation_count == 1

    @pytest.mark.parametrize(
        "anchor_url",
        [
            "https://api.sofascore.com/",
            "https://other.example/",
            "https://www.sofascore.com/?query=1",
            "https://www.sofascore.com/football",
        ],
    )
    def test_exact_navigation_rejects_invalid_requested_anchor(self, anchor_url):
        cap = _cap()
        cap._page = MagicMock()

        with pytest.raises(ValueError, match="canonical official HTTPS"):
            cap._navigate_exact_origin(anchor_url)

        cap._page.goto.assert_not_called()
        assert cap._navigation_count == 0
        assert cap._source_request_count == 0

    def test_exact_warm_error_exposes_only_safe_final_url_shape(self):
        cap = _cap()
        cap._page = MagicMock()
        response = MagicMock(
            status=200,
            url=(
                "https://credential-alice:credential-password@"
                "WWW.SOFASCORE.COM:444/football"
                "?token=query-secret#fragment-secret"
            ),
        )

        def navigate(*_args, **_kwargs):
            cap._source_request_count += 1
            return response

        cap._page.goto.side_effect = navigate
        cap._page.evaluate.return_value = "https://www.sofascore.com"

        with pytest.raises(RuntimeError) as captured:
            cap.warm_exact_json("https://www.sofascore.com/event/1")

        message = str(captured.value)
        assert '"type":"str"' in message
        assert '"scheme":"https"' in message
        assert '"hostname":"www.sofascore.com"' in message
        assert '"port":444' in message
        assert '"path":"/football"' in message
        assert '"query_present":true' in message
        assert '"fragment_present":true' in message
        assert '"userinfo_present":true' in message
        for secret in (
            "credential-alice",
            "credential-password",
            "query-secret",
            "fragment-secret",
        ):
            assert secret not in message

    def test_probe_accepts_direct_valid_ip_and_retains_no_probe_url(self):
        cap = _cap()
        cap._page = MagicMock()
        cap._page.evaluate.return_value = {
            "status": 200,
            "body": '{"ip":"2001:db8::1"}',
            "url": "https://api.ipify.org/?format=json",
            "redirected": False,
        }

        assert cap.probe_proxy_exit() == "2001:db8::1"
        assert cap._allowed_external_url is None

    @pytest.mark.parametrize(
        "response",
        [
            {
                "status": 200,
                "body": '{"ip":"203.0.113.7"}',
                "url": "https://redirect.invalid/",
                "redirected": True,
            },
            {
                "status": 200,
                "body": '{"ip":"credential lease-secret"}',
                "url": "https://api.ipify.org/?format=json",
                "redirected": False,
            },
        ],
    )
    def test_probe_rejects_redirect_or_non_ip_without_echoing_body(self, response):
        cap = _cap()
        cap._page = MagicMock()
        cap._page.evaluate.return_value = response

        with pytest.raises(RuntimeError) as captured:
            cap.probe_proxy_exit()

        assert "lease-secret" not in str(captured.value)
        assert "203.0.113.7" not in str(captured.value)
        assert cap._allowed_external_url is None
