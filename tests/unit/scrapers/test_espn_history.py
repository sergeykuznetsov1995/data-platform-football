"""History lane of the ESPN contour on recorded answers (#1509).

Core lists are the bodies recorded 28.09 for this task (eng.1 2015: seasons,
season, one type over four pages of ``limit=100``; UCL 2010: the eight types
missing next to the recorded group stage) and 24.09 (UCL season 2010 with nine
types, group stage events).  Summary bodies are recorded ones; a season of
hundreds of matches reuses one recorded Summary of that season under each
listed event id (header ids rewritten).  No network, no Trino: the fake
client of the wave test serves bodies by address and keeps what it served as
its raw store; the queue lives in DuckDB, bronze in the in-memory Trino.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import json
import re

import pytest

from scrapers.espn import history, urls
from scrapers.espn.denominator import load_denominator
from scrapers.espn.models import AgeClass, Competition, Edition, Gender
from scrapers.espn.schedule_parser import parse_scoreboards, schedule_row_from_header
from scrapers.espn.transport_contracts import HttpStatusError, LaneClosed
from scrapers.espn.wave import _UNKNOWN
from tests.unit.scrapers.test_espn_probes import PROBES
from tests.unit.scrapers.test_espn_wave import FakeClient, WaveTrino, _key, _matches, _req_key

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
ENG_2015_PAGES = [PROBES / f"type_events_eng1_2015_t1_p{page}.json" for page in (1, 2, 3, 4)]
_REF = re.compile(r"/events/(\d+)\?")


def _ids(*paths) -> list[int]:
    return [
        int(_REF.search(item["$ref"])[1])
        for path in paths
        for item in json.loads(path.read_bytes())["items"]
    ]


# Parts of a Summary no history assertion reads; a "light" template drops
# them so that the runner tests of hundreds of matches stay quick.
_HEAVY = ("rosters", "commentary", "news", "standings", "lastFiveGames", "seasonseries", "leaders", "article")


@lru_cache(maxsize=None)
def _template(name: str, light: bool) -> dict:
    body = json.loads((PROBES / name).read_bytes())
    return {key: value for key, value in body.items() if not (light and key in _HEAVY)}


def _summary_as(template: str, event_id: int, light: bool = False) -> bytes:
    body = _template(template, light)
    header = body["header"]
    competition = {**header["competitions"][0], "id": str(event_id)}
    return json.dumps(
        {**body, "header": {**header, "id": str(event_id), "competitions": [competition]}}
    ).encode()


class Bodies(dict):
    """Recorded bodies by address; a Summary of a listed id from its template."""

    def __init__(self, summaries: dict[str, str], *, light: bool = False, **bodies) -> None:
        super().__init__(bodies)
        self.summaries = summaries
        self.light = light
        # Served for any other address when set (an error for every other slug).
        self.otherwise = None

    def __missing__(self, key: str):
        match = re.search(r"/soccer/([^/]+)/summary\?event=(\d+)$", key)
        if (match is None or match[1] not in self.summaries) and self.otherwise is not None:
            return self.otherwise
        if match is None or match[1] not in self.summaries:
            raise KeyError(key)
        return _summary_as(self.summaries[match[1]], int(match[2]), self.light)


class HistoryClient(FakeClient):
    """Counts what reached the network; ``hook`` runs before each download."""

    def __init__(self, responses) -> None:
        super().__init__(responses)
        self.network: list[str] = []
        self.hook = None

    def fetch_json(self, url, endpoint, params=None, *, force_refresh=False):
        key = _key(url, params)
        if force_refresh or key not in self.stored:
            if self.hook is not None:
                self.hook(key)
            self.network.append(key)
        return super().fetch_json(url, endpoint, params, force_refresh=force_refresh)


class HistoryTrino(WaveTrino):
    """In-memory bronze answering the live debt and the duplicate lookup too."""

    def __init__(self, debt: int = 0) -> None:
        super().__init__()
        self.debt = debt

    def execute_query(self, sql, params=None):
        if sql.startswith("UPDATE"):
            owner, slug, year = params
            ids = {int(item) for item in re.search(r"IN \(([\d, ]+)\)", sql)[1].split(", ")}
            for row in self.tables["espn_match"]:
                if (row["competition_slug"], row["season_year"]) == (slug, year) and row["event_id"] in ids:
                    row["duplicate_of"] = owner
            return [[len(ids)]]
        if "AS debt" in sql:
            self.queries.append(sql)
            return [[self.debt]]
        if "duplicate_of IS NULL AND event_id IN" in sql:
            slug, year = params
            ids = {int(item) for item in re.search(r"IN \(([\d, ]+)\)", sql)[1].split(", ")}
            return [
                [row["event_id"], row["competition_slug"], row["season_year"]]
                for row in self._rows()
                if row["event_id"] in ids
                and row["duplicate_of"] is None
                and (row["competition_slug"], row["season_year"]) != (slug, year)
            ]
        return super().execute_query(sql, params)


@pytest.fixture()
def conn():
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect(":memory:")
    connection.execute("ATTACH ':memory:' AS iceberg")
    history.ensure_queue_table(connection)
    return connection


def _eng_2015(light: bool = True, **extra) -> Bodies:
    bodies = {
        _req_key(urls.league_seasons("eng.1", limit=100)): (PROBES / "seasons_eng1_limit100.json").read_bytes(),
        _req_key(urls.season("eng.1", 2015)): (PROBES / "season_eng1_2015.json").read_bytes(),
    }
    for page, path in enumerate(ENG_2015_PAGES, start=1):
        bodies[_req_key(urls.type_events("eng.1", 2015, 1, page, limit=100))] = path.read_bytes()
    bodies.update(extra)
    return Bodies({"eng.1": "summary_eng1_2015_422285.json"}, light=light, **bodies)


def _run(client, trino, conn, *, scope=(("eng.1", 2015),), stop_file=None, now=NOW, budget=30):
    return history.run_history(
        client=client,
        trino=trino,
        conn=conn,
        denominator=load_denominator(),
        scope=scope,
        run_id="r1",
        deadline=now + timedelta(minutes=budget),
        stop_file=stop_file,
        now_fn=lambda: now,
    )


def _queue(conn) -> dict:
    return history.load_queue(conn)


def _run_rows(conn) -> list:
    return conn.execute(
        "SELECT state, matches, failed, last_error FROM iceberg.ops.espn_history_queue_v1 "
        "WHERE slug = '(run)'"
    ).fetchall()


def _summaries(client) -> list[str]:
    return [key for key in client.network if "/summary?" in key]


# ------------------------------------------------------------------ header


def test_schedule_row_from_header_equals_the_scoreboard_row_of_the_same_match() -> None:
    edition = Edition(2005, "2005-06", date(2005, 7, 1), date(2006, 6, 30), True, _UNKNOWN)
    competition = Competition(
        700, "eng.1", "English Premier League", Gender.MALE, AgeClass.SENIOR, True, (edition,)
    )
    scoreboard = {
        row.event_id: row
        for row in parse_scoreboards(
            (PROBES / "scoreboard_eng1_20050813_postponed.json").read_bytes(),
            competition=competition,
            edition=edition,
            query_start=date(2005, 8, 13),
            query_end=date(2005, 8, 13),
        )
    }[184188]

    header = schedule_row_from_header(
        (PROBES / "summary_eng1_2005.json").read_bytes(), competition=competition, edition=edition
    )

    assert header.status == "STATUS_FULL_TIME" and header.venue_id == 253
    assert header.attendance_value == 38610 and header.kickoff_confirmed
    for name in scoreboard.__slots__:
        if name != "extra_json":
            assert getattr(header, name) == getattr(scoreboard, name), name


def test_header_of_another_season_is_an_error_not_a_skipped_match() -> None:
    edition = Edition(2006, "2006-07", date(2006, 7, 1), date(2007, 6, 30), True, _UNKNOWN)
    competition = Competition(700, "eng.1", "EPL", Gender.MALE, AgeClass.SENIOR, True, (edition,))
    with pytest.raises(ValueError, match="outside 700:2006"):
        schedule_row_from_header(
            (PROBES / "summary_eng1_2005.json").read_bytes(),
            competition=competition,
            edition=edition,
        )


# ------------------------------------------------------------------ scope


def test_scope_file_and_the_ten_past_seasons() -> None:
    assert history.load_scope() == (("eng.1", 2015),)
    assert history.history_years(range(2001, 2027), 2026) == tuple(range(2025, 2015, -1))
    # A national-team tournament: the editions of the last ten years.
    assert history.history_years([2010, 2014, 2018, 2022, 2026], 2026) == (2022, 2018)


# ------------------------------------------------------------------ runs


def test_league_with_one_type_over_four_pages_is_written_whole(conn) -> None:
    client, trino = HistoryClient(_eng_2015(light=False)), HistoryTrino()

    run = _run(client, trino, conn)

    listed = _ids(*ENG_2015_PAGES)
    assert len(listed) == len(set(listed)) == 380
    assert run.reason == history.IDLE and run.matches == 380 and run.failed == 0
    assert run.batches == 1  # 380 <= BATCH_MATCHES: one publication
    rows = _matches(trino)
    assert set(rows) == set(listed)
    assert all(row["disposition"] == "captured" and row["season_year"] == 2015 for row in rows.values())
    assert all(row["duplicate_of"] is None for row in rows.values())
    queue = _queue(conn)
    assert queue[("eng.1", 0, 0)].state == history.DONE and queue[("eng.1", 0, 0)].matches == 1
    type_row = queue[("eng.1", 2015, 1)]
    assert (type_row.state, type_row.matches, type_row.done, type_row.failed) == ("done", 380, 380, 0)
    assert ("eng.1", 2015, 0) not in queue  # the season row became its type rows
    # seasons + season + 4 pages + one Summary per match: ~1 request per match.
    assert len(client.network) == 1 + 1 + 4 + 380
    assert any("page=4" in key and "limit=100" in key for key in client.network)
    assert _run_rows(conn) == [("idle", 380, 0, None)]


def test_repeat_of_a_closed_season_makes_no_network_request(conn) -> None:
    client, trino = HistoryClient(_eng_2015()), HistoryTrino()
    _run(client, trino, conn)
    before = len(client.network)
    # The queue forgets the season and bronze loses it: everything comes back
    # from the raw store (season closed 01.06.2016).
    conn.execute("DELETE FROM iceberg.ops.espn_history_queue_v1 WHERE season_year = 2015")
    history.save_rows(conn, [history.QueueRow("eng.1", 2015, 0, history.PENDING)])
    trino.tables = {name: [] for name in trino.tables}

    run = _run(client, trino, conn)

    assert run.matches == 380 and len(client.network) == before
    # And a third run finds nothing to do and reads nothing.
    assert _run(client, trino, conn).matches == 0 and len(client.network) == before


def test_interrupted_batch_continues_without_downloading_again(conn, tmp_path) -> None:
    client, trino = HistoryClient(_eng_2015()), HistoryTrino()
    stop = tmp_path / "history.off"

    def hook(key: str) -> None:
        if len(_summaries(client)) == 149:
            stop.touch()

    client.hook = hook
    first = _run(client, trino, conn, stop_file=stop)

    assert first.reason == history.STOPPED and first.matches == 150
    assert len(_matches(trino)) == 150
    row = _queue(conn)[("eng.1", 2015, 1)]
    assert (row.state, row.done, row.attempts) == ("listed", 150, 0)

    stop.unlink()
    client.hook = None
    second = _run(client, trino, conn, stop_file=stop)

    assert second.reason == history.IDLE and second.matches == 230
    assert len(_matches(trino)) == 380
    assert len(_summaries(client)) == len(set(_summaries(client))) == 380
    assert _queue(conn)[("eng.1", 2015, 1)].state == history.DONE


def test_stop_file_before_the_run_reads_nothing(conn, tmp_path) -> None:
    client, trino = HistoryClient(_eng_2015()), HistoryTrino()
    stop = tmp_path / "history.off"
    stop.touch()

    run = _run(client, trino, conn, stop_file=stop)

    assert run.reason == history.STOPPED and client.network == [] and _queue(conn) == {}
    assert _run_rows(conn)[0][0] == "stopped"


def test_live_debt_ends_the_run_before_any_request(conn) -> None:
    client, trino = HistoryClient(_eng_2015()), HistoryTrino(debt=2)

    run = _run(client, trino, conn)

    assert run.reason == history.LIVE_DEBT and "2 live match" in run.detail
    assert client.network == [] and trino.tables["espn_match"] == []
    assert any("AS debt" in sql and "'eng.1'" in sql for sql in trino.queries)


def test_debt_appearing_between_batches_ends_the_run(conn, monkeypatch) -> None:
    monkeypatch.setattr(history, "BATCH_MATCHES", 100)
    client, trino = HistoryClient(_eng_2015()), HistoryTrino()

    def hook(key: str) -> None:
        if len(_summaries(client)) == 150:
            trino.debt = 1

    client.hook = hook
    run = _run(client, trino, conn)

    assert run.reason == history.LIVE_DEBT and run.matches == 200 and run.batches == 2
    assert _queue(conn)[("eng.1", 2015, 1)].done == 200


def test_lane_closed_ends_the_run_cleanly_after_writing_what_it_has(conn) -> None:
    client, trino = HistoryClient(_eng_2015()), HistoryTrino()

    def hook(key: str) -> None:
        if len(_summaries(client)) == 50:
            raise LaneClosed("ESPN history lane is frozen")

    client.hook = hook
    run = _run(client, trino, conn)

    assert run.reason == history.LANE_CLOSED and run.matches == 50
    assert len(_matches(trino)) == 50
    assert _run_rows(conn)[0][0] == "lane_closed"


def test_budget_ends_the_run(conn) -> None:
    client, trino = HistoryClient(_eng_2015()), HistoryTrino()

    run = _run(client, trino, conn, budget=0)

    assert run.reason == history.BUDGET and client.network == []


def test_failed_match_is_red_tried_once_more_then_stays_red(conn) -> None:
    listed = _ids(*ENG_2015_PAGES)
    broken = _req_key(urls.summary("eng.1", listed[7]))
    client = HistoryClient(_eng_2015(**{broken: HttpStatusError(404, "not found")}))
    trino = HistoryTrino()

    first = _run(client, trino, conn)

    assert first.matches == 379 and first.failed == 1
    row = _queue(conn)[("eng.1", 2015, 1)]
    assert (row.state, row.done, row.failed, row.attempts) == ("red", 379, 1, 1)
    assert str(listed[7]) in row.last_error

    second = _run(client, trino, conn)
    assert second.matches == 0 and second.failed == 1
    assert _summaries(client).count(broken) == 2  # only the failed match again
    assert _queue(conn)[("eng.1", 2015, 1)].attempts == 2
    third = _run(client, trino, conn)
    assert third.failed == 0 and _summaries(client).count(broken) == 2


# ------------------------------------------------------------------ cup


def _ucl_2010(type9_extra: int | None = None) -> Bodies:
    bodies = {
        _req_key(urls.league_seasons("uefa.champions", limit=100)): (PROBES / "seasons_uefa.champions.json").read_bytes(),
        _req_key(urls.season("uefa.champions", 2010)): (PROBES / "season_uefa.champions_2010.json").read_bytes(),
        # Recorded with limit=1000: 96 events, one page either way.
        _req_key(urls.type_events("uefa.champions", 2010, 5, limit=100)): (PROBES / "core_events_ucl_2010_type5.json").read_bytes(),
    }
    for type_id in (1, 2, 3, 4, 6, 7, 8, 9):
        bodies[_req_key(urls.type_events("uefa.champions", 2010, type_id, limit=100))] = (
            PROBES / f"type_events_uefa.champions_2010_t{type_id}.json"
        ).read_bytes()
    if type9_extra is not None:
        key = _req_key(urls.type_events("uefa.champions", 2010, 9, limit=100))
        body = json.loads(bodies[key])
        body["items"].append({"$ref": body["items"][0]["$ref"].replace("316520", str(type9_extra))})
        body["count"] = len(body["items"])
        bodies[key] = json.dumps(body).encode()
    return Bodies({"uefa.champions": "summary_ucl_2010.json"}, light=True, **bodies)


def _ucl_ids() -> list[int]:
    return _ids(
        PROBES / "core_events_ucl_2010_type5.json",
        *(PROBES / f"type_events_uefa.champions_2010_t{t}.json" for t in (1, 2, 3, 4, 6, 7, 8, 9)),
    )


def test_cup_with_nine_types_is_the_union_of_its_types(conn) -> None:
    ids = _ucl_ids()
    semi_final = _ids(PROBES / "type_events_uefa.champions_2010_t8.json")[0]
    client, trino = HistoryClient(_ucl_2010(type9_extra=semi_final)), HistoryTrino()

    run = _run(client, trino, conn, scope=(("uefa.champions", 2010),))

    assert len(ids) == len(set(ids)) == 213
    assert run.reason == history.IDLE and run.matches == 213
    assert set(_matches(trino)) == set(ids)
    queue = _queue(conn)
    types = {key[2]: row for key, row in queue.items() if key[:2] == ("uefa.champions", 2010)}
    assert sorted(types) == list(range(1, 10))
    assert all(row.state == history.DONE for row in types.values())
    # The semi-final listed again under the final is written once, by type 8.
    assert types[9].matches == 1 and types[8].matches == 4
    assert _summaries(client).count(_req_key(urls.summary("uefa.champions", semi_final))) == 1


def test_event_already_held_by_another_tournament_becomes_a_duplicate(conn) -> None:
    ids = _ucl_ids()
    client, trino = HistoryClient(_ucl_2010()), HistoryTrino()
    _run(client, trino, conn, scope=(("uefa.champions", 2010),))
    qualifier = _qual_2010(client)

    run = _run(client, trino, conn, scope=(("uefa.champions_qual", 2010),))

    rows = [row for row in trino._rows() if row["event_id"] == qualifier]
    assert run.matches == 1 and len(rows) == 2
    by_slug = {row["competition_slug"]: row["duplicate_of"] for row in rows}
    assert by_slug == {"uefa.champions": None, "uefa.champions_qual": "uefa.champions:2010"}
    assert len(_matches(trino)) == len(ids)


# ------------------------------------------------------------------ depth


def _one_match_season(slug: str, year: int, event_id: int, template: str, start: str, end: str) -> Bodies:
    """A season of one type and one event (list shapes of the recorded bodies)."""

    seasons = {"count": 1, "pageIndex": 1, "pageSize": 100, "pageCount": 1, "items": [
        {"$ref": f"http://sports.core.api.espn.com/v2/sports/soccer/leagues/{slug}/seasons/{year}?lang=en&region=us"}]}
    season = {"year": year, "displayName": f"{slug} {year}", "startDate": start, "endDate": end,
              "types": {"count": 1, "pageIndex": 1, "pageSize": 25, "pageCount": 1, "items": [
                  {"$ref": f"http://sports.core.api.espn.com/v2/sports/soccer/leagues/{slug}/seasons/{year}/types/1?lang=en&region=us", "id": "1", "name": "Regular Season", "startDate": start, "endDate": end}]}}
    events = {"count": 1, "pageIndex": 1, "pageSize": 100, "pageCount": 1, "items": [
        {"$ref": f"http://sports.core.api.espn.com/v2/sports/soccer/leagues/{slug}/events/{event_id}?lang=en&region=us"}]}
    return Bodies({}, **{
        _req_key(urls.league_seasons(slug, limit=100)): json.dumps(seasons).encode(),
        _req_key(urls.season(slug, year)): json.dumps(season).encode(),
        _req_key(urls.type_events(slug, year, 1, limit=100)): json.dumps(events).encode(),
        _req_key(urls.summary(slug, event_id)): (PROBES / template).read_bytes(),
    })


@pytest.mark.parametrize(
    ("slug", "year", "event_id", "template", "window", "lineup", "stats"),
    [
        # 2005: no formations, 10-11 team statistics of 28 — captured, not malformed.
        ("eng.1", 2005, 184188, "summary_eng1_2005.json",
         ("2005-07-01T04:00Z", "2006-06-01T03:59Z"), "captured", "captured"),
        # World Cup 2010: empty lineups next to full team statistics.
        ("fifa.world", 2010, 264031, "summary_fifaworld_2010.json",
         ("2010-06-01T04:00Z", "2010-07-20T03:59Z"), "valid_empty", "captured"),
    ],
)
def test_old_seasons_are_parsed_leniently(conn, slug, year, event_id, template, window, lineup, stats) -> None:
    client, trino = HistoryClient(_one_match_season(slug, year, event_id, template, *window)), HistoryTrino()

    run = _run(client, trino, conn, scope=((slug, year),))

    row = _matches(trino)[event_id]
    assert run.matches == 1 and run.failed == 0
    assert row["disposition"] != "source_malformed"
    assert (row["lineup_state"], row["team_stats_state"]) == (lineup, stats)


def _qual_2010(client) -> int:
    """uefa.champions_qual 2010 of one type listing the first qualifier of UCL 2010."""

    qualifier = _ids(PROBES / "type_events_uefa.champions_2010_t2.json")[0]
    season = json.loads((PROBES / "season_uefa.champions_2010.json").read_bytes())
    season["types"] = {**season["types"], "count": 1, "pageSize": 1, "items": season["types"]["items"][1:2]}
    events = json.loads((PROBES / "type_events_uefa.champions_2010_t2.json").read_bytes())
    events["items"], events["count"] = events["items"][:1], 1
    client.responses.update(
        {
            _req_key(urls.league_seasons("uefa.champions_qual", limit=100)): (PROBES / "seasons_uefa.champions.json").read_bytes().replace(b"uefa.champions/", b"uefa.champions_qual/"),
            _req_key(urls.season("uefa.champions_qual", 2010)): json.dumps(season).encode(),
            _req_key(urls.type_events("uefa.champions_qual", 2010, 2, limit=100)): json.dumps(events).encode(),
        }
    )
    client.responses.summaries["uefa.champions_qual"] = "summary_ucl_2010.json"
    return qualifier


def test_main_competition_written_after_its_qualifying_takes_the_match_over(conn) -> None:
    client, trino = HistoryClient(_ucl_2010()), HistoryTrino()
    qualifier = _qual_2010(client)
    _run(client, trino, conn, scope=(("uefa.champions_qual", 2010),))

    _run(client, trino, conn, scope=(("uefa.champions", 2010),))

    rows = {row["competition_slug"]: row["duplicate_of"] for row in trino._rows() if row["event_id"] == qualifier}
    assert rows == {"uefa.champions": None, "uefa.champions_qual": "uefa.champions:2010"}


def test_stored_pre_match_summary_of_a_closed_season_is_downloaded_again(conn) -> None:
    listed = _ids(*ENG_2015_PAGES)
    client, trino = HistoryClient(_eng_2015()), HistoryTrino()
    key = _req_key(urls.summary("eng.1", listed[0]))
    stub = json.loads(_summary_as("summary_eng1_2015_422285.json", listed[0], light=True))
    stub["header"]["competitions"][0]["status"]["type"]["name"] = "STATUS_SCHEDULED"
    client.stored[key] = json.dumps(stub).encode()  # what the live lane stored before kickoff

    run = _run(client, trino, conn)

    assert run.matches == 380 and key in client.network
    assert _matches(trino)[listed[0]]["status"] == "STATUS_FULL_TIME"


def test_queue_follows_the_scope_file(conn) -> None:
    bodies = _eng_2015()
    for year in range(2016, 2026):  # the other past seasons fail: their rows turn red
        bodies[_req_key(urls.season("eng.1", year))] = HttpStatusError(503, "busy")
    client, trino = HistoryClient(bodies), HistoryTrino()
    _run(client, trino, conn)
    before = len(client.network)

    # An empty allow: every past season of the last ten years joins the queue
    # (the season lists of the other target slugs fail here: red inventories).
    bodies.otherwise = HttpStatusError(503, "busy")
    wide = _run(client, trino, conn, scope=())

    queue = _queue(conn)
    assert queue[("eng.1", 0, history.ALL_SEASONS)].matches == 10
    assert {key[1] for key in queue if key[0] == "eng.1" and key[1]} == set(range(2015, 2026))
    assert all(queue[("eng.1", year, 0)].state == history.RED for year in range(2016, 2026))
    assert wide.reason == history.IDLE
    assert queue[("ger.2", 0, history.ALL_SEASONS)].state == history.RED
    # Back to the narrow allow: the red seasons outside it are not tried again.
    after = len(client.network)
    _run(client, trino, conn)
    assert len(client.network) == after and after > before
    assert queue[("eng.1", 2015, 1)].state == history.DONE


def test_allowed_season_core_does_not_list_is_a_red_season(conn) -> None:
    client, trino = HistoryClient(_eng_2015()), HistoryTrino()

    run = _run(client, trino, conn, scope=(("eng.1", 1990), ("eng.1", 2026)))

    queue = _queue(conn)
    assert run.matches == 0 and queue[("eng.1", 0, 0)].state == history.DONE
    for year in (1990, 2026):
        assert queue[("eng.1", year, 0)].state == history.RED
        assert queue[("eng.1", year, 0)].attempts == history.MAX_ATTEMPTS
    assert len(client.network) == 1  # the season list only
