"""Current-data waves of the ESPN contour (#1504).

Acceptance of #1504 on recorded answers: a wave of three tournaments publishes
two of them while the third fails; a match that leaves its day is withdrawn or
moved, never an accident.  Day bodies are assembled from the headers of
recorded Summary bodies (probes of 24.09, ``tests/fixtures/espn/probes/``)
moved onto the wave days; the Summary bodies themselves are the recorded ones.
No network, no Trino: a fake client serves bodies by address and the
in-memory Trino of the #1503 writer test also answers the wave's reads.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import re
from types import SimpleNamespace
from urllib.parse import urlencode

import pandas as pd
import pytest

from scrapers.espn import editions_store, urls, wave
from scrapers.espn.denominator import load_denominator
from scrapers.espn.editions import EditionState
from scrapers.espn.parser_contracts import PARSER_VERSION
from scrapers.espn.raw_store import RawTargetNotFound
from scrapers.espn.transport_contracts import HttpStatusError
from tests.unit.scrapers.test_espn_bronze_writer import FakeTrino
from tests.unit.scrapers.test_espn_probes import PROBES

NOW = datetime(2026, 9, 25, 13, 0, tzinfo=timezone.utc)
YESTERDAY, TODAY = date(2026, 9, 24), date(2026, 9, 25)
SLUGS = ("eng.1", "ger.2", "uefa.champions")
SUMMARIES = {
    "eng.1": "summary_eng1_2020.json",
    "ger.2": "summary_ger2_2016.json",
    "uefa.champions": "summary_ucl_2010.json",
}


def _key(url: str, params) -> str:
    return f"{url}?{urlencode(list((params or {}).items()))}"


def _req_key(request: urls.EspnRequest) -> str:
    return _key(request.url, request.params)


class FakeClient:
    """``EspnHttpClient`` surface the wave uses; bodies are served by address."""

    def __init__(self, responses: dict[str, bytes | Exception]) -> None:
        self.responses = responses
        self.stored: dict[str, bytes] = {}
        self.calls: list[tuple[str, bool]] = []
        self.replays: list[str] = []
        self.flushes = 0
        self.ledger = ()
        # Time of a download; a stored body keeps the time it was downloaded.
        self.clock = "2026-09-25T13:00:00+00:00"
        self.fetched: dict[str, str] = {}

    def _result(self, body: bytes, *, cache_hit: bool, fetched_at: str | None = None):
        sha = hashlib.sha256(body).hexdigest()
        return SimpleNamespace(
            json_data=json.loads(body),
            body=body,
            raw_uri=f"raw://{sha[:12]}",
            content_hash=sha,
            fetched_at=fetched_at or self.clock,
            cache_hit=cache_hit,
        )

    def fetch_json(self, url, endpoint, params=None, *, force_refresh=False):
        key = _key(url, params)
        self.calls.append((key, force_refresh))
        if not force_refresh and key in self.stored:
            return self._result(self.stored[key], cache_hit=True, fetched_at=self.fetched.get(key))
        value = self.responses[key]
        if isinstance(value, Exception):
            raise value
        self.stored[key] = value
        self.fetched[key] = self.clock
        return self._result(value, cache_hit=False)

    def replay_json(self, url, endpoint, params=None):
        key = _key(url, params)
        self.replays.append(key)
        if key not in self.stored:
            raise RawTargetNotFound(key)
        return self._result(self.stored[key], cache_hit=True, fetched_at=self.fetched.get(key))

    def flush(self) -> None:
        self.flushes += 1

    def network(self, fragment: str) -> list[tuple[str, bool]]:
        return [call for call in self.calls if fragment in call[0]]


def _py(value):
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        return value.item()
    return value


class WaveTrino(FakeTrino):
    """In-memory bronze that also answers the wave's reads."""

    def __init__(self, fail_slug: str | None = None, fail_table: str | None = None) -> None:
        super().__init__()
        self.fail_slug = fail_slug
        self.fail_table = fail_table
        self.queries: list[str] = []
        # ``(slug, season_year, event_id, kind, outcome)`` rows of the recheck journal.
        self.rechecks: list[tuple[str, int, int, str, str]] = []

    def insert_dataframe_atomic(self, schema, table, df, **kwargs):
        if (
            self.fail_slug
            and f"competition_slug = '{self.fail_slug}'" in kwargs["delete_filter"]
            and self.fail_table in (None, table)
        ):
            raise RuntimeError(f"commit of {table} refused")
        return super().insert_dataframe_atomic(schema, table, df, **kwargs)

    def _rows(self):
        return [{key: _py(value) for key, value in row.items()} for row in self.tables["espn_match"]]

    def execute_query(self, sql, params=None):
        self.queries.append(sql)
        if "espn_recheck_v1" in sql:
            slug, year = params
            return [[e, k, o] for s_, y, e, k, o in self.rechecks if (s_, y) == (slug, year)]
        columns = wave._MATCH_COLUMNS
        if "bool_and(terminal)" in sql:
            groups: dict[tuple[str, int], bool] = {}
            for row in self._rows():
                key = (row["competition_slug"], row["season_year"])
                groups[key] = groups.get(key, True) and bool(row["terminal"])
            return [[slug, year, terminal] for (slug, year), terminal in groups.items()]
        if "kickoff >= ? AND kickoff < ?" in sql:
            start, end = params
            keep = [row for row in self._rows() if start <= row["kickoff"] < end]
        elif "status IN" in sql:
            (before,) = params
            keep = [
                row
                for row in self._rows()
                if row["status"] in {"STATUS_POSTPONED", "STATUS_SUSPENDED"}
                and not row["terminal"]
                and row["kickoff"] < before
            ]
        else:
            slug, year = params
            ids = {int(item) for item in re.search(r"IN \(([\d, ]+)\)", sql)[1].split(", ")}
            keep = [
                row
                for row in self._rows()
                if row["competition_slug"] == slug
                and row["season_year"] == year
                and row["event_id"] in ids
            ]
        return [[row[name] for name in columns] for row in keep]


class FakeConn:
    def __init__(self, refuse: str | None = None) -> None:
        self.sql: list[str] = []
        # A statement containing ``refuse`` fails (a journal write refused).
        self.refuse = refuse

    def cursor(self):
        conn = self

        class _Cursor:
            def execute(self, sql):
                if conn.refuse is not None and conn.refuse in sql:
                    raise RuntimeError("journal write refused")
                conn.sql.append(sql)

            def fetchall(self):
                return []

            def close(self):
                pass

        return _Cursor()


# ---------------------------------------------------------------- fixtures


def _summary(slug: str) -> dict:
    return json.loads((PROBES / SUMMARIES[slug]).read_bytes())


def _event(slug: str, *, when: str, event_id: int | None = None, status: str | None = None) -> dict:
    """A scoreboard event built from the recorded Summary header of ``slug``."""

    header = _summary(slug)["header"]
    competition = header["competitions"][0]
    status_node = deepcopy(competition["status"])
    sides = []
    for side in competition["competitors"]:
        item = {
            "homeAway": side["homeAway"],
            "team": {"id": side["team"]["id"], "displayName": side["team"]["displayName"]},
        }
        if status is None:
            item["score"] = side.get("score")
        sides.append(item)
    if status is not None:
        status_node["type"]["name"] = status
    identifier = event_id or int(header["id"])
    return {
        "id": str(identifier),
        "uid": f"s:600~l:{header['league']['id']}~e:{identifier}",
        "date": when,
        "season": {"year": header["season"]["year"]},
        "status": status_node,
        "competitions": [{"competitors": sides}],
    }


def _day(*events: dict) -> bytes:
    return json.dumps({"leagues": [{}], "events": list(events)}).encode()


def _year(slug: str) -> int:
    return _summary(slug)["header"]["season"]["year"]


def _rows():
    denominator = load_denominator()
    return [denominator.row(slug) for slug in SLUGS]


def _state_path(tmp_path, slugs=SLUGS):
    path = tmp_path / "editions.json"
    editions_store.save(
        path,
        editions_store.EditionsSnapshot(
            NOW - timedelta(hours=1),
            tuple(
                EditionState(slug, _year(slug), f"{slug} test", date(2026, 7, 1), date(2027, 6, 30))
                for slug in slugs
            ),
        ),
    )
    return path


def _summary_key(slug: str, event_id: int | None = None) -> str:
    return _req_key(urls.summary(slug, event_id or int(_summary(slug)["header"]["id"])))


def _responses(yesterday: bytes, today: bytes, **extra) -> dict:
    responses = {
        _req_key(urls.all_scoreboard_day(YESTERDAY)): yesterday,
        _req_key(urls.all_scoreboard_day(TODAY)): today,
    }
    for slug in SLUGS:
        responses[_summary_key(slug)] = (PROBES / SUMMARIES[slug]).read_bytes()
    responses.update(extra)
    return responses


def _first_day() -> tuple[bytes, bytes]:
    yesterday = _day(
        _event("eng.1", when="2026-09-24T15:00Z"),
        _event("ger.2", when="2026-09-24T16:00Z"),
        _event("uefa.champions", when="2026-09-24T19:00Z"),
        _event("eng.1", when="2026-09-24T21:00Z", event_id=900002, status="STATUS_SCHEDULED"),
        _event("eng.1", when="2026-09-24T21:30Z", event_id=900003, status="STATUS_SCHEDULED"),
    )
    today = _day(
        _event("eng.1", when="2026-09-25T19:00Z", event_id=900001, status="STATUS_SCHEDULED"),
    )
    return yesterday, today


def _plan(client, trino, tmp_path, *, now=NOW, check_stale=False):
    return wave.plan_wave(
        client=client,
        trino=trino,
        rows=_rows(),
        state_path=tmp_path / "editions.json",
        now=now,
        check_stale=check_stale,
    )


def _run(plan, client, trino):
    """What the mapped tasks do: each tournament on its own, red on error."""

    outcomes = []
    for work in plan.works:
        work = wave.TournamentWork.from_xcom(json.loads(json.dumps(work.to_xcom())))
        try:
            outcome = wave.run_tournament(
                work, client=client, trino=trino, conn=FakeConn(), run_id="r", task_id="t"
            )
        except Exception as exc:  # noqa: BLE001 - the mapped task fails, the wave goes on
            outcome = wave.failed_outcome(work, exc)
        outcomes.append(outcome.as_dict())
    return outcomes


def _matches(trino) -> dict[int, dict]:
    return {row["event_id"]: {k: _py(v) for k, v in row.items()} for row in trino.tables["espn_match"]}


def _wave1(tmp_path, *, fail_slug=None, **extra):
    _state_path(tmp_path)
    client = FakeClient(_responses(*_first_day(), **extra))
    trino = WaveTrino(fail_slug)
    plan = _plan(client, trino, tmp_path)
    return client, trino, plan, _run(plan, client, trino)


# ------------------------------------------------------------------- tests


@pytest.mark.unit
def test_one_failing_tournament_does_not_stop_its_neighbours(tmp_path) -> None:
    broken_ucl = {_summary_key("uefa.champions"): b'{"header": "not a header"}'}
    client, trino, plan, outcomes = _wave1(tmp_path, fail_slug="ger.2", **broken_ucl)

    assert [(work.slug, work.season_year) for work in plan.works] == [
        ("eng.1", 2020), ("ger.2", 2016), ("uefa.champions", 2010)
    ]
    by_slug = {item["slug"]: item for item in outcomes}
    assert by_slug["ger.2"]["state"] == wave.RED
    assert by_slug["ger.2"]["first_error"] == "RuntimeError: commit of espn_match_lineup refused"
    assert by_slug["eng.1"]["state"] == by_slug["uefa.champions"]["state"] == wave.GREEN
    matches = _matches(trino)
    assert {row["competition_slug"] for row in matches.values()} == {"eng.1", "uefa.champions"}
    # A broken Summary body is a disposition of its match, not a failure.
    assert matches[307787]["disposition"] == "source_malformed"
    assert matches[578281]["disposition"] == "captured"
    assert len([r for r in trino.tables["espn_match_lineup"] if r["event_id"] == 578281]) == 40
    assert by_slug["eng.1"]["matches"] == 4  # final + three scheduled
    summary = wave.summarize_wave(outcomes, [])
    assert summary.table[1] == (
        "ger.2:2016 -> red -> RuntimeError: commit of espn_match_lineup refused"
    )
    assert summary.red and summary.reason == "1 of 3 tournaments red (> 20%)"


@pytest.mark.unit
def test_red_share_threshold_of_the_wave() -> None:
    def outcomes(red: int) -> list[dict]:
        return [
            {"slug": f"x.{i}", "season_year": 2026, "state": wave.RED if i < red else wave.GREEN,
             "matches": 1, "dispositions": {}, "first_error": "E: x" if i < red else None}
            for i in range(6)
        ]

    assert wave.summarize_wave(outcomes(1), []).red is False
    two = wave.summarize_wave(outcomes(2), [])
    assert two.red is True and two.red_tournaments == 2
    # A mapped task that ended without an outcome counts as red.
    assert wave.summarize_wave(outcomes(1), ["run_tournament[7]=failed"]).red is True
    assert wave.summarize_wave([], []).red is False
    assert wave.summarize_wave([], [], plan_error="failed").reason == "plan_wave: failed"


@pytest.mark.unit
def test_day_statuses_are_fresh_and_summaries_cache_first(tmp_path) -> None:
    client, _, _, outcomes = _wave1(tmp_path)

    assert {item["state"] for item in outcomes} == {wave.GREEN}
    # plan_wave forces both day statuses; run_tournament re-reads the stored bodies.
    for day in (YESTERDAY, TODAY):
        key = _req_key(urls.all_scoreboard_day(day))
        calls = [refresh for url, refresh in client.calls if url == key]
        assert calls[0] is True and set(calls[1:]) == {False}
    summary_calls = client.network("/summary?")
    assert sorted(url for url, _ in summary_calls) == sorted(_summary_key(s) for s in SLUGS)
    assert {refresh for _, refresh in summary_calls} == {False}
    assert _summary_key("eng.1") == (
        "https://site.web.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary?event=578281"
    )


@pytest.mark.unit
def test_captured_unchanged_match_stays_out_of_the_next_wave(tmp_path) -> None:
    client, trino, _, _ = _wave1(tmp_path)

    again = _plan(client, trino, tmp_path)

    assert again.works == ()
    assert again.status_checks == 0


@pytest.mark.unit
def test_status_change_of_a_captured_final_replays_its_summary(tmp_path) -> None:
    client, trino, _, _ = _wave1(tmp_path)
    yesterday = json.loads(_first_day()[0])
    yesterday["events"][0]["status"]["type"]["name"] = "STATUS_FINAL_AET"
    client.responses[_req_key(urls.all_scoreboard_day(YESTERDAY))] = json.dumps(yesterday).encode()
    before = len(client.network("/summary?"))

    plan = _plan(client, trino, tmp_path)
    (outcome,) = _run(plan, client, trino)

    assert [(w.slug, w.event_ids) for w in plan.works] == [("eng.1", (578281,))]
    assert outcome["state"] == wave.GREEN
    assert client.replays == [_summary_key("eng.1")]
    assert len(client.network("/summary?")) == before
    row = _matches(trino)[578281]
    assert row["status"] == "STATUS_FINAL_AET" and row["lineup_state"] == "captured"
    assert len([r for r in trino.tables["espn_match_lineup"] if r["event_id"] == 578281]) == 40


@pytest.mark.unit
def test_match_leaving_its_day_is_withdrawn_or_moved(tmp_path) -> None:
    client, trino, _, _ = _wave1(tmp_path)
    yesterday, _ = _first_day()
    # 900001 (today) and 900002 (yesterday) are gone; 900003 moved to today.
    client.responses[_req_key(urls.all_scoreboard_day(YESTERDAY))] = _day(
        *[e for e in json.loads(yesterday)["events"] if int(e["id"]) not in {900002, 900003}]
    )
    client.responses[_req_key(urls.all_scoreboard_day(TODAY))] = _day(
        _event("eng.1", when="2026-09-25T20:00Z", event_id=900003, status="STATUS_SCHEDULED")
    )
    client.responses[_req_key(urls.event_status("eng.1", 900001))] = HttpStatusError(404, "gone")
    client.responses[_req_key(urls.event_status("eng.1", 900002))] = json.dumps(
        {"type": {"name": "STATUS_POSTPONED"}}
    ).encode()

    plan = _plan(client, trino, tmp_path)
    outcomes = _run(plan, client, trino)

    (work,) = plan.works
    assert work.presence == {900001: "withdrawn", 900002: "moved", 900003: "moved"}
    assert work.statuses == {900002: "STATUS_POSTPONED"}
    rows = _matches(trino)
    assert rows[900001]["disposition"] == "withdrawn"
    assert rows[900001]["kickoff"] == datetime(2026, 9, 25, 19)
    assert rows[900002]["disposition"] == "moved"
    assert rows[900002]["status"] == "STATUS_POSTPONED"
    assert rows[900002]["kickoff"] == datetime(2026, 9, 24, 21)
    assert rows[900003]["disposition"] == "moved"
    assert rows[900003]["kickoff"] == datetime(2026, 9, 25, 20)
    summary = wave.summarize_wave(outcomes, [])
    assert (summary.withdrawn_count, summary.moved_count, summary.red) == (1, 2, False)
    # Marked matches are not checked again every wave.
    assert _plan(client, trino, tmp_path).status_checks == 0


@pytest.mark.unit
def test_many_withdrawn_matches_warn_without_failing() -> None:
    outcomes = [
        {"slug": "eng.1", "season_year": 2026, "state": wave.GREEN, "matches": 9,
         "dispositions": {"withdrawn": 9}, "first_error": None}
    ]

    summary = wave.summarize_wave(outcomes, [])

    assert summary.red is False
    assert summary.warnings == ("9 withdrawn matches in one wave (alert above 8)",)
    assert wave.summarize_wave(
        [{**outcomes[0], "dispositions": {"withdrawn": 8}}], []
    ).warnings == ()


@pytest.mark.unit
def test_day_cut_at_1000_events_is_topped_up_by_league(tmp_path) -> None:
    _state_path(tmp_path)
    filler = [
        {"id": str(10_000 + i), "uid": f"s:600~l:99999~e:{10_000 + i}"} for i in range(999)
    ]
    final = _event("eng.1", when="2026-09-24T15:00Z")
    cut = json.dumps({"leagues": [{}], "events": filler + [{}]}).encode()
    league = json.dumps({"leagues": [{"id": "700", "slug": "eng.1"}], "events": [final]}).encode()
    extra = {_req_key(urls.league_scoreboard_day("eng.1", YESTERDAY)): league}
    for slug, espn_id in (("ger.2", "3927"), ("uefa.champions", "775")):
        extra[_req_key(urls.league_scoreboard_day(slug, YESTERDAY))] = json.dumps(
            {"leagues": [{"id": espn_id, "slug": slug}], "events": []}
        ).encode()
    client = FakeClient(_responses(cut, _day(), **extra))
    trino = WaveTrino()

    plan = _plan(client, trino, tmp_path)
    outcomes = _run(plan, client, trino)

    assert plan.topup_days == (YESTERDAY,)
    league_calls = client.network("/scoreboard?dates=20260924&limit=1000")
    assert sorted(url.split("/soccer/")[1].split("/")[0] for url, refresh in league_calls if refresh) == [
        "all", "eng.1", "ger.2", "uefa.champions"
    ]
    assert [(w.slug, w.event_ids) for w in plan.works] == [("eng.1", (578281,))]
    assert outcomes[0]["state"] == wave.GREEN
    assert _matches(trino)[578281]["disposition"] == "captured"


@pytest.mark.unit
def test_plan_on_the_recorded_all_scoreboard_day(tmp_path) -> None:
    denominator = load_denominator()
    rows = wave.live_rows(denominator)
    assert len(rows) == 161
    editions_store.save(
        tmp_path / "editions.json",
        editions_store.EditionsSnapshot(
            NOW,
            tuple(
                EditionState(row.slug, year, f"{year} {row.slug}", date(year, 1, 1), date(year + 1, 12, 31))
                for row in rows
                for year in (2025, 2026)
            ),
        ),
    )
    responses = {
        _req_key(urls.all_scoreboard_day(date(2026, 9, 22))): _day(),
        _req_key(urls.all_scoreboard_day(date(2026, 9, 23))): (
            PROBES / "all_scoreboard_20260923.json"
        ).read_bytes(),
    }

    plan = wave.plan_wave(
        client=FakeClient(responses),
        trino=WaveTrino(),
        rows=rows,
        state_path=tmp_path / "editions.json",
        now=datetime(2026, 9, 23, 20, tzinfo=timezone.utc),
        check_stale=False,
    )

    counts = {work.slug: len(work.event_ids) for work in plan.works}
    assert counts == {
        "ned.cup": 6, "concacaf.nations.league": 3, "chi.copa_chi": 3,
        "slv.1": 2, "global.gulf_cup": 2, "usa.1": 1, "usa.usl.1": 1,
        "eng.fa_qual": 1, "sco.challenge": 1, "col.1": 1, "per.1": 1, "gua.1": 1,
    }


@pytest.mark.unit
def test_stale_postponed_match_is_checked_in_the_midnight_wave(tmp_path) -> None:
    client, trino, _, _ = _wave1(tmp_path)
    later = NOW + timedelta(days=5)
    for row in trino.tables["espn_match"]:
        if row["event_id"] == 900002:
            row["status"] = "STATUS_POSTPONED"
    status_key = _req_key(urls.event_status("eng.1", 900002))
    client.responses[status_key] = json.dumps({"type": {"name": "STATUS_CANCELED"}}).encode()
    days = {
        _req_key(urls.all_scoreboard_day(later.date() - timedelta(days=1))): _day(),
        _req_key(urls.all_scoreboard_day(later.date())): _day(),
    }
    client.responses.update(days)
    # Editions of the cache are older than 24 h by then: core is read again.
    for slug in SLUGS:
        client.responses[_req_key(urls.league_detail(slug))] = HttpStatusError(503, "busy")

    assert _plan(client, trino, tmp_path, now=later, check_stale=False).works == ()
    plan = _plan(client, trino, tmp_path, now=later, check_stale=True)
    _run(plan, client, trino)

    assert plan.works[0].statuses == {900002: "STATUS_CANCELED"}
    row = _matches(trino)[900002]
    assert (row["status"], row["terminal"], row["disposition"]) == ("STATUS_CANCELED", True, "moved")


@pytest.mark.unit
def test_batch_cut_after_a_child_commit_is_written_again_next_wave(tmp_path) -> None:
    # The match row is committed last: a batch that fails on its events
    # leaves no captured match row, so the next wave takes the final again.
    _state_path(tmp_path)
    client = FakeClient(_responses(*_first_day()))
    trino = WaveTrino("ger.2", fail_table="espn_match_events")
    first = _run(_plan(client, trino, tmp_path), client, trino)

    assert {o["slug"]: o["state"] for o in first}["ger.2"] == wave.RED
    assert 456996 not in _matches(trino)
    assert any(r["event_id"] == 456996 for r in trino.tables["espn_match_lineup"])

    trino.fail_slug = None
    again = _plan(client, trino, tmp_path)
    assert [(w.slug, w.event_ids) for w in again.works] == [("ger.2", (456996,))]
    _run(again, client, trino)
    assert _matches(trino)[456996]["lineup_state"] == "captured"


@pytest.mark.unit
def test_one_broken_event_of_a_day_reds_only_its_tournament(tmp_path) -> None:
    _state_path(tmp_path)
    yesterday = json.loads(_first_day()[0])
    broken = next(e for e in yesterday["events"] if int(e["id"]) == 456996)
    broken["competitions"] = [{"competitors": []}]  # the day parser rejects it
    client = FakeClient(_responses(json.dumps(yesterday).encode(), _first_day()[1]))
    trino = WaveTrino()

    plan = _plan(client, trino, tmp_path)
    outcomes = {o["slug"]: o for o in _run(plan, client, trino)}

    assert outcomes["ger.2"]["state"] == wave.RED
    assert outcomes["ger.2"]["first_error"].startswith(
        "WavePlanError: 2026-09-24: EspnParseError: event[456996] must have exactly two"
    )
    assert outcomes["eng.1"]["state"] == outcomes["uefa.champions"]["state"] == wave.GREEN
    assert {r["competition_slug"] for r in _matches(trino).values()} == {"eng.1", "uefa.champions"}


@pytest.mark.unit
def test_league_without_any_edition_is_red_until_core_answers(tmp_path) -> None:
    path = tmp_path / "editions.json"
    detail = {
        _req_key(urls.league_detail(slug)): HttpStatusError(503, "busy") for slug in SLUGS
    }
    client = FakeClient(_responses(*_first_day(), **detail))
    trino = WaveTrino()

    plan = _plan(client, trino, tmp_path)
    summary = wave.summarize_wave(_run(plan, client, trino), [])

    assert [(w.slug, w.error) for w in plan.works] == [
        (slug, "no open edition: HttpStatusError: busy") for slug in SLUGS
    ]
    assert summary.red and summary.red_tournaments == 3
    assert editions_store.load(path).failed.keys() == set(SLUGS)
    # A fresh snapshot still reads the failed leagues again.
    calls = len(client.network("/leagues/"))
    _plan(client, trino, tmp_path)
    assert len(client.network("/leagues/")) == calls + 3


@pytest.mark.unit
def test_cut_day_with_a_failed_league_top_up_reds_that_league(tmp_path) -> None:
    _state_path(tmp_path)
    filler = [{"id": str(10_000 + i), "uid": f"s:600~l:99999~e:{10_000 + i}"} for i in range(1000)]
    cut = json.dumps({"leagues": [{}], "events": filler}).encode()
    extra = {
        _req_key(urls.league_scoreboard_day("eng.1", YESTERDAY)): HttpStatusError(500, "down"),
    }
    for slug, espn_id in (("ger.2", "3927"), ("uefa.champions", "775")):
        extra[_req_key(urls.league_scoreboard_day(slug, YESTERDAY))] = json.dumps(
            {"leagues": [{"id": espn_id, "slug": slug}], "events": []}
        ).encode()
    client = FakeClient(_responses(cut, _day(), **extra))

    plan = _plan(client, WaveTrino(), tmp_path)

    assert [(w.slug, w.error) for w in plan.works] == [
        ("eng.1", "2026-09-24: HttpStatusError: down")
    ]


@pytest.mark.unit
def test_stale_match_that_core_reports_played_is_written_with_its_summary(tmp_path) -> None:
    client, trino, _, _ = _wave1(tmp_path)
    later = NOW + timedelta(days=5)
    for row in trino.tables["espn_match"]:
        if row["event_id"] == 900002:
            row["status"] = "STATUS_POSTPONED"
    client.responses[_req_key(urls.event_status("eng.1", 900002))] = json.dumps(
        {"type": {"name": "STATUS_FULL_TIME"}}
    ).encode()
    # The recorded eng.1 Summary (Arsenal 2-0 Brighton) stands for the match.
    body = json.loads((PROBES / SUMMARIES["eng.1"]).read_bytes())
    body["header"]["id"] = "900002"
    client.responses[_summary_key("eng.1", 900002)] = json.dumps(body).encode()
    client.responses.update({
        _req_key(urls.all_scoreboard_day(later.date() - timedelta(days=1))): _day(),
        _req_key(urls.all_scoreboard_day(later.date())): _day(),
    })
    for slug in SLUGS:
        client.responses[_req_key(urls.league_detail(slug))] = HttpStatusError(503, "busy")

    plan = _plan(client, trino, tmp_path, now=later, check_stale=True)
    (outcome,) = [o for o in _run(plan, client, trino) if o["slug"] == "eng.1"]

    (work,) = [w for w in plan.works if w.slug == "eng.1"]
    assert work.statuses == {900002: "STATUS_FULL_TIME"} and work.presence == {}
    assert outcome["state"] == wave.GREEN
    row = _matches(trino)[900002]
    assert (row["status"], row["played_final"], row["home_score"], row["away_score"]) == (
        "STATUS_FULL_TIME", True, 2, 0
    )
    assert row["lineup_state"] == "captured"


@pytest.mark.unit
@pytest.mark.parametrize(
    "body",
    [
        b'{"events": []}',  # no root league: the body itself is broken
        b"[]",  # not an object
        # An event no league owns cannot be attributed to a tournament.
        json.dumps({"leagues": [{}], "events": [{"id": "1", "uid": "s:600~e:1"}]}).encode(),
    ],
)
def test_broken_day_body_fails_the_plan_instead_of_an_empty_day(tmp_path, body) -> None:
    from scrapers.espn.parser_common import EspnParseError

    _state_path(tmp_path)
    client = FakeClient(_responses(body, _day()))

    with pytest.raises(EspnParseError):
        _plan(client, WaveTrino(), tmp_path)


@pytest.mark.unit
def test_failed_status_check_reds_the_tournament_after_its_matches_publish(tmp_path) -> None:
    client, trino, _, _ = _wave1(tmp_path)
    yesterday = json.loads(_first_day()[0])
    for event in yesterday["events"]:
        if int(event["id"]) == 900003:
            event["date"] = "2026-09-24T22:00Z"  # kickoff changed: planned again
    client.responses[_req_key(urls.all_scoreboard_day(YESTERDAY))] = _day(
        *[e for e in yesterday["events"] if int(e["id"]) != 900002]
    )
    client.responses[_req_key(urls.event_status("eng.1", 900002))] = HttpStatusError(503, "busy")

    plan = _plan(client, trino, tmp_path)
    (outcome,) = _run(plan, client, trino)

    (work,) = plan.works
    assert (work.slug, work.event_ids, work.error) == (
        "eng.1", (900003,), "status of 900002: HttpStatusError: busy"
    )
    assert outcome["state"] == wave.RED
    assert outcome["first_error"] == "WavePlanError: status of 900002: HttpStatusError: busy"
    rows = _matches(trino)
    assert rows[900003]["kickoff"] == datetime(2026, 9, 24, 22)
    assert rows[900002]["disposition"] is None  # left unmarked, checked again next wave


@pytest.mark.unit
def test_single_403_is_the_tournament_error_and_all_blocked_stops_the_wave(tmp_path) -> None:
    from scrapers.espn.transport_contracts import AllOriginsBlocked, OriginBlocked

    client, trino, _, _ = _wave1(tmp_path)
    client.responses[_req_key(urls.all_scoreboard_day(TODAY))] = _day()  # 900001 gone
    status_key = _req_key(urls.event_status("eng.1", 900001))
    client.responses[status_key] = OriginBlocked("403 core")

    (work,) = _plan(client, trino, tmp_path).works
    assert (work.slug, work.error) == ("eng.1", "status of 900001: OriginBlocked: 403 core")

    client.responses[status_key] = AllOriginsBlocked("every origin 403")
    with pytest.raises(AllOriginsBlocked):
        _plan(client, trino, tmp_path)


# ------------------------------------------------------- freshness meter (#1505)


@pytest.mark.unit
def test_status_checked_at_and_first_published_at_are_written_and_carried(tmp_path) -> None:
    client, trino, _, _ = _wave1(tmp_path)
    rows = _matches(trino)
    fetched = datetime(2026, 9, 25, 13)  # fetched_at of the recorded day bodies
    assert rows[578281]["status_checked_at"] == fetched
    assert rows[900001]["status_checked_at"] == fetched
    first = rows[578281]["first_published_at"]
    # Stamped just before the match commit: not before the batch stamp.
    assert first is not None and first >= rows[578281]["_ingested_at"]
    assert rows[900001]["first_published_at"] is None  # not played yet
    # An older first publication stays through a republication of the match.
    old = datetime(2026, 9, 24, 23)
    for row in trino.tables["espn_match"]:
        if row["event_id"] == 578281:
            row["first_published_at"] = old
    yesterday = json.loads(_first_day()[0])
    yesterday["events"][0]["status"]["type"]["name"] = "STATUS_FINAL_AET"
    client.responses[_req_key(urls.all_scoreboard_day(YESTERDAY))] = json.dumps(yesterday).encode()

    plan = _plan(client, trino, tmp_path)
    _run(plan, client, trino)

    assert [(w.slug, w.event_ids) for w in plan.works] == [("eng.1", (578281,))]
    row = _matches(trino)[578281]
    assert row["status"] == "STATUS_FINAL_AET"
    assert row["first_published_at"] == old
    assert row["_ingested_at"] > old


@pytest.mark.unit
def test_status_read_before_kickoff_is_read_again_once_after_it(tmp_path) -> None:
    client, trino, _, _ = _wave1(tmp_path)
    # 900002 (kickoff 24.09 21:00) was last read before its kickoff.
    for row in trino.tables["espn_match"]:
        if row["event_id"] == 900002:
            row["status_checked_at"] = datetime(2026, 9, 24, 12)

    plan = _plan(client, trino, tmp_path)
    _run(plan, client, trino)

    # 900001 (kickoff 25.09 19:00) is not due yet: it stays out.
    assert [(w.slug, w.event_ids) for w in plan.works] == [("eng.1", (900002,))]
    assert _matches(trino)[900002]["status_checked_at"] == datetime(2026, 9, 25, 13)
    assert _plan(client, trino, tmp_path).works == ()


def _core_list(*event_ids: int) -> bytes:
    items = [
        {"$ref": f"http://sports.core.api.espn.com/v2/sports/soccer/leagues/x/events/{event_id}"}
        for event_id in event_ids
    ]
    if not items:
        return json.dumps({"count": 0, "pageIndex": 0, "pageSize": 1000, "pageCount": 0, "items": []}).encode()
    return json.dumps(
        {"count": len(items), "pageIndex": 1, "pageSize": 1000, "pageCount": 1, "items": items}
    ).encode()


@pytest.mark.unit
def test_midnight_wave_adds_an_event_core_lists_and_bronze_lacks(tmp_path) -> None:
    client, trino, _, _ = _wave1(tmp_path)
    first = TODAY - timedelta(days=2)
    body = _summary("eng.1")
    body["header"]["id"] = "900009"
    league = lambda slug, espn_id, *events: json.dumps(  # noqa: E731
        {"leagues": [{"id": espn_id, "slug": slug}], "events": list(events)}
    ).encode()
    client.responses.update({
        # 578281 is already in bronze; 900009 is on no fetched day.
        _req_key(urls.events_window("eng.1", first, TODAY)): _core_list(578281, 900009),
        _req_key(urls.events_window("ger.2", first, TODAY)): _core_list(),
        # 777777: core lists it, no league day has it.
        _req_key(urls.events_window("uefa.champions", first, TODAY)): _core_list(777777),
        _req_key(urls.league_scoreboard_day("eng.1", first)): league(
            "eng.1", "700", _event("eng.1", when="2026-09-23T18:00Z", event_id=900009)
        ),
        _summary_key("eng.1", 900009): json.dumps(body).encode(),
    })
    for day in (first, YESTERDAY, TODAY):
        client.responses[_req_key(urls.league_scoreboard_day("uefa.champions", day))] = league(
            "uefa.champions", "775"
        )

    assert _plan(client, trino, tmp_path).works == ()  # not the 00 wave: no core read
    plan = wave.plan_wave(
        client=client, trino=trino, rows=_rows(), state_path=tmp_path / "editions.json",
        now=NOW, check_stale=False, check_core=True,
    )
    outcomes = {o["slug"]: o for o in _run(plan, client, trino)}

    works = {w.slug: w for w in plan.works}
    assert works["eng.1"].event_ids == (900009,)
    assert works["eng.1"].topup_days == (first,)
    assert works["uefa.champions"].error == (
        "core lists 1 event(s) no league day has: 777777"
    )
    assert "ger.2" not in works
    assert outcomes["eng.1"]["state"] == wave.GREEN
    assert outcomes["uefa.champions"]["state"] == wave.RED
    row = _matches(trino)[900009]
    assert (row["played_final"], row["lineup_state"]) == (True, "captured")
    assert row["first_published_at"] is not None
    core_calls = client.network("/events?dates=20260923-20260925")
    assert len(core_calls) == 3 and {refresh for _, refresh in core_calls} == {True}


@pytest.mark.unit
def test_failed_summary_download_keeps_the_final_in_the_denominator(tmp_path) -> None:
    """#1505 (Astra r1 p.1): the match row lands pending, the tournament is red."""
    busy = {_summary_key("ger.2"): HttpStatusError(503, "busy")}
    client, trino, plan, outcomes = _wave1(tmp_path, **busy)

    by_slug = {o["slug"]: o for o in outcomes}
    assert by_slug["ger.2"]["state"] == wave.RED
    assert by_slug["ger.2"]["first_error"].startswith(
        "SummaryFetchError: 1 Summary download(s) failed, first: summary of 456996: HttpStatusError"
    )
    row = _matches(trino)[456996]
    assert (row["played_final"], row["lineup_state"], row["first_published_at"]) == (
        True, "pending", None
    )
    assert not [r for r in trino.tables["espn_match_lineup"] if r["event_id"] == 456996]
    # Next wave takes the final again with its Summary.
    client.responses[_summary_key("ger.2")] = (PROBES / SUMMARIES["ger.2"]).read_bytes()
    again = _plan(client, trino, tmp_path)
    assert [(w.slug, w.event_ids) for w in again.works] == [("ger.2", (456996,))]
    _run(again, client, trino)
    assert _matches(trino)[456996]["lineup_state"] == "captured"


# ------------------------------------------------------ recheck key (#1506)


@pytest.mark.unit
def test_shootout_change_alone_brings_the_final_back_once(tmp_path) -> None:
    client, trino, _, _ = _wave1(tmp_path)
    yesterday = json.loads(_first_day()[0])
    for side, score in zip(yesterday["events"][0]["competitions"][0]["competitors"], (4, 3)):
        side["shootoutScore"] = score
    client.responses[_req_key(urls.all_scoreboard_day(YESTERDAY))] = json.dumps(yesterday).encode()
    downloads = len(client.network("/summary?"))

    plan = _plan(client, trino, tmp_path)
    _run(plan, client, trino)

    assert [(w.slug, w.event_ids) for w in plan.works] == [("eng.1", (578281,))]
    row = _matches(trino)[578281]
    assert (row["home_shootout"], row["away_shootout"]) == (4, 3)
    assert row["lineup_state"] == "captured"
    assert len(client.network("/summary?")) == downloads  # replayed from the raw store
    # Written with the day's shootout: the next wave has nothing to do.
    assert _plan(client, trino, tmp_path).works == ()


@pytest.mark.unit
def test_new_parser_version_downloads_nothing(tmp_path) -> None:
    client, trino, _, _ = _wave1(tmp_path)
    for row in trino.tables["espn_match"]:
        row["parser_version"] = "espn-native-parser-v4"
    downloads = len(client.network("/summary?"))

    assert _plan(client, trino, tmp_path).works == ()  # not a reason to fetch

    yesterday = json.loads(_first_day()[0])
    yesterday["events"][0]["status"]["type"]["name"] = "STATUS_FINAL_AET"
    client.responses[_req_key(urls.all_scoreboard_day(YESTERDAY))] = json.dumps(yesterday).encode()
    _run(_plan(client, trino, tmp_path), client, trino)

    assert len(client.network("/summary?")) == downloads
    assert client.replays == [_summary_key("eng.1")]
    assert _matches(trino)[578281]["parser_version"] == PARSER_VERSION
