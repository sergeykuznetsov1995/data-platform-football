"""Recheck and the "not worse" rule of the ESPN contour (#1506).

Acceptance of #1506 on recorded answers: a full Summary (``summary_eng1_2020``)
answered again cut (no player or team statistics, as bra.copa_do_brazil on
13.08) keeps the stored parse and journals ``downgrade_rejected``; an empty
match that ESPN completed later is filled; a complete match is never a recheck
candidate (the candidate SQL runs on DuckDB).  No network, no Trino: the fakes
of the wave test, plus a raw store that returns stored bodies by hash.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import re

import pytest

from scrapers.espn import recheck, urls, wave
from scrapers.espn.transport_contracts import HttpStatusError
from tests.unit.scrapers.test_espn_wave import (
    NOW,
    PROBES,
    SUMMARIES,
    YESTERDAY,
    FakeClient,
    FakeConn,
    WaveTrino,
    _first_day,
    _matches,
    _plan,
    _req_key,
    _responses,
    _rows,
    _state_path,
    _summary_key,
)

pytestmark = pytest.mark.unit

EVENT = 578281  # eng.1 2020, Arsenal 2-0 Brighton: 40 players, 28 + 28 team statistics


class RawClient(FakeClient):
    """The fake client with a raw store: every body it served, by hash."""

    def __init__(self, responses) -> None:
        super().__init__(responses)
        self.blobs: dict[str, bytes] = {}
        self.exact: list[str] = []
        client = self

        class _Store:
            def load_exact(self, raw_uri, content_hash):
                assert raw_uri == f"raw://{content_hash[:12]}"
                client.exact.append(content_hash)
                return client.blobs[content_hash]

        self.raw_store = _Store()

    def _result(self, body, *, cache_hit):
        result = super()._result(body, cache_hit=cache_hit)
        self.blobs[result.content_hash] = body
        return result


def _body(mutate=None) -> bytes:
    document = json.loads((PROBES / SUMMARIES["eng.1"]).read_bytes())
    if mutate is not None:
        mutate(document)
    return json.dumps(document).encode()


def _cut(document) -> None:
    """ESPN's cut answer: the players stay, their statistics and the team ones go."""

    for roster in document["rosters"]:
        for player in roster["roster"]:
            player["stats"] = []
    for team in document["boxscore"]["teams"]:
        team["statistics"] = []


def _empty(document) -> None:
    for roster in document["rosters"]:
        roster["roster"] = []
    for team in document["boxscore"]["teams"]:
        team["statistics"] = []


def _edit(document) -> None:
    """A corrected value at the same completeness (R-23: 2–7 % of rechecks)."""

    stats = document["rosters"][0]["roster"][0]["stats"]
    stat = next(item for item in stats if item["name"] == "foulsCommitted")
    stat["value"], stat["displayValue"] = 3.0, "3"


def _captured(tmp_path, first_body: bytes | None = None):
    """First wave: every final captured (eng.1 with ``first_body`` when given)."""

    _state_path(tmp_path)
    extra = {_summary_key("eng.1"): first_body} if first_body is not None else {}
    client = RawClient(_responses(*_first_day(), **extra))
    trino = WaveTrino()
    _execute(_plan(client, trino, tmp_path), client, trino)
    return client, trino


_JOURNAL_ROW = r"'([^']*)', (\d+), (\d+), '([a-z_0-9]+)'"


def _execute(plan, client, trino):
    conns, outcomes = [], []
    for work in plan.works:
        work = wave.TournamentWork.from_xcom(json.loads(json.dumps(work.to_xcom())))
        conn = FakeConn()
        conns.append(conn)
        try:
            outcome = wave.run_tournament(
                work, client=client, trino=trino, conn=conn, run_id="r", task_id="t"
            )
        except Exception as exc:  # noqa: BLE001 - the mapped task fails, the wave goes on
            outcome = wave.failed_outcome(work, exc)
        outcomes.append(outcome.as_dict())
    journal = [sql for conn in conns for sql in conn.sql if recheck.RECHECK_TABLE in sql]
    # What the journal now holds, for the next run's ``recheck.journalled``.
    for sql in journal:
        for slug, year, event_id, kind in re.findall(_JOURNAL_ROW, sql):
            trino.rechecks.append((slug, int(year), int(event_id), kind))
    return outcomes, journal


def _recheck_wave(monkeypatch, client, trino, tmp_path, body, kind=recheck.RECHECK):
    """The 00 wave with ``EVENT`` due for ``kind``; ESPN now answers ``body``."""

    return _execute(_recheck_plan(monkeypatch, client, trino, tmp_path, body, kind), client, trino)


def _recheck_plan(monkeypatch, client, trino, tmp_path, body, kind=recheck.RECHECK):

    client.responses[_summary_key("eng.1")] = body
    monkeypatch.setattr(
        recheck, "plan_recheck", lambda trino, now: {EVENT: ("eng.1", 2020, kind)}
    )
    plan = wave.plan_wave(
        client=client, trino=trino, rows=_rows(), state_path=tmp_path / "editions.json",
        now=NOW, check_stale=False, check_recheck=True,
    )
    assert [(w.slug, w.event_ids, dict(w.rechecks)) for w in plan.works] == [
        ("eng.1", (EVENT,), {EVENT: kind})
    ]
    return plan


def _children(trino, table) -> list[dict]:
    return [row for row in trino.tables[table] if row["event_id"] == EVENT]


def _with_stats(trino) -> int:
    return sum(
        row["fouls_committed"] is not None or row["appearances"] is not None
        for row in _children(trino, "espn_match_lineup")
    )


# ------------------------------------------------------------ criterion 1


def test_full_then_cut_answer_keeps_the_stored_parse(tmp_path, monkeypatch) -> None:
    client, trino = _captured(tmp_path)
    before = _matches(trino)[EVENT]
    assert (_with_stats(trino), len(_children(trino, "espn_team_stats"))) == (40, 2)

    outcomes, journal = _recheck_wave(monkeypatch, client, trino, tmp_path, _body(_cut))

    assert outcomes[0]["state"] == wave.GREEN
    after = _matches(trino)[EVENT]
    # The richer stored parse stays, read back from its exact raw body.
    assert _with_stats(trino) == 40
    assert len(_children(trino, "espn_team_stats")) == 2
    assert (after["raw_uri"], after["raw_sha256"]) == (before["raw_uri"], before["raw_sha256"])
    assert client.exact == [before["raw_sha256"]]
    assert after["rechecked_at"] is not None
    assert after["first_published_at"] == before["first_published_at"]
    # One download from the network, forced past the cache.
    assert client.network("/summary?event=578281")[-1] == (_summary_key("eng.1"), True)
    (sql,) = journal
    assert "'downgrade_rejected'" in sql and "'recheck'" in sql and "578281" in sql
    assert '"player_stats": 40' in sql and '"player_stats": 0' in sql
    assert '"team_stats": 56' in sql and '"team_stats": 0' in sql


def test_compare_parts_on_the_recorded_summary(tmp_path) -> None:
    client, trino = _captured(tmp_path)
    known = wave.bronze_matches(trino, "eng.1", 2020, [EVENT])[EVENT]
    work = wave.TournamentWork(
        "eng.1", 2020, (), 700, "Premier League", "x", date(2026, 7, 1), date(2027, 6, 30), ()
    )
    competition, edition = work.context()
    schedule = known.schedule_row(competition, edition)

    def parse(body):
        return wave.parse_summary(body, competition=competition, edition=edition, event=schedule)

    full, cut, empty, edited = (parse(_body(m)) for m in (None, _cut, _empty, _edit))
    assert recheck.summary_parts(full) == {
        "lineup": 40, "player_stats": 40, "team_stats": 56, "events": 98,
    }
    assert recheck.compare_parts(full, cut) == recheck.DOWNGRADE_REJECTED
    assert recheck.compare_parts(cut, full) == recheck.FILLED
    assert recheck.compare_parts(empty, full) == recheck.FILLED
    assert recheck.compare_parts(full, empty) == recheck.DOWNGRADE_REJECTED
    assert recheck.compare_parts(full, parse(_body())) == recheck.SAME
    assert recheck.compare_parts(full, edited) == recheck.CHANGED


def test_empty_match_completed_later_is_filled(tmp_path, monkeypatch) -> None:
    client, trino = _captured(tmp_path, _body(_empty))
    assert _matches(trino)[EVENT]["disposition"] == "valid_empty"

    _, journal = _recheck_wave(monkeypatch, client, trino, tmp_path, _body())

    row = _matches(trino)[EVENT]
    assert (row["disposition"], row["lineup_state"], row["team_stats_state"]) == (
        "captured", "captured", "captured"
    )
    assert _with_stats(trino) == 40 and row["rechecked_at"] is not None
    assert "'filled'" in journal[0]


def test_edited_value_at_the_same_completeness_is_written(tmp_path, monkeypatch) -> None:
    client, trino = _captured(tmp_path)
    athlete = json.loads(_body())["rosters"][0]["roster"][0]["athlete"]["id"]

    _, journal = _recheck_wave(monkeypatch, client, trino, tmp_path, _body(_edit))

    (player,) = [
        row for row in _children(trino, "espn_match_lineup") if row["athlete_id"] == int(athlete)
    ]
    assert player["fouls_committed"] == 3.0
    assert "'changed'" in journal[0]


def test_failed_recheck_is_red_keeps_the_match_and_is_not_repeated(tmp_path, monkeypatch) -> None:
    client, trino = _captured(tmp_path)
    plan = _recheck_plan(monkeypatch, client, trino, tmp_path, HttpStatusError(503, "busy"))
    downloads = len(client.network("/summary?"))

    outcomes, journal = _execute(plan, client, trino)

    # The failure is the tournament's: red, so the alarm of a red tournament fires.
    assert outcomes[0]["state"] == wave.RED
    assert "SummaryFetchError" in outcomes[0]["first_error"]
    assert "recheck of 578281: HttpStatusError" in outcomes[0]["first_error"]
    row = _matches(trino)[EVENT]
    assert row["lineup_state"] == "captured" and _with_stats(trino) == 40
    assert row["rechecked_at"] is not None
    assert "'failed'" in journal[0] and "NULL" in journal[0]
    assert len(client.network("/summary?")) == downloads + 1

    # The task retries from the same plan: no second download, no second row.
    client.responses[_summary_key("eng.1")] = _body()
    outcomes, journal = _execute(plan, client, trino)

    assert outcomes[0]["state"] == wave.GREEN and journal == []
    assert len(client.network("/summary?")) == downloads + 1
    assert _matches(trino)[EVENT]["rechecked_at"] == row["rechecked_at"]


@pytest.mark.parametrize("kind", [recheck.RECHECK, recheck.SAMPLE_24H])
def test_retry_never_downloads_a_journalled_recheck_again(tmp_path, monkeypatch, kind) -> None:
    """Retries of the task plan from the same XCom: each forced download is
    made once (the recheck by ``rechecked_at`` and the journal, the sample by
    the journal)."""

    client, trino = _captured(tmp_path)
    plan = _recheck_plan(monkeypatch, client, trino, tmp_path, _body(_edit), kind)
    downloads = len(client.network("/summary?"))
    _, first = _execute(plan, client, trino)
    row = _matches(trino)[EVENT]

    for _attempt in range(2):
        _, journal = _execute(plan, client, trino)
        assert journal == []

    assert len(first) == 1 and len(client.network("/summary?")) == downloads + 1
    after = _matches(trino)[EVENT]
    assert (after["raw_sha256"], after["rechecked_at"]) == (row["raw_sha256"], row["rechecked_at"])


def test_recheck_already_stamped_is_not_downloaded_even_without_journal(
    tmp_path, monkeypatch
) -> None:
    """The journal written after bronze can be missing: ``rechecked_at`` is enough."""

    client, trino = _captured(tmp_path)
    plan = _recheck_plan(monkeypatch, client, trino, tmp_path, _body())
    _execute(plan, client, trino)
    trino.rechecks.clear()
    downloads = len(client.network("/summary?"))

    _, journal = _execute(plan, client, trino)

    assert journal == [] and len(client.network("/summary?")) == downloads


@pytest.mark.parametrize(
    "answer", [_body(), HttpStatusError(503, "busy")], ids=["same", "failed"]
)
def test_recheck_of_a_match_no_day_lists_keeps_its_shootout(
    tmp_path, monkeypatch, answer
) -> None:
    """The stored shootout (the day score won over the Summary one) survives
    a recheck rebuilt from the bronze row, whatever the outcome."""

    client, trino = _captured(tmp_path)
    for row in trino.tables["espn_match"]:
        if row["event_id"] == EVENT:
            row["home_shootout"], row["away_shootout"] = 4, 3
    yesterday = json.loads(_first_day()[0])
    yesterday["events"] = [item for item in yesterday["events"] if item["id"] != str(EVENT)]
    client.responses[_req_key(urls.all_scoreboard_day(YESTERDAY))] = json.dumps(yesterday).encode()

    _, journal = _recheck_wave(monkeypatch, client, trino, tmp_path, answer)

    row = _matches(trino)[EVENT]
    assert (row["home_shootout"], row["away_shootout"]) == (4, 3)
    assert row["rechecked_at"] is not None and len(journal) == 1


def test_sample_journals_without_setting_rechecked_at(tmp_path, monkeypatch) -> None:
    client, trino = _captured(tmp_path)

    _, journal = _recheck_wave(
        monkeypatch, client, trino, tmp_path, _body(), kind=recheck.SAMPLE_24H
    )

    assert _matches(trino)[EVENT]["rechecked_at"] is None
    assert "'sample_24h'" in journal[0] and "'same'" in journal[0]


def test_rechecked_at_is_carried_through_a_later_republication(tmp_path, monkeypatch) -> None:
    client, trino = _captured(tmp_path)
    _recheck_wave(monkeypatch, client, trino, tmp_path, _body())
    stamped = _matches(trino)[EVENT]["rechecked_at"]
    yesterday = json.loads(_first_day()[0])
    yesterday["events"][0]["status"]["type"]["name"] = "STATUS_FINAL_AET"
    client.responses[_req_key(urls.all_scoreboard_day(YESTERDAY))] = json.dumps(yesterday).encode()

    _execute(_plan(client, trino, tmp_path), client, trino)

    row = _matches(trino)[EVENT]
    assert row["status"] == "STATUS_FINAL_AET" and row["rechecked_at"] == stamped


def test_status_change_after_a_cut_answer_keeps_the_richer_parse(tmp_path) -> None:
    """The rule covers every repeated Summary: a replayed body poorer than the
    stored one (the alias moved to a cut answer) is rejected as well."""

    client, trino = _captured(tmp_path)
    stored = _matches(trino)[EVENT]["raw_sha256"]
    client.stored[_summary_key("eng.1")] = _body(_cut)  # the alias now names the cut body
    yesterday = json.loads(_first_day()[0])
    yesterday["events"][0]["status"]["type"]["name"] = "STATUS_FINAL_AET"
    client.responses[_req_key(urls.all_scoreboard_day(YESTERDAY))] = json.dumps(yesterday).encode()
    downloads = len(client.network("/summary?"))

    _, journal = _execute(_plan(client, trino, tmp_path), client, trino)

    row = _matches(trino)[EVENT]
    assert row["status"] == "STATUS_FINAL_AET"
    assert row["raw_sha256"] == stored and _with_stats(trino) == 40
    assert len(client.network("/summary?")) == downloads  # replayed, not downloaded
    assert "'refresh'" in journal[0] and "'downgrade_rejected'" in journal[0]


# ------------------------------------------------------------ criterion 3

PLAN_AT = datetime(2026, 10, 20, 0, 5, tzinfo=timezone.utc)


def _db():
    pytest.importorskip("sqlglot")
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA bronze")
    con.execute(
        "CREATE TABLE bronze.espn_match (competition_slug varchar, season_year integer, "
        "event_id bigint, kickoff timestamp, played_final boolean, disposition varchar, "
        "lineup_state varchar, team_stats_state varchar, events_state varchar, "
        "rechecked_at timestamp, first_published_at timestamp)"
    )
    return con


def _row(con, event_id, *, slug="eng.1", ago=timedelta(days=8), disposition="captured",
         lineup="captured", team="captured", events="captured", played=True,
         rechecked=None, published=None):
    kickoff = (PLAN_AT - ago).replace(tzinfo=None)
    con.execute(
        "INSERT INTO bronze.espn_match VALUES (?, 2026, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [slug, event_id, kickoff, played, disposition, lineup, team, events, rechecked,
         None if published is None else (PLAN_AT - published).replace(tzinfo=None)],
    )


def _candidates(con):
    import sqlglot

    sql = sqlglot.transpile(
        recheck.build_candidates_sql(PLAN_AT), read="trino", write="duckdb"
    )[0].replace("iceberg.bronze.", "bronze.")
    return recheck.select_candidates(con.execute(sql).fetchall())


def test_only_incomplete_matches_are_recheck_candidates() -> None:
    con = _db()
    # eng.1 lineups, team statistics and events are the norm of the league.
    for event_id in range(1, 6):
        _row(con, event_id, ago=timedelta(days=15 + event_id))
    _row(con, 101)  # complete: never rechecked
    _row(con, 102, lineup="valid_empty", team="valid_empty", disposition="valid_empty")
    _row(con, 103, events="valid_empty")  # the league has events: incomplete
    _row(con, 104, lineup="valid_empty", rechecked=datetime(2026, 10, 19))  # done once
    _row(con, 105, disposition="lineup_anomaly", ago=timedelta(days=9, hours=23))
    _row(con, 106, lineup="valid_empty", ago=timedelta(days=6))  # too early
    _row(con, 107, lineup="valid_empty", ago=timedelta(days=11))  # too late
    _row(con, 108, lineup="pending", team="pending", events="pending", disposition=None)
    _row(con, 109, lineup="valid_empty", played=False)
    # gua.1 publishes no lineups: a match without one is complete for its league.
    for event_id in range(201, 205):
        _row(con, event_id, slug="gua.1", lineup="valid_empty", ago=timedelta(days=12))
    _row(con, 205, slug="gua.1", lineup="valid_empty")
    _row(con, 206, slug="gua.1", lineup="valid_empty", team="valid_empty")

    assert _candidates(con) == {
        102: ("eng.1", 2026, "recheck"),
        103: ("eng.1", 2026, "recheck"),
        105: ("eng.1", 2026, "recheck"),
        206: ("gua.1", 2026, "recheck"),
    }


def test_sample_is_every_twentieth_match_at_24_and_72_hours() -> None:
    con = _db()
    _row(con, 300, published=timedelta(hours=30))
    _row(con, 320, published=timedelta(hours=80))
    _row(con, 301, published=timedelta(hours=30))  # not % 20
    _row(con, 340, published=timedelta(hours=20))  # before +24 h
    _row(con, 360, published=timedelta(hours=50))  # the +24 h wave was yesterday
    _row(con, 380, published=timedelta(hours=100))  # the +72 h wave was yesterday
    _row(con, 400, published=timedelta(hours=24))  # exactly +24 h
    _row(con, 420, published=timedelta(hours=72))  # exactly +72 h
    # Incomplete and due for its recheck: the recheck wins over the sample.
    _row(con, 440, lineup="valid_empty", published=timedelta(hours=30))
    _row(con, 460, published=None)

    assert _candidates(con) == {
        300: ("eng.1", 2026, "sample_24h"),
        320: ("eng.1", 2026, "sample_72h"),
        400: ("eng.1", 2026, "sample_24h"),
        420: ("eng.1", 2026, "sample_72h"),
        440: ("eng.1", 2026, "recheck"),
    }


def test_recheck_candidate_outside_the_live_editions_is_skipped(tmp_path, monkeypatch) -> None:
    client, trino = _captured(tmp_path)
    monkeypatch.setattr(
        recheck,
        "plan_recheck",
        lambda trino, now: {1: ("eng.1", 1999, "recheck"), 2: ("fra.1", 2026, "recheck")},
    )

    plan = wave.plan_wave(
        client=client, trino=trino, rows=_rows(), state_path=tmp_path / "editions.json",
        now=NOW, check_stale=False, check_recheck=True,
    )

    assert plan.works == ()


def test_recheck_journal_table_and_rows() -> None:
    conn = FakeConn()
    recheck.ensure_recheck_table(conn)
    row = recheck.recheck_row(
        checked_at=NOW, run_id="r'1", slug="eng.1", season_year=2020, event_id=EVENT,
        kind=recheck.RECHECK, before={"lineup": 1}, after=None, outcome=recheck.FAILED,
    )
    assert recheck.flush_rechecks(conn, [row]) == 1
    assert conn.sql[1].startswith("CREATE TABLE IF NOT EXISTS iceberg.ops.espn_recheck_v1 (")
    assert conn.sql[2] == (
        "INSERT INTO iceberg.ops.espn_recheck_v1 (checked_at, run_id, slug, season_year, "
        "event_id, kind, before_parts, after_parts, outcome) VALUES (TIMESTAMP "
        "'2026-09-25 13:00:00.000000', 'r''1', 'eng.1', 2020, 578281, 'recheck', "
        "'{\"lineup\": 1}', NULL, 'failed')"
    )
    with pytest.raises(ValueError):
        recheck.recheck_row(
            checked_at=NOW, run_id="r", slug="x", season_year=1, event_id=1,
            kind="daily", before=None, after=None, outcome=recheck.SAME,
        )


def test_day_row_of_a_recheck_is_used_when_the_day_lists_it(tmp_path, monkeypatch) -> None:
    """A match both changed on its day and due for a recheck is downloaded once."""

    client, trino = _captured(tmp_path)
    yesterday = json.loads(_first_day()[0])
    yesterday["events"][0]["status"]["type"]["name"] = "STATUS_FINAL_AET"
    client.responses[_req_key(urls.all_scoreboard_day(YESTERDAY))] = json.dumps(yesterday).encode()
    before = len(client.network("/summary?"))

    _recheck_wave(monkeypatch, client, trino, tmp_path, _body())

    assert len(client.network("/summary?")) == before + 1
    assert client.replays == []
    row = _matches(trino)[EVENT]
    assert row["status"] == "STATUS_FINAL_AET" and row["rechecked_at"] is not None

