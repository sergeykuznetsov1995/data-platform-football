from contextlib import nullcontext
from datetime import date, datetime, timezone
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from trino.exceptions import (
    Http502Error, Http503Error, Http504Error, TrinoConnectionError, TrinoUserError,
)

from scrapers.base.trino_manager import TrinoAmbiguousWriteError
from scrapers.espn import bronze_writer, grouped_wave as grouped, ready_replace, recheck, wave
from scrapers.espn.bronze_schema import EVENTS_TABLE, LINEUP_TABLE, MATCH_TABLE, TEAM_STATS_TABLE
from tests.unit.scrapers.test_espn_bronze_writer import _match
from tests.unit.scrapers.test_espn_grouped_writer import GroupTrino, _edition


def work(slug, *, year=2024, ids=(17,), days=(), **changes):
    return wave.TournamentWork(
        slug=slug, season_year=year, event_ids=ids, espn_id=1, name=slug,
        display_name=str(year), start=date(int(year), 1, 1), end=date(int(year), 12, 31),
        days=days, **changes,
    )


@pytest.fixture
def group(monkeypatch):
    calls = []
    client = SimpleNamespace(attempt_journal=object(), flush=lambda: calls.append("raw"))
    trino = SimpleNamespace(execute_query=lambda *args: [])
    conn, factory = SimpleNamespace(), object()

    def write(batches, **kwargs):
        assert kwargs == dict(trino=trino, state_dir="shared", child_trino_factory=factory,
                              replace_frame=ready_replace.replace_ready_frame)
        keys = [(b.competition_slug, b.season_year) for b in batches]
        calls.append(("write", keys))
        return dict.fromkeys(keys, object())

    def finish(actual_client, actual_conn, *, run_id, task_id):
        assert (actual_client, actual_conn) == (client, conn)
        assert (client.run_id, client.task_id) == (run_id, task_id) == ("run", "mapped")
        calls.append("requests")

    def run(*works):
        return grouped.run_tournament_group(
            works, client=client, trino=trino, conn=conn, run_id="run",
            task_id="mapped", child_trino_factory=factory,
        )

    monkeypatch.setattr(grouped, "client_state_dir", lambda _: "shared")
    monkeypatch.setattr(bronze_writer, "write_ready_batches", write)
    monkeypatch.setattr(bronze_writer, "preflight_tournament_batch", lambda batch: None)
    monkeypatch.setattr(wave, "finish_requests", finish)
    return SimpleNamespace(run=run, client=client, trino=trino, conn=conn, calls=calls)


def stored(slug, year, event_id):
    return SimpleNamespace(
        competition_slug=slug, season_year=year, event_id=event_id, summary_captured=True,
        rechecked_at=None, status_checked_at=None, carried=lambda payload: payload,
        schedule_row=lambda *args: SimpleNamespace(event_id=event_id),
    )


def provide_matches(group, monkeypatch, matches, decisions=()):
    queries = []

    def query(sql, params):
        queries.append((sql, params))
        if recheck.RECHECK_TABLE in sql:
            return decisions
        return [(m.competition_slug, m.season_year, m.event_id, m) for m in matches]

    monkeypatch.setattr(wave.BronzeMatch, "from_row", staticmethod(lambda row: row[3]))
    monkeypatch.setattr(group.trino, "execute_query", query)
    monkeypatch.setattr(
        wave, "_absent_payload",
        lambda client, plan, known, *args, presence: wave.MatchPayload(
            known.schedule_row(), None, object(), presence
        ),
    )
    return queries


def test_scoped_reads_and_failed_forced_recheck_publish_with_peer(group, monkeypatch):
    a, b = "eng.1", "eng.2"
    first = work(a, ids=(17, 18), rechecks={17: recheck.RECHECK})
    second = work(a, year=2025, rechecks={17: recheck.RECHECK})
    third = work(b, ids=(19,))
    matches = [stored(a, 2024, 17), stored(a, 2024, 18), stored(a, 2025, 17), stored(b, 2024, 19)]
    decisions = [
        (a, 2024, 17, recheck.RECHECK, recheck.FAILED),
        (a, 2024, 17, recheck.RECHECK, recheck.SAME),
        (a, 2024, 17, recheck.RECHECK, recheck.FAILED),
        (a, 2025, 17, recheck.SAMPLE_24H, recheck.SAME),
    ]
    queries = provide_matches(group, monkeypatch, matches, decisions)
    old_raw = object()
    old_summary = SimpleNamespace(disposition=SimpleNamespace(value="captured"))

    def again(client, known, *args, force, planned_at, stamped):
        assert known is matches[2] and force
        group.calls.append("recheck")
        return wave._Again(old_summary, old_raw, recheck.FAILED, None, None, error="download")

    def preflight(batch):
        if batch.season_year == 2025:
            payload, = batch.matches
            assert payload.summary is old_summary and payload.raw is old_raw

    def journal(conn, rows):
        row, = rows
        assert row["slug"] == a and row["season_year"] == 2025
        assert row["event_id"] == 17 and row["outcome"] == recheck.FAILED
        group.calls.append("journal")

    monkeypatch.setattr(wave, "_summary_again", again)
    monkeypatch.setattr(bronze_writer, "preflight_tournament_batch", preflight)
    monkeypatch.setattr(recheck, "flush_rechecks", journal)
    with pytest.raises(grouped.GroupRunError) as raised:
        group.run(first, second, third)
    green, red, peer = raised.value.outcomes
    assert (green.state, red.state, peer.state) == (wave.GREEN, wave.RED, wave.GREEN)
    assert [green.matches, red.matches, peer.matches] == [1, 1, 1]
    assert red.dispositions == {"captured": 1} and "download" in red.first_error
    assert not raised.value.no_retry
    assert len(queries) == 2
    (bronze_sql, bronze_params), (journal_sql, journal_params) = queries
    assert bronze_params == (a, 2024, a, 2025, b, 2024)
    assert "event_id IN (17, 18)" in bronze_sql and "event_id IN (19)" in bronze_sql
    assert journal_params == (a, 2024, a, 2025)
    assert journal_sql.count("event_id IN (17)") == 2
    assert group.calls == [
        "raw", "recheck", "raw", "raw", ("write", [(a, 2024), (a, 2025), (b, 2024)]),
        "journal", "requests",
    ]


def test_planned_failures_write_only_ready_rows(group, monkeypatch):
    provide_matches(group, monkeypatch, [stored("b", 2024, 17), stored("c", 2024, 17)])
    with pytest.raises(grouped.GroupRunError) as raised:
        group.run(work("a", ids=(), error="planning A"), work("b", error="planning B"), work("c"))
    a, b, c = raised.value.outcomes
    assert a.state == b.state == wave.RED and c.state == wave.GREEN
    assert (a.matches, b.matches, c.matches) == (0, 1, 1)
    assert "planning A" in a.first_error and "planning B" in b.first_error
    assert raised.value.no_retry
    assert group.calls == ["raw", "raw", "raw", ("write", [("b", 2024), ("c", 2024)]), "requests"]


def test_new_final_download_failure_writes_pending_with_healthy_peer(group, monkeypatch):
    day = date(2024, 4, 1)
    provide_matches(group, monkeypatch, [stored("b", 2024, 17)])
    raw = wave.RawRef("raw/day", "day-hash", datetime.now(timezone.utc))
    schedule = SimpleNamespace(event_id=17, source_season_year=2024, played_final=True)
    requests = []

    def fetch(client, request, *, force_refresh):
        assert not force_refresh
        requests.append(request.url)
        if len(requests) == 1:
            return SimpleNamespace(body={})
        raise wave.EspnParseError("failed Summary")

    def preflight(batch):
        if batch.competition_slug == "a":
            payload, = batch.matches
            assert payload.schedule is schedule and payload.summary is None
            assert payload.presence is None

    monkeypatch.setattr(wave, "_fetch", fetch)
    monkeypatch.setattr(wave, "_raw_ref", lambda _: raw)
    monkeypatch.setattr(wave, "split_day", lambda *args: ({"a": [schedule]}, {}))
    monkeypatch.setattr(bronze_writer, "preflight_tournament_batch", preflight)
    with pytest.raises(grouped.GroupRunError) as raised:
        group.run(work("a", days=(day,)), work("b"))
    red, green = raised.value.outcomes
    assert "SummaryFetchError" in red.first_error and red.matches == 1
    assert not raised.value.no_retry
    assert green.state == wave.GREEN and green.matches == 1
    assert len(requests) == 2
    assert group.calls == ["raw", "raw", ("write", [("a", 2024), ("b", 2024)]), "requests"]


def test_duplicate_normalized_key_before_io(group):
    with pytest.raises(ValueError, match="duplicate"):
        group.run(work(" Eng.1 ", year="2024"), work("eng.1"))
    assert not group.calls and not hasattr(group.client, "run_id")


def test_empty_scope_does_not_read_or_write(group, monkeypatch):
    queries = provide_matches(group, monkeypatch, [])
    outcome, = group.run(work("a", ids=()))
    assert outcome.state == wave.GREEN and outcome.matches == 0
    assert not queries and group.calls == ["raw", "requests"]


def test_journalled_kind_still_processes_new_status(group, monkeypatch):
    decisions = [("a", 2024, 17, recheck.RECHECK, recheck.SAME)]
    provide_matches(group, monkeypatch, [stored("a", 2024, 17)], decisions)
    plan = work("a", rechecks={17: recheck.RECHECK}, statuses={17: "STATUS_POSTPONED"})
    applied = []

    def absent(client, work, known, *args, presence):
        applied.append(work.statuses[known.event_id])
        return wave.MatchPayload(known.schedule_row(), None, object(), presence)

    monkeypatch.setattr(wave, "_absent_payload", absent)
    outcome, = group.run(plan)
    assert outcome.state == wave.GREEN and outcome.matches == 1
    assert applied == ["STATUS_POSTPONED"]
    assert group.calls == ["raw", ("write", [("a", 2024)]), "requests"]


@pytest.mark.parametrize("stage", ["prepare", "raw", "preflight"])
def test_failed_edition_is_drained_and_excluded(group, monkeypatch, stage):
    provide_matches(group, monkeypatch, [stored("a", 2024, 17), stored("b", 2024, 17)])
    prepare = wave.prepare_tournament
    queue = []

    def enqueue(plan, **kwargs):
        assert not queue
        queue.append(plan.slug)
        if stage == "prepare" and plan.slug == "a":
            raise ValueError("prepare")
        return prepare(plan, **kwargs)

    def flush():
        slug = queue.pop()
        group.calls.append(("flush", slug))
        if stage == "raw" and slug == "a":
            raise OSError("raw")

    def preflight(batch):
        if stage == "preflight" and batch.competition_slug == "a":
            raise TypeError("preflight")

    monkeypatch.setattr(wave, "prepare_tournament", enqueue)
    monkeypatch.setattr(group.client, "flush", flush)
    monkeypatch.setattr(bronze_writer, "preflight_tournament_batch", preflight)
    with pytest.raises(grouped.GroupRunError) as raised:
        group.run(work("a"), work("b"))
    red, green = raised.value.outcomes
    assert red.state == wave.RED and stage in red.first_error and red.matches == 0
    assert green.state == wave.GREEN and green.matches == 1
    assert group.calls == [("flush", "a"), ("flush", "b"), ("write", [("b", 2024)]), "requests"]


@pytest.mark.parametrize("gate_type", [grouped.AllOriginsBlocked, grouped.LaneClosed])
def test_gate_publishes_ready_editions_and_stops_fetches(group, monkeypatch, gate_type):
    provide_matches(group, monkeypatch, [stored("a", 2024, 17)])
    prepare = wave.prepare_tournament

    class Closed(gate_type):
        def __init__(self):
            Exception.__init__(self, "gate closed")

    def gated(plan, **kwargs):
        group.calls.append(("prepare", plan.slug))
        if plan.slug == "b":
            raise Closed()
        return prepare(plan, **kwargs)

    monkeypatch.setattr(wave, "prepare_tournament", gated)
    with pytest.raises(grouped.GroupRunError) as raised:
        group.run(work("a"), work("b"), work("c"))
    green, blocked, skipped = raised.value.outcomes
    assert green.state == wave.GREEN and blocked.state == skipped.state == wave.RED
    assert blocked.first_error == skipped.first_error
    assert "Closed" in blocked.first_error
    assert raised.value.no_retry
    assert group.calls == [
        ("prepare", "a"), "raw", ("prepare", "b"), "raw", ("write", [("a", 2024)]), "requests",
    ]


def test_journal_retry_uses_receipted_bronze_and_actual_decisions(group, monkeypatch):
    plans = [work(slug, rechecks={17: recheck.RECHECK}) for slug in ("a", "b")]
    plans.append(work("c"))
    matches = [stored(plan.slug, 2024, 17) for plan in plans]
    decisions = []
    provide_matches(group, monkeypatch, matches, decisions)
    attempted = []
    summary = SimpleNamespace(disposition=SimpleNamespace(value="captured"))

    def again(client, known, *args, force, planned_at, stamped):
        attempted.append((known.competition_slug, stamped))
        return wave._Again(summary, object(), recheck.SAME, None, None)

    write = bronze_writer.write_ready_batches

    def publish(batches, **kwargs):
        receipts = write(batches, **kwargs)
        for batch in batches:
            known = next(m for m in matches if m.competition_slug == batch.competition_slug)
            known.rechecked_at = batch.matches[0].rechecked_at
        return receipts

    def journal(conn, rows):
        assert [row["slug"] for row in rows] == ["a", "b"]
        assert all(m.rechecked_at is not None for m in matches[:2])
        group.calls.append("journal")
        if attempted == [("a", False), ("b", False)]:
            raise TrinoUserError({
                "errorType": "USER_ERROR",
                "errorName": "COLUMN_NOT_FOUND",
                "message": "unknown journal column",
            })
        decisions.extend((r["slug"], r["season_year"], r["event_id"], r["kind"], r["outcome"])
                         for r in rows)

    monkeypatch.setattr(wave, "_summary_again", again)
    monkeypatch.setattr(bronze_writer, "write_ready_batches", publish)
    monkeypatch.setattr(recheck, "flush_rechecks", journal)
    with pytest.raises(grouped.GroupRunError) as raised:
        group.run(*plans)
    a, b, c = raised.value.outcomes
    assert a.state == b.state == wave.RED and a.matches == b.matches == 1
    assert a.first_error == b.first_error and "unknown journal column" in a.first_error
    assert c.state == wave.GREEN and c.matches == 1 and not raised.value.no_retry
    write_all = ("write", [(slug, 2024) for slug in ("a", "b", "c")])
    assert group.calls == [
        "raw", "raw", "raw", write_all, "journal", "requests",
    ]

    group.calls.clear()
    assert all(o.state == wave.GREEN for o in group.run(*plans))
    assert attempted == [("a", False), ("b", False), ("a", True), ("b", True)]
    assert group.calls == ["raw", "raw", "raw", write_all, "journal", "requests"]
    group.calls.clear()
    assert [o.matches for o in group.run(*plans)] == [0, 0, 1]
    assert group.calls == ["raw", "raw", "raw", ("write", [("c", 2024)]), "requests"]


@pytest.mark.parametrize("error_type", [TrinoConnectionError, Http502Error, Http503Error, Http504Error])
def test_recheck_insert_losing_final_page_is_terminal(group, monkeypatch, caplog, error_type):
    provide_matches(group, monkeypatch, [stored("a", 2024, 17), stored("b", 2024, 17)])
    summary = SimpleNamespace(disposition=SimpleNamespace(value="captured"))
    monkeypatch.setattr(
        wave, "_summary_again",
        lambda *args, **kwargs: wave._Again(summary, object(), recheck.SAME, None, None),
    )
    statements = []
    failure = error_type("lost final recheck page")

    def lose_final_page():
        raise failure

    group.conn.cursor = lambda: SimpleNamespace(
        execute=statements.append, fetchall=lose_final_page, close=lambda: None,
    )
    with pytest.raises(grouped.GroupRunError) as raised:
        group.run(work("a", rechecks={17: recheck.RECHECK}), work("b"))

    sql, = statements
    assert sql.lstrip().startswith(f"INSERT INTO {recheck.RECHECK_TABLE}")
    assert raised.value.no_retry
    journalled, peer = raised.value.outcomes
    assert journalled.state == wave.RED and journalled.matches == 1
    assert "lost final recheck page" in journalled.first_error
    assert any(record.exc_info and record.exc_info[1] is failure for record in caplog.records)
    assert peer.state == wave.GREEN and peer.matches == 1


@pytest.mark.parametrize("error_type", [TrinoConnectionError, Http502Error, Http503Error, Http504Error])
def test_read_transport_error_with_ambiguous_write_message_is_retryable(group, monkeypatch, error_type):
    def read(sql, params):
        raise error_type("ambiguous write")

    monkeypatch.setattr(group.trino, "execute_query", read)
    with pytest.raises(grouped.GroupRunError) as raised:
        group.run(work("a"), work("b", ids=()))

    assert not raised.value.no_retry
    failed, empty = raised.value.outcomes
    assert failed.state == wave.RED and "ambiguous write" in failed.first_error
    assert empty.state == wave.GREEN and group.calls == ["raw", "requests"]


@pytest.mark.parametrize("missing_receipt", [False, True])
def test_unconfirmed_bronze_is_never_journalled(group, monkeypatch, missing_receipt):
    plans = [work(slug, rechecks={17: recheck.RECHECK}) for slug in ("a", "b", "c")]
    matches = [stored(plan.slug, 2024, 17) for plan in plans]
    decisions = [("c", 2024, 17, recheck.RECHECK, recheck.SAME)]
    provide_matches(group, monkeypatch, matches, decisions)
    summary = SimpleNamespace(disposition=SimpleNamespace(value="captured"))
    monkeypatch.setattr(
        wave, "_summary_again", lambda *a, **kw: wave._Again(summary, object(), recheck.SAME, None, None)
    )

    def publish(batches, **kwargs):
        group.calls.append(("publish", [b.competition_slug for b in batches]))
        if missing_receipt:
            return {("b", 2024): object()}
        raise TrinoAmbiguousWriteError("response connection reset")

    def journal(conn, rows):
        group.calls.append(("journal", [row["slug"] for row in rows]))

    monkeypatch.setattr(bronze_writer, "write_ready_batches", publish)
    monkeypatch.setattr(recheck, "flush_rechecks", journal)
    with pytest.raises(grouped.GroupRunError) as raised:
        group.run(*plans)
    a, b, noop = raised.value.outcomes
    assert a.state == wave.RED and noop.state == wave.GREEN
    assert b.state == (wave.GREEN if missing_receipt else wave.RED)
    assert raised.value.no_retry is (not missing_receipt)
    expected = ["raw", "raw", "raw", ("publish", ["a", "b"])]
    if missing_receipt:
        expected.append(("journal", ["b"]))
    assert group.calls == expected + ["requests"]


@pytest.mark.parametrize(("failure", "no_retry"), [
    (OSError("request journal unavailable"), False),
    (TrinoConnectionError("request journal connection reset"), True),
])
def test_group_request_journal_failure_keeps_first_error(group, monkeypatch, failure, no_retry):
    provide_matches(group, monkeypatch, [stored("b", 2024, 17)])

    def fail_requests(*args, **kwargs):
        group.calls.append("requests")
        raise failure

    monkeypatch.setattr(wave, "finish_requests", fail_requests)
    with pytest.raises(grouped.GroupRunError) as raised:
        group.run(work("a", ids=(), error="missing day"), work("b"))
    a, b = raised.value.outcomes
    assert a.state == b.state == wave.RED
    assert "missing day" in a.first_error and "request journal" in b.first_error
    assert b.matches == 1 and raised.value.no_retry is no_retry
    assert group.calls.count("requests") == 1


@pytest.mark.parametrize(("events_error", "no_retry"), [
    (TrinoAmbiguousWriteError("lost final result"), True),
    (TimeoutError("timed out publishing events"), True),
    (ValueError("invalid events"), False),
])
def test_parallel_child_failures_set_mapped_retry_policy(monkeypatch, events_error, no_retry):
    batch = _edition(2024, _match(17, "aa"))
    trino = GroupTrino()
    barrier = Barrier(3, timeout=10)
    journal = Mock()

    def prepare(edition, **kwargs):
        edition.prepared = SimpleNamespace(batch=batch, journal=[object()])

    def publish(manager, table, frame, *, match_keys, delete_filter, staging_id):
        if table != MATCH_TABLE:
            barrier.wait()
        if table == LINEUP_TABLE:
            raise ValueError("invalid lineup")
        if table == EVENTS_TABLE:
            raise RuntimeError("events write failed") from events_error
        return manager.insert_dataframe_atomic(
            bronze_writer.BRONZE_DATABASE, table, frame, delete_filter=delete_filter,
        )

    monkeypatch.setattr(grouped, "_previous_state", lambda *args: ({}, {}))
    monkeypatch.setattr(grouped, "client_state_dir", lambda _: None)
    monkeypatch.setattr(grouped, "_prepare", prepare)
    monkeypatch.setattr(wave, "finish_requests", lambda *args, **kwargs: None)
    monkeypatch.setattr(ready_replace, "replace_ready_frame", publish)
    monkeypatch.setattr(recheck, "flush_rechecks", journal)
    with pytest.raises(grouped.GroupRunError) as raised:
        grouped.run_tournament_group(
            [work("eng.1")], client=None, trino=trino, conn=None, run_id="run",
            task_id="mapped", child_trino_factory=lambda: nullcontext(trino),
        )

    outcome, = raised.value.outcomes
    assert raised.value.no_retry is no_retry
    assert outcome.state == wave.RED and "invalid lineup" in outcome.first_error
    assert set(trino.committed) == {TEAM_STATS_TABLE}
    journal.assert_not_called()


def test_wrapped_ambiguous_write_overrides_peer_retry(group, monkeypatch):
    provide_matches(group, monkeypatch, [stored("a", 2024, 17), stored("b", 2024, 17)])

    def preflight(batch):
        if batch.competition_slug == "a":
            raise OSError("temporary preflight failure")

    def write(*args, **kwargs):
        try:
            raise TrinoAmbiguousWriteError("response connection reset")
        except TrinoAmbiguousWriteError as exc:
            raise RuntimeError("replacement failed") from exc

    monkeypatch.setattr(bronze_writer, "preflight_tournament_batch", preflight)
    monkeypatch.setattr(bronze_writer, "write_ready_batches", write)
    with pytest.raises(grouped.GroupRunError) as raised:
        group.run(work("a"), work("b"))
    assert raised.value.no_retry
    assert [o.first_error for o in raised.value.outcomes] == [
        "OSError: temporary preflight failure", "RuntimeError: replacement failed",
    ]


@pytest.mark.parametrize("interruption", [KeyboardInterrupt, TimeoutError])
def test_interruption_propagates_after_request_flush(group, monkeypatch, interruption):
    def interrupt(*args, **kwargs):
        raise interruption()

    monkeypatch.setattr(wave, "prepare_tournament", interrupt)
    with pytest.raises(interruption):
        group.run(work("a"), work("b"))
    assert group.calls[-1] == "requests" and "write" not in str(group.calls)
