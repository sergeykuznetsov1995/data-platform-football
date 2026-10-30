"""Offline deep contracts, failures and Summary compatibility (#1513)."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest

from scrapers.espn import deep, deep_parser as parser
from scrapers.espn.bronze_writer import write_tournament_batch
from scrapers.espn.deep_schema import PLAYS_SCHEMA, PLAYS_TABLE
from scrapers.espn.gate import TransportGate
from scrapers.espn.transport_contracts import LaneClosed
from tests.unit.scrapers.test_espn_bronze_writer import FakeTrino, _batch, _match

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    monkeypatch.setenv("ESPN_GATE_STATE_PATH", str(tmp_path / "gate.json"))


def stats(value=None):
    values = [{"name": "totalPasses", "value": 400}]
    if value is not None:
        values.append({"name": "expectedGoals", "value": value})
    return {"splits": {"categories": [{"name": "general", "stats": values}]}}


def page(index=1, count=1000, size=500):
    return {"count": count, "pageIndex": index, "pageCount": (count + size - 1) // size,
            "items": [{"id": str(i + 1), "expectedGoals": 0.0} for i in
                      range((index - 1) * size, min(index * size, count))]}


@pytest.mark.parametrize("count,level", [(0, "light"), (49, "light"), (50, None), (99, None),
    (100, "medium"), (400, "medium"), (401, None), (999, None), (1000, "full")])
def test_depth_thresholds(count, level):
    assert parser.deep_level(count) == level


@pytest.mark.parametrize("count,support,value,expected", [
    (14, None, 0.0, None), (1450, False, 0.0, None),
    (1450, True, 0.0, 0.0), (1450, True, 1.7, 1.7),
])
def test_xg_placeholder_and_real_zero(count, support, value, expected):
    # Actual source JSON is kept, with unsupported placeholders replaced by NULL.
    body = parser.statistics(stats(value))
    masked = parser.mask_xg(body, support if parser.deep_level(count) == "full" else None)
    assert masked["splits"]["categories"][0]["stats"][1]["value"] == expected
    assert parser.play_values({"id": "1", "expectedGoals": value}, support)["xg"] == expected


def test_xg_requires_content_evidence():
    assert parser.xg_support([stats(0.0)]) is None
    assert parser.xg_support([stats()]) is False
    assert parser.xg_support([stats(0), stats(0.2)]) is True
    assert parser.xg_support([{"shotMapAvailable": True, "count": 1500}]) is False


def test_two_pages_and_duplicate_guard():
    assert len(parser.collect_plays(lambda index: page(index))) == 1000
    with pytest.raises(parser.EspnParseError, match="duplicate"):
        parser.collect_plays(lambda index: {**page(index), "items": page(1)["items"]})
    with pytest.raises(parser.EspnParseError):
        parser.collect_plays(lambda index: {**page(index), "pageIndex": 2})


def fixture_bronze():
    trino = FakeTrino()
    trino.tables[PLAYS_TABLE] = []
    payload = _match(1, "aa")
    write_tournament_batch(_batch(payload), trino=trino)
    return trino, payload


def publish(trino, payload, support=True):
    raw = payload.raw
    teams = {row.team_id: (stats(0 if row.home_away == "home" else 0.2), raw)
             for row in payload.summary.matchsheet}
    plays = [({"id": str(index + 1), "expectedGoals": 0.0}, raw) for index in range(1000)]
    return deep.publish_deep(trino=trino, slug="eng.1", year=2020, event_id=1,
                             plays=plays, teams=teams, manifest=[{"raw_uri": raw.raw_uri}], has_xg=support)


def test_deep_repeat_and_summary_preserve_data():
    trino, payload = fixture_bronze()
    first_published = trino.tables["espn_match"][0]["first_published_at"]
    assert publish(trino, payload)
    assert not publish(trino, payload)
    assert len(trino.tables[PLAYS_TABLE]) == 1000
    before = deepcopy(trino.tables[PLAYS_TABLE])
    write_tournament_batch(_batch(payload), trino=trino)
    assert trino.tables[PLAYS_TABLE] == before
    match = trino.tables["espn_match"][0]
    assert match["deep_state"] == "completed"
    assert match["first_published_at"] == first_published
    assert all(json.loads(row["deep_stats_json"])["parser_version"] == parser.PARSER_VERSION
               for row in trino.tables["espn_team_stats"])
    assert len({row["_batch_id"] for row in before}) == 1


def test_interrupted_publication_never_marks_completed():
    trino, payload = fixture_bronze()
    original = trino.insert_dataframe_atomic

    def fail(schema, table, frame, **kwargs):
        if table == "espn_match":
            raise RuntimeError("interrupted marker")
        return original(schema, table, frame, **kwargs)

    trino.insert_dataframe_atomic = fail
    with pytest.raises(RuntimeError, match="interrupted"):
        publish(trino, payload)
    assert trino.tables["espn_match"][0]["deep_state"] == "pending"
    trino.insert_dataframe_atomic = original
    write_tournament_batch(_batch(payload), trino=trino)
    assert all(row["deep_stats_json"] is not None for row in trino.tables["espn_team_stats"])
    assert publish(trino, payload)
    assert len(trino.tables[PLAYS_TABLE]) == 1000


def test_play_schema_and_provenance():
    import pyarrow as pa
    trino, payload = fixture_bronze()
    publish(trino, payload)
    pa.Table.from_pylist(trino.tables[PLAYS_TABLE], schema=PLAYS_SCHEMA)
    assert trino.tables[PLAYS_TABLE][0]["raw_sha256"] == payload.raw.raw_sha256
    assert trino.calls[-3]["table"] == PLAYS_TABLE
    assert trino.calls[-1]["table"] == "espn_match"


def test_deep_quota_shared_by_concurrent_clients(tmp_path):
    now = datetime(2026, 10, 30, tzinfo=timezone.utc)
    # Request directly under the gate lock at explicit instants: no network/waits.
    gates = [TransportGate(state_path=tmp_path / "gate.json", lane="history", purpose="deep",
                           utcnow_fn=lambda: now, admission_check=lambda: None) for _ in range(2)]
    def acquire(gate):
        with gate._state() as state:
            return gate._try_acquire(state, "core", now.timestamp())
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(acquire, gates))
    from scrapers.espn.gate import Permit
    assert sum(isinstance(result, Permit) for result in results) == 1
    snap = gates[0].snapshot()
    assert snap["daily"]["history"]["requests"] == 1
    # Remove only global spacing: the independent deep share must still block.
    with gates[0]._state() as state:
        for index in range(1, 6):
            state["next_permit_at"] = 0
            assert isinstance(gates[0]._try_acquire(state, "core", now.timestamp() + index), Permit)
        state["next_permit_at"] = 0
        result = gates[0]._try_acquire(state, "core", now.timestamp() + 10)
    assert isinstance(result, float)


def test_deep_requires_debt_check(tmp_path):
    gate = TransportGate(state_path=tmp_path / "gate.json", lane="history", purpose="deep")
    with pytest.raises(LaneClosed, match="no live-debt"):
        gate.acquire("core")


@pytest.fixture
def runner(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scrapers.espn.deep_store import ensure_depth_table
    duckdb = pytest.importorskip("duckdb")
    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS iceberg")
    ensure_depth_table(conn)
    base, payload = fixture_bronze()

    class Bronze(FakeTrino):
        def execute_query(self, sql, params=None):
            if "AS debt" in sql:
                return self.debt
            if "espn_depth_v1" in sql or "espn_deep_retry_v1" in sql:
                return conn.execute(sql, params).fetchall()
            if sql.startswith("SELECT event_id, home_team_id"):
                return [(row["event_id"], row["home_team_id"], row["away_team_id"], row["deep_state"])
                        for row in self.tables["espn_match"]]
            return super().execute_query(sql, params)

    bronze = Bronze()
    bronze.tables = base.tables
    bronze.debt = [(0,)]
    now = datetime(2026, 10, 30, tzinfo=timezone.utc)

    class Client:
        before_attempt = None
        sleep_fn = None
        ledger = ()
        failed_page = False
        flushes = 0
        calls = []
        gate = TransportGate(state_path=tmp_path / "gate.json", lane="history", purpose="deep",
                             utcnow_fn=lambda: now)

        def fetch_json(self, url, endpoint, params, **kwargs):
            self.before_attempt()
            self.calls.append((url, params))
            if "plays" in url:
                if params["limit"] == "1":
                    data = page(1, size=1)
                elif self.failed_page and params.get("page") == "2":
                    raise parser.EspnParseError("page interrupted")
                else:
                    data = page(int(params.get("page", "1")))
            else:
                data = stats(0.2)
            return SimpleNamespace(json_data=data, raw_uri=payload.raw.raw_uri,
                                   content_hash=payload.raw.raw_sha256,
                                   fetched_at=now.isoformat())

        def flush(self):
            self.flushes += 1

    client = Client()
    monkeypatch.setattr(deep, "flush_journal", lambda *a, **k: None)
    denominator = SimpleNamespace(targets=lambda: {"eng.1"}, live_targets=lambda: {"eng.1"},
                                  row=lambda slug: SimpleNamespace(current_season_year=2020))
    arguments = dict(client=client, trino=bronze, conn=conn, denominator=denominator,
                     run_id="offline", deadline=now + timedelta(minutes=12), stop_file=tmp_path / "history.off",
                     now_fn=lambda: now)
    yield SimpleNamespace(args=arguments, client=client, bronze=bronze, conn=conn)
    conn.close()


def test_runner_maps_publishes_and_resumes_without_requests(runner):
    report = deep.run_deep(**runner.args)
    assert report["completed"] == 1
    assert report["mapped"] == 1
    assert report["failed"] == 0
    assert runner.client.flushes > 0
    assert len(runner.bronze.tables[PLAYS_TABLE]) == 1000
    before = len(runner.client.calls)
    assert deep.run_deep(**runner.args)["completed"] == 0
    assert len(runner.client.calls) == before
    rows = runner.conn.execute("SELECT competition_slug, source_season_year, deep_level, has_xg "
                               "FROM iceberg.ops.espn_depth_v1").fetchall()
    assert rows == [("eng.1", 2020, "full", True)]


def test_runner_incomplete_page_does_not_publish(runner):
    runner.client.failed_page = True
    report = deep.run_deep(**runner.args)
    assert report["failed"] == 1
    assert report["completed"] == 0
    assert not runner.bronze.tables[PLAYS_TABLE]
    assert runner.bronze.tables["espn_match"][0]["deep_state"] == "pending"
    runner.client.failed_page = False
    assert deep.run_deep(**runner.args)["completed"] == 0
    later = runner.args["now_fn"]() + timedelta(days=1)
    runner.args.update(now_fn=lambda: later, deadline=later + timedelta(minutes=12))
    assert deep.run_deep(**runner.args)["completed"] == 1


@pytest.mark.parametrize("debt", [[(1,)], [(None,)], []])
def test_runner_debt_or_unknown_never_requests(runner, debt):
    runner.bronze.debt = debt
    assert "live debt" in deep.run_deep(**runner.args)["reason"]
    assert not runner.client.calls


def test_runner_checks_stop_again_before_network(runner):
    # The callback installed in the gate is re-evaluated after each pace wait.
    original = runner.client.fetch_json

    def stopped(*args, **kwargs):
        runner.args["stop_file"].touch()
        return original(*args, **kwargs)

    runner.client.fetch_json = stopped
    assert deep.run_deep(**runner.args)["reason"] == "stop file"
    assert not runner.client.calls


def test_depth_registry_seasons_do_not_overwrite(runner):
    from scrapers.espn.deep_store import Depth, save_depth
    save_depth(runner.conn, Depth("eng.1", 2015, 200, "medium"))
    save_depth(runner.conn, Depth("eng.1", 2026, 1450, "full", False))
    save_depth(runner.conn, Depth("eng.1", 2026, 1600, "full", True))
    assert runner.conn.execute("SELECT source_season_year, plays_count, has_xg FROM iceberg.ops.espn_depth_v1 "
                               "ORDER BY source_season_year").fetchall() == [(2015, 200, None), (2026, 1600, True)]


def test_summary_and_deep_publications_serialize():
    trino, payload = fixture_bronze()
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(write_tournament_batch, _batch(payload), trino=trino),
                   pool.submit(publish, trino, payload)]
        for future in futures:
            future.result()
    assert trino.tables["espn_match"][0]["deep_state"] == "completed"
    assert len(trino.tables[PLAYS_TABLE]) == 1000
    assert all(row["deep_stats_json"] is not None for row in trino.tables["espn_team_stats"])


def test_confirmed_xg_upgrades_previous_nulls():
    trino, payload = fixture_bronze()
    assert publish(trino, payload, support=None)
    assert trino.tables[PLAYS_TABLE][0]["xg"] is None
    assert publish(trino, payload, support=True)
    assert trino.tables[PLAYS_TABLE][0]["xg"] == 0.0
    assert all(json.loads(row["deep_stats_json"])["has_xg"] is True
               for row in trino.tables["espn_team_stats"])


def test_bad_first_match_does_not_block_remaining_matches(runner):
    write_tournament_batch(_batch(_match(2, "bb")), trino=runner.bronze)
    original = runner.client.fetch_json

    def broken(url, endpoint, params, **kwargs):
        if "/events/1/" in url and params.get("page") == "2":
            raise parser.EspnParseError("first match page interrupted")
        return original(url, endpoint, params, **kwargs)

    runner.client.fetch_json = broken
    report = deep.run_deep(**runner.args)
    assert report["failed"] == 1 and report["completed"] == 1
    assert {row["event_id"]: row["deep_state"] for row in runner.bronze.tables["espn_match"]} == {
        1: "pending", 2: "completed"}


def test_runner_upgrades_completed_match_after_later_xg_confirmation(runner):
    write_tournament_batch(_batch(_match(2, "bb")), trino=runner.bronze)
    original = runner.client.fetch_json

    def content(url, endpoint, params, **kwargs):
        result = original(url, endpoint, params, **kwargs)
        if "/events/1/" in url and url.endswith("statistics"):
            result.json_data = stats(0.0)
        return result

    runner.client.fetch_json = content
    assert deep.run_deep(**runner.args)["completed"] == 2
    evidence = json.loads(runner.conn.execute("SELECT manifest_json FROM iceberg.ops.espn_depth_v1").fetchone()[0])
    assert any(entry.get("probe") is True and entry["event_id"] == 1 for entry in evidence)
    assert any("team_id" in entry and entry["event_id"] == 2 for entry in evidence)
    # The second match proves season support. On resumption the first one is upgraded.
    assert deep.run_deep(**runner.args)["completed"] == 1
    assert all(row["xg"] == 0.0 for row in runner.bronze.tables[PLAYS_TABLE])


def test_real_transport_stop_during_gate_wait(tmp_path):
    from scrapers.espn.transport import EspnHttpClient
    from scrapers.espn.raw_store import EspnRawStore
    from scrapers.espn import urls
    from tests.unit.scrapers.test_espn_transport import FakeSession

    now = datetime(2026, 10, 30, tzinfo=timezone.utc)
    stopped = False

    def check():
        if stopped:
            raise LaneClosed("stop after wait")

    def sleep(seconds):
        nonlocal stopped
        stopped = True

    gate = TransportGate(state_path=tmp_path / "gate.json", lane="history", purpose="deep",
                         admission_check=check, sleep_fn=sleep, utcnow_fn=lambda: now)
    with gate._state() as state:
        state["next_permit_at"] = now.timestamp() + 10
    client = EspnHttpClient(raw_store=EspnRawStore.from_uri(f"file://{tmp_path}/raw"),
                            gate=gate, session=FakeSession([]), environ={})
    request = urls.plays("eng.1", 1)
    try:
        with pytest.raises(LaneClosed, match="stop after wait"):
            client.fetch_json(request.url, request.endpoint, request.params)
        assert not client.session.calls
    finally:
        client.close()


def test_cost_uses_attempts_even_without_request_ledger(runner, tmp_path):
    from scrapers.espn.attempts import AttemptJournal
    from scrapers.espn.transport_contracts import EndpointType
    journal = AttemptJournal(tmp_path / "attempts.sqlite3")
    runner.client.attempt_journal = journal
    def interrupted(*args, **kwargs):
        attempt = journal.begin(run_id="offline", task_id="run_deep", requested_at=runner.args["now_fn"](),
                                origin="https://sports.core.api.espn.com", endpoint=EndpointType.PLAYS,
                                lane="history", step=0)
        journal.finish(attempt, status=503, timeout=False, http_ms=1, direct_bytes=123)
        raise LaneClosed("stopped before retry ledger")

    runner.client.fetch_json = interrupted
    report = deep.run_deep(**runner.args)
    assert report["requests"] == 1 and report["direct_bytes"] == 123
    assert report["request_ledger_complete"] is False
    assert report["retries"] is None


@pytest.mark.parametrize("level,expected", [("light", None), ("full_without_xg", None), ("full_with_xg", 0.0)])
def test_runner_xg_source_contract(runner, level, expected):
    original = runner.client.fetch_json

    def content(url, endpoint, params, **kwargs):
        result = original(url, endpoint, params, **kwargs)
        if level == "light" and "plays" in url:
            result.json_data = page(1, count=14, size=1)
        if level == "full_without_xg" and url.endswith("statistics"):
            result.json_data = stats(0.0)
        return result

    runner.client.fetch_json = content
    report = deep.run_deep(**runner.args)
    assert report["failed"] == 0
    if level == "light":
        assert report["completed"] == 0
        assert all(row["deep_stats_json"] is None for row in runner.bronze.tables["espn_team_stats"])
        assert not any(url.endswith("statistics") for url, _ in runner.client.calls)
    else:
        assert report["completed"] == 1
        assert runner.bronze.tables[PLAYS_TABLE][0]["xg"] == expected
        values = [json.loads(row["deep_stats_json"])["statistics"]["splits"]["categories"][0]["stats"][1]["value"]
                  for row in runner.bronze.tables["espn_team_stats"]]
        assert values == ([None, None] if expected is None else [0.2, 0.2])


def test_map_pass_precedes_expensive_full_collection(runner):
    from types import SimpleNamespace
    runner.args["denominator"] = SimpleNamespace(
        targets=lambda: {"eng.1", "esp.1"}, live_targets=lambda: {"eng.1", "esp.1"},
        row=lambda slug: SimpleNamespace(current_season_year=2020))
    report = deep.run_deep(**runner.args)
    # Fixture stores the same candidate for both slugs; the important ordering
    # is the two cheap registry probes before either full-page download.
    calls = runner.client.calls
    assert calls[0][1]["limit"] == calls[1][1]["limit"] == "1"
    assert "/leagues/eng.1/" in calls[0][0]
    assert "/leagues/esp.1/" in calls[1][0]


def test_new_depth_targets_precede_failed_probe_retries(runner):
    from types import SimpleNamespace
    from scrapers.espn.deep_store import Depth, save_depth
    save_depth(runner.conn, Depth("eng.1", 2020, checked_at=runner.args["now_fn"](), error="previous error"))
    runner.args["denominator"] = SimpleNamespace(
        targets=lambda: {"eng.1", "esp.1"}, live_targets=lambda: {"eng.1", "esp.1"},
        row=lambda slug: SimpleNamespace(current_season_year=2020))
    deep.run_deep(**runner.args)
    assert "/leagues/esp.1/" in runner.client.calls[0][0]


@pytest.mark.parametrize("remaining", [0, 1])
def test_deep_creates_missing_summary_teams(remaining):
    trino, payload = fixture_bronze()
    trino.tables["espn_team_stats"] = trino.tables["espn_team_stats"][:remaining]
    assert publish(trino, payload)
    rows = trino.tables["espn_team_stats"]
    assert len(rows) == 2
    match = trino.tables["espn_match"][0]
    assert {row["team_id"] for row in rows} == {match["home_team_id"], match["away_team_id"]}
    assert all(row["deep_stats_json"] is not None for row in rows)
    assert all(row["raw_sha256"] == payload.raw.raw_sha256 for row in rows)
    write_tournament_batch(_batch(payload), trino=trino)
    assert all(row["deep_stats_json"] is not None for row in trino.tables["espn_team_stats"])


def test_writer_lock_uses_explicit_state_only(tmp_path, monkeypatch):
    from scrapers.espn import deep_store, gate

    def forbidden():
        raise AssertionError("implicit production state")

    monkeypatch.setattr(gate, "default_state_path", forbidden)
    monkeypatch.setattr(deep_store, "default_state_path", forbidden, raising=False)
    trino = FakeTrino()
    write_tournament_batch(_batch(_match(1, "aa")), trino=trino)
    write_tournament_batch(_batch(_match(1, "aa")), trino=trino, state_dir=tmp_path)
    assert len(list((tmp_path / "edition-locks").glob("*.lock"))) == 1


def test_probe_budget_preserves_full_collection_and_defers_errors(runner):
    from types import SimpleNamespace
    from scrapers.espn.deep_store import Depth, save_depth
    now = runner.args["now_fn"]()
    clock = [now]
    runner.args.update(now_fn=lambda: clock[0], deadline=now + timedelta(minutes=2))
    slugs = {"eng.1", *(f"bad{i}" for i in range(13))}
    runner.args["denominator"] = SimpleNamespace(
        targets=lambda: slugs, live_targets=lambda: slugs,
        row=lambda slug: SimpleNamespace(current_season_year=2020))
    save_depth(runner.conn, Depth("eng.1", 2020, 1000, "full", checked_at=now))
    original = runner.client.fetch_json

    def content(url, endpoint, params, **kwargs):
        if "/leagues/bad" in url:
            clock[0] += timedelta(seconds=10)
            runner.client.before_attempt()
            runner.client.calls.append((url, params))
            raise parser.EspnParseError("invalid depth probe")
        return original(url, endpoint, params, **kwargs)

    runner.client.fetch_json = content
    report = deep.run_deep(**runner.args)
    assert report["completed"] == 1
    assert clock[0] < runner.args["deadline"]
    failed_slugs = {row[0] for row in runner.conn.execute(
        "SELECT competition_slug FROM iceberg.ops.espn_depth_v1 WHERE error IS NOT NULL").fetchall()}
    assert failed_slugs
    runner.client.calls.clear()
    deep.run_deep(**runner.args)
    assert not any(f"/leagues/{slug}/" in url for slug in failed_slugs for url, _ in runner.client.calls)


def test_probe_budget_expiring_during_debt_query_still_collects_full(runner):
    from scrapers.espn.deep_store import Depth, save_depth
    now = runner.args["now_fn"]()
    clock = [now]
    runner.args.update(now_fn=lambda: clock[0], deadline=now + timedelta(minutes=2))
    save_depth(runner.conn, Depth("eng.1", 2020, 1000, "full", checked_at=now))
    original = runner.bronze.execute_query
    debt_checks = 0

    def query(sql, params=None):
        nonlocal debt_checks
        if "AS debt" in sql:
            debt_checks += 1
            if debt_checks == 2:
                clock[0] += timedelta(seconds=25)
        return original(sql, params)

    runner.bronze.execute_query = query
    assert deep.run_deep(**runner.args)["completed"] == 1


def test_probe_budget_expires_inside_real_gate_and_full_still_runs(runner, tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scrapers.espn.deep_store import Depth, save_depth
    from scrapers.espn.raw_store import EspnRawStore
    from scrapers.espn.transport import EspnHttpClient
    from tests.unit.scrapers.test_espn_transport import FakeSession

    now = runner.args["now_fn"]()
    clock = [now]
    runner.args.update(now_fn=lambda: clock[0], deadline=now + timedelta(minutes=2))
    runner.args["denominator"] = SimpleNamespace(
        targets=lambda: {"bad", "eng.1"}, live_targets=lambda: {"bad", "eng.1"},
        row=lambda slug: SimpleNamespace(current_season_year=2020))
    save_depth(runner.conn, Depth("eng.1", 2020, 1000, "full", checked_at=now))
    gate = runner.client.gate
    gate.utcnow_fn = lambda: clock[0]
    with gate._state() as state:
        state["next_permit_at"] = now.timestamp() + 60

    def sleep(seconds, check):
        clock[0] += timedelta(seconds=25)
        check()

    monkeypatch.setattr(deep, "interruptible_sleep", sleep)
    real = EspnHttpClient(raw_store=EspnRawStore.from_uri((tmp_path / "raw").as_uri()),
                         gate=gate, session=FakeSession([]), environ={})
    original = runner.client.fetch_json

    def fetch(url, endpoint, params, **kwargs):
        if "/leagues/bad/" in url:
            return real.fetch_json(url, endpoint, params, **kwargs)
        return original(url, endpoint, params, **kwargs)

    runner.client.fetch_json = fetch
    try:
        assert deep.run_deep(**runner.args)["completed"] == 1
        assert real.ledger[-1].disposition == "lane_closed"
        assert not real.session.calls
    finally:
        real.close()


def test_failed_prefix_defers_and_queue_advances_across_bounded_runs(runner):
    from scrapers.espn.deep_store import Depth, save_depth
    now = runner.args["now_fn"]()
    clock = [now]
    runner.args.update(now_fn=lambda: clock[0])
    save_depth(runner.conn, Depth("eng.1", 2020, 1000, "full", checked_at=now))
    for event_id in range(2, 6):
        write_tournament_batch(_batch(_match(event_id, str(event_id))), trino=runner.bronze)
    original = runner.client.fetch_json
    attempted = []

    def content(url, endpoint, params, **kwargs):
        result = original(url, endpoint, params, **kwargs)
        clock[0] += timedelta(seconds=1)
        if url.endswith("statistics") and "/events/5/" not in url:
            clock[0] += timedelta(seconds=30)
            attempted.append(int(url.split("/events/")[1].split("/")[0]))
            raise parser.EspnParseError("bad team statistics")
        return result

    runner.client.fetch_json = content
    reports = []
    for _ in range(5):
        runner.args["deadline"] = clock[0] + timedelta(seconds=31)
        reports.append(deep.run_deep(**runner.args))
    assert sum(report["completed"] for report in reports) == 1
    assert attempted == [1, 2, 3, 4]
    assert runner.conn.execute("SELECT event_id FROM iceberg.ops.espn_deep_retry_v1 "
                               "ORDER BY event_id").fetchall() == [(1,), (2,), (3,), (4,)]
    # Once due, a repaired match is retried and its persistent deferral removed.
    clock[0] = now + timedelta(days=1, minutes=5)
    runner.args["deadline"] = clock[0] + timedelta(minutes=12)
    runner.client.fetch_json = original
    assert deep.run_deep(**runner.args)["completed"] == 4
    assert runner.conn.execute("SELECT * FROM iceberg.ops.espn_deep_retry_v1").fetchall() == []


def test_interrupted_match_is_deferred_on_next_run(runner):
    from scrapers.espn.deep_store import Depth, save_depth
    now = runner.args["now_fn"]()
    save_depth(runner.conn, Depth("eng.1", 2020, 1000, "full", checked_at=now))
    write_tournament_batch(_batch(_match(2, "bb")), trino=runner.bronze)
    original = runner.client.fetch_json

    def interrupted(url, endpoint, params, **kwargs):
        if "/events/1/" in url and params.get("page") == "2":
            raise LaneClosed("deadline during match")
        return original(url, endpoint, params, **kwargs)

    runner.client.fetch_json = interrupted
    assert deep.run_deep(**runner.args)["reason"] == "deadline during match"
    runner.client.calls.clear()
    runner.client.fetch_json = original
    assert deep.run_deep(**runner.args)["completed"] == 1
    assert all("/events/2/" in url for url, _ in runner.client.calls)


def test_completed_prefix_never_starves_new_matches_across_bounded_runs(runner):
    # #1986: SQL time is the budget; completed matches must not be re-read each run.
    now = runner.args["now_fn"]()
    clock = [now]
    runner.args.update(now_fn=lambda: clock[0])
    for event_id in range(2, 4):
        write_tournament_batch(_batch(_match(event_id, str(event_id))), trino=runner.bronze)
    assert deep.run_deep(**runner.args)["completed"] == 3
    for event_id in range(4, 8):
        write_tournament_batch(_batch(_match(event_id, str(event_id))), trino=runner.bronze)
    original = runner.bronze.execute_query
    team_reads = []

    def timed(sql, params=None):
        clock[0] += timedelta(seconds=1)
        if "espn_team_stats" in sql:
            clock[0] += timedelta(seconds=10)
            team_reads.append(sql)
        return original(sql, params)

    runner.bronze.execute_query = timed
    completed = []
    for _ in range(4):
        runner.args["deadline"] = clock[0] + timedelta(seconds=40)
        completed.append(deep.run_deep(**runner.args)["completed"])
    assert completed == [1, 1, 1, 1]
    stored = {row["event_id"]: row["deep_state"] for row in runner.bronze.tables["espn_match"]}
    assert stored == dict.fromkeys(range(1, 8), "completed")
