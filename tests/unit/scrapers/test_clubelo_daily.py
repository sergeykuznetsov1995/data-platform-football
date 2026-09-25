"""ClubElo daily snapshot (#1463): fixtures over a fake HTTP, stores in memory.

The /Ranking and /Results fixtures of 2026-09-24 (rating date 2026-09-22),
the history manifest pre-closes every linked slug except the clubs whose page
fixtures exist, so the new-slug branch fetches exactly those.
"""

from __future__ import annotations

import gzip
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from scrapers.clubelo import daily, history
from scrapers.clubelo.parse import parse_ranking_slugs
from scrapers.clubelo.transport import ClubEloTransport
from tests.unit.scrapers.clubelo_fakes import (
    FakeResponse,
    FakeSession,
    MemoryStore,
    fixture_html,
    fixture_response,
    gzip_response,
    network_error,
    no_sleep,
)

RATING_DATE = date(2026, 9, 22)
QUEUE = parse_ranking_slugs(fixture_html("Ranking.html.gz"))[1]
NEW = ["Arsenal", "santos-fc_2", "riverplate", "lsapi-4199"]  # /Ranking order
T0 = datetime(2026, 9, 24, 12, 0, 0)


class MemoryDailyStore:
    """In-memory ``IcebergDailyStore``: replace by rating_date, MERGE by key."""

    def __init__(self, counts: Optional[Dict[date, int]] = None) -> None:
        self.snapshot: Dict[date, List[Dict]] = {}
        self.results: Dict[Tuple, Dict] = {}
        self.counts = dict(counts or {})
        self.replaces: List[date] = []

    def ensure_tables(self) -> None:
        pass

    def snapshot_counts(self, rating_date):
        earlier = [d for d in self.counts if d < rating_date]
        return self.counts.get(rating_date), (self.counts[max(earlier)] if earlier else None)

    def replace_snapshot(self, rating_date, rows):
        assert all(r["rating_date"] == rating_date for r in rows)
        self.replaces.append(rating_date)
        self.snapshot[rating_date] = list(rows)
        self.counts[rating_date] = sum(not r["is_provisional"] for r in rows)
        return len(rows)

    def merge_results(self, rows):
        for row in rows:
            self.results[tuple(row[k] for k in daily.RESULT_KEYS)] = row


def _manifest_closed_except(open_slugs):
    return [{"slug": s, "status": "done", "fetched_at": T0, "_ingested_at": T0}
            for s in QUEUE if s not in open_slugs]


def _answers(**extra):
    answers = {"/Ranking": fixture_response("Ranking.html.gz"),
               "/Results": fixture_response("Results.html.gz")}
    for slug in NEW:
        answers[f"/{slug}"] = fixture_response(f"club_{slug}.html.gz")
    answers.update(extra)
    return answers


def _run(answers=None, *, store=None, open_slugs=NEW, limit=10, retries=3):
    session = FakeSession(answers or _answers())
    transport = ClubEloTransport(session, min_interval=0, retry_pause=0, sleep=no_sleep)
    store = store or MemoryDailyStore()
    history_store = MemoryStore(_manifest_closed_except(open_slugs))
    sent, slept = [], []
    result = daily.run_daily(
        transport, store, history_store, notifier=sent.append, sleep=slept.append,
        results_retries=retries, new_slugs_limit=limit, batch_id="test-daily",
    )
    return result, store, history_store, session, sent, slept


def _results_with_date(day: str) -> FakeResponse:
    html = fixture_html("Results.html.gz").replace(
        '<h1><a href="/2026-09-22/Results">', f'<h1><a href="/{day}/Results">', 1)
    return gzip_response(html)


def test_complete_run_writes_snapshot_results_and_new_slugs():
    result, store, hist, session, sent, slept = _run()
    assert daily.exit_code(result) == 0 and sent == [] and slept == []
    assert result["rating_date"] == "2026-09-22"
    assert result["page_created_at"] == "2026-09-24T08:50:18"
    assert (result["rows"], result["provisional"], result["snapshot_rows"]) == (1741, 53, 1794)
    assert result["levels_matched_pct"] == 98.91 and result["elo_precise_matched"] == 100
    assert (result["results_rows"], result["results_final"], result["results_attempts"]) == (63, 59, 1)
    assert result["same_date"] is False and result["written"] is True
    # Ranking + Results wire bytes are the daily cost; the club pages come on top
    assert result["wire_bytes_daily"] == 88609 + 46210
    assert result["wire_bytes"] == result["wire_bytes_daily"] + sum(
        len(session.answers[f"/{s}"].raw.wire) for s in NEW)
    assert result["requests"] == 2 + 4
    assert (result["history_new_candidates"], result["history_new_fetched"],
            result["history_new_ok"], result["history_new_failed"]) == (4, 4, 4, 0)

    rows = store.snapshot[RATING_DATE]
    assert len(rows) == 1794
    bayern = rows[0]
    assert (bayern["club_key"], bayern["elo"], bayern["model_version"], bayern["_batch_id"]) == (
        "Bayern", 2046, "web-2026", "test-daily")
    assert bayern["page_created_at"] == datetime(2026, 9, 24, 8, 50, 18)
    assert isinstance(bayern["fetched_at"], datetime)
    assert len(store.results) == 63
    # raw-first: both pages and the four club pages, gzip bytes of the body
    raw = hist.rows(history.RAW_TABLE)
    assert [r["page"] for r in raw] == ["/Ranking", "/Results"] + [f"/{s}" for s in NEW]
    assert gzip.decompress(raw[0]["body"]).decode("utf-8") == fixture_html("Ranking.html.gz")
    manifest = [r for r in hist.rows(history.MANIFEST_TABLE) if r.get("_batch_id") == "test-daily"]
    assert sorted(r["slug"] for r in manifest) == sorted(NEW)
    assert {r["rating_date"] for r in manifest} == {RATING_DATE}


def test_date_comes_from_the_page_not_the_run_date():
    result, store, *_ = _run()
    assert list(store.snapshot) == [RATING_DATE]  # the run is on 2026-09-24+
    assert {r["rating_date"] for r in store.snapshot[RATING_DATE]} == {RATING_DATE}


def test_new_slugs_are_limited_and_known_ones_skipped():
    result, _, hist, session, *_ = _run(limit=2)
    assert (result["history_new_candidates"], result["history_new_fetched"]) == (4, 2)
    fetched = [c["path"] for c in session.calls][2:]
    assert fetched == ["/Arsenal", "/santos-fc_2"]
    assert daily.exit_code(result) == 0


def test_failed_new_slug_is_red_but_snapshot_stays():
    answers = _answers(**{"/riverplate": [FakeResponse(500), FakeResponse(500), FakeResponse(500)]})
    result, store, *_ = _run(answers)
    assert result["history_new_failed"] == 1 and result["written"] is True
    assert daily.exit_code(result) == 1  # partial failure → non-zero exit
    assert RATING_DATE in store.snapshot and len(store.results) == 63


def test_same_rating_date_is_a_replace_without_alert_or_new_slugs():
    store = MemoryDailyStore({RATING_DATE: 1741, date(2026, 9, 21): 1740})
    result, store, _, session, sent, _ = _run(store=store)
    assert daily.exit_code(result) == 0 and sent == []
    assert result["same_date"] is True and store.replaces == [RATING_DATE]
    assert result["history_new_fetched"] == 0
    assert [c["path"] for c in session.calls] == ["/Ranking", "/Results"]
    assert result["wire_bytes"] == result["wire_bytes_daily"] == 88609 + 46210


def test_completeness_against_the_previous_date_guards_a_new_partition():
    # the new rating date has no partition yet: G2 still compares with 2026-09-21
    store = MemoryDailyStore({date(2026, 9, 21): 1900})
    result, store, _, _, sent, _ = _run(store=store)
    assert result["check"].startswith("G2 1741 clubs")
    assert store.replaces == [] and store.results == {}
    assert daily.exit_code(result) == 1 and "G2" in sent[0]


@pytest.mark.parametrize("elo_rows, same, previous, check", [
    (1653, None, None, "G1"),
    (1700, None, 1800, "G2"),
    (1700, 1900, None, "G3"),
])
def test_check_completeness_refuses(elo_rows, same, previous, check):
    with pytest.raises(daily.GuardRefused, match=check):
        daily.check_completeness(elo_rows, same, previous)


def test_check_completeness_passes():
    daily.check_completeness(1654, None, None)
    daily.check_completeness(1741, 1741, 1741)
    daily.check_completeness(1700, 1850, 1780)


def test_layout_change_writes_nothing_parsed_and_names_the_check():
    broken = fixture_html("Ranking.html.gz").replace("eloData = [", "eloRows = [")
    result, store, hist, session, sent, _ = _run(_answers(**{"/Ranking": gzip_response(broken)}))
    assert result["check"] == "C1 eloData not found"
    assert daily.exit_code(result) == 1 and "C1 eloData not found" in sent[0]
    assert store.snapshot == {} and store.results == {}
    assert [r["page"] for r in hist.rows(history.RAW_TABLE)] == ["/Ranking"]  # raw kept
    assert [c["path"] for c in session.calls] == ["/Ranking"]


def test_dates_differ_retry_three_times_then_nothing_written():
    stale = [_results_with_date("2026-09-23") for _ in range(4)]
    result, store, hist, session, sent, slept = _run(_answers(**{"/Results": stale}))
    assert slept == [daily.RESULTS_RETRY_PAUSE] * 3
    assert [c["path"] for c in session.calls].count("/Results") == 4
    assert result["results_attempts"] == 4
    assert result["check"].startswith("M-09 /Results h1 date 2026-09-23")
    assert store.snapshot == {} and store.results == {}
    assert daily.exit_code(result) == 1 and "M-09" in sent[0]


def test_dates_converge_on_a_retry():
    answers = _answers(**{"/Results": [_results_with_date("2026-09-23"),
                                       fixture_response("Results.html.gz")]})
    result, store, _, _, _, slept = _run(answers)
    assert slept == [daily.RESULTS_RETRY_PAUSE] and result["results_attempts"] == 2
    assert daily.exit_code(result) == 0 and len(store.results) == 63


def test_block_stops_the_run():
    result, store, _, _, sent, _ = _run(_answers(**{"/Results": FakeResponse(403)}))
    assert result["blocked"] and daily.exit_code(result) == 1
    assert store.snapshot == {} and "блокирует" in sent[0]


def test_network_failure_is_an_error():
    result, store, *_ = _run(_answers(**{"/Ranking": [network_error()] * 3}))
    assert "ClubEloFetchError" in result["error"] and daily.exit_code(result) == 1
    assert store.snapshot == {}


def test_results_merge_updates_is_final():
    store = MemoryDailyStore()
    _run(store=store)
    key = (date(2026, 9, 22), "audax-italiano", "colo-colo")
    assert store.results[key]["is_final"] is False
    final = fixture_html("Results.html.gz").replace(
        '<td class="r"><span class="min961"></span></td> <td class="r"><span class="min961"></span></td> </tr>',
        '<td class="r"><span class="min961">12.5<span class="min961"> ±80</span></span></td> '
        '<td class="r"><span class="min961">-3.0<span class="min961"> ±70</span></span></td> </tr>', 1)
    result, store, *_ = _run(_answers(**{"/Results": gzip_response(final)}), store=store)
    assert daily.exit_code(result) == 0 and len(store.results) == 63
    assert store.results[key]["is_final"] is True and store.results[key]["game_delta"] == 12.5


# --- IcebergDailyStore wiring (writer mocked; the SQL/arrow shapes matter) ---

def test_store_replace_snapshot_uses_one_partition_transaction():
    writer = MagicMock()
    writer.replace_identity_partition_arrow_batches.return_value = 2
    rows = [
        {"rating_date": RATING_DATE, "club_key": "Bayern", "slug": "Bayern", "elo": 2046,
         "is_provisional": False},
        {"rating_date": RATING_DATE, "club_key": "~GER:Lok Leipzig", "elo": 1255,
         "is_provisional": True},
    ]
    assert daily.IcebergDailyStore(writer).replace_snapshot(RATING_DATE, rows) == 2
    (tables,), kwargs = writer.replace_identity_partition_arrow_batches.call_args
    assert kwargs == {"database": "bronze", "table": "clubelo_rank_snapshot",
                      "partition_column": "rating_date", "partition_value": "2026-09-22"}
    table = tables[0]
    assert table.column("rating_date").to_pylist() == ["2026-09-22"] * 2
    # the string is what the writer compares; its alignment casts it to date32
    assert table.column("rating_date").cast(pa.date32()).to_pylist() == [RATING_DATE] * 2
    assert table.schema.field("elo").type == pa.int32()
    writer.write_dataframe.assert_not_called()  # never APPEND (#314)


def test_store_merge_results_by_key():
    writer = MagicMock()
    rows = [{"match_date": RATING_DATE, "home_key": "a", "away_key": "~COL:Jaguares",
             "home_rank": None, "away_rank": 12, "is_final": False}]
    daily.IcebergDailyStore(writer).merge_results(rows)
    (frame, database, table), kwargs = writer.write_dataframe.call_args
    assert (database, table) == ("bronze", "clubelo_result")
    assert kwargs == {"mode": "append", "add_metadata": False,
                      "merge_keys": ["match_date", "home_key", "away_key"]}
    assert frame.loc[0, "match_date"] == RATING_DATE
    assert frame.loc[0, "home_rank"] is None and frame.loc[0, "away_rank"] == 12


def test_store_snapshot_counts_same_and_previous():
    writer = MagicMock(catalog="iceberg")
    trino = writer._get_trino_manager.return_value
    trino.execute_query.return_value = [[RATING_DATE, 1741], [date(2026, 9, 21), 1739]]
    assert daily.IcebergDailyStore(writer).snapshot_counts(RATING_DATE) == (1741, 1739)
    sql = trino.execute_query.call_args[0][0]
    assert "iceberg.bronze.clubelo_rank_snapshot" in sql and "DATE '2026-09-22'" in sql
    assert "NOT is_provisional" in sql
    trino.execute_query.return_value = [[date(2026, 9, 21), 1739]]
    assert daily.IcebergDailyStore(writer).snapshot_counts(RATING_DATE) == (None, 1739)
    trino.execute_query.return_value = []
    assert daily.IcebergDailyStore(writer).snapshot_counts(RATING_DATE) == (None, None)


def test_store_creates_date_partitioned_tables():
    writer = MagicMock()
    daily.IcebergDailyStore(writer).ensure_tables()
    calls = {c.args[1]: c for c in writer.create_table_if_not_exists.call_args_list}
    assert calls["clubelo_rank_snapshot"].kwargs["partition_spec"] == [("rating_date", "identity")]
    assert calls["clubelo_result"].kwargs["partition_spec"] == [("match_date", "identity")]
    assert calls["clubelo_rank_snapshot"].args[2].field("rating_date").type == pa.date32()


def test_results_frame_survives_the_real_trino_conversion():
    """The MERGE frame of the real fixture passes the writer's schema inference
    and the production SQL-literal formatter against the table types."""

    from scrapers.base.iceberg_writer import IcebergWriter
    from scrapers.base.trino_manager import TrinoTableManager

    _, mem, *_ = _run()
    writer = MagicMock()
    daily.IcebergDailyStore(writer).merge_results(list(mem.results.values()))
    frame = writer.write_dataframe.call_args[0][0]
    trino = TrinoTableManager.__new__(TrinoTableManager)
    arrow = IcebergWriter.__new__(IcebergWriter)._pandas_to_arrow(frame)
    inferred = trino.arrow_schema_to_trino(arrow.schema)
    table_types = trino.arrow_schema_to_trino(daily.RESULT_SCHEMA)
    assert set(inferred) == set(table_types)
    trino.validate_dataframe_values(frame, table_types)
    assert table_types["match_date"] == "DATE" and table_types["home_rank"] == "INTEGER"


def test_snapshot_rows_build_the_arrow_batch():
    _, mem, *_ = _run()
    writer = MagicMock()
    writer.replace_identity_partition_arrow_batches.side_effect = lambda t, **kw: t[0].num_rows
    assert daily.IcebergDailyStore(writer).replace_snapshot(RATING_DATE, mem.snapshot[RATING_DATE]) == 1794
    table = writer.replace_identity_partition_arrow_batches.call_args[0][0][0]
    assert table.column("is_provisional").to_pylist().count(True) == 53
    assert table.column("elo_precise").null_count == 1794 - 100
