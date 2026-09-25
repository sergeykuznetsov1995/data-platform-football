"""ClubElo history run (#1462): fixtures over a fake HTTP, store in memory.

The /Ranking fixture queues 498 slugs; the manifest pre-closes all of them but
the clubs under test, which also proves the resume rule (pending = queue minus
slugs whose last manifest row is done/no_page).
"""

from __future__ import annotations

import gzip
from datetime import date, datetime

import pyarrow as pa
import pytest

from scrapers.clubelo import history
from scrapers.clubelo.parse import parse_ranking_slugs
from scrapers.clubelo.transport import ClubEloTransport
from tests.unit.scrapers.clubelo_fakes import (
    FakeResponse,
    FakeSession,
    MemoryStore,
    fixture_html,
    fixture_response,
    network_error,
    no_sleep,
    redirect_response,
)

CLUBS = ["Arsenal", "santos-fc_2", "riverplate", "lsapi-4199"]  # /Ranking order
QUEUE = parse_ranking_slugs(fixture_html("Ranking.html.gz"))[1]
T0 = datetime(2026, 9, 24, 12, 0, 0)


def _closed_except(open_slugs, status="done"):
    return [
        {"slug": s, "status": status, "fetched_at": T0, "_ingested_at": T0}
        for s in QUEUE if s not in open_slugs
    ]


def _answers(**extra):
    answers = {"/Ranking": fixture_response("Ranking.html.gz")}
    for slug in CLUBS:
        answers[f"/{slug}"] = fixture_response(f"club_{slug}.html.gz")
    answers.update(extra)
    return answers


def _run(answers, open_slugs, batch_size=200, notifier=None):
    session = FakeSession(answers)
    transport = ClubEloTransport(session, min_interval=0, retry_pause=0, sleep=no_sleep)
    store = MemoryStore(_closed_except(open_slugs))
    sent = []
    result = history.run_history(
        transport, store, batch_size=batch_size,
        notifier=notifier or sent.append, batch_id="test-batch",
    )
    return result, store, session, sent


def _manifest_new(store):
    return [r for r in store.rows(history.MANIFEST_TABLE) if r.get("_batch_id") == "test-batch"]


def test_four_fixture_clubs_complete_run():
    result, store, session, sent = _run(_answers(), CLUBS)
    assert result["rating_date"] == "2026-09-22"
    assert (result["queue"], result["pending_before"], result["pending_after"]) == (498, 4, 0)
    assert (result["pages_ok"], result["pages_failed"], result["no_page"]) == (4, 0, 0)
    assert result["redirects"] == 0 and result["date_mismatch"] == 0
    assert result["requests"] == 5
    assert result["wire_bytes"] == 88609 + sum(
        len(session.answers[f"/{c}"].raw.wire) for c in CLUBS)
    assert history.exit_code(result) == 0 and sent == []

    points = store.rows(history.POINT_TABLE)
    by_slug = {s: [p for p in points if p["slug"] == s] for s in CLUBS}
    assert {s: len(v) for s, v in by_slug.items()} == {
        "riverplate": 205, "Arsenal": 220, "lsapi-4199": 30, "santos-fc_2": 216}
    river = by_slug["riverplate"]
    assert (river[0]["point_date"], river[-1]["point_date"]) == (date(2022, 9, 24), date(2026, 9, 12))
    assert all(p["captured_rating_date"] == date(2026, 9, 22) for p in points)
    assert {p["model_version"] for p in points} == {"web-2026"}
    assert len(store.rows(history.MATCH_TABLE)) == 64

    manifest = {r["slug"]: r for r in _manifest_new(store)}
    assert set(manifest) == set(CLUBS)
    rp = manifest["riverplate"]
    assert (rp["status"], rp["points"], rp["first_point"], rp["last_point"]) == (
        "done", 205, date(2022, 9, 24), date(2026, 9, 12))
    assert (rp["elo"], rp["elo_best"], rp["elo_best_reached_on"]) == (1732, 1866, date(1986, 7, 24))


def test_raw_first_every_page_stored_gzipped_before_parse_tables():
    result, store, _, _ = _run(_answers(), CLUBS)
    raw = store.rows(history.RAW_TABLE)
    assert [r["page"] for r in raw] == ["/Ranking"] + [f"/{c}" for c in CLUBS]
    river = raw[3]
    assert gzip.decompress(river["body"]) == fixture_html("club_riverplate.html.gz").encode("utf-8")
    assert river["wire_bytes"] == 48980 and river["gzip_by"] == "wire"
    assert river["rating_date"] == date(2026, 9, 22)
    # order of commits: Ranking raw, then per batch raw → points → matches → manifest
    assert store.appends == ["clubelo_raw_page", "clubelo_raw_page", "clubelo_club_elo_point",
                             "clubelo_club_match", "clubelo_history_manifest"]
    assert set(history.SCHEMAS[history.RAW_TABLE].names) >= set(raw[0])


def test_rows_fit_their_schemas():
    _, store, _, _ = _run(_answers(), CLUBS)
    for table in history.WRITE_ORDER:
        rows = [r for r in store.rows(table) if r.get("_batch_id") == "test-batch"]
        pa.Table.from_pylist(rows, schema=history.SCHEMAS[table])  # raises on a type mismatch


def test_302_is_no_page_and_counted_as_redirect():
    result, store, _, _ = _run(_answers(**{"/Arsenal": redirect_response()}), CLUBS)
    manifest = {r["slug"]: r for r in _manifest_new(store)}
    assert manifest["Arsenal"]["status"] == "no_page"
    assert manifest["Arsenal"]["http_status"] == 302
    assert (result["no_page"], result["redirects"], result["pages_ok"]) == (1, 1, 3)
    assert result["pending_after"] == 0
    # 0 unexpected redirects is part of «сдано»: a redirect keeps the run red
    assert history.exit_code(result) == 1


def test_403_stops_the_run_alerts_and_exits_non_zero():
    answers = _answers(**{"/riverplate": FakeResponse(403, b"forbidden")})
    result, store, session, sent = _run(answers, CLUBS)
    assert result["blocked"] and "403" in result["blocked"]
    assert len(sent) == 1 and "блокирует" in sent[0]
    # pages before the block are committed; nothing after riverplate is requested
    assert [c["path"] for c in session.calls] == ["/Ranking", "/Arsenal", "/santos-fc_2", "/riverplate"]
    assert [r["slug"] for r in _manifest_new(store)] == ["Arsenal", "santos-fc_2"]
    assert result["pending_after"] == 2
    assert history.exit_code(result) == 1


def test_failed_page_makes_a_partial_batch_red_and_resumable():
    answers = _answers(**{"/lsapi-4199": [network_error()] * 3})
    result, store, _, _ = _run(answers, CLUBS)
    assert (result["pages_ok"], result["pages_failed"], result["pending_after"]) == (3, 1, 1)
    assert result["failed_slugs"][0].startswith("lsapi-4199: ")
    manifest = {r["slug"]: r for r in _manifest_new(store)}
    assert manifest["lsapi-4199"]["status"] == "failed"
    assert history.exit_code(result) == 1
    # next run: only the failed slug is pending
    assert history.closed_from_manifest(store.rows(history.MANIFEST_TABLE)) == set(QUEUE) - {"lsapi-4199"}


def test_layout_change_is_failed_with_raw_kept():
    broken = fixture_html("club_riverplate.html.gz").replace("var vegaJson = ", "var x = ")
    answers = _answers(**{"/riverplate": FakeResponse(
        200, gzip.compress(broken.encode()), {"content-encoding": "gzip"})})
    result, store, _, _ = _run(answers, CLUBS)
    assert result["pages_failed"] == 1 and "LayoutChanged" in result["failed_slugs"][0]
    assert "/riverplate" in [r["page"] for r in store.rows(history.RAW_TABLE)]
    assert not [p for p in store.rows(history.POINT_TABLE) if p["slug"] == "riverplate"]
    assert history.exit_code(result) == 1


def test_500_after_retries_is_failed():
    answers = _answers(**{"/Arsenal": [FakeResponse(500, b"Server Error (500)")] * 3})
    result, _, session, _ = _run(answers, CLUBS)
    assert result["pages_failed"] == 1 and "HTTP 500" in result["failed_slugs"][0]
    assert sum(c["path"] == "/Arsenal" for c in session.calls) == 3
    assert history.exit_code(result) == 1


def test_batches_commit_separately():
    result, store, _, _ = _run(_answers(), CLUBS, batch_size=3)
    assert result["batches"] == 2
    assert store.appends.count("clubelo_history_manifest") == 2
    assert history.exit_code(result) == 0


def test_latest_manifest_row_decides():
    later = datetime(2026, 9, 25)
    rows = [
        {"slug": "a", "status": "done", "fetched_at": T0, "_ingested_at": T0},
        {"slug": "a", "status": "failed", "fetched_at": later, "_ingested_at": later},
        {"slug": "b", "status": "failed", "fetched_at": T0, "_ingested_at": T0},
        {"slug": "b", "status": "no_page", "fetched_at": later, "_ingested_at": later},
    ]
    assert history.closed_from_manifest(rows) == {"b"}


def test_date_mismatch_is_counted_not_failed():
    html = fixture_html("club_Arsenal.html.gz").replace('href="/2026-09-22/Arsenal"', 'href="/2026-09-23/Arsenal"')
    answers = _answers(**{"/Arsenal": FakeResponse(200, gzip.compress(html.encode()), {"content-encoding": "gzip"})})
    result, _, _, _ = _run(answers, CLUBS)
    assert result["date_mismatch"] == 1 and result["pages_failed"] == 0
    assert history.exit_code(result) == 0


def test_ranking_error_is_red():
    result, _, _, _ = _run({"/Ranking": [FakeResponse(500, b"x")] * 3}, CLUBS)
    assert "HTTP 500" in result["error"]
    assert history.exit_code(result) == 1


def test_api_source_is_not_implemented():
    with pytest.raises(NotImplementedError):
        history.run_history(None, MemoryStore(), source="api")


class _Writer:
    def __init__(self, manifest=None):
        self.created, self.written, self.manifest = [], [], manifest or []

    def create_table_if_not_exists(self, database, table, schema):
        self.created.append((database, table, schema))

    def read_table(self, database, table, columns=None):
        import pandas as pd

        assert (database, table) == ("bronze", "clubelo_history_manifest")
        return pd.DataFrame(self.manifest, columns=columns)

    def write_dataframe(self, frame, database, table, **kwargs):
        self.written.append((database, table, frame, kwargs))


def test_iceberg_store_is_append_only_arrow_with_run_metadata():
    writer = _Writer(_closed_except(CLUBS))
    store = history.IcebergHistoryStore(writer)
    store.ensure_tables()
    assert [(d, t) for d, t, _ in writer.created] == [("bronze", t) for t in history.WRITE_ORDER]
    assert writer.created[0][2].field("body").type == pa.binary()
    assert store.closed_slugs() == set(QUEUE) - set(CLUBS)

    _, mem, _, _ = _run(_answers(), CLUBS)
    store.append(history.POINT_TABLE, mem.rows(history.POINT_TABLE))
    store.append(history.MATCH_TABLE, [])
    [(database, table, frame, kwargs)] = writer.written
    assert (database, table) == ("bronze", "clubelo_club_elo_point")
    assert kwargs == {"mode": "append", "add_metadata": False, "bulk_arrow": True}
    assert list(frame.columns) == history.SCHEMAS[history.POINT_TABLE].names
    assert set(frame["_batch_id"]) == {"test-batch"} and set(frame["_source"]) == {"clubelo_html"}


def _through_real_writer_conversion(frame, table):
    """What IcebergWriter does with the frame in bulk_arrow append mode."""
    from pyiceberg.catalog import Catalog

    from scrapers.base.iceberg_writer import IcebergWriter

    target_schema = Catalog._convert_schema_if_needed(history.SCHEMAS[table])

    class _Target:
        def schema(self):
            return target_schema

    arrow = IcebergWriter()._pandas_to_arrow(frame)
    return IcebergWriter._align_pyiceberg_arrow_table(arrow, target=_Target())


@pytest.mark.parametrize("extra", [
    {},  # all four tables of a complete batch
    {"/Arsenal": redirect_response(), "/santos-fc_2": redirect_response(),
     "/riverplate": redirect_response(), "/lsapi-4199": [network_error()] * 3},  # manifest-only batch
])
def test_store_frames_survive_the_real_arrow_conversion(extra):
    _, mem, _, _ = _run(_answers(**extra), CLUBS)
    writer = _Writer()
    store = history.IcebergHistoryStore(writer)
    for table in history.WRITE_ORDER:
        store.append(table, [r for r in mem.rows(table) if r.get("_batch_id") == "test-batch"])
    assert writer.written
    for _, table, frame, _ in writer.written:
        aligned = _through_real_writer_conversion(frame, table)
        assert aligned.num_rows == len(frame)
        if table == history.MANIFEST_TABLE:
            assert aligned.column("first_point").type == pa.date32()
