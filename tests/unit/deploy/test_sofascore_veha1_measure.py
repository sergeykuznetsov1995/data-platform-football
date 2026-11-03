"""Offline journal migration and status-debt accounting for milestone one."""
import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from deploy.sofascore import veha1_measure as measure


@pytest.mark.parametrize("status", ["notstarted", "inprogress", "interrupted", "unknown"])
@pytest.mark.parametrize("prev_observe_delay", [None, 2 * 86400])
def test_status_debt_counts_once_then_finished_keeps_identity(
    tmp_path, monkeypatch, status, prev_observe_delay,
):
    now = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
    start = int(now.timestamp()) - 3 * 86400
    monkeypatch.setattr(measure, "read_last_observe", lambda: (
        start + prev_observe_delay if prev_observe_delay is not None else None
    ))
    for name, path in (("STATE", tmp_path), ("JOURNAL", tmp_path / "journal.jsonl"),
                       ("DAILY", tmp_path / "daily.jsonl"),
                       ("OBSERVE_STATE", tmp_path / "observe.json")):
        monkeypatch.setattr(measure, name, path)
    finished = [["1", str(start), None, "SS-7", "2627", None, None, None]]
    # Actual observe layout: id, start, change, SS league/season, daily league/season, tournament.
    finished[0][2] = str(start + 7200)
    monkeypatch.setattr(measure, "_sql", lambda name, **_: name)
    monkeypatch.setattr(measure, "SS_ENV", str(tmp_path / "absent.env"))  # вердикт #2094 — без хоста
    monkeypatch.setattr(measure, "trino_rows", lambda *_: finished)
    monkeypatch.setattr(measure, "closures", lambda _: {"1": [start + 8000]})
    monkeypatch.setattr(measure, "unmarked", lambda *_: Counter({status: 1}))
    debt_rows = [["2", str(start), "SS-7", "2627", None, None, None, status, "2026-10-25"]]
    monkeypatch.setattr(measure, "status_debt", lambda _, **kwargs: debt_rows + (
        [["1", str(start), "SS-7", "2627", None, None, None, "finished", "2026-10-25"]]
        if kwargs.get("include_finished") else []), raising=False)
    day = now.date() - timedelta(days=1)
    cutoff = datetime(2026, 10, 28, tzinfo=timezone.utc).timestamp()
    args = ({7: "core"}, Path("denominator.tsv"), now, day, cutoff, False)
    line = measure._report_locked(*args)
    assert "1/2 = 50,0" in line
    expected_late = int(prev_observe_delay is not None)
    assert f"finished увиден позже 24 ч после старта {expected_late} (в знаменателе; " in line
    assert measure._report_locked(*args) == line
    journal = measure.read_journal()
    assert len(journal) == 2
    assert journal["2"]["status_deadline_ts"] == start + 86400
    assert journal["1"]["deadline_ts"] == start + 7200 + 86400
    summary = measure.read_daily()[0]
    assert summary["seen_finished_late_in_denominator"] == expected_late
    assert summary["status_debt_in_denominator"] == {status: 1}
    tomorrow = now + timedelta(days=1)
    measure._report_locked({7: "core"}, Path("denominator.tsv"), tomorrow,
                           day + timedelta(days=1), cutoff + 86400, False)
    assert measure.read_daily()[1]["core_total"] == 0
    assert measure.read_daily()[1]["status_debt_current"] == {status: 1}
    assert measure.read_daily()[1].get("status_debt_current_total") == 2
    assert measure.read_daily()[1].get("status_debt_current_share") == 50.0
    assert "текущий долг 1/2 = 50.00 %" in measure.read_daily()[1]["line"]
    # Later the source confirms finished. Existing reported_day survives;
    # the capture deadline may change, the measured status deadline may not.
    finished.append(["2", str(start), str(start + 7200), "SS-7", "2627", None, None, None])
    debt_rows.clear()
    measure._report_locked({7: "core"}, Path("denominator.tsv"), now + timedelta(days=2),
                           day + timedelta(days=2), cutoff + 2 * 86400, False)
    assert measure.read_daily()[2]["status_debt_current"] == {}
    journal = measure.read_journal()
    assert len(journal) == 2
    assert journal["2"]["latest_status"] == "finished"
    assert journal["2"]["deadline_ts"] == start + 7200 + 86400
    assert journal["2"]["measurement_deadline_ts"] == start + 86400
    assert sum(row["core_total"] for row in measure.read_daily()) == 2


def test_status_debt_query_deduplicates_before_status_filter(monkeypatch):
    import sqlite3
    import sqlglot

    now = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
    start = int(now.timestamp()) - 20 * 86400
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE schedule (league, season, game_id, start_timestamp, status_type, _ingested_at, tournament_unique_tournament_id)")
    db.executemany("INSERT INTO schedule VALUES (?, ?, ?, ?, ?, ?, ?)", [
        ("SS-7", "2627", 1, start, "notstarted", 1, 7),
        ("SS-7", "2627", 1, start, "finished", 2, 7),
        ("SS-7", "2627", 2, start, "interrupted", 1, 7),
        ("SS-7", "2627", 3, start, "notstarted", 1, 7),
        ("RUS-Premier League", "2627", 3, start, "finished", 2, 7),
        ("SS-7", "2627", 4, start, "postponed", 1, 7),
        ("SS-7", "2627", 5, start, "canceled", 1, 7),
        ("SS-7", "2627", 6, int(now.timestamp()), "notstarted", 1, 7),
        ("SS-7", "1920", 7, start, "notstarted", 1, 7),
    ])

    def query(sql, ncols):
        assert ncols == 9
        sqlglot.parse_one(sql, read="trino")
        return [row for row in db.execute(sql.replace("iceberg.bronze.sofascore_schedule", "schedule"))
                if row[0] != "__sentinel__"]

    monkeypatch.setattr(measure, "trino_rows", query)
    rows = measure.status_debt(now)
    assert [row[0] for row in rows] == ["2"]
    assert rows[0][7] == "interrupted"
    population = measure.status_debt(now, include_finished=True)
    assert [row[0] for row in population] == ["1", "2", "3"]
    db.close()


def test_status_debt_parses_double_start_timestamp(monkeypatch):
    import sqlite3

    # Bronze stores start_timestamp as double; a bare CAST AS varchar yields
    # "1.7875044E9" on Trino ("1787504400.0" here) and int() would raise.
    now = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
    start = int(now.timestamp()) - 20 * 86400
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE schedule (league, season, game_id, start_timestamp, status_type, _ingested_at, tournament_unique_tournament_id)")
    db.execute("INSERT INTO schedule VALUES (?, ?, ?, ?, ?, ?, ?)",
               ("SS-7", "2627", 2, float(start), "notstarted", 1, 7))

    def query(sql, ncols):
        return [row for row in db.execute(sql.replace("iceberg.bronze.sofascore_schedule", "schedule"))
                if row[0] != "__sentinel__"]

    monkeypatch.setattr(measure, "trino_rows", query)
    rows = measure.status_debt(now)
    assert int(rows[0][1]) == start
    db.close()


@pytest.mark.parametrize("name", ["observe", "closure", "unmarked", "c1"])
def test_sql_templates_ship_with_the_measurer(name):
    # Шаблоны жили только в /root/watchdog/tools/ — из релиза измеритель падал FileNotFoundError.
    sql = measure._sql(name, since="1", until="2", terminal="'success'", ids="'1'", statuses="'notstarted'")
    assert "__sentinel__" in sql and "{" not in sql
    assert len((measure.TOOLS / "sofascore_veha1_c1_noncore.txt").read_text().split()) == 211


def test_sweep_verdict_source_late_vs_our_sweep_late(tmp_path, monkeypatch):
    # Логика #2094 жила только в копии хоста /root/watchdog/tools/ — из релиза вердикта не было.
    start = int(datetime(2026, 10, 20, 18, tzinfo=timezone.utc).timestamp())

    def iso(ts):
        return datetime.fromtimestamp(ts, timezone.utc).isoformat().replace("+00:00", "Z")

    results = tmp_path / "runtime" / "refresh-results"
    results.mkdir(parents=True)
    env = tmp_path / "sofascore.env"
    env.write_text(f'SOFASCORE_ALL_MENS_RUNTIME_HOST_DIR="{tmp_path / "runtime"}"\n', encoding="utf-8")
    monkeypatch.setattr(measure, "SS_ENV", str(env))

    def bank(name, banked, payload):
        path = results / f"schedule-refresh-{name}.json"
        path.write_text(payload if isinstance(payload, str) else json.dumps(payload), encoding="utf-8")
        os.utime(path, (banked, banked))

    # Визит SS-8 до старт + 24 ч и следующий — уже после T0; SS-7 — страница после 24 ч и покрыла старт.
    bank("early", start + 7200, {"visits": {"SS-8": {"2627": [iso(start + 7000), 2, start - 86400]}}})
    bank("late", start + 30 * 3600, {"visits": {"SS-7": {"2627": [iso(start + 30 * 3600 - 60), 3, start - 3600]}}})
    bank("after", start + 4 * 86400, {"visits": {"SS-8": {"2627": [iso(start + 4 * 86400 - 60), 1, start]}}})
    bank("broken", start + 3600, "{not json")
    bank("novisits", start + 3600, {"games": []})
    bank("badentry", start + 3600, {"visits": {"SS-9": {"2627": ["not-a-date", 1, None]}, "SS-10": []}})

    visits, logs = measure.load_visits(start - 86400)
    assert sorted(visits) == [("SS-7", "2627"), ("SS-8", "2627")]
    t0 = start + 3 * 86400

    def verdict(tid):
        rec = {"start_ts": start, "prev_observe_at": t0, "tournament_id": tid, "season": "2627"}
        return measure.sweep_verdict(rec, visits, logs)

    assert verdict(7)["late_verdict"] == "source_late"
    assert verdict(7)["late_reason"] == "page_covered"
    ours = verdict(8)
    assert (ours["late_verdict"], ours["late_reason"]) == ("our_sweep_late", "visit_before_24h")
    assert ours["next_sweep_at"] == iso(start + 4 * 86400 - 60)
    assert verdict(11)["late_reason"] == "not_visited"
    assert verdict(None)["late_reason"] == "no_tournament"
    rec = {"start_ts": start + 10 * 86400, "prev_observe_at": start + 11 * 86400, "tournament_id": 7, "season": "2627"}
    assert measure.sweep_verdict(rec, visits, logs)["late_reason"] == "no_visit_log"
