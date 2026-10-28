"""Offline journal migration and status-debt accounting for milestone one."""
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from deploy.sofascore import veha1_measure as measure


@pytest.mark.parametrize("status", ["notstarted", "inprogress", "interrupted", "unknown"])
def test_status_debt_counts_once_then_finished_keeps_identity(tmp_path, monkeypatch, status):
    now = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
    start = int(now.timestamp()) - 3 * 86400
    for name, path in (("STATE", tmp_path), ("JOURNAL", tmp_path / "journal.jsonl"),
                       ("DAILY", tmp_path / "daily.jsonl"),
                       ("OBSERVE_STATE", tmp_path / "observe.json")):
        monkeypatch.setattr(measure, name, path)
    finished = [["1", str(start), None, "SS-7", "2627", None, None, None]]
    # Actual observe layout: id, start, change, SS league/season, daily league/season, tournament.
    finished[0][2] = str(start + 7200)
    monkeypatch.setattr(measure, "_sql", lambda name, **_: name)
    monkeypatch.setattr(measure, "trino_rows", lambda *_: finished)
    monkeypatch.setattr(measure, "closures", lambda _: {"1": [start + 8000]})
    monkeypatch.setattr(measure, "unmarked", lambda *_: Counter({status: 1}))
    debt_rows = [["2", str(start), "SS-7", "2627", None, None, None, status, "2026-10-25"]]
    monkeypatch.setattr(measure, "status_debt", lambda _: debt_rows, raising=False)
    day = now.date() - timedelta(days=1)
    cutoff = datetime(2026, 10, 28, tzinfo=timezone.utc).timestamp()
    args = ({7: "core"}, Path("denominator.tsv"), now, day, cutoff, False)
    line = measure._report_locked(*args)
    assert "1/2 = 50,0" in line
    assert measure._report_locked(*args) == line
    journal = measure.read_journal()
    assert len(journal) == 2
    assert journal["2"]["status_deadline_ts"] == start + 86400
    assert journal["1"]["deadline_ts"] == start + 7200 + 86400
    summary = measure.read_daily()[0]
    assert summary["status_debt_in_denominator"] == {status: 1}
    tomorrow = now + timedelta(days=1)
    measure._report_locked({7: "core"}, Path("denominator.tsv"), tomorrow,
                           day + timedelta(days=1), cutoff + 86400, False)
    assert measure.read_daily()[1]["core_total"] == 0
    assert measure.read_daily()[1]["status_debt_current"] == {status: 1}
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
    db.close()
