"""Offline checks for the read-only seven-day Bronze parity report (#1370)."""
import json
from datetime import date, datetime, timedelta, timezone

import pytest

from deploy.sofascore import daily_parity as parity
from deploy.sofascore import veha1_measure as measure

START = date(2026, 10, 20)


def rows(*items):
    return [dict(zip(parity.REPORT_COLUMNS, item)) for item in items]


def test_seven_day_window_union_coverage_pass_including_zero_match_day():
    # Union coverage: любой один закрытый ключ (daily или SS) достаточен;
    # match 103 закрыт обоими — physical overlap, логический матч один.
    data = rows(
        ("2026-10-20", "101", "finished", 1, 0),
        ("2026-10-20", "102", "finished", 1, 0),
        ("2026-10-22", "103", "finished", 1, 1),
        ("2026-10-26", "104", "postponed", 0, 0),
    )
    report = parity.build_report(data, start=START)
    assert report["acceptance"] == "scheduler_and_dq_evidence_required"
    assert report["status"] == "pass"
    assert report["missing_finished"] == []
    assert report["logical_duplicates"] == 0
    assert report["physical_both_complete"] == 1
    assert report["finished_total"] == 3
    assert report["covered_total"] == 3
    assert [d["day"] for d in report["days"]] == [
        (START + timedelta(offset)).isoformat() for offset in range(7)
    ]
    assert report["days"][1] == {"day": "2026-10-21", "finished": 0, "covered": 0}
    by_day = {d["day"]: d for d in report["days"]}
    assert by_day["2026-10-20"] == {"day": "2026-10-20", "finished": 2, "covered": 2}
    assert by_day["2026-10-22"] == {"day": "2026-10-22", "finished": 1, "covered": 1}


def test_finished_without_coverage_fails_with_exact_missing_ids():
    data = rows(
        ("2026-10-20", "201", "finished", 1, 0),
        ("2026-10-21", "202", "finished", 0, 0),
        ("2026-10-22", "203", "finished", 0, 0),
    )
    report = parity.build_report(data, start=START)
    assert report["status"] == "fail"
    assert report["missing_finished"] == ["202", "203"]
    assert report["covered_total"] == 1
    assert report["physical_both_complete"] == 0
    by_day = {d["day"]: d for d in report["days"]}
    assert by_day["2026-10-21"] == {"day": "2026-10-21", "finished": 1, "covered": 0}


def test_postponed_and_canceled_ignored_and_physical_overlap_not_logical_duplicate():
    data = rows(
        ("2026-10-20", "301", "postponed", 0, 0),
        ("2026-10-20", "302", "canceled", 0, 0),
        ("2026-10-21", "303", "finished", 1, 1),
    )
    report = parity.build_report(data, start=START)
    assert report["status"] == "pass"
    assert report["missing_finished"] == []
    assert report["finished_total"] == 1
    assert report["logical_duplicates"] == 0
    assert report["physical_both_complete"] == 1
    by_day = {d["day"]: d for d in report["days"]}
    assert by_day["2026-10-20"] == {"day": "2026-10-20", "finished": 0, "covered": 0}


def test_duplicate_match_id_out_of_window_and_other_days_rejected():
    good = rows(("2026-10-20", "401", "finished", 1, 0))
    with pytest.raises(ValueError, match="повтор match_id"):
        parity.build_report(good + rows(("2026-10-21", "401", "finished", 1, 0)), start=START)
    with pytest.raises(ValueError, match="вне окна"):
        parity.build_report(rows(("2026-10-19", "402", "finished", 1, 0)), start=START)
    with pytest.raises(ValueError, match="вне окна"):
        parity.build_report(rows(("2026-10-27", "403", "finished", 1, 0)), start=START)
    with pytest.raises(ValueError, match="ровно 7"):
        parity.build_report([], start=START, days=6)
    with pytest.raises(ValueError, match="ровно 7"):
        parity.build_report([], start=START, days=8)


def test_sql_template_read_only_deduplicated_with_sentinel_and_placeholders():
    sql = (parity.TOOLS / "sofascore_daily_parity.sql").read_text(encoding="utf-8")
    for placeholder in ("{tournament_ids}", "{since}", "{until}"):
        assert placeholder in sql
    assert "__sentinel__" in sql
    # Trino: позиционные GROUP BY (schedule — 1, 2; capture — ровно 1).
    assert "GROUP BY 1, 2" in sql
    assert "GROUP BY 1\n" in sql
    # Ключи таблиц: schedule — game_id, capture — match_id (не путать).
    assert "CAST(game_id AS varchar) AS match_id" in sql
    assert "CAST(match_id AS varchar) AS match_id" in sql
    assert "capture_rows" in sql
    # Sentinel UNION требует строковый day.
    assert "CAST(s.day AS varchar) AS day" in sql
    assert "max_by(" in sql
    assert "bronze.sofascore_schedule" in sql
    assert "bronze.sofascore_match_capture_status" in sql

    import sqlglot

    rendered = parity.parity_sql([7, 11], since=1, until=2)
    assert "{" not in rendered
    assert "IN (7, 11)" in rendered
    assert "start_timestamp >= 1" in rendered
    assert "start_timestamp < 2" in rendered
    statements = sqlglot.parse(rendered, read="trino")
    assert len(statements) == 1
    tree = statements[0]
    forbidden = {"Insert", "Update", "Delete", "Merge", "Create", "Drop", "Command"}
    forbidden |= {name for name in ("Alter", "AlterTable") if hasattr(sqlglot.exp, name)}
    kinds = {type(node).__name__ for node in tree.walk()}
    assert not kinds & forbidden
    assert any(lit.this == "__sentinel__" for lit in tree.find_all(sqlglot.exp.Literal))
    main = [s for s in tree.find_all(sqlglot.exp.Select)
            if len(s.expressions) == len(parity.REPORT_COLUMNS) and s.args.get("joins")]
    assert len(main) == 1
    assert [e.alias_or_name for e in main[0].expressions] == list(parity.REPORT_COLUMNS)


class _FakeCatalog:
    @staticmethod
    def load():
        return _FakeCatalog()

    def tournament_map(self, *, enabled_only):
        assert enabled_only is True
        return {"eng.premier": 11, "rus.premier": 7, "third.enabled": 99}


def test_cli_dry_run_window_ids_columns_and_atomic_output(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(parity, "_catalog_source", lambda: _FakeCatalog)
    seen = {}

    def fake_trino_rows(sql, ncols):
        seen["sql"] = sql
        seen["ncols"] = ncols
        return [
            ["2026-10-20", "501", "finished", "1", "0"],
            ["2026-10-26", "502", "finished", "0", "1"],
        ]

    monkeypatch.setattr(measure, "trino_rows", fake_trino_rows)
    out = tmp_path / "parity.json"
    rc = parity.main(["--start", "2026-10-20", "--days", "7", "--output", str(out)])
    assert rc == 0
    since = int(datetime(2026, 10, 20, tzinfo=timezone.utc).timestamp())
    until = int(datetime(2026, 10, 27, tzinfo=timezone.utc).timestamp())
    sql = seen["sql"]
    assert seen["ncols"] == len(parity.REPORT_COLUMNS) == 5
    assert f"start_timestamp >= {since}" in sql
    assert f"start_timestamp < {until}" in sql
    assert "IN (7, 11, 99)" in sql
    assert "{" not in sql
    stdout = capsys.readouterr().out
    report = json.loads(stdout)
    assert report["status"] == "fail"
    assert report["missing_finished"] == ["502"]
    assert report["physical_both_complete"] == 1
    assert len(report["days"]) == 7
    assert out.read_text(encoding="utf-8") == stdout
    assert [p.name for p in tmp_path.iterdir()] == ["parity.json"]


def test_cli_rejects_days_other_than_seven_without_query(monkeypatch, capsys):
    def explode(*_):
        raise AssertionError("trino_rows must not run")

    monkeypatch.setattr(measure, "trino_rows", explode)
    monkeypatch.setattr(
        parity, "_catalog_source",
        lambda: (_ for _ in ()).throw(AssertionError("catalog must not load")))
    rc = parity.main(["--start", "2026-10-20", "--days", "6"])
    assert rc == 1
    assert "ровно 7" in capsys.readouterr().err
