"""Synthetic Bronze DQ: no source, database or Airflow access."""
from datetime import date, timedelta
from unittest.mock import Mock

import pytest

from scrapers.clubelo import dq
from deploy.clubelo.dq_summary import summary_lines

TODAY = date(2026, 10, 28)


def row(day, slug="stable", name="Stable", country="ENG", **extra):
    return {"rating_date": day, "club_key": slug or "~ENG:Pending", "slug": slug,
            "name": name, "country": country, "is_provisional": False,
            "level_section": "Lower", "elo_delta_1d_raw": "-0.00", **extra}


def snapshots():
    rows = []
    for i in range(5):
        day = TODAY - timedelta(days=4-i)
        rows.extend([
            row(day),
            row(day, "old" if i == 0 else "new", "Renamed"),
            row(day, "legacy", "Twin"), row(day, "lsapi-1", "Twin"),
            row(day, "other-country", "Twin", "ESP"),
            row(day, None, "Pending", is_provisional=True, level_section=None,
                elo_delta_1d_raw=None),
            row(day, "no-country", country=None, level_section=None, elo_delta_1d_raw="+0.00"),
            row(day, "afc", country="AFC"),
        ])
    return rows


def client(rows=None, latest=TODAY):
    rows = snapshots() if rows is None else rows
    return Mock(execute_query=Mock(side_effect=[[(latest,)],
                [tuple(r[c] for c in dq.COLUMNS) for r in rows]]))


def test_five_day_metrics_and_exact_identities():
    report = dq.analyze_snapshots(snapshots(), today=TODAY)
    assert report["status"] == "complete"
    assert report["five_consecutive_days"] is True
    assert len(report["days"]) == 5
    first, second = report["days"][:2]
    assert first["appeared"] is None
    assert second["appeared"] == ["new"]
    assert second["disappeared"] == ["old"]
    assert second["slug_changes"] == [
        {"name": "Renamed", "country": "ENG", "before": ["old"], "after": ["new"]}]
    assert second["duplicate_candidates"] == [
        {"name": "Twin", "country": "ENG", "slugs": ["legacy", "lsapi-1"]}]
    assert second["provisional_share"] == 1 / 8
    assert second["unmatched_level_share"] == 1 / 7
    assert second["without_country"] == ["no-country"]
    assert second["afc"] == ["afc"]
    assert second["delta_raw"] == {"-0.00": 6, "+0.00": 1, None: 1}
    assert all(d["appeared"] == [] and d["disappeared"] == []
               and d["slug_changes"] == [] for d in report["days"][2:])


def test_lower_null_level_and_case_preserved():
    rows = [row(TODAY, "Club", level=None), row(TODAY, "club", level=None)]
    day = dq.analyze_snapshots(rows, today=TODAY)["days"][0]
    assert day["unmatched_levels"] == 0
    assert day["duplicate_candidates"] == [
        {"name": "Stable", "country": "ENG", "slugs": ["Club", "club"]}]


def test_missing_calendar_date_not_replaced_by_runs_or_gap_comparison():
    missing = TODAY - timedelta(days=2)
    rows = [r for r in snapshots() if r["rating_date"] != missing]
    report = dq.analyze_snapshots(rows, today=TODAY)
    assert report["status"] == "incomplete"
    assert report["missing_dates"] == [missing]
    assert report["five_consecutive_days"] is False
    assert report["days"][2]["appeared"] is None
    # Even many observations of one date cannot make five distinct dates.
    assert not dq.analyze_snapshots([row(TODAY)] * 6, today=TODAY)["five_consecutive_days"]


@pytest.mark.parametrize("age,stale", [(3, False), (4, True), (-1, True)])
def test_stale_or_future_data(age, stale):
    report = dq.analyze_snapshots(snapshots(), today=TODAY + timedelta(days=age))
    assert report["stale"] is stale
    assert report["status"] == ("incomplete" if stale else "complete")


def test_provisional_only_has_no_level_denominator():
    report = dq.analyze_snapshots([row(TODAY, None, is_provisional=True)], today=TODAY)
    assert report["days"][0]["unmatched_level_share"] is None
    assert "нет знаменателя" in "\n".join(dq.format_summary(report))


def test_read_only_bounded_query_and_repeat_is_same_report():
    trino = client()
    report = dq.read_report(trino, today=TODAY)
    assert report == dq.read_report(client(), today=TODAY)
    assert report == dq.analyze_snapshots(snapshots(), today=TODAY)
    queries = [call.args[0] for call in trino.execute_query.call_args_list]
    assert len(queries) == 2
    assert all(q.startswith("SELECT ") for q in queries)
    assert queries[0] == f"SELECT max(rating_date) FROM {dq.TABLE}"
    assert "BETWEEN DATE '2026-10-24' AND DATE '2026-10-28'" in queries[1]
    assert queries[1].endswith("LIMIT 20001")


@pytest.mark.parametrize("result", [[], [(None,)]])
def test_empty_database(result):
    trino = Mock(execute_query=Mock(return_value=result))
    report = dq.read_report(trino, today=TODAY)
    assert report["status"] == "incomplete"
    assert report["days"] == []
    assert trino.execute_query.call_count == 1


@pytest.mark.parametrize("at", [0, 1])
def test_select_failure_never_green_or_leaks_driver_details(at):
    trino = Mock(execute_query=Mock(side_effect=
                 [RuntimeError("secret")] if at == 0 else [[(TODAY,)], RuntimeError("secret")]))
    lines = summary_lines(trino, today=TODAY)
    assert lines[0] == "ClubElo DQ: unavailable"
    assert "ошибка чтения Bronze" in lines[1]
    assert "secret" not in "\n".join(lines)


def test_truncation_and_snapshot_disappearance_fail_closed():
    trino = client([row(TODAY)] * (dq.MAX_ROWS + 1))
    assert dq.read_report(trino, today=TODAY)["status"] == "unavailable"
    assert dq.read_report(client([]), today=TODAY)["status"] == "unavailable"


def test_adapter_includes_all_formatted_lines_and_limits_examples():
    expected = dq.format_summary(dq.analyze_snapshots(snapshots(), today=TODAY))
    assert summary_lines(client(), today=TODAY) == expected
    text = "\n".join(expected)
    for label in ("appeared", "disappeared", "slug_changes", "duplicate_candidates",
                  "provisional", "unmatched_levels", "without_country", "afc", "elo_delta_1d_raw"):
        assert text.count(f"  {label}:") == 5
    assert "None: 1" in text and "'+0.00': 1" in text and "'-0.00': 6" in text
    rows = [row(TODAY, f"slug-{i}", country=None) for i in range(10)]
    text = "\n".join(dq.format_summary(dq.analyze_snapshots(rows, today=TODAY)))
    assert "without_country: 10;" in text
    assert "'slug-9'" not in text


def test_provisional_pair_gaining_slug_is_a_change():
    rows = [row(TODAY - timedelta(days=1), None, is_provisional=True), row(TODAY)]
    day = dq.analyze_snapshots(rows, today=TODAY)["days"][-1]
    assert day["slug_changes"] == [
        {"name": "Stable", "country": "ENG", "before": [], "after": ["stable"]}]
