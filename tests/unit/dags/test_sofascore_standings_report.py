"""The independent warehouse report follows the same scope as #1368."""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from dags import dag_refresh_sofascore_all_mens as refresh
from deploy.sofascore import standings_report as report


NOW = datetime(2026, 11, 26, 12, tzinfo=timezone.utc)
EPOCH = int(NOW.timestamp())


def test_census_ranks_corrections_and_preserves_missing_tables(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.executescript("""
        CREATE TABLE schedule(league, season, game_id, start_timestamp, _ingested_at);
        CREATE TABLE standings(league, season, _ingested_at);
    """)
    db.executemany(
        "INSERT INTO schedule VALUES (?, ?, ?, ?, ?)",
        [
            ("SS-7", "2627", 1, EPOCH, 1),
            ("SS-7", "2627", 1, EPOCH + 7 * 3600, 2),
            ("SS-8", "2627", 2, EPOCH - 36 * 3600, 1),
            ("SS-8", "2627", 3, EPOCH, 1),
            ("SS-281", "2627", 4, EPOCH + 6 * 3600, 1),
            ("SS-9", "2026's", 5, EPOCH - 40 * 3600, 1),
            ("SS-9", "2026's", 5, EPOCH, 2),
            ("noncore", "2627", 6, EPOCH, 1),
        ],
    )
    db.executemany(
        "INSERT INTO standings VALUES (?, ?, ?)",
        [
            ("SS-8", "2627", "2026-11-01"),
            ("SS-8", "2627", "2026-11-25"),
            ("SS-9", "other", "2026-11-25"),
        ],
    )

    def query(sql, columns):
        sql = sql.replace(
            "iceberg.bronze.sofascore_schedule FOR VERSION AS OF 101", "schedule"
        )
        sql = sql.replace(
            "iceberg.bronze.sofascore_league_table FOR VERSION AS OF 202", "standings"
        )
        rows = db.execute(sql).fetchall()
        assert ("__sentinel__",) + (None,) * (columns - 1) in rows
        return [row for row in rows if row[0] != "__sentinel__"]

    monkeypatch.setattr(report, "trino_rows", query)
    try:
        core = frozenset({"SS-7", "SS-8", "SS-281", "SS-9"})
        result = report.read_census(EPOCH, 101, 202, core)
        canonical = (
            refresh.SEASON_FIELDS_CENSUS_SQL.format(hours=36)
            .replace("to_unixtime(current_timestamp)", str(EPOCH))
            .replace("iceberg.bronze.sofascore_schedule", "schedule")
            .replace("iceberg.bronze.sofascore_league_table", "standings")
        )
        assert sorted(result) == sorted(
            row for row in db.execute(canonical) if row[0] in core
        )
        assert sorted(result) == [
            ("SS-281", "2627", None),
            ("SS-8", "2627", "2026-11-25"),
            ("SS-9", "2026's", None),
        ]
    finally:
        db.close()


def test_invalid_core_season_cannot_produce_a_green_report(monkeypatch):
    monkeypatch.setattr(report, "trino_rows", lambda *_: [("SS-8", None)])
    with pytest.raises(ValueError, match="Missing season"):
        report.read_census(EPOCH, 101, 202, frozenset({"SS-8"}))


def test_report_agrees_with_scheduled_metric_at_seven_days():
    cutoff = (NOW - timedelta(days=7)).replace(tzinfo=None)
    census = [
        ("SS-281", "2627", None),
        ("SS-737", "2627", NOW),
        ("SS-14710", "2026", None),
        ("SS-281", "2526", None),
        ("SS-8", "2627", cutoff),
        ("SS-9", "2627", cutoff - timedelta(microseconds=1)),
    ]
    names = {league: league for league, _, _ in census}
    expected = refresh._season_fields_admission(
        census, as_of=NOW, core_keys=frozenset(names)
    )
    csv_rows = [
        (league, season, stamp.isoformat() if stamp else None)
        for league, season, stamp in census
    ]
    text = report.render_report(
        csv_rows, as_of=NOW, names=names, schedule=1, standings=2
    )
    assert f"{expected['fresh_7d']} из {expected['core_ongoing']}" in text
    assert f"в этом окне — {expected['standings_not_applicable']}" in text
    assert "обновить ещё 2" in text
    assert "(SS-281, 2526) — таблицы нет" in text and "(SS-281, 2627)" not in text
    assert "(SS-9, 2627)" in text and "(SS-8, 2627)" not in text


@pytest.mark.parametrize(
    "stamps, expected",
    [
        ([], "Нет подходящих сезонов"),
        ([NOW.isoformat()] * 19 + [None], "Цель 95% достигнута"),
        ([NOW.isoformat()] * 18 + [None, None], "обновить ещё 1"),
    ],
)
def test_target_is_inclusive_and_requires_an_eligible_season(stamps, expected):
    rows = [("SS-8", str(i), stamp) for i, stamp in enumerate(stamps)]
    rows.append(("SS-737", "2627", None))
    text = report.render_report(
        rows,
        as_of=NOW,
        names={"SS-8": "Liga", "SS-737": "Cup"},
        schedule=1,
        standings=2,
    )
    assert expected in text


def test_failed_query_preserves_previous_complete_publication(tmp_path, monkeypatch):
    destination = tmp_path / "latest.txt"
    responses = iter(
        [[[str(EPOCH), "101", "202"]], [["SS-281", "2627"]], [["SS-281", "2627", None]]]
    )
    monkeypatch.setattr(report, "trino_rows", lambda *_: next(responses))
    report.publish(destination)
    saved = destination.read_text()
    assert "26.11.2026 15:00:00 МСК" in saved and "Нет подходящих сезонов" in saved
    assert destination.stat().st_mode & 0o777 == 0o644

    def failed(*args):
        raise RuntimeError("warehouse unavailable")

    monkeypatch.setattr(report, "trino_rows", failed)
    with pytest.raises(RuntimeError, match="warehouse unavailable"):
        report.publish(destination)
    assert destination.read_text() == saved
