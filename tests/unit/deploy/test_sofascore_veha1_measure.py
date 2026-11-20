"""Offline journal migration and status-debt accounting for milestone one."""
import json
import os
from collections import Counter
from datetime import date, datetime, timedelta, timezone
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
    # Actual closures layout: closed_at, observed_at, five statuses in CORE_ENDPOINTS order.
    closure_state = {"1": [float(start + 8000), float(start + 8600), *["collected"] * 5]}
    monkeypatch.setattr(measure, "closures", lambda gids: dict(closure_state))
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
    assert summary["journal_source"] == "schedule_core_x_ops_manifest_v1"
    assert summary["endpoint_status_counts"] == {
        "collected": 5, "source_404": 0, "quarantine": 0, "not_collected": 0}
    assert ("журнал (schedule_core_x_ops_manifest_v1): collected 5, "
            "source_404 0, quarantine 0, not_collected 0") in summary["line"]
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
    closure_state["1"][0] = float(start + 50000)  # replay: поздний closed_at не перезаписывает
    measure._report_locked({7: "core"}, Path("denominator.tsv"), now + timedelta(days=2),
                           day + timedelta(days=2), cutoff + 2 * 86400, False)
    assert measure.read_daily()[2]["status_debt_current"] == {}
    journal = measure.read_journal()
    assert len(journal) == 2
    assert journal["1"]["closed_at"] == start + 8000  # первое закрытие переживает replay
    assert journal["1"]["endpoint_journal_closed_at"] == start + 8000
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


@pytest.mark.parametrize("name", ["observe", "unmarked", "c1"])
def test_sql_templates_ship_with_the_measurer(name):
    # Шаблоны жили только в /root/watchdog/tools/ — из релиза измеритель падал FileNotFoundError.
    # closure больше не рендерится _sql: это указатель на канонический журнал (тест ниже).
    sql = measure._sql(name, since="1", until="2", statuses="'notstarted'")
    assert "__sentinel__" in sql and "{" not in sql
    assert len((measure.TOOLS / "sofascore_veha1_c1_noncore.txt").read_text().split()) == 211


def test_closure_sql_is_a_pointer_to_the_canonical_journal():
    # Старый SELECT по compatibility-таблице удалён — второй семантики закрытия нет.
    text = (measure.TOOLS / "sofascore_veha1_closure.sql").read_text(encoding="utf-8")
    assert "sofascore_endpoint_journal.sql" in text
    assert "sofascore_match_capture_status" not in text
    assert "{" not in text  # плейсхолдеров нет — файл не рендерится


def test_c1_sql_is_schedule_only():
    # Закрытие считает closures() по каноническому журналу, а не любая строка capture_status.
    sql = measure._sql("c1", since="1", until="2")
    assert "sofascore_match_capture_status" not in sql
    assert "__sentinel__" in sql and "{" not in sql
    assert sql.count("'__sentinel__'") == 1


def test_closures_reads_the_canonical_endpoint_journal(monkeypatch):
    # Compatibility-таблица не используется: closed_at только при capture_complete;
    # partial и quarantine не закрыты, точный 404 терминален, отсутствающий матч не входит.
    from deploy.sofascore import endpoint_journal

    ts = "2026-10-26T12:00:00Z"
    later = "2026-10-26T13:30:00Z"
    manifest = {"collected": "success", "source_404": "not_supported",
                "quarantine": "schema_error", "not_collected": "retryable_failure"}
    http = {"collected": "200", "source_404": "404", "quarantine": "200", "not_collected": None}
    rows = []
    for gid, statuses in (
        ("1", ("collected",) * 5),
        ("2", ("source_404",) * 5),
        ("3", ("collected", "collected", "not_collected", "collected", "collected")),
        ("4", ("collected", "quarantine", "collected", "collected", "collected")),
    ):
        for endpoint, status in zip(endpoint_journal.CORE_ENDPOINTS, statuses):
            rows.append([gid, "SS-7", "2627", "7", endpoint, status, manifest[status],
                         http[status], later if status in measure.TERMINAL else None,
                         ts, None])

    seen = {}

    def query(sql, ncols):
        seen["sql"], seen["ncols"] = sql, ncols
        return [list(row) for row in rows]

    monkeypatch.setattr(measure, "trino_rows", query)
    ids = ["1", "2", "3", "4", "404missing"]
    out = measure.closures(ids)
    assert seen["ncols"] == len(endpoint_journal.JOURNAL_COLUMNS)
    assert seen["sql"] == endpoint_journal.render_journal_sql(ids)
    assert "sofascore_match_capture_status" not in seen["sql"]
    closed = datetime(2026, 10, 26, 13, 30, tzinfo=timezone.utc).timestamp()
    observed = datetime(2026, 10, 26, 12, tzinfo=timezone.utc).timestamp()
    assert out["1"] == [closed, observed, *["collected"] * 5]
    assert out["2"] == [closed, observed, *["source_404"] * 5]
    assert out["3"][0] is None and out["3"][2:] == [
        "collected", "collected", "not_collected", "collected", "collected"]
    assert out["4"][0] is None and "quarantine" in out["4"]
    assert "404missing" not in out


def test_closures_empty_ids_return_nothing_without_trino(monkeypatch):
    def boom(*_args):
        raise AssertionError("trino_rows не должен вызываться для пустого списка")
    monkeypatch.setattr(measure, "trino_rows", boom)
    assert measure.closures([]) == {}


def test_c1_closes_only_on_canonical_capture_complete(tmp_path, monkeypatch):
    start = int(datetime(2026, 10, 20, 18, tzinfo=timezone.utc).timestamp())
    fin_rows = [["1", str(start), "SS-7", None, "7"],
                ["2", str(start), "SS-7", None, "7"],
                ["3", str(start), "SS-7", None, "7"]]
    monkeypatch.setattr(measure, "TOOLS", tmp_path)  # noncore-список не зависит от релиза
    (tmp_path / "sofascore_veha1_c1_noncore.txt").write_text("SS-1000 SS-1001\n", encoding="utf-8")
    monkeypatch.setattr(measure, "_sql", lambda name, **_: name)
    calls = {}

    def rows(sql, ncols):
        calls["ncols"] = ncols
        return [list(row) for row in fin_rows]

    monkeypatch.setattr(measure, "trino_rows", rows)
    closure_state = {
        "1": [float(start + 8000), None, *["collected"] * 5],                # в 24 ч
        "2": [float(start + 7200 + 86400 + 500), None, *["collected"] * 5],  # закрыт позже 24 ч
    }
    requested = []

    def closures(gids):
        requested.append(list(gids))
        return {gid: closure_state[gid] for gid in gids if gid in closure_state}

    monkeypatch.setattr(measure, "closures", closures)
    out = measure.c1({7: "core"}, date(2026, 10, 20), date(2026, 10, 22))
    assert calls["ncols"] == 5  # c1.sql schedule-only: без колонки закрытия
    assert requested == [["1", "2", "3"]]  # canonical closures, не compatibility-строки
    assert "finished 3" in out
    assert "ядро 1/3 (закрыто когда-либо 2)" in out  # 3 не закрыт: нет строк журнала


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


def test_game_tournament_prefers_ss_partition_and_falls_back_to_daily():
    # #1370: партиция SS-<id> (полоса актуалки) важнее дейли-реестра.
    assert measure.game_tournament("SS-17", "ENG-Premier League", "17") == (17, "SS", "SS-17")
    assert measure.game_tournament(None, "ENG-Premier League", "17") == (17, "daily", "ENG-Premier League")


def test_report_revalidates_legacy_closures_without_rewriting_published_days(tmp_path, monkeypatch):
    now = datetime(2026, 11, 20, 9, tzinfo=timezone.utc)
    day = now.date() - timedelta(days=1)
    prior_day = (day - timedelta(days=1)).isoformat()
    cutoff = now.replace(hour=0).timestamp()
    for name, path in (("STATE", tmp_path), ("JOURNAL", tmp_path / "journal.jsonl"),
                       ("DAILY", tmp_path / "daily.jsonl"), ("OBSERVE_STATE", tmp_path / "observe.json")):
        monkeypatch.setattr(measure, name, path)
    monkeypatch.setattr(measure, "status_debt", lambda *_, **kwargs: [])
    monkeypatch.setattr(measure, "trino_rows", lambda *_: [])
    monkeypatch.setattr(measure, "unmarked", lambda *_: Counter())
    recent = int(cutoff - 3 * 86400)
    old = int(cutoff - 12 * 86400)

    def legacy(gid, start, **values):
        return dict(game_id=gid, tournament_id=17, lane="SS", league="SS-17", season="2627",
                    start_ts=start, deadline_ts=start + 86400, closed_at=start + 3600, **values)

    previous = {"day": prior_day, "line": "published legacy line"}
    rows = [
        legacy("open", recent),
        legacy("late", old),
        legacy("timely", old, reported_day="2026-11-17"),  # interrupted before its daily commit
        legacy("published", recent, reported_day=prior_day),
    ]
    measure.JOURNAL.write_text("".join(json.dumps(r) + "\n" for r in rows))
    measure.DAILY.write_text(json.dumps(previous) + "\n")
    sources = {
        "open": [None, recent, "quarantine", *(["collected"] * 4)],
        "late": [old + 90000, old, *(["collected"] * 5)],
        "timely": [old + 7200, old, *(["collected"] * 5)],
    }
    requests = []

    def closures(gids):
        requests.extend(gids)
        return {gid: sources[gid] for gid in gids}

    monkeypatch.setattr(measure, "closures", closures)
    args = ({17: "core"}, tmp_path / "denominator.tsv", now, day, cutoff, False)
    line = measure._report_locked(*args)
    history = measure.read_daily()
    assert history[0] == previous
    assert (history[1]["core_in24"], history[1]["core_total"]) == (1, 3)
    assert Counter(requests) == {"open": 1, "late": 1, "timely": 1}
    result = measure.read_journal()
    assert result["published"] == rows[-1]
    assert result["open"].get("endpoint_journal_closed_at") is None
    assert result["timely"]["closed_at"] == rows[2]["closed_at"]
    assert result["timely"]["endpoint_journal_closed_at"] == old + 7200
    assert result["late"]["endpoint_journal_closed_at"] == old + 90000
    assert measure._report_locked(*args) == line
    assert measure.read_daily() == history
