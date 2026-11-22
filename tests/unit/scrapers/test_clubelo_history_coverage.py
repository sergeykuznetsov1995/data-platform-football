"""Offline ClubElo history coverage using a saved ranking page and a mock Trino client."""
import base64
import gzip
import re
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest

from scrapers.clubelo.history_coverage import analyze_coverage, format_line, read_report
from scrapers.clubelo.parse import parse_ranking_slugs

ANCHOR = date(2026, 10, 1)
TODAY = date(2026, 10, 2)
SHA = "7d0062b4" + "a" * 56
RANKING_PAGE = Path(__file__).resolve().parents[2] / "fixtures/clubelo/20261002/Ranking.html.gz"


def _page_response():
    return [(base64.b64encode(RANKING_PAGE.read_bytes()).decode("ascii"), SHA)]


def _client(responses):
    return Mock(execute_query=Mock(side_effect=responses))


def test_analyze_counts_each_queue_slug_into_one_bucket():
    latest = [("a", "done"), ("b", "no_page"), ("c", "failed"), ("d", "reopened")]
    assert analyze_coverage(ANCHOR, ["a", "b", "c", "d", "e"], latest) == {
        "anchor_date": ANCHOR, "queue": 5, "done": 1, "no_page": 1, "failed": 1,
        "pending": 1, "unknown": 1, "covered": 2,
    }


def test_analyze_ignores_stale_manifest_slugs_outside_the_queue():
    latest = [("a", "done"), ("gone", "done"), ("gone2", "failed")]
    assert analyze_coverage(ANCHOR, ["a"], latest) == {
        "anchor_date": ANCHOR, "queue": 1, "done": 1, "no_page": 0, "failed": 0,
        "pending": 0, "unknown": 0, "covered": 1,
    }


def test_analyze_rejects_duplicate_queue_slugs():
    with pytest.raises(ValueError, match="duplicate slug in /Ranking queue"):
        analyze_coverage(ANCHOR, ["a", "a"], [])


def test_read_report_full_flow_with_saved_ranking_page():
    with gzip.open(RANKING_PAGE, "rt", encoding="utf-8") as page:
        page_date, slugs = parse_ranking_slugs(page.read())
    assert page_date == ANCHOR
    latest = [(slugs[0], "done"), (slugs[1], "done"), (slugs[2], "no_page")]
    trino = _client([[("2026-10-01",)], _page_response(), latest])

    assert read_report(trino, today=TODAY) == {
        "status": "ok", "anchor_date": ANCHOR, "queue": 498, "done": 2,
        "no_page": 1, "failed": 0, "pending": 495, "unknown": 0, "covered": 3,
        "queue_sha256": SHA, "queue_age_days": 1,
    }
    anchor_sql, page_sql, statuses_sql = [
        call.args[0] for call in trino.execute_query.call_args_list
    ]
    assert "max(rating_date)" in anchor_sql
    assert "to_base64(body)" in page_sql
    assert "page = '/Ranking'" in page_sql
    assert "DATE '2026-10-01'" in page_sql
    assert "ORDER BY fetched_at DESC" in page_sql
    assert "row_number()" in statuses_sql
    assert "PARTITION BY slug" in statuses_sql


def test_read_report_accepts_date_object_anchor():
    trino = _client([[(ANCHOR,)], _page_response(), [("Bayern", "done")]])
    assert read_report(trino, today=TODAY) == {
        "status": "ok", "anchor_date": ANCHOR, "queue": 498, "done": 1,
        "no_page": 0, "failed": 0, "pending": 497, "unknown": 0, "covered": 1,
        "queue_sha256": SHA, "queue_age_days": 1,
    }


def test_read_report_empty_manifest_short_circuits():
    trino = _client([[]])
    assert read_report(trino, today=TODAY) == {
        "status": "unavailable", "reason": "манифест истории пуст",
        "done": 0, "no_page": 0, "failed": 0, "pending": 0, "unknown": 0,
    }
    trino.execute_query.assert_called_once()


def test_read_report_missing_queue_page():
    trino = _client([[(ANCHOR,)], []])
    report = read_report(trino, today=TODAY)
    assert report["status"] == "unavailable"
    assert report["reason"] == "страница очереди /Ranking не найдена"


@pytest.mark.parametrize("body", ["%%% not base64", base64.b64encode(b"not gzip").decode("ascii")],
                         ids=["invalid-base64", "invalid-gzip"])
def test_read_report_corrupt_queue_page_never_raises(body):
    trino = _client([[(ANCHOR,)], [(body, SHA)]])
    report = read_report(trino, today=TODAY)
    assert report["status"] == "unavailable"
    assert report["reason"].startswith("очередь не читается: ")


def test_read_report_page_date_must_match_manifest_anchor():
    trino = _client([[("2026-10-03",)], _page_response()])
    report = read_report(trino, today=date(2026, 10, 4))
    assert report["status"] == "unavailable"
    assert report["reason"] == "дата страницы очереди не совпадает с манифестом"


def test_read_report_malformed_status_row():
    trino = _client([[(ANCHOR,)], _page_response(), [("only",)]])
    report = read_report(trino, today=TODAY)
    assert report["status"] == "unavailable"
    assert report["reason"] == "не разобран ответ манифеста"


def test_read_report_trino_failure_is_unavailable_not_raise():
    trino = _client([RuntimeError("trino")])
    report = read_report(trino, today=TODAY)
    assert report["status"] == "unavailable"
    assert report["reason"] == "ошибка чтения Bronze"


def test_read_report_queue_composition_change_moves_the_denominator():
    latest = [("Bayern", "done"), ("Dortmund", "no_page")]
    queue = ["Bayern", "Dortmund"]
    before = analyze_coverage(ANCHOR, queue, latest)
    after = analyze_coverage(ANCHOR, [*queue, "Leverkusen"], latest)
    assert (before["queue"], before["pending"], before["covered"]) == (2, 0, 2)
    assert (after["queue"], after["pending"], after["covered"]) == (3, 1, 2)


@pytest.fixture
def ok_report():
    return {
        "status": "ok", "anchor_date": date(2026, 11, 21), "queue": 1781,
        "queue_sha256": "7d0062b4" + "f" * 56, "queue_age_days": 1,
        "done": 1275, "no_page": 506, "failed": 0, "pending": 0, "unknown": 0,
        "covered": 1781,
    }


def test_format_line_exact_strings(ok_report):
    assert format_line(ok_report) == (
        "история: очередь 1781 (Ranking 21.11, sha 7d0062b4): "
        "done 1275, no_page 506, failed 0, pending 0, unknown 0"
    )
    unavailable = {"status": "unavailable", "reason": "манифест истории пуст"}
    assert format_line(unavailable) == "история: не посчитана — манифест истории пуст"


def test_format_line_cannot_print_a_fraction_denominator(ok_report):
    assert re.search(r"\d+\s*/\s*\d+", format_line(ok_report)) is None


def _fake_run(stdout, returncode=0):
    """A subprocess.run stand-in returning a captured CLI result."""
    def run(cmd, **kwargs):
        return Mock(returncode=returncode, stdout=stdout)
    return run


def test_cli_trino_parses_quoted_rows_and_nulls(monkeypatch):
    from scrapers.clubelo.history_coverage import CliTrino

    monkeypatch.setattr("scrapers.clubelo.history_coverage.subprocess.run",
                        _fake_run('"2026-10-01"\n"a","b"\n\n"c","","d"\n'))
    assert CliTrino("trino-ro.sh").execute_query("SELECT 1") == [
        ("2026-10-01",), ("a", "b"), ("c", None, "d")]


def test_cli_trino_failure_raises(monkeypatch):
    from scrapers.clubelo.history_coverage import CliTrino

    monkeypatch.setattr("scrapers.clubelo.history_coverage.subprocess.run",
                        _fake_run("", returncode=1))
    with pytest.raises(RuntimeError, match="trino"):
        CliTrino("trino-ro.sh").execute_query("SELECT 1")


def test_main_prints_formatted_line_then_json_report(monkeypatch, capsys):
    import json

    from scrapers.clubelo.history_coverage import main

    def run(cmd, **kwargs):
        sql = cmd[1]
        if "max(rating_date)" in sql:
            return Mock(returncode=0, stdout='"2026-10-01"\n')
        if "to_base64" in sql:
            return Mock(returncode=0, stdout=f'"{_page_response()[0][0]}","{SHA}"\n')
        return Mock(returncode=0, stdout='"Bayern","done"\n')

    monkeypatch.setattr("scrapers.clubelo.history_coverage.subprocess.run", run)
    assert main(["--trino", "trino-ro.sh", "--today", TODAY.isoformat()]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines[0] == (
        f"история: очередь 498 (Ranking 01.10, sha 7d0062b4): "
        "done 1, no_page 0, failed 0, pending 497, unknown 0")
    report = json.loads(lines[1])
    assert report["status"] == "ok" and report["queue"] == 498 and report["covered"] == 1


def test_main_unavailable_report_still_prints_two_lines(monkeypatch, capsys):
    import json

    from scrapers.clubelo.history_coverage import main

    monkeypatch.setattr("scrapers.clubelo.history_coverage.subprocess.run",
                        _fake_run("", returncode=1))
    assert main(["--trino", "trino-ro.sh", "--today", TODAY.isoformat()]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines[0] == "история: не посчитана — ошибка чтения Bronze"
    assert json.loads(lines[1])["status"] == "unavailable"


def test_read_report_rejects_malformed_page_digest():
    # Astra r2: валидное тело очереди — недоступность вызывает ТОЛЬКО дайджест,
    # не побочное падение gzip на мусорном теле
    trino = _client([[("2026-10-01",)], [(_page_response()[0][0], "x")], []])
    report = read_report(trino, today=TODAY)
    assert report["status"] == "unavailable"
    assert report["reason"].startswith("очередь не читается: ")


def test_cli_trino_keeps_quotes_that_belong_to_the_value(monkeypatch):
    from scrapers.clubelo.history_coverage import CliTrino

    # CSV удваивает кавычки значения: дайджест из 64 «a» с литеральными кавычками
    # не должен превращаться в валидный hex после обрезки краёв (Astra r2)
    csv_line = '"x","""' + "a" * 64 + '"""'
    monkeypatch.setattr("scrapers.clubelo.history_coverage.subprocess.run",
                        _fake_run(csv_line + "\n"))
    rows = CliTrino("trino-ro.sh").execute_query("SELECT 1")
    assert rows == [("x", '"' + "a" * 64 + '"')]
    assert not re.fullmatch("[0-9a-f]{64}", rows[0][1])


def test_main_invalid_today_reason_is_one_line(monkeypatch, capsys):
    import json

    from scrapers.clubelo.history_coverage import main

    monkeypatch.setattr("scrapers.clubelo.history_coverage.subprocess.run",
                        _fake_run("", returncode=0))
    assert main(["--trino", "trino-ro.sh", "--today", "2026-02-30\nUTC"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 2
    assert lines[0] == "история: не посчитана — некорректная дата --today: '2026-02-30\\nUTC'"
    assert json.loads(lines[1])["status"] == "unavailable"


def test_main_invalid_today_prints_unavailable(monkeypatch, capsys):
    import json

    from scrapers.clubelo.history_coverage import main

    monkeypatch.setattr("scrapers.clubelo.history_coverage.subprocess.run",
                        _fake_run("", returncode=0))
    assert main(["--trino", "trino-ro.sh", "--today", "2026-02-30"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines[0].startswith("история: не посчитана — некорректная дата --today")
    assert json.loads(lines[1])["status"] == "unavailable"


def test_cli_trino_accepts_large_base64_fields(monkeypatch):
    from scrapers.clubelo.history_coverage import CliTrino

    # Astra r3: живая страница /Ranking даёт ~140 k символов base64 в одном поле;
    # дефолтный csv-лимит 131072 ломал бы чтение очереди
    big = "a" * 140_000
    monkeypatch.setattr("scrapers.clubelo.history_coverage.subprocess.run",
                        _fake_run(f'"{big}","{SHA}"\n'))
    assert CliTrino("trino-ro.sh").execute_query("SELECT 1") == [(big, SHA)]
