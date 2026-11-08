#!/usr/bin/env python3
"""Read-only отчёт семидневного Bronze-паритета daily/all-men (#1370).

Проверяет, что каждый ``finished`` матч турниров enabled-реестра
(``SofaScoreCatalog.tournament_map(enabled_only=True)``) в окне ``[start, start+7)``
имеет ``capture_complete`` хотя бы под одним ключом — daily или SS (union coverage).
Во время параллельной работы двух копий матч НЕ обязан иметь обе: capture
пропускается глобально по match_id. ``postponed``/``canceled`` покрытия не требуют.
Физический overlap (``both_complete`` — закрыты оба ключа) информационный и логическим
дублем не считается: логических дублей нет после дедупликации по match_id
(``logical_duplicates=0``). Дни без матчей остаются в отчёте с нулевыми счётчиками.

Отчёт описывает только Bronze: каждый JSON несёт
``acceptance="scheduler_and_dq_evidence_required"`` и НЕ доказывает приёмку
scheduler/DQ — решение о выводе дейли владелец принимает по отдельному доказательству.

Запуск (только SELECT; Trino — контракт ``veha1_measure.trino_rows`` с sentinel
``__sentinel__``):

  python deploy/sofascore/daily_parity.py --start YYYY-MM-DD --days 7 --output PATH

JSON печатается в stdout; ``--output`` атомарно (tmp + rename) сохраняет тот же текст —
единственная запись на диске, других runtime-записей нет. Pure-ядро —
``build_report(rows, *, start, days=7)``: ровно 7 дней, каждая строка обязана попадать
в окно, повтор ``match_id`` — ValueError (SQL группирует по (day, match_id)).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

try:  # пакетный импорт: тесты и PYTHONPATH=корень релиза
    from deploy.sofascore import veha1_measure as measure
except ImportError:  # прямой запуск: sys.path[0] — каталог deploy/sofascore
    import veha1_measure as measure

TOOLS = Path(__file__).resolve().parent
PARITY_DAYS = 7
ACCEPTANCE = "scheduler_and_dq_evidence_required"
REPORT_COLUMNS = ("day", "match_id", "status_type", "any_complete", "both_complete")
FINISHED = "finished"


class ParityError(RuntimeError):
    pass


def _catalog_source():
    try:
        from scrapers.sofascore.catalog import SofaScoreCatalog
    except ImportError:
        root = TOOLS.parent.parent
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        from scrapers.sofascore.catalog import SofaScoreCatalog
    return SofaScoreCatalog


def enabled_tournament_ids() -> list[int]:
    ids = sorted({int(v)
                  for v in _catalog_source().load().tournament_map(enabled_only=True).values()})
    if not ids:
        raise ParityError("enabled-реестр пуст: source tournament IDs не найдены")
    return ids


def parity_sql(tournament_ids: list[int], since: int, until: int) -> str:
    text = (TOOLS / "sofascore_daily_parity.sql").read_text(encoding="utf-8")
    return (text.replace("{tournament_ids}", ", ".join(str(int(v)) for v in tournament_ids))
                .replace("{since}", str(int(since)))
                .replace("{until}", str(int(until))))


def window_bounds(start: date, days: int) -> tuple[int, int]:
    since = int(datetime.combine(start, time(0), timezone.utc).timestamp())
    until = int(datetime.combine(start + timedelta(days=days), time(0), timezone.utc).timestamp())
    return since, until


def fetch_rows(start: date, days: int) -> list[dict]:
    since, until = window_bounds(start, days)
    sql = parity_sql(enabled_tournament_ids(), since, until)
    return [dict(zip(REPORT_COLUMNS, row)) for row in measure.trino_rows(sql, len(REPORT_COLUMNS))]


def _row_day(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError:
        raise ValueError(f"колонка day не ISO-дата: {value!r}") from None


def _match_id(value) -> str:
    match_id = "" if value is None else str(value).strip()
    if not match_id:
        raise ValueError("пустой match_id в строке отчёта")
    return match_id


def _status_type(value) -> str:
    return "unknown" if value in (None, "") else str(value).strip()


def _coverage_flag(value) -> int:
    if value in (None, ""):
        return 0
    flag = int(value)
    if flag not in (0, 1):
        raise ValueError(f"флаг покрытия должен быть 0 или 1, получено {value!r}")
    return flag


def build_report(rows: list[dict], *, start: date, days: int = 7) -> dict:
    """Pure-агрегация строк SQL (day, match_id, status_type, any_complete, both_complete)."""
    if days != PARITY_DAYS:
        raise ValueError(f"приёмка требует ровно {PARITY_DAYS} дней в окне, получено {days}")
    horizon = [start + timedelta(offset) for offset in range(days)]
    counters = {day: {"finished": 0, "covered": 0} for day in horizon}
    seen: set[str] = set()
    missing: list[str] = []
    both = 0
    for row in rows:
        day = _row_day(row.get("day"))
        if day not in counters:
            raise ValueError(
                f"строка вне окна [{start}, {start + timedelta(days=days)}): {day.isoformat()}")
        match_id = _match_id(row.get("match_id"))
        if match_id in seen:
            raise ValueError(
                f"повтор match_id (SQL обязан группировать по (day, match_id)): {match_id}")
        seen.add(match_id)
        any_complete = _coverage_flag(row.get("any_complete"))
        both += _coverage_flag(row.get("both_complete"))
        if _status_type(row.get("status_type")) == FINISHED:
            counters[day]["finished"] += 1
            if any_complete:
                counters[day]["covered"] += 1
            else:
                missing.append(match_id)
    missing.sort()
    return {
        "acceptance": ACCEPTANCE,
        "status": "fail" if missing else "pass",
        "window": {
            "start": start.isoformat(),
            "until": (start + timedelta(days=days)).isoformat(),
            "days": days,
        },
        "days": [{"day": day.isoformat(), **counters[day]} for day in horizon],
        "finished_total": sum(c["finished"] for c in counters.values()),
        "covered_total": sum(c["covered"] for c in counters.values()),
        "missing_finished": missing,
        "physical_both_complete": both,
        "logical_duplicates": 0,
    }


def _atomic_write(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    os.replace(tmp, path)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="daily_parity.py",
        description="Read-only отчёт семидневного Bronze-паритета daily/all-men (#1370).",
    )
    parser.add_argument("--start", required=True, metavar="YYYY-MM-DD",
                        help="начало окна, UTC")
    parser.add_argument("--days", type=int, default=PARITY_DAYS,
                        help=f"дней в окне (приёмка — только {PARITY_DAYS})")
    parser.add_argument("--output", type=Path, default=None,
                        help="атомарно записать тот же JSON в файл")
    args = parser.parse_args(list(argv))
    try:
        start = date.fromisoformat(args.start)
        if args.days != PARITY_DAYS:
            raise ValueError(
                f"приёмка требует ровно {PARITY_DAYS} дней в окне, получено {args.days}")
        report = build_report(fetch_rows(start, args.days), start=start, days=args.days)
    except (ValueError, ParityError, measure.MeasureError, OSError) as exc:
        print(f"⚠️ отчёт паритета дейли: {exc}", file=sys.stderr)
        return 1
    text = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output is not None:
        _atomic_write(args.output, text)
    print(text, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
