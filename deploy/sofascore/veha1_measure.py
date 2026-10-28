#!/usr/bin/env python3
"""Измеритель вехи 1 SofaScore (#1355): доля матчей ядра, собранных за 24 ч после окончания.

Режимы:
  report [--dry-run] [--denominator PATH]
      observe (журнал первого наблюдения finished) + суточный итог одной строкой в stdout.
      Итог дня D (вчера по UTC) — матчи журнала с deadline < сегодня 00:00 UTC, ещё не
      отчитанные. Повторный запуск за тот же D печатает сохранённую строку.
      --dry-run ничего не пишет (журнал дополняется только в памяти).
  observe [--denominator PATH]
      только дополнить журнал.
  --c1 START END [--denominator PATH]
      воспроизведение замера C1 (окно старта [START, END), даты YYYY-MM-DD) без журнала:
      окончание = старт + 7200, закрытие = любая строка capture_status; ядро по старому
      правилу (211 ключей sofascore_veha1_c1_noncore.txt) и по файлу знаменателя.

Знаменатель — class == core в <SOFASCORE_RELEASE_ROOT>/configs/sofascore/denominator.tsv
(из /etc/data-platform/sofascore.env читается только строка SOFASCORE_RELEASE_ROOT=).
Матч → турнир: партиция SS-<id> (полоса SS) или tournament_unique_tournament_id расписания
для партиций реестра дейли (ENG-… и т. п., полоса daily). Trino — только trino-ro.sh.
Пишет только в /root/watchdog/state/sofascore_veha1/.
"""
from __future__ import annotations

import csv
import fcntl
import io
import json
import os
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

TRINO = "/root/.claude/bin/trino-ro.sh"
SS_ENV = "/etc/data-platform/sofascore.env"
TOOLS = Path(__file__).resolve().parent
STATE = Path("/root/watchdog/state/sofascore_veha1")
JOURNAL = STATE / "journal.jsonl"
DAILY = STATE / "daily.jsonl"
LOCK = STATE / ".lock"
OBSERVE_STATE = STATE / "observe.json"  # время прошлого observe: доказательство «поздно размечено»
CLOSURE_DAYS = 9
THRESHOLD = 99.0
OBSERVE_DAYS = 7
CHUNK = 2000
# scrapers/sofascore/adapters.py _legacy_state: success; LEGITIMATE_EMPTY/NOT_SUPPORTED → not_available
# (404 источника терминально). schema_error/blocked/rate_limited/server_error/transport_error — не закрыт.
TERMINAL = ("success", "not_available")
ENDPOINTS = ("event", "lineups", "statistics", "shotmap", "incidents")
UNMARKED = ("notstarted", "inprogress", "postponed", "canceled", "interrupted", "suspended")
DENOM_COLUMNS = ["tournament_id", "capture_key", "name", "class", "queue_priority", "basis"]


class MeasureError(RuntimeError):
    pass


# ---------------------------------------------------------------- Trino / файлы
def _sql(name: str, **params: str) -> str:
    text = (TOOLS / f"sofascore_veha1_{name}.sql").read_text(encoding="utf-8")
    for key, value in params.items():
        text = text.replace("{" + key + "}", value)
    return text


def trino_rows(sql: str, ncols: int) -> list[list[str | None]]:
    """Строки ответа без сентинела; "" → None. Любое отклонение формата — MeasureError."""
    if len(sql.encode()) > 120_000:
        raise MeasureError("SQL длиннее 120 КБ (MAX_ARG_STRLEN)")
    try:
        r = subprocess.run([TRINO, sql], capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        raise MeasureError("trino: таймаут 600 с") from None
    if r.returncode != 0:
        raise MeasureError(f"trino: код {r.returncode}: {(r.stdout + r.stderr).strip()[:200]}")
    rows, sentinel = [], False
    for row in csv.reader(io.StringIO(r.stdout)):
        if not row:
            continue
        if len(row) != ncols:
            raise MeasureError(f"trino: неожиданный ответ: {','.join(row)[:200]}")
        if row[0] == "__sentinel__":
            sentinel = True
            continue
        rows.append([c if c != "" else None for c in row])
    if not sentinel:
        raise MeasureError("trino: нет строки __sentinel__ в ответе")
    return rows


def release_denominator() -> Path:
    try:
        for line in Path(SS_ENV).read_text(encoding="utf-8").splitlines():
            if line.startswith("SOFASCORE_RELEASE_ROOT="):
                root = line.split("=", 1)[1].strip().strip('"').strip("'")
                return Path(root) / "configs" / "sofascore" / "denominator.tsv"
    except OSError as exc:
        raise MeasureError(f"не читается {SS_ENV}: {type(exc).__name__}") from None
    raise MeasureError(f"в {SS_ENV} нет строки SOFASCORE_RELEASE_ROOT=")


def load_classes(path: Path) -> dict[int, str]:
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle, delimiter="\t", quoting=csv.QUOTE_NONE)
            if next(reader, None) != DENOM_COLUMNS:
                raise MeasureError(f"{path}: неожиданный заголовок")
            classes = {int(row[0]): row[3] for row in reader if row}
    except FileNotFoundError:
        raise MeasureError(f"файла знаменателя нет: {path}") from None
    except (OSError, ValueError, IndexError) as exc:
        raise MeasureError(f"{path}: {type(exc).__name__}") from None
    if not any(c == "core" for c in classes.values()):
        raise MeasureError(f"{path}: нет ни одного турнира core")
    return classes


def _int(value: str | None) -> int | None:
    return int(float(value)) if value not in (None, "") else None


def game_tournament(ss_league: str | None, reg_league: str | None, tut: str | None):
    """(tournament_id, lane, league): партиция реестра дейли важнее SS."""
    if reg_league:
        return _int(tut), "daily", reg_league
    if ss_league and ss_league.startswith("SS-"):
        return int(ss_league[3:]), "SS", ss_league
    return None, None, ss_league or reg_league


def read_journal() -> dict[str, dict]:
    journal: dict[str, dict] = {}
    if JOURNAL.exists():
        for line in JOURNAL.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rec = json.loads(line)
                journal[str(rec["game_id"])] = rec
    return journal


def _atomic_write(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    os.replace(tmp, path)


def write_journal(journal: dict[str, dict], observed_at: datetime) -> None:
    _atomic_write(JOURNAL, "".join(
        json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n" for rec in journal.values()
    ))
    _atomic_write(OBSERVE_STATE, json.dumps({"last_observe_at": int(observed_at.timestamp())}) + "\n")


def _iso(ts: float | None) -> str:
    if ts is None:
        return "-"
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M")


# ---------------------------------------------------------------- observe
def read_last_observe() -> int | None:
    try:
        return int(json.loads(OBSERVE_STATE.read_text(encoding="utf-8"))["last_observe_at"])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def status_debt(now: datetime, *, include_finished: bool = False) -> list[list[str | None]]:
    """Latest natural keys, then one lane per game; no seven-day expiry of debt."""
    year = now.year
    seasons = (str(year), f"{(year - 1) % 100:02d}{year % 100:02d}",
               f"{year % 100:02d}{(year + 1) % 100:02d}")
    slugs = ", ".join(f"'{season}'" for season in seasons)
    excluded = "'postponed', 'canceled'" if include_finished else "'finished', 'postponed', 'canceled'"
    return trino_rows(f"""
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY league, season, game_id ORDER BY _ingested_at DESC
    ) AS rn
    FROM iceberg.bronze.sofascore_schedule
), games AS (
    SELECT *, row_number() OVER (
        PARTITION BY game_id
        ORDER BY CASE WHEN league LIKE 'SS-%' THEN 1 ELSE 0 END, _ingested_at DESC
    ) AS game_rn
    FROM ranked WHERE rn = 1
)
SELECT CAST(game_id AS varchar), CAST(start_timestamp AS varchar),
       CASE WHEN league LIKE 'SS-%' THEN league END,
       CASE WHEN league LIKE 'SS-%' THEN CAST(season AS varchar) END,
       CASE WHEN league NOT LIKE 'SS-%' THEN league END,
       CASE WHEN league NOT LIKE 'SS-%' THEN CAST(season AS varchar) END,
       CAST(tournament_unique_tournament_id AS varchar),
       coalesce(status_type, 'unknown'), CAST(_ingested_at AS varchar)
FROM games
WHERE game_rn = 1 AND CAST(season AS varchar) IN ({slugs})
  AND start_timestamp < {int(now.timestamp()) - 86400}
  AND coalesce(status_type, 'unknown') NOT IN ({excluded})
UNION ALL SELECT '__sentinel__', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
""", 9)


def observe(journal: dict[str, dict], now: datetime, *, debt_rows=None) -> tuple[int, dict[str, list]]:
    """В памяти: (1) дописать finished-матчи со стартом за 7 суток, которых нет в журнале, с отметкой
    прошлого observe; (2) запомнить в записи первое увиденное закрытие (closed_at) — capture_status
    хранит одну строку на матч и MERGE перезаписывает _ingested_at, поэтому min() в Trino со временем
    уезжает вперёд. Возвращает (добавлено, последние статусы незакрытых)."""
    prev_observe = read_last_observe()
    until = int(now.timestamp())
    since = until - OBSERVE_DAYS * 86400
    rows = trino_rows(_sql("observe", since=str(since), until=str(until)), 8)
    added = 0
    for gid, st, change, ss_league, ss_season, reg_league, reg_season, tut in rows:
        if st is None:
            continue
        previous = journal.get(gid)
        if previous is not None and previous.get("latest_status", previous.get("status_first_seen")) == "finished":
            continue
        start = int(st)
        tid, lane, league = game_tournament(ss_league, reg_league, tut)
        change_ts = _int(change)
        if change_ts is not None and start + 45 * 60 <= change_ts <= start + 4 * 3600:
            end, end_source = change_ts, "changes_change_timestamp"
        else:
            end, end_source = start + 7200, "start+7200"
        journal[gid] = {
            **(previous or {}),
            "game_id": gid,
            "tournament_id": tid,
            "lane": lane,
            "league": league,
            "season": reg_season if lane == "daily" else ss_season,
            "start_ts": start,
            "end_ts": end,
            "end_source": end_source,
            "source_change_ts": change_ts,
            "deadline_ts": end + 86400,
            "first_seen_finished_at": int(now.timestamp()),
            "prev_observe_at": prev_observe,
            "status_first_seen": (previous or {}).get("status_first_seen", "finished"),
            "latest_status": "finished",
        }
        if previous is not None:
            journal[gid]["first_seen_finished_at"] = previous.get("first_seen_finished_at", until)
        else:
            added += 1
    for gid, st, ss_league, ss_season, reg_league, reg_season, tut, status, ingested in (
        status_debt(now) if debt_rows is None else debt_rows
    ):
        if gid in journal or st is None:
            continue
        start = int(st)
        tid, lane, league = game_tournament(ss_league, reg_league, tut)
        journal[gid] = {
            "game_id": gid, "tournament_id": tid, "lane": lane, "league": league,
            "season": reg_season if lane == "daily" else ss_season,
            "start_ts": start, "end_ts": None, "end_source": None,
            "deadline_ts": start + 86400, "status_deadline_ts": start + 86400,
            "measurement_deadline_ts": start + 86400,
            "status_first_seen": status, "latest_status": status,
            "status_debt": True, "schedule_ingested_at": ingested,
            "prev_observe_at": prev_observe,
        }
        added += 1
    horizon = until - CLOSURE_DAYS * 86400
    open_ids = [g for g, rec in journal.items()
                if rec.get("closed_at") is None and rec["start_ts"] >= horizon]
    latest = closures(open_ids)
    for gid, info in latest.items():
        if info[0] and gid in journal and journal[gid].get("closed_at") is None:
            journal[gid]["closed_at"] = float(info[0])
    return added, latest


def seen_late(rec: dict) -> bool:
    """finished увиден НАМИ позже 24 ч после старта: прошлый observe был позже старт + 24 ч и матча
    finished в bronze не видел. Это не доказывает позднюю разметку ИСТОЧНИКОМ (мог отстать наш сбор
    афиши; changes_change_timestamp — время последней правки, не перехода в finished), поэтому матч
    ОСТАЁТСЯ в знаменателе, а число печатается справочно (Astra #1355, круг 3). Исключать из критерия
    можно будет только по улике времени перехода статуса у источника — её в данных нет."""
    prev = rec.get("prev_observe_at")
    return (rec.get("first_seen_finished_at") is not None
            and prev is not None and prev - rec["start_ts"] > 86400)


# ---------------------------------------------------------------- report
def closures(game_ids: list[str]) -> dict[str, list]:
    terminal = ", ".join(f"'{s}'" for s in TERMINAL)
    out: dict[str, list] = {}
    for i in range(0, len(game_ids), CHUNK):
        ids = ", ".join(f"'{g}'" for g in game_ids[i:i + CHUNK])
        for row in trino_rows(_sql("closure", terminal=terminal, ids=ids), 8):
            out[row[0]] = row[1:]
    return out


def unmarked(classes: dict[int, str], now: datetime) -> Counter:
    until = int(now.timestamp()) - 86400
    since = int(now.timestamp()) - OBSERVE_DAYS * 86400
    statuses = ", ".join(f"'{s}'" for s in UNMARKED)
    counts: Counter = Counter()
    for _gid, status, ss_league, reg_league, tut in trino_rows(
        _sql("unmarked", since=str(since), until=str(until), statuses=statuses), 5
    ):
        tid, _lane, _league = game_tournament(ss_league, reg_league, tut)
        if tid is not None and classes.get(tid) == "core":
            counts[status] += 1
    return counts


def report(classes: dict[int, str], denominator: Path, dry_run: bool) -> str:
    now = datetime.now(timezone.utc)
    day = now.date() - timedelta(days=1)
    cutoff = datetime.combine(now.date(), time(0), timezone.utc).timestamp()
    if dry_run:  # сухой прогон ничего не создаёт и не берёт замок
        return _report_locked(classes, denominator, now, day, cutoff, True)
    STATE.mkdir(parents=True, exist_ok=True)
    with open(LOCK, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        return _report_locked(classes, denominator, now, day, cutoff, False)


def read_daily() -> list[dict]:
    """Итоги дней; битая строка (старый обрыв записи) пропускается, а не роняет измеритель."""
    rows = []
    if DAILY.exists():
        for line in DAILY.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if isinstance(row, dict) and row.get("day"):
                rows.append(row)
    return rows


def _report_locked(classes, denominator, now, day, cutoff, dry_run: bool) -> str:
    daily = read_daily()
    for row in daily:
        if row["day"] == day.isoformat():
            return row["line"]
    journal = read_journal()
    current_rows = status_debt(now, include_finished=True)
    debt_rows = [row for row in current_rows if row[7] != "finished"]
    current_total = sum(classes.get(game_tournament(row[2], row[4], row[6])[0]) == "core"
                        for row in current_rows)
    added, latest = observe(journal, now, debt_rows=debt_rows)
    current_debt = []
    for gid, st, ss_league, _ss_season, reg_league, _reg_season, tut, status, ingested in debt_rows:
        tid, lane, _league = game_tournament(ss_league, reg_league, tut)
        if classes.get(tid) == "core" and st is not None:
            current_debt.append({
                "game_id": gid, "status": status, "lane": lane,
                "age_hours": round((now.timestamp() - int(st)) / 3600, 1),
                "schedule_ingested_at": ingested,
            })
    current_statuses = Counter(rec["status"] for rec in current_debt)
    current_share = 100.0 * len(current_debt) / current_total if current_total else None
    current_share_txt = f"{current_share:.2f} %" if current_share is not None else "не наблюдается"
    # Обрыв после записи журнала и до daily.jsonl оставляет reported_day = X без итога X. Такие записи
    # (любого X, не только сегодняшнего) снова входят в due и отчитываются этим днём — не теряются.
    done_days = {row["day"] for row in daily}
    due = [rec for rec in journal.values()
           if rec.get("measurement_deadline_ts", rec["deadline_ts"]) < cutoff
           and (not rec.get("reported_day") or rec["reported_day"] not in done_days)]
    core = [rec for rec in due if classes.get(rec["tournament_id"]) == "core"]
    late = [rec for rec in core if seen_late(rec)]
    population = core
    lanes = {"SS": [0, 0], "daily": [0, 0]}
    misses: Counter = Counter()
    violators = []
    ok = 0
    for rec in population:
        info = latest.get(rec["game_id"])
        closed_at = rec.get("closed_at")
        hit = (not rec.get("status_debt") and closed_at is not None
               and closed_at <= rec["deadline_ts"])
        lane = lanes.setdefault(rec["lane"] or "?", [0, 0])
        lane[1] += 1
        if hit:
            ok += 1
            lane[0] += 1
            continue
        misses[rec["tournament_id"]] += 1
        if rec.get("status_debt"):
            missing = "status overdue: " + rec["status_first_seen"]
        elif closed_at is not None:
            missing = "terminal after deadline"
        elif info is None:
            missing = "no capture row"
        else:
            missing = ",".join(
                name for name, st in zip(ENDPOINTS, info[2:]) if st not in TERMINAL
            ) or "terminal after deadline"
        violators.append([rec["game_id"], rec["league"], rec["season"] or "-",
                          _iso(rec["start_ts"]), _iso(rec["deadline_ts"]),
                          _iso(closed_at), missing])
    debt = Counter(rec["status_first_seen"] for rec in core if rec.get("status_debt"))
    n = len(population)
    share = 100.0 * ok / n if n else 0.0
    mark = "✅" if n > 0 and share >= THRESHOLD else "⛔"
    un = unmarked(classes, now)
    vfile = STATE / f"{day.isoformat()}.tsv"
    share_txt = f"{share:.1f}".replace(".", ",")
    line = (
        f"• веха 1 SofaScore за {day:%d.%m} (UTC): ядро ≤ 24 ч {ok}/{n} = "
        f"{share_txt} % {mark} (порог ≥ 99 %; "
        f"SS {lanes['SS'][0]}/{lanes['SS'][1]}, дейли {lanes['daily'][0]}/{lanes['daily'][1]}); "
        f"finished увиден позже 24 ч после старта {len(late)} (в знаменателе); не размечено: notstarted {un['notstarted']}, "
        f"inprogress {un['inprogress']}, postponed {un['postponed']}, "
        f"canceled {un['canceled']}; долг статусов в знаменателе {sum(debt.values())}: "
        f"{dict(debt)}; текущий долг {len(current_debt)}/{current_total} = "
        f"{current_share_txt}: {dict(current_statuses)}; "
        f"нарушители → {vfile}"
    )
    if dry_run:
        return line + f" [dry-run, в журнал бы добавлено {added}]"
    # Порядок записи: нарушители → журнал → daily.jsonl (точка фиксации дня, атомарная перезапись).
    # Обрыв до daily.jsonl: следующий прогон (в любой день) берёт эти записи снова — см. due выше.
    _atomic_write(vfile, "".join(
        "\t".join(str(c) for c in row) + "\n" for row in
        [["game_id", "league", "season", "start", "deadline", "closed_at", "missing_endpoints"],
         *violators]
    ))
    summary = {
        "day": day.isoformat(), "measured_at": now.isoformat(timespec="seconds"),
        "denominator": str(denominator), "core_in24": ok, "core_total": n,
        "share": round(share, 2), "pass": mark == "✅", "threshold": THRESHOLD,
        "lanes": {k: {"in24": v[0], "total": v[1]} for k, v in lanes.items()},
        "seen_finished_late_in_denominator": len(late), "unmarked": dict(un),
        "top_misses": [[tid, cnt] for tid, cnt in misses.most_common(10)],
        "status_debt_current": dict(current_statuses),
        "status_debt_current_total": current_total,
        "status_debt_current_share": round(current_share, 2) if current_share is not None else None,
        "status_debt_current_details": current_debt,
        "status_debt_in_denominator": dict(debt),
        "status_debt_share": round(100.0 * sum(debt.values()) / n, 2) if n else None,
        "status_debt_details": [
            {"game_id": rec["game_id"], "status": rec["status_first_seen"],
             "lane": rec["lane"], "age_hours": round((now.timestamp() - rec["start_ts"]) / 3600, 1),
             "schedule_ingested_at": rec.get("schedule_ingested_at")}
            for rec in core if rec.get("status_debt")
        ],
        "journal_added": added, "violators_file": str(vfile), "line": line,
    }
    for rec in due:
        rec["reported_day"] = day.isoformat()
    write_journal(journal, now)
    _atomic_write(DAILY, "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in [*daily, summary]))
    return line


# ---------------------------------------------------------------- C1
def c1(classes: dict[int, str], start: date, end: date) -> str:
    since = int(datetime.combine(start, time(0), timezone.utc).timestamp())
    until = int(datetime.combine(end, time(0), timezone.utc).timestamp())
    noncore_old = set((TOOLS / "sofascore_veha1_c1_noncore.txt").read_text().split())
    rows = trino_rows(_sql("c1", since=str(since), until=str(until)), 6)
    old = {"core": [0, 0, 0], "noncore": [0, 0, 0]}  # fin, closed_ever, in24
    new = {"core": [0, 0, 0], "noncore": [0, 0, 0]}
    for _gid, st, ss_league, reg_league, tut, closed in rows:
        start_ts = int(st)
        is_old_core = not (ss_league in noncore_old and not reg_league)
        tid, _lane, _league = game_tournament(ss_league, reg_league, tut)
        is_new_core = tid is not None and classes.get(tid) == "core"
        closed_ts = float(closed) if closed else None
        in24 = closed_ts is not None and closed_ts <= start_ts + 7200 + 86400
        for bucket, flag in ((old, is_old_core), (new, is_new_core)):
            cell = bucket["core" if flag else "noncore"]
            cell[0] += 1
            cell[1] += closed_ts is not None
            cell[2] += in24
    fin = len(rows)
    in24 = old["core"][2] + old["noncore"][2]
    lines = [
        f"C1 окно старта {start}–{end} (UTC): finished {fin} (замер C1 22.09 — 3 074; "
        f"разница {fin - 3074:+d}: размечены finished после замера C1; этим режимом не проверяется — "
        f"первого наблюдения в расписании нет, MERGE перезаписывает _ingested_at), ≤ 24 ч всего {in24}",
        f"  старое правило (211 ключей): ядро {old['core'][2]}/{old['core'][0]} "
        f"(закрыто когда-либо {old['core'][1]}), не ядро {old['noncore'][2]}/{old['noncore'][0]}",
        f"  файл знаменателя: ядро {new['core'][2]}/{new['core'][0]} "
        f"(закрыто когда-либо {new['core'][1]}), не ядро {new['noncore'][2]}/{new['noncore'][0]}",
    ]
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    args = list(argv)
    denominator = None
    if "--denominator" in args:
        i = args.index("--denominator")
        denominator = Path(args[i + 1])
        del args[i:i + 2]
    dry_run = "--dry-run" in args
    args = [a for a in args if a != "--dry-run"]
    try:
        path = denominator or release_denominator()
        classes = load_classes(path)
        if args[:1] == ["--c1"] and len(args) == 3:
            print(c1(classes, date.fromisoformat(args[1]), date.fromisoformat(args[2])))
        elif args == ["report"]:
            print(report(classes, path, dry_run))
        elif args == ["observe"]:
            now = datetime.now(timezone.utc)
            if dry_run:
                journal = read_journal()
                added, _ = observe(journal, now)
            else:
                STATE.mkdir(parents=True, exist_ok=True)
                with open(LOCK, "a") as lock:
                    fcntl.flock(lock, fcntl.LOCK_EX)
                    journal = read_journal()
                    added, _ = observe(journal, now)
                    write_journal(journal, now)
            print(f"observe: добавлено {added}, в журнале {len(journal)}")
        else:
            print(__doc__, file=sys.stderr)
            return 2
    except MeasureError as exc:
        print(f"⚠️ измеритель вехи 1: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
