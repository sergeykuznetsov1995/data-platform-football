"""Frozen daily section of the pre-#2342 morning report for offline patch tests."""
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

UNDERSTAT_DAILY_STATE = "/root/watchdog/state/understat_daily_criterion.json"
UNDERSTAT_FAILURES = "iceberg.ops.understat_ingest_failures_v1"
UNDERSTAT_TARGET_PCT = Decimal("99")
UNDERSTAT_FINAL_WINDOW_H = 26      # итоговая попытка (complete/not_published/upstream_pending) за сутки + запас
UNDERSTAT_PENDING_RED_H = 48       # not_published/upstream_pending дольше — сверка с FotMob
UNDERSTAT_SHORT = {"ENG-Premier League": "ENG", "ESP-La Liga": "ESP", "GER-Bundesliga": "GER",
                   "ITA-Serie A": "ITA", "FRA-Ligue 1": "FRA", "RUS-Premier League": "RUS"}
UNDERSTAT_DAILY_SQL = """WITH
lc AS (
    SELECT league, season, batch_id
    FROM (
        SELECT league, season, batch_id,
               ROW_NUMBER() OVER (
                   PARTITION BY league, season
                   ORDER BY completed_at DESC, attempt_id DESC
               ) AS rn
        FROM iceberg.ops.understat_ingest_manifest_v1
        WHERE contract_version = 'understat-bronze-v2' AND status = 'complete'
    )
    WHERE rn = 1
),
sched AS (
    SELECT s.league, s.season, CAST(s.game_id AS varchar) AS game_id,
           s.date AS kickoff, s.is_result,
           CASE WHEN s.is_result THEN count_if(s.is_result) OVER (
               PARTITION BY s.league, s.season ORDER BY s.date
           ) END AS ordinal
    FROM iceberg.bronze.understat_schedule s
    JOIN lc ON lc.league = s.league AND lc.season = s.season
    WHERE s._batch_id = lc.batch_id
),
cand AS (
    SELECT league, season, game_id, kickoff, is_result, ordinal,
           kickoff + INTERVAL '26' HOUR AS deadline
    FROM sched
    WHERE kickoff + INTERVAL '26' HOUR >= TIMESTAMP '{day} 00:00:00'
      AND kickoff + INTERVAL '26' HOUR < TIMESTAMP '{day} 00:00:00' + INTERVAL '1' DAY
),
att AS (
    SELECT league, season,
           CAST(from_iso8601_timestamp(completed_at) AT TIME ZONE 'UTC'
                AS timestamp(6)) AS done,
           CAST(json_extract_scalar(quality_json, '$.completed_game_count')
                AS integer) AS cg,
           CAST(json_extract(quality_json, '$.covered_game_ids')
                AS array(varchar)) AS covered,
           CAST(json_extract(quality_json, '$.site_result_game_ids')
                AS array(varchar)) AS site
    FROM iceberg.ops.understat_ingest_manifest_v1
    WHERE contract_version = 'understat-bronze-v2' AND status = 'complete'
),
journal AS (
    SELECT f.league, f.season,
           CAST(from_iso8601_timestamp(f.completed_at) AT TIME ZONE 'UTC'
                AS timestamp(6)) AS done,
           CAST(json_extract(f.quality_json, '$.site_result_game_ids')
                AS array(varchar)) AS site
    FROM {failures} f
    WHERE f.contract_version = 'understat-bronze-v2'
      AND json_extract_scalar(f.quality_json, '$.site_result_known') = 'true'
),
seen AS (
    SELECT league, season, done, site FROM att WHERE site IS NOT NULL
    UNION ALL
    SELECT league, season, done, site FROM journal
),
seen_by_deadline AS (
    SELECT DISTINCT c.league, c.season, c.game_id
    FROM cand c
    JOIN seen v
      ON v.league = c.league AND v.season = c.season
     AND v.done <= c.deadline AND contains(v.site, c.game_id)
),
due AS (
    SELECT c.league, c.season, c.game_id, c.kickoff, c.ordinal, c.deadline
    FROM cand c
    LEFT JOIN seen_by_deadline sd
      ON sd.league = c.league AND sd.season = c.season AND sd.game_id = c.game_id
    WHERE c.is_result OR sd.game_id IS NOT NULL
),
journal_seen AS (
    SELECT d.league, d.game_id, count(*) AS hits
    FROM due d
    JOIN journal j
      ON j.league = d.league AND j.season = d.season
     AND j.done > d.kickoff AND j.done <= d.deadline
     AND contains(j.site, d.game_id)
    GROUP BY d.league, d.game_id
),
per_game AS (
    SELECT d.league, d.game_id,
           count_if(a.done IS NOT NULL) AS attempts,
           count_if(coalesce(
               CASE WHEN a.covered IS NOT NULL
                    THEN contains(a.covered, d.game_id)
                    ELSE a.cg >= d.ordinal END,
               false
           )) AS covered_hits,
           count_if(coalesce(
               CASE WHEN a.site IS NOT NULL
                    THEN contains(a.site, d.game_id)
                    ELSE a.cg >= d.ordinal END,
               false
           )) AS site_hits
    FROM due d
    LEFT JOIN att a
      ON a.league = d.league AND a.season = d.season
     AND a.done > d.kickoff AND a.done <= d.deadline
    GROUP BY d.league, d.game_id
),
graded AS (
    SELECT p.league,
           p.covered_hits > 0 AS ok,
           p.covered_hits = 0 AND p.attempts > 0
               AND p.site_hits = 0 AND coalesce(js.hits, 0) = 0 AS site_late
    FROM per_game p
    LEFT JOIN journal_seen js
      ON js.league = p.league AND js.game_id = p.game_id
)
SELECT league,
       count_if(NOT site_late) AS due,
       count_if(ok) AS ok,
       count_if(site_late) AS site_late
FROM graded
GROUP BY league
ORDER BY league"""
UNDERSTAT_COMPLETENESS_SQL = """WITH
m AS (
    SELECT league, season, batch_id, status
    FROM (
"""

UNDERSTAT_SENTINEL = "__sentinel__"
UNDERSTAT_FINAL_STATUSES = ("complete", "not_published", "upstream_pending")
UNDERSTAT_RUNNING_GRACE_H = 3      # маркер in_progress моложе — идущий прогон, не тревога


def _understat_short(league):
    return UNDERSTAT_SHORT.get(league, league)


def _understat_current_season(today):
    """Слаг текущего сезона Understat (старт в июле): 24.09.2026 -> '2627'."""
    start = today.year if today.month >= 7 else today.year - 1
    return f"{start % 100:02d}{(start + 1) % 100:02d}"


def _understat_season_closed(season, today):
    """#1431, правило каталога: сезон закрыт, если начался раньше текущего (старт в июле).
    Закрытые скоупы качаются раз в неделю (понедельник), сторож суток их не ждёт."""
    current = today.year if today.month >= 7 else today.year - 1
    return 2000 + int(season[:2]) < current


def _understat_rows(sql, width, sentinel=None):
    """Строки Trino без строки-заглушки; None — запрос упал / не разобран.

    Обёртка trino-ro.sh считает пустой ответ падением, поэтому к запросу добавляется
    строка-заглушка (sentinel — её значения после имени, типы как у колонок запроса)."""
    if sentinel is not None:
        sql = f"SELECT * FROM ({sql}) q UNION ALL SELECT '{UNDERSTAT_SENTINEL}', {sentinel}"
    rows = _trino_rows(sql)
    if rows is None:
        return None
    rows = [r for r in rows if r and r[0] != UNDERSTAT_SENTINEL]
    if any(len(r) != width for r in rows):
        return None
    return rows


def _understat_journal_missing():
    """True, если журнала неудачных попыток ещё нет (его создаёт раннер #1429 при запуске)."""
    rows = _trino_rows("SHOW TABLES FROM iceberg.ops LIKE 'understat_ingest_%'", timeout=60)
    if rows is None:
        return False
    return not any(r and r[0] == UNDERSTAT_FAILURES.rsplit(".", 1)[1] for r in rows)


def _understat_pct(ok, due):
    return float((Decimal(100 * ok) / Decimal(due)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))


def _understat_meets(ok, due):
    # Порог — по точному отношению, не по округлённому (95/96 = 99,0 % после округления, но < 99 %).
    return Decimal(100 * ok) >= UNDERSTAT_TARGET_PCT * due


def _understat_iso_add(iso, days):
    return (datetime.fromisoformat(iso).date() + timedelta(days=days)).isoformat()


def _understat_carry(state, day):
    """Серия, которую продолжают сутки day: серия последних записанных суток (last_day), если
    day идёт сразу за ними или все сутки между ними подтверждённо нейтральны — непрерывный
    отрезок neutral_since..neutral_through (due = 0), без ограничения длины. Неизмеренные сутки
    (Trino упал, сводка не бежала) — обрыв: нейтральность есть только у измеренного нуля."""
    last = state.get("last_day")
    if not last:
        return 0
    prev = (day - timedelta(days=1)).isoformat()
    if prev == last or (state.get("neutral_since") == _understat_iso_add(last, 1)
                        and state.get("neutral_through") == prev):
        return int(state.get("streak") or 0)
    return 0


def _understat_mark_neutral(state, d):
    """Сутки d без сроков продлевают непрерывный нейтральный отрезок или начинают новый.
    Идемпотентно: сутки не позже last_day или уже лежащие в отрезке ничего не меняют
    (повторный прогон за те же сутки не рвёт серию — Astra 1430 р3)."""
    if (state.get("last_day") and d <= state["last_day"]) or (
            state.get("neutral_since") and state["neutral_since"] <= d <= state["neutral_through"]):
        return
    if state.get("neutral_through") == _understat_iso_add(d, -1):
        state["neutral_through"] = d
    else:
        state["neutral_since"] = state["neutral_through"] = d


def _understat_daily_line(rows, state, day):
    """Строка «Understat за сутки» и запись суток (None — сутки не записываются).

    rows — строки DAILY_CRITERION_SQL (league, due, ok, site_late) без заглушки либо None (упал).
    Сутки без сроков (due = 0) нейтральны: в days не записываются и серию не рвут (допущение
    карты), но отмечаются отрезком neutral_since..neutral_through — иначе их не отличить от
    неизмеренных."""
    dd = f"{day.day:02d}.{day.month:02d}"
    carry = _understat_carry(state, day)
    last = state.get("last_day")
    tail = f"(серия {carry}" + (f" на {last[8:10]}.{last[5:7]})" if last and carry else ")")
    if rows is None:
        return f"• Understat за сутки {dd}: не посчитано ⚠️ {tail}", None
    due = sum(int(r[1]) for r in rows)
    ok = sum(int(r[2]) for r in rows)
    site_late = sum(int(r[3]) for r in rows)
    if due == 0:
        extra = f"; задержка сайта {site_late}" if site_late else ""
        return f"• Understat за сутки {dd}: матчей со сроком нет{extra} ⚠️ {tail}", {"neutral": True}
    meets = _understat_meets(ok, due)
    streak = carry + 1 if meets else 0
    late = {_understat_short(r[0]): int(r[1]) - int(r[2]) for r in rows if int(r[1]) > int(r[2])}
    rec = {"due": due, "ok": ok, "site_late": site_late, "pct": _understat_pct(ok, due),
           "streak": streak, "late": late}
    return _understat_format_day(rec, dd), rec


def _understat_format_day(rec, dd):
    meets = _understat_meets(rec["ok"], rec["due"])
    line = (f"• Understat за сутки {dd}: {rec['ok']}/{rec['due']} = {rec['pct']:.1f}% "
            f"(серия {rec['streak']}; задержка сайта {rec['site_late']}) "
            + ("✅" if meets else "‼️"))
    if rec.get("late"):
        line += " не в срок: " + ", ".join(f"{k} {v}" for k, v in sorted(rec["late"].items()))
    if rec["streak"] >= 3:
        line += " — веха 1: три дня со сроками подряд ≥ 99 %"
    return line


def _understat_save_state(state):
    tmp = None
    try:
        os.makedirs(os.path.dirname(UNDERSTAT_DAILY_STATE), exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(UNDERSTAT_DAILY_STATE),
                                   prefix=".understat_daily_criterion.")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, indent=1)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, UNDERSTAT_DAILY_STATE)
        return True
    except OSError:
        if tmp:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        return False


def understat_daily_criterion():
    """«Understat за сутки D: ok/due = pct % (серия N; задержка сайта k)» — измеритель #1429.

    D — вчерашние полные UTC-сутки срока (срок = начало матча + 26 ч). Сутки записываются
    в UNDERSTAT_DAILY_STATE один раз: повторный прогон печатает строку из state и в Trino
    не ходит. --dry-run state не пишет."""
    try:
        with open(UNDERSTAT_DAILY_STATE, encoding="utf-8") as fh:
            state = json.load(fh)
    except (OSError, ValueError):
        state = {}
    days = state.setdefault("days", {})
    day = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    d = day.isoformat()
    rec = days.get(d)
    if rec is not None:
        return [_understat_format_day(rec, f"{day.day:02d}.{day.month:02d}")]
    sql = UNDERSTAT_DAILY_SQL.format(day=d, failures=UNDERSTAT_FAILURES)
    rows = _understat_rows(sql, 4, sentinel="0, 0, 0")
    if rows is None and _understat_journal_missing():
        return ["• Understat за сутки: сторож не отработал ⚠️ (журнал ещё не создан)"]
    line, rec = _understat_daily_line(rows, state, day)
    if rec is not None and "--dry-run" not in sys.argv:
        if rec.get("neutral"):
            _understat_mark_neutral(state, d)
        else:
            days[d] = rec
            state.update({"streak": rec["streak"], "last_day": d})
            for k in sorted(days)[:-30]:
                days.pop(k, None)
        if not _understat_save_state(state):
            line += " (state не записан ⚠️)"
    return [line]


def _understat_completeness_lines(rows, season):
    """«understat 26/27: have/played = pct% …; свежесть Nч (LG)» по строкам
    (league, played, have, max_ingested_at, status последней строки манифеста сезона) —
    по одной на каждую лигу UNDERSTAT_SHORT."""
