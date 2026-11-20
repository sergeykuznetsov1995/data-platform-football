"""Watch section of the external morning report before #2343, for offline patch tests."""

import json
import re
import subprocess
from datetime import datetime, timedelta, timezone

UNDERSTAT_FAILURES = "iceberg.ops.understat_ingest_failures_v1"
UNDERSTAT_FINAL_WINDOW_H = 26      # итоговая попытка (complete/not_published/upstream_pending) за сутки + запас
UNDERSTAT_PENDING_RED_H = 48       # not_published/upstream_pending дольше — сверка с FotMob
UNDERSTAT_SHORT = {"ENG-Premier League": "ENG", "ESP-La Liga": "ESP", "GER-Bundesliga": "GER",
                   "ITA-Serie A": "ITA", "FRA-Ligue 1": "FRA", "RUS-Premier League": "RUS"}

# Context for applying the watch patch to this isolated section.
_QUARANTINE_TAIL = """
SELECT league, game_id, state, first_seen_at, last_checked_at, retry_until
FROM quarantine
ORDER BY league, game_id"""
# Сторож: скоупы с попыткой в манифесте за 7 суток (= окно ежедневного DAG); FotMob-сверка по id
# турниров (решение 9 плана: 47 ENG, 87 ESP, 54 GER, 55 ITA, 53 FRA, 63 RUS).
UNDERSTAT_WATCH_SQL = """WITH a AS (
    SELECT league, season, status,
           CAST(from_iso8601_timestamp(completed_at) AT TIME ZONE 'UTC' AS timestamp(6)) AS done
    FROM iceberg.ops.understat_ingest_manifest_v1
    WHERE contract_version = 'understat-bronze-v2'
),
now_utc AS (SELECT CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6)) AS t),
scopes AS (
    SELECT DISTINCT a.league, a.season FROM a CROSS JOIN now_utc n
    WHERE a.done > n.t - INTERVAL '7' DAY
),
agg AS (
    SELECT a.league, a.season,
           max(a.done) FILTER (WHERE a.status IN ('complete', 'not_published', 'upstream_pending')) AS last_final,
           max_by(a.status, a.done) FILTER (WHERE a.status IN ('complete', 'not_published', 'upstream_pending')) AS last_final_status,
           max_by(a.status, a.done) AS last_status,
           max(a.done) AS last_at,
           max(a.done) FILTER (WHERE a.status = 'complete') AS last_complete
    FROM a JOIN scopes s ON s.league = a.league AND s.season = a.season
    GROUP BY a.league, a.season
),
pend AS (
    SELECT a.league, a.season, min(a.done) AS pending_since
    FROM a JOIN agg g ON g.league = a.league AND g.season = a.season
    WHERE a.status IN ('not_published', 'upstream_pending')
      AND (g.last_complete IS NULL OR a.done > g.last_complete)
    GROUP BY a.league, a.season
),
j AS (
    SELECT f.league, f.season,
           max_by(f.status, f.completed_at) AS st,
           max(f.completed_at) AS jt,
           max_by(f.error_message, f.completed_at) AS msg
    FROM {failures} f CROSS JOIN now_utc n
    WHERE f.contract_version = 'understat-bronze-v2'
      AND CAST(from_iso8601_timestamp(f.completed_at) AT TIME ZONE 'UTC' AS timestamp(6)) > n.t - INTERVAL '48' HOUR
    GROUP BY f.league, f.season
),
lg (league, cid) AS (
    VALUES ('ENG-Premier League', 47), ('ESP-La Liga', 87), ('GER-Bundesliga', 54),
           ('ITA-Serie A', 55), ('FRA-Ligue 1', 53), ('RUS-Premier League', 63)
),
fm AS (
    SELECT lg.league, m.source_season_key, count_if(m.finished AND NOT m.cancelled) AS finished
    FROM iceberg.bronze.fotmob_matches_current m
    JOIN lg ON lg.cid = m.competition_id
    GROUP BY lg.league, m.source_season_key
)
SELECT g.league, g.season,
       coalesce(CAST(g.last_final AS varchar), ''), coalesce(g.last_final_status, ''),
       g.last_status, CAST(g.last_at AS varchar),
       coalesce(CAST(p.pending_since AS varchar), ''),
       coalesce(j.st, ''), coalesce(substr(j.jt, 1, 16), ''),
       coalesce(replace(replace(replace(substr(j.msg, 1, 160), '"', ''''), chr(10), ' '), chr(13), ' '), ''),
       CAST(coalesce(fm.finished, 0) AS varchar)
FROM agg g
LEFT JOIN pend p ON p.league = g.league AND p.season = g.season
LEFT JOIN j ON j.league = g.league AND j.season = g.season
LEFT JOIN fm ON fm.league = g.league
    AND fm.source_season_key = '20' || substr(g.season, 1, 2) || '/20' || substr(g.season, 3, 2)
UNION ALL
SELECT '__sentinel__', '', '', '', '', '', '', '', '', '', '0'
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


def _understat_ts(value):
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(value.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _understat_watch_lines(rows, now):
    """Сторож итоговой попытки по строкам UNDERSTAT_WATCH_SQL (без заглушки)."""
    if rows is None:
        return ["• итоговые попытки Understat: сторож не отработал ⚠️ (trino)"]
    rows = [r for r in rows if not _understat_season_closed(r[1], now.date())]
    if not rows:
        return ["• итоговые попытки Understat: скоупов с попыткой за 7 суток нет ‼️"]
    alarms = []
    for (league, season, last_final, last_final_status, last_status, last_at, pending_since,
         j_status, j_at, j_msg, fotmob_finished) in rows:
        name = f"{_understat_short(league)} {season}"
        if j_status:
            jt = _understat_ts(j_at)
            j_why = f"{j_status} {jt.strftime('%d.%m %H:%M') if jt else j_at} — " \
                    f"{j_msg or 'причина не записана'}"
        else:
            j_why = None
        lt = _understat_ts(last_at)
        running = (last_status == "in_progress" and lt is not None
                   and now - lt < timedelta(hours=UNDERSTAT_RUNNING_GRACE_H))
        if last_status not in UNDERSTAT_FINAL_STATUSES and not running:
            # Последняя строка манифеста — сбой после маркера записи или брошенный маркер:
            # ограда потребителей прячет скоуп, даже если предыдущая итоговая попытка моложе 26 ч.
            when = lt.strftime("%d.%m %H:%M") if lt else last_at
            alarms.append(f"• ‼️ {name}: последняя попытка {last_status} {when} — скоуп скрыт "
                          f"оградой (причина: {j_why or 'неизвестна'})")
            continue
        final_ts = _understat_ts(last_final) if last_final else None
        if final_ts is None or now - final_ts > timedelta(hours=UNDERSTAT_FINAL_WINDOW_H):
            if j_why:
                why = j_why
            else:
                lt = _understat_ts(last_at)
                when = lt.strftime("%d.%m %H:%M") if lt else last_at
                why = f"{last_status} {when} — причина неизвестна"
            alarms.append(f"• ‼️ {name}: нет итоговой попытки за сутки (последняя: {why})")
            continue
        since = _understat_ts(pending_since) if pending_since else None
        if (last_final_status in ("not_published", "upstream_pending") and since is not None
                and now - since > timedelta(hours=UNDERSTAT_PENDING_RED_H)
                and int(fotmob_finished or 0) > 0):
            days_ = (now - since).days
            alarms.append(f"• ‼️ {name}: сайт не публикует {days_} дн. ({last_final_status}), "
                          f"FotMob видит {int(fotmob_finished)} сыгранных")
    if alarms:
        return alarms
    return [f"• итоговые попытки: {len(rows)}/{len(rows)} скоупов ✅"]


def understat_watch():
    rows = _understat_rows(UNDERSTAT_WATCH_SQL.format(failures=UNDERSTAT_FAILURES), 11)
    if rows is None and _understat_journal_missing():
        return ["• итоговые попытки Understat: сторож не отработал ⚠️ (журнал ещё не создан)"]
    return _understat_watch_lines(rows, datetime.now(timezone.utc))


def _understat_quarantine_date(value):
    """Aware ISO time -> Moscow DD.MM; naive/invalid values stay unknown."""
