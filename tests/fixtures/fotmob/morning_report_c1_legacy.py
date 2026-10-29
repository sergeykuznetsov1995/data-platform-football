"""Frozen host C1 fragment, 2026-10-28; oracle for #1896. No host I/O."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

C1_GAP_MIN_RUNS = 2
C1_GAP_MIN_SPREAD_S = 2 * 3600
C1_LATE_GRACE_S = 24 * 3600   # #1533: после первого FT источника у нас 24 ч
C1_SQL_TIMEOUT_S = 900        # #1533: запрос C1 идёт ~280 с (замер 25.09), 240 с не хватает
C1_REFRESH_DAYS = 7           # #1533: сутки с «ждут срока» переоцениваются не дольше недели


def _c1_classify(rows, now=None):
    """Правило C1 (#1450, #1533): (due, ok, gap, late, pending) по строкам «матч со сроком в D».

    Строка: (match_id, deadline, first_ingested, na_runs, na_spread_s, success_count,
    first_ft, saw_unfinished). deadline = начало + 26 ч; na_runs — разных ранов с
    not_available/source_data_not_found, na_spread_s — разброс их completed_at;
    first_ft — первый снимок расписания с finished=true; saw_unfinished — снимков
    finished=false в окне [начало+3 ч, начало+26 ч).
    * Подтверждённая дыра источника (#1450: na_runs >= 2, разброс >= 2 ч, ни одного
      success) в due не входит и считается в gap.
    * #1533: источник показал FT позже, а в окне мы смотрели и видели «не сыгран» —
      срок = первый FT + 24 ч; матч остаётся в due и считается в late. Если такой срок
      ещё не истёк (now) и деталей нет — матч временно промах и считается в pending:
      сутки с pending переоценит следующий прогон. Так же pending — матч без деталей, по
      которому мы видели «не сыгран», а FT источник ещё не показал, пока с исходного срока
      прошло меньше C1_REFRESH_DAYS (Astra р2 п.1); дальше — окончательный промах.
    Строка с пустым match_id — заглушка «матчей нет», пропускается."""
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    due = ok = gap = late = pending = 0
    for match_id, deadline, first, na_runs, na_spread, success, first_ft, saw_unfinished in rows:
        if match_id in (None, ""):
            continue
        if (int(na_runs or 0) >= C1_GAP_MIN_RUNS
                and int(na_spread or 0) >= C1_GAP_MIN_SPREAD_S
                and int(success or 0) == 0):
            gap += 1
            continue
        due += 1
        deadline = (deadline if isinstance(deadline, datetime)
                    else datetime.fromisoformat(deadline))
        if first_ft not in (None, "") and int(saw_unfinished or 0) > 0:
            first_ft = (first_ft if isinstance(first_ft, datetime)
                        else datetime.fromisoformat(first_ft))
            extended = first_ft + timedelta(seconds=C1_LATE_GRACE_S)
            if extended > deadline:
                deadline = extended
                late += 1
        if first not in (None, ""):
            first = first if isinstance(first, datetime) else datetime.fromisoformat(first)
            if first <= deadline:
                ok += 1
        elif deadline > now or (
                first_ft in (None, "") and int(saw_unfinished or 0) > 0
                and deadline + timedelta(days=C1_REFRESH_DAYS) > now):
            pending += 1
    return due, ok, gap, late, pending


def _c1_sql(d):
    """Запрос C1 за сутки d (ISO-дата): по строке на матч со сроком (начало + 26 ч) в d.

    #1563: сирота (сезон у источника не is_latest при наличии текущего, матч не finished) — вне due.
    """
    return (
        "WITH inc AS (SELECT DISTINCT CAST(competition_id AS varchar) AS cid "
        "FROM iceberg.bronze.fotmob_competition_scope_observations_current "
        "WHERE decision='included'), "
        "p AS (SELECT match_id, min(_ingested_at) AS fi "
        "FROM iceberg.bronze.fotmob_match_payloads GROUP BY match_id), "
        "st AS (SELECT DISTINCT s.competition_id AS cid, s.source_season_key AS sk "
        "FROM iceberg.bronze.fotmob_competition_seasons_current s WHERE NOT s.is_latest "
        "AND s.competition_id IN (SELECT competition_id FROM "
        "iceberg.bronze.fotmob_competition_seasons_current WHERE is_latest)), "
        "m AS (SELECT m.match_id, CAST(from_iso8601_timestamp(m.utc_time) "
        "AT TIME ZONE 'UTC' AS timestamp(6)) + interval '26' hour AS deadline "
        "FROM iceberg.bronze.fotmob_matches_current m "
        "JOIN inc i ON i.cid=CAST(m.competition_id AS varchar) "
        "LEFT JOIN st ON st.cid=CAST(m.competition_id AS varchar) "
        "AND st.sk=m.source_season_key "
        "WHERE m.cancelled=false AND (st.cid IS NULL OR m.finished) "
        f"AND m.utc_time >= to_iso8601(timestamp '{d} 00:00:00' - interval '3' day)) "
        ", md AS (SELECT CAST(m.match_id AS varchar) AS mid, m.deadline, p.fi "
        "FROM m LEFT JOIN p ON p.match_id=m.match_id "
        f"WHERE m.deadline >= timestamp '{d} 00:00:00' "
        f"AND m.deadline < timestamp '{d} 00:00:00' + interval '1' day), "
        "h AS (SELECT entity_id AS mid, count(DISTINCT CASE WHEN status='not_available' "
        "AND error_code='source_data_not_found' THEN run_id END) AS na_runs, "
        "date_diff('second', min(CASE WHEN status='not_available' "
        "AND error_code='source_data_not_found' THEN completed_at END), "
        "max(CASE WHEN status='not_available' "
        "AND error_code='source_data_not_found' THEN completed_at END)) AS na_spread, "
        "count_if(status='success') AS succ "
        "FROM iceberg.bronze.fotmob_ingest_manifest WHERE target_type='match' "
        "AND entity_id IN (SELECT mid FROM md) GROUP BY entity_id), "
        # #1533: снимки расписания; окно чтения по _ingested_at (4 сут) — без него Trino
        # читает всю append-таблицу. deadline - 23 ч = начало + 3 ч.
        "snap AS (SELECT CAST(s.match_id AS varchar) AS mid, "
        "min(CASE WHEN s.finished THEN s._observed_at END) AS first_ft, "
        "count_if(NOT s.finished AND s._observed_at >= md.deadline - interval '23' hour "
        "AND s._observed_at < md.deadline) AS saw_unfinished "
        "FROM iceberg.bronze.fotmob_matches s "
        "JOIN md ON CAST(s.match_id AS varchar)=md.mid "
        f"WHERE s._ingested_at >= timestamp '{d} 00:00:00' - interval '4' day "
        f"AND s.utc_time >= to_iso8601(timestamp '{d} 00:00:00' - interval '3' day) "
        "GROUP BY 1), "
        "r AS (SELECT md.mid, md.deadline, md.fi, coalesce(h.na_runs, 0) AS na_runs, "
        "coalesce(h.na_spread, 0) AS na_spread, coalesce(h.succ, 0) AS succ, "
        "snap.first_ft, coalesce(snap.saw_unfinished, 0) AS saw_unfinished "
        "FROM md LEFT JOIN h ON h.mid=md.mid LEFT JOIN snap ON snap.mid=md.mid) "
        "SELECT r.mid, r.deadline, r.fi, r.na_runs, r.na_spread, r.succ, "
        "r.first_ft, r.saw_unfinished "
        "FROM (VALUES 1) AS one(x) LEFT JOIN r ON true")


def _c1_measure(d, now):
    """Запись суток d для state или None (Trino недоступен / ответ не разобран)."""
    rows = _trino_rows(_c1_sql(d), timeout=C1_SQL_TIMEOUT_S)
    try:
        due, ok, gap, late, pending = _c1_classify(rows, now)
    except (TypeError, IndexError, ValueError):
        return None
    # Округление — как в C1/Trino (десятичное, половина вверх), а не round()
    # Python (половина к чётному): 397/400 = 99,25 % даёт 99,3 %, не 99,2 %.
    pct = (float((Decimal(100 * ok) / Decimal(due)).quantize(Decimal("0.1"),
                                                              rounding=ROUND_HALF_UP))
           if due else 0.0)
    rec = {"due": due, "ok": ok, "pct": pct, "gap": gap, "late": late}
    if pending:
        rec["pending"] = pending
    return rec




def _c1_streak(days, k):
    """Серия суток k — от НЕПОСРЕДСТВЕННО предыдущих суток (не от всего days)."""
    if (days[k].get("pct") or 0) < 99.0:
        return 0
    prev = days.get((datetime.fromisoformat(k) - timedelta(days=1)).date().isoformat())
    carry = prev.get("streak") if prev and (prev.get("pct") or 0) >= 99.0 else 0
    return int(carry or 0) + 1


def fotmob_daily_criterion():
    """«FotMob за сутки D: ok/due = pct % (серия N)» — критерий приёмки прода (#1280).

    Метрика ЗАМОРОЖЕНА (C1, комментарий 5667128294 в #1277, раздел «Замороженная
    метрика приёмки»; решение 2 гриля 14.09 — определение не менять без слова
    владельца):
      * ключ суток — день истечения срока: deadline = начало матча (utc_time) + 26 ч;
      * знаменатель due — матчи included-турниров текущего каталога с cancelled=false,
        чей deadline попадает в сутки D;
      * числитель ok — те из них, у кого в append-таблице fotmob_match_payloads есть
        хотя бы одна строка с min(_ingested_at) <= deadline (деталь взята в срок);
      * pct = round(100 * ok / due, 1), считается в Python.
    Оцениваются ВЧЕРАШНИЕ полные UTC-сутки D (сторож бежит в 05:00 UTC).
    Серия — число смежных суток назад от D с pct >= 99.0; три подряд = веха 1.
    Неизмеренные (пропущенные) сутки серию рвут: дырка в days — обрыв.

    Изменено 24.09 по решению владельца (#1450): подтверждённая дыра источника —
    матч, по которому FotMob ответил «данных нет» (not_available/source_data_not_found)
    минимум в двух разных ранах с разбросом их completed_at >= 2 ч и без единой строки
    success в fotmob_ingest_manifest, — из due вычитается и показывается отдельным
    числом «дыр источника G». Матч с одной такой попыткой — промах (до второй попытки).
    Правило — _c1_classify(); SQL отдаёт по матчу агрегаты. Сутки, записанные до 24.09,
    не пересчитываются (поля gap в них нет).

    Правило вычета задержки источника (#1281, замер 15–17.09.2026, 49 сыгранных матчей
    из 32 турниров, 200 проб): ВЫЧЕТ НЕ НУЖЕН — карточка matchFacts готова у 100 %
    сыгранных уже через 1 ч после окончания (p95 <= 1 ч при сроке 26 ч). due считается
    по всем матчам included-турниров; если понадобится вычет — условие подзапроса m ниже,
    менять только вместе с заморозкой C1.

    Сутки D записываются в DAILY_CRITERION_STATE ОДИН раз: повторный прогон в те же
    сутки печатает строку из state и в Trino не ходит и на диск не пишет (приёмка
    R2-18: у дня нет подвижного baseline по построению — deadline и min(_ingested_at)
    от времени прогона не зависят). --dry-run состояние не пишет.
    """
    try:
        with open(DAILY_CRITERION_STATE, encoding="utf-8") as fh:
            state = json.load(fh)
    except (OSError, ValueError):
        state = {}
    days = state.setdefault("days", {})
    day = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    d = day.isoformat()
    dd = f"{day.day:02d}.{day.month:02d}"

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    lines = []
    # #1533: сутки, где срок от позднего FT источника на момент оценки ещё не истёк
    # («ждут срока»), переоцениваются следующими прогонами; ключ суток не меняется,
    # серия пересчитывается от первых изменённых суток.
    refreshed = None
    oldest = (day - timedelta(days=C1_REFRESH_DAYS)).isoformat()
    for k in sorted(days):
        if oldest <= k < d and days[k].get("pending"):
            got = _c1_measure(k, now)
            if got is not None:
                days[k] = got
                refreshed = refreshed or k
                kd = datetime.fromisoformat(k)
                lines.append(f"• FotMob за сутки {kd.day:02d}.{kd.month:02d} (пересчёт): "
                             f"{got['ok']}/{got['due']} = {got['pct']:.1f}%"
                             f" (поздних у источника {got['late']}"
                             + (f"; ждут срока {got['pending']}" if got.get("pending") else "")
                             + ")")
    if refreshed:
        # и вчерашние сутки, если уже есть в days (повторный прогон, Astra р3 п.1)
        for k in sorted(days):
            if refreshed <= k <= d:
                days[k]["streak"] = _c1_streak(days, k)

    rec, fresh = days.get(d), False
    if rec is None or rec.get("pending"):
        got = _c1_measure(d, now)
        if got is None or got["due"] == 0:
            # Серия — из последних измеренных суток: неизмеренные сутки её не продлевают,
            # но и не стирают задним числом (обрыв увидит завтрашний прогон по дырке в days).
            # Сделанный выше пересчёт прошлых суток сохраняется и здесь (Astra р2 п.2).
            if refreshed and "--dry-run" not in sys.argv and not _c1_save(state):
                lines.append("• FotMob: пересчёт прошлых суток не записан ⚠️")
            done = [k for k in days if k < d]
            last = max(done) if done else None
            tail = f"(серия {int(days[last].get('streak') or 0) if last else 0}"
            tail += f" на {last[8:10]}.{last[5:7]})" if last else ")"
            what = ("не посчитано ⚠️" if got is None else
                    "матчей со сроком нет"
                    + (f" (дыр источника {got['gap']})" if got["gap"] else "") + " ⚠️")
            return lines + [f"• FotMob за сутки {dd}: {what} {tail}"]
        rec, fresh = got, True
        days[d] = rec

    # Длина серии хранится в самих сутках: она считается от НЕПОСРЕДСТВЕННО
    # предыдущих суток, поэтому не зависит от того, сколько истории осталось в
    # days после очистки (обход по всему days давал «серия 31» на первом прогоне
    # и «серия 30» на повторном — Astra р1 п.1).
    streak = _c1_streak(days, d)
    rec["streak"] = streak

    flag = "✅" if rec["pct"] >= 99.0 else "‼️"
    line = (f"• FotMob за сутки {dd}: {rec['ok']}/{rec['due']} = {rec['pct']:.1f}% "
            f"(серия {streak}"
            + (f"; дыр источника {rec['gap']}" if "gap" in rec else "")
            + (f"; поздних у источника {rec['late']}" if "late" in rec else "")
            + (f"; ждут срока {rec['pending']}" if rec.get("pending") else "")
            + f") {flag}")
    if streak >= 3:
        line += " — веха 1: три дня подряд ≥ 99 %"

    if (fresh or refreshed) and "--dry-run" not in sys.argv and not _c1_save(state):
        line += " (state не записан ⚠️)"
    return lines + [line]
