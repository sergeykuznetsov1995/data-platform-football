#!/usr/bin/env python3
"""Сторож простоя сбора ESPN (#1496; реестр ревью 24.09 R-01, C1-F4, C8-F9).

Сбор ESPN стоит с 13.08: при сбое 15.08 сторож старого контура поставил на паузу все 7 DAG
(`pause_all`/`fail_closed`), и 42 дня этого не видел никто — ESPN не было ни в crontab, ни в
утренней сводке. Здесь каждые 15 минут два правила, у каждого свой эпизод:
  paused — метабаза контура отвечает, и любой DAG из EXPECTED_DAGS на паузе или отсутствует в
           таблице `dag`; метабаза не отвечает (`docker exec` упал / `SELECT 1` не вернул 1) —
           тревога того же правила «metadb недоступен» (тот же ключ: серия не дублируется);
  stall  — в bronze (BRONZE_TABLES) нет ни одного матча с TS_COL за последние STALL_H часов.
           Меряем по матчам, а не по возрасту прогона (монитор старого контура мерил возраст
           прогона — R-02), и по `_source_fetched_at` (у старых таблиц `_ingested_at` врал —
           = execution_date прогона). Trino недоступен — правило пропускается, эпизод не
           трогаем («не знаю» ≠ «простоя нет»).
  red:<slug> — (#1505) турнир красный в RED_WAVES последних волнах `dag_espn_current` подряд
           (журнал волн WAVE_LOG, строка волны slug = '(wave)'): тревога один раз, «продолжается»
           раз в сутки, без issue; отбой — когда последняя волна с этим турниром зелёная или его
           нет ни в одной из RED_WAVES последних волн. Однократный сбой не тревожит. Журнала ещё
           нет (до #1507) или Trino недоступен — правило молча пропускается.
  downgrade — (#1506) за последние DOWNGRADE_H часов в журнале перепроверок RECHECK_LOG есть
           хотя бы один `downgrade_rejected` (ESPN прислал беднее, чем лежит, — оставили старое):
           тревога с лигами, «продолжается» раз в сутки, без issue; отбой — когда за DOWNGRADE_H
           часов случаев нет. Журнала ещё нет (до #1507) или Trino недоступен — молчит.
Эпизод (механика — /root/watchdog/transfermarkt_stall_watch.py, #1389): новое правило —
тревога; то же — молчим, «⏳ продолжается N ч» не чаще раза в 24 ч от последнего сообщения;
через ISSUE_AFTER_H от первой тревоги — issue (labels ISSUE_LABELS, заголовок с ключом
правила; открытая issue с тем же заголовком не дублируется) + карточка на доску в Blocked
(и для найденной открытой issue; не встала — повтор каждым тиком, без новой issue), номер и
успех карточки в state (`issue`, `blocked`); условие снято — «✅ отбой» одной строкой и эпизод
забыт. Telegram — напрямую через API бота с проверкой ответа, неподтверждённое ждёт в
state.pending.

Контракт на #1504 (новый контур): реакция на сбой — красный турнир + тревога этого сторожа;
никаких `pause_all` / `on_failure → pause` — пауза контура и есть та тишина, которую сторож
ловит правилом `paused`.

Cron: */15 * * * *. Ручной прогон: --dry-run --state <ОТДЕЛЬНЫЙ файл> [--now ISO UTC]
(state пишется и в --dry-run, поэтому --dry-run без --state запрещён; Telegram и issue в
--dry-run не трогаются — печатается, что было бы).
"""
import argparse
import csv
import fcntl
import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

# #1507: новый контур — проект espn-live (deploy/espn/airflow.compose.yaml), его единственный
# DAG актуалки и таблица матчей нового bronze (#1503). Старый контур espn-airflow (7 DAG на
# паузе) сторож больше не спрашивает — он точка отката до задачи 21.
METADB = "espn-live-airflow-metadb-1"
EXPECTED_DAGS = ("dag_espn_current",)
BRONZE_TABLES = ("espn_match",)
TS_COL = "_source_fetched_at"   # время ответа ESPN; _ingested_at — метка последней пачки
# #1505: журнал волн нового контура (scrapers/espn/wave_log.py) и порог «красный N волн подряд».
WAVE_LOG = "iceberg.ops.espn_wave_tournament_v1"
WAVE_ROW = "(wave)"
RED_WAVES = 3
# #1506: журнал перепроверок (scrapers/espn/recheck.py) и окно правила downgrade.
RECHECK_LOG = "iceberg.ops.espn_recheck_v1"
DOWNGRADE_H = 24
SENTINEL = "__sentinel__"
STALL_H = 36
ISSUE_AFTER_H = 24
STATE_DEFAULT = Path("/root/watchdog/state/espn_stall_state.json")
TRINO = "/root/.claude/bin/trino-ro.sh"
PENDING_MAX = 10
GH_REPO = "sergeykuznetsov1995/data-platform-football"
ISSUE_LABELS = "source:espn,area:bronze,type:bug"
BOARD_PROJECT = "PVT_kwHOA8FU3c4BXy0F"            # skill project-board
BOARD_STATUS_FIELD = "PVTSSF_lAHOA8FU3c4BXy0FzhS9P6c"
BOARD_STATUS_BLOCKED = "aedb5014"
TG_ENV = Path("/root/.claude/telegram.env")
RULE_TITLE = {
    "paused": "DAG ESPN на паузе или metadb недоступен",
    "stall": f"нет новых матчей в bronze {STALL_H} ч",
    "downgrade": f"downgrade_rejected за {DOWNGRADE_H} ч",
}
RED_PREFIX = "red:"


def rule_title(rule):
    if rule.startswith(RED_PREFIX):
        return f"турнир {rule[len(RED_PREFIX):]} красный {RED_WAVES} волны подряд"
    return RULE_TITLE[rule]


def psql(sql, timeout=40):
    """(ok, stdout) в формате `psql -At`; ok=False — метабаза недоступна или запрос упал."""
    try:
        r = subprocess.run(["docker", "exec", METADB, "psql", "-U", "airflow",
                            "-d", "airflow", "-tAc", sql],
                           capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError):
        return False, ""
    return r.returncode == 0, r.stdout


def trino(sql, timeout=240):
    """stdout trino-ro.sh или None — Trino недоступен/запрос упал."""
    try:
        r = subprocess.run([TRINO, sql], capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError):
        return None
    return r.stdout if r.returncode == 0 else None


def parse_ts(s):
    """ISO со смещением или naive (naive = UTC: так пишет bronze) -> aware UTC."""
    dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def fmt_ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def sql_ts(dt):
    """Литерал для naive-колонок Trino (UTC) — без current_timestamp с часовым поясом."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def read_dags():
    """[(dag_id, is_paused)] ожидаемых DAG или None — метабаза не отвечает."""
    ok, out = psql("SELECT 1")
    if not ok or out.strip() != "1":
        return None
    wanted = ", ".join(f"'{d}'" for d in EXPECTED_DAGS)
    ok, out = psql(f"SELECT dag_id, is_paused FROM dag WHERE dag_id IN ({wanted}) ORDER BY 1")
    if not ok:
        return None
    return [tuple(ln.strip().split("|")) for ln in out.splitlines() if ln.count("|") == 1]


def bronze_sql(now):
    union = " UNION ALL ".join(f"SELECT event_id, {TS_COL} AS ts FROM iceberg.bronze.{t}"
                               for t in BRONZE_TABLES)
    lo, hi = sql_ts(now - timedelta(hours=STALL_H)), sql_ts(now)
    # Агрегат без GROUP BY отдаёт ровно одну строку — пустого ответа (= падение для
    # trino-ro.sh) не бывает.
    return (f"SELECT cast(count(DISTINCT CASE WHEN ts > timestamp '{lo}' THEN event_id END) "
            f"AS varchar), coalesce(cast(max(ts) AS varchar), 'нет') FROM ({union}) u "
            f"WHERE ts <= timestamp '{hi}'")


def read_bronze(now):
    """(матчей с загрузкой за STALL_H ч, последняя загрузка строкой | None) или None."""
    out = trino(bronze_sql(now))
    for line in (out or "").splitlines():
        cells = [c.strip('"') for c in line.strip().split('","')]
        if len(cells) == 2 and cells[0].isdigit():
            return int(cells[0]), (None if cells[1] == "нет" else cells[1])
    return None


def red_waves_sql():
    """Турниры RED_WAVES последних волн. Строка-заглушка: пустой ответ trino-ro.sh = падение."""
    return (f"SELECT w.run_id, cast(w.started AS varchar), t.slug, t.state, "
            f"coalesce(t.first_error, '') FROM (SELECT run_id, max(wave_started_at) AS started "
            f"FROM {WAVE_LOG} WHERE slug = '{WAVE_ROW}' GROUP BY run_id ORDER BY started DESC "
            f"LIMIT {RED_WAVES}) w JOIN {WAVE_LOG} t ON t.run_id = w.run_id "
            f"WHERE t.slug <> '{WAVE_ROW}' "
            f"UNION ALL SELECT '{SENTINEL}', '', '', '', ''")


def read_red_waves():
    """[(run_id, старт, {slug: (state, first_error)})] последних волн, новые первыми; None —
    журнала нет или Trino недоступен (правило пропускается)."""
    out = trino(red_waves_sql())
    if out is None:
        return None
    waves, seen = {}, False
    for cells in csv.reader((out or "").splitlines()):
        if len(cells) != 5:
            continue
        if cells[0] == SENTINEL:
            seen = True
            continue
        run_id, started, slug, state, error = cells
        # Строка журнала — турнир-сезон: любой красный сезон делает турнир красным,
        # ошибка — наименьшая по тексту (детерминированно при любом порядке строк).
        slugs = waves.setdefault((started, run_id), {})
        prev = slugs.get(slug)
        if prev is None or (state == "red") > (prev[0] == "red") or (
                state == prev[0] == "red" and error < prev[1]):
            slugs[slug] = (state, error)
    if not seen:
        return None
    return [(run_id, started, slugs) for (started, run_id), slugs in sorted(waves.items(), reverse=True)]


def evaluate_red(waves, known):
    """{"red:<slug>": текст | None (отбой)} по волнам read_red_waves; known — правила red:*
    из state (их отбой считается, даже если турнира уже нет в волнах)."""
    alerts = {}
    slugs = {slug for _, _, tournaments in waves for slug in tournaments}
    slugs |= {rule[len(RED_PREFIX):] for rule in known if rule.startswith(RED_PREFIX)}
    for slug in sorted(slugs):
        states = [tournaments.get(slug) for _, _, tournaments in waves]
        if len(waves) >= RED_WAVES and all(s and s[0] == "red" for s in states[:RED_WAVES]):
            _, started, _ = waves[0]
            alerts[RED_PREFIX + slug] = (f"🔴 ESPN: турнир {slug} красный {RED_WAVES} волны подряд "
                                         f"(последняя {started[:16]} UTC): {states[0][1] or '—'}. #1505")
            continue
        latest = next((s for s in states if s), None)
        if latest is None or latest[0] != "red":
            alerts[RED_PREFIX + slug] = None
    return alerts


def downgrade_sql(now):
    """Лиги с downgrade_rejected за DOWNGRADE_H ч. Строка-заглушка: пустой ответ = падение."""
    lo, hi = sql_ts(now - timedelta(hours=DOWNGRADE_H)), sql_ts(now)
    return (f"SELECT slug, cast(count(*) AS varchar) FROM {RECHECK_LOG} "
            f"WHERE outcome = 'downgrade_rejected' AND checked_at > timestamp '{lo}' "
            f"AND checked_at <= timestamp '{hi}' GROUP BY slug "
            f"UNION ALL SELECT '{SENTINEL}', ''")


def read_downgrades(now):
    """{slug: случаев} за DOWNGRADE_H ч; None — журнала нет или Trino недоступен."""
    out = trino(downgrade_sql(now))
    if out is None:
        return None
    found, seen = {}, False
    for cells in csv.reader(out.splitlines()):
        if len(cells) != 2:
            continue
        if cells[0] == SENTINEL:
            seen = True
        elif cells[1].isdigit():
            found[cells[0]] = int(cells[1])
    return found if seen else None


def evaluate_downgrade(found):
    """{"downgrade": текст | None (отбой)} по read_downgrades."""
    if not found:
        return {"downgrade": None}
    leagues = ", ".join(f"{slug} {n}" for slug, n in sorted(found.items()))
    return {"downgrade": (f"🟠 ESPN: downgrade_rejected за {DOWNGRADE_H} ч — "
                          f"{sum(found.values())} (ESPN прислал беднее, оставлено старое): "
                          f"{leagues}. #1506")}


def evaluate(now, dag_rows, bronze):
    """{правило: текст тревоги | None (условие снято)}; правила без данных в ответ не входят."""
    alerts = {}
    if dag_rows is None:
        alerts["paused"] = (f"⏸ ESPN: metadb `{METADB}` недоступен — паузу DAG ESPN проверить "
                            "нечем. #1496")
    else:
        seen = dict(dag_rows)
        bad = [f"{d} ({'нет в metadb' if d not in seen else 'пауза'})"
               for d in EXPECTED_DAGS if seen.get(d) != "f"]
        alerts["paused"] = (f"⏸ ESPN: DAG на паузе/нет: {', '.join(bad)} — сбор ESPN не идёт. "
                            "#1496") if bad else None
    if bronze is not None:
        fresh, last = bronze
        if fresh:
            alerts["stall"] = None
        elif last:
            hours = (now - parse_ts(last)).total_seconds() / 3600
            alerts["stall"] = (f"🔴 ESPN: нет новых матчей в bronze {hours:.0f} ч (последняя "
                               f"загрузка {last[:16]} UTC, {TS_COL}). #1496")
        else:
            alerts["stall"] = f"🔴 ESPN: в bronze нет ни одной загрузки ({TS_COL}). #1496"
    return alerts


def tg_send(text):
    """Тем же ботом и чатом, что tg-send.sh, но с проверкой ответа API."""
    try:
        env = dict(ln.split("=", 1) for ln in TG_ENV.read_text().splitlines()
                   if "=" in ln and not ln.lstrip().startswith("#"))
        token = env["TELEGRAM_BOT_TOKEN"].strip().strip("'\"")
        chat = env["TELEGRAM_CHAT_ID"].strip().strip("'\"")
        r = subprocess.run(["curl", "-s", "--max-time", "10",
                            f"https://api.telegram.org/bot{token}/sendMessage",
                            "-d", f"chat_id={chat}",
                            "--data-urlencode", f"text=[{os.uname().nodename}] {text}"],
                           capture_output=True, text=True, timeout=20)
        return r.returncode == 0 and json.loads(r.stdout).get("ok") is True
    except (OSError, ValueError, KeyError, subprocess.TimeoutExpired):
        return False


def gh(args, timeout=60):
    try:
        r = subprocess.run(["gh", *args], capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError):
        return None
    return r.stdout.strip() if r.returncode == 0 else None


def issue_title(rule, ep):
    return f"ESPN: сторож [{rule}] — {RULE_TITLE[rule]}, с {parse_ts(ep['first']):%d.%m.%Y}"


def find_open_issue(title):
    out = gh(["issue", "list", "--repo", GH_REPO, "--state", "open", "--search",
              f"\"{title}\" in:title", "--json", "number,title", "--limit", "20"])
    try:
        for item in json.loads(out or "[]"):
            if item.get("title") == title:
                return int(item["number"])
    except (ValueError, TypeError, KeyError):
        pass
    return None


def board_blocked(number):
    """Карточка на доску со Status=Blocked (skill project-board). False — не вышло."""
    node = gh(["api", "graphql", "-f", "query=query($n:Int!){repository(owner:\"sergeykuznetsov1995\","
               "name:\"data-platform-football\"){issue(number:$n){id}}}", "-F", f"n={number}",
               "--jq", ".data.repository.issue.id"])
    if not node:
        return False
    item = gh(["api", "graphql", "-f", "query=mutation($p:ID!,$c:ID!){addProjectV2ItemById("
               "input:{projectId:$p,contentId:$c}){item{id}}}", "-f", f"p={BOARD_PROJECT}",
               "-f", f"c={node}", "--jq", ".data.addProjectV2ItemById.item.id"])
    if not item:
        return False
    done = gh(["api", "graphql", "-f", "query=mutation($p:ID!,$i:ID!,$f:ID!,$o:String!){"
               "updateProjectV2ItemFieldValue(input:{projectId:$p,itemId:$i,fieldId:$f,"
               "value:{singleSelectOptionId:$o}}){projectV2Item{id}}}", "-f", f"p={BOARD_PROJECT}",
               "-f", f"i={item}", "-f", f"f={BOARD_STATUS_FIELD}", "-f", f"o={BOARD_STATUS_BLOCKED}"])
    return done is not None


def escalate(rule, ep, text, now, dry_run):
    """Одна issue на эпизод (номер в ep["issue"]) + карточка Blocked (успех — ep["blocked"]).
    Строка второй тревоги — только когда issue впервые появилась в эпизоде; карточка не встала —
    повтор следующим тиком без новой issue. None — issue завести не вышло (повтор тиком позже)."""
    title = issue_title(rule, ep)
    if dry_run:
        print(f"DRY: завёл бы issue «{title}» (labels {ISSUE_LABELS}) и карточку Blocked на доске")
        return None
    first = not ep.get("issue")
    reused = False
    if first:
        number = find_open_issue(title)
        reused = number is not None
        if number is None:
            body = (f"Сторож `/root/watchdog/espn_stall_watch.py` (#1496): правило `{rule}` держится "
                    f"с {ep['first']} ({(now - parse_ts(ep['first'])).total_seconds() / 3600:.0f} ч).\n\n"
                    f"Последний сигнал: {text}\n\n"
                    f"Что смотреть: `is_paused` DAG ESPN в metadb `{METADB}`, max(`{TS_COL}`) в "
                    f"`{'`, `'.join(BRONZE_TABLES)}`, строку ESPN в утренней сводке, лог "
                    "`/root/watchdog/espn_stall_watch.log`.")
            url = gh(["issue", "create", "--repo", GH_REPO, "--title", title, "--body", body,
                      "--label", ISSUE_LABELS])
            m = re.search(r"/issues/(\d+)", url or "")
            if not m:
                return None
            number = int(m.group(1))
        ep["issue"] = number
    ep["blocked"] = board_blocked(ep["issue"])
    if not ep["blocked"]:
        print(f"espn_stall_watch: issue #{ep['issue']}: карточку Blocked поставить не вышло — "
              "повтор следующим тиком")
    if not first:
        return None
    board = "карточка Blocked" if ep["blocked"] else "карточку Blocked поставить не вышло — повторю"
    return (f"📌 ESPN: {RULE_TITLE[rule]} — держится ≥ {ISSUE_AFTER_H} ч, issue #{ep['issue']} "
            f"{'уже была открыта' if reused else 'заведена'}, {board}. #1496")


def load_state(path):
    try:
        st = json.loads(path.read_text())
    except (OSError, ValueError):
        st = {}
    st.setdefault("episodes", {})
    st.setdefault("pending", [])
    return st


def save_state(path, state):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=0))
    os.replace(tmp, path)


def send_pending(state, now):
    keep = []
    for item in state["pending"]:
        late = "" if item["at"] == fmt_ts(now) else f" (повтор: первая попытка {item['at'][11:16]} UTC не подтверждена)"
        if not tg_send(item["text"] + late):
            keep.append(item)
    state["pending"] = keep[-PENDING_MAX:]
    return len(keep)


def episode(state, rule, text, now, msgs, bits, dry_run, issue=True):
    """text=None — условие снято: отбой и эпизод забыт. Иначе новое — тревога; то же — раз в
    сутки «продолжается», через ISSUE_AFTER_H — issue (issue=False — без issue, #1505)."""
    ep = state["episodes"].get(rule)
    if text is None:
        if ep:
            state["episodes"].pop(rule)
            msgs.append(f"✅ ESPN: отбой — {rule_title(rule)} (эпизод с {ep['first'][:16]}Z"
                        + (f", issue #{ep['issue']}" if ep.get("issue") else "") + ")")
            bits.append(f"{rule}=cleared")
        return
    if not ep:
        msgs.append(text)
        state["episodes"][rule] = {"first": fmt_ts(now), "last_reminded_at": fmt_ts(now)}
        bits.append(f"{rule}=new")
        return
    hours = (now - parse_ts(ep["first"])).total_seconds() / 3600
    escalated = issue and hours >= ISSUE_AFTER_H and not ep.get("blocked")
    if escalated:   # issue ещё нет или карточка не встала — (повторная) эскалация
        second = escalate(rule, ep, text, now, dry_run)
        if second:
            msgs.append(second)
            ep["last_reminded_at"] = fmt_ts(now)   # вторая тревога заменяет «продолжается»
        bits.append(f"{rule}=escalate")
    if now - parse_ts(ep.get("last_reminded_at") or ep["first"]) >= timedelta(hours=24):
        msgs.append(f"⏳ ESPN: продолжается ({hours:.0f} ч) — {text}")
        ep["last_reminded_at"] = fmt_ts(now)
        bits.append(f"{rule}=reminded")
    elif not escalated:
        bits.append(f"{rule}=silenced")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="не слать в Telegram и не заводить issue (state пишется — нужен --state)")
    ap.add_argument("--state", type=Path, default=STATE_DEFAULT)
    ap.add_argument("--now", help="подмена «сейчас», ISO UTC (пауза DAG — живая)")
    args = ap.parse_args(argv)
    if args.dry_run and args.state == STATE_DEFAULT:
        ap.error("--dry-run пишет state: укажите отдельный --state, боевой не трогаем")

    args.state.parent.mkdir(parents=True, exist_ok=True)
    lock = open(args.state.with_name(args.state.name + ".lock"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("espn_stall_watch: предыдущий запуск ещё идёт — выхожу")
        return 0

    now = parse_ts(args.now) if args.now else datetime.now(timezone.utc)
    dag_rows = read_dags()
    bronze = read_bronze(now)
    alerts = evaluate(now, dag_rows, bronze)
    waves = read_red_waves()
    downgrades = read_downgrades(now)

    state = load_state(args.state)
    msgs, bits = [], []
    if bronze is None:
        bits.append("stall=trino_unavailable")
    for rule in ("paused", "stall"):
        if rule in alerts:
            episode(state, rule, alerts[rule], now, msgs, bits, args.dry_run)
    if waves is None:
        bits.append("red=no_wave_log")
    else:
        for rule, text in evaluate_red(waves, state["episodes"]).items():
            episode(state, rule, text, now, msgs, bits, args.dry_run, issue=False)
    if downgrades is None:
        bits.append("downgrade=no_recheck_log")
    else:
        for rule, text in evaluate_downgrade(downgrades).items():
            episode(state, rule, text, now, msgs, bits, args.dry_run, issue=False)

    failed = 0
    if args.dry_run:
        for text in msgs:
            print("DRY:", text)
    else:
        for text in msgs:
            state["pending"].append({"text": text, "at": fmt_ts(now)})
        failed = send_pending(state, now)
    save_state(args.state, state)
    dags_txt = ("metadb_unavailable" if dag_rows is None
                else ",".join(f"{d}={p}" for d, p in dag_rows) or "none")
    bronze_txt = "?" if bronze is None else f"{bronze[0]} last={bronze[1] or '-'}"
    print(f"espn_stall_watch: alerts={len(msgs)} dags={dags_txt} fresh{STALL_H}h={bronze_txt} "
          f"{' '.join(bits)}{' now=' + args.now if args.now else ''}"
          + (f" send_failed={failed} pending={len(state['pending'])}" if failed or state["pending"] else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
