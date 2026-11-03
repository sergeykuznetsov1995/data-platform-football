#!/usr/bin/env python3
"""«Ночь без данных — красная» (#1246, #1376): кампания SofaScore за одни UTC-сутки.

Строки утренней сводки «кампания SofaScore за <день>» и «ожидание пула актуалкой»
считаются кодом релиза, а не копией в morning_report.py на хосте. Только чтение:
Bronze — через read-only обёртку Trino (--trino), метабаза — через pipeline_metrics.psql
(READ ONLY), полоса истории — отчёты results/ каталога all-men. Разбор
SOFASCORE-STALL-<день>.md заводит вызывающий (сводка); здесь — только строки и вердикт.

Код выхода: 0 — зелёный, 3 — красный (‼️/⛔), 2 — сторож не отработал.
Запуск: run_from_release.sh campaign_day.py --day YYYY-MM-DD --trino PATH [--runtime DIR]
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Executable by absolute path on a host without installing the project.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from deploy.sofascore import pipeline_metrics as metrics  # noqa: E402

# Гриль 03.09 §3. Калибровка 08.09 (28.08–08.09): коллапс 02.09=243, 03.09=893;
# худший здоровый день 05.09=1270 — двигать некуда.
SS_DAY_MIN_MATCHES = 1000
# ‼️ при 0 скоупов истории со строками за сутки. Гриль §3 (б) «≥15 скоупов/сутки» — цель
# «готов», не порог тревоги; сутки без единого продуктивного скоупа — остановка по
# определению (Sol 08.09 круг 2 п.4: общую таблицу пишут три полосы).
SS_DAY_MIN_SCOPES = 1
# Порог красноты — один, в dags/utils/sofascore_red_share.py (#1356); своего здесь нет.

RUNTIME_KEY = "SOFASCORE_ALL_MENS_RUNTIME_HOST_DIR"


def runtime_from_env_file(path: Path) -> Path:
    """Каталог all-men из env-файла контура; читается только строка RUNTIME_KEY."""
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if line.startswith(RUNTIME_KEY + "="):
                return Path(line.split("=", 1)[1].strip().strip("'\""))
    raise ValueError(f"{RUNTIME_KEY} missing from {path}")


def bounds(day: date) -> tuple[str, str]:
    return f"{day.isoformat()}T00:00:00Z", f"{(day + timedelta(days=1)).isoformat()}T00:00:00Z"


def matches_day(trino: str, day: date, timeout=120) -> int | None:
    """distinct match_id Bronze за UTC-сутки; None — Trino недоступен или запрос упал.
    _ingested_at — timestamp без зоны, сессия Trino в UTC. Пустой ответ обёртка считает
    падением — отсюда строка-страж."""
    sql = ("SELECT cast(count(DISTINCT match_id) AS varchar) "
           "FROM iceberg.bronze.sofascore_match_capture_status "
           f"WHERE _ingested_at >= TIMESTAMP '{day.isoformat()} 00:00:00' "
           f"AND _ingested_at < TIMESTAMP '{(day + timedelta(days=1)).isoformat()} 00:00:00' "
           "UNION ALL SELECT '__sentinel__'")
    try:
        result = subprocess.run([trino, sql], capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        value = line.strip().strip('"')
        if value.isdigit():
            return int(value)
    return None


def history_day(runtime: Path, day: date) -> tuple[int, int]:
    """(скоупов, строк) полосы истории за UTC-сутки: results/*.json с mtime в сутках,
    status=success и <hash>/matches.json capture_status_rows > 0."""
    t0 = datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp()
    scopes = rows = 0
    for path in Path(runtime, "results").glob("*.json"):
        try:
            if not t0 <= path.stat().st_mtime < t0 + 86400:
                continue
            if json.loads(path.read_text()).get("status") != "success":
                continue
            n = int(json.loads((path.with_suffix("") / "matches.json").read_text()).get("capture_status_rows") or 0)
        except (OSError, ValueError, TypeError, AttributeError):
            continue
        if n > 0:
            scopes, rows = scopes + 1, rows + n
    return scopes, rows


def red_share_day(runtime: Path, day: date, query) -> dict | None:
    """{dag_id: (failed, total)} как pipeline_metrics.collect: история слотов — из квитанций
    и отчётов скоупов (scope_reports=True), не (-1,-1). None — метабаза недоступна."""
    rs = metrics.red_share
    t0, t1 = bounds(day)
    try:
        per_dag = rs.parse_rows(query(rs.red_share_sql(t0, t1, scope_reports=True)))
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired):
        return None
    try:
        runs = set(query(rs.history_slot_runs_sql(t0, t1)).splitlines())
        if runs:
            counts = rs.history_scope_counts(runtime / "results", t0, t1, runs)
            old = per_dag.get(rs.HISTORY_DAG_ID, (0, 0))
            per_dag[rs.HISTORY_DAG_ID] = tuple(a + b for a, b in zip(old, counts))
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.TimeoutExpired):
        per_dag[rs.HISTORY_DAG_ID] = (-1, -1)
    return per_dag


def pool_wait_day(day: date, query):
    t0, t1 = bounds(day)
    pw = metrics.pool_wait
    try:
        return pw.parse_rows(query(pw.pool_wait_sql(t0, t1)), query(pw.lane_overlap_sql(t0, t1)))
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired):
        return None


def lines(day: date, *, matches, scopes, rows, red_share, pool_wait) -> tuple[list[str], bool]:
    """Две строки сводки и вердикт (True — красный). Чистая функция."""
    bang = False
    if red_share is None:
        r_txt = "красных run_*_scope TI не посчитаны ⚠️ (metadb)"
    else:
        r_txt = metrics.red_share.format_line(day.isoformat(), red_share)
        bang = metrics.red_share.total_share(red_share).verdict != "ok"
    if matches is None:
        m_txt = "матчей не посчитаны ⚠️ (trino)"
    elif matches < SS_DAY_MIN_MATCHES:
        m_txt, bang = f"матчей {matches}‼️ (<{SS_DAY_MIN_MATCHES})", True
    else:
        m_txt = f"матчей {matches}✅"
    if scopes < SS_DAY_MIN_SCOPES:
        h_txt, bang = f"история {scopes} скоупов‼️", True
    else:
        h_txt = f"история {scopes} скоупов/{rows} строк✅"
    if pool_wait is None:
        wait = "• ожидание пула актуалкой не посчитано ⚠️ (metadb)"
    else:
        wait = f"• {metrics.pool_wait.format_line(pool_wait)} (за {day:%d.%m} UTC)"
    return [f"• кампания SofaScore за {day:%d.%m} (UTC): {m_txt}, {h_txt}, {r_txt}", wait], bang


def main(argv=None, query=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True, type=date.fromisoformat, help="UTC-сутки YYYY-MM-DD")
    parser.add_argument("--trino", required=True, help="read-only обёртка Trino: <путь> \"SELECT …\"")
    parser.add_argument("--runtime", type=Path, help=f"каталог all-men; по умолчанию {RUNTIME_KEY} из env-файла")
    args = parser.parse_args(argv)
    try:
        runtime = args.runtime or runtime_from_env_file(
            Path(os.environ.get("SOFASCORE_ENV_FILE", "/etc/data-platform/sofascore.env")))
        query = query or metrics.psql
        scopes, rows = history_day(runtime, args.day)
        out, bang = lines(args.day, matches=matches_day(args.trino, args.day), scopes=scopes, rows=rows,
                          red_share=red_share_day(runtime, args.day, query),
                          pool_wait=pool_wait_day(args.day, query))
    except Exception as exc:  # noqa: BLE001 — любой сбой = строка ⚠️ и код 2, не тишина
        print(f"• кампания SofaScore: сторож не отработал ⚠️ ({type(exc).__name__})")
        return 2
    print("\n".join(out))
    return 3 if bang else 0


if __name__ == "__main__":
    sys.exit(main())
