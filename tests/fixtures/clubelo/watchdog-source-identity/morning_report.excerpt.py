# ---- hunk 1 and hunk 2: complete helpers, before the patch ----
    return None


def _trino_row(sql, timeout=240):
    """Первая непустая строка ответа Trino — списком полей без кавычек; None, если
    Trino недоступен, запрос упал или не уложился в timeout.

    Отличается от run(): та возвращает "" и при падении, и при пустом ответе."""
    try:
        r = subprocess.run([TRINO, sql], capture_output=True, text=True, timeout=timeout)
    except Exception:
        return None
    if r.returncode != 0:
        return None
    for line in r.stdout.splitlines():
        line = line.strip()
        if line:
            return [c.strip('"') for c in line.split('","')]
    return None


def _trino_rows(sql, timeout=240):
    """Все непустые строки ответа Trino — списками полей без кавычек; None, если
    Trino недоступен, запрос упал или не уложился в timeout. Пустой ответ обёртка
    trino-ro.sh считает падением, поэтому запрос обязан вернуть хотя бы одну строку."""
    try:
        r = subprocess.run([TRINO, sql], capture_output=True, text=True, timeout=timeout)
    except Exception:
        return None
    if r.returncode != 0:
        return None
    return [[c.strip('"') for c in line.strip().split('","')]
            for line in r.stdout.splitlines() if line.strip()]


# ---- hunk 3 ----
CLUBELO_LOOKBACK_DAYS = 60
CLUBELO_SENTINEL = "__sentinel__"
CLUBELO_TABLES = ("clubelo_rank_snapshot", "clubelo_raw_page", "clubelo_history_manifest")
CLUBELO_DATES_SQL = """WITH raw AS (
  SELECT rating_date, min(fetched_at) AS first_seen FROM iceberg.bronze.clubelo_raw_page
  WHERE page = '/Ranking' AND rating_date IS NOT NULL AND rating_date >= DATE '{lo}'
# ---- hunk 4 ----

def _clubelo_rows(sql, width, sentinel):
    """Строки Trino без строки-заглушки; None — запрос упал / не разобран (как _espn_rows)."""
    rows = _trino_rows(f"SELECT * FROM ({sql}) q UNION ALL SELECT '{CLUBELO_SENTINEL}', {sentinel}")
    if rows is None:
        return None
    rows = [r for r in rows if r and r[0] != CLUBELO_SENTINEL]
# ---- hunk 5 ----
    try:
        r = subprocess.run(
            [CLUBELO_COVERAGE_PY, CLUBELO_HISTORY_COVERAGE_MODULE,
             "--trino", TRINO, "--today", now.date().isoformat()],
            capture_output=True, text=True, timeout=CLUBELO_COVERAGE_TIMEOUT_S)
        out = r.stdout.strip().splitlines()
        rep = json.loads(out[-1]) if r.returncode == 0 and len(out) >= 2 else None
# ---- hunk 6 ----
    series = _clubelo_series(dates, fetches, naive, frozen, first_full, rules)
    history_line, history_bad = _clubelo_history_coverage(now)
    cut = (naive - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    wire_row = _trino_row(CLUBELO_BYTES_SQL.format(cut=cut))
    wire = int(wire_row[0]) if wire_row and wire_row[0].isdigit() else None
    line = _clubelo_format(dates, series, history_line, history_bad, wire, frozen, rules)
    if "--dry-run" not in sys.argv:
# ---- hunk 7 ----

class _ClubeloDqTrino:
    def execute_query(self, sql):
        rows = _trino_rows(sql)
        if rows is None:
            raise RuntimeError("trino")
        out = []
