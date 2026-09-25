"""Daily DQ checks of the ESPN bronze tables (#1505) and the recheck lines (#1506).

Every check runs on DuckDB (Trino -> DuckDB via sqlglot) over synthetic
rows: it stays silent on clean data and names its own breakage.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from scrapers.espn.quality import (
    build_quality_sql,
    build_recheck_sql,
    build_summary_loads_sql,
    render_quality_lines,
)

pytestmark = pytest.mark.unit

DAY = "2026-10-11"
AT = datetime(2026, 10, 11, 6)
FETCHED = datetime(2026, 10, 11, 5)


def _db():
    pytest.importorskip("sqlglot")
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA bronze")
    con.execute("CREATE SCHEMA ops")
    con.execute(
        "CREATE TABLE bronze.espn_match (competition_slug varchar, event_id bigint, "
        "played_final boolean, disposition varchar, home_score integer, away_score integer, "
        "_ingested_at timestamp, _source_fetched_at timestamp)"
    )
    for table in ("espn_match_lineup", "espn_team_stats", "espn_match_events"):
        con.execute(
            f"CREATE TABLE bronze.{table} (event_id bigint, _ingested_at timestamp, "
            "_source_fetched_at timestamp)"
        )
    con.execute(
        "CREATE TABLE ops.espn_request_journal_v1 (request_date date, url_fingerprint varchar, "
        "endpoint varchar, disposition varchar)"
    )
    con.execute(
        "CREATE TABLE ops.espn_recheck_v1 (checked_at timestamp, run_id varchar, slug varchar, "
        "season_year integer, event_id bigint, kind varchar, before_parts varchar, "
        "after_parts varchar, outcome varchar)"
    )
    return con


def _load(con, fingerprint, day="2026-10-11", disposition="success", times=1):
    for _ in range(times):
        con.execute(
            "INSERT INTO ops.espn_request_journal_v1 VALUES (CAST(? AS date), ?, 'summary', ?)",
            [day, fingerprint, disposition],
        )


def _recheck(con, event_id, outcome, *, slug="eng.1", kind="recheck", at=AT):
    con.execute(
        "INSERT INTO ops.espn_recheck_v1 VALUES (?, 'r', ?, 2026, ?, ?, '{}', '{}', ?)",
        [at, slug, event_id, kind, outcome],
    )


def _match(con, event_id, slug="eng.1", disposition="captured", *, played=True,
           score=(1, 0), at=AT, fetched=FETCHED):
    con.execute(
        "INSERT INTO bronze.espn_match VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [slug, event_id, played, disposition, *score, at, fetched],
    )


def _clean(con):
    for event_id in range(1, 11):
        _match(con, event_id, disposition="valid_empty" if event_id == 1 else "captured")
        con.execute("INSERT INTO bronze.espn_match_lineup VALUES (?, ?, ?)", [event_id, AT, FETCHED])
        con.execute(
            "INSERT INTO ops.espn_request_journal_v1 VALUES (DATE '2026-10-11', ?, 'summary', 'success')",
            [f"fp{event_id}"],
        )
    # A small tournament above the share: fewer than 5 matches never alarms.
    for event_id in range(20, 23):
        _match(con, event_id, slug="ger.2", disposition="source_malformed", score=(0, 0))
    # Not played, not written that day, cache hits of the journal: outside.
    _match(con, 30, played=False, disposition=None, score=(None, None))
    _match(con, 31, disposition="source_malformed", at=AT - timedelta(days=1))
    for _ in range(3):
        con.execute(
            "INSERT INTO ops.espn_request_journal_v1 VALUES (DATE '2026-10-11', 'fp1', 'summary', 'cache_hit')"
        )


def _query(con, sql):
    import sqlglot

    duck = sqlglot.transpile(sql, read="trino", write="duckdb")[0]
    duck = duck.replace("iceberg.bronze.", "bronze.").replace("iceberg.ops.", "ops.")
    return con.execute(duck).fetchall()


def _lines(con):
    rows = _query(con, build_quality_sql(DAY)) + _query(con, build_summary_loads_sql(DAY))
    return render_quality_lines(DAY, rows, _query(con, build_recheck_sql(DAY)))


NO_RECHECK = [
    "• ESPN перепроверка 11.10: downgrade_rejected за сутки: 0",
    "• ESPN перепроверка 11.10: перепроверок не было",
]


def test_clean_day_prints_only_the_recheck_lines():
    con = _db()
    _clean(con)
    assert _lines(con) == NO_RECHECK


@pytest.mark.parametrize(
    "breakage, expected",
    [
        (
            lambda con: [_match(con, 40 + i, disposition="valid_empty") for i in range(3)],
            "• ESPN DQ 11.10: доля valid_empty > 20 % (от 5 матчей): eng.1 4/13 ‼️",
        ),
        (
            lambda con: [_match(con, 40 + i, disposition="source_malformed") for i in range(3)],
            "• ESPN DQ 11.10: доля source_malformed > 20 % (от 5 матчей): eng.1 3/13 ‼️",
        ),
        (
            lambda con: [_match(con, 40 + i, disposition="lineup_anomaly") for i in range(3)],
            "• ESPN DQ 11.10: доля lineup_anomaly > 20 % (от 5 матчей): eng.1 3/13 ‼️",
        ),
        (
            lambda con: _match(con, 5, slug="eng.1", at=AT - timedelta(days=3)),
            "• ESPN DQ 11.10: дубли event_id в espn_match: 1 ‼️",
        ),
        (
            lambda con: con.execute(
                "INSERT INTO bronze.espn_match_lineup VALUES (1, ?, ?)", [AT, AT + timedelta(hours=1)]
            ),
            "• ESPN DQ 11.10: _ingested_at < _source_fetched_at в espn_match_lineup: 1 из 11 ‼️",
        ),
        (
            lambda con: _match(con, 50, score=(None, 0)),
            "• ESPN DQ 11.10: сыгранный без счёта (NULL вместо 0) eng.1: 1 из 11 ‼️",
        ),
        (
            # fp2: two more loads on D-6 (outside the 3-day peak): the week is 12/10.
            lambda con: _load(con, "fp2", day="2026-10-05", times=2),
            "• ESPN DQ 11.10: summary в среднем 1.20 загрузки на матч за 7 суток (> 1.1): "
            "12 на 10 матч(ей) ‼️",
        ),
    ],
    ids=["valid_empty", "source_malformed", "lineup_anomaly", "duplicate", "ingest_order",
         "null_score", "summary_loads"],
)
def test_each_check_catches_its_breakage(breakage, expected):
    con = _db()
    _clean(con)
    breakage(con)
    assert _lines(con) == [expected, *NO_RECHECK]


# ---------------------------------------------------- summary loads (#1506)


def _loads_lines(con):
    return render_quality_lines(DAY, _query(con, build_summary_loads_sql(DAY)), [])[:-2]


@pytest.mark.parametrize(
    "extra, expected",
    [
        # 10 matches over the week with 10, 11 and 12 network loads.
        (0, []),
        (1, []),
        (2, ["• ESPN DQ 11.10: summary в среднем 1.20 загрузки на матч за 7 суток (> 1.1): "
             "12 на 10 матч(ей) ‼️"]),
    ],
    ids=["1.0", "1.1", "1.2"],
)
def test_week_average_of_summary_loads(extra, expected):
    con = _db()
    for index in range(10):
        # Spread over the week; D-7 and cache hits are outside.
        _load(con, f"fp{index}", day=f"2026-10-{5 + index % 7:02d}")
        _load(con, f"fp{index}", day="2026-10-04")
        _load(con, f"fp{index}", disposition="cache_hit")
    for index in range(extra):
        # A second load of fp{index} four days later: the 3-day peak stays at 1.
        _load(con, f"fp{index}", day=f"2026-10-{9 + index % 7:02d}")
    assert _loads_lines(con) == expected


@pytest.mark.parametrize(
    "loads, expected",
    [
        (2, []),
        (3, ["• ESPN DQ 11.10: summary качали > 2 раз за 3 суток: 1 матч(ей) ‼️"]),
    ],
)
def test_three_day_peak_of_summary_loads(loads, expected):
    con = _db()
    # 30 other matches keep the week average under 1.1 whatever fp0 does.
    for index in range(1, 31):
        _load(con, f"fp{index}")
    for offset in range(loads):
        _load(con, "fp0", day=f"2026-10-{11 - offset:02d}")
    assert _loads_lines(con) == expected


def test_downgrades_and_fill_rate_of_the_recheck():
    con = _db()
    _clean(con)
    for event_id, outcome in ((1, "filled"), (2, "same"), (3, "failed"), (4, "same")):
        _recheck(con, event_id, outcome)
    _recheck(con, 5, "filled", slug="gua.1")
    _recheck(con, 6, "downgrade_rejected", slug="bra.copa_do_brazil")
    _recheck(con, 7, "downgrade_rejected", slug="bra.copa_do_brazil", kind="sample_24h")
    _recheck(con, 8, "downgrade_rejected", slug="eng.1", kind="refresh")
    # Samples do not count in the fill rate; another day is outside.
    _recheck(con, 9, "filled", kind="sample_72h")
    _recheck(con, 10, "downgrade_rejected", at=AT - timedelta(days=1))

    assert _lines(con) == [
        "• ESPN перепроверка 11.10: downgrade_rejected за сутки: 3 "
        "(bra.copa_do_brazil 2, eng.1 1) ‼️",
        "• ESPN перепроверка 11.10: дозаполнено при перепроверке: 2 из 6 (33.3 %); "
        "по лигам: eng.1 1/4, gua.1 1/1",
    ]


def test_failed_query_is_named_not_hidden():
    assert render_quality_lines(DAY, None, None) == [
        "• ESPN DQ 11.10: не посчитано ⚠️", "• ESPN перепроверка 11.10: не посчитано ⚠️",
    ]
