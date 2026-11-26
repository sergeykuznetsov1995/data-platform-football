"""Publish #1368 standings freshness from the two existing Bronze tables."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

if __name__ == "__main__" and not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from deploy.sofascore.veha1_measure import trino_rows
from scrapers.sofascore.denominator import (
    DEFAULT_DENOMINATOR_PATH,
    STANDINGS_NOT_APPLICABLE,
    load_denominator,
)


SNAPSHOTS_SQL = """
SELECT CAST(CAST(floor(to_unixtime(current_timestamp)) AS bigint) AS varchar),
       (SELECT snapshot_id FROM iceberg.bronze."sofascore_schedule$refs" WHERE name = 'main'),
       (SELECT snapshot_id FROM iceberg.bronze."sofascore_league_table$refs" WHERE name = 'main')
UNION ALL SELECT '__sentinel__', NULL, NULL
"""


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def read_census(epoch: int, schedule: int, standings: int, core: frozenset[str]):
    lower, upper = epoch - 36 * 3600, epoch + 6 * 3600
    # Use recent rows only to find candidate partitions. A later correction can move
    # a game outside the window, so rank its entire partition before testing kickoff.
    candidates = trino_rows(
        f"""
        SELECT DISTINCT league, CAST(season AS varchar)
        FROM iceberg.bronze.sofascore_schedule FOR VERSION AS OF {schedule}
        WHERE start_timestamp BETWEEN {lower} AND {upper}
        UNION ALL SELECT '__sentinel__', NULL
    """,
        2,
    )
    pairs = [(league, season) for league, season in candidates if league in core]
    if any(not season for _, season in pairs):
        raise ValueError("Missing season in a core schedule partition")
    if not pairs:
        return []
    values = ", ".join(
        f"({sql_literal(key)}, {sql_literal(season)})" for key, season in pairs
    )
    return trino_rows(
        f"""
        WITH candidate(league, season) AS (VALUES {values}), ranked AS (
            SELECT s.league, CAST(s.season AS varchar) AS season, s.start_timestamp,
                   row_number() OVER (
                       PARTITION BY s.league, s.season, s.game_id ORDER BY s._ingested_at DESC
                   ) AS rn
            FROM iceberg.bronze.sofascore_schedule FOR VERSION AS OF {schedule} AS s
            JOIN candidate c ON c.league = s.league AND c.season = CAST(s.season AS varchar)
        ), ongoing AS (
            SELECT DISTINCT league, season FROM ranked
            WHERE rn = 1 AND start_timestamp BETWEEN {lower} AND {upper}
        ), ages AS (
            SELECT s.league, CAST(s.season AS varchar) AS season, max(s._ingested_at) AS last_ingested
            FROM iceberg.bronze.sofascore_league_table FOR VERSION AS OF {standings} AS s
            JOIN candidate c ON c.league = s.league AND c.season = CAST(s.season AS varchar)
            GROUP BY 1, 2
        )
        SELECT o.league, o.season, CAST(a.last_ingested AS varchar)
        FROM ongoing o LEFT JOIN ages a ON a.league = o.league AND a.season = o.season
        UNION ALL SELECT '__sentinel__', NULL, NULL
    """,
        3,
    )


def render_report(
    rows, *, as_of: datetime, names: dict[str, str], schedule: int, standings: int
) -> str:
    cutoff = as_of - timedelta(days=7)
    stale = []
    seen = set()
    excluded = 0
    for league, season, last in rows:
        if league not in names or not season or (league, season) in seen:
            raise ValueError(
                "Unexpected or duplicate league/season in standings census"
            )
        seen.add((league, season))
        if (league, season) in STANDINGS_NOT_APPLICABLE:
            excluded += 1
            continue
        stamp = datetime.fromisoformat(last) if last else None
        if stamp is not None and stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        if stamp is None or stamp < cutoff:
            stale.append((league, season, stamp))

    total = len(seen) - excluded
    fresh = total - len(stale)
    needed = max(0, (95 * total + 99) // 100 - fresh)
    moscow = ZoneInfo("Europe/Moscow")
    lines = [
        f"SofaScore #1368 · {as_of.astimezone(moscow):%d.%m.%Y %H:%M:%S} МСК",
        f"Свежие таблицы за последние 7 дней — {fresh} из {total}.",
    ]
    if not total:
        lines.append("Нет подходящих сезонов для проверки цели 95%.")
    elif needed:
        lines.append(f"Для цели 95% нужно обновить ещё {needed}.")
    else:
        lines.append("Цель 95% достигнута.")
    lines.append(f"Кубковых сезонов без турнирной таблицы в этом окне — {excluded}.")
    if stale:
        lines.extend(["", "Какие таблицы ждут обновления"])
    for league, season, stamp in sorted(
        stale, key=lambda row: (row[2] is not None, row[2], row[0], row[1])
    ):
        date = (
            stamp.astimezone(moscow).strftime("%d.%m.%Y %H:%M МСК")
            if stamp
            else "таблицы нет"
        )
        lines.append(f"{names[league]} ({league}, {season}) — {date}")
    lines.extend(["", f"Снимок расписания {schedule} · снимок таблиц {standings}."])
    return "\n".join(lines) + "\n"


def publish(path: Path) -> None:
    registry = load_denominator(DEFAULT_DENOMINATOR_PATH)
    core = registry.core_capture_keys()
    if not core:
        raise ValueError("Empty core tournament registry")
    names = {
        row.capture_key: row.name
        for row in registry.rows.values()
        if row.tournament_class == "core"
    }
    [snapshot] = trino_rows(SNAPSHOTS_SQL, 3)
    epoch, schedule, standings = (int(value) for value in snapshot)
    if min(epoch, schedule, standings) <= 0:
        raise ValueError("Missing warehouse timestamp or snapshot")
    rows = read_census(epoch, schedule, standings, core)
    report = render_report(
        rows,
        as_of=datetime.fromtimestamp(epoch, timezone.utc),
        names=names,
        schedule=schedule,
        standings=standings,
    )
    # Readers always see a complete file, including when a query or a write fails.
    with tempfile.NamedTemporaryFile(
        dir=path.parent, mode="w", encoding="utf-8", delete=False
    ) as tmp:
        tmp_path = Path(tmp.name)
        try:
            tmp.write(report)
            tmp.flush()
            os.fchmod(tmp.fileno(), 0o644)
            os.replace(tmp_path, path)
        finally:
            tmp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    publish(parser.parse_args().output)
