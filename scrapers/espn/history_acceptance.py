"""Read-only tier A acceptance and legacy parity (#1511).

Core evidence comes from saved season/list bodies, never from queue.matches.
This module renders SELECTs; it neither fetches nor writes data.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from .core_lists import collect_refs, event_ids, parse_season
from .journal import _literal
from .parser_common import EspnParseError

LEGACY_LEAGUES = {
    "eng.1": "ENG-Premier League", "esp.1": "ESP-La Liga",
    "ger.1": "GER-Bundesliga", "ita.1": "ITA-Serie A",
    "fra.1": "FRA-Ligue 1", "fifa.world": "INT-World Cup",
}


@dataclass(frozen=True)
class CoreEvidence:
    year: int
    types: tuple[tuple[int, tuple[int, ...]], ...]


def core_evidence(season: dict, pages: Mapping[int, Sequence[bytes]]) -> CoreEvidence:
    """Validate every saved page of every type, retaining the original count.

    collect_refs checks the declared count against all refs before event_ids
    deduplicates/validates ids. No cross-type deduplication here.
    """
    core, types = parse_season(season)
    if set(pages) != {item.id for item in types}:
        raise EspnParseError("core evidence must cover exactly all season types")
    result = []
    for item in types:
        bodies = pages[item.id]
        requested = []

        def fetch(number):
            requested.append(number)
            if number > len(bodies):
                raise EspnParseError("core evidence lacks a page")
            return bodies[number - 1]

        ids = event_ids(collect_refs(fetch, f"history evidence type {item.id}"))
        if len(requested) != len(bodies):
            raise EspnParseError("core evidence has extra pages")
        result.append((item.id, ids))
    return CoreEvidence(core.year, tuple(result))


def _values(rows, columns):
    values = list(rows)
    if not values:
        return "SELECT " + ", ".join(f"CAST(NULL AS {kind}) AS {name}" for name, kind in columns) + " WHERE false"
    return "SELECT * FROM (VALUES " + ", ".join(
        "(" + ", ".join(_literal(value, kind) for value, (_, kind) in zip(row, columns)) + ")" for row in values
    ) + ") AS v(" + ", ".join(name for name, _ in columns) + ")"


def _scope(scope):
    pairs = tuple(scope)
    if not pairs or len(set(pairs)) != len(pairs):
        raise ValueError("acceptance requires a nonempty unique pinned scope")
    return pairs


def render_acceptance_sql(scope, evidence: Mapping[tuple[str, int], CoreEvidence],
                          quality_errors=()) -> str:
    """One row per pinned pair, including absent pairs and failed inventories.

    quality_errors are confirmed pair-level defects (e.g. capability-aware
    empty-part defects); unknown/missing evidence blocks acceptance separately.
    """
    scope = _scope(scope)
    for pair, item in evidence.items():
        if pair not in scope or pair[1] != item.year:
            raise ValueError("core evidence does not belong to the pinned season")
    if not set(quality_errors).issubset(scope):
        raise ValueError("quality error outside pinned scope")
    pairs = _values(scope, (("slug", "varchar"), ("season_year", "integer")))
    counts = _values(((slug, year, sum(len(ids) for _, ids in item.types), len(item.types))
                      for (slug, year), item in evidence.items()),
                     (("slug", "varchar"), ("season_year", "integer"),
                      ("core_count", "bigint"), ("type_count", "bigint")))
    types = _values(((slug, year, kind) for (slug, year), item in evidence.items()
                     for kind, _ in item.types),
                    (("slug", "varchar"), ("season_year", "integer"), ("season_type", "integer")))
    events = _values(((slug, year, event) for (slug, year), item in evidence.items()
                      for _, ids in item.types for event in ids),
                     (("slug", "varchar"), ("season_year", "integer"), ("event_id", "bigint")))
    errors = _values(quality_errors, (("slug", "varchar"), ("season_year", "integer")))
    return f"""WITH scope AS ({pairs}), counts AS ({counts}),
expected_types AS ({types}), events AS ({events}), errors AS ({errors}),
q AS (
    SELECT slug, season_year, count(*) AS rows,
           bool_and(state IN ('done', 'empty', 'red')) AS finished,
           bool_or(state = 'red') AS red
    FROM iceberg.ops.espn_history_queue_v1 WHERE season_year > 0
    GROUP BY slug, season_year
), inventory AS (
    SELECT slug, bool_or(state = 'red') AS red
    FROM iceberg.ops.espn_history_queue_v1 WHERE season_year = 0 AND slug <> '(run)'
    GROUP BY slug
), type_gaps AS (
    SELECT t.slug, t.season_year, count_if(q.slug IS NULL) AS gaps
    FROM expected_types t LEFT JOIN iceberg.ops.espn_history_queue_v1 q
      ON q.slug = t.slug AND q.season_year = t.season_year AND q.season_type = t.season_type
    GROUP BY t.slug, t.season_year
), expected AS (
    SELECT DISTINCT slug, season_year, event_id FROM events
), published AS (
    SELECT DISTINCT m.competition_slug AS slug, m.season_year, m.event_id
    FROM iceberg.bronze.espn_match m JOIN scope s
      ON s.slug = m.competition_slug AND s.season_year = m.season_year
    WHERE m.first_published_at IS NOT NULL
), ids AS (
    SELECT coalesce(e.slug, p.slug) AS slug, coalesce(e.season_year, p.season_year) AS season_year,
           count(e.event_id) AS core_unique, count(p.event_id) AS published,
           count_if(e.event_id IS NOT NULL AND p.event_id IS NULL) AS missing_ids,
           count_if(p.event_id IS NOT NULL AND e.event_id IS NULL) AS extra_ids
    FROM expected e FULL JOIN published p
      ON e.slug = p.slug AND e.season_year = p.season_year AND e.event_id = p.event_id
    GROUP BY coalesce(e.slug, p.slug), coalesce(e.season_year, p.season_year)
)
SELECT s.slug, s.season_year, q.slug IS NULL AS missing,
       coalesce(NOT q.finished, false) AS pending,
       (coalesce(q.red, false) OR (coalesce(i.red, false) AND q.slug IS NULL) OR err.slug IS NOT NULL
        OR (coalesce(q.finished, false) AND c.slug IS NOT NULL
            AND (coalesce(d.missing_ids, 0) > 0 OR coalesce(d.extra_ids, 0) > 0))) AS red,
       c.slug IS NULL AS evidence_missing,
       c.core_count, coalesce(d.core_unique, 0) AS core_unique,
       c.core_count - coalesce(d.core_unique, 0) AS cross_type_duplicates,
       coalesce(d.published, 0) AS published,
       coalesce(d.missing_ids, 0) AS missing_ids, coalesce(d.extra_ids, 0) AS extra_ids,
       (coalesce(q.finished, false) AND ((c.slug IS NULL AND coalesce(q.red, false))
        OR (c.slug IS NOT NULL AND coalesce(t.gaps, 0) = 0
        AND ((c.type_count = 0 AND q.rows = 1) OR c.type_count = q.rows)))) AS finished
FROM scope s LEFT JOIN q ON q.slug = s.slug AND q.season_year = s.season_year
LEFT JOIN inventory i ON i.slug = s.slug
LEFT JOIN counts c ON c.slug = s.slug AND c.season_year = s.season_year
LEFT JOIN ids d ON d.slug = s.slug AND d.season_year = s.season_year
LEFT JOIN type_gaps t ON t.slug = s.slug AND t.season_year = s.season_year
LEFT JOIN errors err ON err.slug = s.slug AND err.season_year = s.season_year
ORDER BY s.slug, s.season_year"""


def render_acceptance(rows: Sequence[Sequence]) -> str:
    total = len(rows)
    red = sum(bool(row[4]) for row in rows)
    blocked = sum(bool(row[2] or row[3] or (row[5] and not row[4]) or not row[12]) for row in rows)
    accepted = total > 0 and not blocked and red * 5 < total
    lines = [f"Tier A: {total} пар, красных {red}, незавершённых {blocked}; "
             f"приёмка: {'да' if accepted else 'нет'} (красных строго < 20%)."]
    lines.extend(f"{r[0]}/{r[1]}: missing={r[2]}, pending={r[3]}, red={r[4]}, "
                 f"evidence_missing={r[5]}, core_count={r[6]}, unique={r[7]}, "
                 f"cross_type_duplicates={r[8]}, published={r[9]}, "
                 f"missing_ids={r[10]}, extra_ids={r[11]}, finished={r[12]}" for r in rows)
    return "\n".join(lines)


def _legacy_ctes(scope):
    scope = _scope(scope)
    pairs = _values(((slug, year, str(year) if slug == "fifa.world" else f"{year % 100:02d}{(year + 1) % 100:02d}")
                     for slug, year in scope if slug in LEGACY_LEAGUES),
                    (("slug", "varchar"), ("season_year", "integer"), ("season", "varchar")))
    leagues = _values(LEGACY_LEAGUES.items(), (("slug", "varchar"), ("league", "varchar")))
    return f"""scope AS ({pairs}), leagues AS ({leagues}), legacy AS (
    SELECT l.slug, s.* FROM iceberg.bronze.espn_schedule s
    JOIN leagues l ON l.league = s.league
    WHERE NOT (l.slug = 'ita.1' AND CAST(s.season AS varchar) = '2021')
)"""


def render_parity_sql(scope) -> str:
    """Match by league + event id across legacy season labels, then compare dates.

    No arbitrary winner for duplicates. ITA '2021' is excluded and reported by
    render_legacy_parts_sql. NULL dates/scores and ambiguous keys are uncomparable.
    """
    return f"""WITH {_legacy_ctes(scope)}, n AS (
    SELECT m.competition_slug AS slug, m.season_year, m.event_id,
           min(m.kickoff) AS kickoff, min(m.home_score) AS home_score,
           min(m.away_score) AS away_score, count(*) AS native_rows
    FROM iceberg.bronze.espn_match m JOIN scope s
      ON s.slug = m.competition_slug AND s.season_year = m.season_year
    WHERE m.first_published_at IS NOT NULL
    GROUP BY m.competition_slug, m.season_year, m.event_id
), native_links AS (
    SELECT n.*, count(*) OVER (PARTITION BY slug, event_id) AS native_labels FROM n
), l AS (
    SELECT slug, CAST(season AS varchar) AS season, TRY_CAST(game_id AS bigint) AS event_id,
           min(TRY_CAST(match_date AS timestamp)) AS kickoff,
           min(TRY_CAST(home_goals AS integer)) AS home_score,
           min(TRY_CAST(away_goals AS integer)) AS away_score, count(*) AS legacy_rows
    FROM legacy GROUP BY slug, CAST(season AS varchar), TRY_CAST(game_id AS bigint)
), links AS (
    SELECT l.*, count(*) OVER (PARTITION BY slug, event_id) AS legacy_labels FROM l
)
SELECT coalesce(n.slug, l.slug) AS slug, n.season_year, l.season AS legacy_season,
       coalesce(n.event_id, l.event_id) AS event_id,
       n.slug IS NOT NULL AND l.slug IS NULL AS native_only,
       n.slug IS NULL AS legacy_only, n.native_rows, l.legacy_rows,
       coalesce(n.native_rows > 1 OR n.native_labels > 1 OR l.legacy_rows > 1 OR l.legacy_labels > 1, false) AS ambiguous,
       CASE WHEN n.native_rows = 1 AND n.native_labels = 1 AND l.legacy_rows = 1 AND l.legacy_labels = 1
                 AND n.kickoff IS NOT NULL AND l.kickoff IS NOT NULL
            THEN CAST(n.kickoff AS date) <> CAST(l.kickoff AS date) END AS date_mismatch,
       CASE WHEN n.native_rows = 1 AND n.native_labels = 1 AND l.legacy_rows = 1 AND l.legacy_labels = 1
                 AND n.home_score IS NOT NULL AND n.away_score IS NOT NULL
                 AND l.home_score IS NOT NULL AND l.away_score IS NOT NULL
            THEN n.home_score <> l.home_score OR n.away_score <> l.away_score END AS score_mismatch,
       CASE WHEN n.slug IS NOT NULL AND l.slug IS NOT NULL
            THEN l.season <> s.season END AS season_mismatch
FROM native_links n FULL JOIN links l ON n.slug = l.slug AND n.event_id = l.event_id
LEFT JOIN scope s ON s.slug = n.slug AND s.season_year = n.season_year
WHERE n.slug IS NOT NULL OR EXISTS (
    SELECT 1 FROM scope x WHERE x.slug = l.slug AND x.season = l.season
)
ORDER BY slug, n.season_year, legacy_season, event_id"""


def render_legacy_parts_sql(scope) -> str:
    """Unmappable lineup/matchsheet rows and excluded ITA schedule rows.

    These tables lack event_id: only a unique schedule game link can identify
    one. NULL scores stay explicitly uncomparable, never a score mismatch.
    """
    return f"""WITH {_legacy_ctes(scope)}, schedule_links AS (
    SELECT league, CAST(season AS varchar) AS season, game,
           count(*) AS links, count(TRY_CAST(game_id AS bigint)) AS ids
    FROM legacy GROUP BY league, CAST(season AS varchar), game
), parts AS (
    SELECT 'lineup' AS part, league, CAST(season AS varchar) AS season, game FROM iceberg.bronze.espn_lineup
    UNION ALL
    SELECT 'matchsheet', league, CAST(season AS varchar), game FROM iceberg.bronze.espn_matchsheet
)
SELECT p.part, l.slug, p.season, count(*) AS rows,
       count_if(coalesce(s.links, 0) <> 1 OR coalesce(s.ids, 0) <> 1) AS uncomparable
FROM parts p JOIN leagues l ON l.league = p.league
JOIN scope x ON x.slug = l.slug AND x.season = p.season
LEFT JOIN schedule_links s ON s.league = p.league AND s.season = p.season AND s.game = p.game
GROUP BY p.part, l.slug, p.season
UNION ALL
SELECT 'excluded_schedule', 'ita.1', '2021', count(*), count(*)
FROM iceberg.bronze.espn_schedule WHERE league = 'ITA-Serie A' AND CAST(season AS varchar) = '2021'"""


def render_parity(rows: Sequence[Sequence]) -> str:
    lines = ["Паритет native/legacy по event_id; legacy не является эталоном полноты."]
    lines.extend(f"{r[0]}/{r[1]} legacy={r[2]} event_id={r[3]}: native_only={r[4]}, "
                 f"legacy_only={r[5]}, native_rows={r[6]}, legacy_rows={r[7]}, "
                 f"ambiguous={r[8]}, date_mismatch={r[9]}, score_mismatch={r[10]}, "
                 f"season_mismatch={r[11]}" for r in rows)
    return "\n".join(lines)
