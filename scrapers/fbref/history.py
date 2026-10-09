"""Durable newest-year-first history campaign; no source requests here."""

from __future__ import annotations

from collections import Counter
import math

from scrapers.fbref.daily_report import adult_competition, season_token
from scrapers.fbref.page_document import PAGE_DOCUMENT_VERSION
from scrapers.fbref.typed_bronze import TYPED_BRONZE_PARSER_VERSION
from scrapers.fbref.discovery import DISCOVERY_PARSER_VERSION
from scrapers.fbref.control.store import _fetchall, _fetchone, _FRONTIER_SCOPE_CTE

CAMPAIGN_ID = "adult-men-2026-v1"
FIRST_YEAR = 2026
LAST_PRIORITY_YEAR = 2017
HISTORY_SLICE_SECONDS = 20 * 60
HISTORY_PAGE_SECONDS_FLOOR = 180
HISTORY_SETUP_SECONDS = 10 * 60
HISTORY_PAGE_KINDS = (
    "season",
    "season_stats",
    "schedule",
    "standings",
    "squad",
    "match",
)


def covered_superseded_alias(season, seasons, catalogs):
    """Fold only a parked duplicate tied to a proven catalog alias."""
    if "#superseded:" not in str(season.get("canonical_url", "")):
        return False
    cid = str(season["competition_id"])
    canonical = (catalogs.get(cid) or {}).get("aliases", {}).get(season["season_id"])
    if canonical is None or canonical == season["season_id"]:
        return False
    return any(
        str(row["competition_id"]) == cid
        and row["season_id"] == canonical
        and row.get("present")
        and row.get("lifecycle_state") == "present"
        and "#superseded:" not in str(row.get("canonical_url", ""))
        for row in seasons
    )


def campaign_rows(competitions, seasons, catalogs=None):
    """Include all adults, even inactive tournaments and unavailable editions."""
    catalogs = catalogs or {}
    adults = {
        str(row["competition_id"]): row
        for row in competitions
        if adult_competition(row)
    }
    rows = []
    present = set()
    for season in seasons:
        cid = str(season["competition_id"])
        token = season_token(season["season_id"])
        if (
            cid not in adults
            or token is None
            or covered_superseded_alias(season, seasons, catalogs)
        ):
            continue
        year = int(token[:4])
        if year > FIRST_YEAR:
            continue
        present.add((cid, year))
        unavailable = (
            not season.get("present")
            or season.get("lifecycle_state") != "present"
            or "#superseded:" in str(season.get("canonical_url", ""))
        )
        state = (
            "missing"
            if unavailable
            else "current_owned"
            if season.get("is_current")
            and not (
                (adults[cid].get("metadata") or {}).get("current_scope_lifecycle")
                == "discontinued"
                and (adults[cid].get("metadata") or {}).get("current_scope_reason")
            )
            else "pending"
        )
        rows.append(
            {
                **season,
                "year": year,
                "state": state,
                "direct_match_only": (season.get("metadata") or {}).get(
                    "direct_match_only"
                )
                is True,
            }
        )
    for cid in adults:
        for year in range(FIRST_YEAR, LAST_PRIORITY_YEAR - 1, -1):
            if (cid, year) not in present:
                metadata = adults[cid].get("metadata") or {}
                first = season_token(metadata.get("first_season"))
                last = season_token(metadata.get("last_season"))
                outside_source_range = (
                    first is not None and year < int(first[:4])
                ) or (
                    last is not None
                    and year > int(last[:4])
                    and metadata.get("current_scope_lifecycle") == "discontinued"
                )
                catalog = catalogs.get(cid)
                catalog_years = {
                    int(token[:4])
                    for edition in (catalog or {}).get("editions", [])
                    if (token := season_token(edition)) is not None
                }
                catalog_absence = catalog is not None and year not in catalog_years
                rows.append(
                    {
                        "competition_id": cid,
                        "season_id": f"missing:{year}",
                        "canonical_url": None,
                        "year": year,
                        "state": "unavailable"
                        if outside_source_range or catalog_absence
                        else "missing",
                        "catalog_snapshot_id": None
                        if catalog is None
                        else catalog.get("snapshot_id"),
                    }
                )
    return sorted(
        rows,
        key=lambda row: (-row["year"], str(row["competition_id"]), row["season_id"]),
    )


def progress_by_year(rows):
    result = {}
    for row in rows:
        counts = result.setdefault(str(row["year"]), Counter())
        counts[row["state"]] += 1
    return {year: dict(counts) for year, counts in sorted(result.items(), reverse=True)}


class HistoryCampaign:
    def __init__(self, control):
        self.control = control

    def reconcile(self):
        """DB checkpoint derives completion from all canonical/provenance scope."""
        with self.control._transaction() as cursor:
            # Same writer fence as source lock acquisition. No external I/O.
            self.control._lock_publication_writer_fence(cursor, "fbref")
            cursor.execute(
                "SELECT * FROM fbref_control.competition_registry WHERE source='fbref'"
            )
            competitions = _fetchall(cursor)
            cursor.execute(
                "SELECT * FROM fbref_control.season_registry WHERE source='fbref'"
            )
            seasons = _fetchall(cursor)
            cursor.execute(
                """
                SELECT DISTINCT ON (snapshot.metadata->>'competition_id')
                    snapshot.metadata->>'competition_id' AS competition_id,
                    snapshot.snapshot_id,
                    (SELECT coalesce(jsonb_object_agg(alias.alias,alias.season_id),'{}'::jsonb)
                     FROM fbref_control.season_alias alias
                     WHERE alias.source=snapshot.source
                       AND alias.competition_id=snapshot.metadata->>'competition_id'
                       AND alias.last_snapshot_id=snapshot.snapshot_id
                       AND alias.alias_kind IN ('source','label','url')
                       AND EXISTS (SELECT 1 FROM fbref_control.snapshot_season target
                         WHERE target.snapshot_id=snapshot.snapshot_id
                           AND target.competition_id=alias.competition_id
                           AND target.season_id=alias.season_id)) AS aliases,
                    array(SELECT edition.season_id FROM fbref_control.snapshot_season edition
                          WHERE edition.snapshot_id=snapshot.snapshot_id) AS editions
                FROM fbref_control.registry_snapshot snapshot
                JOIN fbref_control.fetch_attempt attempt
                  ON attempt.attempt_id::text=snapshot.metadata->'history_raw'->>'attempt_id'
                 AND attempt.content_hash=snapshot.content_hash
                 AND attempt.raw_manifest_key=snapshot.metadata->'history_raw'->>'manifest_key'
                 AND attempt.target_id=snapshot.metadata->'history_raw'->>'target_id'
                JOIN fbref_control.observation_processing observed
                  ON observed.logical_refresh_id=attempt.logical_refresh_id
                JOIN fbref_control.competition_registry competition
                  ON competition.source=snapshot.source
                 AND competition.competition_id=snapshot.metadata->>'competition_id'
                WHERE snapshot.source='fbref' AND snapshot.successful
                  AND snapshot.metadata->>'page_kind'='competition'
                  AND observed.parser_version=%s AND observed.typed_parser_version=%s
                  AND observed.stateful_parser_version=%s
                  AND observed.status='succeeded' AND observed.generic_status='succeeded'
                  AND observed.typed_status IN ('succeeded','skipped')
                  AND observed.stateful_status='succeeded' AND observed.validation_status='succeeded'
                  AND EXISTS (SELECT 1 FROM fbref_control.snapshot_season latest
                    WHERE latest.snapshot_id=snapshot.snapshot_id
                      AND latest.season_id=coalesce(competition.metadata->>'advertised_current_season_id',
                                                   competition.metadata->>'last_season'))
                ORDER BY snapshot.metadata->>'competition_id',snapshot.fetched_at DESC
            """,
                (
                    PAGE_DOCUMENT_VERSION,
                    TYPED_BRONZE_PARSER_VERSION,
                    DISCOVERY_PARSER_VERSION,
                ),
            )
            catalogs = {str(row["competition_id"]): row for row in _fetchall(cursor)}
            rows = campaign_rows(competitions, seasons, catalogs)
            for season in seasons:
                if covered_superseded_alias(season, seasons, catalogs):
                    cursor.execute(
                        "DELETE FROM fbref_control.history_campaign_season "
                        "WHERE campaign_id=%s AND competition_id=%s AND season_id=%s",
                        (
                            CAMPAIGN_ID,
                            str(season["competition_id"]),
                            season["season_id"],
                        ),
                    )
            for row in rows:
                cursor.execute(
                    """
                    INSERT INTO fbref_control.history_campaign_season
                      (campaign_id,competition_id,season_id,year,canonical_url,state,catalog_snapshot_id,direct_match_only)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (campaign_id,competition_id,season_id) DO UPDATE
                    SET canonical_url=excluded.canonical_url,
                        catalog_snapshot_id=excluded.catalog_snapshot_id,
                        direct_match_only=excluded.direct_match_only,
                        state=CASE WHEN excluded.state IN ('current_owned','unavailable','missing')
                                   THEN excluded.state
                                   WHEN history_campaign_season.state IN ('current_owned','unavailable','missing')
                                   THEN 'pending' ELSE history_campaign_season.state END,
                        updated_at=clock_timestamp()
                """,
                    (
                        CAMPAIGN_ID,
                        str(row["competition_id"]),
                        row["season_id"],
                        row["year"],
                        row.get("canonical_url"),
                        row["state"],
                        row.get("catalog_snapshot_id"),
                        bool(row.get("direct_match_only")),
                    ),
                )
            # Missing placeholders stop being debt once that edition is discovered.
            cursor.execute(
                """
                DELETE FROM fbref_control.history_campaign_season AS missing
                WHERE missing.campaign_id=%s AND missing.season_id LIKE 'missing:%%'
                  AND EXISTS (SELECT 1 FROM fbref_control.history_campaign_season actual
                    WHERE actual.campaign_id=missing.campaign_id
                      AND actual.competition_id=missing.competition_id AND actual.year=missing.year
                      AND actual.season_id NOT LIKE 'missing:%%')
            """,
                (CAMPAIGN_ID,),
            )
            cursor.execute(
                _FRONTIER_SCOPE_CTE
                + """
                , health AS (
                  SELECT campaign.competition_id,campaign.season_id,
                    count(DISTINCT frontier.target_id) AS discovered,
                    count(DISTINCT frontier.target_id) FILTER (WHERE
                      frontier.state <> 'fetched' OR frontier.last_content_hash IS NULL
                      OR NOT EXISTS (
                        SELECT 1 FROM fbref_control.fetch_attempt attempt
                        JOIN fbref_control.observation_processing observed
                          ON observed.logical_refresh_id=attempt.logical_refresh_id
                        WHERE attempt.target_id=frontier.target_id
                          AND attempt.content_hash=frontier.last_content_hash
                          AND attempt.status='succeeded'
                          AND observed.parser_version=%s AND observed.typed_parser_version=%s
                          AND observed.stateful_parser_version=%s
                          AND observed.status='succeeded' AND observed.generic_status='succeeded'
                          AND observed.typed_status IN ('succeeded','skipped')
                          AND observed.stateful_status IN ('succeeded','skipped')
                          AND observed.validation_status='succeeded'
                      )) AS remaining,
                    bool_or(frontier.page_kind='season' OR (campaign.direct_match_only AND frontier.page_kind='match')) AS root_seen
                  FROM fbref_control.history_campaign_season campaign
                  LEFT JOIN canonical_scope scope ON scope.competition_id=campaign.competition_id
                    AND scope.season_id=campaign.season_id AND scope.source='fbref'
                  LEFT JOIN fbref_control.page_frontier frontier ON frontier.target_id=scope.target_id
                    AND frontier.page_kind=ANY(%s)
                    AND strpos(frontier.canonical_url,'#superseded:')=0
                  WHERE campaign.campaign_id=%s AND campaign.state NOT IN ('current_owned','unavailable','missing')
                  GROUP BY campaign.competition_id,campaign.season_id
                )
                UPDATE fbref_control.history_campaign_season campaign
                SET state=CASE WHEN health.discovered>0 AND health.root_seen AND health.remaining=0
                               THEN 'closed' WHEN health.discovered>0 THEN 'in_progress' ELSE 'pending' END,
                    discovered=health.discovered,remaining=health.remaining,
                    updated_at=clock_timestamp()
                FROM health WHERE campaign.campaign_id=%s
                  AND campaign.competition_id=health.competition_id AND campaign.season_id=health.season_id
            """,
                (
                    PAGE_DOCUMENT_VERSION,
                    TYPED_BRONZE_PARSER_VERSION,
                    DISCOVERY_PARSER_VERSION,
                    list(HISTORY_PAGE_KINDS),
                    CAMPAIGN_ID,
                    CAMPAIGN_ID,
                ),
            )
        return self.summary()

    def summary(self):
        with self.control._transaction() as cursor:
            cursor.execute(
                "SELECT year,state,count(*) AS count FROM fbref_control.history_campaign_season "
                "WHERE campaign_id=%s GROUP BY year,state ORDER BY year DESC,state",
                (CAMPAIGN_ID,),
            )
            years = {}
            for row in _fetchall(cursor):
                years.setdefault(str(row["year"]), {})[row["state"]] = int(row["count"])
            return {"campaign_id": CAMPAIGN_ID, "years": years}

    def select(self, run_id):
        """Pin one season for this logical run; retries retain membership."""
        with self.control._transaction() as cursor:
            cursor.execute(
                "SELECT metadata FROM fbref_control.crawl_run WHERE run_id=%s FOR UPDATE",
                (run_id,),
            )
            run = _fetchone(cursor)
            pinned = run["metadata"].get("history_seasons")
            if pinned:
                return pinned
            cursor.execute(
                """
                SELECT competition_id,season_id,canonical_url,direct_match_only FROM fbref_control.history_campaign_season
                WHERE campaign_id=%s AND state IN ('pending','in_progress')
                  AND canonical_url IS NOT NULL
                  AND year=(SELECT max(year) FROM fbref_control.history_campaign_season barrier
                    WHERE barrier.campaign_id=%s AND barrier.state IN ('pending','in_progress','missing'))
                ORDER BY year DESC,competition_id,season_id LIMIT 1
            """,
                (CAMPAIGN_ID, CAMPAIGN_ID),
            )
            selected = _fetchall(cursor)
            from psycopg2.extras import Json

            cursor.execute(
                "UPDATE fbref_control.crawl_run SET metadata=metadata || %s::jsonb WHERE run_id=%s",
                (
                    Json(
                        {"history_campaign": CAMPAIGN_ID, "history_seasons": selected}
                    ),
                    run_id,
                ),
            )
            return selected

    def page_seconds(self):
        with self.control._transaction() as cursor:
            cursor.execute("""SELECT max(ceil((fetch_ms+parse_ms)::numeric/pages)) AS seconds
                FROM (SELECT * FROM fbref_control.history_timing ORDER BY observed_at DESC LIMIT 100) recent
                WHERE pages>0""")
            row = _fetchone(cursor)
            if row["seconds"] is not None:
                return max(
                    HISTORY_PAGE_SECONDS_FLOOR, math.ceil(float(row["seconds"]) / 1000)
                )
            # Initial admission uses actual committed fetch-through-parse time,
            # rather than the old page throttle projection or an invented speed.
            cursor.execute(
                """SELECT max(seconds) AS seconds FROM (
                SELECT extract(epoch FROM (observed.completed_at-attempt.started_at)) AS seconds
                FROM fbref_control.observation_processing observed
                JOIN fbref_control.fetch_attempt attempt USING(logical_refresh_id)
                WHERE observed.status='succeeded' AND observed.generic_status='succeeded'
                  AND observed.typed_status IN ('succeeded','skipped')
                  AND observed.stateful_status IN ('succeeded','skipped')
                  AND observed.validation_status='succeeded'
                  AND observed.parser_version=%s AND observed.typed_parser_version=%s
                  AND observed.stateful_parser_version=%s
                  AND observed.completed_at>attempt.started_at
                ORDER BY observed.completed_at DESC LIMIT 100
            ) timings""",
                (
                    PAGE_DOCUMENT_VERSION,
                    TYPED_BRONZE_PARSER_VERSION,
                    DISCOVERY_PARSER_VERSION,
                ),
            )
            row = _fetchone(cursor)
            if row["seconds"] is None:
                raise RuntimeError(
                    "No successful measured FBref fetch+parse duration; history stays closed"
                )
            return max(HISTORY_PAGE_SECONDS_FLOOR, math.ceil(float(row["seconds"])))

    def record_timing(self, run_id, result):
        fetch, parse = result.get("fetch", {}), result.get("parse", {})
        pages = max(int(fetch.get("claimed", 0)), int(parse.get("cohort_size", 0)))
        if not pages:
            return
        if int(fetch.get("wall_ms", 0)) <= 0 or int(parse.get("wall_ms", 0)) <= 0:
            raise ValueError("History timing needs measured fetch and parse durations")
        with self.control._transaction() as cursor:
            cursor.execute(
                """INSERT INTO fbref_control.history_timing(run_id,pages,fetch_ms,parse_ms)
                VALUES (%s,%s,%s,%s) ON CONFLICT(run_id) DO NOTHING""",
                (
                    run_id,
                    pages,
                    int(fetch.get("wall_ms", 0)),
                    int(parse.get("wall_ms", 0)),
                ),
            )
