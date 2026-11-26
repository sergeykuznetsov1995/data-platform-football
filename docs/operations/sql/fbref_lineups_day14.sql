-- Filled offline by scripts/report_fbref_lineup_coverage.py from archived cutoff evidence.
WITH input AS (
    SELECT json_parse('__COHORT_JSON__') AS packet
), fixtures AS (
    SELECT json_extract_scalar(item, '$[0]') AS competition_id, json_extract_scalar(item, '$[1]') AS season_id,
        json_extract_scalar(item, '$[2]') AS match_id, json_extract_scalar(item, '$[3]') AS fixture_key
    FROM input CROSS JOIN UNNEST(CAST(json_extract(packet, '$.fixtures') AS array(json))) AS f(item)
), editions AS (
    SELECT json_extract_scalar(item, '$[0]') AS competition_id, json_extract_scalar(item, '$[1]') AS season_id
    FROM input CROSS JOIN UNNEST(CAST(json_extract(packet, '$.pairs') AS array(json))) AS e(item)
), undated AS (
    -- The canonical windowed schedule read excludes rows whose source date cannot be parsed.
    SELECT count(*) AS undated_scored_rows
    FROM iceberg.bronze.fbref_schedule FOR TIMESTAMP AS OF TIMESTAMP '__CUTOFF__' s
    JOIN editions e ON s.source_competition_id = e.competition_id AND s.source_season_id = e.season_id
    WHERE try_cast(s.date AS date) IS NULL AND regexp_like(coalesce(s.score, ''), '[0-9]+\s*[–—-]\s*[0-9]+')
      AND NOT regexp_like(lower(coalesce(s.notes, '')), 'postpon|cancel|abandon|award|walkover')
), evidence AS (
    SELECT f.match_id, count(*) AS reports,
        count_if(a.source_competition_id = f.competition_id AND a.source_season_id = f.season_id
            AND a.availability IN ('empty', 'restricted', 'not_applicable')) AS empty_reports,
        max(IF(a.source_competition_id = f.competition_id AND a.source_season_id = f.season_id
            AND a.availability = 'available', a._batch_id, NULL)) AS available_batch
    FROM fixtures f JOIN iceberg.bronze.fbref_dataset_availability FOR TIMESTAMP AS OF TIMESTAMP '__CUTOFF__' a
      ON a.match_id = f.match_id AND a.dataset = 'lineups'
    GROUP BY f.match_id
), physical AS (
    SELECT f.match_id, count(*) AS physical_rows,
        count_if(p.source_competition_id = f.competition_id AND p.source_season_id = f.season_id) AS scoped_rows,
        count_if(p.source_competition_id = f.competition_id AND p.source_season_id = f.season_id
            AND p._batch_id = e.available_batch) AS matched_rows
    FROM fixtures f JOIN iceberg.bronze.fbref_lineups FOR TIMESTAMP AS OF TIMESTAMP '__CUTOFF__' p
      ON p.match_id = f.match_id
    LEFT JOIN evidence e ON e.match_id = f.match_id
    GROUP BY f.match_id
), checked AS (
    SELECT f.fixture_key, f.match_id, coalesce(p.scoped_rows, 0) AS lineup_rows,
        coalesce(e.reports = 1 AND (
            (e.empty_reports = 1 AND coalesce(p.physical_rows, 0) = 0)
            OR (e.available_batch IS NOT NULL AND p.matched_rows > 0 AND p.matched_rows = p.physical_rows)
        ), false) AS verified
    FROM fixtures f
    LEFT JOIN physical p ON p.match_id = f.match_id
    LEFT JOIN evidence e ON e.match_id = f.match_id
), totals AS (
    SELECT count(*) AS played, count(*) FILTER (WHERE lineup_rows = 0) AS empty_lineups,
        coalesce(array_agg(fixture_key ORDER BY fixture_key) FILTER (WHERE lineup_rows = 0), ARRAY[]) AS empty_fixtures,
        count(*) FILTER (WHERE NOT verified) AS unverified,
        coalesce(array_agg(fixture_key ORDER BY fixture_key) FILTER (WHERE NOT verified), ARRAY[]) AS unverified_fixtures,
        count(*) FILTER (WHERE match_id IS NULL) AS unreported
    FROM checked
), scope AS (
    SELECT CAST(json_extract(packet, '$.issues') AS array(varchar)) AS issues FROM input
)
SELECT '__DEPLOYMENT_SHA__' AS deployed_revision, '__DEPLOYED_AT__' AS deployed_at,
    TIMESTAMP '__CUTOFF__' AS cutoff, played, empty_lineups, 100e0 * empty_lineups / nullif(played, 0) AS empty_pct,
    unreported, unverified, undated_scored_rows, issues, empty_fixtures, unverified_fixtures,
    CASE WHEN current_timestamp < TIMESTAMP '__CUTOFF__' OR played = 0 OR unverified > 0
              OR unreported > 0 OR undated_scored_rows > 0 OR cardinality(issues) > 0 THEN 'PRELIMINARY'
         WHEN empty_lineups * 100 <= played THEN 'PASS' ELSE 'FAIL' END AS verdict
FROM totals CROSS JOIN scope CROSS JOIN undated;
