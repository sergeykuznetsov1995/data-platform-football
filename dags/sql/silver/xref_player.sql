-- =============================================================================
-- Silver: xref_player — Trino-side sanity / validation SELECT
-- =============================================================================
--
-- This file is a READ-ONLY validation query, NOT a CTAS.
-- Materialisation of `iceberg.silver.xref_player` is performed by the Python
-- resolver in `dags/utils/xref_player_resolver.py` (rapidfuzz / unidecode are
-- not expressible in pure Trino SQL — see docs/research/R2_player_resolver.md).
--
-- Use this file as a hand-run sanity probe after the resolver materialises:
--
--     trino> \i dags/sql/silver/xref_player.sql
--
-- Asserts validated by T5 integration test (data_quality.py wraps each as a
-- separate check — see dags/utils/data_quality.py for severity mapping):
--
--   1. Row count > 0 (per-source × per-confidence breakdown)
--   2. canonical_id is non-null and matches the 9-source prefix format
--      ^(fb|us|ws|fm|ss|tm|cap|sf|es)_.+
--   3. Per-source rejection_pct ≤ 25%   (R2 verdict target)
--   4. ≥8/10 known APL 2024-25 pairs resolve to a single canonical_id
--      across all 3 core sources (fbref/understat/whoscored); the extended
--      fbref+sofascore+fotmob gate is WARNING-only (resolver summary)
--   5. confidence ∈ {exact, name_team, name_team_alias, name_team_surname,
--      name_team_subset, name_team_nickname, name_team_dob, orphan}
--      (name_team_jersey reserved STUB; 'ambiguous' must NEVER appear here —
--      those rows live in silver.xref_player_review)
--   6. (canonical_id, source, source_id) is unique
-- =============================================================================

-- 1) Row count + per-source × per-confidence breakdown --------------------------
SELECT
    'row_count_per_source_confidence' AS check_name,
    source,
    confidence,
    COUNT(*) AS rows_total
FROM iceberg.silver.xref_player
GROUP BY source, confidence
ORDER BY source, confidence;

-- 1b) Per-source rejection_pct (orphan share) -----------------------------------
SELECT
    'rejection_pct_per_source' AS check_name,
    source,
    COUNT(*) AS rows_total,
    COUNT_IF(confidence = 'orphan') AS orphan_rows,
    CAST(ROUND(100.0 * COUNT_IF(confidence='orphan') / COUNT(*), 2) AS double)
        AS rejection_pct
FROM iceberg.silver.xref_player
GROUP BY source
ORDER BY source;

-- 2) canonical_id format guard ---------------------------------------------------
-- 9-source prefix map: fb/us/ws/fm/ss/tm/cap/sf/es
-- (see xref_player_resolver._orphan_prefix; mirrored in xref_dq.py).
SELECT
    'canonical_id_format' AS check_name,
    COUNT(*) AS bad_rows
FROM iceberg.silver.xref_player
WHERE canonical_id IS NULL
   OR NOT regexp_like(canonical_id, '^(fb|us|ws|fm|ss|tm|cap|sf|es)_.+$');

-- 3) Confidence allow-list -------------------------------------------------------
-- Expected values: exact, name_team, name_team_alias, name_team_surname,
-- name_team_subset, name_team_nickname, name_team_dob, orphan.
SELECT
    'confidence_values' AS check_name,
    confidence,
    COUNT(*) AS rows
FROM iceberg.silver.xref_player
GROUP BY confidence
ORDER BY confidence;

-- 4) PK uniqueness (canonical_id, source, source_id) -----------------------------
SELECT
    'pk_uniqueness' AS check_name,
    COUNT(*) AS duplicate_keys
FROM (
    SELECT canonical_id, source, source_id, COUNT(*) AS n
    FROM iceberg.silver.xref_player
    GROUP BY canonical_id, source, source_id
    HAVING COUNT(*) > 1
);

-- 5) Known-pair pass rate (target ≥8/10, core sources) ---------------------------
WITH expected(name, expected_cid) AS (
    VALUES
        ('Bukayo Saka',     'fb_bc7dc64d'),
        ('Mohamed Salah',   'fb_e342ad68'),
        ('Erling Haaland',  'fb_1f44ac21'),
        ('Bruno Fernandes', 'fb_507c7bdf'),
        ('Rodri',           'fb_6434f10d'),
        ('Son Heung-min',   'fb_92e7e919'),
        ('Virgil van Dijk', 'fb_e06683ca'),
        ('Cole Palmer',     'fb_dc7f8a28'),
        ('Bruno Guimarães', 'fb_82518f62'),
        ('Joško Gvardiol',  'fb_5ad50391')
),
resolved AS (
    SELECT canonical_id, COUNT(DISTINCT source) AS n_sources
    FROM iceberg.silver.xref_player
    WHERE canonical_id IN (SELECT expected_cid FROM expected)
      AND source IN ('fbref', 'understat', 'whoscored')
    GROUP BY canonical_id
    HAVING COUNT(DISTINCT source) >= 3
)
SELECT
    'known_pairs_pass_rate' AS check_name,
    (SELECT COUNT(*) FROM resolved) AS passed,
    (SELECT COUNT(*) FROM expected) AS total;

-- 5b) Extended known-pair pass rate (fbref+sofascore+fotmob, WARNING-only) -------
WITH expected(name, expected_cid) AS (
    VALUES
        ('Bukayo Saka',     'fb_bc7dc64d'),
        ('Mohamed Salah',   'fb_e342ad68'),
        ('Erling Haaland',  'fb_1f44ac21'),
        ('Bruno Fernandes', 'fb_507c7bdf'),
        ('Rodri',           'fb_6434f10d'),
        ('Son Heung-min',   'fb_92e7e919'),
        ('Virgil van Dijk', 'fb_e06683ca'),
        ('Cole Palmer',     'fb_dc7f8a28'),
        ('Bruno Guimarães', 'fb_82518f62'),
        ('Joško Gvardiol',  'fb_5ad50391')
),
resolved AS (
    SELECT canonical_id, COUNT(DISTINCT source) AS n_sources
    FROM iceberg.silver.xref_player
    WHERE canonical_id IN (SELECT expected_cid FROM expected)
      AND source IN ('fbref', 'sofascore', 'fotmob')
    GROUP BY canonical_id
    HAVING COUNT(DISTINCT source) >= 3
)
SELECT
    'known_pairs_pass_rate_ext' AS check_name,
    (SELECT COUNT(*) FROM resolved) AS passed,
    (SELECT COUNT(*) FROM expected) AS total;

-- 6) Orphan canonical_id prefix matches source -----------------------------------
SELECT
    'orphan_prefix_matches_source' AS check_name,
    source,
    COUNT(*) AS bad_rows
FROM iceberg.silver.xref_player
WHERE confidence = 'orphan'
  AND (
       (source = 'understat'     AND NOT canonical_id LIKE 'us\_%'  ESCAPE '\')
    OR (source = 'whoscored'     AND NOT canonical_id LIKE 'ws\_%'  ESCAPE '\')
    OR (source = 'fotmob'        AND NOT canonical_id LIKE 'fm\_%'  ESCAPE '\')
    OR (source = 'sofascore'     AND NOT canonical_id LIKE 'ss\_%'  ESCAPE '\')
    OR (source = 'transfermarkt' AND NOT canonical_id LIKE 'tm\_%'  ESCAPE '\')
    OR (source = 'capology'      AND NOT canonical_id LIKE 'cap\_%' ESCAPE '\')
    OR (source = 'sofifa'        AND NOT canonical_id LIKE 'sf\_%'  ESCAPE '\')
    OR (source = 'espn'          AND NOT canonical_id LIKE 'es\_%'  ESCAPE '\')
  )
GROUP BY source
ORDER BY source;
