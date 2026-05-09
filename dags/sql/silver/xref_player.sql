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
--   1. Row count > 0
--   2. canonical_id is non-null and matches the format ^(fb|us|ws|ss)_.+
--   3. Per-source rejection_pct ≤ 25%   (R2 verdict target)
--   4. ≥8/10 known APL 2024-25 pairs resolve to a single canonical_id
--      across all 3 sources
--   5. confidence ∈ {exact, name_team, orphan}  (jersey/dob STUBS not yet
--      populated; expand allow-list when E1.5 lights them up)
--   6. (canonical_id, source, source_id) is unique
-- =============================================================================

-- 1) Row count + per-source breakdown ------------------------------------------
SELECT
    'row_count_per_source' AS check_name,
    source,
    COUNT(*) AS rows_total,
    COUNT_IF(confidence = 'exact')      AS exact_rows,
    COUNT_IF(confidence = 'name_team')  AS name_team_rows,
    COUNT_IF(confidence = 'orphan')     AS orphan_rows,
    CAST(ROUND(100.0 * COUNT_IF(confidence='orphan') / COUNT(*), 2) AS double)
        AS rejection_pct
FROM iceberg.silver.xref_player
GROUP BY source
ORDER BY source;

-- 2) canonical_id format guard -------------------------------------------------
SELECT
    'canonical_id_format' AS check_name,
    COUNT(*) AS bad_rows
FROM iceberg.silver.xref_player
WHERE canonical_id IS NULL
   OR NOT regexp_like(canonical_id, '^(fb|us|ws|ss)_.+$');

-- 3) Confidence allow-list -----------------------------------------------------
SELECT
    'confidence_values' AS check_name,
    confidence,
    COUNT(*) AS rows
FROM iceberg.silver.xref_player
GROUP BY confidence
ORDER BY confidence;

-- 4) PK uniqueness (canonical_id, source, source_id) ---------------------------
SELECT
    'pk_uniqueness' AS check_name,
    COUNT(*) AS duplicate_keys
FROM (
    SELECT canonical_id, source, source_id, COUNT(*) AS n
    FROM iceberg.silver.xref_player
    GROUP BY canonical_id, source, source_id
    HAVING COUNT(*) > 1
);

-- 5) Known-pair pass rate (target ≥8/10) ---------------------------------------
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
    GROUP BY canonical_id
    HAVING COUNT(DISTINCT source) >= 3
)
SELECT
    'known_pairs_pass_rate' AS check_name,
    (SELECT COUNT(*) FROM resolved) AS passed,
    (SELECT COUNT(*) FROM expected) AS total;

-- 6) Orphan canonical_id prefix matches source --------------------------------
SELECT
    'orphan_prefix_matches_source' AS check_name,
    source,
    COUNT(*) AS bad_rows
FROM iceberg.silver.xref_player
WHERE confidence = 'orphan'
  AND (
       (source = 'understat'  AND NOT canonical_id LIKE 'us\_%' ESCAPE '\')
    OR (source = 'whoscored'  AND NOT canonical_id LIKE 'ws\_%' ESCAPE '\')
    OR (source = 'sofascore'  AND NOT canonical_id LIKE 'ss\_%' ESCAPE '\')
  )
GROUP BY source
ORDER BY source;
