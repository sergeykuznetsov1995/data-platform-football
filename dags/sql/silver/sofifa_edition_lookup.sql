-- =============================================================================
-- Silver: sofifa_edition_lookup
-- =============================================================================
--
-- FIFA/EA FC edition → sofifa version_id catalogue (#601). Conform-only из
-- bronze.sofifa_versions — справочник всех релизов + rating-обновлений, полезен
-- чтобы резолвить edition-метку в sofifa version_id (downstream sofifa_player_*).
--
-- `update` — зарезервированное слово, в bronze это метка обновления рейтингов
-- ('Jun 10, 2026' и т.п.) — переименовано в update_label. Dedup по version_id
-- (replace_partitions(['fifa_edition']) может дать повторы между прогонами).
-- Беспартиционная (компактный справочник).
-- =============================================================================

WITH dedup AS (
    SELECT
        version_id,
        fifa_edition,
        "update",
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY version_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM iceberg.bronze.sofifa_versions
    WHERE version_id IS NOT NULL
)

SELECT
    TRY_CAST(version_id AS BIGINT)  AS version_id,
    fifa_edition,
    "update"                        AS update_label,
    _ingested_at                    AS _bronze_ingested_at
FROM dedup
WHERE rn = 1
