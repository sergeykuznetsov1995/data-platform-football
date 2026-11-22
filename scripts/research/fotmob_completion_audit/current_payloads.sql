WITH physical AS (
    SELECT competition_id, source_season_key, match_id,
           _parser_version AS parser_version, _observed_at, _ingested_at, _target_batch_id,
           available_sections_json AS available_sections,
           match_facts_json, stats_json, player_stats_json, lineup_json, shotmap_json, momentum_json,
           TRY(json_parse(payload_json)) AS raw_document,
           payload_json IS NOT NULL AND
               TRY(CAST(json_parse(payload_json) AS map(varchar, json))) IS NOT NULL AS payload_json_valid
    FROM iceberg.bronze.fotmob_match_payloads FOR VERSION AS OF {payloads_snapshot}
    WHERE {manifest_scope_predicate} AND {selected_batch_predicate} AND {match_id_predicate}
), rooted AS (
    SELECT *, CASE
        WHEN TRY(CAST(json_extract(raw_document, '$.content') AS map(varchar, json))) IS NOT NULL
        THEN json_extract(raw_document, '$.content') ELSE raw_document END AS raw_root
    FROM physical
)
SELECT competition_id, source_season_key, match_id, parser_version,
       _observed_at, _ingested_at, _target_batch_id, available_sections, payload_json_valid,
       CASE WHEN match_facts_json IS NOT NULL AND TRY(json_parse(match_facts_json)) IS NULL THEN CAST(NULL AS boolean)
            ELSE CASE WHEN TRY(json_parse(match_facts_json)) IS NULL THEN false
            WHEN json_format(TRY(json_parse(match_facts_json))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END END AS "typed_matchFacts",
       CASE WHEN stats_json IS NOT NULL AND TRY(json_parse(stats_json)) IS NULL THEN CAST(NULL AS boolean)
            ELSE CASE WHEN TRY(json_parse(stats_json)) IS NULL THEN false
            WHEN json_format(TRY(json_parse(stats_json))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END END AS "typed_stats",
       CASE WHEN player_stats_json IS NOT NULL AND TRY(json_parse(player_stats_json)) IS NULL THEN CAST(NULL AS boolean)
            ELSE CASE WHEN TRY(json_parse(player_stats_json)) IS NULL THEN false
            WHEN json_format(TRY(json_parse(player_stats_json))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END END AS "typed_playerStats",
       CASE WHEN lineup_json IS NOT NULL AND TRY(json_parse(lineup_json)) IS NULL THEN CAST(NULL AS boolean)
            ELSE CASE WHEN TRY(json_parse(lineup_json)) IS NULL THEN false
            WHEN json_format(TRY(json_parse(lineup_json))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END END AS "typed_lineup",
       CASE WHEN shotmap_json IS NOT NULL AND TRY(json_parse(shotmap_json)) IS NULL THEN CAST(NULL AS boolean)
            ELSE CASE WHEN TRY(json_parse(shotmap_json)) IS NULL THEN false
            WHEN json_format(TRY(json_parse(shotmap_json))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END END AS "typed_shotmap",
       CASE WHEN momentum_json IS NULL THEN false
            WHEN lower(trim(CAST(momentum_json AS VARCHAR))) = 'false' THEN false
            WHEN TRY(json_parse(CAST(momentum_json AS VARCHAR))) IS NULL THEN CAST(NULL AS boolean)
            WHEN json_format(TRY(json_parse(CAST(momentum_json AS VARCHAR)))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END AS "typed_momentum",
       CASE WHEN TRY(json_extract(raw_root, '$.matchFacts')) IS NULL THEN false
            WHEN json_format(TRY(json_extract(raw_root, '$.matchFacts'))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END AS "raw_matchFacts",
       CASE WHEN TRY(json_extract(raw_root, '$.stats')) IS NULL THEN false
            WHEN json_format(TRY(json_extract(raw_root, '$.stats'))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END AS "raw_stats",
       CASE WHEN TRY(json_extract(raw_root, '$.playerStats')) IS NULL THEN false
            WHEN json_format(TRY(json_extract(raw_root, '$.playerStats'))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END AS "raw_playerStats",
       CASE WHEN TRY(json_extract(raw_root, '$.lineup')) IS NULL THEN false
            WHEN json_format(TRY(json_extract(raw_root, '$.lineup'))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END AS "raw_lineup",
       CASE WHEN TRY(json_extract(raw_root, '$.shotmap')) IS NULL THEN false
            WHEN json_format(TRY(json_extract(raw_root, '$.shotmap'))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END AS "raw_shotmap",
       CASE WHEN TRY(json_extract(raw_root, '$.momentum')) IS NULL THEN false
            WHEN json_format(TRY(json_extract(raw_root, '$.momentum'))) IN ('null', 'false', '{{}}', '[]') THEN false ELSE true END AS "raw_momentum"
FROM rooted
ORDER BY competition_id, source_season_key, match_id, _target_batch_id, _observed_at, _ingested_at
