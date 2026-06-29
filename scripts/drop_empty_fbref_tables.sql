-- =============================================================================
-- Drop empty FBref Bronze tables (Apr 2026)
-- =============================================================================
--
-- Context:
--   FBref restricted advanced stats. Tables exist but ALL stat cells are empty
--   (`<td class="iz"></td>`). Verified counts in iceberg.bronze:
--
--     fbref_player_passing        — 22617 rows / 0 non-empty
--     fbref_player_passing_types  — 22617 rows / 0 non-empty
--     fbref_player_gca            — 22617 rows / 0 non-empty
--     fbref_player_defense        — 22603 rows / 0 non-empty
--     fbref_player_possession     — 22617 rows / 0 non-empty
--     fbref_team_passing          — 820   rows / 0 non-empty
--     fbref_team_passing_types    — 820   rows / 0 non-empty
--     fbref_team_gca              — 820   rows / 0 non-empty
--     fbref_team_defense          — 820   rows / 0 non-empty
--     fbref_team_possession       — 800   rows / 0 non-empty
--
--   These stat_types were removed from PLAYER_STAT_TYPES / TEAM_STAT_TYPES /
--   PLAYER_MATCH_STAT_TYPES in scrapers/fbref/constants.py. The DAG no longer
--   creates these tasks.
--
-- Execution (manual; not auto-applied):
--   docker compose exec trino bash -c '\
--     trino --server https://localhost:8443 --user airflow --password \
--           --insecure -f /opt/sql/drop_empty_fbref_tables.sql' \
--     <<< "$TRINO_PASSWORD"
--
-- Or run line by line via `make shell-trino`.
--
-- WARNING: DROP TABLE removes both the metastore entry and HDFS data
-- (Iceberg-managed). There is no undo without rescraping.
-- =============================================================================

DROP TABLE IF EXISTS iceberg.bronze.fbref_player_passing;
DROP TABLE IF EXISTS iceberg.bronze.fbref_player_passing_types;
DROP TABLE IF EXISTS iceberg.bronze.fbref_player_gca;
DROP TABLE IF EXISTS iceberg.bronze.fbref_player_defense;
DROP TABLE IF EXISTS iceberg.bronze.fbref_player_possession;

DROP TABLE IF EXISTS iceberg.bronze.fbref_team_passing;
DROP TABLE IF EXISTS iceberg.bronze.fbref_team_passing_types;
DROP TABLE IF EXISTS iceberg.bronze.fbref_team_gca;
DROP TABLE IF EXISTS iceberg.bronze.fbref_team_defense;
DROP TABLE IF EXISTS iceberg.bronze.fbref_team_possession;
