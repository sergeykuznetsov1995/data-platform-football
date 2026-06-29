-- =============================================================================
-- Drop orphan Silver tables (June 2026) — issue #422
-- =============================================================================
--
-- Context:
--   Поднимали OpenMetadata посмотреть описания Silver-слоя и нашли 8 таблиц без
--   описаний. У всех 8 НЕТ producer'а в текущем коде DAG-ов и НЕТ консьюмеров —
--   ни один gold/feat SQL/DAG/spark-job их не читает. Это мёртвые остатки.
--
--   Группа A (6) — без tracked producer'а в git, застыли 2026-05-04:
--     iceberg.silver.clubelo_team_ratings         — 160   rows
--     iceberg.silver.sofascore_league_standings   — 21    rows
--     iceberg.silver.sofascore_match_results       — 381   rows
--     iceberg.silver.understat_match_xg            — 1900  rows
--     iceberg.silver.understat_player_match_xg     — 11567 rows
--     iceberg.silver.understat_shot_events         — 39281 rows
--
--   Группа B (2) — свёрнуты в Gold коммитом 823dc62 (#382, 2026-06-09);
--   silver SQL удалён, cross-source сборка теперь инлайнится в gold:
--     iceberg.silver.match_cards                   — 13760 rows
--     iceberg.silver.match_substitutions           — 23178 rows
--
--   Understat xG НЕ теряется при дропе — он живёт в:
--     gold.fct_shot (читает bronze.understat_shots),
--     silver.understat_team_match,
--     silver.understat_player_match_aggregate.
--   Замороженные understat_*_xg — устаревшие промежуточные копии, восстанавливать
--   не нужно.
--
-- Execution (manual; not auto-applied):
--   docker compose exec trino bash -c '\
--     trino --server https://localhost:8443 --user airflow --password \
--           --insecure -f /opt/sql/drop_orphan_silver_tables.sql' \
--     <<< "$TRINO_PASSWORD"
--
--   Or run line by line via `make shell-trino`.
--   После — `make om-ingest-trino`, чтобы каталог не показывал призраков.
--
-- WARNING: DROP TABLE у Iceberg-managed таблиц сносит и метастор-запись, и данные
-- в HDFS. Undo нет без пересборки из Bronze.
-- =============================================================================

-- Группа A — без producer'а, застыли 2026-05-04
DROP TABLE IF EXISTS iceberg.silver.clubelo_team_ratings;
DROP TABLE IF EXISTS iceberg.silver.sofascore_league_standings;
DROP TABLE IF EXISTS iceberg.silver.sofascore_match_results;
DROP TABLE IF EXISTS iceberg.silver.understat_match_xg;
DROP TABLE IF EXISTS iceberg.silver.understat_player_match_xg;
DROP TABLE IF EXISTS iceberg.silver.understat_shot_events;

-- Группа B — свёрнуты в gold.fct_card / gold.fct_substitution (#382)
DROP TABLE IF EXISTS iceberg.silver.match_cards;
DROP TABLE IF EXISTS iceberg.silver.match_substitutions;
