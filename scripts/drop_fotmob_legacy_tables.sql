-- Drop the 9 frozen legacy FotMob bronze tables (#1423).
--
-- Context
--   The legacy FotMob scraper contour stopped writing on 2026-07-17/21 (#930
--   cutover). Silver reads only the native *_current views since the
--   #2016-#2020 series; the last textual legacy references and the
--   #930 rollback plan/validate commands were retired in the #1423 PR that
--   carries this script. Row counts verified read-only on 2026-11-21:
--     fotmob_match_details     17 385   (last write 2026-07-17)
--     fotmob_player_details    31 717   (last write 2026-07-21; 195 unique
--                                         coach cards exported first, see below)
--     fotmob_player_stats     409 138   (last write 2026-07-21; WC-2026 deep
--                                         leaderboard tails exported first)
--     fotmob_schedule          17 218   (last write 2026-07-21)
--     fotmob_team_leaderboards 24 799   (last write 2026-07-21; WC-2026 part
--                                         exported first)
--     fotmob_team_profile       1 018   (last write 2026-07-21)
--     fotmob_team_squad        32 494   (last write 2026-07-21)
--     fotmob_team_stats         1 030   (last write 2026-07-21)
--     fotmob_transfers          2 500   (last write 2026-07-21)
--   The 5 fotmob `__stg_` leftovers listed in #1423 were already removed by
--   the staging cleanup on 2026-11-21; nothing `fotmob%__stg_%` remains.
--
--   Unique data preserved before this drop (owner decision 2026-11-21):
--   /root/archive/1423-fotmob-legacy-drain-20261121/ — 923 coach cards
--   (fotmob_player_details WHERE is_coach), 13 888 + 1 216 WC-2026 leaderboard
--   rows (fotmob_player_stats / fotmob_team_leaderboards WHERE
--   league='INT-World Cup' AND season=2026), CSV + SHA256SUMS + README.
--
-- Execution (manual; not auto-applied)
--   1. Execute ONLY after the #1423 PR is merged AND the owner explicitly
--      authorizes the production drop session. Do not run from a dev session.
--   2. HARD GATE — archive integrity: verify the pre-drop export before
--      touching anything (the coach cards and WC-2026 tails are unrecoverable):
--        cd /root/archive/1423-fotmob-legacy-drain-20261121 && sha256sum -c SHA256SUMS
--      Any mismatch => STOP, do not drop, investigate.
--   3. HARD GATE — metadata backup AND count match. In the same authorized
--      session, for each table below capture
--        SHOW CREATE TABLE iceberg.bronze.<table>;
--        SELECT count(*) FROM iceberg.bronze.<table>;
--        SELECT snapshot_id, committed_at FROM "iceberg"."bronze"."<table>$snapshots"
--        ORDER BY committed_at DESC LIMIT 1;
--      and store the outputs next to the #1423 evidence. Every count(*) MUST
--      equal the frozen 2026-11-21 count listed in the Context section above
--      (17385, 31717, 409138, 17218, 24799, 1018, 32494, 1030, 2500
--      respectively). ANY mismatch or failed capture => STOP, do not drop;
--      the tables changed since the baseline and the review is void.
--   4. Confirm no FotMob ingest/refresh wave is running (fotmob writers never
--      touch these tables, but keep the session quiet) and that
--      dag_transform_fotmob_silver_v2 is not mid-run.
--   5. Run statement by statement (make shell-trino or the trino CLI), checking
--      each succeeds before the next.
--   6. After the last DROP refresh the catalog so OpenMetadata shows no ghosts:
--        make om-ingest-trino
--
-- WARNING
--   DROP TABLE removes both the Lakekeeper metastore entry and the Iceberg
--   data files in object storage. There is no undo: rolling back this script
--   or the repo does NOT restore the tables (the writer contour is gone).
--   The archive export above is the only copy of the coach/WC-2026 tails.

DROP TABLE iceberg.bronze.fotmob_match_details;
DROP TABLE iceberg.bronze.fotmob_player_details;
DROP TABLE iceberg.bronze.fotmob_player_stats;
DROP TABLE iceberg.bronze.fotmob_schedule;
DROP TABLE iceberg.bronze.fotmob_team_leaderboards;
DROP TABLE iceberg.bronze.fotmob_team_profile;
DROP TABLE iceberg.bronze.fotmob_team_squad;
DROP TABLE iceberg.bronze.fotmob_team_stats;
DROP TABLE iceberg.bronze.fotmob_transfers;

-- Post-state verification (#1423 acceptance; read-only):
--   SELECT table_name FROM iceberg.information_schema.tables
--   WHERE table_schema = 'bronze' AND table_name LIKE 'fotmob%'
--   AND (table_name LIKE '%\_\_stg\_%' ESCAPE '\'
--        OR table_name IN ('fotmob_match_details','fotmob_player_details',
--           'fotmob_player_stats','fotmob_schedule','fotmob_team_leaderboards',
--           'fotmob_team_profile','fotmob_team_squad','fotmob_team_stats',
--           'fotmob_transfers'));
--   -> expected: empty result. Then confirm the next dag_ingest_fotmob wave
--   and the dag_transform_fotmob_silver_v2 run are both green.
