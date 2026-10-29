-- ПОДГОТОВЛЕНО, НЕ ИСПОЛНЕНО. Только метабаза изолята FotMob, в паузе стека.
-- Владелец: psql -X -v ON_ERROR_STOP=1 -v fotmob_isolate_system_identifier=<ID>
--   -f remove_foreign_dags_1300.sql
-- ID заранее сверить с pg_control_system() именно контейнера метабазы изолята.
-- dag_bootstrap_fbref сначала явно поставить на паузу; массового UPDATE здесь нет.
-- Любые дочерние записи блокируют DELETE (включая ON DELETE CASCADE).
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
-- Нет переменной/прав чтения cluster ID — отказ до DELETE.
SELECT set_config('fotmob.expected_system_identifier', :'fotmob_isolate_system_identifier', true);
CREATE TEMP TABLE foreign_dags_1300 (dag_id text PRIMARY KEY) ON COMMIT DROP;
INSERT INTO foreign_dags_1300 VALUES
    ('dag_accept_fbref_bronze'),
    ('dag_backfill_fbref'),
    ('dag_backfill_sofascore_all_mens'),
    ('dag_backfill_transfermarkt'),
    ('dag_backfill_understat'),
    ('dag_backfill_whoscored'),
    ('dag_bootstrap_fbref'),
    ('dag_canary_sofascore_proxy'),
    ('dag_discover_transfermarkt_registry'),
    ('dag_e3_backfill'),
    ('dag_iceberg_maintenance'),
    ('dag_iceberg_maintenance_daily'),
    ('dag_ingest_capology'),
    ('dag_ingest_clubelo'),
    ('dag_ingest_espn'),
    ('dag_ingest_fbref'),
    ('dag_ingest_matchhistory'),
    ('dag_ingest_sofifa'),
    ('dag_ingest_transfermarkt'),
    ('dag_ingest_understat'),
    ('dag_ingest_whoscored'),
    ('dag_master_pipeline'),
    ('dag_proxy_traffic_report'),
    ('dag_refresh_sofascore_all_mens'),
    ('dag_replay_fbref'),
    ('dag_replay_fbref_bronze'),
    ('dag_sofascore_pipeline'),
    ('dag_transform_capology_silver'),
    ('dag_transform_e3'),
    ('dag_transform_e4'),
    ('dag_transform_espn_silver'),
    ('dag_transform_fbref_gold'),
    ('dag_transform_fbref_silver'),
    ('dag_transform_sofifa_silver'),
    ('dag_transform_transfermarkt_silver'),
    ('dag_transform_xref'),
    ('superset_alerts');
LOCK TABLE dag IN ACCESS EXCLUSIVE MODE;
DO $$
DECLARE
    dep record;
    found_rows bigint;
BEGIN
    IF (SELECT system_identifier::text FROM pg_control_system())
           <> current_setting('fotmob.expected_system_identifier')
       OR current_database() <> 'airflow'
       OR (SELECT count(*) FROM dag WHERE is_active AND dag_id IN (
           'dag_orchestrate_fotmob','dag_ingest_fotmob','dag_backfill_fotmob',
           'dag_refresh_fotmob','dag_collect_fotmob_players','dag_trigger_fotmob_daily',
           'dag_refresh_fotmob_c1')) < 6
       OR EXISTS (SELECT 1 FROM dag WHERE is_active AND dag_id NOT LIKE '%fotmob%')
    THEN RAISE EXCEPTION 'Not the FotMob isolate'; END IF;
    IF (SELECT count(*) FROM dag JOIN foreign_dags_1300 USING (dag_id)) <> 37
       OR EXISTS (SELECT 1 FROM dag JOIN foreign_dags_1300 USING (dag_id)
                  WHERE is_active OR NOT is_paused)
       OR EXISTS (SELECT 1 FROM dag WHERE NOT is_paused)
    THEN RAISE EXCEPTION 'Expected exactly 37 inactive foreign DAGs and all DAGs paused'; END IF;
    IF EXISTS (SELECT 1 FROM dag_run WHERE state IN ('running','queued'))
       OR EXISTS (SELECT 1 FROM task_instance WHERE state IN
           ('running','queued','scheduled','restarting','deferred','up_for_retry','up_for_reschedule'))
    THEN RAISE EXCEPTION 'Live work in isolate'; END IF;
    -- Проверяем и связи без FK. Блокировка защищает проверку от новых вставок.
    FOR dep IN
        SELECT c.table_schema, c.table_name FROM information_schema.columns c
        JOIN information_schema.tables t USING (table_schema, table_name)
        WHERE c.column_name='dag_id' AND c.table_schema='public'
          AND c.table_name <> 'dag' AND t.table_type='BASE TABLE'
        ORDER BY c.table_name
    LOOP
        EXECUTE format('LOCK TABLE %I.%I IN SHARE MODE', dep.table_schema, dep.table_name);
        EXECUTE format('SELECT count(*) FROM %I.%I d JOIN foreign_dags_1300 f USING (dag_id)',
                       dep.table_schema, dep.table_name) INTO found_rows;
        IF found_rows <> 0 THEN
            RAISE EXCEPTION 'Dependencies in %: %, no DELETE', dep.table_name, found_rows;
        END IF;
    END LOOP;
    DELETE FROM dag WHERE dag_id IN (SELECT dag_id FROM foreign_dags_1300);
    GET DIAGNOSTICS found_rows = ROW_COUNT;
    IF found_rows <> 37 THEN RAISE EXCEPTION 'DELETE count mismatch: %', found_rows; END IF;
END $$;
COMMIT;
