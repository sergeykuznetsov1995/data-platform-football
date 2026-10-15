\set ON_ERROR_STOP on
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL timezone = 'UTC';
SET LOCAL statement_timeout = '15s';
WITH children AS (
 SELECT * FROM dag_run
 WHERE dag_id = 'dag_transform_fbref_silver' AND state = 'queued'
), parents AS (
 SELECT DISTINCT parent.* FROM children child JOIN dag_run parent
 ON child.run_id = 'fbref_silver__' || parent.dag_id || '__' || parent.run_id
 WHERE parent.dag_id IN ('dag_ingest_fbref','dag_backfill_fbref','dag_replay_fbref')
)
SELECT jsonb_build_object(
 'checked_at', CURRENT_TIMESTAMP,
 'runs', COALESCE((SELECT jsonb_agg(to_jsonb(child) ORDER BY id) FROM children child),'[]'::jsonb),
 'tasks', COALESCE((SELECT jsonb_agg(to_jsonb(ti) ORDER BY ti.run_id,ti.task_id,ti.map_index)
   FROM task_instance ti JOIN children child USING (dag_id,run_id)), '[]'::jsonb),
 'parents', COALESCE((SELECT jsonb_agg(to_jsonb(parent) ORDER BY id) FROM parents parent),'[]'::jsonb)
);
ROLLBACK;
