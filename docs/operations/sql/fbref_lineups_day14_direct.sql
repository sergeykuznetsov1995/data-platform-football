-- PostgreSQL. Save this SELECT on the same verified registry cut as the daily snapshot.
SELECT jsonb_build_object('editions', COALESCE(jsonb_agg(to_jsonb(edition)), '[]'::jsonb))
FROM (
    SELECT s.competition_id, s.season_id, s.canonical_url,
        s.metadata -> 'current_season_install' -> 'history_raw' AS catalog_source,
        COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'match_id', f.source_ids ->> 'match_id', 'raw_key', latest.raw_manifest_key
            ) ORDER BY f.target_id)
            FROM fbref_control.page_frontier f
            LEFT JOIN LATERAL (
                SELECT attempt.raw_manifest_key
                FROM fbref_control.fetch_attempt attempt
                WHERE attempt.target_id = f.target_id AND attempt.status = 'succeeded'
                  AND attempt.content_hash = f.last_content_hash AND attempt.raw_manifest_key IS NOT NULL
                ORDER BY attempt.finished_at DESC NULLS LAST, attempt.attempt_number DESC, attempt.attempt_id
                LIMIT 1
            ) latest ON true
            WHERE f.source = s.source AND f.page_kind = 'match'
              AND f.source_ids ->> 'competition_id' = s.competition_id
              AND f.source_ids ->> 'season_id' = s.season_id
              AND f.source_ids ->> 'match_id' IS NOT NULL
        ), '[]'::jsonb) AS targets
    FROM fbref_control.season_registry s
    WHERE s.source = 'fbref' AND s.is_current AND s.present AND s.lifecycle_state = 'present'
      AND s.metadata ->> 'direct_match_only' = 'true'
    ORDER BY s.competition_id, s.season_id
) edition;
