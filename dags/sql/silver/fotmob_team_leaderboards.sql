-- =============================================================================
-- Silver: fotmob_team_leaderboards
-- =============================================================================
--
-- Long-form: one row per (team_id, stat_category_group, stat_name, league,
-- season) — conform-only проекция сезонных командных лидербордов из FotMob
-- (index /api/data/leagues + полные списки data.fotmob.com/stats/.../*.json).
-- Грейн bronze сохраняем как есть (charter §1 grain rule) — это НЕ PIVOT в wide
-- и НЕ rollup: место (rank) + значение метрики (stat_value) на команду в каждой
-- статистической категории.
--
-- Источник Bronze (#930 cutover, см. scrapers/fotmob/repository.py
-- CURRENT_VIEW_SPECS и parsers.parse_leaderboards):
--   bronze.fotmob_leaderboards_current WHERE participant_type = 'team' —
--     team_id (bigint), team_name (varchar),
--     stat_category_group / stat_category_header / stat_name (varchar),
--     rank / stat_value_count / matches_played / minutes_played (bigint),
--     stat_value / sub_stat_value (double), competition_id (bigint),
--     source_season_key (varchar, '2025/2026' | '2025').
--
-- Notes:
--   * Низкоценные колонки НЕ переносим: participant_name (= team_name),
--     team_color, country_code.
--   * league ← competition_id через league_map (обратная карта
--     configs/fotmob/competitions.json); INNER JOIN одновременно скоупит выдачу
--     прежними 14 лигами — расширение скоупа НЕ входит в cutover.
--   * season ← year-start = substr(source_season_key, 1, 4); слаг НЕ выводить
--     из формы ключа (AFCON single-year обязан дать '2526', как legacy).
--   * Dedup key включает stat_category_group: один stat_name может встречаться в
--     нескольких группах — без группы в ключе разные категории схлопнулись бы.
--     Natural key _current мельче (содержит rank/top_list_index) — ROW_NUMBER
--     по legacy-ключу оставляем (#464-семантика);
--     ORDER BY _observed_at DESC, _target_batch_id DESC (native без _batch_id).
--   * Резолв canonical_id отложен в Gold (charter §5) — храним numeric team_id +
--     team_name.
--   * Season → slug ('2526') тем же выражением, что fotmob_team_match.sql.
--   * replace_partitions(['league','season']) → ROW_NUMBER dedup defensive.
-- =============================================================================

WITH league_map(competition_id, league) AS (
    VALUES
        {{ fotmob_league_map_values_sql }}
),

native_scoped AS (
    SELECT
        l.team_id,
        l.team_name,
        l.stat_category_group,
        l.stat_name,
        l.rank,
        l.stat_value,
        l.sub_stat_value,
        l.stat_value_count,
        l.matches_played,
        l.minutes_played,
        l.stat_category_header,
        l._observed_at,
        l._target_batch_id,
        lm.league,
        TRY_CAST(substr(l.source_season_key, 1, 4) AS integer) AS season_year
    FROM iceberg.bronze.fotmob_leaderboards_current l
    JOIN league_map lm ON lm.competition_id = l.competition_id
    WHERE l.participant_type = 'team'
      AND l.team_id IS NOT NULL
      AND l.stat_name IS NOT NULL
),

bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY team_id, stat_category_group, stat_name, league, season_year
                ORDER BY _observed_at DESC, _target_batch_id DESC
            ) AS rn
        FROM native_scoped s
    )
    WHERE rn = 1
)

SELECT
    -- ===== Identity =====
    b.team_id,
    b.team_name,
    b.stat_category_group,
    b.stat_name,

    -- ===== Leaderboard values =====
    b.rank,
    b.stat_value,
    b.sub_stat_value,
    b.stat_value_count,
    b.matches_played,
    b.minutes_played,
    b.stat_category_header,

    -- ===== Lineage =====
    b._observed_at AS _bronze_ingested_at,

    -- ===== Partition keys (season → slug to match other Silver tables) =====
    b.league,
    -- #913 Phase 2
    CASE WHEN b.league = 'INT-World Cup'
         THEN LPAD(CAST(b.season_year AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(b.season_year, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(b.season_year + 1, 100) AS varchar), 2, '0')
    END AS season

FROM bronze_dedup b
