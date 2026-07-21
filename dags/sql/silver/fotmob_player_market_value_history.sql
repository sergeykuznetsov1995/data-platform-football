-- =============================================================================
-- Silver: fotmob_player_market_value_history
-- =============================================================================
--
-- Time-series рыночной стоимости игроков из FotMob. Один row per
-- (player_id, value_date, league, season) — точка timeline = snapshot MV на
-- конкретную дату.
--
-- Bronze shape (native, #930 cutover):
--   `fotmob_player_snapshots_current.market_values_json`:
--   {"values": [{"date": "ISO", "value": <int>, "currency": "EUR", ...}, ...]}
-- (json-путь `$.values[]` идентичен legacy fotmob_player_details; тот же
--  UNNEST-идиом, что в silver/fotmob_player_profile.sql).
--
-- (league, season): native-снапшот игрока ГЛОБАЛЬНЫЙ (natural key player_id,
-- без лиги/сезона) — принадлежность игрока к (лиге, сезону) реконструируется
-- player_scope-каркасом (cutover-карта §3.3): per-season вселенная из
-- fotmob_leaderboards_current (история) ∪ текущий состав
-- (fotmob_squad_snapshots_current × fotmob_season_teams_current).
-- INNER JOIN к league_map одновременно скоупит выдачу прежними 14 лигами.
--
-- Cross-season дубликаты: FotMob отдаёт полную history до сегодня в каждом
-- ingest-snapshot. Игрок в APL 2024/25 и 2025/26 → исторические точки timeline
-- лягут в обе партиции. PK включает (league, season) — потребитель фильтрует
-- WHERE season = (MAX) для «last view» либо специфический season-snapshot.
-- Полностью обещанный timeline = WHERE season = latest.
--
-- canonical_id НЕ резолвится в Silver (отличие от transfermarkt_market_value_history,
-- где canonical_id мерджится из silver.xref_player в момент Silver-материализации).
-- Здесь FotMob bridge применяется в Gold (fct_player_market_value), чтобы Silver
-- оставался pure-shaping слоем bronze→silver без cross-Silver dependencies.
--
-- Зерно: (player_id, value_date, league, season).
-- =============================================================================

WITH league_map(competition_id, league) AS (
    VALUES
        {{ fotmob_league_map_values_sql }}
),

-- Игрок ∈ (лига, сезон): лидерборды дают per-season истину (работает и для
-- исторических сезонов), squad × season_teams добавляет текущий состав
-- (игроки без единой stat-строки). UNION дедуплицирует.
player_scope AS (
    SELECT DISTINCT
        lb.competition_id,
        lb.source_season_key,
        CAST(lb.participant_id AS varchar) AS player_id
    FROM iceberg.bronze.fotmob_leaderboards_current lb
    WHERE lb.participant_type = 'player'

    UNION

    SELECT DISTINCT
        st.competition_id,
        st.source_season_key,
        sq.member_id AS player_id
    FROM iceberg.bronze.fotmob_season_teams_current st
    JOIN iceberg.bronze.fotmob_squad_snapshots_current sq
      ON CAST(sq.team_id AS bigint) = st.team_id
    WHERE sq.member_type = 'player'
),

-- *_current уже дедуплицирован по natural key (player_id): манифест-гейт +
-- ROW_NUMBER внутри view — legacy ROW_NUMBER-дедуп по (player_id, league,
-- season) не нужен (cutover-карта §2.3).
snapshots AS (
    SELECT
        ps.player_id,
        ps.market_values_json,
        ps._observed_at
    FROM iceberg.bronze.fotmob_player_snapshots_current ps
    WHERE NOT ps.is_coach
      AND ps.market_values_json IS NOT NULL
      AND ps.market_values_json <> 'null'
      AND ps.market_values_json <> '{}'
),

scoped AS (
    SELECT
        s.player_id,
        s.market_values_json,
        s._observed_at,
        lm.league,
        -- год-старта сезона: substr корректен для обеих форм source_season_key
        -- ('2025/2026' → 2025, '2025' → 2025); слаг НЕ выводить из формы ключа.
        TRY_CAST(SUBSTR(sc.source_season_key, 1, 4) AS integer) AS season_year
    FROM snapshots s
    JOIN player_scope sc ON sc.player_id = s.player_id
    JOIN league_map lm ON lm.competition_id = sc.competition_id
)

SELECT
    d.player_id,
    TRY_CAST(SUBSTR(json_extract_scalar(v, '$.date'), 1, 10) AS DATE) AS value_date,
    TRY_CAST(json_extract_scalar(v, '$.value')    AS BIGINT)          AS market_value_eur,
    json_extract_scalar(v, '$.currency')                              AS currency,
    d._observed_at                                                    AS _bronze_ingested_at,
    -- season → slug ('2425'); year-start из source_season_key (см. scoped).
    d.league,
    -- #913 Phase 2
    CASE WHEN d.league = 'INT-World Cup'
         THEN LPAD(CAST(d.season_year AS varchar), 4, '0')
         ELSE LPAD(CAST(MOD(d.season_year, 100) AS varchar), 2, '0')
              || LPAD(CAST(MOD(d.season_year + 1, 100) AS varchar), 2, '0')
    END AS season

FROM scoped d
CROSS JOIN UNNEST(
    CAST(json_extract(d.market_values_json, '$.values') AS array<json>)
) AS t(v)
WHERE TRY_CAST(SUBSTR(json_extract_scalar(v, '$.date'), 1, 10) AS DATE) IS NOT NULL
