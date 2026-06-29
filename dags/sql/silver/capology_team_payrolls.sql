-- =============================================================================
-- Silver: capology_team_payrolls
-- =============================================================================
--
-- Заявленный клубный фонд оплаты труда из Capology — ПРЯМОЙ team-level снимок
-- (не агрегат зарплат игроков). Один row per (club_slug, league, season).
-- `canonical_id` подтягивается через silver.xref_team (source='capology',
-- non-orphan) как сторонний ключ для будущих Gold team-витрин.
--
-- Это богаче, чем salary-sum wage bill (gold.fct_team_season_stats.cap_finance):
-- несёт net/bonus/total/adjusted_total разбивку, которой нет в агрегате gross-зарплат.
-- Issue #603 (consume write-only bronze), прецедент #601 (SoFIFA promote).
--
-- Bronze schema (контракт scripts/audit_bronze_columns.py; типы live-verified):
--   club_slug, club_name, club_code (varchar),
--   {weekly,annual}_{gross,net}_{gbp,eur,usd}, {bonus,total,adjusted_total}_
--     {gross,net}_{gbp,eur,usd} — ВСЕ bigint (30 money-колонок, 3 валюты inline),
--   league, season (varchar slug '2526'), _ingested_at (timestamp).
--   Bronze partitioning = (league, season). Live: 240 rows / 12 сезонов (APL).
--
-- Notes:
--   * (league, season) JOIN predicate против xref_team MANDATORY
--     (CLAUDE.md / feedback_xref_join_season_predicate.md), иначе 1.5-4× fan-out.
--   * xref_team source_id = club_name (xref_team.sql.j2: capology-блок строит
--     universe из capology_player_salaries.club_name) — JOIN по club_name, НЕ slug.
--   * canonical_id NULLable: live 239/240 матчатся (1 relegated/alias edge).
--   * Bronze ingest mode = replace_partitions → (club_slug, league, season)
--     уникален (0 дублей live); ROW_NUMBER dedup defensive.
--   * Money-колонки уже bigint в Bronze → проброс без CAST.
-- =============================================================================

WITH bronze_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY club_slug, league, season
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM iceberg.bronze.capology_team_payrolls b
        WHERE club_slug IS NOT NULL
    )
    WHERE rn = 1
),

xt AS (
    SELECT canonical_id, source_id, league, season
    FROM iceberg.silver.xref_team
    WHERE source = 'capology'
      AND confidence <> 'orphan'
)

SELECT
    b.club_slug,
    xt.canonical_id,
    b.club_name,
    b.club_code,

    -- Weekly / annual gross + net (фонд оплаты).
    b.weekly_gross_gbp,
    b.weekly_gross_eur,
    b.weekly_gross_usd,
    b.annual_gross_gbp,
    b.annual_gross_eur,
    b.annual_gross_usd,
    b.weekly_net_gbp,
    b.weekly_net_eur,
    b.weekly_net_usd,
    b.annual_net_gbp,
    b.annual_net_eur,
    b.annual_net_usd,

    -- Bonus.
    b.bonus_gross_gbp,
    b.bonus_gross_eur,
    b.bonus_gross_usd,
    b.bonus_net_gbp,
    b.bonus_net_eur,
    b.bonus_net_usd,

    -- Total (salary + bonus).
    b.total_gross_gbp,
    b.total_gross_eur,
    b.total_gross_usd,
    b.total_net_gbp,
    b.total_net_eur,
    b.total_net_usd,

    -- Adjusted total (рыночно-скорректированный).
    b.adjusted_total_gross_gbp,
    b.adjusted_total_gross_eur,
    b.adjusted_total_gross_usd,
    b.adjusted_total_net_gbp,
    b.adjusted_total_net_eur,
    b.adjusted_total_net_usd,

    b._ingested_at                                      AS _bronze_ingested_at,

    -- Partition keys last (matching writer convention).
    b.league,
    b.season

FROM bronze_dedup b
LEFT JOIN xt
    ON xt.source_id = b.club_name
   AND xt.league    = b.league
   AND xt.season    = b.season
