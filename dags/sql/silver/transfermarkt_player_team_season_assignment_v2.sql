-- =============================================================================
-- Shadow Silver v2: deterministic end-of-edition membership assignment
-- =============================================================================
-- Grain: (competition_id, edition_id, player_id).  league/season remain
-- compatibility columns for canonical readers.  The transfer cutoff comes
-- from the edition registry, so single-year tournaments use 31 December and
-- split-year competitions use their source-backed end date.

WITH edition_bounds AS (
    SELECT
        competition_id,
        edition_id,
        COALESCE(
            end_date,
            CASE
                WHEN season_format = 'single_year' THEN CAST(
                    CONCAT(canonical_season, '-12-31') AS date
                )
                WHEN season_format = 'split_year' THEN CAST(
                    CONCAT('20', SUBSTR(canonical_season, 3, 2), '-06-30')
                    AS date
                )
            END
        ) AS edition_end_date
    FROM iceberg.silver.transfermarkt_competition_editions_v2
),

member_counts AS (
    SELECT
        competition_id,
        edition_id,
        league,
        season,
        player_id,
        COUNT(*)                                           AS membership_count,
        MIN(club_id)                                       AS only_club_id,
        MIN(club_name)                                     AS only_club_name
    FROM iceberg.silver.transfermarkt_squad_memberships_v2
    GROUP BY competition_id, edition_id, league, season, player_id
),

eligible_transfers AS (
    SELECT
        m.competition_id,
        m.edition_id,
        m.league,
        m.season,
        m.player_id,
        e.transfer_date,
        e.transfer_id,
        e.to_club_id
    FROM member_counts m
    JOIN edition_bounds bounds
      ON bounds.competition_id = m.competition_id
     AND bounds.edition_id = m.edition_id
    JOIN iceberg.silver.transfermarkt_transfer_events_v2 e
      ON e.player_id = m.player_id
     AND (
          e.source_competition_id = m.competition_id
          OR e.source_competition_id IS NULL
     )
    WHERE e.transfer_date IS NOT NULL
      AND bounds.edition_end_date IS NOT NULL
      AND e.transfer_date <= bounds.edition_end_date
),

latest_transfer_date AS (
    SELECT
        competition_id,
        edition_id,
        league,
        season,
        player_id,
        MAX(transfer_date)                                 AS latest_transfer_date
    FROM eligible_transfers
    GROUP BY competition_id, edition_id, league, season, player_id
),

latest_transfer_target AS (
    SELECT
        t.competition_id,
        t.edition_id,
        t.league,
        t.season,
        t.player_id,
        MIN(t.to_club_id)                                  AS transfer_club_id,
        COUNT(DISTINCT t.to_club_id)                       AS latest_target_count,
        d.latest_transfer_date
    FROM eligible_transfers t
    JOIN latest_transfer_date d
      ON d.competition_id = t.competition_id
     AND d.edition_id = t.edition_id
     AND d.player_id = t.player_id
     AND d.latest_transfer_date = t.transfer_date
    GROUP BY
        t.competition_id, t.edition_id, t.league, t.season, t.player_id,
        d.latest_transfer_date
),

latest_target_membership AS (
    SELECT
        t.competition_id,
        t.edition_id,
        t.league,
        t.season,
        t.player_id,
        t.transfer_club_id,
        MIN(m.club_name)                                   AS transfer_club_name,
        t.latest_target_count,
        COUNT(DISTINCT m.club_id)                          AS membership_target_count,
        t.latest_transfer_date
    FROM latest_transfer_target t
    LEFT JOIN iceberg.silver.transfermarkt_squad_memberships_v2 m
      ON m.competition_id = t.competition_id
     AND m.edition_id = t.edition_id
     AND m.player_id = t.player_id
     AND m.club_id = t.transfer_club_id
    GROUP BY
        t.competition_id, t.edition_id, t.league, t.season, t.player_id,
        t.transfer_club_id, t.latest_target_count, t.latest_transfer_date
),

assigned AS (
    SELECT
        m.competition_id,
        m.edition_id,
        m.league,
        m.season,
        m.player_id,
        m.membership_count,
        CASE
            WHEN m.membership_count = 1 THEN m.only_club_id
            WHEN t.latest_target_count = 1
             AND t.membership_target_count = 1 THEN t.transfer_club_id
        END                                               AS club_id,
        CASE
            WHEN m.membership_count = 1 THEN m.only_club_name
            WHEN t.latest_target_count = 1
             AND t.membership_target_count = 1 THEN t.transfer_club_name
        END                                               AS club_name,
        CASE
            WHEN m.membership_count = 1 THEN 'single_membership'
            WHEN t.latest_target_count = 1
             AND t.membership_target_count = 1 THEN 'latest_transfer'
            ELSE 'ambiguous'
        END                                               AS assignment_status,
        t.latest_transfer_date
    FROM member_counts m
    LEFT JOIN latest_target_membership t
      ON t.competition_id = m.competition_id
     AND t.edition_id = m.edition_id
     AND t.player_id = m.player_id
),

team_xref AS (
    SELECT
        source_id AS club_name,
        league,
        season,
        CASE WHEN COUNT(DISTINCT canonical_id) FILTER (
                       WHERE confidence <> 'orphan'
                   ) = 1
             THEN MAX(canonical_id) FILTER (WHERE confidence <> 'orphan')
        END AS team_id
    FROM iceberg.silver.xref_team
    WHERE source = 'transfermarkt'
    GROUP BY source_id, league, season
)

SELECT
    a.competition_id,
    a.edition_id,
    a.player_id,
    x.canonical_id,
    a.club_id,
    tx.team_id,
    a.club_name,
    a.membership_count,
    a.assignment_status,
    a.latest_transfer_date,
    a.league,
    a.season
FROM assigned a
LEFT JOIN iceberg.silver.transfermarkt_player_xref_global_v2 x
  ON x.player_id = a.player_id
 AND x.resolution_status = 'resolved'
LEFT JOIN team_xref tx
  ON tx.club_name = a.club_name
 AND tx.league = a.league
 AND tx.season = a.season
