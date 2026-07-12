-- =============================================================================
-- Shadow Gold v2: global Transfermarkt transfer events
-- =============================================================================
-- PK: transfer_id.  Upcoming events without transfer_date are retained.
-- This is intentionally shadow-only until the native readiness gate passes.

SELECT
    t.transfer_id,
    COALESCE(t.canonical_id, 'tm_' || t.player_id)         AS player_id,
    t.transfer_date,
    t.event_season,
    COALESCE(
        t.from_club_id_canonical,
        'tm_' || COALESCE(
            NULLIF(t.from_club_id, ''),
            LOWER(REGEXP_REPLACE(t.from_club_name, '[^a-zA-Z0-9]+', '_'))
        )
    )                                                     AS from_team_id,
    COALESCE(
        t.to_club_id_canonical,
        'tm_' || COALESCE(
            NULLIF(t.to_club_id, ''),
            LOWER(REGEXP_REPLACE(t.to_club_name, '[^a-zA-Z0-9]+', '_'))
        )
    )                                                     AS to_team_id,
    t.from_club_name,
    t.to_club_name,
    t.fee_eur,
    t.market_value_eur                                    AS market_value_at_transfer_eur,
    t.is_loan,
    t.is_upcoming,
    t._bronze_ingested_at
FROM iceberg.silver.transfermarkt_transfer_events_v2 t
