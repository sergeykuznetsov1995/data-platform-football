"""Global player/endpoint career debt, independent of scope refresh windows."""

import os
from datetime import timedelta

CAREER_TTL = timedelta(days=7)
CAREER_DEBT_POLICY = 'player-endpoint-v1'
FETCH_STATE_TABLE = 'iceberg.ops.transfermarkt_fetch_state'
MEMBERSHIPS_TABLE = 'iceberg.bronze.transfermarkt_squad_memberships'
SUCCESS_STATUSES = frozenset({'success', 'valid_empty', 'authoritative_empty'})


def career_debt_ctes() -> str:
    # Keep the existing fetch-state identity, including environment overrides.
    parser = os.environ.get('TM_PARSER_VERSION', 'v2').replace("'", "''")
    schema = os.environ.get('TM_SCHEMA_VERSION', '2').replace("'", "''")
    return f"""career_roster AS (
    SELECT DISTINCT competition_id, edition_id, CAST(player_id AS varchar) AS player_id
    FROM {MEMBERSHIPS_TABLE}
), career_success AS (
    SELECT DISTINCT source_id, endpoint
    FROM {FETCH_STATE_TABLE}
    WHERE parser_version = '{parser}' AND schema_version = '{schema}'
      AND last_success_at IS NOT NULL
      AND endpoint IN ('transfer_events', 'market_value_points')
), career_debt AS (
    SELECT r.competition_id, r.edition_id, r.player_id
    FROM career_roster r
    LEFT JOIN career_success te
      ON te.source_id = r.player_id AND te.endpoint = 'transfer_events'
    LEFT JOIN career_success mv
      ON mv.source_id = r.player_id AND mv.endpoint = 'market_value_points'
    WHERE te.source_id IS NULL OR mv.source_id IS NULL
)"""
