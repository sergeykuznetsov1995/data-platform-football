"""Archived BRP4 edition label reconciliation for the pinned Bronze audit (#2330)."""

_HISTORICAL_SNAPSHOT = "tm-discovery-e44a99b60bb12ac25ac59641"

# One October 2026 dual-write cycle, reconciled to the archived BRP4/2025
# source edition and its write receipts (#2330). Each certificate records
# SHA256(batch_id), row count and SHA256(sorted concatenated full-row digests).
# Drift must invalidate the certificate; never refresh it from a failing batch.
_BATCHES = {
    "iceberg.bronze.transfermarkt_coaches": (
        "8C6D715E2136251B5BA19EA23324258A1A65EEAA4A9F2429B3BD1D11F1D03EE8", 8,
        "950ACE227571AFE005AE1CC22611F0FA4ACFDB3724D296DD09BDB4E898456E47",
    ),
    "iceberg.bronze.transfermarkt_players": (
        "5911894267FDBC05F7DDEFAAFBF995CFD768C7A3F46F029C8B3EDFD67D514E5D", 81,
        "3FAB06A94C4756B4C9A30EB4A65243AB724A33B0A8D9B656D323572CF2EC0813",
    ),
    "iceberg.bronze.transfermarkt_market_value_history": (
        "2C4D695E615BD74FD51ECAFD4C7D4F6C8C6685AE5035756F1706FAB417453C6F", 41,
        "0956475EE0438CEC4B386A0B52EB7D540906C57001B25A5013234223BE91CF29",
    ),
    "iceberg.bronze.transfermarkt_transfers": (
        "8DE7229A08B730A4B56FF1D5F748F27CA49AF4474188EA2688A95E92E1E5B8F7", 685,
        "AA913D673C4825C6D56D2346D8FE97A8F0AF8DF0B2A5053D30C8B444AB7BE04B",
    ),
}


def historical_season_predicate(
    table, bronze_relation, competitions_relation, editions_relation, snapshot_literal
):
    """Match an attested archived row using caller-pinned relations and snapshot SQL."""
    if table not in _BATCHES:
        return "FALSE"
    batch_hash, size, multiset_hash = _BATCHES[table]
    # For an oversized batch LIMIT returns size+1, so the count guard rejects
    # it regardless of row order. At exactly size, sorting covers every hash.
    return f"""(
        b.league = 'TM-BRP4' AND b.season = '2526'
        AND to_hex(sha256(to_utf8(b._batch_id))) = '{batch_hash}'
        AND (
            SELECT count(*) = 2 AND count(DISTINCT e.registry_snapshot_id) = 2
                AND count(DISTINCT e.source_url) = 1
                AND count(*) FILTER (WHERE trim(e.source_url) <> ''
                    AND COALESCE(c.canonical_competition_id, 'TM-' || c.competition_id) = 'TM-BRP4'
                    AND ((e.registry_snapshot_id = '{_HISTORICAL_SNAPSHOT}'
                        AND e.canonical_season = '2526' AND e.edition_label = '25/26'
                        AND e.season_format = 'split_year')
                    OR (e.registry_snapshot_id = {snapshot_literal}
                        AND e.canonical_season = '2026' AND e.edition_label = '2026'
                        AND e.season_format = 'single_year'))) = 2
            FROM {editions_relation} e
            JOIN {competitions_relation} c ON c.registry_snapshot_id = e.registry_snapshot_id
                AND c.competition_id = e.competition_id
            WHERE e.competition_id = 'BRP4' AND e.edition_id = '2025'
                AND e.registry_snapshot_id IN ('{_HISTORICAL_SNAPSHOT}', {snapshot_literal})
        )
        AND (
            SELECT count(*) = {size}
                AND to_hex(sha256(to_utf8(array_join(array_sort(array_agg(row_hash)), '')))) = '{multiset_hash}'
            FROM (
                SELECT to_hex(sha256(to_utf8(json_format(CAST((SELECT r.*) AS JSON))))) AS row_hash
                FROM (
                    SELECT r.* FROM {bronze_relation} r
                    WHERE r.league = 'TM-BRP4' AND r.season = '2526'
                        AND to_hex(sha256(to_utf8(r._batch_id))) = '{batch_hash}'
                    LIMIT {size + 1}
                ) r
            ) hashes
        )
    )"""
