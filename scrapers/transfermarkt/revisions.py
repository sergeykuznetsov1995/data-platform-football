"""Shared defaults for parser, cache and manifest identity."""
PARSER_REVISION = 'registry-v2'
SCHEMA_REVISION = '4'

# Older immutable scope manifests remain valid evidence for recovery.
SUPPORTED_MANIFEST_REVISIONS = {('v2', '2'), (PARSER_REVISION, SCHEMA_REVISION)}

CAREER_NULLABLE_ADDITIONS = {
    'first_seen_at', 'is_value_removed', 'club_id', 'mv_last_change',
    'from_is_special', 'to_is_special', 'from_country_flag', 'to_country_flag',
    'from_country_id', 'to_country_id', 'is_loan_end',
}
