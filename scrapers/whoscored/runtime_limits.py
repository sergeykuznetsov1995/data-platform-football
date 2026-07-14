"""Shared, lightweight WhoScored source-limit contracts."""

# HTML page requests back matches, previews, profiles, and parts of schedule
# ingestion.  This is a per-service-instance limiter; Airflow's source pool
# supplies the independently enforced concurrency multiplier.
SOURCE_PAGE_REQUESTS_PER_MINUTE = 30

