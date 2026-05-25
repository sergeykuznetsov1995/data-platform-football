"""
Unit tests for ``dags/utils/maintenance_tasks.py``.

Covers the small but load-bearing constants that drive the daily Iceberg
maintenance DAG. Without ``clubelo_team_history`` in
``HIGH_CHURN_BRONZE`` the table accumulates daily ``INSERT`` snapshots
and metadata bloats indefinitely (see Apr 2026 incident:
~26 GB metadata on whoscored_events).

Pure constant-table tests — no Trino, no Airflow, no DAG load.
"""

from __future__ import annotations

import pytest


@pytest.mark.unit
class TestHighChurnBronzeAllowlist:
    """Verify the daily-VACUUM allowlist contains every known
    delete-then-insert / replace-partitions table."""

    def test_clubelo_team_history_is_listed(self):
        """clubelo_team_history was added after switching the scraper to
        ``replace_partitions=['team']`` — every run rewrites the team's
        partition, so daily snapshot expiry is mandatory."""
        from utils.maintenance_tasks import HIGH_CHURN_BRONZE

        assert "clubelo_team_history" in HIGH_CHURN_BRONZE, (
            "clubelo_team_history must be in HIGH_CHURN_BRONZE — "
            "it does daily delete-then-insert per team partition and "
            "WILL bloat without periodic snapshot expiry."
        )

    def test_allowlist_is_a_tuple(self):
        """HIGH_CHURN_BRONZE is declared as a tuple (immutable) so it
        can't be mutated at runtime by accident."""
        from utils.maintenance_tasks import HIGH_CHURN_BRONZE

        assert isinstance(HIGH_CHURN_BRONZE, tuple)
        # Sanity: at least the historically-known offenders are present.
        for table in (
            "clubelo_team_history",
            "whoscored_events",
            "whoscored_schedule",
            "fbref_match_events",
        ):
            assert table in HIGH_CHURN_BRONZE, (
                f"{table!r} disappeared from HIGH_CHURN_BRONZE — "
                "if intentional, update this test."
            )

    def test_allowlist_has_no_duplicates(self):
        """Defensive check: a misordered edit could land the same table
        twice. Trino EXECUTE expire_snapshots is idempotent, but we
        still want config to be clean."""
        from utils.maintenance_tasks import HIGH_CHURN_BRONZE

        assert len(set(HIGH_CHURN_BRONZE)) == len(HIGH_CHURN_BRONZE)
