"""
Unit tests for ``dags/utils/maintenance_tasks.py``.

Covers the small but load-bearing constants that drive the daily Iceberg
maintenance DAG. Without a delete-then-insert table in ``HIGH_CHURN_BRONZE``
it accumulates daily ``INSERT`` snapshots and metadata bloats indefinitely
(see Apr 2026 incident: ~26 GB metadata on whoscored_events).

Pure constant-table tests — no Trino, no Airflow, no DAG load.
"""

from __future__ import annotations

import pytest


@pytest.mark.unit
class TestHighChurnBronzeAllowlist:
    """Verify the daily-VACUUM allowlist contains every known
    delete-then-insert / replace-partitions table."""

    def test_allowlist_is_a_tuple(self):
        """HIGH_CHURN_BRONZE is declared as a tuple (immutable) so it
        can't be mutated at runtime by accident."""
        from utils.maintenance_tasks import HIGH_CHURN_BRONZE

        assert isinstance(HIGH_CHURN_BRONZE, tuple)
        # Sanity: at least the historically-known offenders are present.
        for table in (
            "whoscored_events",
            "whoscored_lineups",
            "whoscored_schedule",
            "whoscored_match_ingest_manifest",
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

    def test_daily_fotmob_sofascore_espn_writers_listed(self):
        """#266: these daily writers bloated to multi-GB metadata
        (fotmob_match_details hit 7.2 GB / 154 MB data) because they were
        never on the allow-list. They must stay listed."""
        from utils.maintenance_tasks import HIGH_CHURN_BRONZE

        for table in (
            "fotmob_match_details",
            "fotmob_player_details",
            "fotmob_player_stats",
            "sofascore_player_ratings",
            "espn_lineup",
            "espn_matchsheet",
        ):
            assert table in HIGH_CHURN_BRONZE, (
                f"{table!r} must be in HIGH_CHURN_BRONZE (#266)."
            )


@pytest.mark.unit
class TestSessionMinRetention:
    """#266: the daily DAG asks for '3d', shorter than Trino's 7d default
    floor. The per-session override must be strictly shorter than any
    threshold the module uses, or every expire is rejected and the sweep
    no-ops silently."""

    def test_session_floor_below_daily_threshold(self):
        from utils.maintenance_tasks import SESSION_MIN_RETENTION

        # '1h' parsed crudely: must be sub-day so it clears the 3d daily ask.
        assert SESSION_MIN_RETENTION.endswith(("h", "m", "s")), (
            "SESSION_MIN_RETENTION must be sub-day (e.g. '1h') so the "
            "daily '3d' threshold is honored."
        )
