from __future__ import annotations

import pytest


def test_database_topology_rejects_distinct_dsns_for_same_database_identity() -> None:
    from scripts.verify_espn_database_topology import (
        DatabaseIdentity,
        DatabaseTopologyError,
        verify_database_topology,
    )

    metadata_dsn = "postgresql://metadata-alias/airflow"
    control_dsn = "postgresql://control-alias/airflow"
    same_database = DatabaseIdentity("10.0.0.8", 5432, "airflow")

    with pytest.raises(DatabaseTopologyError, match="must be distinct"):
        verify_database_topology(
            environ={
                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": metadata_dsn,
                "ESPN_CONTROL_DATABASE_URL": control_dsn,
            },
            identity_reader=lambda _dsn: same_database,
        )


def test_database_topology_accepts_distinct_connected_database_identities() -> None:
    from scripts.verify_espn_database_topology import (
        DatabaseIdentity,
        verify_database_topology,
    )

    metadata_dsn = "postgresql://metadata/airflow"
    control_dsn = "postgresql://control/espn_control"
    identities = {
        metadata_dsn: DatabaseIdentity("10.0.0.8", 5432, "airflow"),
        control_dsn: DatabaseIdentity("10.0.0.9", 5432, "espn_control"),
    }
    reads = []

    result = verify_database_topology(
        environ={
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": metadata_dsn,
            "ESPN_CONTROL_DATABASE_URL": control_dsn,
        },
        identity_reader=lambda dsn: reads.append(dsn) or identities[dsn],
    )

    assert reads == [metadata_dsn, control_dsn]
    assert result == {
        "metadata": identities[metadata_dsn],
        "control": identities[control_dsn],
    }
