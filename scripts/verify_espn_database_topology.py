#!/usr/bin/env python3
"""Fail closed unless ESPN metadata and shared control use distinct databases."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import os


METADATA_DSN_ENV = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
CONTROL_DSN_ENV = "ESPN_CONTROL_DATABASE_URL"


class DatabaseTopologyError(RuntimeError):
    """The isolated metadata and shared control topology is not proven safe."""


@dataclass(frozen=True, slots=True)
class DatabaseIdentity:
    """Connected PostgreSQL server/database identity without credentials."""

    server_address: str
    server_port: int
    database: str


def _required_dsn(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DatabaseTopologyError(f"{name} must be a non-empty DSN")
    return value


def connected_database_identity(dsn: str) -> DatabaseIdentity:
    """Open one DSN and return its credential-free PostgreSQL identity."""

    from sqlalchemy import create_engine, text

    engine = create_engine(
        dsn,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 10},
    )
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT COALESCE(inet_server_addr()::text, 'local'), "
                    "COALESCE(inet_server_port(), 0), current_database()"
                )
            ).one()
    finally:
        engine.dispose()
    address, port, database = row
    if (
        not isinstance(address, str)
        or not address
        or type(port) is not int
        or port < 0
        or not isinstance(database, str)
        or not database
    ):
        raise DatabaseTopologyError(
            "connected PostgreSQL database returned an invalid identity"
        )
    return DatabaseIdentity(address, port, database)


def verify_database_topology(
    *,
    environ: Mapping[str, str] | None = None,
    identity_reader: Callable[[str], DatabaseIdentity] = connected_database_identity,
) -> dict[str, DatabaseIdentity]:
    """Prove that aliases do not resolve metadata and control to one database."""

    runtime_environ = os.environ if environ is None else environ
    metadata_dsn = _required_dsn(runtime_environ, METADATA_DSN_ENV)
    control_dsn = _required_dsn(runtime_environ, CONTROL_DSN_ENV)
    try:
        metadata_identity = identity_reader(metadata_dsn)
    except DatabaseTopologyError:
        raise
    except Exception:
        raise DatabaseTopologyError(
            "unable to read the connected ESPN metadata database identity"
        ) from None
    try:
        control_identity = identity_reader(control_dsn)
    except DatabaseTopologyError:
        raise
    except Exception:
        raise DatabaseTopologyError(
            "unable to read the connected shared ESPN control database identity"
        ) from None
    if metadata_identity == control_identity:
        raise DatabaseTopologyError(
            "ESPN metadata and shared control databases must be distinct"
        )
    return {"metadata": metadata_identity, "control": control_identity}


def main() -> int:
    verify_database_topology()
    print("ESPN connected database topology verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
