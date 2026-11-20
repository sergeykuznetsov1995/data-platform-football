"""Authentication for ClubElo's Trino sessions."""

import os


def source_password() -> str:
    password = os.environ.get("TRINO_SRC_CLUBELO_PASSWORD")
    if not password:
        raise RuntimeError("ClubElo requires a nonempty TRINO_SRC_CLUBELO_PASSWORD")
    return password


def connect(*, host=None, port=None, catalog="iceberg"):
    """Open a fresh HTTPS connection with the current source credential."""
    password = source_password()

    from trino.auth import BasicAuthentication
    from trino.dbapi import connect as dbapi_connect

    return dbapi_connect(
        host=host or os.environ.get("TRINO_HOST", "trino"),
        port=port or int(os.environ.get("TRINO_PORT", "8443")),
        catalog=catalog,
        user="src_clubelo",
        http_scheme="https",
        verify=False,
        auth=BasicAuthentication("src_clubelo", password),
    )
