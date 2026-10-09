"""Independent Trino reader used by the Understat history plan."""

from unittest.mock import MagicMock

import pytest

from scrapers.understat.manifest import UnderstatManifestQuery


@pytest.fixture
def trino_connection(monkeypatch):
    import trino.dbapi

    for name in ("TRINO_HOST", "TRINO_PORT", "TRINO_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    connection = MagicMock()
    connection.cursor.return_value.fetchall.return_value = [(1,)]
    connect = MagicMock(return_value=connection)
    monkeypatch.setattr(trino.dbapi, "connect", connect)
    return connect, connection, connection.cursor.return_value


def test_unbound_query_has_no_ping_and_closes_resources(trino_connection):
    connect, connection, cursor = trino_connection

    assert UnderstatManifestQuery().execute_query("SELECT manifest") == [(1,)]
    connect.assert_called_once_with(host="trino", port=8080, user="airflow", catalog="iceberg")
    cursor.execute.assert_called_once_with("SELECT manifest")
    cursor.close.assert_called_once_with()
    connection.close.assert_called_once_with()


@pytest.mark.parametrize("explicit_port", [None, "9443"])
def test_authenticated_connection_matches_writer_defaults(monkeypatch, trino_connection, explicit_port):
    from trino.auth import BasicAuthentication

    connect, _, _ = trino_connection
    monkeypatch.setenv("TRINO_HOST", "test-trino")
    monkeypatch.setenv("TRINO_PASSWORD", "fixture-password")
    if explicit_port:
        monkeypatch.setenv("TRINO_PORT", explicit_port)
    UnderstatManifestQuery(catalog="test_catalog").execute_query("SELECT manifest")

    options = connect.call_args.kwargs
    assert options["host"] == "test-trino"
    assert options["port"] == (int(explicit_port) if explicit_port else 8443)
    assert options["user"] == "airflow"
    assert options["catalog"] == "test_catalog"
    assert options["http_scheme"] == "https"
    assert options["verify"] is False
    assert options["auth"] == BasicAuthentication("airflow", "fixture-password")


def test_bound_parameters_are_forwarded(trino_connection):
    _, _, cursor = trino_connection
    UnderstatManifestQuery().execute_query("SELECT ?", (42,))
    cursor.execute.assert_called_once_with("SELECT ?", (42,))


@pytest.mark.parametrize("stage", ["cursor", "execute", "fetchall"])
def test_query_failure_propagates_and_closes_resources(trino_connection, stage):
    _, connection, cursor = trino_connection
    failing = connection.cursor if stage == "cursor" else getattr(cursor, stage)
    failing.side_effect = RuntimeError("fixture query failure")

    with pytest.raises(RuntimeError, match="fixture query failure"):
        UnderstatManifestQuery().execute_query("SELECT manifest")

    connection.close.assert_called_once_with()
    if stage != "cursor":
        cursor.close.assert_called_once_with()
