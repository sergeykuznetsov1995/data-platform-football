"""Trino manager of the ESPN bronze contour: no dynamic filtering (#1557).

``insert_dataframe_atomic(single_statement_replace=True)`` removes the old rows
of a batch with one MERGE matching tombstones ``IS NOT DISTINCT FROM`` on every
column.  With dynamic filtering on, Trino builds a dynamic filter from such a
join column and drops, on the target scan, the rows whose value is NULL: a row
with any NULL column never met its tombstone, so every wave appended another
generation of the batch matches.  ``get_trino_connection()`` of the base module
already runs without dynamic filtering; the base ``TrinoTableManager`` is left
as is (it is sealed in the WhoScored runtime contract).
"""

from __future__ import annotations

import os

from trino.exceptions import Http502Error, Http503Error, Http504Error, TrinoConnectionError

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base import trino_manager as base
from scrapers.espn.trino_budget import (
    BudgetConnection,
    DEFAULT_SCHEMA,
    default_budget,
    may_change_data,
)
from scrapers.whoscored.trino_budget import WRITE_TIMEOUT

ESPN_USER = "src_espn"
AMBIGUOUS_TRANSPORT_ERRORS = (TrinoConnectionError, Http502Error, Http503Error, Http504Error)
# The driver default is a 30-second per-request timeout. Submitting the
# statement itself — an offline replay batch carries one INSERT with tens of
# thousands of literal rows — can block past it while Trino parses the body,
# surfacing as an ambiguous-write stop (#2108: 28,022-row events INSERT).
# 300s covers statement submission with margin; execution stays bounded
# server-side by ``query_max_run_time``.
REQUEST_TIMEOUT = 300
SESSION_PROPERTIES = {
    "enable_dynamic_filtering": "false",
    "query_max_run_time": f"{WRITE_TIMEOUT}s",
}


class EspnTrinoTableManager(base.TrinoTableManager):
    """``TrinoTableManager`` whose connections disable dynamic filtering.

    Tombstone replace-MERGEs also run non-replay-safe here
    (``_execute_replacement``): an ambiguous connection loss raises
    ``TrinoAmbiguousWriteError`` instead of replaying the insert rows.
    """

    def __init__(self, host="trino", port=None, user=ESPN_USER, catalog="iceberg",
                 *, budget=None):
        # Keep the base constructor's positional API and its connection/cache locks.
        super().__init__(host, port or int(os.environ.get("TRINO_PORT", 8443)),
                         ESPN_USER, catalog)
        self._password = os.environ.get("TRINO_SRC_ESPN_PASSWORD")
        self._budget = default_budget() if budget is None else budget

    def _require_password(self):
        self._password = os.environ.get("TRINO_SRC_ESPN_PASSWORD")
        if not self._password:
            raise base.TrinoError("ESPN Trino requires TRINO_SRC_ESPN_PASSWORD")
        return self._password

    def _connect_with_retry(self):
        self._require_password()
        return super()._connect_with_retry()

    def _create_connection(self):
        password = self._require_password()
        connection = base.trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=ESPN_USER,
            catalog=self.catalog,
            schema=DEFAULT_SCHEMA,
            http_scheme="https",
            auth=base.trino.auth.BasicAuthentication(ESPN_USER, password),
            verify=False,
            session_properties=dict(SESSION_PROPERTIES),
            # A retry of the initial HTTP POST could replay a committed write.
            max_attempts=1,
            # Large literal INSERTs outrun the driver's 30s default (#2108).
            request_timeout=REQUEST_TIMEOUT,
        )
        return BudgetConnection(connection, self._budget, self.catalog)

    def _execute(self, sql, fetch=False, params=None, *, replay_safe=True):
        mutation = may_change_data(sql)
        try:
            return super()._execute(sql, fetch, params, replay_safe=replay_safe and not mutation)
        except base.TrinoAmbiguousWriteError:
            raise
        except base.TrinoError as error:
            # The base wraps statement failures with their driver cause. An
            # exhausted connection probe raises a TrinoError without one.
            cause = error.__cause__
            if mutation and isinstance(cause, AMBIGUOUS_TRANSPORT_ERRORS):
                raise base.TrinoAmbiguousWriteError(
                    f"ESPN write may have committed and must not be replayed: {cause}"
                ) from cause
            raise

    def _execute_committing(self, sql, *, replay_safe=False):
        # Only a structured, definite commit rejection permits another attempt.
        return super()._execute_committing(sql, replay_safe=False)

    def _execute_replacement(self, sql: str) -> None:
        """Run the tombstone replace-MERGE with strict replay protection.

        The MERGE ON clause never matches insert rows, so re-executing it
        after an ambiguous COMMIT outcome would re-apply already-committed
        inserts as duplicates; ``replay_safe=False`` makes a classified
        connection error raise ``TrinoAmbiguousWriteError`` instead of
        replaying.
        """
        return self._execute_committing(sql, replay_safe=False)


class EspnIcebergWriter(IcebergWriter):
    def _get_trino_manager(self):
        if self._trino_manager is None:
            self._trino_manager = EspnTrinoTableManager(
                host=self.trino_host, port=self.trino_port, catalog=self.catalog
            )
        return self._trino_manager
