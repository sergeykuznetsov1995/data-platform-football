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

from scrapers.base import trino_manager as base

SESSION_PROPERTIES = {"enable_dynamic_filtering": "false"}


class EspnTrinoTableManager(base.TrinoTableManager):
    """``TrinoTableManager`` whose connections disable dynamic filtering.

    Tombstone replace-MERGEs also run non-replay-safe here
    (``_execute_replacement``): an ambiguous connection loss raises
    ``TrinoAmbiguousWriteError`` instead of replaying the insert rows.
    """

    def _create_connection(self):
        # max_attempts=1 disables the trino client's internal HTTP retry
        # loop: after a network exception it silently re-POSTs the same
        # statement, which would replay a possibly-committed write and
        # defeat the strict no-replay protection of _execute_replacement.
        # Resilience stays where replay is safe: _connect_with_retry keeps
        # re-trying connections (its probe is a read-only SELECT 1), and
        # commit retries remain explicit in _execute_committing.
        if self._password:
            return base.trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                http_scheme="https",
                auth=base.trino.auth.BasicAuthentication(self.user, self._password),
                verify=False,  # self-signed certificate
                session_properties=dict(SESSION_PROPERTIES),
                max_attempts=1,
            )
        return base.trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            session_properties=dict(SESSION_PROPERTIES),
            max_attempts=1,
        )

    def _execute_replacement(self, sql: str) -> None:
        """Run the tombstone replace-MERGE with strict replay protection.

        The MERGE ON clause never matches insert rows, so re-executing it
        after an ambiguous COMMIT outcome would re-apply already-committed
        inserts as duplicates; ``replay_safe=False`` makes a classified
        connection error raise ``TrinoAmbiguousWriteError`` instead of
        replaying.
        """
        return self._execute_committing(sql, replay_safe=False)
