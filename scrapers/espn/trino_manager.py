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
    """``TrinoTableManager`` whose connections disable dynamic filtering."""

    def _create_connection(self):
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
            )
        return base.trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            session_properties=dict(SESSION_PROPERTIES),
        )
