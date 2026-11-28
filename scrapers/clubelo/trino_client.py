"""Trino connections owned by the ClubElo Bronze source."""

import os

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base.trino_manager import TrinoTableManager
from scrapers.clubelo.trino_auth import connect, source_password


class ClubEloTrinoTableManager(TrinoTableManager):
    def __init__(self, *, host=None, port=None, catalog="iceberg"):
        password = source_password()
        super().__init__(
            host=host or os.environ.get("TRINO_HOST", "trino"),
            port=port or int(os.environ.get("TRINO_PORT", "8443")),
            user="src_clubelo",
            catalog=catalog,
        )
        self._password = password

    def _connect_with_retry(self):
        # A missing credential on reopen must not trip the shared unreachable flag.
        source_password()
        return super()._connect_with_retry()

    def _create_connection(self):
        return connect(host=self.host, port=self.port, catalog=self.catalog)


# ClubElo's bulk appends reach the catalog without Trino, so the snapshot is
# the only place that can name their writer. The Trino meter accepts an
# engine-less append on a ClubElo table only with this stamp (#2132).
SNAPSHOT_STAMP = {"dpf.source": "clubelo", "dpf.operation": "bulk-arrow-append"}


class ClubEloIcebergWriter(IcebergWriter):
    def __init__(self, *args, **kwargs):
        source_password()
        super().__init__(*args, **kwargs)

    def _get_trino_manager(self):
        if self._trino_manager is None:
            self._trino_manager = ClubEloTrinoTableManager(
                host=self.trino_host, port=self.trino_port, catalog=self.catalog
            )
        return self._trino_manager

    def _append_dataframe_pyiceberg(self, arrow_table, *, database: str, table: str) -> int:
        # The shared method commits the same frame without snapshot properties.
        target = self._load_pyiceberg_table(database=database, table=table)
        aligned = self._align_pyiceberg_arrow_table(arrow_table, target=target)
        target.append(aligned, snapshot_properties=dict(SNAPSHOT_STAMP))
        return len(aligned)
