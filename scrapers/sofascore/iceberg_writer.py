"""Partition-scoped Bronze MERGEs without changing the shared writer contract."""

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base.trino_manager import TrinoTableManager


# These tables use identity partitions (league, season) and include both
# columns in their natural keys. rejected_rows has a different natural key;
# the ops capture manifest has different partition columns. Neither is scoped.
PARTITION_SCOPED_TABLES = frozenset({
    "sofascore_schedule",
    "sofascore_league_table",
    "sofascore_match_stats",
    "sofascore_event_shotmap",
    "sofascore_venue",
    "sofascore_events",
    "sofascore_event_participants",
    "sofascore_incidents",
    "sofascore_lineups",
    "sofascore_event_player_stats",
    "sofascore_player_ratings",
    "sofascore_match_capture_status",
    "sofascore_player_universe",
    "sofascore_player_profile",
    "sofascore_player_season_stats",
})


class SofaScoreTrinoTableManager(TrinoTableManager):
    def insert_dataframe_atomic(self, schema, table, df, **kwargs):
        if schema == "bronze" and table in PARTITION_SCOPED_TABLES and kwargs.get("merge_keys"):
            kwargs["merge_partition_cols"] = ["league", "season"]
        return super().insert_dataframe_atomic(schema, table, df, **kwargs)


class SofaScoreIcebergWriter(IcebergWriter):
    def _get_trino_manager(self):
        if self._trino_manager is None:
            self._trino_manager = SofaScoreTrinoTableManager(
                host=self.trino_host,
                port=self.trino_port,
                catalog=self.catalog,
            )
        return self._trino_manager
