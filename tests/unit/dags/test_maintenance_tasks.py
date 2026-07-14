"""
Unit tests for ``dags/utils/maintenance_tasks.py``.

Covers the small but load-bearing constants that drive the daily Iceberg
maintenance DAG. Without a delete-then-insert table in ``HIGH_CHURN_BRONZE``
it accumulates daily ``INSERT`` snapshots and old warehouse objects
indefinitely. The Apr 2026 incident footprint included old data and metadata;
current table data itself was much smaller. Tests below also cover bounded
compaction without issuing live DML.
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

    def test_all_whoscored_v2_objects_receive_daily_maintenance(self):
        from scripts.whoscored_v2_object_contract import (
            BUSINESS_TABLES,
            MANIFEST_TABLES,
        )
        from utils.maintenance_tasks import WHOSCORED_HIGH_CHURN

        assert set(WHOSCORED_HIGH_CHURN) == (
            set(BUSINESS_TABLES)
            | set(MANIFEST_TABLES)
            | {"whoscored_backfill_dq_population"}
        )

    def test_frozen_dq_stage_receives_snapshot_and_orphan_maintenance(self):
        from utils.maintenance_tasks import (
            HIGH_CHURN_BRONZE,
            WHOSCORED_OPERATIONAL_HIGH_CHURN,
        )

        assert WHOSCORED_OPERATIONAL_HIGH_CHURN == ("whoscored_backfill_dq_population",)
        assert set(WHOSCORED_OPERATIONAL_HIGH_CHURN) <= set(HIGH_CHURN_BRONZE)

    def test_whoscored_has_longer_daily_rollback_window(self):
        from utils.maintenance_tasks import (
            DEFAULT_RETENTION,
            NON_WHOSCORED_HIGH_CHURN,
            OTHER_HIGH_CHURN_DAILY_RETENTION,
            WHOSCORED_DAILY_RETENTION,
            WHOSCORED_HIGH_CHURN,
        )

        assert WHOSCORED_DAILY_RETENTION == "14d"
        assert OTHER_HIGH_CHURN_DAILY_RETENTION == "3d"
        # The weekly all-table sweep must not truncate the 14d WhoScored
        # rollback window promised by the daily split maintenance.
        assert DEFAULT_RETENTION == "30d"
        assert set(NON_WHOSCORED_HIGH_CHURN).isdisjoint(WHOSCORED_HIGH_CHURN)

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


@pytest.mark.unit
class TestFrozenDQLogicalRetention:
    def test_scheduled_cleanup_is_bounded_and_closes_connection(self, monkeypatch):
        from dags.scripts import whoscored_frozen_dq
        import utils.maintenance_tasks as maintenance

        calls: list[dict] = []

        class Cursor:
            closed = False

            def close(self):
                self.closed = True

        class Connection:
            def __init__(self):
                self.closed = False
                self.cur = Cursor()

            def cursor(self):
                return self.cur

            def close(self):
                self.closed = True

        conn = Connection()
        monkeypatch.setattr(maintenance, "_connect", lambda: conn)
        monkeypatch.setattr(
            maintenance,
            "_list_tables",
            lambda _conn, _schema: [whoscored_frozen_dq.DQ_STAGE_TABLE],
        )

        def _cleanup(_cur, **kwargs):
            calls.append(kwargs)
            return 7

        monkeypatch.setattr(
            whoscored_frozen_dq,
            "cleanup_staged_frozen_populations",
            _cleanup,
        )

        result = maintenance.cleanup_whoscored_dq_stage_partitions()

        assert result == {
            "status": "success",
            "retention_days": whoscored_frozen_dq.DQ_STAGE_RETENTION_DAYS,
            "partitions_deleted": 7,
        }
        assert calls == [
            {"retention_days": whoscored_frozen_dq.DQ_STAGE_RETENTION_DAYS}
        ]
        assert conn.cur.closed is True
        assert conn.closed is True

    def test_scheduled_cleanup_skips_an_absent_stage_table(self, monkeypatch):
        import utils.maintenance_tasks as maintenance

        class Connection:
            closed = False

            def close(self):
                self.closed = True

        conn = Connection()
        monkeypatch.setattr(maintenance, "_connect", lambda: conn)
        monkeypatch.setattr(maintenance, "_list_tables", lambda _conn, _schema: [])

        result = maintenance.cleanup_whoscored_dq_stage_partitions()

        assert result == {
            "status": "skipped",
            "reason": "stage_table_missing",
            "partitions_deleted": 0,
        }
        assert conn.closed is True

    def test_scheduled_cleanup_fails_closed_and_releases_resources(self, monkeypatch):
        from dags.scripts import whoscored_frozen_dq
        import utils.maintenance_tasks as maintenance

        class Cursor:
            closed = False

            def close(self):
                self.closed = True

        class Connection:
            def __init__(self):
                self.closed = False
                self.cur = Cursor()

            def cursor(self):
                return self.cur

            def close(self):
                self.closed = True

        conn = Connection()
        monkeypatch.setattr(maintenance, "_connect", lambda: conn)
        monkeypatch.setattr(
            maintenance,
            "_list_tables",
            lambda _conn, _schema: [whoscored_frozen_dq.DQ_STAGE_TABLE],
        )
        monkeypatch.setattr(
            whoscored_frozen_dq,
            "cleanup_staged_frozen_populations",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("cleanup failed")
            ),
        )

        with pytest.raises(RuntimeError, match="cleanup failed"):
            maintenance.cleanup_whoscored_dq_stage_partitions()

        assert conn.cur.closed is True
        assert conn.closed is True


class _MetadataCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None

    def execute(self, sql):
        self.connection.statements.append(sql)

    def fetchall(self):
        return list(self.connection.rows)

    def close(self):
        self.connection.closed_cursors += 1


class _MetadataConnection:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.statements: list[str] = []
        self.closed_cursors = 0
        self.closed = False

    def cursor(self):
        return _MetadataCursor(self)

    def close(self):
        self.closed = True


def _candidate_probe_result(maintenance, table: str, *, skipped: int = 0):
    return maintenance._CompactionProbeResult(
        (
            (f"s3://bucket/{table}/a.parquet", 10),
            (f"s3://bucket/{table}/b.parquet", 20),
        ),
        skipped,
    )


@pytest.mark.unit
class TestBoundedLiveFileCompaction:
    def test_candidate_probe_is_exact_deterministic_and_sql_bounded(self):
        import utils.maintenance_tasks as maintenance

        conn = _MetadataConnection(
            [
                ("s3://bucket/a.parquet", 10, 3),
                ("s3://bucket/b.parquet", 20, 3),
            ]
        )

        probe = maintenance._compaction_candidates(
            conn,
            schema="bronze",
            table="whoscored_events",
            max_input_bytes=maintenance.COMPACTION_MAX_INPUT_BYTES_PER_TABLE,
        )

        assert probe == maintenance._CompactionProbeResult(
            (
                ("s3://bucket/a.parquet", 10),
                ("s3://bucket/b.parquet", 20),
            ),
            3,
        )
        assert conn.closed_cursors == 1
        sql = " ".join(conn.statements[0].split())
        assert '"whoscored_events$files"' in sql
        assert "$partitions" not in sql
        assert "partition_inventory AS" in sql
        assert "count_if(content IS DISTINCT FROM 0) AS delete_files_count" in sql
        assert "small_data_files_count >= 2" in sql
        assert "delete_files_count = 0" in sql
        assert "ORDER BY first_small_file_path" in sql
        assert "IS NOT DISTINCT FROM" in sql
        assert f"LIMIT {maintenance.COMPACTION_DISCOVERY_MAX_FILES}" in sql
        assert "row_number() OVER (ORDER BY file_path)" in sql
        assert "sum(file_size_in_bytes) OVER" in sql
        assert f"file_rank <= {maintenance.COMPACTION_MAX_FILES_PER_TABLE}" in sql
        assert (
            f"running_bytes <= {maintenance.COMPACTION_MAX_INPUT_BYTES_PER_TABLE}"
            in sql
        )
        assert "AS skipped_delete_partitions" in sql
        assert "WHERE NOT EXISTS (SELECT 1 FROM selected_files)" in sql

    def test_mixed_and_singleton_delete_partitions_are_skipped_by_contract(self):
        import utils.maintenance_tasks as maintenance

        conn = _MetadataConnection([(None, None, 2)])

        probe = maintenance._compaction_candidates(
            conn,
            schema="bronze",
            table="mixed_delete_table",
            max_input_bytes=maintenance.COMPACTION_MAX_INPUT_BYTES_PER_TABLE,
        )

        assert probe.candidates == ()
        assert probe.skipped_delete_partitions == 2
        sql = " ".join(conn.statements[0].split())
        # The inventory excludes both a mixed partition and a singleton data
        # file with an attached delete before exact paths can reach OPTIMIZE.
        assert "count_if(content IS DISTINCT FROM 0)" in sql
        assert "small_data_files_count >= 2" in sql
        assert "delete_files_count = 0" in sql
        assert (
            maintenance.COMPACTION_DELETE_FILE_POLICY
            == "skip_partitions_with_live_delete_files"
        )

    @pytest.mark.parametrize(
        "rows, message",
        [
            (
                [(f"s3://bucket/{index}.parquet", 1, 0) for index in range(65)],
                "exceeded its SQL bounds",
            ),
            (
                [
                    ("s3://bucket/a.parquet", 300 * 1024 * 1024, 0),
                    ("s3://bucket/b.parquet", 300 * 1024 * 1024, 0),
                ],
                "file size is invalid",
            ),
        ],
    )
    def test_candidate_probe_revalidates_metadata_bounds(self, rows, message):
        import utils.maintenance_tasks as maintenance

        with pytest.raises(RuntimeError, match=message):
            maintenance._compaction_candidates(
                _MetadataConnection(rows),
                schema="bronze",
                table="events",
                max_input_bytes=maintenance.COMPACTION_MAX_INPUT_BYTES_PER_TABLE,
            )

    def test_optimize_uses_only_injection_safe_exact_paths(self):
        import utils.maintenance_tasks as maintenance

        dangerous = "s3://bucket/a'); DROP TABLE bronze.events; --.parquet"
        conn = _MetadataConnection()

        maintenance._compact_exact_files(
            conn,
            schema='bro"nze',
            table='eve"nts',
            candidates=((dangerous, 10), ("s3://bucket/safe.parquet", 20)),
        )

        sql = conn.statements[0]
        assert 'ALTER TABLE iceberg."bro""nze"."eve""nts"' in sql
        assert 'WHERE "$path" IN (' in sql
        assert "a''); DROP TABLE bronze.events; --.parquet'" in sql
        assert "EXECUTE optimize(file_size_threshold => '64MB')" in sql

    @pytest.mark.parametrize(
        "candidates, message",
        [
            ((("s3://bucket/a.parquet", 1),), "count is outside"),
            (
                (
                    ("s3://bucket/a.parquet", 1),
                    ("s3://bucket/a.parquet", 1),
                ),
                "candidates are invalid",
            ),
            (
                (
                    ("s3://bucket/a\n.parquet", 1),
                    ("s3://bucket/b.parquet", 1),
                ),
                "control character",
            ),
            (
                (
                    ("s3://bucket/a.parquet", 100 * 1024 * 1024),
                    ("s3://bucket/b.parquet", 100 * 1024 * 1024),
                ),
                "candidates are invalid",
            ),
            (
                tuple(
                    (f"s3://bucket/{index}.parquet", 60 * 1024 * 1024)
                    for index in range(9)
                ),
                "input exceeds its per-table bound",
            ),
            (
                tuple((f"s3://bucket/{index}.parquet", 1) for index in range(65)),
                "count is outside",
            ),
        ],
    )
    def test_optimize_rejects_invalid_exact_path_sets(self, candidates, message):
        import utils.maintenance_tasks as maintenance

        with pytest.raises(RuntimeError, match=message):
            maintenance._compact_exact_files(
                _MetadataConnection(),
                schema="bronze",
                table="events",
                candidates=candidates,
            )

    def test_rotation_covers_eight_perpetual_candidates_in_two_runs(self, monkeypatch):
        import utils.maintenance_tasks as maintenance

        tables = [f"table_{index}" for index in range(8)]
        compacted_by_run: list[list[str]] = []
        retained_by_run: list[list[str]] = []

        monkeypatch.setattr(maintenance, "_connect", lambda: _MetadataConnection())
        monkeypatch.setattr(maintenance, "_list_tables", lambda _conn, _schema: tables)
        monkeypatch.setattr(
            maintenance,
            "_compaction_candidates",
            lambda _conn, *, table, **_kwargs: _candidate_probe_result(
                maintenance, table, skipped=1
            ),
        )

        current_compacted: list[str] = []
        current_retained: list[str] = []

        def _compact(_conn, *, table, **_kwargs):
            current_compacted.append(table)
            return {
                "rewritten_data_files_count": 2,
                "added_data_files_count": 1,
                "removed_delete_files_count": 0,
            }

        def _retain(_conn, fq, _retention):
            current_retained.append(fq)
            return {"scanned_files_count": 2, "deleted_files_count": 1}

        monkeypatch.setattr(maintenance, "_compact_exact_files", _compact)
        monkeypatch.setattr(maintenance, "_maintain_one", _retain)

        results = []
        for rotation in (0, 1):
            current_compacted = []
            current_retained = []
            results.append(
                maintenance.maintain_iceberg_tables(
                    schemas=("bronze",),
                    compact_live_files=True,
                    compaction_rotation=rotation,
                )
            )
            compacted_by_run.append(current_compacted)
            retained_by_run.append(current_retained)

        assert compacted_by_run == [tables[:4], tables[4:]]
        assert set(compacted_by_run[0] + compacted_by_run[1]) == set(tables)
        assert all(len(retained) == len(tables) for retained in retained_by_run)
        assert all(
            result["compaction_tables_selected"]
            <= maintenance.COMPACTION_MAX_TABLES_PER_RUN
            for result in results
        )
        assert all(
            result["compaction_input_bytes_selected"]
            <= maintenance.COMPACTION_MAX_INPUT_BYTES_PER_RUN
            for result in results
        )
        assert results[0]["compaction_rewritten_data_files"] == 8
        assert results[0]["retention_tables_succeeded"] == len(tables)
        assert results[0]["compaction_delete_partitions_skipped"] == len(tables)
        assert results[0]["compaction_delete_file_policy"] == (
            "skip_partitions_with_live_delete_files"
        )

    def test_compaction_failure_is_reported_once_and_retention_continues(
        self, monkeypatch
    ):
        import utils.maintenance_tasks as maintenance

        monkeypatch.setattr(maintenance, "_connect", lambda: _MetadataConnection())
        monkeypatch.setattr(
            maintenance,
            "_list_tables",
            lambda _conn, _schema: ["broken", "healthy"],
        )
        monkeypatch.setattr(
            maintenance,
            "_compaction_candidates",
            lambda _conn, *, table, **_kwargs: _candidate_probe_result(
                maintenance, table
            ),
        )

        def _compact(_conn, *, table, **_kwargs):
            if table == "broken":
                raise RuntimeError("optimize failed")
            return {}

        retained: list[str] = []

        def _retain(_conn, fq, _retention):
            retained.append(fq)
            return {}

        monkeypatch.setattr(maintenance, "_compact_exact_files", _compact)
        monkeypatch.setattr(maintenance, "_maintain_one", _retain)

        result = maintenance.maintain_iceberg_tables(
            schemas=("bronze",),
            compact_live_files=True,
            compaction_rotation=0,
        )

        assert retained == [
            'iceberg."bronze"."broken"',
            'iceberg."bronze"."healthy"',
        ]
        assert result["retention_tables_succeeded"] == 2
        assert result["compaction_tables_succeeded"] == 1
        assert len(result["failures"]) == 1
        assert result["failures"][0][0] == "iceberg.bronze.broken"
        assert "compaction: optimize failed" in result["failures"][0][1]

    def test_multiple_stage_errors_collapse_to_one_failure_per_table(self, monkeypatch):
        import utils.maintenance_tasks as maintenance

        monkeypatch.setattr(maintenance, "_connect", lambda: _MetadataConnection())
        monkeypatch.setattr(
            maintenance,
            "_list_tables",
            lambda _conn, _schema: ["broken", "healthy"],
        )

        def _probe(_conn, *, table, **_kwargs):
            if table == "broken":
                raise RuntimeError("metadata failed")
            return maintenance._CompactionProbeResult((), 0)

        def _retain(_conn, fq, _retention):
            if fq.endswith('"broken"'):
                raise RuntimeError("retention failed")
            return {}

        monkeypatch.setattr(maintenance, "_compaction_candidates", _probe)
        monkeypatch.setattr(maintenance, "_maintain_one", _retain)

        result = maintenance.maintain_iceberg_tables(
            schemas=("bronze",),
            compact_live_files=True,
            compaction_rotation=0,
        )

        assert result["retention_tables_succeeded"] == 1
        assert len(result["failures"]) == 1
        assert "compaction_probe: metadata failed" in result["failures"][0][1]
        assert "retention: retention failed" in result["failures"][0][1]

    def test_successful_compaction_cannot_mask_systemic_retention_failure(
        self, monkeypatch
    ):
        import utils.maintenance_tasks as maintenance

        monkeypatch.setattr(maintenance, "_connect", lambda: _MetadataConnection())
        monkeypatch.setattr(
            maintenance,
            "_list_tables",
            lambda _conn, _schema: ["first", "second"],
        )
        monkeypatch.setattr(
            maintenance,
            "_compaction_candidates",
            lambda _conn, *, table, **_kwargs: _candidate_probe_result(
                maintenance, table
            ),
        )
        monkeypatch.setattr(
            maintenance, "_compact_exact_files", lambda *_args, **_kwargs: {}
        )
        retention_calls: list[str] = []

        def _retain(_conn, fq, _retention):
            retention_calls.append(fq)
            raise RuntimeError(f"cannot retain {fq}")

        monkeypatch.setattr(maintenance, "_maintain_one", _retain)

        with pytest.raises(
            RuntimeError,
            match="Iceberg maintenance failed on all 2 tables",
        ):
            maintenance.maintain_iceberg_tables(
                schemas=("bronze",),
                compact_live_files=True,
                compaction_rotation=0,
            )

        assert len(retention_calls) == 2
