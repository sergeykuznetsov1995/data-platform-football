"""Tests for strict (non-replay-safe) tombstone replace-MERGE execution.

Covers the ESPN opt-in hook (``EspnTrinoTableManager._execute_replacement``)
and the replay-safe default on ``TrinoTableManager``: an ambiguous connection
loss raises ``TrinoAmbiguousWriteError`` without replaying the statement,
genuine Iceberg commit conflicts still retry, and the atomic swap retains its
stage on ambiguity while dropping it on success.
"""

import unittest
from unittest.mock import Mock, patch

import pandas as pd
import requests

from scrapers.base import trino_manager as base
from scrapers.base.trino_manager import (
    TrinoAmbiguousWriteError,
    TrinoError,
    TrinoTableManager,
)
from scrapers.espn.trino_manager import EspnTrinoTableManager


class FakeConflictError(Exception):
    """Stands in for a TrinoExternalError with ICEBERG_COMMIT_ERROR."""

    error_name = "ICEBERG_COMMIT_ERROR"

    def __init__(self, message="Found conflicting files"):
        super().__init__(message)


class FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self.sql = None

    def execute(self, sql, params=None):
        self.sql = sql
        self._conn.executed.append(sql)
        if self._conn.on_execute is not None:
            self._conn.on_execute(sql)

    def fetchall(self):
        outcome = self._conn.next_outcome()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def close(self):
        if self._conn.cursor_close_error is not None:
            raise self._conn.cursor_close_error


class FakeConnection:
    """Scripted DBAPI connection: one fetchall outcome per statement."""

    def __init__(self, outcomes=(), cursor_close_error=None, on_execute=None):
        self.executed = []
        self.cursors = []
        self._outcomes = list(outcomes)
        self.cursor_close_error = cursor_close_error
        self.on_execute = on_execute
        self.closed = False

    def cursor(self):
        cursor = FakeCursor(self)
        self.cursors.append(cursor)
        return cursor

    def next_outcome(self):
        if self._outcomes:
            return self._outcomes.pop(0)
        return []

    def close(self):
        self.closed = True


class LegacyCommitManager(TrinoTableManager):
    """Pre-hook subclass: only the old committing API, no replay_safe."""

    def __init__(self):
        super().__init__()
        self.legacy_calls = []

    def _execute_committing(self, sql):
        self.legacy_calls.append((sql,))
        return TrinoTableManager._execute_committing(self, sql)


class StrictReplacementTests(unittest.TestCase):
    def test_commit_then_connection_reset_is_ambiguous_not_replayed(self):
        multiset = []
        conn_error = RuntimeError("Connection reset by peer")
        conn = FakeConnection(
            outcomes=[conn_error], on_execute=multiset.append
        )
        manager = EspnTrinoTableManager()
        manager._conn = conn
        sql = "MERGE INTO iceberg.bronze.t USING iceberg.bronze.stg"

        with self.assertRaises(TrinoAmbiguousWriteError) as ctx:
            manager._execute_replacement(sql)

        self.assertEqual(conn.executed, [sql])
        self.assertEqual(multiset, [sql])
        self.assertIs(ctx.exception.__cause__, conn_error)
        self.assertIsInstance(ctx.exception, TrinoError)
        self.assertTrue(conn.closed)

    def test_strict_connection_loss_is_not_retried(self):
        conn = FakeConnection(
            outcomes=[RuntimeError("Connection aborted by peer")]
        )
        manager = EspnTrinoTableManager()
        manager._conn = conn
        sql = "MERGE INTO iceberg.bronze.t USING iceberg.bronze.stg"

        with patch.object(base.time, "sleep") as sleep:
            with self.assertRaises(TrinoAmbiguousWriteError):
                manager._execute_committing(sql, replay_safe=False)

        self.assertEqual(conn.executed, [sql])
        sleep.assert_not_called()

    def test_reset_and_close_failures_cannot_mask_ambiguity(self):
        conn_error = RuntimeError(
            "Connection reset by peer: Found conflicting files"
        )
        conn = FakeConnection(
            outcomes=[conn_error],
            cursor_close_error=RuntimeError("cursor close failed"),
        )
        manager = EspnTrinoTableManager()
        manager._conn = conn
        manager._reset_connection = Mock(
            side_effect=RuntimeError("reset failed")
        )

        with patch.object(base.time, "sleep") as sleep:
            with self.assertRaises(TrinoAmbiguousWriteError) as ctx:
                manager._execute_replacement("MERGE INTO iceberg.bronze.t")

        self.assertEqual(len(conn.executed), 1)
        self.assertIs(ctx.exception.__cause__, conn_error)
        sleep.assert_not_called()

    def test_conflict_then_success_still_retries_in_strict_mode(self):
        conn = FakeConnection(outcomes=[FakeConflictError(), []])
        manager = EspnTrinoTableManager()
        manager._conn = conn
        sql = "MERGE INTO iceberg.bronze.t USING iceberg.bronze.stg"

        with patch.object(base.time, "sleep") as sleep:
            manager._execute_replacement(sql)

        self.assertEqual(conn.executed, [sql, sql])
        self.assertEqual(sleep.call_count, 1)


class DefaultReplacementHookTests(unittest.TestCase):
    def test_default_path_replays_after_connection_reset(self):
        conn1 = FakeConnection(
            outcomes=[RuntimeError("Connection reset by peer")]
        )
        conn2 = FakeConnection(outcomes=[[], []])
        manager = TrinoTableManager()
        manager._conn = conn1
        manager._create_connection = Mock(return_value=conn2)
        sql = "INSERT INTO iceberg.bronze.t VALUES (1)"

        self.assertIsNone(manager._execute_replacement(sql))

        self.assertEqual(conn1.executed, [sql])
        self.assertTrue(conn1.closed)
        self.assertEqual(conn2.executed, ["SELECT 1", sql])
        self.assertEqual(manager._create_connection.call_count, 1)

    def test_legacy_committing_override_receives_no_new_kwarg(self):
        conn = FakeConnection(outcomes=[[]])
        manager = LegacyCommitManager()
        manager._conn = conn
        sql = "MERGE INTO iceberg.bronze.t USING iceberg.bronze.stg"

        self.assertIsNone(manager._execute_replacement(sql))

        self.assertEqual(manager.legacy_calls, [(sql,)])
        self.assertEqual(conn.executed, [sql])


class EspnConnectionFactoryTests(unittest.TestCase):
    """The ESPN factories must opt out of the SDK's hidden HTTP retries."""

    def _connect_kwargs(self, password):
        manager = EspnTrinoTableManager()
        manager._password = password
        connect = Mock(return_value=object())
        with patch.object(base.trino.dbapi, "connect", connect):
            connection = manager._create_connection()
        self.assertIs(connection, connect.return_value)
        return connect.call_args.kwargs

    def test_https_factory_passes_max_attempts_one(self):
        kwargs = self._connect_kwargs("synthetic-no-secret")
        self.assertEqual(kwargs["max_attempts"], 1)
        self.assertEqual(kwargs["http_scheme"], "https")
        self.assertIsInstance(
            kwargs["auth"], base.trino.auth.BasicAuthentication
        )

    def test_http_factory_passes_max_attempts_one(self):
        kwargs = self._connect_kwargs(None)
        self.assertEqual(kwargs["max_attempts"], 1)
        self.assertNotIn("auth", kwargs)


class StrictConflictRetryTests(unittest.TestCase):
    """Strict mode retries only a definite rejected commit."""

    sql = "MERGE INTO iceberg.bronze.t USING iceberg.bronze.stg"

    def _strict_manager(self, outcomes):
        conn = FakeConnection(outcomes=outcomes)
        manager = EspnTrinoTableManager()
        manager._conn = conn
        return conn, manager

    def test_structured_generic_commit_error_is_not_retried(self):
        generic = FakeConflictError("Failed to commit the transaction")
        conn, manager = self._strict_manager([generic])

        with patch.object(base.time, "sleep") as sleep:
            with self.assertRaises(TrinoError) as ctx:
                manager._execute_committing(self.sql, replay_safe=False)

        self.assertEqual(conn.executed, [self.sql])
        sleep.assert_not_called()
        self.assertNotIsInstance(ctx.exception, TrinoAmbiguousWriteError)
        self.assertIs(ctx.exception.__cause__, generic)

    def test_unstructured_commit_message_is_not_retried(self):
        generic = RuntimeError("Failed to commit the transaction")
        conn, manager = self._strict_manager([generic])

        with patch.object(base.time, "sleep") as sleep:
            with self.assertRaises(TrinoError) as ctx:
                manager._execute_committing(self.sql, replay_safe=False)

        self.assertEqual(conn.executed, [self.sql])
        sleep.assert_not_called()
        self.assertIs(ctx.exception.__cause__, generic)

    def test_strict_conflict_message_match_is_case_insensitive(self):
        conflict = FakeConflictError(
            "Iceberg could not commit: FOUND CONFLICTING FILES in snapshot"
        )
        conn, manager = self._strict_manager([conflict, []])

        with patch.object(base.time, "sleep") as sleep:
            manager._execute_committing(self.sql, replay_safe=False)

        self.assertEqual(conn.executed, [self.sql, self.sql])
        self.assertEqual(sleep.call_count, 1)

    def test_default_mode_generic_commit_message_still_retries(self):
        generic = RuntimeError("Failed to commit the transaction")
        conn = FakeConnection(outcomes=[generic, []])
        manager = TrinoTableManager()
        manager._conn = conn

        with patch.object(base.time, "sleep") as sleep:
            manager._execute_committing(self.sql)

        self.assertEqual(conn.executed, [self.sql, self.sql])
        self.assertEqual(sleep.call_count, 1)


class SdkTransportRetryTests(unittest.TestCase):
    """The real SDK transport must not replay a reset initial POST."""

    def test_max_attempts_one_sends_initial_post_only_once(self):
        if not base.TRINO_AVAILABLE:
            self.skipTest("trino package not installed")
        sent = Mock(
            side_effect=requests.exceptions.ConnectionError(
                "Connection reset by peer"
            )
        )
        with patch.object(requests.Session, "post", sent), \
                patch.object(requests.Session, "request", sent):
            conn = base.trino.dbapi.connect(
                host="localhost",
                port=1,
                user="airflow",
                catalog="default",
                max_attempts=1,
            )
            cursor = conn.cursor()
            with self.assertRaises(Exception):
                cursor.execute("SELECT 1")
                cursor.fetchall()
        self.assertEqual(sent.call_count, 1)


class AtomicReplacementStageTests(unittest.TestCase):
    df = pd.DataFrame({"match_id": [101, 102], "stat": ["a", "b"]})
    replace_kwargs = dict(
        delete_filter="stat_scope = 'season'",
        single_statement_replace=True,
        target_column_types={"match_id": "INTEGER", "stat": "VARCHAR"},
    )

    def test_ambiguous_final_merge_retains_stage(self):
        conn = FakeConnection(
            outcomes=[
                [],
                [],
                [[2]],
                [],
                RuntimeError("Connection reset by peer"),
            ]
        )
        manager = EspnTrinoTableManager()
        manager._conn = conn

        with self.assertRaises(TrinoAmbiguousWriteError):
            manager.insert_dataframe_atomic(
                "bronze", "espn_stats", self.df, **self.replace_kwargs
            )

        merges = [s for s in conn.executed if s.startswith("MERGE INTO")]
        self.assertEqual(len(merges), 1)
        self.assertFalse(
            [s for s in conn.executed if s.startswith("DROP TABLE")]
        )

    def test_successful_final_merge_drops_stage(self):
        conn = FakeConnection(outcomes=[[], [], [[2]], [], [], []])
        manager = EspnTrinoTableManager()
        manager._conn = conn

        inserted = manager.insert_dataframe_atomic(
            "bronze", "espn_stats", self.df, **self.replace_kwargs
        )

        self.assertEqual(inserted, 2)
        merges = [s for s in conn.executed if s.startswith("MERGE INTO")]
        self.assertEqual(len(merges), 1)
        self.assertTrue(
            any(s.startswith("DROP TABLE") for s in conn.executed)
        )


if __name__ == "__main__":
    unittest.main()
