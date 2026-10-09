"""Инструментирование Trino-клиента FotMob: ``source=`` и слоу-лог (#1284).

Две полосы FotMob получают подпись в очереди Trino и общий слоу-лог (#1284).
Менеджер также связывает каждый запрос физической записи с проверяемым
lease PostgreSQL: потеря замка отменяет активный запрос и запрещает следующий.
Обработанное прерывание убирает stage только до изменения целевой таблицы.

* :class:`FotMobTrinoTableManager` — добавляет в ``connect(...)`` ``source=``
  (и при необходимости ``session_properties``) и пишет в лог одну строку
  ``FotMob slow SQL`` на каждый запрос дольше :data:`FOTMOB_SLOW_SQL_SECONDS`.
* :class:`FotMobIcebergWriter` — писатель bronze, который строит именно такой
  менеджер с подписью ``fotmob:<полоса>:<run_id>``.

Базовые модули ``scrapers/base/trino_manager.py`` и
``scrapers/base/iceberg_writer.py`` опечатаны контрактом WhoScored
(``scrapers/whoscored/runtime_contract.lock``) — их можно импортировать, но
не править, поэтому инструментирование живёт отдельным модулем FotMob.
"""

from __future__ import annotations

import logging
import re
import sys
import threading
import time
from typing import Any, List, Mapping, Optional

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base.trino_manager import TrinoTableManager
from scrapers.fotmob.writer_lock import WriterLockBusy, WriterLockLost

logger = logging.getLogger(__name__)

# Порог слоу-лога. Медианы тяжёлых функций волны «до» — 6,9-31 с (#1283):
# при пороге 30 с половина вызовов не попала бы в лог вовсе, а после компакции
# не попал бы ни один, и сравнить «до/после» одним инструментом было бы нечем.
FOTMOB_SLOW_SQL_SECONDS = 5.0

# Сколько символов SQL смотреть и сколько оставить в строке лога: длинные
# INSERT'ы волны дают сотни килобайт на запрос, в логе нужен только префикс.
SQL_SCAN_CHARS = 2000
SQL_LOG_CHARS = 400

_HISTORY_RUN_PREFIX = "fotmob-hist-"

_WHITESPACE_RE = re.compile(r"\s+")
_IN_LIST_RE = re.compile(r"\bIN\s*\(([^()]*)\)", re.IGNORECASE)


def _now_seconds() -> float:
    """Часы слоу-лога (монотонные); отдельной функцией — чтобы их подменяли тесты."""

    return time.perf_counter()


def fotmob_query_source(run_id: str) -> str:
    """Подпись запросов рана в очереди Trino: полоса и идентификатор рана."""

    lane = "history" if str(run_id).startswith(_HISTORY_RUN_PREFIX) else "current"
    return f"fotmob:{lane}:{run_id}"


def normalize_sql_for_log(sql: object) -> str:
    """Свернуть SQL в одну короткую строку: пробелы схлопнуты, списки — числом."""

    text = _WHITESPACE_RE.sub(" ", str(sql)[:SQL_SCAN_CHARS]).strip()

    def _shrink(match: "re.Match[str]") -> str:
        items = [item for item in match.group(1).split(",") if item.strip()]
        return f"IN (…{len(items)}…)"

    return _IN_LIST_RE.sub(_shrink, text)[:SQL_LOG_CHARS]


class FotMobTrinoTableManager(TrinoTableManager):
    """Базовый менеджер плюс подпись запроса и слоу-лог.

    ``source`` виден в ``system.runtime.queries`` — по нему полосы FotMob
    отделяются друг от друга и от остальных источников. ``wall`` в слоу-логе —
    время вокруг ``_execute``, то есть включает и повтор соединения базового
    клиента при обрыве, а не только исполнение запроса в Trino.
    """

    def __init__(
        self,
        *,
        source: str,
        session_properties: Optional[Mapping[str, str]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._source = source
        self._session_properties = (
            dict(session_properties) if session_properties else None
        )
        self._write_authority = None
        self._authority_required = False
        self._write_failure = None
        self._stage_state = threading.local()
        self._active_cursor = None
        self._active_cancelled = False

    def cancel_active_query(self) -> bool:
        """Best-effort cancellation for the runner's handled signal interruption."""
        cursor = self._active_cursor
        if cursor is None or self._active_cancelled:
            return False
        self._active_cancelled = True
        try:
            cursor.cancel()
            return True
        except Exception:
            logger.warning("FotMob active query cancellation failed", exc_info=True)
            return False

    def _close_cursor(self, cursor) -> None:
        error_type = sys.exc_info()[0]
        interrupted = error_type is not None
        if self._active_cancelled or (interrupted and not issubclass(error_type, Exception)):
            # Trino cursor.close() is another cancellation RPC. TERM already
            # attempted it and the cancelled connection is reset locally.
            return
        try:
            cursor.close()
        except Exception:
            if not interrupted:
                raise
            logger.warning("FotMob cursor cleanup failed during interruption", exc_info=True)

    def set_write_authority(self, lease_or_none) -> None:
        """Bind the repository's physical-write guard to all SQL in that guard."""
        self._write_authority = lease_or_none or None
        if self._write_authority is not None:
            self._authority_required = True

    def revoke_write_authority(self, error: WriterLockLost) -> None:
        """Latch loss found by the repository boundary, before it unbinds us."""
        self._write_failure = error

    def _check_write_authority(self) -> None:
        if self._write_failure is not None:
            raise self._write_failure
        if self._write_authority is not None:
            try:
                self._write_authority.check()
            except WriterLockLost as exc:
                self._write_failure = exc
                raise

    def _create_connection(self):
        """Соединение базового клиента с добавленным ``source``."""

        from scrapers.base import trino_manager as base

        self._check_write_authority()

        # Internal HTTP retries can replay a POST after a lost response without
        # another lease check. The repository owns uncertain-write reconciliation.
        extra: dict[str, Any] = {"source": self._source, "max_attempts": 1,
                                 "request_timeout": (3, 5)}
        if self._session_properties:
            extra["session_properties"] = dict(self._session_properties)

        if self._password:
            return base.trino.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                http_scheme='https',
                auth=base.trino.auth.BasicAuthentication(self.user, self._password),
                verify=False,  # self-signed certificate
                **extra,
            )
        return base.trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            **extra,
        )

    def _connect_with_retry(self):
        self._check_write_authority()
        if self._write_authority is None:
            return super()._connect_with_retry()
        from scrapers.base import trino_manager as base

        last_error = None
        for attempt in range(self._CONNECT_RETRIES):
            self._check_write_authority()
            conn = cursor = None
            try:
                conn = self._create_connection()
                self._check_write_authority()
                cursor = conn.cursor()
                self._active_cursor = cursor
                self._active_cancelled = False
                with self._write_authority.watch(self.cancel_active_query):
                    self._check_write_authority()
                    cursor.execute("SELECT 1")
                    cursor.fetchall()
                self._conn = conn
                base.TrinoTableManager._trino_unreachable = False
                return
            except WriterLockLost as exc:
                self._write_failure = exc
                raise
            except Exception as exc:
                self._check_write_authority()
                last_error = exc
                if attempt < self._CONNECT_RETRIES - 1:
                    time.sleep(self._CONNECT_BACKOFF[min(attempt, len(self._CONNECT_BACKOFF) - 1)])
            except BaseException:
                self.cancel_active_query()
                raise
            finally:
                if cursor is not None:
                    self._active_cursor = None
                    self._close_cursor(cursor)
                if conn is not None and conn is not self._conn:
                    conn.close()
        self._check_write_authority()
        base.TrinoTableManager._trino_unreachable = True
        raise base.TrinoError(f"Failed to connect to Trino: {last_error}")

    def _execute_guarded(self, sql, fetch, params):
        from scrapers.base import trino_manager as base

        with self._conn_lock:
            for attempt in range(2):
                self._check_write_authority()
                cursor = self.connection.cursor()
                self._active_cursor = cursor
                self._active_cancelled = False
                try:
                    with self._write_authority.watch(self.cancel_active_query):
                        self._check_write_authority()
                        cursor.execute(sql, params) if params else cursor.execute(sql)
                        rows = cursor.fetchall()
                    return rows if fetch else None
                except WriterLockLost as exc:
                    self._write_failure = exc
                    raise
                except Exception as exc:
                    self._check_write_authority()
                    read = re.match(r"\s*(?:SELECT|SHOW|DESCRIBE|DESC|WITH)\b", sql, re.I)
                    if read and attempt == 0 and any(msg in str(exc) for msg in self._CONNECTION_ERRORS):
                        self._reset_connection()
                        continue
                    raise base.TrinoError(f"SQL execution failed: {exc}") from exc
                except BaseException:
                    # Handled TERM/KeyboardInterrupt can be followed by an
                    # interrupted-manifest write under a fresh intact lease.
                    self.cancel_active_query()
                    self._reset_connection()
                    raise
                finally:
                    self._active_cursor = None
                    self._close_cursor(cursor)

    def insert_dataframe_atomic(self, schema, table, df, *args, **kwargs):
        """Extend the sealed base's stage lifecycle to handled interruptions.

        A target statement may have committed even when fetching its response
        failed. Its recovery stage must therefore survive every such failure.
        SIGKILL never enters this handler; separate stale-stage cleanup owns it.
        """
        previous = getattr(self._stage_state, "current", None)
        state = {"target": f"{self.catalog}.{schema}.{table}", "stages": set(),
                 "target_started": False}
        self._stage_state.current = state
        try:
            return super().insert_dataframe_atomic(schema, table, df, *args, **kwargs)
        except BaseException:
            if not state["target_started"]:
                for qualified_stage in tuple(state["stages"]):
                    try:
                        self._check_write_authority()
                        self.drop_table(schema, qualified_stage.rsplit(".", 1)[1], if_exists=True)
                    except WriterLockLost:
                        # Losing the lease forbids even a seemingly harmless DROP.
                        break
                    except Exception:
                        logger.warning("FotMob interrupted stage cleanup failed: %s",
                                       qualified_stage, exc_info=True)
            raise
        finally:
            self._stage_state.current = previous

    def _execute(
        self,
        sql: str,
        fetch: bool = False,
        params: Optional[tuple] = None,
    ) -> Optional[List[Any]]:
        started = _now_seconds()
        try:
            self._check_write_authority()
            if self._authority_required and self._write_authority is None:
                # Fail closed for unknown statements. Strip leading comments so
                # a comment cannot disguise a physical mutation as a read.
                statement = re.sub(r"\A(?:\s|--[^\n]*(?:\n|$)|/\*.*?\*/)*", "", sql,
                                   flags=re.S)
                read = re.match(r"(?:SELECT|SHOW|DESCRIBE|DESC|WITH)\b", statement, re.I)
                explain = re.match(r"EXPLAIN\b(?!\s+ANALYZE\b)", statement, re.I)
                if not (read or explain):
                    raise WriterLockBusy("FotMob mutation requires an active writer lease")
            state = getattr(self._stage_state, "current", None)
            drop = None
            if state is not None:
                create = re.match(r"\s*CREATE TABLE ([\w.]+__stg_[\w]+)\b", sql, re.I)
                if create:
                    state["stages"].add(create.group(1))
                mutation = re.match(r"\s*(?:DELETE FROM|INSERT INTO|MERGE INTO) ([\w.]+)\b", sql, re.I)
                if mutation and mutation.group(1) == state["target"]:
                    state["target_started"] = True
                drop = re.match(r"\s*DROP TABLE(?: IF EXISTS)? ([\w.]+)\b", sql, re.I)
            if self._write_authority is None:
                result = super()._execute(sql, fetch=fetch, params=params)
            else:
                result = self._execute_guarded(sql, fetch, params)
            if state is not None and drop:
                state["stages"].discard(drop.group(1))
            return result
        finally:
            elapsed = _now_seconds() - started
            if elapsed >= FOTMOB_SLOW_SQL_SECONDS:
                logger.warning(
                    "FotMob slow SQL: wall=%.1fs source=%s sql=%s",
                    elapsed,
                    self._source,
                    normalize_sql_for_log(sql),
                )


class FotMobIcebergWriter(IcebergWriter):
    """Писатель bronze FotMob: тот же Iceberg, но с подписанным соединением."""

    def __init__(self, *, run_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.run_id = run_id
        self.source = fotmob_query_source(run_id)

    def _get_trino_manager(self):
        if self._trino_manager is None:
            self._trino_manager = FotMobTrinoTableManager(
                source=self.source,
                host=self.trino_host,
                port=self.trino_port,
                catalog=self.catalog,
            )
        return self._trino_manager
