"""Инструментирование Trino-клиента FotMob: ``source=`` и слоу-лог (#1284).

Зачем: обе полосы FotMob ходят в Trino под `source='trino-python-client'`
и `user='airflow'`, поэтому в очереди Trino 79,9 % запросов нельзя отнести ни
к волне актуалки, ни к юниту кампании (#1283). Здесь два наследника базовых
классов — они не меняют ни SQL, ни поведение записи:

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
import time
from typing import Any, List, Mapping, Optional

from scrapers.base.iceberg_writer import IcebergWriter
from scrapers.base.trino_manager import TrinoTableManager

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

    def _create_connection(self):
        """Соединение базового клиента с добавленным ``source``."""

        from scrapers.base import trino_manager as base

        extra: dict[str, Any] = {"source": self._source}
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

    def _execute(
        self,
        sql: str,
        fetch: bool = False,
        params: Optional[tuple] = None,
    ) -> Optional[List[Any]]:
        started = _now_seconds()
        try:
            return super()._execute(sql, fetch=fetch, params=params)
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
