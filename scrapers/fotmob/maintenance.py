"""Ночная компакция мелких файлов bronze FotMob (#1284).

Зачем: журнал записей ``fotmob_ingest_manifest`` разбит на ~64 000 файлов по
19 строк (10,7 КиБ на файл), и пять из шести тяжёлых функций волны актуалки
читают его целиком — время волны уходит на чтение мелких файлов (#1283).
Общий обслуживающий DAG склеивает 64 файла за прогон при приросте ~1 100 в
сутки, то есть не догоняет. Этот модуль склеивает их порциями под тем же
замком писателя, что берёт сама волна.

Запуск (хостовая обёртка ``deploy/fotmob/compaction.sh``)::

    docker exec fotmob-airflow-scheduler python -m scrapers.fotmob.maintenance

Установка обёртки — задача владельца-сессии, не автомата доставки::

    cp deploy/fotmob/compaction.sh /root/watchdog/fotmob_compaction.sh
    chmod 755 /root/watchdog/fotmob_compaction.sh
    crontab -l > /root/watchdog/crontab.prev-<дата>
    # 23:30 UTC летом (Europe/Moscow-хост в UTC+3 — 01:30 локального):
    30 1 * * * /root/watchdog/fotmob_compaction.sh
    # после перевода часов 25.10 строку сменить на `30 0 * * *`

Барьеры, из-за которых прогон безопасен рядом с боем:

* окно 23:30-23:50 UTC — новых волн нет с 19:00, фоновая полоса стартует 00:00;
* идёт волна или она кончилась меньше 30 минут назад — прогон не начинается;
* каждая порция берёт замок писателя B7 и ограничена сервером 180 секундами,
  новых порций после 23:47 нет — к 23:50 замок свободен при любой цене порции;
* занятый замок (юнит кампании истории) — не ошибка: прогон уходит зелёным.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)

SCHEMA = "bronze"

# Порядок — по измеренному вкладу в длительность волны (#1283 §4-§7): манифест
# читают функции 1/3/4 (70,2 % T), field_inventory джойнится функцией 1
# (50,8 КиБ на файл — тоже мелкие), squad_snapshots читает функция 2 (36,1 %).
FOTMOB_COMPACTION_TABLES: tuple[str, ...] = (
    "fotmob_ingest_manifest",
    "fotmob_field_inventory",
    "fotmob_squad_snapshots",
    "fotmob_team_snapshots",
    "fotmob_competition_seasons",
    "fotmob_season_stages",
)

# Кандидат в склейку — живой файл данных меньше этого порога. Порог кандидата
# намеренно НИЖЕ размера склеенного файла (порция ≤ 64 МиБ входа): выход
# компакции второй раз в кандидаты не попадает, и ночи не гоняют одно и то же.
SMALL_FILE_MAX_BYTES = 8 * 1024 * 1024
FILE_SIZE_THRESHOLD = "128MB"

MAX_FILES_PER_CHUNK = 1_000
MAX_INPUT_BYTES_PER_CHUNK = 64 * 1024 * 1024
# `query.max-length` Trino — 1 000 000 байт, клиент оборачивает запрос в
# EXECUTE IMMEDIATE: 250 000 байт литералов оставляют запас на обёртку.
MAX_SQL_BYTES = 250_000

WINDOW_START = dtime(23, 30)
WINDOW_END = dtime(23, 50)
CHUNK_CUTOFF = dtime(23, 47)
WAVE_POLL_SECONDS = 60.0
CHUNK_PAUSE_SECONDS = 3.0
DELIVERY_TAIL = timedelta(minutes=30)

# Серверный потолок одной порции: дольше — ошибка Trino, а не удержанный замок.
QUERY_MAX_EXECUTION_TIME = "180s"
COMPACTION_SOURCE = "fotmob:maintenance"

WAVE_DAG_ID = "dag_ingest_fotmob"

_TABLE_RE = re.compile(r"^[a-z0-9_]+$")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _path_sql_literal(value: object) -> str:
    """Отрендерить путь файла, выданный метаданными, как безопасный литерал."""

    if not isinstance(value, str) or not value or len(value) > 8192:
        raise RuntimeError("FotMob compaction received an invalid file path")
    if any(ord(char) < 32 for char in value):
        raise RuntimeError("FotMob compaction file path contains a control character")
    return "'" + value.replace("'", "''") + "'"


def _table_sql(table: str, *, files: bool = False) -> str:
    if not _TABLE_RE.match(table):
        raise RuntimeError(f"FotMob compaction received an invalid table name: {table}")
    suffix = "$files" if files else ""
    return f'iceberg.{SCHEMA}."{table}{suffix}"'


def candidates_sql(table: str) -> str:
    """Живые мелкие файлы таблицы с ключом партиции (ROW держим непрозрачным)."""

    return (
        "SELECT CAST(partition AS json), file_path, file_size_in_bytes "
        f"FROM {_table_sql(table, files=True)} "
        "WHERE content = 0 AND file_size_in_bytes > 0 "
        f"AND file_size_in_bytes < {SMALL_FILE_MAX_BYTES}"
    )


def metrics_sql(table: str) -> str:
    """Число и объём живых файлов данных таблицы."""

    return (
        "SELECT count(*), coalesce(sum(file_size_in_bytes), 0) "
        f"FROM {_table_sql(table, files=True)} WHERE content = 0"
    )


def chunk_sql(table: str, paths: Sequence[str]) -> str:
    """Склейка ровно перечисленных файлов — голого OPTIMIZE не бывает."""

    literals = ",".join(_path_sql_literal(path) for path in paths)
    return (
        f"ALTER TABLE {_table_sql(table)} EXECUTE optimize("
        f"file_size_threshold => '{FILE_SIZE_THRESHOLD}') "
        f'WHERE "$path" IN ({literals})'
    )


def _empty_chunk_sql_bytes(table: str) -> int:
    return len(chunk_sql(table, ["x"]).encode("utf-8")) - 4


def plan_chunks(
    table: str, candidates: Iterable[tuple[Any, str, int]]
) -> list[dict[str, Any]]:
    """Разложить кандидатов по порциям: одна партиция, мелкие первыми.

    Партиции идут по убыванию числа кандидатов (где мусора больше — там и
    выигрыш волны), партиция с одним кандидатом пропускается: склеивать нечего.
    """

    groups: dict[str, list[tuple[int, str]]] = {}
    for partition, path, size in candidates:
        if not isinstance(size, int) or not 0 < size < SMALL_FILE_MAX_BYTES:
            continue
        groups.setdefault(str(partition), []).append((size, path))

    base_bytes = _empty_chunk_sql_bytes(table)
    chunks: list[dict[str, Any]] = []
    for partition, files in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        if len(files) < 2:
            continue
        files.sort()
        paths: list[str] = []
        chunk_bytes = 0
        sql_bytes = base_bytes
        for size, path in files:
            cost = len(_path_sql_literal(path).encode("utf-8")) + 1
            if paths and (
                len(paths) + 1 > MAX_FILES_PER_CHUNK
                or chunk_bytes + size > MAX_INPUT_BYTES_PER_CHUNK
                or sql_bytes + cost > MAX_SQL_BYTES
            ):
                _append_chunk(chunks, partition, paths, chunk_bytes, sql_bytes)
                paths, chunk_bytes, sql_bytes = [], 0, base_bytes
            paths.append(path)
            chunk_bytes += size
            sql_bytes += cost
        _append_chunk(chunks, partition, paths, chunk_bytes, sql_bytes)
    return chunks


def _append_chunk(
    chunks: list[dict[str, Any]],
    partition: str,
    paths: list[str],
    chunk_bytes: int,
    sql_bytes: int,
) -> None:
    # Одинокий хвост партиции склеивать не с чем — он подождёт следующей ночи.
    if len(paths) < 2:
        return
    chunks.append(
        {
            "partition": partition,
            "paths": list(paths),
            "files": len(paths),
            "bytes": chunk_bytes,
            "sql_bytes": sql_bytes,
        }
    )


def build_compaction_manager():
    """Подписанное соединение компакции с серверным потолком порции."""

    from scrapers.fotmob.trino_instrumentation import FotMobTrinoTableManager

    return FotMobTrinoTableManager(
        source=COMPACTION_SOURCE,
        session_properties={"query_max_execution_time": QUERY_MAX_EXECUTION_TIME},
    )


def probe_wave(environ: Optional[Mapping[str, str]] = None):
    """Идёт ли волна контура и когда кончилась последняя (метабаза изолята)."""

    import os

    import psycopg2

    from scrapers.fbref.control.store import resolve_control_db_uri

    connection = psycopg2.connect(
        resolve_control_db_uri(os.environ if environ is None else environ)
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FILTER (WHERE state IN ('running', 'queued')), "
                "max(end_date) FROM dag_run WHERE dag_id = %s",
                (WAVE_DAG_ID,),
            )
            running, last_end = cursor.fetchone()
    finally:
        connection.close()
    return bool(running), last_end


def writer_lock():
    """Тот же advisory-замок писателя bronze, что берут волна и юнит кампании."""

    from dags.scripts.run_fotmob_scraper import _writer_lock

    return _writer_lock()


def _writer_lock_busy_class():
    from dags.scripts.run_fotmob_scraper import WriterLockBusy

    return WriterLockBusy


def notify(message: str) -> None:
    """Телеграм той же функцией, что и алерты DAG-ов контура."""

    from utils.alerts import send_telegram_message

    send_telegram_message(message, level="error")


def exit_code(result: Mapping[str, Any]) -> int:
    return 1 if result.get("stopped") == "error" else 0


def run(
    *,
    tables: Sequence[str] = FOTMOB_COMPACTION_TABLES,
    now: Callable[[], datetime] = _utcnow,
    manager_factory: Callable[[], Any] = build_compaction_manager,
    wave_probe: Callable[[], tuple[bool, Optional[datetime]]] = probe_wave,
    writer_lock: Callable[[], Any] = writer_lock,
    sleep: Callable[[float], None] = time.sleep,
    notify: Callable[[str], None] = notify,
) -> dict[str, Any]:
    """Отработать одну ночь компакции и вернуть метрики прогона."""

    started_at = now()
    result: dict[str, Any] = {
        "started_at": started_at.isoformat(),
        "stopped": "complete",
        "chunks": 0,
        "tables": [],
        "files_before": 0,
        "files_after": 0,
        "bytes_before": 0,
        "bytes_after": 0,
    }

    if not WINDOW_START <= started_at.timetz().replace(tzinfo=None) < WINDOW_END:
        return _finish(result, "out_of_window", now)

    window_end = started_at.replace(
        hour=WINDOW_END.hour, minute=WINDOW_END.minute, second=0, microsecond=0
    )
    cutoff = started_at.replace(
        hour=CHUNK_CUTOFF.hour, minute=CHUNK_CUTOFF.minute, second=0, microsecond=0
    )

    while True:
        running, last_end = wave_probe()
        if not running:
            break
        if now() >= window_end:
            logger.warning("FotMob compaction skipped: волна ещё идёт в конце окна")
            return _finish(result, "wave_running", now)
        sleep(WAVE_POLL_SECONDS)

    if last_end is not None and started_at - last_end < DELIVERY_TAIL:
        logger.info("FotMob compaction skipped: волна кончилась %s", last_end)
        return _finish(result, "delivery_tail", now)

    manager = manager_factory()
    busy_class = _writer_lock_busy_class()
    stopped = "complete"
    for table in tables:
        entry: dict[str, Any] = {"table": table, "chunks": 0}
        result["tables"].append(entry)
        entry["files_before"], entry["bytes_before"] = _metrics(manager, table)
        chunks = plan_chunks(table, manager.execute_query(candidates_sql(table)))
        for chunk in chunks:
            if now() >= cutoff:
                stopped = "deadline"
                break
            try:
                with writer_lock():
                    started = time.perf_counter()
                    manager.execute_query(chunk_sql(table, chunk["paths"]))
                elapsed = time.perf_counter() - started
            except busy_class:
                logger.warning(
                    "FotMob compaction stopped: bronze writer lock is busy"
                )
                stopped = "lock_busy"
                break
            except Exception as error:  # noqa: BLE001 — любая ошибка Trino красит прогон
                logger.error(
                    "FotMob compaction failed: table=%s partition=%s files=%s: %s",
                    table,
                    chunk["partition"],
                    chunk["files"],
                    error,
                )
                notify(
                    f"FotMob compaction failed on {table} "
                    f"(partition={chunk['partition']}, files={chunk['files']}): {error}"
                )
                stopped = "error"
                break
            entry["chunks"] += 1
            result["chunks"] += 1
            logger.info(
                "FotMob compaction: table=%s partition=%s files=%s bytes=%s "
                "sql_bytes=%s elapsed=%.1fs",
                table,
                chunk["partition"],
                chunk["files"],
                chunk["bytes"],
                chunk["sql_bytes"],
                elapsed,
            )
            sleep(CHUNK_PAUSE_SECONDS)
        entry["files_after"], entry["bytes_after"] = _metrics(
            manager, table, optional=True
        )
        if stopped != "complete":
            break

    measured = [
        entry for entry in result["tables"] if entry.get("files_after") is not None
    ]
    for field in ("files", "bytes"):
        result[f"{field}_before"] = sum(entry[f"{field}_before"] for entry in measured)
        result[f"{field}_after"] = sum(entry[f"{field}_after"] for entry in measured)
    return _finish(result, stopped, now)


def _metrics(manager, table: str, *, optional: bool = False):
    try:
        row = manager.execute_query(metrics_sql(table))[0]
    except Exception as error:  # noqa: BLE001
        if not optional:
            raise
        logger.warning("FotMob compaction: метрики %s недоступны: %s", table, error)
        return None, None
    return int(row[0]), int(row[1])


def _finish(
    result: dict[str, Any], stopped: str, now: Callable[[], datetime]
) -> dict[str, Any]:
    result["stopped"] = stopped
    result["finished_at"] = now().isoformat()
    logger.info(
        "FotMob compaction done: tables=%s chunks=%s files_before=%s files_after=%s "
        "stopped=%s",
        len(result["tables"]),
        result["chunks"],
        result["files_before"],
        result["files_after"],
        result["stopped"],
    )
    return result


def report(result: Mapping[str, Any]) -> None:
    """Последняя строка stdout — JSON тех же полей (его хранит обёртка)."""

    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    result = run()
    report(result)
    return exit_code(result)


if __name__ == "__main__":
    raise SystemExit(main())
