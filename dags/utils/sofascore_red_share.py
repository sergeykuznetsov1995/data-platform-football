"""One red-share threshold for the SofaScore scope lanes (#1356).

The criterion is fewer than 20 % red scope attempts per UTC day. Legacy mapped
scope task instances retain their latest try. History slot workers consume many
scopes, so their task colour is excluded. Finalize receipts and exact per-scope
reports provide the replacement counts. Without those reports, history is unknown.
Host reporting loads this file by path without Airflow; it uses stdlib only.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
import json
import hashlib
from typing import NamedTuple

RED_SHARE_THRESHOLD_PCT = 20

HISTORY_DAG_ID = "dag_backfill_sofascore_all_mens"
REFRESH_DAG_ID = "dag_refresh_sofascore_all_mens"
SCOPE_DAG_IDS = (HISTORY_DAG_ID, REFRESH_DAG_ID)

# LIKE mask for run_historical_scope / run_refresh_scope; the daily
# run_sofascore_dq is not a scope and must not match.
SCOPE_TASK_PATTERN = "run\\_%\\_scope"

_UTC_LITERAL = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


def _bounds(t0_utc: str, t1_utc: str) -> None:
    for bound in (t0_utc, t1_utc):
        if not _UTC_LITERAL.match(bound):
            raise ValueError(f"not a UTC timestamp literal: {bound!r}")


def _slot_runs() -> str:
    # Airflow 2 JSON XCom is bytea containing the JSON string "slots".
    return (
        "SELECT run_id FROM xcom WHERE dag_id='" + HISTORY_DAG_ID + "' "
        "AND task_id='plan_historical_batch' AND key='history_mode' "
        "AND convert_from(value,'UTF8')='\"slots\"'"
    )


def history_slot_runs_sql(t0_utc: str, t1_utc: str) -> str:
    """Slot runs overlapping the day. DagRun dates survive worker TI retries.

    DagRun state only finds runs whose durable attempt reports must be read;
    it contributes no numeric success or failure counts.
    """
    _bounds(t0_utc, t1_utc)
    return (
        "SELECT DISTINCT d.run_id FROM dag_run d "
        f"WHERE d.dag_id='{HISTORY_DAG_ID}' AND d.run_id IN (" + _slot_runs() + ") "
        f"AND d.start_date < '{t1_utc}'::timestamptz "
        f"AND (d.end_date IS NULL OR d.end_date >= '{t0_utc}'::timestamptz) "
        "ORDER BY 1"
    )


def red_share_sql(t0_utc: str, t1_utc: str, *, scope_reports: bool = False) -> str:
    """Terminal legacy scope TIs. Slot workers never count as scope attempts.

    Without the durable report adapter, (-1,-1) marks history unavailable when
    slot workers overlap the window. This prevents a refresh-only green total
    from presenting history as healthy in the older host consumers.
    """
    _bounds(t0_utc, t1_utc)
    dag_ids = ", ".join(f"'{dag_id}'" for dag_id in SCOPE_DAG_IDS)
    counts = (
        "SELECT dag_id, count(*) FILTER (WHERE state = 'failed') AS failed, count(*) AS total "
        "FROM task_instance "
        f"WHERE dag_id IN ({dag_ids}) "
        f"AND task_id LIKE '{SCOPE_TASK_PATTERN}' ESCAPE '\\' "
        "AND map_index >= 0 AND state IN ('success', 'failed') "
        f"AND start_date >= '{t0_utc}'::timestamptz "
        f"AND start_date < '{t1_utc}'::timestamptz "
        f"AND NOT (dag_id='{HISTORY_DAG_ID}' AND run_id IN (" + _slot_runs() + ")) "
        "GROUP BY 1"
    )
    if scope_reports:
        return counts + " ORDER BY 1"
    return (
        "WITH legacy AS (" + counts + "), slots AS (" + history_slot_runs_sql(t0_utc, t1_utc) + ") "
        f"SELECT dag_id, failed, total FROM legacy WHERE dag_id <> '{HISTORY_DAG_ID}' "
        "OR NOT EXISTS (SELECT 1 FROM slots) UNION ALL "
        f"SELECT '{HISTORY_DAG_ID}', -1, -1 WHERE EXISTS (SELECT 1 FROM slots) ORDER BY 1"
    )


def history_scope_counts(result_dir: Path, t0_utc: str, t1_utc: str,
                         required_runs: set[str]) -> tuple[int, int]:
    """Count finalized slot attempts from receipts and their exact scope reports.

    The finalize receipt proves completeness. File mtimes and the worker task
    state cannot establish a scope's outcome or its UTC day. Metadata is excluded.
    """
    _bounds(t0_utc, t1_utc)
    if not result_dir.is_dir():
        raise ValueError("history results directory missing")
    d0, d1 = (datetime.fromisoformat(t.replace("Z", "+00:00")) for t in (t0_utc, t1_utc))
    reports = {}
    for run in required_runs:
        digest = hashlib.sha256(run.encode()).hexdigest()[:20]
        receipt = json.loads((result_dir / f"history-slots-{digest}.json").read_text())
        if (not isinstance(receipt, dict) or receipt.get("history_slots_receipt") is not True
                or receipt.get("finalized") is not True or receipt.get("dag_run_id") != run
                or not isinstance(receipt.get("items"), list)):
            raise ValueError("invalid history slot finalize receipt")
        content = {key: value for key, value in receipt.items() if key != "receipt_digest"}
        if receipt.get("receipt_digest") != hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest():
            raise ValueError("history slot receipt digest mismatch")
        if not isinstance(receipt.get("plan_digest"), str) or not re.fullmatch(r"[0-9a-f]{64}", receipt["plan_digest"]):
            raise ValueError("invalid history slot plan digest")
        claimed = receipt.get("claimed_count")
        if isinstance(claimed, bool) or not isinstance(claimed, int) or claimed != len(receipt["items"]):
            raise ValueError("history slot receipt claimed count mismatch")
        indexes = [item.get("index") if isinstance(item, dict) else None for item in receipt["items"]]
        if (any(isinstance(index, bool) or not isinstance(index, int) for index in indexes)
                or set(indexes) != set(range(claimed))):
            raise ValueError("history slot receipt indexes mismatch")
        plan_length = receipt.get("plan_length")
        plan_indexes = [item.get("plan_index") for item in receipt["items"]]
        if (isinstance(plan_length, bool) or not isinstance(plan_length, int) or plan_length < claimed
                or any(isinstance(index, bool) or not isinstance(index, int)
                       or not 0 <= index < plan_length for index in plan_indexes)
                or len(set(plan_indexes)) != claimed):
            raise ValueError("history slot receipt plan indexes mismatch")
        for item in receipt["items"]:
            if not isinstance(item, dict) or not isinstance(item.get("environment"), dict):
                raise ValueError("invalid history slot receipt item")
            env = item["environment"]
            if (item.get("accounted") is not True or not isinstance(item.get("outcome"), dict)
                    or item["outcome"].get("status") not in ("success", "failed", "not_started")):
                raise ValueError("unaccounted history slot attempt")
            if item["outcome"].get("status") == "not_started":
                if isinstance(item.get("attempts"), bool) or item.get("attempts") != 0:
                    raise ValueError("unstarted scope has capture attempts")
                continue
            if env.get("SOFASCORE_CAMPAIGN_ACTION") != "capture":
                continue
            path = result_dir / Path(env["SOFASCORE_SCOPE_RESULT_PATH"]).name
            payload = json.loads(path.read_text())
            if not isinstance(payload, dict) or not isinstance(payload.get("history_slot"), dict):
                raise ValueError("invalid history slot report")
            stamp = payload["history_slot"]
            if not all(isinstance(stamp.get(key), str) for key in ("started_at", "finished_at")):
                raise ValueError("invalid history slot attempt timestamps")
            started = datetime.fromisoformat(stamp["started_at"].replace("Z", "+00:00"))
            finished = datetime.fromisoformat(stamp["finished_at"].replace("Z", "+00:00"))
            status = stamp["terminal_state"]
            if (payload.get("history_scope_attempt") is not True
                    or stamp.get("dag_run_id") != run
                    or isinstance(stamp.get("slot"), bool)
                    or not isinstance(stamp.get("slot"), int)
                    or str(stamp.get("slot")) != str(item.get("slot"))
                    or stamp["started_at"] != item.get("started_at")
                    or stamp["finished_at"] != item.get("finished_at")
                    or started.tzinfo is None or finished.tzinfo is None or finished < started
                    or status not in ("success", "failed") or status != item["outcome"].get("status")
                    or not payload.get("scope_digest")
                    or payload.get("run_id") != env["SOFASCORE_SCOPE_RUN_ID"]
                    or payload.get("campaign_id") != env["SOFASCORE_EXPECTED_CAMPAIGN_ID"]
                    or str(payload.get("tournament_id")) != str(env["SOFASCORE_TOURNAMENT_ID"])
                    or str(payload.get("source_season_id")) != str(env["SOFASCORE_SOURCE_SEASON_ID"])):
                raise ValueError("invalid history scope attempt report")
            identity = (run, payload["scope_digest"])
            if identity in reports:
                raise ValueError("duplicate history scope attempt in finalize receipt")
            reports[identity] = (started, status)
    attempts = [status for started, status in reports.values() if d0 <= started < d1]
    return sum(status == "failed" for status in attempts), len(attempts)


class RedShare(NamedTuple):
    # NamedTuple, not a dataclass: the host loads this file by path without
    # registering it in sys.modules, which dataclasses require.
    failed: int
    total: int
    pct: float
    verdict: str  # "ok" | "red" | "no_data" | "unavailable"

    @classmethod
    def of(cls, failed: int, total: int) -> "RedShare":
        if total <= 0:
            return cls(failed, total, 0.0, "no_data")
        pct = 100.0 * failed / total
        verdict = "red" if pct >= RED_SHARE_THRESHOLD_PCT else "ok"
        return cls(failed, total, pct, verdict)


def parse_rows(psql_out: str) -> dict[str, tuple[int, int]]:
    """``psql -tA`` output of :func:`red_share_sql` → ``{dag_id: (failed, total)}``."""

    per_dag: dict[str, tuple[int, int]] = {}
    for line in psql_out.splitlines():
        if not line.strip():
            continue
        dag_id, failed, total = line.strip().split("|")
        per_dag[dag_id] = (int(failed), int(total))
    return per_dag


def total_share(per_dag: dict[str, tuple[int, int]]) -> RedShare:
    if any(total < 0 for _, total in per_dag.values()):
        return RedShare(0, 0, 0.0, "unavailable")
    failed = sum(f for f, _ in per_dag.values())
    total = sum(t for _, t in per_dag.values())
    return RedShare.of(failed, total)


def format_line(day: str, per_dag: dict[str, tuple[int, int]]) -> str:
    """The one line the morning report and the stall watch both print;
    ``day`` is the UTC day ``YYYY-MM-DD`` the window covers."""

    share = total_share(per_dag)
    hf, ht = per_dag.get(HISTORY_DAG_ID, (0, 0))
    rf, rt = per_dag.get(REFRESH_DAG_ID, (0, 0))
    ddmm = f"{day[8:10]}.{day[5:7]}"
    if ht < 0:
        return f"красных попыток за {ddmm} (UTC): ⚠️ история недоступна (нужны отчёты скоупов; актуалка {rf}/{rt})"
    lanes = (
        f"(порог < {RED_SHARE_THRESHOLD_PCT} %; история {hf}/{ht}, актуалка {rf}/{rt})"
    )
    if share.verdict == "no_data":
        return f"красных попыток скоупов за {ddmm} (UTC): ⚠️ нет терминальных попыток {lanes}"
    mark = "✅" if share.verdict == "ok" else "⛔"
    pct = f"{share.pct:.1f}".replace(".", ",")
    return (
        f"красных попыток скоупов за {ddmm} (UTC): "
        f"{share.failed}/{share.total} = {pct} % {mark} {lanes}"
    )
