#!/usr/bin/env python3
"""Versioned, read-only host adapter for the ESPN rollout probe.

The host side reads the exact dedicated metadata container and loopback-only
Airflow health endpoint, asks the immutable scheduler runtime for the remaining
observations, and streams the merged snapshot to ``espn_rollout_probe_v1.py``.
No temporary file, notification, repair, Docker lifecycle command, database
writer, or artifact writer is available in this module.

The internal ``--collect-runtime`` mode is executed inside the isolated ESPN
scheduler, where the release's read credentials already exist.  Each reader is
isolated so one unavailable dependency cannot hide another probe result.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager, redirect_stdout
from datetime import datetime, time, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from typing import Any
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener


UTC = timezone.utc
ADAPTER_KIND = "espn-watchdog-adapter-v1"
RUNTIME_SNAPSHOT_KIND = "espn-watchdog-runtime-snapshot-v1"
SCHEMA_VERSION = 1
EXPECTED_METADB_CONTAINER = "espn-airflow-airflow-metadb-1"
EXPECTED_SCHEDULER_CONTAINER = "espn-airflow-airflow-scheduler-1"
EXPECTED_HEALTH_URL = "http://127.0.0.1:8086/health"
EXPECTED_PROBE_KIND = "espn-rollout-probe-v1"
EXPECTED_PROBE_PATH = Path("scripts/espn_rollout_probe_v1.py")
EXPECTED_PROBE_SHA256 = (
    "040c79abf7f6757f5dbbe1541b53711d44f2ef74578d0bbbda99dbcce278ed64"
)
CONTAINER_PROBE_PATH = "/opt/airflow/scripts/espn_watchdog_adapter_v1.py"
DOCKER = "/usr/bin/docker"
DEFAULT_PROBE_PYTHON = "/root/.venvs/dpf-test/bin/python"
ARM_START_UTC = time(13, 50)
ARM_END_UTC = time(14, 15)

EXPECTED_RESULT_CODES = (
    "runtime.metadb_container",
    "runtime.ui_health",
    "airflow.dag_inventory",
    "airflow.pause_posture",
    "airflow.parent_child",
    "artifact.final_receipt",
    "registry.frozen",
    "target.exact_181",
    "heads.physical_versions",
    "qualification.dispositions",
    "freshness.per_scope",
    "leases.zero_active",
    "events.leagues_cup_five",
    "serving.layout_parity",
)

RUNTIME_READ_METHODS = (
    ("read_dags", "dags"),
    ("read_parent_child", "parent_child"),
    ("read_receipt", "receipt"),
    ("read_registry", "registry"),
    ("read_target", "target"),
    ("read_scope_heads", "scope_heads"),
    ("read_dispositions", "dispositions"),
    ("read_active_leases", "active_leases"),
    ("read_known_events", "known_events"),
    ("read_layout", "layout"),
)


class AdapterError(RuntimeError):
    """A host/runtime observation or versioned envelope is unusable."""


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
HealthReader = Callable[..., Mapping[str, Any]]


def _utc_iso(value: datetime) -> str:
    if not isinstance(value, datetime):
        raise TypeError("observed_at must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="seconds")


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_value(item) for item in value]
    if isinstance(value, datetime):
        return _utc_iso(value)
    if isinstance(value, float) and not math.isfinite(value):
        raise AdapterError("runtime reader returned a non-finite number")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return f"<{type(value).__name__}>"


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _adapter_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _strict_json_loads(raw: str | bytes, *, label: str) -> Any:
    """Decode RFC JSON while rejecting duplicate keys and non-finite numbers."""

    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise AdapterError(f"{label} is not UTF-8 JSON") from exc
    if not isinstance(raw, str):
        raise AdapterError(f"{label} is not text JSON")

    def object_without_duplicates(pairs):  # noqa: ANN001
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise AdapterError(f"{label} contains a duplicate object key")
            value[key] = item
        return value

    def finite_float(token: str) -> float:
        value = float(token)
        if not math.isfinite(value):
            raise AdapterError(f"{label} contains a non-finite number")
        return value

    def reject_constant(_token: str):
        raise AdapterError(f"{label} contains a non-finite number")

    try:
        return json.loads(
            raw,
            object_pairs_hook=object_without_duplicates,
            parse_float=finite_float,
            parse_constant=reject_constant,
        )
    except AdapterError:
        raise
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise AdapterError(f"{label} is not strict JSON") from exc


def _run_command(
    argv: Sequence[str], *, input_text: str | None = None, timeout: float
) -> subprocess.CompletedProcess[str]:
    if isinstance(argv, (str, bytes)) or not argv:
        raise TypeError("command must be a non-empty argv sequence")
    return subprocess.run(
        list(argv),
        input=input_text,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        raise AdapterError("Airflow health endpoint redirected")


def _read_health(url: str, *, timeout: float) -> Mapping[str, Any]:
    if url != EXPECTED_HEALTH_URL:
        raise AdapterError("Airflow health URL differs from the fixed loopback URL")
    request = Request(
        url,
        headers={"Accept": "application/json", "User-Agent": ADAPTER_KIND},
        method="GET",
    )
    opener = build_opener(_RejectRedirects())
    try:
        response = opener.open(request, timeout=timeout)
    except HTTPError as exc:
        response = exc
    with response:
        final_url = response.geturl()
        status_code = int(response.status)
        body = response.read()
    if final_url != EXPECTED_HEALTH_URL:
        raise AdapterError("Airflow health response came from a different URL")
    decoded = _strict_json_loads(body, label="Airflow health body")
    if not isinstance(decoded, Mapping):
        raise AdapterError("Airflow health body is not an object")
    return {
        "url": EXPECTED_HEALTH_URL,
        "status_code": status_code,
        "body": decoded,
    }


def _container_observation(run_command: CommandRunner) -> dict[str, Any]:
    command = (
        DOCKER,
        "inspect",
        "--type",
        "container",
        EXPECTED_METADB_CONTAINER,
    )
    completed = run_command(command, input_text=None, timeout=10.0)
    if completed.returncode != 0:
        raise AdapterError("exact ESPN metadata container is unavailable")
    payload = _strict_json_loads(completed.stdout, label="Docker inspect output")
    if not isinstance(payload, list) or len(payload) != 1:
        raise AdapterError("Docker inspect did not return one exact container")
    item = payload[0]
    state = item.get("State") if isinstance(item, Mapping) else None
    health = state.get("Health") if isinstance(state, Mapping) else None
    if not isinstance(item, Mapping) or not isinstance(state, Mapping):
        raise AdapterError("Docker inspect container state is malformed")
    return {
        "name": str(item.get("Name", "")).removeprefix("/"),
        "status": state.get("Status"),
        "health": health.get("Status") if isinstance(health, Mapping) else None,
    }


def _runtime_observations(
    run_command: CommandRunner, *, observed_at: datetime
) -> tuple[dict[str, Any], dict[str, str]]:
    observed = _utc_iso(observed_at)
    command = (
        DOCKER,
        "exec",
        EXPECTED_SCHEDULER_CONTAINER,
        "python",
        "-B",
        CONTAINER_PROBE_PATH,
        "--collect-runtime",
        "--observed-at",
        observed,
    )
    completed = run_command(command, input_text=None, timeout=900.0)
    if completed.returncode != 0:
        raise AdapterError("scheduler runtime snapshot command failed")
    envelope = _strict_json_loads(
        completed.stdout, label="scheduler runtime snapshot"
    )
    expected_keys = {key for _method, key in RUNTIME_READ_METHODS}
    if (
        not isinstance(envelope, Mapping)
        or set(envelope)
        != {
            "kind",
            "schema_version",
            "adapter_sha256",
            "observations",
            "errors",
        }
        or envelope.get("kind") != RUNTIME_SNAPSHOT_KIND
        or type(envelope.get("schema_version")) is not int
        or envelope.get("schema_version") != SCHEMA_VERSION
        or envelope.get("adapter_sha256") != _adapter_sha256()
        or not isinstance(envelope.get("observations"), Mapping)
        or not isinstance(envelope.get("errors"), Mapping)
    ):
        raise AdapterError("scheduler runtime snapshot envelope is malformed")
    observations = dict(envelope["observations"])
    errors = dict(envelope["errors"])
    if (
        not set(observations).issubset(expected_keys)
        or not set(errors).issubset(expected_keys)
        or set(observations) & set(errors)
        or set(observations) | set(errors) != expected_keys
        or any(not isinstance(value, str) or not value for value in errors.values())
    ):
        raise AdapterError("scheduler runtime snapshot reader identities are malformed")
    return observations, errors


def _validate_probe_report(
    value: object, *, observed_at: datetime, returncode: int
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AdapterError("versioned rollout probe report is not an object")
    if set(value) != {
        "kind",
        "schema_version",
        "observed_at",
        "status",
        "counts",
        "results",
    }:
        raise AdapterError("versioned rollout probe report fields differ")
    if (
        value.get("kind") != EXPECTED_PROBE_KIND
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != SCHEMA_VERSION
        or value.get("observed_at") != _utc_iso(observed_at)
    ):
        raise AdapterError("versioned rollout probe identity or time differs")
    raw_results = value.get("results")
    if not isinstance(raw_results, list):
        raise AdapterError("versioned rollout probe results are not a list")
    codes = [
        item.get("code") if isinstance(item, Mapping) else None
        for item in raw_results
    ]
    if codes != list(EXPECTED_RESULT_CODES):
        raise AdapterError("versioned rollout probe result identities differ")
    results = []
    for item in raw_results:
        if (
            not isinstance(item, Mapping)
            or set(item) != {"code", "status", "severity", "summary", "details"}
            or item.get("status") not in {"ok", "fail", "unknown"}
            or item.get("severity") != "hard"
            or not isinstance(item.get("summary"), str)
            or not isinstance(item.get("details"), Mapping)
        ):
            raise AdapterError("versioned rollout probe result is malformed")
        results.append(dict(item))
    counts = {
        status: sum(item["status"] == status for item in results)
        for status in ("ok", "fail", "unknown")
    }
    expected_status = (
        "ok"
        if counts == {"ok": len(EXPECTED_RESULT_CODES), "fail": 0, "unknown": 0}
        else "fail"
    )
    raw_counts = value.get("counts")
    if (
        not isinstance(raw_counts, Mapping)
        or set(raw_counts) != {"ok", "fail", "unknown"}
        or any(type(raw_counts.get(status)) is not int for status in raw_counts)
        or dict(raw_counts) != counts
        or value.get("status") != expected_status
        or returncode not in {0, 1}
        or returncode != (0 if expected_status == "ok" else 1)
    ):
        raise AdapterError("versioned rollout probe aggregate or exit status differs")
    return {**dict(value), "results": results}


def _unknown_results(exc: BaseException) -> list[dict[str, Any]]:
    details = {"adapter_error_type": type(exc).__name__}
    return [
        {
            "code": code,
            "status": "unknown",
            "severity": "hard",
            "summary": "versioned rollout probe output is unavailable",
            "details": details,
        }
        for code in EXPECTED_RESULT_CODES
    ]


def _observer_phase(observed_at: datetime) -> str:
    current = observed_at.astimezone(UTC).time().replace(tzinfo=None)
    return "arm-window" if ARM_START_UTC <= current < ARM_END_UTC else "rest"


def observe(
    release_root: str | Path,
    *,
    observer: str,
    observed_at: datetime,
    run_command: CommandRunner = _run_command,
    read_health: HealthReader = _read_health,
    python_executable: str | None = None,
) -> dict[str, Any]:
    """Collect a snapshot and return every result from the versioned probe."""

    if observer not in {"morning", "hourly"}:
        raise ValueError("observer must be morning or hourly")
    observed = _utc_iso(observed_at)
    root = Path(release_root).resolve()
    probe_path = (root / EXPECTED_PROBE_PATH).resolve()
    if root not in probe_path.parents:
        raise AdapterError("versioned rollout probe escaped the release root")
    snapshot: dict[str, Any] = {}
    collection_errors: dict[str, str] = {}
    try:
        snapshot["container"] = _container_observation(run_command)
    except Exception as exc:
        collection_errors["container"] = type(exc).__name__
    try:
        snapshot["ui_health"] = _json_value(
            read_health(EXPECTED_HEALTH_URL, timeout=10.0)
        )
    except Exception as exc:
        collection_errors["ui_health"] = type(exc).__name__
    try:
        runtime, runtime_errors = _runtime_observations(
            run_command, observed_at=observed_at
        )
        snapshot.update(runtime)
        collection_errors.update(runtime_errors)
    except Exception as exc:
        collection_errors["runtime_snapshot"] = type(exc).__name__

    try:
        if not probe_path.is_file():
            raise AdapterError("versioned rollout probe is absent from release root")
        if hashlib.sha256(probe_path.read_bytes()).hexdigest() != EXPECTED_PROBE_SHA256:
            raise AdapterError("versioned rollout probe bytes differ from the reviewed v1")
        command = (
            python_executable or DEFAULT_PROBE_PYTHON,
            "-B",
            str(probe_path),
            "--snapshot",
            "-",
            "--observed-at",
            observed,
        )
        completed = run_command(
            command,
            input_text=_canonical_json(snapshot),
            timeout=900.0,
        )
        raw_report = _strict_json_loads(
            completed.stdout, label="versioned rollout probe stdout"
        )
        probe_report = _validate_probe_report(
            raw_report,
            observed_at=observed_at,
            returncode=completed.returncode,
        )
        results = probe_report["results"]
    except Exception as exc:
        collection_errors["probe"] = type(exc).__name__
        results = _unknown_results(exc)

    status = "ok" if all(item["status"] == "ok" for item in results) else "fail"
    return {
        "kind": ADAPTER_KIND,
        "schema_version": SCHEMA_VERSION,
        "observer": observer,
        "phase": _observer_phase(observed_at),
        "observed_at": observed,
        "status": status,
        "collection_errors": dict(sorted(collection_errors.items())),
        "results": results,
    }


def collect_runtime_snapshot(readers: object) -> dict[str, Any]:
    """Call every runtime reader once and preserve independent availability."""

    observations: dict[str, Any] = {}
    errors: dict[str, str] = {}
    for method_name, key in RUNTIME_READ_METHODS:
        try:
            method = getattr(readers, method_name)
            if not callable(method):
                raise TypeError(f"{method_name} is not callable")
            with redirect_stdout(sys.stderr):
                observations[key] = _json_value(method())
        except Exception as exc:
            errors[key] = type(exc).__name__
    return {
        "kind": RUNTIME_SNAPSHOT_KIND,
        "schema_version": SCHEMA_VERSION,
        "adapter_sha256": _adapter_sha256(),
        "observations": observations,
        "errors": errors,
    }


def _postgres_dsn(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if value.startswith("postgresql+psycopg2://"):
        value = "postgresql://" + value.removeprefix("postgresql+psycopg2://")
    if not value.startswith(("postgresql://", "postgres://")):
        raise AdapterError(f"{name} must be a PostgreSQL URL")
    return value


@contextmanager
def _readonly_postgres(dsn: str):
    connection = _readonly_postgres_connection(dsn)
    try:
        yield connection
    finally:
        connection.rollback()
        connection.close()


def _readonly_postgres_connection(dsn: str):
    import psycopg2

    connection = psycopg2.connect(dsn)
    connection.set_session(readonly=True, autocommit=False)
    return connection


class _TrinoReadClient:
    """Minimal query-only Trino boundary; it exposes no catalog writer."""

    def __init__(self, connection: object):
        self.connection = connection

    @classmethod
    def from_env(cls) -> "_TrinoReadClient":
        try:
            import trino
            from trino.auth import BasicAuthentication
        except ImportError as exc:
            raise AdapterError("scheduler image has no Trino client") from exc
        host = os.environ.get("TRINO_HOST", "").strip()
        if not host:
            raise AdapterError("TRINO_HOST is required")
        port = int(os.environ.get("TRINO_PORT", "8443"))
        user = os.environ.get("TRINO_USER", "airflow").strip()
        password = os.environ.get("TRINO_PASSWORD", "")
        scheme = os.environ.get("TRINO_HTTP_SCHEME", "https").strip()
        verify = os.environ.get("TRINO_TLS_VERIFY", "false").lower() not in {
            "0",
            "false",
            "no",
        }
        kwargs: dict[str, Any] = {
            "host": host,
            "port": port,
            "user": user,
            "catalog": "iceberg",
            "schema": "bronze",
            "http_scheme": scheme,
            "verify": verify,
        }
        if password:
            kwargs["auth"] = BasicAuthentication(user, password)
        return cls(trino.dbapi.connect(**kwargs))

    def execute_query(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        if not isinstance(sql, str) or not sql.strip():
            raise TypeError("Trino query must be non-empty text")
        first_keyword = sql.lstrip().split(None, 1)[0].lower()
        if first_keyword not in {"select", "with"}:
            raise AdapterError("watchdog Trino boundary permits queries only")
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params)
            return list(cursor.fetchall())
        finally:
            cursor.close()


def _scope_ids(registry: object) -> tuple[str, ...]:
    promoted = getattr(registry, "promoted")
    scopes = tuple(
        sorted(item.scope_id(item.current_edition) for item in promoted)
    )
    if not scopes or len(scopes) != len(set(scopes)):
        raise AdapterError("frozen registry scope set is empty or duplicated")
    return scopes


def _target_sha256(scopes: Sequence[str]) -> str:
    import hashlib

    return hashlib.sha256(_canonical_json(list(scopes)).encode("utf-8")).hexdigest()


class SchedulerRuntimeReaders:
    """Read-only observation sources available inside the ESPN scheduler."""

    def __init__(self, *, observed_at: datetime):
        _utc_iso(observed_at)
        self.observed_at = observed_at.astimezone(UTC)
        self._parent: dict[str, Any] | None = None
        self._child_completed_at: datetime | None = None
        self._receipt: dict[str, Any] | None = None
        self._frozen: tuple[Any, dict[str, Any], tuple[str, ...]] | None = None
        self._store_value: Any = None
        self._repository_value: Any = None

    def _metadata_rows(
        self, sql: str, params: Sequence[object] = ()
    ) -> list[tuple[Any, ...]]:
        dsn = _postgres_dsn("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
        with _readonly_postgres(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                return list(cursor.fetchall())

    def read_dags(self) -> dict[str, bool]:
        from airflow.models import DagBag

        dag_folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
        bag = DagBag(
            dag_folder=dag_folder,
            include_examples=False,
            safe_mode=True,
            read_dags_from_db=False,
        )
        if bag.import_errors:
            raise AdapterError("isolated ESPN DagBag has import errors")
        dag_ids = tuple(sorted(bag.dags))
        if not dag_ids:
            raise AdapterError("isolated ESPN DagBag is empty")
        rows = self._metadata_rows(
            "SELECT dag_id, is_paused FROM dag WHERE dag_id = ANY(%s) ORDER BY dag_id",
            (list(dag_ids),),
        )
        paused = {str(dag_id): is_paused for dag_id, is_paused in rows}
        if set(paused) != set(dag_ids) or any(type(value) is not bool for value in paused.values()):
            raise AdapterError("isolated ESPN pause state is incomplete")
        return {dag_id: paused[dag_id] for dag_id in dag_ids}

    @staticmethod
    def _iso(value: object) -> str | None:
        return _utc_iso(value) if isinstance(value, datetime) else None

    def _daily_cycle(self) -> tuple[datetime, datetime]:
        """Return the one scheduler interval relevant to this observation.

        Before the 13:50 UTC arm window, the last boundary remains current.
        At arm start, ownership advances to the interval ending at today's
        14:00 boundary, even when its scheduled parent has not been created.
        """

        boundary = self.observed_at.replace(
            hour=14, minute=0, second=0, microsecond=0
        )
        observed_time = self.observed_at.time().replace(tzinfo=None)
        if observed_time < ARM_START_UTC:
            boundary -= timedelta(days=1)
        return boundary - timedelta(days=1), boundary

    def read_parent_child(self) -> dict[str, Any]:
        if self._parent is not None:
            return dict(self._parent)
        expected_interval_start, expected_interval_end = self._daily_cycle()
        rows = self._metadata_rows(
            "SELECT dag_id, run_id, run_type, logical_date, data_interval_start, "
            "data_interval_end, state FROM dag_run "
            "WHERE dag_id = %s AND run_type = 'scheduled' "
            "AND logical_date = %s AND data_interval_start = %s "
            "AND data_interval_end = %s ORDER BY run_id",
            (
                "dag_trigger_espn_daily",
                expected_interval_start,
                expected_interval_start,
                expected_interval_end,
            ),
        )
        if len(rows) > 1:
            raise AdapterError("current UTC daily cycle parent identity is duplicated")
        if not rows:
            self._parent = {
                "parent_created": False,
                "parent_dag_id": "dag_trigger_espn_daily",
                "parent_run_id": None,
                "parent_run_type": None,
                "parent_logical_date": None,
                "parent_data_interval_start": None,
                "parent_data_interval_end": None,
                "parent_state": None,
                "child_dag_id": "dag_ingest_espn",
                "child_run_id": None,
                "child_state": None,
            }
            return dict(self._parent)
        (
            parent_dag_id,
            parent_run_id,
            parent_run_type,
            logical_date,
            interval_start,
            interval_end,
            parent_state,
        ) = rows[0]
        expected_run_id = f"scheduled__{expected_interval_start.isoformat()}"
        if (
            str(parent_dag_id) != "dag_trigger_espn_daily"
            or str(parent_run_id) != expected_run_id
            or str(parent_run_type) != "scheduled"
            or self._iso(logical_date) != self._iso(expected_interval_start)
            or self._iso(interval_start) != self._iso(expected_interval_start)
            or self._iso(interval_end) != self._iso(expected_interval_end)
        ):
            raise AdapterError("parent row differs from the current UTC daily cycle")
        child_run_id = (
            "espn_daily__dag_trigger_espn_daily__" + str(parent_run_id)
        )
        child_rows = self._metadata_rows(
            "SELECT state, end_date FROM dag_run WHERE dag_id = %s AND run_id = %s",
            ("dag_ingest_espn", child_run_id),
        )
        if len(child_rows) > 1:
            raise AdapterError("derived ESPN child run identity is duplicated")
        child_state = child_rows[0][0] if child_rows else None
        self._child_completed_at = child_rows[0][1] if child_rows else None
        self._parent = {
            "parent_created": True,
            "parent_dag_id": str(parent_dag_id),
            "parent_run_id": str(parent_run_id),
            "parent_run_type": str(parent_run_type),
            "parent_logical_date": self._iso(logical_date),
            "parent_data_interval_start": self._iso(interval_start),
            "parent_data_interval_end": self._iso(interval_end),
            "parent_state": str(parent_state),
            "child_dag_id": "dag_ingest_espn",
            "child_run_id": child_run_id,
            "child_state": None if child_state is None else str(child_state),
        }
        return dict(self._parent)

    def read_receipt(self) -> dict[str, Any]:
        if self._receipt is not None:
            return dict(self._receipt)
        parent = self.read_parent_child()
        child_run_id = parent.get("child_run_id")
        if not isinstance(child_run_id, str) or not child_run_id:
            raise AdapterError("latest derived ESPN child run is unavailable")
        from dags.utils import espn_native_tasks as tasks
        from scrapers.espn import runner

        uri = tasks._join_uri(
            tasks._artifact_root(),
            "runs",
            tasks._run_key("dag_ingest_espn", child_run_id),
            "run-success.json",
        )
        artifact = _strict_json_loads(
            runner._read_artifact(uri), label="latest ESPN success receipt"
        )
        if not isinstance(artifact, Mapping):
            raise AdapterError("latest ESPN success receipt is not an object")
        self._receipt = {
            "artifact": dict(artifact),
            "completed_at": self._iso(self._child_completed_at),
        }
        return dict(self._receipt)

    def _frozen_target(self) -> tuple[Any, dict[str, Any], tuple[str, ...]]:
        if self._frozen is not None:
            return self._frozen
        from dags.utils import espn_native_tasks as tasks

        if tasks._frozen_discovery_state_ref() is None:
            raise AdapterError("release has no frozen discovery-state reference")
        registry, state = tasks._load_discovered_registry(now=self.observed_at)
        scopes = _scope_ids(registry)
        self._frozen = (registry, state, scopes)
        return self._frozen

    def read_registry(self) -> dict[str, Any]:
        registry, state, scopes = self._frozen_target()
        signature = registry.signature()
        configured_signature = state.get("male_registry_signature")
        return {
            "configured_signature": configured_signature,
            "frozen_signature": signature,
            "configured_scope_ids": list(scopes),
            "frozen_scope_ids": list(scopes),
            "target_scope_sha256": _target_sha256(scopes),
            "frozen_target_scope_sha256": _target_sha256(scopes),
        }

    def read_target(self) -> dict[str, Any]:
        _registry, _state, scopes = self._frozen_target()
        return {
            "scope_ids": list(scopes),
            "target_scope_sha256": _target_sha256(scopes),
        }

    def _store(self):
        if self._store_value is None:
            from scrapers.espn.operations import PostgresEspnControlStore

            raw_dsn = os.environ.get("ESPN_CONTROL_DATABASE_URL") or os.environ.get(
                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", ""
            )
            dsn = raw_dsn.strip()
            if dsn.startswith("postgresql+psycopg2://"):
                dsn = "postgresql://" + dsn.removeprefix(
                    "postgresql+psycopg2://"
                )
            if not dsn.startswith(("postgresql://", "postgres://")):
                raise AdapterError("ESPN control database must be PostgreSQL")
            self._store_value = PostgresEspnControlStore(
                lambda: _readonly_postgres_connection(dsn)
            )
        return self._store_value

    def _repository(self):
        if self._repository_value is None:
            from scrapers.espn.layout import require_layout_mode
            from scrapers.espn.repository import EspnBronzeRepository

            self._repository_value = EspnBronzeRepository(
                writer=object(),
                query=_TrinoReadClient.from_env(),
                layout_mode=require_layout_mode(),
                ensure_objects_on_write=False,
                validate_catalog_layout_on_write=False,
            )
        return self._repository_value

    def read_scope_heads(self) -> list[dict[str, Any]]:
        from dags.utils import espn_native_tasks as tasks

        registry, state, scopes = self._frozen_target()
        store = self._store()
        heads = store.read_scope_heads(scopes)
        evidence = store.read_latest_run_evidence_by_scope(
            scopes, dag_id="dag_ingest_espn"
        )
        rows = []
        for scope_id, head in sorted(heads.items()):
            latest = evidence.get(scope_id)
            qualified_head, qualified_state, latest_at = (
                tasks._qualified_freshness_at(
                    head,
                    latest,
                    expected_registry_ref=state["male_registry_ref"],
                    expected_registry_signature=registry.signature(),
                    observed_at=self.observed_at,
                    repository=self._repository(),
                )
            )
            generation = None
            physical_verified = (
                qualified_head is not None and qualified_state == "complete"
            )
            if physical_verified:
                try:
                    generation = tasks._load_exact_scope_head_snapshot(head)
                except Exception:
                    physical_verified = False
            rows.append(
                {
                    "scope_id": scope_id,
                    "state": "complete" if physical_verified else "incomplete",
                    "registry_signature": head.registry_signature,
                    "target_scope_sha256": _target_sha256(scopes),
                    "parser_version": (
                        None if generation is None else generation.parser_version
                    ),
                    "runtime_version": (
                        None if generation is None else generation.runtime_version
                    ),
                    "physical_verified": physical_verified,
                    "last_complete_at": self._iso(latest_at),
                }
            )
        return rows

    def read_dispositions(self) -> Any:
        receipt = self.read_receipt()
        artifact = receipt.get("artifact")
        if not isinstance(artifact, Mapping) or "qualification" not in artifact:
            raise AdapterError("latest ESPN receipt has no qualification")
        return artifact["qualification"]

    def read_active_leases(self) -> list[dict[str, str]]:
        store = self._store()
        connection = store._connect()
        try:
            connection.set_session(readonly=True, autocommit=False)
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT scope_id, owner_id FROM espn_control.scope_lease_v2 "
                    "WHERE expires_at > clock_timestamp() ORDER BY scope_id"
                )
                rows = list(cursor.fetchall())
        finally:
            connection.rollback()
            connection.close()
        return [
            {"scope_id": str(scope_id), "owner_id": str(owner_id)}
            for scope_id, owner_id in rows
        ]

    def read_known_events(self) -> dict[str, Any]:
        from scrapers.espn.layout import LEGACY14, require_layout_mode

        mode = require_layout_mode()
        relation = (
            "iceberg.bronze.espn_schedule_current"
            if mode == LEGACY14
            else "iceberg.bronze.espn_schedule"
        )
        rows = self._repository()._execute(
            f"SELECT DISTINCT event_id FROM {relation} "
            "WHERE scope_id = ? ORDER BY event_id",
            ("19425:2026",),
        )
        return {
            "scope_id": "19425:2026",
            "event_ids": [int(row[0]) for row in rows],
        }

    def read_layout(self) -> dict[str, Any]:
        from scrapers.espn.layout import (
            COMPACT6,
            LEGACY14,
            catalog_inventory_sql,
            require_layout_mode,
        )

        mode = require_layout_mode()
        repository = self._repository()
        inventory_rows = repository._execute(catalog_inventory_sql())
        inventory = [
            {
                "table_schema": row[0],
                "table_name": row[1],
                "table_type": row[2],
            }
            for row in inventory_rows
        ]
        serving = (
            "iceberg.bronze.espn_schedule_current"
            if mode == LEGACY14
            else "iceberg.bronze.espn_schedule"
        )
        repository._execute(f"SELECT 1 FROM {serving} LIMIT 1")
        parity: dict[str, bool] = {}
        if mode == COMPACT6:
            for entity in ("schedule", "lineup", "matchsheet"):
                public = f"iceberg.bronze.espn_{entity}"
                internal = f"iceberg.espn_internal.espn_{entity}_current"
                mismatch = repository._execute(
                    "SELECT 1 FROM ((SELECT * FROM "
                    f"{internal} EXCEPT ALL SELECT * FROM {public}) UNION ALL "
                    f"(SELECT * FROM {public} EXCEPT ALL SELECT * FROM {internal})) "
                    "mismatch LIMIT 1"
                )
                parity[entity] = not bool(mismatch)
        return {
            "layout_mode": mode,
            "inventory": inventory,
            "serving_relation": serving,
            "serving_readable": True,
            "parity": parity,
        }


def render_lines(report: Mapping[str, Any]) -> list[str]:
    """Render a compact header plus every independent probe result."""

    results = report.get("results")
    if not isinstance(results, Sequence) or isinstance(results, (str, bytes)):
        results = _unknown_results(AdapterError("adapter result list is absent"))
    elif [
        item.get("code") if isinstance(item, Mapping) else None for item in results
    ] != list(EXPECTED_RESULT_CODES):
        results = _unknown_results(AdapterError("adapter result identities differ"))
    header = (
        f"• ESPN rollout v1: observer={report.get('observer')} "
        f"phase={report.get('phase')} status={report.get('status')}"
    )
    icons = {"ok": "✅", "fail": "‼️", "unknown": "⚠️"}
    lines = [header]
    for item in results:
        value = item if isinstance(item, Mapping) else {}
        status = value.get("status", "unknown")
        icon = icons.get(status, "⚠️")
        lines.append(
            f"• {value.get('code', 'unknown')}: {status} {icon} — "
            f"{value.get('summary', 'result unavailable')}"
        )
    return lines


def morning_report_lines(
    *,
    release_root: str | Path,
    observed_at: datetime | None = None,
) -> list[str]:
    report = observe(
        release_root,
        observer="morning",
        observed_at=observed_at or datetime.now(UTC),
    )
    return render_lines(report)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-root", type=Path)
    parser.add_argument(
        "--observer", choices=("morning", "hourly"), default="hourly"
    )
    parser.add_argument("--observed-at", help="aware ISO-8601 timestamp")
    parser.add_argument("--format", choices=("lines", "json"), default="lines")
    parser.add_argument("--collect-runtime", action="store_true", help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    observed_at = (
        datetime.now(UTC)
        if args.observed_at is None
        else _parse_timestamp(args.observed_at)
    )
    if observed_at is None:
        parser.error("--observed-at must be a timezone-aware ISO-8601 timestamp")
    if args.collect_runtime:
        envelope = collect_runtime_snapshot(
            SchedulerRuntimeReaders(observed_at=observed_at)
        )
        print(_canonical_json(envelope))
        return 0
    if args.release_root is None:
        parser.error("--release-root is required")
    report = observe(
        args.release_root,
        observer=args.observer,
        observed_at=observed_at,
    )
    if args.format == "json":
        print(_canonical_json(report))
    else:
        print("\n".join(render_lines(report)))
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ADAPTER_KIND",
    "EXPECTED_HEALTH_URL",
    "EXPECTED_METADB_CONTAINER",
    "EXPECTED_RESULT_CODES",
    "EXPECTED_SCHEDULER_CONTAINER",
    "RUNTIME_READ_METHODS",
    "SchedulerRuntimeReaders",
    "collect_runtime_snapshot",
    "main",
    "morning_report_lines",
    "observe",
    "render_lines",
]
