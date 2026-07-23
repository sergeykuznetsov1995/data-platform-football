"""Finite, fail-closed scheduling contract for the WhoScored production bootstrap.

The signed rollout authority contains six historical logical slots.  Airflow
creates those slots as ordinary ``scheduled`` DagRuns, one at a time, and then
the timetable resumes a non-catchup daily 10:00 UTC cadence.  Runtime tasks
replay the same projection from the per-run paid authority; the timetable's
pointer projection is only scheduling input and can never authorize traffic.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


ACCEPTANCE_MODE = "accelerated-bootstrap-v1"
BOOTSTRAP_WAVES = (
    "wave-20",
    "wave-20",
    "wave-70",
    "wave-70",
    "wave-all",
    "wave-all",
)
BOOTSTRAP_SLOT_COUNT = len(BOOTSTRAP_WAVES)
BOOTSTRAP_LIMIT_SECONDS = 21_600
NORMAL_DAILY_HOUR_UTC = 10
MAX_PROVIDER_ORDER_CAP_BYTES = 1_000_000_000
BOOTSTRAP_POINTER_NAME = "bootstrap.json"
BOOTSTRAP_POINTER_ROOT_ENV = "WHOSCORED_SCHEDULED_PAID_POINTER_ROOT"
WHOSCORED_INGEST_DAG_ID = "dag_ingest_whoscored"
BOOTSTRAP_TERMINAL_TASK_ID = "seal_rollout_acceptance_and_pause"
_DIGEST = re.compile(r"[0-9a-f]{64}")
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_SLOT_FIELDS = {"run_id", "logical_date", "wave_id"}
_RUN_POINTER_FIELDS = {
    "schema_version",
    "dag_id",
    "run_id",
    "approval_id",
    "approval_sha256",
}
_POINTER_UNSIGNED_FIELDS = {
    "schema_version",
    "acceptance_mode",
    "bootstrap_slots",
    "capacity_receipt_sha256",
    "provider_order_cap_bytes",
    "rollout_id",
    "runtime_sha256",
    "provider_policy_sha256",
}
_POINTER_FIELDS = _POINTER_UNSIGNED_FIELDS | {"authority_sha256", "signature"}


class WhoScoredBootstrapError(ValueError):
    """The accelerated-bootstrap authority is malformed or inconsistent."""


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise WhoScoredBootstrapError(
            "WhoScored bootstrap authority is not canonical JSON"
        ) from exc


def _canonical_utc(value: Any, *, label: str) -> tuple[str, datetime]:
    raw = str(value or "")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WhoScoredBootstrapError(f"invalid WhoScored bootstrap {label}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise WhoScoredBootstrapError(f"invalid WhoScored bootstrap {label}")
    parsed = parsed.astimezone(timezone.utc)
    canonical = parsed.isoformat().replace("+00:00", "Z")
    if raw != canonical:
        raise WhoScoredBootstrapError(f"non-canonical WhoScored bootstrap {label}")
    return canonical, parsed


def scheduled_run_id(logical_date: datetime) -> str:
    """Return Airflow 2.7/2.11's canonical scheduled-run identifier."""

    if logical_date.tzinfo is None or logical_date.utcoffset() != timedelta(0):
        raise WhoScoredBootstrapError("WhoScored bootstrap logical date must be UTC")
    return "scheduled__" + logical_date.astimezone(timezone.utc).isoformat()


def normalize_bootstrap_slots(value: Any) -> list[dict[str, str]]:
    """Validate the exact ordered six-slot bootstrap contract."""

    if not isinstance(value, (list, tuple)) or len(value) != BOOTSTRAP_SLOT_COUNT:
        raise WhoScoredBootstrapError(
            "WhoScored bootstrap requires exactly six logical slots"
        )
    normalized: list[dict[str, str]] = []
    logical_dates: list[datetime] = []
    for index, (raw, expected_wave) in enumerate(zip(value, BOOTSTRAP_WAVES)):
        if not isinstance(raw, Mapping) or set(raw) != _SLOT_FIELDS:
            raise WhoScoredBootstrapError("invalid WhoScored bootstrap slot schema")
        logical_raw, logical_date = _canonical_utc(
            raw.get("logical_date"), label=f"slot {index} logical date"
        )
        run_id = raw.get("run_id")
        wave_id = raw.get("wave_id")
        if (
            logical_date.timetz() != time(NORMAL_DAILY_HOUR_UTC, tzinfo=timezone.utc)
            or run_id != scheduled_run_id(logical_date)
            or wave_id != expected_wave
        ):
            raise WhoScoredBootstrapError(
                "WhoScored bootstrap slot differs from its run/wave contract"
            )
        logical_dates.append(logical_date)
        normalized.append(
            {"run_id": str(run_id), "logical_date": logical_raw, "wave_id": wave_id}
        )
    if any(
        right - left != timedelta(days=1)
        for left, right in zip(logical_dates, logical_dates[1:])
    ):
        raise WhoScoredBootstrapError(
            "WhoScored bootstrap logical slots must be consecutive daily 10:00 UTC"
        )
    if logical_dates[-1] >= datetime.now(timezone.utc):
        raise WhoScoredBootstrapError(
            "WhoScored bootstrap logical slots must all be backdated"
        )
    return normalized


def normalize_bootstrap_authority(value: Mapping[str, Any]) -> dict[str, Any]:
    """Project the four fields common to rollout, charter, pointer, and run plan."""

    if not isinstance(value, Mapping):
        raise WhoScoredBootstrapError("WhoScored bootstrap authority is not an object")
    if value.get("acceptance_mode") != ACCEPTANCE_MODE:
        raise WhoScoredBootstrapError("invalid WhoScored bootstrap acceptance mode")
    capacity_receipt_sha256 = str(value.get("capacity_receipt_sha256") or "")
    provider_order_cap_bytes = value.get("provider_order_cap_bytes")
    if _DIGEST.fullmatch(capacity_receipt_sha256) is None:
        raise WhoScoredBootstrapError(
            "invalid WhoScored bootstrap capacity receipt digest"
        )
    if (
        isinstance(provider_order_cap_bytes, bool)
        or not isinstance(provider_order_cap_bytes, int)
        or not 1 <= provider_order_cap_bytes <= MAX_PROVIDER_ORDER_CAP_BYTES
    ):
        raise WhoScoredBootstrapError("invalid WhoScored bootstrap provider order cap")
    return {
        "acceptance_mode": ACCEPTANCE_MODE,
        "bootstrap_slots": normalize_bootstrap_slots(value.get("bootstrap_slots")),
        "capacity_receipt_sha256": capacity_receipt_sha256,
        "provider_order_cap_bytes": provider_order_cap_bytes,
    }


def bootstrap_slot_for_run(
    authority: Mapping[str, Any],
    *,
    run_id: Any,
    logical_date: Any,
    wave_id: Any = None,
) -> dict[str, Any]:
    """Return the unique signed slot for one scheduler-owned bootstrap run."""

    normalized = normalize_bootstrap_authority(authority)
    logical_raw = (
        logical_date
        if isinstance(logical_date, str)
        else (
            logical_date.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            if isinstance(logical_date, datetime) and logical_date.tzinfo is not None
            else ""
        )
    )
    matches = [
        (index, slot)
        for index, slot in enumerate(normalized["bootstrap_slots"])
        if slot["run_id"] == run_id and slot["logical_date"] == logical_raw
    ]
    if len(matches) != 1:
        raise WhoScoredBootstrapError(
            "WhoScored DagRun is outside the exact signed bootstrap slots"
        )
    index, slot = matches[0]
    if wave_id is not None and slot["wave_id"] != wave_id:
        raise WhoScoredBootstrapError(
            "WhoScored bootstrap DagRun is authorized for a different wave"
        )
    return {"slot_index": index, **slot}


def _protected_pointer_bytes(
    path: Path,
    *,
    expected_name: str,
    expected_uids: Sequence[int],
    label: str,
    maximum_bytes: int,
) -> bytes:
    """Read one immutable pointer without following or racing mutable links."""

    absolute = Path(os.path.abspath(path))
    if absolute != path or path.name != expected_name:
        raise WhoScoredBootstrapError(f"invalid WhoScored {label} path")
    try:
        before = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid not in set(expected_uids)
            or before.st_mode & 0o022
            or before.st_size <= 0
            or before.st_size > maximum_bytes
        ):
            raise WhoScoredBootstrapError(f"WhoScored {label} is not protected")
        flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            raw = bytearray()
            while True:
                chunk = os.read(descriptor, 64 * 1024)
                if not chunk:
                    break
                raw.extend(chunk)
                if len(raw) > maximum_bytes:
                    raise WhoScoredBootstrapError(f"WhoScored {label} is oversized")
        finally:
            os.close(descriptor)
        after = path.lstat()
    except OSError as exc:
        raise WhoScoredBootstrapError(f"WhoScored {label} is unreadable") from exc
    identity = ("st_dev", "st_ino", "st_mode", "st_uid", "st_size", "st_mtime_ns")
    if any(
        getattr(before, field) != getattr(candidate, field)
        for candidate in (opened, after)
        for field in identity
    ):
        raise WhoScoredBootstrapError(f"WhoScored {label} changed while reading")
    return bytes(raw)


def _protected_bootstrap_pointer(path: Path) -> dict[str, Any]:
    """Read the scheduler-visible projection without following mutable links."""

    raw = _protected_pointer_bytes(
        path,
        expected_name=BOOTSTRAP_POINTER_NAME,
        expected_uids=(0,),
        label="bootstrap pointer",
        maximum_bytes=256 * 1024,
    )
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WhoScoredBootstrapError(
            "WhoScored bootstrap pointer is not strict JSON"
        ) from exc
    if (
        not isinstance(value, dict)
        or set(value) != _POINTER_FIELDS
        or raw != canonical_json_bytes(value) + b"\n"
        or value.get("schema_version") != 1
    ):
        raise WhoScoredBootstrapError("WhoScored bootstrap pointer schema is invalid")
    unsigned = {field: value[field] for field in _POINTER_UNSIGNED_FIELDS}
    expected_digest = hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
    if value.get("authority_sha256") != expected_digest:
        raise WhoScoredBootstrapError(
            "WhoScored bootstrap pointer content digest is invalid"
        )
    for field in (
        "runtime_sha256",
        "provider_policy_sha256",
        "signature",
    ):
        if _DIGEST.fullmatch(str(value.get(field) or "")) is None:
            raise WhoScoredBootstrapError(
                f"WhoScored bootstrap pointer {field} is invalid"
            )
    if _TOKEN.fullmatch(str(value.get("rollout_id") or "")) is None:
        raise WhoScoredBootstrapError(
            "WhoScored bootstrap pointer rollout id is invalid"
        )
    normalize_bootstrap_authority(value)
    return value


def scheduled_run_pointer_ready(run_id: str) -> bool:
    """Return true only for this exact protected scheduler issuance pointer."""

    if not isinstance(run_id, str) or not run_id.startswith("scheduled__"):
        raise WhoScoredBootstrapError("invalid WhoScored scheduled run id")
    root_raw = os.environ.get(BOOTSTRAP_POINTER_ROOT_ENV, "").strip()
    if not root_raw:
        return False
    root = Path(root_raw)
    if not root.is_absolute() or ".." in root.parts:
        raise WhoScoredBootstrapError("WhoScored bootstrap pointer root is invalid")
    name = hashlib.sha256(run_id.encode("utf-8")).hexdigest() + ".json"
    path = root / name
    if not path.exists() and not path.is_symlink():
        return False
    raw = _protected_pointer_bytes(
        path,
        expected_name=name,
        expected_uids=(0, os.geteuid()),
        label="scheduled run pointer",
        maximum_bytes=16 * 1024,
    )
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WhoScoredBootstrapError(
            "WhoScored scheduled run pointer is not strict JSON"
        ) from exc
    if (
        not isinstance(value, dict)
        or set(value) != _RUN_POINTER_FIELDS
        or raw != canonical_json_bytes(value) + b"\n"
        or value.get("schema_version") != 1
        or value.get("dag_id") != WHOSCORED_INGEST_DAG_ID
        or value.get("run_id") != run_id
        or _TOKEN.fullmatch(str(value.get("approval_id") or "")) is None
        or _DIGEST.fullmatch(str(value.get("approval_sha256") or "")) is None
    ):
        raise WhoScoredBootstrapError(
            "WhoScored scheduled run pointer schema is invalid"
        )
    return True


def load_bootstrap_pointer(*, required: bool = False) -> Optional[dict[str, Any]]:
    """Load ``bootstrap.json`` from the existing protected pointer mount."""

    root_raw = os.environ.get(BOOTSTRAP_POINTER_ROOT_ENV, "").strip()
    if not root_raw:
        if required:
            raise WhoScoredBootstrapError(
                "WhoScored bootstrap pointer root is not configured"
            )
        return None
    root = Path(root_raw)
    if not root.is_absolute() or ".." in root.parts:
        raise WhoScoredBootstrapError("WhoScored bootstrap pointer root is invalid")
    path = root / BOOTSTRAP_POINTER_NAME
    try:
        return _protected_bootstrap_pointer(path)
    except WhoScoredBootstrapError:
        if required or path.exists() or path.is_symlink():
            raise
        return None


def _metadata_utc(value: Any, *, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise WhoScoredBootstrapError(f"WhoScored bootstrap {label} is invalid")
    return value.astimezone(timezone.utc)


def validate_bootstrap_metadata_preflight(
    bootstrap_slots: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
    *,
    phase: str,
    run_id: Optional[str] = None,
) -> dict[str, Any]:
    """Prove the Airflow frontier cannot skip or replay an immutable slot."""

    slots = normalize_bootstrap_slots(bootstrap_slots)
    if phase not in {"publish", "issue", "complete"}:
        raise WhoScoredBootstrapError("invalid WhoScored metadata preflight phase")
    slot_dates = tuple(
        datetime.fromisoformat(slot["logical_date"].replace("Z", "+00:00"))
        for slot in slots
    )
    slot_run_ids = tuple(slot["run_id"] for slot in slots)
    if phase == "issue":
        if run_id not in slot_run_ids:
            raise WhoScoredBootstrapError(
                "WhoScored metadata preflight run is outside bootstrap slots"
            )
        required_count = slot_run_ids.index(str(run_id))
    elif phase == "complete":
        if run_id is not None:
            raise WhoScoredBootstrapError(
                "WhoScored complete preflight does not accept a run id"
            )
        required_count = len(slots)
    else:
        if run_id is not None:
            raise WhoScoredBootstrapError(
                "WhoScored publish preflight does not accept a run id"
            )
        required_count = 0

    normalized_records: list[dict[str, Any]] = []
    seen_run_ids: set[str] = set()
    for raw in records:
        if not isinstance(raw, Mapping):
            raise WhoScoredBootstrapError(
                "WhoScored metadata preflight record is invalid"
            )
        record_run_id = str(raw.get("run_id") or "")
        if not record_run_id or record_run_id in seen_run_ids:
            raise WhoScoredBootstrapError(
                "WhoScored metadata preflight DagRun identity is invalid"
            )
        seen_run_ids.add(record_run_id)
        execution_date = _metadata_utc(
            raw.get("execution_date"), label="execution date"
        )
        interval_value = raw.get("data_interval_start")
        interval_start = (
            execution_date
            if interval_value is None
            else _metadata_utc(interval_value, label="data interval start")
        )
        normalized_records.append(
            {
                "run_id": record_run_id,
                "run_type": str(raw.get("run_type") or "")
                .lower()
                .split(".")[-1],
                "external_trigger": raw.get("external_trigger"),
                "conf": raw.get("conf"),
                "state": str(raw.get("state") or "").lower().split(".")[-1],
                "execution_date": execution_date,
                "data_interval_start": interval_start,
                "terminal_task_state": str(raw.get("terminal_task_state") or "")
                .lower()
                .split(".")[-1],
            }
        )

    by_run_id = {record["run_id"]: record for record in normalized_records}
    for record in normalized_records:
        for index, logical_date in enumerate(slot_dates):
            if logical_date not in {
                record["execution_date"],
                record["data_interval_start"],
            }:
                continue
            if record["run_id"] != slot_run_ids[index]:
                raise WhoScoredBootstrapError(
                    "WhoScored bootstrap logical slot collides with another DagRun"
                )

    for index, (slot, logical_date) in enumerate(zip(slots, slot_dates)):
        record = by_run_id.get(slot["run_id"])
        if index < required_count:
            if (
                record is None
                or record["run_type"] != "scheduled"
                or record["external_trigger"] is not False
                or type(record["conf"]) is not dict
                or record["conf"] != {}
                or record["state"] != "success"
                or record["execution_date"] != logical_date
                or record["data_interval_start"] != logical_date
                or record["terminal_task_state"] != "success"
            ):
                raise WhoScoredBootstrapError(
                    "WhoScored predecessor slot lacks a sealed terminal receipt"
                )
        elif record is not None:
            raise WhoScoredBootstrapError(
                "WhoScored future bootstrap run id already exists"
            )

    automated_starts = [
        record["data_interval_start"]
        for record in normalized_records
        if record["run_type"] in {"scheduled", "backfill_job"}
    ]
    latest = max(automated_starts, default=None)
    if required_count == 0:
        if latest is not None and latest >= slot_dates[0]:
            raise WhoScoredBootstrapError(
                "WhoScored automated frontier is not before bootstrap slot zero"
            )
    elif phase == "issue" and latest != slot_dates[required_count - 1]:
        raise WhoScoredBootstrapError(
            "WhoScored automated frontier differs from the sealed predecessor"
        )
    elif phase == "complete" and (
        latest is None or latest < slot_dates[required_count - 1]
    ):
        # Completion is monotonic. Once all six exact slots and terminal tasks
        # are green, later ordinary daily DagRuns must not invalidate it.
        raise WhoScoredBootstrapError(
            "WhoScored automated frontier precedes the completed bootstrap"
        )
    return {
        "schema_version": 1,
        "status": "bootstrap-metadata-preflight-green",
        "phase": phase,
        "run_id": run_id,
        "sealed_predecessor_count": required_count,
        "latest_automated_data_interval_start": (
            None
            if latest is None
            else latest.isoformat().replace("+00:00", "Z")
        ),
    }


def _run_metadata_preflight(
    *, bootstrap_slots: Any, phase: str, run_id: Optional[str]
) -> dict[str, Any]:
    """Read the real Airflow metadata DB inside the admitted scheduler."""

    try:
        from airflow.models.dagrun import DagRun
        from airflow.models.taskinstance import TaskInstance
        from airflow.utils.session import create_session
    except (ImportError, ModuleNotFoundError) as exc:  # pragma: no cover - production
        raise WhoScoredBootstrapError(
            "Airflow metadata API is unavailable for bootstrap preflight"
        ) from exc

    slots = normalize_bootstrap_slots(bootstrap_slots)
    run_ids = [slot["run_id"] for slot in slots]
    with create_session() as session:
        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == WHOSCORED_INGEST_DAG_ID)
            .all()
        )
        task_rows = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == WHOSCORED_INGEST_DAG_ID,
                TaskInstance.run_id.in_(run_ids),
                TaskInstance.task_id == BOOTSTRAP_TERMINAL_TASK_ID,
            )
            .all()
        )
    task_states: dict[str, str] = {}
    for task in task_rows:
        if task.map_index != -1 or task.run_id in task_states:
            raise WhoScoredBootstrapError(
                "WhoScored terminal receipt task identity is ambiguous"
            )
        task_states[str(task.run_id)] = str(task.state or "")
    records = [
        {
            "run_id": dag_run.run_id,
            "run_type": dag_run.run_type,
            "external_trigger": dag_run.external_trigger,
            "conf": dag_run.conf,
            "state": dag_run.state,
            "execution_date": dag_run.execution_date,
            "data_interval_start": getattr(dag_run, "data_interval_start", None),
            "terminal_task_state": task_states.get(str(dag_run.run_id)),
        }
        for dag_run in dag_runs
    ]
    return validate_bootstrap_metadata_preflight(
        slots, records, phase=phase, run_id=run_id
    )


# Airflow is absent from host unit-test environments.  The small fallback
# types keep authority/timetable tests pure; production always imports the real
# public Airflow timetable API.
try:  # pragma: no cover - real classes are exercised by Airflow matrix tests
    import pendulum
    from airflow.exceptions import AirflowTimetableInvalid
    from airflow.timetables.base import (
        DagRunInfo,
        DataInterval,
        TimeRestriction,
        Timetable,
    )
except (ImportError, ModuleNotFoundError):  # pragma: no cover - fallback tested
    pendulum = None

    class AirflowTimetableInvalid(ValueError):
        pass

    @dataclass(frozen=True)
    class DataInterval:  # type: ignore[no-redef]
        start: datetime
        end: datetime

        @classmethod
        def exact(cls, value: datetime) -> "DataInterval":
            return cls(start=value, end=value)

    @dataclass(frozen=True)
    class DagRunInfo:  # type: ignore[no-redef]
        run_after: datetime
        data_interval: DataInterval

    @dataclass(frozen=True)
    class TimeRestriction:  # type: ignore[no-redef]
        earliest: Optional[datetime]
        latest: Optional[datetime]
        catchup: bool

    class Timetable:  # type: ignore[no-redef]
        pass


def _aware_utc(value: datetime) -> datetime:
    value = value.astimezone(timezone.utc)
    if pendulum is None:
        return value
    return pendulum.instance(value, tz="UTC")


def _future_daily_interval(
    *, now: datetime, after_start: datetime
) -> tuple[datetime, datetime]:
    """Mirror ``CronDataIntervalTimetable('0 10 * * *')`` after bootstrap.

    Airflow's scheduled run id/logical date is the interval start, while the
    run becomes eligible at the interval end.  Advancing until the start is
    newer than the last automated start prevents a duplicate of slot six.
    """

    current = now.astimezone(timezone.utc)
    run_after = datetime.combine(
        current.date(), time(NORMAL_DAILY_HOUR_UTC), tzinfo=timezone.utc
    )
    if run_after <= current:
        run_after += timedelta(days=1)
    start = run_after - timedelta(days=1)
    after = after_start.astimezone(timezone.utc)
    while start <= after:
        start += timedelta(days=1)
        run_after += timedelta(days=1)
    return _aware_utc(start), _aware_utc(run_after)


class AcceleratedBootstrapTimetable(Timetable):
    """Emit six finite backdated slots, then only future daily slots."""

    periodic = True
    active_runs_limit = 1

    def __init__(
        self,
        bootstrap_slots: Sequence[Mapping[str, Any]],
        *,
        now_factory: Any = None,
        pointer_ready: Any = None,
    ) -> None:
        self._bootstrap_slots = normalize_bootstrap_slots(bootstrap_slots)
        self._logical_dates = tuple(
            _aware_utc(
                datetime.fromisoformat(slot["logical_date"].replace("Z", "+00:00"))
            )
            for slot in self._bootstrap_slots
        )
        self._now_factory = now_factory or (
            (lambda: pendulum.now("UTC"))
            if pendulum is not None
            else (lambda: datetime.now(timezone.utc))
        )
        self._pointer_ready = pointer_ready or scheduled_run_pointer_ready

    @property
    def summary(self) -> str:
        return "WhoScored accelerated bootstrap (6 slots), then daily 10:00 UTC"

    @property
    def bootstrap_slots(self) -> list[dict[str, str]]:
        return [dict(slot) for slot in self._bootstrap_slots]

    def serialize(self) -> dict[str, Any]:
        return {"bootstrap_slots": self.bootstrap_slots}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "AcceleratedBootstrapTimetable":
        if not isinstance(data, dict) or set(data) != {"bootstrap_slots"}:
            raise AirflowTimetableInvalid(
                "WhoScored accelerated-bootstrap timetable data is invalid"
            )
        return cls(data["bootstrap_slots"])

    def validate(self) -> None:
        try:
            normalize_bootstrap_slots(self._bootstrap_slots)
        except WhoScoredBootstrapError as exc:
            raise AirflowTimetableInvalid(str(exc)) from exc

    def infer_manual_data_interval(self, *, run_after: datetime) -> DataInterval:
        # Manual runs remain manual and are rejected by rollout acceptance.
        return DataInterval.exact(_aware_utc(run_after))

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        last = (
            None
            if last_automated_data_interval is None
            else last_automated_data_interval.start.astimezone(timezone.utc)
        )
        candidate = next(
            (
                (index, slot)
                for index, slot in enumerate(self._logical_dates)
                if last is None or slot > last
            ),
            None,
        )
        if candidate is not None:
            slot_index, logical_date = candidate
            if not self._pointer_ready(
                self._bootstrap_slots[slot_index]["run_id"]
            ):
                return None
            if (
                restriction.latest is not None
                and logical_date > restriction.latest
            ):
                return None
            interval = DataInterval.exact(_aware_utc(logical_date))
            return DagRunInfo(
                run_after=_aware_utc(logical_date),
                data_interval=interval,
            )

        start, run_after = _future_daily_interval(
            now=_aware_utc(self._now_factory()),
            after_start=last or self._logical_dates[-1],
        )
        if restriction.earliest is not None:
            while start < restriction.earliest:
                start += timedelta(days=1)
                run_after += timedelta(days=1)
        if restriction.latest is not None and start > restriction.latest:
            return None
        if not self._pointer_ready(scheduled_run_id(start)):
            return None
        interval = DataInterval(
            start=_aware_utc(start),
            end=_aware_utc(run_after),
        )
        return DagRunInfo(run_after=_aware_utc(run_after), data_interval=interval)


def production_schedule() -> Any:
    """Return the accelerated timetable only when its protected pointer exists."""

    pointer = load_bootstrap_pointer(required=False)
    if pointer is None:
        return "0 10 * * *"
    return AcceleratedBootstrapTimetable(pointer["bootstrap_slots"])


def _main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    preflight = subparsers.add_parser("metadata-preflight")
    preflight.add_argument("--bootstrap-slots-json", required=True)
    preflight.add_argument(
        "--phase", required=True, choices=("publish", "issue", "complete")
    )
    preflight.add_argument("--run-id")
    args = parser.parse_args()
    try:
        slots = json.loads(args.bootstrap_slots_json)
    except json.JSONDecodeError as exc:
        raise SystemExit("WhoScored bootstrap slots JSON is invalid") from exc
    result = _run_metadata_preflight(
        bootstrap_slots=slots,
        phase=args.phase,
        run_id=args.run_id,
    )
    print(canonical_json_bytes(result).decode("utf-8"))


if __name__ == "__main__":
    _main()
