#!/usr/bin/env python3
"""Prepare and acknowledge Miro table writes; this module never calls Miro.

The caller saves a complete table_list_rows response, prepares a durable plan,
marks it ``dispatch`` before calling table_sync_rows once, and saves a fresh
complete readback. Only ``ack`` (or ``abandon`` before dispatch)
releases the pending publication. After a lost response, ``reconcile`` compares
the readback with the saved targets; it never derives success from a receipt.
The plan ID is the ownership token. No timeout releases ownership. A caller
must finish its external call before reconciliation; local locks cannot cancel
an in-flight remote request.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Any, Iterator
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
import uuid


COLUMNS = (
    "Задача", "Источник", "Статус", "Прогресс", "Дни и проверки",
    "Причина / следующий шаг", "Проверено, МСК", "GitHub",
)
SOURCE_COLUMNS = ("Источник", "Уже сделано", "Следующий шаг", "Проверено, МСК", "Сейчас")
MODES = {"acceptances", "sources"}
MSK = timezone(timedelta(hours=3))
STATUS_LABELS = {
    "draft": "Условия проверяются", "observing": "Приёмка",
    "ready": "Критерии выполнены; закрытие ожидается", "closed": "Приёмка завершена",
    "on_hold": "На паузе", "waiting": "Ожидание", "pass": "Успех подтверждён",
    "failed": "Провал", "fail": "Провал", "unknown": "Нет подтверждения",
}
RECORD_STATUSES = {"draft", "waiting", "observing", "failed", "unknown", "ready", "closed", "on_hold"}
CHECK_STATUSES = {"pass", "unknown", "fail", "waiting"}


class SyncError(ValueError):
    """Invalid input or an unsafe publication transition."""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return _json(value)


def _nonempty(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SyncError(f"{field} must be a nonempty string")
    return value


def _revision(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SyncError(f"{field} must be a nonnegative integer")
    return value


def _msk(value: Any) -> str:
    if value in (None, ""):
        return "нет подтверждения"
    if not isinstance(value, str):
        raise SyncError("Time must be an ISO-8601 string with a timezone")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SyncError(f"Invalid timestamp: {value}") from exc
    if parsed.tzinfo is None:
        raise SyncError("Time must include a timezone")
    return parsed.astimezone(MSK).strftime("%d.%m.%Y %H:%M:%S МСК")


def _generated_at(snapshot: dict[str, Any]) -> str | None:
    value = snapshot.get("generated_at")
    if value is None:
        return None
    return _utc(value)


def _utc(value: str) -> str:
    _msk(value)  # The same strict aware-timestamp validation applies.
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()


def _status(value: Any) -> str:
    text = _text(value)
    return STATUS_LABELS.get(text, text or "Нет подтверждения")


def _table_url(value: str) -> str:
    parsed = urlparse(value)
    query = parse_qs(parsed.query)
    table_id = query.get("moveToWidget") or query.get("focusWidget")
    if (parsed.scheme != "https" or parsed.netloc != "miro.com"
            or not parsed.path.startswith("/app/board/") or len(parsed.path.strip("/").split("/")) != 3
            or not table_id or len(table_id) != 1 or not table_id[0]):
        raise SyncError("table URL must identify a Miro board and table widget")
    return urlunparse(("https", "miro.com", parsed.path.rstrip("/") + "/", "",
                       urlencode({"moveToWidget": table_id[0]}), ""))


def render_targets(snapshot: dict[str, Any], mode: str = "acceptances") -> dict[str, dict[str, Any]]:
    """Render only the acceptance rows and the eight owned columns."""
    if snapshot.get("schema") != 1:
        raise SyncError("Unsupported acceptance snapshot schema")
    _revision(snapshot.get("revision"), "snapshot revision")
    if mode not in MODES:
        raise SyncError("Unsupported publication mode")
    if mode == "sources":
        return _source_targets(snapshot)
    sources = snapshot.get("sources")
    records = snapshot.get("acceptances")
    if not isinstance(sources, dict) or not isinstance(records, list):
        raise SyncError("snapshot requires sources and acceptances")
    targets: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            raise SyncError("Acceptance must be an object")
        key = _nonempty(record.get("id"), "acceptance id")
        if key in targets:
            raise SyncError(f"Duplicate acceptance key: {key}")
        source = _nonempty(record.get("source"), "acceptance source")
        source_info = sources.get(source)
        if not isinstance(source_info, dict):
            raise SyncError(f"Unknown source: {source}")
        _nonempty(record.get("title"), "acceptance title")
        if not isinstance(record.get("reason", ""), str):
            raise SyncError("acceptance reason must be text")
        if record.get("status") not in RECORD_STATUSES:
            raise SyncError(f"Unsupported acceptance status for {key}")
        if _revision(record.get("series"), "series") < 1:
            raise SyncError("series must be positive")
        criteria = record.get("criteria", [])
        days = record.get("days", [])
        if not isinstance(criteria, list) or not isinstance(days, list):
            raise SyncError("criteria and days must be arrays")
        progress = []
        for criterion in criteria:
            if not isinstance(criterion, dict):
                raise SyncError("criterion must be an object")
            if criterion.get("status") not in CHECK_STATUSES:
                raise SyncError("Unsupported criterion status")
            _revision(criterion.get("progress"), "criterion progress")
            title = _text(criterion.get("title") or criterion.get("id"))
            value = _text(criterion.get("progress")) or "нет подтверждения"
            if criterion.get("target") is not None:
                value += "/" + _text(criterion["target"])
            progress.append(f"{title}: {value} ({_status(criterion.get('status'))})")
        if record.get("series") is not None:
            progress.insert(0, "Серия: " + _text(record["series"]))
        day_lines = []
        for day in days:
            if not isinstance(day, dict):
                raise SyncError("day must be an object")
            if day.get("status") not in CHECK_STATUSES:
                raise SyncError("Unsupported day status")
            day_lines.append(f"{_text(day.get('label'))}: {_status(day.get('status'))}")
        notes = [_text(record.get("title")), _text(record.get("reason"))]
        if record.get("next_check_at"):
            notes.append("Следующая проверка: " + _msk(record["next_check_at"]))
        if record.get("deployed_revision"):
            notes.append("Версия: " + _text(record["deployed_revision"]))
        values = dict(zip(COLUMNS, (
            key, _text(source_info.get("name") or source), _status(record.get("status")),
            "\n".join(progress) or "нет подтверждения",
            "\n".join(day_lines) or "нет подтверждения",
            "\n".join(note for note in notes if note), _msk(record.get("checked_at")),
            _text(record.get("issue_url")),
        )))
        if snapshot.get("generated_at"):
            values["Проверено, МСК"] = ("Проверено: " + _msk(record.get("checked_at"))
                + "\nСводка подготовлена: " + _msk(snapshot["generated_at"]))
        targets[key] = {
            "revision": _revision(record.get("revision"), f"acceptance {key} revision"),
            "values": values,
        }
    return targets


def _source_targets(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    sources = snapshot.get("sources")
    handoffs = snapshot.get("handoffs")
    if not isinstance(sources, dict) or not isinstance(handoffs, dict):
        raise SyncError("Source snapshot requires sources and handoffs")
    targets = {}
    for source, handoff in handoffs.items():
        config = sources.get(source)
        if not isinstance(config, dict) or not isinstance(handoff, dict):
            raise SyncError(f"Unknown source or invalid handoff: {source}")
        name = _nonempty(config.get("name"), "configured source name")
        row_id = _nonempty(config.get("miro_row_id"), "configured source miro_row_id")
        if name in targets:
            raise SyncError(f"Duplicate configured source name: {name}")
        for time_field in ("checked_at", "recorded_at"):
            _nonempty(handoff.get(time_field), f"handoff {time_field}")
        values = {
            "Уже сделано": _nonempty(handoff.get("done") or handoff.get("summary"), "handoff done/summary"),
            "Следующий шаг": _nonempty(handoff.get("next_step"), "handoff next_step"),
            "Проверено, МСК": ("Проверено: " + _msk(handoff["checked_at"])
                               + "\nhandoff сохранён: " + _msk(handoff["recorded_at"])),
            "Сейчас": _nonempty(handoff.get("status"), "handoff status"),
        }
        targets[name] = {"revision": snapshot["revision"], "required_row_id": row_id, "values": values,
                         "checked_at": _utc(handoff["checked_at"]), "recorded_at": _utc(handoff["recorded_at"])}
    return targets


def _cell_text(cell: dict[str, Any]) -> str:
    if cell.get("valueType") == "select" and isinstance(cell.get("options"), list):
        options = cell["options"]
        if len(options) > 1 or any(not isinstance(option.get("displayValue"), str) for option in options):
            raise SyncError("Select cell must contain at most one display value")
        return options[0]["displayValue"] if options else ""
    value = cell.get("content", cell.get("value"))
    # A link column can return one link object instead of its URL string.
    if isinstance(value, list) and len(value) == 1 and isinstance(value[0], dict):
        value = value[0].get("url", value)
    if isinstance(value, dict) and isinstance(value.get("url"), str):
        value = value["url"]
    return _text(value)


def read_remote(remote: dict[str, Any], table_url: str, mode: str = "acceptances",
                targets: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    """Validate a complete, unfiltered table_list_rows result."""
    if mode not in MODES:
        raise SyncError("Unsupported publication mode")
    if remote.get("isError"):
        raise SyncError("Miro readback reports an error")
    if isinstance(remote.get("structuredContent"), dict):
        remote = remote["structuredContent"]
    if remote.get("miro_url") is None or _table_url(remote["miro_url"]) != table_url:
        raise SyncError("Readback is for a different table")
    rows = remote.get("rows")
    total = remote.get("total")
    if (not isinstance(rows, list) or isinstance(total, bool) or not isinstance(total, int)
            or len(rows) != total or any(remote.get(k) for k in ("cursor", "next_cursor", "nextCursor"))
            or remote.get("filter_by")):
        raise SyncError("Readback must contain all unfiltered rows and no next cursor")
    columns = remote.get("columns")
    if not isinstance(columns, list):
        raise SyncError("Readback must include column metadata")
    column_types = {}
    column_options = {}
    for column in columns:
        title = column.get("column_title")
        if title in column_types:
            raise SyncError(f"Duplicate column title: {title}")
        column_types[title] = column.get("column_type")
        column_options[title] = column.get("selectOptions")
    managed_columns = SOURCE_COLUMNS if mode == "sources" else COLUMNS
    for title in managed_columns:
        allowed = {"select"} if mode == "sources" and title == "Сейчас" else (
            {"text", "link"} if title == "GitHub" else {"text"})
        if column_types.get(title) not in allowed:
            raise SyncError(f"Managed column {title} must exist and have a supported type")
    if mode == "sources":
        options = column_options.get("Сейчас")
        if not isinstance(options, list):
            raise SyncError("Source status column must include existing select options")
        allowed_statuses = {option.get("displayValue") for option in options}
        if any(target["values"]["Сейчас"] not in allowed_statuses for target in (targets or {}).values()):
            raise SyncError("Handoff status is not an existing Miro select option")
    result = {}
    row_ids = set()
    for row in rows:
        if not isinstance(row, dict):
            raise SyncError("Remote row must be an object")
        row_id = _nonempty(row.get("rowId"), "remote rowId")
        if row_id in row_ids:
            raise SyncError(f"Duplicate remote rowId: {row_id}")
        row_ids.add(row_id)
        cells = row.get("cells")
        if not isinstance(cells, list):
            raise SyncError("Remote cells must be an array")
        values = {}
        for cell in cells:
            if not isinstance(cell, dict):
                raise SyncError("Remote cell must be an object")
            title = cell.get("columnTitle")
            if title in values:
                raise SyncError(f"Duplicate cell column: {title}")
            values[title] = _cell_text(cell)
        key = values.get("Источник" if mode == "sources" else "Задача", "")
        if not key:
            continue  # A foreign row without our stable key remains untouched.
        if key in result:
            raise SyncError(f"Duplicate remote acceptance key: {key}")
        result[key] = {"rowId": row_id, "values": values}
    return result


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def _locked_state(path: str | Path) -> Iterator[tuple[Path, dict[str, Any]]]:
    state_path = Path(path).resolve()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = state_path.with_name(state_path.name + ".lock")
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(fd, "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if state_path.exists():
            try:
                state = json.loads(state_path.read_text())
            except (ValueError, OSError) as exc:
                raise SyncError("Publication state is unreadable; do not reset it") from exc
            if not isinstance(state, dict) or state.get("schema") != 1:
                raise SyncError("Unsupported publication state; do not reset it")
        else:
            state = {"schema": 1, "table_url": None, "row_ids": {}, "pending": None,
                     "last_ack": None}
        yield state_path, state


def _save(path: Path, state: dict[str, Any]) -> None:
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(_json(state) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _bind_rows(state: dict[str, Any], targets: dict[str, Any], remote: dict[str, Any]) -> None:
    mapping = state["row_ids"]
    remote_keys_by_id = {row["rowId"]: key for key, row in remote.items()}
    for key in targets:
        known = mapping.get(key)
        found = remote.get(key)
        required = targets[key].get("required_row_id")
        if required is not None and (found is None or found["rowId"] != required):
            raise SyncError(f"Source row missing or configured rowId differs: {key}")
        if known is not None:
            if found is None:
                raise SyncError(f"Previously bound row missing or key changed: {key}")
            if found["rowId"] != known or remote_keys_by_id.get(known) != key:
                raise SyncError(f"Stable rowId changed for {key}; explicit repair required")
        elif found is not None:
            if found["rowId"] in mapping.values():
                raise SyncError(f"Remote rowId is already bound to a different key: {key}")
            mapping[key] = found["rowId"]


def _operations(targets: dict[str, Any], remote: dict[str, Any]) -> list[dict[str, Any]]:
    operations = []
    for key in sorted(targets):
        target = targets[key]
        current = remote.get(key)
        # Include only changed owned cells when updating an existing row.
        cells = [{"columnTitle": title, "value": target["values"][title]}
                 for title in sorted(target["values"])
                 if current is None or current["values"].get(title, "") != target["values"][title]]
        if cells:
            row = {"cells": cells}
            if current is not None:
                row["rowId"] = current["rowId"]
            operations.append(row)
    return operations


def _public_plan(state: dict[str, Any]) -> dict[str, Any]:
    pending = state["pending"]
    return {"plan_id": pending["plan_id"], "miro_url": state["table_url"], "rows": pending["rows"]}


def _owned_pending(state: dict[str, Any], plan_id: str) -> dict[str, Any]:
    pending = state.get("pending")
    if not isinstance(pending, dict) or pending.get("plan_id") != plan_id:
        raise SyncError("No pending publication owned by this plan_id")
    return pending


def prepare(snapshot: dict[str, Any], remote: dict[str, Any], table_url: str,
            state_path: str | Path, mode: str = "acceptances") -> dict[str, Any]:
    targets = render_targets(snapshot, mode)
    table_url = _table_url(table_url)
    current = read_remote(remote, table_url, mode, targets)
    target_hash = _digest(targets)
    generated_at = _generated_at(snapshot)
    with _locked_state(state_path) as (path, state):
        if state.get("pending") is not None:
            raise SyncError("A publication is pending; reconcile or ack its plan_id first")
        if state.get("table_url") not in (None, table_url):
            raise SyncError("Publication state belongs to another table")
        if state.get("mode") not in (None, mode):
            raise SyncError("Publication state belongs to another mode")
        last = state.get("last_ack")
        if last:
            if snapshot["revision"] < last["revision"]:
                raise SyncError("Stale snapshot revision")
            if snapshot["revision"] == last["revision"]:
                prior_time = last.get("generated_at")
                if prior_time is not None and (generated_at is None or generated_at < prior_time):
                    raise SyncError("Stale snapshot generated_at")
                if target_hash != last["target_hash"] and not (
                        prior_time is not None and generated_at is not None and generated_at > prior_time):
                    raise SyncError("Snapshot content changed without a new revision or generation time")
        state["table_url"] = table_url
        state["mode"] = mode
        _bind_rows(state, targets, current)
        if mode == "sources":
            for key, target in targets.items():
                prior = state.get("source_times", {}).get(key, {})
                if any(target[field] < prior.get(field, "") for field in ("checked_at", "recorded_at")):
                    raise SyncError(f"Stale handoff for {key}")
        state["pending"] = {
            "plan_id": str(uuid.uuid4()), "revision": snapshot["revision"],
            "generated_at": generated_at, "phase": "prepared", "mode": mode,
            "key_column": "Источник" if mode == "sources" else "Задача",
            "target_hash": target_hash, "targets": targets,
            "rows": _operations(targets, current), "prepared_at": _utc_now(),
        }
        _save(path, state)
        return _public_plan(state)


def dispatch(plan_id: str, state_path: str | Path) -> dict[str, Any]:
    """Record the sole permitted attempt before making the external tool call."""
    with _locked_state(state_path) as (path, state):
        pending = _owned_pending(state, plan_id)
        if pending["phase"] != "prepared":
            raise SyncError("Plan already dispatched; read back before reconciliation")
        if not pending["rows"]:
            raise SyncError("Plan has no changes; acknowledge the readback instead")
        pending["phase"] = "dispatched"
        pending["dispatched_at"] = _utc_now()
        _save(path, state)
        return _public_plan(state)


def abandon(plan_id: str, state_path: str | Path) -> dict[str, Any]:
    """Explicitly release a plan only while no external attempt was permitted."""
    with _locked_state(state_path) as (path, state):
        pending = _owned_pending(state, plan_id)
        if pending["phase"] != "prepared":
            raise SyncError("Cannot abandon a dispatched plan; resolve its outcome")
        state["pending"] = None
        _save(path, state)
        return {"plan_id": plan_id, "abandoned": True}


def _retry(state: dict[str, Any], pending: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    _bind_rows(state, pending["targets"], current)
    pending["rows"] = _operations(pending["targets"], current)
    pending["plan_id"] = str(uuid.uuid4())
    pending["phase"] = "prepared"
    pending.pop("dispatched_at", None)
    pending["reconciled_at"] = _utc_now()
    return _public_plan(state)


def reconcile(plan_id: str, remote: dict[str, Any], state_path: str | Path) -> dict[str, Any]:
    """Rebuild undelivered operations from a fresh, complete readback.

    Existing keys always resolve to updates by rowId. Missing previously bound
    keys fail closed. Unbound inserts missing after an ambiguous call require
    explicit investigation; they are not blindly inserted again.
    """
    with _locked_state(state_path) as (path, state):
        pending = _owned_pending(state, plan_id)
        if pending["phase"] != "dispatched":
            raise SyncError("Only a dispatched plan needs reconciliation")
        current = read_remote(remote, state["table_url"], state["mode"], pending["targets"])
        targets = pending["targets"]
        _bind_rows(state, targets, current)
        missing_inserts = [key for key in targets if key not in current and key not in state["row_ids"]]
        if missing_inserts:
            raise SyncError("Insert outcome unresolved for: " + ", ".join(missing_inserts)
                            + "; pending plan retained, investigate before retry")
        output = _retry(state, pending, current)
        _save(path, state)
        return output


def resolve(plan_id: str, remote: dict[str, Any], state_path: str | Path,
            resolution_evidence: str | Path, *, retry_missing_inserts: bool = False) -> dict[str, Any]:
    """Permit retry only after explicit evidence of a completed, unapplied call.

    The evidence JSON must attest plan_id, outcome="not_applied",
    request_finished=true and a nonempty summary. A caller must ground that
    attestation in a definite tool failure; a missing row alone is insufficient.
    """
    if retry_missing_inserts is not True:
        raise SyncError("Resolution requires explicit retry_missing_inserts")
    evidence_path = Path(resolution_evidence).resolve()
    try:
        evidence_bytes = evidence_path.read_bytes()
        evidence = json.loads(evidence_bytes)
    except (ValueError, OSError) as exc:
        raise SyncError("Cannot read resolution evidence") from exc
    if (not isinstance(evidence, dict) or evidence.get("plan_id") != plan_id
            or evidence.get("outcome") != "not_applied" or evidence.get("request_finished") is not True):
        raise SyncError("Evidence must prove this plan's completed, not_applied request")
    summary = _nonempty(evidence.get("summary"), "resolution summary")
    with _locked_state(state_path) as (path, state):
        pending = _owned_pending(state, plan_id)
        if pending["phase"] != "dispatched":
            raise SyncError("Only a dispatched plan needs resolution")
        current = read_remote(remote, state["table_url"], state["mode"], pending["targets"])
        state.setdefault("resolutions", []).append({
            "plan_id": plan_id, "path": str(evidence_path),
            "sha256": hashlib.sha256(evidence_bytes).hexdigest(), "summary": summary,
            "resolved_at": _utc_now(),
        })
        output = _retry(state, pending, current)
        _save(path, state)
        return output


def ack(plan_id: str, remote: dict[str, Any], state_path: str | Path) -> dict[str, Any]:
    with _locked_state(state_path) as (path, state):
        if state.get("pending") is None and (state.get("last_ack") or {}).get("plan_id") == plan_id:
            read_remote(remote, state["table_url"], state["mode"])
            # Retry of a lost local receipt: retain the original verification time.
            return {**state["last_ack"], "row_ids": dict(state["row_ids"])}
        pending = _owned_pending(state, plan_id)
        if pending["phase"] != "dispatched" and pending["rows"]:
            raise SyncError("Plan must be dispatched before acknowledging a write")
        current = read_remote(remote, state["table_url"], state["mode"], pending["targets"])
        _bind_rows(state, pending["targets"], current)
        if _operations(pending["targets"], current):
            raise SyncError("Readback does not confirm every planned value; pending retained")
        acknowledged = {"plan_id": plan_id, "revision": pending["revision"],
                        "generated_at": pending.get("generated_at"),
                        "target_hash": pending["target_hash"], "synced_at": _utc_now()}
        state["last_ack"] = acknowledged
        if state["mode"] == "sources":
            times = state.setdefault("source_times", {})
            for key, target in pending["targets"].items():
                times[key] = {field: target[field] for field in ("checked_at", "recorded_at")}
        state["pending"] = None
        _save(path, state)
        return {**acknowledged, "row_ids": dict(state["row_ids"])}


def _read_file(path: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text())
    except (ValueError, OSError) as exc:
        raise SyncError(f"Cannot read JSON from {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SyncError(f"Expected a JSON object in {path}")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("prepare", "dispatch", "abandon", "reconcile", "resolve", "ack"):
        sub = commands.add_parser(command)
        if command not in {"dispatch", "abandon"}:
            sub.add_argument("--remote", required=True, help="Complete unfiltered Miro table readback JSON")
        sub.add_argument("--state", required=True, help="Local durable publication state")
        if command == "prepare":
            sub.add_argument("--snapshot", required=True)
            sub.add_argument("--table-url", required=True)
            sub.add_argument("--mode", choices=sorted(MODES), default="acceptances")
        else:
            sub.add_argument("--plan-id", required=True)
        if command == "resolve":
            sub.add_argument("--resolution-evidence", required=True)
            sub.add_argument("--retry-missing-inserts", action="store_true")
    args = parser.parse_args(argv)
    try:
        remote = _read_file(args.remote) if hasattr(args, "remote") else None
        if args.command == "prepare":
            output = prepare(_read_file(args.snapshot), remote, args.table_url, args.state, args.mode)
        elif args.command == "dispatch":
            output = dispatch(args.plan_id, args.state)
        elif args.command == "abandon":
            output = abandon(args.plan_id, args.state)
        elif args.command == "reconcile":
            output = reconcile(args.plan_id, remote, args.state)
        elif args.command == "resolve":
            output = resolve(args.plan_id, remote, args.state, args.resolution_evidence,
                             retry_missing_inserts=args.retry_missing_inserts)
        else:
            output = ack(args.plan_id, remote, args.state)
        print(_json(output))
        return 0
    except (SyncError, OSError) as exc:
        print(f"acceptance_miro: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
