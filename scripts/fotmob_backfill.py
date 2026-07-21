#!/usr/bin/env python3
"""Run the reviewed issue-930 FotMob v2 scope set through the writer fence.

This coordinator is intentionally limited to an isolated deployment admitted
with ``deploy.py --keep-paused``.  It acquires one synthetic publication
generation, triggers the parent ingest DAG (never a writer child directly),
waits for its exact Silver child, then abandons the unclaimed ready candidate.

Every identity is written durably before the corresponding external action.
After Airflow acknowledges a trigger, an absent or non-terminal DagRun is
ambiguous and keeps the singleton lock for the explicit ``recover`` command.
A write-ahead trigger intent with no exact run is safely releasable only after
all isolated writers are paused and proven idle.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

sys.dont_write_bytecode = True
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]

try:  # package import in tests / ``python -m``
    from scripts import fotmob_runtime as runtime_binding
    from scripts.fotmob_acceptance import (
        APPROVED_SCOPE_ARTIFACT,
        APPROVED_SCOPE_ARTIFACT_SHA256,
        APPROVED_SCOPE_COUNT,
        REQUIRED_SCOPE_ENTITIES,
        load_scopes,
        validate_approved_scope_contract,
    )
    from scrapers.fotmob.source_refresh import (
        PLAYER_SOURCE_REFRESH_ARTIFACT,
        PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB,
        PLAYER_SOURCE_REFRESH_MAX_REQUESTS,
        PLAYER_SOURCE_REFRESH_PROFILE,
        PLAYER_SOURCE_REFRESH_SHA256,
        PLAYER_SOURCE_REFRESH_TARGET_COUNT,
        PlayerSourceRefreshContractError,
        load_player_source_refresh_contract,
    )
except ModuleNotFoundError:  # direct ``python scripts/fotmob_backfill.py``
    if str(REPOSITORY_ROOT) not in sys.path:
        sys.path.insert(0, str(REPOSITORY_ROOT))
    import fotmob_runtime as runtime_binding
    from fotmob_acceptance import (
        APPROVED_SCOPE_ARTIFACT,
        APPROVED_SCOPE_ARTIFACT_SHA256,
        APPROVED_SCOPE_COUNT,
        REQUIRED_SCOPE_ENTITIES,
        load_scopes,
        validate_approved_scope_contract,
    )
    from scrapers.fotmob.source_refresh import (
        PLAYER_SOURCE_REFRESH_ARTIFACT,
        PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB,
        PLAYER_SOURCE_REFRESH_MAX_REQUESTS,
        PLAYER_SOURCE_REFRESH_PROFILE,
        PLAYER_SOURCE_REFRESH_SHA256,
        PLAYER_SOURCE_REFRESH_TARGET_COUNT,
        PlayerSourceRefreshContractError,
        load_player_source_refresh_contract,
    )


SCHEMA_VERSION = "fotmob-issue-930-backfill-v1"
CONFIRM_RUN = "RUN_FOTMOB_ISSUE_930_BACKFILL"
CONFIRM_RECOVER = "RECOVER_FOTMOB_ISSUE_930_BACKFILL"
PUBLICATION_OWNER_DAG_ID = "fotmob_issue_930_backfill"
PUBLICATION_TTL_SECONDS = 14 * 24 * 60 * 60
INGEST_DAG_ID = "dag_ingest_fotmob"
SILVER_DAG_ID = "dag_transform_fotmob_silver"
DAILY_DAG_ID = "dag_trigger_fotmob_daily"
DAGS = (INGEST_DAG_ID, SILVER_DAG_ID, DAILY_DAG_ID)
MODES = frozenset({"backfill", "replay"})
ISSUE_930_SCOPE_ENTITIES = (
    "season",
    "leaderboards",
    "matches",
    "teams",
    "players",
)
if frozenset(ISSUE_930_SCOPE_ENTITIES) != REQUIRED_SCOPE_ENTITIES:
    raise RuntimeError("issue-930 backfill entities differ from acceptance contract")
_MODE_INTERVAL_PARITY = {"backfill": 0, "replay": 1}
_PLAN_SIGNATURE_RE = re.compile(r"fmplan1-[0-9a-f]{64}")


class BackfillError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Atomically publish and fsync both the report and its directory entry."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, default=str)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        temporary.replace(path)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _timestamp(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise BackfillError(f"invalid timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise BackfillError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _validate_sha(value: str) -> str:
    normalized = str(value or "").strip().casefold()
    if re.fullmatch(r"[0-9a-f]{40}", normalized) is None:
        raise BackfillError("--expected-git-sha must be a full 40-hex Git SHA")
    return normalized


def _parse_json_array(output: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    for index, character in enumerate(output):
        if character != "[":
            continue
        try:
            value, _ = decoder.raw_decode(output[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return value
    raise BackfillError("Airflow command did not return a JSON array of objects")


def _deployment_context(args: argparse.Namespace) -> Mapping[str, Any]:
    cached = getattr(args, "_deployment_context_cache", None)
    if isinstance(cached, Mapping):
        return cached
    try:
        context = runtime_binding.load_deployment_context(
            args.deployment_report,
            project=args.project,
            compose_file=args.compose_file,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise BackfillError(str(exc)) from exc
    setattr(args, "_deployment_context_cache", context)
    return context


def _compose_base(args: argparse.Namespace) -> tuple[str, ...]:
    try:
        return runtime_binding.compose_base(
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise BackfillError(str(exc)) from exc


def _compose_environment(args: argparse.Namespace) -> dict[str, str]:
    return runtime_binding.compose_environment(_deployment_context(args))


def _airflow(
    args: argparse.Namespace,
    *command: str,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> str:
    return run(
        (
            *_compose_base(args),
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            *command,
        ),
        check=True,
        capture_output=True,
        text=True,
        env=_compose_environment(args),
    ).stdout


def _container_python_json(
    args: argparse.Namespace,
    *,
    code: str,
    marker: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> Any:
    output = run(
        (
            *_compose_base(args),
            "exec",
            "-T",
            "airflow-scheduler",
            "python",
            "-c",
            code,
        ),
        check=True,
        capture_output=True,
        text=True,
        env=_compose_environment(args),
    ).stdout
    for line in reversed(output.splitlines()):
        if not line.startswith(marker):
            continue
        try:
            return json.loads(line.removeprefix(marker))
        except json.JSONDecodeError as exc:
            raise BackfillError(f"invalid {marker} evidence") from exc
    raise BackfillError(f"container did not emit {marker} evidence")


def validate_live_deployment(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    try:
        return runtime_binding.validate_live_deployment(
            _deployment_context(args),
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
            require_running=True,
            run=run,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise BackfillError(str(exc)) from exc


def require_no_active_publication(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    try:
        return runtime_binding.assert_no_active_fotmob_publication(
            _deployment_context(args), run=run
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise BackfillError(str(exc)) from exc


def _load_scope_contract(path: Path, reviewed_sha256: str) -> dict[str, Any]:
    reviewed = str(reviewed_sha256 or "").strip().casefold()
    if reviewed != APPROVED_SCOPE_ARTIFACT_SHA256:
        raise BackfillError(
            "--scope-sha256 must equal the reviewed issue-930 artifact SHA-256"
        )
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise BackfillError(f"cannot read scope artifact: {exc}") from exc
    actual = hashlib.sha256(content).hexdigest()
    if actual != APPROVED_SCOPE_ARTIFACT_SHA256:
        raise BackfillError("scope artifact differs from its reviewed SHA-256")
    try:
        scopes = load_scopes(path, content=content)
        approved = validate_approved_scope_contract("verify", scopes)
    except ValueError as exc:
        raise BackfillError(str(exc)) from exc
    identities = [scope.identity for scope in scopes]
    if len(identities) != APPROVED_SCOPE_COUNT or len(set(identities)) != len(
        identities
    ):
        raise BackfillError("scope artifact does not contain exactly 158 identities")
    return {
        "name": approved["name"],
        "artifact": str(path.resolve()),
        "sha256": actual,
        "count": len(identities),
        "identities": identities,
    }


def _load_source_refresh_contract(
    args: argparse.Namespace,
) -> dict[str, Any] | None:
    """Admit only the fixed seven-player artifact; no CLI target list exists."""

    profile = str(getattr(args, "source_refresh_profile", "") or "").strip()
    supplied_sha = (
        str(getattr(args, "source_refresh_targets_sha256", "") or "").strip().casefold()
    )
    if not profile and not supplied_sha:
        setattr(args, "_source_refresh_contract", None)
        return None
    if profile != PLAYER_SOURCE_REFRESH_PROFILE:
        raise BackfillError("unknown --source-refresh-profile")
    if supplied_sha != PLAYER_SOURCE_REFRESH_SHA256:
        raise BackfillError(
            "--source-refresh-targets-sha256 differs from reviewed artifact"
        )
    try:
        contract = load_player_source_refresh_contract(
            REPOSITORY_ROOT / PLAYER_SOURCE_REFRESH_ARTIFACT
        )
    except PlayerSourceRefreshContractError as exc:
        raise BackfillError(str(exc)) from exc
    setattr(args, "_source_refresh_contract", contract)
    return contract


def inspect_writer_state(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    """Read all three pause flags and active runs in one metadata transaction."""

    marker = "FOTMOB_BACKFILL_WRITER_STATE_JSON="
    code = (
        "import json; from sqlalchemy import and_; "
        "from airflow.models import DagModel,DagRun; "
        "from airflow.settings import Session; s=Session(); "
        f"ids={DAGS!r}; "
        "q=s.query(DagModel.dag_id,DagModel.is_paused,DagRun.run_id,"
        "DagRun.state).outerjoin(DagRun,and_(DagRun.dag_id==DagModel.dag_id,"
        "DagRun.state.in_(('running','queued')))).filter(DagModel.dag_id.in_(ids)); "
        "rows=[{'dag_id':d,'is_paused':p,'run_id':r,'state':"
        "getattr(st,'value',st)} for d,p,r,st in q.all()]; "
        f"print('{marker}'+json.dumps(rows,sort_keys=True)); s.close()"
    )
    payload = _container_python_json(args, code=code, marker=marker, run=run)
    if not isinstance(payload, list) or any(
        not isinstance(item, Mapping) for item in payload
    ):
        raise BackfillError("atomic writer-state query returned invalid evidence")
    paused: dict[str, bool] = {}
    active: dict[str, dict[str, list[str]]] = {}
    for row in payload:
        dag_id = str(row.get("dag_id") or "")
        if dag_id not in DAGS:
            raise BackfillError("writer-state query returned an unknown DAG")
        paused[dag_id] = row.get("is_paused") in {True, "True", "true", "1", 1}
        run_id = row.get("run_id")
        state = str(row.get("state") or "").casefold()
        if run_id is not None and state in {"running", "queued"}:
            active.setdefault(dag_id, {}).setdefault(state, []).append(str(run_id))
    if set(paused) != set(DAGS):
        raise BackfillError("writer-state query misses an admitted DAG")
    return {"pause_states": paused, "active_runs": active}


def _require_writers_stopped(state: Mapping[str, Any]) -> None:
    pause_states = state.get("pause_states")
    if not isinstance(pause_states, Mapping):
        raise BackfillError("writer state has no pause evidence")
    unpaused = [dag_id for dag_id in DAGS if pause_states.get(dag_id) is not True]
    if unpaused:
        raise BackfillError(f"FotMob DAGs are not all paused: {unpaused!r}")
    if state.get("active_runs"):
        raise BackfillError(
            f"FotMob DAGs still have active runs: {state['active_runs']!r}"
        )


def _pause_all(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    errors = []
    for dag_id in DAGS:
        try:
            _airflow(args, "dags", "pause", dag_id, run=run)
        except Exception as exc:  # continue so every writer gets a pause attempt
            errors.append(f"{dag_id}: {type(exc).__name__}: {exc}")
    try:
        state = inspect_writer_state(args, run=run)
        _require_writers_stopped(state)
    except Exception as exc:
        detail = "; ".join(errors) if errors else "pause commands completed"
        raise BackfillError(
            f"writer quiescence is unproven ({detail}): {type(exc).__name__}: {exc}"
        ) from exc
    return state


def _publication_envelope(
    args: argparse.Namespace,
    mode: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    context = _deployment_context(args)
    attempt = int(args.publication_attempt)
    if attempt <= 0:
        raise BackfillError("--publication-attempt must be a positive integer")
    # Rollback owns generated_at + attempt seconds.  Backfill uses a separate
    # one-day namespace and even/odd mode slots so every (mode, attempt) pair
    # has a distinct deterministic generation on the same deployment.
    start = _timestamp(context["generated_at"]) + timedelta(
        seconds=86_400 + (attempt - 1) * 2 + _MODE_INTERVAL_PARITY[mode]
    )
    end = start + timedelta(seconds=1)
    expected_start = start.isoformat(timespec="microseconds")
    expected_end = end.isoformat(timespec="microseconds")
    marker = "FOTMOB_BACKFILL_BINDING_JSON="
    code = (
        "import json,sys; sys.path.insert(0,'/opt/airflow/dags'); "
        "from utils.fotmob_publication import make_publication_binding,make_generation_id; "
        f"b=make_publication_binding(owner='isolated',data_interval_start={start.isoformat()!r},"
        f"data_interval_end={end.isoformat()!r},fingerprint={context['git_sha']!r}); "
        f"print('{marker}'+json.dumps({{'generation_id':make_generation_id(b),'binding':b}},"
        "sort_keys=True))"
    )
    payload = _container_python_json(args, code=code, marker=marker, run=run)
    binding = payload.get("binding") if isinstance(payload, Mapping) else None
    generation_id = (
        str(payload.get("generation_id", "")) if isinstance(payload, Mapping) else ""
    )
    if (
        not isinstance(binding, Mapping)
        or binding.get("schema") != "fotmob-publication-v1"
        or binding.get("source") != "fotmob"
        or binding.get("owner") != "isolated"
        or binding.get("runtime_fingerprint") != context["git_sha"]
        or binding.get("data_interval_start") != expected_start
        or binding.get("data_interval_end") != expected_end
        or re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            generation_id,
        )
        is None
    ):
        raise BackfillError("synthetic publication binding is not exact")
    return {"generation_id": generation_id, "binding": dict(binding)}


def _initialize_publication(
    args: argparse.Namespace,
    publication: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    marker = "FOTMOB_BACKFILL_PUBLICATION_JSON="
    binding_json = json.dumps(publication["binding"], sort_keys=True)
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        f"b=json.loads({binding_json!r}); "
        "r=ControlStore.from_env().initialize_publication_generation("
        f"{str(publication['generation_id'])!r},dag_id={PUBLICATION_OWNER_DAG_ID!r},"
        f"binding=b,source='fotmob',ttl_seconds={PUBLICATION_TTL_SECONDS}); "
        f"print('{marker}'+json.dumps(r,default=str,sort_keys=True))"
    )
    state = _container_python_json(args, code=code, marker=marker, run=run)
    if (
        not isinstance(state, Mapping)
        or state.get("generation_id") != publication["generation_id"]
        or state.get("binding") != publication["binding"]
        or state.get("owner_dag_id") != PUBLICATION_OWNER_DAG_ID
        or state.get("status") != "running"
        or state.get("phase") != "writing"
        or state.get("active") is not True
    ):
        raise BackfillError("publication generation was not acquired exactly")
    return dict(state)


def _get_publication(
    args: argparse.Namespace,
    generation_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any] | None:
    marker = "FOTMOB_BACKFILL_PUBLICATION_JSON="
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        "r=ControlStore.from_env().get_publication_generation("
        f"{generation_id!r},source='fotmob'); "
        f"print('{marker}'+json.dumps(r,default=str,sort_keys=True))"
    )
    payload = _container_python_json(args, code=code, marker=marker, run=run)
    if payload is not None and not isinstance(payload, Mapping):
        raise BackfillError("publication lookup returned invalid evidence")
    return None if payload is None else dict(payload)


def _transition_publication(
    args: argparse.Namespace,
    generation_id: str,
    *,
    action: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    if action == "abandon":
        expression = (
            "s.complete_publication_generation("
            f"{generation_id!r},consumer=None,published=False,source='fotmob')"
        )
    elif action == "fail_release":
        expression = (
            "s.fail_publication_generation("
            f"{generation_id!r},safe_to_release=True,source='fotmob')"
        )
    else:  # pragma: no cover - internal callers use the two constants above
        raise AssertionError(action)
    marker = "FOTMOB_BACKFILL_PUBLICATION_JSON="
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        f"s=ControlStore.from_env(); r={expression}; "
        f"print('{marker}'+json.dumps(r,default=str,sort_keys=True))"
    )
    payload = _container_python_json(args, code=code, marker=marker, run=run)
    if not isinstance(payload, Mapping):
        raise BackfillError("publication transition returned invalid evidence")
    return dict(payload)


def _publication_summary(state: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if state is None:
        return None
    candidate = state.get("candidate")
    return {
        "generation_id": state.get("generation_id"),
        "status": state.get("status"),
        "phase": state.get("phase"),
        "active": state.get("active"),
        "released": state.get("released"),
        "published": state.get("published"),
        "candidate": (
            {
                "generation_id": candidate.get("generation_id"),
                "digest": candidate.get("digest"),
                "transform_task_ids": candidate.get("transform_task_ids"),
            }
            if isinstance(candidate, Mapping)
            else None
        ),
    }


def _best_effort_publication_summary(
    args: argparse.Namespace,
    generation_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any] | None:
    try:
        return _publication_summary(_get_publication(args, generation_id, run=run))
    except Exception as exc:
        return {
            "generation_id": generation_id,
            "lookup_error": f"{type(exc).__name__}: {exc}",
        }


def _exact_run(
    args: argparse.Namespace,
    dag_id: str,
    run_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any] | None:
    rows = _parse_json_array(
        _airflow(
            args,
            "dags",
            "list-runs",
            "-d",
            dag_id,
            "--output",
            "json",
            run=run,
        )
    )
    matches = [row for row in rows if str(row.get("run_id")) == run_id]
    if len(matches) > 1:
        raise BackfillError(f"duplicate exact DagRun identity for {dag_id}")
    return matches[0] if matches else None


def _validation_xcom(
    args: argparse.Namespace,
    ingest_run_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> Mapping[str, Any]:
    marker = "FOTMOB_BACKFILL_VALIDATION_JSON="
    code = (
        "import json; from airflow.models.xcom import XCom; "
        "from airflow.settings import Session; s=Session(); "
        "r=XCom.get_one("
        f"run_id={ingest_run_id!r},dag_id={INGEST_DAG_ID!r},"
        "task_id='validate_data',key='return_value',session=s); "
        f"print('{marker}'+json.dumps(r,default=str,sort_keys=True)); s.close()"
    )
    payload = _container_python_json(args, code=code, marker=marker, run=run)
    if not isinstance(payload, Mapping):
        raise BackfillError("exact ingest validation XCom is absent")
    return payload


def _candidate(
    state: Mapping[str, Any], publication: Mapping[str, Any]
) -> Mapping[str, Any]:
    candidate = state.get("candidate")
    if (
        state.get("generation_id") != publication["generation_id"]
        or state.get("binding") != publication["binding"]
        or not isinstance(candidate, Mapping)
        or candidate.get("generation_id") != publication["generation_id"]
        or re.fullmatch(r"[0-9a-f]{64}", str(candidate.get("digest") or "")) is None
    ):
        raise BackfillError("publication does not contain the exact Silver candidate")
    return candidate


def _validation_summary(
    payload: Mapping[str, Any],
    *,
    mode: str,
    generation_id: str,
    scope_contract: Mapping[str, Any],
    source_refresh_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    selection = payload.get("selection")
    if source_refresh_contract is not None:
        budget = payload.get("budget")
        transport = payload.get("transport")
        expected_source = {
            key: source_refresh_contract[key]
            for key in (
                "profile",
                "artifact",
                "sha256",
                "target_count",
                "targets",
                "plan_signature",
            )
        }
        outcomes = (
            selection.get("target_outcomes") if isinstance(selection, Mapping) else None
        )
        valid_outcomes = (
            isinstance(outcomes, list)
            and len(outcomes) == PLAYER_SOURCE_REFRESH_TARGET_COUNT
            and all(
                isinstance(item, Mapping)
                and set(item)
                == {
                    "competition_id",
                    "source_season_key",
                    "team_id",
                    "player_id",
                    "status",
                }
                and item.get("status") in {"success", "not_available"}
                for item in outcomes
            )
        )
        if valid_outcomes:
            observed_targets = [
                {key: item[key] for key in source_refresh_contract["targets"][0]}
                for item in outcomes
            ]
            valid_outcomes = observed_targets == source_refresh_contract["targets"]
        if (
            payload.get("status") != "success"
            or payload.get("run_id") != generation_id
            or payload.get("mode") != "backfill"
            or mode != "backfill"
            or not isinstance(selection, Mapping)
            or selection.get("entities") != ["players"]
            or selection.get("explicit_scope_count") != 0
            or selection.get("explicit_scope_sha256") != hashlib.sha256(b"").hexdigest()
            or selection.get("competition_limit") != 0
            or selection.get("season_limit") != 0
            or selection.get("profile") != PLAYER_SOURCE_REFRESH_PROFILE
            or selection.get("source_refresh") != expected_source
            or selection.get("scope_plan_signature")
            != source_refresh_contract["plan_signature"]
            or not valid_outcomes
            or not isinstance(budget, Mapping)
            or budget.get("max_requests") != PLAYER_SOURCE_REFRESH_MAX_REQUESTS
            or int(budget.get("requests") or 0) > PLAYER_SOURCE_REFRESH_MAX_REQUESTS
            or budget.get("max_direct_bytes")
            != PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB * 1024 * 1024
            or budget.get("max_proxy_bytes") != 0
            or not isinstance(transport, Mapping)
            or int(transport.get("proxy_bytes") or 0) != 0
        ):
            raise BackfillError(
                "ingest XCom is not bound to the exact seven-player source refresh"
            )
        return {
            "run_id": generation_id,
            "mode": mode,
            "profile": PLAYER_SOURCE_REFRESH_PROFILE,
            "scope_count": 0,
            "scope_sha256": hashlib.sha256(b"").hexdigest(),
            "entities": ["players"],
            "plan_signature": source_refresh_contract["plan_signature"],
            "source_refresh": expected_source,
            "target_outcomes": outcomes,
            "transport": dict(transport),
            "budget": dict(budget),
        }

    expected_entities = sorted(ISSUE_930_SCOPE_ENTITIES)
    if (
        payload.get("status") != "success"
        or payload.get("run_id") != generation_id
        or payload.get("mode") != mode
        or not isinstance(selection, Mapping)
        or selection.get("entities") != expected_entities
        or selection.get("explicit_scope_count") != scope_contract["count"]
        or selection.get("explicit_scope_sha256") != scope_contract["sha256"]
        or selection.get("competition_limit") != 0
        or selection.get("season_limit") != 0
        or _PLAN_SIGNATURE_RE.fullmatch(
            str(selection.get("scope_plan_signature") or "")
        )
        is None
    ):
        raise BackfillError("ingest XCom is not bound to the exact issue-930 plan")
    return {
        "run_id": generation_id,
        "mode": mode,
        "scope_count": scope_contract["count"],
        "scope_sha256": scope_contract["sha256"],
        "entities": expected_entities,
        "plan_signature": selection["scope_plan_signature"],
        "transport": payload.get("transport"),
        "budget": payload.get("budget"),
    }


def _run_ids(publication: Mapping[str, Any], mode: str, attempt: int) -> dict[str, str]:
    compact = str(publication["generation_id"]).replace("-", "")
    return {
        "ingest_dag_id": INGEST_DAG_ID,
        "ingest_run_id": f"issue930_{mode}_a{attempt}__{compact}",
        "silver_dag_id": SILVER_DAG_ID,
        "silver_run_id": f"fotmob_silver__{publication['generation_id']}",
        "native_runner_run_id": str(publication["generation_id"]),
    }


def _base_report(
    args: argparse.Namespace,
    *,
    mode: str,
    publication: Mapping[str, Any],
    scope_contract: Mapping[str, Any],
) -> dict[str, Any]:
    context = _deployment_context(args)
    source_refresh = getattr(args, "_source_refresh_contract", None)
    bounded_scope = {
        key: scope_contract[key] for key in ("name", "artifact", "sha256", "count")
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _now(),
        "passed": False,
        "command": "run",
        "phase": "prepared_pending_acquire",
        "recovery_required": True,
        "mode": mode,
        "publication_attempt": int(args.publication_attempt),
        "project": args.project,
        "deployment_report": str(args.deployment_report.resolve()),
        "deployment_id": context["deployment_id"],
        "git_sha": context["git_sha"],
        "scope": bounded_scope,
        "entities": (
            ["players"]
            if isinstance(source_refresh, Mapping)
            else list(ISSUE_930_SCOPE_ENTITIES)
        ),
        "publication": dict(publication),
        "runs": _run_ids(publication, mode, int(args.publication_attempt)),
        "limits": {
            "max_requests": args.max_requests,
            "max_direct_mib": args.max_direct_mib,
            "competition_limit": 0,
            "season_limit": 0,
            "executed_scope_count": (
                0 if isinstance(source_refresh, Mapping) else scope_contract["count"]
            ),
        },
        "publication_action": "abandon_unclaimed_candidate",
    }
    if isinstance(source_refresh, Mapping):
        report["profile"] = source_refresh["profile"]
        report["source_refresh"] = {
            key: source_refresh[key]
            for key in (
                "profile",
                "artifact",
                "sha256",
                "target_count",
                "targets",
                "plan_signature",
            )
        }
    return report


def _write_phase(
    report: dict[str, Any], output: Path, phase: str, **values: Any
) -> None:
    report.update({"generated_at": _now(), "phase": phase, **values})
    _atomic_json(output, report)


def _release_failed(
    args: argparse.Namespace,
    report: dict[str, Any],
    publication: Mapping[str, Any],
    state: Mapping[str, Any],
    *,
    failure_reason: str = "exact issue-930 ingest DagRun failed",
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    phase = str(state.get("phase") or "").casefold()
    generation_id = str(publication["generation_id"])
    if phase == "ready":
        released = _transition_publication(
            args, generation_id, action="abandon", run=run
        )
    elif phase in {"writing", "failed"}:
        released = _transition_publication(
            args, generation_id, action="fail_release", run=run
        )
    elif phase == "abandoned" and state.get("active") is False:
        if not state.get("released_at"):
            raise BackfillError("abandoned generation has no exact release proof")
        released = {**state, "released": True, "published": False}
    else:
        raise BackfillError(f"failed ingest has unsafe publication phase {phase!r}")
    if released.get("active") is not False or released.get("phase") not in {
        "failed",
        "abandoned",
    }:
        raise BackfillError("failed generation was not released safely")
    _write_phase(
        report,
        args.output,
        "failed_generation_released",
        passed=False,
        recovery_required=False,
        publication_state=_publication_summary(released),
        error=failure_reason,
        next_publication_attempt=int(args.publication_attempt) + 1,
    )
    return report


def _resolve_quiet_generation(
    args: argparse.Namespace,
    report: dict[str, Any],
    publication: Mapping[str, Any],
    scope_contract: Mapping[str, Any],
    *,
    allow_absent_release: bool,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    ids = report["runs"]
    ingest = _exact_run(args, INGEST_DAG_ID, ids["ingest_run_id"], run=run)
    silver = _exact_run(args, SILVER_DAG_ID, ids["silver_run_id"], run=run)
    ingest_state = str((ingest or {}).get("state") or "").casefold()
    silver_state = str((silver or {}).get("state") or "").casefold()
    state = _get_publication(args, str(publication["generation_id"]), run=run)

    if ingest is None:
        if not allow_absent_release:
            raise BackfillError(
                "exact ingest run is absent after trigger intent; lock retained"
            )
        if state is None:
            _write_phase(
                report,
                args.output,
                "not_acquired",
                passed=False,
                recovery_required=False,
                error="publication acquire did not commit and no ingest run exists",
                retry_publication_attempt=int(args.publication_attempt),
            )
            return report
        return _release_failed(
            args,
            report,
            publication,
            state,
            failure_reason=(
                "publication acquired but no exact ingest run was created; "
                "generation released after writer quiescence proof"
            ),
            run=run,
        )

    report["ingest_terminal"] = dict(ingest)
    report["silver_terminal"] = None if silver is None else dict(silver)
    if ingest_state == "failed":
        if state is None:
            raise BackfillError("failed ingest lost its publication generation")
        return _release_failed(args, report, publication, state, run=run)
    if ingest_state != "success":
        raise BackfillError(
            f"exact ingest run is not terminal: state={ingest_state or 'absent'!r}"
        )
    if silver_state != "success":
        raise BackfillError(
            "successful ingest has no successful exact Silver child; lock retained"
        )
    if state is None:
        raise BackfillError("successful ingest lost its publication generation")

    candidate = _candidate(state, publication)
    validation = _validation_summary(
        _validation_xcom(args, ids["ingest_run_id"], run=run),
        mode=report["mode"],
        generation_id=str(publication["generation_id"]),
        scope_contract=scope_contract,
        source_refresh_contract=getattr(args, "_source_refresh_contract", None),
    )
    phase = str(state.get("phase") or "").casefold()
    if phase == "ready":
        if state.get("status") != "succeeded" or state.get("active") is not True:
            raise BackfillError("ready publication does not retain its exact lock")
        _write_phase(
            report,
            args.output,
            "ready_pending_abandon",
            validation=validation,
            plan_signature=validation["plan_signature"],
            candidate={
                "generation_id": candidate["generation_id"],
                "digest": candidate["digest"],
                "transform_task_ids": candidate.get("transform_task_ids"),
            },
            publication_state=_publication_summary(state),
        )
        state = _transition_publication(
            args,
            str(publication["generation_id"]),
            action="abandon",
            run=run,
        )
        phase = str(state.get("phase") or "").casefold()
    elif phase == "abandoned":
        if state.get("active") is not False or not state.get("released_at"):
            raise BackfillError("abandoned generation has no exact release proof")
        # A read-only ControlStore lookup exposes released_at rather than the
        # transition-only convenience flags.  Normalize those proven facts in
        # recovery evidence without performing another mutation.
        state = {**state, "released": True, "published": False}
    if (
        phase != "abandoned"
        or state.get("active") is not False
        or state.get("released") is not True
        or state.get("published") is not False
    ):
        raise BackfillError("successful generation was not abandoned safely")
    _write_phase(
        report,
        args.output,
        "abandoned",
        passed=True,
        recovery_required=False,
        validation=validation,
        plan_signature=validation["plan_signature"],
        candidate={
            "generation_id": candidate["generation_id"],
            "digest": candidate["digest"],
            "transform_task_ids": candidate.get("transform_task_ids"),
        },
        publication_state=_publication_summary(state),
    )
    return report


def run_backfill(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    sleeper: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    if not args.execute or args.confirm != CONFIRM_RUN:
        raise BackfillError(f"run requires --execute --confirm {CONFIRM_RUN}")
    mode = str(args.mode or "").casefold()
    if mode not in MODES:
        raise BackfillError("run requires --mode backfill or --mode replay")
    if int(args.publication_attempt) <= 0:
        raise BackfillError("--publication-attempt must be a positive integer")
    if args.max_requests <= 0 or args.max_direct_mib <= 0:
        raise BackfillError("request and direct-byte limits must be positive")
    source_refresh = _load_source_refresh_contract(args)
    if source_refresh is not None and (
        mode != "backfill"
        or args.max_requests != PLAYER_SOURCE_REFRESH_MAX_REQUESTS
        or args.max_direct_mib != PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB
    ):
        raise BackfillError(
            "source refresh requires backfill mode and its exact reviewed budgets"
        )
    expected_sha = _validate_sha(args.expected_git_sha)
    scope_contract = _load_scope_contract(args.scopes, args.scope_sha256)
    context = _deployment_context(args)
    if context.get("kept_paused") is not True or context["git_sha"] != expected_sha:
        raise BackfillError("run requires the exact admitted --keep-paused deployment")
    validate_live_deployment(args, run=run)
    initial_writers = inspect_writer_state(args, run=run)
    _require_writers_stopped(initial_writers)
    quiescence_before = require_no_active_publication(args, run=run)
    publication = _publication_envelope(args, mode, run=run)
    report = _base_report(
        args,
        mode=mode,
        publication=publication,
        scope_contract=scope_contract,
    )
    report["writer_state_before"] = initial_writers
    report["publication_quiescence_before"] = quiescence_before
    _atomic_json(args.output, report)

    try:
        acquired = _initialize_publication(args, publication, run=run)
    except Exception as exc:
        _write_phase(
            report,
            args.output,
            "acquire_ambiguous",
            passed=False,
            recovery_required=True,
            error=f"{type(exc).__name__}: {exc}",
        )
        return report

    _write_phase(
        report,
        args.output,
        "acquired_writers_paused",
        publication_state=_publication_summary(acquired),
    )
    trigger_confirmed = False
    operation_error: str | None = None
    try:
        # Silver must be schedulable before its parent starts waiting.  The
        # daily schedule owner remains paused throughout this operation.
        _airflow(args, "dags", "unpause", SILVER_DAG_ID, run=run)
        _airflow(args, "dags", "unpause", INGEST_DAG_ID, run=run)
        conf = {
            "fotmob_publication": publication,
            "mode": mode,
            "scope": (
                ""
                if source_refresh is not None
                else ",".join(scope_contract["identities"])
            ),
            "entities": (
                "players"
                if source_refresh is not None
                else ",".join(ISSUE_930_SCOPE_ENTITIES)
            ),
            "max_requests": args.max_requests,
            "max_direct_mib": args.max_direct_mib,
            "max_proxy_mib": 0,
            "competition_limit": 0,
            "season_limit": 0,
            "match_limit": 0,
            "team_limit": 0,
            "player_limit": 0,
            "requests_per_minute": 30,
            "max_attempts": 4,
            "next_build_id": "",
            "source_refresh_profile": (
                source_refresh["profile"] if source_refresh is not None else ""
            ),
            "source_refresh_targets_sha256": (
                source_refresh["sha256"] if source_refresh is not None else ""
            ),
            "source_refresh_target_count": (
                source_refresh["target_count"] if source_refresh is not None else 0
            ),
        }
        _write_phase(report, args.output, "trigger_intent", trigger_conf=conf)
        _airflow(
            args,
            "dags",
            "trigger",
            INGEST_DAG_ID,
            "--run-id",
            report["runs"]["ingest_run_id"],
            "--conf",
            json.dumps(conf, sort_keys=True, separators=(",", ":")),
            run=run,
        )
        _write_phase(report, args.output, "ingest_running")
        trigger_confirmed = True
        deadline = monotonic() + max(1, int(args.timeout_seconds))
        while monotonic() < deadline:
            observed = _exact_run(
                args,
                INGEST_DAG_ID,
                report["runs"]["ingest_run_id"],
                run=run,
            )
            if str((observed or {}).get("state") or "").casefold() in {
                "success",
                "failed",
            }:
                break
            sleeper(2)
        else:
            operation_error = "exact ingest run did not terminate before timeout"
    except Exception as exc:
        operation_error = f"{type(exc).__name__}: {exc}"

    try:
        writer_state = _pause_all(args, run=run)
        validate_live_deployment(args, run=run)
    except Exception as exc:
        _write_phase(
            report,
            args.output,
            "lock_retained_writer_state_ambiguous",
            passed=False,
            recovery_required=True,
            error=(
                operation_error
                or f"writer quiescence could not be proven: {type(exc).__name__}: {exc}"
            ),
        )
        return report
    report["writer_state_after"] = writer_state

    try:
        resolved = _resolve_quiet_generation(
            args,
            report,
            publication,
            scope_contract,
            # ``trigger_intent`` is write-ahead: an absent exact run after a
            # lost CLI response is safe to release once all writers are
            # proven stopped.  After a successful CLI response, disappearance
            # of that DagRun is anomalous and remains fail-closed.
            allow_absent_release=not trigger_confirmed,
            run=run,
        )
    except Exception as exc:
        _write_phase(
            report,
            args.output,
            "lock_retained_pending_recovery",
            passed=False,
            recovery_required=True,
            error=operation_error or f"{type(exc).__name__}: {exc}",
            publication_state=_best_effort_publication_summary(
                args, str(publication["generation_id"]), run=run
            ),
        )
        return report
    if resolved.get("recovery_required") is False:
        try:
            resolved["publication_quiescence_after"] = require_no_active_publication(
                args, run=run
            )
            resolved["writer_state_final"] = inspect_writer_state(args, run=run)
            _require_writers_stopped(resolved["writer_state_final"])
            _atomic_json(args.output, resolved)
        except Exception as exc:
            _write_phase(
                resolved,
                args.output,
                "post_release_attestation_failed",
                passed=False,
                recovery_required=True,
                error=f"{type(exc).__name__}: {exc}",
            )
    return resolved


def _load_recovery_report(
    args: argparse.Namespace,
    scope_contract: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if args.recovery_report is None:
        raise BackfillError("recover requires --recovery-report")
    try:
        report = json.loads(args.recovery_report.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackfillError(f"invalid recovery report: {exc}") from exc
    context = _deployment_context(args)
    source_refresh = getattr(args, "_source_refresh_contract", None)
    expected_entities = (
        ["players"]
        if isinstance(source_refresh, Mapping)
        else list(ISSUE_930_SCOPE_ENTITIES)
    )
    expected_source = (
        {
            key: source_refresh[key]
            for key in (
                "profile",
                "artifact",
                "sha256",
                "target_count",
                "targets",
                "plan_signature",
            )
        }
        if isinstance(source_refresh, Mapping)
        else None
    )
    if (
        not isinstance(report, dict)
        or report.get("schema_version") != SCHEMA_VERSION
        or report.get("project") != args.project
        or report.get("deployment_report") != str(args.deployment_report.resolve())
        or report.get("deployment_id") != context["deployment_id"]
        or report.get("git_sha") != context["git_sha"]
        or report.get("mode") not in MODES
        or report.get("publication_attempt") != int(args.publication_attempt)
        or report.get("entities") != expected_entities
        or report.get("profile")
        != (
            PLAYER_SOURCE_REFRESH_PROFILE
            if isinstance(source_refresh, Mapping)
            else None
        )
        or report.get("source_refresh") != expected_source
        or (isinstance(source_refresh, Mapping) and report.get("mode") != "backfill")
        or report.get("scope")
        != {key: scope_contract[key] for key in ("name", "artifact", "sha256", "count")}
    ):
        raise BackfillError("recovery report stack or scope identity differs")
    publication = _publication_envelope(args, report["mode"], run=run)
    if report.get("publication") != publication or report.get("runs") != _run_ids(
        publication, report["mode"], int(args.publication_attempt)
    ):
        raise BackfillError("recovery report publication identities differ")
    return report, publication


def recover_backfill(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    if not args.execute or args.confirm != CONFIRM_RECOVER:
        raise BackfillError(f"recover requires --execute --confirm {CONFIRM_RECOVER}")
    source_refresh = _load_source_refresh_contract(args)
    if source_refresh is not None and (
        getattr(args, "mode", None) not in {None, "backfill"}
        or args.max_requests != PLAYER_SOURCE_REFRESH_MAX_REQUESTS
        or args.max_direct_mib != PLAYER_SOURCE_REFRESH_MAX_DIRECT_MIB
    ):
        raise BackfillError("source-refresh recovery mode or budgets differ")
    expected_sha = _validate_sha(args.expected_git_sha)
    context = _deployment_context(args)
    if context.get("kept_paused") is not True or context["git_sha"] != expected_sha:
        raise BackfillError(
            "recover requires the exact admitted --keep-paused deployment"
        )
    if int(args.publication_attempt) <= 0:
        raise BackfillError("--publication-attempt must be a positive integer")
    scope_contract = _load_scope_contract(args.scopes, args.scope_sha256)
    report, publication = _load_recovery_report(args, scope_contract, run=run)
    report = {**report, "command": "recover", "passed": False}
    validate_live_deployment(args, run=run)
    try:
        report["writer_state_after"] = _pause_all(args, run=run)
        validate_live_deployment(args, run=run)
    except Exception as exc:
        _write_phase(
            report,
            args.output,
            "lock_retained_writer_state_ambiguous",
            recovery_required=True,
            error=f"{type(exc).__name__}: {exc}",
        )
        return report

    pre_trigger_phases = {
        "prepared_pending_acquire",
        "acquire_ambiguous",
        "acquired_writers_paused",
        "trigger_intent",
    }
    try:
        resolved = _resolve_quiet_generation(
            args,
            report,
            publication,
            scope_contract,
            allow_absent_release=str(report.get("phase")) in pre_trigger_phases,
            run=run,
        )
    except Exception as exc:
        _write_phase(
            report,
            args.output,
            "lock_retained_pending_recovery",
            recovery_required=True,
            error=f"{type(exc).__name__}: {exc}",
            publication_state=_best_effort_publication_summary(
                args, str(publication["generation_id"]), run=run
            ),
        )
        return report
    if resolved.get("recovery_required") is False:
        try:
            resolved["publication_quiescence_after"] = require_no_active_publication(
                args, run=run
            )
            resolved["writer_state_final"] = inspect_writer_state(args, run=run)
            _require_writers_stopped(resolved["writer_state_final"])
            _atomic_json(args.output, resolved)
        except Exception as exc:
            _write_phase(
                resolved,
                args.output,
                "post_release_attestation_failed",
                passed=False,
                recovery_required=True,
                error=f"{type(exc).__name__}: {exc}",
            )
    return resolved


def build_parser() -> argparse.ArgumentParser:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("command", choices=("run", "recover"))
    parser.add_argument(
        "--compose-file", type=Path, default=root / "deploy/fotmob/airflow.compose.yaml"
    )
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--deployment-report", type=Path, required=True)
    parser.add_argument("--recovery-report", type=Path)
    parser.add_argument("--project", default="fotmob-airflow")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=sorted(MODES))
    parser.add_argument(
        "--publication-attempt",
        type=int,
        default=1,
        help=(
            "Positive deterministic generation attempt; increment only after "
            "the previous attempt is terminal and released"
        ),
    )
    parser.add_argument("--scopes", type=Path, default=APPROVED_SCOPE_ARTIFACT)
    parser.add_argument("--scope-sha256", required=True)
    parser.add_argument("--source-refresh-profile", default="")
    parser.add_argument("--source-refresh-targets-sha256", default="")
    parser.add_argument("--expected-git-sha", required=True)
    parser.add_argument("--max-requests", type=int, default=2_000)
    parser.add_argument("--max-direct-mib", type=int, default=256)
    parser.add_argument("--timeout-seconds", type=int, default=24 * 60 * 60)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = run_backfill(args) if args.command == "run" else recover_backfill(args)
    except Exception as exc:
        existing: dict[str, Any] = {}
        try:
            candidate = json.loads(args.output.read_text(encoding="utf-8"))
            if isinstance(candidate, dict) and candidate.get("schema_version") == (
                SCHEMA_VERSION
            ):
                existing = candidate
        except (OSError, json.JSONDecodeError):
            pass
        report = {
            **existing,
            "schema_version": SCHEMA_VERSION,
            "generated_at": _now(),
            "passed": False,
            "command": args.command,
            "phase": existing.get("phase", "preflight_failed"),
            "error": f"{type(exc).__name__}: {exc}",
            "recovery_required": bool(existing),
        }
    _atomic_json(args.output, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if report.get("passed") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
