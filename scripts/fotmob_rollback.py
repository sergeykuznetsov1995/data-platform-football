#!/usr/bin/env python3
"""Fail-closed rollback coordinator for FotMob consumers.

Rollback is a four-step operation, intentionally split across invocations:

1. ``plan`` records the exact procedure without changing anything.
2. ``pause --execute`` pauses all FotMob DAGs and proves no writer is running.
3. After an operator deploys the reviewed consumer revert, ``run-silver``
   creates a synthetic publication fence, runs exact Silver/DQ and abandons
   the candidate. ``recover-publication`` handles an ambiguous interrupted run
   without guessing whether its lock is safe to release.
4. ``validate`` proves the deployed revision, exact fenced Silver/DQ run,
   abandoned candidate and availability of every frozen legacy Bronze table.

The command never drops, truncates or otherwise mutates native Bronze objects.
"""

from __future__ import annotations

import argparse
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

try:  # package import in tests / ``python -m``
    from scripts.fotmob_acceptance import QueryClient, connect_from_env
    from scripts import fotmob_runtime as runtime_binding
except ModuleNotFoundError:  # direct ``python scripts/fotmob_rollback.py``
    from fotmob_acceptance import QueryClient, connect_from_env
    import fotmob_runtime as runtime_binding


DAGS = (
    "dag_ingest_fotmob",
    "dag_transform_fotmob_silver",
    "dag_trigger_fotmob_daily",
)
LEGACY_TABLES = (
    "fotmob_match_details",
    "fotmob_player_details",
    "fotmob_player_stats",
    "fotmob_schedule",
    "fotmob_team_leaderboards",
    "fotmob_team_profile",
    "fotmob_team_squad",
    "fotmob_team_stats",
    "fotmob_transfers",
)
CONFIRM_PAUSE = "PAUSE_FOTMOB_WRITERS"
CONFIRM_RUN_SILVER = "RUN_FOTMOB_ROLLBACK_VALIDATION_SILVER"
CONFIRM_RECOVER_PUBLICATION = "RECOVER_FOTMOB_ROLLBACK_PUBLICATION"
ROLLBACK_PUBLICATION_SCHEMA = "fotmob-rollback-publication-v1"
PUBLICATION_TTL_SECONDS = 14 * 24 * 60 * 60
SILVER_DAG_ID = "dag_transform_fotmob_silver"


class RollbackError(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        temporary.replace(path)
        directory_fd = os.open(
            path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def _parse_json_array(output: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    for index, character in enumerate(output):
        if character != "[":
            continue
        try:
            value, _ = decoder.raw_decode(output[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, list) and all(isinstance(row, dict) for row in value):
            return value
    raise RollbackError("Airflow command did not return a JSON array of objects")


def _timestamp(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise RollbackError(f"invalid evidence timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise RollbackError("evidence timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _deployment_context(args: argparse.Namespace) -> Mapping[str, Any]:
    cached = getattr(args, "_deployment_context_cache", None)
    if isinstance(cached, Mapping):
        return cached
    try:
        payload = runtime_binding.load_deployment_context(
            args.deployment_report,
            project=args.project,
            compose_file=args.compose_file,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc
    setattr(args, "_deployment_context_cache", payload)
    return payload


def _compose_environment(args: argparse.Namespace) -> dict[str, str]:
    return runtime_binding.compose_environment(_deployment_context(args))


def _compose_base(args: argparse.Namespace) -> tuple[str, ...]:
    try:
        return runtime_binding.compose_base(
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc


def _airflow(
    args: argparse.Namespace,
    *command: str,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> str:
    result = run(
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
    )
    return result.stdout


def _container_deploy_sha(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> str:
    return run(
        (
            *_compose_base(args),
            "exec",
            "-T",
            "airflow-scheduler",
            "sh",
            "-c",
            "printenv FOTMOB_DEPLOY_GIT_SHA",
        ),
        check=True,
        capture_output=True,
        text=True,
        env=_compose_environment(args),
    ).stdout.strip()


def validate_live_deployment(
    args: argparse.Namespace,
    *,
    require_running: bool,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    try:
        return runtime_binding.validate_live_deployment(
            _deployment_context(args),
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
            require_running=require_running,
            run=run,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc


def bind_admitted_trino(
    args: argparse.Namespace,
    *,
    require_running: bool,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    try:
        return runtime_binding.bind_admitted_trino(
            _deployment_context(args),
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
            require_running=require_running,
            run=run,
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc


def require_no_active_fotmob_publication(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    try:
        return runtime_binding.assert_no_active_fotmob_publication(
            _deployment_context(args), run=run
        )
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc


def _container_python_json(
    args: argparse.Namespace,
    *,
    code: str,
    marker: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
    allow_null: bool = False,
) -> dict[str, Any] | None:
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
            payload = json.loads(line.removeprefix(marker))
        except json.JSONDecodeError as exc:
            raise RollbackError(f"invalid {marker} evidence") from exc
        if isinstance(payload, dict) or (allow_null and payload is None):
            return payload
        break
    raise RollbackError(f"container did not emit {marker} evidence")


def _rollback_publication_envelope(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    context = _deployment_context(args)
    attempt = int(args.publication_attempt)
    if attempt <= 0:
        raise RollbackError("--publication-attempt must be a positive integer")
    start = _timestamp(context["generated_at"]) + timedelta(seconds=attempt)
    end = start + timedelta(seconds=1)
    expected_start = start.isoformat(timespec="microseconds")
    expected_end = end.isoformat(timespec="microseconds")
    marker = "FOTMOB_ROLLBACK_PUBLICATION_BINDING_JSON="
    code = (
        "import json,sys; sys.path.insert(0,'/opt/airflow/dags'); "
        "from utils.fotmob_publication import make_publication_binding,make_generation_id; "
        f"b=make_publication_binding(owner='isolated',data_interval_start={start.isoformat()!r},"
        f"data_interval_end={end.isoformat()!r},fingerprint={context['git_sha']!r}); "
        f"print('{marker}'+json.dumps({{'generation_id':make_generation_id(b),'binding':b}},"
        "sort_keys=True))"
    )
    payload = _container_python_json(
        args, code=code, marker=marker, run=run
    )
    binding = payload.get("binding")
    generation_id = str(payload.get("generation_id", ""))
    if not isinstance(binding, Mapping):
        raise RollbackError("rollback publication binding is absent")
    if (
        binding.get("schema") != "fotmob-publication-v1"
        or binding.get("source") != "fotmob"
        or binding.get("owner") != "isolated"
        or binding.get("runtime_fingerprint") != context["git_sha"]
        or binding.get("data_interval_start") != expected_start
        or binding.get("data_interval_end") != expected_end
        or _timestamp(binding.get("data_interval_end"))
        - _timestamp(binding.get("data_interval_start"))
        != timedelta(seconds=1)
        or not re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            generation_id,
        )
    ):
        raise RollbackError("rollback publication binding is not exact and synthetic")
    return {"generation_id": generation_id, "binding": dict(binding)}


def _initialize_rollback_publication(
    args: argparse.Namespace,
    publication: Mapping[str, Any],
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    marker = "FOTMOB_ROLLBACK_PUBLICATION_STATE_JSON="
    binding_json = json.dumps(publication["binding"], sort_keys=True)
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        f"b=json.loads({binding_json!r}); "
        "r=ControlStore.from_env().initialize_publication_generation("
        f"{str(publication['generation_id'])!r},dag_id='fotmob_rollback_validation',"
        f"binding=b,source='fotmob',ttl_seconds={PUBLICATION_TTL_SECONDS}); "
        f"print('{marker}'+json.dumps(r,default=str,sort_keys=True))"
    )
    state = _container_python_json(args, code=code, marker=marker, run=run)
    if (
        state.get("generation_id") != publication["generation_id"]
        or state.get("binding") != publication["binding"]
        or state.get("status") != "running"
        or state.get("phase") != "writing"
        or state.get("active") is not True
        or state.get("owner_dag_id") != "fotmob_rollback_validation"
    ):
        raise RollbackError("rollback publication generation was not acquired exactly")
    return state


def _get_rollback_publication(
    args: argparse.Namespace,
    generation_id: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any] | None:
    marker = "FOTMOB_ROLLBACK_PUBLICATION_STATE_JSON="
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        "r=ControlStore.from_env().get_publication_generation("
        f"{generation_id!r},source='fotmob'); "
        f"print('{marker}'+json.dumps(r,default=str,sort_keys=True))"
    )
    return _container_python_json(
        args, code=code, marker=marker, run=run, allow_null=True
    )


def _transition_rollback_publication(
    args: argparse.Namespace,
    generation_id: str,
    *,
    action: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    marker = "FOTMOB_ROLLBACK_PUBLICATION_STATE_JSON="
    if action == "seal":
        expression = (
            "s.seal_publication_generation("
            f"{generation_id!r},source='fotmob',ttl_seconds={PUBLICATION_TTL_SECONDS})"
        )
    elif action == "abandon":
        expression = (
            "s.complete_publication_generation("
            f"{generation_id!r},consumer=None,published=False,source='fotmob')"
        )
    elif action in {"fail_retain", "fail_release"}:
        safe = action == "fail_release"
        expression = (
            "s.fail_publication_generation("
            f"{generation_id!r},safe_to_release={safe!r},source='fotmob')"
        )
    else:  # pragma: no cover - internal callers use constants above
        raise AssertionError(action)
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        f"s=ControlStore.from_env(); r={expression}; "
        f"print('{marker}'+json.dumps(r,default=str,sort_keys=True))"
    )
    return _container_python_json(args, code=code, marker=marker, run=run)


def _publication_state_summary(state: Mapping[str, Any]) -> dict[str, Any]:
    candidate = state.get("candidate")
    candidate_summary = None
    if isinstance(candidate, Mapping):
        candidate_summary = {
            "generation_id": candidate.get("generation_id"),
            "digest": candidate.get("digest"),
            "transform_task_ids": candidate.get("transform_task_ids"),
        }
    return {
        "generation_id": state.get("generation_id"),
        "status": state.get("status"),
        "phase": state.get("phase"),
        "active": state.get("active"),
        "released": state.get("released"),
        "published": state.get("published"),
        "candidate": candidate_summary,
    }


def inspect_writer_state(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    # One SQL statement gives a single metadata-DB snapshot for every DAG's
    # pause flag and all active runs. Sequential ``list-runs`` calls can miss a
    # run that starts in an earlier-scanned DAG while a later DAG is queried.
    marker = "FOTMOB_WRITER_STATE_JSON="
    code = (
        "import json; from sqlalchemy import and_; "
        "from airflow.models import DagModel, DagRun; "
        "from airflow.settings import Session; s=Session(); "
        "ids=" + repr(tuple(DAGS)) + "; "
        "q=s.query(DagModel.dag_id,DagModel.is_paused,DagRun.run_id,"
        "DagRun.state).outerjoin(DagRun,and_(DagRun.dag_id==DagModel.dag_id,"
        "DagRun.state.in_(('running','queued')))).filter(DagModel.dag_id.in_(ids)); "
        "rows=[{'dag_id':d,'is_paused':p,'run_id':r,'state':"
        "getattr(st,'value',st)} for d,p,r,st in q.all()]; "
        f"print('{marker}'+json.dumps(rows,sort_keys=True)); s.close()"
    )
    result = run(
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
    payload = None
    for line in reversed(result.splitlines()):
        if line.startswith(marker):
            try:
                payload = json.loads(line.removeprefix(marker))
            except json.JSONDecodeError as exc:
                raise RollbackError("invalid atomic writer-state evidence") from exc
            break
    if not isinstance(payload, list) or any(
        not isinstance(row, Mapping) for row in payload
    ):
        raise RollbackError("atomic writer-state query returned invalid evidence")
    pause_states = {dag_id: False for dag_id in DAGS}
    active: dict[str, dict[str, list[str]]] = {}
    observed_dags: set[str] = set()
    for row in payload:
        dag_id = str(row.get("dag_id", ""))
        if dag_id not in DAGS:
            raise RollbackError("atomic writer-state query returned an unknown DAG")
        observed_dags.add(dag_id)
        pause_states[dag_id] = row.get("is_paused") in (
            True,
            "True",
            "true",
            "1",
            1,
        )
        run_id = row.get("run_id")
        state = str(row.get("state") or "").lower()
        if run_id is not None and state in {"running", "queued"}:
            active.setdefault(dag_id, {}).setdefault(state, []).append(str(run_id))
    if observed_dags != set(DAGS):
        raise RollbackError("atomic writer-state query misses an admitted DAG")
    return {"pause_states": pause_states, "active_runs": active}


def require_writers_stopped(state: Mapping[str, Any]) -> None:
    pause_states = state.get("pause_states")
    if not isinstance(pause_states, Mapping):
        raise RollbackError("writer state has no pause-state evidence")
    not_paused = [dag_id for dag_id in DAGS if pause_states.get(dag_id) is not True]
    if not_paused:
        raise RollbackError(f"writers are not paused: {not_paused!r}")
    if state.get("active_runs"):
        raise RollbackError(f"writers still have active runs: {state['active_runs']!r}")


def _sanitized_pause_failure(exc: Exception) -> str:
    """Describe a pause failure without copying commands or process output."""

    error_type = type(exc).__name__
    if isinstance(exc, subprocess.CalledProcessError):
        return f"{error_type}(returncode={exc.returncode})"
    return error_type


def _exact_silver_run(
    args: argparse.Namespace,
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
            SILVER_DAG_ID,
            "--output",
            "json",
            run=run,
        )
    )
    matches = [row for row in rows if str(row.get("run_id")) == run_id]
    if len(matches) > 1:
        raise RollbackError("Airflow returned duplicate exact Silver run identity")
    return matches[0] if matches else None


def _pause_all_writers(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    failures: list[str] = []
    for dag_id in DAGS:
        try:
            _airflow(args, "dags", "pause", dag_id, run=run)
        except Exception as exc:
            failures.append(f"pause[{dag_id}]={_sanitized_pause_failure(exc)}")

    state: dict[str, Any] = {}
    try:
        state = inspect_writer_state(args, run=run)
    except Exception as exc:
        failures.append("inspect_writer_state=" + _sanitized_pause_failure(exc))
    try:
        require_writers_stopped(state)
    except Exception as exc:
        failures.append(
            "require_writers_stopped=" + _sanitized_pause_failure(exc)
        )

    if failures:
        raise RollbackError(
            "writer pause/quiescence failed: " + "; ".join(failures)
        )
    return state


def _candidate_from_state(
    state: Mapping[str, Any], publication: Mapping[str, Any]
) -> Mapping[str, Any]:
    candidate = state.get("candidate")
    if (
        state.get("generation_id") != publication["generation_id"]
        or state.get("binding") != publication["binding"]
        or not isinstance(candidate, Mapping)
        or candidate.get("generation_id") != publication["generation_id"]
        or not re.fullmatch(r"[0-9a-f]{64}", str(candidate.get("digest", "")))
    ):
        raise RollbackError("Silver run did not record the exact publication candidate")
    return candidate


def _rollback_publication_report_base(
    args: argparse.Namespace,
    publication: Mapping[str, Any],
    *,
    silver_run_id: str,
) -> dict[str, Any]:
    context = _deployment_context(args)
    return {
        "schema_version": ROLLBACK_PUBLICATION_SCHEMA,
        "generated_at": _now(),
        "passed": False,
        "mode": "run-silver",
        "phase": "prepared_pending_acquire",
        "project": args.project,
        "deployment_report": str(args.deployment_report.resolve()),
        "consumer_git_sha": context["git_sha"],
        "publication_attempt": int(args.publication_attempt),
        "publication": dict(publication),
        "silver_dag_id": SILVER_DAG_ID,
        "silver_run_id": silver_run_id,
        "native_objects_action": "retain",
    }


def run_rollback_validation_silver(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    sleeper: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Run one fenced Silver validation and abandon its synthetic generation."""

    if not args.execute or args.confirm != CONFIRM_RUN_SILVER:
        raise RollbackError(
            "run-silver requires --execute --confirm " + CONFIRM_RUN_SILVER
        )
    expected_sha = _validate_sha(args.expected_consumer_sha)
    context = _deployment_context(args)
    if context.get("kept_paused") is not True or context["git_sha"] != expected_sha:
        raise RollbackError(
            "run-silver requires the exact --keep-paused rollback deployment"
        )
    validate_live_deployment(args, require_running=True, run=run)
    require_writers_stopped(inspect_writer_state(args, run=run))
    require_no_active_fotmob_publication(args, run=run)
    publication = _rollback_publication_envelope(args, run=run)
    silver_run_id = "rollback_silver__" + str(publication["generation_id"]).replace(
        "-", ""
    )
    if args.silver_run_id and args.silver_run_id != silver_run_id:
        raise RollbackError(
            f"run-silver uses deterministic --silver-run-id {silver_run_id!r}"
        )
    report = _rollback_publication_report_base(
        args, publication, silver_run_id=silver_run_id
    )
    # This is a write-ahead recovery identity. A process kill during DB acquire
    # still leaves the deterministic generation/run IDs needed for inspection.
    _atomic_json(args.output, report)
    try:
        acquired = _initialize_rollback_publication(args, publication, run=run)
    except Exception as exc:
        report.update(
            {
                "generated_at": _now(),
                "passed": False,
                "phase": "acquire_ambiguous",
                "error": f"{type(exc).__name__}: {exc}",
                "recovery_required": True,
            }
        )
        _atomic_json(args.output, report)
        return report
    report.update(
        {
            "generated_at": _now(),
            "phase": "acquired_pending_trigger",
            "publication_state": _publication_state_summary(acquired),
        }
    )
    _atomic_json(args.output, report)

    terminal: dict[str, Any] | None = None
    ambiguous_error: str | None = None
    writer_state: Mapping[str, Any] | None = None
    conf = json.dumps(
        {"fotmob_publication": publication}, sort_keys=True, separators=(",", ":")
    )
    try:
        _airflow(args, "dags", "unpause", SILVER_DAG_ID, run=run)
        _airflow(
            args,
            "dags",
            "trigger",
            SILVER_DAG_ID,
            "--run-id",
            silver_run_id,
            "--conf",
            conf,
            run=run,
        )
        report.update({"generated_at": _now(), "phase": "silver_running"})
        _atomic_json(args.output, report)
        deadline = monotonic() + max(1, int(args.timeout_seconds))
        while monotonic() < deadline:
            observed = _exact_silver_run(args, silver_run_id, run=run)
            state = str((observed or {}).get("state", "")).casefold()
            if state in {"success", "failed"}:
                terminal = observed
                break
            sleeper(2)
        if terminal is None:
            ambiguous_error = "exact Silver run did not reach a terminal state before timeout"
    except Exception as exc:
        ambiguous_error = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            writer_state = _pause_all_writers(args, run=run)
        except Exception as exc:
            ambiguous_error = ambiguous_error or (
                f"writer quiescence could not be proven: {type(exc).__name__}: {exc}"
            )

    if ambiguous_error is not None or terminal is None or writer_state is None:
        try:
            retained = _transition_rollback_publication(
                args,
                str(publication["generation_id"]),
                action="fail_retain",
                run=run,
            )
            retained_summary: Mapping[str, Any] = _publication_state_summary(retained)
        except Exception as exc:
            retained_summary = {
                "generation_id": publication["generation_id"],
                "active": True,
                "retention_error": f"{type(exc).__name__}: {exc}",
            }
        report.update(
            {
                "generated_at": _now(),
                "passed": False,
                "phase": "lock_retained_pending_terminal_proof",
                "error": ambiguous_error or "Silver terminal state is ambiguous",
                "publication_state": retained_summary,
                "recovery_required": True,
            }
        )
        _atomic_json(args.output, report)
        return report

    run_state = str(terminal.get("state", "")).casefold()
    report["silver_terminal"] = dict(terminal)
    report["writer_state_after"] = dict(writer_state)
    if run_state == "failed":
        released = _transition_rollback_publication(
            args,
            str(publication["generation_id"]),
            action="fail_release",
            run=run,
        )
        report.update(
            {
                "generated_at": _now(),
                "passed": False,
                "phase": "failed_generation_released",
                "publication_state": _publication_state_summary(released),
                "error": "rollback Silver validation run failed",
            }
        )
        _atomic_json(args.output, report)
        return report

    generation_id = str(publication["generation_id"])
    try:
        state = _get_rollback_publication(args, generation_id, run=run)
        if state is None:
            raise RollbackError("successful Silver generation is absent")
        candidate = _candidate_from_state(state, publication)
        if (
            state.get("status") != "running"
            or state.get("phase") != "writing"
            or state.get("active") is not True
        ):
            raise RollbackError("successful Silver candidate is not in writing phase")
        sealed = _transition_rollback_publication(
            args, generation_id, action="seal", run=run
        )
        if (
            sealed.get("status") != "succeeded"
            or sealed.get("phase") != "ready"
            or sealed.get("active") is not True
        ):
            raise RollbackError("rollback publication generation was not sealed ready")
        abandoned = _transition_rollback_publication(
            args, generation_id, action="abandon", run=run
        )
        if (
            abandoned.get("status") != "succeeded"
            or abandoned.get("phase") != "abandoned"
            or abandoned.get("active") is not False
            or abandoned.get("released") is not True
            or abandoned.get("published") is not False
        ):
            raise RollbackError(
                "rollback publication generation was not abandoned safely"
            )
    except Exception as exc:
        retained_summary: Mapping[str, Any] = {
            "generation_id": generation_id,
            "active": True,
        }
        try:
            current = _get_rollback_publication(args, generation_id, run=run)
            if current is None:
                raise RollbackError("rollback publication generation is absent")
            if (
                current.get("status") == "running"
                and current.get("phase") == "writing"
            ):
                current = _transition_rollback_publication(
                    args, generation_id, action="fail_retain", run=run
                )
            retained_summary = _publication_state_summary(current)
        except Exception as state_exc:
            retained_summary = {
                **retained_summary,
                "retention_error": f"{type(state_exc).__name__}: {state_exc}",
            }
        report.update(
            {
                "generated_at": _now(),
                "passed": False,
                "phase": "lock_retained_pending_terminal_proof",
                "error": f"{type(exc).__name__}: {exc}",
                "publication_state": retained_summary,
                "recovery_required": True,
            }
        )
        _atomic_json(args.output, report)
        return report
    report.update(
        {
            "generated_at": _now(),
            "passed": True,
            "phase": "abandoned",
            "candidate": {
                "generation_id": candidate["generation_id"],
                "digest": candidate["digest"],
                "transform_task_ids": candidate.get("transform_task_ids"),
            },
            "publication_state": _publication_state_summary(abandoned),
            "recovery_required": False,
        }
    )
    _atomic_json(args.output, report)
    return report


def _load_rollback_publication_report(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    path = getattr(args, "publication_report", None)
    if path is None:
        raise RollbackError("operation requires --publication-report")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RollbackError(f"invalid rollback publication report: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != (
        ROLLBACK_PUBLICATION_SCHEMA
    ):
        raise RollbackError("unsupported rollback publication report")
    context = _deployment_context(args)
    if (
        payload.get("project") != args.project
        or payload.get("deployment_report")
        != str(args.deployment_report.resolve())
        or payload.get("consumer_git_sha") != context["git_sha"]
        or payload.get("publication_attempt") != int(args.publication_attempt)
        or payload.get("silver_dag_id") != SILVER_DAG_ID
    ):
        raise RollbackError("rollback publication report stack identity differs")
    publication = payload.get("publication")
    expected = _rollback_publication_envelope(args, run=run)
    if not isinstance(publication, Mapping) or dict(publication) != expected:
        raise RollbackError("rollback publication report binding is not exact")
    expected_run_id = "rollback_silver__" + expected["generation_id"].replace(
        "-", ""
    )
    if payload.get("silver_run_id") != expected_run_id:
        raise RollbackError("rollback publication report has a different Silver run")
    return payload, expected


def recover_rollback_publication(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    """Release a retained synthetic generation only after exact terminal proof."""

    if not args.execute or args.confirm != CONFIRM_RECOVER_PUBLICATION:
        raise RollbackError(
            "recover-publication requires --execute --confirm "
            + CONFIRM_RECOVER_PUBLICATION
        )
    report, publication = _load_rollback_publication_report(args, run=run)
    validate_live_deployment(args, require_running=True, run=run)
    writer_state = _pause_all_writers(args, run=run)
    silver_run_id = str(report["silver_run_id"])
    terminal = _exact_silver_run(args, silver_run_id, run=run)
    run_state = str((terminal or {}).get("state", "")).casefold()
    generation_id = str(publication["generation_id"])
    if terminal is None and report.get("phase") in {
        "prepared_pending_acquire",
        "acquire_ambiguous",
        "acquired_pending_trigger",
    }:
        state = _get_rollback_publication(args, generation_id, run=run)
        if state is None and report.get("phase") in {
            "prepared_pending_acquire",
            "acquire_ambiguous",
        }:
            recovered = {
                **report,
                "generated_at": _now(),
                "passed": False,
                "mode": "recover-publication",
                "phase": "no_generation_acquired",
                "writer_state_after": writer_state,
                "publication_state": None,
                "error": (
                    "exact generation and Silver run are both absent after "
                    "pre-acquire recovery; retry the same publication attempt"
                ),
                "recovery_required": False,
            }
            _atomic_json(args.output, recovered)
            return recovered
        if state is None:
            raise RollbackError(
                "acquired-pending-trigger report has no exact control DB generation"
            )
        if (
            state.get("generation_id") != generation_id
            or state.get("binding") != publication["binding"]
            or state.get("owner_dag_id") != "fotmob_rollback_validation"
            or state.get("status") != "running"
            or state.get("phase") != "writing"
            or state.get("active") is not True
        ):
            raise RollbackError(
                "pre-trigger recovery did not find the exact active writing generation"
            )
        released = _transition_rollback_publication(
            args, generation_id, action="fail_release", run=run
        )
        if (
            released.get("status") != "failed"
            or released.get("phase") != "failed"
            or released.get("active") is not False
            or released.get("released") is not True
        ):
            raise RollbackError(
                "pre-trigger publication generation was not released safely"
            )
        recovered = {
            **report,
            "generated_at": _now(),
            "passed": False,
            "mode": "recover-publication",
            "phase": "pretrigger_generation_released",
            "writer_state_after": writer_state,
            "publication_state": _publication_state_summary(released),
            "error": (
                "acquire committed but the exact Silver run was never created; "
                "generation released after pre-trigger quiescence proof. Retry with "
                f"--publication-attempt {int(args.publication_attempt) + 1}"
            ),
            "recovery_required": False,
        }
        _atomic_json(args.output, recovered)
        return recovered
    if run_state not in {"success", "failed"}:
        retained = {
            **report,
            "generated_at": _now(),
            "passed": False,
            "mode": "recover-publication",
            "phase": "lock_retained_pending_terminal_proof",
            "error": "exact Silver run is absent or non-terminal; lock retained",
            "recovery_required": True,
        }
        _atomic_json(args.output, retained)
        return retained

    state = _get_rollback_publication(args, generation_id, run=run)
    if state is None:
        raise RollbackError("exact rollback publication generation is absent")
    if state.get("binding") != publication["binding"]:
        raise RollbackError("retained publication binding differs from recovery report")
    phase = str(state.get("phase", "")).casefold()
    recovered = {
        **report,
        "generated_at": _now(),
        "mode": "recover-publication",
        "silver_terminal": dict(terminal),
        "writer_state_after": writer_state,
    }
    if run_state == "failed" or phase == "failed":
        if phase == "writing" or phase == "failed":
            released = _transition_rollback_publication(
                args, generation_id, action="fail_release", run=run
            )
        else:
            raise RollbackError(
                f"failed Silver run has unsafe publication phase {phase!r}"
            )
        recovered.update(
            {
                "passed": False,
                "phase": "failed_generation_released",
                "publication_state": _publication_state_summary(released),
                "error": (
                    "retained generation was released after terminal proof; "
                    "start a new rollback Silver validation with "
                    f"--publication-attempt {int(args.publication_attempt) + 1}"
                ),
                "recovery_required": False,
            }
        )
        _atomic_json(args.output, recovered)
        return recovered

    candidate = _candidate_from_state(state, publication)
    if phase == "writing":
        sealed = _transition_rollback_publication(
            args, generation_id, action="seal", run=run
        )
        if sealed.get("phase") != "ready" or sealed.get("active") is not True:
            raise RollbackError("recovered generation was not sealed ready")
        state = sealed
        phase = "ready"
    if phase == "ready":
        state = _transition_rollback_publication(
            args, generation_id, action="abandon", run=run
        )
        phase = str(state.get("phase", "")).casefold()
    if phase != "abandoned" or state.get("active") is not False:
        raise RollbackError("recovered generation was not abandoned safely")
    recovered.update(
        {
            "passed": True,
            "phase": "abandoned",
            "candidate": {
                "generation_id": candidate["generation_id"],
                "digest": candidate["digest"],
                "transform_task_ids": candidate.get("transform_task_ids"),
            },
            "publication_state": _publication_state_summary(state),
            "recovery_required": False,
        }
    )
    _atomic_json(args.output, recovered)
    return recovered


def validate_rollback_publication_evidence(
    args: argparse.Namespace,
    *,
    expected_sha: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, Any]:
    report, publication = _load_rollback_publication_report(args, run=run)
    candidate = report.get("candidate")
    if (
        report.get("passed") is not True
        or report.get("phase") != "abandoned"
        or report.get("recovery_required") is not False
        or report.get("consumer_git_sha") != expected_sha
        or report.get("silver_run_id") != args.silver_run_id
        or not isinstance(candidate, Mapping)
        or candidate.get("generation_id") != publication["generation_id"]
        or not re.fullmatch(r"[0-9a-f]{64}", str(candidate.get("digest", "")))
    ):
        raise RollbackError("rollback publication report is not safely completed")
    state = _get_rollback_publication(
        args, str(publication["generation_id"]), run=run
    )
    if state is None:
        raise RollbackError("exact rollback publication generation is absent")
    observed_candidate = _candidate_from_state(state, publication)
    if (
        state.get("status") != "succeeded"
        or state.get("phase") != "abandoned"
        or state.get("active") is not False
        or observed_candidate.get("digest") != candidate.get("digest")
    ):
        raise RollbackError("control DB does not contain the abandoned exact candidate")
    return {
        "generation_id": publication["generation_id"],
        "binding": publication["binding"],
        "candidate_digest": candidate["digest"],
        "phase": "abandoned",
        "active": False,
        "released": True,
    }


def rollback_plan(args: argparse.Namespace) -> dict[str, Any]:
    context = _deployment_context(args)
    return {
        "schema_version": "fotmob-rollback-v1",
        "generated_at": _now(),
        "passed": True,
        "mode": "plan",
        "mutated": False,
        "project": args.project,
        "deployed_git_sha": context["git_sha"],
        "deployment_report": str(args.deployment_report.resolve()),
        "native_objects_action": "retain",
        "steps": [
            {
                "order": 1,
                "action": "pause_writers",
                "command": (
                    "python scripts/fotmob_rollback.py pause --execute "
                    f"--confirm {CONFIRM_PAUSE} <common options>"
                ),
            },
            {
                "order": 2,
                "action": "deploy_consumer_revert",
                "requirement": (
                    "Deploy a reviewed immutable commit whose FotMob Silver/xref/DQ "
                    "consumers read the frozen legacy tables. Do not alter native data."
                ),
            },
            {
                "order": 3,
                "action": "run_fenced_legacy_silver_and_dq",
                "command": (
                    "python scripts/fotmob_rollback.py run-silver --execute "
                    f"--confirm {CONFIRM_RUN_SILVER} "
                    "--expected-consumer-sha <40-hex> "
                    "--publication-attempt <positive-int> <common options>"
                ),
            },
            {
                "order": 4,
                "action": "validate",
                "command": (
                    "python scripts/fotmob_rollback.py validate "
                    "--expected-consumer-sha <40-hex> --silver-run-id <run-id> "
                    "--publication-report <run-silver-report> "
                    "--publication-attempt <same-int> <common options>"
                ),
            },
        ],
    }


def pause_writers(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    if not args.execute or args.confirm != CONFIRM_PAUSE:
        raise RollbackError(
            f"pause requires --execute --confirm {CONFIRM_PAUSE}; nothing changed"
        )
    validate_live_deployment(args, require_running=True, run=run)
    _pause_all_writers(args, run=run)
    context = _deployment_context(args)
    deployed_sha = _container_deploy_sha(args, run=run)
    if deployed_sha != context["git_sha"]:
        raise RollbackError(
            "paused scheduler Git SHA differs from the admitted deployment report"
        )
    # Bracket the atomic metadata snapshot with live runtime attestation: a
    # container replacement on either side must fail rollback validation.
    validate_live_deployment(args, require_running=True, run=run)
    final_writer_state = inspect_writer_state(args, run=run)
    require_writers_stopped(final_writer_state)
    live_identity = validate_live_deployment(args, require_running=True, run=run)
    return {
        "schema_version": "fotmob-rollback-v1",
        "generated_at": _now(),
        "passed": True,
        "mode": "pause",
        "project": args.project,
        "catalog": args.catalog,
        "schema": args.bronze_schema,
        "git_sha": context["git_sha"],
        "paused": list(DAGS),
        "pause_states": final_writer_state["pause_states"],
        "running_runs": {},
        "queued_runs": {},
        "live_deployment": live_identity,
        "native_objects_action": "retain",
    }


def _validate_sha(value: str) -> str:
    sha = value.strip().lower()
    if len(sha) != 40 or any(character not in "0123456789abcdef" for character in sha):
        raise RollbackError("--expected-consumer-sha must be a full 40-hex Git SHA")
    return sha


def _legacy_counts(
    client: QueryClient, *, catalog: str, schema: str
) -> dict[str, int]:
    for value in (catalog, schema):
        if not re.fullmatch(r"[A-Za-z0-9_]+", value):
            raise RollbackError(f"unsafe SQL identifier: {value!r}")
    counts: dict[str, int] = {}
    for table in LEGACY_TABLES:
        rows = client.query(
            f'-- rollback:legacy:{table}\nSELECT COUNT(*) FROM "{catalog}"."{schema}"."{table}"'
        )
        if len(rows) != 1 or len(rows[0]) != 1:
            raise RollbackError(f"{table}: count query did not return one scalar")
        count = int(rows[0][0])
        if count <= 0:
            raise RollbackError(f"{table}: frozen legacy table is empty")
        counts[table] = count
    return counts


def validate_rollback(
    args: argparse.Namespace,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    client_factory: Callable[..., QueryClient] = connect_from_env,
) -> dict[str, Any]:
    expected_sha = _validate_sha(args.expected_consumer_sha)
    context = _deployment_context(args)
    if context.get("kept_paused") is not True:
        raise RollbackError(
            "rollback validation requires a deploy report created with --keep-paused"
        )
    if str(context.get("git_sha")) != expected_sha:
        raise RollbackError("deployment report Git SHA differs from consumer revert SHA")
    deployed_at = _timestamp(context["generated_at"])
    validate_live_deployment(args, require_running=True, run=run)
    env_result = _container_deploy_sha(args, run=run)
    if env_result != expected_sha:
        raise RollbackError(
            f"consumer revert is not deployed: expected={expected_sha}, observed={env_result!r}"
        )

    writer_state = inspect_writer_state(args, run=run)
    require_writers_stopped(writer_state)

    silver_runs = _parse_json_array(
        _airflow(
            args,
            "dags",
            "list-runs",
            "-d",
            "dag_transform_fotmob_silver",
            "--output",
            "json",
            run=run,
        )
    )
    matching = [row for row in silver_runs if str(row.get("run_id")) == args.silver_run_id]
    if len(matching) != 1 or str(matching[0].get("state")).lower() != "success":
        raise RollbackError(
            f"Silver/DQ run {args.silver_run_id!r} is absent or not successful"
        )
    run_started_at = _timestamp(matching[0].get("start_date"))
    if run_started_at < deployed_at:
        raise RollbackError("Silver/DQ evidence predates the rollback deployment")
    publication_evidence = validate_rollback_publication_evidence(
        args, expected_sha=expected_sha, run=run
    )
    publication_before = require_no_active_fotmob_publication(args, run=run)

    # Local/ambient Trino variables are not evidence. Bind the query client to
    # the exact non-secret endpoint and credential configured in the admitted
    # scheduler container before reading rollback data.
    trino_binding = bind_admitted_trino(args, require_running=True, run=run)
    if not args.trino_env_file:
        raise RollbackError("validate requires --trino-env-file")
    try:
        runtime_binding.load_host_trino_environment(args.trino_env_file)
    except runtime_binding.RuntimeBindingError as exc:
        raise RollbackError(str(exc)) from exc
    client = client_factory(catalog=args.catalog, schema=args.bronze_schema)
    try:
        marker_before = runtime_binding.validate_data_plane_marker(client, context)
        counts = _legacy_counts(
            client, catalog=args.catalog, schema=args.bronze_schema
        )
        marker_after = runtime_binding.validate_data_plane_marker(client, context)
    finally:
        client.close()
    live_identity = validate_live_deployment(args, require_running=True, run=run)
    final_writer_state = inspect_writer_state(args, run=run)
    require_writers_stopped(final_writer_state)
    live_identity = validate_live_deployment(args, require_running=True, run=run)
    publication_after = require_no_active_fotmob_publication(args, run=run)
    return {
        "schema_version": "fotmob-rollback-v1",
        "generated_at": _now(),
        "passed": True,
        "mode": "validate",
        "consumer_git_sha": expected_sha,
        "silver_dq_run_id": args.silver_run_id,
        "silver_dq_started_at": run_started_at.isoformat(),
        "deployment_generated_at": deployed_at.isoformat(),
        "legacy_table_counts": counts,
        "writers_paused": list(DAGS),
        "pause_states": final_writer_state["pause_states"],
        "trino_binding": {
            "runtime": trino_binding["trino"],
            "data_plane_before": marker_before,
            "data_plane_after": marker_after,
        },
        "live_deployment": live_identity,
        "publication_quiescence_before": publication_before,
        "publication_quiescence_after": publication_after,
        "rollback_publication": publication_evidence,
        "native_objects_action": "retain",
    }


def build_parser() -> argparse.ArgumentParser:
    default_compose = Path(__file__).resolve().parents[1] / "deploy/fotmob/airflow.compose.yaml"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "plan",
            "pause",
            "run-silver",
            "recover-publication",
            "validate",
        ),
    )
    parser.add_argument("--compose-file", type=Path, default=default_compose)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument(
        "--trino-env-file",
        type=Path,
        help="Host-reachable Trino endpoint used only by validate",
    )
    parser.add_argument("--deployment-report", type=Path, required=True)
    parser.add_argument(
        "--publication-report",
        type=Path,
        help="Durable report from run-silver/recover-publication",
    )
    parser.add_argument("--project", default="fotmob-airflow")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm")
    parser.add_argument("--expected-consumer-sha", default="")
    parser.add_argument("--silver-run-id", default="")
    parser.add_argument("--timeout-seconds", type=int, default=12 * 60 * 60)
    parser.add_argument(
        "--publication-attempt",
        type=int,
        default=1,
        help=(
            "Positive deterministic synthetic-generation attempt; increment only "
            "after an earlier attempt is proven terminal and released"
        ),
    )
    parser.add_argument("--catalog", default="iceberg")
    parser.add_argument("--bronze-schema", default="bronze")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "plan":
            report = rollback_plan(args)
        elif args.command == "pause":
            report = pause_writers(args)
        elif args.command == "run-silver":
            report = run_rollback_validation_silver(args)
        elif args.command == "recover-publication":
            report = recover_rollback_publication(args)
        else:
            if not args.silver_run_id:
                raise RollbackError("validate requires --silver-run-id")
            if not args.publication_report:
                raise RollbackError("validate requires --publication-report")
            report = validate_rollback(args)
    except Exception as exc:
        existing: dict[str, Any] | None = None
        if args.command in {"run-silver", "recover-publication"}:
            try:
                candidate = json.loads(args.output.read_text(encoding="utf-8"))
                if (
                    isinstance(candidate, dict)
                    and candidate.get("schema_version")
                    == ROLLBACK_PUBLICATION_SCHEMA
                ):
                    existing = candidate
            except (OSError, json.JSONDecodeError):
                pass
        report = {
            **(existing or {}),
            "schema_version": (
                ROLLBACK_PUBLICATION_SCHEMA
                if existing is not None
                else "fotmob-rollback-v1"
            ),
            "generated_at": _now(),
            "passed": False,
            "mode": args.command,
            "error": f"{type(exc).__name__}: {exc}",
            "native_objects_action": "retain",
        }
        if existing is not None:
            report["recovery_required"] = True
    _atomic_json(args.output, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report.get("passed") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
