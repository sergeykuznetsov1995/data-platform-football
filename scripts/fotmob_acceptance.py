#!/usr/bin/env python3
"""Fail-closed production acceptance for the source-native FotMob pipeline.

The command deliberately talks to Trino through its Python client instead of
parsing the human-oriented CLI output.  Every SQL error, absent mandatory
result, violated invariant, or malformed scope file is represented in the JSON
report and makes the process exit non-zero.

Examples::

    python scripts/fotmob_acceptance.py verify \
      --scopes deploy/fotmob/acceptance-scopes.example.json \
      --lifecycle-report /var/lib/fotmob-evidence/replay.json \
      --output /var/lib/fotmob-evidence/verify.json

    python scripts/fotmob_acceptance.py parity \
      --scopes deploy/fotmob/acceptance-scopes.example.json \
      --lifecycle-report /var/lib/fotmob-evidence/replay.json \
      --output /var/lib/fotmob-evidence/parity.json
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
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

sys.dont_write_bytecode = True

try:  # package import in tests / ``python -m``
    from scripts import fotmob_runtime as runtime_binding
except ModuleNotFoundError:  # direct ``python scripts/fotmob_acceptance.py``
    import fotmob_runtime as runtime_binding

TERMINAL_STATUSES = frozenset({"success", "not_modified", "not_available", "excluded"})
PARSER_VERSION = "fotmob-native-v2"
ROLLING_FALLBACK_PARSER_VERSION = "fotmob-native-v1"
LIFECYCLE_SCHEMA_VERSION = "fotmob-issue-930-backfill-v1"
APPROVED_SCOPE_COUNT = 158
APPROVED_SCOPE_ARTIFACT = (
    Path(__file__).resolve().parents[1] / "configs/fotmob/issue-930-scopes.txt"
)
APPROVED_SCOPE_ARTIFACT_SHA256 = (
    "f1d95f916c78ed80e5784e2cd5bda7263cece37d9fde6d52fb2a1a4d9e97cb58"
)
APPROVED_PARITY_SCOPE_ARTIFACT = (
    Path(__file__).resolve().parents[1] / "deploy/fotmob/issue-930-parity-scopes.json"
)
APPROVED_PARITY_SCOPE_ARTIFACT_SHA256 = (
    "2fceb11fd69dcd136f4879b6dad85193924b1a7d7484cf00fc9f7f4a7305568d"
)
APPROVED_PARITY_SCOPES = frozenset(
    {
        (47, "2025/2026", "ENG-Premier League", 2025),
        (87, "2025/2026", "ESP-La Liga", 2025),
        (54, "2025/2026", "GER-Bundesliga", 2025),
        (55, "2025/2026", "ITA-Serie A", 2025),
        (53, "2025/2026", "FRA-Ligue 1", 2025),
    }
)
APPROVED_PARITY_SCOPE_COUNT = len(APPROVED_PARITY_SCOPES)
REQUIRED_SCOPE_ENTITIES = frozenset(
    {"season", "leaderboards", "matches", "teams", "players"}
)
ISSUE_930_SCOPE_ENTITIES = (
    "season",
    "leaderboards",
    "matches",
    "teams",
    "players",
)
PUBLICATION_OWNER_DAG_ID = "fotmob_issue_930_backfill"
INGEST_DAG_ID = "dag_ingest_fotmob"
SILVER_DAG_ID = "dag_transform_fotmob_silver"
PUBLICATION_SCHEMA = "fotmob-publication-v1"
_GENERATION_ID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
)
EXPECTED_SCOPE_COUNT_KEYS = frozenset({"leaderboards", "matches", "teams", "players"})
CURRENT_VIEW_KEYS: Mapping[str, tuple[str, ...]] = {
    "fotmob_competitions": ("competition_id",),
    "fotmob_competition_seasons": ("competition_id", "source_season_key"),
    "fotmob_competition_season_history": (
        "competition_id",
        "history_season_label",
    ),
    "fotmob_season_stages": (
        "competition_id",
        "source_season_key",
        "stage_id",
    ),
    "fotmob_matches": ("competition_id", "source_season_key", "match_id"),
    "fotmob_standings": (
        "competition_id",
        "source_season_key",
        "table_id",
        "table_name",
        "table_type",
        "team_id",
        "position",
    ),
    "fotmob_playoff_brackets": (
        "competition_id",
        "source_season_key",
        "stage_id",
        "draw_order",
        "match_ids",
    ),
    "fotmob_season_teams": (
        "competition_id",
        "source_season_key",
        "team_id",
    ),
    "fotmob_leaderboard_categories": (
        "competition_id",
        "source_season_key",
        "participant_type",
        "source_order",
    ),
    "fotmob_leaderboards": (
        "competition_id",
        "source_season_key",
        "participant_type",
        "participant_id",
        "team_id",
        "stat_name",
        "rank",
        "top_list_index",
    ),
    "fotmob_match_payloads": (
        "competition_id",
        "source_season_key",
        "match_id",
    ),
    "fotmob_team_snapshots": ("team_id",),
    "fotmob_squad_snapshots": ("team_id", "member_type", "member_id"),
    "fotmob_player_snapshots": ("player_id",),
    "fotmob_transfer_events": ("transfer_event_id",),
}
TRINO_ENV_KEYS = frozenset(
    {
        "TRINO_HOST",
        "TRINO_PORT",
        "TRINO_USER",
        "TRINO_PASSWORD",
        "TRINO_HTTP_SCHEME",
        "TRINO_TLS_VERIFY",
    }
)


class QueryClient(Protocol):
    def query(self, sql: str) -> list[tuple[Any, ...]]: ...

    def close(self) -> None: ...


class TrinoQueryClient:
    """Small adapter that always consumes the complete Trino result."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            return list(cursor.fetchall())
        finally:
            cursor.close()

    def close(self) -> None:
        self.connection.close()


@dataclass(frozen=True)
class Scope:
    competition_id: int
    source_season_key: str
    legacy_league: str | None = None
    legacy_season: int | None = None

    @property
    def identity(self) -> str:
        return f"{self.competition_id}={self.source_season_key}"


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    details: Mapping[str, Any]
    error: str | None = None


@dataclass(frozen=True)
class AcceptanceLineage:
    """Validated immutable lineage derived from one completed #930 lifecycle."""

    report_path: str
    report_sha256: str
    command: str
    mode: str
    publication_attempt: int
    deployment_id: str
    git_sha: str
    generation_id: str
    runner_run_id: str
    ingest_run_id: str
    silver_run_id: str
    plan_signature: str
    completed_since: str
    scope_artifact: str
    scope_sha256: str
    scope_count: int
    entities: tuple[str, ...]
    candidate_digest: str
    candidate_transform_task_ids: tuple[str, ...]
    publication_binding: Mapping[str, str]

    def summary(self) -> dict[str, Any]:
        return {
            "schema_version": LIFECYCLE_SCHEMA_VERSION,
            "report_path": self.report_path,
            "report_sha256": self.report_sha256,
            "command": self.command,
            "mode": self.mode,
            "phase": "abandoned",
            "recovery_required": False,
            "publication_attempt": self.publication_attempt,
            "deployment_id": self.deployment_id,
            "git_sha": self.git_sha,
            "generation_id": self.generation_id,
            "runner_run_id": self.runner_run_id,
            "ingest_run_id": self.ingest_run_id,
            "silver_run_id": self.silver_run_id,
            "plan_signature": self.plan_signature,
            "completed_since": self.completed_since,
            "scope": {
                "artifact": self.scope_artifact,
                "sha256": self.scope_sha256,
                "count": self.scope_count,
            },
            "entities": list(self.entities),
            "candidate_digest": self.candidate_digest,
            "candidate_transform_task_ids": list(self.candidate_transform_task_ids),
            "publication_binding": dict(self.publication_binding),
        }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _git_sha() -> str | None:
    configured = os.environ.get("FOTMOB_DEPLOY_GIT_SHA", "").strip()
    if configured:
        return configured
    try:
        result = subprocess.run(
            ("git", "rev-parse", "HEAD"),
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2, default=str)
        stream.write("\n")
        temporary = Path(stream.name)
    temporary.replace(path)


def _literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def load_trino_env(path: Path) -> None:
    """Load only Trino connection keys from a Compose-style secret env file.

    Existing process variables win.  Values are never returned or copied into
    evidence, and unrelated credentials in the shared env file are ignored.
    """

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise RuntimeError(f"cannot read Trino env file {path}: {exc}") from exc
    for line_number, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in TRINO_ENV_KEYS or key in os.environ:
            continue
        value = value.strip()
        if value[:1] in {"'", '"'}:
            if len(value) < 2 or value[-1] != value[0]:
                raise RuntimeError(
                    f"{path}:{line_number}: unterminated quoted {key} value"
                )
            value = value[1:-1]
        os.environ[key] = value


def _qualified(catalog: str, schema: str, table: str) -> str:
    for value in (catalog, schema, table):
        if not value.replace("_", "").isalnum():
            raise ValueError(f"unsafe SQL identifier: {value!r}")
    return f'"{catalog}"."{schema}"."{table}"'


def load_scopes(
    path: Path,
    *,
    parity: bool = False,
    content: bytes | None = None,
) -> list[Scope]:
    try:
        raw = content if content is not None else path.read_bytes()
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError(f"cannot read scope JSON {path}: {exc}") from exc
    if text.lstrip().startswith("["):
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"cannot read scope JSON {path}: {exc}") from exc
    else:
        if parity:
            raise ValueError(
                "parity scopes must be JSON with legacy_league and legacy_season"
            )
        payload = []
        for line_number, raw_line in enumerate(text.splitlines(), start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.count("=") != 1:
                raise ValueError(
                    f"{path}:{line_number}: expected competition_id=source_season_key"
                )
            competition_id, source_season_key = line.split("=", 1)
            payload.append(
                {
                    "competition_id": competition_id,
                    "source_season_key": source_season_key,
                }
            )
    if not isinstance(payload, list) or not payload:
        raise ValueError(
            "scope file must contain a non-empty JSON array or ID=season list"
        )
    scopes: list[Scope] = []
    identities: set[tuple[int, str]] = set()
    for index, item in enumerate(payload):
        if not isinstance(item, Mapping):
            raise ValueError(f"scope[{index}] must be an object")
        try:
            competition_id = int(item["competition_id"])
            season_key = str(item["source_season_key"]).strip()
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"scope[{index}] has invalid native identity") from exc
        if competition_id <= 0 or not season_key:
            raise ValueError(f"scope[{index}] has invalid native identity")
        legacy_league = item.get("legacy_league")
        legacy_season = item.get("legacy_season")
        if parity and (not legacy_league or legacy_season is None):
            raise ValueError(
                f"scope[{index}] requires legacy_league and legacy_season for parity"
            )
        identity = (competition_id, season_key)
        if identity in identities:
            raise ValueError(f"duplicate scope: {competition_id}={season_key}")
        identities.add(identity)
        scopes.append(
            Scope(
                competition_id=competition_id,
                source_season_key=season_key,
                legacy_league=(
                    str(legacy_league).strip() if legacy_league is not None else None
                ),
                legacy_season=(
                    int(legacy_season) if legacy_season is not None else None
                ),
            )
        )
    return scopes


def validate_approved_scope_contract(
    command: str,
    scopes: Sequence[Scope],
) -> Mapping[str, Any]:
    """Bind production acceptance to the reviewed #930 identity sets."""

    if command == "verify":
        try:
            approved_bytes = APPROVED_SCOPE_ARTIFACT.read_bytes()
        except OSError as exc:
            raise ValueError(
                f"cannot read approved scope artifact {APPROVED_SCOPE_ARTIFACT}: {exc}"
            ) from exc
        artifact_sha256 = hashlib.sha256(approved_bytes).hexdigest()
        if artifact_sha256 != APPROVED_SCOPE_ARTIFACT_SHA256:
            raise ValueError(
                "approved #930 scope artifact differs from its pinned SHA-256"
            )
        approved = load_scopes(
            APPROVED_SCOPE_ARTIFACT,
            content=approved_bytes,
        )
        expected = frozenset(
            (scope.competition_id, scope.source_season_key) for scope in approved
        )
        observed = frozenset(
            (scope.competition_id, scope.source_season_key) for scope in scopes
        )
        if len(expected) != APPROVED_SCOPE_COUNT:
            raise ValueError(
                "approved #930 scope artifact has an invalid identity count: "
                f"observed={len(expected)}, expected={APPROVED_SCOPE_COUNT}"
            )
        if observed != expected:
            missing = sorted(expected - observed)[:10]
            unexpected = sorted(observed - expected)[:10]
            raise ValueError(
                "approved verify scope mismatch: "
                f"observed={len(observed)}, expected={APPROVED_SCOPE_COUNT}, "
                f"missing={missing!r}, unexpected={unexpected!r}"
            )
        return {
            "name": "issue-930-verify",
            "expected_scope_count": APPROVED_SCOPE_COUNT,
            "identity_artifact": str(APPROVED_SCOPE_ARTIFACT),
            "identity_artifact_sha256": artifact_sha256,
            "scope_file_sha256": artifact_sha256,
        }

    if command != "parity":
        raise ValueError(f"unsupported acceptance command: {command!r}")
    observed_parity = frozenset(
        (
            scope.competition_id,
            scope.source_season_key,
            scope.legacy_league,
            scope.legacy_season,
        )
        for scope in scopes
    )
    if observed_parity != APPROVED_PARITY_SCOPES:
        missing = sorted(APPROVED_PARITY_SCOPES - observed_parity)[:10]
        unexpected = sorted(observed_parity - APPROVED_PARITY_SCOPES)[:10]
        raise ValueError(
            "approved parity scope mismatch: "
            f"observed={len(observed_parity)}, "
            f"expected={APPROVED_PARITY_SCOPE_COUNT}, "
            f"missing={missing!r}, unexpected={unexpected!r}"
        )
    try:
        parity_artifact_bytes = APPROVED_PARITY_SCOPE_ARTIFACT.read_bytes()
    except OSError as exc:
        raise ValueError(
            "cannot read approved parity artifact "
            f"{APPROVED_PARITY_SCOPE_ARTIFACT}: {exc}"
        ) from exc
    parity_artifact_sha256 = hashlib.sha256(parity_artifact_bytes).hexdigest()
    if parity_artifact_sha256 != APPROVED_PARITY_SCOPE_ARTIFACT_SHA256:
        raise ValueError(
            "approved #930 parity artifact differs from its pinned SHA-256"
        )
    artifact_scopes = load_scopes(
        APPROVED_PARITY_SCOPE_ARTIFACT,
        parity=True,
        content=parity_artifact_bytes,
    )
    artifact_identities = frozenset(
        (
            scope.competition_id,
            scope.source_season_key,
            scope.legacy_league,
            scope.legacy_season,
        )
        for scope in artifact_scopes
    )
    if artifact_identities != APPROVED_PARITY_SCOPES:
        raise ValueError("approved #930 parity artifact has unexpected identities")
    return {
        "name": "issue-930-parity",
        "expected_scope_count": APPROVED_PARITY_SCOPE_COUNT,
        "identities": [list(identity) for identity in sorted(APPROVED_PARITY_SCOPES)],
        "identity_artifact": str(APPROVED_PARITY_SCOPE_ARTIFACT),
        "identity_artifact_sha256": parity_artifact_sha256,
        "scope_file_sha256": parity_artifact_sha256,
    }


def _validated_plan_signature(value: str) -> str:
    signature = str(value).strip().lower()
    if not re.fullmatch(r"fmplan1-[0-9a-f]{64}", signature):
        raise ValueError("plan signature must be fmplan1- followed by 64 hex chars")
    return signature


def _validated_parser_version(value: str) -> str:
    version = str(value).strip()
    if version != PARSER_VERSION:
        raise ValueError(
            f"production acceptance is pinned to parser_version={PARSER_VERSION!r}"
        )
    return version


def _completed_since_sql(value: str) -> tuple[str, str]:
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            "lifecycle completed_since must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise ValueError("lifecycle completed_since must include an explicit timezone")
    utc = parsed.astimezone(timezone.utc)
    canonical = utc.isoformat().replace("+00:00", "Z")
    sql = utc.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")
    return canonical, f"TIMESTAMP '{sql}'"


def _timestamp(value: Any, *, field: str) -> tuple[datetime, str]:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include an explicit timezone")
    utc = parsed.astimezone(timezone.utc)
    return utc, utc.isoformat().replace("+00:00", "Z")


def _mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"lifecycle {field} must be an object")
    return value


def _publication_generation_id(binding: Mapping[str, Any]) -> str:
    material = json.dumps(dict(binding), sort_keys=True, separators=(",", ":"))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"fotmob-publication:{material}"))


def load_lifecycle_report(
    path: Path,
    *,
    deployment_context: Mapping[str, Any],
    deployment_report: Path,
    project: str,
) -> AcceptanceLineage:
    """Load and fully bind acceptance to one successful #930 lifecycle.

    The report is operator-supplied evidence, so every identity later used in
    SQL is derived here and reconciled across the independent report fields.
    The live ControlStore state is checked separately immediately around SQL.
    """

    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid lifecycle report {path}: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("lifecycle report must be a JSON object")

    digest = hashlib.sha256(raw).hexdigest()
    if (
        payload.get("schema_version") != LIFECYCLE_SCHEMA_VERSION
        or payload.get("passed") is not True
        or payload.get("phase") != "abandoned"
        or payload.get("recovery_required") is not False
    ):
        raise ValueError(
            "lifecycle report must be an exact passed abandoned "
            f"{LIFECYCLE_SCHEMA_VERSION} report with no recovery required"
        )
    command = str(payload.get("command") or "")
    if command not in {"run", "recover"}:
        raise ValueError("lifecycle command must be run or recover")
    mode = str(payload.get("mode") or "")
    if mode not in {"backfill", "replay"}:
        raise ValueError("lifecycle mode must be backfill or replay")
    attempt = payload.get("publication_attempt")
    if type(attempt) is not int or attempt <= 0:
        raise ValueError("lifecycle publication_attempt must be a positive integer")

    deployment_id = str(deployment_context.get("deployment_id") or "")
    git_sha = str(deployment_context.get("git_sha") or "")
    if (
        deployment_context.get("activation_state") != "kept_paused"
        or deployment_context.get("kept_paused") is not True
    ):
        raise ValueError(
            "lifecycle acceptance requires its original kept-paused deployment"
        )
    if (
        payload.get("project") != project
        or payload.get("deployment_report") != str(deployment_report.resolve())
        or payload.get("deployment_id") != deployment_id
        or payload.get("git_sha") != git_sha
    ):
        raise ValueError("lifecycle report deployment identity differs")

    approved_scopes = load_scopes(APPROVED_SCOPE_ARTIFACT)
    approved = validate_approved_scope_contract("verify", approved_scopes)
    scope = _mapping(payload.get("scope"), field="scope")
    expected_scope = {
        "name": "issue-930-verify",
        "artifact": str(APPROVED_SCOPE_ARTIFACT.resolve()),
        "sha256": APPROVED_SCOPE_ARTIFACT_SHA256,
        "count": APPROVED_SCOPE_COUNT,
    }
    if dict(scope) != expected_scope or approved["scope_file_sha256"] != scope.get(
        "sha256"
    ):
        raise ValueError("lifecycle scope is not the exact approved 158-scope artifact")
    if payload.get("entities") != list(ISSUE_930_SCOPE_ENTITIES):
        raise ValueError("lifecycle entities are not the exact five closure entities")
    if payload.get("publication_action") != "abandon_unclaimed_candidate":
        raise ValueError("lifecycle publication action is not the closure action")

    publication = _mapping(payload.get("publication"), field="publication")
    if set(publication) != {"generation_id", "binding"}:
        raise ValueError("lifecycle publication envelope has unexpected fields")
    generation_id = str(publication.get("generation_id") or "").casefold()
    if _GENERATION_ID_RE.fullmatch(generation_id) is None:
        raise ValueError("lifecycle publication generation_id is invalid")
    binding = _mapping(publication.get("binding"), field="publication.binding")
    deployed_at, _ = _timestamp(
        deployment_context.get("generated_at"), field="deployment generated_at"
    )
    interval_start = deployed_at + timedelta(
        seconds=86_400 + (attempt - 1) * 2 + (1 if mode == "replay" else 0)
    )
    interval_end = interval_start + timedelta(seconds=1)
    expected_binding = {
        "schema": PUBLICATION_SCHEMA,
        "source": "fotmob",
        "owner": "isolated",
        "data_interval_start": interval_start.isoformat(timespec="microseconds"),
        "data_interval_end": interval_end.isoformat(timespec="microseconds"),
        "runtime_fingerprint": git_sha,
    }
    if dict(binding) != expected_binding:
        raise ValueError("lifecycle publication binding is not exact")
    if generation_id != _publication_generation_id(expected_binding):
        raise ValueError("lifecycle generation_id does not match its binding")

    compact_generation = generation_id.replace("-", "")
    expected_runs = {
        "ingest_dag_id": INGEST_DAG_ID,
        "ingest_run_id": (f"issue930_{mode}_a{attempt}__{compact_generation}"),
        "silver_dag_id": SILVER_DAG_ID,
        "silver_run_id": f"fotmob_silver__{generation_id}",
        "native_runner_run_id": generation_id,
    }
    runs = _mapping(payload.get("runs"), field="runs")
    if dict(runs) != expected_runs:
        raise ValueError("lifecycle DAG and native runner identities differ")

    ingest_terminal = _mapping(payload.get("ingest_terminal"), field="ingest_terminal")
    silver_terminal = _mapping(payload.get("silver_terminal"), field="silver_terminal")
    if (
        ingest_terminal.get("dag_id") != INGEST_DAG_ID
        or ingest_terminal.get("run_id") != expected_runs["ingest_run_id"]
        or str(ingest_terminal.get("state") or "").casefold() != "success"
        or silver_terminal.get("dag_id") != SILVER_DAG_ID
        or silver_terminal.get("run_id") != expected_runs["silver_run_id"]
        or str(silver_terminal.get("state") or "").casefold() != "success"
    ):
        raise ValueError("lifecycle terminal DAG run identities are not exact")
    completed_at, completed_since = _timestamp(
        ingest_terminal.get("start_date"), field="ingest_terminal.start_date"
    )
    if completed_at < deployed_at:
        raise ValueError("lifecycle ingest started before its deployment")
    final_at, _ = _timestamp(payload.get("generated_at"), field="generated_at")
    if final_at < completed_at:
        raise ValueError("lifecycle report predates its ingest start")

    plan_signature = _validated_plan_signature(str(payload.get("plan_signature") or ""))
    validation = _mapping(payload.get("validation"), field="validation")
    if (
        validation.get("run_id") != generation_id
        or validation.get("mode") != mode
        or validation.get("scope_count") != APPROVED_SCOPE_COUNT
        or validation.get("scope_sha256") != APPROVED_SCOPE_ARTIFACT_SHA256
        or validation.get("entities") != sorted(ISSUE_930_SCOPE_ENTITIES)
        or validation.get("plan_signature") != plan_signature
    ):
        raise ValueError("lifecycle validation identity or plan signature differs")

    candidate = _mapping(payload.get("candidate"), field="candidate")
    candidate_digest = str(candidate.get("digest") or "").casefold()
    transform_task_ids = candidate.get("transform_task_ids")
    if (
        set(candidate) != {"generation_id", "digest", "transform_task_ids"}
        or candidate.get("generation_id") != generation_id
        or re.fullmatch(r"[0-9a-f]{64}", candidate_digest) is None
        or not isinstance(transform_task_ids, list)
        or not transform_task_ids
        or any(not isinstance(item, str) or not item for item in transform_task_ids)
        or len(set(transform_task_ids)) != len(transform_task_ids)
    ):
        raise ValueError("lifecycle Silver candidate identity is invalid")
    normalized_candidate = {
        "generation_id": generation_id,
        "digest": candidate_digest,
        "transform_task_ids": list(transform_task_ids),
    }
    publication_state = _mapping(
        payload.get("publication_state"), field="publication_state"
    )
    if (
        publication_state.get("generation_id") != generation_id
        or publication_state.get("status") != "succeeded"
        or publication_state.get("phase") != "abandoned"
        or publication_state.get("active") is not False
        or publication_state.get("released") is not True
        or publication_state.get("published") is not False
        or publication_state.get("candidate") != normalized_candidate
    ):
        raise ValueError(
            "lifecycle publication_state is not abandoned, inactive, released, "
            "and unpublished for the exact candidate"
        )

    return AcceptanceLineage(
        report_path=str(path.resolve()),
        report_sha256=digest,
        command=command,
        mode=mode,
        publication_attempt=attempt,
        deployment_id=deployment_id,
        git_sha=git_sha,
        generation_id=generation_id,
        runner_run_id=generation_id,
        ingest_run_id=expected_runs["ingest_run_id"],
        silver_run_id=expected_runs["silver_run_id"],
        plan_signature=plan_signature,
        completed_since=completed_since,
        scope_artifact=expected_scope["artifact"],
        scope_sha256=APPROVED_SCOPE_ARTIFACT_SHA256,
        scope_count=APPROVED_SCOPE_COUNT,
        entities=ISSUE_930_SCOPE_ENTITIES,
        candidate_digest=candidate_digest,
        candidate_transform_task_ids=tuple(transform_task_ids),
        publication_binding=expected_binding,
    )


def read_live_publication_state(
    context: Mapping[str, Any],
    *,
    generation_id: str,
    project: str,
    compose_file: Path,
    env_file: Path,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> Mapping[str, Any]:
    """Read the exact generation through the already-admitted scheduler."""

    if _GENERATION_ID_RE.fullmatch(generation_id) is None:
        raise ValueError("live publication generation_id is invalid")
    marker = "FOTMOB_ACCEPTANCE_PUBLICATION_JSON="
    code = (
        "import json; from scrapers.fbref.control import ControlStore; "
        "r=ControlStore.from_env().get_publication_generation("
        f"{generation_id!r},source='fotmob'); "
        f"print('{marker}'+json.dumps(r,default=str,sort_keys=True))"
    )
    try:
        result = run(
            (
                *runtime_binding.compose_base(
                    project=project,
                    compose_file=compose_file,
                    env_file=env_file,
                ),
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
            env=runtime_binding.compose_environment(context),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError("cannot read live FotMob publication generation") from exc
    marked = [
        line[len(marker) :]
        for line in result.stdout.splitlines()
        if line.startswith(marker)
    ]
    if len(marked) != 1:
        raise RuntimeError("live publication lookup returned ambiguous evidence")
    try:
        payload = json.loads(marked[0])
    except json.JSONDecodeError as exc:
        raise RuntimeError("live publication lookup returned invalid JSON") from exc
    if not isinstance(payload, Mapping):
        raise RuntimeError("live publication generation is absent")
    return payload


def validate_live_publication_state(
    state: Mapping[str, Any], lineage: AcceptanceLineage
) -> Mapping[str, Any]:
    """Reconcile durable ControlStore state with the reviewed lifecycle bytes."""

    candidate = {
        "generation_id": lineage.generation_id,
        "digest": lineage.candidate_digest,
        "transform_task_ids": list(lineage.candidate_transform_task_ids),
    }
    released_at, released_at_text = _timestamp(
        state.get("released_at"), field="live publication released_at"
    )
    _ = released_at
    if (
        state.get("generation_id") != lineage.generation_id
        or state.get("source") != "fotmob"
        or state.get("status") != "succeeded"
        or state.get("phase") != "abandoned"
        or state.get("binding") != dict(lineage.publication_binding)
        or state.get("candidate") != candidate
        or state.get("consumer") is not None
        or state.get("owner_dag_id") != PUBLICATION_OWNER_DAG_ID
        or state.get("active") is not False
        or state.get("lock_active") is not False
    ):
        raise ValueError(
            "live publication state differs from the exact abandoned lifecycle"
        )
    return {
        "generation_id": lineage.generation_id,
        "status": "succeeded",
        "phase": "abandoned",
        "active": False,
        "released": True,
        "released_at": released_at_text,
        "published": False,
        "candidate_digest": lineage.candidate_digest,
    }


def _run_check(name: str, operation: Callable[[], Mapping[str, Any]]) -> Check:
    try:
        details = dict(operation())
        if "passed" not in details:
            raise RuntimeError("check implementation did not return 'passed'")
        passed = bool(details.pop("passed"))
        return Check(name=name, passed=passed, details=details)
    except Exception as exc:  # every query failure belongs in durable evidence
        return Check(
            name=name,
            passed=False,
            details={},
            error=f"{type(exc).__name__}: {exc}",
        )


def _one_row(client: QueryClient, sql: str, *, columns: int) -> tuple[Any, ...]:
    rows = client.query(sql)
    if len(rows) != 1 or len(rows[0]) != columns:
        raise RuntimeError(
            f"mandatory SQL returned shape rows={len(rows)}, "
            f"columns={len(rows[0]) if rows else 0}; expected 1x{columns}"
        )
    return rows[0]


def _scope_values(scopes: Sequence[Scope]) -> str:
    return ",\n".join(
        f"({scope.competition_id}, {_literal(scope.source_season_key)})"
        for scope in scopes
    )


def _scope_coverage_hash(
    coverage: Mapping[str, Any],
    counts: Mapping[str, int],
) -> str:
    material = json.dumps(
        {"coverage": dict(coverage), "counts": dict(counts)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _identity_hash(values: Sequence[Any] | set[Any]) -> str:
    material = "\0".join(sorted(str(value) for value in values)).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _current_scope_coverage(
    client: QueryClient,
    scopes: Sequence[Scope],
    *,
    catalog: str,
    schema: str,
    manifest: str,
    parser_version: str,
    plan_signature: str,
    completed_since_sql: str,
    runner_run_id: str,
) -> Mapping[tuple[int, str], Mapping[str, Mapping[str, Any]]]:
    expected = {(scope.competition_id, scope.source_season_key) for scope in scopes}
    identities: dict[tuple[int, str], dict[str, set[Any]]] = {
        identity: {
            "leaderboards": set(),
            "matches": set(),
            "teams": set(),
            "players": set(),
        }
        for identity in expected
    }
    ctes = f"""
        {
        _candidate_ctes(
            scopes,
            manifest=manifest,
            parser_version=parser_version,
            plan_signature=plan_signature,
            completed_since_sql=completed_since_sql,
            runner_run_id=runner_run_id,
        )
    }, scope_manifest_ranked AS (
            SELECT c.competition_id, c.source_season_key, m.batch_id, m.status,
                   ROW_NUMBER() OVER (
                       PARTITION BY c.competition_id, c.source_season_key
                       ORDER BY m.completed_at DESC, m.batch_id DESC
                   ) manifest_rn
            FROM completions c
            JOIN {manifest} m
              ON CAST(m.competition_id AS BIGINT) = c.competition_id
             AND m.source_season_key = c.source_season_key
             AND m.target_type = 'league_season'
             AND m.parser_version = {_literal(parser_version)}
             AND m.run_id = {_literal(runner_run_id)}
             AND m.completed_at <= c.completed_at
             AND m.status IN ('success', 'not_modified', 'not_available')
        ), scope_batches AS (
            SELECT competition_id, source_season_key, batch_id
            FROM scope_manifest_ranked
            WHERE manifest_rn = 1
              AND status IN ('success', 'not_modified')
        )
    """

    def checked_rows(sql: str, width: int) -> list[tuple[Any, ...]]:
        rows = client.query(sql)
        for row in rows:
            if len(row) != width:
                raise RuntimeError(
                    f"scope coverage query returned width {len(row)}, expected {width}"
                )
            identity = (int(row[0]), str(row[1]))
            if identity not in expected:
                raise RuntimeError(
                    f"scope coverage query returned unexpected identity {identity!r}"
                )
        return rows

    category_table = _qualified(catalog, schema, "fotmob_leaderboard_categories")
    for row in checked_rows(
        f"""-- acceptance:scope-coverage:leaderboards
        WITH {ctes}
        SELECT CAST(category.competition_id AS BIGINT),
               category.source_season_key, category.participant_type,
               category.name, category.fetch_all_url,
               CAST(category.source_order AS BIGINT)
        FROM {category_table} category
        JOIN scope_batches scope
          ON scope.competition_id = CAST(category.competition_id AS BIGINT)
         AND scope.source_season_key = category.source_season_key
         AND scope.batch_id = category._target_batch_id
        """,
        6,
    ):
        identities[(int(row[0]), str(row[1]))]["leaderboards"].add(
            (str(row[2]), str(row[3]), row[4], int(row[5]))
        )

    matches_table = _qualified(catalog, schema, "fotmob_matches")
    for row in checked_rows(
        f"""-- acceptance:scope-coverage:matches
        WITH {ctes}
        SELECT CAST(match_row.competition_id AS BIGINT),
               match_row.source_season_key, CAST(match_row.match_id AS VARCHAR)
        FROM {matches_table} match_row
        JOIN scope_batches scope
          ON scope.competition_id = CAST(match_row.competition_id AS BIGINT)
         AND scope.source_season_key = match_row.source_season_key
         AND scope.batch_id = match_row._target_batch_id
        WHERE COALESCE(match_row.finished, FALSE)
        """,
        3,
    ):
        identities[(int(row[0]), str(row[1]))]["matches"].add(str(row[2]))

    teams_table = _qualified(catalog, schema, "fotmob_season_teams")
    for row in checked_rows(
        f"""-- acceptance:scope-coverage:teams
        WITH {ctes}
        SELECT CAST(team.competition_id AS BIGINT), team.source_season_key,
               CAST(team.team_id AS VARCHAR)
        FROM {teams_table} team
        JOIN scope_batches scope
          ON scope.competition_id = CAST(team.competition_id AS BIGINT)
         AND scope.source_season_key = team.source_season_key
         AND scope.batch_id = team._target_batch_id
        """,
        3,
    ):
        identities[(int(row[0]), str(row[1]))]["teams"].add(str(row[2]))

    squads_table = _qualified(catalog, schema, "fotmob_squad_snapshots")
    for row in checked_rows(
        f"""-- acceptance:scope-coverage:players
        WITH {ctes}, scoped_teams AS (
            SELECT CAST(team.competition_id AS BIGINT) competition_id,
                   team.source_season_key, CAST(team.team_id AS VARCHAR) team_id
            FROM {teams_table} team
            JOIN scope_batches scope
              ON scope.competition_id = CAST(team.competition_id AS BIGINT)
             AND scope.source_season_key = team.source_season_key
             AND scope.batch_id = team._target_batch_id
        ), team_manifest_ranked AS (
            SELECT team.competition_id, team.source_season_key, team.team_id,
                   m.batch_id, m.status,
                   ROW_NUMBER() OVER (
                       PARTITION BY team.competition_id,
                                    team.source_season_key, team.team_id
                       ORDER BY m.completed_at DESC, m.batch_id DESC
                   ) manifest_rn
            FROM scoped_teams team
            JOIN completions completion
              ON completion.competition_id = team.competition_id
             AND completion.source_season_key = team.source_season_key
            JOIN {manifest} m
              ON m.target_type = 'team'
             AND m.entity_id = team.team_id
             AND m.parser_version = {_literal(parser_version)}
             AND m.run_id = {_literal(runner_run_id)}
             AND m.completed_at <= completion.completed_at
             AND m.status IN ('success', 'not_modified', 'not_available')
        ), team_batches AS (
            SELECT competition_id, source_season_key, team_id, batch_id
            FROM team_manifest_ranked
            WHERE manifest_rn = 1
              AND status IN ('success', 'not_modified')
        )
        SELECT DISTINCT team.competition_id, team.source_season_key,
               CAST(squad.member_id AS VARCHAR)
        FROM team_batches team
        JOIN {squads_table} squad
          ON CAST(squad.team_id AS VARCHAR) = team.team_id
         AND squad._target_batch_id = team.batch_id
        WHERE squad.member_type = 'player'
        """,
        3,
    ):
        identities[(int(row[0]), str(row[1]))]["players"].add(str(row[2]))

    return {
        identity: {
            entity: {
                "count": len(values),
                "identity_hash": _identity_hash(values),
            }
            for entity, values in by_entity.items()
        }
        for identity, by_entity in identities.items()
    }


def _candidate_ctes(
    scopes: Sequence[Scope],
    *,
    manifest: str,
    parser_version: str,
    plan_signature: str,
    completed_since_sql: str,
    runner_run_id: str,
) -> str:
    return f"""
        expected(competition_id, source_season_key) AS (
            VALUES {_scope_values(scopes)}
        ), completion_ranked AS (
            SELECT CAST(m.competition_id AS BIGINT) competition_id,
                   m.source_season_key, m.run_id, m.completed_at,
                   m.expected_counts_json, m.capabilities_json,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.competition_id, m.source_season_key
                       ORDER BY m.completed_at DESC, m.batch_id DESC
                   ) completion_rn
            FROM {manifest} m
            JOIN expected e
              ON e.competition_id = CAST(m.competition_id AS BIGINT)
             AND e.source_season_key = m.source_season_key
            WHERE m.target_type = 'scope_completion'
              AND m.status = 'success'
              AND m.parser_version = {_literal(parser_version)}
              AND m.entity_id = {_literal(plan_signature)}
              AND m.run_id = {_literal(runner_run_id)}
              AND m.completed_at >= {completed_since_sql}
        ), completions AS (
            SELECT competition_id, source_season_key, run_id, completed_at,
                   expected_counts_json, capabilities_json
            FROM completion_ranked
            WHERE completion_rn = 1
        ), candidate_runs AS (
            SELECT run_id, MAX(completed_at) completed_through
            FROM completions
            GROUP BY run_id
        )
    """


def _scope_completion_check(
    client: QueryClient,
    scopes: Sequence[Scope],
    *,
    manifest: str,
    parser_version: str,
    plan_signature: str,
    completed_since_sql: str,
    runner_run_id: str,
    catalog: str,
    schema: str,
) -> Mapping[str, Any]:
    rows = client.query(
        f"""-- acceptance:scope-completion
        WITH {
            _candidate_ctes(
                scopes,
                manifest=manifest,
                parser_version=parser_version,
                plan_signature=plan_signature,
                completed_since_sql=completed_since_sql,
                runner_run_id=runner_run_id,
            )
        }
        SELECT e.competition_id, e.source_season_key, c.run_id, c.completed_at,
               c.expected_counts_json, c.capabilities_json
        FROM expected e
        LEFT JOIN completions c USING (competition_id, source_season_key)
        ORDER BY 1, 2
        """
    )
    if len(rows) != len(scopes):
        raise RuntimeError(
            f"scope completion query returned {len(rows)} rows, expected {len(scopes)}"
        )
    malformed: list[dict[str, Any]] = []
    completed_scopes: list[dict[str, Any]] = []
    expected_coverage: dict[
        tuple[int, str], tuple[dict[str, int], Mapping[str, Any]]
    ] = {}
    missing = [f"{row[0]}={row[1]}" for row in rows if row[2] is None]
    for row in rows:
        if row[2] is None:
            continue
        identity = f"{row[0]}={row[1]}"
        try:
            if str(row[2]) != runner_run_id:
                raise ValueError("completion belongs to a different runner run_id")
            counts = json.loads(str(row[4]))
            capabilities = json.loads(str(row[5]))
            coverage = capabilities["coverage"]
            if not isinstance(coverage, Mapping):
                raise ValueError("coverage is not an object")
            entities = frozenset(coverage["scope_entities"])
            if (
                not isinstance(counts, Mapping)
                or set(counts) != EXPECTED_SCOPE_COUNT_KEYS
            ):
                raise ValueError("expected target counts have wrong keys")
            normalized_counts = {key: int(value) for key, value in counts.items()}
            if any(value < 0 for value in normalized_counts.values()):
                raise ValueError("expected target counts contain a negative value")
            if entities != REQUIRED_SCOPE_ENTITIES:
                raise ValueError(f"scope entities differ: {sorted(entities)!r}")
            for hash_key in (
                "leaderboard_identity_hash",
                "match_identity_hash",
                "team_identity_hash",
                "player_identity_hash",
            ):
                if not re.fullmatch(r"[0-9a-f]{64}", str(coverage.get(hash_key, ""))):
                    raise ValueError(f"invalid {hash_key}")
            if not re.fullmatch(
                r"[0-9a-f]{64}", str(capabilities.get("coverage_hash", ""))
            ):
                raise ValueError("invalid coverage_hash")
            if capabilities.get("plan_signature") != plan_signature:
                raise ValueError(
                    "completion capabilities use a different plan signature"
                )
            expected_coverage_hash = _scope_coverage_hash(
                coverage,
                normalized_counts,
            )
            if capabilities["coverage_hash"] != expected_coverage_hash:
                raise ValueError("coverage_hash does not match coverage and counts")
            completed_scopes.append(
                {
                    "scope": identity,
                    "run_id": str(row[2]),
                    "completed_at": str(row[3]),
                    "expected_targets": normalized_counts,
                }
            )
            expected_coverage[(int(row[0]), str(row[1]))] = (
                normalized_counts,
                coverage,
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            malformed.append({"scope": identity, "error": str(exc)})
    coverage_mismatches: list[dict[str, Any]] = []
    if not missing and not malformed:
        actual_coverage = _current_scope_coverage(
            client,
            scopes,
            catalog=catalog,
            schema=schema,
            manifest=manifest,
            parser_version=parser_version,
            plan_signature=plan_signature,
            completed_since_sql=completed_since_sql,
            runner_run_id=runner_run_id,
        )
        hash_keys = {
            "leaderboards": "leaderboard_identity_hash",
            "matches": "match_identity_hash",
            "teams": "team_identity_hash",
            "players": "player_identity_hash",
        }
        for scope_identity, (expected_counts, coverage) in expected_coverage.items():
            actual_entities = actual_coverage[scope_identity]
            for entity, hash_key in hash_keys.items():
                expected_count = expected_counts[entity]
                expected_hash = str(coverage[hash_key])
                actual = actual_entities[entity]
                if (
                    actual["count"] != expected_count
                    or actual["identity_hash"] != expected_hash
                ):
                    coverage_mismatches.append(
                        {
                            "scope": f"{scope_identity[0]}={scope_identity[1]}",
                            "entity": entity,
                            "expected_count": expected_count,
                            "actual_count": actual["count"],
                            "expected_identity_hash": expected_hash,
                            "actual_identity_hash": actual["identity_hash"],
                        }
                    )
    return {
        "passed": not missing and not malformed and not coverage_mismatches,
        "expected": len(scopes),
        "completed": len(scopes) - len(missing),
        "missing": missing,
        "malformed": malformed,
        "coverage_mismatches": coverage_mismatches,
        "coverage_reconciled": len(expected_coverage) if not coverage_mismatches else 0,
        "candidate_run_ids": sorted({item["run_id"] for item in completed_scopes}),
        "scope_lineage": completed_scopes,
    }


def _catalog_scope_check(
    client: QueryClient,
    scopes: Sequence[Scope],
    *,
    seasons_view: str,
) -> Mapping[str, Any]:
    rows = client.query(
        f"""-- acceptance:catalog-scopes
        WITH expected(competition_id, source_season_key) AS (
            VALUES {_scope_values(scopes)}
        ), observed AS (
            SELECT DISTINCT CAST(competition_id AS BIGINT) competition_id,
                            source_season_key
            FROM {seasons_view}
        )
        SELECT e.competition_id, e.source_season_key, o.competition_id IS NOT NULL
        FROM expected e
        LEFT JOIN observed o USING (competition_id, source_season_key)
        ORDER BY 1, 2
        """
    )
    if len(rows) != len(scopes):
        raise RuntimeError(
            f"catalog scope query returned {len(rows)} rows, expected {len(scopes)}"
        )
    missing = [f"{row[0]}={row[1]}" for row in rows if not bool(row[2])]
    return {
        "passed": not missing,
        "missing": missing,
        "observed": len(rows) - len(missing),
    }


def _latest_manifest_check(
    client: QueryClient,
    scopes: Sequence[Scope],
    *,
    manifest: str,
    parser_version: str,
    plan_signature: str,
    completed_since_sql: str,
    runner_run_id: str,
) -> Mapping[str, Any]:
    rows = client.query(
        f"""-- acceptance:latest-manifests
        WITH {
            _candidate_ctes(
                scopes,
                manifest=manifest,
                parser_version=parser_version,
                plan_signature=plan_signature,
                completed_since_sql=completed_since_sql,
                runner_run_id=runner_run_id,
            )
        }, ranked AS (
            SELECT m.target_type, m.target_key, m.status, m.error_code,
                   m.competition_id, m.source_season_key,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.target_type,
                                    COALESCE(m.entity_id, m.target_key),
                                    COALESCE(m.competition_id, ''),
                                    COALESCE(m.source_season_key, '')
                       ORDER BY m.completed_at DESC, m.batch_id DESC
                   ) rn
            FROM {manifest} m
            JOIN candidate_runs candidate ON candidate.run_id = m.run_id
            WHERE m.parser_version = {_literal(parser_version)}
              AND m.completed_at >= {completed_since_sql}
              AND m.completed_at <= candidate.completed_through
        )
        SELECT target_type, target_key, status, error_code,
               competition_id, source_season_key
        FROM ranked
        WHERE rn = 1
        ORDER BY 1, 2
        """
    )
    if not rows:
        raise RuntimeError("no manifests found for requested parser version")
    bad = [
        {
            "target_type": row[0],
            "target_key": row[1],
            "status": row[2],
            "error_code": row[3],
            "scope": f"{row[4]}={row[5]}",
        }
        for row in rows
        if str(row[2]) not in TERMINAL_STATUSES
    ]
    return {"passed": not bad, "targets": len(rows), "non_terminal": bad}


def _view_check(
    client: QueryClient,
    *,
    catalog: str,
    schema: str,
    table: str,
    keys: Sequence[str],
    manifest: str,
    parser_version: str,
) -> Mapping[str, Any]:
    view = _qualified(catalog, schema, f"{table}_current")
    key_sql = ", ".join(f'"{key}"' for key in keys)
    row_count, duplicates, uncommitted = _one_row(
        client,
        f"""-- acceptance:current-view:{table}
        SELECT
          (SELECT COUNT(*) FROM {view}),
          (SELECT COUNT(*) FROM (
             SELECT {key_sql}
             FROM {view}
             GROUP BY {key_sql}
             HAVING COUNT(*) > 1
          ) duplicate_keys),
          (SELECT COUNT(*)
             FROM {view} current_row
             LEFT JOIN (
                 SELECT DISTINCT batch_id
                 FROM {manifest}
                 WHERE status IN ('success', 'not_modified')
                   AND parser_version IN (
                       {_literal(ROLLING_FALLBACK_PARSER_VERSION)},
                       {_literal(parser_version)}
                   )
             ) committed
               ON committed.batch_id = current_row._target_batch_id
            WHERE committed.batch_id IS NULL)
        """,
        columns=3,
    )
    return {
        "passed": int(duplicates) == 0 and int(uncommitted) == 0,
        "rows": int(row_count),
        "duplicate_keys": int(duplicates),
        "uncommitted_rows": int(uncommitted),
        "natural_key": list(keys),
    }


def verify(
    client: QueryClient,
    scopes: Sequence[Scope],
    *,
    catalog: str,
    bronze_schema: str,
    parser_version: str,
    lineage: AcceptanceLineage,
) -> dict[str, Any]:
    parser_version = _validated_parser_version(parser_version)
    plan_signature = _validated_plan_signature(lineage.plan_signature)
    completed_since, completed_since_sql = _completed_since_sql(lineage.completed_since)
    manifest = _qualified(catalog, bronze_schema, "fotmob_ingest_manifest")
    checks: list[Check] = []
    checks.append(
        _run_check(
            "target_scope_completion",
            lambda: _scope_completion_check(
                client,
                scopes,
                manifest=manifest,
                parser_version=parser_version,
                plan_signature=plan_signature,
                completed_since_sql=completed_since_sql,
                runner_run_id=lineage.runner_run_id,
                catalog=catalog,
                schema=bronze_schema,
            ),
        )
    )
    checks.append(
        _run_check(
            "catalog_scope_presence",
            lambda: _catalog_scope_check(
                client,
                scopes,
                seasons_view=_qualified(
                    catalog, bronze_schema, "fotmob_competition_seasons_current"
                ),
            ),
        )
    )
    checks.append(
        _run_check(
            "latest_target_manifests",
            lambda: _latest_manifest_check(
                client,
                scopes,
                manifest=manifest,
                parser_version=parser_version,
                plan_signature=plan_signature,
                completed_since_sql=completed_since_sql,
                runner_run_id=lineage.runner_run_id,
            ),
        )
    )
    checks.append(
        _run_check(
            "direct_only_traffic",
            lambda: _proxy_check(
                client,
                scopes,
                manifest=manifest,
                parser_version=parser_version,
                plan_signature=plan_signature,
                completed_since_sql=completed_since_sql,
                runner_run_id=lineage.runner_run_id,
            ),
        )
    )
    checks.append(
        _run_check(
            "field_inventory",
            lambda: _field_inventory_check(
                client,
                scopes,
                inventory=_qualified(catalog, bronze_schema, "fotmob_field_inventory"),
                manifest=manifest,
                parser_version=parser_version,
            ),
        )
    )
    for table, keys in CURRENT_VIEW_KEYS.items():
        checks.append(
            _run_check(
                f"current_view:{table}",
                lambda table=table, keys=keys: _view_check(
                    client,
                    catalog=catalog,
                    schema=bronze_schema,
                    table=table,
                    keys=keys,
                    manifest=manifest,
                    parser_version=parser_version,
                ),
            )
        )
    report = _report("verify", scopes, checks, parser_version=parser_version)
    report["plan_signature"] = plan_signature
    report["completed_since"] = completed_since
    report["runner_run_id"] = lineage.runner_run_id
    return report


def _proxy_check(
    client: QueryClient,
    scopes: Sequence[Scope],
    *,
    manifest: str,
    parser_version: str,
    plan_signature: str,
    completed_since_sql: str,
    runner_run_id: str,
) -> Mapping[str, Any]:
    targets, attempts, direct_bytes, proxy_bytes = _one_row(
        client,
        f"""-- acceptance:direct-only
        WITH {
            _candidate_ctes(
                scopes,
                manifest=manifest,
                parser_version=parser_version,
                plan_signature=plan_signature,
                completed_since_sql=completed_since_sql,
                runner_run_id=runner_run_id,
            )
        }
        SELECT COUNT(*), COALESCE(SUM(m.attempts), 0),
               COALESCE(SUM(m.direct_bytes), 0), COALESCE(SUM(m.proxy_bytes), 0)
        FROM {manifest} m
        JOIN candidate_runs candidate ON candidate.run_id = m.run_id
        WHERE m.parser_version = {_literal(parser_version)}
          AND m.completed_at >= {completed_since_sql}
          AND m.completed_at <= candidate.completed_through
        """,
        columns=4,
    )
    if int(targets) <= 0:
        raise RuntimeError("no manifests exist for requested parser version")
    return {
        "passed": int(proxy_bytes) == 0,
        "targets": int(targets),
        "attempts": int(attempts),
        "direct_bytes": int(direct_bytes),
        "proxy_bytes": int(proxy_bytes),
    }


def _field_inventory_check(
    client: QueryClient,
    scopes: Sequence[Scope],
    *,
    inventory: str,
    manifest: str,
    parser_version: str,
) -> Mapping[str, Any]:
    rows, unknown, invalid, duplicates = _one_row(
        client,
        f"""-- acceptance:field-inventory
        WITH expected(competition_id, source_season_key) AS (
          VALUES {_scope_values(scopes)}
        ), committed AS (
          SELECT DISTINCT m.batch_id
          FROM {manifest} m
          WHERE m.parser_version IN (
              {_literal(ROLLING_FALLBACK_PARSER_VERSION)},
              {_literal(parser_version)}
          )
            AND m.status IN ('success', 'not_modified')
        ), scoped AS (
          SELECT inventory_row.*
          FROM {inventory} inventory_row
          JOIN committed
            ON committed.batch_id = inventory_row._target_batch_id
          WHERE inventory_row.competition_id IS NULL
             OR inventory_row.source_season_key IS NULL
             OR EXISTS (
                  SELECT 1
                  FROM expected e
                  WHERE e.competition_id = CAST(inventory_row.competition_id AS BIGINT)
                    AND e.source_season_key = inventory_row.source_season_key
             )
        )
        SELECT
          COUNT(*),
          (SELECT COUNT_IF(disposition = 'unknown') FROM (
             SELECT disposition,
                    ROW_NUMBER() OVER (
                        PARTITION BY target_type, competition_id,
                                     source_season_key, json_path
                        ORDER BY _ingested_at DESC, disposition DESC
                    ) latest_rn
             FROM scoped
          ) latest_paths WHERE latest_rn = 1),
          COUNT_IF(disposition NOT IN ('typed', 'raw_only', 'excluded', 'unknown')),
          (SELECT COUNT(*) FROM (
             SELECT target_type, competition_id, source_season_key,
                    json_path, disposition
             FROM scoped
             GROUP BY 1, 2, 3, 4, 5
             HAVING COUNT(*) > 1
          ) duplicate_keys)
        FROM scoped
        """,
        columns=4,
    )
    if int(rows) <= 0:
        raise RuntimeError("field inventory is empty")
    return {
        "passed": int(unknown) == 0 and int(invalid) == 0 and int(duplicates) == 0,
        "rows": int(rows),
        "unknown_paths": int(unknown),
        "invalid_dispositions": int(invalid),
        "duplicate_keys": int(duplicates),
    }


def _fetch_set(
    client: QueryClient,
    sql: str,
    *,
    width: int,
) -> set[tuple[str, ...]]:
    rows = client.query(sql)
    output: set[tuple[str, ...]] = set()
    for row in rows:
        if len(row) != width:
            raise RuntimeError(f"set query returned width {len(row)}, expected {width}")
        output.add(tuple("<NULL>" if value is None else str(value) for value in row))
    return output


def _set_comparison(
    client: QueryClient,
    *,
    name: str,
    native_sql: str,
    legacy_sql: str,
    width: int,
    minimum_overlap: float | None = None,
    allow_native_extras: bool = False,
) -> Mapping[str, Any]:
    native = _fetch_set(client, native_sql, width=width)
    legacy = _fetch_set(client, legacy_sql, width=width)
    both = native & legacy
    only_native = native - legacy
    only_legacy = legacy - native
    if not native and not legacy:
        raise RuntimeError(f"{name}: both native and legacy sets are empty")
    denominator = len(legacy)
    overlap = len(both) / denominator if denominator else (1.0 if not native else 0.0)
    if minimum_overlap is None:
        passed = not only_native and not only_legacy
    else:
        passed = overlap >= minimum_overlap and (allow_native_extras or not only_native)
    return {
        "passed": passed,
        "native": len(native),
        "legacy": len(legacy),
        "both": len(both),
        "only_native": len(only_native),
        "only_legacy": len(only_legacy),
        "legacy_coverage": overlap,
        "only_native_sample": [list(item) for item in sorted(only_native)[:25]],
        "only_legacy_sample": [list(item) for item in sorted(only_legacy)[:25]],
    }


def _scope_parity_checks(
    client: QueryClient,
    scope: Scope,
    *,
    catalog: str,
    bronze_schema: str,
) -> list[Check]:
    assert scope.legacy_league is not None and scope.legacy_season is not None

    def q(table: str) -> str:
        return _qualified(catalog, bronze_schema, table)

    cid = scope.competition_id
    season = _literal(scope.source_season_key)
    league = _literal(scope.legacy_league)
    legacy_season = scope.legacy_season
    comparisons = (
        (
            "matches",
            f"""-- parity:{scope.identity}:matches:native
            SELECT DISTINCT CAST(match_id AS VARCHAR)
            FROM {q("fotmob_matches_current")}
            WHERE CAST(competition_id AS BIGINT) = {cid}
              AND source_season_key = {season}
            """,
            f"""-- parity:{scope.identity}:matches:legacy
            SELECT DISTINCT CAST(match_id AS VARCHAR)
            FROM {q("fotmob_schedule")}
            WHERE league = {league} AND season = {legacy_season}
            """,
            1,
            None,
        ),
        (
            "payloads",
            f"""-- parity:{scope.identity}:payloads:native
            SELECT DISTINCT CAST(match_id AS VARCHAR)
            FROM {q("fotmob_match_payloads_current")}
            WHERE CAST(competition_id AS BIGINT) = {cid}
              AND source_season_key = {season}
            """,
            f"""-- parity:{scope.identity}:payloads:legacy
            SELECT DISTINCT CAST(match_id AS VARCHAR)
            FROM {q("fotmob_match_details")}
            WHERE league = {league} AND season = {legacy_season}
            """,
            1,
            None,
        ),
        (
            "standings",
            f"""-- parity:{scope.identity}:standings:native
            SELECT DISTINCT CAST(team_id AS VARCHAR)
            FROM {q("fotmob_standings_current")}
            WHERE CAST(competition_id AS BIGINT) = {cid}
              AND source_season_key = {season}
              AND LOWER(COALESCE(table_type, 'all')) = 'all'
            """,
            f"""-- parity:{scope.identity}:standings:legacy
            SELECT DISTINCT CAST(team_id AS VARCHAR)
            FROM {q("fotmob_team_stats")}
            WHERE league = {league} AND season = {legacy_season}
            """,
            1,
            None,
        ),
        (
            "roster",
            f"""-- parity:{scope.identity}:roster:native
            SELECT DISTINCT CAST(s.team_id AS VARCHAR), CAST(s.member_id AS VARCHAR)
            FROM {q("fotmob_squad_snapshots_current")} s
            JOIN {q("fotmob_season_teams_current")} t
              ON CAST(t.team_id AS VARCHAR) = CAST(s.team_id AS VARCHAR)
            WHERE CAST(t.competition_id AS BIGINT) = {cid}
              AND t.source_season_key = {season}
              AND s.member_type = 'player'
            """,
            f"""-- parity:{scope.identity}:roster:legacy
            SELECT DISTINCT CAST(team_id AS VARCHAR), CAST(player_id AS VARCHAR)
            FROM {q("fotmob_team_squad")}
            WHERE league = {league} AND season = {legacy_season}
            """,
            2,
            0.90,
        ),
    )
    checks: list[Check] = []
    for name, native_sql, legacy_sql, width, minimum_overlap in comparisons:
        checks.append(
            _run_check(
                f"{scope.identity}:{name}",
                lambda name=name, native_sql=native_sql, legacy_sql=legacy_sql, width=width, minimum_overlap=minimum_overlap: (
                    _set_comparison(
                        client,
                        name=name,
                        native_sql=native_sql,
                        legacy_sql=legacy_sql,
                        width=width,
                        minimum_overlap=minimum_overlap,
                        allow_native_extras=minimum_overlap is not None,
                    )
                ),
            )
        )
    return checks


def _transfer_preservation_check(
    client: QueryClient,
    *,
    catalog: str,
    bronze_schema: str,
    silver_schema: str,
) -> Mapping[str, Any]:
    def q_bronze(table: str) -> str:
        return _qualified(catalog, bronze_schema, table)

    legacy_sql = f"""-- parity:transfers:legacy
        SELECT DISTINCT CAST(player_id AS VARCHAR),
               CAST(from_club_id AS VARCHAR), CAST(to_club_id AS VARCHAR),
               CAST(TRY_CAST(SUBSTR(transfer_date, 1, 10) AS DATE) AS VARCHAR),
               CAST(league AS VARCHAR)
        FROM {q_bronze("fotmob_transfers")}
        WHERE player_id IS NOT NULL
          AND TRY_CAST(SUBSTR(transfer_date, 1, 10) AS DATE) IS NOT NULL
    """
    silver_sql = f"""-- parity:transfers:silver
        SELECT DISTINCT CAST(player_id AS VARCHAR),
               CAST(from_club_id AS VARCHAR), CAST(to_club_id AS VARCHAR),
               CAST(transfer_date AS VARCHAR), CAST(league AS VARCHAR)
        FROM {_qualified(catalog, silver_schema, "fotmob_transfers")}
        WHERE player_id IS NOT NULL AND transfer_date IS NOT NULL
    """
    return _set_comparison(
        client,
        name="legacy transfer identity preservation",
        native_sql=silver_sql,
        legacy_sql=legacy_sql,
        width=5,
        minimum_overlap=1.0,
        allow_native_extras=True,
    )


def parity(
    client: QueryClient,
    scopes: Sequence[Scope],
    *,
    catalog: str,
    bronze_schema: str,
    silver_schema: str,
    parser_version: str,
    lineage: AcceptanceLineage,
) -> dict[str, Any]:
    parser_version = _validated_parser_version(parser_version)
    plan_signature = _validated_plan_signature(lineage.plan_signature)
    completed_since, completed_since_sql = _completed_since_sql(lineage.completed_since)
    manifest = _qualified(catalog, bronze_schema, "fotmob_ingest_manifest")
    checks: list[Check] = [
        _run_check(
            "target_scope_completion",
            lambda: _scope_completion_check(
                client,
                scopes,
                manifest=manifest,
                parser_version=parser_version,
                plan_signature=plan_signature,
                completed_since_sql=completed_since_sql,
                runner_run_id=lineage.runner_run_id,
                catalog=catalog,
                schema=bronze_schema,
            ),
        )
    ]
    for scope in scopes:
        checks.extend(
            _scope_parity_checks(
                client,
                scope,
                catalog=catalog,
                bronze_schema=bronze_schema,
            )
        )
    checks.append(
        _run_check(
            "silver_transfer_legacy_identity_preservation",
            lambda: _transfer_preservation_check(
                client,
                catalog=catalog,
                bronze_schema=bronze_schema,
                silver_schema=silver_schema,
            ),
        )
    )
    report = _report("parity", scopes, checks, parser_version=parser_version)
    report["plan_signature"] = plan_signature
    report["completed_since"] = completed_since
    report["runner_run_id"] = lineage.runner_run_id
    return report


def _report(
    kind: str,
    scopes: Sequence[Scope],
    checks: Sequence[Check],
    *,
    parser_version: str,
) -> dict[str, Any]:
    passed = bool(checks) and all(check.passed for check in checks)
    return {
        "schema_version": "fotmob-acceptance-v1",
        "kind": kind,
        "generated_at": _now(),
        "git_sha": _git_sha(),
        "parser_version": parser_version,
        "scopes": [asdict(scope) for scope in scopes],
        "passed": passed,
        "summary": {
            "checks": len(checks),
            "passed": sum(check.passed for check in checks),
            "failed": sum(not check.passed for check in checks),
        },
        "checks": [asdict(check) for check in checks],
    }


def connect_from_env(*, catalog: str, schema: str) -> QueryClient:
    try:
        import trino
        from trino.auth import BasicAuthentication
    except ImportError as exc:  # pragma: no cover - production dependency
        raise RuntimeError("the 'trino' package is required") from exc
    host = os.environ.get("TRINO_HOST", "").strip()
    if not host:
        raise RuntimeError("TRINO_HOST is required")
    port = int(os.environ.get("TRINO_PORT", "8443"))
    user = os.environ.get("TRINO_USER", "airflow").strip()
    password = os.environ.get("TRINO_PASSWORD", "")
    http_scheme = os.environ.get("TRINO_HTTP_SCHEME", "https").strip()
    kwargs: dict[str, Any] = {
        "host": host,
        "port": port,
        "user": user,
        "catalog": catalog,
        "schema": schema,
        "http_scheme": http_scheme,
        "verify": os.environ.get("TRINO_TLS_VERIFY", "true").lower()
        not in {"0", "false", "no"},
    }
    if password:
        kwargs["auth"] = BasicAuthentication(user, password)
    return TrinoQueryClient(trino.dbapi.connect(**kwargs))


def build_parser() -> argparse.ArgumentParser:
    default_compose = (
        Path(__file__).resolve().parents[1] / "deploy/fotmob/airflow.compose.yaml"
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("verify", "parity"))
    parser.add_argument(
        "--scopes",
        type=Path,
        required=True,
        help=(
            "Verify accepts JSON or ID=season; parity requires JSON with the "
            "legacy identity. Both commands enforce their reviewed #930 set."
        ),
    )
    parser.add_argument(
        "--scope-sha256",
        required=True,
        help="Reviewed byte-exact SHA-256 of --scopes",
    )
    parser.add_argument(
        "--lifecycle-report",
        type=Path,
        required=True,
        help=(
            "Exact passed issue-930 replay/backfill report; plan, time and "
            "runner identities are derived from it"
        ),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--catalog", default="iceberg")
    parser.add_argument("--bronze-schema", default="bronze")
    parser.add_argument("--silver-schema", default="silver")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument(
        "--trino-env-file",
        type=Path,
        required=True,
        help="Host-reachable Trino endpoint; ambient TRINO_* values are ignored",
    )
    parser.add_argument("--deployment-report", type=Path, required=True)
    parser.add_argument("--compose-file", type=Path, default=default_compose)
    parser.add_argument("--project", default="fotmob-airflow")
    parser.add_argument(
        "--parser-version",
        default=PARSER_VERSION,
        help=f"Pinned production parser version (must be {PARSER_VERSION})",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    client_factory: Callable[..., QueryClient] = connect_from_env,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    runtime_binder: Callable[..., Mapping[str, Any]] = (
        runtime_binding.bind_admitted_trino
    ),
    publication_reader: Callable[..., Mapping[str, Any]] = (
        read_live_publication_state
    ),
) -> int:
    args = build_parser().parse_args(argv)
    lifecycle_evidence: Mapping[str, Any] | None = None
    try:
        _validated_parser_version(args.parser_version)
        scope_bytes = args.scopes.read_bytes()
        actual_scope_sha256 = hashlib.sha256(scope_bytes).hexdigest()
        if not re.fullmatch(r"[0-9a-fA-F]{64}", args.scope_sha256):
            raise ValueError("--scope-sha256 must contain exactly 64 hex chars")
        if actual_scope_sha256 != args.scope_sha256.lower():
            raise ValueError(
                "scope file SHA-256 differs from the reviewed acceptance scope"
            )
        scopes = load_scopes(
            args.scopes,
            parity=args.command == "parity",
            content=scope_bytes,
        )
        scope_contract = validate_approved_scope_contract(args.command, scopes)
        if actual_scope_sha256 != scope_contract["scope_file_sha256"]:
            raise ValueError(
                "scope file is identity-equivalent but not the byte-exact "
                f"reviewed {args.command} artifact"
            )
        context = runtime_binding.load_deployment_context(
            args.deployment_report,
            project=args.project,
            compose_file=args.compose_file,
        )
        lineage = load_lifecycle_report(
            args.lifecycle_report,
            deployment_context=context,
            deployment_report=args.deployment_report,
            project=args.project,
        )
        lifecycle_evidence = lineage.summary()
        deployment_binding = runtime_binder(
            context,
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
            require_running=True,
            run=run,
        )
        live_publication_before = validate_live_publication_state(
            publication_reader(
                context,
                generation_id=lineage.generation_id,
                project=args.project,
                compose_file=args.compose_file,
                env_file=args.env_file,
                run=run,
            ),
            lineage,
        )
        runtime_binding.load_host_trino_environment(args.trino_env_file)
        client = client_factory(catalog=args.catalog, schema=args.bronze_schema)
        try:
            marker_before = runtime_binding.validate_data_plane_marker(client, context)
            if args.command == "verify":
                report = verify(
                    client,
                    scopes,
                    catalog=args.catalog,
                    bronze_schema=args.bronze_schema,
                    parser_version=args.parser_version,
                    lineage=lineage,
                )
            else:
                report = parity(
                    client,
                    scopes,
                    catalog=args.catalog,
                    bronze_schema=args.bronze_schema,
                    silver_schema=args.silver_schema,
                    parser_version=args.parser_version,
                    lineage=lineage,
                )
            marker_after = runtime_binding.validate_data_plane_marker(client, context)
        finally:
            client.close()
        live_publication_after = validate_live_publication_state(
            publication_reader(
                context,
                generation_id=lineage.generation_id,
                project=args.project,
                compose_file=args.compose_file,
                env_file=args.env_file,
                run=run,
            ),
            lineage,
        )
        final_deployment_binding = runtime_binder(
            context,
            project=args.project,
            compose_file=args.compose_file,
            env_file=args.env_file,
            require_running=True,
            run=run,
        )
        report["scope_file_sha256"] = actual_scope_sha256
        report["scope_contract"] = scope_contract
        report["expected_scope_count"] = scope_contract["expected_scope_count"]
        report["git_sha"] = context["git_sha"]
        report["lifecycle_report"] = {
            **dict(lifecycle_evidence),
            "live_publication_before": dict(live_publication_before),
            "live_publication_after": dict(live_publication_after),
        }
        report["deployment_binding"] = {
            "before": deployment_binding,
            "after": final_deployment_binding,
            "data_plane_before": marker_before,
            "data_plane_after": marker_after,
        }
    except Exception as exc:
        report = {
            "schema_version": "fotmob-acceptance-v1",
            "kind": args.command,
            "generated_at": _now(),
            "git_sha": _git_sha(),
            "passed": False,
            "fatal_error": f"{type(exc).__name__}: {exc}",
            "checks": [],
        }
        if lifecycle_evidence is not None:
            report["lifecycle_report"] = dict(lifecycle_evidence)
    _atomic_json(args.output, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report.get("passed") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
