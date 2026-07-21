import json
import hashlib
import subprocess
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts import fotmob_acceptance as mod
from scrapers.fotmob.repository import (
    CURRENT_VIEW_SPECS,
    LEGACY_PARSER_VERSION,
    PARSER_VERSION,
)
from tests.unit.scripts.fotmob_runtime_fixture import (
    isolated_runtime_proof,
    shared_handoff_proof,
)


PLAN_SIGNATURE = "fmplan1-" + "a" * 64
COMPLETED_SINCE = "2026-07-20T00:00:00Z"
RUNNER_RUN_ID = "12345678-1234-5678-9234-123456789abc"
FAKE_LEADERBOARD_IDENTITY = ("player", "Goals", "/api/leaders", 0)


def runtime_options(tmp_path):
    compose = tmp_path / "compose.yaml"
    env_file = tmp_path / "compose.env"
    trino_env = tmp_path / "host-trino.env"
    deployment = tmp_path / "deployment.json"
    compose.write_text("services: {}\n")
    env_file.write_text("FOTMOB_AIRFLOW_DB_PASSWORD=safe\n")
    trino_env.write_text("TRINO_HOST=127.0.0.1\nTRINO_PASSWORD=host-secret\n")
    deployment_id = "f" * 32
    scheduler_id = "1" * 64
    image_id = "sha256:" + "d" * 64
    shared_control = {
        "same_shared_database": True,
        "migrations": {
            "status": "passed",
            "versions": [1],
            "checksum_verified": True,
        },
    }
    deployment.write_text(
        json.dumps(
            {
                "schema_version": "fotmob-deploy-v2",
                "passed": True,
                "activation_state": "kept_paused",
                "kept_paused": True,
                "paused": [
                    "dag_ingest_fotmob",
                    "dag_transform_fotmob_silver",
                    "dag_trigger_fotmob_daily",
                ],
                "unpaused": [],
                "generated_at": "2026-07-21T10:00:00Z",
                "project": "fotmob-airflow",
                "compose_file": str(compose.resolve()),
                "release_root": str(tmp_path.resolve()),
                "evidence_dir": str((tmp_path / "evidence").resolve()),
                "container_report_path": ("/opt/airflow/logs/fotmob/deployment.json"),
                "shared_container_report_path": (
                    "/opt/airflow/fotmob-admission/deployment.json"
                ),
                "dagbag_root": str((tmp_path / "dagbag").resolve()),
                "git_sha": "a" * 40,
                "image": "registry/image@sha256:" + "b" * 64,
                "postgres_image": "postgres@sha256:" + "c" * 64,
                "resolved_image_id": image_id,
                "resolved_postgres_image_id": "sha256:" + "e" * 64,
                "deployment_id": deployment_id,
                "scheduler_container_id": scheduler_id,
                "metadb_container_id": "2" * 64,
                "data_plane_marker": {
                    "table": "iceberg.bronze.fotmob_runtime_deployments",
                    "deployment_id": deployment_id,
                    "git_sha": "a" * 40,
                    "scheduler_container_id": scheduler_id,
                    "scheduler_image_id": image_id,
                },
                "delivery_credentials": {
                    "telegram_bot_token_configured": True,
                    "telegram_chat_id_configured": True,
                },
                "isolated_runtime_sha256": isolated_runtime_proof(tmp_path),
                "control_database": {
                    "same_runtime_configuration": True,
                    "shared": shared_control,
                    "isolated": {
                        "migrations": {
                            "status": "passed",
                            "versions": [1],
                            "checksum_verified": True,
                        }
                    },
                },
                "shared_handoff_initial": shared_handoff_proof(
                    shared_control, release_root=tmp_path
                ),
                "shared_handoff_final": shared_handoff_proof(
                    shared_control, release_root=tmp_path
                ),
            }
        )
    )
    return [
        "--env-file",
        str(env_file),
        "--trino-env-file",
        str(trino_env),
        "--deployment-report",
        str(deployment),
        "--compose-file",
        str(compose),
    ]


def fake_lineage(**overrides):
    values = {
        "report_path": "/evidence/replay.json",
        "report_sha256": "b" * 64,
        "command": "run",
        "mode": "replay",
        "publication_attempt": 1,
        "deployment_id": "f" * 32,
        "git_sha": "a" * 40,
        "generation_id": RUNNER_RUN_ID,
        "runner_run_id": RUNNER_RUN_ID,
        "ingest_run_id": "issue930_replay_a1__" + RUNNER_RUN_ID.replace("-", ""),
        "silver_run_id": f"fotmob_silver__{RUNNER_RUN_ID}",
        "plan_signature": PLAN_SIGNATURE,
        "completed_since": COMPLETED_SINCE,
        "scope_artifact": str(mod.APPROVED_SCOPE_ARTIFACT.resolve()),
        "scope_sha256": mod.APPROVED_SCOPE_ARTIFACT_SHA256,
        "scope_count": mod.APPROVED_SCOPE_COUNT,
        "entities": mod.ISSUE_930_SCOPE_ENTITIES,
        "candidate_digest": "d" * 64,
        "candidate_transform_task_ids": ("transform_a", "transform_b"),
        "publication_binding": {
            "schema": mod.PUBLICATION_SCHEMA,
            "source": "fotmob",
            "owner": "isolated",
            "data_interval_start": "2026-07-21T00:00:00.000000+00:00",
            "data_interval_end": "2026-07-21T00:00:01.000000+00:00",
            "runtime_fingerprint": "a" * 40,
        },
    }
    values.update(overrides)
    return mod.AcceptanceLineage(**values)


def full_live_candidate(generation_id, task_ids=("transform_a", "transform_b")):
    evidence = {
        "schema": mod.PUBLICATION_SCHEMA,
        "generation_id": generation_id,
        "transform_task_ids": list(task_ids),
        "transform_results": {
            task_id: {
                "status": "success",
                "table": f"iceberg.silver.{task_id}",
                "rows": index + 10,
            }
            for index, task_id in enumerate(task_ids)
        },
        "row_count_gate": {
            "status": "success",
            "warnings": [],
            "details": {task_id: index + 10 for index, task_id in enumerate(task_ids)},
            "total_rows": 21,
        },
        "quality_gate": {
            "passed": 4,
            "total": 5,
            "errors": [],
            "warnings": ["freshness_warning"],
        },
    }
    evidence["digest"] = hashlib.sha256(
        json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return evidence


def write_lifecycle_report(tmp_path, options, *, mutate=None):
    deployment_path = Path(options[options.index("--deployment-report") + 1])
    deployment = json.loads(deployment_path.read_text(encoding="utf-8"))
    deployed_at = datetime.fromisoformat(
        deployment["generated_at"].replace("Z", "+00:00")
    )
    mode = "replay"
    attempt = 1
    interval_start = deployed_at + timedelta(days=1, seconds=1)
    interval_end = interval_start + timedelta(seconds=1)
    binding = {
        "schema": mod.PUBLICATION_SCHEMA,
        "source": "fotmob",
        "owner": "isolated",
        "data_interval_start": interval_start.isoformat(timespec="microseconds"),
        "data_interval_end": interval_end.isoformat(timespec="microseconds"),
        "runtime_fingerprint": deployment["git_sha"],
    }
    generation_id = mod._publication_generation_id(binding)
    compact = generation_id.replace("-", "")
    ingest_run_id = f"issue930_{mode}_a{attempt}__{compact}"
    silver_run_id = f"fotmob_silver__{generation_id}"
    live_candidate = full_live_candidate(generation_id)
    candidate = {
        "generation_id": generation_id,
        "digest": live_candidate["digest"],
        "transform_task_ids": live_candidate["transform_task_ids"],
    }
    report = {
        "schema_version": mod.LIFECYCLE_SCHEMA_VERSION,
        "generated_at": (deployed_at + timedelta(hours=2))
        .isoformat()
        .replace("+00:00", "Z"),
        "passed": True,
        "command": "run",
        "phase": "abandoned",
        "recovery_required": False,
        "mode": mode,
        "publication_attempt": attempt,
        "project": "fotmob-airflow",
        "deployment_report": str(deployment_path.resolve()),
        "deployment_id": deployment["deployment_id"],
        "git_sha": deployment["git_sha"],
        "scope": {
            "name": "issue-930-verify",
            "artifact": str(mod.APPROVED_SCOPE_ARTIFACT.resolve()),
            "sha256": mod.APPROVED_SCOPE_ARTIFACT_SHA256,
            "count": mod.APPROVED_SCOPE_COUNT,
        },
        "entities": list(mod.ISSUE_930_SCOPE_ENTITIES),
        "publication": {
            "generation_id": generation_id,
            "binding": binding,
        },
        "runs": {
            "ingest_dag_id": mod.INGEST_DAG_ID,
            "ingest_run_id": ingest_run_id,
            "silver_dag_id": mod.SILVER_DAG_ID,
            "silver_run_id": silver_run_id,
            "native_runner_run_id": generation_id,
        },
        "publication_action": "abandon_unclaimed_candidate",
        "ingest_terminal": {
            "dag_id": mod.INGEST_DAG_ID,
            "run_id": ingest_run_id,
            "state": "success",
            "start_date": (deployed_at + timedelta(minutes=5))
            .astimezone(timezone.utc)
            .isoformat(),
        },
        "silver_terminal": {
            "dag_id": mod.SILVER_DAG_ID,
            "run_id": silver_run_id,
            "state": "success",
        },
        "validation": {
            "run_id": generation_id,
            "mode": mode,
            "scope_count": mod.APPROVED_SCOPE_COUNT,
            "scope_sha256": mod.APPROVED_SCOPE_ARTIFACT_SHA256,
            "entities": sorted(mod.ISSUE_930_SCOPE_ENTITIES),
            "plan_signature": PLAN_SIGNATURE,
        },
        "plan_signature": PLAN_SIGNATURE,
        "candidate": candidate,
        "publication_state": {
            "generation_id": generation_id,
            "status": "succeeded",
            "phase": "abandoned",
            "active": False,
            "released": True,
            "published": False,
            "candidate": candidate,
        },
    }
    if mutate is not None:
        mutate(report)
    path = tmp_path / "replay.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    return path, report


def live_publication_reader(report):
    def reader(_context, **kwargs):
        assert kwargs["generation_id"] == report["publication"]["generation_id"]
        return {
            "generation_id": report["publication"]["generation_id"],
            "source": "fotmob",
            "status": "succeeded",
            "phase": "abandoned",
            "binding": report["publication"]["binding"],
            "candidate": full_live_candidate(
                report["publication"]["generation_id"],
                report["candidate"]["transform_task_ids"],
            ),
            "consumer": None,
            "owner_dag_id": mod.PUBLICATION_OWNER_DAG_ID,
            "active": False,
            "lock_active": False,
            "released_at": "2026-07-21T11:00:00Z",
        }

    return reader


def fake_runtime_binding(*_args, **_kwargs):
    return {"passed": True, "trino": {"credential_bound": True}}


def fake_completion_payload():
    counts = {key: 1 for key in mod.EXPECTED_SCOPE_COUNT_KEYS}
    scope_coverage = {
        "scope_entities": sorted(mod.REQUIRED_SCOPE_ENTITIES),
        "leaderboard_identity_hash": mod._identity_hash({FAKE_LEADERBOARD_IDENTITY}),
        "match_identity_hash": mod._identity_hash({"101"}),
        "team_identity_hash": mod._identity_hash({"1"}),
        "player_identity_hash": mod._identity_hash({"10"}),
    }
    capabilities = {
        "plan_signature": PLAN_SIGNATURE,
        "coverage_hash": mod._scope_coverage_hash(scope_coverage, counts),
        "coverage": scope_coverage,
    }
    return counts, capabilities


def fake_scope_coverage_rows(sql, scope_identities=((47, "2025/2026"),)):
    if "acceptance:scope-coverage:leaderboards" in sql:
        return [
            (competition_id, season_key, *FAKE_LEADERBOARD_IDENTITY)
            for competition_id, season_key in scope_identities
        ]
    if "acceptance:scope-coverage:matches" in sql:
        return [
            (competition_id, season_key, "101")
            for competition_id, season_key in scope_identities
        ]
    if "acceptance:scope-coverage:teams" in sql:
        return [
            (competition_id, season_key, "1")
            for competition_id, season_key in scope_identities
        ]
    if "acceptance:scope-coverage:players" in sql:
        return [
            (competition_id, season_key, "10")
            for competition_id, season_key in scope_identities
        ]
    return None


class FakeAcceptanceClient:
    def __init__(
        self,
        *,
        fail_marker=None,
        runner_run_id=RUNNER_RUN_ID,
        scope_identities=((47, "2025/2026"),),
    ):
        self.fail_marker = fail_marker
        self.runner_run_id = runner_run_id
        self.scope_identities = tuple(scope_identities)
        self.closed = False
        self.sql = []

    def query(self, sql):
        self.sql.append(sql)
        if self.fail_marker and self.fail_marker in sql:
            raise RuntimeError("synthetic SQL failure")
        if "runtime-binding:data-plane-marker" in sql:
            return [(1,)]
        if "acceptance:scope-completion" in sql:
            counts, capabilities = fake_completion_payload()
            return [
                (
                    competition_id,
                    season_key,
                    self.runner_run_id,
                    datetime(2026, 7, 21),
                    json.dumps(counts),
                    json.dumps(capabilities),
                )
                for competition_id, season_key in self.scope_identities
            ]
        coverage_rows = fake_scope_coverage_rows(sql, self.scope_identities)
        if coverage_rows is not None:
            return coverage_rows
        if "acceptance:catalog-scopes" in sql:
            return [
                (competition_id, season_key, True)
                for competition_id, season_key in self.scope_identities
            ]
        if "acceptance:latest-manifests" in sql:
            return [("league_season", "scope-47", "success", None, "47", "2025/2026")]
        if "acceptance:direct-only" in sql:
            return [(12, 30, 1000, 0)]
        if "acceptance:field-inventory" in sql:
            return [(100, 0, 0, 0)]
        if "acceptance:current-view:" in sql:
            return [(10, 0, 0)]
        raise AssertionError(f"unexpected SQL: {sql}")

    def close(self):
        self.closed = True


class FakeParityClient:
    def __init__(
        self,
        *,
        runner_run_id=RUNNER_RUN_ID,
        scope_identities=((47, "2025/2026"),),
    ):
        self.runner_run_id = runner_run_id
        self.scope_identities = tuple(scope_identities)
        self.closed = False

    def query(self, sql):
        if "runtime-binding:data-plane-marker" in sql:
            return [(1,)]
        if "acceptance:scope-completion" in sql:
            counts, capabilities = fake_completion_payload()
            return [
                (
                    competition_id,
                    season_key,
                    self.runner_run_id,
                    datetime(2026, 7, 21),
                    json.dumps(counts),
                    json.dumps(capabilities),
                )
                for competition_id, season_key in self.scope_identities
            ]
        coverage_rows = fake_scope_coverage_rows(sql, self.scope_identities)
        if coverage_rows is not None:
            return coverage_rows
        if ":roster:native" in sql:
            return [("1", str(index)) for index in range(1, 10)] + [
                ("1", "native-extra")
            ]
        if ":roster:legacy" in sql:
            return [("1", str(index)) for index in range(1, 11)]
        if "parity:transfers:silver" in sql:
            return [
                ("1", "2", "3", "2026-07-01", "ENG-Premier League"),
                ("4", "5", "6", "2026-07-02", "ENG-Premier League"),
            ]
        if "parity:transfers:legacy" in sql:
            return [("1", "2", "3", "2026-07-01", "ENG-Premier League")]
        if ":native" in sql or ":legacy" in sql:
            return [("101",), ("102",)]
        raise AssertionError(f"unexpected SQL: {sql}")

    def close(self):
        self.closed = True


def scope():
    return mod.Scope(
        competition_id=47,
        source_season_key="2025/2026",
        legacy_league="ENG-Premier League",
        legacy_season=2025,
    )


def test_acceptance_contract_tracks_repository_view_keys_and_parser_version():
    expected = {
        table: tuple(dict.fromkeys(keys))
        for table, (_, keys) in CURRENT_VIEW_SPECS.items()
    }
    assert mod.CURRENT_VIEW_KEYS == expected
    assert mod.PARSER_VERSION == PARSER_VERSION
    assert mod.ROLLING_FALLBACK_PARSER_VERSION == LEGACY_PARSER_VERSION


def test_load_scopes_rejects_duplicate_native_identity(tmp_path):
    path = tmp_path / "scopes.json"
    path.write_text(
        json.dumps(
            [
                {"competition_id": 47, "source_season_key": "2025/2026"},
                {"competition_id": 47, "source_season_key": "2025/2026"},
            ]
        )
    )
    try:
        mod.load_scopes(path)
    except ValueError as exc:
        assert "duplicate scope" in str(exc)
    else:
        raise AssertionError("duplicate scope was accepted")


def test_approved_verify_scope_artifact_is_exact_and_runnable():
    approved = mod.load_scopes(mod.APPROVED_SCOPE_ARTIFACT)

    assert len(approved) == mod.APPROVED_SCOPE_COUNT
    contract = mod.validate_approved_scope_contract("verify", approved)
    assert contract["name"] == "issue-930-verify"
    assert (
        contract["identity_artifact_sha256"]
        == hashlib.sha256(mod.APPROVED_SCOPE_ARTIFACT.read_bytes()).hexdigest()
    )


def test_versioned_parity_scope_is_the_exact_approved_five():
    path = mod.APPROVED_PARITY_SCOPE_ARTIFACT
    scopes = mod.load_scopes(path, parity=True)

    contract = mod.validate_approved_scope_contract("parity", scopes)
    assert contract["name"] == "issue-930-parity"
    assert contract["expected_scope_count"] == 5
    assert (
        contract["scope_file_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    )


def test_trino_env_loader_ignores_unrelated_secrets_and_preserves_environment(
    tmp_path, monkeypatch
):
    path = tmp_path / "fotmob.env"
    path.write_text(
        "TRINO_HOST=trino.internal\n"
        "TRINO_PASSWORD='file-password'\n"
        "S3_SECRET_KEY=must-not-load\n"
    )
    monkeypatch.setenv("TRINO_PASSWORD", "environment-password")
    monkeypatch.delenv("TRINO_HOST", raising=False)
    monkeypatch.delenv("S3_SECRET_KEY", raising=False)
    mod.load_trino_env(path)
    assert __import__("os").environ["TRINO_HOST"] == "trino.internal"
    assert __import__("os").environ["TRINO_PASSWORD"] == "environment-password"
    assert "S3_SECRET_KEY" not in __import__("os").environ


def test_host_trino_binding_overrides_ambient_docker_dns(tmp_path, monkeypatch):
    path = tmp_path / "host.env"
    path.write_text("TRINO_HOST=127.0.0.1\nTRINO_PASSWORD=host-secret\n")
    monkeypatch.setenv("TRINO_HOST", "trino")
    monkeypatch.setenv("TRINO_PASSWORD", "ambient-secret")

    mod.runtime_binding.load_host_trino_environment(path)

    assert __import__("os").environ["TRINO_HOST"] == "127.0.0.1"
    assert __import__("os").environ["TRINO_PASSWORD"] == "host-secret"


def test_data_plane_binding_rejects_absent_exact_deployment_marker(tmp_path):
    options = runtime_options(tmp_path)
    deployment_path = Path(options[options.index("--deployment-report") + 1])
    compose_path = Path(options[options.index("--compose-file") + 1])
    context = mod.runtime_binding.load_deployment_context(
        deployment_path,
        project="fotmob-airflow",
        compose_file=compose_path,
    )

    class WrongPlane:
        def query(self, _sql):
            return [(0,)]

    try:
        mod.runtime_binding.validate_data_plane_marker(WrongPlane(), context)
    except mod.runtime_binding.RuntimeBindingError as exc:
        assert "does not contain the exact deployment marker" in str(exc)
    else:
        raise AssertionError("a different Trino data plane was accepted")


def test_runtime_binding_rejects_pending_trigger_activation(tmp_path):
    options = runtime_options(tmp_path)
    deployment_path = Path(options[options.index("--deployment-report") + 1])
    compose_path = Path(options[options.index("--compose-file") + 1])
    payload = json.loads(deployment_path.read_text(encoding="utf-8"))
    payload.update(
        {
            "activation_state": "committed_pending_trigger",
            "paused": ["dag_trigger_fotmob_daily"],
            "unpaused": [
                "dag_ingest_fotmob",
                "dag_transform_fotmob_silver",
            ],
        }
    )
    deployment_path.write_text(json.dumps(payload), encoding="utf-8")

    try:
        mod.runtime_binding.load_deployment_context(
            deployment_path,
            project="fotmob-airflow",
            compose_file=compose_path,
        )
    except mod.runtime_binding.RuntimeBindingError as exc:
        assert "activation is incomplete" in str(exc)
    else:
        raise AssertionError("pending trigger activation was accepted as complete")


def test_lifecycle_report_derives_all_acceptance_lineage(tmp_path):
    options = runtime_options(tmp_path)
    lifecycle_path, report = write_lifecycle_report(tmp_path, options)
    deployment_path = Path(options[options.index("--deployment-report") + 1])
    compose_path = Path(options[options.index("--compose-file") + 1])
    context = mod.runtime_binding.load_deployment_context(
        deployment_path,
        project="fotmob-airflow",
        compose_file=compose_path,
    )

    lineage = mod.load_lifecycle_report(
        lifecycle_path,
        deployment_context=context,
        deployment_report=deployment_path,
        project="fotmob-airflow",
    )

    generation_id = report["publication"]["generation_id"]
    assert lineage.plan_signature == PLAN_SIGNATURE
    assert lineage.runner_run_id == generation_id
    assert lineage.generation_id == generation_id
    assert lineage.completed_since == "2026-07-21T10:05:00Z"
    assert lineage.scope_count == 158
    assert lineage.entities == mod.ISSUE_930_SCOPE_ENTITIES
    assert (
        lineage.report_sha256 == hashlib.sha256(lifecycle_path.read_bytes()).hexdigest()
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda report: report.update(passed=False),
        lambda report: report.update(deployment_id="0" * 32),
        lambda report: report["scope"].update(count=157),
        lambda report: report.update(
            entities=[*mod.ISSUE_930_SCOPE_ENTITIES, "transfers"]
        ),
        lambda report: report["publication"].update(
            generation_id="12345678-1234-5678-9234-123456789abc"
        ),
        lambda report: report["runs"].update(native_runner_run_id="old-run"),
        lambda report: report["validation"].update(
            plan_signature="fmplan1-" + "0" * 64
        ),
        lambda report: report.update(
            candidate={
                **report["candidate"],
                "generation_id": "12345678-1234-5678-9234-123456789abc",
            }
        ),
        lambda report: report["publication_state"].update(released=False),
        lambda report: report["ingest_terminal"].pop("start_date"),
    ],
)
def test_lifecycle_report_rejects_malformed_or_divergent_fields(tmp_path, mutate):
    options = runtime_options(tmp_path)
    lifecycle_path, _ = write_lifecycle_report(tmp_path, options, mutate=mutate)
    deployment_path = Path(options[options.index("--deployment-report") + 1])
    compose_path = Path(options[options.index("--compose-file") + 1])
    context = mod.runtime_binding.load_deployment_context(
        deployment_path,
        project="fotmob-airflow",
        compose_file=compose_path,
    )

    with pytest.raises(ValueError):
        mod.load_lifecycle_report(
            lifecycle_path,
            deployment_context=context,
            deployment_report=deployment_path,
            project="fotmob-airflow",
        )


def test_live_publication_must_match_report_candidate_and_release(tmp_path):
    options = runtime_options(tmp_path)
    lifecycle_path, report = write_lifecycle_report(tmp_path, options)
    deployment_path = Path(options[options.index("--deployment-report") + 1])
    compose_path = Path(options[options.index("--compose-file") + 1])
    context = mod.runtime_binding.load_deployment_context(
        deployment_path,
        project="fotmob-airflow",
        compose_file=compose_path,
    )
    lineage = mod.load_lifecycle_report(
        lifecycle_path,
        deployment_context=context,
        deployment_report=deployment_path,
        project="fotmob-airflow",
    )
    state = dict(
        live_publication_reader(report)(context, generation_id=lineage.generation_id)
    )
    evidence = mod.validate_live_publication_state(state, lineage)
    assert evidence["candidate"] == {
        "schema": mod.PUBLICATION_SCHEMA,
        "generation_id": lineage.generation_id,
        "digest": lineage.candidate_digest,
        "transform_task_ids": ["transform_a", "transform_b"],
        "transform_count": 2,
        "row_count_status": "success",
        "row_count_total": 21,
        "quality_passed": 4,
        "quality_total": 5,
    }
    state["candidate"] = {**state["candidate"], "digest": "0" * 64}

    with pytest.raises(ValueError, match="differs"):
        mod.validate_live_publication_state(state, lineage)


def test_live_candidate_rejects_extra_missing_tamper_and_bad_semantics():
    lineage = fake_lineage()
    candidate = full_live_candidate(lineage.generation_id)
    lineage = replace(lineage, candidate_digest=candidate["digest"])

    extra = {**candidate, "unexpected": True}
    with pytest.raises(ValueError, match="fields are not exact"):
        mod._validate_live_candidate(extra, lineage)

    missing = dict(candidate)
    missing.pop("quality_gate")
    with pytest.raises(ValueError, match="fields are not exact"):
        mod._validate_live_candidate(missing, lineage)

    tampered = json.loads(json.dumps(candidate))
    tampered["transform_results"]["transform_a"]["rows"] = 999
    with pytest.raises(ValueError, match="digest differs"):
        mod._validate_live_candidate(tampered, lineage)

    bad_semantics = json.loads(json.dumps(candidate))
    bad_semantics["quality_gate"]["errors"] = ["broken uniqueness gate"]
    unsigned = {key: value for key, value in bad_semantics.items() if key != "digest"}
    bad_semantics["digest"] = hashlib.sha256(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    bad_lineage = replace(lineage, candidate_digest=bad_semantics["digest"])
    with pytest.raises(ValueError, match="quality gate is not clean"):
        mod._validate_live_candidate(bad_semantics, bad_lineage)


def test_live_publication_reader_uses_admitted_scheduler(tmp_path):
    options = runtime_options(tmp_path)
    lifecycle_path, report = write_lifecycle_report(tmp_path, options)
    deployment_path = Path(options[options.index("--deployment-report") + 1])
    compose_path = Path(options[options.index("--compose-file") + 1])
    env_path = Path(options[options.index("--env-file") + 1])
    context = mod.runtime_binding.load_deployment_context(
        deployment_path,
        project="fotmob-airflow",
        compose_file=compose_path,
    )
    generation_id = report["publication"]["generation_id"]
    observed = {}

    def run(command, **kwargs):
        observed["command"] = command
        observed["environment"] = kwargs["env"]
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=(
                "FOTMOB_ACCEPTANCE_PUBLICATION_JSON="
                + json.dumps({"generation_id": generation_id})
                + "\n"
            ),
        )

    state = mod.read_live_publication_state(
        context,
        generation_id=generation_id,
        project="fotmob-airflow",
        compose_file=compose_path,
        env_file=env_path,
        run=run,
    )

    assert state == {"generation_id": generation_id}
    assert observed["command"][-6:-3] == (
        "exec",
        "-T",
        "airflow-scheduler",
    )
    assert observed["environment"]["FOTMOB_DEPLOYMENT_ID"] == "f" * 32


def test_removed_operator_lineage_knobs_are_rejected():
    parser = mod.build_parser()
    required = [
        "verify",
        "--scopes",
        "scopes.txt",
        "--scope-sha256",
        "a" * 64,
        "--lifecycle-report",
        "replay.json",
        "--output",
        "verify.json",
        "--env-file",
        "compose.env",
        "--trino-env-file",
        "trino.env",
        "--deployment-report",
        "deployment.json",
    ]
    with pytest.raises(SystemExit):
        parser.parse_args([*required, "--plan-signature", PLAN_SIGNATURE])
    with pytest.raises(SystemExit):
        parser.parse_args([*required, "--completed-since", COMPLETED_SINCE])


def test_verify_checks_every_current_view_and_is_green():
    client = FakeAcceptanceClient()
    report = mod.verify(
        client,
        [scope()],
        catalog="iceberg",
        bronze_schema="bronze",
        parser_version="fotmob-native-v2",
        lineage=fake_lineage(),
    )
    assert report["passed"] is True
    assert report["summary"] == {
        "checks": 5 + len(mod.CURRENT_VIEW_KEYS),
        "passed": 5 + len(mod.CURRENT_VIEW_KEYS),
        "failed": 0,
    }
    checked_views = [sql for sql in client.sql if "acceptance:current-view:" in sql]
    assert len(checked_views) == len(mod.CURRENT_VIEW_KEYS)


def test_verify_records_sql_error_and_fails_closed():
    client = FakeAcceptanceClient(fail_marker="current-view:fotmob_matches")
    report = mod.verify(
        client,
        [scope()],
        catalog="iceberg",
        bronze_schema="bronze",
        parser_version="fotmob-native-v2",
        lineage=fake_lineage(),
    )
    assert report["passed"] is False
    failed = [check for check in report["checks"] if not check["passed"]]
    assert len(failed) == 1
    assert failed[0]["name"] == "current_view:fotmob_matches"
    assert "synthetic SQL failure" in failed[0]["error"]


def test_verify_recomputes_scope_completion_coverage_hash():
    class BadCoverageClient(FakeAcceptanceClient):
        def query(self, sql):
            rows = super().query(sql)
            if "acceptance:scope-completion" not in sql:
                return rows
            row = list(rows[0])
            capabilities = json.loads(row[5])
            capabilities["coverage_hash"] = "0" * 64
            row[5] = json.dumps(capabilities)
            return [tuple(row)]

    report = mod.verify(
        BadCoverageClient(),
        [scope()],
        catalog="iceberg",
        bronze_schema="bronze",
        parser_version="fotmob-native-v2",
        lineage=fake_lineage(),
    )

    completion = next(
        check
        for check in report["checks"]
        if check["name"] == "target_scope_completion"
    )
    assert completion["passed"] is False
    assert "coverage_hash does not match" in str(completion["details"]["malformed"])


def test_verify_reconciles_claimed_coverage_with_manifest_gated_current_rows():
    class MissingTeamClient(FakeAcceptanceClient):
        def query(self, sql):
            if "acceptance:scope-coverage:teams" in sql:
                self.sql.append(sql)
                return []
            return super().query(sql)

    report = mod.verify(
        MissingTeamClient(),
        [scope()],
        catalog="iceberg",
        bronze_schema="bronze",
        parser_version="fotmob-native-v2",
        lineage=fake_lineage(),
    )

    completion = next(
        check
        for check in report["checks"]
        if check["name"] == "target_scope_completion"
    )
    assert completion["passed"] is False
    assert completion["details"]["coverage_mismatches"] == [
        {
            "scope": "47=2025/2026",
            "entity": "teams",
            "expected_count": 1,
            "actual_count": 0,
            "expected_identity_hash": mod._identity_hash({"1"}),
            "actual_identity_hash": mod._identity_hash(set()),
        }
    ]


def test_scope_completion_rejects_mixed_runner_lineage():
    class MixedRunClient(FakeAcceptanceClient):
        def query(self, sql):
            rows = super().query(sql)
            if "acceptance:scope-completion" not in sql:
                return rows
            row = list(rows[0])
            row[2] = "old-or-unrelated-run"
            return [tuple(row)]

    client = MixedRunClient()
    report = mod.verify(
        client,
        [scope()],
        catalog="iceberg",
        bronze_schema="bronze",
        parser_version="fotmob-native-v2",
        lineage=fake_lineage(),
    )

    completion = next(
        check
        for check in report["checks"]
        if check["name"] == "target_scope_completion"
    )
    assert completion["passed"] is False
    assert "different runner run_id" in str(completion["details"]["malformed"])
    assert not any("acceptance:scope-coverage:" in sql for sql in client.sql)


def test_parity_requires_exact_sets_roster_90_percent_and_transfer_preservation():
    report = mod.parity(
        FakeParityClient(),
        [scope()],
        catalog="iceberg",
        bronze_schema="bronze",
        silver_schema="silver",
        parser_version="fotmob-native-v2",
        lineage=fake_lineage(),
    )
    assert report["passed"] is True
    by_name = {check["name"]: check for check in report["checks"]}
    roster = by_name["47=2025/2026:roster"]
    assert roster["details"]["legacy_coverage"] == 0.9
    assert roster["details"]["only_legacy"] == 1
    transfers = by_name["silver_transfer_legacy_identity_preservation"]
    assert transfers["details"]["legacy_coverage"] == 1.0
    assert transfers["details"]["only_native"] == 1


def test_main_writes_valid_red_json_and_nonzero_on_missing_sql(tmp_path):
    scopes = tmp_path / "scopes.txt"
    scopes.write_bytes(mod.APPROVED_SCOPE_ARTIFACT.read_bytes())
    output = tmp_path / "verify.json"
    options = runtime_options(tmp_path)
    lifecycle_path, lifecycle = write_lifecycle_report(tmp_path, options)
    client = FakeAcceptanceClient(
        fail_marker="acceptance:direct-only",
        runner_run_id=lifecycle["publication"]["generation_id"],
    )
    scope_sha = hashlib.sha256(scopes.read_bytes()).hexdigest()
    code = mod.main(
        [
            "verify",
            "--scopes",
            str(scopes),
            "--scope-sha256",
            scope_sha,
            "--lifecycle-report",
            str(lifecycle_path),
            "--output",
            str(output),
            *options,
        ],
        client_factory=lambda **_: client,
        runtime_binder=fake_runtime_binding,
        publication_reader=live_publication_reader(lifecycle),
    )
    payload = json.loads(output.read_text())
    assert code == 1
    assert payload["passed"] is False
    assert client.closed is True
    assert (
        payload["lifecycle_report"]["report_sha256"]
        == hashlib.sha256(lifecycle_path.read_bytes()).hexdigest()
    )
    assert (
        payload["lifecycle_report"]["live_publication_before"]["phase"] == "abandoned"
    )
    assert payload["lifecycle_report"]["live_publication_after"]["released"] is True


def test_verify_and_parity_use_the_same_158_scope_lifecycle_report(tmp_path):
    options = runtime_options(tmp_path)
    lifecycle_path, lifecycle = write_lifecycle_report(tmp_path, options)
    generation_id = lifecycle["publication"]["generation_id"]
    reader = live_publication_reader(lifecycle)

    verify_scopes = mod.load_scopes(mod.APPROVED_SCOPE_ARTIFACT)
    verify_client = FakeAcceptanceClient(
        runner_run_id=generation_id,
        scope_identities=[
            (item.competition_id, item.source_season_key) for item in verify_scopes
        ],
    )
    verify_output = tmp_path / "verify-green.json"
    verify_code = mod.main(
        [
            "verify",
            "--scopes",
            str(mod.APPROVED_SCOPE_ARTIFACT),
            "--scope-sha256",
            mod.APPROVED_SCOPE_ARTIFACT_SHA256,
            "--lifecycle-report",
            str(lifecycle_path),
            "--output",
            str(verify_output),
            *options,
        ],
        client_factory=lambda **_: verify_client,
        runtime_binder=fake_runtime_binding,
        publication_reader=reader,
    )

    parity_scopes = mod.load_scopes(mod.APPROVED_PARITY_SCOPE_ARTIFACT, parity=True)
    parity_client = FakeParityClient(
        runner_run_id=generation_id,
        scope_identities=[
            (item.competition_id, item.source_season_key) for item in parity_scopes
        ],
    )
    parity_output = tmp_path / "parity-green.json"
    parity_code = mod.main(
        [
            "parity",
            "--scopes",
            str(mod.APPROVED_PARITY_SCOPE_ARTIFACT),
            "--scope-sha256",
            mod.APPROVED_PARITY_SCOPE_ARTIFACT_SHA256,
            "--lifecycle-report",
            str(lifecycle_path),
            "--output",
            str(parity_output),
            *options,
        ],
        client_factory=lambda **_: parity_client,
        runtime_binder=fake_runtime_binding,
        publication_reader=reader,
    )

    verify_report = json.loads(verify_output.read_text(encoding="utf-8"))
    parity_report = json.loads(parity_output.read_text(encoding="utf-8"))
    assert verify_code == parity_code == 0
    assert verify_report["passed"] is parity_report["passed"] is True
    assert verify_report["lifecycle_report"]["scope"]["count"] == 158
    assert parity_report["lifecycle_report"]["scope"]["count"] == 158
    assert (
        verify_report["lifecycle_report"]["report_sha256"]
        == (parity_report["lifecycle_report"]["report_sha256"])
    )
    assert verify_report["runner_run_id"] == generation_id
    assert parity_report["runner_run_id"] == generation_id


def test_same_count_wrong_verify_identities_are_rejected_before_trino(tmp_path):
    scopes = tmp_path / "scopes.json"
    scopes.write_text(
        json.dumps(
            [
                {"competition_id": index, "source_season_key": "2025/2026"}
                for index in range(1, mod.APPROVED_SCOPE_COUNT + 1)
            ]
        )
    )
    output = tmp_path / "verify.json"
    called = False

    def factory(**_):
        nonlocal called
        called = True
        raise AssertionError("Trino must not be contacted")

    code = mod.main(
        [
            "verify",
            "--scopes",
            str(scopes),
            "--scope-sha256",
            hashlib.sha256(scopes.read_bytes()).hexdigest(),
            "--lifecycle-report",
            str(tmp_path / "not-read.json"),
            "--output",
            str(output),
            *runtime_options(tmp_path),
        ],
        client_factory=factory,
        runtime_binder=fake_runtime_binding,
    )
    payload = json.loads(output.read_text())
    assert code == 1
    assert called is False
    assert "approved verify scope mismatch" in payload["fatal_error"]
    assert "observed=158, expected=158" in payload["fatal_error"]


def test_identity_equivalent_scope_file_must_still_be_reviewed_bytes(tmp_path):
    scopes = tmp_path / "equivalent.json"
    scopes.write_text(
        json.dumps(
            [
                {
                    "competition_id": item.competition_id,
                    "source_season_key": item.source_season_key,
                }
                for item in mod.load_scopes(mod.APPROVED_SCOPE_ARTIFACT)
            ]
        )
    )
    output = tmp_path / "verify.json"
    called = False

    def factory(**_):
        nonlocal called
        called = True
        raise AssertionError("Trino must not be contacted")

    code = mod.main(
        [
            "verify",
            "--scopes",
            str(scopes),
            "--scope-sha256",
            hashlib.sha256(scopes.read_bytes()).hexdigest(),
            "--lifecycle-report",
            str(tmp_path / "not-read.json"),
            "--output",
            str(output),
            *runtime_options(tmp_path),
        ],
        client_factory=factory,
        runtime_binder=fake_runtime_binding,
    )

    assert code == 1
    assert called is False
    assert (
        "not the byte-exact reviewed verify artifact"
        in json.loads(output.read_text())["fatal_error"]
    )


def test_default_approved_scope_count_rejects_partial_file_before_trino(tmp_path):
    scopes = tmp_path / "scopes.json"
    scopes.write_text(
        json.dumps([{"competition_id": 47, "source_season_key": "2025/2026"}])
    )
    output = tmp_path / "verify.json"
    called = False

    def factory(**_):
        nonlocal called
        called = True
        raise AssertionError("Trino must not be contacted")

    code = mod.main(
        [
            "verify",
            "--scopes",
            str(scopes),
            "--scope-sha256",
            hashlib.sha256(scopes.read_bytes()).hexdigest(),
            "--lifecycle-report",
            str(tmp_path / "not-read.json"),
            "--output",
            str(output),
            *runtime_options(tmp_path),
        ],
        client_factory=factory,
        runtime_binder=fake_runtime_binding,
    )
    assert code == 1
    assert called is False
    assert "observed=1, expected=158" in json.loads(output.read_text())["fatal_error"]


def test_production_acceptance_rejects_legacy_parser_version_before_trino(tmp_path):
    scopes = tmp_path / "scopes.json"
    scopes.write_text(
        json.dumps(
            [
                {"competition_id": index, "source_season_key": "2025/2026"}
                for index in range(1, mod.APPROVED_SCOPE_COUNT + 1)
            ]
        )
    )
    output = tmp_path / "verify.json"
    called = False

    def factory(**_):
        nonlocal called
        called = True
        return FakeAcceptanceClient()

    code = mod.main(
        [
            "verify",
            "--scopes",
            str(scopes),
            "--scope-sha256",
            hashlib.sha256(scopes.read_bytes()).hexdigest(),
            "--lifecycle-report",
            str(tmp_path / "not-read.json"),
            "--parser-version",
            "fotmob-native-v1",
            "--output",
            str(output),
            *runtime_options(tmp_path),
        ],
        client_factory=factory,
        runtime_binder=fake_runtime_binding,
    )
    assert code == 1
    assert called is False
    assert "pinned" in json.loads(output.read_text())["fatal_error"]


def test_verify_scopes_completion_to_exact_plan_parser_and_time():
    client = FakeAcceptanceClient()
    mod.verify(
        client,
        [scope()],
        catalog="iceberg",
        bronze_schema="bronze",
        parser_version="fotmob-native-v2",
        lineage=fake_lineage(),
    )
    completion_sql = next(sql for sql in client.sql if "scope-completion" in sql)
    assert PLAN_SIGNATURE in completion_sql
    assert f"m.run_id = '{RUNNER_RUN_ID}'" in completion_sql
    assert "fotmob-native-v2" in completion_sql
    assert "completed_at >= TIMESTAMP '2026-07-20" in completion_sql
    inventory_sql = next(sql for sql in client.sql if "field-inventory" in sql)
    assert "inventory_row._target_batch_id" in inventory_sql
    assert "fotmob-native-v2" in inventory_sql
    for marker in ("latest-manifests", "direct-only"):
        candidate_sql = next(sql for sql in client.sql if marker in sql)
        assert "JOIN candidate_runs" in candidate_sql
        assert PLAN_SIGNATURE in candidate_sql
        assert f"m.run_id = '{RUNNER_RUN_ID}'" in candidate_sql
    assert "JOIN candidate_runs" not in inventory_sql
    assert "fotmob-native-v1" in inventory_sql

    view_sql = next(sql for sql in client.sql if "current-view:fotmob_matches" in sql)
    assert "fotmob-native-v1" in view_sql
    assert "fotmob-native-v2" in view_sql

    coverage_sql = [sql for sql in client.sql if "acceptance:scope-coverage:" in sql]
    assert len(coverage_sql) == 4
    assert all("scope_batches" in sql for sql in coverage_sql)
    assert all("fotmob-native-v2" in sql for sql in coverage_sql)
    assert all(f"m.run_id = '{RUNNER_RUN_ID}'" in sql for sql in coverage_sql)
    player_coverage_sql = next(sql for sql in coverage_sql if ":players" in sql)
    assert "team_manifest_ranked" in player_coverage_sql
    assert "m.completed_at <= completion.completed_at" in player_coverage_sql
