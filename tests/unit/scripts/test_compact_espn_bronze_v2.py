from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

import pytest


UTC = timezone.utc


def _legacy_tables() -> dict[str, dict]:
    output = {}
    for index, entity in enumerate(("schedule", "lineup", "matchsheet"), 1):
        per_scope = [
            {
                "league": pair["league"],
                "season": pair["season"],
                "row_count": 1,
                "row_hash": hashlib.sha256(
                    f"{entity}:{pair['league']}:{pair['season']}".encode()
                ).hexdigest(),
                "distinct_key_count": 1,
            }
            for pair in _observed_pairs()
            if entity in pair["observed_entities"]
        ]
        output[entity] = {
            "source_table": f"espn_{entity}",
            "snapshot_id": 100 + index,
            "columns": [
                {"name": "league", "type": "varchar"},
                {"name": "season", "type": "varchar"},
                {"name": "game", "type": "varchar"},
                {"name": "_batch_id", "type": "varchar"},
            ],
            "whole_rowset_metrics": {
                "row_count": len(per_scope),
                "row_hash": str(index) * 64,
                "distinct_key_count": len(per_scope),
            },
            "per_scope_metrics": per_scope,
        }
    return output


def _observed_pairs() -> list[dict]:
    from scrapers.espn.layout import REVIEWED_NATIVE_REPLACEMENTS

    pairs = [
        {
            "league": league,
            "season": season,
            "observed_entities": ["lineup", "matchsheet", "schedule"],
        }
        for _scope, league, season in REVIEWED_NATIVE_REPLACEMENTS
    ]
    pairs.extend(
        [
            {
                "league": "ENG-Premier League",
                "season": "1516",
                "observed_entities": ["schedule"],
            },
            {
                "league": "ENG-Premier League",
                "season": "0001",
                "observed_entities": ["schedule"],
            },
            {
                "league": None,
                "season": None,
                "observed_entities": ["lineup"],
            },
        ]
    )
    return pairs


def _baseline_row(scope: str, league: str, season: str, index: int) -> dict:
    from scrapers.espn.repository import canonical_json, canonical_sha256

    captured_at = datetime(2026, 8, 8, 11, 30, tzinfo=UTC)
    base = {
        "baseline_version": "espn-legacy-baseline-v1",
        "scope_id": scope,
        "legacy_league": league,
        "legacy_season": season,
        "captured_at": captured_at,
        "entity_metrics_json": canonical_json(
            {
                entity: {
                    "distinct_key_count": 1,
                    "max_ingested_at": "2026-08-08T11:00:00Z",
                    "row_count": 1,
                }
                for entity in ("schedule", "lineup", "matchsheet")
            }
        ),
        "legacy_snapshot_ids_json": canonical_json(
            {
                "espn_schedule": 101,
                "espn_lineup": 102,
                "espn_matchsheet": 103,
            }
        ),
        "registry_signature": "a" * 64,
        "durable_manifest_uri": f"s3://raw/espn/{scope}/manifest.json",
        "durable_manifest_sha256": f"{index + 1:x}" * 64,
        "replay_raw_manifest_uri": f"s3://raw/espn/{scope}/raw.json",
        "replay_raw_manifest_sha256": f"{index + 7:x}" * 64,
        "trust_label": "trusted",
    }
    return {
        **base,
        "captured_at": captured_at.isoformat(),
        "baseline_sha256": canonical_sha256(base),
    }


def _plan():
    from scripts.compact_espn_bronze_v2 import (
        build_dispositions,
        build_legacy_archive_id,
        seal_native_route,
        seal_plan,
    )
    from scrapers.espn.layout import REVIEWED_NATIVE_REPLACEMENTS

    replacement_by_scope = {
        scope: (league, season)
        for scope, league, season in REVIEWED_NATIVE_REPLACEMENTS
    }
    target_scope_ids = tuple(replacement_by_scope) + tuple(
        f"{10000 + index}:2026" for index in range(175)
    )
    target_scope_sha256 = hashlib.sha256(
        json.dumps(
            sorted(target_scope_ids), separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    ).hexdigest()
    native_routes = []
    for index, scope_id in enumerate(target_scope_ids):
        legacy = replacement_by_scope.get(scope_id)
        native_routes.append(
            seal_native_route(
                {
                    "scope_id": scope_id,
                    "previous_source": "legacy" if legacy else "absent",
                    "legacy_league": legacy[0] if legacy else None,
                    "legacy_season": legacy[1] if legacy else None,
                    "generation_id": f"generation-{index}",
                    "generation_signature": "e" * 64,
                    "manifest_sha256": "f" * 64,
                    "registry_signature": "a" * 64,
                    "registry_snapshot_uri": "s3://raw/espn/registry.json.gz",
                    "registry_snapshot_sha256": "9" * 64,
                    "target_scope_sha256": target_scope_sha256,
                    "parser_version": "espn-native-parser-v3",
                    "runtime_version": "espn-native-runtime-v4",
                    "route_action": "retain_existing" if legacy else "append_root",
                    "effective_at": "2026-08-08T12:15:00+00:00",
                }
            )
        )

    legacy_tables = _legacy_tables()
    base = {
        "schema_version": "espn-compact6-plan-v2",
        "transition_id": "compact6-20260808-a1b2c3d4",
        "created_at": "2026-08-08T12:00:00+00:00",
        "catalog": "iceberg",
        "bronze_schema": "bronze",
        "internal_schema": "espn_internal",
        "source_layout": "legacy14",
        "target_layout": "compact6",
        "legacy_tables": legacy_tables,
        "legacy_dispositions": build_dispositions(
            build_legacy_archive_id(legacy_tables), _observed_pairs()
        ),
        "native_replacements": [
            {
                "scope_id": scope,
                "legacy_league": league,
                "legacy_season": season,
            }
            for scope, league, season in __import__(
                "scrapers.espn.layout", fromlist=["REVIEWED_NATIVE_REPLACEMENTS"]
            ).REVIEWED_NATIVE_REPLACEMENTS
        ],
        "registry_signature": "a" * 64,
        "registry_snapshot_uri": "s3://raw/espn/registry.json.gz",
        "registry_snapshot_sha256": "9" * 64,
        "native_scope_count": 181,
        "target_scope_ids": list(target_scope_ids),
        "target_scope_sha256": target_scope_sha256,
        "native_routes": native_routes,
        "manifest_snapshot_id": 206,
        "source_cutover_row_count": 6,
        "source_cutover_heads": [
            {
                "scope_id": route["scope_id"],
                "cutover_id": f"promotion-{index + 1}",
                "cutover_sha256": f"{index + 1:x}" * 64,
                "active_source": "native",
                "previous_source": "legacy",
                "legacy_league": route["legacy_league"],
                "legacy_season": route["legacy_season"],
                "registry_signature": route["registry_signature"],
                "native_generation_id": route["generation_id"],
                "native_generation_signature": route["generation_signature"],
                "native_manifest_sha256": route["manifest_sha256"],
            }
            for index, route in enumerate(native_routes[:6])
        ],
        "state_snapshots": {
            "espn_schedule_generation_v2": 201,
            "espn_lineup_generation_v2": 202,
            "espn_matchsheet_generation_v2": 203,
            "espn_scope_cutover_v2": 204,
            "espn_legacy_baseline_v2": 205,
        },
        "baseline_sha256_by_scope": {},
    }
    base["baseline_evidence_by_scope"] = {
        scope: _baseline_row(scope, league, season, index)
        for index, (scope, league, season) in enumerate(
            __import__(
                "scrapers.espn.layout",
                fromlist=["REVIEWED_NATIVE_REPLACEMENTS"],
            ).REVIEWED_NATIVE_REPLACEMENTS
        )
    }
    base["baseline_sha256_by_scope"] = {
        scope: row["baseline_sha256"]
        for scope, row in base["baseline_evidence_by_scope"].items()
    }
    return seal_plan(base)


def _manifest(plan=None):
    from scripts.compact_espn_bronze_v2 import build_archive_manifest

    plan = _plan() if plan is None else plan
    return build_archive_manifest(
        plan,
        archive_snapshot_ids={
            "espn_schedule_legacy_archive_v1": 301,
            "espn_lineup_legacy_archive_v1": 302,
            "espn_matchsheet_legacy_archive_v1": 303,
        },
        disposition_snapshot_id=304,
        captured_at=datetime(2026, 8, 8, 12, 30, tzinfo=UTC),
    )


def _run_guard(*, active_runs=None, paused_dags=None, plan=None, captured_at=None):
    from scripts.compact_espn_bronze_v2 import (
        REQUIRED_PAUSED_DAGS,
        RUN_GUARD_VERSION,
        seal_run_guard,
    )

    plan = _plan() if plan is None else plan
    return seal_run_guard(
        {
            "schema_version": RUN_GUARD_VERSION,
            "captured_at": (
                datetime.now(UTC).isoformat() if captured_at is None else captured_at
            ),
            "transition_id": plan["transition_id"],
            "plan_sha256": plan["plan_sha256"],
            "paused_dags": list(
                REQUIRED_PAUSED_DAGS if paused_dags is None else paused_dags
            ),
            "all_paused": True,
            "active_runs": [] if active_runs is None else active_runs,
        }
    )


def _plan_source_main_rows(plan=None) -> list[tuple[str, int]]:
    plan = _plan() if plan is None else plan
    rows = [
        (
            f"{plan['catalog']}.{plan['bronze_schema']}.{table['source_table']}",
            table["snapshot_id"],
        )
        for table in plan["legacy_tables"].values()
    ]
    rows.extend(
        (
            f"{plan['catalog']}.{plan['bronze_schema']}.{relation}",
            snapshot_id,
        )
        for relation, snapshot_id in plan["state_snapshots"].items()
    )
    rows.append(
        (
            f"{plan['catalog']}.{plan['bronze_schema']}.espn_ingest_manifest_v2",
            plan["manifest_snapshot_id"],
        )
    )
    return sorted(rows)


def test_dispositions_preserve_historical_rows_and_quarantine_null_scope() -> None:
    from scripts.compact_espn_bronze_v2 import build_dispositions

    rows = build_dispositions("archive-1", _observed_pairs())
    by_pair = {(row["league"], row["season"]): row for row in rows}

    assert by_pair[("ENG-Premier League", "1516")]["disposition"] == (
        "compatibility_only"
    )
    assert by_pair[("ENG-Premier League", "0001")]["disposition"] == (
        "compatibility_only"
    )
    assert by_pair[(None, None)]["disposition"] == "quarantined"
    assert sum(row["disposition"] == "native_current_replaced" for row in rows) == 6
    assert all(len(row["disposition_sha256"]) == 64 for row in rows)


def test_plan_rejects_arbitrary_six_and_tampered_disposition_hash() -> None:
    from scripts.compact_espn_bronze_v2 import Compact6Error, seal_plan, validate_plan

    plan = _plan()
    validate_plan(plan)

    arbitrary = json.loads(json.dumps(plan))
    arbitrary.pop("plan_sha256")
    arbitrary["native_replacements"][0]["scope_id"] = "9999:2026"
    with pytest.raises(Compact6Error, match="reviewed six"):
        validate_plan(seal_plan(arbitrary))

    corrupt = json.loads(json.dumps(plan))
    corrupt.pop("plan_sha256")
    corrupt["legacy_dispositions"][0]["season"] = "tampered"
    with pytest.raises(Compact6Error, match="disposition hash"):
        validate_plan(seal_plan(corrupt))


@pytest.mark.parametrize("mutation", ["missing", "extra", "duplicate"])
def test_plan_rejects_any_native_route_set_drift(mutation: str) -> None:
    from scripts.compact_espn_bronze_v2 import Compact6Error, seal_plan, validate_plan

    plan = json.loads(json.dumps(_plan()))
    plan.pop("plan_sha256")
    if mutation == "missing":
        plan["native_routes"].pop()
    elif mutation == "extra":
        route = dict(plan["native_routes"][-1])
        route["scope_id"] = "99999:2026"
        route.pop("route_sha256")
        from scripts.compact_espn_bronze_v2 import seal_native_route

        plan["native_routes"].append(seal_native_route(route))
    else:
        plan["native_routes"][-1] = plan["native_routes"][0]
    with pytest.raises(Compact6Error, match="181|route set|duplicate"):
        validate_plan(seal_plan(plan))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("parser_version", "espn-native-parser-v2", "parser/runtime"),
        ("runtime_version", "espn-native-runtime-v3", "parser/runtime"),
        ("registry_snapshot_sha256", "8" * 64, "registry"),
        ("target_scope_sha256", "7" * 64, "target"),
    ],
)
def test_plan_rejects_mixed_native_route_identity(
    field: str, value: str, message: str
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        seal_native_route,
        seal_plan,
        validate_plan,
    )

    plan = json.loads(json.dumps(_plan()))
    plan.pop("plan_sha256")
    route = dict(plan["native_routes"][17])
    route.pop("route_sha256")
    route[field] = value
    plan["native_routes"][17] = seal_native_route(route)
    with pytest.raises(Compact6Error, match=message):
        validate_plan(seal_plan(plan))


def test_plan_rejects_existing_or_forked_source_cutover_heads() -> None:
    from scripts.compact_espn_bronze_v2 import Compact6Error, seal_plan, validate_plan

    plan = json.loads(json.dumps(_plan()))
    plan.pop("plan_sha256")
    plan["source_cutover_row_count"] = 7
    plan["source_cutover_heads"].append(dict(plan["source_cutover_heads"][0]))
    with pytest.raises(Compact6Error, match="cutover.*six|fork|duplicate"):
        validate_plan(seal_plan(plan))


@pytest.mark.parametrize("mutation", ("missing", "extra", "malformed"))
def test_plan_rejects_incomplete_or_invalid_baseline_evidence(mutation: str) -> None:
    from scripts.compact_espn_bronze_v2 import Compact6Error, seal_plan, validate_plan

    plan = json.loads(json.dumps(_plan()))
    plan.pop("plan_sha256")
    if mutation == "missing":
        scope = next(iter(plan["baseline_sha256_by_scope"]))
        plan["baseline_sha256_by_scope"].pop(scope)
        plan["baseline_evidence_by_scope"].pop(scope)
    elif mutation == "extra":
        plan["baseline_sha256_by_scope"]["9999:2026"] = "a" * 64
        plan["baseline_evidence_by_scope"]["9999:2026"] = {
            "baseline_sha256": "a" * 64,
            "baseline_table_snapshot_id": 205,
            "legacy_snapshot_ids": {
                "espn_schedule": 101,
                "espn_lineup": 102,
                "espn_matchsheet": 103,
            },
        }
    else:
        scope = next(iter(plan["baseline_sha256_by_scope"]))
        plan["baseline_sha256_by_scope"][scope] = "g" * 64
        plan["baseline_evidence_by_scope"][scope]["baseline_sha256"] = "g" * 64
    with pytest.raises(Compact6Error, match="baseline"):
        validate_plan(seal_plan(plan))


def test_plan_rejects_self_hashed_baseline_metrics_that_differ_from_archive() -> None:
    from scrapers.espn.repository import canonical_json, canonical_sha256
    from scripts.compact_espn_bronze_v2 import Compact6Error, seal_plan, validate_plan

    plan = json.loads(json.dumps(_plan()))
    plan.pop("plan_sha256")
    scope = next(iter(plan["baseline_evidence_by_scope"]))
    baseline = plan["baseline_evidence_by_scope"][scope]
    metrics = json.loads(baseline["entity_metrics_json"])
    metrics["schedule"]["row_count"] = 2
    baseline["entity_metrics_json"] = canonical_json(metrics)
    hash_base = {
        key: (datetime.fromisoformat(value) if key == "captured_at" else value)
        for key, value in baseline.items()
        if key != "baseline_sha256"
    }
    baseline["baseline_sha256"] = canonical_sha256(hash_base)
    plan["baseline_sha256_by_scope"][scope] = baseline["baseline_sha256"]

    with pytest.raises(Compact6Error, match="global archive scope"):
        validate_plan(seal_plan(plan))


def test_plan_rejects_disposition_that_omits_historical_archive_pair() -> None:
    from scripts.compact_espn_bronze_v2 import Compact6Error, seal_plan, validate_plan

    plan = json.loads(json.dumps(_plan()))
    plan.pop("plan_sha256")
    plan["legacy_dispositions"] = [
        row
        for row in plan["legacy_dispositions"]
        if (row["league"], row["season"]) != ("ENG-Premier League", "0001")
    ]

    with pytest.raises(Compact6Error, match="complete per-scope inventory"):
        validate_plan(seal_plan(plan))


def test_archive_manifest_binds_source_archive_and_disposition_snapshots() -> None:
    from scripts.compact_espn_bronze_v2 import (
        build_archive_manifest,
        validate_archive_manifest,
    )

    plan = _plan()
    archive_snapshots = {
        "espn_schedule_legacy_archive_v1": 301,
        "espn_lineup_legacy_archive_v1": 302,
        "espn_matchsheet_legacy_archive_v1": 303,
    }
    manifest = build_archive_manifest(
        plan,
        archive_snapshot_ids=archive_snapshots,
        disposition_snapshot_id=304,
        captured_at=datetime(2026, 8, 8, 12, 30, tzinfo=UTC),
    )

    validate_archive_manifest(manifest, plan["legacy_dispositions"])
    assert json.loads(manifest["legacy_snapshot_ids_json"]) == {
        "espn_schedule": 101,
        "espn_lineup": 102,
        "espn_matchsheet": 103,
    }
    assert json.loads(manifest["archive_snapshot_ids_json"]) == archive_snapshots
    assert manifest["legacy_disposition_snapshot_id"] == 304

    corrupt = dict(manifest)
    corrupt["legacy_disposition_metrics_json"] = json.dumps(
        {"row_count": 1, "row_hash": "0" * 64}, separators=(",", ":"), sort_keys=True
    )
    with pytest.raises(Exception, match="manifest hash|disposition metrics"):
        validate_archive_manifest(corrupt, plan["legacy_dispositions"])


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("registry_signature", "b" * 64),
        (
            "legacy_snapshot_ids_json",
            '{"espn_lineup":102,"espn_matchsheet":103,"espn_schedule":999}',
        ),
        (
            "whole_rowset_metrics_json",
            json.dumps(
                {
                    "lineup": {
                        "distinct_key_count": 6,
                        "row_count": 6,
                        "row_hash": "2" * 64,
                    },
                    "matchsheet": {
                        "distinct_key_count": 6,
                        "row_count": 6,
                        "row_hash": "3" * 64,
                    },
                    "schedule": {
                        "distinct_key_count": 8,
                        "row_count": 9,
                        "row_hash": "1" * 64,
                    },
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        ),
    ),
)
def test_self_resealed_manifest_still_must_match_exact_plan(
    field: str, value: str
) -> None:
    from scrapers.espn.repository import canonical_sha256
    from scripts.compact_espn_bronze_v2 import Compact6Error, render_apply_steps

    plan = _plan()
    manifest = dict(_manifest(plan))
    manifest[field] = value
    manifest["manifest_sha256"] = canonical_sha256(
        {key: item for key, item in manifest.items() if key != "manifest_sha256"}
    )

    with pytest.raises(Compact6Error, match="sealed plan"):
        render_apply_steps(plan, manifest)


def test_rendered_apply_steps_pin_snapshots_and_end_with_exact_six_audit() -> None:
    from scripts.compact_espn_bronze_v2 import (
        build_archive_manifest,
        render_apply_steps,
    )

    plan = _plan()
    manifest = build_archive_manifest(
        plan,
        archive_snapshot_ids={
            "espn_schedule_legacy_archive_v1": 301,
            "espn_lineup_legacy_archive_v1": 302,
            "espn_matchsheet_legacy_archive_v1": 303,
        },
        disposition_snapshot_id=304,
        captured_at=datetime(2026, 8, 8, 12, 30, tzinfo=UTC),
    )
    steps = render_apply_steps(plan, manifest)
    sql = "\n".join(step.sql for step in steps)

    assert "espn_schedule FOR VERSION AS OF 101" in sql
    assert "espn_lineup FOR VERSION AS OF 102" in sql
    assert "espn_matchsheet FOR VERSION AS OF 103" in sql
    assert "SECURITY DEFINER AS" in sql
    assert "espn_internal.espn_schedule_current" in sql
    assert "expected_public_object_count = 6" in sql
    assert "verify_current_composition_schedule" in {step.name for step in steps}
    assert "verify_pinned_archive_per_scope_schedule" in {step.name for step in steps}
    assert "preflight_archive_manifest" in {step.name for step in steps}
    assert "preflight_exact_175_native_routes" in {step.name for step in steps}
    by_name = {step.name: step.index for step in steps}
    assert by_name["insert_archive_manifest"] > by_name["verify_exact_181_native_heads"]
    assert by_name["insert_archive_manifest"] > by_name["verify_exact_six_baselines"]
    assert (
        by_name["insert_archive_manifest"] < by_name["append_exact_181_native_routes"]
    )
    baseline_gate = next(
        step for step in steps if step.name == "verify_exact_six_baselines"
    ).sql
    assert "iceberg.bronze.espn_legacy_baseline_v2 FOR VERSION AS OF 205" in (
        baseline_gate
    )
    assert "iceberg.espn_internal.espn_legacy_baseline_v2 FOR VERSION" not in (
        baseline_gate
    )
    assert all(
        "SELECT *" not in step.sql for step in steps if step.name.startswith("publish_")
    )


def test_every_rendered_trino_statement_and_postcondition_parses() -> None:
    sqlglot = pytest.importorskip("sqlglot")
    from scripts.compact_espn_bronze_v2 import (
        render_apply_steps,
        render_disposition_persistence_steps,
        render_materialization_steps,
        render_precommit_validation_steps,
        render_repromotion_steps,
        render_rollback_steps,
    )

    plan = _plan()
    groups = (
        render_apply_steps(plan, _manifest(plan)),
        render_materialization_steps(plan),
        render_precommit_validation_steps(
            plan,
            archive_snapshot_ids={
                "espn_schedule_legacy_archive_v1": 301,
                "espn_lineup_legacy_archive_v1": 302,
                "espn_matchsheet_legacy_archive_v1": 303,
            },
        ),
        render_disposition_persistence_steps(
            plan,
            archive_snapshot_ids={
                "espn_schedule_legacy_archive_v1": 301,
                "espn_lineup_legacy_archive_v1": 302,
                "espn_matchsheet_legacy_archive_v1": 303,
            },
        ),
        render_rollback_steps(plan, _manifest(plan), reason="parser regression"),
        render_repromotion_steps(plan, _manifest(plan)),
    )
    for steps in groups:
        for step in steps:
            for statement in (step.sql, step.postcondition_sql):
                if statement is None:
                    continue
                statement = "\n".join(
                    line
                    for line in statement.splitlines()
                    if not line.lstrip().startswith("--")
                )
                sqlglot.parse_one(statement, read="trino")


def test_irreversible_singletons_are_excluded_from_prevalidation_phase() -> None:
    from scripts.compact_espn_bronze_v2 import (
        render_disposition_persistence_steps,
        render_materialization_steps,
        render_precommit_validation_steps,
    )

    plan = _plan()
    snapshots = {
        "espn_schedule_legacy_archive_v1": 301,
        "espn_lineup_legacy_archive_v1": 302,
        "espn_matchsheet_legacy_archive_v1": 303,
    }
    materialization_names = {step.name for step in render_materialization_steps(plan)}
    validation_names = {
        step.name
        for step in render_precommit_validation_steps(
            plan, archive_snapshot_ids=snapshots
        )
    }
    disposition_names = [
        step.name
        for step in render_disposition_persistence_steps(
            plan, archive_snapshot_ids=snapshots
        )
    ]

    assert "insert_legacy_dispositions" not in materialization_names
    assert "insert_archive_manifest" not in materialization_names
    assert "verify_exact_181_native_heads" in validation_names
    assert "verify_exact_six_baselines" in validation_names
    assert disposition_names == [
        "preflight_legacy_dispositions",
        "insert_legacy_dispositions",
    ]


class _Client:
    def __init__(self):
        self.sql: list[str] = []

    def query(self, sql: str):
        self.sql.append(sql)
        if "$refs" in sql:
            return _plan_source_main_rows()
        return []


def test_journaled_failure_after_every_step_resumes_idempotently(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        InjectedFailure,
        MigrationStep,
        run_journaled_steps,
    )

    steps = tuple(
        MigrationStep(index, f"step_{index}", f"CREATE SCHEMA IF NOT EXISTS s{index}")
        for index in range(4)
    )
    for fail_after in range(len(steps)):
        journal = tmp_path / f"journal-{fail_after}.json"
        client = _Client()
        with pytest.raises(InjectedFailure, match=f"step_{fail_after}"):
            run_journaled_steps(
                plan_sha256="a" * 64,
                transition_id=f"transition-{fail_after}",
                command="apply",
                steps=steps,
                client=client,
                journal_path=journal,
                fail_after_step=fail_after,
            )
        result = run_journaled_steps(
            plan_sha256="a" * 64,
            transition_id=f"transition-{fail_after}",
            command="resume",
            steps=steps,
            client=client,
            journal_path=journal,
        )
        assert result["status"] == "complete"
        assert len(client.sql) == len(steps)


def test_journal_persists_publication_result_count_and_hash(tmp_path: Path) -> None:
    from scrapers.espn.repository import canonical_sha256
    from scripts.compact_espn_bronze_v2 import MigrationStep, run_journaled_steps

    evidence_rows = [
        ("schedule", 20, "1" * 64),
        ("lineup", 30, "2" * 64),
        ("matchsheet", 40, "3" * 64),
    ]

    class EvidenceClient:
        def query(self, sql):
            if sql == "PUBLICATION METRICS":
                return evidence_rows
            if sql == "SELECT PARITY":
                return [(True,)]
            raise AssertionError(sql)

    step = MigrationStep(
        0,
        "capture_publication_evidence",
        "PUBLICATION METRICS",
        postcondition_sql="SELECT PARITY",
        capture_result=True,
    )
    result = run_journaled_steps(
        plan_sha256="a" * 64,
        transition_id="transition-1",
        command="apply",
        steps=(step,),
        client=EvidenceClient(),
        journal_path=tmp_path / "evidence.json",
    )
    completed = result["completed_steps"][0]
    assert completed["result_row_count"] == 3
    assert completed["result_sha256"] == canonical_sha256(evidence_rows)


def test_resume_replays_ctas_when_crash_followed_intent_but_preceded_sql(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        InjectedFailure,
        MigrationStep,
        run_journaled_steps,
    )

    class TableNotFound(RuntimeError):
        error_name = "TABLE_NOT_FOUND"

    class Client:
        def __init__(self):
            self.exists = False
            self.executions = 0

        def query(self, sql):
            if sql == "SELECT * FROM target":
                if not self.exists:
                    raise TableNotFound("target is absent")
                return []
            if sql == "CREATE TABLE target AS SELECT 1":
                self.exists = True
                self.executions += 1
                return []
            raise AssertionError(sql)

    step = MigrationStep(
        0,
        "archive_schedule",
        "CREATE TABLE target AS SELECT 1",
        postcondition_sql="SELECT * FROM target",
        result_must_be_empty=True,
    )
    journal = tmp_path / "pre-sql.json"
    client = Client()
    with pytest.raises(InjectedFailure, match="after intent before SQL"):
        run_journaled_steps(
            plan_sha256="a" * 64,
            transition_id="transition-1",
            command="apply",
            steps=(step,),
            client=client,
            journal_path=journal,
            fail_before_execute_step=0,
        )
    assert client.executions == 0

    result = run_journaled_steps(
        plan_sha256="a" * 64,
        transition_id="transition-1",
        command="resume",
        steps=(step,),
        client=client,
        journal_path=journal,
    )
    assert result["status"] == "complete"
    assert client.executions == 1


def test_active_or_unpaused_runs_block_before_first_mutation(tmp_path: Path) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        MigrationStep,
        apply_with_guard,
    )

    client = _Client()
    with pytest.raises(Compact6Error, match="zero active runs"):
        apply_with_guard(
            plan=_plan(),
            steps=(MigrationStep(0, "mutate", "CREATE SCHEMA x"),),
            client=client,
            journal_path=tmp_path / "blocked.json",
            run_guard=lambda: _run_guard(
                active_runs=[{"dag_id": "dag_ingest_espn", "run_id": "r1"}]
            ),
        )
    assert client.sql == []


@pytest.mark.parametrize("mutation", ["hash", "missing_dag", "unknown_dag"])
def test_run_guard_evidence_mismatch_blocks_before_first_mutation(
    tmp_path: Path, mutation: str
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        MigrationStep,
        REQUIRED_PAUSED_DAGS,
        apply_with_guard,
    )

    guard = _run_guard()
    if mutation == "hash":
        guard["captured_at"] = "2026-08-08T12:30:00+00:00"
    elif mutation == "missing_dag":
        guard = _run_guard(paused_dags=REQUIRED_PAUSED_DAGS[:-1])
    else:
        guard = _run_guard(paused_dags=(*REQUIRED_PAUSED_DAGS[:-1], "unknown"))
    client = _Client()
    with pytest.raises(Compact6Error, match="run guard"):
        apply_with_guard(
            plan=_plan(),
            steps=(MigrationStep(0, "mutate", "CREATE SCHEMA x"),),
            client=client,
            journal_path=tmp_path / f"blocked-{mutation}.json",
            run_guard=lambda: guard,
        )
    assert client.sql == []


@pytest.mark.parametrize("mutation", ["stale", "plan", "transition"])
def test_run_guard_is_fresh_and_bound_to_exact_transition(
    tmp_path: Path, mutation: str
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        MigrationStep,
        apply_with_guard,
        seal_run_guard,
    )

    plan = _plan()
    guard = _run_guard(
        plan=plan,
        captured_at=(datetime.now(UTC) - timedelta(minutes=3)).isoformat()
        if mutation == "stale"
        else None,
    )
    if mutation in {"plan", "transition"}:
        guard.pop("evidence_sha256")
        guard["plan_sha256" if mutation == "plan" else "transition_id"] = (
            "b" * 64 if mutation == "plan" else "another-transition"
        )
        guard = seal_run_guard(guard)
    client = _Client()
    with pytest.raises(Compact6Error, match="stale|plan SHA|transition identity"):
        apply_with_guard(
            plan=plan,
            steps=(MigrationStep(0, "mutate", "CREATE SCHEMA x"),),
            client=client,
            journal_path=tmp_path / f"guard-{mutation}.json",
            run_guard=lambda: guard,
        )
    assert client.sql == []


@pytest.mark.parametrize("mutation", ["stale", "missing", "extra", "fork"])
def test_plan_source_main_snapshot_guard_requires_exact_multiset(
    mutation: str,
) -> None:
    import scripts.compact_espn_bronze_v2 as compact

    plan = _plan()
    rows = _plan_source_main_rows(plan)
    if mutation == "stale":
        relation, snapshot_id = rows[0]
        rows[0] = (relation, snapshot_id + 1)
    elif mutation == "missing":
        rows.pop()
    elif mutation == "extra":
        relation, snapshot_id = rows[0]
        rows.append((relation, snapshot_id + 1000))
    else:
        rows.append(rows[0])

    class Client:
        def __init__(self) -> None:
            self.sql = []

        def query(self, sql):
            self.sql.append(sql)
            return rows

    client = Client()
    with pytest.raises(compact.Compact6Error, match="source main snapshot drift"):
        compact._assert_plan_source_main_snapshots(client, plan)

    assert len(client.sql) == 1
    assert client.sql[0].count('$refs"') == 9


def test_plan_source_main_snapshot_guard_reads_only_bronze_sources() -> None:
    import scripts.compact_espn_bronze_v2 as compact

    plan = _plan()

    class Client:
        def __init__(self) -> None:
            self.sql = []

        def query(self, sql):
            self.sql.append(sql)
            return _plan_source_main_rows(plan)

    client = Client()
    compact._assert_plan_source_main_snapshots(client, plan)

    assert len(client.sql) == 1
    assert "iceberg.espn_internal" not in client.sql[0]
    assert client.sql[0].count("WHERE name = 'main' AND type = 'BRANCH'") == 9
    for relation, _snapshot_id in _plan_source_main_rows(plan):
        assert relation.replace("iceberg.bronze.", "") in client.sql[0]


def test_apply_rejects_post_review_source_write_before_first_mutation(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        MigrationStep,
        apply_with_guard,
    )

    plan = _plan()
    stale_rows = _plan_source_main_rows(plan)
    relation, snapshot_id = stale_rows[0]
    stale_rows[0] = (relation, snapshot_id + 1)

    class Client:
        def __init__(self) -> None:
            self.mutations = []

        def query(self, sql):
            if "$refs" in sql:
                return stale_rows
            self.mutations.append(sql)
            return []

    client = Client()
    journal = tmp_path / "post-review-write.json"
    with pytest.raises(Compact6Error, match="source main snapshot drift"):
        apply_with_guard(
            plan=plan,
            steps=(MigrationStep(0, "archive_schedule", "ARCHIVE"),),
            client=client,
            journal_path=journal,
            run_guard=lambda: _run_guard(plan=plan),
            command="apply",
        )

    assert client.mutations == []
    assert not journal.exists()


def test_compaction_apply_checks_source_refs_before_materialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import scripts.compact_espn_bronze_v2 as compact

    plan = _plan()
    rows = _plan_source_main_rows(plan)
    relation, snapshot_id = rows[-1]
    rows[-1] = (relation, snapshot_id + 1)

    class Client:
        def query(self, sql):
            assert "$refs" in sql
            return rows

    class ForbiddenSink:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("materialization setup preceded the source guard")

    monkeypatch.setattr(compact, "IcebergJournalSink", ForbiddenSink)
    with pytest.raises(compact.Compact6Error, match="source main snapshot drift"):
        compact.apply_compaction(
            plan=plan,
            client=Client(),
            run_guard=lambda: _run_guard(plan=plan),
            journal_path=tmp_path / "apply.json",
            manifest_path=tmp_path / "manifest.json",
            command="apply",
        )

    assert list(tmp_path.iterdir()) == []


def test_first_public_swap_rechecks_source_refs_after_preflight(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        MigrationStep,
        apply_with_guard,
    )

    plan = _plan()

    class Client:
        def __init__(self) -> None:
            self.drifted = False
            self.dropped = False
            self.mutations = []
            self.source_checks = 0

        def query(self, sql):
            if "$refs" in sql:
                self.source_checks += 1
                rows = _plan_source_main_rows(plan)
                if self.drifted:
                    relation, snapshot_id = rows[0]
                    rows[0] = (relation, snapshot_id + 1)
                return rows
            if sql == "PREFLIGHT":
                self.mutations.append(sql)
                self.drifted = True
                return []
            if sql == "DROP LEGACY":
                self.mutations.append(sql)
                self.dropped = True
                return []
            if sql == "POST DROP":
                return [(self.dropped,)]
            raise AssertionError(sql)

    client = Client()
    with pytest.raises(Compact6Error, match="source main snapshot drift"):
        apply_with_guard(
            plan=plan,
            steps=(
                MigrationStep(0, "preflight_archive_manifest", "PREFLIGHT"),
                MigrationStep(
                    1,
                    "drop_legacy_main_schedule",
                    "DROP LEGACY",
                    postcondition_sql="POST DROP",
                ),
            ),
            client=client,
            journal_path=tmp_path / "preflight-race.json",
            run_guard=lambda: _run_guard(plan=plan),
            command="apply",
            compensation_steps=(MigrationStep(0, "recover", "RECOVER"),),
        )

    assert client.source_checks == 2
    assert client.mutations == ["PREFLIGHT"]


def test_first_drop_cannot_recover_an_externally_missing_source(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        MigrationStep,
        apply_with_guard,
    )

    plan = _plan()

    class Client:
        def __init__(self) -> None:
            self.missing = False
            self.mutations = []

        def query(self, sql):
            if "$refs" in sql:
                rows = _plan_source_main_rows(plan)
                return rows[1:] if self.missing else rows
            if sql == "PREFLIGHT":
                self.mutations.append(sql)
                self.missing = True
                return []
            if sql == "POST DROP":
                return [(self.missing,)]
            if sql == "DROP LEGACY":
                self.mutations.append(sql)
                return []
            raise AssertionError(sql)

    client = Client()
    journal = tmp_path / "missing-before-drop.json"
    with pytest.raises(Compact6Error, match="source main snapshot drift"):
        apply_with_guard(
            plan=plan,
            steps=(
                MigrationStep(0, "preflight", "PREFLIGHT"),
                MigrationStep(
                    1,
                    "drop_legacy_main_schedule",
                    "DROP LEGACY",
                    postcondition_sql="POST DROP",
                ),
            ),
            client=client,
            journal_path=journal,
            run_guard=lambda: _run_guard(plan=plan),
            command="apply",
            compensation_steps=(MigrationStep(0, "recover", "RECOVER"),),
        )

    interrupted = json.loads(journal.read_text(encoding="utf-8"))
    assert interrupted["started_step"] is None
    with pytest.raises(Compact6Error, match="source main snapshot drift"):
        apply_with_guard(
            plan=plan,
            steps=(
                MigrationStep(0, "preflight", "PREFLIGHT"),
                MigrationStep(
                    1,
                    "drop_legacy_main_schedule",
                    "DROP LEGACY",
                    postcondition_sql="POST DROP",
                ),
            ),
            client=client,
            journal_path=journal,
            run_guard=lambda: _run_guard(plan=plan),
            command="resume",
            compensation_steps=(MigrationStep(0, "recover", "RECOVER"),),
        )

    assert client.mutations == ["PREFLIGHT"]


def test_first_drop_rechecks_after_durable_intent_checkpoint(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        MigrationStep,
        apply_with_guard,
    )

    plan = _plan()

    class Client:
        def __init__(self) -> None:
            self.drifted = False
            self.dropped = False

        def query(self, sql):
            if "$refs" in sql:
                rows = _plan_source_main_rows(plan)
                if self.drifted:
                    relation, snapshot_id = rows[0]
                    rows[0] = (relation, snapshot_id + 1)
                return rows
            if sql == "POST DROP":
                return [(self.dropped,)]
            if sql == "DROP LEGACY":
                self.dropped = True
                return []
            raise AssertionError(sql)

    client = Client()

    class Sink:
        def record(self, journal):
            started = journal.get("started_step")
            if isinstance(started, dict) and started.get("name") == (
                "drop_legacy_main_schedule"
            ):
                client.drifted = True

        def load(self, **_identity):
            return None

    with pytest.raises(Compact6Error, match="source main snapshot drift"):
        apply_with_guard(
            plan=plan,
            steps=(
                MigrationStep(
                    0,
                    "drop_legacy_main_schedule",
                    "DROP LEGACY",
                    postcondition_sql="POST DROP",
                ),
            ),
            client=client,
            journal_path=tmp_path / "drift-after-intent.json",
            run_guard=lambda: _run_guard(plan=plan),
            command="apply",
            compensation_steps=(MigrationStep(0, "recover", "RECOVER"),),
            checkpoint_sink=Sink(),
        )

    assert client.dropped is False


def test_resume_rechecks_after_failure_between_intent_and_drop(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        InjectedFailure,
        MigrationStep,
        apply_with_guard,
    )

    plan = _plan()

    class Client:
        def __init__(self) -> None:
            self.missing = False
            self.dropped = False

        def query(self, sql):
            if "$refs" in sql:
                rows = _plan_source_main_rows(plan)
                return rows[1:] if self.missing else rows
            if sql == "POST DROP":
                return [(self.missing or self.dropped,)]
            if sql == "DROP LEGACY":
                self.dropped = True
                return []
            raise AssertionError(sql)

    step = MigrationStep(
        0,
        "drop_legacy_main_schedule",
        "DROP LEGACY",
        postcondition_sql="POST DROP",
    )
    compensation = (MigrationStep(0, "recover", "RECOVER"),)
    client = Client()
    journal = tmp_path / "failure-before-drop.json"
    with pytest.raises(InjectedFailure, match="after intent before SQL"):
        apply_with_guard(
            plan=plan,
            steps=(step,),
            client=client,
            journal_path=journal,
            run_guard=lambda: _run_guard(plan=plan),
            command="apply",
            fail_before_execute_step=0,
            compensation_steps=compensation,
        )

    interrupted = json.loads(journal.read_text(encoding="utf-8"))
    assert interrupted["started_step"] is None
    client.missing = True
    with pytest.raises(Compact6Error, match="source main snapshot drift"):
        apply_with_guard(
            plan=plan,
            steps=(step,),
            client=client,
            journal_path=journal,
            run_guard=lambda: _run_guard(plan=plan),
            command="resume",
            compensation_steps=compensation,
        )

    assert client.dropped is False


def test_uncheckpointed_first_drop_intent_is_not_postswap_proof(
    tmp_path: Path,
) -> None:
    import scripts.compact_espn_bronze_v2 as compact

    plan = _plan()
    step = compact.MigrationStep(
        0,
        "drop_legacy_main_schedule",
        "DROP LEGACY",
        postcondition_sql="POST DROP",
    )

    class Client:
        def __init__(self) -> None:
            self.ref_checks = 0
            self.dropped = False

        def query(self, sql):
            if "$refs" in sql:
                self.ref_checks += 1
                return _plan_source_main_rows(plan)[1:]
            if sql == "POST DROP":
                return [(True,)]
            if sql == "DROP LEGACY":
                self.dropped = True
                return []
            raise AssertionError(sql)

    journal = tmp_path / "uncheckpointed-drop-intent.json"
    compact._write_journal(
        journal,
        compact._journal_payload(
            {
                "journal_version": compact.JOURNAL_VERSION,
                "transition_id": plan["transition_id"],
                "command": "apply",
                "plan_sha256": plan["plan_sha256"],
                "status": "running",
                "context": {},
                "started_step": {
                    "index": step.index,
                    "name": step.name,
                    "sql_sha256": step.sql_sha256,
                },
                "completed_steps": [],
            }
        ),
    )
    client = Client()
    with pytest.raises(compact.Compact6Error, match="source main snapshot drift"):
        compact.apply_with_guard(
            plan=plan,
            steps=(step,),
            client=client,
            journal_path=journal,
            run_guard=lambda: _run_guard(plan=plan),
            command="resume",
            compensation_steps=(compact.MigrationStep(0, "recover", "RECOVER"),),
        )

    assert client.ref_checks == 1
    assert client.dropped is False


def test_resume_ignores_expected_internal_snapshot_writes(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        InjectedFailure,
        MigrationStep,
        apply_with_guard,
    )

    plan = _plan()

    class Client:
        def __init__(self) -> None:
            self.internal_snapshot_id = plan["state_snapshots"][
                "espn_scope_cutover_v2"
            ]
            self.finished = False
            self.source_checks = 0

        def query(self, sql):
            if "$refs" in sql:
                self.source_checks += 1
                assert "iceberg.espn_internal" not in sql
                return _plan_source_main_rows(plan)
            if sql == "WRITE INTERNAL":
                self.internal_snapshot_id += 1
                return []
            if sql == "POST INTERNAL":
                return [(self.internal_snapshot_id > 204,)]
            if sql == "FINISH":
                self.finished = True
                return []
            raise AssertionError(sql)

    steps = (
        MigrationStep(
            0,
            "append_exact_181_native_routes",
            "WRITE INTERNAL",
            postcondition_sql="POST INTERNAL",
        ),
        MigrationStep(1, "finish_preflight", "FINISH"),
    )
    client = Client()
    journal = tmp_path / "internal-write-resume.json"
    with pytest.raises(InjectedFailure, match="append_exact_181_native_routes"):
        apply_with_guard(
            plan=plan,
            steps=steps,
            client=client,
            journal_path=journal,
            run_guard=lambda: _run_guard(plan=plan),
            command="apply",
            fail_after_step=0,
        )

    result = apply_with_guard(
        plan=plan,
        steps=steps,
        client=client,
        journal_path=journal,
        run_guard=lambda: _run_guard(plan=plan),
        command="resume",
    )

    assert result["status"] == "complete"
    assert client.source_checks == 2
    assert client.finished is True


def test_resume_rejects_source_drift_after_preflight_crash(tmp_path: Path) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        InjectedFailure,
        MigrationStep,
        apply_with_guard,
    )

    plan = _plan()

    class Client:
        def __init__(self) -> None:
            self.rows = _plan_source_main_rows(plan)
            self.finished = False

        def query(self, sql):
            if "$refs" in sql:
                return self.rows
            if sql == "PREFLIGHT":
                return []
            if sql == "FINISH":
                self.finished = True
                return []
            raise AssertionError(sql)

    steps = (
        MigrationStep(0, "preflight", "PREFLIGHT"),
        MigrationStep(1, "finish", "FINISH"),
    )
    client = Client()
    journal = tmp_path / "stale-resume.json"
    with pytest.raises(InjectedFailure, match="preflight"):
        apply_with_guard(
            plan=plan,
            steps=steps,
            client=client,
            journal_path=journal,
            run_guard=lambda: _run_guard(plan=plan),
            command="apply",
            fail_after_step=0,
        )
    relation, snapshot_id = client.rows[0]
    client.rows[0] = (relation, snapshot_id + 1)

    with pytest.raises(Compact6Error, match="source main snapshot drift"):
        apply_with_guard(
            plan=plan,
            steps=steps,
            client=client,
            journal_path=journal,
            run_guard=lambda: _run_guard(plan=plan),
            command="resume",
        )

    assert client.finished is False


def test_resume_revalidates_sources_after_local_journal_loss(tmp_path: Path) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        InjectedFailure,
        MigrationStep,
        apply_with_guard,
    )

    plan = _plan()

    class Sink:
        def __init__(self) -> None:
            self.latest = None

        def record(self, journal):
            self.latest = json.loads(json.dumps(journal))

        def load(self, *, transition_id, plan_sha256, command):
            assert (transition_id, plan_sha256, command) == (
                plan["transition_id"],
                plan["plan_sha256"],
                "apply",
            )
            return self.latest

    class Client:
        def __init__(self) -> None:
            self.rows = _plan_source_main_rows(plan)
            self.finished = False

        def query(self, sql):
            if "$refs" in sql:
                return self.rows
            if sql == "PREFLIGHT":
                return []
            if sql == "FINISH":
                self.finished = True
                return []
            raise AssertionError(sql)

    steps = (
        MigrationStep(0, "preflight", "PREFLIGHT"),
        MigrationStep(1, "finish", "FINISH"),
    )
    client = Client()
    sink = Sink()
    journal = tmp_path / "lost-local-source-guard.json"
    with pytest.raises(InjectedFailure, match="preflight"):
        apply_with_guard(
            plan=plan,
            steps=steps,
            client=client,
            journal_path=journal,
            run_guard=lambda: _run_guard(plan=plan),
            command="apply",
            fail_after_step=0,
            checkpoint_sink=sink,
        )
    journal.unlink()
    relation, snapshot_id = client.rows[-1]
    client.rows[-1] = (relation, snapshot_id + 1)

    with pytest.raises(Compact6Error, match="source main snapshot drift"):
        apply_with_guard(
            plan=plan,
            steps=steps,
            client=client,
            journal_path=journal,
            run_guard=lambda: _run_guard(plan=plan),
            command="resume",
            checkpoint_sink=sink,
        )

    assert client.finished is False


def test_rollback_selects_frozen_emergency_archive_and_never_runtime_v2() -> None:
    from scripts.compact_espn_bronze_v2 import render_rollback_steps

    plan = _plan()
    steps = render_rollback_steps(
        plan, _manifest(plan), reason="downstream parity regression"
    )
    sql = "\n".join(step.sql for step in steps)

    assert "emergency_legacy_v1" in sql
    assert "legacy_main_retained" not in sql
    assert "e12b85a" not in sql
    assert "DROP TABLE iceberg.espn_internal" not in sql
    assert "verify_pinned_disposition_integrity" in {step.name for step in steps}
    assert {
        f"verify_frozen_archive_whole_{entity}"
        for entity in ("schedule", "lineup", "matchsheet")
    }.issubset({step.name for step in steps})
    emergency_sql = "\n".join(
        step.sql for step in steps if step.name.startswith("recreate_frozen_emergency_")
    )
    assert "espn_legacy_disposition_v1 FOR VERSION AS OF" in emergency_sql
    assert "d.disposition IN ('compatibility_only', 'native_current_replaced')" in (
        emergency_sql
    )
    assert "d.league IS NOT NULL" in emergency_sql
    assert "d.season IS NOT NULL" in emergency_sql
    assert 'SELECT l."league", l."season", l."game", l."_batch_id"' in emergency_sql
    assert "quarantined" not in emergency_sql


def test_rollback_reason_is_never_interpolated_into_sql_comments() -> None:
    from scripts.compact_espn_bronze_v2 import render_rollback_steps

    hostile = "operator note\nDROP TABLE iceberg.espn_internal.secret"
    plan = _plan()
    sql = "\n".join(
        step.sql
        for step in render_rollback_steps(plan, _manifest(plan), reason=hostile)
    )
    assert hostile not in sql
    assert "DROP TABLE iceberg.espn_internal.secret" not in sql


def test_emergency_recovery_refuses_tampered_pinned_disposition_before_mutation() -> (
    None
):
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        recover_public_wrappers_to_emergency,
    )

    class Client:
        def __init__(self) -> None:
            self.sql = []

        def query(self, sql):
            self.sql.append(sql)
            if "espn_legacy_disposition_v1 FOR VERSION AS OF" in sql:
                return [(1,)]
            raise AssertionError("recovery mutated after a failed disposition gate")

    plan = _plan()
    client = Client()
    with pytest.raises(Compact6Error, match="pinned_disposition_integrity"):
        recover_public_wrappers_to_emergency(plan, _manifest(plan), client)

    assert len(client.sql) == 1
    assert "CREATE OR REPLACE VIEW" not in client.sql[0]
    assert "DROP TABLE" not in client.sql[0]


def test_repromotion_reuses_verified_current_views_after_logical_rollback() -> None:
    from scripts.compact_espn_bronze_v2 import (
        render_repromotion_steps,
        render_rollback_steps,
    )

    plan = _plan()
    rollback_sql = "\n".join(
        step.sql
        for step in render_rollback_steps(
            plan, _manifest(plan), reason="parity regression"
        )
    )
    repromotion = render_repromotion_steps(plan, _manifest(plan))
    repromotion_sql = "\n".join(step.sql for step in repromotion)

    assert "emergency_legacy_v1" in rollback_sql
    assert all(
        "emergency_legacy_v1" not in step.sql
        for step in repromotion
        if step.name.startswith("publish_")
    )
    assert repromotion_sql.count("SECURITY DEFINER AS") == 3
    assert "espn_internal.espn_schedule_current" in repromotion_sql
    assert "verify_dynamic_exact_181_native_routes" in {
        step.name for step in repromotion
    }
    assert "verify_current_composition_schedule" not in {
        step.name for step in repromotion
    }
    dynamic_gate = next(
        step
        for step in repromotion
        if step.name == "verify_dynamic_exact_181_native_routes"
    ).sql
    assert "iceberg.espn_internal.espn_schedule_generation_v2" in dynamic_gate
    assert "iceberg.espn_internal.espn_lineup_generation_v2" in dynamic_gate
    assert "iceberg.espn_internal.espn_matchsheet_generation_v2" in dynamic_gate
    assert "iceberg.bronze.espn_request_ledger_generation_v2" in dynamic_gate
    assert "espn_internal.espn_request_ledger_generation_v2" not in dynamic_gate
    assert "m.row_counts_json" in dynamic_gate
    assert "m.row_hashes_json" in dynamic_gate
    assert "m.ledger_count" in dynamic_gate
    assert "m.ledger_hash" in dynamic_gate
    assert "capture_publication_evidence" in {step.name for step in repromotion}
    assert {
        "verify_exact_layout_state",
        "verify_exact_archive_manifest",
        "verify_pinned_disposition_integrity",
        "verify_repromotion_serving_nonempty",
    }.issubset({step.name for step in repromotion})
    assert {
        f"verify_pinned_archive_whole_{entity}"
        for entity in ("schedule", "lineup", "matchsheet")
    }.issubset({step.name for step in repromotion})
    assert repromotion[-1].name == "audit_exact_compact6_inventory"


class _CrashAwareClient:
    def __init__(self, steps):
        self.steps = tuple(steps)
        self.executed: list[str] = []
        self.next_execution_index = 0
        self.postconditions = {}
        for step in self.steps:
            if step.postcondition_sql is not None:
                self.postconditions.setdefault(step.postcondition_sql, []).append(step)

    def query(self, sql: str):
        if (
            self.next_execution_index < len(self.steps)
            and sql == self.steps[self.next_execution_index].sql
        ):
            self.executed.append(sql)
            self.next_execution_index += 1
            return []
        postcondition_steps = self.postconditions.get(sql)
        if postcondition_steps is not None:
            done_steps = [
                step for step in postcondition_steps if step.sql in self.executed
            ]
            done = bool(done_steps)
            if postcondition_steps[0].result_must_be_empty:
                return [] if done else [(1,)]
            return [(done,)]
        if any(
            step.capture_result and step.sql == sql and sql in self.executed
            for step in self.steps
        ):
            # A recovered evidence-capture step must query again because its
            # pre-crash result never reached the durable completion checkpoint.
            return []
        raise AssertionError(f"unexpected SQL: {sql[:120]}")


def test_every_actual_step_recovers_crash_between_sql_and_checkpoint(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        InjectedFailure,
        render_apply_steps,
        run_journaled_steps,
    )

    plan = _plan()
    steps = render_apply_steps(plan, _manifest(plan))
    assert steps
    assert all(step.postcondition_sql for step in steps)

    for failed_index in range(len(steps)):
        client = _CrashAwareClient(steps)
        journal = tmp_path / f"actual-{failed_index}.json"
        with pytest.raises(InjectedFailure, match="before checkpoint"):
            run_journaled_steps(
                plan_sha256=plan["plan_sha256"],
                transition_id=plan["transition_id"],
                command="apply",
                steps=steps,
                client=client,
                journal_path=journal,
                fail_after_execute_step=failed_index,
            )
        result = run_journaled_steps(
            plan_sha256=plan["plan_sha256"],
            transition_id=plan["transition_id"],
            command="resume",
            steps=steps,
            client=client,
            journal_path=journal,
        )
        assert result["status"] == "complete"
        assert client.executed == [step.sql for step in steps]


def test_every_actual_step_recovers_crash_after_intent_before_sql(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        InjectedFailure,
        render_apply_steps,
        run_journaled_steps,
    )

    plan = _plan()
    steps = render_apply_steps(plan, _manifest(plan))
    for failed_index in range(len(steps)):
        client = _CrashAwareClient(steps)
        journal = tmp_path / f"intent-{failed_index}.json"
        with pytest.raises(InjectedFailure, match="after intent before SQL"):
            run_journaled_steps(
                plan_sha256=plan["plan_sha256"],
                transition_id=plan["transition_id"],
                command="apply",
                steps=steps,
                client=client,
                journal_path=journal,
                fail_before_execute_step=failed_index,
            )
        result = run_journaled_steps(
            plan_sha256=plan["plan_sha256"],
            transition_id=plan["transition_id"],
            command="resume",
            steps=steps,
            client=client,
            journal_path=journal,
        )
        assert result["status"] == "complete"
        assert client.executed == [step.sql for step in steps]


def test_journal_tamper_and_plan_fork_fail_closed(tmp_path: Path) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        MigrationStep,
        run_journaled_steps,
    )

    step = MigrationStep(0, "one", "CREATE SCHEMA IF NOT EXISTS one")
    journal = tmp_path / "journal.json"
    run_journaled_steps(
        plan_sha256="a" * 64,
        transition_id="transition-1",
        command="apply",
        steps=(step,),
        client=_Client(),
        journal_path=journal,
    )
    raw = json.loads(journal.read_text(encoding="utf-8"))
    raw["completed_steps"][0]["name"] = "tampered"
    journal.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(Compact6Error, match="journal hash"):
        run_journaled_steps(
            plan_sha256="a" * 64,
            transition_id="transition-1",
            command="resume",
            steps=(step,),
            client=_Client(),
            journal_path=journal,
        )

    journal.unlink()
    run_journaled_steps(
        plan_sha256="a" * 64,
        transition_id="transition-1",
        command="apply",
        steps=(step,),
        client=_Client(),
        journal_path=journal,
    )
    with pytest.raises(Compact6Error, match="plan SHA-256 mismatch"):
        run_journaled_steps(
            plan_sha256="b" * 64,
            transition_id="transition-1",
            command="resume",
            steps=(step,),
            client=_Client(),
            journal_path=journal,
        )


def test_resume_recovers_from_durable_sink_when_local_journal_is_lost(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        InjectedFailure,
        MigrationStep,
        run_journaled_steps,
    )

    class Sink:
        def __init__(self):
            self.latest = None

        def record(self, journal):
            self.latest = json.loads(json.dumps(journal))

        def load(self, *, transition_id, plan_sha256, command):
            assert transition_id == "transition-1"
            assert plan_sha256 == "a" * 64
            assert command == "apply"
            return self.latest

    class StatefulClient:
        def __init__(self):
            self.executions = 0

        def query(self, sql):
            if sql == "CREATE SCHEMA IF NOT EXISTS one":
                self.executions += 1
                return []
            if sql == "SELECT TRUE":
                return [(self.executions == 1,)]
            raise AssertionError(sql)

    step = MigrationStep(
        0,
        "one",
        "CREATE SCHEMA IF NOT EXISTS one",
        postcondition_sql="SELECT TRUE",
    )
    journal = tmp_path / "durable.json"
    sink = Sink()
    client = StatefulClient()
    with pytest.raises(InjectedFailure, match="before checkpoint"):
        run_journaled_steps(
            plan_sha256="a" * 64,
            transition_id="transition-1",
            command="apply",
            steps=(step,),
            client=client,
            journal_path=journal,
            fail_after_execute_step=0,
            checkpoint_sink=sink,
        )
    journal.unlink()

    result = run_journaled_steps(
        plan_sha256="a" * 64,
        transition_id="transition-1",
        command="resume",
        steps=(step,),
        client=client,
        journal_path=journal,
        checkpoint_sink=sink,
    )

    assert result["status"] == "complete"
    assert client.executions == 1


def test_precommit_subphase_recovers_from_distinct_durable_checkpoint(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        InjectedFailure,
        MigrationStep,
        run_journaled_steps,
    )

    class Sink:
        def __init__(self):
            self.latest = None

        def record(self, journal):
            self.latest = json.loads(json.dumps(journal))

        def load(self, *, transition_id, plan_sha256, command):
            assert (transition_id, plan_sha256, command) == (
                "transition-1",
                "a" * 64,
                "materialize",
            )
            return self.latest

    class Client:
        def __init__(self):
            self.created = False

        def query(self, sql):
            if sql == "CREATE TABLE archive AS SELECT 1":
                self.created = True
                return []
            if sql == "SELECT archive exists":
                return [(self.created,)]
            raise AssertionError(sql)

    step = MigrationStep(
        0,
        "archive_schedule",
        "CREATE TABLE archive AS SELECT 1",
        postcondition_sql="SELECT archive exists",
    )
    journal = tmp_path / "materialize.json"
    sink = Sink()
    client = Client()
    with pytest.raises(InjectedFailure, match="before checkpoint"):
        run_journaled_steps(
            plan_sha256="a" * 64,
            transition_id="transition-1",
            command="materialize",
            steps=(step,),
            client=client,
            journal_path=journal,
            checkpoint_sink=sink,
            fail_after_execute_step=0,
            journal_context={"phase": "materialize"},
        )
    journal.unlink()

    result = run_journaled_steps(
        plan_sha256="a" * 64,
        transition_id="transition-1",
        command="resume",
        resume_from_command="materialize",
        steps=(step,),
        client=client,
        journal_path=journal,
        checkpoint_sink=sink,
        journal_context={"phase": "materialize"},
    )
    assert result["status"] == "complete"
    assert client.created is True


def test_apply_resume_recovers_bound_manifest_when_both_local_files_are_lost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import scripts.compact_espn_bronze_v2 as compact
    from scrapers.espn.repository import canonical_json

    plan = _plan()
    manifest = json.loads(canonical_json(_manifest(plan)))
    durable = compact._journal_payload(
        {
            "journal_version": compact.JOURNAL_VERSION,
            "transition_id": plan["transition_id"],
            "command": "apply",
            "plan_sha256": plan["plan_sha256"],
            "status": "compensated",
            "context": {"archive_manifest": manifest},
            "started_step": None,
            "completed_steps": [],
        }
    )

    class Sink:
        def __init__(self, *_args, **_kwargs):
            pass

        def load(self, **identity):
            assert identity == {
                "transition_id": plan["transition_id"],
                "plan_sha256": plan["plan_sha256"],
                "command": "apply",
            }
            return durable

    def no_materialization(_plan):
        raise AssertionError("durable full journal must skip source materialization")

    observed = {}

    def finish(**kwargs):
        observed.update(kwargs)
        return {**durable, "status": "complete"}

    monkeypatch.setattr(compact, "IcebergJournalSink", Sink)
    monkeypatch.setattr(compact, "render_materialization_steps", no_materialization)
    monkeypatch.setattr(compact, "render_apply_steps", lambda *_args: ())
    monkeypatch.setattr(compact, "apply_with_guard", finish)

    manifest_path = tmp_path / "lost-manifest.json"
    result = compact.apply_compaction(
        plan=plan,
        client=object(),
        run_guard=lambda: _run_guard(plan=plan),
        journal_path=tmp_path / "lost-journal.json",
        manifest_path=manifest_path,
        command="resume",
    )

    assert result["status"] == "complete"
    assert json.loads(manifest_path.read_text(encoding="utf-8")) == manifest
    assert observed["journal_context"] == {"archive_manifest": manifest}


def test_resume_from_prephase_starts_a_new_full_apply_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import scripts.compact_espn_bronze_v2 as compact

    plan = _plan()
    snapshots = {
        "espn_schedule_legacy_archive_v1": 301,
        "espn_lineup_legacy_archive_v1": 302,
        "espn_matchsheet_legacy_archive_v1": 303,
    }

    class Sink:
        def __init__(self, *_args, **_kwargs):
            pass

        def load(self, *, command, **_identity):
            return {"durable": True} if command == "materialize" else None

    class Client:
        def query(self, sql):
            assert "$refs" in sql
            return _plan_source_main_rows(plan)

    observed = {}

    monkeypatch.setattr(compact, "IcebergJournalSink", Sink)
    monkeypatch.setattr(compact, "_assert_catalog_layout", lambda *_a, **_k: {})
    monkeypatch.setattr(compact, "render_materialization_steps", lambda _plan: ())
    monkeypatch.setattr(
        compact, "render_precommit_validation_steps", lambda *_a, **_k: ()
    )
    monkeypatch.setattr(
        compact, "render_disposition_persistence_steps", lambda *_a, **_k: ()
    )
    monkeypatch.setattr(compact, "render_apply_steps", lambda *_a, **_k: ())
    monkeypatch.setattr(
        compact, "capture_archive_snapshot_ids", lambda *_a, **_k: snapshots
    )
    monkeypatch.setattr(compact, "capture_latest_snapshot_id", lambda *_a: 304)
    monkeypatch.setattr(
        compact,
        "run_journaled_steps",
        lambda **_kwargs: {"status": "complete"},
    )

    def finish(**kwargs):
        observed.update(kwargs)
        return {"status": "complete", "completed_steps": []}

    monkeypatch.setattr(compact, "apply_with_guard", finish)
    result = compact.apply_compaction(
        plan=plan,
        client=Client(),
        run_guard=lambda: _run_guard(plan=plan),
        journal_path=tmp_path / "full.json",
        manifest_path=tmp_path / "manifest.json",
        command="resume",
        clock=lambda: datetime(2026, 8, 8, 12, 30, tzinfo=UTC),
    )
    assert result["status"] == "complete"
    assert observed["command"] == "apply"


def test_empty_journal_table_is_the_only_safe_bootstrap_without_checkpoint() -> None:
    import scripts.compact_espn_bronze_v2 as compact

    class Client:
        def __init__(self, count):
            self.count = count

        def query(self, sql):
            if "information_schema.tables" in sql:
                return [("BASE TABLE",)]
            if "SELECT count(*)" in sql:
                return [(self.count,)]
            raise AssertionError(sql)

    plan = _plan()
    assert compact._empty_journal_bootstrap_exists(Client(0), plan=plan) is True
    with pytest.raises(compact.Compact6Error, match="outside the selected transition"):
        compact._empty_journal_bootstrap_exists(Client(1), plan=plan)


def test_durable_sink_uses_monotonic_sequence_after_compensation_rewind() -> None:
    import scripts.compact_espn_bronze_v2 as compact
    from scrapers.espn.repository import canonical_json, canonical_sha256

    base = compact._journal_payload(
        {
            "journal_version": compact.JOURNAL_VERSION,
            "transition_id": "transition-1",
            "command": "apply",
            "plan_sha256": "a" * 64,
            "status": "running",
            "context": {},
            "started_step": None,
            "completed_steps": [
                {
                    "index": 50,
                    "name": "publish_schedule",
                    "sql_sha256": "b" * 64,
                    "recovered_from_postcondition": False,
                    "result_row_count": 0,
                    "result_sha256": "c" * 64,
                }
            ],
        }
    )
    interrupted = compact._journal_payload(
        {
            **{key: value for key, value in base.items() if key != "journal_sha256"},
            "status": "interrupted",
        }
    )
    compensated = compact._journal_payload(
        {
            **{
                key: value
                for key, value in interrupted.items()
                if key != "journal_sha256"
            },
            "status": "compensated",
            "completed_steps": [],
        }
    )
    recorded_at = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)

    def checkpoint_row(journal, step_index, step_name):
        row_base = {
            "journal_version": compact.JOURNAL_VERSION,
            "transition_id": "transition-1",
            "plan_sha256": "a" * 64,
            "command": "apply",
            "step_index": step_index,
            "step_name": step_name,
            "status": journal["status"],
            "statement_sha256": "b" * 64,
            "recorded_at": recorded_at,
            "detail_json": canonical_json(journal),
        }
        return (*row_base.values(), canonical_sha256(row_base))

    rows = [
        checkpoint_row(interrupted, 50, "publish_schedule"),
        checkpoint_row(compensated, -1, "journal_start"),
    ]

    class Client:
        def query(self, _sql):
            return rows

    recovered = compact.IcebergJournalSink(Client()).load(
        transition_id="transition-1", plan_sha256="a" * 64, command="apply"
    )
    assert recovered == compensated
    assert recovered["checkpoint_sequence"] > interrupted["checkpoint_sequence"]


def test_catalog_singleton_reconstructs_lost_local_manifest(tmp_path: Path) -> None:
    import scripts.compact_espn_bronze_v2 as compact
    from scrapers.espn.layout import ARCHIVE_MANIFEST_COLUMNS
    from scrapers.espn.repository import canonical_json

    plan = _plan()
    manifest = json.loads(canonical_json(_manifest(plan)))
    catalog_row = dict(manifest)
    catalog_row["captured_at"] = datetime.fromisoformat(
        catalog_row["captured_at"]
    ).replace(tzinfo=None)

    class Client:
        def query(self, sql):
            assert "espn_legacy_archive_manifest_v1" in sql
            return [tuple(catalog_row[column] for column in ARCHIVE_MANIFEST_COLUMNS)]

    path = tmp_path / "recovered-manifest.json"
    recovered = compact._load_or_recover_manifest(
        path, plan=plan, client_factory=Client
    )
    assert recovered == manifest
    assert json.loads(path.read_text(encoding="utf-8")) == manifest


def test_cli_has_explicit_rollback_and_repromotion_resume_targets() -> None:
    from scripts.compact_espn_bronze_v2 import build_parser

    parser = build_parser()
    assert (
        parser.parse_args(
            [
                "resume",
                "--resume-operation",
                "rollback",
                "--plan",
                "plan.json",
            ]
        ).resume_operation
        == "rollback"
    )
    assert (
        parser.parse_args(
            [
                "resume",
                "--resume-operation",
                "repromote",
                "--plan",
                "plan.json",
            ]
        ).resume_operation
        == "repromote"
    )
    assert parser.parse_args(["repromote", "--plan", "plan.json"]).command == (
        "repromote"
    )


def test_rollback_resume_recovers_byte_exact_reason_from_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import scripts.compact_espn_bronze_v2 as compact
    from scrapers.espn.repository import canonical_json

    plan = _plan()
    manifest = json.loads(canonical_json(_manifest(plan)))
    reason = "custom rollback: downstream hash Δ"
    journal = compact._journal_payload(
        {
            "journal_version": compact.JOURNAL_VERSION,
            "transition_id": plan["transition_id"],
            "command": "rollback",
            "plan_sha256": plan["plan_sha256"],
            "status": "interrupted",
            "context": {
                "archive_manifest": manifest,
                "rollback_reason": reason,
            },
            "started_step": None,
            "completed_steps": [],
        }
    )
    journal_path = tmp_path / "rollback.json"
    compact._write_journal(journal_path, journal)
    observed = {}

    class Sink:
        def __init__(self, *_args, **_kwargs):
            pass

    monkeypatch.setattr(compact, "IcebergJournalSink", Sink)
    monkeypatch.setattr(compact, "_assert_catalog_layout", lambda *_a, **_k: {})

    def render(_plan, _manifest, *, reason):
        observed["reason"] = reason
        return ()

    monkeypatch.setattr(compact, "render_rollback_steps", render)

    def finish(**kwargs):
        observed["context"] = kwargs["journal_context"]
        return {**journal, "status": "complete"}

    monkeypatch.setattr(compact, "run_journaled_steps", finish)
    result = compact.rollback_with_guard(
        plan=plan,
        manifest=manifest,
        client=object(),
        journal_path=journal_path,
        run_guard=lambda: _run_guard(plan=plan),
        reason="forgotten default",
        command="resume",
    )
    assert result["status"] == "complete"
    assert observed["reason"] == reason
    assert observed["context"]["rollback_reason"] == reason


def test_interrupted_public_swap_automatically_points_all_wrappers_to_emergency(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        MigrationStep,
        apply_with_guard,
    )

    class FailingSwapClient:
        def __init__(self):
            self.sql = []
            self.drop_done = False
            self.emergency_ready = set()
            self.kinds = {
                f"espn_{entity}": "BASE TABLE"
                for entity in ("schedule", "lineup", "matchsheet")
            }

        def query(self, sql):
            self.sql.append(sql)
            if "$refs" in sql:
                return _plan_source_main_rows(plan)
            if sql == "BROKEN PUBLISH":
                raise RuntimeError("injected publish failure")
            if sql == "DROP TABLE legacy_schedule":
                self.drop_done = True
                return []
            if sql == "SELECT DROP DONE":
                return [(self.drop_done,)]
            if sql == "SELECT PUBLISH DONE":
                return [(False,)]
            if "VIEW iceberg.espn_internal.espn_" in sql and (
                "_emergency_legacy_v1 SECURITY DEFINER AS" in sql
            ):
                self.emergency_ready.add(
                    next(
                        entity
                        for entity in ("schedule", "lineup", "matchsheet")
                        if f"espn_{entity}_emergency" in sql
                    )
                )
                return []
            if "information_schema.views" in sql and "_emergency_legacy_v1" in sql:
                entity = next(
                    entity
                    for entity in ("schedule", "lineup", "matchsheet")
                    if f"espn_{entity}_emergency" in sql
                )
                return [(entity in self.emergency_ready,)]
            if "_emergency_legacy_v1\nHAVING COUNT(*)" in sql:
                return []
            if "espn_legacy_disposition_v1 FOR VERSION AS OF" in sql and sql.startswith(
                "SELECT 1"
            ):
                return []
            if "_legacy_archive_v1 FOR VERSION AS OF" in sql and (
                "HAVING COUNT(*)" in sql or "EXCEPT ALL" in sql
            ):
                return []
            if sql.startswith("WITH definition_match AS"):
                return [(True,)]
            for relation in self.kinds:
                if (
                    "SELECT table_type FROM iceberg.information_schema.tables" in sql
                    and f"table_name = '{relation}'" in sql
                ):
                    kind = self.kinds[relation]
                    return [] if kind is None else [(kind,)]
                if sql == f"DROP TABLE iceberg.bronze.{relation}":
                    if self.kinds[relation] != "BASE TABLE":
                        raise AssertionError(f"unsafe table drop for {relation}")
                    self.kinds[relation] = None
                    return []
                if f"VIEW iceberg.bronze.{relation} SECURITY DEFINER AS" in sql:
                    if self.kinds[relation] == "BASE TABLE":
                        raise AssertionError(f"view over live base {relation}")
                    self.kinds[relation] = "VIEW"
                    return []
            return []

    steps = (
        MigrationStep(
            0,
            "drop_legacy_main_schedule",
            "DROP TABLE legacy_schedule",
            postcondition_sql="SELECT DROP DONE",
        ),
        MigrationStep(
            1,
            "publish_schedule",
            "BROKEN PUBLISH",
            postcondition_sql="SELECT PUBLISH DONE",
        ),
    )
    client = FailingSwapClient()
    plan = _plan()
    with pytest.raises(RuntimeError, match="publish failure"):
        apply_with_guard(
            plan=plan,
            steps=steps,
            client=client,
            journal_path=tmp_path / "swap.json",
            run_guard=_run_guard,
            emergency_manifest=_manifest(plan),
        )

    compensated = "\n".join(client.sql)
    assert compensated.count("emergency_legacy_v1") >= 3
    assert "legacy_main_retained" not in compensated
    assert set(client.kinds.values()) == {"VIEW"}


def test_compensated_public_swap_rewinds_and_successfully_resumes(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        InjectedFailure,
        MigrationStep,
        apply_with_guard,
    )

    entities = ("schedule", "lineup", "matchsheet")

    class SwapClient:
        def __init__(self):
            self.kinds = {f"espn_{entity}": "BASE TABLE" for entity in entities}
            self.targets = {f"espn_{entity}": "legacy" for entity in entities}
            self.unsafe_drops: list[str] = []
            self.emergency_ready = set()

        def query(self, sql):
            if "$refs" in sql:
                return _plan_source_main_rows(plan)
            if "VIEW iceberg.espn_internal.espn_" in sql and (
                "_emergency_legacy_v1 SECURITY DEFINER AS" in sql
            ):
                self.emergency_ready.add(
                    next(
                        entity
                        for entity in entities
                        if f"espn_{entity}_emergency" in sql
                    )
                )
                return []
            if "information_schema.views" in sql and "_emergency_legacy_v1" in sql:
                entity = next(
                    entity for entity in entities if f"espn_{entity}_emergency" in sql
                )
                return [(entity in self.emergency_ready,)]
            if "_emergency_legacy_v1\nHAVING COUNT(*)" in sql:
                return []
            if "espn_legacy_disposition_v1 FOR VERSION AS OF" in sql and sql.startswith(
                "SELECT 1"
            ):
                return []
            if "_legacy_archive_v1 FOR VERSION AS OF" in sql and (
                "HAVING COUNT(*)" in sql or "EXCEPT ALL" in sql
            ):
                return []
            if sql.startswith("WITH definition_match AS"):
                return [(True,)]
            for entity in entities:
                relation = f"espn_{entity}"
                if sql == f"DROP {entity}":
                    if self.kinds[relation] != "BASE TABLE":
                        self.unsafe_drops.append(relation)
                    self.kinds[relation] = None
                    return []
                if sql == f"PUBLISH {entity}":
                    self.kinds[relation] = "VIEW"
                    self.targets[relation] = "current"
                    return []
                if sql == f"POST DROP {entity}":
                    return [(self.kinds[relation] != "BASE TABLE",)]
                if sql == f"POST PUBLISH {entity}":
                    return [(self.targets[relation] == "current",)]
                if (
                    "SELECT table_type FROM iceberg.information_schema.tables" in sql
                    and f"table_name = '{relation}'" in sql
                ):
                    kind = self.kinds[relation]
                    return [] if kind is None else [(kind,)]
                if sql == f"DROP TABLE iceberg.bronze.{relation}":
                    if self.kinds[relation] != "BASE TABLE":
                        self.unsafe_drops.append(relation)
                    self.kinds[relation] = None
                    return []
                if f"VIEW iceberg.bronze.{relation} SECURITY DEFINER AS" in sql:
                    self.kinds[relation] = "VIEW"
                    self.targets[relation] = "emergency"
                    return []
            raise AssertionError(sql)

    steps = tuple(
        step
        for index, entity in enumerate(entities)
        for step in (
            MigrationStep(
                index * 2,
                f"drop_legacy_main_{entity}",
                f"DROP {entity}",
                postcondition_sql=f"POST DROP {entity}",
            ),
            MigrationStep(
                index * 2 + 1,
                f"publish_{entity}",
                f"PUBLISH {entity}",
                postcondition_sql=f"POST PUBLISH {entity}",
            ),
        )
    )
    client = SwapClient()
    plan = _plan()
    journal = tmp_path / "compensated-resume.json"
    with pytest.raises(InjectedFailure, match="publish_schedule"):
        apply_with_guard(
            plan=plan,
            steps=steps,
            client=client,
            journal_path=journal,
            run_guard=_run_guard,
            command="apply",
            fail_after_step=1,
            emergency_manifest=_manifest(plan),
        )
    interrupted = json.loads(journal.read_text(encoding="utf-8"))
    assert interrupted["status"] == "compensated"
    assert interrupted["completed_steps"] == []
    assert set(client.targets.values()) == {"emergency"}

    result = apply_with_guard(
        plan=plan,
        steps=steps,
        client=client,
        journal_path=journal,
        run_guard=_run_guard,
        command="resume",
        emergency_manifest=_manifest(plan),
    )
    assert result["status"] == "complete"
    assert set(client.targets.values()) == {"current"}
    assert client.unsafe_drops == []


def test_compensating_intent_survives_local_loss_and_rewinds_final_step(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        MigrationStep,
        apply_with_guard,
    )

    class MemorySink:
        def __init__(self) -> None:
            self.checkpoints = []

        def record(self, journal):
            self.checkpoints.append(json.loads(json.dumps(journal)))

        def load(self, *, transition_id, plan_sha256, command):
            matches = [
                checkpoint
                for checkpoint in self.checkpoints
                if checkpoint["transition_id"] == transition_id
                and checkpoint["plan_sha256"] == plan_sha256
                and checkpoint["command"] == command
            ]
            return matches[-1] if matches else None

    class Client:
        def __init__(self) -> None:
            self.target = "legacy"
            self.compensation_count = 0

        def query(self, sql):
            if "$refs" in sql:
                return _plan_source_main_rows(plan)
            if sql == "DROP LEGACY":
                self.target = "missing"
                return []
            if sql == "POST DROP":
                return [(self.target != "legacy",)]
            if sql == "PUBLISH CURRENT":
                self.target = "current"
                return []
            if sql == "POST PUBLISH":
                return [(self.target == "current",)]
            if sql == "AUDIT":
                return []
            if sql == "POST AUDIT":
                return [] if self.target == "current" else [(self.target,)]
            if sql == "PUBLISH EMERGENCY":
                self.target = "emergency"
                self.compensation_count += 1
                return []
            if sql == "POST EMERGENCY":
                return [(self.target == "emergency",)]
            raise AssertionError(sql)

    steps = (
        MigrationStep(
            0,
            "drop_legacy_main_schedule",
            "DROP LEGACY",
            postcondition_sql="POST DROP",
        ),
        MigrationStep(
            1,
            "publish_schedule",
            "PUBLISH CURRENT",
            postcondition_sql="POST PUBLISH",
        ),
        MigrationStep(
            2,
            "audit_exact_compact6_inventory",
            "AUDIT",
            postcondition_sql="POST AUDIT",
            result_must_be_empty=True,
        ),
    )
    compensation = (
        MigrationStep(
            0,
            "publish_emergency_schedule",
            "PUBLISH EMERGENCY",
            postcondition_sql="POST EMERGENCY",
        ),
    )
    client = Client()
    sink = MemorySink()
    plan = _plan()
    journal = tmp_path / "compensating-final-step.json"

    with pytest.raises(Compact6Error, match="compensation failed"):
        apply_with_guard(
            plan=plan,
            steps=steps,
            client=client,
            journal_path=journal,
            run_guard=_run_guard,
            command="apply",
            fail_after_step=2,
            fail_after_compensation=True,
            compensation_steps=compensation,
            checkpoint_sink=sink,
        )

    interrupted = json.loads(journal.read_text(encoding="utf-8"))
    assert interrupted["status"] == "compensating"
    assert sink.checkpoints[-1]["status"] == "compensating"
    assert client.target == "emergency"
    journal.unlink()

    result = apply_with_guard(
        plan=plan,
        steps=steps,
        client=client,
        journal_path=journal,
        run_guard=_run_guard,
        command="resume",
        compensation_steps=compensation,
        checkpoint_sink=sink,
    )

    assert result["status"] == "complete"
    assert client.target == "current"
    assert client.compensation_count == 2
    assert any(row["status"] == "compensated" for row in sink.checkpoints)


def test_guard_failure_before_first_drop_never_triggers_compensation(
    tmp_path: Path,
) -> None:
    from scripts.compact_espn_bronze_v2 import (
        Compact6Error,
        MigrationStep,
        apply_with_guard,
    )

    calls = 0

    def guard():
        nonlocal calls
        calls += 1
        return _run_guard(
            active_runs=[]
            if calls == 1
            else [{"dag_id": "dag_ingest_espn", "run_id": "late"}]
        )

    client = _Client()
    plan = _plan()
    with pytest.raises(Compact6Error, match="zero active runs"):
        apply_with_guard(
            plan=plan,
            steps=(
                MigrationStep(
                    0,
                    "drop_legacy_main_schedule",
                    "DROP TABLE iceberg.bronze.espn_schedule",
                    postcondition_sql="SELECT FALSE",
                ),
            ),
            client=client,
            journal_path=tmp_path / "late-active-run.json",
            run_guard=guard,
            command="apply",
            emergency_manifest=_manifest(plan),
        )
    assert client.sql
    assert all("$refs" in sql for sql in client.sql)


def test_dynamic_archive_snapshot_capture_is_exact() -> None:
    from scripts.compact_espn_bronze_v2 import capture_archive_snapshot_ids

    class SnapshotClient:
        def query(self, sql):
            if "schedule_legacy_archive_v1$refs" in sql:
                return [(301,)]
            if "lineup_legacy_archive_v1$refs" in sql:
                return [(302,)]
            if "matchsheet_legacy_archive_v1$refs" in sql:
                return [(303,)]
            raise AssertionError(sql)

    assert capture_archive_snapshot_ids(SnapshotClient()) == {
        "espn_schedule_legacy_archive_v1": 301,
        "espn_lineup_legacy_archive_v1": 302,
        "espn_matchsheet_legacy_archive_v1": 303,
    }


def test_catalog_plan_capture_pins_all_sources_and_exact_181_heads() -> None:
    from scrapers.espn.layout import REVIEWED_NATIVE_REPLACEMENTS
    from scripts.compact_espn_bronze_v2 import capture_plan_from_catalog

    replacements = {scope for scope, _league, _season in REVIEWED_NATIVE_REPLACEMENTS}
    targets = sorted(replacements | {f"{10000 + index}:2026" for index in range(175)})

    class CaptureClient:
        def __init__(self):
            self.sql = []

        def query(self, sql):
            self.sql.append(sql)
            if "information_schema.tables" in sql:
                from scrapers.espn.layout import LEGACY14_PUBLIC_OBJECTS

                return [
                    ("bronze", name, kind)
                    for name, kind in LEGACY14_PUBLIC_OBJECTS.items()
                ]
            if "$refs" in sql:
                table = sql.split('"')[1].split("$")[0]
                snapshot_by_table = {
                    "espn_schedule": 101,
                    "espn_lineup": 102,
                    "espn_matchsheet": 103,
                    "espn_schedule_generation_v2": 201,
                    "espn_lineup_generation_v2": 202,
                    "espn_matchsheet_generation_v2": 203,
                    "espn_scope_cutover_v2": 204,
                    "espn_legacy_baseline_v2": 205,
                    "espn_ingest_manifest_v2": 206,
                }
                return [(snapshot_by_table[table],)]
            if "information_schema.columns" in sql:
                if "espn_schedule'" in sql:
                    names = ("league", "season", "game", "_batch_id")
                elif "espn_lineup'" in sql:
                    names = (
                        "league",
                        "season",
                        "game",
                        "team",
                        "player",
                        "_batch_id",
                    )
                else:
                    names = ("league", "season", "game", "team", "_batch_id")
                return [(name, "varchar") for name in names]
            if "GROUP BY league" in sql:
                rows = [
                    (league, season, 1, "1" * 64, 1)
                    for _scope, league, season in REVIEWED_NATIVE_REPLACEMENTS
                ]
                if "espn_schedule FOR VERSION" in sql:
                    rows.append(("ENG-Premier League", "0001", 1, "2" * 64, 1))
                return rows
            if "COUNT(*) row_count" in sql:
                row_count = 7 if "espn_schedule FOR VERSION" in sql else 6
                return [(row_count, "3" * 64, row_count)]
            if "SELECT scope_id, cutover_id" in sql:
                return [
                    (
                        scope,
                        f"promotion-{index + 1}",
                        f"{index + 1:x}" * 64,
                        "native",
                        "legacy",
                        league,
                        season,
                        "a" * 64,
                        f"generation-{targets.index(scope)}",
                        "e" * 64,
                        "f" * 64,
                    )
                    for index, (scope, league, season) in enumerate(
                        REVIEWED_NATIVE_REPLACEMENTS
                    )
                ]
            if "WITH complete_manifests AS" in sql:
                return [
                    (
                        scope,
                        f"generation-{index}",
                        "e" * 64,
                        "f" * 64,
                        "s3://raw/espn/registry.json.gz",
                        "a" * 64,
                        "espn-native-parser-v3",
                        "espn-native-runtime-v4",
                        datetime(2026, 8, 8, 11, tzinfo=UTC),
                    )
                    for index, scope in enumerate(targets)
                ]
            if "FROM iceberg.bronze.espn_legacy_baseline_v2" in sql:
                columns = (
                    "baseline_version",
                    "scope_id",
                    "legacy_league",
                    "legacy_season",
                    "captured_at",
                    "entity_metrics_json",
                    "legacy_snapshot_ids_json",
                    "registry_signature",
                    "durable_manifest_uri",
                    "durable_manifest_sha256",
                    "replay_raw_manifest_uri",
                    "replay_raw_manifest_sha256",
                    "trust_label",
                    "baseline_sha256",
                )
                output = []
                for index, (scope, league, season) in enumerate(
                    REVIEWED_NATIVE_REPLACEMENTS
                ):
                    row = _baseline_row(scope, league, season, index)
                    row["captured_at"] = datetime.fromisoformat(row["captured_at"])
                    output.append(tuple(row[column] for column in columns))
                return output
            raise AssertionError(sql)

    client = CaptureClient()
    plan = capture_plan_from_catalog(
        client,
        transition_id="compact6-20260808-a1b2c3d4",
        registry_snapshot_uri="s3://raw/espn/registry.json.gz",
        registry_snapshot_sha256="9" * 64,
        registry_signature="a" * 64,
        target_scope_ids=targets,
        created_at=datetime(2026, 8, 8, 12, tzinfo=UTC),
    )

    assert plan["native_scope_count"] == 181
    assert len(plan["native_routes"]) == 181
    assert plan["manifest_snapshot_id"] == 206
    assert any('"espn_schedule$refs"' in sql for sql in client.sql)
    assert not any("$snapshots" in sql for sql in client.sql)
    assert all("FOR VERSION AS OF" in sql for sql in client.sql if "row_count" in sql)
    assert any("FOR VERSION AS OF 206" in sql for sql in client.sql)
