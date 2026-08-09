from pathlib import Path


RUNBOOK = Path("docs/operations/espn-native-v2.md")
RELEASE_GUARD = Path("scripts/espn_release_guard_v1.py")


def test_runbook_covers_every_operator_path_and_retention_boundary():
    text = RUNBOOK.read_text(encoding="utf-8")

    for required in (
        "dag_discover_espn_registry",
        "dag_ingest_espn",
        "dag_replay_espn",
        "dag_repair_espn",
        "dag_monitor_espn",
        "scripts/audit_espn_repair.py",
        "scripts/migrate_espn_native_v2.py",
        "deploy/espn/deploy.py apply",
        "90 дней",
        "365 дней",
        "бессрочно",
        "Three scheduler-created parent/child receipts",
        "legacy_untrusted",
        "append-only",
    ):
        assert required in text


def test_runbook_makes_release_and_compact6_plans_reviewable_before_apply():
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "deploy/espn/deploy.py plan" in text
    assert "deploy/espn/deploy.py apply" in text
    assert "deploy/espn/deploy.py resume" in text
    assert "canonical `plan_sha256`" in text
    assert "строго non-mutating" in text
    assert "restore proof" in text
    assert "scripts/compact_espn_bronze_v2.py plan" in text
    assert "scripts/compact_espn_bronze_v2.py apply" in text
    assert "не удаляет legacy" in text
    assert "DROP TABLE" not in text


def test_runbook_contracts_ordered_all_male_rollout_and_reversal():
    text = " ".join(RUNBOOK.read_text(encoding="utf-8").split())
    rollout_steps = text.split("## Gated production ceremony", 1)[1].split(
        "## Replay и repair", 1
    )[0]

    for required in (
        "explicit-core-gender-MALE-v1",
        "2026-08-02: 181 MALE / 38 FEMALE / 1 UNKNOWN",
        "zero duplicate IDs/slugs",
        "last good immutable discovery-state ref",
        "`latest-state.json` никогда не является registry запуска",
        "dag_trigger_espn_daily",
        "ESPN_DISCOVERY_STATE_REF_URI",
        "ESPN_DISCOVERY_STATE_REF_SHA256",
        "deploy/espn/airflow.compose.yaml",
        "scripts/build_espn_dagbag_projection.py",
        "ESPN_ISOLATED_STACK=1",
        "dag_master_pipeline",
        "--force-recreate",
        "ESPN_AIRFLOW_DATABASE_URL",
        "ESPN_CONTROL_DATABASE_URL",
        "второй reviewed deploy transition",
        "временно unpause только `dag_discover_espn_registry`",
        "third reviewed deploy transition",
        "fourth reviewed deploy transition",
        "временно unpause **только** `dag_backfill_espn`",
        "posture намеренно hard-red для probe",
        "тем же global `stack_lock_root`",
        "--wait-timeout",
        "scripts/verify_espn_database_topology.py",
        "connected server/database identity",
        "пока обе переменные атомарно не удалены или не заменены",
        "181/181 v3/v4 heads",
        "espn-native-parser-v3",
        "espn-native-runtime-v4",
        "e12b85a",
        "zero isolated active DagRuns",
        "ordinal001",
        "`canary_campaign=null`",
        "sealed successful ordinal001 campaign artifact",
        "401863559",
        "401863560",
        "401863562",
        "401863563",
        "401863564",
        "captured",
        "valid_empty",
        "not_applicable",
        "exact 48h",
        "compact6",
        "exactly six public Bronze objects",
        "E3/xref/Gold",
        "secret-safe",
    ):
        assert required in text

    assert "airflow dags trigger dag_trigger_espn_daily" not in rollout_steps
    assert "docker compose restart" not in rollout_steps
    assert "ровно один `scope_id`" not in rollout_steps

    ordered_markers = (
        "1. Reviewed deploy plan and restore proof",
        "2. Fresh campaign ordinal001",
        "3. Full 181-scope v2→v3 reconciliation",
        "4. Exact 181/181 v3/v4 gate",
        "5. All-181 canary",
        "6. Three scheduler-created parent/child receipts",
        "7. compact6 ACL and rollback proof",
        "8. One post-cutover scheduled cycle",
        "9. Six-scope E3/xref/Gold reconciliation",
        "10. Rollback and secret-safe security evidence",
    )
    cursor = 0
    for marker in ordered_markers:
        position = rollout_steps.find(marker, cursor)
        assert position >= 0, marker
        cursor = position + len(marker)


def test_runbook_pause_posture_is_transient_and_returns_to_all_paused():
    text = " ".join(RUNBOOK.read_text(encoding="utf-8").split())

    assert "13:50–14:15 UTC" in text
    assert "ingest → monitor → discovery → parent" in text
    assert "parent снова paused" in text
    assert "exact derived child" in text
    assert "все семь DAG-ов paused" in text
    assert "четыре DAG-а постоянно unpaused" not in text


def test_runbook_documents_six_guard_attempts_and_probe_contract():
    text = RUNBOOK.read_text(encoding="utf-8")

    for phase in (
        "initial_state",
        "pre_backup",
        "pre_checkpoint_mutation",
        "pre_airflow_init",
        "pre_recreate",
        "post_deploy",
    ):
        assert phase in text
    assert "1,800 секунд" in text
    assert "10,800 секунд" in text
    assert "с интервалом не более 60 секунд" in text
    assert "started-only" in text
    assert "scripts/espn_rollout_probe_v1.py" in text
    assert "--snapshot /protected/read-only/espn-rollout-snapshot.json" in text

    canary = text.split("### 5. All-181 canary", 1)[1].split(
        "### 6. Three scheduler-created parent/child receipts", 1
    )[0]
    assert "entry DAG `dag_backfill_espn`" in canary
    assert "entry DAG `dag_ingest_espn`" not in canary


def test_runbook_binds_the_packaged_readonly_guard_and_docker_artifacts():
    text = RUNBOOK.read_text(encoding="utf-8")
    source = RELEASE_GUARD.read_text(encoding="utf-8")

    assert RELEASE_GUARD.is_file()
    assert "scripts/espn_release_guard_v1.py" in text
    assert "espn-release-guard-<sha>.py" not in text
    assert 'export ESPN_DOCKER="/usr/bin/docker"' in text
    assert '\\"--poll-seconds\\",\\"15\\",\\"--max-wait-seconds\\",\\"1740\\"' in text
    assert '--guard-artifact "$ESPN_RELEASE_GUARD"' in text
    assert '--guard-artifact "$ESPN_DOCKER"' in text
    assert "ESPN_DEPLOY_GUARD_PHASE" in source
    assert "ESPN_DEPLOY_GUARD_ATTEMPT" in source
    assert "ESPN_DEPLOY_TRANSITION_ID" in source
    assert "ESPN_DEPLOY_PLAN_SHA256" in source
    assert "BEGIN TRANSACTION READ ONLY" in source
    assert "MAX_WAIT_SECONDS = 1_740" in source


def test_runbook_has_exact_crash_recoverable_shared_canary_cli_commands():
    text = RUNBOOK.read_text(encoding="utf-8")

    for required in (
        "ESPN_CANARY_STATE_ROOT=/durable/espn/canary-state",
        "-m scripts.espn_canary_campaign claim",
        "-m scripts.espn_canary_campaign finish",
        "-m scripts.espn_canary_campaign recover",
        '--target-scopes "$ESPN_CANARY_TARGETS"',
        '--ledger-path "$ESPN_CANARY_LEDGER"',
        "--successful",
        "--failed",
        "--predecessor-failure-uri",
        "--predecessor-failure-sha256",
        "--remediation",
        "максимум `ordinal003`",
        "immutable evidence раньше active ledger",
        "одинаковый absolute `file://` URI",
    ):
        assert required in text
