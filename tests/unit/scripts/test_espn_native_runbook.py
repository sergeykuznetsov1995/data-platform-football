from pathlib import Path


RUNBOOK = Path("docs/operations/espn-native-v2.md")


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
        "--apply",
        "90 дней",
        "365 дней",
        "бессрочно",
        "три последовательных зелёных",
        "legacy_untrusted",
        "append-only",
    ):
        assert required in text


def test_runbook_makes_dry_run_and_one_scope_cutover_explicit():
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "по умолчанию dry-run" in text
    assert "ровно один `scope_id`" in text
    assert "не удаляет legacy" in text
    assert "DROP TABLE" not in text


def test_runbook_contracts_automatic_all_male_rollout_and_reversal():
    text = " ".join(RUNBOOK.read_text(encoding="utf-8").split())
    automatic_rollout = text.split("## Automatic all-male rollout", 1)[1].split(
        "## Canary и три зелёных запуска", 1
    )[0]

    for required in (
        "explicit-core-gender-MALE-v1",
        "2026-08-02: 181 MALE / 38 FEMALE / 1 UNKNOWN",
        "первые 10 отсутствующих scope",
        "exact coverage reconciliation",
        "target_scope_ids - COMPLETE scope_head_v2",
        "zero duplicate IDs/slugs",
        "last good immutable discovery-state ref",
        "latest-state.json никогда не является registry запуска",
        "dag_trigger_espn_daily",
        "три новых scheduled green",
        "ESPN_DISCOVERY_STATE_REF_URI",
        "ESPN_DISCOVERY_STATE_REF_SHA256",
        "dag_backfill_espn",
        "explicit <=10 cohort",
        "airflow dags pause dag_discover_espn_registry",
        'state["candidate_ref"]',
        'state["male_registry_ref"]',
        '"MALE": 181, "FEMALE": 38, "UNKNOWN": 1',
    ):
        assert required in text

    assert "airflow dags trigger dag_trigger_espn_daily" not in automatic_rollout
    assert "WHERE dag_id" not in automatic_rollout
