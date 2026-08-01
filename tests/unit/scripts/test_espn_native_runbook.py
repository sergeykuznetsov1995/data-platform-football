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
