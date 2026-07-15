from __future__ import annotations

import fcntl
import os
from pathlib import Path
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[3]


def test_cutover_is_quiesced_backup_gated_and_ordered() -> None:
    script = (ROOT / "scripts/cutover_seaweedfs_topology.sh").read_text()

    assert "SEAWEEDFS_CUTOVER_CONFIRM" in script
    assert "capacity-check" in script
    assert "whoscored_raw_backup.py preflight" in script
    assert "SEAWEEDFS_CUTOVER_REHEARSAL_INVENTORY" in script
    stop_marker = "Stopping every storage writer before the recovery cut"
    assert script.index("Verifying rehearsal backup access before downtime") < (
        script.index(stop_marker)
    )
    assert script.index("capacity-check") < script.index(stop_marker)
    assert "verify-backup" in script
    assert "verify-restore" in script
    assert script.index("stop --timeout 120 seaweedfs") < script.index(
        "up -d --no-deps seaweedfs-master"
    )
    assert script.index("legacy_running=") < script.index(
        "up -d --no-deps seaweedfs-master"
    )
    assert "Legacy weed mini is still running" in script
    permission_probe = 'test -w "$(dirname "$1")"'
    assert "install -d -o 50000 -g 0 -m 0770" in script
    assert script.index(permission_probe) < script.index(stop_marker)
    assert 'python3 - "${host_run_dir}/inventory.json"' not in script
    assert script.index("up -d --no-deps seaweedfs-master") < script.index(
        "up -d --no-deps seaweedfs-volume"
    )
    assert script.index("up -d --no-deps seaweedfs-volume") < script.index(
        "up -d --no-deps seaweedfs-filer"
    )
    assert "all storage writers are stopped" in script
    assert 'stop --timeout 120 "${writers[@]}"' in script
    assert "writer shutdown could not be confirmed" in script
    secret_mount = script.split("/tmp/s3.config.json:ro", 1)[0].rsplit(
        '"${compose[@]}" run', 1
    )[1]
    assert "--user 0:0" in secret_mount


def test_cutover_runbook_has_measured_rehearsal_and_empty_volume_recovery() -> None:
    runbook = (ROOT / "docs" / "operations" / "seaweedfs-topology-cutover.md").read_text(
        encoding="utf-8"
    )

    assert "inventory-metrics.json" in runbook
    assert "backup-metrics.json" in runbook
    assert "verify-backup-metrics.json" in runbook
    assert "verify-source-metrics.json" in runbook
    assert "mib_per_second" in runbook
    assert "full-cutover-wall-seconds.txt" in runbook
    assert "SEAWEEDFS_DATA_VOLUME_NAME" in runbook
    assert 'docker volume inspect "$FAILED_SEAWEEDFS_DATA_VOLUME_NAME"' in runbook
    assert "docker volume create" in runbook
    assert "list-inventories" in runbook
    assert "fetch-inventory" in runbook
    assert "--create-bucket" in runbook
    assert "local recovery Admin/Write access key is required" in runbook
    recovery_block = runbook.rsplit("List only full-warehouse markers", 1)[1]
    assert 'WHOSCORED_BACKUP_RESTORE_S3_ACCESS_KEY="$S3_ACCESS_KEY"' in recovery_block
    assert "WHOSCORED_BACKUP_SOURCE_S3_ACCESS_KEY" not in recovery_block
    assert runbook.index("fetch-inventory") < runbook.rindex("verify-restore")


def test_cutover_rejects_group_readable_s3_credentials(tmp_path) -> None:
    script_path = ROOT / "scripts" / "cutover_seaweedfs_topology.sh"
    config = tmp_path / "configs" / "seaweedfs" / "s3.config.json"
    config.parent.mkdir(parents=True)
    config.write_text("{}", encoding="utf-8")
    config.chmod(0o640)
    result = subprocess.run(
        ["bash", str(script_path)],
        cwd=tmp_path,
        env={
            **os.environ,
            "SEAWEEDFS_CUTOVER_LOCK_FILE": str(tmp_path / "cutover.lock"),
            "SEAWEEDFS_CUTOVER_CONFIRM": "backup-and-downtime-approved",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "must not grant group/other permissions" in result.stderr


def test_cutover_trap_never_claims_safe_when_docker_ps_fails(tmp_path) -> None:
    script_path = ROOT / "scripts" / "cutover_seaweedfs_topology.sh"
    config = tmp_path / "configs" / "seaweedfs" / "s3.config.json"
    config.parent.mkdir(parents=True)
    config.write_text("{}", encoding="utf-8")
    config.chmod(0o600)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    state = tmp_path / "ps-count"
    state.write_text("0", encoding="utf-8")
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -eu
            if [[ "${1:-}" == "inspect" ]]; then
              echo '["mini"]'
              exit 0
            fi
            [[ "${1:-}" == "compose" ]] || exit 64
            shift
            case "${1:-}" in
              ps)
                count="$(cat "${FAKE_DOCKER_STATE}")"
                count=$((count + 1))
                printf '%s' "${count}" > "${FAKE_DOCKER_STATE}"
                if ((count == 1)); then
                  printf '%s\n' seaweedfs airflow-scheduler
                  exit 0
                fi
                exit 42
                ;;
              run) exit 0 ;;
              stop) exit 41 ;;
              *) exit 64 ;;
            esac
            """
        ),
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)

    result = subprocess.run(
        ["bash", str(script_path)],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "FAKE_DOCKER_STATE": str(state),
            "SEAWEEDFS_CUTOVER_LOCK_FILE": str(tmp_path / "cutover.lock"),
            "SEAWEEDFS_CUTOVER_CONFIRM": "backup-and-downtime-approved",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "writer shutdown could not be confirmed" in result.stderr
    assert "all storage writers are stopped" not in result.stderr


def test_second_cutover_process_is_rejected_by_host_lock(tmp_path) -> None:
    script_path = ROOT / "scripts" / "cutover_seaweedfs_topology.sh"
    lock_path = tmp_path / "cutover.lock"
    with lock_path.open("w", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = subprocess.run(
            ["bash", str(script_path)],
            cwd=ROOT,
            env={
                **os.environ,
                "SEAWEEDFS_CUTOVER_LOCK_FILE": str(lock_path),
                "SEAWEEDFS_CUTOVER_CONFIRM": "backup-and-downtime-approved",
            },
            capture_output=True,
            text=True,
            check=False,
        )

    assert result.returncode == 73
    assert "already running" in result.stderr
