from __future__ import annotations

import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/seaweedfs_plane_entrypoint.sh"


def _run(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(SCRIPT), "master"],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "SEAWEEDFS_DATA_ROOT": str(root),
            "SEAWEEDFS_IMAGE_ENTRYPOINT": "/bin/true",
        },
    )


def test_plane_refuses_unapproved_legacy_volume(tmp_path: Path) -> None:
    assert os.access(SCRIPT, os.X_OK)
    (tmp_path / "m9333").mkdir()

    result = _run(tmp_path)

    assert result.returncode == 78
    assert "backup-gated cutover" in result.stderr


def test_plane_initializes_only_a_genuinely_empty_volume(tmp_path: Path) -> None:
    result = _run(tmp_path)

    assert result.returncode == 0
    marker = tmp_path / ".supervised-topology-cutover-approved"
    assert marker.read_text().strip() == "fresh-supervised-volume-v1"


def test_plane_accepts_backup_approved_legacy_volume(tmp_path: Path) -> None:
    (tmp_path / "m9333").mkdir()
    (tmp_path / ".supervised-topology-cutover-approved").write_text(
        "full-bucket-inventory-v2:abc\n"
    )

    assert _run(tmp_path).returncode == 0
