from __future__ import annotations

import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/seaweedfs_plane_entrypoint.sh"


def _run(
    root: Path, *, expected_inventory: str = "", allow_fresh: bool = False
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(SCRIPT), "master"],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "SEAWEEDFS_DATA_ROOT": str(root),
            "SEAWEEDFS_IMAGE_ENTRYPOINT": "/bin/true",
            "SEAWEEDFS_EXPECTED_INVENTORY_SHA256": expected_inventory,
            "SEAWEEDFS_VOLUME_SIZE_LIMIT_MB": "1024",
            "SEAWEEDFS_ALLOW_FRESH_TRANSITION": (
                "restore-empty-volume-v1" if allow_fresh else ""
            ),
        },
    )


def test_plane_refuses_unapproved_legacy_volume(tmp_path: Path) -> None:
    assert os.access(SCRIPT, os.X_OK)
    (tmp_path / "m9333").mkdir()

    result = _run(tmp_path)

    assert result.returncode == 78
    assert "backup-gated cutover" in result.stderr


def test_plane_initializes_empty_volume_only_during_explicit_recovery(
    tmp_path: Path,
) -> None:
    denied = _run(tmp_path)
    assert denied.returncode == 78
    assert "explicit recovery transition" in denied.stderr

    result = _run(tmp_path, allow_fresh=True)

    assert result.returncode == 0
    marker = tmp_path / ".supervised-topology-cutover-approved"
    assert marker.read_text().strip() == "fresh-supervised-volume-v1"
    assert (tmp_path / "mini.options").read_text().endswith(
        "master.volumeSizeLimitMB=1024\n"
    )


def test_plane_accepts_backup_approved_legacy_volume(tmp_path: Path) -> None:
    (tmp_path / "m9333").mkdir()
    (tmp_path / ".supervised-topology-cutover-approved").write_text(
        "full-bucket-inventory-v2:" + "a" * 64 + "\n"
    )
    (tmp_path / "mini.options").write_text(
        "master.volumeSizeLimitMB=1024\n"
    )

    assert _run(tmp_path, expected_inventory="a" * 64).returncode == 0
    stale = _run(tmp_path, expected_inventory="b" * 64)
    assert stale.returncode == 78
    assert "differs from protected state" in stale.stderr


def test_plane_rejects_forged_or_symlinked_topology_marker(tmp_path: Path) -> None:
    marker = tmp_path / ".supervised-topology-cutover-approved"
    marker.write_text("full-bucket-inventory-v2:abc\n", encoding="utf-8")
    result = _run(tmp_path)
    assert result.returncode == 78
    assert "differs from protected state" in result.stderr

    marker.unlink()
    authority = tmp_path / "authority"
    authority.write_text("fresh-supervised-volume-v1\n", encoding="utf-8")
    marker.symlink_to(authority)
    result = _run(tmp_path)
    assert result.returncode == 78
    assert "non-symlink" in result.stderr
