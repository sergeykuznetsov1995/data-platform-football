from __future__ import annotations

import os
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/seaweedfs_legacy_entrypoint.sh"


def _run(tmp_path: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    binary = tmp_path / "weed"
    output = tmp_path / "weed.args"
    binary.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$@\" >\"$WEED_ARGS_OUTPUT\"\n",
        encoding="utf-8",
    )
    binary.chmod(0o755)
    return subprocess.run(
        (str(SCRIPT), *arguments),
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "SEAWEEDFS_DATA_DIR": str(tmp_path / "data"),
            "WEED_ARGS_OUTPUT": str(output),
        },
    )


def test_legacy_entrypoint_runs_mini_only_without_a_marker(tmp_path: Path) -> None:
    (tmp_path / "data").mkdir()
    result = _run(tmp_path, "mini", "-dir=/data")

    assert result.returncode == 0
    assert (tmp_path / "weed.args").read_text().splitlines()[0] == "mini"


def test_legacy_entrypoint_rejects_even_a_full_inventory_marker(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    (data / ".supervised-topology-cutover-approved").write_text(
        "full-bucket-inventory-v2:" + "a" * 64 + "\n", encoding="utf-8"
    )
    result = _run(tmp_path, "mini", "-dir=/data")

    assert result.returncode == 78
    assert "every supervised-marked volume" in result.stderr
    assert not (tmp_path / "weed.args").exists()


def test_legacy_entrypoint_rejects_fresh_or_symlink_marker(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    marker = data / ".supervised-topology-cutover-approved"
    marker.write_text("fresh-supervised-volume-v1\n", encoding="utf-8")
    assert _run(tmp_path, "mini").returncode == 78

    marker.unlink()
    authority = data / "authority"
    authority.write_text(
        "full-bucket-inventory-v2:" + "a" * 64 + "\n", encoding="utf-8"
    )
    marker.symlink_to(authority)
    assert _run(tmp_path, "mini").returncode == 78
