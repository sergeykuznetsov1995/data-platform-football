"""Private host resources for WhoScored tests; production defaults stay intact."""

from pathlib import Path
import subprocess
import tempfile

import pytest


@pytest.fixture(autouse=True)
def whoscored_test_locks(request, monkeypatch):
    if "whoscored" not in request.node.path.name:
        return
    root = request.getfixturevalue("tmp_path_factory").mktemp("whoscored-locks")
    for variable, name in (
        ("WHOSCORED_LOCK_DIR", "commit-locks"),
        ("WHOSCORED_RAW_LOCK_DIR", "raw-locks"),
    ):
        directory = root / name
        directory.mkdir(mode=0o700)
        monkeypatch.setenv(variable, str(directory))


@pytest.fixture(scope="session")
def protected_whoscored_checkout():
    """Copy tracked bytes, retaining Git identity without altering the checkout.

    Do not regenerate evidence: stale hashes must still fail. Explicit /tmp
    avoids writable checkout ancestors used by TMPDIR in some runners.
    """
    source = Path(__file__).resolve().parents[2]
    names = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=source
    ).decode().split("\0")
    with tempfile.TemporaryDirectory(prefix="whoscored-checkout-test-", dir="/tmp") as name:
        root = Path(name)
        for relative in filter(None, names):
            original = source / relative
            if not original.is_file():
                continue
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            # mkdir(parents=True) uses the umask for intermediate directories.
            for parent in target.parents:
                if parent == root:
                    break
                parent.chmod(0o700)
            target.write_bytes(original.read_bytes())
            target.chmod(0o700 if original.stat().st_mode & 0o111 else 0o600)
        git_dir = subprocess.check_output(
            ["git", "rev-parse", "--absolute-git-dir"], cwd=source, text=True
        ).strip()
        (root / ".git").write_text(f"gitdir: {git_dir}\n")
        (root / ".git").chmod(0o600)
        yield root
