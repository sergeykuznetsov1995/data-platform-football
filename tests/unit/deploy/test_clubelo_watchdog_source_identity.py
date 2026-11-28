"""#2132: ClubElo watchdogs read Bronze as src_clubelo through the shared monitor client.

The host watchdog scripts are not tracked. Their excerpts carry the "before"
side of every hunk, so the patches are applied to an exact copy and the
resulting calls are exercised without Trino.
"""

from pathlib import Path
import re
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


ROOT = Path(__file__).parents[3]
DEPLOY = ROOT / "deploy" / "clubelo"
FIXTURES = ROOT / "tests" / "fixtures" / "clubelo" / "watchdog-source-identity"


def apply(patch_text, before):
    """Apply unified hunks to an exact copy of their "before" sides, in order."""
    after, cursor, lines, index = before, 0, patch_text.splitlines(keepends=True), 0
    while index < len(lines):
        if not lines[index].startswith("@@"):
            index += 1
            continue
        index += 1
        old, new = [], []
        while index < len(lines) and not lines[index].startswith(("@@", "--- ", "+++ ")):
            line = lines[index]
            empty = "\n" if line == "\n" else None   # a blank context line is stored without its space
            if line[:1] in (" ", "-") or empty is not None:
                old.append(line[1:] if empty is None else empty)
            if line[:1] in (" ", "+") or empty is not None:
                new.append(line[1:] if empty is None else empty)
            index += 1
        old, new = "".join(old), "".join(new)
        position = after.find(old, cursor)
        assert position != -1 and old != new
        after = after[:position] + new + after[position + len(old):]
        cursor = position + len(new)
    return after


def patched(name):
    patch = DEPLOY / name
    return apply(patch.read_text(encoding="utf-8"),
                 (FIXTURES / ("morning_report.excerpt.py" if name.startswith("morning")
                              else "clubelo_stall_watch.excerpt.py")).read_text(encoding="utf-8"))


def test_morning_report_helpers_keep_the_shared_client_by_default():
    after = patched("morning-report-2132-source-identity.patch")
    helpers = after[:after.index("# ---- hunk 3")]
    assert not [line for line in helpers.splitlines() if line.startswith("# ---- hunk")][1:]
    helpers = helpers[helpers.index("def _trino_row("):]  # drop the tail of the previous function
    run = Mock(return_value=SimpleNamespace(returncode=0, stdout='"a","b"\n', stderr=""))
    namespace = {"subprocess": SimpleNamespace(run=run), "TRINO": "shared-client"}
    exec(compile(helpers, "morning_report.py", "exec"), namespace)
    assert namespace["_trino_rows"]("SELECT 1") == [["a", "b"]]
    assert run.call_args.args[0] == ["shared-client", "SELECT 1"]
    assert namespace["_trino_row"]("SELECT 1", client="source-client") == ["a", "b"]
    assert run.call_args.args[0] == ["source-client", "SELECT 1"]
    assert namespace["_trino_row"]("SELECT 1", timeout=5, client="source-client") == ["a", "b"]


def test_every_clubelo_bronze_read_of_the_morning_report_uses_the_source_client():
    after = patched("morning-report-2132-source-identity.patch")
    section = after[after.index("CLUBELO_TABLES"):]
    assert 'CLUBELO_TRINO = "/root/watchdog/clubelo_trino_ro.sh"' in section
    calls = [section[match.start():match.start() + 160] for match in re.finditer(r"_trino_rows?\(", section)]
    assert len(calls) == 3, calls
    # _clubelo_rows spans two lines; its client argument follows the SQL.
    assert all("client=CLUBELO_TRINO" in call for call in calls), calls
    assert '"--trino", CLUBELO_TRINO,' in section
    assert section.count("trino-ro.sh") == 0


def test_stall_watch_reads_as_the_source():
    after = patched("stall-watch-2132-source-identity.patch")
    assert after.count('TRINO = "/root/watchdog/clubelo_trino_ro.sh"') == 1
    assert "trino-ro.sh" not in after


def test_wrapper_hands_the_identity_to_the_shared_monitor(tmp_path):
    wrapper = DEPLOY / "clubelo_trino_ro.sh"
    text = wrapper.read_text(encoding="utf-8")
    assert text.count("/root/.claude/bin/trino-ro.sh") == 1 and "PASSWORD" not in text
    argv = tmp_path / "argv.txt"
    monitor = tmp_path / "trino-ro.sh"
    monitor.write_text(f'#!/bin/sh\nprintf "%s\\n" "$@" > "{argv}"\nexit 7\n', encoding="utf-8")
    monitor.chmod(0o755)
    staged = tmp_path / "clubelo_trino_ro.sh"
    staged.write_text(text.replace("/root/.claude/bin/trino-ro.sh", str(monitor)), encoding="utf-8")
    result = subprocess.run(["sh", str(staged), "SELECT 'a b' -- c"], capture_output=True, text=True)
    assert (result.returncode, result.stdout, result.stderr) == (7, "", "")
    assert argv.read_text(encoding="utf-8").splitlines() == ["--identity", "src_clubelo", "SELECT 'a b' -- c"]


@pytest.mark.parametrize("name", ["morning-report-2132-source-identity.patch", "stall-watch-2132-source-identity.patch"])
def test_patches_carry_no_credentials(name):
    text = (DEPLOY / name).read_text(encoding="utf-8")
    added = [line[1:] for line in text.splitlines() if line.startswith("+") and not line.startswith("+++")]
    assert added and not [line for line in added if "PASSWORD" in line.upper() or "TOKEN" in line.upper()]


def test_patches_apply_with_both_tools(tmp_path):
    """The host uses GNU patch; the first meter installation used git apply."""
    for name, source in (("morning-report-2132-source-identity.patch", "morning_report.excerpt.py"),
                         ("stall-watch-2132-source-identity.patch", "clubelo_stall_watch.excerpt.py")):
        patch = DEPLOY / name
        assert not [line for line in patch.read_text(encoding="utf-8").splitlines() if line != line.rstrip()]
        script = "morning_report.py" if source.startswith("morning") else "clubelo_stall_watch.py"
        for tool in (["patch", "--silent", "-p1", "-i", str(patch), script], ["git", "apply", str(patch)]):
            work = tmp_path / script.removesuffix(".py") / tool[0]
            work.mkdir(parents=True)
            # The excerpt carries the "before" side of every hunk under the real file name.
            shutil.copyfile(FIXTURES / source, work / script)
            result = subprocess.run(tool, cwd=work, capture_output=True, text=True)
            assert result.returncode == 0, (name, tool[0], result.stdout, result.stderr)
            assert (work / script).read_text(encoding="utf-8") != (FIXTURES / source).read_text(encoding="utf-8")
