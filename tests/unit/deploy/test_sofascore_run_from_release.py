"""Обёртка run_from_release.sh (#1376): сторожа SofaScore — из текущего релиза env-файла.

Обёртка гоняется на установленной копии (рядом env.sh, как в /usr/local/libexec/sofascore/):
cron и утренняя сводка зовут её, а не дерево release-<sha>, зашитое литералом.
"""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy" / "sofascore"
PROBE = (
    "import os, sys\n"
    "print(sys.argv[0]); print(sys.argv[1:]); print(os.environ['PYTHONPATH'])\n"
    "print(os.environ.get('PYTHONDONTWRITEBYTECODE'))\n"
)


def _stand(tmp_path: Path) -> tuple[Path, dict]:
    libexec = tmp_path / "libexec"
    libexec.mkdir()
    for name in ("run_from_release.sh", "env.sh"):
        shutil.copy2(DEPLOY / name, libexec / name)
    for sha in ("release-old", "release-new"):
        tree = tmp_path / sha / "deploy" / "sofascore"
        tree.mkdir(parents=True)
        (tree / "probe.py").write_text(PROBE, encoding="utf-8")
    env_file = tmp_path / "sofascore.env"
    env_file.write_text(f"SOFASCORE_RELEASE_ROOT={tmp_path / 'release-new'}\n", encoding="utf-8")
    # Экспортированный устаревший корень не должен перекрыть env-файл.
    env = {"PATH": os.environ["PATH"], "SOFASCORE_ENV_FILE": str(env_file),
           "SOFASCORE_RELEASE_ROOT": str(tmp_path / "release-old")}
    return libexec / "run_from_release.sh", env


@pytest.mark.unit
def test_run_from_release_execs_the_script_of_the_env_file_tree(tmp_path):
    wrapper, env = _stand(tmp_path)
    r = subprocess.run([str(wrapper), "probe.py", "--collect", "a b"], env=env,
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr
    release = tmp_path / "release-new"
    assert r.stdout.splitlines() == [
        str(release / "deploy" / "sofascore" / "probe.py"), "['--collect', 'a b']", str(release), "1",
    ]


@pytest.mark.unit
@pytest.mark.parametrize("args", [[], ["absent.py"], ["../../release-old/deploy/sofascore/probe.py"]])
def test_run_from_release_without_the_script_exits_2(tmp_path, args):
    wrapper, env = _stand(tmp_path)
    r = subprocess.run([str(wrapper), *args], env=env, capture_output=True, text=True, timeout=30)
    assert r.returncode == 2
    assert r.stdout == ""


@pytest.mark.unit
def test_run_from_release_without_the_env_file_exits_2(tmp_path):
    wrapper, env = _stand(tmp_path)
    env["SOFASCORE_ENV_FILE"] = str(tmp_path / "absent.env")
    r = subprocess.run([str(wrapper), "probe.py"], env=env, capture_output=True, text=True, timeout=30)
    assert r.returncode == 2
