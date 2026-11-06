"""#2182: SOFASCORE_EVIDENCE_CODE_REVISION не должен переживать перепин релиза.

Тесты гоняют настоящий bash: source deploy/sofascore/env.sh + sofascore_load_env
над синтетическими env-файлами и временными git-деревьями релизов. Сеть и
продакшн-данные не используются.
"""
import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
ENV_SH = REPO / "deploy" / "sofascore" / "env.sh"
assert ENV_SH.is_file(), ENV_SH


def git(repo: Path, *args: str) -> str:
    out = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True, capture_output=True, text=True,
    )
    return out.stdout.strip()


def make_release(base: Path, name: str, files: dict | None = None) -> Path:
    """Минимальное git-дерево «замороженного релиза» с одним коммитом."""
    repo = base / name
    repo.mkdir(parents=True)
    git(repo, "init", "-q")
    git(repo, "config", "user.email", "t@example.invalid")
    git(repo, "config", "user.name", "t")
    git(repo, "config", "commit.gpgsign", "false")
    for rel, text in (files or {"code.py": "print('v1')\n"}).items():
        f = repo / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(text)
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "init")
    return repo


def head(repo: Path) -> str:
    return git(repo, "rev-parse", "HEAD")


def write_env(path: Path, **kv) -> None:
    path.write_text("".join(f"{k}={v}\n" for k, v in kv.items()))


def run_load(env_file: Path, extra_env: dict | None = None,
             after: str = "") -> subprocess.CompletedProcess:
    """Сорсит env.sh, грузит env_file и печатает итоговое состояние ревизии."""
    script = (
        f'. "{ENV_SH}"\n'
        f'sofascore_load_env "{env_file}"\n'
        "rc=$?\n"
        'echo "LOAD_RC=$rc"\n'
        'echo "REV_SET=$([ "${SOFASCORE_EVIDENCE_CODE_REVISION+x}" = x ] && echo 1 || echo 0)"\n'
        'echo "REV=${SOFASCORE_EVIDENCE_CODE_REVISION-}"\n'
        'echo "REV_EXPORTED=$(export -p | grep -c "declare -x SOFASCORE_EVIDENCE_CODE_REVISION=")"\n'
        f"{after}\n"
        "exit 0\n"
    )
    env = dict(os.environ)
    # Чистый лист: никакого наследия CI-окружения.
    env.pop("SOFASCORE_EVIDENCE_CODE_REVISION", None)
    env.pop("SOFASCORE_REFRESH_EVIDENCE_DIR", None)
    env.pop("SOFASCORE_RELEASE_ROOT", None)
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, env=env,
    )


def parse(out: str) -> dict:
    res = {}
    for line in out.splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            res[k] = v
    return res


def test_forward_repin_derives_new_head(tmp_path):
    """Forward: перепин SOFASCORE_RELEASE_ROOT и повторная загрузка → ревизия НОВОГО дерева."""
    a = make_release(tmp_path, "release-a")
    b = make_release(tmp_path, "release-b", {"code.py": "print('v2')\n"})
    envf = tmp_path / "sofascore.env"
    write_env(envf, SOFASCORE_RELEASE_ROOT=a, SOFASCORE_REFRESH_EVIDENCE_DIR="/evidence")

    r = run_load(envf)
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "0", r.stderr
    assert st["REV"] == head(a)
    assert st["REV_EXPORTED"] == "1"

    # Имитация шага repin из deploy.sh: правим файл, грузим повторно в ТОМ ЖЕ процессе.
    r2 = run_load(
        envf,
        after=(
            f'. "{ENV_SH}"\n'
            f'sofascore_set_env_var "{envf}" SOFASCORE_RELEASE_ROOT "{b}"\n'
            f'sofascore_load_env "{envf}"\n'
            'echo "LOAD2_RC=$?"\n'
            'echo "REV2=${SOFASCORE_EVIDENCE_CODE_REVISION-}"\n'
        ),
    )
    st2 = parse(r2.stdout)
    assert st2["LOAD2_RC"] == "0", r2.stderr
    assert st2["REV"] == head(a)
    assert st2["REV2"] == head(b)
    assert head(a) != head(b)


def test_rollback_repin_restores_old_revision(tmp_path):
    """Rollback: перепин на старое дерево → ревизия возвращается на старый HEAD."""
    old = make_release(tmp_path, "release-old")
    new = make_release(tmp_path, "release-new", {"code.py": "print('v2')\n"})
    envf = tmp_path / "sofascore.env"
    write_env(envf, SOFASCORE_RELEASE_ROOT=old, SOFASCORE_EVIDENCE_CODE_REVISION="placeholder")
    r = run_load(
        envf,
        after=(
            f'. "{ENV_SH}"\n'
            f'sofascore_set_env_var "{envf}" SOFASCORE_RELEASE_ROOT "{new}"\n'
            f'sofascore_load_env "{envf}"\n'
            'echo "FWD=${SOFASCORE_EVIDENCE_CODE_REVISION-}"\n'
            f'sofascore_set_env_var "{envf}" SOFASCORE_RELEASE_ROOT "{old}"\n'
            f'sofascore_load_env "{envf}"\n'
            'echo "BACK_RC=$?"\n'
            'echo "BACK=${SOFASCORE_EVIDENCE_CODE_REVISION-}"\n'
        ),
    )
    st = parse(r.stdout)
    assert st["FWD"] == head(new)
    assert st["BACK_RC"] == "0", r.stderr
    assert st["BACK"] == head(old)


def test_stale_exported_pin_is_overwritten(tmp_path):
    """Унаследованный export SOFASCORE_EVIDENCE_CODE_REVISION (пина нет в файле) — перезаписан выводом."""
    a = make_release(tmp_path, "release-a")
    envf = tmp_path / "sofascore.env"
    write_env(envf, SOFASCORE_RELEASE_ROOT=a, SOFASCORE_REFRESH_EVIDENCE_DIR="/evidence")
    r = run_load(envf, extra_env={"SOFASCORE_EVIDENCE_CODE_REVISION": "f" * 40})
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "0", r.stderr
    assert st["REV"] == head(a)
    assert st["REV"] != "f" * 40


def test_absent_pin_and_inherited_env_dropped_when_recorder_off(tmp_path):
    """Recorder выключен (в файле ни каталога, ни пина): унаследованный пин сброшен,
    git не требуется, ничего не экспортируется."""
    plain = tmp_path / "not-a-repo"
    plain.mkdir()
    envf = tmp_path / "sofascore.env"
    write_env(envf, SOFASCORE_RELEASE_ROOT=plain)
    r = run_load(envf, extra_env={"SOFASCORE_EVIDENCE_CODE_REVISION": "e" * 40})
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "0", r.stderr
    assert st["REV_SET"] == "0"
    assert st["REV_EXPORTED"] == "0"


def test_invalid_nonrepo_root_fails_closed(tmp_path):
    """Recorder настроен, а корень не git-дерево → загрузка падает (rc=2), ревизии нет."""
    plain = tmp_path / "plain"
    plain.mkdir()
    envf = tmp_path / "sofascore.env"
    write_env(envf, SOFASCORE_RELEASE_ROOT=plain, SOFASCORE_REFRESH_EVIDENCE_DIR="/evidence")
    r = run_load(envf)
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "2"
    assert "не git-дерево" in r.stderr
    assert st["REV_SET"] == "0"


def test_root_inside_parent_repo_fails_closed(tmp_path):
    """Корень — подкаталог чужого репозитория: вершина не совпадает → отказ."""
    parent = make_release(tmp_path, "parent-repo")
    sub = parent / "nested" / "release"
    sub.mkdir(parents=True)
    envf = tmp_path / "sofascore.env"
    write_env(envf, SOFASCORE_RELEASE_ROOT=sub, SOFASCORE_REFRESH_EVIDENCE_DIR="/evidence")
    r = run_load(envf)
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "2"
    assert "не вершина git-дерева" in r.stderr


def test_dirty_tracked_tree_fails_closed(tmp_path):
    """Грязный отслеживаемый код → отказ; неотслеживаемые файлы отказа не дают."""
    a = make_release(tmp_path, "release-a")
    (a / "untracked-note.txt").write_text("scratch\n")
    envf = tmp_path / "sofascore.env"
    write_env(envf, SOFASCORE_RELEASE_ROOT=a, SOFASCORE_REFRESH_EVIDENCE_DIR="/evidence")
    r = run_load(envf)
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "0", r.stderr
    assert st["REV"] == head(a)

    (a / "code.py").write_text("print('tampered')\n")
    r2 = run_load(envf)
    st2 = parse(r2.stdout)
    assert st2["LOAD_RC"] == "2"
    assert "грязный" in r2.stderr


def test_pin_configured_alone_enables_derivation(tmp_path):
    """Пин в файле без каталога — тоже конфигурация: ревизия выводится и переопределяет пин."""
    a = make_release(tmp_path, "release-a")
    envf = tmp_path / "sofascore.env"
    write_env(envf, SOFASCORE_RELEASE_ROOT=a, SOFASCORE_EVIDENCE_CODE_REVISION="a" * 40)
    r = run_load(envf)
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "0", r.stderr
    assert st["REV"] == head(a)


def test_env_parser_compatibility(tmp_path):
    """Совместимость парсера: кавычки, CRLF, комментарии, снятие export, отказ на чужом ключе."""
    envf = tmp_path / "sofascore.env"
    envf.write_bytes(
        b"# comment\r\n"
        b"SOFASCORE_SINGLE='li  teral'\r\n"
        b'SOFASCORE_DOUBLE="a\\"b\\\\c"\r\n'
        b"SOFASCORE_PLAIN=value\r\n"
    )
    r = run_load(
        envf,
        extra_env={"SOFASCORE_SINGLE": "inherited"},
        after=(
            'echo "SINGLE=${SOFASCORE_SINGLE-}"\n'
            'echo "DOUBLE=${SOFASCORE_DOUBLE-}"\n'
            'echo "SINGLE_EXPORTED=$(export -p | grep -c "declare -x SOFASCORE_SINGLE=")"\n'
        ),
    )
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "0", r.stderr
    assert st["SINGLE"] == "li  teral"  # одинарные кавычки — литерал, наследие перекрыто
    assert st["DOUBLE"] == 'a"b\\c'  # в двойных декодируются только \" и \\
    assert st["SINGLE_EXPORTED"] == "0"  # export-атрибут наследия снят

    bad = tmp_path / "bad.env"
    bad.write_text("PATH=/evil\n")
    r2 = run_load(bad)
    st2 = parse(r2.stdout)
    assert st2["LOAD_RC"] == "2"
    assert "недопустимый ключ" in r2.stderr


def test_missing_recorder_no_revision_no_git(tmp_path):
    """Полностью пустой в части evidence файл: git не вызывается вовсе, rc=0."""
    envf = tmp_path / "sofascore.env"
    write_env(envf, SOFASCORE_RELEASE_ROOT="/nonexistent/path")
    r = run_load(envf)
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "0", r.stderr
    assert st["REV_SET"] == "0"


# --- регрессии failure-path #2182 -------------------------------------------------------

AUTO_DELIVER = REPO / "deploy" / "sofascore" / "auto_deliver.sh"
DEPLOY_SH = REPO / "deploy" / "sofascore" / "deploy.sh"


def extract_func(path: Path, name: str) -> str:
    """Точный текст функции `name(){ ... }` из скрипта (до закрывающей '}' в колонке 0)."""
    lines = path.read_text().splitlines(keepends=True)
    start = next(i for i, l in enumerate(lines) if l.startswith(f"{name}(){{"))
    end = next(i for i in range(start + 1, len(lines)) if lines[i].rstrip("\n") == "}")
    return "".join(lines[start:end + 1])


def extract_block(path: Path, first: str, last: str) -> str:
    """Точный текст блока скрипта от строки first до строки last (не включая её)."""
    lines = path.read_text().splitlines(keepends=True)
    start = next(i for i, l in enumerate(lines) if l.strip() == first)
    end = next(i for i in range(start + 1, len(lines)) if lines[i].strip() == last)
    return "".join(lines[start:end])


def make_git_wrapper(base: Path, fail_on: str) -> Path:
    """Обёртка git: подкоманда fail_on падает молча (пустой stdout), остальное — настоящий git."""
    bin_dir = base / "fakebin"
    bin_dir.mkdir()
    (bin_dir / "git").write_text(
        "#!/bin/sh\n"
        'for a in "$@"; do\n'
        f'  if [ "$a" = "{fail_on}" ]; then exit 1; fi\n'
        "done\n"
        "exec /usr/bin/git \"$@\"\n"
    )
    (bin_dir / "git").chmod(0o755)
    return bin_dir


def run_script(script: str, extra_env: dict | None = None) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.pop("SOFASCORE_EVIDENCE_CODE_REVISION", None)
    if extra_env:
        env.update(extra_env)
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env)


def test_git_status_failure_with_empty_stdout_fails_closed(tmp_path):
    """git status падает с ПУСТЫМ stdout (обёртка): пустой $dirty не должен выглядеть
    чистым деревом — загрузка обязана упасть кодом 2, ревизии нет."""
    a = make_release(tmp_path, "release-a")
    fakebin = make_git_wrapper(tmp_path, "status")
    envf = tmp_path / "sofascore.env"
    write_env(envf, SOFASCORE_RELEASE_ROOT=a, SOFASCORE_REFRESH_EVIDENCE_DIR="/evidence")
    r = run_load(envf, extra_env={"PATH": f"{fakebin}:{os.environ['PATH']}"})
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "2"
    assert "git status" in r.stderr
    assert st["REV_SET"] == "0"


def test_missing_release_root_returns_catchable_code(tmp_path):
    """Recorder настроен, а SOFASCORE_RELEASE_ROOT в файле нет: rc=2, но shell НЕ
    терминирован (${:?} убит бы весь процесс — метки после загрузки не напечатались бы)."""
    envf = tmp_path / "sofascore.env"
    write_env(envf, SOFASCORE_REFRESH_EVIDENCE_DIR="/evidence")
    r = run_load(envf, after='echo "AFTER=alive"\n')
    st = parse(r.stdout)
    assert r.returncode == 0  # процесс жив: код пойман вызывающим
    assert st["LOAD_RC"] == "2"
    assert st["AFTER"] == "alive"
    assert "SOFASCORE_RELEASE_ROOT не задан" in r.stderr
    assert st["REV_SET"] == "0"


def test_stale_inherited_pin_cleared_on_missing_file(tmp_path):
    """Файл отсутствует: унаследованный пин сброшен ДО проверки читаемости, rc=2."""
    r = run_load(
        tmp_path / "no-such.env",
        extra_env={"SOFASCORE_EVIDENCE_CODE_REVISION": "d" * 40},
    )
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "2"
    assert st["REV_SET"] == "0"


def test_stale_inherited_pin_cleared_on_parse_error(tmp_path):
    """Битая строка в файле: унаследованный пин сброшен, rc=2."""
    envf = tmp_path / "bad.env"
    envf.write_text("SOFASCORE_RELEASE_ROOT=/x\nстрока без равно\n")
    r = run_load(envf, extra_env={"SOFASCORE_EVIDENCE_CODE_REVISION": "d" * 40})
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "2"
    assert st["REV_SET"] == "0"


def test_file_pin_never_installed_on_derive_failure(tmp_path):
    """Пин ЕСТЬ в файле, но вывод ревизии падает (корень не git-дерево): в процесс пин
    не устанавливается вообще — REV_SET=0, а не файловое значение."""
    plain = tmp_path / "plain"
    plain.mkdir()
    envf = tmp_path / "sofascore.env"
    write_env(
        envf,
        SOFASCORE_RELEASE_ROOT=plain,
        SOFASCORE_EVIDENCE_CODE_REVISION="c" * 40,
    )
    r = run_load(envf)
    st = parse(r.stdout)
    assert st["LOAD_RC"] == "2"
    assert st["REV_SET"] == "0"
    assert st["REV"] != "c" * 40


def test_repin_env_to_old_load_failure_no_success_no_side_effects(tmp_path):
    """Точный текст repin_env_to_old (+ env_file_value, snapshot_ok) из auto_deliver.sh
    на синтетических стабах: sofascore_load_env падает → функция возвращает неуспех,
    и вызывающий под `set +e` / условным вызовом НЕ доходит до пересоздания контейнеров."""
    envf = tmp_path / "sofascore.env"
    write_env(
        envf,
        SOFASCORE_RELEASE_ROOT="/new-tree",
        SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST="/new-artifact",
        SOFASCORE_PROXY_BUDGET_ARTIFACT_ID="new-id",
    )
    marker = tmp_path / "recreate-marker"
    funcs = "".join(
        extract_func(AUTO_DELIVER, n)
        for n in ("env_file_value", "snapshot_ok", "repin_env_to_old")
    )
    script = (
        "set +e\n"  # как rollback_to_old: errexit снят
        f'ENV_FILE="{envf}"\n'
        f'LOG="{tmp_path}/log"\n'
        'log(){ echo "LOG: $*" >> "$LOG"; }\n'
        # Снимок полон; правки файла «успешны» — падает ТОЛЬКО загрузка.
        'snap_get(){ case "$1" in\n'
        '  OLD_RELEASE_ROOT) echo /old-tree ;; NEW_RELEASE_ROOT) echo /new-tree ;;\n'
        '  OLD_ARTIFACT_HOST) echo /old-artifact ;; OLD_ARTIFACT_ID) echo old-id ;;\n'
        '  HIST_PAUSED|REFRESH_PAUSED) echo t ;; POOL_A) echo 1 ;; esac; }\n'
        'POOLS="A"\n'
        'sofascore_set_env_var(){ return 0; }\n'  # файл уже «совпадает» со снимком ниже
        'sofascore_load_env(){ return 2; }\n'  # загрузка сломана (напр. ревизия не выводится)
        f"{funcs}\n"
        # Ветка «поля файла совпали»: sed-стабы не трогаем, но env_file_value читает файл,
        # поэтому подсовываем файл, уже перепиннутый на старое дерево.
        f'sed -i "s#^SOFASCORE_RELEASE_ROOT=.*#SOFASCORE_RELEASE_ROOT=/old-tree#" "{envf}"\n'
        f'sed -i "s#^SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST=.*#SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST=/old-artifact#" "{envf}"\n'
        f'sed -i "s#^SOFASCORE_PROXY_BUDGET_ARTIFACT_ID=.*#SOFASCORE_PROXY_BUDGET_ARTIFACT_ID=old-id#" "{envf}"\n'
        'if repin_env_to_old /old-tree; then\n'
        f'  touch "{marker}"\n'  # «пересоздание контейнеров» — недопустимый side effect
        '  echo "REPIN=success"\n'
        "else\n"
        '  echo "REPIN=failed rc=$?"\n'
        "fi\n"
    )
    r = run_script(script)
    assert "REPIN=success" not in r.stdout, r.stderr
    assert "REPIN=failed" in r.stdout
    assert not marker.exists(), "пересоздание контейнеров запустилось при сломанной загрузке env"
    assert "НЕ ЗАГРУЖАЕТСЯ" in (tmp_path / "log").read_text()


def test_deploy_forward_repin_block_stops_before_compose(tmp_path):
    """Точный блок deploy.sh между STEP=repin-env и STEP=scheduler-up: sofascore_load_env
    падает → блок завершается кодом 2 ДО вызова docker compose (замокан маркером),
    независимо от errexit вызывающего."""
    block = extract_block(DEPLOY_SH, 'STEP="repin-env"', 'STEP="scheduler-up"')
    assert 'sofascore_load_env "$ENV_FILE"' in block
    marker = tmp_path / "compose-marker"
    envf = tmp_path / "sofascore.env"
    envf.write_text("SOFASCORE_RELEASE_ROOT=/old\n")
    script = (
        "set +e\n"  # даже при подавленном errexit блок обязан остановиться сам
        f'ENV_FILE="{envf}"\nRELEASE="/new-release"\n'
        'ARTIFACT_DEST=/a ARTIFACT_ID=id\n'
        f'LOG="{tmp_path}/log"\n'
        'log(){ echo "LOG: $*" >> "$LOG"; }\n'
        'sofascore_set_env_var(){ return 0; }\n'
        'sofascore_load_env(){ return 2; }\n'
        'docker(){ touch "' + str(marker) + '"; }\n'
        f"{block}\n"
        'echo "BLOCK_SURVIVED"\n'
    )
    r = run_script(script)
    assert r.returncode == 2, (r.stdout, r.stderr)
    assert "BLOCK_SURVIVED" not in r.stdout
    assert not marker.exists(), "docker compose запустился при незагруженном env"
    assert "failed to load after repin" in (tmp_path / "log").read_text()
