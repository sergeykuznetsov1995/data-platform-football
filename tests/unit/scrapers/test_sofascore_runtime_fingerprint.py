"""Runtime qualification must pin the code that accepts healthy continuation."""

from pathlib import Path
import shutil

from scrapers.sofascore.runtime_fingerprint import runtime_files, runtime_fingerprint


def test_continuation_validator_change_invalidates_runtime_fingerprint(tmp_path):
    root = Path(__file__).resolve().parents[3]
    validator = "dags/utils/sofascore_red_share.py"
    for relative in set(runtime_files(root)) | {validator}:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / relative, target)
    before = runtime_fingerprint(tmp_path)
    with (tmp_path / validator).open("a") as stream:
        stream.write("\n# Changed continuation validator, requires new runtime qualification.\n")
    after = runtime_fingerprint(tmp_path)
    assert before["digest"] != after["digest"]
    assert validator in before["files"]


def test_replay_runtime_entries_are_pinned_and_change_the_fingerprint(tmp_path):
    root = Path(__file__).resolve().parents[3]
    replay_dag = "dags/dag_replay_sofascore_raw.py"
    replay_worker = "dags/scripts/replay_sofascore_raw.py"
    for relative in set(runtime_files(root)) | {replay_dag, replay_worker}:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / relative, target)
    before = runtime_fingerprint(tmp_path)
    for relative in (replay_dag, replay_worker):
        assert relative in before["files"]
    digest = before["digest"]
    for relative in (replay_dag, replay_worker):
        with (tmp_path / relative).open("a") as stream:
            stream.write("\n# Changed replay runtime, requires new runtime qualification.\n")
        changed = runtime_fingerprint(tmp_path)["digest"]
        assert changed != digest
        digest = changed
