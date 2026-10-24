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
