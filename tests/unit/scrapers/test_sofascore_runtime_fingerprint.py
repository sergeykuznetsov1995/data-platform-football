from pathlib import Path

import pytest
import yaml

from scrapers.sofascore import runtime_fingerprint as runtime


ENTRIES = (
    "scrapers/sofascore/capture.py",
    "docker/images/airflow/requirements-scraping.txt",
)
REQUIREMENTS = "camoufox[geoip]==0.4.11\nplaywright==1.59.0\n"


def _runtime_tree(root: Path, *, source: str = "value = 1\n") -> None:
    capture = root / "scrapers/sofascore/capture.py"
    capture.parent.mkdir(parents=True)
    capture.write_text(source, encoding="utf-8")
    requirements = root / "docker/images/airflow/requirements-scraping.txt"
    requirements.parent.mkdir(parents=True)
    requirements.write_text(REQUIREMENTS, encoding="utf-8")


def test_fingerprint_is_independent_of_checkout_path_and_mtime(tmp_path):
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    _runtime_tree(first_root)
    _runtime_tree(second_root)
    (second_root / "scrapers/sofascore/capture.py").touch()

    first = runtime.runtime_fingerprint(first_root, entries=ENTRIES)
    second = runtime.runtime_fingerprint(second_root, entries=ENTRIES)

    assert first == second
    assert all(not path.startswith(str(tmp_path)) for path in first["files"])


def test_changing_one_runtime_file_rejects_old_evidence(tmp_path):
    _runtime_tree(tmp_path)
    measured = runtime.runtime_fingerprint(tmp_path, entries=ENTRIES)
    (tmp_path / "scrapers/sofascore/capture.py").write_text(
        "value = 2\n", encoding="utf-8"
    )

    with pytest.raises(runtime.RuntimeFingerprintError, match="does not match"):
        runtime.validate_runtime_fingerprint(
            measured,
            root=tmp_path,
            entries=ENTRIES,
            enforce_installed_browser=False,
        )


def test_installed_browser_version_mismatch_fails_closed(tmp_path, monkeypatch):
    _runtime_tree(tmp_path)
    measured = runtime.runtime_fingerprint(tmp_path, entries=ENTRIES)

    def installed_version(package):
        return {"camoufox": "0.4.10", "playwright": "1.59.0"}[package]

    monkeypatch.setattr(runtime.importlib.metadata, "version", installed_version)
    with pytest.raises(runtime.RuntimeFingerprintError, match="camoufox=0.4.10"):
        runtime.validate_runtime_fingerprint(
            measured,
            root=tmp_path,
            entries=ENTRIES,
            enforce_installed_browser=True,
        )


def test_missing_pinned_browser_dependency_is_rejected(tmp_path):
    capture = tmp_path / "scrapers/sofascore/capture.py"
    capture.parent.mkdir(parents=True)
    capture.write_text("value = 1\n", encoding="utf-8")
    requirements = tmp_path / "docker/images/airflow/requirements-scraping.txt"
    requirements.parent.mkdir(parents=True)
    requirements.write_text("camoufox==0.4.11\n", encoding="utf-8")

    with pytest.raises(runtime.RuntimeFingerprintError, match="playwright"):
        runtime.runtime_fingerprint(tmp_path, entries=ENTRIES)


def test_build_contract_fallback_keeps_logical_paths(tmp_path):
    capture = tmp_path / "scrapers/sofascore/capture.py"
    capture.parent.mkdir(parents=True)
    capture.write_text("value = 1\n", encoding="utf-8")
    requirements = (
        tmp_path
        / "runtime-contract/docker/images/airflow/requirements-scraping.txt"
    )
    requirements.parent.mkdir(parents=True)
    requirements.write_text(REQUIREMENTS, encoding="utf-8")

    measured = runtime.runtime_fingerprint(tmp_path, entries=ENTRIES)

    assert measured["files"] == sorted(ENTRIES)
    assert all("runtime-contract" not in path for path in measured["files"])
    assert measured["browser_runtime_pins"] == {
        "camoufox": "0.4.11",
        "playwright": "1.59.0",
    }


def test_class_manifest_and_cohorts_stay_out_of_the_fingerprint():
    """Declaring classes must not, by itself, rotate the measured runtime digest.

    The canary class manifest and its cohorts declare *what* is collected, not
    *how* bytes are spent, so changing the class declaration alone leaves
    verified samples valid.  Onboarding a new league is a separate matter: it
    edits ``configs/medallion/competitions.yaml``, which *is* in the
    fingerprint, so those measurements rotate and are re-collected regardless of
    the manifest (an accepted limitation, batched per F2/§2.4).
    """

    files = runtime.runtime_fingerprint()["files"]

    assert "configs/sofascore/proxy_canary_classes.json" not in files
    assert not [
        path
        for path in files
        if path.startswith("configs/sofascore/proxy_canary_cohort")
    ]
    assert not [path for path in files if path.startswith("configs/sofascore/proxy_")]


def test_proxy_filter_compose_exposes_every_fingerprint_runtime_path():
    root = Path(__file__).resolve().parents[3]
    compose = yaml.safe_load((root / "compose.yaml").read_text(encoding="utf-8"))
    volumes = compose["services"]["proxy_filter"]["volumes"]
    targets = {str(volume).split(":")[1] for volume in volumes}

    assert {
        "/opt/airflow/dags",
        "/opt/airflow/scripts",
        "/opt/airflow/scrapers",
        "/opt/airflow/configs/medallion",
        "/opt/airflow/configs/proxy_filter",
        "/opt/airflow/configs/sofascore",
    } <= targets

    for dockerfile_name in ("Dockerfile", "Dockerfile.scheduler-runtime"):
        dockerfile = (
            root / "docker/images/airflow" / dockerfile_name
        ).read_text(encoding="utf-8")
        for filename in (
            "Dockerfile",
            "requirements-airflow.txt",
            "requirements-scraping.txt",
            "requirements.txt",
        ):
            assert (
                "/opt/airflow/runtime-contract/docker/images/airflow/"
                f"{filename}" in dockerfile
            )
