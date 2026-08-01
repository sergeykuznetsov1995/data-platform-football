from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

from scripts.audit_espn_runtime_imports import production_files, scan_file


ROOT = Path(__file__).resolve().parents[3]


def test_production_surface_includes_native_runtime_but_not_retained_legacy():
    paths = {path.relative_to(ROOT).as_posix() for path in production_files(ROOT)}

    assert "scrapers/espn/runner.py" in paths
    assert "dags/utils/espn_native_tasks.py" in paths
    assert "dags/scripts/run_espn_scraper.py" in paths
    assert "dags/dag_discover_espn_registry.py" in paths
    assert "scrapers/espn/scraper.py" not in paths
    assert "scrapers/espn/__init__.py" not in paths


def test_static_scan_catches_direct_aliased_and_dynamic_imports(tmp_path):
    source = tmp_path / "hidden.py"
    source.write_text(
        """
import importlib as loader
from importlib import import_module as load
import soccerdata.espn as old
loader.import_module("soccer" + "data")
load("soccerdata.espn")
__import__("soccerdata")
""",
        encoding="utf-8",
    )

    findings = scan_file(source)

    assert {item["kind"] for item in findings} == {
        "import",
        "dynamic_import",
    }
    assert len(findings) == 4


def test_static_scan_catches_legacy_scraper_module_and_lazy_facade(tmp_path):
    source = tmp_path / "escape.py"
    source.write_text(
        """
from scrapers.espn.scraper import ESPNScraper as Legacy
from scrapers.espn import ESPNScraper
import scrapers.espn as espn
espn.ESPNScraper
getattr(espn, "ESPN" + "Scraper")
__import__("scrapers.espn." + "scraper")
""",
        encoding="utf-8",
    )

    findings = scan_file(source)

    assert len(findings) >= 6
    assert {item["module"] for item in findings} == {
        "scrapers.espn.scraper",
        "scrapers.espn.scraper.ESPNScraper",
        "scrapers.espn.ESPNScraper",
    }


def test_static_scan_resolves_relative_legacy_imports(tmp_path):
    source = tmp_path / "scrapers" / "espn" / "native.py"
    source.parent.mkdir(parents=True)
    source.write_text(
        """
from .scraper import ESPNScraper
from . import scraper
from . import ESPNScraper
import importlib
importlib.import_module(".scraper", package="scrapers.espn")
""",
        encoding="utf-8",
    )

    findings = scan_file(source)

    assert len(findings) >= 5
    assert all(str(item["module"]).startswith("scrapers.espn") for item in findings)


def test_static_scan_catches_unaliased_and_parent_import_facades(tmp_path):
    source = tmp_path / "facades.py"
    source.write_text(
        """
import scrapers.espn
scrapers.espn.ESPNScraper
getattr(scrapers.espn, "ESPNScraper")
from scrapers import espn
espn.ESPNScraper
from scrapers import espn as source
source.ESPNScraper
""",
        encoding="utf-8",
    )

    findings = scan_file(source)

    assert len(findings) == 4
    assert {item["kind"] for item in findings} == {"legacy_facade_access"}


def test_actual_static_and_runtime_gate_is_machine_readable_and_green():
    completed = subprocess.run(
        [sys.executable, "scripts/audit_espn_runtime_imports.py", "--root", str(ROOT)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    report = json.loads(completed.stdout)
    assert report["status"] == "pass"
    assert report["static_findings"] == []
    assert all(item["status"] == "imported" for item in report["runtime_probe"])
    assert len(report["result_sha256"]) == 64
