import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace


SCRIPT = Path(__file__).resolve().parents[3] / "scripts/research/audit_fotmob_catalog.py"
SPEC = importlib.util.spec_from_file_location("audit_fotmob_catalog", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_catalog_report_deduplicates_ids_and_exposes_scope_decisions(monkeypatch):
    monkeypatch.setattr(MODULE, "MANDATORY_COMPETITION_IDS", frozenset({47}))
    payload = {
        "countries": [
            {
                "ccode": "ENG",
                "name": "England",
                "leagues": [
                    {"id": 47, "name": "Premier League"},
                    {"id": 99, "name": "Women Friendly Cup"},
                ],
            }
        ],
        "popularLeagues": [{"id": 47, "name": "Premier League"}],
    }
    fetch = SimpleNamespace(
        status="success",
        attempts=1,
        retries=0,
        direct_bytes=100,
        decoded_bytes=1000,
        proxy_bytes=0,
        content_hash="a" * 64,
        raw_uri="memory://raw",
    )

    report = MODULE.build_report(payload, fetch=fetch)

    assert report["complete"]
    assert report["catalog"]["unique_competitions"] == 2
    assert report["catalog"]["decision_counts"] == {
        "excluded": 1,
        "included": 1,
    }
    assert report["transport"]["proxy_bytes"] == 0
