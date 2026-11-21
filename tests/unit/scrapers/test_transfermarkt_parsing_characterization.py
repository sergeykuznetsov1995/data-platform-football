"""Stage-A parsing characterization (epic #2400, issue #2403).

Freezes the behaviour of the pure Transfermarkt parsers on a recorded
fixture sample of real source responses (seed 20261121, 44 captures
copied under ``tests/fixtures/transfermarkt/stage_a_rewrite``).  The
golden files were generated from the current donor implementation in
``scrapers.transfermarkt.scraper`` BEFORE the layer-1..3 extraction and
are the behavioural reference for the rewrite: any extraction must keep
them green without editing the goldens.

The parsers under test are pure functions (no network, files, or env);
dates serialize through ``str`` exactly like the frozen goldens.
"""

from __future__ import annotations

import gzip
import json
import re
from pathlib import Path

import pytest

from scrapers.transfermarkt import TransfermarktScraper
from scrapers.transfermarkt.scraper import (
    _parse_coach_history,
    _parse_coach_profile,
    _parse_mv_history,
    _parse_player_contract_profile,
    _parse_squad_page,
    _parse_transfers,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures/transfermarkt/stage_a_rewrite"
EXPECTED = FIXTURES / "expected"

JSON_ENDPOINTS = {"transfer_events", "market_value_points"}

# endpoint -> (parser, id pattern in the recorded URL)
PARSERS = {
    "squad": (_parse_squad_page, r"verein/(\d+)"),
    "coach_history": (_parse_coach_history, r"verein/(\d+)"),
    "coach_profile": (_parse_coach_profile, r"trainer/(\d+)"),
    "transfer_events": (_parse_transfers, r"list/(\d+)"),
    "market_value_points": (_parse_mv_history, r"graph/(\d+)"),
    "current_player_profile": (_parse_player_contract_profile, r"spieler/(\d+)"),
}

_MANIFEST_INDEX: dict | None = None


def _manifest_index() -> dict:
    global _MANIFEST_INDEX
    if _MANIFEST_INDEX is None:
        manifest = json.loads((FIXTURES / "manifest.json").read_text())
        _MANIFEST_INDEX = {
            entry["capture_id"]: entry for entry in manifest["fixtures"]
        }
    return _MANIFEST_INDEX


def _fixture_body(capture_id: str) -> str:
    entry = _manifest_index()[capture_id]
    raw = gzip.decompress((FIXTURES / entry["blob_file"]).read_bytes())
    return raw.decode("utf-8", errors="replace")


def _golden_files():
    if not EXPECTED.exists():
        return []
    return sorted(EXPECTED.glob("*/*.json"))


def _normalise(rows):
    """Serialise like the frozen goldens (dates via str, sorted keys)."""

    return json.loads(json.dumps(rows, default=str, sort_keys=True))


@pytest.mark.unit
@pytest.mark.parametrize(
    "golden_file",
    _golden_files(),
    ids=lambda path: f"{path.parent.name}-{path.stem[:12]}",
)
def test_recorded_fixture_matches_frozen_parser_golden(golden_file: Path) -> None:
    golden = json.loads(golden_file.read_text())
    endpoint = golden["endpoint"]
    parser, id_pattern = PARSERS[endpoint]

    text = _fixture_body(golden["capture_id"])
    url = golden["url"]
    entity_id = re.search(id_pattern, url).group(1)
    value = json.loads(text) if endpoint in JSON_ENDPOINTS else text

    if isinstance(golden["parser_rows"], dict) and "__parser_error__" in golden["parser_rows"]:
        with pytest.raises(Exception) as caught:  # noqa: B017, PT011 - pinned by golden
            parser(value, entity_id)
        assert golden["parser_rows"]["__parser_error__"].startswith(
            f"{type(caught.value).__name__}:"
        )
    else:
        rows = parser(value, entity_id)
        assert _normalise(rows) == golden["parser_rows"]

    validator_factory = TransfermarktScraper._endpoint_validator(
        endpoint, endpoint in JSON_ENDPOINTS
    )
    if validator_factory is not None:
        assert validator_factory(value) == golden["validator_problem"]


@pytest.mark.unit
def test_fixture_sample_covers_every_parser_family() -> None:
    """The frozen sample must keep exercising each parser family."""

    covered = {path.parent.name for path in _golden_files()}
    assert covered == set(PARSERS), (
        f"golden coverage drift: {covered ^ set(PARSERS)}"
    )


@pytest.mark.unit
def test_goldens_carry_provenance_seed() -> None:
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    assert manifest["seed"] == 20261121
    assert manifest["source"] == "s3://warehouse/raw/transfermarkt"
    assert len(manifest["fixtures"]) == 44
