from pathlib import Path

import pytest

from scrapers.fotmob.source_refresh import (
    PLAYER_SOURCE_REFRESH_ARTIFACT,
    PLAYER_SOURCE_REFRESH_PROFILE,
    PLAYER_SOURCE_REFRESH_SHA256,
    PlayerSourceRefreshContractError,
    load_player_source_refresh_contract,
)


pytestmark = pytest.mark.unit


def _artifact() -> Path:
    return Path(__file__).resolve().parents[3] / PLAYER_SOURCE_REFRESH_ARTIFACT


def test_source_refresh_contract_binds_exact_full_target_tuples():
    contract = load_player_source_refresh_contract(_artifact())

    assert contract["profile"] == PLAYER_SOURCE_REFRESH_PROFILE
    assert contract["sha256"] == PLAYER_SOURCE_REFRESH_SHA256
    assert contract["target_count"] == 7
    assert contract["targets"] == [
        {
            "competition_id": 47,
            "player_id": player_id,
            "source_season_key": "2026/2027",
            "team_id": 8669,
        }
        for player_id in (302783, 798654, 863822, 1025603, 1074750, 1292100, 1334842)
    ]


def test_source_refresh_contract_rejects_any_byte_change(tmp_path):
    changed = tmp_path / "targets.json"
    changed.write_bytes(_artifact().read_bytes().replace(b"302783", b"302784", 1))

    with pytest.raises(PlayerSourceRefreshContractError, match="reviewed SHA"):
        load_player_source_refresh_contract(changed)
