"""Retained J. League 2 bracket regression for issue #1597."""

import json
from pathlib import Path

import pytest

from scrapers.fotmob.domain import ScopeRef
from scrapers.fotmob.parsers import parse_season_bundle
from scrapers.fotmob.repository import ManifestStatus
from scrapers.fotmob.transport import canonicalize_target
from tests.unit.scrapers.test_fotmob_service import _service


NULL_PATH = "$.playoff.rounds[0].matchups[2]"
REAL_MATCHUPS = {
    "$.playoff.rounds[0].matchups[1]": (4053616, 164739, 164720, 0, 3),
    "$.playoff.rounds[0].matchups[3]": (4053615, 162196, 4427, 2, 2),
    "$.playoff.rounds[1].matchups[0]": (4053697, 162196, 164720, 2, 2),
}


@pytest.fixture
def retained_playoff_payload():
    path = Path(__file__).parents[2] / "fixtures/fotmob/league_8974_2022_null_playoff.json"
    return json.loads(path.read_text())


def _assert_real_matchups(rows):
    actual = {
        row["source_path"]: (
            row["match_ids"][0], row["home_team_id"], row["away_team_id"],
            row["home_score"], row["away_score"],
        )
        for row in rows if row["match_ids"]
    }
    assert actual == REAL_MATCHUPS
    # Preserve the source's existing empty-object row; only null is omitted.
    assert len(rows) == 4
    assert rows[0]["source_path"] == "$.playoff.rounds[0].matchups[0]"


def test_retained_null_slot_preserves_bracket_and_source_indexes(retained_playoff_payload):
    bundle = parse_season_bundle(retained_playoff_payload, ScopeRef(8974, "2022"))

    assert [(issue.code, issue.path) for issue in bundle.issues] == [
        ("empty_playoff_matchup", NULL_PATH)
    ]
    _assert_real_matchups(bundle.playoffs)
    assert {row["match_id"]: row["source_path"] for row in bundle.matches} == {
        values[0]: path + ".matches[0]" for path, values in REAL_MATCHUPS.items()
    }


def test_retained_null_slot_service_publishes_real_bracket(retained_playoff_payload):
    target = canonicalize_target("leagues", {"id": 8974, "season": "2022"})
    service, _, repository = _service({target.canonical_url: retained_playoff_payload})

    result, bundle = service.sync_season(8974, "2022")

    assert result.ok, result.errors
    assert bundle is not None
    assert result.succeeded == 1
    assert result.counts["parse_issues"] == 1
    assert repository.commits[-1].status == ManifestStatus.SUCCESS
    assert repository.commits[-1].raw_uri
    _assert_real_matchups(repository.tables["fotmob_playoff_brackets"])
    assert {row["match_id"] for row in repository.tables["fotmob_matches"]} == {
        4053616, 4053615, 4053697
    }


@pytest.mark.parametrize("malformed", ["unexpected", 1, 0, False, [], [None]])
def test_non_null_malformed_slot_stays_blocking(retained_playoff_payload, malformed):
    retained_playoff_payload["playoff"]["rounds"][0]["matchups"][2] = malformed
    bundle = parse_season_bundle(retained_playoff_payload, ScopeRef(8974, "2022"))
    assert [(issue.code, issue.path) for issue in bundle.issues] == [
        ("invalid_playoff_matchup", NULL_PATH)
    ]
    _assert_real_matchups(bundle.playoffs)
    target = canonicalize_target("leagues", {"id": 8974, "season": "2022"})
    service, _, repository = _service({target.canonical_url: retained_playoff_payload})

    result, published_bundle = service.sync_season(8974, "2022")

    assert not result.ok
    assert published_bundle is None
    assert repository.commits[-1].status == ManifestStatus.SCHEMA_DRIFT
    assert "invalid_playoff_matchup@" + NULL_PATH in result.errors[0]
    assert "fotmob_playoff_brackets" not in repository.tables
    assert "fotmob_matches" not in repository.tables
