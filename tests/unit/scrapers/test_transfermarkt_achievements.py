"""Synthetic saved captures. No ingestion services are required."""

import json

import pytest

from scrapers.transfermarkt.achievements import (
    ACHIEVEMENT_COLUMNS,
    ACHIEVEMENT_NATURAL_KEY,
    AchievementParseError,
    confirmed_empty_achievements,
    parse_achievements,
)


def capture(kind):
    first = {
        "id": "0017",
        "trophyId": 31,
        "seasonId": "02022",
        "season": {"id": 2022, "name": " 2022 "},
        "club": {"name": " Example   XI ", "id": 61},
        "competition": {"id": "Q1"},
        "result": " Runner-up ",
    }
    if kind == "player":
        first["details"] = {"playerId": "007", "clubId": 61, "competitionId": "Q1"}
    return {
        "success": True,
        "message": "ok",
        "data": {
            "aggregated": [{"id": 17, "name": "Copa É", "count": 2}],
            "achievements": [
                first,
                {
                    "id": 17, "name": "Detail title", "seasonId": 2025,
                    "season": {"id": 2025, "name": "25/26"}, "trophyId": 0,
                    "club": {"id": "", "name": "National XI"}, "competition": {"id": ""},
                    "details": {"playerId": 0, "competitionId": 0, "clubId": 0},
                    "sourceRowId": "0041",
                },
            ],
        },
    }


def parse(source, kind="player"):
    return parse_achievements(source, recipient_type=kind, recipient_id="0007")


def replay(source, kind):
    first, second = parse(source, kind), parse(source, kind)
    assert first == second
    encoded = first.canonical_json()
    assert encoded.encode("utf-8") == second.canonical_json().encode("utf-8")
    payload = json.loads(encoded)
    assert payload["rows"] == [row.as_dict() for row in first.rows]
    assert payload["parser_revision"] == "achievements-v1"
    assert payload["schema_revision"] == 1
    assert json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) == encoded
    return first


@pytest.mark.parametrize("kind", ["player", "club"])
def test_json_detail_before_grouping(kind):
    source = capture(kind)
    parsed = replay(source, kind)
    assert parsed == parse(source["data"], kind)
    assert (parsed.recipient_type, parsed.recipient_id, parsed.status) == (kind, "7", "observed_present")
    first, second = parsed.rows
    assert first.as_dict() == {
        "recipient_type": kind, "recipient_id": "7", "award_id": "17", "title": "Copa É",
        "trophy_id": "31", "season_id": "2022", "season": "2022", "club_id": "61",
        "club_name": "Example XI", "competition_id": "Q1", "result": "Runner-up",
        "source_row_id": "achievements:0",
    }
    assert second.as_dict() == {
        "recipient_type": kind, "recipient_id": "7", "award_id": "17", "title": "Detail title",
        "trophy_id": None, "season_id": "2025", "season": "25/26", "club_id": None,
        "club_name": "National XI", "competition_id": None, "result": None,
        "source_row_id": "source:41",
    }
    group, = parsed.reconciliation
    assert (group.award_id, group.title, group.published_count, group.detail_count) == ("17", "Copa É", 2, 2)
    assert "Copa É" in parsed.canonical_json()


@pytest.mark.parametrize("explicit", [False, True])
def test_identical_source_rows_remain_separate(explicit):
    row = {"id": 1, "season": {"name": "2022"}}
    if explicit:
        row["observationId"] = "sample"
    parsed = replay({
        "achievements": [dict(row), dict(row)],
        "aggregated": [{"id": 1, "name": "Cup", "count": "2"}],
    }, "player")
    assert [r.title for r in parsed.rows] == ["Cup", "Cup"]
    a, b = parsed.rows
    assert a.natural_key != b.natural_key
    assert a.as_dict() | {"source_row_id": b.source_row_id} == b.as_dict()
    assert [a.source_row_id, b.source_row_id] == (
        ["source:sample", "source:sample:2"] if explicit else ["achievements:0", "achievements:1"]
    )
    assert parsed.reconciliation[0].detail_count == parsed.reconciliation[0].published_count == 2


def test_column_and_key_contract():
    row = parse(capture("club"), "club").rows[0]
    assert ACHIEVEMENT_COLUMNS == tuple(row.as_dict()) == (
        "recipient_type", "recipient_id", "title", "award_id", "trophy_id", "season_id",
        "season", "result", "club_id", "club_name", "competition_id", "source_row_id",
    )
    assert row.natural_key == (
        "club", "7", "17", "31", "Q1", "Copa É", "2022", "2022", "Runner-up", "achievements:0",
    )
    assert row.natural_key == tuple(row.as_dict()[k] for k in ACHIEVEMENT_NATURAL_KEY)
    assert {"observed_at", "captured_at", "raw_uri", "raw_sha256"}.isdisjoint(ACHIEVEMENT_COLUMNS)


def test_nullable_fields_and_large_id():
    large = 2**79 + 5
    row, = parse({"achievements": [{"id": large, "name": "Winner"}]}).rows
    assert row.award_id == str(large)
    assert (row.season_id, row.season, row.club_id, row.trophy_id, row.result) == (None,) * 5


@pytest.mark.parametrize("kind", ["player", "club"])
def test_empty_json(kind):
    parsed = replay({"achievements": []}, kind)
    assert parsed.status == "observed_empty"
    assert parsed.rows == () and parsed.completeness_proof is None


@pytest.mark.parametrize("kind", ["player", "club", "coach"])
def test_external_empty_requires_proof(kind):
    owner = {"recipient_type": kind, "recipient_id": "007"}
    observed = confirmed_empty_achievements(**owner, completeness_proof=" witness/1 ")
    assert observed.status == "observed_empty" and observed.rows == ()
    assert observed.recipient_type == kind and observed.recipient_id == "7"
    assert json.loads(observed.canonical_json())["completeness_proof"] == "witness/1"
    for value in ({}, {"completeness_proof": ""}, {"completeness_proof": " \n"}, {"completeness_proof": 5}):
        with pytest.raises(AchievementParseError) as exc:
            confirmed_empty_achievements(**owner, **value)
        assert exc.value.reason == "missing_completeness_proof"


@pytest.mark.parametrize(("source", "reason"), [
    ("{}", "invalid_json_source"),
    ({"success": False, "data": {"achievements": []}}, "unsuccessful_envelope"),
    ({"success": 1, "data": {"achievements": []}}, "unsuccessful_envelope"),
    ({"success": True}, "invalid_data"),
    ({"success": True, "data": []}, "invalid_data"),
    ({"success": True, "data": None}, "invalid_data"),
    ({}, "invalid_achievements"),
    ({"achievements": None}, "invalid_achievements"),
    ({"achievements": {}}, "invalid_achievements"),
    ({"achievements": [{"name": "Cup"}, None]}, "invalid_row"),
    ({"achievements": [{"name": " \n "}]}, "missing_title"),
    ({"achievements": [{"name": "Cup", "club": []}]}, "invalid_club"),
    ({"achievements": [{"name": "Cup", "seasonId": 10, "season": {"id": 11}}]}, "season_id_conflict"),
    ({"achievements": [{"name": "Cup", "details": {"playerId": 8}}]}, "owner_conflict"),
    ({"achievements": [{"name": "Cup", "details": {"playerId": False}}]}, "invalid_id"),
    ({"achievements": [{"name": "Cup", "trophyId": 0.0}]}, "invalid_id"),
    ({"achievements": [{"name": "Cup", "competition": {"id": "bad/id"}}]}, "invalid_id"),
    ({"achievements": [{"name": "Cup", "club": {"id": -2}}]}, "invalid_id"),
    ({"achievements": [{"name": "Cup", "seasonId": "2.0"}]}, "invalid_id"),
    ({"achievements": [], "aggregated": {}}, "invalid_aggregated"),
    ({"achievements": [], "trophies": {}}, "invalid_trophies"),
    ({"achievements": [], "trophies": [{"id": 0, "name": "Cup"}]}, "invalid_trophy_id"),
    ({"achievements": [], "aggregated": [{"id": 1, "count": True}]}, "invalid_count"),
    ({"achievements": [], "aggregated": [{"id": 1, "count": 3}]}, "aggregate_count_mismatch"),
    ({"achievements": [{"id": 1, "name": "Cup"}] * 2, "aggregated": [{"id": 1, "count": 1}]},
     "aggregate_count_mismatch"),
])
def test_json_fails_whole_capture(source, reason):
    with pytest.raises(AchievementParseError) as exc:
        parse(source)
    assert exc.value.reason == reason


@pytest.mark.parametrize(("kind", "owner", "reason"), [
    ("referee", 1, "invalid_recipient_type"),
    ("player", True, "invalid_id"),
    ("coach", -1, "invalid_id"),
    ("club", None, "invalid_recipient_id"),
])
def test_recipient_validation(kind, owner, reason):
    with pytest.raises(AchievementParseError) as exc:
        parse_achievements({}, recipient_type=kind, recipient_id=owner)
    assert exc.value.reason == reason


@pytest.mark.parametrize("kind", ["player", "club"])
def test_donor_display_and_received_count_schema(kind):
    def detail(season_id, display, trophy_id):
        value = {
            "id": 17,
            "seasonId": season_id,
            "trophyId": trophy_id,
            "season": {
                "id": season_id,
                "display": display,
                "cyclicalName": "ignored",
                "nonCyclicalName": "also ignored",
            },
            "details": {"clubId": "61", "competitionId": "Q1"},
        }
        if kind == "player":
            value["details"]["playerId"] = 7
        return value

    parsed = replay({
        "aggregated": [{"id": 17, "receivedCount": 2, "name": "Cup", "order": 100}],
        "achievements": [detail(2022, "2022", 31), detail(2025, "25/26", 32)],
    }, kind)
    assert [row.title for row in parsed.rows] == ["Cup", "Cup"]
    assert [row.season for row in parsed.rows] == ["2022", "25/26"]
    assert [row.season_id for row in parsed.rows] == ["2022", "2025"]
    assert [row.trophy_id for row in parsed.rows] == ["31", "32"]
    assert [row.club_id for row in parsed.rows] == ["61", "61"]
    assert [row.competition_id for row in parsed.rows] == ["Q1", "Q1"]
    assert [row.source_row_id for row in parsed.rows] == ["achievements:0", "achievements:1"]
    group, = parsed.reconciliation
    assert (group.published_count, group.detail_count) == (2, 2)


@pytest.mark.parametrize(("aggregated", "reason"), [
    ([{"id": 17, "receivedCount": 2, "name": "Cup"}], "aggregate_count_mismatch"),
    ([{"id": 17, "count": 2, "receivedCount": 1, "name": "Cup"}], "aggregate_count_conflict"),
])
def test_donor_received_count_fails_closed(aggregated, reason):
    source = {
        "aggregated": aggregated,
        "achievements": [{"id": 17, "season": {"id": 2022, "display": "2022"}}],
    }
    with pytest.raises(AchievementParseError) as exc:
        parse(source)
    assert exc.value.reason == reason


def test_club_trophy_name_fallback_without_aggregate_preserves_rows():
    parsed = replay({
        "trophies": [{"id": 6, "name": "Champions League Participant"}],
        "achievements": [
            {"id": 6, "trophyId": 6, "season": {"id": 2025, "display": "25/26"}},
            {"id": 6, "trophyId": 6, "season": {"id": 2024, "display": "24/25"}},
        ],
    }, "club")
    assert parsed.status == "observed_present"
    assert [row.title for row in parsed.rows] == ["Champions League Participant"] * 2
    assert [row.season for row in parsed.rows] == ["25/26", "24/25"]
    assert [row.source_row_id for row in parsed.rows] == ["achievements:0", "achievements:1"]
    assert parsed.reconciliation == ()


def box(heading, rows):
    return f'<div class="box"><h2 class="content-box-headline">{heading}</h2><table>{rows}</table></div>'


def title(heading):
    return f'<tr><td colspan="3">{heading}</td></tr>'


ALPHA = '<tr><td> 2022 </td><td><a href="/alpha/startseite/verein/021">Alpha XI</a></td></tr>'
BETA = (
    '<tr><td>25/26</td><td><a href="/beta/profil/verein/22"><img alt="Beta XI"></a></td>'
    '<td><a href="/beta/profil/verein/22">Beta XI</a></td><td>Runner-up</td></tr>'
)
HEADERS = "<tr><th>Season</th><th>Club</th></tr>"


@pytest.mark.parametrize("single", [True, False], ids=["all-titles", "separate-boxes"])
def test_coach_layouts_preserve_each_section_and_observation(single):
    if single:
        html = box("All titles", title("2x Cup") + HEADERS + ALPHA + BETA + title("1x Cup") + ALPHA)
        locators = ["box:0:row:2", "box:0:row:3", "box:0:row:5"]
    else:
        html = box("2x Cup", HEADERS + ALPHA + BETA) + box("1x Cup", ALPHA)
        locators = ["box:0:row:1", "box:0:row:2", "box:1:row:0"]
    parsed = replay(html, "coach")
    assert parsed.status == "observed_present"
    assert [(r.recipient_type, r.recipient_id, r.title) for r in parsed.rows] == [("coach", "7", "Cup")] * 3
    assert [(r.season, r.club_id, r.club_name, r.result) for r in parsed.rows] == [
        ("2022", "21", "Alpha XI", None), ("25/26", "22", "Beta XI", "Runner-up"),
        ("2022", "21", "Alpha XI", None),
    ]
    assert [r.source_row_id for r in parsed.rows] == locators
    assert len({r.natural_key for r in parsed.rows}) == 3
    assert [(g.title, g.published_count, g.detail_count) for g in parsed.reconciliation] == [
        ("Cup", 2, 2), ("Cup", 1, 1),
    ]
    assert len({g.source_group_id for g in parsed.reconciliation}) == 2


def test_coach_plain_team_and_explicit_note_next_to_link():
    html = box("2x League", '<tr><td>2022</td><td>National XI</td></tr>' +
               ALPHA.replace("</a>", "</a> (shared)"))
    a, b = parse(html, "coach").rows
    assert (a.club_id, a.club_name, a.result) == (None, "National XI", None)
    assert (b.club_id, b.club_name, b.result) == ("21", "Alpha XI", "(shared)")


def test_coach_uses_published_link_ids():
    linked_title = '<a href="/example/startseite/wettbewerb/2EX">Example Cup</a>'
    html = box("All titles", title("1x " + linked_title) + ALPHA.replace("/021", "/021/saison_id/01999"))
    row, = parse(html, "coach").rows
    assert (row.title, row.competition_id, row.season_id, row.season) == ("Example Cup", "2EX", "1999", "2022")


@pytest.mark.parametrize(("source", "reason"), [
    ({"achievements": []}, "invalid_html_source"),
    ("", "missing_coach_layout"),
    ("<title>Access denied</title>", "blocked_html"),
    ('<form id="challenge-form"></form>' + box("1x Cup", ALPHA), "blocked_html"),
    (box("All titles", ""), "missing_coach_title"),
    (box("New layout", ALPHA), "unknown_coach_section"),
    (box("One x Cup", ALPHA), "unknown_coach_section"),
    (box("All titles", ALPHA + title("1x Cup")), "missing_coach_title"),
    (box("All titles", title("2.5x Cup") + ALPHA), "missing_coach_title"),
    (box("All titles", title("1x Cup") + ALPHA) + box("1x Cup", ALPHA), "ambiguous_coach_layout"),
    (box("1x Cup", title("1x Cup") + ALPHA), "ambiguous_coach_layout"),
    (box("2x Cup", ALPHA), "aggregate_count_mismatch"),
    (box("0x Cup", ""), "missing_coach_details"),
    (box("2x Cup", ALPHA + "<tr><td>2022</td></tr>"), "invalid_coach_row"),
    (box("1x Cup", ALPHA + "<td>2024</td><td>National XI</td>"), "invalid_coach_row"),
    (box("1x Cup", ALPHA.replace("2022", "unknown")), "invalid_coach_season"),
    (box("1x Cup", ALPHA.replace("/021", "/oops")), "invalid_club_link"),
    (box("1x Cup", ALPHA.replace('href="/alpha/startseite/verein/021"', "")), "invalid_club_link"),
    (box("1x Cup", BETA.replace("/22", "/23", 1)), "club_id_conflict"),
])
def test_coach_fails_whole_capture(source, reason):
    with pytest.raises(AchievementParseError) as exc:
        parse(source, "coach")
    assert exc.value.reason == reason
