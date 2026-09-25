"""ClubElo page parsers (#1462) on the 2026-09-24 fixtures + synthetic layout breaks."""

from __future__ import annotations

from datetime import date

import pytest

from scrapers.clubelo.parse import LayoutChanged, parse_club_page, parse_ranking_slugs
from tests.unit.scrapers.clubelo_fakes import fixture_html

RATING_DATE = date(2026, 9, 22)


@pytest.fixture(scope="module")
def riverplate_html():
    return fixture_html("club_riverplate.html.gz")


def test_ranking_queue_is_the_498_linked_slugs():
    rating_date, slugs = parse_ranking_slugs(fixture_html("Ranking.html.gz"))
    assert rating_date == RATING_DATE
    assert len(slugs) == len(set(slugs)) == 498
    assert slugs[:3] == ["Bayern", "Dortmund", "Leverkusen"]
    assert {"riverplate", "Arsenal", "lsapi-4199", "santos-fc_2"} <= set(slugs)
    # the dead club (302) and country flags are not in the queue
    assert "lsapi-2483" not in slugs and "GER" not in slugs


def test_ranking_without_country_links_fails_closed():
    with pytest.raises(LayoutChanged):
        parse_ranking_slugs('<h1><a href="/2026-09-22/Ranking">Ranking</a></h1><div class="stamm"></div>')


def test_ranking_without_h1_fails_closed():
    with pytest.raises(LayoutChanged, match="h1"):
        parse_ranking_slugs(fixture_html("Ranking.html.gz").replace("<h1>", "<h2>"))


@pytest.mark.parametrize(
    "slug, points, first, last, no_result",
    [
        ("riverplate", 205, date(2022, 9, 24), date(2026, 9, 12), 2),
        ("Arsenal", 220, date(2022, 10, 1), date(2026, 9, 15), 0),
        ("lsapi-4199", 30, date(2026, 1, 21), date(2026, 9, 13), 4),
        ("santos-fc_2", 216, date(2022, 9, 28), date(2026, 9, 16), 2),
    ],
)
def test_club_pages(slug, points, first, last, no_result):
    page = parse_club_page(fixture_html(f"club_{slug}.html.gz"), slug)
    assert page.captured_rating_date == RATING_DATE
    assert len(page.points) == points
    assert (page.points[0]["point_date"], page.points[-1]["point_date"]) == (first, last)
    assert [p["point_seq"] for p in page.points] == list(range(points))
    assert len(page.matches) == 16
    assert [m["row_seq"] for m in page.matches] == list(range(16))
    assert sum(not m["has_result"] for m in page.matches) == no_result


def test_riverplate_header_and_rows(riverplate_html):
    page = parse_club_page(riverplate_html, "riverplate")
    assert page.club_name == "River Plate"
    assert page.header == {"elo": 1732, "elo_best": 1866,
                           "elo_best_reached_on": date(1986, 7, 24), "golo": 1.12}
    assert page.points[0] == {"point_seq": 0, "point_date": date(2022, 9, 24),
                              "elo": 1784.0233989796975, "golo": 1.3348056, "segment_id": 0}
    first = page.matches[0]
    assert first["match_date"] == date(2026, 9, 19)
    assert (first["venue"], first["opp_slug"], first["opp_tlc"], first["opp_name"]) == (
        "H", "huracan", "HUR", "Huracán")
    assert (first["opp_country"], first["opp_rank"]) == ("ARG", 127)
    # value and sigma apart; Elo % from span.min1081 (not the rounded max1080)
    assert (first["prior_delta"], first["prior_delta_sigma"]) == (55.0, 80.0)
    assert first["hfa"] == 75.0 and first["elo_pct"] == 67.9
    assert first["ft"] == "1-2" and first["et"] is None and first["pen"] is None
    assert (first["game_delta"], first["game_delta_sigma"]) == (-438.0, 436.0)
    assert (first["post_game_delta"], first["post_game_delta_sigma"]) == (39.0, 79.0)
    assert first["elo_change"] is None and first["new_elo"] == 1732 and first["new_rank"] == 80
    assert first["has_result"] is True


def test_six_cell_row_is_a_fixture_without_result(riverplate_html):
    rows = [m for m in parse_club_page(riverplate_html, "riverplate").matches if not m["has_result"]]
    row = next(m for m in rows if m["match_date"] == date(2026, 8, 12))
    assert (row["opp_slug"], row["prior_delta"], row["prior_delta_sigma"], row["elo_pct"]) == (
        "santa-fe", 62.0, 81.0, 46.6)
    assert row["ft"] is None and row["new_elo"] is None and row["game_delta"] is None


def test_santos_keeps_both_rows_of_the_duplicate_match():
    page = parse_club_page(fixture_html("club_santos-fc_2.html.gz"), "santos-fc_2")
    same_day = [m for m in page.matches if m["match_date"] == date(2026, 9, 2)]
    assert len(same_day) == 2
    assert sorted(m["has_result"] for m in same_day) == [False, True]


def _break_first_row(html: str, cells: int) -> str:
    """Rewrite the first match row to ``cells`` cells (synthetic layout change)."""

    start = html.index("<tr><td>", html.index("<h3>Calculation</h3>"))
    end = html.index("</tr>", start)
    row = "<tr>" + "".join(f"<td>{i}</td>" for i in range(cells))
    return html[:start] + row + html[end:]


@pytest.mark.parametrize("cells", [5, 7, 13, 15])
def test_row_with_other_cell_count_fails_closed(riverplate_html, cells):
    with pytest.raises(LayoutChanged, match=f"{cells} cells"):
        parse_club_page(_break_first_row(riverplate_html, cells), "riverplate")


def test_missing_vega_fails_closed(riverplate_html):
    with pytest.raises(LayoutChanged, match="vegaJson"):
        parse_club_page(riverplate_html.replace("var vegaJson = ", "var otherJson = "), "riverplate")


def test_second_non_empty_dataset_fails_closed(riverplate_html):
    broken = riverplate_html.replace('"datasets": {', '"datasets": {"data-x": [{"Date": "2026-01-01"}],', 1)
    with pytest.raises(LayoutChanged, match="one non-empty vega dataset"):
        parse_club_page(broken, "riverplate")


def test_missing_header_fails_closed(riverplate_html):
    with pytest.raises(LayoutChanged, match="header"):
        parse_club_page(riverplate_html.replace("reached on", "reached"), "riverplate")


def test_header_of_14_th_required(riverplate_html):
    with pytest.raises(LayoutChanged, match="14 th"):
        parse_club_page(riverplate_html.replace("<th>New Rank</th>", ""), "riverplate")


def test_h1_of_another_page_fails_closed(riverplate_html):
    with pytest.raises(LayoutChanged, match="expected 'boca'"):
        parse_club_page(riverplate_html, "boca")


def test_non_numeric_cell_fails_closed(riverplate_html):
    broken = riverplate_html.replace('<td class="r">1732</td>', '<td class="r">n/a</td>', 1)
    with pytest.raises(LayoutChanged, match="New Elo"):
        parse_club_page(broken, "riverplate")
