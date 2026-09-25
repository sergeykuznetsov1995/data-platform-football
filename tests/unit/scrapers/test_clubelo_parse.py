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


@pytest.mark.parametrize("old, new", [
    ("<th>Elo %</th><th>FT</th>", "<th>FT</th><th>Elo %</th>"),  # swapped columns
    ("<th>P</th>", "<th>Pen</th>"),  # renamed column
])
def test_match_headers_must_match_exactly(riverplate_html, old, new):
    assert old in riverplate_html
    with pytest.raises(LayoutChanged, match="headers changed"):
        parse_club_page(riverplate_html.replace(old, new), "riverplate")


def test_match_table_without_rows_fails_closed(riverplate_html):
    start = riverplate_html.index("<tr><td>", riverplate_html.index("<h3>Calculation</h3>"))
    end = riverplate_html.index("</table>", start)
    with pytest.raises(LayoutChanged, match="no rows"):
        parse_club_page(riverplate_html[:start] + riverplate_html[end:], "riverplate")


def test_missing_elo_pct_span_fails_closed(riverplate_html):
    broken = riverplate_html.replace('<span class="min1081">67.9</span>', "", 1)
    with pytest.raises(LayoutChanged, match="Elo %"):
        parse_club_page(broken, "riverplate")


@pytest.mark.parametrize("old, new, what", [
    ('"Elo": 1784.0233989796975', '"Elo": NaN', "Elo"),
    ('"Golo": 1.3348056', '"Golo": Infinity', "Golo"),
    ('"Elo": 1784.0233989796975', '"Elo": "1784"', "Elo"),
    ('"segment_id": 0', '"segment_id": 1.5', "segment_id"),
])
def test_bad_vega_values_fail_closed(riverplate_html, old, new, what):
    assert old in riverplate_html
    with pytest.raises(LayoutChanged, match=what):
        parse_club_page(riverplate_html.replace(old, new, 1), "riverplate")


def test_long_opponent_names_are_joined_from_both_spans():
    page = parse_club_page(fixture_html("club_riverplate.html.gz"), "riverplate")
    names = {m["opp_name"] for m in page.matches}
    assert "Argentinos Juniors" in names and "Argentinos Junio" not in names


@pytest.mark.parametrize("datasets", ['"datasets": [], "x": {', '"datasets": null, "x": {',
                                      '"datasets": {"a": 5}, "x": {'])
def test_bad_vega_datasets_shape_fails_closed(riverplate_html, datasets):
    with pytest.raises(LayoutChanged, match="vegaJson"):
        parse_club_page(riverplate_html.replace('"datasets": {', datasets, 1), "riverplate")


# ---------------------------------------------------------------------------
# /Ranking and /Results — daily snapshot (#1463)
# ---------------------------------------------------------------------------

from datetime import datetime  # noqa: E402
import re  # noqa: E402

from scrapers.clubelo.parse import check_ranking, parse_ranking, parse_results  # noqa: E402


@pytest.fixture(scope="module")
def ranking_html():
    return fixture_html("Ranking.html.gz")


@pytest.fixture(scope="module")
def ranking(ranking_html):
    return parse_ranking(ranking_html)


@pytest.fixture(scope="module")
def results_html():
    return fixture_html("Results.html.gz")


def test_ranking_fixture_snapshot(ranking):
    check_ranking(ranking)  # the 2026-09-22 page passes the contract
    assert ranking.rating_date == RATING_DATE  # from the page h1, not the run date
    assert ranking.page_created_at == datetime(2026, 9, 24, 8, 50, 18)
    assert (ranking.elo_rows, ranking.provisional, len(ranking.rows)) == (1741, 53, 1794)
    assert ranking.levels_matched == 1722 and ranking.levels_matched_pct == 98.91
    assert ranking.elo_precise_matched == 100 and ranking.elo_precise_ambiguous == 0
    assert len(ranking.linked_slugs) == 498
    assert ranking.rows[0] == {
        "club_key": "Bayern", "slug": "Bayern", "name": "Bayern München", "country": "GER",
        "rank": 1, "elo": 2046, "elo_delta_1d_raw": "-0.00", "golo": 2.38, "level": 1,
        "level_group": "Level 1 (18 teams) ⌀1751", "level_section": "Germany",
        "elo_precise": 2046.021137444632, "is_provisional": False,
    }
    assert all(r["elo_delta_1d_raw"] == "-0.00" for r in ranking.rows[:1741])
    assert sum(r["elo_precise"] is not None for r in ranking.rows) == 100


def test_ranking_provisional_rows(ranking):
    prov = [r for r in ranking.rows if r["is_provisional"]]
    assert len(prov) == 53
    assert all(r["slug"] is None and r["club_key"].startswith("~") for r in prov)
    lok = next(r for r in prov if r["name"] == "Lok Leipzig")
    assert (lok["club_key"], lok["elo"], lok["golo"], lok["level_group"]) == (
        "~GER:Lok Leipzig", 1255, None, "Lower")
    assert len({r["club_key"] for r in ranking.rows}) == len(ranking.rows)


def test_negative_provisional_elo(ranking_html):
    # "-29p" occurs in the continent tables (Oceania, no flag); here in a country table
    assert '<td class="r">1255p</td>' in ranking_html
    page = parse_ranking(ranking_html.replace('<td class="r">1255p</td>', '<td class="r">-29p</td>', 1))
    lok = next(r for r in page.rows if r["club_key"] == "~GER:Lok Leipzig")
    assert lok["elo"] == -29 and lok["is_provisional"] is True


def test_slug_twins_are_not_glued(ranking):
    twins = {r["club_key"]: r for r in ranking.rows if r["slug"] in ("Vikingur", "vikingur")}
    assert set(twins) == {"Vikingur", "vikingur"}
    assert (twins["Vikingur"]["country"], twins["vikingur"]["country"]) == ("ISL", "FRO")


def test_unmatched_levels_fail_closed(ranking_html):
    # club links of the country tables no longer point to eloData slugs
    broken = re.sub(r'href="/([^"]+)"><span class="NonAst">', r'href="/zz\1"><span class="NonAst">',
                    ranking_html)
    page = parse_ranking(broken)
    with pytest.raises(LayoutChanged, match="C6 levels matched"):
        check_ranking(page)


def test_thresholds_are_checked(ranking):
    with pytest.raises(LayoutChanged, match="C5 eloData has 1741 rows"):
        check_ranking(ranking, min_rows=1742)
    with pytest.raises(LayoutChanged, match="C6"):
        check_ranking(ranking, min_levels_ratio=0.99)


@pytest.mark.parametrize("old, new, check", [
    ("<h1>", "<h2>", "h1"),
    ("Page created on", "Page built on", "Page created"),
    ("eloData = [", "eloRows = [", "C1 eloData not found"),
    ("var vegaJson = ", "var otherJson = ", "C1 vegaJson"),
    ("'2046', '-0.00', '2.38']", "'2046', '-0.00']", "C2 eloData row 0"),
    ("'2046', '-0.00', '2.38']", "'2046.5', '-0.00', '2.38']", "C3 eloData row 0 Elo"),
    ('<small> 1 </small><a href="/Bayern">', '<small> 1 </small><a class="x" href="/Bayern">', "C3 eloData row 0 club cell"),
    ('<td class="r">2046</td>', '<td class="r">2046?</td>', "C3 country table 'Germany'"),
])
def test_ranking_layout_breaks_fail_closed(ranking_html, old, new, check):
    assert old in ranking_html
    with pytest.raises(LayoutChanged, match=re.escape(check)):
        parse_ranking(ranking_html.replace(old, new, 1))


def test_ranking_without_country_tables_fails_closed(ranking_html):
    broken = ranking_html.replace('<div class="accordion-header"> <a href=',
                                  '<div class="accordion-header"> <b x=')
    with pytest.raises(LayoutChanged, match="C1 no country tables"):
        parse_ranking(broken)


def test_results_fixture(results_html):
    assert "</tr><tr><tr>" in results_html  # the broken markup is really there
    page = parse_results(results_html)
    assert page.rating_date == RATING_DATE
    assert page.page_created_at == datetime(2026, 9, 24, 8, 57, 26)
    assert len(page.rows) == 63 and page.duplicates == 0
    by_date = {}
    for row in page.rows:
        by_date[row["match_date"]] = by_date.get(row["match_date"], 0) + 1
    # the block before the first separator carries the h1 date
    assert by_date == {date(2026, 9, 22): 3, date(2026, 9, 21): 15, date(2026, 9, 20): 45}
    assert [r["row_seq"] for r in page.rows] == list(range(63))
    assert sum(not r["is_final"] for r in page.rows) == 4
    no_score = [r for r in page.rows if r["ft"] is None]
    assert len(no_score) == 1 and no_score[0]["is_final"] is False  # kept, not dropped
    jag = page.rows[2]
    assert (jag["home_key"], jag["away_key"], jag["away_slug"], jag["away_name"]) == (
        "independiente-medellin", "~COL:Jaguares", None, "Jaguares")
    assert (jag["prior_delta"], jag["prior_delta_sigma"], jag["hfa"], jag["elo_pct"], jag["ft"]) == (
        201.9, 82.0, 85.0, 83.9, "2-1")
    final = next(r for r in page.rows if r["home_slug"] == "barracas-central")
    assert final["match_date"] == date(2026, 9, 21) and final["is_final"] is True
    assert (final["game_delta"], final["game_delta_sigma"]) == (-423.4, 456.0)
    assert (final["post_game_delta"], final["post_game_delta_sigma"]) == (-110.5, 79.0)


def test_results_duplicate_key_keeps_first(results_html):
    start = results_html.index('<tr><td class="l"><a href="/CHI">')
    end = results_html.index("</tr>", start) + len("</tr>")
    page = parse_results(results_html[:end] + results_html[start:end] + results_html[end:])
    assert len(page.rows) == 63 and page.duplicates == 1
    assert page.rows[1]["row_seq"] == 2  # the page order is kept


@pytest.mark.parametrize("old, new, check", [
    ('<td class="l" colspan="3">2026-09-21</td>', '<td class="l" colspan="3">21.09.2026</td>',
     "not an ISO date"),
    ("<th>FT</th>", "<th>Score</th>", "C1 results table headers changed"),
    ('<td class="c">57</td>', '<td class="c">57</td><td>x</td>', "C3 results row 0 has 11 cells"),
    ('<h1><a href="/2026-09-22/Results">', '<h1><a href="/2026-09-22/Fixtures">', "expected 'Results'"),
])
def test_results_layout_breaks_fail_closed(results_html, old, new, check):
    assert old in results_html
    with pytest.raises(LayoutChanged, match=re.escape(check)):
        parse_results(results_html.replace(old, new, 1))
