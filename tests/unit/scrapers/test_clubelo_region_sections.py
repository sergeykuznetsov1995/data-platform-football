"""Regional header links must not turn aggregate tables into country data."""

import pytest

from scrapers.clubelo.parse import LayoutChanged, parse_ranking, parse_ranking_slugs
from tests.unit.scrapers.clubelo_fakes import fixture_html


ROW = ('<tr><td class="l"><span class="NonAst">EUR</span>'
       '<span class="Ast">Regional Club</span></td><td class="r">902p</td></tr>')
LINK_ROW = ('<tr><td class="l"><a href="/regional-only">'
            '<span class="NonAst">REG</span><span class="Ast">Regional Club</span>'
            '</a></td><td class="r">902p</td></tr>')


def section(title, row, link=None, nested=""):
    header = f'<a href="{link}">{title}</a>' if link else title
    return (f'<div class="accordion-item"><div class="accordion-header"> {header}</div>'
            f'<div class="accordion-content"><table class="ast">{row}</table></div>'
            f'{nested}</div>')


def page_with(extra):
    return fixture_html("Ranking.html.gz") + f'<div class="stamm">{extra}</div>'


@pytest.mark.parametrize("title,link", [
    ("Europe", "UEFA"), ("Europe", None), ("Africa", "CAF"),
    ("Asia", "AFC"), ("Oceania", "OFC"), ("World", "FIFA"),
    ("North America", "CONCACAF"), ("South America", "CONMEBOL"),
])
def test_regional_sections_leave_snapshot_and_queue_unchanged(title, link):
    original = fixture_html("Ranking.html.gz")
    changed = page_with(section(title, ROW + LINK_ROW, link))
    assert parse_ranking(changed) == parse_ranking(original)
    assert parse_ranking_slugs(changed) == parse_ranking_slugs(original)


def test_nested_region_cannot_leak_through_country_ancestor():
    original = fixture_html("Ranking.html.gz")
    nested = section("Europe", ROW + LINK_ROW, "UEFA")
    changed = page_with(section("England", "", "ENG", nested))
    assert parse_ranking(changed) == parse_ranking(original)
    assert parse_ranking_slugs(changed) == parse_ranking_slugs(original)


def test_nested_country_still_contributes_once():
    row = LINK_ROW.replace('regional-only', 'synthetic-country-club').replace(
        '<td class="l">', '<td class="l"><img alt="ENG">')
    nested = section("England", row, "ENG")
    changed = page_with(section("Europe", ROW, "UEFA", nested))
    parsed = parse_ranking(changed)
    added = [r for r in parsed.rows if r['slug'] == 'synthetic-country-club']
    assert len(added) == 1
    assert added[0]['country'] == 'ENG'
    assert added[0]['level_section'] == 'England'
    assert added[0]['is_provisional'] is True
    assert parse_ranking_slugs(changed)[1].count('synthetic-country-club') == 1


def test_actual_country_provisional_without_identity_still_fails():
    with pytest.raises(LayoutChanged, match='C3 provisional club without country or name'):
        parse_ranking(page_with(section("England", ROW, "ENG")))


def test_regions_alone_do_not_satisfy_country_queue_contract():
    changed = ('<h1><a href="/2026-11-04/Ranking">Ranking</a></h1>'
               '<div class="stamm">' + section("Europe", LINK_ROW, "UEFA") + '</div>')
    with pytest.raises(LayoutChanged, match='no country sections'):
        parse_ranking_slugs(changed)


@pytest.mark.parametrize("second_cell", ["", '<td class="r">902p</td>'])
def test_region_nested_inside_country_table_does_not_add_history_links(second_cell):
    nested = section("Europe", LINK_ROW, "UEFA")
    wrapper = f'<tr><td class="l">{nested}</td>{second_cell}</tr>'
    original = fixture_html("Ranking.html.gz")
    changed = page_with(section("England", wrapper, "ENG"))
    assert parse_ranking_slugs(changed) == parse_ranking_slugs(original)
    # The malformed country row itself still fails closed, rather than being
    # silently skipped while trying to ignore its nested regional data.
    with pytest.raises(LayoutChanged, match='C3 country table'):
        parse_ranking(changed)
