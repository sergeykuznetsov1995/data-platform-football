"""Synthetic offline captures; no live Transfermarkt data or calls."""

import hashlib
import json
from dataclasses import FrozenInstanceError

import pytest

import re
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

from scrapers.transfermarkt import rumours as rumour_v2
from scrapers.transfermarkt.rumours import (
    ASSERTION_KEY_FIELDS,
    ENTITY_TYPE,
    OBSERVATION_KEY_FIELDS,
    PARSER_VERSION,
    SCHEMA_VERSION,
    CompetitionContext,
    RumourParseError,
    parse_rumours,
)


_GB1 = rumour_v2.CompetitionContext("GB1", "Premier League",
                                     "/premier-league/startseite/wettbewerb/GB1")
_LISTING = "/premier-league/geruechte/wettbewerb/GB1"
_SAVED = Path(__file__).resolve().parents[2] / "fixtures/transfermarkt/rumours"
_GB1_CAPTURES = (
    (1, "53ae52cdcb469843f3de7d23a9722e3a4b5578e71a59d7cb0f5b6ee107ba3edc",
     "2026-11-20T21:21:09.485303+00:00"),
    (2, "33e9f518feea9bccd4463cdfa3d7437a2ad398695381caad4e4b56e700a88687",
     "2026-11-20T21:21:24.425970+00:00"),
)


def _gb1_html(number):
    return (_SAVED / f"gb1-page-{number}.html").read_bytes()


def _discussion_url(raw):
    parts = urlsplit(raw[1:] if raw.startswith("//thread/") else raw)
    return parts._replace(scheme="https", netloc="www.transfermarkt.com").geturl()


@pytest.mark.parametrize("number,digest,clock", _GB1_CAPTURES)
def test_gb1_saved_observations(number, digest, clock):
    raw = _gb1_html(number)
    assert hashlib.sha256(raw).hexdigest() == digest
    html = raw.decode("utf-8")
    capture = rumour_v2.parse_rumours(html, competition=_GB1, observed_at=clock)
    assert capture.status == "observed_present"
    assert capture.parser_version == "transfermarkt-rumours/2"
    assert capture.schema_version == "transfer_rumour_assertion/2"
    assert len(capture.observations) == 25
    assert capture.duplicate_count == 0
    assert {obs.entity_type for obs in capture.observations} == {"transfer_rumour_assertion"}

    # Compare the player/club/date/post association on every source row. The
    # parser sorts observations by identity, independently of HTML row order.
    soup = BeautifulSoup(html, "html.parser")
    assert soup.select_one("#yw2 table.items") is not None
    table = soup.select_one("#yw1 table.items")
    headers = [cell.get_text(" ", strip=True)
               for cell in table.select(":scope > thead > tr > :is(th, td)")]
    expected = set()
    for row in table.select(":scope > tbody > tr"):
        cells = dict(zip(headers, row.find_all("td", recursive=False)))
        player = cells["Player"].select_one("a[href*='/spieler/']")["href"]
        club = cells["Interested club"].select_one("a[href*='/verein/']")["href"]
        source = cells["Most recent source from"]
        day, month, year = map(int, re.search(
            r"\b(\d{2})/(\d{2})/(\d{4})\b", source.get_text(" ", strip=True)
        ).groups())
        url = _discussion_url(source.select_one("a[href]")["href"])
        assert re.search(r"/thread_id/[1-9][0-9]*(?:/|$)", urlsplit(url).path)
        expected.add((
            re.search(r"/spieler/(\d+)", player)[1],
            re.search(r"/verein/(\d+)", club)[1],
            date(year, month, day).isoformat(), url,
        ))
    assert len(expected) == 25
    assert len({obs.player.id for obs in capture.observations}) > 1
    assert {(obs.player.id, obs.interested_club.id, obs.last_source_date, obs.thread_link)
            for obs in capture.observations} == expected
    for obs in capture.observations:
        assert obs.subject_id == f"transfermarkt:player:{obs.player.id}"
        assert obs.rumour_identity == f"thread:{obs.thread_link}"


def test_gb1_saved_page_plan():
    pages = [rumour_v2.parse_rumour_pagination(_gb1_html(n).decode("utf-8"), competition=_GB1)
             for n in (1, 2)]
    assert [(page.current_page, page.last_page) for page in pages] == [(1, 5), (2, 5)]
    plan = rumour_v2.build_rumour_page_plan(pages)
    assert len(set(plan)) == len(plan) == 5
    for page in pages:
        for number, url in page.links:
            assert url == plan[number - 1]
            assert urlsplit(url).netloc == "www.transfermarkt.com"
            assert "/geruechte/wettbewerb/GB1" in urlsplit(url).path


_SHORT_POST = "//thread/forum/154/thread_id/1208229/post_id/3426507#3426507"
_POST = "https://www.transfermarkt.com" + _SHORT_POST[1:]
_GRID_TABLE = (
    '<table class="items"><thead><tr><th>Player</th><th>Interested club</th>'
    '<th>Most recent source from</th><th>Assessment</th></tr></thead><tbody><tr>'
    '<td><a href="/p/profil/spieler/17">Player P</a></td>'
    '<td><a href="/c/startseite/verein/27">Club C</a></td>'
    f'<td><a href="{_POST}">20/11/2026</a></td>'
    f'<td>-<a href="{_SHORT_POST}"><i></i></a></td></tr></tbody></table>'
)
_STANDINGS = _GRID_TABLE.replace("<th>Player</th>", "<th>Rank</th>")


def _read_v2(html):
    return rumour_v2.parse_rumours(html, competition=_GB1, observed_at=_GB1_CAPTURES[0][2])


def test_rumour_grid_selection_and_post_identity():
    for html in (_GRID_TABLE + _STANDINGS,
                 f'<div id="yw1">{_GRID_TABLE}</div><div id="yw2">{_STANDINGS}</div>'):
        for source in (_POST, _SHORT_POST):
            obs, = _read_v2(html.replace(_POST, source)).observations
            assert obs.thread_link == _POST
            assert obs.rumour_identity == f"thread:{_POST}"
            assert obs.last_source_date == "2026-11-20"
            assert obs.probability_percent is None


def test_source_and_assessment_have_independent_values():
    obs, = _read_v2(_GRID_TABLE.replace("20/11/2026", "06/07/2026")).observations
    assert obs.last_source_date == "2026-07-06"
    html = _GRID_TABLE.replace(f'<a href="{_POST}">20/11/2026</a>', "-")
    obs, = _read_v2(html).observations
    assert (obs.thread_link, obs.last_source_date, obs.probability_percent) == (_POST, None, None)


@pytest.mark.parametrize("html", [
    _STANDINGS, _GRID_TABLE * 2, f'<div id="yw1">{_GRID_TABLE * 2}</div>',
    f'<div id="yw1">{_STANDINGS}</div><div id="yw2">{_GRID_TABLE}</div>',
    f'<div id="yw1"></div>{_GRID_TABLE}',
])
def test_invalid_rumour_grid_fails(html):
    with pytest.raises(rumour_v2.RumourParseError):
        _read_v2(html)


@pytest.mark.parametrize("old,bad,reason", [
    (_SHORT_POST, "//www.transfermarkt.com/thread/123", "invalid_url"),
    (_POST, "//other.example/post/123", "invalid_url"),
    (_POST, "https://other.example/post/123", "invalid_url"),
    (_SHORT_POST, "//thread/forum/154/thread_id/nope", "invalid_url"),
    ("20/11/2026", "11/20/2026", "invalid_date"),
    ("20/11/2026", "1/2/2026", "invalid_date"),
    ("20/11/2026", "31/02/2026", "invalid_date"),
])
def test_bad_rumour_source_or_assessment_fails(old, bad, reason):
    with pytest.raises(rumour_v2.RumourParseError) as error:
        _read_v2(_GRID_TABLE.replace(old, bad))
    assert (error.value.reason, error.value.row) == (reason, 1)


def _page_control(n, role="", label=None, href=None):
    url = href if href is not None else f"{_LISTING}/page/{n}"
    return (f'<li class="tm-pagination__list-item--{role}"><a class="tm-pagination__link" '
            f'href="{url}">{n if label is None else label}</a></li>')


def _read_pager(controls, **kwargs):
    html = f'<div id="yw1">{_GRID_TABLE}<ul class="pager">{controls}</ul></div>'
    return rumour_v2.parse_rumour_pagination(html, competition=_GB1, **kwargs)


_PAGE_ONE = _page_control(1, "active")
_PAGE_LAST = _page_control(3, "icon-last-page", "")


@pytest.mark.parametrize("extra", [
    _page_control(2, href="https://elsewhere.example" + _LISTING + "/page/2"),
    _page_control(2, href=_LISTING.replace("GB1", "L1") + "/page/2"),
    _page_control(2, href=_LISTING.replace("geruechte", "tabelle") + "/page/2"),
    _page_control(2, href=_LISTING + "/page/02"),
    _page_control(2, href=_LISTING + "?page=2&page=3"),
    _page_control(2, href=_LISTING + "?page=2&"),
    _page_control(2, href=_LISTING + "/page/2?page=2"),
    _page_control(2, label="3"),
    _page_control(1, "icon-next-page", ""),
    _page_control(3, "icon-next-page", ""),
    _page_control(2, "active"),
    _page_control(4, "icon-last-page", ""),
    _page_control(2) + _page_control(2, href=_LISTING + "?page=2"),
])
def test_pager_rejects_invalid_target(extra):
    with pytest.raises(rumour_v2.RumourParseError):
        _read_pager(_PAGE_ONE + extra + _PAGE_LAST)


def test_full_page_plan_requires_consistent_contiguous_evidence():
    partial = _read_pager(_PAGE_ONE + _PAGE_LAST)
    with pytest.raises(rumour_v2.RumourParseError, match="incomplete_page_plan"):
        rumour_v2.build_rumour_page_plan([partial])
    second = _read_pager(_page_control(2, "active") + _PAGE_LAST)
    assert rumour_v2.build_rumour_page_plan([partial, second]) == tuple(
        f"https://www.transfermarkt.com{_LISTING}/page/{n}" for n in (1, 2, 3))
    with pytest.raises(rumour_v2.RumourParseError, match="invalid_page_navigation"):
        rumour_v2.build_rumour_page_plan([partial, second, partial])
    with pytest.raises(rumour_v2.RumourParseError, match="invalid_page_navigation"):
        _read_pager(_PAGE_ONE + _PAGE_LAST, page_url=_LISTING + "/page/2")
    with pytest.raises(rumour_v2.RumourParseError, match="invalid_pager"):
        _read_pager(_PAGE_ONE + _page_control(2))


CONTEXT = CompetitionContext("GB1", "Example League", "/example/startseite/wettbewerb/GB1")
CLOCK = "2026-11-20T10:00:00+01:00"
UTC_CLOCK = "2026-11-20T09:00:00.000000Z"
HEADERS = ["Player", "Nation", "Age", "Club", "Interested club",
           "Most recent source", "Last reply", "Assessment"]
CELLS = [
    '<table class="inline-table"><tr><td><a href="/alex-example/profil/spieler/101">Alex Example</a></td></tr>'
    '<tr><td>Centre-Forward</td></tr></table>',
    '<img title="Exampleland"><img alt="Otherland">',
    '24',
    '<a href="/current/startseite/verein/201">Current FC</a>',
    '<a href="/interested/startseite/verein/301">Interested FC</a>'
    '<a href="/other/startseite/wettbewerb/ES1">Other League</a>',
    '<a href="/rumour/thread/501?b=2&amp;a=1#latest">19.11.2026</a>',
    '<a href="/rumour/thread/501?b=2&amp;a=1">Nov 20, 2026</a>',
    '72.5 %',
]


def row(cells=None):
    return '<tr>' + ''.join(f'<td>{c}</td>' for c in (CELLS if cells is None else cells)) + '</tr>'


def table(rows=None, headers=None):
    names = HEADERS if headers is None else headers
    return ('<table class="items"><thead><tr>' +
            ''.join(f'<th>{h}</th>' for h in names) +
            '</tr></thead><tbody>' + (row() if rows is None else rows) + '</tbody></table>')


def parse(html=None, clock=CLOCK, context=CONTEXT):
    return parse_rumours(table() if html is None else html, observed_at=clock, competition=context)


def changed(index, value):
    cells = list(CELLS)
    cells[index] = value
    return row(cells)


def error(html, reason):
    with pytest.raises(RumourParseError) as caught:
        parse(html)
    assert caught.value.reason == reason
    return caught.value


def test_complete_donor_row():
    result = parse()
    observation, = result.observations
    assert result.status == "observed_present"
    assert result.duplicate_count == 0
    assert observation.player.id == "101"
    assert observation.player.name == "Alex Example"
    assert observation.player.url == "https://www.transfermarkt.com/alex-example/profil/spieler/101"
    assert observation.subject_id == "transfermarkt:player:101"
    assert observation.interested_club.id == "301"
    assert observation.interested_club.name == "Interested FC"
    assert observation.interested_club.url == "https://www.transfermarkt.com/interested/startseite/verein/301"
    assert observation.interested_club_competition.id == "ES1"
    assert observation.interested_club_competition.name == "Other League"
    assert observation.interested_club_competition.url == "https://www.transfermarkt.com/other/startseite/wettbewerb/ES1"
    assert observation.current_club.id == "201"
    assert observation.current_club.name == "Current FC"
    assert observation.current_club.url == "https://www.transfermarkt.com/current/startseite/verein/201"
    assert observation.position == "Centre-Forward"
    assert observation.nationalities == ("Exampleland", "Otherland")
    assert observation.age == 24
    assert observation.probability_percent == 72.5
    assert observation.last_source_date == "2026-11-19"
    assert observation.last_reply_date == "2026-11-20"
    assert observation.thread_link == "https://www.transfermarkt.com/rumour/thread/501?a=1&b=2#latest"
    assert observation.observed_at == result.observed_at == UTC_CLOCK
    assert observation.competition.id == "GB1"
    assert observation.competition.url == "https://www.transfermarkt.com/example/startseite/wettbewerb/GB1"


def test_alias_headers_and_nullable_fields():
    headers = ["Player", "Nat.", "Age", "Current club", "Interested club",
               "Most recent source", "Last reply", "User assessment"]
    cells = list(CELLS)
    cells[1:4] = ['', '—', '-']
    cells[5] = ''
    cells[7] = '-'
    observation, = parse(table(row(cells), headers)).observations
    assert observation.last_source_date is None
    assert observation.last_reply_date == "2026-11-20"
    assert observation.thread_link.endswith("/rumour/thread/501?a=1&b=2")
    assert observation.current_club is None
    assert observation.age is None
    assert observation.nationalities == ()
    assert observation.probability_percent is None
    cells[6] = '-'
    observation, = parse(table(row(cells), headers)).observations
    assert observation.last_reply_date is None
    assert observation.thread_link is None
    assert observation.rumour_identity.startswith("surrogate:sha256:")


def test_minimal_supported_row_does_not_invent_optional_data():
    result = parse(table(row([CELLS[0], '<a href="/club/startseite/verein/301">Club</a>']),
                         ["Player", "Interested club"]))
    observation, = result.observations
    assert observation.interested_club_competition is None
    assert observation.current_club is None
    assert observation.last_source_date is observation.last_reply_date is None
    assert observation.probability_percent is None
    assert observation.thread_link is None


def test_replay_and_canonical_json_keys():
    first, second = parse(), parse()
    assert first == second
    assert first.to_json().encode() == second.to_json().encode()
    assert parse(clock="2026-11-20T09:00:00Z") == first
    payload = json.loads(first.to_json())
    assert payload == first.to_dict()
    assert isinstance(payload["observations"][0]["nationalities"], list)
    assert first.to_json() == json.dumps(payload, sort_keys=True, ensure_ascii=False,
                                         separators=(",", ":"), allow_nan=False)
    assert PARSER_VERSION == "transfermarkt-rumours/2"
    assert SCHEMA_VERSION == "transfer_rumour_assertion/2"
    assert ASSERTION_KEY_FIELDS == ("rumour_identity", "subject_id")
    assert OBSERVATION_KEY_FIELDS == ("rumour_identity", "subject_id", "observed_at", "correction_identity")
    obs = payload["observations"][0]
    expected = hashlib.sha256(json.dumps({k: obs[k] for k in OBSERVATION_KEY_FIELDS},
                                         sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert obs["observation_key"] == expected
    assert obs["correction_identity"] == "sha256:" + obs["content_hash"]
    assert obs["parser_version"] == PARSER_VERSION
    assert obs["schema_version"] == SCHEMA_VERSION


def test_identical_duplicates_reconcile_and_conflicts_fail_atomically():
    result = parse(table(row() * 3))
    assert result.duplicate_count == 2
    assert result.observations == parse().observations
    failure = error(table(row() + changed(7, "50%")), "conflicting_duplicate")
    assert failure.row == 2


def test_unchanged_observation_at_later_clock_is_history():
    first = parse().observations[0]
    later = parse(clock="2026-11-21T09:00:00Z").observations[0]
    assert first.rumour_identity == later.rumour_identity
    assert first.correction_identity == later.correction_identity
    assert first.observation_key != later.observation_key
    assert len({first.observation_key, later.observation_key}) == 2


@pytest.mark.parametrize("index,value", [
    (7, "80%"),
    (4, '<a href="/other/startseite/verein/302">Other FC</a>'),
    (6, '<a href="/rumour/thread/501?a=1&amp;b=2">2026-11-21</a>'),
    (5, '<a href="/rumour/thread/502">2026-11-19</a>'),
])
def test_later_changes_do_not_mutate_prior_observation(index, value):
    first = parse().observations[0]
    before = first.to_dict()
    later = parse(table(changed(index, value)), clock="2026-11-21T09:00:00Z").observations[0]
    assert first.to_dict() == before
    assert first.observation_key != later.observation_key
    assert first.content_hash != later.content_hash
    with pytest.raises(FrozenInstanceError):
        first.age = 99


def test_correction_at_same_clock_has_distinct_key():
    original = parse().observations[0]
    correction = parse(table(changed(7, "81%"))).observations[0]
    assert original.observed_at == correction.observed_at
    assert original.rumour_identity == correction.rumour_identity
    assert original.subject_id == correction.subject_id
    assert original.correction_identity != correction.correction_identity
    assert original.observation_key != correction.observation_key
    assert original.probability_percent == 72.5


def test_surrogate_replay_duplicates_and_changed_content():
    cells = list(CELLS)
    cells[5:7] = ['2026-11-19', '']
    original = parse(table(row(cells))).observations[0]
    assert original.rumour_identity == "surrogate:sha256:" + original.content_hash
    assert parse(table(row(cells) * 2)).duplicate_count == 1
    assert parse(table(row(cells))).observations[0] == original
    cells[7] = '90%'
    correction = parse(table(row(cells))).observations[0]
    assert correction.rumour_identity != original.rumour_identity
    assert correction.correction_identity != original.correction_identity


@pytest.mark.parametrize("rows", ['', '<tr><td class="no-entries" colspan="8">No entries available.</td></tr>'])
def test_structurally_confirmed_empty(rows):
    result = parse(table(rows))
    assert result.status == "observed_empty"
    assert result.observations == ()
    assert result.duplicate_count == 0
    assert result.observed_at == UTC_CLOCK
    assert "retracted" not in result.to_dict()
    assert "missing" not in result.to_dict()


@pytest.mark.parametrize("html,reason", [
    ('', 'blank_html'), ('  ', 'blank_html'), ('<html></html>', 'missing_table'),
    ('<table></table>', 'missing_table'),
    ('<table class="items"></table>', 'invalid_table_structure'),
    (table().replace('<tbody>', '').replace('</tbody>', ''), 'invalid_table_structure'),
    (table(headers=["Athlete", *HEADERS[1:]]), 'unknown_header'),
    (table(headers=[*HEADERS, "Player"]), 'duplicate_header'),
    (table(headers=[*HEADERS, "Nat."]), 'duplicate_header'),
    (table(headers=HEADERS[1:]), 'missing_header'),
    (table() + table(), 'ambiguous_table'),
    (table('<tr><td class="no-entries" colspan="7">No entries available.</td></tr>'), 'malformed_empty_row'),
    (table('<tr><td class="no-entries" colspan="8">Service unavailable</td></tr>'), 'malformed_empty_row'),
    (table('<tr><td colspan="8">No entries available.</td></tr>'), 'invalid_row_structure'),
    (table('<tr><td class="no-entries" colspan="8"><a href="/x">No entries available.</a></td></tr>'), 'malformed_empty_row'),
    (table('<tr><td class="no-entries" colspan="8">No entries available.</td><th>Unexpected</th></tr>'), 'malformed_empty_row'),
    (table('<tr><td class="no-entries" colspan="8" rowspan="2">No entries available.</td></tr>'), 'malformed_empty_row'),
    (table(row() + '<tr><td class="no-entries" colspan="8">No entries available.</td></tr>'), 'malformed_empty_row'),
    (table('<tr></tr>'), 'invalid_row_structure'),
    (table('<div>Broken layout</div>'), 'invalid_table_structure'),
])
def test_missing_broken_changed_or_malformed_empty_is_error(html, reason):
    error(html, reason)


@pytest.mark.parametrize("index,value,reason", [
    (0, '', 'invalid_player'),
    (0, '<a href="/a/profil/spieler/no-id">Alex</a>', 'invalid_player'),
    (0, '<a href="/a/profil/spieler/101"></a>', 'invalid_player'),
    (0, '<a href="/a/profil/spieler/101">A</a><a href="/b/profil/spieler/102">B</a>', 'invalid_player'),
    (4, '-', 'invalid_club'),
    (4, '<a href="/club/startseite/verein/broken">Club</a>', 'invalid_club'),
    (3, 'Unknown club', 'invalid_club'),
    (3, '<img title="Club without identity">', 'invalid_club'),
    (5, '31.02.2026', 'invalid_date'),
    (5, 'yesterday', 'invalid_date'),
    (6, '2026-11-20 2026-11-21', 'invalid_date'),
    (7, '101%', 'invalid_probability'),
    (7, '-1%', 'invalid_probability'),
    (7, 'NaN%', 'invalid_probability'),
    (7, '50', 'invalid_probability'),
    (2, 'adult', 'invalid_age'),
    (1, '<img>', 'invalid_nationality'),
    (5, '<a href="javascript:alert(1)">2026-11-19</a>', 'invalid_url'),
    (5, '<a href="https://evil.example/thread/1">2026-11-19</a>', 'invalid_url'),
    (5, '<a href="https://user@www.transfermarkt.com/thread/1">2026-11-19</a>', 'invalid_url'),
    (5, '<a href="/../thread/1">2026-11-19</a>', 'invalid_url'),
    (5, '<a href="/thread/%zz">2026-11-19</a>', 'invalid_url'),
    (6, '<a href="//evil.example/thread">2026-11-20</a>', 'invalid_url'),
    (0, '<a href="https://evil.example/a/profil/spieler/101">Alex</a>', 'invalid_url'),
    (4, '<a href="https://evil.example/club/startseite/verein/301">Club</a>', 'invalid_url'),
])
def test_bad_row_never_returns_partial_capture(index, value, reason):
    failure = error(table(row() + changed(index, value)), reason)
    assert failure.row == 2


@pytest.mark.parametrize("index", [0, 3, 4], ids=["player", "current_club", "interested_club"])
def test_entity_cell_rejects_external_non_identity_link(index):
    cell = CELLS[index] + '<a href="https://evil.example/external">External</a>'
    failure = error(table(changed(index, cell)), "invalid_url")
    assert failure.row == 1


def test_source_cell_rejects_external_link_after_valid_first_link():
    source = (CELLS[5] + '<a href="https://evil.example/thread">External source</a>')
    failure = error(table(changed(5, source)), "invalid_url")
    assert failure.row == 1


@pytest.mark.parametrize("clock", [None, '', '2026-11-20', '2026-11-20T09:00:00',
                                    '2026-02-30T09:00:00Z', '2026-11-20T09:00:00+01:60',
                                    '2026-11-20T09:00:00-00:00'])
def test_invalid_observation_clock(clock):
    with pytest.raises(RumourParseError, match="invalid_observed_at") as caught:
        parse(clock=clock)
    assert caught.value.reason == "invalid_observed_at"


@pytest.mark.parametrize("context", [None, {}, CompetitionContext('GB1', '', CONTEXT.url),
                                      CompetitionContext('GB2', 'League', CONTEXT.url),
                                      CompetitionContext('GB1', 'League', 'https://evil.example/wettbewerb/GB1')])
def test_invalid_competition_context(context):
    with pytest.raises(RumourParseError) as caught:
        parse(context=context)
    assert caught.value.reason == "invalid_competition"


def test_row_order_and_supported_host_normalization():
    other = changed(0, '<a href="/blair/profil/spieler/102">Blair Example</a>')
    assert parse(table(row() + other)).to_json() == parse(table(other + row())).to_json()
    assert parse(table().replace('/alex-example/profil/spieler/101',
                                 'http://transfermarkt.de/alex-example/profil/spieler/101')) == parse()


@pytest.mark.parametrize("value,expected", [('0%', 0.0), ('100%', 100.0), ('72,5%', 72.5)])
def test_probability_boundaries_and_decimal_comma(value, expected):
    assert parse(table(changed(7, value))).observations[0].probability_percent == expected


def test_same_clock_correction_has_independently_reproducible_content_hash():
    observation = parse().observations[0].to_dict()
    excluded = {'parser_version', 'schema_version', 'rumour_identity', 'observed_at',
                'content_hash', 'correction_identity', 'observation_key'}
    content = {key: value for key, value in observation.items() if key not in excluded}
    serialized = json.dumps(content, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    assert hashlib.sha256(serialized.encode('utf-8')).hexdigest() == observation['content_hash']


def test_rumours_produce_zero_matches_and_transfer_events():
    payload = parse().to_dict()
    observations = payload["observations"]
    assert payload["entity_type"] == ENTITY_TYPE == "transfer_rumour_assertion"
    assert {o["entity_type"] for o in observations} == {"transfer_rumour_assertion"}
    assert sum(o["entity_type"] in {"match", "transfer_event", "transfer_events"} for o in observations) == 0
    assert "matches" not in payload and "match_rows" not in payload
    assert "transfer_events" not in payload
    assert not any("match_id" in o or "transfer_event_id" in o for o in observations)
