"""Synthetic offline captures; no live Transfermarkt data or calls."""

import hashlib
import json
from dataclasses import FrozenInstanceError

import pytest

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
    assert PARSER_VERSION == "transfermarkt-rumours/1"
    assert SCHEMA_VERSION == "transfer_rumour_assertion/1"
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
