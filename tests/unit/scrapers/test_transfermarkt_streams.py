import copy
import pytest

from scrapers.transfermarkt.streams import TransfermarktStreams, StreamRateState, browser_profile


def test_default_is_current_only_and_slots_are_disjoint():
    streams = TransfermarktStreams.from_env({})
    assert streams.stream_ids == ('current-0',)
    assert streams.history_capacity == 0
    streams = TransfermarktStreams.from_env({'TM_HISTORY_STREAMS': '3'})
    assert streams.total_streams == 4
    assert len({browser_profile(stream)[0] for stream in streams.stream_ids}) == 4


@pytest.mark.parametrize('env', [
    {'TM_HISTORY_STREAMS': '-1'}, {'TM_HISTORY_STREAMS': '4'},
    {'TM_REQUESTS_PER_MINUTE': '0'}, {'TM_REQUESTS_PER_MINUTE': '13'},
    {'TM_REQUESTS_PER_MINUTE': '1.5'},
])
def test_capacity_cannot_extend_the_approved_limit(env):
    with pytest.raises(ValueError):
        TransfermarktStreams.from_env(env)


def test_independent_streams_no_burst_and_first_block_halves_one_stream():
    rates = StreamRateState(TransfermarktStreams(history_streams=1), 0)
    for stream in rates.streams.stream_ids:
        rates.grant(stream, 5)
    assert rates.site_block('history-0', 5, status=403) == ()
    assert rates.ready_at('current-0', 5) == 10
    assert rates.ready_at('history-0', 5) == 15
    with pytest.raises(ValueError, match='not ready'):
        rates.grant('history-0', 10)


def test_two_blocked_streams_halves_source_series_pauses_and_survives_restart():
    rates = StreamRateState(TransfermarktStreams(history_streams=1), 0)
    rates.site_block('history-0', 5, status=429)
    alerts = rates.site_block('current-0', 10, challenge=True)
    assert 'transfermarkt_source_rate_halved' in alerts
    rates.site_block('history-0', 15, status=403)
    assert 'transfermarkt_stream_paused' in rates.site_block('history-0', 20, status=403)
    assert rates.pause['history-0'] == 920
    saved = copy.deepcopy(rates.dump())
    resumed = StreamRateState(rates.streams, 21, saved)
    assert resumed.ready_at('history-0', 21) == 920
    assert resumed.source_slow == 1820
    # No lease/connection identity is part of the rate clock.
    assert resumed.ready_at('current-0', 21) == 31


def test_gateway_wait_does_not_count_as_site_block():
    rates = StreamRateState(TransfermarktStreams(history_streams=1), 0)
    before = copy.deepcopy(rates.dump())
    # Only the authenticated site-feedback API calls site_block: a local
    # permit wait never calls it, regardless of its own HTTP 429 response.
    assert rates.ready_at('history-0', 1) == 5
    assert rates.dump() == before


def test_disabling_and_reenabling_a_slot_keeps_its_site_cooldown():
    streams = TransfermarktStreams(history_streams=1)
    rates = StreamRateState(streams, 0)
    rates.site_block('history-0', 5, status=403)
    disabled = StreamRateState(TransfermarktStreams(), 6, copy.deepcopy(rates.dump()))
    with pytest.raises(ValueError, match='disabled'):
        disabled.ready_at('history-0', 6)
    resumed = StreamRateState(streams, 7, copy.deepcopy(disabled.dump()))
    assert resumed.slow['history-0'] == 1805


def test_legacy_state_migration_is_conservative_and_corruption_rejected():
    rates = StreamRateState(TransfermarktStreams(history_streams=2), 20,
                           {'schema_version': 1, 'last_granted_at_epoch': 19})
    assert all(rates.ready_at(key, 20) == 25 for key in rates.streams.stream_ids)
    bad = copy.deepcopy(rates.dump())
    bad['pause']['current-0'] = float('nan')
    with pytest.raises(ValueError):
        StreamRateState(rates.streams, 20, bad)
