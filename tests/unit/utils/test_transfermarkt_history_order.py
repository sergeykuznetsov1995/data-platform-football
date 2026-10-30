from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from scrapers.transfermarkt.denominator import COLUMNS, load_denominator
from scrapers.transfermarkt.registry import deterministic_scope_id
from utils import transfermarkt_backfill_state as state
from utils.transfermarkt_history_order import history_claim_key

NOW = datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)
WINDOW = ",".join(str(year) for year in range(2015, 2025))
# id, tier, class, route, step2 window (cups and national teams list none)
COMPETITIONS = (
    ("GB1", "1", "core_club", "wettbewerb", WINDOW),
    ("ES1", "1", "core_club", "wettbewerb", WINDOW),
    ("ES2", "2", "core_club", "wettbewerb", WINDOW),
    ("FAC", "cup", "core_club", "pokal", ""),
    ("EM", "international", "core_national", "pokal", ""),
)


def _denominator(tmp_path):
    path = tmp_path / "denominator.tsv"
    path.write_text("\t".join(COLUMNS) + "\n" + "".join(
        "\t".join([cid, "Competition", "England", "UEFA", tier, klass, route, "1",
                   "2025", step2, "", "", "", "rule:default"]) + "\n"
        for cid, tier, klass, route, step2 in COMPETITIONS
    ))
    return load_denominator(path)


def _campaign(pairs):
    targets = tuple(
        state.HistoricalScopeTarget(
            scope_id=deterministic_scope_id(cid, edition),
            competition_id=cid,
            edition_id=edition,
            canonical_competition_id=f"TM-{cid}",
            canonical_season=edition,
            registry_snapshot_id="registry-1",
        )
        for cid, edition in pairs
    )
    waiting = state.BackfillCampaign.build(
        registry_snapshot_id="registry-1", policy_sha256="a" * 64,
        parser_revision="parser", schema_revision="schema", targets=targets, now=NOW,
    )
    campaign = waiting.transition(state.CampaignStatus.ACTIVE, now=NOW)
    return campaign, [state.BackfillScopeState.initial(campaign, item, now=NOW) for item in campaign.targets]


def _claimed(campaign, scopes, key, **kwargs):
    claim = state.claim_scopes(campaign, scopes, lease_owner="worker", now=NOW, sort_key=key, **kwargs)
    by_id = {item.target.scope_id: item.target for item in scopes}
    return [(by_id[sid].competition_id, by_id[sid].edition_id) for sid in claim.batch.scope_ids]


def _order(key, scopes):
    return [(item.target.competition_id, item.target.edition_id)
            for item in sorted(scopes, key=lambda item: (1, *key(item)))]


# EM is played every 4 years: its newest historical edition 2020 is the previous one.
EXPECTED = [
    ("GB1", "2025"), ("ES1", "2025"), ("ES2", "2025"), ("FAC", "2025"), ("EM", "2020"),
    ("GB1", "2024"), ("ES1", "2024"), ("ES2", "2019"), ("GB1", "2015"), ("FAC", "2024"), ("EM", "2016"),
    ("GB1", "2005"), ("EM", "2008"),
    ("XX9", "2024"),
]


def test_steps_then_kind_season_top_leagues_and_tier(tmp_path):
    campaign, scopes = _campaign(list(reversed(EXPECTED)))
    key = history_claim_key(_denominator(tmp_path), campaign.targets)
    assert _order(key, scopes) == EXPECTED
    assert _claimed(campaign, scopes, key) == EXPECTED[:state.MAX_SCOPE_BATCH]
    assert _claimed(campaign, list(reversed(scopes)), key) == EXPECTED[:state.MAX_SCOPE_BATCH]


def test_retry_first_and_filters_still_apply(tmp_path):
    campaign, scopes = _campaign(EXPECTED)
    key = history_claim_key(_denominator(tmp_path), campaign.targets)
    retry_index = next(i for i, item in enumerate(scopes) if item.target.edition_id == "2008")
    scopes[retry_index] = replace(scopes[retry_index], status=state.ScopeStatus.RETRYABLE_ERROR,
                                  next_retry_at=NOW - timedelta(minutes=1))
    assert _claimed(campaign, scopes, key, limit=2) == [("EM", "2008"), ("GB1", "2025")]
    allowed = {item.target.scope_id for item in scopes if item.target.competition_id == "ES2"}
    assert _claimed(campaign, scopes, key, limit=8, eligible_scope_ids=allowed) == [
        ("ES2", "2025"), ("ES2", "2019")]


def test_non_numeric_edition_goes_to_tail_without_error(tmp_path):
    campaign, scopes = _campaign([("GB1", "2005"), ("GB1", "abc")])
    key = history_claim_key(_denominator(tmp_path), campaign.targets)
    assert _claimed(campaign, scopes, key) == [("GB1", "2005"), ("GB1", "abc")]


def test_real_denominator_puts_cup_window_before_deep_league_history():
    denominator = load_denominator()
    assert denominator.row("CL").route == "pokal" and denominator.row("CL").step2_saison_ids == ()
    campaign, scopes = _campaign([("GB1", "2025"), ("GB1", "2005"), ("CL", "2025"), ("CL", "2024")])
    key = history_claim_key(denominator, campaign.targets)
    assert _order(key, scopes) == [("GB1", "2025"), ("CL", "2025"), ("CL", "2024"), ("GB1", "2005")]
