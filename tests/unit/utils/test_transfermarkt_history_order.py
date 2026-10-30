from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from scrapers.transfermarkt.denominator import COLUMNS, load_denominator
from scrapers.transfermarkt.registry import deterministic_scope_id
from utils import transfermarkt_backfill_state as state
from utils.transfermarkt_history_order import history_claim_key

NOW = datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)
WINDOW = ",".join(str(year) for year in range(2015, 2025))
# id, tier, class, route, step2 window
COMPETITIONS = (
    ("GB1", "1", "core_club", "wettbewerb", WINDOW),
    ("L1", "1", "core_club", "wettbewerb", WINDOW),
    ("ES2", "2", "core_club", "wettbewerb", WINDOW),
    ("FAC", "cup", "core_club", "pokal", WINDOW),
    ("EM", "international", "core_national", "pokal", WINDOW),
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


PAIRS = [(cid, edition) for cid, *_ in COMPETITIONS for edition in ("2024", "2019", "2005")]
EXPECTED = [
    ("GB1", "2024"), ("L1", "2024"), ("ES2", "2024"), ("FAC", "2024"), ("EM", "2024"),
    ("GB1", "2019"), ("L1", "2019"), ("ES2", "2019"), ("FAC", "2019"), ("EM", "2019"),
    ("GB1", "2005"), ("L1", "2005"), ("ES2", "2005"), ("FAC", "2005"), ("EM", "2005"),
    ("XX9", "2024"),
]


def test_steps_then_top_leagues_kind_and_tier(tmp_path):
    key = history_claim_key(_denominator(tmp_path))
    campaign, scopes = _campaign(PAIRS + [("XX9", "2024")])
    ordered = sorted(scopes, key=lambda item: (1, *key(item)))
    assert [(item.target.competition_id, item.target.edition_id) for item in ordered] == EXPECTED
    assert _claimed(campaign, scopes, key) == EXPECTED[:state.MAX_SCOPE_BATCH]
    assert _claimed(campaign, list(reversed(scopes)), key) == EXPECTED[:state.MAX_SCOPE_BATCH]


def test_retry_first_and_filters_still_apply(tmp_path):
    key = history_claim_key(_denominator(tmp_path))
    campaign, scopes = _campaign(PAIRS)
    retry_index = next(i for i, item in enumerate(scopes) if item.target.edition_id == "2005"
                       and item.target.competition_id == "EM")
    scopes[retry_index] = replace(scopes[retry_index], status=state.ScopeStatus.RETRYABLE_ERROR,
                                  next_retry_at=NOW - timedelta(minutes=1))
    assert _claimed(campaign, scopes, key, limit=2) == [("EM", "2005"), ("GB1", "2024")]
    allowed = {item.target.scope_id for item in scopes if item.target.competition_id == "ES2"}
    assert _claimed(campaign, scopes, key, limit=8, eligible_scope_ids=allowed) == [
        ("ES2", "2024"), ("ES2", "2019"), ("ES2", "2005")]


def test_non_numeric_edition_goes_to_tail_without_error(tmp_path):
    key = history_claim_key(_denominator(tmp_path))
    campaign, scopes = _campaign([("GB1", "2005"), ("GB1", "abc")])
    assert _claimed(campaign, scopes, key) == [("GB1", "2005"), ("GB1", "abc")]
