"""#1387: the budget canon has no parent (daily) byte cap any more."""

from scrapers.transfermarkt import models


def test_parent_daily_byte_caps_are_unset_and_planning_is_an_estimate():
    assert models.PARENT_DAILY_HARD_PROVIDER_BYTE_CAP is None
    assert models.PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP is None
    assert not hasattr(models, 'EXTERNAL_DAILY_PROVIDER_BYTE_LIMIT')
    assert models.PARENT_DAILY_PLANNING_BYTES == 352_321_536
    assert models.BACKFILL_BATCH_SOFT_BYTE_STOP == 335_544_320
    # The scope cap is untouched.
    assert models.SCOPE_HARD_PROVIDER_BYTE_CAP == 24 * 1024 * 1024
    models._assert_budget_canon()


def test_canon_rejects_a_half_set_parent_pair(monkeypatch):
    import pytest

    monkeypatch.setattr(models, 'PARENT_DAILY_HARD_PROVIDER_BYTE_CAP', 400_000_000)
    with pytest.raises(AssertionError, match='parent byte cap'):
        models._assert_budget_canon()


def test_canon_and_scope_cycle_accept_the_same_parent_pairs():
    import pytest

    from dags.scripts.run_transfermarkt_scope_cycle import (
        ScopeCycleError,
        _parent_byte_caps,
    )

    scope_hard = models.SCOPE_HARD_PROVIDER_BYTE_CAP
    cases = [
        ((None, None), True),
        ((scope_hard, scope_hard), True),
        ((models.PARENT_DAILY_PLANNING_BYTES,
          models.BACKFILL_BATCH_SOFT_BYTE_STOP), True),
        ((scope_hard, 1), True),
        ((scope_hard - 1, 1), False),
        ((scope_hard, scope_hard + 1), False),
        ((scope_hard, 0), False),
        ((scope_hard, None), False),
        ((None, 1), False),
    ]
    for (hard, soft), ok in cases:
        assert models.parent_byte_caps_valid(hard, soft) is ok, (hard, soft)
        if ok:
            assert _parent_byte_caps(hard, soft) == (hard, soft)
        else:
            with pytest.raises(ScopeCycleError):
                _parent_byte_caps(hard, soft)
