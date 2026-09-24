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
