"""Unit tests for utils.data_quality.build_generic_table_checks (tier-1 #8).

The helper composes existing CHECK factories into a baseline set
(row_count + no_nulls + no_duplicates). These tests assert the composition
only — no Trino connection is needed.
"""

import pytest

from utils.data_quality import build_generic_table_checks


@pytest.mark.unit
def test_table_only_emits_row_count():
    checks = build_generic_table_checks('gold.dim_manager')
    assert [c.kind for c in checks] == ['row_count']
    assert checks[0].params['table'] == 'gold.dim_manager'
    assert checks[0].params['min_rows'] == 1


@pytest.mark.unit
def test_pk_emits_row_count_no_nulls_no_duplicates():
    checks = build_generic_table_checks('gold.dim_manager', pk=['manager_id'])
    kinds = [c.kind for c in checks]
    assert kinds == ['row_count', 'no_nulls', 'no_duplicates']

    no_nulls = next(c for c in checks if c.kind == 'no_nulls')
    no_dups = next(c for c in checks if c.kind == 'no_duplicates')
    # PK columns are folded into the NOT NULL set.
    assert no_nulls.params['cols'] == ['manager_id']
    assert no_dups.params['pk'] == ['manager_id']


@pytest.mark.unit
def test_not_null_only_emits_no_nulls_without_duplicates():
    checks = build_generic_table_checks('silver.x', not_null=['a', 'b'])
    kinds = [c.kind for c in checks]
    assert kinds == ['row_count', 'no_nulls']
    assert next(c for c in checks if c.kind == 'no_nulls').params['cols'] == ['a', 'b']


@pytest.mark.unit
def test_pk_and_not_null_dedupe_and_order():
    checks = build_generic_table_checks(
        'gold.fct_x', pk=['id'], not_null=['id', 'name', 'name'],
    )
    no_nulls = next(c for c in checks if c.kind == 'no_nulls')
    # pk first, then not_null extras, de-duplicated, order preserved.
    assert no_nulls.params['cols'] == ['id', 'name']


@pytest.mark.unit
def test_params_propagate():
    checks = build_generic_table_checks(
        'gold.dim_season',
        pk=['season_id'],
        min_rows=5,
        where="is_current = true",
        severity='WARNING',
    )
    assert all(c.severity == 'WARNING' for c in checks)
    row_count = next(c for c in checks if c.kind == 'row_count')
    assert row_count.params['min_rows'] == 5
    assert all(c.params.get('where') == "is_current = true" for c in checks)
