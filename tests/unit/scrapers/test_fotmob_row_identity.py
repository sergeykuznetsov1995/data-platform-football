from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest

from scrapers.fotmob.row_identity import identity_value, row_multiset


@pytest.mark.parametrize(('expected', 'persisted', 'kind'), [
    ('7', 7, 'BIGINT'),
    (7, '7', 'VARCHAR'),
    ('False', False, 'BOOLEAN'),
    (datetime(2026, 7, 1, 12), date(2026, 7, 1), 'DATE'),
    ('2026-07-01T12:00:00Z', datetime(2026, 7, 1, 12), 'TIMESTAMP(6)'),
    (datetime(2026, 7, 1, 12, tzinfo=timezone.utc), datetime(2026, 7, 1, 12), 'TIMESTAMP(6)'),
    (Decimal('1.235'), Decimal('1.24'), 'DECIMAL(8, 2)'),
    (pd.NA, None, 'VARCHAR'),
    (float('nan'), None, 'DOUBLE'),
])
def test_identity_matches_persisted_trino_scalar(expected, persisted, kind):
    assert identity_value(expected, kind) == identity_value(persisted, kind)


def test_identity_preserves_duplicate_multiplicity_and_ignores_row_order():
    left = row_multiset([{'id': '1'}, {'id': '2'}, {'id': '1'}], ['id'], {})
    same = row_multiset([{'id': '1'}, {'id': '1'}, {'id': '2'}], ['id'], {})
    fewer = row_multiset([{'id': '1'}, {'id': '2'}], ['id'], {})
    assert left == same
    assert left != fewer


@pytest.mark.parametrize(('value', 'kind'), [
    (True, 'BIGINT'), (1.5, 'BIGINT'), (Decimal('1.5'), 'INTEGER'),
    (True, 'DOUBLE'), (False, 'REAL'),
])
def test_identity_refuses_values_the_physical_writer_would_reject(value, kind):
    with pytest.raises(ValueError, match='incompatible'):
        identity_value(value, kind)
