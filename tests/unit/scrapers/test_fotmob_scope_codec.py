"""Exact source-scope codec tests for FotMob."""

import pytest

from scrapers.fotmob.scope_codec import (
    format_scope_token,
    parse_scope_groups,
    parse_scope_token,
    validate_scope_tokens,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "season", ["2025 Apertura", "2025 Clausura", "2024/2025", "Апертура 2025"]
)
def test_scope_round_trip_preserves_source_season(season):
    token = format_scope_token(230, season)

    assert parse_scope_token(token) == (230, season)


@pytest.mark.unit
def test_scope_parser_splits_only_on_first_equals_sign():
    assert parse_scope_token("230=2025=playoffs") == (230, "2025=playoffs")


@pytest.mark.unit
@pytest.mark.parametrize(
    "token",
    ["0=2025", "+1=2025", "１=2025", "1=", " 1=2025", "1=2025 ", "1=20,25", "1=20\n25"],
)
def test_invalid_scope_tokens_fail_closed(token):
    with pytest.raises(ValueError):
        parse_scope_token(token)


@pytest.mark.unit
def test_scope_groups_split_commas_once_and_deduplicate_without_reordering():
    values = ["230=2025 Apertura,47=2024/2025", "230=2025 Apertura"]

    assert parse_scope_groups(values) == (
        (230, "2025 Apertura"),
        (47, "2024/2025"),
    )
    assert validate_scope_tokens(
        ["230=2025 Apertura", "47=2024/2025", "230=2025 Apertura"]
    ) == ("230=2025 Apertura", "47=2024/2025")


@pytest.mark.unit
@pytest.mark.parametrize(
    "values",
    [
        ["230=2025,"],
        [",230=2025"],
        ["230=2025,,47=2024/2025"],
        [""],
    ],
)
def test_scope_groups_reject_empty_comma_fragments(values):
    with pytest.raises(ValueError, match="empty scope fragment"):
        parse_scope_groups(values)
