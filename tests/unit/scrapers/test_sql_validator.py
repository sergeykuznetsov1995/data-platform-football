"""Tests for SQL validator module."""
import pytest

from scrapers.base.sql_validator import (
    validate_identifier,
    validate_catalog_qualified_name,
    sanitize_filter_expr,
    validate_snapshot_id,
)


class TestValidateIdentifier:
    """Tests for validate_identifier()."""

    def test_valid_identifiers(self):
        assert validate_identifier("bronze") == "bronze"
        assert validate_identifier("silver") == "silver"
        assert validate_identifier("fbref_schedule") == "fbref_schedule"
        assert validate_identifier("_private") == "_private"
        assert validate_identifier("Table1") == "Table1"
        assert validate_identifier("a" * 128) == "a" * 128

    def test_empty_identifier(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_identifier("")

    def test_none_identifier(self):
        with pytest.raises(ValueError, match="must be a string"):
            validate_identifier(None)

    def test_numeric_identifier(self):
        with pytest.raises(ValueError, match="must be a string"):
            validate_identifier(123)

    def test_too_long_identifier(self):
        with pytest.raises(ValueError, match="too long"):
            validate_identifier("a" * 129)

    def test_starts_with_number(self):
        with pytest.raises(ValueError, match="Must match"):
            validate_identifier("1table")

    def test_contains_special_chars(self):
        with pytest.raises(ValueError, match="Must match"):
            validate_identifier("table-name")
        with pytest.raises(ValueError, match="Must match"):
            validate_identifier("table.name")
        with pytest.raises(ValueError, match="Must match"):
            validate_identifier("table name")

    def test_sql_injection_in_identifier(self):
        with pytest.raises(ValueError):
            validate_identifier("table; DROP TABLE users")
        with pytest.raises(ValueError):
            validate_identifier("table--comment")

    def test_dangerous_keywords(self):
        with pytest.raises(ValueError, match="reserved/dangerous"):
            validate_identifier("DROP")
        with pytest.raises(ValueError, match="reserved/dangerous"):
            validate_identifier("delete")
        with pytest.raises(ValueError, match="reserved/dangerous"):
            validate_identifier("INSERT")
        with pytest.raises(ValueError, match="reserved/dangerous"):
            validate_identifier("ALTER")

    def test_context_in_error(self):
        with pytest.raises(ValueError, match="schema"):
            validate_identifier("", context="schema")


class TestValidateCatalogQualifiedName:
    """Tests for validate_catalog_qualified_name()."""

    def test_catalog_schema(self):
        assert validate_catalog_qualified_name("iceberg", "bronze") == "iceberg.bronze"

    def test_catalog_schema_table(self):
        result = validate_catalog_qualified_name("iceberg", "bronze", "fbref_schedule")
        assert result == "iceberg.bronze.fbref_schedule"

    def test_invalid_catalog(self):
        with pytest.raises(ValueError):
            validate_catalog_qualified_name("ice;berg", "bronze")

    def test_invalid_schema(self):
        with pytest.raises(ValueError):
            validate_catalog_qualified_name("iceberg", "DROP")

    def test_invalid_table(self):
        with pytest.raises(ValueError):
            validate_catalog_qualified_name("iceberg", "bronze", "my-table")


class TestSanitizeFilterExpr:
    """Tests for sanitize_filter_expr()."""

    def test_valid_expressions(self):
        assert sanitize_filter_expr("season = 2024") == "season = 2024"
        assert sanitize_filter_expr("league = 'ENG-Premier League'") == "league = 'ENG-Premier League'"
        assert sanitize_filter_expr("age > 18 AND status = 'active'") == "age > 18 AND status = 'active'"

    def test_empty_expression(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            sanitize_filter_expr("")

    def test_none_expression(self):
        with pytest.raises(ValueError, match="must be a string"):
            sanitize_filter_expr(None)

    def test_semicolon_injection(self):
        with pytest.raises(ValueError, match="dangerous"):
            sanitize_filter_expr("1=1; DROP TABLE users")

    def test_comment_injection(self):
        with pytest.raises(ValueError, match="dangerous"):
            sanitize_filter_expr("1=1 -- comment")
        with pytest.raises(ValueError, match="dangerous"):
            sanitize_filter_expr("1=1 /* comment */")

    def test_ddl_injection(self):
        with pytest.raises(ValueError, match="dangerous"):
            sanitize_filter_expr("1=1 UNION SELECT * FROM secrets")
        with pytest.raises(ValueError, match="dangerous"):
            sanitize_filter_expr("x = 1 DROP TABLE users")
        with pytest.raises(ValueError, match="dangerous"):
            sanitize_filter_expr("a = 1 DELETE FROM users")
        with pytest.raises(ValueError, match="dangerous"):
            sanitize_filter_expr("a = 1 INSERT INTO admin")
        with pytest.raises(ValueError, match="dangerous"):
            sanitize_filter_expr("a = 1 ALTER TABLE users")
        with pytest.raises(ValueError, match="dangerous"):
            sanitize_filter_expr("a = 1 TRUNCATE TABLE users")

    def test_file_access_injection(self):
        with pytest.raises(ValueError, match="dangerous"):
            sanitize_filter_expr("x INTO OUTFILE '/etc/passwd'")
        with pytest.raises(ValueError, match="dangerous"):
            sanitize_filter_expr("x INTO DUMPFILE '/tmp/dump'")


class TestValidateSnapshotId:
    """Tests for validate_snapshot_id()."""

    def test_valid_integer(self):
        assert validate_snapshot_id(12345) == 12345
        assert validate_snapshot_id(0) == 0

    def test_valid_string_integer(self):
        assert validate_snapshot_id("12345") == 12345

    def test_negative_id(self):
        with pytest.raises(ValueError, match="non-negative"):
            validate_snapshot_id(-1)

    def test_non_numeric(self):
        with pytest.raises(ValueError, match="must be an integer"):
            validate_snapshot_id("abc")

    def test_none(self):
        with pytest.raises(ValueError, match="must be an integer"):
            validate_snapshot_id(None)

    def test_injection_string(self):
        with pytest.raises(ValueError, match="must be an integer"):
            validate_snapshot_id("1; DROP TABLE users")
