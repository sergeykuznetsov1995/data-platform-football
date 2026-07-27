"""Public API for the source-native Understat ingestion."""

from .catalog import (
    LEAGUES,
    PRODUCTION_LEAGUES,
    UnderstatCatalog,
    UnderstatScope,
    season_slug,
    source_season_id_from_slug,
)
from .client import UnderstatClient, UnderstatHTTPError, UnderstatPayloadError
from .contracts import TABLE_CONTRACTS, UnderstatTableContract
from .coverage import coverage_exceptions_for_scope
from .parsers import UnderstatSchemaDrift
from .manifest import (
    CONTRACT_VERSION,
    UNDERSTAT_ENTITIES,
    ManifestStatus,
    ScopeAttempt,
    ScopeKey,
    UnderstatManifestRepository,
)
from .quality import (
    QualityReport,
    UnderstatQualityError,
    build_failure_attempt,
    build_scope_attempt,
    validate_understat_scope,
)
from .scraper import UnderstatScraper
from .service import UnderstatSource

__all__ = [
    "LEAGUES",
    "CONTRACT_VERSION",
    "ManifestStatus",
    "PRODUCTION_LEAGUES",
    "QualityReport",
    "ScopeAttempt",
    "ScopeKey",
    "TABLE_CONTRACTS",
    "UNDERSTAT_ENTITIES",
    "UnderstatCatalog",
    "UnderstatClient",
    "UnderstatHTTPError",
    "UnderstatPayloadError",
    "UnderstatManifestRepository",
    "UnderstatQualityError",
    "UnderstatSchemaDrift",
    "UnderstatScope",
    "UnderstatScraper",
    "UnderstatSource",
    "UnderstatTableContract",
    "build_failure_attempt",
    "build_scope_attempt",
    "coverage_exceptions_for_scope",
    "season_slug",
    "source_season_id_from_slug",
    "validate_understat_scope",
]
