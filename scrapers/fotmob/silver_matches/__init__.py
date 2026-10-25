"""Isolated first-release FotMob match conforming; no ingest or scheduler hooks."""

from .contract import Contract, load_contract
from .decoder import MaterializationError, MaterializedMatches, materialize

__all__ = [
    "Contract",
    "load_contract",
    "MaterializationError",
    "MaterializedMatches",
    "materialize",
]
