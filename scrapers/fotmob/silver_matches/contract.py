"""Explicit loading of the portable, versioned match contract."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Contract:
    schema: dict[str, Any]
    registry: dict[str, dict[str, str]]


def load_contract(directory: Path | str | None = None) -> Contract:
    """Read configuration explicitly; decoding itself performs no file access."""
    root = (
        Path(directory)
        if directory is not None
        else (
            Path(__file__).resolve().parents[3]
            / "configs/medallion/fotmob_match_silver"
        )
    )
    schema = json.loads((root / "schema.json").read_text(encoding="utf-8"))
    with (root / "field-registry.tsv").open(encoding="utf-8", newline="") as stream:
        registry = {r["json_path"]: r for r in csv.DictReader(stream, delimiter="\t")}
    return Contract(schema=schema, registry=registry)
