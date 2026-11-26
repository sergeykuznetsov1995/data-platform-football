"""Table declarations and lineage shared by the non-match Bronze sections."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ObservationTable:
    columns: tuple[str, ...]
    natural_key: tuple[str, ...]
    scope_columns: tuple[str, ...]
    types: Mapping[str, str] = field(default_factory=dict)
    write_mode: str = 'merge'


def make_outcome(entity, capture, scope, *, status, reason, records=(), proof=None,
                 parser_revision, extracted_at, universe_version):
    from . import core

    context = core._context(capture, scope, extracted_at, universe_version)
    revisions = dict(parser_revision=parser_revision, schema_revision='1')
    rows = tuple(
        core._render(
            entity, {**record, 'observed_at': capture.captured_at}, scope, capture,
            extracted_at, context['run_id'], universe_version, status, reason, **revisions,
        )
        for record in records
    )
    return core.LaneOutcome(entity, status, reason, rows, proof=proof, **context, **revisions)
