"""Current season final capture is a durable prerequisite of historical work."""
from __future__ import annotations

import json
from datetime import timezone

TABLE = 'iceberg.ops.transfermarkt_season_close_v1'


def ddl():
    return f'''CREATE TABLE IF NOT EXISTS {TABLE} (
        scope_id varchar, competition_id varchar, edition_id varchar,
        status varchar, registry_snapshot_id varchar, detected_at timestamp(6),
        completed_at timestamp(6), proof_json varchar
    ) WITH (format = 'PARQUET')'''


def _q(value):
    return "'" + str(value).replace("'", "''") + "'"


def mark_sql(target, *, status, at, proof=None):
    if status not in {'current', 'pending', 'complete'}:
        raise ValueError('invalid season handoff status')
    if status == 'complete' and not proof:
        raise ValueError('season close requires committed capture proof')
    instant = "TIMESTAMP " + _q(at.astimezone(timezone.utc).replace(tzinfo=None).isoformat(' '))
    completed = instant if status == 'complete' else 'CAST(NULL AS timestamp(6))'
    proof_json = _q(json.dumps(proof, sort_keys=True)) if proof else 'CAST(NULL AS varchar)'
    return f'''MERGE INTO {TABLE} t USING (SELECT
      {_q(target.scope_id)} scope_id, {_q(target.competition_id)} competition_id,
      {_q(target.edition_id)} edition_id, {_q(status)} status,
      {_q(target.registry_snapshot_id)} registry_snapshot_id,
      {instant} detected_at, {completed} completed_at, {proof_json} proof_json) s
    ON t.scope_id = s.scope_id
    WHEN MATCHED AND t.status <> 'complete' AND s.status <> 'current' THEN UPDATE SET
      status=s.status, registry_snapshot_id=s.registry_snapshot_id,
      completed_at=s.completed_at, proof_json=s.proof_json
    WHEN NOT MATCHED THEN INSERT (scope_id, competition_id, edition_id, status,
      registry_snapshot_id, detected_at, completed_at, proof_json)
    VALUES (s.scope_id,s.competition_id,s.edition_id,s.status,s.registry_snapshot_id,
      s.detected_at,s.completed_at,s.proof_json)'''


def closed_targets(previous, registry_rows):
    """Require a proven new current edition before treating an old one as closed."""
    from .transfermarkt_current_state import CurrentTarget
    current = {str(row['competition_id']): set() for row in registry_rows if row.get('is_current') is True}
    for row in registry_rows:
        if row.get('is_current') is True:
            current[str(row['competition_id'])].add(str(row['edition_id']))
    rows = {(str(row['competition_id']), str(row['edition_id'])): row for row in registry_rows}
    result = []
    for record in previous:
        competition, edition = str(record['competition_id']), str(record['edition_id'])
        row = rows.get((competition, edition))
        if competition in current and edition not in current[competition] and row and row.get('is_current') is False:
            result.append(CurrentTarget(record['scope_id'], competition, edition,
                                        str(row['registry_snapshot_id']), float(record.get('tier', 0)), 0))
    return tuple(result)
