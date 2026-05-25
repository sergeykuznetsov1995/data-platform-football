# OpenMetadata — descriptions as code

This directory holds the metadata-as-code layer for the OpenMetadata catalog at
http://localhost:8585. YAML files in `descriptions/` are the source of truth
for table/column descriptions, tags, and relationships (FK pointers used to
build ER diagrams).

## Workflow

```
1) ingest          (schema discovery from Trino) — creates table entities in OM
2) apply           (this dir)                    — patches descriptions/tags/relationships
3) lineage         (Trino query history → edges) — wires fct→dim → mart→fct
```

Cadence:
- `om-ingest-trino`: ad-hoc after a schema change in Gold.
- `om-apply-descriptions`: after editing any YAML here (idempotent, safe to re-run).
- `om-lineage-trino`: nightly (Trino query history rolls up the latest day).

## JWT setup

`apply_descriptions.py` authenticates via a bot JWT issued by OpenMetadata:

1. UI → `Settings → Bots → ingestion-bot` → copy the JWT token.
   (Or create a new bot under `Settings → Bots → Add Bot`.)
2. Export to your shell / `.env`:

   ```bash
   export OPENMETADATA_HOST=http://openmetadata-server:8585
   export OPENMETADATA_JWT_TOKEN='<paste token>'
   ```

3. Verify with `--dry-run` (no HTTP, just renders patches):

   ```bash
   make om-apply-descriptions  # runs apply_descriptions.py inside airflow-webserver
   ```

`HTTP 404` on a table = not yet ingested → re-run `om-ingest-trino`.
`HTTP 401` = bad / expired JWT → re-issue.

## YAML format

See [`descriptions/dim_referee.yaml`](descriptions/dim_referee.yaml) for the
canonical example. Schema:

```yaml
table:
  fullyQualifiedName: trino_iceberg.iceberg.gold.<table>
  description: |
    2-3 sentence summary (what the table is, source, grain).
  tags:
    - Tier.Gold
    - Domain.Football
    - PII.None
columns:
  - name: <pk_or_metric_col>
    description: One-line.
relationships:
  - from: trino_iceberg.iceberg.gold.<self>
    to:   trino_iceberg.iceberg.gold.<other>
    type: FOREIGN_KEY
    description: Optional human-readable join hint.
```

Only PK + key FK + 3-5 main metrics need explicit column descriptions; OM
keeps native column types from the ingested schema regardless.

## Adding a new table

1. Drop a new `<table>.yaml` in `descriptions/`.
2. `make om-apply-descriptions` (idempotent — re-runs safely).
3. Refresh the table page in the OM UI.

Lineage edges (`relationships`) are best-effort: the `apply` script logs WARN
on 4xx and continues, since edge creation depends on entity IDs that may not
yet exist when the catalog is freshly bootstrapped — `om-lineage-trino` will
backfill from query history on the next nightly run.
