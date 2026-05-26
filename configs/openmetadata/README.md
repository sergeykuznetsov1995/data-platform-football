# OpenMetadata — descriptions as code

This directory holds the metadata-as-code layer for the OpenMetadata catalog at
http://localhost:8585. YAML files in `descriptions/` are the source of truth
for table/column descriptions, tags, and relationships (FK pointers used to
build ER diagrams).

## Workflow

```
0) bootstrap       (this dir, one-time per OM instance) — creates Tier/Domain/PII/UseCase classifications + tags
1) ingest          (schema discovery from Trino)        — creates table entities in OM
2) apply           (this dir)                           — patches descriptions/tags/relationships
3) lineage         (Trino query history → edges)        — wires fct→dim → mart→fct
```

Cadence:
- `om-bootstrap`: one-time per OM instance (and after `docker compose down openmetadata-server -v`); idempotent, safe to re-run.
- `om-ingest-trino`: ad-hoc after a schema change in Gold.
- `om-apply-descriptions`: after editing any YAML here (idempotent, safe to re-run).
- `om-lineage-trino`: nightly (Trino query history rolls up the latest day).

> Without `om-bootstrap`, `om-apply-descriptions` returns `PATCH HTTP 404: tag instance for Tier.Gold not found` on every YAML — the tag FQNs referenced in `tags:` must exist as real OM tags first.

## JWT setup

`apply_descriptions.py` authenticates via a bot JWT issued by OpenMetadata:

1. UI → `Settings → Bots → ingestion-bot` → copy the JWT token.
   (Or create a new bot under `Settings → Bots → Add Bot`.)
2. Export to your shell / `.env`:

   ```bash
   export OPENMETADATA_HOST=http://openmetadata-server:8585
   export OPENMETADATA_JWT_TOKEN='<paste token>'
   ```

3. One-time bootstrap of classifications/tags:

   ```bash
   make om-bootstrap   # creates Tier{Bronze,Silver,Gold} / Domain.Football / PII.{None,Low} / UseCase.ML
   ```

4. Apply YAML descriptions:

   ```bash
   make om-apply-descriptions  # runs apply_descriptions.py inside openmetadata-ingestion
   ```

`HTTP 404` on a table = not yet ingested → re-run `om-ingest-trino`.
`HTTP 404: tag instance for X not found` on every YAML = classifications not bootstrapped → run `make om-bootstrap`.
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
