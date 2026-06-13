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

## Removing dropped tables (stale lineage cleanup)

`om-ingest-trino` runs with `markDeletedTables: true`, so when a table disappears
from Trino its OpenMetadata entity is **soft-deleted**. In practice that leaves its
lineage edges in place — in particular the edges added **manually** by
`apply_descriptions.py` (`PUT /api/v1/lineage` from the `relationships:` blocks),
which ingestion's mark-deleted handling does not touch. So edges keep pointing
to/from the dropped table. This is issue #529 — surfaced when the derived gold tier
was dropped in epic #478, where edges like `feat_team_form → dim_team` and
`fct_match → dim_match` were still visible in the catalog.

Lineage edges are removed when the entity is **hard-deleted** with `recursive=true`
(a hard delete removes the entity together with all its relationship rows).
`cleanup_lineage.py` does exactly that for the dropped tables:

```bash
make om-cleanup-lineage          # DRY-RUN: prints the tables it would hard-delete
# review the list, then actually delete (entity + its lineage edges):
docker compose exec openmetadata-ingestion python /opt/configs/cleanup_lineage.py --apply
```

The script targets a curated list of the 19 derived-gold tables dropped in epic
#478 (idempotent — a table already gone is reported `ABSENT` and skipped). For a
general sweep of any future drop, first re-run `om-ingest-trino` (to soft-delete
tables gone from Trino), then add `--all-soft-deleted`:

```bash
docker compose exec openmetadata-ingestion python /opt/configs/cleanup_lineage.py --all-soft-deleted --apply
```

This step is **deliberately manual, not wired into the nightly `om-lineage-trino`** —
auto-hard-deleting any transiently-missing table would permanently destroy its
descriptions, tags, and lineage. Always review the dry-run list first.

> `entity_xref` is **not** cleaned here: it is still a live table (its drop is the
> separate followup #146), so it is never soft-deleted and its edges are not stale.
