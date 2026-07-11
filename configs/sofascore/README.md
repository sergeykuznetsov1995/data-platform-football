# SofaScore tournament registry

`tournaments.json` is the single versioned source of truth for SofaScore
tournament IDs, slugs, source season IDs, source classification evidence,
operator review, and activation.

Schema v2 separates fields by ownership:

- discovery owns source identity, slugs, category, `classification`, and season
  source metadata (`season_id`, original name/year, dates, format, canonical
  season, and evidence);
- operators own `canonical_id`, `enabled`, `review`, custom fields, explicit
  season aliases, and named-season canonical overrides. Discovery preserves
  these fields byte-for-byte. Euro 2020's
  `2021` alias is an explicit exception, not a generic single-year heuristic.

New tournaments are always `enabled: false` with a pending review. Production
capture is fail-closed: source gender must explicitly be male, source evidence
must contain no women/mixed/youth/reserve/futsal marker, and review must confirm
adult men's first-team football with evidence. A plain name without `Women` or
`U21` is not positive evidence. Schema-v1 files remain readable for rollback,
but cannot be production-capture eligible until migrated and reviewed.

Refresh every discoverable tournament and all source season records with:

```bash
make sofascore-discovery
```

The scheduled GitHub workflow performs an `active-reviewed` direct refresh on
Monday through Saturday and a complete category scan on Sunday. It opens or
updates a review PR when metadata changes. Both scopes use the public JSON API
only. `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, lower-case variants, and the
repository proxy file are disabled at the libcurl transport layer; the report
always records zero paid-proxy bytes, browser sessions, and navigations. A 403,
missing category fan-out, missing season response, or schema error aborts before
the atomic compare-and-swap write.

For a read-only drift check (exit 2 means the registry would change):

```bash
make sofascore-discovery-check
```

Review and activation are separate atomic operations. Approval deliberately
leaves the row disabled:

```bash
python dags/scripts/manage_sofascore_registry.py 7 approve \
  --canonical-id "UEFA-Champions League" \
  --reviewed-by operator@example.com \
  --evidence https://www.uefa.com/uefachampionsleague/

python dags/scripts/manage_sofascore_registry.py 7 enable
```

`enable` re-evaluates the full source-plus-review evidence and refuses unknown,
women, mixed, youth, reserve, futsal, or seasonless records. Airflow mounts this
registry read-only; discovery and operator commands are the only writers.
