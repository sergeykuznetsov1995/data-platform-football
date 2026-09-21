# Africa Cup of Nations qualification 2027 keepers — real tableless capture

`acnq-2027-keepers-e9cfdc24.html.gz` is committed FBref raw read on 2026-09-21
from the existing local S3 archive (`blobs/sha256/e9/e9cfdc24….html.gz`),
without a request to fbref.com.

- Decompressed SHA-256: `e9cfdc24966cf53ac0cada496b383bf150d9db3a49b18f315bcdf4aff8767e10`
  — the exact `page_frontier.last_content_hash` of the target below.
- Affected target: `fbref:season_stats:657:2027:keepers`,
  `https://fbref.com/en/comps/657/keepers/Africa-Cup-of-Nations-qualification-Stats`.
- Shape: HTTP 200, zero `<table>` elements in the DOM and in comments, a
  `link rel=canonical` and an `og:url` naming this very address, and a
  non-empty `<h1>` inside `div#meta`.
- Regression: with `season_stats` missing from the generic zero-table
  allowlist, this page produced `page_contract:no_tables` and killed
  `recover_raw_before_fetch` on every `dag_ingest_fbref` run from 2026-09-16
  onward, before a single paid request (#1317).
