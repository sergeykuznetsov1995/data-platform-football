# ClubElo `/Results`, captured 2026-09-30

`Results.html.gz` — the `/Results` page fetched by the first run of the new daily code
(`dag_ingest_clubelo`, run `scheduled__2026-09-30T00:30:00+00:00`, 2026-09-30 05:07:52 UTC),
exported byte-for-byte from `iceberg.bronze.clubelo_raw_page` (batch
`clubelo-daily-20260930T050739-acb3a36c`): wire gzip 46 592 B, identity 567 972 B,
identity sha256 `0d671e99c5a2a366d0a6df82d1fb6e7990ee7b6a900ca3f15e1b03d86cc753d6`.

h1 date 2026-09-26, `Page created on 2026-09-27 05:09:02`; 63 result rows of one day and
**no date separators** — the run failed on the old check "C3 results table has no date
separators" (#1465).
