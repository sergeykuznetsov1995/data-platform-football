# /Ranking regression, 2026-10-02

`Ranking.html.gz` is the unmodified gzip body already stored by production in
`iceberg.bronze.clubelo_raw_page`, page `/Ranking`, batch
`clubelo-daily-20261002T044804-1a522c65` (fetched 2026-10-02 around 04:48 UTC).
Retrieved with a read-only query, without another request to ClubElo.

Decoded body SHA256:
`6b630d953414cede731367c8e1df9767dd9ef309939faa55d7d4f030f268ced1`.
Gzip size: 93,158 bytes; decoded size: 1,234,381 bytes. Page rating date: 2026-10-01.

The flag `<img>` in `eloData` now includes `loading="lazy" decoding="async"`
before `src`. The previous exact cell regex rejected row 0 before any parsed
snapshot write. Removing only these attributes in a diagnostic copy lets the
previous parser and completeness checks pass: 1744 rated clubs, 52 provisional,
1723 matched levels and 498 linked club pages. The committed fixture retains
the original attributes and bytes.
