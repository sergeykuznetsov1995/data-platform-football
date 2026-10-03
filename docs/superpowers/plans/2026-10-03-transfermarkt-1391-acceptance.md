# Transfermarkt #1391: first full acceptance follow-up

Evidence: full run `scheduled__2026-10-01T18:00:00+00:00`, completed
2026-10-02 22:02:54 UTC; promoted snapshot `tm-discovery-1d3f5ef19ca90146bbf0ab53`.
842 competitions, 15,982 editions; carried 17/842 (2.02%); unknown active 0.
Discovery manifest `6cb3a48d117d5ad661b00e5bfd14cd5ba5b345edbac2e2138eb87a3343521ca2`.
Publication manifest `bb4340e44a814d2a93e1fa0d8241d07a6362c49be29fb8534adc1ab27256a667`.
Local immutable source evidence: `/root/tm-acceptance-20261003/full-checkpoint.json`
and `full-manifests/`. No new source requests were made for this diagnosis.

## Discovered defects

Configured country context was lost on `/saison_id/2026/plus/1` country pages.
Resolve the country by the complete country-ID path segment, retaining the
existing configured context and classification guards.

Profile normalization removed both season delimiters, producing
`AC2Qgruppe/QR` from `AC2Q/saison_id/2026/gruppe/QR`. The first five new IDs
AC2Qgruppe, ACEQgruppe, ACHQgruppe, ACL2gruppe, ACLEgruppe returned 404;
1,097 subsequent new candidates returned HTTP0, including required cups.
This sequence is consistent with the unchanged five-failure endpoint circuit.
Preserve the path boundary; do not relax the circuit or budget.

## A6 denominator correction

241 live pokal rows checked, 19 mismatches. Each row below has exactly one
`isCurrentSeason=true` in its saved `/competition/{id}/regulation` response.
Only `current_saison_id` changes. The contract is the source's current flag,
not the newest scheduled edition. In particular 20AC, AFAC, AFCN and EURO
select the last completed edition; their newer scheduled editions are false.

| ID | Before | After | Source display | Explanation |
|---|---:|---:|---|---|
| 20AC | 2026 | 2024 | 2025 | Last completed edition explicitly current; upcoming edition false |
| A169 | 2025 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| ACL | 2025 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| AFAC | 2026 | 2022 | 2023 | Last completed edition explicitly current; upcoming edition false |
| AFCN | 2026 | 2024 | 2025 | Last completed edition explicitly current; upcoming edition false |
| CAFC | 2025 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| CNLA | 2024 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| CNLB | 2024 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| CNLC | 2024 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| CNNF | 2024 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| EURO | 2027 | 2023 | 2024 | Last completed edition explicitly current; upcoming edition false |
| G19C | 2025 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| GEP1 | 2024 | 2025 | 2026 | Source current flag advanced beyond denominator |
| GRP3 | 2025 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| INDC | 2025 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| SAKC | 2025 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| SCGU | 2025 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| SSC | 2025 | 2026 | 26/27 | Source current flag advanced beyond denominator |
| UCOL | 2025 | 2026 | 26/27 | Source current flag advanced beyond denominator |

## Validation and remaining acceptance

Regression tests reproduce lost FA Cup context and corrupted group-route IDs
on the original code. Check the complete discovery/runner suite, denominator
loading/A6 reconciliation and the required unit suite before PR delivery.
Independent read-only review is required. Parser revision advances v4 → v5 because the same saved pages now produce
corrected country provenance and competition identities. Snapshot identity
includes this revision; downstream schemas and batch format are unchanged.

Production acceptance remains open: the next daily and ingest must finish,
required cups and >=95% ten-season coverage must be proven after automatic
delivery and a scheduled full. No manual rerun, deployment, wave 2 or backfill
is authorized. A merged fix does not itself satisfy these gates.

Initial offline replay of 454 configured-country pages found 64 remaining
parse failures, including five empty club-round anchors addressed below; no
country-context failures remained.
All 1,631 saved group links retained their original competition identity.
After the denominator correction A6 replays as checked=241, mismatches=[].
Focused discovery/runner: 84 passed; denominator/publication: 25 passed.
Independent native review found no unresolved material defects after fixes.

Read-only Silver query `20261003_090733_36063_pkbgt` confirms 546/547 core
competitions present, 383 with at least ten editions at any dates, and 290
covering every saison_id 2015..2024. These are diagnostics, not acceptance:
nonannual/new competitions need source-aware evaluation of the agreed window.
RKPO is quarantined. Saved results: `silver-edition-coverage.json` and
`coverage-summary.json` in the local evidence directory above.

Actual scheduler mounts on 2026-10-03 point to release `1efea301`, including
`configs -> /opt/airflow/configs`. Automatic delivery completed at 01:02:28 UTC.
The old missing-denominator-mount concern is no longer current; no production
configuration changes are part of this follow-up.

Required local full suite: 13,367 passed, 24 skipped, 10 failures in
`test_bench_whoscored_capacity.py` (host/runtime identity contract). The same
10 failures reproduce on unchanged base `1efea301` (10 failed, 153 deselected).
Logs: `unit-suite.log`, `baseline-whoscored.log`. Final affected suites passed
84 discovery/runner + 25 denominator/publication tests. No CI bypass authorized.

The 164 core competitions below ten editions comprise 72 complete accepted
API histories, 88 HTML fallbacks (69 ambiguous current flags, 19 empty API),
three carried competitions and RKPO quarantined for classification conflict.
All saved API edition IDs for those72 were preserved; source history cannot
be fabricated. Failed/invalid API bodies are not in the checkpoint.
The owner subsequently approved all real editions with TM saison_id 2015..2024,
keeping all live core tournaments in the denominator and requiring evidence
for short/irregular histories. Unproved cases do not count; mandatory cups remain
a separate gate. Retain the RKPO classification guard and current-season validation.


## Follow-up: country participation links and shared CI

The saved France listing (country 50) aborts at an empty FIC1 round link in a
club row; Mexico, Morocco, South Africa and Tunisia have the same structure.
Skip this duplicate only in the country participation panel, in the explicitly
verified Round achieved column, with a club link and the nearest named header
in the same table matching competition ID, route and slug. Column shifts,
colspan/rowspan changes and unrecognized nameless links remain fail-closed.
No competition name is invented. The real direct-table header plus tbody layout
is covered by the fixture; targeted review caught and corrected column matching.
All five unmodified saved pages now parse; final affected suites: 151 passed.
France continuation pages were not collected in the saved run, and its root has
no FRP link. This fix enables navigation but does not prove Coupe de France
coverage until a scheduled full run after delivery.

The owner also authorized the shared CI repairs in a separate commit:
official Buildx v0.37.1 linux-amd64 SHA256
`9447199cdb435f25880548343c128a4b6650e8891ee598905d8d29d39a8e359b`
and peeled tag commit `0b265a9f62db554fa9aba6dd19e1bd5704bc7d8a` were checked
against Docker release checksums, the downloaded binary, and the runner image.
All immutable binary guards retain unknown-value rejection. The ESPN fixture
race was reproduced on base and PR head: a periodic debt query crossed promotion
and correctly invalidated the fixture before final freshness. Explicit observer
requests synchronize only this test; its safety assertions and runtime remain
unchanged. Focused checks: 17 CI/TLS and 48 ESPN tests passed. Separate reviews
found no unresolved material defects in either repair package.

Final combined local full suite: 13,392 passed, 24 skipped, 11 failed in 1040.12s.
Ten failures match the reproduced unchanged-base WhoScored host-contract failures.
The additional FBref raw-audit test failure was also reproduced on clean base:
when rewritten file metadata including ctime collide, audit reuses the old hash
and misses changed bytes. This is an existing runtime audit defect, not a harmless
test flake; FBref runtime changes are outside this approval and were not made.
No tests or required CI checks were disabled. GitHub checks on the updated head
must be evaluated separately; this local result is not an all-green claim.
Evidence: `tm-integrated-final-full-suite.log`, `fbref-audit-diagnosis.txt`,
`tm-empty-link-review-final.txt`, `ci-repair-review.txt` under the local evidence
root. An earlier intermediate full run was interrupted after review changed the
code; it is not used as final verification.

For the approved coverage rule, CAF's complete edition history and the saved
TM regulation prove CHAN expected IDs `{2015,2017,2020,2022,2024}`. All five occur
in the saved Silver result. The 2024 source ID denotes the edition played in
2025 and still belongs to the approved source-ID window. Evidence and official
CAF links: `coverage-chan-evidence.txt`, `coverage-chan-verified.json`.
This proves one sparse-history exception, not the overall 95% threshold or
post-delivery acceptance. No merge, manual source run or deployment is authorized.
