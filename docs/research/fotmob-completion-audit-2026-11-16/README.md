# FotMob #2256 — completion audit method (no current results yet)

This is a procedure template. The collector has not been executed by this
implementation session. Current counts, query IDs, snapshot pins, runtimes and
replay results must come from saved read-only collection evidence. No planning
estimate or old #1294 count is a current measurement.

Implementation: [stdlib CLI](../../../scripts/research/audit_fotmob_completion.py).
Procedure, SQL contracts, artifacts and commands:
[audit README](../../../scripts/research/fotmob_completion_audit/README.md).

The audit freezes four Iceberg snapshots, exact registry season labels, the
original historical CSV bytes, code/SQL hashes, deterministic query chunks and
one `captured_at`. Future applicability compares `utc_time + 3 hours` with that
frozen timestamp on every pass; wall-clock invocation time cannot change it.
It reconstructs calendar and global payload winners from base tables with
`FOR VERSION AS OF`; it does not query current views, modify Bronze, access live
FotMob, replay S3, create database objects or change schedules.

The primary report cohort is the #2258 required scopes. Its known, finished,
current-payload and literal-gap counts and scoped/global ID hashes come first;
the union with marker-only and historical scopes is supplementary. The static
registry resolves to 5,540 required scopes across 452 included competitions.
These are input-contract counts, not current content measurements. Manifest
predicates use VARCHAR competition IDs, physical-table predicates use numeric
IDs, and season labels retain their exact spaces and quotes.

Collection uses two payload templates: `current_payloads` supplies the six
raw/typed/mask comparisons for chunks containing marker or historical scopes;
`current_payload_ids` supplies physical identity evidence for other chunks
without reading the full JSON. The chunk plan freezes `payload_modes`, and
replay validates each payload query against its selected template and ID chunk.

Required outputs are current scoped/global ID counts and hashes, literal and
actionable gaps, complete mismatch sets, all latest v2 marker classifications,
six-section raw/typed/mask comparisons, competition/reason breakdowns, and a
separate limited C1 reconciliation. `current-id-evidence.csv` and
`section-evidence.csv` stay in the local run directory, with their paths, row
counts and SHA256 hashes linked from the generated run manifest.
`section-evidence.csv` contains all six comparisons, including agreements, for
each current payload in marker or historical scopes. `section-mismatches.csv`
is its exact non-confirmed subset; both files participate in content replay.
Ordinary unfinished matches are `not_applicable` before writer classification
and do not poison completion totals, marker outcomes or the match mismatch set.
Full section evidence still retains their comparisons. Awarded unfinished
matches remain unknown; finished matches with a current last-good payload retain
that content even when a later failure is reported separately.

C1 is a limited cache of `kind=input` observations keyed by competition, exact
source season and match ID, with `deadline_day`/`deadline_days` cohorts retained;
control/day rows are excluded. `first_ft` defines the C1 known-finished cohort,
while `first_ingested` means ever ingested, not currently present. Reconciliation
preserves scoped IDs, observations, deadline status, unconfirmed/no-evidence
sets and deterministic hashes without collapsing shared global match IDs.
Source-unavailable thresholds require no success and either three observations
or two observations spread over at least 7,200 seconds. These cache thresholds
do not replace current physical evidence or turn an unresolved gap into content.

The historical baseline of **2026-10-21 reported 5,305 finished matches without
details in total; 1,762 belong to the exactly 18 pairs rechecked here**. These are
old figures only. The preserved `historical-1294-input.csv` supplies exact keys,
competition names, historical parser/plan signatures and old gaps. The generated
`historical-1294-results.csv` retains these alongside separate current counts,
primary outcomes and completion/section reasons.

Before replacing this template, the lead checks schema/decoder compatibility,
runs the synthetic tests and read-only calibration, then completes two full
passes on identical snapshots and frozen chunks. Each statement must finish
within 25 seconds and each pass within 60 minutes. `verify` must demonstrate
byte-identical content artifacts and complete query coverage. Unknown evidence
is reported explicitly and never counted as confirmed content. Every required
scope, latest marker and historical pair must reconcile; missing section
evidence fails validation even if the remaining comparisons all agree.

Independent prerequisite checks have passed for the real registry loader
(5,540 scopes / 452 competitions), the historical loader (18 pairs / 1,762 old
gaps), and Trino `EXPLAIN VALIDATE` of all 11 SQL templates. These checks do not
establish a completed audit, replay or runtime acceptance. The lead still needs
to run the corrected tests and save the real collection and verification evidence.

Current execution status: **not measured**. Replay status: **not run**.
