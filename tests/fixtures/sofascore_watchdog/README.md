# Frozen #1361 evidence

`history_scope_daily.tsv` is an exact copy of
`/root/sofascore-review-20260922/evidence/C3/scope_occupancy_by_day.tsv`,
SHA-256 `308d1b51d712ae92e464c6e1badbfb844759482a2fe3bb32b5066be3844123ba`.
Columns: September day, successful history scope TIs, failed scope TIs,
successful slot-hours, failed slot-hours, occupancy percent. Counts came from
the dedicated Airflow metabase (C3 review, 22 September 2026). The first and
last dates are observation-window boundaries; this is a retrospective replay
of saved counts, not a reconstruction of the metabase as it looked each day.
The synthetic replay clock is the following day at 05:00 UTC.

`coverage_daily.jsonl` contains the unchanged saved #1355 measurements for
5–7 October from `/root/watchdog/state/sofascore_veha1/daily.jsonl`.
Fixture SHA-256 `82e28c18aded22d7345070ea34606eba605fa0eda34ffc94eb6a3dd23990b7cc`.
The original denominator, output line, timestamps and evidence paths remain
part of each record. The fixture does not query or recalculate Bronze.

Missing September coverage, pool waits, closed-match counts and actual request
counts are deliberately unknown. In particular, G1's `paid-events-by-hour.tsv`
counts **byte chunks**, not requests, and is not used as a request-count fixture.
Controlled healthy/pause cases in unit tests are synthetic; they do not satisfy
the issue's three-day production acceptance criterion.
