# Transfermarkt rumour assertions: offline parser contract

## Scope and provenance

`scrapers.transfermarkt.rumours.parse_rumours` parses one supplied saved HTML
capture. It does not fetch URLs, read files, consult the clock, import Airflow,
write data, or combine captures. This implementation is limited to the parser,
its contract, synthetic tests, and donor attribution for Football #2293.

Rumours are **source assertions, never confirmed transfers**. Both the capture
and every observation have entity type `transfer_rumour_assertion`. The parser
does not write to `transfer_events` and does not create match rows or expose a
match API.

The row extraction logic is adapted from `parse_rumour_rows` in
`omkarcloud/transfermarkt-scraper`, commit
`ad660278c0c95bd46c2d0553658fcd14ebd9cb8f`, file
`transfermarkt/competitions.py`, blob
`1abc32204ff6345f8dffa4707be9276f2850726c`.
Copyright (c) 2026 Chetan Jain. The complete, verbatim MIT notice is in
[`transfermarkt-scraper-omkarcloud-MIT.txt`](../../scrapers/transfermarkt/licenses/transfermarkt-scraper-omkarcloud-MIT.txt).

Reused semantics include header-driven rows, player identity, separate position,
flag nationalities, age, current/interested clubs, the interested club's
competition when present, both source/reply dates, assessment percentage, and
the first source/discussion link. The local implementation replaces donor
helpers with BeautifulSoup parsing and strict validation. It adds explicit
competition and observation time, immutable types, canonical serialization,
capture-wide failure, duplicate reconciliation, and historical/correction keys.
Unlike the donor's silent player skip, an invalid row fails the entire capture.
The interested club is required. A missing source link falls back to the first
reply link; both cells' links are validated even when only one is retained.
Every link in each cell is validated: a valid first source link followed by an
external link fails the entire capture with `invalid_url`.

## Input

```python
from scrapers.transfermarkt.rumours import CompetitionContext, parse_rumours

capture = parse_rumours(
    saved_html,
    observed_at="2026-11-20T10:00:00+01:00",
    competition=CompetitionContext(
        id="GB1",
        name="Example League",
        url="/example/startseite/wettbewerb/GB1",
    ),
)
serialized = capture.to_json()
```

`saved_html` must be a nonblank string. There must be exactly one `table.items`,
with one direct `thead`, one header row, and one direct `tbody`. Direct table
children may also be `caption` or `colgroup`; unrelated outer rows, text, cells,
and other elements are rejected. Header cells cannot span columns or rows.
Body rows consist of direct `td` cells matching the header count, without spans
except for the explicitly recognized empty marker below. Nested player tables
are permitted. Header text is case-sensitive after whitespace normalization.

| Header | Contract |
| --- | --- |
| Player | Required; a named canonical player reference must exist in every row. |
| Interested club | Required; a named canonical club reference must exist in every row. |
| Nation or Nat. | Optional; flag image `title`, falling back to `alt`. |
| Age | Optional; one to three ASCII digits or null marker. |
| Club or Current club | Optional; a named canonical club reference or null marker. |
| Most recent source | Optional; source date and first link. |
| Last reply | Optional; reply date and fallback first link. |
| Assessment or User assessment | Optional; percentage or null marker. |

Unknown headers, missing required headers, and duplicate semantic headers
(including aliases such as both `Nation` and `Nat.`) fail. Optional headers may
be absent. Empty text, `-`, and `—` represent null optional scalar values.
Other nonempty malformed values fail; they are not silently converted to null.

Player and club IDs are positive, nonzero decimal strings evidenced by
`/spieler/<id>` and `/verein/<id>` URL path segments. A cell containing multiple
different IDs of the requested kind fails. Names come from anchor text, then
anchor title, then image title/alt. Repeated references to the same entity, such
as logo and name anchors, are permitted; the first named reference is retained.
Optional current club text without a valid reference is an error. The
interested club's competition comes only from a named `/wettbewerb/<id>` link
in that cell; multiple different competition IDs fail. It is not inferred from
the capture's competition.

Every anchor URL in player, current club, and interested club cells is validated
before entity-kind filtering. Valid links of another Transfermarkt entity kind
are not used as the requested identity. An external or malformed non-identity
link fails the entire capture with `invalid_url`, even beside a valid identity
link.

Position comes from `.position`, or the second row of a two-row player
`table.inline-table`. Missing position is null. Nationalities preserve distinct
image labels in source order; absent flags produce an empty array. An unlabeled
flag or unrecognized nonempty nationality text fails.

`competition` must be a `CompetitionContext` with a nonempty name, an uppercase
alphanumeric source ID, and a supported URL containing that exact
`/wettbewerb/<id>` path segment. It is explicit capture context, not a guess
from the player or clubs. Competition names have whitespace normalized.

`observed_at` is mandatory: `YYYY-MM-DDTHH:MM:SS`, optional 1–6 fractional
digits, and `Z` or a numeric `±HH:MM` offset. Invalid calendar/time values,
naive timestamps, malformed offsets, and the unknown offset `-00:00` fail with
`invalid_observed_at`. The value normalizes to UTC
with six fractional digits, for example `2026-11-20T09:00:00.000000Z`.
This is the caller's observation clock, not a source publication date or an
ingestion timestamp invented by the parser. Equivalent offsets yield the same
canonical clock and keys. The parser does not require dates to precede the
observation time and does not consult today's date.

## URL, date, and percentage normalization

Root-relative links and absolute HTTP(S) URLs on `transfermarkt.com`,
`transfermarkt.de`, and `transfermarkt.co.uk`, with or without `www`, normalize
to `https://www.transfermarkt.com`. Entity path slugs and suffixes are preserved;
there is no entity lookup or slug inference. Query pairs are sorted and encoded
deterministically, including blank/repeated values; fragments are preserved.
Protocol-relative URLs, other hosts/schemes, credentials, explicit ports,
whitespace/control characters, backslashes, malformed percent escapes, and
decoded dot path segments are rejected. External publisher URLs are outside
this version's supported link contract and fail instead of being rewritten.

Dates accept `YYYY-MM-DD`, `D.M.YYYY`/`DD.MM.YYYY`, or English abbreviated
`Mon D, YYYY`, with a real calendar date. Date cells may contain source labels,
but exactly one recognized date token must occur in a non-null cell. Multiple
date tokens or an invalid date fail. Both dates are preserved independently as
ISO `YYYY-MM-DD`; a missing date is not populated from the other cell or clock.
Assessment is a finite decimal number followed by `%`, inclusive range 0–100;
decimal comma and whitespace before `%` are accepted. Output is a JSON number.

## Output types and columns

`RumourCapture`, `RumourObservation`, `SourceEntity`, and `CompetitionContext`
are frozen dataclasses. Collection fields are tuples internally. `to_dict()`
returns a detached JSON-native dictionary, including arrays rather than tuples.
`RumourCapture.to_json()` returns canonical JSON text, without a trailing
newline: sorted object keys, compact separators, UTF-8-compatible Unicode,
and no NaN or infinity. Encode that text as UTF-8 for deterministic bytes.

| Capture field | Type / meaning |
| --- | --- |
| entity_type | Literal `transfer_rumour_assertion` |
| parser_version | `transfermarkt-rumours/1` |
| schema_version | `transfer_rumour_assertion/1` |
| competition | Object with string `id`, `name`, `url` |
| observed_at | Canonical UTC timestamp |
| status | `observed_present` or `observed_empty` |
| observations | Array of observations sorted by `(rumour_identity, subject_id)` |
| duplicate_count | Number of reconciled identical extra rows |

| Observation field | Type / meaning |
| --- | --- |
| entity_type, parser_version, schema_version | Same constants as the capture |
| competition | Same explicit competition context |
| player | Object with string `id`, `name`, `url` |
| subject_id | `transfermarkt:player:<player.id>` |
| position | String or null |
| nationalities | Array of strings |
| age | Integer or null |
| current_club | Entity object or null |
| interested_club | Required entity object |
| interested_club_competition | Entity object or null |
| last_source_date, last_reply_date | ISO date strings or null |
| probability_percent | Number or null |
| thread_link | Canonical source/discussion URL or null |
| rumour_identity | Thread identity or deterministic content surrogate |
| observed_at | Explicit canonical observation clock |
| content_hash | Lowercase SHA-256 hex digest of assertion content |
| correction_identity | `sha256:<content_hash>` |
| observation_key | Lowercase SHA-256 hex digest of the observation key object |

## Assertion, observation, and correction keys

The assertion content is the canonical JSON object containing `entity_type`,
`competition`, `player`, `subject_id`, `position`, `nationalities`, `age`,
`current_club`, `interested_club`, `interested_club_competition`,
`last_source_date`, `last_reply_date`, `probability_percent`, and `thread_link`.
`content_hash` is SHA-256 of its UTF-8 canonical JSON. Clock, revisions, counters,
and derived keys are excluded. `correction_identity` is `sha256:` plus this hash.

With a link, `rumour_identity` is `thread:<canonical thread_link>`. Without a
link it is `surrogate:sha256:<content_hash>`, based only on retained assertion
fields. `ASSERTION_KEY_FIELDS` is `(rumour_identity, subject_id)`: one discussion
can refer to different players without merging them. No cross-thread or
cross-capture identity resolution is attempted. In particular, changed content
without a thread link changes the surrogate; the parser cannot claim that this
is a correction of the earlier unlinked assertion. Changed source links also
change thread identity. Preserved slug/query/fragment changes can therefore
change keys; the fragment is part of the canonical thread identity.

`OBSERVATION_KEY_FIELDS` is exactly
`(rumour_identity, subject_id, observed_at, correction_identity)`.
`observation_key` is SHA-256 of the canonical JSON object with these four field
names and values. An unchanged assertion at a later clock has a different
observation key but the same content hash. A changed probability, club, date,
or link at a later clock is a new observation. A separately supplied correction
at the same clock has a different content hash and observation key; for a
stable thread and player its assertion identity remains the same. No object
is mutated, no winner is selected, and no supersession/deletion is performed.
Historical persistence and correction ordering belong to a future writer.

Within one capture, identical observations with the same assertion identity
are retained once; each extra occurrence increments `duplicate_count`.
Different content for that same identity raises `conflicting_duplicate` and
fails the whole capture, even if it could be interpreted as a correction.
Corrections are supported across separately parsed captures, not competing
rows in one capture. Identical HTML plus identical explicit inputs gives
identical JSON bytes and keys. Source row order does not affect observation
ordering; changing the number of duplicates changes only the capture counter.

## Empty and error semantics

`observed_present` means the capture contains at least one retained observation.

`observed_empty` requires the supported headers and an explicit `tbody` that
is empty, or exactly one row containing one `td.no-entries` with colspan equal
to the header count, exact normalized text `No entries available` (optional
final period), and no link, image, or nested table. An empty marker mixed with
data rows, a wrong colspan, or unrecognized empty text fails. Blank HTML,
missing tables, altered layout, and partial rows never become empty results.

An empty feed is **not retraction and not missing-state evidence**. It describes
only the supplied capture; pagination and completeness have not been proved.
No player/club disappearance, transfer confirmation, expiration, or lifecycle
transition is inferred from either empty or nonempty captures.

All supported validation failures raise `RumourParseError`, with stable
`reason` and optional one-based `row` (body row index). A bad row fails the
entire function; no partial capture is returned.

| Reasons | Meaning |
| --- | --- |
| blank_html, missing_table, ambiguous_table | Missing/ambiguous input |
| invalid_table_structure, invalid_headers | Unsupported table/header structure |
| unknown_header, duplicate_header, missing_header | Header contract violation |
| invalid_row_structure, malformed_empty_row | Broken data/empty row |
| invalid_player, invalid_club, invalid_competition | Missing, inconsistent, or malformed identity/context |
| invalid_url | Unsupported or malformed source/entity link |
| invalid_date, invalid_probability, invalid_age, invalid_nationality | Malformed donor value |
| invalid_observed_at | Invalid explicit observation clock |
| conflicting_duplicate | Same assertion identity with different retained content |

## Verification and remaining boundaries

`tests/unit/scrapers/test_transfermarkt_rumours.py` uses synthetic BeautifulSoup
fixtures with pytest. It covers complete donor fields, aliases/nulls, minimal
rows, canonical JSON and replay, duplicate reconciliation/conflicts, later
historical observations, same-clock corrections, surrogate identities, valid
empty captures, malformed layouts/rows/values, competition/clock validation
(including rejection of unknown offset `-00:00`), `observed_present` status,
fragment preservation, rejection of an external source link after a valid first
link, rejection of external non-identity links beside valid identities in player,
current club, and interested club cells, and explicit zero match/transfer-event
output.

The implementation session does not run tests. The lead runs deterministic
checks after the runner publication gate, verifies the exact diff, and obtains
the independent review required for the data contract. Test execution and
review outcomes must be recorded separately; this document does not assert
that those gates have passed.

No live capture or production compatibility has been demonstrated by synthetic
fixtures. Live source acquisition, pagination, source access policy, completeness,
runtime receipt/readback, DQ thresholds, correction persistence, lifecycle and
retraction rules, writer/storage merge, table DDL, grants, DAGs, schedules,
delivery, and production acceptance remain outside this package. No Compose,
Silver, or Gold changes are included. This contract does not claim completion
of the full three-day issue acceptance, and does not close the issue.
