# Transfermarkt rumour assertions

## Scope and provenance

`scrapers.transfermarkt.rumours.parse_rumours` parses one supplied saved HTML
capture. It does not fetch URLs, read files, consult the clock, import Airflow,
write data, or combine captures. Football #2293 also provides a separate offline
extraction API and an isolated writer for a complete competition listing.

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
The interested club is required. The first source link takes precedence,
followed by the first reply link and then the first assessment link. Every
anchor with an href in those three cells undergoes URL validation. The real
source column identifies a Transfermarkt discussion and dates it. The
assessment cell may repeat that discussion as an icon beside a dash. The
observation retains the source date and canonical discussion URL, with a null
percentage for the dash. It makes no claim about an external publisher.

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

`saved_html` must be a nonblank string. When the page contains `#yw1`, that
grid must be unique and contain exactly one `table.items` with rumour headers.
The standings in `#yw2` are independent. A broken or empty target grid fails
even when a valid table exists elsewhere. For legacy HTML without `#yw1`,
the parser uses the unique items table with both required rumour headers.
Each selected table requires one direct `thead`, one header row, and one direct `tbody`. Direct table
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
| Most recent source or Most recent source from | Optional source date and first discussion link. |
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
In source, reply, and assessment cells only, the internal shorthand
`//thread/forum/<id>/thread_id/<id>` can include a `/post_id/<id>` suffix
and numeric fragment. For example, `//thread/forum/154/thread_id/1208229/post_id/3426507#3426507`
resolves to `https://www.transfermarkt.com/thread/forum/154/thread_id/1208229/post_id/3426507#3426507`.
Other protocol-relative URLs, other hosts/schemes, credentials, explicit ports,
whitespace/control characters, backslashes, malformed percent escapes, and
decoded dot path segments are rejected. External publisher URLs are outside
this version's supported link contract and fail instead of being rewritten.

Dates accept `YYYY-MM-DD`, `D.M.YYYY`/`DD.MM.YYYY`, fixed-width `DD/MM/YYYY`,
or English abbreviated `Mon D, YYYY`, with a real calendar date. The slash
form is always day first. For example, `20/11/2026` yields `2026-11-20`.
Date cells may contain source labels,
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
| parser_version | `transfermarkt-rumours/2` |
| schema_version | `transfer_rumour_assertion/2` |
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
The Bronze writer preserves each of these keys. Consumers see the original
observation clocks and correction identities, including ties at the same clock.

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

An empty page describes only the supplied capture. Feed completeness requires
separate evidence for every numbered page.
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
| invalid_pager, invalid_page_ref | Missing or ambiguous controls, or an invalid listing URL or page number |
| invalid_page_navigation | Current page conflicts with the requested URL, a direction is wrong, or a plan revisits a page |
| conflicting_page_ref, incomplete_page_plan | References disagree or the numbered plan has gaps |

## Saved GB1 evidence

The two Premier League pages each returned HTTP 200 with 25 rumour rows in
one attempt and zero retries. The raw-response digests identify acquisition
evidence. The saved-file digests identify the bytes consumed by parser tests
under `tests/fixtures/transfermarkt/rumours/`.

| Saved page | Fetched at (UTC) | Saved SHA-256 | Raw SHA-256 |
| --- | --- | --- | --- |
| `gb1-page-1.html` | `2026-11-20T21:21:09.485303+00:00` | `53ae52cdcb469843f3de7d23a9722e3a4b5578e71a59d7cb0f5b6ee107ba3edc` | `2f0e474c90688fee4da43d0cacfbeead812c8e677b90e2ed7dbc253318f5161d` |
| `gb1-page-2.html` | `2026-11-20T21:21:24.425970+00:00` | `33e9f518feea9bccd4463cdfa3d7437a2ad398695381caad4e4b56e700a88687` | `2ed996ad6cbeb089cc7516865df181f557a44c680906af2fe8c7d1c663e255d0` |

## Offline pagination

`parse_rumour_pagination(html, competition=context, page_url=requested_url)`
reads the selected grid's single pager using `.pager a.tm-pagination__link[href]`.
Its result contains `current_page`, `last_page`, and numbered canonical URLs.
The active item identifies the current page. The last-page control identifies
the upper bound. Number labels must agree with URL page numbers. The first,
previous and next controls must point to their respective destinations. Passing
the optional requested URL also checks the returned page identity and listing.

Targets must share one Transfermarkt rumour listing for the supplied
competition. Page numbers are positive decimal integers in the path or the
`page` query parameter. The bare listing denotes page one. Repeated links
to one number must agree on the canonical URL. Foreign listings, conflicting
references, and malformed controls fail capture planning.

`build_rumour_page_plan(pages)` combines the evidenced references, checks the
last-page boundary and rejects revisited current pages, conflicts and gaps.
It returns one URL per page in numeric order. The saved pages identify pages
1 through 5. Neither helper fetches a page. A complete URL plan provides no
row-count or acquisition evidence for pages 3 through 5.

## Verification and remaining boundaries

### Offline extraction

`scrapers.transfermarkt.rumour_observations.extract_rumour_page` takes the
supplied body, competition, page number, exact canonical listing URL, raw
capture ID, body SHA-256, capture and extraction clocks, scope, run, cycle and
universe version. Supply original UTF-8 bytes, or their exact UTF-8 text.
The digest must match those bytes. A saved fixture with different bytes has
its own digest. The extractor checks hexadecimal capture identity, converts
aware clocks to UTC microseconds, and passes the capture clock to parser v2
as the observation time. Parsing, pager validation or lineage failure rejects
the page. Extraction requires no clock lookup, storage connection or fetch.

`RumourFeedExtraction(pages)` orders the captures by page number. The
numbered URL plan from `build_rumour_page_plan` must agree with the acquired
pages from one through the evidenced last page, with a single competition,
scope, run, cycle and universe. Each page keeps its capture status, reconciled
duplicate count, pager links and raw lineage. The feed plan SHA-256 covers
that evidence, parser and schema revisions, assertion payloads, keys and
projected rows, including empty pages.

### Bronze observations and page receipts

`dags.utils.transfermarkt_rumours.write_rumours` accepts a complete typed
feed and current-write preflight. It checks both table grants in the standing
policy and the existing current authorization before opening a connection and
again under the writer deadline and lock. The authorized writer creates
these two Iceberg tables if needed.

| Table | Grain and proof |
| --- | --- |
| `iceberg.bronze.transfermarkt_rumour_observations` | One retained assertion per `(rumour_identity, subject_id, observed_at, correction_identity)`. |
| `iceberg.ops.transfermarkt_rumour_receipts_v1` | One receipt per feed plan SHA-256 and numbered page, including an explicitly empty page. |

Bronze columns retain player, interested club, optional interested competition
and current club references, position, nationalities as a JSON array, age,
both source dates, probability and the Transfermarkt discussion link.
`assertion_json` preserves the complete parser observation. Dates use
`date`, counts and age use `bigint`, probability uses `double`, and
timestamps use UTC `timestamp(6)`. Missing optional scalars are SQL NULL.
Each row carries competition, page URL and number, raw ID and digest, capture
and extraction clocks, parser and schema revisions, scope, run, cycle and
universe. The source is `transfermarkt` and the entity type is
`transfer_rumour_assertion`. The page batch ID is derived from its lineage.
The ingestion metadata clock is the explicit extraction clock. The receipt
separately records the commit clock and batch.

Both tables use insert-only MERGE. Before insertion, the writer checks the
page batch and the incoming natural keys across batches. A stale extraction,
duplicate physical key, unexpected row or differing payload or lineage fails
the write. After insertion it compares the complete physical key and payload
set to the page projection. Only then can the page receive a receipt binding
its raw lineage, pager, feed hash, expected assertions and distinct subjects,
source and duplicate counts, capture status, physical row hash and commit
metadata. The writer rereads every receipt and the full receipt set for the
plan. A failure between pages can leave proven pages committed. An exact
retry checks their rows and original receipts and can finish missing writes.

An `observed_empty` page has zero business rows. Its receipt requires a
physical lookup of the page batch and the SHA-256 of the canonical empty JSON
array. Replay repeats this lookup. A populated replay verifies all original
columns and uses the original commit clock. Later observations and same-clock
corrections with a different correction identity append separate rows.

### Reconciliation and transfer exclusion

`build_rumour_dq` compares expected and terminal pages, retained assertions
and source rows (including reconciled parser duplicates). It reports distinct
subjects, page status counts, conflicting payloads or lineage for the same
natural key, unique and duplicate natural keys, and missing required lineage.
Fresh conclusive counts validated terminal captures in this feed. Unresolved
counts outstanding pages against the evidenced last page. With no captures
the upper bound is unknown. Missing captures, inconsistent plans and repeated
natural keys prevent structural success. When supplied, receipts must cover
every expected page exactly once and match the plan, page counts, physical
hash, commit seal and readback evidence.

`rumour_exclusion_sql()` builds a read-only count against
`iceberg.bronze.transfermarkt_transfer_events`. A transfer row violates
exclusion if its `source_url` contains `/geruechte/`, its `parser_revision`
starts with `transfermarkt-rumours/`, or its `source_body_hash` matches a
rumour receipt. The current slot runs the query after the writer verifies the
write and requires a zero count before marking the check complete. Production
acceptance also verifies the query result.
The only business output here is rumour assertions. Transfer events, matches,
Silver and Gold have separate writers and lifecycles.

### Verification status

`tests/unit/scrapers/test_transfermarkt_rumours.py` covers the saved GB1 layout,
page plan and discussion references alongside synthetic fixtures for malformed
inputs and the historical contract. The cases cover complete donor fields, aliases/nulls, minimal
rows, canonical JSON and replay, duplicate reconciliation/conflicts, later
historical observations, same-clock corrections, surrogate identities, valid
empty captures, malformed layouts/rows/values, competition/clock validation
(including rejection of unknown offset `-00:00`), `observed_present` status,
fragment preservation, rejection of an external source link after a valid first
link, rejection of external non-identity links beside valid identities in player,
current club, and interested club cells, and explicit zero match/transfer-event
output.

The saved pages provide layout evidence for this parser revision. Passage of
the final deterministic tests and independent data-contract review is unconfirmed.
After the runner publication gate, the lead verifies the exact diff and records
the test and review outcomes separately.
Source access, standing grants and delivery follow the existing approval
process. Football #2293 remains open.

## Current slot and acceptance for #2293

The rumour collector is integrated into the current runner's daily signals slot. For club
competitions (including cups), the listing starts at
`https://www.transfermarkt.com/-/geruechte/wettbewerb/<competition_id>`. Each fetched page
needs its own raw capture ID, body hash and capture time. Pagination follows the next
canonical link and must reach a consistent last page before the rumours writer receives
the feed. Corrections and repeat observations belong to the rumours writer's history.

A complete check needs a verified write, receipt readback, reconciliation, and an exclusion
query returning one numeric zero for rumour violations in transfer events. These conditions
also apply to an empty listing. National-team scopes use the complete current listing's
raw IDs and check time for a zero-row check. The result must identify the registry snapshot,
team type, `applicability_status=not_applicable`, and the rumour-specific rule
`league_rumour_listing:club_competitions`.

The local rumour limiter caps each scope portion at 32 request attempts and 16 MiB
of decoded data. The shared scope and deadline limits also apply. Daily acceptance requires fresh
listing, players, injury and rumours checks. The rumours clock belongs in
`resume_json.daily_check_clocks`. The national-team injury and rumours exceptions each
require the rule for that check kind and the matching registry snapshot.

Production delivery and the bounded E2E remain outstanding. The E2E must
demonstrate the production write, receipt readback, DQ and a zero exclusion
count before scheduled daily enablement. Scheduled collection and three-day
acceptance are still outstanding.
### Observed pagination and empty-grid layouts (#2293)

A unique `#yw1` with exactly the `span.empty` marker `No information
available.` and an empty `div.keys`, without tables, pagination, links, or
other content, represents `observed_empty`. Its observations are empty and
its duplicate count is zero. Missing or altered markers and unexpected
content retain the existing parse errors; absence of a table alone does not
prove that a listing is empty.

This empty grid and a valid rumours table without a pager represent a single
page (`current=1`, `last=1`) using the canonical bare listing for the requested
competition. A supplied page URL must identify that same listing's first
page; `/page/2` and later pages are rejected. Multiple pagers remain invalid.
When a pager lacks a last-page control, its current page can be the last page
only if it is the highest linked page, there is no next-page control, and
numbered links cover every page from 1 through current without gaps. Existing
navigation, competition, and conflicting-reference checks still apply.

Verification status for this layout patch: synthetic regression cases cover
the empty-grid capture and pagination, single-page listings without a pager,
and terminal pagers without a last control. Astra did not execute tests;
test execution and independent review remain for the lead after the
publication gate. This patch supplies no new delivery or E2E evidence and
does not revise those acceptance statuses. Parser and schema versions remain
unchanged.
