# Transfermarkt achievements offline replay

Issue #2288 remains open for delivery and acceptance. This package does not
authorize production collection, writes, scheduling, or deployment.

## Saved inputs and API

Import `parse_achievements`, `confirmed_empty_achievements`, and
`AchievementParseError` from `scrapers.transfermarkt.achievements`.
Both entry points require a recipient type (`player`, `club`, or `coach`)
and a positive numeric recipient ID. The caller supplies the identity of
the saved capture. The parser has no client, clock, or storage dependency.

```python
result = parse_achievements(saved_input, recipient_type="player", recipient_id="7")
rows = [row.as_dict() for row in result.rows]
encoded = result.canonical_json().encode("utf-8")
```

For a player or club, pass a decoded tmapi dictionary. A response envelope
must have boolean `success: true` and a dictionary in `data`. The inner
dictionary alone is also accepted. It must contain the list `achievements`.
The optional `aggregated` and `trophies` fields are lists. Input files, URLs,
and undecoded JSON strings are outside this API.

- Each detail's `id` identifies the award. `trophyId` identifies the trophy.
  A nonblank detail `name` supplies the title. Otherwise the parser uses
  the name of the aggregate with the same canonical award ID, then the name
  of the trophy with the same canonical trophy ID. The trophy fallback covers
  published participations/runner-up records that have no aggregate group.
- `seasonId` and `season.id` must agree when both are present. The display
  comes from `season.display`, with `season.name` and `season.year` as fallbacks. Whitespace is
  normalized. The strings `2022` and `25/26` retain their original meanings.
- `club.id` and `club.name` describe the associated club or national team.
  The flat fields `clubId` and `clubName` are also supported.
  `competition.id` or `competitionId` identifies the competition.
  An explicit `result` supplies the result. A title alone leaves it null.
- Optional `details` can carry player, club, competition, season, and trophy
  IDs or an explicit result. Published IDs in corresponding fields must
  agree. A nonzero `details.playerId` on a player record must identify the
  supplied recipient. Club records can omit `details`.
- Numeric IDs accept unsigned integers and ASCII decimal strings. Leading
  zeroes are stripped without converting strings through floating point.
  Booleans, floats, negatives, and malformed identifiers fail the capture.
  Competition and explicit observation IDs also accept alphanumeric codes
  containing a letter, with internal hyphens or underscores. Zero in
  `trophyId` or a details ID, and an empty string in a club or competition
  ID, signify an unpublished ID. Optional unpublished values serialize as null.

For a coach, pass saved, decoded HTML text. One supported layout has a
`.box` headed **All titles**, with counted title rows inside the table.
The other has separate boxes, each headed with a count and title such as
**2x Example Cup**. Each season/club table row supplies one observation.
Calendar years and slash or hyphen season ranges are accepted. A team may
have printed text, a linked name, or a linked crest and an adjacent name.
Club links identify the team through `/startseite/verein/<id>` or
`/profil/verein/<id>`. An explicit link suffix `/saison_id/<id>` can
supply the season ID. A competition link in the title or detail can supply
the `/wettbewerb/<id>`. Text alongside a linked team name or in a trailing
note cell is retained in `result`. The parser makes no request to these links.

## Rows, identity, and replay

The output is a frozen `AchievementResult`, revision `achievements-v1`,
schema revision `1`. Its `rows` tuple follows the original array or document
order. One item corresponds to one detail observation for one recipient.
Repeated titles, seasons, teams, and even completely identical details retain
their multiplicity. `ACHIEVEMENT_COLUMNS` defines the row projection.

```text
recipient_type, recipient_id, title, award_id, trophy_id, season_id,
season, result, club_id, club_name, competition_id, source_row_id
```

`Achievement.natural_key` follows `ACHIEVEMENT_NATURAL_KEY` in this order.

```text
recipient_type, recipient_id, award_id, trophy_id, competition_id,
title, season_id, season, result, source_row_id
```

JSON observations use `achievements:<zero-based index>` by default. An
explicit `sourceRowId`, `rowId`, or `observationId` (also accepted in
snake case) gives `source:<id>`. Further occurrences of the same explicit
ID receive suffixes `:2`, `:3`, and so on in source order. Coach locators
are `box:<index>:row:<index>`, counting all boxes and the box's table rows
from zero, including header rows. Positional locators identify observations
within a saved capture. Reordering upstream rows can change these locators.

`reconciliation` is a separate tuple in published group order. For JSON,
each unique aggregate award ID is compared to the number of details bearing
that ID. A published `count` (also `receivedCount`, `amount`, `number`, or
`total`; all supplied aliases must agree) must match exactly. A name-only aggregate must match at least one detail and
records a null published count. Detail awards can exist without an aggregate.
For HTML, each title section must contain at least one detail and its
published count must match that section's row count. Repeated sections with
the same title have separate group locators. Only detail records produce rows.

Identical inputs and owner identity produce equal dataclasses and identical
UTF-8 replay bytes. `canonical_json()` serializes the entire result with
sorted keys, compact separators, literal Unicode, and `allow_nan=False`.

## Empty evidence and errors

A successfully validated nonempty capture returns `observed_present`. An
explicit empty JSON detail list with consistent aggregate evidence returns
`observed_empty`. Missing coach titles, an empty box, and a blocked page
cannot establish absence. An external completeness check may record an
empty result for any of the three recipient types through this separate API.

```python
empty = confirmed_empty_achievements(
    recipient_type="coach", recipient_id="7", completeness_proof="evidence/sample-proof",
)
```

The caller supplies a nonblank proof reference and owns verification of the
underlying evidence. The result carries that reference. The parser never
invokes this constructor as a recovery action.

Invalid input raises `AchievementParseError`. The `reason` attribute
identifies the failure, for example `invalid_achievements`,
`owner_conflict`, `aggregate_count_mismatch`, or `ambiguous_coach_layout`.
An invalid late detail invalidates the whole result. Mixed layouts, malformed
counts or links, missing cells, and a detail before its title also fail the
capture. Preserve the failed capture for diagnosis in the future capture lane.

## Replay and remaining acceptance

Run the focused synthetic suite from the repository worktree root.

```bash
/root/.venvs/dpf-test/bin/pytest tests/unit/scrapers/test_transfermarkt_achievements.py -q
```

The future capture lane owns the source URL, owner/universe membership,
capture and observation clocks (including `observed_at`), raw object reference
and content hash, and batch identity. These belong outside the parser columns.
A future Bronze writer must bind each capture to its parser revision, record
a write receipt, and verify persisted rows and counts by readback. Universe
coverage must account for players, clubs, and coaches, including proven empty
observations and failures. Monthly and post-season coverage and the three
required DQ observations need separate operational evidence tied to the
relevant runs and code revision. Focused replay exercises only saved-input
behavior. Production acceptance and completion of #2288 remain outstanding.

## Production lane

The production extension keeps the offline parser unchanged and adds raw-first
capture through the existing metered Transfermarkt client. tmapi achievement
paths are admitted explicitly by the raw store; coach pages use the saved slug
and numeric ID from the existing coach universe. Direct HTTP fallback is
forbidden.

The frozen universe is read from player attribute observations, squad
memberships and coach profiles. `AchievementState` stores every recipient and
terminal status in durable SQLite, including errors and exact remaining work.
An assessment completes only after every recipient has a terminal outcome and
each recipient type has at least 95% verified conclusive observations.

`iceberg.bronze.transfermarkt_achievement_observations` is append-only. Its
physical key is `(recipient_type,recipient_id,source_capture_id,source_row_id)`.
The same capture replays without duplicates; a later capture appends history.
A conclusive empty recipient has one status row with null achievement fields.
`iceberg.ops.transfermarkt_achievement_receipts_v1` binds each recipient plan to
its raw hash, parser/schema, universe/run, detail/status counts, batch and full
physical hash. The writer verifies all physical columns and then rereads the
receipt. Missing receipts can be recovered from the identical committed plan;
any physical drift fails.

`dag_ingest_transfermarkt_achievements` is paused at creation. Its hourly
45-minute portions use one metered stream, exactly 180 initial requests and 360
retries under the unexpired dedicated achievements policy, with a 16 MiB provider
byte cap. The runner refuses unmetered or rawless execution before capture.
Monthly refresh is due after
30 days. A completed season-close record, or a completed registry edition with
a current successor, binds a once-only post-season assessment. DQ reports
per-type expected/conclusive/unresolved coverage and physical count/key/lineage
reconciliation, including physical captures without receipts. Initial incomplete
coverage is RED, not silently green.

Delivery requires the reviewed merge and existing automatic frozen-release
pipeline. Before any paid E2E, record exact recipient URLs, attempt/byte caps,
raw destination and writer tables. Unpause only after E2E receipts and physical
readback pass. Full initial capture, following no-op, three green DQ days and
post-season refresh remain acceptance evidence; this implementation alone does
not close #2288.

## Adaptations and notices

The player and club mappings adapt Felipe Allegretti's achievement service
and the player/club route context at commit
`71aa75ecd708e5c8c283c4af5968dd370777b3f8`. The service maps individual
details before grouping, including title, season, club, and competition.
This implementation replaces the async entity fetch and grouped output with
saved-input replay, individual rows, and strict count reconciliation.
[Achievement service](https://github.com/felipeall/transfermarkt-api/blob/71aa75ecd708e5c8c283c4af5968dd370777b3f8/transfermarkt_api/services/achievements.py)
· [Pinned project and route context](https://github.com/felipeall/transfermarkt-api/tree/71aa75ecd708e5c8c283c4af5968dd370777b3f8)

The coach mapping adapts Chetan Jain's manager `get_achievements`, which
delegates to the player module's `parse_achievements` / `_achievement_boxes`,
at commit `ad660278c0c95bd46c2d0553658fcd14ebd9cb8f`. Both the combined
title table and separate title boxes are represented here. The offline
adapter preserves each detail, checks the published counts, and rejects
ambiguous or incomplete layouts.
[Manager entry point](https://github.com/omkarcloud/transfermarkt-scraper/blob/ad660278c0c95bd46c2d0553658fcd14ebd9cb8f/transfermarkt/managers.py)
· [Player HTML parser](https://github.com/omkarcloud/transfermarkt-scraper/blob/ad660278c0c95bd46c2d0553658fcd14ebd9cb8f/transfermarkt/players.py)

The following licenses cover the upstream software adaptations. Transfermarkt
data rights are outside the scope of these software licenses.

### felipeall/transfermarkt-api

```text
MIT License

Copyright (c) 2023 Felipe Allegretti

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### omkarcloud/transfermarkt-scraper

```text
MIT License

Copyright (c) 2026 Chetan Jain

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
