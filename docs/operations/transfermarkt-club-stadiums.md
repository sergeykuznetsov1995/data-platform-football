# Transfermarkt club stadium observations

For one saved tmapi `/club/{club_id}/stadium` response, the offline parser records the stadium and the club’s relationship to it at capture time. The caller supplies the club ID from the capture context. One complete response suffices.

## Provenance and MIT license

The stadium field mapping and image deduplication come from the nested stadium construction in `get_club_profile`, in `transfermarkt_api/services/clubs/profile.py` from `felipeall/transfermarkt-api` at commit `71aa75ecd708e5c8c283c4af5968dd370777b3f8`. This adaptation reads an existing envelope. [Pinned donor source](https://github.com/felipeall/transfermarkt-api/blob/71aa75ecd708e5c8c283c4af5968dd370777b3f8/transfermarkt_api/services/clubs/profile.py)

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

## Saved input and API

```python
from scrapers.transfermarkt.club_stadiums import parse_club_stadium

observation = parse_club_stadium(
    payload, club_id='131', observed_at='2026-10-03T06:05:00+00:00',
    country_names={'157': 'España'},
)
encoded = observation.canonical_json().encode('utf-8')
```

`payload` is the decoded JSON envelope. Its minimum successful shape is `{"success":true,"data":{"name":"Ground"}}`. A supplied `message` must be a string. The table gives paths relative to `data`. Both `location` and `buildingDetails` are JSON objects when supplied.

| Saved field | Observation field | Rule |
| --- | --- | --- |
| `clubId` | `club_id` | If supplied, must match the caller’s club ID after conversion to decimal text. |
| `id` | `stadium_id` | Preserve the source ID as decimal text. |
| `name` | `name` | Required string, trimmed and nonempty. |
| `capacity` | `capacity` | Nonnegative integer. |
| `internationalCapacity` | `international_capacity` | Nonnegative integer. |
| `url` | `website` | Source string, including a scheme-less site if supplied. |
| `location.street` | `address_line_1` | Trim whitespace. A blank street gives NULL. |
| `location.postcode`, `location.city` | `address_line_2` | Trim each string and join nonblank parts with one space. Preserve leading postcode zeros. No parts gives NULL. |
| `location.countryId` | `country_id` | Preserve the source ID as decimal text. |
| Optional caller mapping `country_names` | `country_name` | Look up the country ID in saved reference data. A missing match gives NULL. |
| `location.latitude` | `latitude` | Finite number in [−90, 90]. |
| `location.longitude` | `longitude` | Finite number in [−180, 180]. |
| `buildingDetails.buildYear` | `build_year` | Nonnegative integer. |
| `buildingDetails.renovationYear` | `renovation_year` | Nonnegative integer or JSON null for an unknown year. |
| `buildingDetails.fieldLength` | `field_length` | Nonnegative integer. |
| `buildingDetails.fieldWidth` | `field_width` | Nonnegative integer. |
| `buildingDetails.fieldSurface` | `field_surface` | Source string. |
| `images[].url` | `images` | Array of objects with nonblank URL strings. Keep the first occurrence of each exact URL in source order. An optional `source` must be a string. |
| `isHistorical` | `source_is_historical` | Preserve the source boolean. |

IDs accept positive integers and canonical ASCII decimal strings without leading zeros. Booleans are invalid in all numeric and ID fields. An optional field that is absent maps to NULL. Only `buildingDetails.renovationYear` also accepts an explicit JSON null. Invalid field values raise `ClubStadiumError`, a `ValueError` with machine-readable `reason` and `field` attributes. For example, a different source club raises `club_mismatch` at `data.clubId`. A supplied country map accepts integer or decimal-string keys and nonblank string names. Two entries for the requested country must agree.

## Status, grain and completeness

`observed_present` records one stadium observation for the supplied club and capture instant. Its `relation` carries `club_id`, `stadium_id` and `observed_at`. If the stadium ID is absent, both the stadium and the relation retain NULL for that ID. The club and capture still identify the observation. A name or address never generates a global identity. Two clubs can report the same source stadium ID and retain distinct relations.

`observed_at` comes from the caller’s capture metadata. Supply an ISO timestamp with a calendar date, uppercase `T`, hours, minutes, seconds and an explicit `±HH:MM` offset or `Z`. Nonzero fractional seconds use six digits. The output preserves the offset and renders `Z` as `+00:00`. The endpoint supplies no published usage period. Consequently, `relation.valid_from` and `relation.valid_to` are NULL, including for a historical stadium with construction and renovation years.

Missing or false success, missing/null/empty data, and malformed recognized fields fail closed. Additional unknown attributes contribute no evidence of absence. Only the separate factory can record an empty observation from external evidence that coverage for the specified club and capture was complete and yielded no stadium.

```python
from scrapers.transfermarkt.club_stadiums import confirmed_empty_club_stadium

empty = confirmed_empty_club_stadium(
    club_id='131', observed_at='2026-10-03T06:05:00Z',
    completeness_proof='saved-receipts/complete-club-131-capture-17',
)
```

The caller establishes completeness. The factory requires and retains a nonblank proof string. Its `observed_empty` result carries the club, capture time and proof, with NULL stadium and relation. Direct parsing never converts a failure into this status.

## Replay and integration boundary

Both APIs return frozen dataclasses. Image URLs form an immutable tuple. A supplied empty image array serializes as `[]`, while missing images serialize as `null`. Identical envelopes, capture arguments and saved country mappings yield equal observations and byte-identical UTF-8 when encoded as above. Canonical JSON contains the parser and schema revisions, status, club ID, capture time, stadium, relation and completeness proof. Keys are sorted throughout, separators are compact, and serialization uses `allow_nan=False`.

This implementation performs no live requests, club walk, second club/profile traversal or country-reference fetch. It adds no production writer, DAG, schedule, grants or Compose changes. Production acceptance for #2287 remains outstanding.

Integration still requires connecting an authorized capture reader to the parser, carrying its source club ID and timestamp, and defining how the writer preserves observations, relation grain, missing IDs and completeness evidence. Any live collection, Bronze write, deployment or scheduling requires the applicable production authorization. A bounded authorized run, its receipt, readback and data-quality checks remain separate production work.
