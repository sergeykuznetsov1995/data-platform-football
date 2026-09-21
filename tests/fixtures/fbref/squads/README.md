# Atlas historical raw regression fixture

`atlas-2009-2010-7a795569.html.gz` is committed FBref raw read from the existing local S3 archive on 2026-09-12, without a request to FBref.

- Decompressed SHA-256: `7a79556910ea5f403efb7850d0754e60d18e270ddcb7109a5f2a32e0a6d21167`.
- Affected target: `fbref:squad:7c76bc53:a667b6f50d092c9f077c`.
- Regression: DOM tables at source ordinals 5 and 6 both have `id=results2009-20103111_overall`, with 5 and 6 rows respectively. Generic Bronze distinguishes their table instances, but the control manifest originally used only source ID and location.
- The immutable-manifest conflict stopped `dag_backfill_fbref` recovery on 2026-09-06. The test exercises offline recovery with an already-written manifest prefix and verifies idempotent completion.
