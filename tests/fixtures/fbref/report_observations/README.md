# Issue #1792: source excerpts

Minimal schedule row excerpts from the last successful raw responses before
2026-10-21 05:39:19 UTC. BeautifulSoup serialization preserves the relevant
attributes and links; these are not full pages.

| Fixture | Fetch time UTC | Original HTML SHA256 |
| --- | --- | --- |
| schedule-15.html | 2026-10-21 00:07:18.314382 | 81379fa7efb8ff9079e1cb4f3431c69883dcb40c46ab02b00c50cd3e968a7c23 |
| schedule-16.html | 2026-10-21 00:07:24.150239 | 987f95d2a6fd9de876160de783b3a07ea161bf6b5ae7df8d0a046dc0a487c1ac |
| schedule-70.html | 2026-10-21 00:09:01.470951 | afd028e882134dabd2d01428602814911fd69f30578e1c22b249eadf5af2db9f |

The first link in a played row is a date-calendar link, before the score/report
links. League One/Two rows dated October 20 contain no score and only a
Head-to-Head link. Match 3f6c6d57 provides epoch 1792508400 (15:00 UTC).
