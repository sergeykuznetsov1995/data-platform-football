"""Source-owned match-report observation helpers; no network or clock reads."""

from datetime import datetime, timezone
import re



def played_score(score: object, notes: object = "") -> bool:
    """A real result; administrative awards and unfinished games are excluded."""
    return bool(re.search(r"\d+\s*[–—-]\s*\d+", str(score or ""))) and not re.search(
        r"postpon|cancel|abandon|award|walkover", str(notes or ""), re.IGNORECASE
    )


def schedule_observations(html: str, rows: list[dict]) -> list[dict]:
    """Use only an explicit epoch/offset; never reinterpret venue time as UTC."""
    from bs4 import BeautifulSoup, Comment
    from scrapers.fbref.raw_store import match_page_target

    kickoff = {}
    soup = BeautifulSoup(html, "html.parser")
    # FBref may wrap schedule tables in comments.
    documents = [soup] + [
        BeautifulSoup(str(comment), "html.parser")
        for comment in soup.find_all(string=lambda s: isinstance(s, Comment))
        if "sched" in str(comment)
    ]
    for document in documents:
        for row in document.select("table[id^=sched] tr"):
            link = row.select_one('a[href*="/en/matches/"]')
            stamp = row.select_one("[data-venue-epoch]") or row.select_one("[data-venue-time]")
            if link is None or stamp is None:
                continue
            try:
                match_id = match_page_target(link["href"]).source_ids["match_id"]
                value = str(stamp.get("data-venue-epoch", stamp.get("data-venue-time")))
                if re.fullmatch(r"\d{10}", value):
                    instant = datetime.fromtimestamp(int(value), timezone.utc)
                else:
                    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    if instant.tzinfo is None or instant.utcoffset() is None:
                        continue
                kickoff[match_id] = instant
            except (ValueError, OverflowError, OSError):
                continue
    observations = {}
    for row in rows:
        if not row.get("match_url"):
            continue
        target = match_page_target(str(row["match_url"]))
        match_id = target.source_ids["match_id"]
        completed = played_score(row.get("score"), row.get("notes"))
        previous = observations.get(match_id)
        observations[match_id] = {
            "match_id": match_id,
            "match_url": target.canonical_url,
            "completed": completed or bool(previous and previous["completed"]),
            "kickoff_at": kickoff.get(match_id),
        }
    return list(observations.values())
