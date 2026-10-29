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
            # The date cell links to /en/matches/YYYY-MM-DD before the
            # score/report link. Validate each candidate, not just the first.
            match_ids = set()
            for link in row.select('a[href*="/en/matches/"]'):
                try:
                    match_ids.add(match_page_target(link["href"]).source_ids["match_id"])
                except ValueError:
                    continue
            if len(match_ids) != 1:
                continue
            match_id = next(iter(match_ids))
            instants = set()
            for stamp in row.select("[data-venue-epoch], [data-venue-time]"):
                for attribute in ("data-venue-epoch", "data-venue-time"):
                    value = str(stamp.get(attribute, ""))
                    try:
                        if re.fullmatch(r"\d{10}", value):
                            value_at = datetime.fromtimestamp(int(value), timezone.utc)
                        else:
                            value_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
                            if value_at.tzinfo is None or value_at.utcoffset() is None:
                                continue
                        instants.add(value_at)
                    except (ValueError, OverflowError, OSError):
                        continue
            if not instants:
                continue
            value_at = next(iter(instants)) if len(instants) == 1 else None
            # Conflicts within a row or across duplicate rows stay unconfirmed.
            if match_id in kickoff and kickoff[match_id] != value_at:
                kickoff[match_id] = None
            else:
                kickoff[match_id] = value_at
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
