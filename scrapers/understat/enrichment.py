"""Optional HTML publication, independent of the seven-table JSON contract.

Consumers must require is_valid and join source_batch_id to the current JSON
batch. A correction first tombstones both enrichment grains, then retries HTML.
The durable activation marker deliberately survives loss of the queue journal.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .cache_state import atomic_json, digest, read_json
from .contracts import UnderstatTableContract
from .manifest import ManifestStatus, ScopeKey
from .match_html import CARD_COLUMNS, parse_match_html, validate_card_counts


logger = logging.getLogger(__name__)
PARSER_VERSION = "understat-html-v2"
_KEY = ("league", "season", "game_id")
_META = ("source_batch_id", "json_hash", "parser_version", "is_valid")


class EnrichmentTableContract(UnderstatTableContract):
    @property
    def column_types(self) -> dict[str, str]:
        return {column: ("bigint" if column in {
            "game_id", "team_id", "player_id", "whoscored_game_id"
        } else "boolean" if column == "is_valid" else "varchar")
                for column in self.required_columns}


ENRICHMENT_CONTRACTS = (
    EnrichmentTableContract("understat_match_enrichment", "match_enrichment", _KEY,
                           _KEY + ("whoscored_game_id",) + _META),
    EnrichmentTableContract("understat_player_match_enrichment", "player_match_enrichment",
                           _KEY + ("team_id", "player_id"),
                           _KEY + ("team_id", "player_id") + CARD_COLUMNS + _META),
)
SCHEDULE_COLUMNS = ("league", "season", "source_season_id", "game_id", "date",
                    "home_team_id", "away_team_id", "_batch_id")
PLAYER_COLUMNS = ("team_id", "player_id", "team_side", "yellow_cards", "red_cards")


def _rows(repository: Any, table: str, columns: tuple[str, ...],
          where: str, params: tuple = ()) -> list[dict]:
    rows = repository._execute(
        f'SELECT {", ".join(columns)} FROM {repository.catalog}.bronze.{table} WHERE {where}',
        params=params,
    )
    return [dict(row) if isinstance(row, dict) else dict(zip(columns, row)) for row in rows]


@contextmanager
def _journal_lock(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    with (root / "queue.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def _write(writer: Any, entry: dict, *, valid: bool, result: dict | None = None) -> None:
    common = {key: entry["row"][key] for key in _KEY}
    common.update(source_batch_id=entry["row"]["_batch_id"], json_hash=entry["hash"],
                  parser_version=PARSER_VERSION, is_valid=valid)
    match = pd.DataFrame([{**common, "whoscored_game_id":
                           result["whoscored_game_id"] if result else None}])
    match["whoscored_game_id"] = pd.array(match.whoscored_game_id, dtype="Int64")
    players = []
    # Include previous identities when invalidating a corrected roster.
    identities = entry["players"] if valid else entry.get("published_players", entry["players"])
    for player in identities:
        cards = (result or {}).get("cards", {}).get(
            (player["team_side"], int(player["player_id"])), {})
        players.append({**common, "team_id": int(player["team_id"]),
                        "player_id": int(player["player_id"]),
                        **{column: json.dumps(cards.get(column, [])) for column in CARD_COLUMNS}})
    player_frame = pd.DataFrame(players)
    for contract, frame in zip(ENRICHMENT_CONTRACTS, (match, player_frame)):
        frame["_entity_type"] = contract.table_name
        writer.write_dataframe(frame, database="bronze", table=contract.table_name,
                               partition_spec=[("league", "identity"), ("season", "identity")],
                               merge_keys=list(contract.natural_key), source="understat")


def run_enrichment(client: Any, repository: Any, *, now: datetime | None = None) -> dict:
    """Drain all published scopes and durable retries, including closed seasons.

    The current DAG runs this only after all mapped JSON tasks finish. Exceptions
    are contained per scope/game and never append a JSON manifest or confirmation.
    """
    now = now or datetime.now(timezone.utc)
    now = now.astimezone(timezone.utc)
    today = now.date().isoformat()
    if client.cache_dir is None:
        return {"status": "waiting", "reason": "no durable cache"}
    root = Path(client.cache_dir) / "html_enrichment"
    with _journal_lock(root):
        marker, journal = root / "activation.json", root / "queue.json"
        state = read_json(journal)
        if marker.exists() and not isinstance(state, dict):
            return {"status": "waiting", "reason": "activation journal lost"}
        schedule = _rows(repository, "understat_schedule", SCHEDULE_COLUMNS, "is_result = true")
        if not marker.exists():
            if journal.exists():
                return {"status": "waiting", "reason": "activation marker lost"}
            pending = _rows(repository, "understat_schedule", SCHEDULE_COLUMNS, "is_result = false")
            atomic_json(marker, {"activated_at": now.isoformat()})
            state = {"activated_at": now.isoformat(), "baseline": [str(r["game_id"]) for r in schedule],
                     "pending": [str(r["game_id"]) for r in pending],
                     "games": {}}
            atomic_json(journal, state)
            return {"status": "activated", "baseline": len(schedule)}
        if read_json(marker) != {"activated_at": state["activated_at"]}:
            return {"status": "waiting", "reason": "activation mismatch"}

        def eligible(row):
            stamp = pd.Timestamp(row["date"])
            if stamp.tzinfo is None:
                stamp = stamp.tz_localize("UTC")
            return (str(row["game_id"]) not in state["baseline"]
                    and (str(row["game_id"]) in state.get("pending", [])
                         or stamp >= pd.Timestamp(state["activated_at"])))

        scopes = {(row["league"], row["season"], int(row["source_season_id"]))
                  for row in schedule if eligible(row)}
        # Queue scopes survive July/window rotation even if absent from discovery.
        scopes.update((e["row"]["league"], e["row"]["season"], int(e["row"]["source_season_id"]))
                      for e in state["games"].values())
        published = failed = 0
        for league, season, year in sorted(scopes):
            try:
                scope = ScopeKey(league=league, season=season, source_season_id=str(year))
                attempt = repository.latest_attempt(scope)
                if (attempt is None or attempt.status is not ManifestStatus.COMPLETE
                        or not repository.verify_physical_batch(attempt)):
                    continue
                current = _rows(repository, "understat_schedule", SCHEDULE_COLUMNS,
                                "league = ? AND season = ? AND is_result = true AND _batch_id = ?",
                                (league, season, attempt.batch_id))
                for row in current:
                    game = str(row["game_id"])
                    if not eligible(row):
                        continue
                    players = _rows(repository, "understat_player_match_stats", PLAYER_COLUMNS,
                                    "league = ? AND season = ? AND game_id = ? AND _batch_id = ?",
                                    (league, season, row["game_id"], attempt.batch_id))
                    if not players:
                        continue
                    row = {**row, "date": str(row["date"])}
                    source_hash = digest({"row": {k: v for k, v in row.items() if k != "_batch_id"},
                                          "players": sorted(players, key=lambda p: (p["team_id"], p["player_id"]))})
                    key = f"{league}/{season}/{game}"
                    entry = state["games"].setdefault(key, {"row": row, "players": players,
                                                            "hash": source_hash, "failures": 0})
                    changed = entry["hash"] != source_hash
                    if changed:
                        # Persist the need to invalidate before either table write.
                        entry["invalidate"] = True
                        entry.update(row=row, players=players, hash=source_hash, failures=0)
                        entry.pop("done", None)
                    elif entry["row"]["_batch_id"] != row["_batch_id"]:
                        entry["row"] = row
                        entry.pop("done", None)  # rebind cached HTML to new publication
                    atomic_json(journal, state)
                    try:
                        writer = repository._get_writer()
                        if entry.get("invalidate"):
                            _write(writer, entry, valid=False)
                            entry.pop("invalidate")
                            entry["deferred_on"] = today
                            entry.pop("html", None)
                            atomic_json(journal, state)
                        if (entry.get("done") or entry["failures"] >= 3
                                or entry.get("deferred_on") == today):
                            continue
                        cached = read_json(root / f"match_{game}.json")
                        html = (cached["html"] if isinstance(cached, dict)
                                and cached.get("hash") == digest(cached.get("html"))
                                and not changed and entry.get("html") else None)
                        if html is None:
                            if entry.get("attempted_on") == today:
                                continue
                            previous = entry.get("attempted_on")
                            entry["attempted_on"] = today
                            # A killed worker has consumed a failed day, too.
                            entry["failures"] += 1
                            atomic_json(journal, state)
                            html = client.get_match_html(game)
                            if html is None:
                                entry["attempted_on"] = previous
                                entry["failures"] -= 1
                                atomic_json(journal, state)
                                continue
                            atomic_json(root / f"match_{game}.json", {"html": html, "hash": digest(html)})
                            entry["html"] = True
                            atomic_json(journal, state)
                        result = parse_match_html(html, row, pd.DataFrame(players))
                        validate_card_counts(result, pd.DataFrame(players))
                        if result["whoscored_game_id"] is None:
                            raise ValueError("match HTML: missing fid")
                        if (repository.latest_attempt(scope).batch_id != attempt.batch_id
                                or not repository.verify_physical_batch(attempt)):
                            continue
                        _write(writer, entry, valid=True, result=result)
                        entry.update(done=True, failures=0, published_players=players)
                        published += 1
                    except Exception:
                        failed += 1
                        if entry["failures"] >= 3:
                            entry["status"] = "unavailable"
                        entry.pop("html", None)
                        logger.warning("Optional Understat HTML failed for %s", key, exc_info=True)
                    atomic_json(journal, state)
            except Exception:
                failed += 1
                logger.warning("Optional Understat HTML scope failed: %s/%s", league, season, exc_info=True)
        return {"status": "complete", "published": published, "failed": failed}
