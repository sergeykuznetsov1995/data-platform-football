"""Read-only Bronze inventory for the history controller; injectable offline.

Two set-based reads per planning run. No discovery or SofaScore requests.
The adapter requires source-native season IDs and published endpoint evidence.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from dags.utils.sofascore_all_mens_state import CampaignPlanningError

TERMINAL = ("success", "legitimate_empty", "not_supported")
ENDPOINTS = ("event", "incidents", "lineups", "shotmap", "statistics")


def publication_complete(records: list[Mapping], *, tournament_id: int, season_id: int,
                         match_id: str, capture_complete: bool) -> bool:
    """Offline canonical-manifest reader, shared definition with SQL below."""
    terminal = set()
    for record in records:
        key = record.get("key", record)
        if (str(key.get("source_tournament_id")) == str(tournament_id)
                and str(key.get("source_season_id")) == str(season_id)
                and str(key.get("target_id")) == str(match_id)
                and key.get("target_type") == "event" and key.get("freshness_key") == "final"
                and record.get("status") in TERMINAL):
            terminal.add(key["endpoint"])
    return capture_complete is True and set(ENDPOINTS) <= terminal


def _aliases(snapshot: Mapping, registry: Mapping) -> tuple[str, dict]:
    aliases = {str(t["capture_key"]): int(t["unique_tournament_id"]) for t in snapshot["tournaments"]}
    seasons = {}
    for t in snapshot["tournaments"]:
        for s in t["seasons"]:
            if s.get("canonical_season"):
                seasons.setdefault((int(t["unique_tournament_id"]), str(s["canonical_season"])), set()).add(int(s["source_season_id"]))
    for t in registry.get("tournaments", []):
        key = t.get("canonical_id")
        if key:
            aliases[str(key)] = int(t["unique_tournament_id"])
        for s in t.get("seasons", []):
            if s.get("canonical_season"):
                seasons.setdefault((int(t["unique_tournament_id"]), str(s["canonical_season"])), set()).add(int(s.get("season_id", s.get("source_season_id"))))
    values = ",".join("('" + key.replace("'", "''") + "'," + str(tid) + ")" for key, tid in sorted(aliases.items()))
    return values, seasons


PUBLICATION_CTE = """
terminal AS (
 SELECT CAST(source_tournament_id AS bigint) tid, CAST(source_season_id AS bigint) sid,
        CAST(target_id AS varchar) mid
 FROM iceberg.ops.sofascore_capture_manifest
 WHERE target_type='event' AND freshness_key='final'
   AND endpoint IN ('event','incidents','lineups','shotmap','statistics')
   AND status IN ('success','legitimate_empty','not_supported')
 GROUP BY 1,2,3 HAVING count(DISTINCT endpoint)=5
), published AS (
 SELECT DISTINCT c.league, CAST(c.season AS varchar) season, CAST(c.match_id AS varchar) mid
 FROM iceberg.bronze.sofascore_match_capture_status c WHERE c.capture_complete=true
)
"""


def queries(alias_values: str, cohort_values: str = "") -> tuple[str, str]:
    prefix = "WITH aliases(league,tid) AS (VALUES " + alias_values + "), " + PUBLICATION_CTE
    scope = prefix + """
SELECT a.tid, CAST(s.season_id AS bigint),
 count(DISTINCT CASE WHEN s.status_type='finished' THEN CAST(s.game_id AS varchar) END),
 count(DISTINCT CASE WHEN s.status_type='finished' AND p.mid IS NOT NULL AND m.mid IS NOT NULL THEN CAST(s.game_id AS varchar) END),
 count_if(s.status_type NOT IN ('finished','canceled','cancelled') AND
          (s.start_timestamp >= ? OR s.status_type IN ('inprogress','interrupted')))
FROM iceberg.bronze.sofascore_schedule s JOIN aliases a ON a.league=s.league
LEFT JOIN published p ON p.league=s.league AND p.season=CAST(s.season AS varchar) AND p.mid=CAST(s.game_id AS varchar)
LEFT JOIN terminal m ON m.tid=a.tid AND m.sid=CAST(s.season_id AS bigint) AND m.mid=CAST(s.game_id AS varchar)
WHERE s.season_id IS NOT NULL GROUP BY 1,2
"""
    cohort = ("VALUES " + cohort_values if cohort_values else
              "SELECT CAST(NULL AS bigint), CAST(NULL AS bigint), CAST(NULL AS varchar), CAST(NULL AS varchar) WHERE false")
    july = prefix + ", saved_cohort(tid,sid,mid,season) AS (" + cohort + """), stats AS (
 SELECT league, CAST(season AS varchar) season, CAST(match_id AS varchar) mid, min(_ingested_at) first_ingested
 FROM iceberg.bronze.sofascore_match_stats GROUP BY 1,2,3
), targets AS (
 SELECT a.tid, s.season, s.mid FROM stats s LEFT JOIN aliases a ON a.league=s.league
 WHERE s.first_ingested >= TIMESTAMP '2026-07-01 00:00:00'
   AND s.first_ingested < TIMESTAMP '2026-08-01 00:00:00'
 UNION SELECT tid,season,mid FROM saved_cohort
), events AS (
 SELECT DISTINCT a.tid, CAST(e.season AS varchar) season, CAST(e.match_id AS varchar) mid
 FROM iceberg.bronze.sofascore_events e JOIN aliases a ON a.league=e.league
), published_aliases AS (
 SELECT DISTINCT a.tid,p.season,p.mid FROM published p JOIN aliases a ON a.league=p.league
), raw_matches AS (
 SELECT DISTINCT CAST(source_tournament_id AS bigint) tid, CAST(source_season_id AS bigint) sid, CAST(target_id AS varchar) mid
 FROM iceberg.ops.sofascore_capture_manifest WHERE target_type='event' AND freshness_key='final'
 AND endpoint IN ('event','incidents','lineups','shotmap','statistics')
 AND ((raw_blob_key IS NOT NULL AND raw_blob_key<>'') OR status='not_supported')
 GROUP BY 1,2,3 HAVING count(DISTINCT endpoint)=5
)
SELECT s.tid, s.season, s.mid, e.mid IS NOT NULL,
       p.mid IS NOT NULL, r.sid, tm.sid
FROM targets s
LEFT JOIN events e ON e.tid=s.tid AND e.season=s.season AND e.mid=s.mid
LEFT JOIN published_aliases p ON p.tid=s.tid AND p.season=s.season AND p.mid=s.mid
LEFT JOIN raw_matches r ON r.tid=s.tid AND r.mid=s.mid
LEFT JOIN terminal tm ON tm.tid=s.tid AND tm.mid=s.mid
"""
    return scope, july


def verified_schedules(result_dir: str | Path) -> set[tuple[int, int]]:
    """Successful season phases establish full page-chain enumeration.

    A successful match phase or legacy completed list alone is insufficient.
    Malformed reports are ignored, so they cannot open the history gate.
    """
    verified = set()
    for path in Path(result_dir).glob("*.json"):
        try:
            report = json.loads(path.read_text())
            if any(p.get("phase") == "season" and p.get("status") == "success"
                   for p in report.get("phases", [])):
                verified.add((int(report["tournament_id"]), int(report["source_season_id"])))
        except (ValueError, TypeError, KeyError, OSError):
            continue
    return verified


def from_rows(snapshot: Mapping, scope_rows, july_rows, *, registry=None,
              schedule_verified=(), july_cohort=(), observed_at=None) -> dict:
    _, seasons = _aliases(snapshot, registry or {})
    snapshot_pairs = {(int(t["unique_tournament_id"]), int(s["source_season_id"]))
                      for t in snapshot["tournaments"] for s in t["seasons"]}
    verified = set(schedule_verified)
    rows = {}
    for tid, sid, finished, closed, future in scope_rows:
        pair = int(tid), int(sid)
        if pair in rows:
            raise CampaignPlanningError("duplicate source season inventory")
        rows[pair] = {"tournament_id": pair[0], "season_id": pair[1],
                      "finished": int(finished), "closed": int(closed),
                      "ongoing": True if int(future) else False if pair in verified else None,
                      "schedule_complete": pair in verified}
    for pair in verified & snapshot_pairs:
        rows.setdefault(pair, {"tournament_id": pair[0], "season_id": pair[1],
                               "finished": 0, "closed": 0, "ongoing": False,
                               "schedule_complete": True})
    cohort = {tuple(str(v) for v in entry) for entry in july_cohort}
    observed_cohort = set()
    july_closed, unresolved = 0, set()
    pending, replayable = {}, {}
    # SQL may return multiple raw pointers for a target. Resolve by its exact
    # partition mapping, never choose the first raw season arbitrarily.
    grouped = {}
    for tid, season, mid, has_event, published, raw_sid, terminal_sid in july_rows:
        key = (int(tid) if tid is not None else None, str(season), str(mid))
        record = grouped.setdefault(key, {"has_event": bool(has_event), "published": bool(published), "raw_seasons": set(), "terminal_seasons": set()})
        if raw_sid is not None:
            record["raw_seasons"].add(int(raw_sid))
        if terminal_sid is not None:
            record["terminal_seasons"].add(int(terminal_sid))
    for (tid, season, mid), record in grouped.items():
        candidates = seasons.get((tid, season), set())
        saved = {int(sid) for t, sid, target in cohort if t == str(tid) and target == mid}
        if len(saved) == 1:
            candidates = saved
        if len(candidates) != 1:
            if not record["has_event"]:
                unresolved.add((tid, season, mid))
            continue
        sid = next(iter(candidates))
        if (tid, sid) not in snapshot_pairs:
            if not record["has_event"] or (str(tid), str(sid), mid) in cohort:
                unresolved.add((tid, season, mid))
            continue
        identity = (str(tid), str(sid), mid)
        observed_cohort.add(identity)
        if record["has_event"] and identity not in cohort:
            continue
        cohort.add(identity)
        # Publication counter is confirmed below using the same source-season
        # terminal set as the normal inventory (a third read is avoided).
        if record["published"] and sid in record["terminal_seasons"]:
            july_closed += 1
        else:
            pair = tid, sid
            pending.setdefault(pair, set()).add(mid)
            if sid in record["raw_seasons"]:
                replayable.setdefault(pair, set()).add(mid)
    for pair, ids in pending.items():
        row = rows.setdefault(pair, {"tournament_id": pair[0], "season_id": pair[1],
                                    "finished": 0, "closed": 0, "ongoing": None, "schedule_complete": False})
        row["july_match_ids"] = sorted(ids, key=int)
        row["july_raw_match_ids"] = sorted(replayable.get(pair, ()), key=int)
    return {"observed_at": observed_at or datetime.now(timezone.utc).isoformat(),
            "scopes": list(rows.values()), "july_closed": july_closed,
            "july_unresolved": len(unresolved) + len(cohort - observed_cohort), "july_cohort": sorted(cohort),
            "verified_schedules": sorted(verified)}


def collect(snapshot: Mapping, *, result_dir: str | Path, checkpoint_path: str | Path,
            registry_path: str | Path, connect=None) -> dict:
    # Import only at task runtime; parsing the DAG cannot touch a service.
    if connect is None:
        from dags.scripts.prepare_sofascore_workload import _trino_connect
        connect = _trino_connect
    registry = json.loads(Path(registry_path).read_text())
    checkpoint = Path(checkpoint_path)
    previous = json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
    values, _ = _aliases(snapshot, registry)
    canonical = {(int(t["unique_tournament_id"]), int(s["source_season_id"])): str(s["canonical_season"])
                 for t in snapshot["tournaments"] for s in t["seasons"] if s.get("canonical_season")}
    cohort_rows = []
    for tid, sid, mid in previous.get("july_cohort", []):
        pair = int(tid), int(sid)
        if pair in canonical:
            cohort_rows.append(f"({pair[0]},{pair[1]},'{str(int(mid))}','{canonical[pair].replace(chr(39), chr(39) * 2)}')")
    scope_sql, july_sql = queries(values, ",".join(cohort_rows))
    conn = connect()
    try:
        cursor = conn.cursor()
        cursor.execute(scope_sql, (datetime.now(timezone.utc).timestamp(),))
        scopes = cursor.fetchall()
        cursor.execute(july_sql)
        july = cursor.fetchall()
    finally:
        conn.close()
    return from_rows(snapshot, scopes, july, registry=registry,
                     schedule_verified=verified_schedules(result_dir) | {tuple(pair) for pair in previous.get("verified_schedules", [])},
                     july_cohort=previous.get("july_cohort", ()))
