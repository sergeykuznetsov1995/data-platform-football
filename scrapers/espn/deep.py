"""Resumable current-season deep collection, only through the scheduled DAG.

Only published, played, non-duplicate matches enter the queue. Registry probes
and retries use the same history gate and its atomic 20% deep sub-quota.
The first stage completes plays + both teams; the second one, per edition after
the first, fills played lineup rows from core rosters and player statistics.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import uuid

import pandas as pd
import pyarrow as pa

from . import urls
from .bronze_rows import BatchStamp, _lineage
from .bronze_schema import LINEUP_TABLE, MATCH_TABLE, TEAM_STATS_TABLE, TABLES
from .bronze_writer import BronzeBatchError, delete_filter, stored_rows
from .deep_parser import (PARSER_VERSION, collect_plays, deep_level, mask_xg, parse_page,
                          parse_roster, play_values, player_statistics, statistics,
                          xg_support)
from .deep_schema import PLAYS_SCHEMA, PLAYS_TABLE
from .deep_store import (PLAYER_RETRY_TABLE, Depth, client_state_dir, edition_lock,
                         load_depth, save_depth, load_retries, save_retry)
from .history import history_stopped
from .history_report import render_live_debt_sql
from .journal import flush_journal, journal_rows
from .parallel import interruptible_sleep, protected
from .parser_common import EspnParseError, canonical_json
from .transport_contracts import (AllOriginsBlocked, DailyCapExceeded, EndpointType,
                                  LaneClosed, EspnTransportError)
from .wave import _fetch, _raw_ref

_PLAYER_ENDPOINTS = (EndpointType.ROSTER, EndpointType.PLAYER_STATISTICS)
# not_applicable is requeued once a later Summary captures the lineup.
PLAYERS_PENDING_SQL = (
    "SELECT t.event_id FROM iceberg.bronze.espn_team_stats t "
    "JOIN iceberg.bronze.espn_match m ON m.competition_slug = t.competition_slug "
    "AND m.season_year = t.season_year AND m.event_id = t.event_id "
    "WHERE t.competition_slug = ? AND t.season_year = ? "
    "AND (json_extract_scalar(t.deep_stats_json, '$.players_state') = 'pending' "
    "OR json_extract_scalar(t.deep_stats_json, '$.players_state') = 'not_applicable' "
    "AND m.lineup_state = 'captured') "
    "GROUP BY t.event_id HAVING count(*) = 2 ORDER BY t.event_id"
)
# Raw bodies of an interrupted match are replayed; failures defer longer (1 day).
PLAYER_RAW_TTL = timedelta(hours=12)


def _manifest(result):
    raw = _raw_ref(result)
    return {"raw_uri": raw.raw_uri, "raw_sha256": raw.raw_sha256,
            "fetched_at": raw.fetched_at.isoformat()}


def _replace(trino, table, schema, rows, scope, stamp):
    pa.Table.from_pylist(rows, schema=schema)
    frame = pd.DataFrame(rows, columns=schema.names)
    written = trino.insert_dataframe_atomic(
        "bronze", table, frame, delete_filter=scope,
        staging_id=f"b{stamp.batch_id}", single_statement_replace=True,
        target_column_types=trino.arrow_schema_to_trino(schema),
    )
    if written != len(rows):
        raise BronzeBatchError(f"{table}: inserted {written}, expected {len(rows)}")


def publish_deep(*, trino, slug, year, event_id, plays, teams, manifest, has_xg, state_dir=None):
    """Children before marker; Summary rows are read again under the same lock.

    A complete existing first stage is never replaced by a partial one. A
    failure after a child commit leaves the marker pending and is replayable.
    """
    if len(plays) < 1000 or len(teams) != 2 or len({play["id"] for play, _ in plays}) != len(plays):
        raise EspnParseError("deep publication is incomplete")
    scope = delete_filter(slug, year, [event_id])
    with edition_lock(slug, year, state_dir):
        matches = stored_rows(trino, MATCH_TABLE, scope)
        if len(matches) != 1:
            raise EspnParseError("deep publication needs one stored match")
        match = matches[0]
        if not match["played_final"] or match["first_published_at"] is None or match["duplicate_of"] is not None:
            raise EspnParseError("deep publication needs a published played owner")
        team_rows = stored_rows(trino, TEAM_STATS_TABLE, scope)
        if match["deep_state"] == "completed":
            if has_xg is not True or len(team_rows) == 2 and all(
                row["deep_stats_json"] is not None and
                json.loads(row["deep_stats_json"]).get("has_xg") is True
                for row in team_rows
            ):
                return False
        expected = {match["home_team_id"], match["away_team_id"]}
        if (None in expected or len(expected) != 2 or set(teams) != expected
                or not {row["team_id"] for row in team_rows}.issubset(expected)
                or len({row["team_id"] for row in team_rows}) != len(team_rows)):
            raise EspnParseError("deep team identities differ from Summary")
        stamp = BatchStamp(uuid.uuid4().hex, datetime.now(timezone.utc).replace(tzinfo=None))
        present = {row["team_id"] for row in team_rows}
        for side in ("home", "away"):
            team_id = match[f"{side}_team_id"]
            if team_id not in present:
                _, raw = teams[team_id]
                team_rows.append({
                    **dict.fromkeys(TABLES[TEAM_STATS_TABLE].names),
                    "competition_slug": slug, "season_year": year, "event_id": event_id,
                    "team_id": team_id, "team": match[f"{side}_team"], "home_away": side,
                    "team_stats_state": match["team_stats_state"],
                    **_lineage(raw=raw, parser_version=PARSER_VERSION, stamp=stamp),
                })
        rows = []
        for play, raw in plays:
            rows.append({"competition_slug": slug, "season_year": year, "event_id": event_id,
                         **play_values(play, has_xg),
                         **_lineage(raw=raw, parser_version=PARSER_VERSION, stamp=stamp)})
        for row in team_rows:
            body, raw = teams[row["team_id"]]
            row["deep_stats_json"] = canonical_json({
                "parser_version": PARSER_VERSION, "has_xg": has_xg,
                "statistics": mask_xg(body, has_xg), "manifest": manifest,
                "players_state": "pending",
            })
            row["_batch_id"], row["_ingested_at"] = stamp.batch_id, stamp.ingested_at
        match["deep_state"] = "completed"
        match["_batch_id"], match["_ingested_at"] = stamp.batch_id, stamp.ingested_at
        # Validate every frame before the first commit.
        pa.Table.from_pylist(rows, schema=PLAYS_SCHEMA)
        pa.Table.from_pylist(team_rows, schema=TABLES[TEAM_STATS_TABLE])
        pa.Table.from_pylist([match], schema=TABLES[MATCH_TABLE])
        _replace(trino, PLAYS_TABLE, PLAYS_SCHEMA, rows, scope, stamp)
        _replace(trino, TEAM_STATS_TABLE, TABLES[TEAM_STATS_TABLE], team_rows, scope, stamp)
        _replace(trino, MATCH_TABLE, TABLES[MATCH_TABLE], [match], scope, stamp)
        return True


def _check_roster(rosters, lineup):
    """Played roster players must be Summary lineup athletes of the same team."""
    for team_id, roster in rosters.items():
        known = {row["athlete_id"] for row in lineup if row["team_id"] == team_id}
        if not {athlete for athlete, played in roster.items() if played} <= known:
            raise EspnParseError("roster player is not in the Summary lineup")


def publish_players(*, trino, slug, year, event_id, rosters, players, manifest, has_xg,
                    state_dir=None):
    """Lineup statistics before the team marker, under the edition lock.

    ``rosters=None`` marks a match without a captured lineup as not applicable.
    Lineup rows outside the roster are kept as they are; a repeat is a no-op.
    """
    scope = delete_filter(slug, year, [event_id])
    with edition_lock(slug, year, state_dir):
        matches = stored_rows(trino, MATCH_TABLE, scope)
        if len(matches) != 1 or matches[0]["deep_state"] != "completed":
            raise EspnParseError("player publication needs a completed first stage")
        match = matches[0]
        team_rows = stored_rows(trino, TEAM_STATS_TABLE, scope)
        expected = {match["home_team_id"], match["away_team_id"]}
        if len(team_rows) != 2 or {row["team_id"] for row in team_rows} != expected:
            raise EspnParseError("player publication needs both team rows")
        envelopes = {row["team_id"]: json.loads(row["deep_stats_json"] or "{}") for row in team_rows}
        states = {envelope.get("players_state") for envelope in envelopes.values()}
        if states != {"pending"} and not (states == {"not_applicable"} and rosters is not None
                                          and match["lineup_state"] == "captured"):
            return False
        stamp = BatchStamp(uuid.uuid4().hex, datetime.now(timezone.utc).replace(tzinfo=None))
        lineup = []
        if rosters is None:
            if match["lineup_state"] == "captured":
                raise EspnParseError("captured lineup needs player statistics")
            states = dict.fromkeys(expected, {"players_state": "not_applicable"})
        else:
            if match["lineup_state"] != "captured" or set(rosters) != expected:
                raise EspnParseError("rosters differ from the Summary match")
            lineup = stored_rows(trino, LINEUP_TABLE, scope)
            _check_roster(rosters, lineup)
            played = {(team_id, athlete) for team_id, roster in rosters.items()
                      for athlete, flag in roster.items() if flag}
            if set(players) != played:
                raise EspnParseError("player statistics are incomplete")
            for row in lineup:
                player = players.get((row["team_id"], row["athlete_id"]))
                if player is not None:
                    body, entry = player
                    row["deep_stats_json"] = canonical_json({
                        "parser_version": PARSER_VERSION, "has_xg": has_xg,
                        "statistics": mask_xg(body, has_xg), "manifest": entry,
                    })
                    row["_batch_id"], row["_ingested_at"] = stamp.batch_id, stamp.ingested_at
            # Unused substitutes are absent by definition, not a gap.
            states = {team_id: {"players_state": "completed",
                                "unused": sorted(athlete for athlete, flag in roster.items() if not flag),
                                "roster_manifest": [entry for entry in manifest if entry["team_id"] == team_id]}
                      for team_id, roster in rosters.items()}
            pa.Table.from_pylist(lineup, schema=TABLES[LINEUP_TABLE])
        for row in team_rows:
            row["deep_stats_json"] = canonical_json({**envelopes[row["team_id"]], **states[row["team_id"]]})
            row["_batch_id"], row["_ingested_at"] = stamp.batch_id, stamp.ingested_at
        pa.Table.from_pylist(team_rows, schema=TABLES[TEAM_STATS_TABLE])
        if lineup:
            _replace(trino, LINEUP_TABLE, TABLES[LINEUP_TABLE], lineup, scope, stamp)
        _replace(trino, TEAM_STATS_TABLE, TABLES[TEAM_STATS_TABLE], team_rows, scope, stamp)
        return True


def run_deep(*, client, trino, conn, denominator, run_id, deadline,
             stop_file: Path | None, now_fn=lambda: datetime.now(timezone.utc)):
    """Serial runner on the shared background pool; no extra workers."""
    if deadline.tzinfo is None:
        raise ValueError("deep deadline must be timezone-aware")
    if client.gate.lane != "history" or client.gate.purpose != "deep":
        raise ValueError("deep requires a history gate with purpose=deep")
    client.run_id, client.task_id = run_id, "run_deep"
    report = {"completed": 0, "failed": 0, "mapped": 0, "waiting": 0, "reason": "idle",
              "players_completed": 0, "players_failed": 0, "players_not_applicable": 0}
    errors = []
    offset = len(client.ledger)
    started_at = now_fn()
    attempt_journal = getattr(client, "attempt_journal", None)
    initial_attempts = ({row["attempt_id"] for row in attempt_journal.rows(started_at)}
                        if attempt_journal is not None else set())
    targets = sorted(denominator.live_targets())
    probe_deadline = started_at + (deadline - started_at) * 0.2
    probing = False

    class ProbeBudgetExhausted(LaneClosed):
        pass

    def check():
        if history_stopped(stop_file):
            raise LaneClosed("stop file")
        if now_fn() >= deadline:
            raise LaneClosed("deadline")
        if protected(client.gate.snapshot(), now_fn().timestamp()):
            raise LaneClosed("gate protection")
        debt = trino.execute_query(render_live_debt_sql(targets, now_fn()))
        # Unknown evidence is a closed gate, including missing aggregates.
        if len(debt) != 1 or debt[0][0] is None or int(debt[0][0]) != 0:
            raise LaneClosed("live debt or unknown live state")
        if probing and now_fn() >= probe_deadline:
            raise ProbeBudgetExhausted("probe budget")

    previous_check, previous_sleep = client.before_attempt, client.sleep_fn
    previous_admission, previous_gate_sleep = client.gate.admission_check, client.gate.sleep_fn
    client.before_attempt = check
    client.gate.admission_check = check
    client.sleep_fn = client.gate.sleep_fn = lambda seconds: interruptible_sleep(seconds, check)

    def fetch(request, refresh=True):
        check()
        return _fetch(client, request, force_refresh=refresh)

    def fetch_player(request):
        result = fetch(request, refresh=False)
        fetched = _raw_ref(result).fetched_at
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=timezone.utc)
        if now_fn() - fetched > PLAYER_RAW_TTL:
            result = fetch(request)
        client.flush()  # each body survives an interruption of the match
        return result

    def teams_for(slug, event_id, home, away, manifest):
        teams = {}
        for team_id in (home, away):
            result = fetch(urls.team_statistics(slug, event_id, int(team_id)))
            teams[int(team_id)] = statistics(result.json_data), _raw_ref(result)
            manifest.append({"team_id": int(team_id), **_manifest(result)})
        return teams

    def players_for(slug, year, event_id, depth):
        scope = delete_filter(slug, year, [event_id])
        matches = stored_rows(trino, MATCH_TABLE, scope)
        if len(matches) != 1:
            raise EspnParseError("player stage needs one stored match")
        match = matches[0]
        state_dir = client_state_dir(client)
        if match["lineup_state"] != "captured":
            check()
            report["players_not_applicable"] += int(publish_players(
                trino=trino, slug=slug, year=year, event_id=event_id, rosters=None,
                players={}, manifest=[], has_xg=depth.has_xg, state_dir=state_dir))
            return depth
        lineup = stored_rows(trino, LINEUP_TABLE, scope)
        manifest, rosters, players = [], {}, {}
        for side in ("home", "away"):
            team_id = int(match[f"{side}_team_id"])
            result = fetch_player(urls.roster(slug, event_id, team_id))
            rosters[team_id] = parse_roster(result.json_data)
            manifest.append({"team_id": team_id, **_manifest(result)})
        # Identity first: a foreign roster spends no player requests.
        _check_roster(rosters, lineup)
        for team_id, roster in rosters.items():
            for athlete, played in roster.items():
                if played:
                    result = fetch_player(urls.player_statistics(slug, event_id, team_id, athlete))
                    players[(team_id, athlete)] = (player_statistics(result.json_data, athlete),
                                                   {"athlete_id": athlete, **_manifest(result)})
        support = xg_support([body for body, _ in players.values()])
        has_xg = True if depth.has_xg is True or support is True else depth.has_xg
        if has_xg is True and depth.has_xg is not True:
            depth = replace(depth, has_xg=True, manifest_json=canonical_json([
                *json.loads(depth.manifest_json or "[]"),
                *({"event_id": event_id, **entry} for entry in manifest),
                *({"event_id": event_id, **entry} for _, entry in players.values()),
            ]))
            client.flush()
            save_depth(conn, depth)
        client.flush()  # raw-before-Bronze for every roster and player body
        check()
        report["players_completed"] += int(publish_players(
            trino=trino, slug=slug, year=year, event_id=event_id, rosters=rosters,
            players=players, manifest=manifest, has_xg=has_xg, state_dir=state_dir))
        return depth

    try:
        check()
        # Never-probed editions precede retries; an unavailable early league
        # must not consume every run's probe budget before its neighbours.
        registry = {}
        for slug in sorted(denominator.targets()):
            year = denominator.row(slug).current_season_year
            registry[slug] = load_depth(trino, slug, year) if year is not None else None

        def probe_order(slug):
            depth = registry[slug]
            checked = depth.checked_at if depth is not None else None
            if checked is not None and checked.tzinfo is None:
                checked = checked.replace(tzinfo=timezone.utc)
            return (checked is not None, checked.timestamp() if checked else 0, slug)

        # Reserve most of the run for already classified full editions.
        for map_only in (True, False):
            probing = map_only
            slugs = sorted(denominator.targets(), key=probe_order) if map_only else sorted(denominator.targets())
            for slug in slugs:
                if map_only and now_fn() >= probe_deadline:
                    break
                try:
                    check()
                except ProbeBudgetExhausted:
                    break
                year = denominator.row(slug).current_season_year
                if year is None:
                    report["waiting"] += int(map_only)
                    continue
                candidates = trino.execute_query(
                    "SELECT event_id, home_team_id, away_team_id, deep_state FROM iceberg.bronze.espn_match "
                    "WHERE competition_slug = ? AND season_year = ? AND played_final = true "
                    "AND first_published_at IS NOT NULL AND duplicate_of IS NULL ORDER BY event_id",
                    (slug, year),
                )
                depth = load_depth(trino, slug, year)
                if map_only and depth is not None and depth.error and depth.checked_at is not None:
                    checked = depth.checked_at.replace(tzinfo=timezone.utc) if depth.checked_at.tzinfo is None else depth.checked_at
                    if now_fn() < checked + timedelta(days=1):
                        continue
                if not candidates:
                    if not map_only:
                        continue
                    if depth is None:
                        save_depth(conn, Depth(slug, year, error="waiting for a published played match"))
                    report["waiting"] += 1
                    continue
                sample_manifest = []
                if not map_only and (depth is None or depth.error is not None or depth.deep_level != "full"):
                    continue
                try:
                    if depth is None or depth.checked_at is None or depth.error is not None:
                        event_id, home, away, _ = candidates[0]
                        result = fetch(urls.plays(slug, int(event_id), limit=1))
                        sample_manifest = [{"probe": True, "event_id": int(event_id), **_manifest(result)}]
                        page = parse_page(result.json_data)
                        if page.pages and page.index != 1:
                            raise EspnParseError("depth probe did not return page 1")
                        level = deep_level(page.count)
                        client.flush()
                        depth = Depth(slug, year, page.count, level, None, int(event_id),
                                      now_fn(), canonical_json(sample_manifest))
                        save_depth(conn, depth)
                        report["mapped"] += 1
                    if map_only or depth.deep_level != "full":
                        continue
                    # Completed matches leave the queue before any per-match work;
                    # only those still missing confirmed xG are read again, in one batch.
                    completed = [int(row[0]) for row in candidates if row[3] == "completed"]
                    upgrade = set()
                    if depth.has_xg is True and completed:
                        confirmed = {}
                        for row in stored_rows(trino, TEAM_STATS_TABLE, delete_filter(slug, year, completed)):
                            confirmed.setdefault(row["event_id"], []).append(
                                row["deep_stats_json"] is not None and
                                json.loads(row["deep_stats_json"]).get("has_xg") is True)
                        upgrade = {event_id for event_id in completed
                                   if confirmed.get(event_id, []) != [True, True]}
                    candidates = [row for row in candidates
                                  if row[3] != "completed" or int(row[0]) in upgrade]
                    done = set(completed)
                    retries = load_retries(trino, slug, year)
                    # Unattempted matches precede due retries, even after a restart.
                    candidates.sort(key=lambda row: (row[0] in retries,
                                                     retries.get(row[0]) or datetime.min, row[0]))
                    for event_id, home, away, _ in candidates:
                        retry_after = retries.get(event_id)
                        if retry_after is not None:
                            if retry_after.tzinfo is None:
                                retry_after = retry_after.replace(tzinfo=timezone.utc)
                            if now_fn() < retry_after:
                                continue
                        check()
                        try:
                            # Persist before I/O so deadline/stop interruptions also
                            # advance the next run instead of replaying its prefix.
                            save_retry(conn, slug, year, event_id, now_fn() + timedelta(days=1))
                            manifest, play_rows = [], []

                            def page_fetch(index):
                                result = fetch(urls.plays(slug, int(event_id), index))
                                manifest.append({"page": index, **_manifest(result)})
                                parsed = parse_page(result.json_data)
                                play_rows.extend((play, _raw_ref(result)) for play in parsed.items)
                                return result.json_data

                            items = collect_plays(page_fetch)
                            if len(items) < 1000:
                                raise EspnParseError("full edition returned incomplete match depth")
                            teams = teams_for(slug, event_id, home, away, manifest)
                            support = xg_support([*items, *(body for body, _ in teams.values())])
                            has_xg = True if depth.has_xg is True or support is True else support
                            if has_xg != depth.has_xg:
                                depth = replace(depth, has_xg=has_xg,
                                                manifest_json=canonical_json([
                                                *json.loads(depth.manifest_json or "[]"),
                                                *({"event_id": int(event_id), **entry} for entry in manifest),
                                            ]))
                                client.flush()
                                save_depth(conn, depth)
                            client.flush()  # raw-before-Bronze, including every page
                            check()
                            report["completed"] += int(publish_deep(
                                trino=trino, slug=slug, year=year, event_id=int(event_id),
                                plays=play_rows, teams=teams, manifest=manifest, has_xg=has_xg,
                                state_dir=client_state_dir(client),
                            ))
                            save_retry(conn, slug, year, event_id, None)
                            done.add(int(event_id))
                        except (LaneClosed, AllOriginsBlocked, DailyCapExceeded):
                            raise
                        except (EspnParseError, EspnTransportError) as exc:
                            report["failed"] += 1
                            errors.append(f"{slug}:{year}:{event_id}: {type(exc).__name__}: {exc}")
                    # Players only after every first-stage match of this edition.
                    pending = [int(row[0]) for row in trino.execute_query(PLAYERS_PENDING_SQL, (slug, year))
                               if int(row[0]) in done]
                    retries = load_retries(trino, slug, year, PLAYER_RETRY_TABLE)
                    pending.sort(key=lambda event_id: (event_id in retries,
                                                       retries.get(event_id) or datetime.min, event_id))
                    for event_id in pending:
                        retry_after = retries.get(event_id)
                        if retry_after is not None:
                            if retry_after.tzinfo is None:
                                retry_after = retry_after.replace(tzinfo=timezone.utc)
                            if now_fn() < retry_after:
                                continue
                        check()
                        # No deferral on interruption: the next run resumes from raw.
                        try:
                            depth = players_for(slug, year, event_id, depth)
                            if event_id in retries:
                                save_retry(conn, slug, year, event_id, None, PLAYER_RETRY_TABLE)
                        except (LaneClosed, AllOriginsBlocked, DailyCapExceeded):
                            raise
                        except (EspnParseError, EspnTransportError) as exc:
                            save_retry(conn, slug, year, event_id, now_fn() + timedelta(days=1),
                                       PLAYER_RETRY_TABLE)
                            report["players_failed"] += 1
                            errors.append(f"{slug}:{year}:{event_id}: players: {type(exc).__name__}: {exc}")
                except ProbeBudgetExhausted:
                    break
                except (LaneClosed, AllOriginsBlocked, DailyCapExceeded):
                    raise
                except (EspnParseError, EspnTransportError) as exc:
                    report["failed"] += 1
                    errors.append(f"{slug}:{year}: {type(exc).__name__}: {exc}")
                    # An uncertain probe cannot leave a previously eligible map.
                    if depth is None or depth.deep_level is None:
                        client.flush()
                        save_depth(conn, Depth(slug, year, sample_event_id=int(candidates[0][0]),
                                               checked_at=now_fn(), manifest_json=canonical_json(sample_manifest),
                                               error=str(exc)))
    except (LaneClosed, AllOriginsBlocked, DailyCapExceeded) as exc:
        report["reason"] = str(exc)
    finally:
        client.before_attempt, client.sleep_fn = previous_check, previous_sleep
        client.gate.admission_check, client.gate.sleep_fn = previous_admission, previous_gate_sleep
        client.flush()
        entries = client.ledger[offset:]
        if hasattr(client, "flush_attempts"):
            client.flush_attempts(conn)
        flush_journal(conn, journal_rows(entries, run_id=run_id, task_id="run_deep"))
    players = [entry for entry in entries if entry.endpoint in _PLAYER_ENDPOINTS]
    report.update(
        players_requests=sum(entry.attempts for entry in players),
        players_direct_bytes=sum(entry.direct_bytes for entry in players),
        errors=errors, requests=sum(entry.attempts for entry in entries),
        direct_bytes=sum(entry.direct_bytes for entry in entries),
        retries=sum(max(0, entry.attempts - 1) for entry in entries),
        cache_hits=sum(entry.attempts == 0 and entry.disposition in ("cache_hit", "raw_replay") for entry in entries),
    )
    if attempt_journal is not None:
        attempts = [row for row in attempt_journal.rows(started_at)
                    if row["run_id"] == run_id and row["task_id"] == "run_deep"
                    and row["attempt_id"] not in initial_attempts]
        ledger_complete = len(attempts) == report["requests"]
        report["requests"] = len(attempts)
        report["direct_bytes"] = sum(row["direct_bytes"] for row in attempts)
        players = [row for row in attempts if row["endpoint"] in _PLAYER_ENDPOINTS]
        report["players_requests"] = len(players)
        report["players_direct_bytes"] = sum(row["direct_bytes"] for row in players)
        report["retry_attempts_lower_bound"] = report["retries"]
        report["request_ledger_complete"] = ledger_complete
        if not ledger_complete:
            report["retries"] = None
    completed = report["completed"]
    report["requests_per_completed_match"] = report["requests"] / completed if completed else None
    report["kb_per_completed_match"] = report["direct_bytes"] / 1024 / completed if completed else None
    completed = report["players_completed"]
    report["requests_per_players_match"] = report["players_requests"] / completed if completed else None
    report["kb_per_players_match"] = report["players_direct_bytes"] / 1024 / completed if completed else None
    return report
